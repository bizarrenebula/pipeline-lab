/* Pipeline Lab — simulation engine.
 * Records travel in "batches" {n, tMin, tMax, u, d}: count, oldest/newest event time,
 * and the fraction of updates and deletes (the rest are inserts / appends).
 * Every node has an input queue (q). Each tick a node takes what its capacity allows,
 * transforms it, optionally holds it (latency, commits, windows) and emits it on its links.
 * Links carry batches as animated particles; arrival appends to the target's queue.
 */
window.PL = window.PL || {};
(function (PL) {
  'use strict';
  const S = PL.S = {
    nodes: [], links: [], view: { x: 0, y: 0, k: 1 }, t: 0, speed: 1, running: true,
    sel: null, particles: [], nid: 1, lid: 1
  };
  const byId = PL.byId = id => S.nodes.find(n => n.id === id);
  PL.outLinks = n => S.links.filter(l => l.from === n.id);
  PL.inLinks = n => S.links.filter(l => l.to === n.id);
  PL.isControlLink = l => !!PL.TYPES[byId(l.from).type].control;
  PL.isOrchestrated = n => PL.inLinks(n).some(l => PL.isControlLink(l));
  PL.upstream = n => PL.inLinks(n).filter(l => !PL.isControlLink(l)).map(l => byId(l.from));

  /* ---------- batches ---------- */
  function mk(n, t, u, d) { return { n, tMin: t, tMax: t, u: u || 0, d: d || 0 }; }
  function merge(a, b) {
    if (!a) return Object.assign({}, b);
    const n = a.n + b.n || 1e-9;
    return { n: a.n + b.n, tMin: Math.min(a.tMin, b.tMin), tMax: Math.max(a.tMax, b.tMax),
      u: (a.u * a.n + b.u * b.n) / n, d: (a.d * a.n + b.d * b.n) / n };
  }
  function push(node, b) {
    if (!b || b.n <= 0) return;
    const r = node.rt; r.q.push(b); r.qN += b.n; r.inW += b.n;
  }
  function take(node, max) {
    const r = node.rt; let out = null, need = max;
    while (need > 1e-9 && r.q.length) {
      const b = r.q[0];
      if (b.n <= need) { r.q.shift(); out = merge(out, b); need -= b.n; r.qN -= b.n; }
      else { const part = Object.assign({}, b, { n: need }); b.n -= need; r.qN -= need; out = merge(out, part); need = 0; }
    }
    if (r.qN < 1e-6) { r.qN = 0; }
    return out;
  }
  PL.take = take;

  /* ---------- node runtime state ---------- */
  PL.initRt = function (n) {
    n.rt = { q: [], qN: 0, hold: [], inW: 0, outW: 0, inRate: 0, outRate: 0, util: 0, total: 0,
      busyUntil: 0, nextAt: null, lastDur: 0, runs: [], log: [], lastT: null, visibleT: null,
      parts: [], files: 0, deleteFiles: 0, snapshots: [], snapId: 0, credits: 0, activeT: 0, lastWork: -99,
      fresh: [], scd: null, runNo: 0, busyCores: 0, used: 0, totalCores: 0, events: 0 };
  };

  /* ---------- emission ---------- */
  function emit(node, b) {
    if (!b || b.n <= 0) return;
    node.rt.outW += b.n;
    for (const l of PL.outLinks(node)) {
      if (PL.isControlLink(l)) continue;
      l.rt.buf = merge(l.rt.buf, b);
    }
  }
  function hold(node, b, at) { if (b && b.n > 0) node.rt.hold.push({ b, at }); }
  function releaseHeld(node) {
    const r = node.rt; if (!r.hold.length) return;
    const keep = [];
    for (const h of r.hold) { if (S.t >= h.at) emit(node, h.b); else keep.push(h); }
    r.hold = keep;
  }
  PL.trigger = function (from, l) {
    S.particles.push({ l, p: 0, dur: linkLen(l) / 240, ctrl: true });
  };

  /* ---------- context helpers ---------- */
  function kafkaUp(n) { return PL.upstream(n).find(u => u.type === 'kafka'); }
  function maxShare(k) { const p = +k.cfg.partitions; return k.cfg.skew ? Math.max(0.5, 1 / p) : 1 / p; }
  PL.partShare = function (k, i) {
    const p = +k.cfg.partitions; if (p === 1) return 1;
    if (!k.cfg.skew) return 1 / p;
    const hot = Math.max(0.5, 1 / p);
    return i === 0 ? hot : (1 - hot) / (p - 1);
  };
  function parLimit(n, workers, per) {
    const k = kafkaUp(n);
    if (!k) return { cap: workers * per, used: workers, limitedBy: null };
    const p = +k.cfg.partitions;
    const cap = Math.min(workers * per, per / maxShare(k));
    return { cap, used: Math.min(workers, p), limitedBy: p < workers ? p : null };
  }
  function readFactor(n) {
    let f = 1;
    for (const u of PL.upstream(n)) {
      if (u.type === 's3') f = Math.min(f, { json: 0.35, csv: 0.5, parquet: 1 }[u.cfg.format] / (1 + u.rt.files / 4000));
      if (u.type === 'iceberg') f = Math.min(f, PL.icebergRead(u));
    }
    return f;
  }
  PL.readFactor = readFactor;
  PL.icebergRead = function (n) {
    const r = n.rt, target = Math.ceil(r.total / 150000) + 1;
    return 1 / (1 + Math.max(0, r.files - target) / 120 + r.deleteFiles / 25);
  };
  const SF_SIZE = { XS: 1, S: 2, M: 4, L: 8, XL: 16 };
  function sfUp(n) { return PL.upstream(n).find(u => u.type === 'snowflake'); }
  function wakeWarehouse(sf, until) {
    if (!sf) return 0;
    const r = sf.rt; const suspended = sf.cfg.suspend && S.t - r.lastWork > 10;
    r.lastWork = Math.max(r.lastWork, until || S.t);
    return suspended ? 3 : 0;
  }
  PL.sfState = function (sf) {
    if (sf.cfg.down) return 'down';
    const r = sf.rt;
    if (!sf.cfg.suspend) return 'running';
    if (S.t < r.lastWork) return 'running';
    return S.t - r.lastWork > 10 ? 'suspended' : 'running';
  };

  /* ---------- processing patterns ---------- */
  function utilize(r, used, possible) { r.util += ((possible > 0 ? Math.min(1, used / possible) : 0) - r.util) * 0.08; }
  function streamProc(n, dt, cap, xf, latency) {
    const r = n.rt; r.cap = cap;
    const b = take(n, cap * dt);
    utilize(r, b ? b.n : 0, cap * dt);
    if (!b) return;
    const o = xf ? xf(b) : b;
    if (latency > 0) hold(n, o, S.t + latency); else emit(n, o);
  }
  function microProc(n, cap, T, xf, overhead) {
    const r = n.rt; r.cap = cap * T / (T + overhead);
    if (r.nextAt == null) r.nextAt = S.t + T;
    utilize(r, S.t < r.busyUntil ? 1 : 0, 1);
    if (S.t < r.nextAt) return;
    if (S.t < r.busyUntil) { r.overrun = true; r.nextAt = r.busyUntil; return; }
    r.overrun = false;
    const b = take(n, cap * T);
    const dur = overhead + (b ? b.n / cap : 0);
    r.busyUntil = S.t + dur; r.lastDur = dur; r.nextAt = S.t + T; r.runNo++;
    if (b) hold(n, xf ? xf(b) : b, S.t + dur);
  }
  /* Batch run, started by a schedule or a trigger. Returns run duration or null if skipped. */
  PL.runBatch = function (n) {
    const r = n.rt, T = SIM[n.type];
    if (n.cfg.down) { r.runs.unshift({ t: S.t, dur: 0, n: 0, status: 'failed' }); r.runs.length = Math.min(r.runs.length, 8); return null; }
    if (S.t < r.busyUntil) { r.runs.unshift({ t: S.t, dur: 0, n: 0, status: 'skipped' }); r.runs.length = Math.min(r.runs.length, 8); return null; }
    const cap = T.cap(n);
    const b = take(n, Infinity);
    const work = T.work ? T.work(n, b) : (b ? b.n : 0);
    const dur = T.overhead(n) + work / Math.max(cap, 1);
    r.busyUntil = S.t + dur; r.runStart = S.t; r.lastDur = dur; r.runNo++;
    if (T.onRun) T.onRun(n, b, dur);
    if (b) { const o = T.xf ? T.xf(n, b) : b; hold(n, o, S.t + dur); }
    r.runs.unshift({ t: S.t, dur, n: b ? b.n : 0, status: 'ok' }); r.runs.length = Math.min(r.runs.length, 8);
    return dur;
  };
  function ownSchedule(n, every) {
    const r = n.rt;
    if (PL.isOrchestrated(n)) { r.nextAt = null; return; }
    if (r.nextAt == null) r.nextAt = S.t + Math.min(every, 4);
    if (S.t >= r.nextAt) { r.nextAt = S.t + every; PL.runBatch(n); }
  }

  /* ---------- per-type behaviour ---------- */
  const MIX = { ins: [0.08, 0.02], mix: [0.35, 0.08], upd: [0.7, 0.05] };
  const SIM = PL.SIM = {
    api: { step(n, dt) { push(n, mk(n.cfg.rate * dt, S.t)); streamProc(n, dt, 1e9, null, 0.2); } },
    file: { step(n, dt) { push(n, mk(n.cfg.rate * dt, S.t)); streamProc(n, dt, 1e9, null, 0.05); } },
    script: {
      cap: n => +n.cfg.cap * readFactor(n),
      overhead: () => 0.6,
      step(n, dt) { if (PL.isOrchestrated(n)) { n.rt.cap = this.cap(n); return; } streamProc(n, dt, this.cap(n), null, 0.1); }
    },
    table: { step(n, dt) { sink(n, dt); } },
    schedule: { step(n) { controller(n, +n.cfg.every, false); } },
    airflow: { step(n) { controller(n, +n.cfg.interval, n.cfg.paused); } },
    postgres: {
      step(n, dt) {
        const [u, d] = MIX[n.cfg.mix];
        push(n, mk(n.cfg.rate * dt, S.t, u, d));
        streamProc(n, dt, 1e9, null, 0);
        n.rt.events += n.cfg.rate * dt;
      }
    },
    app: {
      step(n, dt) {
        const f = n.cfg.burst ? 1 + 0.8 * Math.sin(S.t * Math.PI * 2 / 20) : 1;
        push(n, mk(n.cfg.rate * f * dt, S.t)); streamProc(n, dt, 1e9, null, 0);
      }
    },
    cdc: {
      step(n, dt) {
        const r = n.rt;
        if (n.cfg.snapshot && !r.snap) { r.snap = true; push(n, mk(20000, S.t)); }
        if (n.cfg.snapshot && r.snap && r.qN < 50) { n.cfg.snapshot = false; r.snap = false; PL.ui && PL.ui.refreshInspector(); }
        if (n.cfg.mode === 'log') streamProc(n, dt, 4000, null, 0.3);
        else microProc(n, 3000, +n.cfg.poll, b => {
          const ins = b.n * (1 - b.u - b.d), upd = b.n * b.u * 0.6, tot = ins + upd;
          return { n: tot, tMin: b.tMin - +n.cfg.poll / 2, tMax: b.tMax, u: tot ? upd / tot : 0, d: 0 };
        }, 0.2);
      }
    },
    kafka: {
      cap(n) {
        const r = n.rt; let c = 500 / maxShare(n);
        if (n.cfg.brokerDown) {
          if (n.cfg.rf === '1') c *= 2 / 3;
          else if (S.t - (r.downAt || 0) < 2.5) c = 0;
          else c *= 0.95;
        }
        return c;
      },
      step(n, dt) {
        const r = n.rt, p = +n.cfg.partitions;
        while (r.parts.length < p) r.parts.push({ end: 0, rate: 0 });
        if (r.parts.length > p) r.parts.length = p;
        if (n.cfg.brokerDown && r.downAt == null) r.downAt = S.t;
        if (!n.cfg.brokerDown) r.downAt = null;
        const lat = n.cfg.acks === 'all' ? (n.cfg.rf === '1' ? 0.05 : 0.15) : 0.03;
        const cap = this.cap(n); r.cap = cap;
        const b = take(n, cap * dt);
        utilize(r, b ? b.n : 0, cap * dt);
        for (let i = 0; i < p; i++) {
          const add = b ? b.n * PL.partShare(n, i) : 0;
          r.parts[i].end += add; r.parts[i].rate += (add / dt - r.parts[i].rate) * 0.1;
        }
        if (b) hold(n, b, S.t + lat);
      }
    },
    spark: {
      cap(n) { const x = parLimit(n, n.cfg.executors * n.cfg.cores, 220); n.rt.used = x.used; n.rt.limitedBy = x.limitedBy; return x.cap * readFactor(n); },
      overhead: () => 3,
      xf: (n, b) => n.cfg.job === 'agg' ? Object.assign({}, b, { n: b.n * 0.1, u: 0, d: 0 }) : Object.assign({}, b, { n: b.n * 0.97 }),
      step(n, dt) {
        const cap = this.cap(n); n.rt.cap = cap; n.rt.totalCores = n.cfg.executors * n.cfg.cores;
        if (n.cfg.mode === 'stream') { n.rt.nextBatchAt = null; microProc(n, cap, +n.cfg.trigger, b => this.xf(n, b), 0.4); }
        else ownSchedule(n, +n.cfg.every);
      }
    },
    flink: {
      route(n, b) {
        const ck = +n.cfg.ckpt; let at = S.t + 0.05;
        if (n.cfg.op === 'window') { at = Math.ceil(S.t / 10) * 10 + +n.cfg.watermark; b = Object.assign({}, b, { n: b.n * 0.05, u: 0, d: 0 }); }
        if (n.cfg.eos) at = Math.ceil(at / ck) * ck;
        hold(n, b, at);
      }
    },
    s3: {
      step(n, dt) {
        const r = n.rt;
        const b = take(n, 1e9); utilize(r, 0, 1); r.cap = 1e9;
        if (b) { r.total += b.n; r.pending = (r.pending || 0) + b.n; hold(n, b, S.t + 1); }
        r.roll = (r.roll || 0) + dt;
        if (r.roll >= 2) { r.roll = 0; if (r.pending > 0) { r.files += Math.max(1, Math.ceil(r.pending / 20000)); r.pending = 0; } }
        r.bytes = r.total * { json: 900, csv: 450, parquet: 120 }[n.cfg.format];
      }
    },
    iceberg: {
      cap(n) { const r = n.rt; return n.cfg.mode === 'cow' ? 5000 / (1 + 6 * (r.ud || 0)) : 7000; },
      step(n, dt) {
        const r = n.rt, T = +n.cfg.commit;
        const cap = this.cap(n); r.cap = cap;
        if (r.nextAt == null) r.nextAt = S.t + T;
        utilize(r, S.t < r.busyUntil ? 1 : 0, 1);
        if (S.t >= r.nextAt) {
          r.nextAt = S.t + T;
          const b = take(n, cap * T);
          if (b && b.n > 0.5) {
            r.ud += ((b.u + b.d) - (r.ud || 0)) * 0.3;
            r.total += b.n;
            r.files += Math.max(1, Math.ceil(b.n / 6000));
            const rowLevel = (b.u + b.d) > 0.01;
            if (n.cfg.mode === 'mor' && rowLevel) r.deleteFiles += Math.max(1, Math.ceil(b.n * (b.u + b.d) / 4000));
            pushSnap(r, rowLevel ? 'overwrite' : 'append', b.n);
            const dur = 0.3 + b.n / cap; r.busyUntil = S.t + dur;
            hold(n, b, S.t + dur);
          }
        }
        if (n.cfg.compaction) {
          r.compactAt = r.compactAt == null ? S.t + 20 : r.compactAt;
          if (S.t >= r.compactAt) {
            r.compactAt = S.t + 20;
            const target = Math.ceil(r.total / 150000) + 1;
            if (r.files > target + 4 || r.deleteFiles > 0) { r.files = target; r.deleteFiles = 0; pushSnap(r, 'replace', 0); }
          }
        } else r.compactAt = null;
      }
    },
    snowflake: {
      cap(n) {
        if (PL.upstream(n).some(u => u.type === 'iceberg')) return 9000;
        if (n.cfg.ingest === 'stream') return 8000;
        if (n.cfg.ingest === 'pipe') return 6000;
        return 1500 * SF_SIZE[n.cfg.size];
      },
      overhead: n => 1.5 + wakeWarehouse(n),
      onRun(n, b, dur) { n.rt.lastWork = Math.max(n.rt.lastWork, S.t + dur); },
      step(n, dt) {
        const r = n.rt, cap = this.cap(n); r.cap = cap;
        if (PL.upstream(n).some(u => u.type === 'iceberg')) streamProc(n, dt, cap, null, 3);
        else if (n.cfg.ingest === 'stream') streamProc(n, dt, cap, null, 1.5);
        else if (n.cfg.ingest === 'pipe') streamProc(n, dt, cap, null, 8);
        else ownSchedule(n, +n.cfg.every);
        const st = PL.sfState(n);
        if (st === 'running') { r.activeT += dt; r.credits += SF_SIZE[n.cfg.size] * dt / 3600; }
      }
    },
    dbt: {
      cap(n) { const sf = sfUp(n); return 4000 * (sf ? SF_SIZE[sf.cfg.size] : 1); },
      overhead(n) { return 1 + n.cfg.models * 0.08 + wakeWarehouse(sfUp(n)); },
      work(n, b) {
        const nn = b ? b.n : 0; n.rt.total += nn;
        let w = n.cfg.mat === 'table' ? n.rt.total : n.cfg.mat === 'incremental' ? nn : 0;
        if (n.cfg.scd !== 'none') w += nn * 0.5;
        return w;
      },
      onRun(n, b, dur) { const sf = sfUp(n); if (sf) sf.rt.lastWork = Math.max(sf.rt.lastWork, S.t + dur); PL.scdApply(n, b); },
      xf(n, b) { return n.cfg.scd === 'scd2' ? Object.assign({}, b, { n: b.n * (1 + b.u + b.d) }) : b; },
      step(n) { n.rt.cap = this.cap(n); ownSchedule(n, +n.cfg.every); }
    },
    bi: {
      step(n, dt) {
        const r = n.rt;
        sink(n, dt);
        if (n.cfg.mode === 'live') {
          if (r.lastT != null) r.visibleT = r.lastT;
          r.qAt = (r.qAt || 0) + dt;
          if (r.qAt > 4) { r.qAt = 0; const sf = sfUp(n); if (sf) wakeWarehouse(sf, S.t + 1); }
        } else {
          if (r.nextAt == null) r.nextAt = S.t + +n.cfg.refresh;
          if (S.t >= r.nextAt) { r.nextAt = S.t + +n.cfg.refresh; r.visibleT = r.lastT; const sf = sfUp(n); if (sf) wakeWarehouse(sf, S.t + 2); }
        }
        const f = r.visibleT == null ? null : S.t - r.visibleT;
        r.freshness = f;
        r.fs = (r.fs || 0) + dt;
        if (r.fs > 0.5) { r.fs = 0; r.fresh.push(f); if (r.fresh.length > 120) r.fresh.shift(); }
      }
    }
  };
  SIM.flink.step = function (n, dt) {
    const x = parLimit(n, +n.cfg.par, 350), r = n.rt;
    r.used = x.used; r.limitedBy = x.limitedBy; r.totalCores = +n.cfg.par;
    const cap = x.cap * readFactor(n); r.cap = cap;
    const b = take(n, cap * dt); utilize(r, b ? b.n : 0, cap * dt);
    if (b) SIM.flink.route(n, b);
  };

  function sink(n, dt) {
    const r = n.rt; const b = take(n, 1e12); r.cap = 1e12;
    if (b) { r.total += b.n; r.lastT = r.lastT == null ? b.tMax : Math.max(r.lastT, b.tMax); }
  }
  function pushSnap(r, op, n) {
    r.snapId++; r.snapshots.unshift({ id: r.snapId, op, n, t: S.t });
    if (r.snapshots.length > 8) r.snapshots.length = 8;
  }
  function controller(n, every, paused) {
    const r = n.rt;
    if (r.nextAt == null) r.nextAt = S.t + Math.min(3, every);
    if (paused) { r.nextAt = Math.max(r.nextAt, S.t + 0.001); return; }
    if (S.t >= r.nextAt) {
      r.nextAt = S.t + every; r.runNo++;
      const run = { no: r.runNo, t: S.t, tasks: {} };
      r.runs.unshift(run); if (r.runs.length > 6) r.runs.length = 6;
      for (const l of PL.outLinks(n)) {
        run.tasks[l.to] = { state: 'queued' };
        S.particles.push({ l, p: 0, dur: linkLen(l) / 240, ctrl: true, run });
      }
    }
  }

  /* ---------- SCD demo table (dbt) ---------- */
  const PEOPLE = [['C-104', 'Ada Moreno'], ['C-221', 'Ken Ito'], ['C-307', 'Lena Novak']];
  const CITIES = ['Lisbon', 'Berlin', 'Austin', 'Osaka', 'Krakow', 'Toronto', 'Nairobi', 'Lyon'];
  PL.scdApply = function (n, b) {
    const r = n.rt;
    if (!r.scd) r.scd = PEOPLE.map(([id, name], i) => ({ id, name, city: CITIES[i], from: 0, to: null, cur: true }));
    if (!b || n.cfg.scd === 'none' || b.u <= 0.01) return;
    const changes = Math.min(2, 1 + Math.floor(Math.random() * 2));
    for (let c = 0; c < changes; c++) {
      const [id] = PEOPLE[Math.floor(Math.random() * PEOPLE.length)];
      const cur = r.scd.find(x => x.id === id && x.cur);
      let city; do { city = CITIES[Math.floor(Math.random() * CITIES.length)]; } while (city === cur.city);
      if (n.cfg.scd === 'scd1') { cur.city = city; cur.from = S.t; }
      else { cur.to = S.t; cur.cur = false; r.scd.unshift({ id, name: cur.name, city, from: S.t, to: null, cur: true }); }
    }
    if (r.scd.length > 9) r.scd.length = 9;
  };

  /* ---------- links & particles ---------- */
  PL.NW = 196; PL.NH = 92;
  PL.bez = function (l) {
    const a = byId(l.from), b = byId(l.to);
    const x1 = a.x + PL.NW, y1 = a.y + PL.NH / 2, x2 = b.x, y2 = b.y + PL.NH / 2;
    const dx = Math.max(50, Math.abs(x2 - x1) * 0.5);
    return [x1, y1, x1 + dx, y1, x2 - dx, y2, x2, y2];
  };
  PL.bpt = function (c, t) {
    const u = 1 - t, a = u * u * u, b = 3 * u * u * t, d = 3 * u * t * t, e = t * t * t;
    return [a * c[0] + b * c[2] + d * c[4] + e * c[6], a * c[1] + b * c[3] + d * c[5] + e * c[7]];
  };
  function linkLen(l) {
    const c = PL.bez(l); let len = 0, p = PL.bpt(c, 0);
    for (let i = 1; i <= 12; i++) { const q = PL.bpt(c, i / 12); len += Math.hypot(q[0] - p[0], q[1] - p[1]); p = q; }
    return len;
  }

  function flushLinks() {
    for (const l of S.links) {
      const r = l.rt;
      if (!r.buf || S.t - r.lastFlush < 0.14) continue;
      if (S.particles.length > 900) continue;
      const b = r.buf; r.buf = null; r.lastFlush = S.t;
      S.particles.push({ l, p: 0, dur: linkLen(l) / 260, b, col: pickColor(b) });
    }
  }
  function pickColor(b) { const x = Math.random(); return x < b.d ? 'del' : x < b.d + b.u ? 'upd' : 'ins'; }

  function moveParticles(dt) {
    const keep = [];
    for (const p of S.particles) {
      if (!S.links.includes(p.l)) continue;
      if (p.real || p.realCtrl) { keep.push(p); continue; } // advanced in real time by ui.js
      p.p += dt / Math.max(p.dur, 0.05);
      if (p.p < 1) { keep.push(p); continue; }
      const to = byId(p.l.to);
      if (p.ctrl) {
        const dur = to.type === 'script' ? PL.runBatch(to) : (SIM[to.type].cap ? PL.runBatch(to) : null);
        if (p.run) p.run.tasks[to.id] = dur == null ? { state: to.cfg.down ? 'failed' : 'skipped' } : { state: 'running', end: S.t + dur, dur };
      } else {
        push(to, p.b); p.l.rt.w += p.b.n;
      }
      if (PL.onArrive) PL.onArrive(p);
    }
    S.particles = keep;
  }

  let rateClock = 0;
  PL.tick = function (dt) {
    S.t += dt;
    for (const n of S.nodes) {
      if (n.cfg.down && n.type !== 'kafka') { n.rt.util *= 0.9; continue; }
      SIM[n.type].step(n, dt);
      releaseHeld(n);
    }
    flushLinks();
    moveParticles(dt);
    rateClock += dt;
    if (rateClock >= 0.5) {
      for (const n of S.nodes) {
        const r = n.rt;
        r.inRate += (r.inW / rateClock - r.inRate) * 0.35; r.outRate += (r.outW / rateClock - r.outRate) * 0.35;
        r.inW = 0; r.outW = 0;
      }
      for (const l of S.links) { l.rt.rate += (l.rt.w / rateClock - l.rt.rate) * 0.35; l.rt.w = 0; }
      rateClock = 0;
    }
  };

  /* ---------- graph edits ---------- */
  PL.addNode = function (type, x, y, cfg, id, name) {
    const T = PL.TYPES[type]; const c = {};
    for (const f of T.cfg) c[f.k] = f.def;
    if (type === 'script') c.code = PL.DEFAULT_CODE.python;
    Object.assign(c, cfg || {});
    const n = { id: id || ('n' + S.nid++), type, x: Math.round(x), y: Math.round(y), cfg: c, name: name || T.label };
    if (id) { const num = +String(id).replace(/\D/g, ''); if (num >= S.nid) S.nid = num + 1; }
    PL.initRt(n); S.nodes.push(n); return n;
  };
  PL.canConnect = function (a, b) {
    if (a === b) return 'A component can’t link to itself.';
    if (S.links.some(l => l.from === a.id && l.to === b.id)) return 'These two are already linked.';
    if (!(PL.ALLOW[a.type] || []).includes(b.type)) return PL.connectHint(a.type, b.type);
    // cycle check
    const seen = new Set(); const stack = [b.id];
    while (stack.length) { const id = stack.pop(); if (id === a.id) return 'That link would create a loop, and records would circle forever.'; if (seen.has(id)) continue; seen.add(id); S.links.filter(l => l.from === id).forEach(l => stack.push(l.to)); }
    return null;
  };
  PL.addLink = function (from, to, id) {
    const l = { id: id || ('l' + S.lid++), from, to, rt: { buf: null, lastFlush: 0, w: 0, rate: 0 } };
    if (id) { const num = +String(id).replace(/\D/g, ''); if (num >= S.lid) S.lid = num + 1; }
    S.links.push(l); return l;
  };
  PL.removeNode = function (id) {
    S.nodes = S.nodes.filter(n => n.id !== id);
    S.links = S.links.filter(l => l.from !== id && l.to !== id);
  };
  PL.removeLink = function (id) { S.links = S.links.filter(l => l.id !== id); };
  PL.resetSim = function () {
    S.t = 0; S.particles = [];
    S.nodes.forEach(PL.initRt);
    S.links.forEach(l => { l.rt = { buf: null, lastFlush: 0, w: 0, rate: 0 }; });
  };
})(window.PL);
