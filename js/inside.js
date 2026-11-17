/* Pipeline Lab — "Look inside" views.
 * One animated canvas per component type that opens the box: Kafka partitions, offsets and consumer lag;
 * Iceberg snapshots, manifests and time travel; Spark stages, tasks and shuffle; the Postgres WAL and
 * Debezium's replication slot; Flink checkpoints and watermarks; Snowflake's layers; dbt's DAG; and more.
 * Everything is drawn from the live simulation state, so changing a knob changes the picture.
 */
window.PL = window.PL || {};
(function (PL) {
  'use strict';
  const S = PL.S, R = PL.REC;
  const $ = s => document.querySelector(s);
  const IN = PL.inside = { id: null };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const fmt = n => PL.ui.fmt(n);
  const fmtS = s => s == null || !isFinite(s) ? '–' : R.fmtS(Math.max(0, s));
  const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
  const lerp = (a, b, t) => a + (b - a) * t;
  const STATE = new Map(); // per node id: animation state
  const st = n => { let x = STATE.get(n.id); if (!x) { x = { fl: [], hist: [] }; STATE.set(n.id, x); } return x; };

  /* ---------- colors ---------- */
  let C = {}, colorClock = 9;
  function readColors() {
    const cs = getComputedStyle(document.documentElement);
    ['bg', 'grid', 'panel', 'panel-2', 'line', 'line-2', 'fg', 'muted', 'faint', 'accent', 'accent-soft', 'ins', 'upd', 'del', 'ctrl', 'ok', 'warn', 'bad',
      'c-basic', 'c-source', 'c-ingest', 'c-stream', 'c-process', 'c-orch', 'c-store', 'c-wh', 'c-serve'].forEach(k => C[k] = cs.getPropertyValue('--' + k).trim());
    C.mono = cs.getPropertyValue('--f-mono'); C.ui = cs.getPropertyValue('--f-ui');
  }
  const GROUP = () => [C.accent, C['c-process'], C['c-wh'], C['c-serve'], C['c-store']];

  /* ---------- drawing kit ---------- */
  function kit(ctx, w, h) {
    const g = { ctx, w, h, hits: [] };
    g.rr = (x, y, ww, hh, r) => { r = Math.min(r, ww / 2, hh / 2); ctx.beginPath(); ctx.moveTo(x + r, y); ctx.arcTo(x + ww, y, x + ww, y + hh, r); ctx.arcTo(x + ww, y + hh, x, y + hh, r); ctx.arcTo(x, y + hh, x, y, r); ctx.arcTo(x, y, x + ww, y, r); ctx.closePath(); };
    g.box = (x, y, ww, hh, o) => {
      o = o || {}; g.rr(x, y, ww, hh, o.r == null ? 7 : o.r);
      if (o.fill) { ctx.globalAlpha = o.alpha == null ? 1 : o.alpha; ctx.fillStyle = o.fill; ctx.fill(); ctx.globalAlpha = 1; }
      if (o.stroke !== false) { ctx.setLineDash(o.dash || []); ctx.strokeStyle = o.stroke || C.line; ctx.lineWidth = o.lw || 1; ctx.stroke(); ctx.setLineDash([]); }
    };
    g.text = (s, x, y, o) => {
      o = o || {};
      ctx.font = `${o.weight || 400} ${o.size || 11}px ${o.mono === false ? C.ui : C.mono}`;
      ctx.fillStyle = o.color || C.muted; ctx.textAlign = o.align || 'left'; ctx.textBaseline = o.base || 'middle';
      if (o.max) { let t = String(s); while (t.length > 2 && ctx.measureText(t).width > o.max) t = t.slice(0, -2); if (t !== String(s)) t = t.slice(0, -1) + '…'; s = t; }
      ctx.fillText(s, x, y);
    };
    g.tw = (s, size, mono) => { ctx.font = `400 ${size || 11}px ${mono === false ? C.ui : C.mono}`; return ctx.measureText(s).width; };
    g.line = (x1, y1, x2, y2, o) => { o = o || {}; ctx.beginPath(); ctx.moveTo(x1, y1); ctx.lineTo(x2, y2); ctx.strokeStyle = o.color || C['line-2']; ctx.lineWidth = o.w || 1; ctx.globalAlpha = o.alpha == null ? 1 : o.alpha; ctx.setLineDash(o.dash || []); ctx.stroke(); ctx.setLineDash([]); ctx.globalAlpha = 1; };
    g.curve = (x1, y1, x2, y2, o) => { o = o || {}; const dx = (x2 - x1) / 2; ctx.beginPath(); ctx.moveTo(x1, y1); ctx.bezierCurveTo(x1 + dx, y1, x2 - dx, y2, x2, y2); ctx.strokeStyle = o.color || C['line-2']; ctx.lineWidth = o.w || 1; ctx.globalAlpha = o.alpha == null ? 1 : o.alpha; ctx.setLineDash(o.dash || []); ctx.stroke(); ctx.setLineDash([]); ctx.globalAlpha = 1; };
    g.arrow = (x1, y1, x2, y2, o) => {
      o = o || {}; g.line(x1, y1, x2, y2, o);
      const a = Math.atan2(y2 - y1, x2 - x1), s = o.head || 6;
      ctx.fillStyle = o.color || C['line-2']; ctx.globalAlpha = o.alpha == null ? 1 : o.alpha; ctx.beginPath(); ctx.moveTo(x2, y2);
      ctx.lineTo(x2 - s * Math.cos(a - 0.45), y2 - s * Math.sin(a - 0.45)); ctx.lineTo(x2 - s * Math.cos(a + 0.45), y2 - s * Math.sin(a + 0.45)); ctx.fill(); ctx.globalAlpha = 1;
    };
    g.dot = (x, y, r, col, alpha) => { ctx.globalAlpha = alpha == null ? 1 : alpha; ctx.fillStyle = col; ctx.beginPath(); ctx.arc(x, y, r, 0, Math.PI * 2); ctx.fill(); ctx.globalAlpha = 1; };
    g.hatch = (x, y, ww, hh, col) => {
      ctx.save(); g.rr(x, y, ww, hh, 2); ctx.clip(); ctx.strokeStyle = col; ctx.lineWidth = 1.2; ctx.globalAlpha = 0.75;
      for (let i = -hh; i < ww; i += 5) { ctx.beginPath(); ctx.moveTo(x + i, y + hh); ctx.lineTo(x + i + hh, y); ctx.stroke(); }
      ctx.restore();
    };
    g.hit = (x, y, ww, hh, act) => g.hits.push({ x, y, w: ww, h: hh, act });
    return g;
  }
  // flying dots: things moving between boxes
  function spawn(s, o) { if (s.fl.length < 260) s.fl.push(Object.assign({ t: 0, dur: 0.8, r: 3 }, o)); }
  function flyers(g, s, dt) {
    s.fl = s.fl.filter(f => {
      f.t += dt / f.dur;
      if (f.t >= 1) { if (f.done) f.done(); return false; }
      const e = f.ease === false ? f.t : f.t < 0.5 ? 2 * f.t * f.t : 1 - Math.pow(-2 * f.t + 2, 2) / 2;
      let x, y;
      if (f.curve) { const u = 1 - e, dx = (f.x1 - f.x0) / 2; const c = [f.x0, f.y0, f.x0 + dx, f.y0, f.x1 - dx, f.y1, f.x1, f.y1]; x = u * u * u * c[0] + 3 * u * u * e * c[2] + 3 * u * e * e * c[4] + e * e * e * c[6]; y = u * u * u * c[1] + 3 * u * u * e * c[3] + 3 * u * e * e * c[5] + e * e * e * c[7]; }
      else { x = lerp(f.x0, f.x1, e); y = lerp(f.y0, f.y1, e) - (f.arc ? Math.sin(e * Math.PI) * f.arc : 0); }
      if (f.sq) { g.ctx.fillStyle = f.col; g.ctx.fillRect(x - f.r, y - f.r, f.r * 2, f.r * 2); } else g.dot(x, y, f.r, f.col, f.alpha);
      return true;
    });
  }
  // a rolling log of lines, newest at the bottom
  function addTick(s, text, col) { s.tk = s.tk || []; s.tk.push({ text, col, t: S.t }); if (s.tk.length > 14) s.tk.shift(); }
  function ticker(g, s, x, y, w, h, title) {
    g.text(title, x, y, { size: 9.5, weight: 600, color: C.faint });
    const lh = 15, n = Math.max(1, Math.floor((h - 12) / lh)), lines = (s.tk || []).slice(-n);
    g.box(x, y + 8, w, h - 8, { fill: C['panel-2'], stroke: false, r: 6 });
    lines.forEach((l, i) => {
      const yy = y + 8 + 10 + i * lh, fresh = S.t - l.t < 0.6;
      g.dot(x + 10, yy, 3, l.col || C.muted);
      g.text(l.text, x + 20, yy, { size: 10.5, color: fresh ? C.fg : C.muted, max: w - 28, weight: fresh ? 500 : 400 });
    });
  }
  // emit about `rate` things per second, smoothly, for a given key
  function every(s, key, rate, dt) { s.acc = s.acc || {}; s.acc[key] = (s.acc[key] || 0) + rate * dt; const n = Math.floor(s.acc[key]); s.acc[key] -= n; return Math.min(n, 6); }
  function pickOp(mixOf) { const m = { ins: [0.08, 0.02], mix: [0.35, 0.08], upd: [0.7, 0.05] }[mixOf] || [0, 0]; const x = Math.random(); return x < m[1] ? 'd' : x < m[1] + m[0] ? 'u' : 'c'; }
  function changed(c, after) { const k = R.changedCols(c).filter(x => x !== 'updated_at')[0] || 'updated_at'; const o = after ? c.after : c.before; return k + '=' + (o ? o[k] : ''); }
  function sqlOf(c) { const r = c.after || c.before; return c.op === 'c' ? `INSERT INTO ${c.table} (id, …) VALUES (${typeof c.key === 'number' ? c.key : "'" + c.key + "'"}, …)` : c.op === 'd' ? `DELETE FROM ${c.table} WHERE id = ${typeof c.key === 'number' ? c.key : "'" + c.key + "'"}` : `UPDATE ${c.table} SET ${changed(c, 1)} WHERE id = ${typeof c.key === 'number' ? c.key : "'" + c.key + "'"}`; }
  function opColor(mixOf) {
    const m = { ins: [0.08, 0.02], mix: [0.35, 0.08], upd: [0.7, 0.05] }[mixOf] || [0, 0];
    const x = Math.random(); return x < m[1] ? C.del : x < m[1] + m[0] ? C.upd : C.ins;
  }
  function rootSource(n) { const p = R.pathTo(n); return p[0]; }
  function mixFor(n) { const r = rootSource(n); return r && r.type === 'postgres' ? r.cfg.mix : null; }
  const consumers = k => PL.outLinks(k).filter(l => !PL.isControlLink(l)).map(l => PL.byId(l.to)).filter(Boolean);
  const traceAt = n => PL.trace && PL.trace.active && PL.trace.phase === 'arrived' && PL.trace.steps[PL.trace.i] && PL.trace.steps[PL.trace.i].node === n ? PL.trace.steps[PL.trace.i] : null;
  function runPhase(n) {
    const r = n.rt;
    if (S.t < r.busyUntil) { const start = r.runStart != null && r.busyUntil - r.runStart <= (r.lastDur || 0) + 1e-6 ? r.runStart : r.busyUntil - (r.lastDur || 1); return { busy: true, p: clamp((S.t - start) / Math.max(r.lastDur || 1, 0.01), 0, 1) }; }
    return { busy: false, p: 0, next: r.nextAt != null ? r.nextAt - S.t : null };
  }

  /* =====================================================================
   * Views. Each: title, draw(g, n, s, dt), info(n) static html, live(n, s) html, act(n, s, a) for clicks.
   * ===================================================================== */
  const V = {};

  /* ---------------- Kafka ---------------- */
  V.kafka = {
    sub: 'Partitions, offsets, replicas and consumer lag',
    draw(g, n, s, dt) {
      const r = n.rt, P = +n.cfg.partitions, rows = Math.min(P, 12), rf = +n.cfg.rf, dead = n.cfg.brokerDown;
      const W = g.w, H = g.h;
      const prodX = 12, prodW = Math.min(108, W * 0.16), stripX0 = prodX + prodW + 34, repW = 66, stripX1 = W - repW - 74;
      const top = 40, rowH = Math.min(34, (H - top - 30) / rows), cellW = 12;
      const maxRate = Math.max(1, ...r.parts.map(p => p.rate || 0));
      // records per cell: a stable "nice" unit so cells crawl at a readable pace
      const want = Math.max(maxRate, r.inRate / P) / 2.5; if (!s.unit || want > s.unit * 2.5 || want < s.unit / 2.5) s.unit = niceUnit(want);
      const unit = s.unit;
      g.text('PRODUCERS', prodX, 16, { size: 10, weight: 600, color: C.faint });
      g.text(`TOPIC · ${P} PARTITION${P > 1 ? 'S' : ''} · 1 cell ≈ ${fmt(unit)} records`, stripX0, 16, { size: 10, weight: 600, color: C.faint, max: stripX1 - stripX0 + 60 });
      g.text('REPLICAS', W - repW - 8, 16, { size: 10, weight: 600, color: C.faint });
      [1, 2, 3].forEach(b => g.text('b' + b, W - repW - 8 + (b - 1) * 22 + 7, 30, { size: 10, color: dead && b === 3 ? C.bad : C.faint, align: 'center' }));
      // producers box
      const ups = PL.upstream(n);
      const pH = Math.max(46, rows * rowH - 8);
      g.box(prodX, top, prodW, pH, { fill: C['panel-2'] });
      ups.slice(0, 3).forEach((u, i) => g.text(u.name, prodX + 10, top + 16 + i * 16, { size: 11, color: C.fg, mono: false, max: prodW - 18 }));
      g.text(`${fmt(r.inRate)}/s`, prodX + 10, top + pH - 14, { size: 11, color: C.muted });
      g.text(`acks=${n.cfg.acks}`, prodX + 10, top + pH - 28, { size: 10, color: C.faint });
      const groups = consumers(n), gc = GROUP();
      const totalLag = groups.map(c => c.rt.qN);
      const mix = mixFor(n);
      const tr = traceAt(n);
      for (let i = 0; i < rows; i++) {
        const y = top + i * rowH, ch = rowH - 8, pt = r.parts[i] || { end: 0, rate: 0 };
        const off = dead && rf === 1 && i % 3 === 2;
        g.text('P' + i, stripX0 - 8, y + ch / 2, { size: 11, color: off ? C.bad : C.muted, align: 'right', weight: 500 });
        g.box(stripX0, y, stripX1 - stripX0, ch, { fill: C['panel-2'], stroke: false, r: 3 });
        const e = pt.end / unit, cells = Math.ceil((stripX1 - stripX0) / cellW) + 1;
        const jEnd = Math.floor(e);
        g.ctx.save(); g.rr(stripX0, y, stripX1 - stripX0, ch, 3); g.ctx.clip();
        for (let j = jEnd; j > jEnd - cells && j >= 0; j--) {
          const x = stripX1 - (e - j) * cellW;
          const frac = j === jEnd ? e - jEnd : 1;
          const age = (e - j) / cells;
          g.ctx.globalAlpha = (off ? 0.25 : 1) * (0.95 - age * 0.55) * (j === jEnd ? frac : 1);
          g.ctx.fillStyle = mix ? cellColor(j * 31 + i, mix) : C['c-stream'];
          g.ctx.fillRect(x + 1, y + 2, cellW - 2, ch - 4);
        }
        g.ctx.globalAlpha = 1;
        // consumer group positions and lag
        groups.forEach((c, gi) => {
          const share = PL.partShare(n, i), lag = totalLag[gi] * share;
          const lx = Math.max(stripX0 + 2, stripX1 - lag / unit * cellW);
          if (lag / unit * cellW > 2) g.hatch(lx, y + 1, stripX1 - lx, ch - 2, C.bad);
          const col = gc[gi % gc.length];
          g.ctx.fillStyle = col; g.ctx.beginPath(); g.ctx.moveTo(lx, y + ch - 1); g.ctx.lineTo(lx - 4, y + ch + 5); g.ctx.lineTo(lx + 4, y + ch + 5); g.ctx.fill();
        });
        g.ctx.restore();
        // the traced record sits in this partition
        if (tr && tr.hop.kafka && tr.hop.kafka.partition === i) {
          const x = stripX1 - (e - tr.hop.kafka.offset / unit) * cellW;
          if (x > stripX0) { g.box(x - 3, y - 3, cellW + 4, ch + 6, { stroke: C.fg, lw: 2, r: 3 }); g.text('your record @' + tr.hop.kafka.offset, Math.min(x + cellW + 6, stripX1 - 70), y - 6, { size: 10, color: C.fg, weight: 600 }); }
        }
        g.text(fmt(pt.end), stripX1 + 6, y + ch / 2, { size: 10.5, color: C.muted });
        // replicas: L leader, F follower, on three brokers
        for (let b = 0; b < 3; b++) {
          const x = W - repW - 8 + b * 22, role = replicaRole(i, b, rf, dead);
          if (role === 'L') g.box(x, y + 1, 15, ch - 2, { fill: C['c-stream'], stroke: false, r: 3 });
          else if (role === 'F') g.box(x + 0.5, y + 1.5, 14, ch - 3, { stroke: C['c-stream'], r: 3 });
          else if (role === 'X') g.box(x + 0.5, y + 1.5, 14, ch - 3, { stroke: C.bad, dash: [2, 2], r: 3 });
          if (role === 'L' && ch > 11) g.text('L', x + 7.5, y + ch / 2, { size: 9.5, color: '#fff', align: 'center', weight: 600 });
        }
        // producer → partition
        const k = every(s, 'p' + i, Math.min(14, Math.sqrt(pt.rate) * 0.9), off ? 0 : dt);
        for (let q = 0; q < k; q++) spawn(s, { x0: prodX + prodW, y0: top + pH / 2, x1: stripX1 - 4, y1: y + ch / 2, col: mix ? opColor(mix) : C['c-stream'], dur: 0.7 + Math.random() * 0.3, r: 2.6, curve: true });
      }
      if (P > rows) g.text(`+ ${P - rows} more partitions`, stripX0, top + rows * rowH + 6, { size: 10.5, color: C.faint });
      if (dead) { const x = W - repW - 8 + 44; g.line(x - 2, 24, x + 17, top + rows * rowH - 6, { color: C.bad, w: 1.5 }); }
      flyers(g, s, dt);
    },
    info(n) {
      return `<p>A topic is split into <b>partitions</b>. Each one is an append-only log: new records go on the right end and get the next <b>offset</b>. Nothing is removed when it is read; old segments are deleted only after the retention time.</p>
      <p>Each <b>consumer group</b> (one per downstream component) remembers its own offset per partition. The triangles are those positions; the hatched part is <b>lag</b>, records written but not yet read.</p>
      <p>Every partition has one <b>leader</b> broker (filled) and followers (outlined) that copy it. If a leader’s broker dies, a follower that is in sync takes over.</p>`;
    },
    live(n) {
      const r = n.rt, groups = consumers(n), gc = GROUP();
      const isr = n.cfg.brokerDown ? (n.cfg.rf === '1' ? 'some partitions offline' : `${+n.cfg.rf - 1} of ${n.cfg.rf} in sync on affected partitions`) : `${n.cfg.rf} of ${n.cfg.rf} in sync`;
      return `<div class="metrics">${m('Write rate', fmt(r.inRate) + '/s')}${m('Log end (total)', fmt(r.parts.reduce((a, p) => a + p.end, 0)))}${m('Replicas', isr)}</div>
        <div class="sec-title" style="margin-top:12px">Consumer groups</div>
        ${groups.length ? `<table class="mini-table"><tr><th></th><th>group</th><th>lag</th><th>catch-up</th></tr>${groups.map((c, i) => `<tr><td><i class="sw" style="background:${gc[i % gc.length]}"></i></td><td>${esc(c.name)}</td><td>${fmt(c.rt.qN)}</td><td>${fmtS(c.rt.qN / Math.max(1, c.rt.cap || 1))}</td></tr>`).join('')}</table>` : '<div class="note">No consumers yet. Link Kafka to Spark, Flink or Snowflake.</div>'}
        ${n.cfg.skew ? '<div class="note warn">Hot key: partition 0 takes half of all writes. One consumer task owns it, so it falls behind first.</div>' : ''}`;
    }
  };
  function niceUnit(x) { if (x <= 1) return 1; const p = Math.pow(10, Math.floor(Math.log10(x))); const f = x / p; return (f < 1.5 ? 1 : f < 3.5 ? 2 : f < 7.5 ? 5 : 10) * p; }
  function cellColor(seed, mix) {
    const m = { ins: [0.08, 0.02], mix: [0.35, 0.08], upd: [0.7, 0.05] }[mix];
    const x = ((Math.sin(seed * 12.9898) * 43758.5453) % 1 + 1) % 1;
    return x < m[1] ? C.del : x < m[1] + m[0] ? C.upd : C.ins;
  }
  function replicaRole(p, b, rf, dead) {
    const lead = p % 3;
    const has = b === lead || (rf >= 2 && b === (lead + 1) % 3) || (rf >= 3 && b === (lead + 2) % 3);
    if (!has) return '';
    if (dead && b === 2) return 'X';
    if (dead && lead === 2) { if (rf === 1) return 'X'; const nl = (lead + 1) % 3; return b === nl ? 'L' : 'F'; }
    return b === lead ? 'L' : 'F';
  }

  /* ---------------- Iceberg ---------------- */
  V.iceberg = {
    sub: 'Catalog, snapshots, manifests and files',
    draw(g, n, s, dt) {
      const r = n.rt, W = g.w, H = g.h;
      const snaps = r.snapshots.slice(0, Math.max(2, Math.min(7, Math.floor((W - 270) / 110)))).slice().reverse(); // oldest → newest
      const curId = r.snapId;
      if (s.lastSnap !== curId) { s.lastSnap = curId; s.flash = 1; }
      s.flash = Math.max(0, (s.flash || 0) - dt * 0.8);
      const sel = s.sel && r.snapshots.find(x => x.id === s.sel) ? s.sel : curId;
      // catalog and metadata
      const cy = 34;
      g.box(10, cy - 16, 96, 32, { fill: C['panel-2'] }); g.text('catalog', 58, cy - 4, { size: 11, color: C.fg, align: 'center', weight: 600 }); g.text(n.name.slice(0, 14), 58, cy + 8, { size: 9.5, align: 'center', max: 88 });
      g.arrow(106, cy, 128, cy, { color: C.accent, w: 1.5 });
      g.box(130, cy - 16, 112, 32, { fill: s.flash > 0 ? C['accent-soft'] : C.panel, stroke: C.accent, lw: 1 + s.flash * 2 });
      g.text(`v${curId + 1}.metadata.json`, 186, cy - 3, { size: 10.5, color: C.fg, align: 'center', max: 104 });
      g.text('current pointer', 186, cy + 9, { size: 9, color: C.faint, align: 'center' });
      // snapshot chain
      const sx0 = 262, sw = 96, gap = 14, sy = 14;
      g.text('SNAPSHOTS · click one to time travel', sx0, 8, { size: 9.5, weight: 600, color: C.faint, base: 'top' });
      snaps.forEach((sn, i) => {
        const x = sx0 + i * (sw + gap), y = sy + 10, isCur = sn.id === curId, isSel = sn.id === sel;
        const opc = sn.op === 'replace' ? C.accent : sn.op === 'overwrite' ? C.upd : C.ins;
        const enter = isCur ? 1 - (s.flash || 0) * 0.6 : 1;
        g.ctx.globalAlpha = enter;
        g.box(x, y, sw, 36, { fill: isSel ? C['accent-soft'] : C['panel-2'], stroke: isSel ? C.accent : C.line, lw: isSel ? 2 : 1 });
        g.box(x, y, 4, 36, { fill: opc, stroke: false, r: 2 });
        g.text('s' + sn.id + (isCur ? ' · current' : ''), x + 10, y + 11, { size: 10.5, color: C.fg, weight: 600, max: sw - 14 });
        g.text(sn.op + (sn.n ? ' +' + fmt(sn.n) : ''), x + 10, y + 25, { size: 9.5, max: sw - 14 });
        g.ctx.globalAlpha = 1;
        if (i > 0) g.arrow(x, y + 18, x - gap + 1, y + 18, { color: C.faint, head: 4 });
        g.hit(x, y, sw, 36, { snap: sn.id });
      });
      if (!snaps.length) g.text('no commits yet', sx0, sy + 28, { size: 11, color: C.faint });
      // pointer from metadata to current snapshot
      const ci = snaps.findIndex(x => x.id === curId);
      if (ci >= 0) { const tx = sx0 + ci * (sw + gap) + sw / 2; g.ctx.beginPath(); g.ctx.moveTo(186, cy + 16); g.ctx.bezierCurveTo(186, sy + 70, tx, sy + 70, tx, sy + 48); g.ctx.strokeStyle = C.accent; g.ctx.globalAlpha = 0.5 + s.flash * 0.5; g.ctx.lineWidth = 1.3; g.ctx.setLineDash([3, 3]); g.ctx.stroke(); g.ctx.setLineDash([]); g.ctx.globalAlpha = 1; }
      // manifest list → manifests → files for the selected snapshot
      const sn = r.snapshots.find(x => x.id === sel);
      const rowsAt = rowsAtSnapshot(r, sel), frac = r.total ? clamp(rowsAt / r.total, 0.02, 1) : 1;
      const files = Math.max(sn ? 1 : 0, Math.round(r.files * frac)), dels = sel === curId ? r.deleteFiles : Math.round(r.deleteFiles * frac);
      const mlY = Math.max(100, H * 0.3), mfY = mlY + Math.max(48, H * 0.13), fY = mfY + 46;
      g.box(W / 2 - 92, mlY, 184, 28, { fill: C['panel-2'] });
      g.text(`manifest list · snap-s${sel}.avro`, W / 2, mlY + 14, { size: 10.5, color: C.fg, align: 'center', max: 176 });
      
      const nm = clamp(Math.ceil((files + dels) / 8), 1, 4);
      const mw = Math.min(150, (W - 40) / nm - 12);
      const per = Math.ceil(files / nm), perD = Math.ceil(dels / nm);
      for (let i = 0; i < nm; i++) {
        const x = 20 + i * ((W - 40) / nm) + ((W - 40) / nm - mw) / 2;
        g.curve(W / 2, mlY + 28, x + mw / 2, mfY, { color: C['line-2'] });
        g.box(x, mfY, mw, 26, { fill: C.panel });
        const fi = Math.max(0, Math.min(per, files - i * per)), di = Math.max(0, Math.min(perD, dels - i * perD));
        g.text(`manifest ${i + 1} · ${fi + di} files`, x + mw / 2, mfY + 13, { size: 10, color: C.muted, align: 'center', max: mw - 8 });
        // files as small squares, data green, delete files red
        const cols = Math.max(4, Math.floor(mw / 17)), sz = 13;
        const showF = Math.min(fi, cols * 6), showD = Math.min(di, cols * 2);
        for (let k = 0; k < showF + showD; k++) {
          const cx = x + (k % cols) * 17, cy2 = fY + Math.floor(k / cols) * 17;
          if (cy2 > H - 34) break;
          const isDel = k >= showF;
          const fresh = sel === curId && s.flash > 0.4 && i === nm - 1 && k >= showF - 2 && !isDel;
          g.box(cx, cy2, sz, sz, { fill: isDel ? C.del : C.ins, stroke: false, r: 2, alpha: fresh ? 1 : 0.75 });
        }
      }
      // files written but not committed yet: invisible to readers
      const T = +n.cfg.commit; const into = r.nextAt != null ? clamp(1 - (r.nextAt - S.t) / T, 0, 1) : 0;
      const pend = n.rt.qN > 0 || n.rt.inRate > 1 ? Math.ceil(into * 4) : 0;
      if (pend && sel === curId) {
        const px = W - 150, py = H - 26;
        g.text('written, not committed:', px - 6, py + 5, { size: 10, color: C.faint, align: 'right' });
        for (let k = 0; k < pend; k++) g.box(px + k * 14, py, 10, 10, { stroke: C.ins, dash: [2, 2], r: 2 });
        g.text(`commit in ${fmtS(r.nextAt - S.t)}`, px + 62, py + 5, { size: 10, color: C.muted });
      }
      // incoming writes
      const k = every(s, 'w', Math.min(10, Math.sqrt(r.inRate) * 0.4), dt);
      for (let q = 0; q < k; q++) spawn(s, { x0: W - 30, y0: H - 4, x1: W - 150 + Math.random() * 50, y1: H - 22, col: C.ins, r: 2.2, dur: 0.6 });
      flyers(g, s, dt);
    },
    info() {
      return `<p>An Iceberg table is files plus a tree of metadata. The <b>catalog</b> stores one pointer: the current <code>metadata.json</code>. That file lists every <b>snapshot</b>. A snapshot points to a <b>manifest list</b>, which points to <b>manifests</b>, which list the data files with min/max stats per column.</p>
        <p>A writer first writes data files (dashed: invisible to readers), then new metadata, then swaps the catalog pointer in one atomic step. That swap is the commit.</p>
        <p>Old snapshots are kept until you expire them, which is what makes <b>time travel</b> and rollback possible. Click a snapshot above.</p>`;
    },
    live(n, s) {
      const r = n.rt, sel = s.sel && r.snapshots.find(x => x.id === s.sel) ? s.sel : r.snapId;
      const sn = r.snapshots.find(x => x.id === sel);
      const at = rowsAtSnapshot(r, sel);
      const tbl = 'lake.' + n.name.toLowerCase().replace(/^iceberg:?\s*/, '').replace(/\W+/g, '_');
      return `<div class="metrics">${m('Data files', r.files)}${m('Delete files', r.deleteFiles)}${m('Read speed', Math.round(PL.icebergRead(n) * 100) + '%')}</div>
        ${sn ? `<div class="sec-title" style="margin-top:12px">${sel === r.snapId ? 'Current snapshot' : 'Time travel'}</div>
        <pre class="sql">SELECT count(*) FROM ${esc(tbl)}
  FOR VERSION AS OF ${R.bigId(sel)};
<span class="cm">-- ${fmt(at)} rows${sel !== r.snapId ? `, ${fmt(r.total - at)} fewer than now` : ''}</span>

SELECT * FROM ${esc(tbl)}
  FOR TIMESTAMP AS OF '${R.iso(R.wall(sn.t)).replace('T', ' ').replace('Z', '')}';</pre>
        ${sel !== r.snapId ? '<button class="btn" data-act="cur">Back to current</button>' : ''}` : '<div class="note">Waiting for the first commit.</div>'}
        <div class="note" style="margin-top:8px">${n.cfg.mode === 'mor' ? 'Merge-on-read: updates add red delete files; readers merge them until compaction rewrites the data.' : 'Copy-on-write: updates rewrite whole data files, so there are no delete files but writes cost more.'} ${n.cfg.compaction ? 'Compaction runs every 20 s and commits a <i>replace</i> snapshot.' : 'Compaction is off, so small files and delete files pile up.'}</div>`;
    },
    act(n, s, a) { if (a.snap) s.sel = a.snap === n.rt.snapId ? null : a.snap; if (a === 'cur') s.sel = null; }
  };
  function rowsAtSnapshot(r, id) { let t = r.total; for (const sn of r.snapshots) { if (sn.id <= id) break; t -= sn.n; } return Math.max(0, t); }

  /* ---------------- Spark ---------------- */
  V.spark = {
    sub: 'Driver, stages, tasks, shuffle and executors',
    draw(g, n, s, dt) {
      const r = n.rt, W = g.w, H = g.h, agg = n.cfg.job === 'agg';
      const k = PL.upstream(n).find(u => u.type === 'kafka');
      const cores = n.cfg.executors * n.cfg.cores;
      const inParts = k ? +k.cfg.partitions : Math.min(8, cores);
      const t0 = Math.min(inParts, 16), t1 = agg ? Math.min(8, cores) : 0;
      const ph = runPhase(n);
      if (ph.busy && s.runKey !== r.runNo) { s.runKey = r.runNo; s.hist.push({ t: S.t, dur: r.lastDur, over: r.overrun }); if (s.hist.length > 20) s.hist.shift(); s.jit = Array.from({ length: 32 }, () => 0.75 + Math.random() * 0.5); }
      const jit = s.jit || Array(32).fill(1);
      // driver
      g.box(10, 14, 110, 50, { fill: C['panel-2'] });
      g.text('driver', 65, 28, { size: 11, color: C.fg, weight: 600, align: 'center' });
      g.text(n.cfg.mode === 'stream' ? `micro-batch #${r.runNo}` : `run #${r.runNo}`, 65, 44, { size: 10, align: 'center' });
      g.text(ph.busy ? 'running' : ph.next != null ? 'next in ' + fmtS(ph.next) : 'waits for trigger', 65, 56, { size: 9.5, color: ph.busy ? C.accent : C.faint, align: 'center' });
      // stages
      const stageTop = 14, stageH = H - 120;
      const cols = agg ? 2 : 1, colW = Math.min(agg ? 300 : 460, (W - 170 - (agg ? 70 : 0)) / cols);
      const sx = [150, 150 + colW + 70];
      const p = ph.busy ? ph.p : (s.hist.length ? 1 : 0);
      const split = agg ? 0.55 : 1;
      const stages = [{ name: agg ? 'Stage 1 · scan + partial agg' : 'Stage 1 · scan → parse → enrich → write', tasks: t0, p0: 0, p1: split }];
      if (agg) stages.push({ name: 'Stage 2 · final agg → write', tasks: t1, p0: split + 0.05, p1: 1 });
      const skew = k && k.cfg.skew;
      stages.forEach((sg, si) => {
        const x = sx[si], bh = Math.min(24, (stageH - 30) / sg.tasks - 3);
        g.text(sg.name, x, stageTop + 4, { size: 10.5, color: C.fg, weight: 600, max: colW });
        g.text(`${sg.tasks} tasks${si === 0 && k ? ' · one per Kafka partition' : si === 1 ? ' · spark.sql.shuffle.partitions (AQE-coalesced)' : ''}`, x, stageTop + 18, { size: 9.5, max: colW });
        for (let i = 0; i < sg.tasks; i++) {
          const y = stageTop + 30 + i * (bh + 3);
          const work = (skew && si === 0 && i === 0 ? 2.6 : 1) * jit[i % jit.length];
          const local = clamp((p - sg.p0) / ((sg.p1 - sg.p0) * work / (skew && si === 0 ? 2.6 : 1.25)), 0, 1);
          g.box(x, y, colW, bh, { fill: C['panel-2'], stroke: false, r: 3 });
          if (local > 0) g.box(x, y, colW * local, bh, { fill: local >= 1 ? C['c-process'] : C['c-process'], stroke: false, r: 3, alpha: local >= 1 ? 0.45 : 0.9 });
          if (bh > 9) g.text(`task ${i}${skew && si === 0 && i === 0 ? ' · hot partition' : ''}`, x + 5, y + bh / 2, { size: 9, color: local > 0.3 ? '#fff' : C.faint, max: colW - 8 });
        }
      });
      // shuffle between stages: every map task sends to every reduce task
      if (agg) {
        const x0 = sx[0] + colW + 4, x1 = sx[1] - 4, bh0 = Math.min(24, (stageH - 30) / t0 - 3), bh1 = Math.min(24, (stageH - 30) / t1 - 3);
        const shuffling = ph.busy && p > split - 0.15 && p < split + 0.15;
        for (let i = 0; i < Math.min(t0, 8); i++) for (let j = 0; j < t1; j++)
          g.line(x0, stageTop + 30 + i * (bh0 + 3) + bh0 / 2, x1, stageTop + 30 + j * (bh1 + 3) + bh1 / 2, { color: shuffling ? C['c-process'] : C.line, alpha: shuffling ? 0.35 : 0.5 });
        g.text('shuffle', (x0 + x1) / 2, stageTop + 18, { size: 9.5, color: shuffling ? C['c-process'] : C.faint, align: 'center', weight: 600 });
        if (shuffling) { const q = every(s, 'sh', 40, dt); for (let z = 0; z < q; z++) { const i = Math.floor(Math.random() * Math.min(t0, 8)), j = Math.floor(Math.random() * t1); spawn(s, { x0, y0: stageTop + 30 + i * (bh0 + 3) + bh0 / 2, x1, y1: stageTop + 30 + j * (bh1 + 3) + bh1 / 2, col: C['c-process'], r: 2.2, dur: 0.5, ease: false }); } }
      }
      // executors and cores
      const ey = H - 92;
      g.text('EXECUTORS · cores', 10, ey - 8, { size: 9.5, weight: 600, color: C.faint });
      const used = Math.min(cores, k ? +k.cfg.partitions : cores);
      const ex = +n.cfg.executors, per = +n.cfg.cores, ew = Math.min(110, (W - 20) / ex - 6), cs = clamp((ew - 10) / per - 3, 6, 14);
      let ci = 0;
      for (let e = 0; e < ex; e++) {
        const x = 10 + e * (ew + 6); g.box(x, ey, ew, cs + 12, { fill: C.panel });
        for (let c = 0; c < per; c++, ci++) {
          const cx = x + 5 + c * (cs + 3), busy = ph.busy && ci < used && p < (ci === 0 && skew ? 1 : split + (agg ? 0.45 : 0)) + 0.001;
          if (ci >= used) g.hatch(cx, ey + 6, cs, cs, C['line-2']);
          else g.box(cx, ey + 6, cs, cs, { fill: busy ? C['c-process'] : C['panel-2'], stroke: busy ? false : C.line, r: 2 });
        }
      }
      // timeline of batches
      const ty = H - 40, tw = W - 20, span = 60;
      g.text('LAST 60 s', 10, ty - 8, { size: 9.5, weight: 600, color: C.faint });
      g.line(10, ty + 10, 10 + tw, ty + 10, { color: C.line });
      if (n.cfg.mode === 'stream') { const T = +n.cfg.trigger; for (let tt = Math.ceil((S.t - span) / T) * T; tt <= S.t; tt += T) { const x = 10 + tw - (S.t - tt) / span * tw; g.line(x, ty + 2, x, ty + 18, { color: C.faint, alpha: 0.6 }); } }
      s.hist.forEach(h => { const x = 10 + tw - (S.t - h.t) / span * tw, w = Math.max(2, Math.min(S.t - h.t, h.dur) / span * tw); if (x + w > 10) g.box(Math.max(10, x), ty + 4, w, 12, { fill: h.over ? C.warn : C['c-process'], stroke: false, r: 2, alpha: 0.8 }); });
      flyers(g, s, dt);
    },
    info(n) {
      return `<p>The <b>driver</b> plans the job and hands out <b>tasks</b>. A task is one slice of the data on one core. ${n.cfg.mode === 'stream' ? `In Structured Streaming the driver wakes every ${n.cfg.trigger} s, asks Kafka for the latest offsets and runs one small batch over the new range.` : 'A batch job reads everything that arrived since the last run.'}</p>
        <p>Work that needs records with the same key together (group by, join) forces a <b>shuffle</b>: map tasks write their output by key hash, and the next <b>stage</b> pulls it over the network. Without that, transformations are narrow and run as one stage.</p>
        <p>Reading Kafka uses one task per partition, so cores beyond the partition count sit idle (striped).</p>`;
    },
    live(n) {
      const r = n.rt, ph = runPhase(n);
      return `<div class="metrics">${m('Cores', `${Math.min(r.totalCores || 0, r.used || 0)}/${r.totalCores || 0} usable`)}${m('Last batch', fmtS(r.lastDur))}${m('Backlog', fmt(r.qN))}</div>
        ${r.overrun ? `<div class="note warn">Batches take longer than the ${n.cfg.trigger} s trigger, so they run back to back and lag grows. Add cores or partitions.</div>` : ''}
        ${r.limitedBy ? `<div class="note warn">Only ${r.limitedBy} cores can work: the topic has ${r.limitedBy} partitions.</div>` : ''}
        ${ph.busy ? `<div class="note">Batch ${Math.round(ph.p * 100)}% done.</div>` : ''}`;
    }
  };

  /* ---------------- Debezium CDC ---------------- */
  V.cdc = {
    sub: n => n.cfg.mode === 'log' ? 'WAL, replication slot and change events' : 'Polling query against the table',
    draw(g, n, s, dt) {
      const r = n.rt, W = g.w, H = g.h;
      const pg = PL.upstream(n).find(u => u.type === 'postgres');
      const rate = pg ? pg.rt.outRate : r.inRate, mix = pg ? pg.cfg.mix : 'mix';
      if (n.cfg.mode === 'poll') return drawPoll(g, n, s, dt, rate, mix);
      // WAL tape
      const tx0 = 14, tx1 = W - 14, ty = 34, th = 34, cw = 78;
      g.text('POSTGRES WAL · newest on the right', tx0, 14, { size: 9.5, weight: 600, color: C.faint });
      s.lsnPos = (s.lsnPos || 5e5) + rate * dt;                       // records written
      const per = Math.max(1, niceUnit(rate / 2.2));                   // records per WAL cell
      if (!s.per || Math.abs(Math.log(per / s.per)) > 0.9) s.per = per;
      const e = s.lsnPos / s.per;
      const lagRec = r.qN + (n.cfg.down ? 0 : 0); const lagCells = lagRec / s.per;
      g.ctx.save(); g.rr(tx0, ty, tx1 - tx0, th, 4); g.ctx.clip();
      g.box(tx0, ty, tx1 - tx0, th, { fill: C['panel-2'], stroke: false, r: 4 });
      const cells = Math.ceil((tx1 - tx0) / cw) + 1, jEnd = Math.floor(e);
      for (let j = jEnd; j > jEnd - cells && j >= 0; j--) {
        const x = tx1 - (e - j) * cw, confirmed = (e - j) > lagCells + 1;
        g.ctx.globalAlpha = confirmed ? 0.35 : 0.95;
        g.ctx.fillStyle = cellColor(j, mix); g.ctx.fillRect(x + 1, ty + 3, cw - 2, 8);
        g.ctx.globalAlpha = confirmed ? 0.4 : 1;
        g.text('0/' + (0x16B3A28 + j * 0x2A8).toString(16).toUpperCase(), x + cw / 2, ty + 23, { size: 9, color: C.muted, align: 'center' });
      }
      g.ctx.globalAlpha = 1; g.ctx.restore();
      // positions
      const headX = tx1 - 2, slotX = Math.max(tx0 + 2, tx1 - lagCells * cw);
      g.line(headX, ty - 6, headX, ty + th + 6, { color: C.fg, w: 2 });
      g.text('pg_current_wal_lsn', headX, ty + th + 14, { size: 9, color: C.fg, align: 'right' });
      g.line(slotX, ty - 6, slotX, ty + th + 6, { color: C['c-ingest'], w: 2 });
      g.text('slot: confirmed_flush_lsn', clamp(slotX, tx0 + 120, tx1), ty - 10, { size: 9, color: C['c-ingest'], align: 'right' });
      if (headX - slotX > 6) { g.hatch(slotX, ty + th - 6, headX - slotX, 5, C.bad); }
      // Debezium box, Kafka box
      const dy = ty + th + 40, dbx = W * 0.32, dbw = Math.min(200, W * 0.3);
      g.box(dbx, dy, dbw, 56, { fill: C['panel-2'], stroke: n.cfg.down ? C.bad : C['c-ingest'], lw: 1.5 });
      g.text('Debezium connector', dbx + dbw / 2, dy + 14, { size: 11, color: C.fg, weight: 600, align: 'center' });
      g.text('pgoutput → envelope', dbx + dbw / 2, dy + 30, { size: 9.5, align: 'center' });
      g.text(n.cfg.down ? 'stopped' : `${fmt(r.outRate)}/s`, dbx + dbw / 2, dy + 44, { size: 9.5, color: n.cfg.down ? C.bad : C.muted, align: 'center' });
      const kx = W - Math.min(190, W * 0.3) - 14, kw = Math.min(190, W * 0.3);
      const outs = PL.outLinks(n).map(l => PL.byId(l.to)).filter(Boolean);
      g.box(kx, dy, kw, 56, { fill: C.panel });
      g.text(outs[0] ? outs[0].name : 'Kafka', kx + kw / 2, dy + 14, { size: 11, color: C.fg, weight: 600, align: 'center', max: kw - 10 });
      g.text('topic shop.public.orders', kx + kw / 2, dy + 30, { size: 9.5, align: 'center', max: kw - 10 });
      g.text('key = primary key', kx + kw / 2, dy + 44, { size: 9.5, color: C.faint, align: 'center' });
      g.arrow(dbx + dbw, dy + 28, kx - 2, dy + 28, { color: C['line-2'] });
      // offsets topic
      const oy = dy + 76;
      g.box(dbx, oy, dbw, 24, { fill: C.panel, dash: [3, 3] });
      s.oc = (s.oc || 0) + dt; const ocFlash = s.oc % 5 < 0.6 && !n.cfg.down;
      g.text('connect-offsets: last LSN sent', dbx + dbw / 2, oy + 12, { size: 9.5, color: ocFlash ? C['c-ingest'] : C.faint, align: 'center' });
      g.line(dbx + dbw / 2, dy + 56, dbx + dbw / 2, oy, { color: ocFlash ? C['c-ingest'] : C.line, dash: [2, 2] });
      if (!n.cfg.down && every(s, 'ev', 2.2, dt)) { const c = R.sourceChange(pg || n, pickOp(mix), Math.random() < 0.3 ? 'customers' : 'orders'); const L = '0/' + (0x16B3A28 + Math.floor(e) * 0x2A8).toString(16).toUpperCase(); addTick(s, `op=${c.op}  key=${c.key}  lsn=${L}  ${c.op === 'u' ? 'before.' + changed(c) + ' → after.' + changed(c, 1) : c.op === 'd' ? 'after=null (+ tombstone)' : 'before=null'}  → shop.public.${c.table}`, { c: C.ins, u: C.upd, d: C.del }[c.op]); }
      if (H - oy - 40 > 40) ticker(g, s, 14, oy + 40, W - 28, H - oy - 44, 'CHANGE EVENTS SENT');
      if (n.cfg.snapshot) { g.box(14, oy, dbx - 30, 24, { fill: C['accent-soft'], stroke: C.accent }); g.text(`initial snapshot · ${fmt(r.qN)} rows left`, 22, oy + 12, { size: 10, color: C.fg, max: dbx - 44 }); }
      // movement: entries drop from the slot position into Debezium, events fly to Kafka
      if (!n.cfg.down) {
        const q = every(s, 'rd', Math.min(12, Math.sqrt(r.outRate) * 0.6), dt);
        for (let z = 0; z < q; z++) {
          const col = opColor(mix);
          spawn(s, { x0: slotX, y0: ty + th, x1: dbx + dbw / 2, y1: dy, col, r: 3, dur: 0.5, curve: false, sq: true,
            done: () => spawn(s, { x0: dbx + dbw, y0: dy + 28, x1: kx, y1: dy + 28, col, r: 3.2, dur: 0.5 }) });
        }
      }
      flyers(g, s, dt);
    },
    info(n) {
      return n.cfg.mode === 'log' ? `<p>Every committed change is first written to the Postgres <b>write-ahead log</b>. Debezium opens a <b>replication slot</b>: a bookmark that tells Postgres “keep WAL from here, I haven’t read it yet”.</p>
        <p>The connector decodes each entry with the <code>pgoutput</code> plugin into a change event with <code>before</code>, <code>after</code> and <code>op</code>, sends it to Kafka keyed by the primary key, and records the LSN it reached in the <code>connect-offsets</code> topic. Then it confirms that LSN to the slot, and Postgres can recycle the older WAL (faded).</p>
        <p>If the connector stops, the slot holds WAL forever and the database disk fills up. Monitor slot lag.</p>`
        : `<p>Query-based capture runs a <code>SELECT … WHERE updated_at &gt; :last</code> every ${n.cfg.poll} s. It needs no database privileges beyond reading, but it misses deletes, merges several updates of one row into one, and adds load and latency.</p>`;
    },
    live(n) {
      const r = n.rt;
      return `<div class="metrics">${m('Events out', fmt(r.outRate) + '/s')}${m('Slot lag', fmt(r.qN) + ' changes')}${m('Mode', n.cfg.mode === 'log' ? 'WAL' : 'polling')}</div>
        ${n.cfg.down ? '<div class="note bad">Connector stopped. The replication slot keeps every WAL segment since its last confirmed LSN, so retained WAL grows (red hatching).</div>' : ''}`;
    }
  };
  function drawPoll(g, n, s, dt, rate, mix) {
    const W = g.w, H = g.h, poll = +n.cfg.poll;
    if (!s.rows) s.rows = Array.from({ length: 8 }, (_, i) => ({ id: 48120 - i * 7, st: 0, ch: 0, del: false, upd: 0 }));
    g.text('TABLE orders (sample rows)', 14, 14, { size: 9.5, weight: 600, color: C.faint });
    const rx = 14, rw = Math.min(330, W * 0.5), rh = 20, ry = 26;
    // random changes on rows, scaled by write rate
    const q = every(s, 'chg', Math.min(3, rate / 300), dt);
    for (let z = 0; z < q; z++) {
      const row = s.rows[Math.floor(Math.random() * s.rows.length)]; if (row.del) continue;
      const c = opColor(mix);
      if (c === C.del) { row.del = true; row.delAt = S.t; } else { row.upd++; row.ch = S.t; }
    }
    const phase = (S.t % poll) / poll;
    if (s.lastPoll == null) s.lastPoll = Math.floor(S.t / poll);
    const pollNo = Math.floor(S.t / poll);
    if (pollNo !== s.lastPoll) {
      s.lastPoll = pollNo;
      s.rows.forEach((row, i) => {
        if (row.del) { row.gone = true; row.missed = S.t; }
        else if (row.upd) { spawn(s, { x0: rx + rw, y0: ry + i * rh + rh / 2, x1: W - 60, y1: H / 2, col: C.upd, r: 3.2, dur: 0.8, curve: true }); s.lostUpd = (s.lostUpd || 0) + row.upd - 1; row.upd = 0; }
      });
    }
    s.rows.forEach((row, i) => {
      const y = ry + i * rh;
      const recent = S.t - row.ch < 1.2;
      g.box(rx, y, rw, rh - 3, { fill: row.del ? 'transparent' : recent ? C['accent-soft'] : C['panel-2'], stroke: row.del ? C.del : false, dash: row.del ? [3, 3] : [], r: 3 });
      g.text(`id=${row.id}  status=${['created', 'paid', 'shipped'][row.upd % 3]}  upd×${row.upd}`, rx + 8, y + rh / 2 - 1, { size: 10, color: row.del ? C.del : C.muted, max: rw - 12 });
      if (row.gone && S.t - row.missed < 3) g.text('deleted · never captured', rx + rw + 8, y + rh / 2 - 1, { size: 10, color: C.del, weight: 600 });
      if (row.gone && S.t - row.missed > 3) { s.rows[i] = { id: row.id + 300, st: 0, ch: S.t, del: false, upd: 0 }; }
    });
    // the polling sweep
    const sy = ry + phase * (s.rows.length * rh);
    g.line(rx - 4, sy, rx + rw + 4, sy, { color: C['c-ingest'], w: 2, alpha: 0.7 });
    g.text(`SELECT * FROM orders WHERE updated_at > :last   ·   next poll in ${fmtS(poll - (S.t % poll))}`, 14, H - 14, { size: 10, color: C['c-ingest'], max: W - 28 });
    g.box(W - 140, H / 2 - 22, 120, 44, { fill: C.panel }); g.text('Kafka', W - 80, H / 2, { size: 11, color: C.fg, align: 'center', weight: 600 });
    if (s.lostUpd) g.text(`${s.lostUpd} intermediate updates merged away`, W - 14, 14, { size: 10, color: C.warn, align: 'right' });
    flyers(g, s, dt);
  }

  /* ---------------- Postgres ---------------- */
  V.postgres = {
    sub: 'Commit path: WAL first, table pages later',
    draw(g, n, s, dt) {
      const r = n.rt, W = g.w, H = g.h, mix = n.cfg.mix;
      const cx = 14, bw = Math.min(150, W * 0.22);
      const cols = [cx, cx + (W - 28 - bw) / 3, cx + 2 * (W - 28 - bw) / 3, W - 14 - bw];
      const y = 40, bh = 50;
      const boxes = [['app sessions', `${fmt(n.cfg.rate)} tx/s`], ['WAL buffers', 'in memory'], ['WAL segments', 'pg_wal/ · fsync on commit'], ['table & index pages', 'written at checkpoint']];
      boxes.forEach((b, i) => { g.box(cols[i], y, bw, bh, { fill: i === 2 ? C['accent-soft'] : C['panel-2'], stroke: i === 2 ? C.accent : C.line }); g.text(b[0], cols[i] + bw / 2, y + 18, { size: 11, color: C.fg, weight: 600, align: 'center', max: bw - 8 }); g.text(b[1], cols[i] + bw / 2, y + 34, { size: 9.5, align: 'center', max: bw - 8 }); if (i) g.arrow(cols[i - 1] + bw, y + bh / 2, cols[i] - 2, y + bh / 2); });
      if (n.cfg.down) { g.text('database offline', W / 2, y + bh + 30, { size: 12, color: C.bad, align: 'center', weight: 600 }); return; }
      const q = every(s, 'tx', Math.min(14, Math.sqrt(n.cfg.rate) * 0.6), dt);
      for (let z = 0; z < q; z++) { const col = opColor(mix); spawn(s, { x0: cols[0] + bw, y0: y + bh / 2, x1: cols[1], y1: y + bh / 2, col, r: 3, dur: 0.3, ease: false, done: () => spawn(s, { x0: cols[1] + bw, y0: y + bh / 2, x1: cols[2], y1: y + bh / 2, col, r: 3, dur: 0.3, ease: false }) }); }
      // checkpoint every 10 s moves dirty pages to the table files
      s.ck = (s.ck || 0) + dt;
      if (s.ck > 10) { s.ck = 0; for (let z = 0; z < 10; z++) spawn(s, { x0: cols[2] + bw, y0: y + bh / 2, x1: cols[3] + 10 + Math.random() * (bw - 20), y1: y + bh / 2 + (Math.random() - 0.5) * 20, col: C.faint, r: 2.5, dur: 0.6 + Math.random() * 0.3, sq: true }); }
      g.text(`checkpoint in ${fmtS(10 - s.ck)}`, cols[3] + bw / 2, y + bh + 14, { size: 9.5, color: C.faint, align: 'center' });
      // segments, with the slot holding old ones
      const segY = y + bh + 40;
      g.text('WAL SEGMENTS (16 MB each)', 14, segY - 8, { size: 9.5, weight: 600, color: C.faint });
      const cdc = PL.outLinks(n).map(l => PL.byId(l.to)).find(x => x && x.type === 'cdc');
      s.seg = (s.seg || 30) + n.cfg.rate * dt / 2000;
      const held = cdc ? Math.min(14, 1 + Math.floor(cdc.rt.qN / 2000)) : 2;
      const segN = Math.floor(s.seg);
      for (let i = 0; i < 14; i++) {
        const x = 14 + i * ((W - 28) / 14), sw = (W - 28) / 14 - 4, idx = segN - 13 + i;
        if (idx < 0) continue;
        const keep = 13 - i < held;
        g.box(x, segY, sw, 22, { fill: keep ? C['accent-soft'] : C['panel-2'], stroke: keep ? C.accent : C.line, alpha: keep ? 1 : 0.6 });
        g.text((0x16 + idx).toString(16).toUpperCase().padStart(4, '0'), x + sw / 2, segY + 11, { size: 9, color: keep ? C.fg : C.faint, align: 'center' });
      }
      if (cdc) g.text(`kept for ${cdc.name}’s replication slot: ${held} segment${held > 1 ? 's' : ''}`, W - 14, segY + 36, { size: 10, color: C.accent, align: 'right' });
      if (every(s, 'sql', 2.2, dt)) { const c = R.sourceChange(n, pickOp(mix), Math.random() < 0.3 ? 'customers' : 'orders'); addTick(s, sqlOf(c), { c: C.ins, u: C.upd, d: C.del }[c.op]); }
      if (H - segY - 60 > 40) ticker(g, s, 14, segY + 52, W - 28, H - segY - 60, 'COMMITTED STATEMENTS');
      flyers(g, s, dt);
    },
    info() { return `<p>A commit is durable as soon as its WAL record is flushed to disk. The table and index pages change in memory and reach disk later, at a <b>checkpoint</b>. After a crash, Postgres replays the WAL from the last checkpoint.</p><p>Because every change is in the WAL in order, the same log feeds replicas and CDC. A <b>replication slot</b> stops Postgres from recycling segments its reader hasn’t confirmed (highlighted).</p>`; },
    live(n) { return `<div class="metrics">${m('Writes', fmt(n.cfg.rate) + '/s')}${m('Workload', n.cfg.mix)}${m('wal_level', 'logical')}</div>`; }
  };

  /* ---------------- App events ---------------- */
  V.app = {
    sub: 'Producer client: batching and partitioning',
    draw(g, n, s, dt) {
      const W = g.w, H = g.h, r = n.rt;
      const k = PL.outLinks(n).map(l => PL.byId(l.to)).find(x => x && x.type === 'kafka');
      const P = k ? Math.min(6, +k.cfg.partitions) : 4;
      g.text('APP → PRODUCER CLIENT → BROKERS', 14, 14, { size: 9.5, weight: 600, color: C.faint });
      g.box(14, 40, 90, H - 70, { fill: C['panel-2'] }); g.text('app', 59, 54, { size: 11, color: C.fg, weight: 600, align: 'center' }); g.text(`${fmt(r.outRate)}/s`, 59, 70, { size: 10, align: 'center' });
      const bx = 150, bw = Math.min(260, W - 330), rowH = (H - 70) / P;
      s.b = s.b || Array(P).fill(0); s.lg = s.lg || Array(P).fill(0);
      if (s.b.length !== P) { s.b = Array(P).fill(0); s.lg = Array(P).fill(0); }
      g.text('accumulator: one batch per partition', bx, 30, { size: 9.5, color: C.faint });
      const linger = 1.2;
      for (let i = 0; i < P; i++) {
        const y = 40 + i * rowH, h = rowH - 8;
        const add = every(s, 'e' + i, Math.min(30, r.outRate / P / 4), dt);
        for (let z = 0; z < add; z++) spawn(s, { x0: 104, y0: 40 + (H - 70) / 2, x1: bx + 6 + s.b[i] * 9, y1: y + h / 2, col: C['c-source'], r: 2.4, dur: 0.4, curve: true });
        s.b[i] += add; s.lg[i] += dt;
        const full = s.b[i] >= Math.floor((bw - 12) / 9);
        if ((full || s.lg[i] > linger) && s.b[i] > 0) { for (let z = 0; z < 3; z++) spawn(s, { x0: bx + bw, y0: y + h / 2, x1: W - 80, y1: y + h / 2, col: C['c-stream'], r: 3.5, dur: 0.45, sq: true }); s.b[i] = 0; s.lg[i] = 0; }
        g.box(bx, y, bw, h, { fill: C.panel });
        for (let z = 0; z < s.b[i]; z++) g.box(bx + 4 + z * 9, y + 4, 7, h - 8, { fill: C['c-source'], stroke: false, r: 2, alpha: 0.8 });
        g.text('P' + i, bx - 8, y + h / 2, { size: 10, align: 'right' });
      }
      g.box(W - 74, 40, 60, H - 70, { fill: C['panel-2'] }); g.text(k ? 'Kafka' : 'broker', W - 44, 54, { size: 11, color: C.fg, weight: 600, align: 'center' });
      g.text(`batch.size or linger.ms=${linger * 1000} → send`, bx, H - 12, { size: 10, color: C.muted });
      flyers(g, s, dt);
    },
    info() { return `<p>The app doesn’t send each event alone. The Kafka producer client hashes the key to a partition and appends the event to that partition’s <b>batch</b>. A batch is sent when it is full (<code>batch.size</code>) or after <code>linger.ms</code>, whichever comes first. Bigger batches mean better compression and throughput, at the cost of a few milliseconds of latency.</p>`; },
    live(n) { return `<div class="metrics">${m('Events', fmt(n.rt.outRate) + '/s')}${m('Peaks', n.cfg.burst ? 'on' : 'off')}${m('Key', 'user_id')}</div>`; }
  };

  /* ---------------- Flink ---------------- */
  V.flink = {
    sub: 'Operators, keyBy, checkpoints and watermarks',
    draw(g, n, s, dt) {
      const r = n.rt, W = g.w, H = g.h, par = Math.min(+n.cfg.par, 8), ck = +n.cfg.ckpt, win = n.cfg.op === 'window', wm = +n.cfg.watermark;
      const k = PL.upstream(n).find(u => u.type === 'kafka');
      const srcPar = Math.min(k ? +k.cfg.partitions : par, 8);
      const ops = [{ name: 'Kafka source', p: srcPar }, { name: win ? 'window 10 s (keyed state)' : 'map', p: par }, { name: n.cfg.eos ? 'sink · 2-phase commit' : 'sink', p: par }];
      const top = 30, oh = win ? H - 150 : H - 80, colX = i => 20 + i * ((W - 40) / 3), ow = Math.min(170, (W - 40) / 3 - 40);
      const pos = (i, j) => [colX(i), top + 18 + j * ((oh - 18) / ops[i].p), ow, Math.min(26, (oh - 18) / ops[i].p - 5)];
      ops.forEach((o, i) => {
        g.text(o.name, colX(i), top + 4, { size: 10.5, weight: 600, color: C.fg, max: ow + 30 });
        for (let j = 0; j < o.p; j++) {
          const [x, y, w, h] = pos(i, j);
          g.box(x, y, w, h, { fill: C['panel-2'] });
          if (i === 1 && win) { const fill = (S.t % 10) / 10; g.box(x + 2, y + 2, (w - 4) * fill, h - 4, { fill: C['c-process'], stroke: false, r: 3, alpha: 0.35 }); }
          if (h > 12) g.text(`${i === 0 ? 'source' : i === 1 ? (win ? 'window' : 'map') : 'sink'} ${j}`, x + 6, y + h / 2, { size: 9.5, max: w - 8 });
        }
      });
      // links: forward between source→map is keyBy (all-to-all) when windowed
      for (let a = 0; a < ops[0].p; a++) for (let b = 0; b < ops[1].p; b++) { if (!win && a % ops[1].p !== b) continue; const p0 = pos(0, a), p1 = pos(1, b); g.line(p0[0] + p0[2], p0[1] + p0[3] / 2, p1[0], p1[1] + p1[3] / 2, { color: C.line, alpha: 0.7 }); }
      for (let b = 0; b < ops[1].p; b++) { const p1 = pos(1, b), p2 = pos(2, b); g.line(p1[0] + p1[2], p1[1] + p1[3] / 2, p2[0], p2[1] + p2[3] / 2, { color: C.line }); }
      g.text(win ? 'keyBy: hash shuffle' : 'forward', (colX(0) + ow + colX(1)) / 2, top + 4, { size: 9.5, color: C.faint, align: 'center' });
      // records
      const q = every(s, 'r', Math.min(18, Math.sqrt(r.inRate) * 0.5), dt);
      for (let z = 0; z < q; z++) {
        const a = Math.floor(Math.random() * ops[0].p), b = win ? Math.floor(Math.random() * ops[1].p) : a % ops[1].p;
        const p0 = pos(0, a), p1 = pos(1, b);
        spawn(s, { x0: p0[0] + p0[2], y0: p0[1] + p0[3] / 2, x1: p1[0], y1: p1[1] + p1[3] / 2, col: C['c-stream'], r: 2.3, dur: 0.4, ease: false,
          done: win ? null : () => { const p2 = pos(2, b); spawn(s, { x0: p1[0] + p1[2], y0: p1[1] + p1[3] / 2, x1: p2[0], y1: p2[1] + p2[3] / 2, col: C['c-process'], r: 2.3, dur: 0.35, ease: false }); } });
      }
      // checkpoint barrier sweeps left to right at the start of each interval
      const cp = (S.t % ck), bx = 20 + Math.min(1, cp / 1.2) * (W - 40);
      if (cp < 1.2) { g.line(bx, top + 10, bx, top + oh, { color: C.ctrl, w: 2, dash: [4, 3] }); g.text(`barrier ${Math.floor(S.t / ck) + 1}`, bx + 4, top + oh + 8, { size: 9.5, color: C.ctrl }); }
      else if (cp < 2.4) g.text(`checkpoint ${Math.floor(S.t / ck) + 1} complete${n.cfg.eos ? ' · sink commits' : ''}`, W - 20, top + oh + 8, { size: 10, color: C.ctrl, align: 'right', weight: 600 });
      else g.text(`next checkpoint in ${fmtS(ck - cp)}`, W - 20, top + oh + 8, { size: 9.5, color: C.faint, align: 'right' });
      // event-time axis with windows and the watermark
      if (win) {
        const ay = H - 70, span = 40, ax0 = 20, ax1 = W - 20, X = t => ax1 - (S.t - t) / span * (ax1 - ax0);
        g.text('EVENT TIME · windows fire when the watermark passes their end', ax0, ay - 14, { size: 9.5, weight: 600, color: C.faint });
        for (let ws = Math.floor((S.t - span) / 10) * 10; ws <= S.t; ws += 10) {
          const x0 = Math.max(ax0, X(ws)), x1 = Math.min(ax1, X(ws + 10)); if (x1 <= x0) continue;
          const fired = S.t - wm >= ws + 10;
          g.box(x0 + 1, ay, x1 - x0 - 2, 22, { fill: fired ? C['c-process'] : C['panel-2'], alpha: fired ? 0.35 : 1, stroke: fired ? false : C.line, r: 3 });
          g.text(fired ? 'fired' : 'open', (x0 + x1) / 2, ay + 11, { size: 9.5, color: fired ? C.fg : C.muted, align: 'center' });
        }
        const wx = X(S.t - wm);
        g.line(wx, ay - 6, wx, ay + 30, { color: C.warn, w: 2 }); g.text(`watermark (now − ${wm} s)`, wx - 4, ay + 40, { size: 9.5, color: C.warn, align: 'right' });
        g.line(ax1, ay - 6, ax1, ay + 30, { color: C.fg, w: 1.5 }); g.text('now', ax1, ay + 40, { size: 9.5, color: C.fg, align: 'right' });
      }
      flyers(g, s, dt);
    },
    info(n) { return `<p>A Flink job is a graph of operators, each running as parallel <b>subtasks</b>. <code>keyBy</code> routes every record by key hash so all events of one key meet the same subtask and its <b>keyed state</b>.</p><p>Every ${n.cfg.ckpt} s the sources inject a <b>checkpoint barrier</b>. When it has passed every operator, their state is snapshotted to durable storage together with the Kafka offsets. ${n.cfg.eos ? 'Exactly-once sinks commit their open transaction only then.' : ''}</p>${n.cfg.op === 'window' ? `<p>The <b>watermark</b> says “no events older than this are expected”. It trails the newest event by ${n.cfg.watermark} s. A window fires once the watermark passes its end; later events count as late.</p>` : ''}`; },
    live(n) { const r = n.rt; return `<div class="metrics">${m('In', fmt(r.inRate) + '/s')}${m('Slots', `${Math.min(r.used || 0, r.totalCores || 0)}/${r.totalCores || 0}`)}${m('Checkpoint', n.cfg.ckpt + ' s')}</div>`; }
  };

  /* ---------------- Snowflake ---------------- */
  V.snowflake = {
    sub: 'Cloud services, virtual warehouse, storage',
    draw(g, n, s, dt) {
      const r = n.rt, W = g.w, H = g.h, size = n.cfg.size, nodes = { XS: 1, S: 2, M: 4, L: 8, XL: 16 }[size], state = PL.sfState(n);
      const run = state === 'running';
      // layers
      g.box(14, 12, W - 28, 34, { fill: C['panel-2'] }); g.text('CLOUD SERVICES · SQL parse, optimizer, metadata, result cache', 24, 29, { size: 10, color: C.muted, max: W - 50 });
      const wy = 62, wh = H - 180;
      const ww = Math.min(W * 0.55, 340);
      g.box(14, wy, ww, wh, { fill: run ? C['accent-soft'] : C.panel, stroke: run ? C['c-wh'] : C.line, lw: run ? 1.5 : 1 });
      g.text(`VIRTUAL WAREHOUSE · ${size} · ${nodes} node${nodes > 1 ? 's' : ''}`, 24, wy + 14, { size: 10, weight: 600, color: C.fg });
      g.text(state, 14 + ww - 10, wy + 14, { size: 10, color: run ? C['c-wh'] : state === 'down' ? C.bad : C.faint, align: 'right', weight: 600 });
      const cols = Math.ceil(Math.sqrt(nodes * 2)), cell = Math.min(36, (ww - 30) / cols - 6);
      for (let i = 0; i < nodes; i++) {
        const x = 24 + (i % cols) * (cell + 6), y = wy + 30 + Math.floor(i / cols) * (cell + 6);
        g.box(x, y, cell, cell, { fill: run ? C['c-wh'] : C['panel-2'], stroke: false, r: 5, alpha: run ? 0.5 + 0.4 * Math.abs(Math.sin(S.t * 2 + i)) : 1 });
      }
      // serverless ingest lane
      const lx = 14 + ww + 20, lw = W - lx - 14;
      const serverless = n.cfg.ingest !== 'copy' || PL.upstream(n).some(u => u.type === 'iceberg');
      g.box(lx, wy, lw, wh, { fill: C.panel, dash: serverless ? [] : [3, 3] });
      g.text(PL.upstream(n).some(u => u.type === 'iceberg') ? 'EXTERNAL ICEBERG TABLE' : n.cfg.ingest === 'stream' ? 'SNOWPIPE STREAMING' : n.cfg.ingest === 'pipe' ? 'SNOWPIPE (serverless)' : 'COPY uses the warehouse', lx + 10, wy + 14, { size: 10, weight: 600, color: serverless ? C.fg : C.faint, max: lw - 16 });
      // storage
      const sy = wy + wh + 16, sh = H - sy - 10;
      g.box(14, sy, W - 28, sh, { fill: C['panel-2'] });
      g.text('STORAGE · micro-partitions (50–500 MB compressed, columnar, immutable)', 24, sy + 12, { size: 10, color: C.muted, max: W - 50 });
      s.rows = (s.rows || 0) + r.inRate * dt; const mp = Math.min(400, 24 + Math.floor(s.rows / 3000));
      const mcols = Math.floor((W - 48) / 12);
      for (let i = 0; i < mp; i++) { const x = 24 + (i % mcols) * 12, y = sy + 24 + Math.floor(i / mcols) * 12; if (y > sy + sh - 12) break; g.box(x, y, 9, 9, { fill: C['c-wh'], stroke: false, r: 2, alpha: i > mp - 3 ? 1 : 0.55 }); }
      // movement
      const q = every(s, 'in', Math.min(10, Math.sqrt(r.inRate) * 0.4), dt);
      for (let z = 0; z < q; z++) {
        if (serverless) spawn(s, { x0: lx + lw / 2, y0: wy + 24, x1: lx + 20 + Math.random() * (lw - 40), y1: sy + 20, col: C['c-wh'], r: 2.5, dur: 0.8 });
        else if (run) spawn(s, { x0: 24 + Math.random() * (ww - 40), y0: wy + wh - 6, x1: 24 + Math.random() * (W - 60), y1: sy + 20, col: C['c-wh'], r: 2.5, dur: 0.6 });
      }
      if (run) { const qq = every(s, 'qq', 2, dt); for (let z = 0; z < qq; z++) spawn(s, { x0: W / 2, y0: 46, x1: 24 + Math.random() * (ww - 40), y1: wy + 30, col: C.ctrl, r: 2.5, dur: 0.5, sq: true }); }
      flyers(g, s, dt);
    },
    info(n) { return `<p>Snowflake separates three layers. <b>Storage</b> holds tables as immutable compressed columnar <b>micro-partitions</b> with min/max metadata, so queries prune what they don’t need. <b>Compute</b> is a virtual warehouse: a cluster you size (XS to XL doubles each step) that bills per second while running, with a 60 s minimum on resume. <b>Cloud services</b> parse, optimize and cache results.</p><p>${n.cfg.suspend ? 'Auto-suspend stops the warehouse when idle, which saves credits but adds a resume delay to the next query.' : 'Without auto-suspend the warehouse bills all the time.'}</p>`; },
    live(n) { const r = n.rt; return `<div class="metrics">${m('State', PL.sfState(n))}${m('Credits', r.credits.toFixed(3))}${m('Running', (S.t ? Math.round(r.activeT / S.t * 100) : 0) + '%')}</div>`; }
  };

  /* ---------------- dbt ---------------- */
  V.dbt = {
    sub: 'Model DAG, materializations and snapshots',
    draw(g, n, s, dt) {
      const W = g.w, H = g.h, ph = runPhase(n), scd = n.cfg.scd;
      const L = [
        [['source', 'raw.orders'], ['source', 'raw.customers']],
        [['stg', 'stg_orders'], ['stg', 'stg_customers']],
        scd !== 'none' ? [['snap', scd === 'scd2' ? 'snap_customers (SCD2)' : 'dim_customer (SCD1)']] : [['int', 'int_customers']],
        [['mart', 'fct_orders'], ['mart', 'dim_customer']],
        [['mart', 'mart_sales_by_city']]
      ];
      const cw = (W - 28) / L.length, bw = Math.min(150, cw - 22), bh = 34;
      const pos = {};
      L.forEach((lv, i) => lv.forEach((mm, j) => { const x = 14 + i * cw, y = 24 + (j + 0.5) * ((H - 80) / lv.length) - bh / 2; pos[mm[1]] = [x, y]; }));
      const edges = [['raw.orders', 'stg_orders'], ['raw.customers', 'stg_customers'], ['stg_customers', L[2][0][1]], ['stg_orders', 'fct_orders'], [L[2][0][1], 'dim_customer'], [L[2][0][1], 'fct_orders'], ['fct_orders', 'mart_sales_by_city'], ['dim_customer', 'mart_sales_by_city']];
      edges.forEach(([a, b]) => { const p = pos[a], q = pos[b]; if (p && q) g.curve(p[0] + bw, p[1] + bh / 2, q[0], q[1] + bh / 2, { color: C['line-2'] }); });
      const lvlDone = ph.busy ? ph.p * (L.length - 1) : -1;
      L.forEach((lv, i) => lv.forEach(([kind, name]) => {
        const [x, y] = pos[name];
        const active = ph.busy && i >= 1 && lvlDone >= i - 1 && lvlDone < i, done = ph.busy && lvlDone >= i;
        const col = kind === 'source' ? C['c-wh'] : kind === 'snap' ? C.upd : C['c-wh'];
        g.box(x, y, bw, bh, { fill: active ? C['accent-soft'] : C['panel-2'], stroke: active ? C.accent : done ? col : C.line, lw: active ? 2 : 1 });
        g.text(name, x + 8, y + 12, { size: 10.5, color: C.fg, weight: 600, max: bw - 14 });
        const mat = kind === 'source' ? 'source' : kind === 'stg' ? 'view' : kind === 'snap' ? 'snapshot' : kind === 'int' ? 'ephemeral' : n.cfg.mat;
        g.text(mat + (done && i ? ' ✓' : active ? ' running…' : ''), x + 8, y + 25, { size: 9.5, color: active ? C.accent : C.muted, max: bw - 14 });
        if (active && Math.random() < dt * 8) spawn(s, { x0: x - 20, y0: y + bh / 2, x1: x + 4, y1: y + bh / 2, col: C.accent, r: 2.5, dur: 0.3 });
      }));
      const more = Math.max(0, n.cfg.models - 8);
      g.text(`${n.cfg.models} models in this project${more ? `, ${more} not shown` : ''} · ${ph.busy ? `run ${Math.round(ph.p * 100)}%` : ph.next != null ? 'next run in ' + fmtS(ph.next) : 'waits for its trigger'}`, 14, H - 18, { size: 10, color: C.muted, max: W - 28 });
      flyers(g, s, dt);
    },
    info(n) { return `<p>dbt turns SELECT statements into tables and views in the warehouse. <code>ref()</code> between models builds this DAG, and <code>dbt build</code> runs it in dependency order: staging models clean the raw tables, then marts join them.</p><p><b>Materialization</b> decides what is stored: a view stores nothing, a table is rebuilt every run, an <b>incremental</b> model only processes new rows and MERGEs them. ${n.cfg.scd === 'scd2' ? '<b>Snapshots</b> implement SCD2: when a tracked column changes, the current row gets a <code>dbt_valid_to</code> and a new version is inserted.' : ''}</p>`; },
    live(n) {
      const r = n.rt; if (!r.scd) PL.scdApply(n, null);
      const rows = n.cfg.scd === 'scd2' ? r.scd : r.scd.filter(x => x.cur);
      return `${PL.ui.runsHTML ? PL.ui.runsHTML(r.runs) : ''}
        ${n.cfg.scd !== 'none' ? `<div class="sec-title" style="margin-top:10px">dim_customer</div><div style="overflow-x:auto"><table class="mini-table"><tr><th>id</th><th>city</th>${n.cfg.scd === 'scd2' ? '<th>valid_from</th><th>valid_to</th>' : '<th>updated</th>'}</tr>${rows.map(x => `<tr class="${x.cur ? (S.t - x.from < 2 || x.traced ? 'new' : '') : 'old'}"><td>${x.id}</td><td>${x.city}</td><td>T+${fmtS(x.from)}</td>${n.cfg.scd === 'scd2' ? `<td>${x.to == null ? '∞' : 'T+' + fmtS(x.to)}</td>` : ''}</tr>`).join('')}</table></div>` : ''}`;
    }
  };

  /* ---------------- Airflow ---------------- */
  V.airflow = {
    sub: 'Scheduler, DAG runs and task states',
    draw(g, n, s, dt) {
      const r = n.rt, W = g.w, H = g.h;
      const tasks = PL.outLinks(n).map(l => PL.byId(l.to)).filter(Boolean);
      const runs = r.runs.slice(0, 10).slice().reverse();
      g.text('GRID · one column per DAG run, one row per task', 14, 14, { size: 9.5, weight: 600, color: C.faint });
      const nameW = Math.min(150, W * 0.3), cell = Math.min(26, (W - nameW - 40) / 10 - 4), top = 34;
      tasks.forEach((t, i) => g.text(t.name, 14, top + i * (cell + 6) + cell / 2 + 16, { size: 10.5, color: C.fg, max: nameW - 10 }));
      runs.forEach((run, j) => {
        const x = 14 + nameW + j * (cell + 4);
        g.text('#' + run.no, x + cell / 2, top + 4, { size: 9, color: C.faint, align: 'center' });
        tasks.forEach((t, i) => {
          const y = top + 16 + i * (cell + 6), ts = run.tasks[t.id] || { state: 'none' };
          const running = ts.state === 'running' && S.t < ts.end;
          const col = ts.state === 'failed' ? C.bad : ts.state === 'skipped' ? C.warn : ts.state === 'queued' ? C.faint : running ? C.accent : ts.state === 'running' ? C.ok : C.line;
          g.box(x, y, cell, cell, { fill: col, stroke: false, r: 4, alpha: running ? 0.5 + 0.5 * Math.abs(Math.sin(S.t * 4)) : 0.85 });
        });
      });
      if (!tasks.length) g.text('No tasks: link Airflow to Spark, Snowflake, dbt or a script.', 14, 50, { size: 11, color: C.faint });
      // the DAG itself, in data-flow order, coloured by the latest run
      const dagY = top + 24 + tasks.length * (cell + 6) + 20, sy0 = H - 54;
      if (tasks.length && sy0 - dagY > 70) {
        g.text('DAG · tasks in dependency order, latest run', 14, dagY, { size: 9.5, weight: 600, color: C.faint });
        const ord = tasks.slice().sort((a, b) => a.x - b.x), last = r.runs[0];
        const bw = Math.min(170, (W - 28) / ord.length - 30), by = dagY + 18 + Math.min(40, (sy0 - dagY - 70) / 2);
        ord.forEach((t, i) => {
          const x = 14 + i * (bw + 30), ts = last && last.tasks[t.id] || { state: 'none' }, running = ts.state === 'running' && S.t < ts.end;
          const col = ts.state === 'failed' ? C.bad : ts.state === 'skipped' ? C.warn : running ? C.accent : ts.state === 'running' ? C.ok : C.line;
          g.box(x, by, bw, 40, { fill: C['panel-2'], stroke: col, lw: running ? 2 : 1.2 });
          g.text(t.name, x + 10, by + 14, { size: 10.5, color: C.fg, weight: 600, max: bw - 16 });
          g.text(running ? 'running ' + Math.round((1 - (ts.end - S.t) / ts.dur) * 100) + '%' : ts.state === 'running' ? 'success' : ts.state, x + 10, by + 28, { size: 9.5, color: col === C.line ? C.faint : col, max: bw - 16 });
          if (i) g.arrow(x - 28, by + 20, x - 3, by + 20, { color: C['line-2'] });
        });
      }
      // scheduler
      const sy = H - 54;
      g.box(14, sy, W - 28, 40, { fill: C['panel-2'] });
      const left = r.nextAt != null ? r.nextAt - S.t : null, iv = +n.cfg.interval;
      g.text(n.cfg.paused ? 'scheduler: DAG paused' : `scheduler: next run in ${fmtS(left)}`, 24, sy + 14, { size: 10.5, color: C.fg });
      g.box(24, sy + 24, W - 48, 6, { fill: C.panel, stroke: false, r: 3 });
      if (left != null && !n.cfg.paused) g.box(24, sy + 24, (W - 48) * clamp(1 - left / iv, 0, 1), 6, { fill: C.ctrl, stroke: false, r: 3 });
      ['success', C.ok, 'running', C.accent, 'failed', C.bad, 'skipped', C.warn].reduce((x, v, i, a) => { if (i % 2) return x; g.box(x, sy - 16, 9, 9, { fill: a[i + 1], stroke: false, r: 2 }); g.text(v, x + 13, sy - 11.5, { size: 9.5 }); return x + 70; }, W - 300);
    },
    info() { return `<p>The <b>scheduler</b> creates a DAG run for each interval and queues its tasks once their upstream tasks succeed. <b>Executors</b> (Celery, Kubernetes) run them. Airflow itself moves no data: it only starts jobs and records their state.</p><p>The grid shows the history: if a run is still going when the next one starts, tasks are skipped or overlap, which is why <code>max_active_runs</code> and sensible timeouts matter.</p>`; },
    live(n) { return `<div class="metrics">${m('Interval', n.cfg.interval + ' s')}${m('Runs', n.rt.runNo)}${m('State', n.cfg.paused ? 'paused' : 'active')}</div>`; }
  };

  /* ---------------- S3 ---------------- */
  V.s3 = {
    sub: 'Prefixes, objects and the small-file problem',
    draw(g, n, s, dt) {
      const r = n.rt, W = g.w, H = g.h, avg = r.files ? (r.bytes || 0) / r.files : 0, target = 128e6;
      g.text('s3://lake/raw/orders/', 14, 16, { size: 11, color: C.fg, weight: 600 });
      const d = R.iso(R.wall(S.t));
      g.text(`└ dt=${d.slice(0, 10)}/ └ hour=${d.slice(11, 13)}/`, 24, 34, { size: 10.5 });
      if (s.lastFiles !== r.files) { s.pop = 1; s.lastFiles = r.files; }
      s.pop = Math.max(0, (s.pop || 0) - dt * 1.5);
      // newest objects as a listing, sizes drawn to scale against a 128 MB file
      const ext = { json: 'json', csv: 'csv', parquet: 'snappy.parquet' }[n.cfg.format];
      const rowsFit = Math.max(1, Math.floor((H - 130) / 22)), list = Math.min(r.files, rowsFit);
      for (let i = 0; i < list; i++) {
        const idx = r.files - i, y = 50 + i * 22, sz = avg * (0.7 + 0.6 * ((Math.sin(idx * 7.1) + 1) / 2));
        const fresh = i === 0 && s.pop > 0;
        g.text(`part-${String(idx).padStart(5, '0')}-${(R.hash(idx) % 1e6).toString(16)}.${ext}`, 34, y + 8, { size: 10.5, color: fresh ? C.fg : C.muted, weight: fresh ? 600 : 400, max: W * 0.45 });
        const bx = W * 0.5, bw = W * 0.5 - 90;
        g.box(bx, y + 2, bw, 12, { stroke: C.line, r: 3, dash: [2, 3] });
        g.box(bx, y + 2, Math.max(2, bw * clamp(sz / target, 0, 1)), 12, { fill: C['c-store'], stroke: false, r: 3, alpha: fresh ? 1 : 0.7 });
        g.text(PL.ui.fmtB(sz), W - 14, y + 8, { size: 10, align: 'right' });
      }
      if (!r.files) g.text('no objects yet', 34, 58, { size: 11, color: C.faint });
      if (r.files > list) g.text(`+ ${fmt(r.files - list)} older objects`, 34, 50 + list * 22 + 8, { size: 10, color: C.faint });
      g.text('dashed box = one 128 MB file', W - 14, 34, { size: 9.5, color: C.faint, align: 'right' });
      const cols = 1, shown = list;
      const by = H - 50;
      g.text(`average object ${PL.ui.fmtB ? PL.ui.fmtB(avg) : fmt(avg / 1e6) + ' MB'} vs the ~128 MB target readers like`, 14, by - 8, { size: 10, color: C.muted, max: W - 28 });
      g.box(14, by, W - 28, 12, { fill: C['panel-2'], stroke: false, r: 3 });
      g.box(14, by, Math.max(2, (W - 28) * clamp(avg / target, 0, 1)), 12, { fill: avg < target / 10 ? C.warn : C['c-store'], stroke: false, r: 3 });
      const q = every(s, 'w', Math.min(8, Math.sqrt(r.inRate) * 0.4), dt);
      for (let z = 0; z < q; z++) spawn(s, { x0: W - 20, y0: 10, x1: 26, y1: 58, col: C['c-store'], r: 2, dur: 0.6 });
      flyers(g, s, dt);
    },
    info(n) { return `<p>S3 stores <b>objects</b> under keys. “Folders” are only key prefixes, and tools use them for partitioning (<code>dt=…/hour=…</code>) so a query can skip whole prefixes.</p><p>Streaming writers roll a file every few seconds, so files stay tiny. Each one costs a request and a footer read, which is why lakes run <b>compaction</b> to merge them toward 128 MB–1 GB. ${n.cfg.format === 'parquet' ? 'Parquet is columnar with statistics; readers fetch only the columns and row groups they need.' : n.cfg.format.toUpperCase() + ' is row text with no statistics.'}</p>`; },
    live(n) { const r = n.rt; return `<div class="metrics">${m('Objects', fmt(r.files))}${m('Format', n.cfg.format)}${m('Rows', fmt(r.total))}</div>`; }
  };

  /* ---------------- BI ---------------- */
  V.bi = {
    sub: 'Queries, caching and freshness',
    draw(g, n, s, dt) {
      const r = n.rt, W = g.w, H = g.h, live = n.cfg.mode === 'live';
      const up = PL.upstream(n)[0];
      const tw = (W - 60) / 3;
      ['Revenue today', 'Orders by status', 'Sales by city'].forEach((t, i) => {
        const x = 14 + i * (tw + 16); g.box(x, 14, tw, 90, { fill: C['panel-2'] }); g.text(t, x + 10, 28, { size: 10.5, color: C.fg, weight: 600, max: tw - 16 });
        for (let b = 0; b < 6; b++) { const hgt = 10 + 40 * Math.abs(Math.sin(b * 1.7 + i + (r.visibleT || 0) * 0.05)); g.box(x + 12 + b * ((tw - 24) / 6), 96 - hgt, (tw - 24) / 6 - 5, hgt, { fill: C['c-serve'], stroke: false, r: 2, alpha: 0.7 }); }
      });
      const wy = H - 70;
      g.box(W / 2 - 90, wy, 180, 40, { fill: C.panel }); g.text(up ? up.name : 'warehouse', W / 2, wy + 20, { size: 11, color: C.fg, align: 'center', weight: 600, max: 170 });
      const q = every(s, 'q', live ? 1.2 : 0, dt);
      for (let z = 0; z < q; z++) { const x = 14 + Math.floor(Math.random() * 3) * (tw + 16) + tw / 2; spawn(s, { x0: x, y0: 104, x1: W / 2, y1: wy, col: C.ctrl, r: 3, dur: 0.6, sq: true, done: () => spawn(s, { x0: W / 2, y0: wy, x1: x, y1: 104, col: C['c-serve'], r: 3, dur: 0.6 }) }); }
      if (!live) { const left = r.nextAt != null ? r.nextAt - S.t : 0; g.text(`extract refresh in ${fmtS(left)}`, W / 2, wy - 16, { size: 10.5, color: C.muted, align: 'center' }); }
      g.text(`showing data ${r.freshness == null ? '–' : fmtS(r.freshness)} old`, 14, H - 12, { size: 11, color: r.freshness > 60 ? C.bad : r.freshness > 15 ? C.warn : C.ok, weight: 600 });
      flyers(g, s, dt);
    },
    info(n) { return `<p>${n.cfg.mode === 'live' ? 'A live connection sends SQL to the warehouse whenever someone opens or filters a tile. The data is as fresh as the warehouse table, but every view costs compute.' : 'An extract copies query results into the BI tool on a schedule. Views are fast and cheap, but show data as old as the last refresh.'}</p><p>Freshness here is end-to-end: the time since the newest record visible on the dashboard was created at the source.</p>`; },
    live(n) { return `<div class="metrics">${m('Freshness', fmtS(n.rt.freshness))}${m('Mode', n.cfg.mode)}${m('Rows seen', fmt(n.rt.total))}</div>`; }
  };

  /* ---------------- browser-run blocks and schedule: a simple in → out view ---------------- */
  const simple = (sub, what) => ({
    sub,
    draw(g, n, s, dt) {
      const W = g.w, H = g.h, res = PL.RT.results[n.id];
      const y = H / 2 - 30;
      [['input', PL.upstream(n).map(u => u.name).join(', ') || (n.type === 'api' ? 'HTTP GET' : n.type === 'file' ? 'local file' : '–')], [n.name, what], ['output', res && !res.error ? `${res.rows.length} rows · ${res.ms} ms` : 'not run yet']].forEach((b, i) => {
        const x = 14 + i * ((W - 28) / 3), w = (W - 28) / 3 - 30;
        g.box(x, y, w, 60, { fill: i === 1 ? C['accent-soft'] : C['panel-2'], stroke: i === 1 ? C.accent : C.line });
        g.text(b[0], x + w / 2, y + 22, { size: 11, color: C.fg, weight: 600, align: 'center', max: w - 10 });
        g.text(b[1], x + w / 2, y + 40, { size: 10, align: 'center', max: w - 10 });
        if (i) g.arrow(x - 28, y + 30, x - 2, y + 30);
      });
      const q = every(s, 'f', n.rt.outRate > 0 || n.rt.inRate > 0 ? 3 : 0.5, dt);
      for (let z = 0; z < q; z++) spawn(s, { x0: 14 + (W - 28) / 3 - 30, y0: y + 30, x1: 14 + 2 * (W - 28) / 3, y1: y + 30, col: C.accent, r: 3, dur: 0.9, ease: false });
      flyers(g, s, dt);
    },
    info() { return `<p>This block really runs in your browser when you press <b>Run flow</b>. The Runtime panel shows its rows and log.</p>`; },
    live(n) { const res = PL.RT.results[n.id]; return res ? (res.error ? `<div class="note bad">${esc(res.error)}</div>` : `<div class="note">Last run: ${res.rows.length} rows in ${res.ms} ms.</div>`) : '<div class="note">Not run yet.</div>'; }
  });
  V.api = simple('A real HTTP request', 'fetch → JSON → records');
  V.file = simple('Parsed in your browser', 'read → parse CSV/JSON');
  V.script = simple('Your code, run in the browser', 'Pyodide or JavaScript');
  V.table = simple('Rows shown in the Runtime panel', 'collect rows');
  V.schedule = simple('A timer that triggers the next step', 'every N seconds');

  function m(label, val) { return `<div class="metric"><small>${label}</small><b>${val}</b></div>`; }

  /* =====================================================================
   * Panel
   * ===================================================================== */
  let panel, cv, ctx, side, liveBox, dpr = 1, cw = 0, ch = 0, liveClock = 0, lastInfoKey = '';
  function ensure() {
    if (panel) return;
    panel = document.createElement('section'); panel.id = 'lookin'; panel.hidden = true; panel.setAttribute('aria-label', 'Look inside');
    panel.innerHTML = `<div class="li-head"><span class="ico"></span><div class="li-title"><b></b><small></small></div>
      <button class="btn ghost li-trace" title="Follow one record through the pipeline"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="5" cy="17" r="2.5"/><path d="M7.5 17H12a4 4 0 0 0 0-8H9a3 3 0 0 1 0-6h9"/><path d="M15 0.5l3 2.5-3 2.5"/></svg><span class="hide-sm">Follow a record</span></button>
      <button class="btn icon ghost li-close" aria-label="Close"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M6 6l12 12M18 6L6 18"/></svg></button></div>
      <div class="li-body"><div class="li-canvas"><canvas></canvas></div><div class="li-side"><div class="li-live"></div><div class="li-info learn"></div></div></div>`;
    $('#stage').appendChild(panel);
    cv = panel.querySelector('canvas'); ctx = cv.getContext('2d'); side = panel.querySelector('.li-info'); liveBox = panel.querySelector('.li-live');
    panel.querySelector('.li-close').onclick = IN.close;
    panel.querySelector('.li-trace').onclick = () => { const n = PL.byId(IN.id); PL.trace.start(n); };
    panel.addEventListener('pointerdown', e => e.stopPropagation());
    panel.addEventListener('wheel', e => e.stopPropagation(), { passive: true });
    cv.addEventListener('click', e => {
      const n = PL.byId(IN.id); if (!n || !V[n.type].act) return;
      const rc = cv.getBoundingClientRect(), x = e.clientX - rc.left, y = e.clientY - rc.top;
      const h = (IN.hits || []).find(q => x >= q.x && x <= q.x + q.w && y >= q.y && y <= q.y + q.h);
      if (h) { V[n.type].act(n, st(n), h.act); liveClock = 9; }
    });
    cv.addEventListener('mousemove', e => {
      const rc = cv.getBoundingClientRect(), x = e.clientX - rc.left, y = e.clientY - rc.top;
      cv.style.cursor = (IN.hits || []).some(q => x >= q.x && x <= q.x + q.w && y >= q.y && y <= q.y + q.h) ? 'pointer' : '';
    });
    liveBox.addEventListener('click', e => {
      const b = e.target.closest('[data-act]'); const n = PL.byId(IN.id); if (!b || !n || !V[n.type].act) return;
      V[n.type].act(n, st(n), b.dataset.act); liveClock = 9;
    });
  }
  IN.has = n => !!(n && V[n.type]);
  IN.open = function (id) {
    const n = PL.byId(id); if (!n || !V[n.type]) return;
    ensure(); readColors();
    IN.id = id; panel.hidden = false; document.getElementById('stage').classList.add('li-open');
    const T = PL.TYPES[n.type], view = V[n.type];
    panel.querySelector('.ico').style.setProperty('--c', PL.ui.catColor(n.type));
    panel.querySelector('.ico').innerHTML = PL.ui.icon(n.type);
    panel.querySelector('.li-title b').textContent = `Inside ${n.name}`;
    panel.querySelector('.li-title small').textContent = typeof view.sub === 'function' ? view.sub(n) : view.sub;
    lastInfoKey = ''; liveClock = 9;
    if (innerWidth < 760) { PL.ui.select(null); }
  };
  IN.close = function () { if (!panel) return; panel.hidden = true; IN.id = null; document.getElementById('stage').classList.remove('li-open'); };
  IN.frame = function (realDt) {
    if (!IN.id || !panel || panel.hidden) return;
    const n = PL.byId(IN.id); if (!n) { IN.close(); return; }
    const box = cv.parentElement; const w = box.clientWidth, h = box.clientHeight;
    if (!w || !h) return;
    const d = Math.min(2, window.devicePixelRatio || 1);
    if (w !== cw || h !== ch || d !== dpr) { cw = w; ch = h; dpr = d; cv.width = w * d; cv.height = h * d; }
    colorClock += realDt; if (colorClock > 1) { colorClock = 0; readColors(); }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, w, h);
    const g = kit(ctx, w, h), view = V[n.type];
    const simDt = S.running ? realDt * S.speed : 0;
    try { view.draw(g, n, st(n), simDt); } catch (e) { console.error(e); }
    IN.hits = g.hits;
    liveClock += realDt;
    if (liveClock > 0.3) {
      liveClock = 0;
      const sub = typeof view.sub === 'function' ? view.sub(n) : view.sub;
      const key = n.type + JSON.stringify(n.cfg).length + JSON.stringify(n.cfg);
      if (key !== lastInfoKey) { lastInfoKey = key; side.innerHTML = view.info(n); panel.querySelector('.li-title small').textContent = sub; panel.querySelector('.li-title b').textContent = `Inside ${n.name}`; }
      const html = view.live(n, st(n));
      if (html !== liveBox._h) { liveBox._h = html; liveBox.innerHTML = html; }
    }
  };
})(window.PL);
