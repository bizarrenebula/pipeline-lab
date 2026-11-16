/* Pipeline Lab — real execution in the browser.
 * Runnable blocks: HTTP API (fetch), File (CSV/JSON parse), Python/JS script (Pyodide / Function),
 * Table output, Scheduler. Big tools (Kafka, Spark…) run in the simulation only; export them to docker.
 */
window.PL = window.PL || {};
(function (PL) {
  'use strict';
  const S = PL.S;
  const RT = PL.RT = { results: {}, logs: [], running: false, pyodide: null, pyLoading: null, listeners: [] };
  const PYODIDE_URL = 'https://cdn.jsdelivr.net/pyodide/v0.26.4/full/';

  function changed() { RT.listeners.forEach(f => { try { f(); } catch (e) { console.error(e); } }); }
  function log(level, msg, node) {
    RT.logs.push({ at: new Date(), level, msg, node: node ? node.name : '' });
    if (RT.logs.length > 400) RT.logs.shift();
    changed();
  }
  PL.rtLog = log;

  /* ---------- helpers ---------- */
  PL.flatten = function flatten(o, pre, out, depth) {
    out = out || {}; pre = pre || ''; depth = depth || 0;
    if (o === null || typeof o !== 'object') { out[pre || 'value'] = o; return out; }
    if (Array.isArray(o)) { out[pre || 'value'] = JSON.stringify(o); return out; }
    for (const k of Object.keys(o)) {
      const v = o[k], key = pre ? pre + '.' + k : k;
      if (v && typeof v === 'object' && !Array.isArray(v) && depth < 2) flatten(v, key, out, depth + 1);
      else out[key] = Array.isArray(v) ? (v.every(x => typeof x !== 'object') ? v.join(', ') : JSON.stringify(v)) : v;
    }
    return out;
  };
  PL.parseCSV = function (text) {
    const rows = []; let row = [], f = '', q = false;
    for (let i = 0; i < text.length; i++) {
      const c = text[i];
      if (q) { if (c === '"') { if (text[i + 1] === '"') { f += '"'; i++; } else q = false; } else f += c; }
      else if (c === '"') q = true;
      else if (c === ',') { row.push(f); f = ''; }
      else if (c === '\n' || c === '\r') { if (c === '\r' && text[i + 1] === '\n') i++; row.push(f); f = ''; if (row.length > 1 || row[0] !== '') rows.push(row); row = []; }
      else f += c;
    }
    if (f !== '' || row.length) { row.push(f); rows.push(row); }
    const head = rows.shift() || [];
    return rows.map(r => {
      const o = {};
      head.forEach((h, i) => { const v = r[i]; o[h.trim()] = v !== undefined && v !== '' && !isNaN(v) ? Number(v) : v; });
      return o;
    });
  };
  function parseFile(n) {
    const text = n.cfg.fileText || PL.SAMPLE_CSV;
    const name = (n.cfg.fileName || 'cities.csv').toLowerCase();
    if (name.endsWith('.json') || /^\s*[\[{]/.test(text)) {
      let j = JSON.parse(text); if (!Array.isArray(j)) j = Object.values(j).find(Array.isArray) || [j];
      return j.map(r => PL.flatten(r));
    }
    if (name.endsWith('.jsonl') || name.endsWith('.ndjson')) return text.split('\n').filter(Boolean).map(l => PL.flatten(JSON.parse(l)));
    return PL.parseCSV(text);
  }
  function dig(o, path) { if (!path) return o; for (const p of path.split('.')) { if (o == null) return o; o = o[p]; } return o; }

  /* ---------- Python ---------- */
  function loadScript(src) {
    return new Promise((res, rej) => { const s = document.createElement('script'); s.src = src; s.onload = res; s.onerror = () => rej(new Error('Could not load ' + src)); document.head.appendChild(s); });
  }
  PL.getPyodide = function () {
    if (RT.pyodide) return Promise.resolve(RT.pyodide);
    if (!RT.pyLoading) {
      log('info', 'Loading Python (Pyodide, about 10 MB, first time only)…');
      RT.pyLoading = loadScript(PYODIDE_URL + 'pyodide.js')
        .then(() => window.loadPyodide({ indexURL: PYODIDE_URL }))
        .then(py => { RT.pyodide = py; log('ok', 'Python ' + py.runPython('import sys; sys.version.split()[0]') + ' ready'); return py; })
        .catch(e => { RT.pyLoading = null; throw e; });
    }
    return RT.pyLoading;
  };
  async function runPython(n, rows) {
    const py = await PL.getPyodide();
    py.setStdout({ batched: s => log('out', s, n) });
    py.setStderr({ batched: s => log('err', s, n) });
    const ns = py.globals.get('dict')();
    ns.set('__rows_json', JSON.stringify(rows));
    ns.set('limit', +n.cfg.limit);
    try {
      py.runPython('import json\nrows = json.loads(__rows_json)\ndel __rows_json', { globals: ns });
      await py.runPythonAsync(n.cfg.code || '', { globals: ns });
      py.runPython(
`_r = globals().get('result', None)
if _r is None:
    _r = rows
try:
    import pandas as _pd
    if isinstance(_r, _pd.DataFrame):
        _r = _r.to_dict(orient='records')
except ImportError:
    pass
if isinstance(_r, dict):
    _r = [_r]
__out = json.dumps(list(_r), default=str)`, { globals: ns });
      return JSON.parse(ns.get('__out'));
    } catch (e) {
      // keep only the part of the traceback that points at the user's code
      const msg = String(e.message || e); const i = msg.indexOf('File "<exec>"');
      throw new Error(i >= 0 ? msg.slice(i).replace(/File "<exec>"/g, 'your script').trim() : msg.trim().split('\n').slice(-3).join('\n'));
    } finally { ns.destroy(); }
  }
  async function runJS(n, rows) {
    const fn = new Function('rows', 'limit', '"use strict";\n' + (n.cfg.code || 'return rows;'));
    let out = await fn(rows, +n.cfg.limit);
    if (out == null) out = rows;
    if (!Array.isArray(out)) out = [out];
    return out;
  }

  /* ---------- run a node ---------- */
  async function runNode(n, inputs) {
    switch (n.type) {
      case 'api': {
        let headers = {};
        if (n.cfg.headers && n.cfg.headers.trim()) { try { headers = JSON.parse(n.cfg.headers); } catch (e) { throw new Error('Headers must be valid JSON, e.g. {"Authorization": "Bearer …"}'); } }
        log('info', 'GET ' + n.cfg.url, n);
        let res;
        try { res = await fetch(n.cfg.url, { headers }); }
        catch (e) { throw new Error('Request failed. The API may not allow browser requests (CORS), or you are offline. It will still work in the docker export.'); }
        if (!res.ok) throw new Error('HTTP ' + res.status + ' ' + res.statusText);
        const j = await res.json();
        let arr = dig(j, n.cfg.path);
        if (!Array.isArray(arr)) {
          if (arr && typeof arr === 'object') { const k = Object.keys(arr).find(k => Array.isArray(arr[k])); arr = k ? arr[k] : [arr]; if (k) log('info', `Used array at "${n.cfg.path ? n.cfg.path + '.' : ''}${k}". Set Records path to choose another.`, n); }
          else throw new Error(`No records at path "${n.cfg.path}".`);
        }
        return arr.map(r => PL.flatten(r));
      }
      case 'file': return parseFile(n);
      case 'script': {
        const rows = [].concat(...inputs);
        if (n.cfg.lang === 'js') return runJS(n, rows);
        return runPython(n, rows);
      }
      case 'table': return [].concat(...inputs).slice(0, +n.cfg.maxRows);
      default: return [].concat(...inputs);
    }
  }

  function isRunnable(n) { return PL.TYPES[n.type].runnable && !PL.TYPES[n.type].control; }

  /* Topological order of runnable nodes, optionally limited to what is downstream of `starts`. */
  function plan(starts) {
    let set = S.nodes.filter(isRunnable);
    if (starts) {
      const keep = new Set(); const stack = starts.slice();
      while (stack.length) { const id = stack.pop(); if (keep.has(id)) continue; keep.add(id); S.links.filter(l => l.from === id).forEach(l => stack.push(l.to)); }
      // include runnable ancestors so the started step gets its inputs
      const anc = []; const st2 = starts.slice();
      while (st2.length) { const id = st2.pop(); S.links.filter(l => l.to === id && !PL.isControlLink(l)).forEach(l => { if (!keep.has(l.from)) { keep.add(l.from); anc.push(l.from); } st2.push(l.from); }); }
      set = set.filter(n => keep.has(n.id));
    }
    const ids = new Set(set.map(n => n.id)), order = [], done = new Set();
    const visit = (n, stack) => {
      if (done.has(n.id) || stack.has(n.id)) return; stack.add(n.id);
      PL.inLinks(n).filter(l => !PL.isControlLink(l) && ids.has(l.from)).forEach(l => visit(PL.byId(l.from), stack));
      done.add(n.id); order.push(n);
    };
    set.forEach(n => visit(n, new Set()));
    return order;
  }

  function animate(l, count) {
    const len = 260;
    S.particles.push({ l, p: 0, dur: 0.9, real: true, label: count + (count === 1 ? ' row' : ' rows') });
  }

  PL.runFlow = async function (starts, reason) {
    if (RT.running) { log('warn', 'A run is already in progress.'); return; }
    const order = plan(starts);
    if (!order.length) { log('warn', 'Nothing to run. Add an HTTP API or File, a Python script and a Table output, link them, then press Run.'); return; }
    RT.running = true; changed();
    const t0 = performance.now();
    log('info', (reason || 'Run started') + ': ' + order.map(n => n.name).join(' → '));
    const outs = {};
    let failed = false;
    for (const n of order) {
      const ins = PL.inLinks(n).filter(l => !PL.isControlLink(l));
      for (const l of ins) { const up = PL.byId(l.from); if (!isRunnable(up)) log('warn', `${up.name} runs only in the simulation. Export to docker to run it for real.`, n); }
      if (ins.some(l => isRunnable(PL.byId(l.from)) && !(l.from in outs))) { log('warn', 'Skipped: an upstream step failed.', n); n.rt.real = 'skipped'; delete RT.results[n.id]; continue; }
      const inputs = ins.filter(l => l.from in outs).map(l => outs[l.from]);
      n.rt.real = 'running'; changed();
      const s = performance.now();
      try {
        const rows = await runNode(n, inputs);
        outs[n.id] = rows;
        const ms = Math.round(performance.now() - s);
        RT.results[n.id] = { rows, at: new Date(), ms };
        n.rt.real = 'ok'; n.rt.realRows = rows.length;
        log('ok', `${rows.length} rows in ${ms} ms`, n);
        PL.outLinks(n).filter(l => !PL.isControlLink(l)).forEach(l => animate(l, rows.length));
        if (n.type === 'table') PL.ui && PL.ui.showResult(n.id);
      } catch (e) {
        failed = true; n.rt.real = 'error';
        RT.results[n.id] = { rows: [], at: new Date(), error: String(e.message || e) };
        log('err', String(e.message || e), n);
      }
      changed();
    }
    log(failed ? 'err' : 'ok', `${failed ? 'Run finished with errors' : 'Run finished'} in ${Math.round(performance.now() - t0)} ms`);
    RT.running = false; changed();
  };

  /* Scheduler blocks: real runs while switched on. */
  setInterval(() => {
    const now = Date.now();
    for (const n of S.nodes) {
      if (n.type !== 'schedule' || !n.cfg.on) { if (n.rt) n.rt.realNext = null; continue; }
      if (!n.rt.realNext) n.rt.realNext = now + 1000;
      if (now >= n.rt.realNext) {
        n.rt.realNext = now + n.cfg.every * 1000;
        const targets = PL.outLinks(n).map(l => l.to);
        if (!targets.length) continue;
        if (RT.running) { log('warn', 'Scheduled run skipped: previous run still going.', n); continue; }
        PL.outLinks(n).forEach(l => S.particles.push({ l, p: 0, dur: 0.6, ctrl: true, realCtrl: true }));
        PL.runFlow(targets, n.name + ' tick');
      }
    }
  }, 500);
})(window.PL);
