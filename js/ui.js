/* Pipeline Lab — canvas, interaction, inspector, runtime dock, persistence, boot. */
(function (PL) {
  'use strict';
  const S = PL.S, RT = PL.RT;
  const $ = s => document.querySelector(s);
  const stage = $('#stage'), cv = $('#cv'), ctx = cv.getContext('2d'), world = $('#world');
  const ui = PL.ui = {};
  const STORE_KEY = 'pipelinelab.v1';

  /* ---------- small helpers ---------- */
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const fmt = n => { n = Math.max(0, n || 0); if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M'; if (n >= 1e4) return Math.round(n / 1e3) + 'k'; if (n >= 1e3) return (n / 1e3).toFixed(1) + 'k'; return n < 10 && n % 1 ? n.toFixed(1) : Math.round(n) + ''; };
  const fmtS = s => s == null || !isFinite(s) ? '–' : s < 1 ? Math.round(s * 1000) + ' ms' : s < 60 ? s.toFixed(1) + ' s' : Math.floor(s / 60) + 'm ' + String(Math.floor(s % 60)).padStart(2, '0') + 's';
  const fmtB = b => b >= 1e9 ? (b / 1e9).toFixed(2) + ' GB' : b >= 1e6 ? (b / 1e6).toFixed(1) + ' MB' : (b / 1e3).toFixed(0) + ' KB';
  ui.esc = esc; ui.fmt = fmt; ui.fmtB = fmtB; ui.fmtS = fmtS;
  const icon = t => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${PL.ICONS[PL.TYPES[t].icon]}</svg>`;
  const catColor = t => PL.CATS[PL.TYPES[t].cat].color;
  ui.icon = icon; ui.catColor = catColor;
  function toast(msg, bad) {
    const t = document.createElement('div'); t.className = 'toast' + (bad ? ' bad' : ''); t.textContent = msg;
    $('#toasts').appendChild(t); setTimeout(() => t.remove(), bad ? 5200 : 3200);
  }
  ui.toast = toast;
  const reduced = matchMedia('(prefers-reduced-motion: reduce)').matches;

  /* ---------- colors from CSS tokens ---------- */
  let C = {};
  function readColors() {
    const cs = getComputedStyle(document.documentElement);
    ['--grid', '--line-2', '--accent', '--ins', '--upd', '--del', '--ctrl', '--real', '--muted', '--panel', '--bad', '--fg', '--faint'].forEach(k => C[k.slice(2)] = cs.getPropertyValue(k).trim());
  }
  readColors();
  matchMedia('(prefers-color-scheme: dark)').addEventListener('change', readColors);
  new MutationObserver(readColors).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });

  /* ---------- view ---------- */
  function toWorld(cx, cy) { const r = stage.getBoundingClientRect(); return [(cx - r.left - S.view.x) / S.view.k, (cy - r.top - S.view.y) / S.view.k]; }
  function applyView() { world.style.transform = `translate(${S.view.x}px,${S.view.y}px) scale(${S.view.k})`; }
  ui.applyView = applyView;
  function zoomAt(f, cx, cy) {
    const r = stage.getBoundingClientRect(); const sx = cx - r.left, sy = cy - r.top;
    const k = Math.min(2.2, Math.max(0.15, S.view.k * f)); const ratio = k / S.view.k;
    S.view.x = sx - (sx - S.view.x) * ratio; S.view.y = sy - (sy - S.view.y) * ratio; S.view.k = k; applyView(); saveSoon();
  }
  function fit() {
    const r = stage.getBoundingClientRect();
    if (!S.nodes.length) { S.view = { x: r.width / 2, y: r.height / 2, k: 1 }; applyView(); return; }
    const xs = S.nodes.map(n => n.x), ys = S.nodes.map(n => n.y);
    const x0 = Math.min(...xs), x1 = Math.max(...xs) + PL.NW, y0 = Math.min(...ys), y1 = Math.max(...ys) + PL.NH;
    const narrow = r.width < 760, tc = $('#tipcard'), ins = $('#inspector');
    const left = narrow ? 16 : $('#palette').offsetWidth + 30, right = (!ins.hidden && !narrow) ? ins.offsetWidth + 30 : 40;
    const top = tc.hidden ? 30 : tc.offsetHeight + 24, bottom = narrow && !ins.hidden ? ins.offsetHeight + 24 : 110;
    const w = r.width - left - right, h = r.height - top - bottom;
    const k = Math.max(narrow ? 0.18 : 0.3, Math.min(1.1, w / (x1 - x0), h / (y1 - y0)));
    S.view.k = k; S.view.x = left + (w - (x1 - x0) * k) / 2 - x0 * k; S.view.y = top + (h - (y1 - y0) * k) / 2 - y0 * k;
    applyView(); saveSoon();
  }
  ui.fit = fit;

  /* ---------- node DOM ---------- */
  const els = new Map();
  function nodeHTML(n) {
    const T = PL.TYPES[n.type];
    return `<span class="port in"></span>
      <div class="nhead"><span class="ico" style="--c:${catColor(n.type)}">${icon(n.type)}</span>
      <div class="ntitle"><b></b><small></small></div><span class="dot"></span></div>
      <div class="nstat"></div><div class="nbar"><i></i></div>
      ${PL.ALLOW[n.type].length ? '<span class="port out" title="Drag to another component to link"></span>' : ''}
      ${T.runnable && !T.control ? '<button class="run-btn" title="Run from here"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M7 4.5v15l12.5-7.5z"/></svg>Run</button>' : ''}`;
  }
  function syncNodes() {
    for (const [id, el] of els) if (!PL.byId(id)) { el.remove(); els.delete(id); }
    for (const n of S.nodes) {
      let el = els.get(n.id);
      // ids restart at 1 when a scenario or file loads: never reuse a card built for another type
      if (el && el.dataset.type !== n.type) { el.remove(); els.delete(n.id); el = null; }
      if (!el) {
        el = document.createElement('div'); el.className = 'node'; el.dataset.id = n.id; el.dataset.type = n.type;
        el.innerHTML = nodeHTML(n);
        if (PL.TYPES[n.type].control) el.classList.add('ctrl');
        if (PL.TYPES[n.type].runnable && !PL.TYPES[n.type].control) el.classList.add('runnable');
        world.appendChild(el); els.set(n.id, el);
      }
      el.style.left = n.x + 'px'; el.style.top = n.y + 'px';
      el.querySelector('b').textContent = n.name;
      el.querySelector('small').textContent = PL.TYPES[n.type].summary(n);
      el.classList.toggle('sel', !!(S.sel && S.sel.kind === 'node' && S.sel.id === n.id));
      el.classList.toggle('down', !!n.cfg.down);
    }
    $('#empty').hidden = S.nodes.length > 0;
    if (PL.trace) PL.trace.onGraphChange();
  }
  ui.syncNodes = syncNodes;

  function downstreamLag(k) { return PL.outLinks(k).reduce((a, l) => a + PL.byId(l.to).rt.qN, 0); }
  function isBatchy(n) {
    return (n.type === 'spark' && n.cfg.mode === 'batch') || n.type === 'dbt' || (n.type === 'snowflake' && n.cfg.ingest === 'copy') || (n.type === 'script' && PL.isOrchestrated(n));
  }
  function nextRunIn(n) { const r = n.rt; return r.nextAt != null ? Math.max(0, r.nextAt - S.t) : null; }

  function nodeStatus(n) {
    const r = n.rt;
    if (n.cfg.down) return { cls: 'bad', bar: 1, barCls: 'bad', text: 'offline' };
    if (r.real === 'running') return { cls: 'run', text: '<span class="real">running for real…</span>', bar: 0 };
    if (r.real === 'error') return { cls: 'bad', text: `<span class="err">error · see Runtime log</span>`, bar: 0 };
    const lag = r.qN / Math.max(r.cap || 1, 1);
    const real = r.real === 'ok' && RT.results[n.id] ? `<span class="real">✓ ${RT.results[n.id].rows.length} rows · ${RT.results[n.id].ms} ms</span>` : null;
    switch (n.type) {
      case 'api': case 'file': return { cls: 'ok', text: real || `sim ${fmt(r.outRate)}/s out`, bar: 0 };
      case 'table': return { cls: real ? 'ok' : 'idle', text: real || `sim ${fmt(r.total)} rows received`, bar: 0 };
      case 'schedule': {
        if (n.cfg.on && r.realNext) return { cls: 'run', text: `<span class="real">next real run ${Math.max(0, Math.ceil((r.realNext - Date.now()) / 1000))}s</span>`, bar: 0 };
        return { cls: 'idle', text: `sim tick in ${fmtS(nextRunIn(n))}`, bar: 0 };
      }
      case 'postgres': case 'app': return { cls: 'ok', text: `${fmt(r.outRate)}/s out`, bar: 0 };
      case 'kafka': {
        const dl = downstreamLag(n);
        const bad = n.cfg.brokerDown ? (n.cfg.rf === '1' ? 'bad' : 'warn') : lag > 5 ? 'warn' : 'ok';
        return { cls: bad, text: `${fmt(r.inRate)}/s · consumer lag ${fmt(dl)}`, bar: Math.min(1, lag / 10), barCls: bad };
      }
      case 'airflow': return { cls: n.cfg.paused ? 'idle' : 'ok', text: n.cfg.paused ? 'paused' : `run #${r.runNo} · next in ${fmtS(nextRunIn(n))}`, bar: 0 };
      case 's3': return { cls: 'ok', text: `${fmt(r.files)} files · ${fmtB(r.bytes || 0)}`, bar: 0 };
      case 'iceberg': return { cls: r.deleteFiles > 30 ? 'warn' : 'ok', text: `snap ${r.snapId} · ${r.files} files · ${r.deleteFiles} del`, bar: Math.min(1, lag / 10) };
      case 'bi': {
        const f = r.freshness; const cls = f == null ? 'idle' : f > 60 ? 'bad' : f > 15 ? 'warn' : 'ok';
        return { cls, text: `fresh: ${f == null ? 'no data yet' : fmtS(f) + ' old'}`, bar: 0 };
      }
    }
    if (isBatchy(n)) {
      if (S.t < r.busyUntil) { const p = (S.t - r.runStart) / Math.max(r.lastDur, 0.01); return { cls: 'run', text: `running… ${Math.round(p * 100)}%`, bar: p }; }
      const nx = nextRunIn(n);
      return { cls: 'ok', text: `${fmt(r.qN)} waiting${nx != null ? ' · next ' + fmtS(nx) : ' · on trigger'}`, bar: 0 };
    }
    if (n.type === 'snowflake') {
      const st = PL.sfState(n);
      return { cls: lag > 10 ? 'warn' : 'ok', text: `${st} · lag ${fmtS(lag)}`, bar: Math.min(1, lag / 10), barCls: lag > 10 ? 'warn' : '' };
    }
    const cls = lag > 10 ? 'bad' : lag > 3 ? 'warn' : 'ok';
    return { cls, text: real || `${fmt(r.inRate)}/s · lag ${fmtS(lag)}`, bar: Math.min(1, lag / 10), barCls: cls === 'ok' ? '' : cls };
  }
  function updateNodeStats() {
    for (const n of S.nodes) {
      const el = els.get(n.id); if (!el) continue;
      const st = nodeStatus(n);
      el.querySelector('.dot').className = 'dot ' + st.cls;
      el.querySelector('.nstat').innerHTML = st.text;
      const bar = el.querySelector('.nbar i'); bar.style.width = (st.bar * 100) + '%'; bar.className = st.barCls || '';
      el.querySelector('small').textContent = PL.TYPES[n.type].summary(n);
    }
  }

  /* ---------- canvas drawing ---------- */
  let dpr = 1, W = 0, H = 0;
  function resize() {
    const r = stage.getBoundingClientRect(); dpr = Math.min(2, window.devicePixelRatio || 1);
    W = r.width; H = r.height; cv.width = W * dpr; cv.height = H * dpr;
  }
  window.addEventListener('resize', resize);
  let tempLink = null, hoverLink = null;
  function draw() {
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, W, H);
    const { x: vx, y: vy, k } = S.view;
    // dot grid
    const g = 24 * (k < 0.5 ? 2 : 1) * k;
    ctx.fillStyle = C.grid;
    const ox = ((vx % g) + g) % g, oy = ((vy % g) + g) % g, rr = Math.max(0.7, 1.1 * Math.min(1, k));
    for (let x = ox; x < W; x += g) for (let y = oy; y < H; y += g) { ctx.fillRect(x - rr / 2, y - rr / 2, rr, rr); }
    ctx.save(); ctx.translate(vx, vy); ctx.scale(k, k);
    // links
    for (const l of S.links) {
      const c = PL.bez(l), ctrl = PL.isControlLink(l);
      const sel = S.sel && S.sel.kind === 'link' && S.sel.id === l.id;
      ctx.beginPath(); ctx.moveTo(c[0], c[1]); ctx.bezierCurveTo(c[2], c[3], c[4], c[5], c[6], c[7]);
      ctx.lineWidth = (sel || hoverLink === l ? 3 : 1.8) / Math.max(k, 0.6);
      ctx.strokeStyle = sel ? C.accent : ctrl ? C.ctrl : C['line-2'];
      ctx.setLineDash(ctrl ? [6, 5] : []); ctx.globalAlpha = ctrl && !sel ? 0.7 : 1; ctx.stroke();
      ctx.setLineDash([]); ctx.globalAlpha = 1;
      // arrow
      ctx.fillStyle = ctx.strokeStyle; ctx.beginPath(); ctx.moveTo(c[6] - 8, c[7] - 4.5); ctx.lineTo(c[6] - 1, c[7]); ctx.lineTo(c[6] - 8, c[7] + 4.5); ctx.fill();
      // rate label
      if (!ctrl && k > 0.55 && l.rt.rate > 0.5) {
        const m = PL.bpt(c, 0.5), txt = fmt(l.rt.rate) + '/s';
        ctx.font = '500 10.5px ' + getComputedStyle(document.body).getPropertyValue('--f-mono');
        const w = ctx.measureText(txt).width + 10;
        ctx.fillStyle = C.panel; ctx.strokeStyle = C['line-2']; ctx.lineWidth = 1;
        roundRect(m[0] - w / 2, m[1] - 9, w, 18, 9); ctx.fill(); ctx.stroke();
        ctx.fillStyle = C.muted; ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillText(txt, m[0], m[1] + 0.5);
      }
    }
    // particles (dimmed while one record is being followed)
    const tracing = PL.trace && PL.trace.active;
    if (tracing) ctx.globalAlpha = 0.22;
    for (const p of S.particles) {
      if (!S.links.includes(p.l)) continue;
      const c = PL.bez(p.l), pt = PL.bpt(c, Math.min(1, p.p));
      if (p.real) {
        ctx.font = '600 11px ' + getComputedStyle(document.body).getPropertyValue('--f-mono');
        const w = ctx.measureText(p.label).width + 14;
        ctx.fillStyle = C.real; roundRect(pt[0] - w / 2, pt[1] - 10, w, 20, 10); ctx.fill();
        ctx.fillStyle = C.panel; ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillText(p.label, pt[0], pt[1] + 0.5);
      } else if (p.ctrl) {
        ctx.fillStyle = C.ctrl; ctx.save(); ctx.translate(pt[0], pt[1]); ctx.rotate(Math.PI / 4); ctx.fillRect(-5, -5, 10, 10); ctx.restore();
      } else {
        const r = 2.6 + Math.min(3.6, Math.log10(p.b.n + 1) * 0.95);
        ctx.fillStyle = C[p.col]; ctx.beginPath(); ctx.arc(pt[0], pt[1], r, 0, Math.PI * 2); ctx.fill();
        if (peek.hl === p) { ctx.strokeStyle = C.fg; ctx.lineWidth = 2 / Math.max(k, 0.6); ctx.beginPath(); ctx.arc(pt[0], pt[1], r + 4, 0, Math.PI * 2); ctx.stroke(); }
      }
    }
    ctx.globalAlpha = 1;
    if (tracing) PL.trace.draw(ctx, k, C);
    if (tempLink) {
      const a = PL.byId(tempLink.from); const x1 = a.x + PL.NW, y1 = a.y + PL.NH / 2, x2 = tempLink.x, y2 = tempLink.y, dx = Math.max(50, Math.abs(x2 - x1) * 0.5);
      ctx.beginPath(); ctx.moveTo(x1, y1); ctx.bezierCurveTo(x1 + dx, y1, x2 - dx, y2, x2, y2);
      ctx.strokeStyle = tempLink.bad ? C.bad : C.accent; ctx.lineWidth = 2 / Math.max(k, 0.6); ctx.setLineDash([5, 4]); ctx.stroke(); ctx.setLineDash([]);
    }
    ctx.restore();
  }
  function roundRect(x, y, w, h, r) { ctx.beginPath(); ctx.moveTo(x + r, y); ctx.arcTo(x + w, y, x + w, y + h, r); ctx.arcTo(x + w, y + h, x, y + h, r); ctx.arcTo(x, y + h, x, y, r); ctx.arcTo(x, y, x + w, y, r); ctx.closePath(); }

  function linkAt(wx, wy, px) {
    const tol = (px || 9) / S.view.k;
    for (const l of S.links) {
      const c = PL.bez(l);
      for (let i = 0; i <= 30; i++) { const p = PL.bpt(c, i / 30); if (Math.hypot(p[0] - wx, p[1] - wy) < tol) return l; }
    }
    return null;
  }
  let lastTap = { id: null, t: 0 };
  function particleAt(wx, wy, px) {
    const tol = px / S.view.k; let best = null, bd = tol;
    for (const p of S.particles) {
      if (!p.b || !S.links.includes(p.l)) continue;
      const pt = PL.bpt(PL.bez(p.l), Math.min(1, p.p)), d = Math.hypot(pt[0] - wx, pt[1] - wy);
      if (d < bd) { bd = d; best = p; }
    }
    return best;
  }
  function nodeAt(wx, wy) {
    for (let i = S.nodes.length - 1; i >= 0; i--) { const n = S.nodes[i]; if (wx >= n.x - 8 && wx <= n.x + PL.NW + 8 && wy >= n.y - 8 && wy <= n.y + PL.NH + 8) return n; }
    return null;
  }

  /* ---------- interaction ---------- */
  let drag = null; const pointers = new Map(); let pinch = null;
  stage.addEventListener('pointerdown', e => {
    if (e.target.closest('#palette, #inspector, #hud, #zoom, #dock, #tipcard, #assist, #tracewrap, #lookin, .rz')) return;
    const nodeEl = e.target.closest('.node');
    pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (pointers.size === 2) {
      const [a, b] = [...pointers.values()];
      pinch = { d: Math.hypot(a.x - b.x, a.y - b.y), k: S.view.k }; drag = null; return;
    }
    if (nodeEl) {
      const n = PL.byId(nodeEl.dataset.id);
      if (e.target.closest('.run-btn')) { e.stopPropagation(); PL.runFlow([n.id], 'Run from ' + n.name); openDock(true); return; }
      if (e.target.closest('.port.out')) {
        const [wx, wy] = toWorld(e.clientX, e.clientY);
        drag = { kind: 'link', from: n.id }; tempLink = { from: n.id, x: wx, y: wy };
      } else {
        const [wx, wy] = toWorld(e.clientX, e.clientY);
        drag = { kind: 'node', id: n.id, dx: wx - n.x, dy: wy - n.y, sx: e.clientX, sy: e.clientY, moved: false };
      }
    } else {
      drag = { kind: 'pan', sx: e.clientX, sy: e.clientY, vx: S.view.x, vy: S.view.y, moved: false };
    }
    stage.setPointerCapture(e.pointerId);
  });
  stage.addEventListener('pointermove', e => {
    if (pointers.has(e.pointerId)) pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (pinch && pointers.size === 2) {
      const [a, b] = [...pointers.values()]; const d = Math.hypot(a.x - b.x, a.y - b.y);
      zoomAt((pinch.k * d / pinch.d) / S.view.k, (a.x + b.x) / 2, (a.y + b.y) / 2); return;
    }
    const [wx, wy] = toWorld(e.clientX, e.clientY);
    if (!drag) { if (e.pointerType !== 'mouse') return; const l = !e.target.closest('.node') && linkAt(wx, wy); hoverLink = l || null; stage.style.cursor = l ? 'pointer' : ''; return; }
    if (drag.kind === 'pan') {
      if (Math.hypot(e.clientX - drag.sx, e.clientY - drag.sy) > 3) { drag.moved = true; stage.classList.add('panning'); }
      S.view.x = drag.vx + e.clientX - drag.sx; S.view.y = drag.vy + e.clientY - drag.sy; applyView();
    } else if (drag.kind === 'node') {
      if (Math.hypot(e.clientX - drag.sx, e.clientY - drag.sy) > 3) drag.moved = true;
      if (!drag.moved) return;
      const n = PL.byId(drag.id); n.x = Math.round((wx - drag.dx) / 8) * 8; n.y = Math.round((wy - drag.dy) / 8) * 8;
      const el = els.get(n.id); el.style.left = n.x + 'px'; el.style.top = n.y + 'px';
    } else if (drag.kind === 'link') {
      tempLink.x = wx; tempLink.y = wy;
      const target = nodeAt(wx, wy); const a = PL.byId(drag.from);
      els.forEach(el => el.classList.remove('drop-ok', 'drop-bad'));
      tempLink.bad = false;
      if (target && target !== a) { const err = PL.canConnect(a, target); els.get(target.id).classList.add(err ? 'drop-bad' : 'drop-ok'); tempLink.bad = !!err; }
    }
  });
  function endPointer(e) {
    pointers.delete(e.pointerId);
    if (pinch) { if (pointers.size < 2) pinch = null; saveSoon(); return; }
    if (!drag) return;
    const [wx, wy] = toWorld(e.clientX, e.clientY);
    if (drag.kind === 'pan') {
      stage.classList.remove('panning');
      if (!drag.moved) {
        const p = particleAt(wx, wy, e.pointerType === 'mouse' ? 9 : 20);
        if (p) { select({ kind: 'link', id: p.l.id }); peekParticle(p); }
        else { const l = linkAt(wx, wy, e.pointerType === 'mouse' ? 9 : 22); select(l ? { kind: 'link', id: l.id } : null); }
      }
      saveSoon();
    } else if (drag.kind === 'node') {
      if (!drag.moved) {
        const now = performance.now(), dbl = lastTap.id === drag.id && now - lastTap.t < 380;
        lastTap = { id: drag.id, t: now };
        if (dbl && PL.inside.has(PL.byId(drag.id))) PL.inside.open(drag.id); else select({ kind: 'node', id: drag.id });
      } else saveSoon();
    } else if (drag.kind === 'link') {
      els.forEach(el => el.classList.remove('drop-ok', 'drop-bad'));
      const target = nodeAt(wx, wy); const a = PL.byId(drag.from);
      if (target && target !== a) connect(a, target);
      tempLink = null;
    }
    drag = null;
  }
  stage.addEventListener('pointerup', endPointer);
  stage.addEventListener('pointercancel', endPointer);
  stage.addEventListener('wheel', e => {
    if (e.target.closest('#palette, #inspector, #dock, #tipcard, #assist, #tracewrap, #lookin')) return;
    e.preventDefault();
    const trackpadPan = !e.ctrlKey && e.deltaMode === 0 && (Math.abs(e.deltaX) > 0 || Math.abs(e.deltaY) < 40);
    if (trackpadPan) { S.view.x -= e.deltaX; S.view.y -= e.deltaY; applyView(); saveSoon(); }
    else zoomAt(Math.exp(-e.deltaY * (e.ctrlKey ? 0.01 : 0.0015)), e.clientX, e.clientY);
  }, { passive: false });

  function connect(a, b) {
    const err = PL.canConnect(a, b);
    if (err) { toast(err, true); return; }
    PL.addLink(a.id, b.id);
    if (PL.TYPES[a.type].control) {
      if (b.type === 'spark' && b.cfg.mode !== 'batch') { b.cfg.mode = 'batch'; toast(`${b.name} switched to Batch mode: an orchestrator starts batch jobs, streaming jobs run all the time.`); }
      if (b.type === 'snowflake' && b.cfg.ingest !== 'copy') { b.cfg.ingest = 'copy'; toast(`${b.name} now loads with scheduled COPY INTO.`); }
    }
    syncNodes(); refreshInspector(); saveSoon();
  }

  /* ---------- keyboard ---------- */
  document.addEventListener('keydown', e => {
    if (e.target.closest('input, textarea, select, [contenteditable]')) return;
    if (!$('#modal').hidden) { if (e.key === 'Escape') ui.closeModal(); return; }
    if (PL.trace.active) {
      if (e.key === 'ArrowRight' || e.key === 'Enter') { e.preventDefault(); PL.trace.next(); return; }
      if (e.key === 'ArrowLeft') { e.preventDefault(); PL.trace.prev(); return; }
      if (e.key === 'Escape') { PL.trace.stop(); return; }
    }
    if (e.key === 'Escape' && PL.inside.id) { PL.inside.close(); return; }
    if (e.key === '.') { stepSim(); return; }
    if (e.key === 't') { PL.trace.start(); return; }
    if (e.key === 'i' && S.sel && S.sel.kind === 'node') { PL.inside.open(S.sel.id); return; }
    if ((e.key === 'Delete' || e.key === 'Backspace') && S.sel) { e.preventDefault(); deleteSelected(); }
    else if (e.key === 'Escape') { select(null); closeMenus(); }
    else if (e.key === ' ') { e.preventDefault(); setRunning(!S.running); }
    else if (e.key === 'f') fit();
    else if (e.key === '+' || e.key === '=') zoomAt(1.2, innerWidth / 2, innerHeight / 2);
    else if (e.key === '-') zoomAt(1 / 1.2, innerWidth / 2, innerHeight / 2);
  });
  function deleteSelected() {
    if (!S.sel) return;
    if (S.sel.kind === 'node') { const n = PL.byId(S.sel.id); PL.removeNode(S.sel.id); delete RT.results[S.sel.id]; toast(`Removed ${n ? n.name : 'component'}`); }
    else PL.removeLink(S.sel.id);
    select(null); syncNodes(); renderDock(); saveSoon();
  }

  /* ---------- palette ---------- */
  function buildPalette(filter) {
    const f = (filter || '').toLowerCase().trim();
    let html = '';
    for (const cat of Object.keys(PL.CATS)) {
      const items = Object.keys(PL.TYPES).filter(t => PL.TYPES[t].cat === cat && (!f || (PL.TYPES[t].label + ' ' + PL.TYPES[t].sub + ' ' + t).toLowerCase().includes(f)));
      if (!items.length) continue;
      html += `<div class="pal-cat">${PL.CATS[cat].label}</div>`;
      for (const t of items) {
        const T = PL.TYPES[t];
        html += `<div class="pal-item" data-type="${t}" tabindex="0" role="button" aria-label="Add ${esc(T.label)}"><span class="ico" style="--c:${catColor(t)}">${icon(t)}</span><div><b>${esc(T.label)}</b><small>${esc(T.sub)}</small></div>${T.runnable ? '<span class="tag" title="Runs for real in the browser">runs</span>' : ''}</div>`;
      }
    }
    $('#palList').innerHTML = html || '<div class="dock-empty">No match.</div>';
  }
  $('#palSearch').addEventListener('input', e => buildPalette(e.target.value));
  let palDrag = null;
  $('#palList').addEventListener('pointerdown', e => {
    const it = e.target.closest('.pal-item'); if (!it) return;
    if (e.pointerType === 'mouse') { e.preventDefault(); it.setPointerCapture(e.pointerId); }
    palDrag = { type: it.dataset.type, sx: e.clientX, sy: e.clientY, ghost: null, touch: e.pointerType !== 'mouse' };
  });
  $('#palList').addEventListener('pointermove', e => {
    if (!palDrag) return;
    const dx = Math.abs(e.clientX - palDrag.sx), dy = Math.abs(e.clientY - palDrag.sy);
    // on touch, vertical movement scrolls the list (the browser cancels the pointer); a sideways drag picks the item up
    if (!palDrag.ghost && (palDrag.touch ? dx > 10 && dx > dy : Math.hypot(dx, dy) > 5)) {
      const g = document.createElement('div'); g.className = 'ghost-node';
      const T = PL.TYPES[palDrag.type];
      g.innerHTML = `<div class="node" style="transform:scale(${S.view.k});transform-origin:center"><div class="nhead"><span class="ico" style="--c:${catColor(palDrag.type)}">${icon(palDrag.type)}</span><div class="ntitle"><b>${esc(T.label)}</b><small>${esc(T.sub)}</small></div></div></div>`;
      document.body.appendChild(g); palDrag.ghost = g;
      if (innerWidth < 760) $('#palette').classList.remove('open');
    }
    if (palDrag.ghost) { palDrag.ghost.style.left = e.clientX + 'px'; palDrag.ghost.style.top = e.clientY + 'px'; }
  });
  $('#palList').addEventListener('pointerup', e => {
    if (!palDrag) return;
    const t = palDrag.type;
    if (palDrag.ghost) {
      palDrag.ghost.remove();
      const r = stage.getBoundingClientRect(); const pal = $('#palette').getBoundingClientRect();
      const overPal = innerWidth >= 760 && e.clientX < pal.right && e.clientY > pal.top;
      if (e.clientX > r.left && e.clientY > r.top && e.clientY < r.bottom && !overPal) {
        const [wx, wy] = toWorld(e.clientX, e.clientY); addAndSelect(t, wx - PL.NW / 2, wy - PL.NH / 2);
      }
    } else addAtCenter(t);
    palDrag = null;
  });
  $('#palList').addEventListener('pointercancel', () => { if (palDrag && palDrag.ghost) palDrag.ghost.remove(); palDrag = null; });
  $('#palList').addEventListener('keydown', e => { const it = e.target.closest('.pal-item'); if (it && (e.key === 'Enter' || e.key === ' ')) { e.preventDefault(); addAtCenter(it.dataset.type); } });
  function addAtCenter(t) {
    const r = stage.getBoundingClientRect();
    const [wx, wy] = toWorld(r.left + r.width / 2 + (Math.random() * 60 - 30), r.top + r.height / 2 + (Math.random() * 60 - 30));
    addAndSelect(t, wx - PL.NW / 2, wy - PL.NH / 2);
    if (innerWidth < 760) $('#palette').classList.remove('open');
  }
  function addAndSelect(t, x, y) {
    const n = PL.addNode(t, Math.round(x / 8) * 8, Math.round(y / 8) * 8);
    let k = 2; while (S.nodes.some(o => o !== n && o.name === n.name)) n.name = PL.TYPES[t].label + ' ' + k++;
    syncNodes(); select({ kind: 'node', id: n.id }); saveSoon();
  }
  $('#palToggle').addEventListener('click', () => $('#palette').classList.toggle('open'));

  /* ---------- selection & inspector ---------- */
  let insTab = 'cfg';
  function select(sel) {
    S.sel = sel; syncNodes();
    const ins = $('#inspector');
    if (!sel) { ins.hidden = true; stage.classList.remove('ins-open'); return; }
    ins.hidden = false; stage.classList.add('ins-open');
    if (sel.kind === 'node' && innerWidth < 760) $('#palette').classList.remove('open');
    buildInspector();
  }
  ui.select = select;
  function refreshInspector() { if (S.sel) buildInspector(true); }
  ui.refreshInspector = refreshInspector;

  function buildInspector(keepScroll) {
    const ins = $('#inspector');
    const body = ins.querySelector('.ins-body'); const st = body ? body.scrollTop : 0;
    if (S.sel.kind === 'link') { buildLinkInspector(); return; }
    const n = PL.byId(S.sel.id); if (!n) { select(null); return; }
    const T = PL.TYPES[n.type];
    ins.innerHTML = `
      <div class="ins-head"><span class="ico" style="--c:${catColor(n.type)}">${icon(n.type)}</span>
        <div class="ins-title"><input id="insName" value="${esc(n.name)}" aria-label="Name"><small>${esc(T.label)} · ${esc(PL.CATS[T.cat].label)}</small></div>
        <button class="btn icon ghost" id="insClose" aria-label="Close"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M6 6l12 12M18 6L6 18"/></svg></button></div>
      <div class="tabs"><button data-tab="cfg" class="${insTab === 'cfg' ? 'on' : ''}">Configure</button><button data-tab="learn" class="${insTab === 'learn' ? 'on' : ''}">Learn</button></div>
      <div class="ins-body">${exploreRow(n)}${insTab === 'cfg' ? cfgTab(n) : learnTab(n)}</div>`;
    ins.querySelector('#insClose').onclick = () => select(null);
    const li = ins.querySelector('#insInside'); if (li) li.onclick = () => PL.inside.open(n.id);
    const tr = ins.querySelector('#insTrace'); if (tr) tr.onclick = () => PL.trace.start(n);
    ins.querySelector('#insName').oninput = e => { n.name = e.target.value || T.label; syncNodes(); renderDock(); saveSoon(); };
    ins.querySelectorAll('.tabs button').forEach(b => b.onclick = () => { insTab = b.dataset.tab; buildInspector(); });
    if (insTab === 'cfg') wireControls(n);
    if (keepScroll) ins.querySelector('.ins-body').scrollTop = st;
    updateLive();
  }
  const IC_INSIDE = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="10.5" cy="10.5" r="6.5"/><path d="M15.5 15.5L20 20"/><path d="M8 10.5h5M10.5 8v5"/></svg>';
  const IC_TRACE = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="5" cy="18" r="2.5"/><path d="M7.5 18H14a4 4 0 0 0 0-8h-4a3.5 3.5 0 0 1 0-7h9"/><path d="M16.5 0.5L19 3l-2.5 2.5"/></svg>';
  ui.IC_TRACE = IC_TRACE;
  function exploreRow(n) {
    const T = PL.TYPES[n.type];
    const a = PL.inside.has(n) ? `<button class="btn" id="insInside" title="Animated view of how it works inside (double-click the component, or press i)">${IC_INSIDE}Look inside</button>` : '';
    const b = !T.control && (PL.REC.isSource(n) ? PL.outLinks(n).length : PL.upstream(n).length) ? `<button class="btn" id="insTrace" title="Step through one record, hop by hop (t)">${IC_TRACE}Follow a record</button>` : '';
    return a || b ? `<div class="ins-foot explore">${a}${b}</div>` : '';
  }
  function cfgTab(n) {
    const T = PL.TYPES[n.type];
    const runnable = T.runnable && !T.control;
    let html = '';
    if (runnable) html += `<div class="ins-foot"><button class="btn primary" id="insRun"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M7 4.5v15l12.5-7.5z"/></svg>Run from here</button>${RT.results[n.id] ? '<button class="btn" id="insShow">Show result</button>' : ''}</div>`;
    html += `<section><div class="sec-title">Live</div><div id="insLive"></div></section>`;
    html += `<section><div class="sec-title">Settings</div><div style="display:flex;flex-direction:column;gap:14px">`;
    for (const f of T.cfg) {
      if (f.show && !f.show(n)) continue;
      const v = n.cfg[f.k]; const id = 'cfg-' + f.k;
      const hint = f.hint ? `<div class="hint">${esc(f.hint)}</div>` : '';
      if (f.type === 'range') html += `<div class="field"><div class="field-row"><label for="${id}">${esc(f.label)}</label><output id="${id}-o">${v}${f.unit ? ' ' + f.unit : ''}</output></div><input type="range" id="${id}" data-k="${f.k}" min="${f.min}" max="${f.max}" step="${f.step}" value="${v}">${hint}</div>`;
      else if (f.type === 'seg') html += `<div class="field"><label>${esc(f.label)}</label><div class="seg" data-k="${f.k}" role="group" aria-label="${esc(f.label)}">${f.opts.map(o => `<button data-v="${o[0]}" class="${String(v) === o[0] ? 'on' : ''}">${esc(o[1])}</button>`).join('')}</div>${hint}</div>`;
      else if (f.type === 'toggle') html += `<div class="field"><div class="switch"><label for="${id}">${esc(f.label)}</label><input type="checkbox" id="${id}" data-k="${f.k}" ${v ? 'checked' : ''}></div>${hint}</div>`;
      else if (f.type === 'select') html += `<div class="field"><label for="${id}">${esc(f.label)}</label><select id="${id}" data-k="${f.k}">${f.opts.map(o => `<option value="${o[0]}" ${v === o[0] ? 'selected' : ''}>${esc(o[1])}</option>`).join('')}</select>${hint}</div>`;
      else if (f.type === 'text') html += `<div class="field"><label for="${id}">${esc(f.label)}</label><input type="text" id="${id}" data-k="${f.k}" value="${esc(v)}" spellcheck="false" autocomplete="off">${hint}</div>`;
      else if (f.type === 'code') html += `<div class="field"><div class="field-row"><label for="${id}">${esc(f.label)}</label><button class="btn ghost" id="codeReset" style="height:24px;font-size:12px">Reset</button></div><textarea id="${id}" data-k="${f.k}" spellcheck="false">${esc(v)}</textarea></div>`;
      else if (f.type === 'file') html += `<div class="field"><label for="${id}">${esc(f.label)}</label><input type="file" id="${id}" accept=".csv,.json,.jsonl,.ndjson,.txt"><div class="hint">${n.cfg.fileName ? 'Loaded: ' + esc(n.cfg.fileName) + '. ' : ''}${esc(f.hint)}</div></div>`;
    }
    html += `</div></section>`;
    if (n.type === 'snowflake') html += `<div class="note">Snowflake is SaaS and can’t run in docker. The export uses a local Postgres “warehouse” instead, or your real account if you enter it when the setup script asks.</div>`;
    html += `<div class="ins-foot"><button class="btn danger" id="insDel">Remove component</button><button class="btn" id="insDup">Duplicate</button></div>`;
    return html;
  }
  function learnTab(n) {
    const th = PL.THEORY[n.type]; if (!th) return '<p>No notes yet.</p>';
    const list = a => `<ul>${a.map(x => `<li>${esc(x)}</li>`).join('')}</ul>`;
    return `<div class="learn" style="display:flex;flex-direction:column;gap:16px">
      <p>${esc(th.what)}</p>
      <div><h4>How it works</h4>${list(th.how)}</div>
      <div><h4>Trade-offs</h4>${list(th.tradeoffs)}</div>
      <div><h4>Used for</h4>${list(th.uses)}</div>
      ${th.try ? `<div class="try"><h4>Try it here</h4><p>${esc(th.try)}</p></div>` : ''}
      ${th.interview && th.interview.length ? `<div><h4>Interview questions</h4>${list(th.interview)}</div>` : ''}
    </div>`;
  }
  function setCfg(n, k, v) {
    const old = n.cfg[k]; n.cfg[k] = v;
    if (n.type === 'api' && k === 'preset' && PL.API_PRESETS[v]) { Object.assign(n.cfg, PL.API_PRESETS[v]); }
    if (n.type === 'api' && (k === 'url' || k === 'path')) { n.cfg.preset = Object.keys(PL.API_PRESETS).find(p => PL.API_PRESETS[p].url === n.cfg.url && PL.API_PRESETS[p].path === n.cfg.path) || 'custom'; }
    if (n.type === 'script' && k === 'lang' && n.cfg.code.trim() === PL.DEFAULT_CODE[old].trim()) n.cfg.code = PL.DEFAULT_CODE[v];
    syncNodes(); saveSoon();
    const f = PL.TYPES[n.type].cfg.find(c => c.k === k);
    if (f.type === 'seg' || f.type === 'toggle' || f.type === 'select' || PL.TYPES[n.type].cfg.some(c => c.show)) {
      if (f.type !== 'text' && f.type !== 'code' && f.type !== 'range') buildInspector(true);
    }
  }
  function wireControls(n) {
    const ins = $('#inspector');
    ins.querySelectorAll('input[type=range]').forEach(i => i.oninput = () => {
      const f = PL.TYPES[n.type].cfg.find(c => c.k === i.dataset.k);
      ins.querySelector('#' + i.id + '-o').textContent = i.value + (f.unit ? ' ' + f.unit : '');
      setCfg(n, i.dataset.k, +i.value);
    });
    ins.querySelectorAll('.seg[data-k]').forEach(s => s.onclick = e => { const b = e.target.closest('button'); if (b) setCfg(n, s.dataset.k, b.dataset.v); });
    ins.querySelectorAll('input[type=checkbox][data-k]').forEach(i => i.onchange = () => setCfg(n, i.dataset.k, i.checked));
    ins.querySelectorAll('select[data-k]').forEach(i => i.onchange = () => setCfg(n, i.dataset.k, i.value));
    ins.querySelectorAll('input[type=text][data-k]').forEach(i => { i.oninput = () => { n.cfg[i.dataset.k] = i.value; i.dataset.dirty = 1; syncNodes(); saveSoon(); }; i.onchange = () => setCfg(n, i.dataset.k, i.value); });
    const ta = ins.querySelector('textarea[data-k]');
    if (ta) {
      ta.oninput = () => { n.cfg.code = ta.value; saveSoon(); };
      ta.onkeydown = e => { if (e.key === 'Tab') { e.preventDefault(); const s = ta.selectionStart; ta.setRangeText('    ', s, ta.selectionEnd, 'end'); n.cfg.code = ta.value; } if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); PL.runFlow([n.id], 'Run from ' + n.name); openDock(true); } };
      ins.querySelector('#codeReset').onclick = () => { n.cfg.code = PL.DEFAULT_CODE[n.cfg.lang]; buildInspector(true); saveSoon(); };
    }
    const fi = ins.querySelector('input[type=file]');
    if (fi) fi.onchange = () => {
      const file = fi.files[0]; if (!file) return;
      const rd = new FileReader();
      rd.onload = () => { n.cfg.fileText = rd.result; n.cfg.fileName = file.name; toast(`Loaded ${file.name} (${fmtB(file.size)}). Press Run flow.`); syncNodes(); buildInspector(true); saveSoon(); };
      rd.readAsText(file);
    };
    const run = ins.querySelector('#insRun'); if (run) run.onclick = () => { PL.runFlow([n.id], 'Run from ' + n.name); openDock(true); };
    const sh = ins.querySelector('#insShow'); if (sh) sh.onclick = () => ui.showResult(n.id);
    ins.querySelector('#insDel').onclick = deleteSelected;
    ins.querySelector('#insDup').onclick = () => {
      const c = PL.addNode(n.type, n.x + 40, n.y + 120, JSON.parse(JSON.stringify(n.cfg)), null, n.name + ' copy');
      syncNodes(); select({ kind: 'node', id: c.id }); saveSoon();
    };
  }
  function buildLinkInspector() {
    const l = S.links.find(x => x.id === S.sel.id); if (!l) { select(null); return; }
    const a = PL.byId(l.from), b = PL.byId(l.to), ctrl = PL.isControlLink(l);
    $('#inspector').innerHTML = `
      <div class="ins-head"><span class="ico" style="--c:${ctrl ? 'var(--ctrl)' : 'var(--line-2)'}"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M4 12h12M12 7l5 5-5 5"/></svg></span>
        <div class="ins-title"><b style="font-size:16px">${esc(a.name)} → ${esc(b.name)}</b><small>${ctrl ? 'Trigger link' : 'Data link'}</small></div>
        <button class="btn icon ghost" id="insClose" aria-label="Close"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M6 6l12 12M18 6L6 18"/></svg></button></div>
      <div class="ins-body">
        <section><div class="sec-title">What flows here</div><p style="margin:0">${esc(PL.LINK_TEXT[a.type] || '')}</p></section>
        ${ctrl ? '' : '<section><div class="sec-title">Live</div><div id="insLive"></div></section>'}
        <section class="peek"><div class="peek-head"><div class="sec-title" style="margin:0">${ctrl ? 'Trigger messages' : 'Data crossing this link'}</div><span class="spacer"></span><button class="btn ghost" id="peekFreeze" style="height:26px;font-size:12px"></button></div>
          <div class="peek-hint">${ctrl ? 'Each purple diamond is one of these.' : 'Every dot on the link is a batch of records like these. Click a dot on the canvas, or a row below.'}</div>
          <div id="peekList" class="peek-list"></div><div id="peekDetail" class="pv"></div></section>
        <div class="ins-foot"><button class="btn danger" id="insDel">Remove link</button></div>
      </div>`;
    $('#insClose').onclick = () => select(null);
    $('#insDel').onclick = deleteSelected;
    $('#peekFreeze').onclick = () => { peek.frozen = !peek.frozen; renderPeek(); };
    $('#peekList').onclick = e => { const r = e.target.closest('[data-pk]'); if (!r) return; peek.selId = +r.dataset.pk; peek.hl = null; renderPeek(); };
    if (peek.linkId !== l.id) { peek = { linkId: l.id, items: [], selId: null, frozen: false, last: 0, seq: peek.seq || 0, hl: null }; prefillPeek(l); }
    renderPeek();
    updateLive();
  }

  /* ---------- data inspector on links ---------- */
  let peek = { linkId: null, items: [], selId: null, frozen: false, last: 0, seq: 0, hl: null };
  const OPCOL = { ins: 'c', upd: 'u', del: 'd' };
  function addPeek(l, op, pin) {
    const rec = PL.REC.sampleOnLink(l, op); if (!rec) return null;
    const it = { id: ++peek.seq, rec, at: S.t, op: rec.op || op };
    peek.items.unshift(it); if (peek.items.length > 8) peek.items.length = 8;
    if (pin) peek.selId = it.id;
    return it;
  }
  function prefillPeek(l) {
    const a = PL.byId(l.from), src = PL.REC.pathTo(a)[0];
    const mix = src && src.cfg.mix ? { ins: [0.08, 0.02], mix: [0.35, 0.08], upd: [0.7, 0.05] }[src.cfg.mix] : [0, 0];
    const ops = PL.isControlLink(l) ? ['c'] : ['c', mix[0] > 0.1 ? 'u' : 'c', mix[1] > 0.04 ? 'd' : 'u'];
    ops.forEach(o => addPeek(l, o));
  }
  PL.onArrive = function (p) {
    if (!S.sel || S.sel.kind !== 'link' || S.sel.id !== p.l.id || peek.frozen || peek.linkId !== p.l.id) return;
    const now = performance.now(); if (now - peek.last < 700) return; peek.last = now;
    addPeek(p.l, p.ctrl ? 'c' : OPCOL[p.col] || 'c'); renderPeek();
  };
  function peekParticle(p) {
    if (peek.linkId !== p.l.id) { peek = { linkId: p.l.id, items: [], selId: null, frozen: false, last: 0, seq: peek.seq || 0, hl: null }; }
    const it = addPeek(p.l, OPCOL[p.col] || 'c', true);
    peek.frozen = true; peek.hl = p;
    if (it) it.batch = p.b.n;
    renderPeek();
  }
  function renderPeek() {
    const list = $('#peekList'); if (!list) return;
    const fz = $('#peekFreeze'); fz.textContent = peek.frozen ? 'Resume feed' : 'Pause feed';
    const live = !peek.frozen && S.running;
    const OPN = { c: 'insert', u: 'update', d: 'delete', r: 'read' };
    const cls = { c: 'ins', u: 'upd', d: 'del' };
    list.innerHTML = peek.items.length ? peek.items.map((it, i) => `<button class="peek-row ${(peek.selId ? it.id === peek.selId : i === 0) ? 'on' : ''}" data-pk="${it.id}"><i class="${it.rec.fmt === 'trigger' ? 'ctrl' : it.rec.fmt === 'dropped' ? 'del' : cls[it.op] || 'ins'}"></i><span class="op">${it.rec.fmt === 'trigger' ? 'trigger' : it.rec.fmt === 'dropped' ? 'lost' : OPN[it.op] || 'record'}</span><span class="sum">${esc(PL.REC.short(it.rec))}</span><span class="t">T+${fmtS(it.at)}</span></button>`).join('')
      : `<div class="note">Nothing has crossed yet. ${S.running ? '' : 'The simulation is paused.'}</div>`;
    const it = peek.items.find(x => x.id === peek.selId) || peek.items[0];
    $('#peekDetail').innerHTML = it ? `${it.batch ? `<div class="peek-batch">This dot carries ${fmt(it.batch)} records. One of them:</div>` : ''}${PL.REC.view(it.rec)}` : '';
    list.classList.toggle('live', live);
  }

  /* ---------- live inspector content ---------- */
  function metric(label, val) { return `<div class="metric"><small>${label}</small><b>${val}</b></div>`; }
  function updateLive() {
    const box = $('#insLive'); if (!box || !S.sel) return;
    if (S.sel.kind === 'link') {
      const l = S.links.find(x => x.id === S.sel.id); if (!l) return;
      const inflight = S.particles.filter(p => p.l === l && p.b).reduce((a, p) => a + p.b.n, 0);
      box.innerHTML = `<div class="metrics">${metric('Throughput', fmt(l.rt.rate) + '/s')}${metric('In flight', fmt(inflight))}${metric('Batches', S.particles.filter(p => p.l === l).length)}</div>`;
      return;
    }
    const n = PL.byId(S.sel.id); if (!n) return;
    const r = n.rt, T = PL.TYPES[n.type];
    let html = '';
    const lag = r.qN / Math.max(r.cap || 1, 1);
    const source = ['postgres', 'app', 'api', 'file'].includes(n.type);
    if (!T.control && !['table', 'bi'].includes(n.type)) {
      html += `<div class="metrics">` + (source ? metric('Out', fmt(r.outRate) + '/s') + metric('Total', fmt(r.events || r.outRate * S.t)) + metric('Sim time', fmtS(S.t))
        : metric('In', fmt(r.inRate) + '/s') + metric('Out', fmt(r.outRate) + '/s') + metric(n.type === 'kafka' ? 'Write capacity' : 'Capacity', r.cap >= 1e8 ? '∞' : fmt(r.cap) + '/s')
          + metric(n.type === 'kafka' ? 'Unwritten' : 'Backlog', fmt(r.qN)) + metric('Lag', isBatchy(n) ? '–' : fmtS(lag)) + metric('Busy', Math.round(r.util * 100) + '%')) + `</div>`;
      if (!source) html += `<div class="util"><i style="width:${Math.min(100, r.util * 100)}%"></i></div>`;
    }
    html += vizFor(n);
    box.innerHTML = html;
    const sb = box.querySelector('[data-show]'); if (sb) sb.onclick = () => ui.showResult(sb.dataset.show);
  }

  /* ---------- per-component visuals ---------- */
  let lsn = 0x16B3A28;
  const walCache = {};
  function walLines(n, kind) {
    const r = n.rt, c = walCache[n.id] = walCache[n.id] || [];
    const [u, d] = { ins: [0.08, 0.02], mix: [0.35, 0.08], upd: [0.7, 0.05] }[n.cfg.mix || 'mix'] || [0, 0];
    const src = kind === 'cdc' ? (PL.upstream(n).find(x => x.type === 'postgres') || n) : n;
    const mix = src.cfg.mix ? { ins: [0.08, 0.02], mix: [0.35, 0.08], upd: [0.7, 0.05] }[src.cfg.mix] : [u, d];
    const add = n.cfg.down || (kind === 'cdc' && r.inRate < 1) ? 0 : 2;
    for (let i = 0; i < add; i++) {
      const x = Math.random(); const op = x < mix[1] ? 'd' : x < mix[1] + mix[0] ? 'u' : 'i';
      const id = 1000 + Math.floor(Math.random() * 9000); lsn += 40 + Math.floor(Math.random() * 400);
      const L = '0/' + lsn.toString(16).toUpperCase();
      if (kind === 'wal') c.push(`<span class="${op}">${L} ${{ i: 'INSERT', u: 'UPDATE', d: 'DELETE' }[op]}</span> public.orders id=${id}`);
      else if (kind === 'cdc') {
        if (n.cfg.mode === 'poll' && op === 'd') continue;
        c.push(`<span class="${op}">op=${{ i: 'c', u: 'u', d: 'd' }[op]}</span> key=${id} ${n.cfg.mode === 'log' ? 'lsn=' + L : 'updated_at>…'} → topic orders`);
      } else c.push(`<span class="i">{"user":"u${id}","event":"${['page_view', 'click', 'add_to_cart', 'search'][id % 4]}","ts":${Date.now()}}</span>`);
    }
    while (c.length > 6) c.shift();
    return `<div class="loglines">${c.join('<br>') || '<span style="color:var(--faint)">waiting for changes…</span>'}</div>`;
  }
  function coresViz(n) {
    const r = n.rt; const total = r.totalCores || 1, used = Math.min(total, r.used || total);
    const busy = Math.round(Math.min(1, r.util * 1.15) * used);
    const per = n.type === 'spark' ? +n.cfg.cores : 1, ex = n.type === 'spark' ? +n.cfg.executors : +n.cfg.par;
    let i = 0, html = '<div class="cores">';
    for (let e = 0; e < ex; e++) { html += `<div class="exec" title="${n.type === 'spark' ? 'Executor ' + (e + 1) : 'Task slot ' + (e + 1)}">`; for (let c = 0; c < per; c++, i++) html += `<span class="core ${i < busy ? 'busy' : i >= used ? 'starved' : ''}"></span>`; html += '</div>'; }
    html += '</div>';
    if (r.limitedBy) html += `<div class="note warn">Only ${r.limitedBy} of ${total} ${n.type === 'spark' ? 'cores' : 'slots'} can work: the Kafka topic has ${r.limitedBy} partitions, and each partition is read by one task. Striped boxes sit idle.</div>`;
    return html;
  }
  ui.runsHTML = runs => runsViz(runs);
  function runsViz(runs, isCtl) {
    if (!runs.length) return '<div class="note">No runs yet.</div>';
    return '<div class="runs">' + runs.map(x => {
      if (isCtl) {
        const tasks = Object.entries(x.tasks).map(([id, t]) => { const nn = PL.byId(id); const cls = t.state === 'failed' ? 'badc' : t.state === 'skipped' ? 'warnc' : t.state === 'running' && S.t < t.end ? '' : 'okc'; const lbl = t.state === 'running' ? (S.t < t.end ? 'running' : '✓ ' + fmtS(t.dur)) : t.state; return `<span class="${cls}">${esc(nn ? nn.name : '?')} ${lbl}</span>`; }).join(' · ');
        return `<div><span>#${x.no}</span><span>T+${fmtS(x.t)}</span>${tasks}</div>`;
      }
      return `<div><span class="${x.status === 'ok' ? 'okc' : x.status === 'failed' ? 'badc' : 'warnc'}">${x.status === 'ok' ? '✓' : x.status}</span><span>T+${fmtS(x.t)}</span><span>${fmt(x.n)} rows</span><span>${x.status === 'ok' ? fmtS(x.dur) : ''}</span></div>`;
    }).join('') + '</div>';
  }
  function nextBar(n, every) {
    const left = nextRunIn(n); if (left == null) return '';
    return `<div class="field-row" style="font-size:12px;color:var(--muted)"><span>Next run</span><span class="mono">${fmtS(left)}</span></div><div class="bar2"><i style="width:${100 - Math.min(100, left / every * 100)}%"></i></div>`;
  }
  function sparkSvg(vals, maxHint) {
    const v = vals.filter(x => x != null); if (v.length < 2) return '';
    const max = Math.max(maxHint || 1, ...v) * 1.1, w = 300, h = 56;
    const pts = vals.map((x, i) => [i / (vals.length - 1) * w, x == null ? h : h - (x / max) * (h - 4)]);
    const d = pts.map((p, i) => (i ? 'L' : 'M') + p[0].toFixed(1) + ' ' + p[1].toFixed(1)).join(' ');
    return `<svg class="spark-svg" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><path d="${d} L${w} ${h} L0 ${h}Z" fill="var(--accent-soft)"/><path d="${d}" fill="none" stroke="var(--accent)" stroke-width="1.6" vector-effect="non-scaling-stroke"/></svg><div class="field-row" style="font-size:11px;color:var(--faint)"><span>last ${fmtS(vals.length / 2)}</span><span>max ${fmtS(max / 1.1)}</span></div>`;
  }
  function resultPreview(n) {
    const res = RT.results[n.id];
    if (!res) return `<div class="note">Not run yet. Press Run from here or Run flow in the top bar.</div>`;
    if (res.error) return `<div class="note bad">${esc(res.error)}</div>`;
    const rows = res.rows.slice(0, 4); const cols = Object.keys(rows[0] || {}).slice(0, 4);
    return `<div class="note">Last run: ${res.rows.length} rows in ${res.ms} ms, ${res.at.toLocaleTimeString()}.</div>
      ${cols.length ? `<div style="overflow-x:auto"><table class="mini-table"><tr>${cols.map(c => `<th>${esc(c)}</th>`).join('')}</tr>${rows.map(r => `<tr>${cols.map(c => `<td>${esc(String(r[c] ?? '').slice(0, 24))}</td>`).join('')}</tr>`).join('')}</table></div>` : ''}
      <button class="btn" data-show="${n.id}">Open in Runtime panel</button>`;
  }
  function vizFor(n) {
    const r = n.rt; let h = '<div class="viz" style="margin-top:12px">';
    switch (n.type) {
      case 'api': case 'file': case 'script': case 'table': h += resultPreview(n); break;
      case 'schedule':
        h += n.cfg.on ? `<div class="note">Real runs every ${n.cfg.every}s while this tab is open. The simulation ticks on the same interval.</div>` : `<div class="note">Switch “Schedule on” to run the linked steps for real every ${n.cfg.every}s.</div>`;
        h += nextBar(n, +n.cfg.every) + runsViz(r.runs, true); break;
      case 'postgres': h += `<div class="sec-title">Write-ahead log (tail)</div>` + walLines(n, 'wal'); break;
      case 'app': h += `<div class="sec-title">Events (sample)</div>` + walLines(n, 'app'); break;
      case 'cdc':
        h += `<div class="sec-title">${n.cfg.mode === 'log' ? 'Change events read from the WAL' : 'Rows found by polling'}</div>` + walLines(n, 'cdc');
        if (n.cfg.mode === 'poll') h += `<div class="note warn">Polling can’t see deletes, and an update between polls overwrites the previous one. Every downstream table is now missing history.</div>`;
        if (n.cfg.snapshot) h += `<div class="note">Snapshot in progress: ${fmt(r.qN)} rows left.</div>`;
        break;
      case 'kafka': {
        const p = +n.cfg.partitions, dead = n.cfg.brokerDown, rf = +n.cfg.rf;
        const consumersLag = downstreamLag(n);
        h += `<div class="sec-title">Brokers</div><div class="brokers">${[1, 2, 3].map(b => `<div class="broker ${dead && b === 3 ? 'dead' : ''}">broker ${b}<br><span class="mono" style="font-size:11px">${Math.ceil(p / 3) - (b > (p % 3 || 3) ? 1 : 0)} leaders</span></div>`).join('')}</div>`;
        if (dead) h += rf === 1 ? `<div class="note bad">Broker 3 is down and partitions have no replicas. Partitions led by broker 3 are offline: producers for those keys fail and consumers can’t read them.</div>` : `<div class="note warn">Broker 3 is down. Followers on other brokers were elected leader${S.t - (r.downAt || 0) < 2.5 ? ' (election in progress, writes paused)' : ''}. No acknowledged data was lost.</div>`;
        h += `<div class="sec-title" style="margin-top:6px">Partitions · rate and consumer lag</div>`;
        const max = Math.max(1, ...r.parts.map(x => x.rate));
        r.parts.forEach((pt, i) => {
          const off = dead && rf === 1 && i % 3 === 2; const share = PL.partShare(n, i);
          h += `<div class="prow ${off ? 'off' : ''}"><span>P${i}</span><div class="ptrack"><i style="width:${pt.rate / max * 100}%"></i>${consumersLag > 0 ? `<em style="left:${Math.min(98, pt.rate / max * 100)}%;width:${Math.min(40, consumersLag * share / Math.max(1, max) * 20)}%"></em>` : ''}</div><span>${fmt(pt.end)} · lag ${fmt(consumersLag * share)}</span></div>`;
        });
        if (n.cfg.skew) h += `<div class="note warn">Partition 0 gets half of all records. Its consumer becomes the bottleneck however many partitions you add.</div>`;
        break;
      }
      case 'spark':
        h += `<div class="sec-title">Executors × cores</div>` + coresViz(n);
        if (n.cfg.mode === 'stream') h += r.overrun ? `<div class="note warn">Micro-batches take longer than the ${n.cfg.trigger}s trigger, so the next batch starts late and lag grows.</div>` : `<div class="note">Micro-batch #${r.runNo} took ${fmtS(r.lastDur)}. Trigger every ${n.cfg.trigger}s.</div>`;
        else h += nextBar(n, +n.cfg.every) + runsViz(r.runs);
        if (PL.readFactor(n) < 0.9) h += `<div class="note warn">Reading is ${Math.round((1 - PL.readFactor(n)) * 100)}% slower because of the upstream file layout (format, small files or delete files).</div>`;
        break;
      case 'flink': {
        const ck = +n.cfg.ckpt; const prog = (S.t % ck) / ck;
        h += `<div class="sec-title">Task slots</div>` + coresViz(n);
        h += `<div class="field-row" style="font-size:12px;color:var(--muted)"><span>Next checkpoint</span><span class="mono">${fmtS(ck - S.t % ck)}</span></div><div class="bar2"><i style="width:${prog * 100}%"></i></div>`;
        if (n.cfg.op === 'window') { const wp = (S.t % 10) / 10; h += `<div class="field-row" style="font-size:12px;color:var(--muted)"><span>Window closes, fires after +${n.cfg.watermark}s watermark</span><span class="mono">${fmtS(10 - S.t % 10)}</span></div><div class="bar2"><i style="width:${wp * 100}%;background:var(--c-process)"></i></div>`; }
        h += `<div class="note">${n.cfg.eos ? `Output is committed when a checkpoint completes, so downstream sees results up to ${ck}s later.` : 'Output goes out immediately. After a failure, records since the last checkpoint are written again (at-least-once).'}</div>`;
        break;
      }
      case 'airflow': h += `<div class="sec-title">DAG runs</div>` + nextBar(n, +n.cfg.interval) + runsViz(r.runs, true); break;
      case 's3': {
        const avg = r.files ? (r.bytes || 0) / r.files : 0;
        h += `<div class="metrics">${metric('Objects', fmt(r.files))}${metric('Size', fmtB(r.bytes || 0))}${metric('Avg file', fmtB(avg))}</div>`;
        h += `<div class="note">${n.cfg.format === 'parquet' ? 'Parquet: columnar, compressed, with min/max stats per row group.' : n.cfg.format.toUpperCase() + ' is row text: readers must parse every byte of every file. Downstream reads are slower.'} Files roll every 2 s, so they stay small. A compaction job would merge them.</div>`;
        break;
      }
      case 'iceberg': {
        const rf = PL.icebergRead(n);
        h += `<div class="sec-title">Snapshots (newest first)</div><div class="snaps">${r.snapshots.map(s => `<span class="snap ${s.op}" title="${s.op}, ${fmt(s.n)} rows">${s.id} ${s.op}</span>`).join('') || '<span class="note">no commits yet</span>'}</div>`;
        h += `<div class="metrics">${metric('Data files', r.files)}${metric('Delete files', r.deleteFiles)}${metric('Read speed', Math.round(rf * 100) + '%')}</div>`;
        if (rf < 0.7) h += `<div class="note warn">Readers must open many small files${r.deleteFiles ? ' and merge delete files' : ''}. ${n.cfg.compaction ? 'Compaction runs every 20 s.' : 'Switch compaction on.'}</div>`;
        if (n.cfg.mode === 'cow' && (r.ud || 0) > 0.2) h += `<div class="note warn">Copy-on-write rewrites whole files for each update batch, so write capacity drops with update-heavy data.</div>`;
        break;
      }
      case 'snowflake': {
        const st = PL.sfState(n);
        h += `<div class="metrics">${metric('Warehouse', st)}${metric('Credits used', r.credits.toFixed(3))}${metric('Credits/h', { XS: 1, S: 2, M: 4, L: 8, XL: 16 }[n.cfg.size])}</div>`;
        h += `<div class="note">Running ${S.t ? Math.round(r.activeT / S.t * 100) : 0}% of the time. ${n.cfg.ingest === 'copy' ? 'COPY uses this warehouse.' : 'Snowpipe uses serverless compute; the warehouse wakes for dbt and dashboards.'}</div>`;
        if (n.cfg.ingest === 'copy') h += nextBar(n, +n.cfg.every) + runsViz(r.runs);
        break;
      }
      case 'dbt': {
        h += nextBar(n, +n.cfg.every) + runsViz(r.runs);
        if (!r.scd) PL.scdApply(n, null);
        if (n.cfg.scd !== 'none') {
          h += `<div class="sec-title" style="margin-top:6px">dim_customer ${n.cfg.scd === 'scd2' ? '(snapshot)' : ''}</div><div style="overflow-x:auto"><table class="mini-table"><tr><th>id</th><th>city</th>${n.cfg.scd === 'scd2' ? '<th>valid_from</th><th>valid_to</th><th>current</th>' : '<th>updated</th>'}</tr>`;
          const rows = n.cfg.scd === 'scd2' ? r.scd : r.scd.filter(x => x.cur);
          rows.forEach(x => { h += `<tr class="${x.cur ? (S.t - x.from < 2 ? 'new' : '') : 'old'}"><td>${x.id}</td><td>${x.city}</td>${n.cfg.scd === 'scd2' ? `<td>T+${fmtS(x.from)}</td><td>${x.to == null ? '∞' : 'T+' + fmtS(x.to)}</td><td>${x.cur ? 'yes' : 'no'}</td>` : `<td>T+${fmtS(x.from)}</td>`}</tr>`; });
          h += '</table></div>';
          h += `<div class="note">${n.cfg.scd === 'scd2' ? 'Each change closes the current row and inserts a new version. Facts join on the version that was valid at their event time.' : 'SCD1 overwrites the city in place. Earlier values are gone, so last month’s sales now show the new city.'}</div>`;
        }
        if (n.cfg.mat === 'table') h += `<div class="note warn">Table materialization rebuilds all ${fmt(r.total)} rows every run, so runs get slower as data grows.</div>`;
        if (n.cfg.mat === 'view') h += `<div class="note">Views build instantly, but every dashboard query re-computes them.</div>`;
        break;
      }
      case 'bi':
        h += `<div class="field-row"><span class="big">${r.freshness == null ? '–' : fmtS(r.freshness)}</span><span style="color:var(--muted)">behind real time</span></div>` + sparkSvg(r.fresh, 10);
        h += `<div class="note">${n.cfg.mode === 'live' ? 'Live mode queries the warehouse on every view, which keeps it awake.' : `Extract refreshes every ${n.cfg.refresh}s; between refreshes the data only gets older.`}</div>`;
        break;
    }
    return h + '</div>';
  }

  /* ---------- runtime dock ---------- */
  let dockTab = 'log', dockOpen = false;
  function openDock(open) {
    const was = dockOpen; dockOpen = open; $('#dock').classList.toggle('open', open); stage.classList.toggle('dock-open', open); renderDock();
    if (open && !was) { // keep the flow visible above the panel
      const r = stage.getBoundingClientRect(), dockTop = r.height - $('#dock').getBoundingClientRect().height - 40;
      const maxY = Math.max(...S.nodes.map(n => (n.y + PL.NH) * S.view.k + S.view.y), 0), minY = Math.min(...S.nodes.map(n => n.y * S.view.k + S.view.y), 1e9);
      const target = $('#dock').getBoundingClientRect().height || Math.min(r.height * 0.48, 340);
      const avail = r.height - target - 60;
      if (maxY > avail) { const shift = Math.min(maxY - avail, Math.max(0, minY - 20)); S.view.y -= shift; applyView(); }
    }
  }
  ui.showResult = id => { dockTab = id; openDock(true); };
  ui.openDock = openDock;
  function renderDock() {
    const withRes = S.nodes.filter(n => RT.results[n.id]).sort((a, b) => (a.type === 'table' ? 0 : 1) - (b.type === 'table' ? 0 : 1));
    if (dockTab !== 'log' && !withRes.find(n => n.id === dockTab)) dockTab = 'log';
    const errs = RT.logs.filter(l => l.level === 'err').length;
    $('#dockHead').innerHTML = `<button class="dtab title" id="dockToggle" aria-expanded="${dockOpen}">${RT.running ? '<span class="spin"></span>' : `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" style="transform:rotate(${dockOpen ? 180 : 0}deg)"><path d="M6 15l6-6 6 6"/></svg>`}Runtime</button>
      <button class="dtab ${dockTab === 'log' ? 'on' : ''}" data-t="log">Log${errs ? `<span class="cnt" style="color:var(--bad)">${errs}</span>` : ''}</button>
      ${withRes.map(n => `<button class="dtab ${dockTab === n.id ? 'on' : ''}" data-t="${n.id}">${esc(n.name)}<span class="cnt">${RT.results[n.id].error ? '!' : RT.results[n.id].rows.length}</span></button>`).join('')}
      <span class="spacer"></span>
      ${dockTab !== 'log' ? '<button class="btn ghost" id="copyCsv" style="height:28px">Copy CSV</button>' : '<button class="btn ghost" id="clearLog" style="height:28px">Clear</button>'}`;
    $('#dockToggle').onclick = () => openDock(!dockOpen);
    $('#dockHead').querySelectorAll('[data-t]').forEach(b => b.onclick = () => { dockTab = b.dataset.t; openDock(true); });
    const cc = $('#copyCsv'); if (cc) cc.onclick = () => copyText(toCSV(RT.results[dockTab].rows), 'Copied as CSV');
    const cl = $('#clearLog'); if (cl) cl.onclick = () => { RT.logs.length = 0; renderDock(); };
    if (!dockOpen) return;
    const body = $('#dockBody');
    if (dockTab === 'log') {
      body.innerHTML = RT.logs.length ? `<div class="rlog">${RT.logs.map(l => `<div class="${l.level}"><span class="t">${l.at.toLocaleTimeString()}</span>${l.node ? `<span class="n">${esc(l.node)}</span>` : ''}${esc(l.msg)}</div>`).join('')}</div>`
        : `<div class="dock-empty">Runs of API, File, Python script and Table blocks show up here. Press <b>Run flow</b> to execute them for real in your browser.</div>`;
      body.scrollTop = body.scrollHeight;
    } else {
      const res = RT.results[dockTab]; const n = PL.byId(dockTab);
      if (res.error) { body.innerHTML = `<div class="dock-empty" style="color:var(--bad)">${esc(res.error)}</div>`; return; }
      const rows = res.rows.slice(0, n && n.type === 'table' ? +n.cfg.maxRows : 500);
      const cols = []; rows.slice(0, 200).forEach(r => Object.keys(r).forEach(k => { if (!cols.includes(k)) cols.push(k); }));
      if (!rows.length) { body.innerHTML = '<div class="dock-empty">0 rows.</div>'; return; }
      body.innerHTML = `<table class="rtable"><thead><tr><th>#</th>${cols.map(c => `<th>${esc(c)}</th>`).join('')}</tr></thead><tbody>${rows.map((r, i) => `<tr><td class="num" style="color:var(--faint)">${i + 1}</td>${cols.map(c => { const v = r[c]; return `<td class="${typeof v === 'number' ? 'num' : ''}" title="${esc(v)}">${esc(v == null ? '' : typeof v === 'object' ? JSON.stringify(v) : v)}</td>`; }).join('')}</tr>`).join('')}</tbody></table>`;
    }
  }
  RT.listeners.push(() => { renderDock(); updateNodeStats(); const rb = $('#runBtn'); rb.disabled = RT.running; rb.querySelector('span').textContent = RT.running ? 'Running…' : 'Run flow'; if (S.sel) { const sh = $('#insShow'); if (!sh && S.sel.kind === 'node' && RT.results[S.sel.id] && insTab === 'cfg') buildInspector(true); } });
  function toCSV(rows) {
    const cols = []; rows.forEach(r => Object.keys(r).forEach(k => { if (!cols.includes(k)) cols.push(k); }));
    const q = v => { v = v == null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v); return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v; };
    return [cols.map(q).join(','), ...rows.map(r => cols.map(c => q(r[c])).join(','))].join('\n');
  }
  function copyText(t, msg) {
    if (navigator.clipboard) navigator.clipboard.writeText(t).then(() => toast(msg || 'Copied'), () => toast('Copy was blocked by the browser. Select the text and copy it manually.', true));
    else toast('Copy is not available here.', true);
  }
  ui.copyText = copyText;
  $('#runBtn').onclick = () => { openDock(true); PL.runFlow(null, 'Run flow'); };

  /* ---------- HUD ---------- */
  function updateHud() {
    const m = Math.floor(S.t / 60), s = Math.floor(S.t % 60);
    $('#clock').textContent = `T+${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
    const ingest = S.nodes.filter(n => ['postgres', 'app', 'api', 'file'].includes(n.type)).reduce((a, n) => a + n.rt.outRate, 0);
    const backlog = S.nodes.reduce((a, n) => a + n.rt.qN, 0);
    const bis = S.nodes.filter(n => n.type === 'bi' && n.rt.freshness != null);
    $('#hIngest').textContent = fmt(ingest) + '/s';
    $('#hBacklog').textContent = fmt(backlog);
    $('#hFresh').textContent = bis.length ? fmtS(Math.min(...bis.map(n => n.rt.freshness))) : '–';
  }

  /* ---------- top bar ---------- */
  const PLAY = '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M7 4.5v15l12.5-7.5z"/></svg>', PAUSE = '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/></svg>';
  function setRunning(v) {
    S.running = v; const b = $('#playBtn'); b.innerHTML = v ? PAUSE : PLAY;
    $('#stepBtn').classList.toggle('dim', v); if (PL.inside) $('#stage').classList.toggle('paused', !v);
    b.title = v ? 'Pause simulation (Space)' : 'Resume simulation (Space)'; b.setAttribute('aria-label', b.title);
  }
  ui.setRunning = setRunning;
  $('#playBtn').onclick = () => setRunning(!S.running);
  // advance a paused simulation by a quarter second, so you can watch one tick at a time
  function stepSim() {
    if (S.running) setRunning(false);
    for (let i = 0; i < 5; i++) PL.tick(0.05);
    updateNodeStats(); updateLive(); updateHud();
  }
  $('#stepBtn').onclick = stepSim;
  $('#traceBtn').onclick = () => PL.trace.active ? PL.trace.stop() : PL.trace.start();
  $('#speedSeg').onclick = e => { const b = e.target.closest('button'); if (!b) return; S.speed = +b.dataset.v; $('#speedSeg').querySelectorAll('button').forEach(x => x.classList.toggle('on', x === b)); };
  function closeMenus() { $('#menus').innerHTML = ''; }
  function menu(anchor, html, alignRight) {
    closeMenus();
    const m = document.createElement('div'); m.className = 'menu'; m.innerHTML = html;
    const r = anchor.getBoundingClientRect();
    m.style.top = (r.bottom + 6) + 'px'; m.style.position = 'fixed'; m.style.maxHeight = Math.max(160, innerHeight - r.bottom - 14) + 'px';
    const w = Math.min(340, innerWidth - 16);
    if (alignRight) m.style.right = Math.max(8, innerWidth - r.right) + 'px'; else m.style.left = Math.max(8, Math.min(r.left, innerWidth - w - 8)) + 'px';
    $('#menus').appendChild(m);
    setTimeout(() => document.addEventListener('pointerdown', function h(e) { if (!m.contains(e.target)) { closeMenus(); document.removeEventListener('pointerdown', h); } }), 0);
    return m;
  }
  $('#scenBtn').onclick = e => {
    const m = menu(e.currentTarget, PL.SCENARIOS.map(s => `<button data-s="${s.id}"><b>${esc(s.name)}</b><small>${esc(s.blurb)}</small></button>`).join(''));
    m.onclick = ev => { const b = ev.target.closest('[data-s]'); if (!b) return; closeMenus(); loadScenario(b.dataset.s); };
  };
  $('#moreBtn').onclick = e => {
    const m = menu(e.currentTarget, `<button data-a="reset"><b>Restart simulation</b><small>Clear queues and counters, keep the design</small></button>
      <button data-a="json"><b>Save or load design</b><small>Copy the design as JSON, or paste one</small></button>
      <hr><button data-a="clear"><b>Clear canvas</b><small>Remove every component</small></button>`, true);
    m.onclick = ev => {
      const b = ev.target.closest('[data-a]'); if (!b) return; closeMenus();
      if (b.dataset.a === 'reset') { PL.resetSim(); toast('Simulation restarted'); }
      if (b.dataset.a === 'json') jsonModal();
      if (b.dataset.a === 'clear') { const snap = serialize(); S.nodes = []; S.links = []; S.particles = []; RT.results = {}; select(null); syncNodes(); renderDock(); saveSoon(); toastUndo('Canvas cleared', snap); }
    };
  };
  function toastUndo(msg, snap) {
    const t = document.createElement('div'); t.className = 'toast'; t.style.pointerEvents = 'auto';
    t.innerHTML = `${esc(msg)} <button class="btn ghost" style="height:24px;color:inherit;margin-left:8px">Undo</button>`;
    t.querySelector('button').onclick = () => { load(snap); t.remove(); };
    $('#toasts').appendChild(t); setTimeout(() => t.remove(), 6000);
  }
  $('#exportBtn').onclick = () => PL.openExport();

  /* ---------- modal ---------- */
  ui.modal = function (title, bodyHTML, footHTML) {
    const m = $('#modal'); m.hidden = false;
    m.innerHTML = `<div class="modal-card" role="dialog" aria-modal="true" aria-label="${esc(title)}"><div class="modal-head"><h2>${esc(title)}</h2>${footHTML || ''}<button class="btn icon ghost" id="mClose" aria-label="Close"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M6 6l12 12M18 6L6 18"/></svg></button></div><div class="modal-body">${bodyHTML}</div></div>`;
    $('#mClose').onclick = ui.closeModal;
    m.onclick = e => { if (e.target === m) ui.closeModal(); };
    return m.querySelector('.modal-card');
  };
  ui.closeModal = () => { const m = $('#modal'); m.hidden = true; m.innerHTML = ''; };
  function jsonModal() {
    const card = ui.modal('Save or load design', `<p style="margin:0;color:var(--muted)">Copy this JSON to keep a design, or paste one and press Load.</p><textarea id="jsonTa" spellcheck="false" aria-label="Design JSON">${esc(JSON.stringify(serialize(), null, 2))}</textarea>`,
      `<button class="btn" id="jCopy">Copy</button><button class="btn primary" id="jLoad">Load</button>`);
    card.querySelector('#jCopy').onclick = () => copyText(card.querySelector('#jsonTa').value, 'Design copied');
    card.querySelector('#jLoad').onclick = () => {
      try { const d = JSON.parse(card.querySelector('#jsonTa').value); load(d); ui.closeModal(); toast('Design loaded'); }
      catch (e) { toast('That isn’t valid design JSON: ' + e.message, true); }
    };
  }

  /* ---------- scenarios & persistence ---------- */
  function showTip(s) {
    const tc = $('#tipcard');
    if (!s || !s.tip) { tc.hidden = true; return; }
    tc.hidden = false;
    tc.innerHTML = `<b>${esc(s.name)}</b>${esc(s.tip)}<button class="btn icon ghost" aria-label="Dismiss tip"><svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M6 6l12 12M18 6L6 18"/></svg></button>`;
    tc.querySelector('button').onclick = () => { tc.hidden = true; S.tipHidden = true; saveSoon(); };
  }
  function loadScenario(id) {
    const s = PL.SCENARIOS.find(x => x.id === id); if (!s) return;
    S.nodes = []; S.links = []; S.particles = []; S.t = 0; S.nid = 1; S.lid = 1; RT.results = {}; S.plan = s.plan ? JSON.parse(JSON.stringify(s.plan)) : null;
    const map = {};
    s.nodes.forEach(([key, type, x, y, cfg, name]) => { map[key] = PL.addNode(type, x, y, JSON.parse(JSON.stringify(cfg)), null, name).id; });
    s.links.forEach(([a, b]) => PL.addLink(map[a], map[b]));
    S.scenario = id; S.tipHidden = false; PL.trace.stop(); PL.inside.close();
    select(null); syncNodes(); showTip(s); renderDock(); requestAnimationFrame(fit); saveSoon();
  }
  ui.loadScenario = loadScenario;
  function serialize() {
    return { v: 1, scenario: S.scenario, tipHidden: !!S.tipHidden, view: S.view, plan: S.plan || null,
      nodes: S.nodes.map(n => ({ id: n.id, type: n.type, name: n.name, x: n.x, y: n.y, cfg: n.cfg })),
      links: S.links.map(l => ({ id: l.id, from: l.from, to: l.to })) };
  }
  PL.serialize = serialize;
  function load(d) {
    if (!d || !Array.isArray(d.nodes)) throw new Error('missing nodes');
    S.nodes = []; S.links = []; S.particles = []; S.nid = 1; S.lid = 1;
    d.nodes.forEach(n => { if (PL.TYPES[n.type]) PL.addNode(n.type, n.x, n.y, n.cfg, n.id, n.name); });
    (d.links || []).forEach(l => { if (PL.byId(l.from) && PL.byId(l.to)) PL.addLink(l.from, l.to, l.id); });
    S.scenario = d.scenario; S.tipHidden = d.tipHidden; S.plan = d.plan || null;
    if (d.view) { S.view = d.view; applyView(); } else requestAnimationFrame(fit);
    select(null); syncNodes(); renderDock();
    const s = PL.SCENARIOS.find(x => x.id === d.scenario); showTip(S.tipHidden ? null : s);
    saveSoon();
  }
  let saveT = null;
  function saveSoon() {
    clearTimeout(saveT);
    saveT = setTimeout(() => {
      try {
        const d = serialize();
        let txt = JSON.stringify(d);
        if (txt.length > 2e6) { d.nodes.forEach(n => { if (n.cfg.fileText && n.cfg.fileText.length > 2e5) n.cfg = Object.assign({}, n.cfg, { fileText: '' }); }); txt = JSON.stringify(d); }
        localStorage.setItem(STORE_KEY, txt);
      } catch (e) { /* storage unavailable: design lives for this session only */ }
    }, 400);
  }
  ui.saveSoon = saveSoon;

  /* ---------- main loop ---------- */
  let last = performance.now(), uiClock = 0;
  function frame(now) {
    const realDt = Math.min(0.1, (now - last) / 1000); last = now;
    if (S.running) { let dt = realDt * S.speed; while (dt > 1e-6) { const st = Math.min(dt, 0.05); PL.tick(st); dt -= st; } }
    // real-run particles move in real time, even when the simulation is paused
    S.particles = S.particles.filter(p => { if (!(p.real || p.realCtrl)) return true; p.p += realDt / p.dur; return p.p < 1; });
    PL.trace.tick(realDt); PL.trace.camera(realDt);
    draw();
    PL.inside.frame(realDt);
    uiClock += realDt;
    if (uiClock > 0.2) { uiClock = 0; updateNodeStats(); updateLive(); updateHud(); if (peek.hl && !S.particles.includes(peek.hl)) peek.hl = null; }
    requestAnimationFrame(frame);
  }

  /* ---------- boot ---------- */
  $('#zIn').onclick = () => { const r = stage.getBoundingClientRect(); zoomAt(1.2, r.left + r.width / 2, r.top + r.height / 2); };
  $('#zOut').onclick = () => { const r = stage.getBoundingClientRect(); zoomAt(1 / 1.2, r.left + r.width / 2, r.top + r.height / 2); };
  $('#zFit').onclick = fit;
  buildPalette(); resize(); setRunning(true); renderDock();
  let saved = null;
  try { saved = JSON.parse(localStorage.getItem(STORE_KEY) || 'null'); } catch (e) { saved = null; }
  const hash = location.hash.replace('#', '');
  if (hash && PL.SCENARIOS.find(s => s.id === hash)) loadScenario(hash);
  else if (saved && saved.nodes) { try { load(saved); } catch (e) { loadScenario('cdc'); } }
  else loadScenario('cdc');
  window.addEventListener('hashchange', () => { const h = location.hash.replace('#', ''); if (PL.SCENARIOS.find(s => s.id === h)) loadScenario(h); });
  applyView();
  requestAnimationFrame(frame);
})(window.PL);
