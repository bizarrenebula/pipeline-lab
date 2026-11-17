/* Pipeline Lab — step-through mode ("Follow a record").
 * Picks one concrete record at a source and walks it hop by hop through every branch downstream.
 * Each step animates a labelled token along one link, then explains what the receiving component
 * does with it, shows the payload as it crosses the link, and adds the hop to a latency budget.
 */
window.PL = window.PL || {};
(function (PL) {
  'use strict';
  const S = PL.S, R = PL.REC;
  const $ = s => document.querySelector(s);
  const TR = PL.trace = { active: false, steps: [], i: 0, phase: 'idle', p: 0, auto: false, dwell: 0, speed: 1, op: 'u', table: null };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  let wasRunning = true, root = null, cardEl = null;

  /* ---------- building the walk ---------- */
  function rootFor(n) {
    if (n && R.isSource(n)) return n;
    if (n && !PL.TYPES[n.type].control) return R.pathTo(n)[0];
    const srcs = S.nodes.filter(x => R.isSource(x) && PL.outLinks(x).length);
    return srcs.find(x => x.type === 'postgres') || srcs.find(x => x.type === 'app') || srcs[0] || null;
  }
  function hasScdDownstream(n) {
    const seen = new Set(), st = [n];
    while (st.length) { const x = st.pop(); if (seen.has(x.id)) continue; seen.add(x.id); if (x.type === 'dbt' && x.cfg.scd !== 'none') return true; PL.outLinks(x).forEach(l => st.push(PL.byId(l.to))); }
    return false;
  }
  function build() {
    const op = root.type === 'postgres' ? TR.op : 'c';
    const table = TR.table || (op === 'u' && hasScdDownstream(root) ? 'customers' : 'orders');
    const rec0 = R.sourceChange(root, op, table);
    const h0 = R.hop(root, rec0, null, false);
    const steps = [{ node: root, link: null, recIn: rec0, hop: h0, total: h0.wait, depth: 0 }];
    (function dfs(node, rec, total, depth) {
      if (steps.length > 24) return;
      const outs = PL.outLinks(node).filter(l => !PL.isControlLink(l));
      outs.forEach((l, bi) => {
        const to = PL.byId(l.to); if (!to) return;
        const h = R.hop(to, rec, node, false);
        const st = { node: to, link: l, recIn: rec, hop: h, total: total + (h.wait || 0), depth: depth + 1, branch: outs.length > 1 ? { idx: bi, of: outs.length, from: node } : null };
        steps.push(st);
        if (h.out) dfs(to, h.out, st.total, depth + 1);
      });
    })(root, h0.out, h0.wait, 0);
    return steps;
  }

  /* ---------- lifecycle ---------- */
  TR.start = function (fromNode) {
    const r = rootFor(fromNode || (S.sel && S.sel.kind === 'node' ? PL.byId(S.sel.id) : null));
    if (!r) { PL.ui.toast('Add a source (Postgres, App events, API or File) and link it to something first.', true); return; }
    if (!TR.active) { wasRunning = S.running; PL.ui.setRunning(false); }
    root = r; TR.active = true; TR.steps = build(); TR.i = 0; TR.phase = 'arrived'; TR.p = 1; TR.dwell = 0;
    document.getElementById('stage').classList.add('tracing');
    if (innerWidth < 760) PL.ui.select(null);
    render(); focusStep(true);
  };
  TR.restart = function () { if (root) { TR.steps = build(); TR.i = 0; TR.phase = 'arrived'; TR.p = 1; TR.dwell = 0; render(); focusStep(true); } };
  TR.stop = function () {
    if (!TR.active) return;
    TR.active = false; TR.auto = false; TR.steps = [];
    document.getElementById('stage').classList.remove('tracing');
    markNodes();
    if (cardEl) cardEl.hidden = true;
    PL.ui.setRunning(wasRunning);
  };
  TR.next = function () {
    if (!TR.active) return;
    if (TR.phase === 'travel') { arrive(); return; }
    if (TR.i >= TR.steps.length - 1) { TR.auto = false; render(); return; }
    TR.i++; TR.phase = 'travel'; TR.p = 0; TR.dwell = 0; render(); focusStep();
  };
  TR.prev = function () {
    if (!TR.active || TR.i === 0) return;
    TR.i--; TR.phase = 'arrived'; TR.p = 1; TR.dwell = 0; TR.auto = false; render(); focusStep();
  };
  TR.toggleAuto = function () {
    TR.auto = !TR.auto; TR.dwell = 0;
    if (TR.auto && TR.phase === 'arrived' && TR.i >= TR.steps.length - 1) { TR.i = 0; }
    if (TR.auto && TR.phase === 'arrived') TR.dwell = 99; // move on now
    render();
  };
  function arrive() {
    TR.phase = 'arrived'; TR.p = 1; TR.dwell = 0;
    const st = TR.steps[TR.i];
    // the traced change really lands in dbt's history table, so the Configure tab shows it
    if (st.node.type === 'dbt' && st.hop.out && st.hop.out.fmt === 'scd') { const a = st.hop.out.rows[1]; R.applyScd(st.node, a.customer_id, a.city, S.t); }
    if (st.node.type === 'dbt' && st.hop.out && st.hop.out.fmt === 'row' && st.hop.out.table === 'customers' && st.node.cfg.scd === 'scd1') R.applyScd(st.node, st.hop.out.data.customer_id, st.hop.out.data.city, S.t);
    render();
  }
  TR.tick = function (dt) {
    if (!TR.active) return;
    const travel = 1.5 / TR.speed;
    if (TR.phase === 'travel') { TR.p += dt / travel; if (TR.p >= 1) arrive(); }
    else if (TR.auto) {
      TR.dwell += dt;
      const st = TR.steps[TR.i]; const words = (st.hop.body || '').replace(/<[^>]+>/g, '').split(/\s+/).length;
      const need = Math.min(14, 3 + words / 9) / TR.speed;
      if (TR.dwell > need) { if (TR.i >= TR.steps.length - 1) { TR.auto = false; render(); } else TR.next(); }
    }
    pulse += dt;
  };
  let pulse = 0;

  /* ---------- camera ---------- */
  let cam = null;
  function focusStep(instant) {
    const st = TR.steps[TR.i]; if (!st) return;
    const n = st.node, a = st.link ? PL.byId(st.link.from) : n;
    const x0 = Math.min(a.x, n.x), x1 = Math.max(a.x, n.x) + PL.NW, y0 = Math.min(a.y, n.y), y1 = Math.max(a.y, n.y) + PL.NH;
    const stage = $('#stage').getBoundingClientRect();
    const card = cardEl && !cardEl.hidden ? cardEl.querySelector('.tcard').getBoundingClientRect() : null;
    const narrow = stage.width < 760;
    const left = narrow ? 12 : $('#palette').offsetWidth + 30;
    const ins = $('#inspector'); const right = stage.width - ((!ins.hidden && !narrow) ? ins.offsetWidth + 30 : 20);
    const top = 16, bottom = card ? card.top - stage.top - 16 : stage.height - 60;
    // on a phone the fitted view is tiny: zoom in to the hop being explained
    const k = narrow ? Math.max(S.view.k, Math.min(0.62, (right - left) / (x1 - x0 + 40))) : S.view.k;
    const cx = (left + right) / 2, cy = (top + bottom) / 2;
    const vx = k === S.view.k ? S.view.x : cx - ((x0 + x1) / 2) * k, vy = k === S.view.k ? S.view.y : cy - ((y0 + y1) / 2) * k;
    let sx0 = x0 * k + vx, sx1 = x1 * k + vx, sy0 = y0 * k + vy, sy1 = y1 * k + vy;
    let dx = 0, dy = 0;
    if (sx1 - sx0 < right - left) { if (sx0 < left) dx = left - sx0; else if (sx1 > right) dx = right - sx1; } else dx = (left + right) / 2 - (sx0 + sx1) / 2;
    if (sy1 - sy0 < bottom - top) { if (sy0 < top) dy = top - sy0; else if (sy1 > bottom) dy = bottom - sy1; } else dy = (top + bottom) / 2 - (sy0 + sy1) / 2;
    if (!dx && !dy && k === S.view.k) return;
    cam = { fx: S.view.x, fy: S.view.y, fk: S.view.k, tx: vx + dx, ty: vy + dy, tk: k, t: instant ? 0.6 : 0 };
  }
  TR.camera = function (dt) {
    if (!cam) return;
    cam.t = Math.min(1, cam.t + dt / 0.45);
    const e = 1 - Math.pow(1 - cam.t, 3);
    S.view.x = cam.fx + (cam.tx - cam.fx) * e; S.view.y = cam.fy + (cam.ty - cam.fy) * e; S.view.k = cam.fk + (cam.tk - cam.fk) * e;
    PL.ui.applyView();
    if (cam.t >= 1) cam = null;
  };

  /* ---------- canvas layer (called inside the world transform) ---------- */
  function rr(ctx, x, y, w, h, r) { ctx.beginPath(); ctx.moveTo(x + r, y); ctx.arcTo(x + w, y, x + w, y + h, r); ctx.arcTo(x + w, y + h, x, y + h, r); ctx.arcTo(x, y + h, x, y, r); ctx.arcTo(x, y, x + w, y, r); ctx.closePath(); }
  TR.draw = function (ctx, k, C) {
    if (!TR.active) return;
    const done = TR.steps.slice(1, TR.i + (TR.phase === 'arrived' ? 1 : 0));
    // path walked so far
    ctx.save();
    ctx.strokeStyle = C.accent; ctx.lineCap = 'round';
    for (const st of done) {
      if (!S.links.includes(st.link)) continue;
      const c = PL.bez(st.link);
      ctx.globalAlpha = 0.55; ctx.lineWidth = 4 / Math.max(k, 0.6);
      ctx.beginPath(); ctx.moveTo(c[0], c[1]); ctx.bezierCurveTo(c[2], c[3], c[4], c[5], c[6], c[7]); ctx.stroke();
    }
    ctx.globalAlpha = 1;
    const st = TR.steps[TR.i]; if (!st) { ctx.restore(); return; }
    // current link: a bright trail behind the token
    if (st.link && S.links.includes(st.link)) {
      const c = PL.bez(st.link);
      ctx.lineWidth = 5 / Math.max(k, 0.6); ctx.globalAlpha = 0.9;
      ctx.beginPath(); let first = true;
      for (let i = 0; i <= 40; i++) { const t = i / 40 * TR.p; const q = PL.bpt(c, t); if (first) { ctx.moveTo(q[0], q[1]); first = false; } else ctx.lineTo(q[0], q[1]); }
      ctx.stroke(); ctx.globalAlpha = 1;
      if (TR.phase === 'travel') drawToken(ctx, PL.bpt(c, easeIO(TR.p)), st.recIn, C, k);
    }
    // ring around the component that holds the record now
    if (TR.phase === 'arrived') {
      const n = st.node, a = 0.5 + 0.5 * Math.sin(pulse * 4);
      ctx.strokeStyle = st.hop.dropped ? C.bad : C.accent; ctx.lineWidth = 2.5 / Math.max(k, 0.6);
      ctx.globalAlpha = 0.35 + 0.5 * a;
      rr(ctx, n.x - 7 - a * 3, n.y - 7 - a * 3, PL.NW + 14 + a * 6, PL.NH + 14 + a * 6, 15); ctx.stroke();
      ctx.globalAlpha = 1;
      // the record now sits on top of the node
      drawToken(ctx, [n.x + PL.NW / 2, n.y - 18], st.hop.out || st.recIn, C, k, st.hop.dropped);
    }
    ctx.restore();
  };
  const easeIO = t => t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
  function drawToken(ctx, pt, rec, C, k, bad) {
    const txt = bad ? '✕ lost' : R.label(rec);
    const s = 1 / Math.max(k, 0.7);
    ctx.save(); ctx.translate(pt[0], pt[1]); ctx.scale(s, s);
    ctx.font = '600 11.5px ' + getComputedStyle(document.body).getPropertyValue('--f-mono');
    const w = ctx.measureText(txt).width + 26;
    const col = bad ? C.bad : C[{ c: 'ins', u: 'upd', d: 'del' }[rec && rec.op] || 'ins'];
    ctx.shadowColor = 'rgba(0,0,0,.25)'; ctx.shadowBlur = 8; ctx.shadowOffsetY = 2;
    ctx.fillStyle = C.panel; rr(ctx, -w / 2, -12, w, 24, 12); ctx.fill();
    ctx.shadowColor = 'transparent';
    ctx.strokeStyle = col; ctx.lineWidth = 2; ctx.stroke();
    ctx.fillStyle = col; ctx.beginPath(); ctx.arc(-w / 2 + 12, 0, 4.5, 0, Math.PI * 2); ctx.fill();
    ctx.fillStyle = C.fg; ctx.textAlign = 'left'; ctx.textBaseline = 'middle'; ctx.fillText(txt, -w / 2 + 21, 0.5);
    ctx.restore();
  }

  /* ---------- the caption card ---------- */
  function markNodes() {
    document.querySelectorAll('.node.traced, .node.trace-seen').forEach(el => el.classList.remove('traced', 'trace-seen'));
    if (!TR.active) return;
    TR.steps.slice(0, TR.i + 1).forEach((st, j) => {
      const el = document.querySelector(`.node[data-id="${st.node.id}"]`); if (!el) return;
      el.classList.add(j === TR.i && TR.phase === 'arrived' ? 'traced' : 'trace-seen');
    });
  }
  const IC = {
    prev: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M15 6l-6 6 6 6"/></svg>',
    next: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 6l6 6-6 6"/></svg>',
    play: '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M7 4.5v15l12.5-7.5z"/></svg>',
    pause: '<svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="5" width="4" height="14" rx="1"/><rect x="14" y="5" width="4" height="14" rx="1"/></svg>',
    x: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M6 6l12 12M18 6L6 18"/></svg>',
    redo: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12a8 8 0 1 0 2.5-5.8"/><path d="M4 4v4h4"/></svg>'
  };
  function render() {
    markNodes();
    if (!cardEl) {
      cardEl = document.createElement('div'); cardEl.id = 'tracewrap';
      $('#stage').appendChild(cardEl);
      cardEl.addEventListener('click', onClick);
    }
    if (!TR.active) { cardEl.hidden = true; return; }
    cardEl.hidden = false;
    const st = TR.steps[TR.i], N = TR.steps.length, last = TR.i === N - 1;
    const travelling = TR.phase === 'travel';
    const from = st.link ? PL.byId(st.link.from) : null;
    const total = st.total;
    const ends = !st.hop.out && TR.phase === 'arrived';
    const branchNote = st.branch && st.branch.idx > 0 ? `<div class="t-branch">Back at ${esc(st.branch.from.name)}: the same record also goes to branch ${st.branch.idx + 1} of ${st.branch.of}.</div>` : '';
    const dots = TR.steps.map((s, j) => `<button class="t-dot ${j === TR.i ? 'on' : j < TR.i ? 'done' : ''} ${s.hop.dropped ? 'lost' : ''}" data-go="${j}" title="${esc(s.node.name)}" aria-label="Go to step ${j + 1}: ${esc(s.node.name)}"></button>`).join('');
    const opSeg = root.type === 'postgres' ? `<div class="seg t-op" role="group" aria-label="Change type">${[['c', 'Insert'], ['u', 'Update'], ['d', 'Delete']].map(([v, l]) => `<button data-op="${v}" class="${TR.op === v ? 'on' : ''}">${l}</button>`).join('')}</div>` : '';
    cardEl.innerHTML = `<div class="tcard" role="region" aria-label="Follow a record">
      <div class="t-head">
        <span class="t-kicker">Following one record · step ${TR.i + 1} of ${N}</span>
        <span class="spacer"></span>
        <button class="btn icon ghost" data-a="redo" title="New record" aria-label="Start again with a new record">${IC.redo}</button>
        <button class="btn icon ghost" data-a="close" title="Exit (Esc)" aria-label="Exit step-through">${IC.x}</button>
      </div>
      <div class="t-body">
        ${branchNote}
        <div class="t-route">${from ? `<span>${esc(from.name)}</span><i>→</i>` : ''}<b>${esc(st.node.name)}</b></div>
        <h3 class="${st.hop.dropped ? 'bad' : ''}">${travelling ? `On the wire to ${esc(st.node.name)}…` : esc(st.hop.title)}</h3>
        ${travelling ? `<p class="t-p">${esc(PL.LINK_TEXT[from.type] || '')}.</p>` : `<p class="t-p">${st.hop.body}</p>`}
        <div class="t-lat">${st.hop.waitText && !travelling ? `<span>This hop <b>+${R.fmtS(st.hop.wait)}</b> · ${esc(st.hop.waitText)}</span>` : '<span></span>'}<span>Since commit <b>≈ ${R.fmtS(travelling ? total - (st.hop.wait || 0) : total)}</b></span></div>
        <div class="t-payload">
          <div class="sec-title">${st.link ? 'What crossed the link' : 'The change'}</div>
          <div class="pv">${R.view(st.recIn)}</div>
          ${!travelling && st.hop.out && st.hop.out.fmt !== st.recIn.fmt ? `<details class="t-out"><summary>What ${esc(st.node.name)} sends on</summary><div class="pv">${R.view(st.hop.out)}</div></details>` : ''}
          ${ends ? `<div class="note ${st.hop.dropped ? 'bad' : ''}">${st.hop.dropped ? 'This branch ends here.' : last ? `End of the line. The change took about <b>${R.fmtS(total)}</b> from commit to here.` : 'This branch ends here.'}</div>` : ''}
        </div>
      </div>
      <div class="t-foot">
        ${opSeg}
        <div class="t-dots">${dots}</div>
        <div class="t-ctl">
          <button class="btn icon" data-a="prev" ${TR.i === 0 ? 'disabled' : ''} aria-label="Previous step (←)" title="Previous (←)">${IC.prev}</button>
          <button class="btn icon" data-a="auto" aria-label="${TR.auto ? 'Pause autoplay' : 'Autoplay'}" title="${TR.auto ? 'Pause' : 'Autoplay'}">${TR.auto ? IC.pause : IC.play}</button>
          <button class="btn primary t-next" data-a="next" ${last && !travelling ? 'disabled' : ''} aria-label="Next step (→)" title="Next (→)">${travelling ? 'Skip' : 'Next'}${IC.next}</button>
          <div class="seg t-speed" role="group" aria-label="Animation speed"><button data-sp="0.5" class="${TR.speed === 0.5 ? 'on' : ''}">Slow</button><button data-sp="1" class="${TR.speed === 1 ? 'on' : ''}">1×</button><button data-sp="2" class="${TR.speed === 2 ? 'on' : ''}">Fast</button></div>
        </div>
      </div>
    </div>`;
  }
  TR.render = render;
  function onClick(e) {
    const b = e.target.closest('button'); if (!b) return;
    if (b.dataset.a === 'close') TR.stop();
    else if (b.dataset.a === 'next') TR.next();
    else if (b.dataset.a === 'prev') TR.prev();
    else if (b.dataset.a === 'auto') TR.toggleAuto();
    else if (b.dataset.a === 'redo') TR.restart();
    else if (b.dataset.op) { TR.op = b.dataset.op; TR.table = null; TR.restart(); }
    else if (b.dataset.sp) { TR.speed = +b.dataset.sp; render(); }
    else if (b.dataset.go != null) { TR.i = +b.dataset.go; TR.phase = 'arrived'; TR.p = 1; TR.auto = false; render(); focusStep(); }
  }
  // the graph changed under us: rebuild so we never point at a removed link
  TR.onGraphChange = function () {
    if (!TR.active) return;
    if (!PL.byId(root.id)) { TR.stop(); return; }
    const ok = TR.steps.every(s => PL.byId(s.node.id) && (!s.link || S.links.includes(s.link)));
    if (!ok) { const i = TR.i; TR.steps = build(); TR.i = Math.min(i, TR.steps.length - 1); TR.phase = 'arrived'; TR.p = 1; render(); }
  };
})(window.PL);
