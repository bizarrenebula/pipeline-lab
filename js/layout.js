/* Pipeline Lab — resizable panels. Drag a panel's edge (mouse or finger) to resize it; double-click the edge to reset.
   Sizes live in CSS variables on <html> and are remembered per device. */
(function (PL) {
  'use strict';
  const $ = s => document.querySelector(s);
  const root = document.documentElement, stage = $('#stage');
  const KEY = 'pipelinelab.layout';
  const narrow = () => innerWidth < 760;

  // each handle: which CSS variable it drives, how a pointer move changes it, and its limits
  const SPECS = {
    pal:    { v: '--pal-w',    axis: 'x', sign: 1,  min: 180, max: () => Math.min(460, innerWidth * 0.45) },
    ins:    { v: '--ins-w',    axis: 'x', sign: -1, min: 300, max: () => Math.min(760, innerWidth * 0.6) },
    insH:   { v: '--ins-h',    axis: 'y', sign: -1, min: 140, max: () => stage.clientHeight - 16 },
    dock:   { v: '--dock-h',   axis: 'y', sign: -1, min: 110, max: () => stage.clientHeight + 40 - 56 },
    assist: { v: '--assist-h', axis: 'y', sign: -1, min: 60,  max: () => Math.max(120, stage.clientHeight - 160) }
  };
  // a phone and a desktop want different sizes, so keep them apart
  const bucket = () => (narrow() ? 'sm' : 'lg');
  let saved = {};
  try { saved = JSON.parse(localStorage.getItem(KEY) || '{}') || {}; } catch (e) { saved = {}; }
  function persist() { try { localStorage.setItem(KEY, JSON.stringify(saved)); } catch (e) { /* storage unavailable */ } }
  function apply() {
    const b = saved[bucket()] || {};
    for (const id in SPECS) {
      const sp = SPECS[id];
      if (b[id] != null) root.style.setProperty(sp.v, Math.round(Math.min(sp.max(), Math.max(sp.min, b[id]))) + 'px');
      else root.style.removeProperty(sp.v);
    }
  }

  const HANDLES = {
    pal: '<div class="rz rz-e" data-rz="pal" title="Drag to resize" aria-hidden="true"></div>',
    ins: '<div class="rz rz-w" data-rz="ins" title="Drag to resize" aria-hidden="true"></div><div class="rz rz-n rz-sm" data-rz="insH" title="Drag to resize" aria-hidden="true"></div>',
    dock: '<div class="rz rz-n" data-rz="dock" title="Drag to resize" aria-hidden="true"></div>',
    assist: '<div class="rz rz-n" data-rz="assist" title="Drag to resize" aria-hidden="true"></div>'
  };
  // the inspector and assistant re-render their contents, so re-attach their handles whenever that happens
  function keepHandle(el, html) {
    const ensure = () => { if (!el.querySelector(':scope > .rz')) el.insertAdjacentHTML('beforeend', html); };
    ensure(); new MutationObserver(ensure).observe(el, { childList: true });
  }
  keepHandle($('#palette'), HANDLES.pal);
  keepHandle($('#inspector'), HANDLES.ins);
  keepHandle($('#dock'), HANDLES.dock);
  keepHandle($('#assist'), HANDLES.assist);

  // the panel's current size, measured, so a drag starts from what is on screen
  function current(id) {
    if (id === 'pal') return $('#palette').offsetWidth;
    if (id === 'ins') return $('#inspector').offsetWidth;
    if (id === 'insH') return $('#inspector').offsetHeight;
    if (id === 'dock') return $('#dock').offsetHeight;
    if (id === 'assist') { const l = $('#assist .assist-list'); return l ? l.offsetHeight : 180; }
  }

  let rz = null;
  document.addEventListener('pointerdown', e => {
    const h = e.target.closest('.rz'); if (!h) return;
    const id = h.dataset.rz, sp = SPECS[id];
    if (id === 'dock' && !$('#dock').classList.contains('open')) return;
    e.preventDefault(); e.stopPropagation();
    h.setPointerCapture(e.pointerId);
    rz = { id, sp, h, start: sp.axis === 'x' ? e.clientX : e.clientY, size: current(id) };
    root.classList.add('resizing'); h.classList.add('on');
  }, true);
  document.addEventListener('pointermove', e => {
    if (!rz) return;
    const pos = rz.sp.axis === 'x' ? e.clientX : e.clientY;
    const v = Math.min(rz.sp.max(), Math.max(rz.sp.min, rz.size + (pos - rz.start) * rz.sp.sign));
    root.style.setProperty(rz.sp.v, Math.round(v) + 'px');
    (saved[bucket()] = saved[bucket()] || {})[rz.id] = Math.round(v);
  });
  function end() {
    if (!rz) return;
    rz.h.classList.remove('on'); root.classList.remove('resizing'); rz = null; persist();
  }
  document.addEventListener('pointerup', end);
  document.addEventListener('pointercancel', end);
  document.addEventListener('dblclick', e => {
    const h = e.target.closest('.rz'); if (!h) return;
    const b = saved[bucket()]; if (b) delete b[h.dataset.rz];
    persist(); apply();
  });

  let lastBucket = bucket();
  window.addEventListener('resize', () => { if (bucket() !== lastBucket) lastBucket = bucket(); apply(); });
  apply();
})(window.PL);
