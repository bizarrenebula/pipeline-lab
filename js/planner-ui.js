/* Pipeline Lab — resource planner panel. */
(function (PL) {
  'use strict';
  const S = PL.S, ui = PL.ui, PLN = PL.planner;
  const esc = ui.esc, usd = PL.fmtUSD;
  const open = new Set(); // expanded component cards

  const INTRO = `<p>Sizing goes the same way for every component:</p>
    <ol>
      <li><b>Volume.</b> Start from rows or bytes per second at the source, then multiply by the peak factor and by growth, because you size for the busiest hour of next year, not today's average.</li>
      <li><b>Parallelism.</b> Divide by what one unit handles: a Kafka partition, a Spark core or a Flink slot. That gives the number of parallel units.</li>
      <li><b>Shape.</b> Group the units into processes: executors, TaskManagers or brokers. Give each enough memory for its state, plus JVM overhead.</li>
      <li><b>Placement.</b> Pack the processes onto machines: EC2 nodes, managed service units or Kubernetes nodes. A node gives up some CPU and memory to the kubelet, the OS and daemonsets.</li>
      <li><b>Cost.</b> Multiply by the hours each piece runs. Always-on streaming bills 730 h a month. Batch bills only while it runs.</li>
    </ol>
    <p>Change the numbers below and watch partitions, executors, warehouse size and the monthly cost respond. Any value can be overridden by hand. Values far below the suggestion are flagged as undersized, and values far above it as overspend.</p>`;

  function srcRow(n) {
    const s = PLN.srcCfg(n);
    return `<div class="pl-src" data-src="${n.id}">
      <span class="pl-src-name"><span class="ico sm" style="--c:${ui.catColor(n.type)}">${ui.icon(n.type)}</span>${esc(n.name)}</span>
      <input type="number" min="0" step="any" data-f="value" value="${s.value}" aria-label="${esc(n.name)} volume">
      <select data-f="unit" aria-label="${esc(n.name)} unit">${Object.keys(PLN.UNITS).map(u => `<option${u === s.unit ? ' selected' : ''}>${u}</option>`).join('')}</select>
      <label class="pl-inl"><input type="number" min="10" step="10" data-f="size" value="${s.size}" aria-label="${esc(n.name)} record size"> B/record</label>
    </div>`;
  }
  function field(k, label, val, unit, hint) {
    return `<label class="pl-field" title="${esc(hint)}"><span>${label}</span><span class="pl-inl"><input type="number" min="0" step="any" data-w="${k}" value="${val}">${unit ? `<small>${unit}</small>` : ''}</span></label>`;
  }

  function results() {
    const R = PLN.compute(), p = PLN.plan();
    const fmtV = (r, val) => typeof val === 'number' ? (Math.round(val * 10) / 10) + (r.unit ? ' ' + r.unit : '') : esc(val);
    const cards = R.comps.map(c => {
      const id = c.n.id, T = PLN.TARGETS[c.n.type];
      const flag = c.rows.some(r => r.state === 'under') ? '<span class="pl-flag under">undersized</span>' : c.rows.some(r => r.state === 'over') ? '<span class="pl-flag over">overspend</span>' : '';
      const manual = c.rows.some(r => r.manual);
      return `<details class="pl-card" data-id="${id}"${open.has(id) ? ' open' : ''}>
        <summary><span class="ico sm" style="--c:${ui.catColor(c.n.type)}">${ui.icon(c.n.type)}</span>
          <span class="pl-cname"><b>${esc(c.n.name)}</b><small>${esc(c.head)}</small></span>${flag}
          <span class="pl-cost">${c.monthly ? usd(c.monthly) + '<small>/mo</small>' : c.pods.length ? '<small>on Kubernetes</small>' : ''}</span></summary>
        <div class="pl-cbody">
          ${T.length > 1 ? `<label class="pl-field wide"><span>Runs on</span><select data-t="${id}">${T.map(t => `<option value="${t[0]}"${t[0] === c.tg ? ' selected' : ''}>${esc(t[1])}</option>`).join('')}</select></label>` : `<p class="pl-runs">Runs on ${esc(T[0][1])}</p>`}
          ${c.rows.length ? `<table class="pl-tab"><thead><tr><th>Setting</th><th>Suggested</th><th>Yours</th></tr></thead><tbody>${c.rows.map(r => `<tr class="${r.state}">
            <td>${esc(r.label)}</td><td class="num">${fmtV(r, r.sug)}</td>
            <td>${r.opts ? `<select data-m="${id}" data-k="${r.k}" aria-label="${esc(r.label)}">${r.opts.map(o => `<option${o === r.eff ? ' selected' : ''}>${o}</option>`).join('')}</select>` : `<input type="number" min="0" step="any" data-m="${id}" data-k="${r.k}" value="${r.manual ? r.eff : ''}" placeholder="${r.sug}" aria-label="${esc(r.label)}">`}
            ${r.state === 'under' ? '<small class="pl-note under">below the suggestion: expect lag or failures at peak</small>' : r.state === 'over' ? '<small class="pl-note over">well above the suggestion: idle capacity you pay for</small>' : ''}</td></tr>`).join('')}</tbody></table>` : ''}
          <ul class="pl-why">${c.why.map(w => `<li>${esc(w)}</li>`).join('')}</ul>
          ${c.cost.length ? `<div class="pl-costs">${c.cost.map(x => `<span>${esc(x.label)} <b>${usd(x.usd)}</b></span>`).join('')}</div>` : ''}
          ${c.pods.length ? `<div class="pl-costs">${c.pods.map(pd => `<span>${pd.count} × ${esc(pd.name)} pod <b>${pd.cpu} vCPU · ${pd.mem} GB</b></span>`).join('')}</div>` : ''}
          ${manual ? `<button class="btn" data-reset="${id}">Back to suggestions</button>` : ''}
        </div></details>`;
    }).join('');
    let k8s = '';
    if (R.k8s) {
      const k = R.k8s;
      k8s = `<section class="pl-k8s"><h3>Kubernetes cluster</h3>
        <div class="pl-row">
          <label class="pl-field"><span>Node type</span><select id="plNode">${Object.keys(PL.PRICES.EC2).map(t => { const e = PL.PRICES.EC2[t]; return `<option value="${t}"${t === p.k8sNode ? ' selected' : ''}>${t} · ${e.cpu} vCPU · ${e.mem} GB</option>`; }).join('')}</select></label>
          <div class="pl-stat"><b>${k.minN === k.maxN ? k.minN : k.minN + '–' + k.maxN}</b><small>nodes (autoscaler min–max)</small></div>
          <div class="pl-stat"><b>${k.reqCpu}</b><small>vCPU requested</small></div>
          <div class="pl-stat"><b>${k.reqMem} GB</b><small>memory requested</small></div>
          <div class="pl-stat"><b>${k.util}%</b><small>CPU packed at peak</small></div>
          <div class="pl-stat"><b>${usd(k.cost)}</b><small>per month incl. EKS</small></div>
        </div>
        ${k.tooBig.length ? `<p class="pl-note under">${esc([...new Set(k.tooBig.map(x => x.comp + ': ' + x.name))].join(', '))} do not fit on a ${esc(p.k8sNode)}. Pick a larger node type.</p>` : ''}
        <ul class="pl-why">
          <li>Each ${esc(p.k8sNode)} offers ${k.cpuA} vCPU and ${k.memA} GB to pods after the kubelet, OS reserve and daemonsets (CNI, log shipper, monitoring) take their share.</li>
          <li>Pods are packed by their <b>requests</b>, largest first. Always-on pods set the minimum node count. Batch pods (Spark batch, dbt, Airflow tasks) come and go, and the cluster autoscaler or Karpenter adds nodes for them up to the maximum.</li>
          <li>Memory limit = request for JVM pods (Spark, Flink, Kafka), because going over the limit gets a pod OOM-killed. CPU limits are best left unset, so pods can use idle CPU instead of being throttled.</li>
          <li>Bigger nodes waste less to overhead and fit large executors, but each node lost takes more pods with it. Memory-optimized r6i nodes suit shuffle-heavy Spark, and general-purpose m6i nodes suit everything else.</li>
        </ul></section>`;
    }
    const top = R.comps.slice().sort((a, b) => b.monthly - a.monthly).filter(c => c.monthly > 0);
    return `<div class="pl-total"><div><small>Estimated monthly cost</small><b>${usd(R.total)}</b></div>
      <div class="pl-split">${top.map(c => `<span>${esc(c.n.name)} <b>${usd(c.monthly)}</b></span>`).join('')}${R.k8s ? `<span>Kubernetes nodes <b>${usd(R.k8s.cost)}</b></span>` : ''}</div></div>
      ${cards}${k8s}
      <p class="pl-fine">Prices are rounded on-demand list prices in us-east-1, without discounts, data transfer or support. Use them to compare designs, not as a quote. The throughput rules of thumb assume typical JSON or Parquet records. Measure your own job and update them.</p>`;
  }

  function openPlanner() {
    if (!PLN.hasCloud()) {
      ui.modal('Resource planner', `<div class="pl-empty"><p>The planner sizes cloud components: Kafka, Spark, Flink, Snowflake, Postgres, Debezium, Airflow, S3 and Iceberg. This design has none. Simple local flows run in your browser and need no sizing.</p><p>Add one from the palette, or load a scenario that has them.</p><button class="btn primary" id="plCdc">Load the CDC scenario</button></div>`);
      document.getElementById('plCdc').onclick = () => { ui.loadScenario('cdc'); openPlanner(); };
      return;
    }
    const p = PLN.plan(), W = p.workload; p.seen = true;
    const srcs = S.nodes.filter(n => PLN.SOURCE_TYPES.includes(n.type));
    const card = ui.modal('Resource planner', `
      <details class="pl-intro"><summary>How sizing works</summary>${INTRO}</details>
      <section class="pl-work"><h3>Incoming data</h3>
        ${srcs.length ? srcs.map(srcRow).join('') : '<p class="pl-fine">No source in the design. Add Postgres or App events to set a volume.</p>'}
        <div class="pl-row">
          ${field('peak', 'Peak factor', W.peak, '×', 'Busiest hour divided by the daily average')}
          ${field('growth', 'Growth', W.growth, '% / year', 'Size for 12 months ahead')}
          ${field('retention', 'Kafka retention', W.retention, 'days', 'How long Kafka keeps events')}
          ${field('history', 'History kept', W.history, 'days', 'How much data the lake and warehouse keep')}
          ${field('runs', 'Scheduled runs', W.runs, 'per day', 'How often batch jobs and dbt run')}
          ${field('window', 'Batch window', W.window, 'min', 'How long a batch run may take')}
        </div></section>
      <div id="plResults"></div>`,
      '<button class="btn" id="plApply" title="Write partitions, executors, parallelism and warehouse size into the components">Apply to design</button>');
    card.classList.add('pl-modal');
    const res = card.querySelector('#plResults');
    const paint = () => {
      res.innerHTML = results();
      const d = PLN.diffs(); const ab = card.querySelector('#plApply');
      ab.disabled = !d.length; ab.textContent = d.length ? `Apply ${d.length} change${d.length === 1 ? '' : 's'} to design` : 'Design matches the plan';
      ab.classList.toggle('primary', !!d.length);
      ui.saveSoon();
    };
    let t = null;
    const soon = () => { clearTimeout(t); t = setTimeout(paint, 180); };
    card.querySelectorAll('.pl-src').forEach(row => row.querySelectorAll('[data-f]').forEach(inp => inp.addEventListener('input', () => {
      const s = PLN.srcCfg(PL.byId(+row.dataset.src) || PL.byId(row.dataset.src)); const f = inp.dataset.f;
      s[f] = f === 'unit' ? inp.value : Math.max(0, +inp.value || 0); if (f === 'size' && !s.size) s.size = 1; soon();
    })));
    card.querySelectorAll('[data-w]').forEach(inp => inp.addEventListener('input', () => {
      const k = inp.dataset.w, v = +inp.value;
      if (!(v > 0) && k !== 'growth') return;
      W[k] = k === 'runs' ? Math.max(1, Math.round(v)) : Math.max(0, v); soon();
    }));
    res.addEventListener('toggle', e => { const d = e.target.closest('.pl-card'); if (d) { if (d.open) open.add(d.dataset.id); else open.delete(d.dataset.id); } }, true);
    res.addEventListener('change', e => {
      const el = e.target;
      if (el.dataset.t) { p.target[el.dataset.t] = el.value; }
      else if (el.dataset.m) { const m = p.manual[el.dataset.m] = p.manual[el.dataset.m] || {}; if (el.value === '') delete m[el.dataset.k]; else m[el.dataset.k] = el.value; }
      else if (el.id === 'plNode') p.k8sNode = el.value;
      else return;
      paint();
    });
    res.addEventListener('click', e => { const b = e.target.closest('[data-reset]'); if (b) { delete p.manual[b.dataset.reset]; paint(); } });
    card.querySelector('#plApply').onclick = () => { const k = PLN.apply(); ui.toast(k ? `Applied ${k} change${k === 1 ? '' : 's'}: the simulation and the export now use the plan` : 'Nothing to apply'); paint(); };
    paint();
  }
  PL.openPlanner = openPlanner;
  const btn = document.getElementById('planBtn'); if (btn) btn.onclick = openPlanner;
})(window.PL);
