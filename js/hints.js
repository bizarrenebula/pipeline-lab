/* Pipeline Lab — assistant: suggests the next step and helps troubleshoot.
 * Each rule looks at the design, the simulation or the last real run and returns hints:
 * { id, level: 'error' | 'warn' | 'tip', text, node?, actions?: [{ label, run }] }.
 * Add a rule by pushing a function to RULES.
 */
(function (PL) {
  'use strict';
  const S = PL.S, RT = PL.RT, ui = PL.ui;
  const $ = s => document.querySelector(s);
  const T = t => PL.TYPES[t];
  const SOURCES = ['postgres', 'app', 'api', 'file'];
  const dismissed = new Set();
  let collapsed = false, lastSig = '', peek = false; // peek: opened by hand while the Runtime panel is up
  try { collapsed = localStorage.getItem('pipelinelab.assist') === 'closed'; } catch (e) { /* no storage */ }

  const fmtS = s => s < 60 ? s.toFixed(0) + ' s' : Math.floor(s / 60) + ' min ' + Math.round(s % 60) + ' s';
  const names = types => types.map(t => T(t).label).join(', ');
  const lagOf = n => n.rt.qN / Math.max(n.rt.cap || 1, 1);
  const isBatch = n => (n.type === 'spark' && n.cfg.mode === 'batch') || n.type === 'dbt' || (n.type === 'snowflake' && n.cfg.ingest === 'copy') || (n.type === 'script' && PL.isOrchestrated(n));
  const feeders = t => Object.keys(PL.ALLOW).filter(a => PL.ALLOW[a].includes(t) && !T(a).control);

  /* actions */
  const sel = n => ({ label: 'Show', run: () => ui.select({ kind: 'node', id: n.id }) });
  const setCfg = (n, k, v, label) => ({ label, run: () => { n.cfg[k] = v; ui.syncNodes(); ui.refreshInspector(); ui.saveSoon(); ui.toast(`${n.name}: ${label.toLowerCase()}`); } });
  function addAfter(n, type, label) {
    return { label: label || 'Add ' + T(type).label, run: () => {
      let x = n.x + 290, y = n.y;
      while (S.nodes.some(o => Math.abs(o.x - x) < 200 && Math.abs(o.y - y) < 100)) y += 130;
      const m = PL.addNode(type, x, y);
      PL.addLink(n.id, m.id); ui.syncNodes(); ui.select({ kind: 'node', id: m.id }); ui.saveSoon();
    } };
  }
  const run = (n, label) => ({ label: label || 'Run flow', run: () => { ui.openDock(true); PL.runFlow(n ? [n.id] : null, n ? 'Run from ' + n.name : 'Run flow'); } });

  /* ---------- troubleshooting real runs ---------- */
  function explainError(n, msg) {
    const up = PL.upstream(n).map(u => RT.results[u.id]).find(r => r && r.rows && r.rows.length);
    const cols = up ? Object.keys(up.rows[0]).slice(0, 8).join(', ') : '';
    let m;
    if (/CORS|Request failed/i.test(msg)) return 'The browser could not reach the API. Open the URL in a new tab to check it works. If it does, the API blocks browser requests (CORS): pick a preset here, or use it in the Docker export, where CORS doesn’t apply.';
    if (/HTTP 40[13]/.test(msg)) return 'The API refused the request: it needs credentials. Put a token in Headers, e.g. {"Authorization": "Bearer <token>"}.';
    if (/HTTP 404/.test(msg)) return 'The URL path doesn’t exist on that server. Check for typos, or open it in a new tab.';
    if (/HTTP 429/.test(msg)) return 'The API is rate limiting you. Wait a minute, or slow the Scheduler down.';
    if (/HTTP 5\d\d/.test(msg)) return 'The API server failed. Try again later; this is not your flow’s fault.';
    if (/No records at path/.test(msg)) return 'Records path doesn’t point to a list. Clear it, or set it to the key that holds the array (for USGS it is "features").';
    if (/Headers must be valid JSON/.test(msg)) return 'Headers must be JSON with double quotes, e.g. {"Authorization": "Bearer abc"}.';
    if ((m = msg.match(/KeyError: '?([^'\n]+)'?/))) return `A row has no column "${m[1]}".${cols ? ' Columns coming in: ' + cols + '.' : ''} Use r.get("${m[1]}") to allow missing values.`;
    if ((m = msg.match(/NameError: name '([^']+)'/))) return `"${m[1]}" isn’t defined. The script gets two variables, rows and limit, and passes on whatever you put in result.`;
    if (/SyntaxError|IndentationError/.test(msg)) { const ln = msg.match(/line (\d+)/); return `Python can’t parse the code${ln ? ' near line ' + ln[1] : ''}. Check brackets, colons and indentation (4 spaces).`; }
    if (/TypeError/.test(msg)) return 'A value has a different type than the code expects, e.g. text where a number should be. Convert with int() or float(), or check for None.';
    if (/is not defined/.test(msg)) return 'A variable isn’t defined. In JavaScript the step gets rows and limit and must return an array.';
    if (/Could not load|pyodide/i.test(msg)) return 'Python could not be downloaded. Check your connection, or switch the script to JavaScript.';
    return 'Open the Runtime log for the full message.';
  }

  const RULES = [
    // nothing on the canvas
    () => S.nodes.length ? [] : [{ id: 'empty', level: 'tip', text: 'Start simple: open Scenarios → “First flow: API → script → table”, or drag an HTTP API from Basics onto the sheet.', actions: [{ label: 'Load first flow', run: () => ui.loadScenario('api') }] }],

    // real-run errors
    () => S.nodes.filter(n => RT.results[n.id] && RT.results[n.id].error).map(n => ({
      id: 'err-' + n.id + RT.results[n.id].error, level: 'error', node: n,
      text: `${n.name} failed: ${RT.results[n.id].error.split('\n').filter(Boolean).slice(-1)[0].slice(0, 140)}. ${explainError(n, RT.results[n.id].error)}`,
      actions: [sel(n), run(n, 'Run again')]
    })),

    // script returned nothing
    () => S.nodes.filter(n => n.type === 'script' && RT.results[n.id] && !RT.results[n.id].error && RT.results[n.id].rows.length === 0).map(n => {
      const ups = PL.upstream(n).map(u => RT.results[u.id]).filter(Boolean);
      const empty = ups.length && ups.every(r => !r.rows.length);
      return { id: 'zero-' + n.id, level: 'warn', node: n, text: empty ? `${n.name} got 0 rows from upstream, so it returned nothing. Check the step before it.` : `${n.name} returned 0 rows. A filter may remove everything, or “Rows to read” is 0.`, actions: [sel(n)] };
    }),

    // outages and broker loss
    () => S.nodes.filter(n => n.cfg.down).map(n => ({ id: 'down-' + n.id, level: 'warn', node: n, text: `${n.name} is offline (Simulate outage is on). Upstream backlog grows until it comes back.`, actions: [setCfg(n, 'down', false, 'Bring back online')] })),
    () => S.nodes.filter(n => n.type === 'kafka' && n.cfg.brokerDown).map(n => n.cfg.rf === '1'
      ? { id: 'rf1-' + n.id, level: 'error', node: n, text: `${n.name} lost a broker with replication factor 1: a third of partitions are offline and their data is unavailable. With RF 3 a follower would take over.`, actions: [setCfg(n, 'rf', '3', 'Set RF to 3'), setCfg(n, 'brokerDown', false, 'Restore broker')] }
      : { id: 'rf3-' + n.id, level: 'tip', node: n, text: `${n.name} survived the broker loss: replicas on other brokers became leaders. That’s what replication factor ${n.cfg.rf} buys you.`, actions: [setCfg(n, 'brokerDown', false, 'Restore broker')] }),

    // structure: unlinked components
    () => S.nodes.filter(n => !S.links.some(l => l.from === n.id || l.to === n.id) && S.nodes.length > 1).map(n => ({
      id: 'lone-' + n.id, level: 'tip', node: n,
      text: T(n.type).control ? `${n.name} isn’t connected. Drag from its right dot to a job it should start: ${names(PL.ALLOW[n.type])}.`
        : `${n.name} isn’t connected. Drag from the dot on its right edge to another component${PL.ALLOW[n.type].length ? ', for example ' + names(PL.ALLOW[n.type].slice(0, 3)) : ''}.`,
      actions: [sel(n)]
    })),
    () => S.nodes.filter(n => !SOURCES.includes(n.type) && !T(n.type).control && PL.upstream(n).length === 0 && S.links.some(l => l.from === n.id)).map(n => ({
      id: 'noin-' + n.id, level: 'warn', node: n, text: `${n.name} has no input, so nothing flows through it. Link one of: ${names(feeders(n.type))}.`, actions: [sel(n)]
    })),
    () => S.nodes.filter(n => SOURCES.includes(n.type) && PL.outLinks(n).length === 0 && S.links.some(l => l.to === n.id)).map(n => ({ id: 'noout-' + n.id, level: 'tip', node: n, text: `${n.name} produces data but nothing reads it.`, actions: [addAfter(n, PL.ALLOW[n.type][0])] })),
    () => S.nodes.filter(n => n.type === 'dbt' && !PL.upstream(n).some(u => u.type === 'snowflake')).map(n => ({ id: 'dbtwh-' + n.id, level: 'warn', node: n, text: `dbt runs SQL inside a warehouse. Put Snowflake in front of ${n.name}.`, actions: [sel(n)] })),

    // next steps for the simple flow
    () => S.nodes.filter(n => n.type === 'script' && PL.upstream(n).length === 0).map(n => ({ id: 'scriptin-' + n.id, level: 'tip', node: n, text: `Give ${n.name} some data: link an HTTP API or a File into it.`, actions: [sel(n)] })),
    () => S.nodes.filter(n => n.type === 'script' && PL.upstream(n).length && !PL.outLinks(n).length).map(n => ({ id: 'scriptout-' + n.id, level: 'tip', node: n, text: `Add a Table output after ${n.name} to see the rows it returns.`, actions: [addAfter(n, 'table', 'Add a table')] })),
    () => S.nodes.filter(n => (n.type === 'api' || n.type === 'file') && PL.outLinks(n).length === 0).map(n => ({ id: 'srcnext-' + n.id, level: 'tip', node: n, text: `Next: add a Python script after ${n.name} to keep the rows you need.`, actions: [addAfter(n, 'script', 'Add a script')] })),
    () => {
      const runnable = S.nodes.filter(n => ['api', 'file'].includes(n.type) && PL.outLinks(n).length);
      if (!runnable.length || S.nodes.some(n => RT.results[n.id]) || RT.running) return [];
      return [{ id: 'firstrun', level: 'tip', text: 'Your flow is ready. Press Run flow to fetch real data in the browser; results open in the Runtime panel below.', actions: [run(null)] }];
    },
    () => {
      const done = S.nodes.find(n => n.type === 'table' && RT.results[n.id] && !RT.results[n.id].error);
      const script = S.nodes.find(n => n.type === 'script' && RT.results[n.id]);
      if (!done || !script || S.nodes.some(n => n.type === 'schedule' && PL.outLinks(n).length)) return [];
      return [{ id: 'aftrun', level: 'tip', node: script, text: `It works: ${RT.results[done.id].rows.length} rows in ${done.name}. Change “Rows to read” or the code in ${script.name} and run again, or Export to Docker to run it as a container.`, actions: [sel(script), { label: 'Export', run: () => PL.openExport() }] }];
    },
    () => S.nodes.filter(n => n.type === 'schedule' && PL.outLinks(n).length && !n.cfg.on && S.nodes.some(m => RT.results[m.id])).map(n => ({ id: 'schedon-' + n.id, level: 'tip', node: n, text: `Switch ${n.name} on to rerun the flow every ${n.cfg.every} s while this tab is open.`, actions: [setCfg(n, 'on', true, 'Switch on')] })),

    // simulation health
    () => {
      if (S.t < 8) return [];
      const out = [];
      for (const n of S.nodes) {
        if (n.cfg.down || isBatch(n) || SOURCES.includes(n.type) || ['bi', 'table', 'airflow', 'schedule', 's3'].includes(n.type)) continue;
        const lag = lagOf(n);
        if (lag < 8 || n.rt.qN < 500) continue;
        let fix = '', actions = [sel(n)];
        if ((n.type === 'spark' || n.type === 'flink') && n.rt.limitedBy) {
          const k = PL.upstream(n).find(u => u.type === 'kafka');
          fix = `Its parallelism is capped by ${n.rt.limitedBy} Kafka partitions, so adding cores won’t help. Add partitions.`;
          if (k) actions = [setCfg(k, 'partitions', Math.min(24, +k.cfg.partitions * 2), `Partitions → ${Math.min(24, +k.cfg.partitions * 2)}`), sel(n)];
        } else if (n.type === 'spark') { fix = 'Give it more executors or cores, or a longer trigger so each micro-batch amortizes its overhead.'; actions = [setCfg(n, 'executors', Math.min(16, n.cfg.executors * 2), `Executors → ${Math.min(16, n.cfg.executors * 2)}`), sel(n)]; }
        else if (n.type === 'flink') { fix = 'Raise parallelism.'; actions = [setCfg(n, 'par', Math.min(16, n.cfg.par * 2), `Parallelism → ${Math.min(16, n.cfg.par * 2)}`)]; }
        else if (n.type === 'kafka') { fix = n.cfg.skew ? 'A hot key sends half the traffic to one partition; adding partitions won’t fix that. Choose a better key.' : 'Each partition takes about 500 records/s here. Add partitions.'; actions = n.cfg.skew ? [setCfg(n, 'skew', false, 'Spread keys evenly')] : [setCfg(n, 'partitions', Math.min(24, +n.cfg.partitions * 2), `Partitions → ${Math.min(24, +n.cfg.partitions * 2)}`)]; }
        else if (n.type === 'iceberg') { fix = n.cfg.mode === 'cow' ? 'Copy-on-write rewrites files on every update. Merge-on-read writes faster.' : 'Commit less often to write bigger files.'; actions = n.cfg.mode === 'cow' ? [setCfg(n, 'mode', 'mor', 'Use merge-on-read')] : [sel(n)]; }
        else if (n.type === 'snowflake') { fix = 'Use a bigger warehouse or Snowpipe Streaming.'; }
        else if (n.type === 'cdc') { fix = n.cfg.snapshot ? 'The initial snapshot is still copying rows; lag drops when it finishes.' : 'The connector can’t keep up with the write rate.'; }
        else if (n.type === 'script') { fix = 'A single-process script has hit its limit. That’s the point to move this step to Spark or Flink.'; }
        out.push({ id: 'lag-' + n.id, level: lag > 30 ? 'error' : 'warn', node: n, text: `${n.name} can’t keep up: ${fmtS(lag)} of backlog and growing. ${fix}`, actions });
      }
      return out;
    },
    () => S.nodes.filter(n => n.type === 'spark' && n.cfg.mode === 'stream' && n.rt.overrun && S.t > 10).map(n => ({ id: 'overrun-' + n.id, level: 'warn', node: n, text: `${n.name}’s micro-batches take longer than its ${n.cfg.trigger} s trigger, so batches start late.`, actions: [setCfg(n, 'trigger', Math.min(30, n.cfg.trigger * 2), `Trigger → ${Math.min(30, n.cfg.trigger * 2)} s`), sel(n)] })),
    () => S.nodes.filter(n => (n.type === 'spark' || n.type === 'flink') && n.rt.limitedBy && lagOf(n) < 8).map(n => ({ id: 'idle-' + n.id, level: 'tip', node: n, text: `${n.name} has ${n.rt.totalCores} ${n.type === 'spark' ? 'cores' : 'slots'} but only ${n.rt.limitedBy} can work, one per Kafka partition. You’re paying for idle capacity.`, actions: [sel(n)] })),
    () => S.nodes.filter(n => n.type === 'iceberg' && (n.rt.deleteFiles > 25 || PL.icebergRead(n) < 0.6) && !n.cfg.compaction).map(n => ({ id: 'compact-' + n.id, level: 'warn', node: n, text: `${n.name} has ${n.rt.files} data files and ${n.rt.deleteFiles} delete files, so readers slow down. Turn on compaction.`, actions: [setCfg(n, 'compaction', true, 'Turn on compaction')] })),
    () => S.nodes.filter(n => (n.type === 'airflow' || n.type === 'schedule') && n.rt.runs.some(r => Object.values(r.tasks || {}).some(t => t.state === 'skipped'))).map(n => ({ id: 'skip-' + n.id, level: 'warn', node: n, text: `Some ${n.name} runs were skipped because the previous run was still going. Lengthen the interval or speed up the job.`, actions: [sel(n)] })),
    () => S.nodes.filter(n => n.type === 'bi').flatMap(n => {
      if (S.t < 15) return [];
      if (n.rt.freshness == null) {
        // find where the chain stops
        const chain = []; let cur = n;
        while (cur && PL.upstream(cur).length) { cur = PL.upstream(cur)[0]; chain.push(cur); }
        const stuck = chain.find(c => c.cfg.down) || chain.find(c => !PL.upstream(c).length && !SOURCES.includes(c.type));
        return [{ id: 'bidata-' + n.id, level: 'warn', node: n, text: `No data has reached ${n.name} yet. ${stuck ? `The chain stops at ${stuck.name}${stuck.cfg.down ? ' (offline)' : ', which has no input'}.` : PL.upstream(n).length ? 'Batch steps upstream may not have run yet; watch for the next Airflow run.' : 'Link a warehouse, dbt or Iceberg to it.'}`, actions: [sel(stuck || n)] }];
      }
      if (n.rt.freshness > 60) {
        const slow = []; const seen = new Set(); const st = PL.upstream(n).slice();
        while (st.length) { const c = st.pop(); if (seen.has(c.id)) continue; seen.add(c.id); if (isBatch(c)) slow.push(`${c.name} (batch)`); else if (lagOf(c) > 3) slow.push(`${c.name} (${fmtS(lagOf(c))} lag)`); PL.upstream(c).forEach(u => st.push(u)); }
        return [{ id: 'bifresh-' + n.id, level: 'warn', node: n, text: `${n.name} shows data ${fmtS(n.rt.freshness)} old. ${slow.length ? 'Time is spent in: ' + slow.join(', ') + '.' : n.cfg.mode === 'extract' ? 'The extract refresh adds delay.' : ''}`, actions: [sel(n)] }];
      }
      return [];
    }),
    () => S.nodes.filter(n => n.type === 'snowflake' && !n.cfg.suspend && S.t > 30).map(n => ({ id: 'suspend-' + n.id, level: 'tip', node: n, text: `${n.name} never suspends, so it bills ${({ XS: 1, S: 2, M: 4, L: 8, XL: 16 })[n.cfg.size]} credits every hour even when idle.`, actions: [setCfg(n, 'suspend', true, 'Turn on auto-suspend')] })),
    () => S.nodes.filter(n => n.type === 'cdc' && n.cfg.mode === 'poll').map(n => ({ id: 'poll-' + n.id, level: 'tip', node: n, text: `${n.name} polls, so deletes never reach downstream tables and quick successive updates are lost.`, actions: [setCfg(n, 'mode', 'log', 'Switch to log-based')] })),
    () => {
      if (!PL.planner || !PL.planner.hasCloud()) return [];
      if (!S.plan || !S.plan.seen) return S.t > 20 ? [{ id: 'plan', level: 'tip', text: 'How big would this be for real? Resources sizes partitions, executors, parallelism and the warehouse from your data volume, packs pods onto Kubernetes nodes and estimates the monthly cost.', actions: [{ label: 'Open Resources', run: () => PL.openPlanner() }] }] : [];
      const d = PL.planner.diffs(); if (!d.length) return [];
      const lab = x => (PL.TYPES[x.n.type].cfg.find(c => c.k === x.k) || { label: x.k }).label;
      const ex = d.slice(0, 2).map(x => `${x.n.name} ${lab(x).toLowerCase()} ${x.from} → ${x.to}`).join(', ');
      return [{ id: 'plandiff-' + d.map(x => x.n.id + x.k + x.to).join(), level: 'tip', node: d[0].n, text: `The design differs from your resource plan in ${d.length} setting${d.length === 1 ? '' : 's'}: ${ex}${d.length > 2 ? '…' : ''}.`, actions: [{ label: 'Apply plan', run: () => { const k = PL.planner.apply(); ui.toast(`Applied ${k} change${k === 1 ? '' : 's'}`); } }, { label: 'Open Resources', run: () => PL.openPlanner() }] }];
    },
    () => S.nodes.length >= 4 && !S.nodes.some(n => RT.results[n.id]) ? [{ id: 'export', level: 'tip', text: 'When the design looks right, Export to Docker gives you a compose file and a setup script that runs it on your machine.', actions: [{ label: 'Export', run: () => PL.openExport() }] }] : []
  ];

  const RANK = { error: 0, warn: 1, tip: 2 };
  function compute() {
    let all = [];
    for (const r of RULES) { try { all = all.concat(r() || []); } catch (e) { console.error('hint rule failed', e); } }
    all = all.filter(h => !dismissed.has(h.id));
    // hints about the selected component come first within their level
    const selId = S.sel && S.sel.kind === 'node' ? S.sel.id : null;
    all.sort((a, b) => RANK[a.level] - RANK[b.level] || ((b.node && b.node.id === selId) - (a.node && a.node.id === selId)));
    return all;
  }

  const ICON = {
    error: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 7.5v5.5M12 16.5v.01"/></svg>',
    warn: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3.5l9.5 16.5h-19z"/><path d="M12 10v4.5M12 17.5v.01"/></svg>',
    tip: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 18h6M10 21h4M12 3a6 6 0 0 0-3.5 10.9c.6.5 1 1.2 1 2.1h5c0-.9.4-1.6 1-2.1A6 6 0 0 0 12 3z"/></svg>'
  };
  let current = [];
  function render() {
    const box = $('#assist'); if (!box) return;
    const hints = compute(); current = hints;
    const show = hints.slice(0, 3);
    // while the Runtime panel is open the assistant folds to its header so it does not cover the flow
    const docked = !!document.querySelector('#stage.dock-open'); if (!docked) peek = false;
    const col = collapsed || (docked && !peek);
    const sig = (col ? 'c' : 'o') + show.map(h => h.id + h.text).join('|') + hints.length;
    if (sig === lastSig) return; lastSig = sig;
    const worst = hints[0] ? hints[0].level : 'tip';
    box.className = 'assist ' + (col ? 'collapsed ' : '') + 'lv-' + worst;
    box.innerHTML = `<button class="assist-head" id="assistToggle" aria-expanded="${!col}">
        <span class="assist-ico">${ICON[hints.length ? worst : 'tip']}</span><b>Assistant</b>
        <span class="assist-count">${hints.length ? hints.length + (hints.length === 1 ? ' hint' : ' hints') : 'all good'}</span>
        <svg class="chev" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M6 9l6 6 6-6"/></svg></button>
      ${col ? '' : `<div class="assist-list">${show.length ? show.map((h, i) => `<div class="hint ${h.level}"><span class="hint-ico">${ICON[h.level]}</span><div class="hint-body"><p>${ui.esc(h.text)}</p>${h.actions && h.actions.length ? `<div class="hint-actions">${h.actions.map((a, j) => `<button class="btn" data-h="${i}" data-a="${j}">${ui.esc(a.label)}</button>`).join('')}</div>` : ''}</div><button class="hint-x" data-x="${i}" aria-label="Dismiss hint">×</button></div>`).join('') : '<p class="assist-ok">Nothing needs attention. Try a knob in any component and watch what changes.</p>'}${hints.length > 3 ? `<p class="assist-more">${hints.length - 3} more after these.</p>` : ''}</div>`}`;
    box.querySelector('#assistToggle').onclick = () => { if (docked) { peek = col; if (col) collapsed = false; lastSig = ''; render(); return; } collapsed = !collapsed; try { localStorage.setItem('pipelinelab.assist', collapsed ? 'closed' : 'open'); } catch (e) { /* ignore */ } lastSig = ''; render(); };
    box.querySelectorAll('[data-a]').forEach(b => b.onclick = () => { const h = show[+b.dataset.h]; h.actions[+b.dataset.a].run(); lastSig = ''; setTimeout(render, 50); });
    box.querySelectorAll('[data-x]').forEach(b => b.onclick = () => { dismissed.add(show[+b.dataset.x].id); lastSig = ''; render(); });
  }
  PL.hints = { compute, render, RULES };
  setInterval(render, 700);
  RT.listeners.push(() => setTimeout(render, 30));
  render();
})(window.PL);
