/* Pipeline Lab — component registry.
 * Every component type lives here: palette metadata, config schema (the game-like knobs),
 * theory content, and which other types it may feed. Simulation behaviour is in engine.js,
 * real in-browser execution in runtime.js, docker export in export.js.
 * To add a component: add an entry to PL.TYPES (+ ALLOW), then optional sim/run/export hooks.
 */
window.PL = window.PL || {};
(function (PL) {
  'use strict';

  PL.CATS = {
    basic:   { label: 'Basics',              color: 'var(--c-basic)' },
    source:  { label: 'Sources',             color: 'var(--c-source)' },
    ingest:  { label: 'Ingest & CDC',        color: 'var(--c-ingest)' },
    stream:  { label: 'Streaming',           color: 'var(--c-stream)' },
    process: { label: 'Processing',          color: 'var(--c-process)' },
    orch:    { label: 'Orchestration',       color: 'var(--c-orch)' },
    store:   { label: 'Storage & tables',    color: 'var(--c-store)' },
    wh:      { label: 'Warehouse & modeling',color: 'var(--c-wh)' },
    serve:   { label: 'Serving',             color: 'var(--c-serve)' }
  };

  PL.ICONS = {
    api: '<path d="M4 7h16M4 12h16M4 17h10"/><circle cx="18.5" cy="17" r="2.2"/>',
    file: '<path d="M6 3h8l4 4v14H6z"/><path d="M14 3v4h4"/><path d="M9 12h6M9 15.5h6"/>',
    script: '<path d="M8 8l-4 4 4 4M16 8l4 4-4 4M13.5 5.5l-3 13"/>',
    table: '<rect x="3.5" y="4.5" width="17" height="15" rx="1.5"/><path d="M3.5 9.5h17M3.5 14.5h17M9.5 9.5v10"/>',
    clock: '<circle cx="12" cy="12" r="8.5"/><path d="M12 7v5l3.5 2"/>',
    db: '<ellipse cx="12" cy="5.5" rx="7" ry="2.5"/><path d="M5 5.5v13c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5v-13"/><path d="M5 12c0 1.4 3.1 2.5 7 2.5s7-1.1 7-2.5"/>',
    app: '<rect x="7" y="2.5" width="10" height="19" rx="2"/><path d="M10.5 18.5h3"/>',
    cdc: '<path d="M4 6h9M4 10h6M4 14h9M4 18h5"/><path d="M15 9l4 3-4 3"/><path d="M19 12h-7"/>',
    log: '<rect x="3" y="4" width="18" height="4" rx="1"/><rect x="3" y="10" width="13" height="4" rx="1"/><rect x="3" y="16" width="16" height="4" rx="1"/>',
    spark: '<path d="M12 2.5l2.4 6.1 6.1 2.4-6.1 2.4L12 19.5l-2.4-6.1L3.5 11l6.1-2.4z"/>',
    wave: '<path d="M2.5 9c3-3 6-3 9.5 0s6.5 3 9.5 0"/><path d="M2.5 15c3-3 6-3 9.5 0s6.5 3 9.5 0"/>',
    dag: '<circle cx="5.5" cy="6" r="2.2"/><circle cx="5.5" cy="18" r="2.2"/><circle cx="18.5" cy="12" r="2.2"/><path d="M7.7 6.8l8.6 4.4M7.7 17.2l8.6-4.4"/>',
    bucket: '<path d="M4 6.5h16l-2 13.5H6z"/><ellipse cx="12" cy="6.5" rx="8" ry="2.2"/>',
    berg: '<path d="M2.5 13h19"/><path d="M6 13l4-7 2.5 3 2-2.5L18 13"/><path d="M7 13l2.5 7h5L17 13" stroke-dasharray="2 2"/>',
    flake: '<path d="M12 2.5v19M3.8 7.25l16.4 9.5M3.8 16.75l16.4-9.5"/><path d="M9.5 4.5L12 6.5l2.5-2M9.5 19.5l2.5-2 2.5 2"/>',
    layers: '<path d="M12 3l9 4.5-9 4.5-9-4.5z"/><path d="M3 12l9 4.5 9-4.5"/><path d="M3 16.5L12 21l9-4.5"/>',
    chart: '<path d="M4 20V10M10 20V4M16 20v-7M21 20H3"/>'
  };

  const DOWN = { k: 'down', label: 'Simulate outage', type: 'toggle', def: false,
    hint: 'Takes the component offline. Watch upstream backlog grow and downstream freshness decay.' };

  PL.TYPES = {
    /* ---------------- Basics: these really run in the browser ---------------- */
    api: {
      label: 'HTTP API', sub: 'REST endpoint', cat: 'basic', icon: 'api', runnable: true,
      cfg: [
        { k: 'preset', label: 'Preset', type: 'select', def: 'posts', opts: [
          ['posts', 'JSONPlaceholder posts (100 rows)'],
          ['comments', 'JSONPlaceholder comments (500 rows)'],
          ['users', 'JSONPlaceholder users (10 rows)'],
          ['quakes', 'USGS earthquakes, past day'],
          ['countries', 'REST Countries'],
          ['custom', 'Custom URL']],
          hint: 'Free public APIs that allow browser requests (CORS), no key needed.' },
        { k: 'url', label: 'URL', type: 'text', def: 'https://jsonplaceholder.typicode.com/posts' },
        { k: 'path', label: 'Records path', type: 'text', def: '',
          hint: 'Dot path to the array of records in the response, e.g. "features". Leave empty if the response is already an array.' },
        { k: 'headers', label: 'Headers (JSON)', type: 'text', def: '',
          hint: 'Optional, e.g. {"Authorization": "Bearer …"}. Stored only in this browser.' },
        { k: 'rate', label: 'Simulated request rate', type: 'range', min: 10, max: 2000, step: 10, unit: 'rec/s', def: 200,
          hint: 'Only affects the animation. Run uses the real API.' }
      ],
      summary: n => shortUrl(n.cfg.url)
    },
    file: {
      label: 'File', sub: 'CSV or JSON', cat: 'basic', icon: 'file', runnable: true,
      cfg: [
        { k: 'file', label: 'Data file', type: 'file', def: '',
          hint: 'Loaded in the browser only; nothing is uploaded. A sample CSV is used until you pick one.' },
        { k: 'rate', label: 'Simulated read rate', type: 'range', min: 10, max: 2000, step: 10, unit: 'rec/s', def: 150 }
      ],
      summary: n => n.cfg.fileName || 'sample: cities.csv'
    },
    script: {
      label: 'Python script', sub: 'transform step', cat: 'basic', icon: 'script', runnable: true,
      cfg: [
        { k: 'limit', label: 'Rows to read', type: 'range', min: 1, max: 500, step: 1, unit: 'rows', def: 10,
          hint: 'Available in the code as limit.' },
        { k: 'lang', label: 'Language', type: 'seg', def: 'python', opts: [['python', 'Python'], ['js', 'JavaScript']],
          hint: 'Python runs in your browser with Pyodide (first run downloads it, about 10 s).' },
        { k: 'code', label: 'Code', type: 'code', def: '' },
        { k: 'cap', label: 'Simulated throughput', type: 'range', min: 50, max: 5000, step: 50, unit: 'rec/s', def: 1500 },
        DOWN
      ],
      summary: n => `${n.cfg.lang === 'js' ? 'JavaScript' : 'Python'} · limit ${n.cfg.limit}`
    },
    table: {
      label: 'Table output', sub: 'shows rows', cat: 'basic', icon: 'table', runnable: true,
      cfg: [
        { k: 'maxRows', label: 'Rows to display', type: 'range', min: 10, max: 1000, step: 10, unit: 'rows', def: 200 }
      ],
      summary: n => 'view in Runtime panel'
    },
    schedule: {
      label: 'Scheduler', sub: 'runs on a timer', cat: 'basic', icon: 'clock', runnable: true, control: true,
      cfg: [
        { k: 'every', label: 'Run every', type: 'range', min: 5, max: 300, step: 5, unit: 's', def: 30,
          hint: 'Real runs in the browser while the schedule is on. In the export this becomes a loop or cron.' },
        { k: 'on', label: 'Schedule on', type: 'toggle', def: false }
      ],
      summary: n => `every ${n.cfg.every}s · ${n.cfg.on ? 'on' : 'off'}`
    },

    /* ---------------- Sources ---------------- */
    postgres: {
      label: 'PostgreSQL', sub: 'OLTP database', cat: 'source', icon: 'db',
      cfg: [
        { k: 'rate', label: 'Write rate', type: 'range', min: 50, max: 3000, step: 50, unit: 'rows/s', def: 400,
          hint: 'Application writes per second. Each one becomes a change in the WAL.' },
        { k: 'mix', label: 'Workload', type: 'seg', def: 'mix', opts: [['ins', 'Insert-heavy'], ['mix', 'Balanced'], ['upd', 'Update-heavy']],
          hint: 'Green dots are inserts, amber updates, red deletes.' },
        DOWN
      ],
      summary: n => `${n.cfg.rate} rows/s · ${optLabel(n, 'mix')}`
    },
    app: {
      label: 'App events', sub: 'clickstream', cat: 'source', icon: 'app',
      cfg: [
        { k: 'rate', label: 'Event rate', type: 'range', min: 100, max: 6000, step: 100, unit: 'ev/s', def: 1500 },
        { k: 'burst', label: 'Traffic peaks', type: 'toggle', def: false,
          hint: 'Rate swings ±80% on a 20 s cycle, like a launch or an evening peak.' },
        DOWN
      ],
      summary: n => `${n.cfg.rate} events/s${n.cfg.burst ? ' · peaks' : ''}`
    },

    /* ---------------- Ingest ---------------- */
    cdc: {
      label: 'Debezium CDC', sub: 'change data capture', cat: 'ingest', icon: 'cdc',
      cfg: [
        { k: 'mode', label: 'Capture mode', type: 'seg', def: 'log', opts: [['log', 'Log-based (WAL)'], ['poll', 'Query polling']],
          hint: 'Polling misses hard deletes and merges several updates of a row into one.' },
        { k: 'poll', label: 'Poll interval', type: 'range', min: 1, max: 30, step: 1, unit: 's', def: 10, show: n => n.cfg.mode === 'poll' },
        { k: 'snapshot', label: 'Run initial snapshot', type: 'toggle', def: false,
          hint: 'Copies 20,000 existing rows before streaming continues. Switches off when done.' },
        DOWN
      ],
      summary: n => n.cfg.mode === 'log' ? 'WAL · logical decoding' : `polling every ${n.cfg.poll}s`
    },

    /* ---------------- Streaming ---------------- */
    kafka: {
      label: 'Kafka', sub: 'event log', cat: 'stream', icon: 'log',
      cfg: [
        { k: 'partitions', label: 'Partitions', type: 'range', min: 1, max: 24, step: 1, def: 6,
          hint: 'The unit of parallelism: a consumer group can use at most this many readers. Real Kafka only lets you add partitions, never remove them.' },
        { k: 'rf', label: 'Replication factor', type: 'seg', def: '3', opts: [['1', '1'], ['2', '2'], ['3', '3']],
          hint: 'Copies of each partition across 3 brokers.' },
        { k: 'acks', label: 'Producer acks', type: 'seg', def: 'all', opts: [['1', 'acks=1'], ['all', 'acks=all']],
          hint: 'acks=all waits for in-sync replicas: safer, slightly slower.' },
        { k: 'skew', label: 'Hot key', type: 'toggle', def: false,
          hint: 'One key (a huge customer) sends half of all records to partition 0.' },
        { k: 'brokerDown', label: 'Take one broker down', type: 'toggle', def: false,
          hint: 'With RF 1 a third of partitions go offline. With RF 2+ a follower is elected leader after a short pause.' }
      ],
      summary: n => `${n.cfg.partitions} partitions · RF ${n.cfg.rf}`
    },

    /* ---------------- Processing ---------------- */
    spark: {
      label: 'Spark', sub: 'distributed compute', cat: 'process', icon: 'spark',
      cfg: [
        { k: 'mode', label: 'Mode', type: 'seg', def: 'stream', opts: [['stream', 'Structured Streaming'], ['batch', 'Batch']] },
        { k: 'executors', label: 'Executors', type: 'range', min: 1, max: 16, step: 1, def: 4 },
        { k: 'cores', label: 'Cores per executor', type: 'range', min: 1, max: 8, step: 1, def: 2 },
        { k: 'trigger', label: 'Trigger interval', type: 'range', min: 1, max: 30, step: 1, unit: 's', def: 5,
          show: n => n.cfg.mode === 'stream', hint: 'One micro-batch per trigger. Latency ≈ trigger + processing time.' },
        { k: 'every', label: 'Run every', type: 'range', min: 10, max: 180, step: 5, unit: 's', def: 30,
          show: n => n.cfg.mode === 'batch' && !PL.isOrchestrated(n), hint: 'Or link a Scheduler or Airflow to it.' },
        { k: 'job', label: 'Job', type: 'seg', def: 'enrich', opts: [['enrich', 'Clean & enrich'], ['agg', 'Aggregate']],
          hint: 'Aggregating shrinks the output about 10×.' },
        DOWN
      ],
      summary: n => `${n.cfg.executors}×${n.cfg.cores} cores · ${n.cfg.mode === 'stream' ? 'trigger ' + n.cfg.trigger + 's' : 'batch'}`
    },
    flink: {
      label: 'Flink', sub: 'stream processor', cat: 'process', icon: 'wave',
      cfg: [
        { k: 'par', label: 'Parallelism', type: 'range', min: 1, max: 16, step: 1, def: 4 },
        { k: 'op', label: 'Operator', type: 'seg', def: 'map', opts: [['map', 'Stateless map'], ['window', 'Window 10 s']] },
        { k: 'watermark', label: 'Watermark delay', type: 'range', min: 0, max: 30, step: 1, unit: 's', def: 5,
          show: n => n.cfg.op === 'window', hint: 'How long a window waits for late events before it fires.' },
        { k: 'eos', label: 'Exactly-once sinks', type: 'toggle', def: true,
          hint: 'Output is committed only when a checkpoint completes.' },
        { k: 'ckpt', label: 'Checkpoint interval', type: 'range', min: 2, max: 60, step: 1, unit: 's', def: 10 },
        DOWN
      ],
      summary: n => `parallelism ${n.cfg.par} · ${n.cfg.op === 'window' ? 'windowed' : 'stateless'}`
    },

    /* ---------------- Orchestration ---------------- */
    airflow: {
      label: 'Airflow', sub: 'orchestrator', cat: 'orch', icon: 'dag', control: true,
      cfg: [
        { k: 'interval', label: 'Schedule interval', type: 'range', min: 10, max: 180, step: 5, unit: 's', def: 30,
          hint: 'Stands in for @hourly or @daily, compressed so you can watch it.' },
        { k: 'paused', label: 'Pause DAG', type: 'toggle', def: false }
      ],
      summary: n => `every ${n.cfg.interval}s${n.cfg.paused ? ' · paused' : ''}`
    },

    /* ---------------- Storage ---------------- */
    s3: {
      label: 'S3', sub: 'object storage', cat: 'store', icon: 'bucket',
      cfg: [
        { k: 'format', label: 'File format', type: 'seg', def: 'parquet', opts: [['json', 'JSON'], ['csv', 'CSV'], ['parquet', 'Parquet']],
          hint: 'Readers scan Parquet about 3× faster than JSON in this model.' },
        DOWN
      ],
      summary: n => `${optLabel(n, 'format')} files`
    },
    iceberg: {
      label: 'Iceberg', sub: 'open table format', cat: 'store', icon: 'berg',
      cfg: [
        { k: 'mode', label: 'Row-level updates', type: 'seg', def: 'mor', opts: [['cow', 'Copy-on-write'], ['mor', 'Merge-on-read']] },
        { k: 'commit', label: 'Commit every', type: 'range', min: 1, max: 30, step: 1, unit: 's', def: 5,
          hint: 'Each commit is a snapshot with new files. Frequent commits mean many small files.' },
        { k: 'compaction', label: 'Compaction', type: 'toggle', def: true,
          hint: 'Rewrites small files and folds delete files into data files.' }
      ],
      summary: n => `${n.cfg.mode === 'cow' ? 'CoW' : 'MoR'} · commit ${n.cfg.commit}s`
    },

    /* ---------------- Warehouse ---------------- */
    snowflake: {
      label: 'Snowflake', sub: 'cloud warehouse', cat: 'wh', icon: 'flake',
      cfg: [
        { k: 'size', label: 'Warehouse size', type: 'seg', def: 'XS', opts: [['XS', 'XS'], ['S', 'S'], ['M', 'M'], ['L', 'L'], ['XL', 'XL']],
          hint: 'Each size doubles compute and credits per hour.' },
        { k: 'ingest', label: 'Loading', type: 'seg', def: 'stream', opts: [['stream', 'Snowpipe Streaming'], ['pipe', 'Snowpipe'], ['copy', 'COPY batch']] },
        { k: 'every', label: 'COPY every', type: 'range', min: 10, max: 180, step: 5, unit: 's', def: 60,
          show: n => n.cfg.ingest === 'copy' && !PL.isOrchestrated(n) },
        { k: 'suspend', label: 'Auto-suspend when idle', type: 'toggle', def: true,
          hint: 'Saves credits. The next query pays a short resume delay.' },
        DOWN
      ],
      summary: n => `${n.cfg.size} · ${optLabel(n, 'ingest')}`
    },
    dbt: {
      label: 'dbt', sub: 'SQL modeling', cat: 'wh', icon: 'layers',
      cfg: [
        { k: 'mat', label: 'Materialization', type: 'seg', def: 'incremental', opts: [['view', 'view'], ['table', 'table'], ['incremental', 'incremental']] },
        { k: 'scd', label: 'Customer history', type: 'seg', def: 'scd2', opts: [['none', 'None'], ['scd1', 'SCD1'], ['scd2', 'SCD2']],
          hint: 'SCD1 overwrites the old value. SCD2 keeps every version with valid_from / valid_to.' },
        { k: 'models', label: 'Models', type: 'range', min: 1, max: 60, step: 1, def: 12 },
        { k: 'every', label: 'Run every', type: 'range', min: 10, max: 180, step: 5, unit: 's', def: 30, show: n => !PL.isOrchestrated(n) },
        DOWN
      ],
      summary: n => `${n.cfg.mat} · ${n.cfg.scd === 'none' ? 'no history' : n.cfg.scd.toUpperCase()}`
    },

    /* ---------------- Serving ---------------- */
    bi: {
      label: 'BI dashboard', sub: 'reports', cat: 'serve', icon: 'chart',
      cfg: [
        { k: 'mode', label: 'Mode', type: 'seg', def: 'live', opts: [['live', 'Live query'], ['extract', 'Extract']] },
        { k: 'refresh', label: 'Extract refresh', type: 'range', min: 10, max: 300, step: 10, unit: 's', def: 60, show: n => n.cfg.mode === 'extract' }
      ],
      summary: n => n.cfg.mode === 'live' ? 'live queries' : `extract every ${n.cfg.refresh}s`
    }
  };

  /* Which types each type may send data (or triggers) to. */
  PL.ALLOW = {
    api: ['script', 'table', 'kafka', 's3'],
    file: ['script', 'table', 's3', 'postgres'],
    script: ['table', 'script', 'kafka', 's3', 'postgres', 'snowflake'],
    table: [],
    schedule: ['script', 'spark', 'dbt', 'snowflake'],
    postgres: ['cdc', 'spark', 'script', 'table'],
    app: ['kafka'],
    cdc: ['kafka'],
    kafka: ['spark', 'flink', 's3', 'iceberg', 'snowflake', 'script'],
    spark: ['s3', 'iceberg', 'snowflake', 'kafka', 'table'],
    flink: ['kafka', 'iceberg', 's3'],
    airflow: ['spark', 'dbt', 'snowflake', 'script'],
    s3: ['spark', 'snowflake', 'flink', 'script'],
    iceberg: ['spark', 'flink', 'snowflake', 'bi'],
    snowflake: ['dbt', 'bi', 'script'],
    dbt: ['bi', 'table'],
    bi: []
  };

  const HINTS = {
    'postgres>kafka': 'A database doesn’t publish its changes by itself. Put a CDC connector (Debezium) between them: it reads the WAL and produces change events.',
    'postgres>snowflake': 'Go through CDC → Kafka → Snowflake for near real time, or extract in batches with Spark. Managed tools like Fivetran do the same job.',
    'postgres>s3': 'Something has to read the database and write files: a Spark batch job or a script.',
    'kafka>dbt': 'dbt runs SQL inside a warehouse, so data has to be loaded into Snowflake first.',
    'kafka>bi': 'Dashboards query tables, not topics. Land the data in a warehouse or lakehouse first.',
    's3>bi': 'Raw files need a query engine and table metadata on top. Go through Iceberg or Snowflake.',
    'spark>dbt': 'dbt works on warehouse tables. Load into Snowflake, then model with dbt.',
    'app>spark': 'Apps send events to Kafka first. The log decouples producers from consumers and lets you replay.',
    'cdc>spark': 'Debezium runs on Kafka Connect and writes change events to Kafka topics. Consume them from Kafka.',
    'iceberg>dbt': 'dbt needs a warehouse engine. Read the Iceberg table from Snowflake, then model with dbt.'
  };

  PL.connectHint = function (a, b) {
    const A = PL.TYPES[a], B = PL.TYPES[b];
    if (HINTS[a + '>' + b]) return HINTS[a + '>' + b];
    if (A.control) return `${A.label} triggers jobs, it doesn’t move data. It can start: ${listLabels(PL.ALLOW[a])}.`;
    if (B.control) return `${B.label} is an orchestrator, data never flows into it. Draw the link from ${B.label} to the job it should start.`;
    if (['postgres', 'app', 'api', 'file'].includes(b) && !(PL.ALLOW[a] || []).includes(b)) return `${B.label} is a source; nothing feeds it here.`;
    if (!PL.ALLOW[a].length) return `${A.label} is the end of a flow.`;
    return `${A.label} can’t send to ${B.label} directly. It usually feeds: ${listLabels(PL.ALLOW[a])}.`;
  };

  PL.LINK_TEXT = {
    api: 'JSON records fetched over HTTP',
    file: 'Rows parsed from the file',
    script: 'Rows returned by the script',
    schedule: 'Trigger: starts the step on a timer',
    postgres: 'Rows and changes from the database',
    app: 'Events sent by a producer client, keyed by user',
    cdc: 'Change events: op, before, after, LSN. One topic per table, keyed by primary key.',
    kafka: 'Records read by a consumer group; progress tracked as offsets per partition',
    spark: 'Processed records written as files or commits',
    flink: 'Processed records, committed on checkpoints when exactly-once is on',
    airflow: 'Trigger: Airflow starts this task on its schedule',
    s3: 'Files (objects) read by prefix',
    iceberg: 'Table snapshots read through the catalog',
    snowflake: 'Loaded tables queried with SQL',
    dbt: 'Modeled marts: dimensions and facts'
  };

  function listLabels(arr) { return arr.map(t => PL.TYPES[t].label).join(', '); }
  function optLabel(n, k) {
    const c = PL.TYPES[n.type].cfg.find(c => c.k === k);
    const o = c && c.opts.find(o => o[0] === n.cfg[k]);
    return o ? o[1] : n.cfg[k];
  }
  function shortUrl(u) { try { const x = new URL(u); return x.host + x.pathname.replace(/\/$/, ''); } catch (e) { return u || 'no URL'; } }
  PL.optLabel = optLabel;

  PL.API_PRESETS = {
    posts: { url: 'https://jsonplaceholder.typicode.com/posts', path: '' },
    comments: { url: 'https://jsonplaceholder.typicode.com/comments', path: '' },
    users: { url: 'https://jsonplaceholder.typicode.com/users', path: '' },
    quakes: { url: 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson', path: 'features' },
    countries: { url: 'https://restcountries.com/v3.1/all?fields=name,capital,region,population', path: '' }
  };

  PL.DEFAULT_CODE = {
    python:
`# rows  : list of dicts from the upstream step
# limit : the "Rows to read" setting
# Put what you want to pass on in result.

result = rows[:limit]

# Example transform:
# result = [{"id": r["id"], "title": r["title"].upper()} for r in rows[:limit]]
`,
    js:
`// rows  : array of objects from the upstream step
// limit : the "Rows to read" setting
// Return what you want to pass on.

return rows.slice(0, limit);
`
  };

  PL.SAMPLE_CSV =
`city,country,population,lat,lon
Tokyo,Japan,37400068,35.68,139.69
Delhi,India,28514000,28.61,77.21
Shanghai,China,25582000,31.23,121.47
São Paulo,Brazil,21650000,-23.55,-46.63
Mexico City,Mexico,21581000,19.43,-99.13
Cairo,Egypt,20076000,30.04,31.24
Mumbai,India,19980000,19.08,72.88
Beijing,China,19618000,39.90,116.41
Dhaka,Bangladesh,19578000,23.81,90.41
Osaka,Japan,19281000,34.69,135.50
New York,United States,18819000,40.71,-74.01
Karachi,Pakistan,15400000,24.86,67.01
Buenos Aires,Argentina,14967000,-34.60,-58.38
Istanbul,Turkey,14751000,41.01,28.98
Lagos,Nigeria,13463000,6.52,3.38
`;
})(window.PL);
