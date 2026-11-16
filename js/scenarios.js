/* Pipeline Lab — ready-made scenarios. Later: failure drills and interview challenges plug in here
 * (a scenario can carry `goal(state) -> {done, text}` checks and scripted events).
 */
window.PL = window.PL || {};
PL.SCENARIOS = [
  {
    id: 'api', name: 'First flow: API → script → table',
    blurb: 'Fetch rows from a free public API, keep N of them in Python, show them in a table.',
    tip: 'Press Run flow. The script keeps the first 10 rows from JSONPlaceholder and the table opens in the Runtime panel below. Change “Rows to read” on the script and run again.',
    nodes: [
      ['a', 'api', 0, 0, {}, 'Posts API'],
      ['s', 'script', 300, 0, {}, 'Read N rows'],
      ['t', 'table', 600, 0, {}, 'Posts table'],
      ['c', 'schedule', 0, 170, { every: 30 }, 'Every 30 s']
    ],
    links: [['a', 's'], ['s', 't'], ['c', 's']]
  },
  {
    id: 'file', name: 'File → script → table',
    blurb: 'Parse a CSV in the browser, filter it with Python, compare input and output.',
    tip: 'Press Run flow. The left table shows the raw CSV, the right one the filtered rows. Pick your own file in the File block.',
    nodes: [
      ['f', 'file', 0, 0, {}, 'cities.csv'],
      ['s', 'script', 300, 0, { limit: 100, code: '# Keep cities over 19 million people\nresult = [r for r in rows if r["population"] > 19_000_000][:limit]\n' }, 'Filter big cities'],
      ['t1', 'table', 300, 170, {}, 'Raw rows'],
      ['t2', 'table', 600, 0, {}, 'Big cities']
    ],
    links: [['f', 's'], ['f', 't1'], ['s', 't2']]
  },
  {
    id: 'cdc', name: 'CDC into lakehouse and warehouse',
    blurb: 'Postgres → Debezium → Kafka, fanned out to Spark → Iceberg and to Snowflake → dbt (SCD2) → BI.',
    tip: 'Records flow from Postgres through CDC into Kafka and fan out. Open Kafka and drop partitions to 2, then look at Spark’s idle cores. Open dbt to watch SCD2 history.',
    nodes: [
      ['pg', 'postgres', 0, 120, {}, 'Orders DB'],
      ['cdc', 'cdc', 270, 120, {}, 'Debezium'],
      ['k', 'kafka', 540, 120, {}, 'Kafka'],
      ['sp', 'spark', 820, 0, {}, 'Spark streaming'],
      ['ice', 'iceberg', 1100, 0, {}, 'Iceberg: orders'],
      ['sf', 'snowflake', 820, 250, {}, 'Snowflake'],
      ['dbt', 'dbt', 1100, 250, {}, 'dbt models'],
      ['bi', 'bi', 1380, 250, {}, 'Sales dashboard'],
      ['af', 'airflow', 820, 430, { interval: 30 }, 'Airflow']
    ],
    links: [['pg', 'cdc'], ['cdc', 'k'], ['k', 'sp'], ['sp', 'ice'], ['k', 'sf'], ['sf', 'dbt'], ['dbt', 'bi'], ['af', 'dbt']]
  },
  {
    id: 'kappa', name: 'Real-time clickstream (Kappa)',
    blurb: 'App events → Kafka → Flink windows with exactly-once → Iceberg → dashboard. One streaming path for everything.',
    tip: 'Freshness here is seconds, set by Flink’s window, watermark and checkpoint. Turn on Traffic peaks in App events, or a Hot key in Kafka.',
    nodes: [
      ['app', 'app', 0, 0, {}, 'Web & mobile'],
      ['k', 'kafka', 280, 0, { partitions: 8 }, 'Kafka: clicks'],
      ['fl', 'flink', 560, 0, { op: 'window', par: 6 }, 'Flink sessions'],
      ['ice', 'iceberg', 840, 0, { mode: 'cow', commit: 10 }, 'Iceberg: metrics'],
      ['bi', 'bi', 1120, 0, {}, 'Live dashboard']
    ],
    links: [['app', 'k'], ['k', 'fl'], ['fl', 'ice'], ['ice', 'bi']]
  },
  {
    id: 'batch', name: 'Nightly batch ELT',
    blurb: 'Airflow runs a Spark extract to S3, a COPY into Snowflake and dbt. Freshness follows the schedule.',
    tip: 'Nothing moves until Airflow fires. Watch the purple triggers, then the batch. Dashboard freshness grows into a sawtooth.',
    nodes: [
      ['pg', 'postgres', 0, 0, { rate: 300 }, 'App DB'],
      ['sp', 'spark', 280, 0, { mode: 'batch', executors: 2 }, 'Spark extract'],
      ['s3', 's3', 560, 0, {}, 'S3 raw zone'],
      ['sf', 'snowflake', 840, 0, { ingest: 'copy', size: 'S' }, 'Snowflake'],
      ['dbt', 'dbt', 1120, 0, { mat: 'table' }, 'dbt marts'],
      ['bi', 'bi', 1400, 0, { mode: 'extract', refresh: 30 }, 'Finance report'],
      ['af', 'airflow', 560, 220, { interval: 40 }, 'Airflow DAG']
    ],
    links: [['pg', 'sp'], ['sp', 's3'], ['s3', 'sf'], ['sf', 'dbt'], ['dbt', 'bi'], ['af', 'sp'], ['af', 'sf'], ['af', 'dbt']],
    // resource planner defaults: one nightly run with a 60 minute window
    plan: { workload: { srcs: {}, peak: 3, growth: 30, retention: 7, history: 365, runs: 1, window: 60 }, k8sNode: 'm6i.2xlarge', target: {}, manual: {} }
  },
  { id: 'empty', name: 'Empty canvas', blurb: 'Start from scratch.', tip: '', nodes: [], links: [] }
];
