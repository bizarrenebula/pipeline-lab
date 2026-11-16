/* Pipeline Lab — built-in theory for each component.
 * Shape: { what, how[], tradeoffs[], uses[], try, interview[] }. Plain text; rendered by ui.js.
 */
window.PL = window.PL || {};
PL.THEORY = {
  api: {
    what: 'An HTTP API returns data as JSON when you send a request. It is the most common way to pull data from SaaS tools and public datasets.',
    how: [
      'A client sends GET https://host/path with optional headers (auth tokens, Accept). The server answers with a status code and a body.',
      'Large results are paginated: page/offset parameters, a cursor, or a Link header to the next page. An extractor loops until there is no next page.',
      'Rate limits (429 Too Many Requests) are normal. Good extractors back off and retry with jitter.',
      'Browsers block cross-site requests unless the server allows them (CORS). Servers and containers have no such limit.'
    ],
    tradeoffs: [
      'Easy to start, but APIs change shape without warning. Validate the schema of what you land.',
      'Full re-pulls are simple but slow and expensive. Incremental pulls (updated_since) need a reliable change timestamp.',
      'Keep raw responses (bronze layer) so you can re-parse later without calling the API again.'
    ],
    uses: ['Pulling CRM, billing or marketing data (Salesforce, Stripe, HubSpot).', 'Weather, finance and open government datasets.', 'Webhooks: the reverse direction, where the API pushes to you.'],
    try: 'Pick the USGS earthquakes preset with records path "features", run, and see nested fields flattened into columns.',
    interview: ['How would you load an API that paginates with a cursor and limits you to 100 requests a minute?', 'How do you make an API extract idempotent so a retry does not create duplicates?']
  },
  file: {
    what: 'Files are the oldest interface in data engineering: CSV exports, JSON dumps, Parquet files dropped by another team.',
    how: [
      'CSV is text: rows separated by newlines, fields by commas, with quoting rules for commas inside values. It has no types; everything is a string until you parse it.',
      'JSON keeps nesting and basic types. JSON Lines (one object per line) streams well.',
      'Parquet is columnar and compressed with a schema inside: the standard for analytics.'
    ],
    tradeoffs: [
      'CSV is universal but ambiguous (encodings, delimiters, dates). Always declare a schema when loading.',
      'Small files are cheap to write but slow to read in bulk. Batch them up.'
    ],
    uses: ['Partner data drops on SFTP or S3.', 'Manual uploads and spreadsheets.', 'Seeds and lookup tables in dbt.'],
    try: 'Load your own CSV and connect it to a script that filters rows.',
    interview: ['A CSV from a partner sometimes has an extra column. How do you make the load robust?']
  },
  script: {
    what: 'A small program that reads rows, changes them and passes them on. Most pipelines start as a script before they need a framework.',
    how: [
      'Here the script receives rows from the step before it as a list of dicts, plus the limit setting, and returns result.',
      'Python runs in your browser using Pyodide (CPython compiled to WebAssembly). In the docker export the same code runs in a python:3.12 container.',
      'Scripts should be deterministic and idempotent: running them twice on the same input gives the same output.'
    ],
    tradeoffs: [
      'A script is the fastest way to start and the easiest to understand.',
      'Single process and in-memory: it stops scaling when data no longer fits in RAM. That is when Spark, Flink or SQL in a warehouse take over.',
      'You own retries, logging and scheduling yourself. Orchestrators like Airflow add that.'
    ],
    uses: ['API extracts and light cleaning.', 'Glue between systems.', 'Prototyping logic that later moves into dbt or Spark.'],
    try: 'Change the code to keep only rows where userId is 1, then run again.',
    interview: ['When would you move a Python script to Spark?', 'How do you test a transformation script?']
  },
  table: {
    what: 'Shows the rows that reach it in the Runtime panel, as a stand-in for a real destination such as a database table or a report.',
    how: ['Columns come from the union of keys in the rows. Nested objects are flattened to dotted names like address.city.', 'In the docker export it prints a table to the logs and writes a CSV to ./output.'],
    tradeoffs: ['Showing data is the simplest data product, and often enough to answer a question.'],
    uses: ['Checking what an API really returns.', 'Debugging a transform step by step.'],
    try: 'Put two tables in a flow, one before the script and one after it, to compare input and output.',
    interview: []
  },
  schedule: {
    what: 'Starts the step it points to on a timer. A tiny version of cron or Airflow.',
    how: ['Each tick starts a run of the linked step and everything downstream of it.', 'If a run is still going when the next tick comes, the tick is skipped, like max_active_runs = 1.'],
    tradeoffs: ['A timer is simple but knows nothing about dependencies, retries or backfills. Airflow adds those.'],
    uses: ['Polling an API every few minutes.', 'Refreshing a small report.'],
    try: 'Link the Scheduler to a script and switch it on. Watch the Runtime log fill with runs.',
    interview: ['What is the difference between cron and an orchestrator?']
  },
  postgres: {
    what: 'A row-oriented OLTP database. In pipelines it is usually the system of record you extract from, and its write-ahead log (WAL) is what CDC reads.',
    how: [
      'Every change is first appended to the WAL, then applied to table pages. The WAL makes crash recovery and replication possible.',
      'Logical decoding turns WAL entries into row-level change events. A replication slot remembers how far a consumer has read, so Postgres keeps WAL until it is acknowledged.',
      'MVCC keeps old row versions for concurrent readers. An UPDATE writes a new tuple; VACUUM removes dead ones later.'
    ],
    tradeoffs: [
      'Great for many small transactional reads and writes; poor at big analytical scans, which compete with production traffic.',
      'Extracting with SELECT … WHERE updated_at > X adds load and misses hard deletes.',
      'An abandoned replication slot makes WAL pile up until the disk fills. Monitor slot lag.'
    ],
    uses: ['Orders, users, payments: the application’s source of truth.', 'Source for CDC into Kafka, a lakehouse or a warehouse.'],
    try: 'Switch the workload to Update-heavy and watch amber updates grow, then open dbt to see SCD2 history pile up.',
    interview: ['How would you replicate a 2 TB Postgres table into a lakehouse without hurting production? (Snapshot from a replica, then log-based CDC from a slot, then MERGE.)']
  },
  app: {
    what: 'Clickstream or telemetry produced by apps and services: page views, clicks, device pings. Append-only, high volume, often bursty.',
    how: [
      'Apps send events through a Kafka producer or an HTTP collector in front of Kafka.',
      'Each event has an event time, which can be well before it arrives (phones buffer while offline).',
      'The key (user_id, session_id) picks the Kafka partition, which keeps one user’s events in order.'
    ],
    tradeoffs: [
      'Per-user keys keep ordering per user, but one heavy user creates a hot partition.',
      'Use a schema registry (Avro or Protobuf) so a producer change can’t break every consumer.'
    ],
    uses: ['Product analytics and A/B tests.', 'Recommendations and personalization.', 'Fraud and abuse signals.'],
    try: 'Turn on Traffic peaks and watch Kafka consumer lag breathe in and out. Then give Spark more cores.',
    interview: ['Design a clickstream pipeline for 1M events/s with dashboards under a minute old.']
  },
  cdc: {
    what: 'Change Data Capture turns every insert, update and delete in a database into an event. Debezium is the common open-source connector; it runs on Kafka Connect.',
    how: [
      'Log-based CDC reads the database log (Postgres WAL via logical decoding, MySQL binlog). It sees every change, including deletes, with before and after values, within milliseconds.',
      'Query-based CDC polls with SELECT … WHERE updated_at > last_seen. Simple, but it misses hard deletes, merges several updates between polls, and loads the database.',
      'An initial snapshot copies existing rows, then streaming resumes from the log position recorded at snapshot start, so nothing is lost or duplicated in between.',
      'An event looks like {op: c|u|d, before, after, source: {lsn, ts_ms}}. One topic per table, keyed by primary key, so changes to one row stay in order.'
    ],
    tradeoffs: [
      'Log-based needs replication privileges and a slot, and schema changes need care.',
      'Delivery is at-least-once. Consumers must be idempotent: MERGE by key, ordered by LSN.',
      'Polling is easy to set up anywhere but can’t give true history.'
    ],
    uses: ['Feeding a lakehouse or warehouse in near real time.', 'Keeping caches and search indexes in sync.', 'The outbox pattern between microservices.'],
    try: 'Switch to Query polling. Red deletes disappear downstream, and dashboard freshness jumps to the poll interval.',
    interview: ['Log-based vs query-based CDC: what do you lose with polling?', 'How do you handle a schema change on a table that is being captured?']
  },
  kafka: {
    what: 'A distributed, replicated, append-only log. Producers append records to topics; consumers read at their own pace and remember their position (offset).',
    how: [
      'A topic is split into partitions. Each partition is an ordered log on one leader broker. Records with the same key always land in the same partition.',
      'Partitions are the unit of parallelism: in a consumer group each partition is read by at most one consumer. Six partitions means at most six busy readers.',
      'Each partition has a leader and followers (replication factor). With acks=all the producer waits until the in-sync replicas have the record.',
      'Consumer lag = log end offset − committed offset. It is the main health metric of a streaming pipeline.',
      'Records stay for the retention period whether anyone read them or not, so a new consumer can replay history. That is the basis of the Kappa architecture.'
    ],
    tradeoffs: [
      'More partitions give more parallelism but more files and slower failover, and you can’t remove them later. Adding partitions remaps keys.',
      'RF 3 with min.insync.replicas = 2 survives a broker loss without losing acknowledged data. RF 1 does not.',
      'Ordering is guaranteed only within a partition.'
    ],
    uses: ['Event backbone between services.', 'Transport for CDC.', 'Clickstream ingestion and log aggregation.', 'Input for Spark and Flink.'],
    try: 'Set partitions to 2 and look at Spark: only 2 cores are busy. Then set RF to 1 and take a broker down.',
    interview: ['How do you choose the number of partitions?', 'What happens when a broker dies with RF 1 vs RF 3?', 'How do you get exactly-once from Kafka to a database?']
  },
  spark: {
    what: 'A distributed compute engine. The driver plans the job; executors run tasks in parallel on partitions of the data. It does batch and streaming (Structured Streaming).',
    how: [
      'A job splits into stages at shuffle boundaries. Each stage runs one task per data partition. Parallel slots = executors × cores.',
      'Structured Streaming runs micro-batches: every trigger it reads new Kafka offsets, processes them as a small batch, and commits. Latency ≈ trigger + processing time.',
      'Reading Kafka, Spark makes one task per topic partition by default, so cores sit idle when partitions < cores.',
      'Checkpoints store offsets and state, so a restarted query resumes where it stopped.',
      'Batch mode reads everything since the last run, usually on an Airflow schedule, and pays a startup cost each time.'
    ],
    tradeoffs: [
      'Micro-batches give seconds of latency, not milliseconds. Flink is better there.',
      'If a batch takes longer than the trigger, the next one starts late and lag grows.',
      'More executors cost more and help only if there is parallelism to use.'
    ],
    uses: ['Big ETL and joins.', 'Lakehouse ingestion into Iceberg or Delta.', 'Feature engineering for ML.'],
    try: 'Give Spark 10 executors while Kafka has 3 partitions. Most cores stay grey. Raise partitions and watch them light up.',
    interview: ['Your Spark job is slow and executors are idle. Why? (Skew, too few partitions, small files, driver bottleneck.)', 'Micro-batch vs continuous processing?']
  },
  flink: {
    what: 'A stream processor that handles records one at a time with managed state. Lower latency than micro-batching and strong event-time support.',
    how: [
      'A job is a graph of operators split into parallel subtasks in task slots. Keyed state lives next to the operator (RocksDB for large state).',
      'Checkpoints use barriers that flow with the data to snapshot all state and source offsets consistently. On failure Flink rewinds to the last checkpoint.',
      'Exactly-once sinks use two-phase commit: output becomes visible only when the checkpoint completes, so downstream latency includes the checkpoint interval.',
      'Event-time windows fire when the watermark passes the window end. The watermark delay is how long you wait for stragglers.'
    ],
    tradeoffs: [
      'Longer watermark: more complete results, later.',
      'Shorter checkpoints: fresher exactly-once output and faster recovery, more overhead.',
      'Running stateful streaming (upgrades, rescaling state) is harder than batch.'
    ],
    uses: ['Fraud detection and real-time alerts.', 'Streaming joins and aggregations.', 'CDC enrichment and Kappa pipelines.'],
    try: 'Keep exactly-once on and raise the checkpoint interval to 30 s. Dashboard freshness now moves in steps.',
    interview: ['Explain watermarks. What happens to an event that arrives after its window fired?']
  },
  airflow: {
    what: 'A workflow orchestrator. You define DAGs of tasks in Python; the scheduler starts them on schedule and in dependency order. It coordinates work and doesn’t move data itself.',
    how: [
      'The scheduler creates one DAG run per data interval, then queues tasks whose upstream tasks succeeded.',
      'Executors (Celery, Kubernetes) run tasks on workers. Tasks usually call other systems: submit a Spark job, run dbt, issue COPY INTO.',
      'Retries, SLAs, backfills and catchup let you re-run past intervals safely when tasks are idempotent.'
    ],
    tradeoffs: [
      'Batch scheduling means data is at best one interval old.',
      'With max_active_runs = 1, a run longer than the interval delays the next one.',
      'Keep tasks idempotent so retries and backfills are safe.'
    ],
    uses: ['Nightly ELT.', 'ML training pipelines.', 'Running dbt after loads finish.'],
    try: 'Load the Nightly batch ELT scenario and change the interval. Dashboard freshness follows a sawtooth.',
    interview: ['How do you make a daily job safe to re-run for a past date?', 'Airflow vs Dagster vs a cron job?']
  },
  s3: {
    what: 'Object storage: cheap, durable, practically unlimited. The foundation of a data lake.',
    how: [
      'Objects are immutable blobs addressed by key. There are no real folders, only key prefixes.',
      'Engines list prefixes and read files in parallel. Columnar formats like Parquet let them read only the needed columns and skip row groups using min/max stats.',
      'S3 has been strongly consistent for reads after writes since 2020.'
    ],
    tradeoffs: [
      'Plain files have no transactions: concurrent readers can see half-written results, and updates mean rewriting files. Table formats fix this.',
      'Many small files slow every reader. Aim for 128–512 MB files.',
      'JSON and CSV are easy but large and slow to scan; Parquet is compressed and columnar.'
    ],
    uses: ['Raw landing zone.', 'Data lake and lakehouse storage.', 'Staging for warehouse loads.'],
    try: 'Switch to JSON and watch the read speed of the next step drop.',
    interview: ['Why does streaming ingestion cause a small-files problem, and how do you fix it?']
  },
  iceberg: {
    what: 'An open table format: metadata over Parquet files in object storage that gives you ACID tables, schema evolution and time travel, readable by Spark, Flink, Trino, Snowflake and others.',
    how: [
      'Layers: catalog → metadata.json (schema, partition spec, snapshots) → manifest list → manifests → data files. A commit writes new metadata and atomically swaps the catalog pointer.',
      'Every commit is a snapshot. Readers see one consistent snapshot, and can time travel to older ones.',
      'Copy-on-write rewrites affected data files on update or delete: slow writes, fast reads. Merge-on-read writes small delete files: fast writes, readers merge at query time.',
      'Hidden partitioning (e.g. days(ts)) means queries don’t need to know the partition column.',
      'Maintenance jobs: compaction (rewrite_data_files), expire_snapshots, remove_orphan_files.'
    ],
    tradeoffs: [
      'Frequent streaming commits create many small files and snapshots, so compaction is mandatory.',
      'MoR suits CDC tables with many updates. CoW suits read-heavy tables with rare updates.'
    ],
    uses: ['Lakehouse tables shared across engines.', 'CDC mirrors of OLTP tables.', 'Large fact tables without warehouse lock-in.'],
    try: 'Set Postgres to Update-heavy, Iceberg to merge-on-read and switch compaction off. Delete files climb and readers slow down. Switch compaction back on.',
    interview: ['Copy-on-write vs merge-on-read for a CDC table with frequent updates?', 'What exactly happens during an Iceberg commit?']
  },
  snowflake: {
    what: 'A cloud data warehouse that separates storage from compute. Data sits in micro-partitions in cloud storage; virtual warehouses are compute clusters you size and pay for by the second.',
    how: [
      'Micro-partitions (50–500 MB uncompressed, columnar) carry min/max metadata, so queries skip what they don’t need.',
      'Warehouse sizes double each step (XS = 1 credit/h, S = 2, M = 4 …) and roughly double throughput for parallel work.',
      'Auto-suspend stops an idle warehouse; the next query resumes it with a short delay and a cold cache.',
      'Loading: COPY INTO (batch, uses your warehouse), Snowpipe (file-triggered, serverless), Snowpipe Streaming (row-level, seconds of latency, used by the Kafka connector).'
    ],
    tradeoffs: [
      'Bigger warehouses are faster and cost more per hour. If a load fits a small one, going bigger only burns credits.',
      'Never suspending is fast to answer and expensive.',
      'Snowflake is SaaS, so the docker export uses Postgres as a local stand-in unless you enter your account.'
    ],
    uses: ['Central analytics warehouse.', 'BI serving.', 'ELT with dbt.', 'Sharing data between companies.'],
    try: 'Switch to COPY batch, then resize XS → L. Compare freshness and credits per hour.',
    interview: ['How would you cut a Snowflake bill without breaking SLAs?']
  },
  dbt: {
    what: 'A transformation framework. You write SELECT statements as models; dbt compiles them to SQL and runs them inside the warehouse in dependency order, with tests and docs.',
    how: [
      'ref() connects models into a DAG; dbt runs them in order.',
      'Materializations: view (computed at query time), table (rebuilt fully each run), incremental (only new or changed rows, via MERGE or INSERT).',
      'Snapshots implement SCD Type 2: each run compares source rows to the snapshot, closes changed rows (dbt_valid_to) and inserts new versions.',
      'Tests (unique, not_null, relationships) run as queries after models build.'
    ],
    tradeoffs: [
      'SCD1 overwrites, so history is lost. SCD2 keeps every version, so facts can join to the version that was true at the time, but the table grows with every change.',
      'Full table rebuilds slow down as data grows. Incremental models are faster but need a reliable unique key and late-data handling.',
      'Views cost nothing to build, but every dashboard query pays for them.',
      'Snapshot frequency limits which changes you capture: two changes between runs become one.'
    ],
    uses: ['Staging and mart layers.', 'Customer dimension history.', 'A tested metrics layer.'],
    try: 'Set materialization to table and watch run time grow with total rows. Switch to incremental.',
    interview: ['Build SCD2 for a customer dimension. How do you handle late-arriving updates?']
  },
  bi: {
    what: 'Dashboards and reports (Metabase, Looker, Tableau, Superset). The end of the pipeline, where people judge freshness and correctness.',
    how: [
      'Live mode sends SQL to the warehouse each time someone opens a view: always fresh, costs compute each time and keeps the warehouse awake.',
      'Extract mode caches data on a refresh schedule: fast and cheap to view, but only as fresh as the last refresh.',
      'Freshness = now − event time of the newest record visible on the dashboard. It adds up every stage’s latency.'
    ],
    tradeoffs: ['Live queries cost warehouse time; extracts add staleness.'],
    uses: ['Executive KPIs.', 'Operational monitoring.', 'Self-serve analytics.'],
    try: 'Compare freshness across the scenarios: streaming gets seconds, nightly batch gets the schedule interval.',
    interview: ['A dashboard is stale. How do you find which stage is late?']
  }
};
