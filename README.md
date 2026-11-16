# Pipeline Lab

A browser sandbox for data engineering, in the spirit of a network simulator: drag components onto an endless canvas, link them, and watch records flow. Then export the design to a docker-compose bundle that really runs it.

## What you can do

- **Design**: drag blocks onto the canvas and link them. Generic blocks: HTTP API, File, Python/JS script, Table, Scheduler. Platform blocks: Postgres, app events, CDC (Debezium), Kafka, Spark, Flink, Airflow, S3, Iceberg, Snowflake, dbt and BI.
- **Simulate**: animated batches of records move along the links. The config changes behaviour: partitions, executors, triggers, checkpoints, warehouse size, refresh schedules. The HUD shows throughput, lag and freshness.
- **Learn**: every block has a Learn tab covering internals, trade-offs, real-world uses, things to try and interview questions.
- **Get hints**: the assistant panel suggests the next step and explains failures, such as CORS, HTTP errors, Python tracebacks, lag, idle cores and stale dashboards. Many hints come with one-click fixes.
- **Plan resources**: open Resources and enter the incoming data volume. It suggests Kafka partitions and brokers, Spark executors, memory and shuffle partitions, Flink parallelism, Snowflake warehouse size, RDS and storage, then packs pods onto Kubernetes nodes and estimates the monthly cost. Every number shows its reasoning. Any value can be overridden, and Apply writes the plan into the design.
- **Run for real**: HTTP API, File, Script (Python via Pyodide, or JavaScript), Table and Scheduler blocks run in the browser. Results show in the Runtime panel.
- **Export to Docker**: generates `docker-compose.yml`, `setup.sh` (it prompts for credentials and connections, writes `.env`, then runs `docker compose up`), connector configs, Spark/Flink jobs, a dbt project, an Airflow DAG and data generators. Cloud designs also get `cloud/PLAN.md` and starter Kubernetes manifests (SparkApplication, FlinkDeployment, Strimzi node pool).
  - Snowflake is SaaS. Locally it is replaced by Postgres, unless you give real credentials to `setup.sh`.

## Ready-made scenarios

- **First flow**: API → script → table
- **File**: file → script → table
- **CDC**: CDC into lakehouse and warehouse, with SCD2 in dbt
- **Kappa**: real-time clickstream with Flink → Iceberg
- **Batch**: nightly batch ELT with Airflow

## Run locally

It is a static site with no build step. Open `index.html`, or serve the folder:

```bash
python3 -m http.server 8000   # then open http://localhost:8000
```

## Structure

| File | What it holds |
| --- | --- |
| `js/registry.js` | Component types, config schemas, link rules |
| `js/theory.js` | Lessons per component |
| `js/engine.js` | Simulation tick, queues, capacity, record batches |
| `js/runtime.js` | Real in-browser execution |
| `js/hints.js` | Assistant rules (next step and troubleshooting) |
| `js/planner.js`, `js/planner-ui.js` | Resource planner: sizing rules, price lists, Kubernetes packing, panel |
| `js/export.js` | Docker bundle generator |
| `js/scenarios.js` | Ready-made scenarios; failure drills and interview challenges go here later |
| `js/ui.js` | Canvas, palette, inspector, runtime dock |
