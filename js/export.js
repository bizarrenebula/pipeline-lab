/* Pipeline Lab — export the canvas to a runnable docker compose bundle.
 * Output: docker-compose.yml, setup.sh (asks for credentials, writes .env, runs compose), README.md,
 * a shared Python image (runner, loader, generators), Spark jobs, Flink SQL, dbt project, Airflow DAG.
 */
window.PL = window.PL || {};
(function (PL) {
  'use strict';
  const S = PL.S;

  /* ---------- versions (checked against Docker Hub / PyPI / Maven, Nov 2026) ---------- */
  const IMG = {
    postgres: 'postgres:16.4', kafka: 'apache/kafka:3.8.1', kafkaUi: 'provectuslabs/kafka-ui:v0.7.2',
    connect: 'debezium/connect:2.7.3.Final', spark: 'apache/spark:3.5.3-scala2.12-java17-python3-ubuntu',
    flink: 'flink:1.19.1-scala_2.12-java17', airflow: 'apache/airflow:2.10.3-python3.12', s3: 'chrislusf/seaweedfs:3.99',
    iceberg: 'tabulario/iceberg-rest:1.6.0', metabase: 'metabase/metabase:v0.50.36', curl: 'curlimages/curl:8.10.1'
  };
  const PKG = {
    kafka: 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3',
    iceberg: 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1',
    s3a: 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-hadoop-cloud_2.12:3.5.3', pg: 'org.postgresql:postgresql:42.7.4'
  };
  const MVN = 'https://repo1.maven.org/maven2/';
  const FLINK_ICEBERG_JARS = [
    'org/apache/iceberg/iceberg-flink-runtime-1.19/1.6.1/iceberg-flink-runtime-1.19-1.6.1.jar',
    'org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar',
    'org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar',
    'org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar'
  ].map(x => MVN + x);
  const FLINK_KAFKA_JAR = 'https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar';

  /* ---------- tiny YAML writer ---------- */
  const isObj = v => v && typeof v === 'object' && !Array.isArray(v);
  function ys(s) {
    if (typeof s === 'number' || typeof s === 'boolean') return String(s);
    s = String(s);
    if (s === '' || /^\s|\s$|[:#{}\[\],&*!|>'"%@`]|^[-?]|^(true|false|yes|no|null|on|off|~)$|^[\d.+-]/i.test(s)) return JSON.stringify(s);
    return s;
  }
  function yaml(v, ind) {
    ind = ind || '';
    if (Array.isArray(v)) return v.map(x => isObj(x) ? `${ind}- ${yaml(x, ind + '  ').trimStart()}` : `${ind}- ${ys(x)}`).join('\n');
    return Object.entries(v).filter(([, x]) => x !== undefined && x !== null).map(([k, x]) => {
      const key = /^[\w.-]+$/.test(k) ? k : JSON.stringify(k);
      if (Array.isArray(x)) return x.length ? `${ind}${key}:\n${yaml(x, ind + '  ')}` : `${ind}${key}: []`;
      if (isObj(x)) return Object.keys(x).length ? `${ind}${key}:\n${yaml(x, ind + '  ')}` : `${ind}${key}: {}`;
      return `${ind}${key}: ${ys(x)}`;
    }).join('\n');
  }
  PL.yaml = yaml;

  const san = s => String(s).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '') || 'x';
  const envKey = s => san(s).toUpperCase();

  /* =====================================================================
   * Python sources shared by every generated bundle (py/ folder)
   * ===================================================================== */
  const PY = {};
  PY.common = String.raw`"""Helpers shared by the Pipeline Lab services."""
import json, os, pathlib, re, time


def log(*a):
    print(time.strftime('%H:%M:%S'), *a, flush=True)


class Trigger:
    """Decides when a batch step runs.

    With TRIGGER_DIR set (Airflow orchestrates this service) it waits for <dir>/<name>.req,
    runs, then writes <dir>/<name>.done so the Airflow task can finish. That file handshake is a
    local stand-in for operators like SparkSubmitOperator or DbtCloudRunJobOperator.
    Without it, the step runs every 'every' seconds.
    """

    def __init__(self, name, every):
        self.dir = os.environ.get('TRIGGER_DIR')
        self.name, self.every, self.last = name, every, None

    def wait(self):
        if self.dir:
            req = pathlib.Path(self.dir) / f'{self.name}.req'
            log(f'waiting for Airflow to trigger {self.name}')
            while True:
                if req.exists():
                    rid = req.read_text().strip()
                    if rid and rid != self.last:
                        self.last = rid
                        return rid
                time.sleep(1)
        if self.last is not None and self.every:
            time.sleep(self.every)
        self.last = str(time.time())
        return self.last

    def done(self, rid, ok=True):
        if self.dir:
            (pathlib.Path(self.dir) / f'{self.name}.done').write_text(f'{rid} {"ok" if ok else "failed"}')


def env(k, default=None):
    v = os.environ.get(k)
    return v if v not in (None, '') else default


def warehouse():
    """Connection to the warehouse: real Snowflake if configured, else the local Postgres stand-in."""
    if env('WAREHOUSE_MODE') == 'snowflake':
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=env('SNOWFLAKE_ACCOUNT'), user=env('SNOWFLAKE_USER'), password=env('SNOWFLAKE_PASSWORD'),
            role=env('SNOWFLAKE_ROLE'), warehouse=env('SNOWFLAKE_WAREHOUSE'),
            database=env('SNOWFLAKE_DATABASE'), schema=env('SNOWFLAKE_SCHEMA'))
        return conn, 'snowflake'
    import psycopg
    for i in range(60):
        try:
            return psycopg.connect(host='warehouse', dbname=env('WH_DB', 'analytics'), user=env('WH_USER', 'analyst'),
                                   password=env('WH_PASSWORD', 'analyst'), autocommit=True), 'postgres'
        except Exception as e:
            log('warehouse not ready yet:', e)
            time.sleep(2)
    raise SystemExit('warehouse unreachable')


def insert_raw(conn, kind, table, rows, source):
    """Land rows as JSON documents in raw.<table> (Postgres) or <schema>.<table> (Snowflake)."""
    if not rows:
        return
    table = re.sub(r'[^a-z0-9_]', '_', table.lower())
    cur = conn.cursor()
    if kind == 'snowflake':
        cur.execute(f'CREATE TABLE IF NOT EXISTS {table} (payload VARIANT, source STRING, loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())')
        for i in range(0, len(rows), 500):
            chunk = rows[i:i + 500]
            sql = (f'INSERT INTO {table} (payload, source) SELECT PARSE_JSON(column1), column2 FROM VALUES '
                   + ','.join(['(%s, %s)'] * len(chunk)))
            cur.execute(sql, [x for r in chunk for x in (json.dumps(r, default=str), source)])
    else:
        cur.execute('CREATE SCHEMA IF NOT EXISTS raw')
        cur.execute(f'CREATE TABLE IF NOT EXISTS raw.{table} (payload JSONB, source TEXT, loaded_at TIMESTAMPTZ DEFAULT now())')
        cur.executemany(f'INSERT INTO raw.{table} (payload, source) VALUES (%s, %s)',
                        [(json.dumps(r, default=str), source) for r in rows])


def s3():
    import boto3
    return boto3.client('s3', endpoint_url=env('S3_ENDPOINT'), region_name=env('AWS_REGION', 'us-east-1'),
                        aws_access_key_id=env('AWS_ACCESS_KEY_ID'), aws_secret_access_key=env('AWS_SECRET_ACCESS_KEY'))


def bucket(name):
    """Logical bucket name (raw / warehouse) -> real bucket name from .env."""
    return env('S3_BUCKET_' + name.upper(), name)


def flatten(o, pre='', out=None, depth=0):
    out = {} if out is None else out
    if not isinstance(o, dict):
        out[pre or 'value'] = json.dumps(o) if isinstance(o, list) else o
        return out
    for k, v in o.items():
        key = f'{pre}.{k}' if pre else k
        if isinstance(v, dict) and depth < 2:
            flatten(v, key, out, depth + 1)
        elif isinstance(v, list):
            out[key] = ', '.join(map(str, v)) if all(not isinstance(x, (dict, list)) for x in v) else json.dumps(v)
        else:
            out[key] = v
    return out


def kafka_consumer(topics, group, **kw):
    from kafka import KafkaConsumer
    for i in range(60):
        try:
            return KafkaConsumer(*topics, bootstrap_servers='kafka:9092', group_id=group, auto_offset_reset='earliest',
                                 enable_auto_commit=False, metadata_max_age_ms=5000,
                                 value_deserializer=lambda b: json.loads(b) if b else None, **kw)
        except Exception as e:
            log('kafka not ready yet:', e)
            time.sleep(2)
    raise SystemExit('kafka unreachable')


def kafka_producer(**kw):
    from kafka import KafkaProducer
    for i in range(60):
        try:
            return KafkaProducer(bootstrap_servers='kafka:9092', value_serializer=lambda v: json.dumps(v, default=str).encode(),
                                 key_serializer=lambda k: str(k).encode() if k is not None else None, **kw)
        except Exception as e:
            log('kafka not ready yet:', e)
            time.sleep(2)
    raise SystemExit('kafka unreachable')
`;

  PY.runner = String.raw`"""Runs the simple blocks of the design (HTTP API, File, Python script, Table) as a small DAG,
exactly like the Run button in Pipeline Lab. Tables print to the log and are saved to ./output/*.csv."""
import csv, io, json, os, pathlib, sys
import requests
from tabulate import tabulate
from common import Trigger, env, flatten, insert_raw, kafka_consumer, kafka_producer, log, s3, bucket, warehouse

FLOW = json.load(open(sys.argv[1]))
OUT = pathlib.Path('/output')
OUT.mkdir(parents=True, exist_ok=True)


def dig(o, path):
    for p in [x for x in (path or '').split('.') if x]:
        o = o.get(p) if isinstance(o, dict) else None
    return o


def coerce(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return float(v)
        except (TypeError, ValueError):
            return v


def step_api(s):
    raw = env(s.get('headers_env') or '', '')
    headers = json.loads(raw) if raw.strip() else {}
    r = requests.get(s['url'], headers=headers, timeout=60)
    r.raise_for_status()
    data = dig(r.json(), s.get('path'))
    if isinstance(data, dict):
        k = next((k for k, v in data.items() if isinstance(v, list)), None)
        data = data[k] if k else [data]
    if not isinstance(data, list):
        raise ValueError(f"no records at path {s.get('path')!r}")
    return [flatten(x) for x in data]


def step_file(s):
    p = s['path']
    text = open(p, encoding='utf-8').read()
    if p.endswith(('.jsonl', '.ndjson')):
        return [flatten(json.loads(l)) for l in text.splitlines() if l.strip()]
    if p.endswith('.json') or text.lstrip()[:1] in '[{':
        j = json.loads(text)
        if isinstance(j, dict):
            j = next((v for v in j.values() if isinstance(v, list)), [j])
        return [flatten(x) for x in j]
    return [{k: coerce(v) for k, v in row.items()} for row in csv.DictReader(io.StringIO(text))]


def step_script(s, rows):
    ns = {'rows': rows, 'limit': s['limit'], '__name__': '__step__'}
    code = open(s['code'], encoding='utf-8').read()
    exec(compile(code, s['code'], 'exec'), ns)
    r = ns.get('result')
    if r is None:
        r = rows
    if hasattr(r, 'to_dict'):
        r = r.to_dict(orient='records')
    if isinstance(r, dict):
        r = [r]
    return list(r)


def step_table(s, rows):
    rows = rows[: s['max_rows']]
    cols = []
    for r in rows[:200]:
        for k in r:
            if k not in cols:
                cols.append(k)
    print(f"\n=== {s['name']}: {len(rows)} rows ===", flush=True)
    if rows:
        print(tabulate([[str(r.get(c, ''))[:40] for c in cols] for r in rows[:50]], headers=cols, tablefmt='github'), flush=True)
        if len(rows) > 50:
            print(f'... {len(rows) - 50} more rows in output/{s["slug"]}.csv', flush=True)
    with open(OUT / f"{s['slug']}.csv", 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)
    return rows


def read_sources(s):
    rows = []
    for src in s.get('sources', []):
        if src['kind'] == 'kafka':
            c = kafka_consumer(src['topics'], 'runner-' + s['slug'], consumer_timeout_ms=8000)
            for m in c:
                if m.value is not None:
                    rows.append(flatten(m.value))
                if len(rows) >= src.get('max', 1000):
                    break
            c.commit()
            c.close()
        elif src['kind'] == 'oltp':
            import psycopg
            with psycopg.connect(host=src['host'], dbname=env('PG_DB'), user=env('PG_USER'), password=env('PG_PASSWORD')) as conn:
                cur = conn.execute(src['query'])
                names = [d.name for d in cur.description]
                rows += [dict(zip(names, r)) for r in cur.fetchall()]
        elif src['kind'] == 'warehouse':
            conn, kind = warehouse()
            cur = conn.cursor()
            cur.execute(src['query'])
            names = [d[0].lower() for d in cur.description]
            rows += [dict(zip(names, r)) for r in cur.fetchall()]
    return rows


def write_sinks(s, rows):
    for snk in s.get('sinks', []):
        if snk['kind'] == 'kafka':
            p = kafka_producer()
            for r in rows:
                p.send(snk['topic'], r)
            p.flush()
        elif snk['kind'] == 'warehouse':
            conn, kind = warehouse()
            insert_raw(conn, kind, snk['table'], rows, s['slug'])
        elif snk['kind'] == 's3':
            import time
            body = '\n'.join(json.dumps(r, default=str) for r in rows).encode()
            s3().put_object(Bucket=bucket(snk['bucket']), Key=f"{snk['prefix']}/{int(time.time() * 1000)}.jsonl", Body=body)
        elif snk['kind'] == 'oltp':
            import psycopg
            with psycopg.connect(host=snk['host'], dbname=env('PG_DB'), user=env('PG_USER'), password=env('PG_PASSWORD'), autocommit=True) as conn:
                conn.execute(f"CREATE TABLE IF NOT EXISTS {snk['table']} (id BIGSERIAL PRIMARY KEY, doc JSONB, loaded_at TIMESTAMPTZ DEFAULT now())")
                with conn.cursor() as cur:
                    cur.executemany(f"INSERT INTO {snk['table']} (doc) VALUES (%s)", [(json.dumps(r, default=str),) for r in rows])


def run_once():
    outs, ok = {}, True
    for s in FLOW['steps']:
        if any(d not in outs for d in s['deps']):
            log(f"[{s['name']}] skipped: an upstream step failed")
            ok = False
            continue
        try:
            ins = [r for d in s['deps'] for r in outs[d]] + read_sources(s)
            t = s['type']
            rows = (step_api(s) if t == 'api' else step_file(s) if t == 'file'
                    else step_script(s, ins) if t == 'script' else step_table(s, ins) if t == 'table' else ins)
            write_sinks(s, rows)
            outs[s['id']] = rows
            log(f"[{s['name']}] {len(rows)} rows")
        except Exception as e:
            ok = False
            log(f"[{s['name']}] ERROR: {type(e).__name__}: {e}")
    return ok


if __name__ == '__main__':
    every = FLOW.get('every')
    trig = Trigger('flow-runner', every)
    if not every and not env('TRIGGER_DIR'):
        ok = run_once()
        log('Finished. CSV copies of every table are in ./output')
        sys.exit(0 if ok else 1)
    while True:
        rid = trig.wait()
        trig.done(rid, run_once())
`;

  PY.loader = String.raw`"""Loads data into the warehouse (or S3): consumes Kafka topics or new files under an S3 prefix
and lands them as JSON documents in raw tables. Commits Kafka offsets only after a successful write
(at-least-once), which is why downstream models deduplicate by key."""
import io, json, os, re, sys, time
from common import Trigger, env, insert_raw, kafka_consumer, log, s3, bucket, warehouse

CFG = json.load(open(sys.argv[1]))
NAME = CFG['name']


def table_for(name):
    return 'raw_' + re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')


def read_s3_new(src, seen):
    cli, rows = s3(), {}
    b = bucket(src['bucket'])
    try:
        pages = cli.get_paginator('list_objects_v2').paginate(Bucket=b, Prefix=src['prefix'] + '/')
        keys = [o['Key'] for p in pages for o in p.get('Contents', [])]
    except Exception as e:
        log('cannot list', b, e)
        return rows
    for k in sorted(keys):
        if k in seen or k.endswith('/') or '_spark_metadata' in k or k.endswith('.crc') or '/_' in k:
            continue
        body = cli.get_object(Bucket=b, Key=k)['Body'].read()
        t = table_for(src['prefix'])
        if k.endswith('.parquet'):
            import pyarrow.parquet as pq
            recs = pq.read_table(io.BytesIO(body)).to_pylist()
        elif k.endswith('.csv'):
            import csv
            recs = list(csv.DictReader(io.StringIO(body.decode())))
        else:
            recs = [json.loads(l) for l in body.decode().splitlines() if l.strip()]
        rows.setdefault(t, []).extend(recs)
        seen.add(k)
    return rows


def write(batches):
    if CFG['target'] == 's3':
        cli = s3()
        for t, rows in batches.items():
            if rows:
                body = '\n'.join(json.dumps(r, default=str) for r in rows).encode()
                cli.put_object(Bucket=bucket(CFG['bucket']), Key=f"{CFG['prefix']}/{t}/{int(time.time() * 1000)}.jsonl", Body=body)
    else:
        conn, kind = warehouse()
        for t, rows in batches.items():
            insert_raw(conn, kind, t, rows, NAME)
    n = sum(len(r) for r in batches.values())
    if n:
        log(f'loaded {n} rows into', ', '.join(f'{t} ({len(r)})' for t, r in batches.items() if r))


def main():
    topics = [t for s in CFG['sources'] if s['kind'] == 'kafka' for t in s['topics']]
    files = [s for s in CFG['sources'] if s['kind'] == 's3']
    consumer = kafka_consumer(topics, NAME) if topics else None
    seen = set()
    trig = Trigger(NAME, CFG.get('every'))
    batched = CFG.get('mode') == 'batch'
    flush_every = CFG.get('flush_every', 2)
    while True:
        rid = trig.wait() if batched else None
        buf, last, idle = {}, time.time(), 0
        while True:
            if consumer:
                polled = consumer.poll(timeout_ms=1000, max_records=5000)
                got = 0
                for tp, msgs in polled.items():
                    for m in msgs:
                        if m.value is not None:
                            buf.setdefault(table_for(m.topic), []).append(m.value)
                            got += 1
                idle = idle + 1 if got == 0 else 0
            else:
                time.sleep(1)
                idle += 1
            for src in files:
                for t, recs in read_s3_new(src, seen).items():
                    buf.setdefault(t, []).extend(recs)
            due = (time.time() - last >= flush_every) if not batched else idle >= 3
            if due:
                write(buf)
                if consumer:
                    consumer.commit()
                buf, last = {}, time.time()
                if batched:
                    break
        trig.done(rid, True)


if __name__ == '__main__':
    main()
`;

  PY.pgwriter = String.raw`"""Simulates an application writing to its OLTP database: inserts, updates and deletes
on orders and customers. Every change lands in the WAL, which Debezium reads."""
import os, random, time
import psycopg
from common import env, log

RATE = float(env('RATE', '20'))
P_UPD, P_DEL = {'ins': (0.08, 0.02), 'mix': (0.35, 0.08), 'upd': (0.7, 0.05)}[env('MIX', 'mix')]
CITIES = ['Lisbon', 'Berlin', 'Austin', 'Osaka', 'Krakow', 'Toronto', 'Nairobi', 'Lyon']
STATUS = ['paid', 'shipped', 'delivered', 'refunded']

for i in range(60):
    try:
        conn = psycopg.connect(host=env('PG_HOST'), dbname=env('PG_DB'), user=env('PG_USER'), password=env('PG_PASSWORD'), autocommit=True)
        break
    except Exception as e:
        log('postgres not ready yet:', e)
        time.sleep(2)
else:
    raise SystemExit('postgres unreachable')

counts, t_log = {'insert': 0, 'update': 0, 'delete': 0}, time.time()
while True:
    t0 = time.time()
    for _ in range(max(1, round(RATE / 5))):
        x = random.random()
        if x < P_DEL:
            conn.execute('DELETE FROM orders WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1)')
            counts['delete'] += 1
        elif x < P_DEL + P_UPD:
            if random.random() < 0.25:
                conn.execute('UPDATE customers SET city = %s, updated_at = now() WHERE id = %s', (random.choice(CITIES), random.randint(1, 50)))
            else:
                conn.execute('UPDATE orders SET status = %s, amount = amount + 1, updated_at = now() '
                             'WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1)', (random.choice(STATUS),))
            counts['update'] += 1
        else:
            conn.execute("INSERT INTO orders (customer_id, amount, status) VALUES (%s, %s, 'new')",
                         (random.randint(1, 50), round(random.uniform(5, 300), 2)))
            counts['insert'] += 1
    if time.time() - t_log > 10:
        log('writes in the last 10 s:', counts)
        counts, t_log = {k: 0 for k in counts}, time.time()
    time.sleep(max(0, 0.2 - (time.time() - t0)))
`;

  PY.events = String.raw`"""Produces clickstream events to Kafka, keyed by user_id."""
import random, time, uuid
from common import env, kafka_producer, log

RATE, TOPIC, HOT = float(env('RATE', '50')), env('TOPIC'), env('HOT_KEY') == '1'
p = kafka_producer(linger_ms=20)
EVENTS, PAGES = ['page_view', 'click', 'add_to_cart', 'search', 'checkout'], ['/', '/shop', '/item/42', '/cart', '/search']
sent, t_log = 0, time.time()
while True:
    t0 = time.time()
    for _ in range(max(1, round(RATE / 5))):
        user = 'u1' if HOT and random.random() < 0.5 else f'u{random.randint(1, 5000)}'
        p.send(TOPIC, {'event_id': str(uuid.uuid4()), 'user_id': user, 'event': random.choice(EVENTS),
                       'page': random.choice(PAGES), 'ts': int(time.time() * 1000)}, key=user)
        sent += 1
    if time.time() - t_log > 10:
        log(f'sent {sent} events to {TOPIC}')
        sent, t_log = 0, time.time()
    time.sleep(max(0, 0.2 - (time.time() - t0)))
`;

  PY.poller = String.raw`"""Query-based CDC: polls tables for rows with updated_at newer than the last seen value.
Simple, but it cannot see hard deletes and merges several updates between polls into one."""
import time
import psycopg
from common import env, kafka_producer, log

PREFIX, EVERY = env('TOPIC_PREFIX'), float(env('POLL_SECONDS', '10'))
last = {'orders': '1970-01-01', 'customers': '1970-01-01'}
p = kafka_producer()
while True:
    try:
        with psycopg.connect(host=env('PG_HOST'), dbname=env('PG_DB'), user=env('PG_USER'), password=env('PG_PASSWORD')) as conn:
            for t in last:
                rows = conn.execute(f'SELECT row_to_json(x), x.updated_at FROM (SELECT * FROM {t} WHERE updated_at > %s ORDER BY updated_at LIMIT 10000) x', (last[t],)).fetchall()
                for doc, ts in rows:
                    p.send(f'{PREFIX}.{t}', doc, key=doc['id'])
                    last[t] = ts
                if rows:
                    log(f'{t}: {len(rows)} changed rows')
        p.flush()
    except Exception as e:
        log('poll failed:', e)
    time.sleep(EVERY)
`;

  PY.init = String.raw`"""One-off setup: open the trigger folder to Airflow and create S3 buckets."""
import os, time
from common import env, log, s3

if os.path.isdir('/triggers'):
    os.chmod('/triggers', 0o777)
for b in [x for x in env('BUCKETS', '').split(',') if x]:
    for i in range(60):
        try:
            cli = s3()
            if b not in [x['Name'] for x in cli.list_buckets().get('Buckets', [])]:
                cli.create_bucket(Bucket=b)
            log('bucket ready:', b)
            break
        except Exception as e:
            log('S3 not ready yet:', e)
            time.sleep(2)
log('init done')
`;

  PY.spark = String.raw`"""Spark job generated by Pipeline Lab. Config is in the JSON file passed as argument.
Structured Streaming in both modes: streaming uses a processing-time trigger, batch mode uses
trigger(availableNow=True), which processes everything new since the last checkpoint and stops."""
import json, os, pathlib, sys, time
sys.path.insert(0, '/py')
from pyspark.sql import SparkSession, functions as F
from common import Trigger, env, log

CFG = json.load(open(sys.argv[1]))
NAME = CFG['name']
b = SparkSession.builder.appName(NAME).config('spark.sql.shuffle.partitions', '4')
ep = env('S3_ENDPOINT')
b = (b.config('spark.hadoop.fs.s3a.access.key', env('AWS_ACCESS_KEY_ID', ''))
      .config('spark.hadoop.fs.s3a.secret.key', env('AWS_SECRET_ACCESS_KEY', ''))
      .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'))
# S3 has no atomic rename, so the classic "write to _temporary, then rename" commit is slow and breaks on
# S3-compatible stores. The S3A directory committer uploads each task's files with multipart uploads instead.
if any(x['kind'] == 'files' for x in CFG['sinks']):
  b = (b.config('spark.hadoop.fs.s3a.committer.name', 'directory')
      .config('spark.hadoop.fs.s3a.committer.staging.conflict-mode', 'append')
      .config('spark.sql.sources.commitProtocolClass', 'org.apache.spark.internal.io.cloud.PathOutputCommitProtocol')
      .config('spark.sql.parquet.output.committer.class', 'org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter'))
if ep:
    b = (b.config('spark.hadoop.fs.s3a.endpoint', ep).config('spark.hadoop.fs.s3a.path.style.access', 'true')
          .config('spark.hadoop.fs.s3a.connection.ssl.enabled', str(ep.startswith('https')).lower()))
if CFG.get('iceberg'):
    b = (b.config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
          .config('spark.sql.catalog.lake', 'org.apache.iceberg.spark.SparkCatalog')
          .config('spark.sql.catalog.lake.type', 'rest')
          .config('spark.sql.catalog.lake.uri', 'http://iceberg-rest:8181')
          .config('spark.sql.catalog.lake.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
          .config('spark.sql.catalog.lake.warehouse', f"s3://{env('S3_BUCKET_WAREHOUSE', 'warehouse')}/"))
    if ep:
        b = b.config('spark.sql.catalog.lake.s3.endpoint', ep).config('spark.sql.catalog.lake.s3.path-style-access', 'true')
spark = b.getOrCreate()
spark.sparkContext.setLogLevel('WARN')
SRC = CFG['source']
CHK = f'/chk/{NAME}'


def bucket_path(p):
    # s3a://raw/... -> real bucket name from .env
    if p.startswith('s3a://raw/'):
        return p.replace('s3a://raw/', f"s3a://{env('S3_BUCKET_RAW', 'raw')}/", 1)
    return p


def source_stream():
    if SRC['kind'] == 'kafka':
        df = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'kafka:9092')
              .option('subscribe', ','.join(SRC['topics'])).option('startingOffsets', 'earliest')
              .option('failOnDataLoss', 'false').load())
        df = df.select(F.col('key').cast('string').alias('key'), F.col('value').cast('string').alias('value'),
                       'topic', 'partition', 'offset', F.col('timestamp').alias('event_time'))
    else:
        spark.conf.set('spark.sql.streaming.schemaInference', 'true')
        df = spark.readStream.format(SRC['format']).load(bucket_path(SRC['path']))
        if 'event_time' not in df.columns:
            df = df.withColumn('event_time', F.current_timestamp())
    return transform(df)


def transform(df):
    if CFG['job'] == 'agg':
        return (df.withWatermark('event_time', '10 seconds').groupBy(F.window('event_time', '10 seconds')).count()
                  .select(F.col('window.start').alias('window_start'), F.col('window.end').alias('window_end'), 'count'))
    return df.withColumn('processed_at', F.current_timestamp())


batches = {'n': 0}


def write_batch(df, batch_id):
    n = df.count()
    if n == 0:
        return
    for s in CFG['sinks']:
        k = s['kind']
        if k == 'iceberg':
            spark.sql('CREATE NAMESPACE IF NOT EXISTS lake.db')
            try:
                df.writeTo(s['table']).append()
            except Exception as e:
                if 'not found' not in str(e).lower() and 'TABLE_OR_VIEW_NOT_FOUND' not in str(e):
                    raise
                mode = 'merge-on-read' if s['mode'] == 'mor' else 'copy-on-write'
                (df.writeTo(s['table']).using('iceberg').tableProperty('format-version', '2')
                   .tableProperty('write.update.mode', mode).tableProperty('write.delete.mode', mode)
                   .tableProperty('write.merge.mode', mode).create())
            batches['n'] += 1
            if s.get('compaction') and batches['n'] % 10 == 0:
                short = s['table'].split('.', 1)[1]
                spark.sql(f"CALL lake.system.rewrite_data_files(table => '{short}')").show()
                spark.sql(f"CALL lake.system.expire_snapshots(table => '{short}', retain_last => 20)").show()
        elif k == 'files':
            # one folder per batch: no overwrite conflicts, and readers can pick up new folders incrementally
            df.write.mode('append').format(s['format']).save(bucket_path(s['path']) + f'batch_id={batch_id}/')
        elif k == 'warehouse':
            if env('WAREHOUSE_MODE') == 'snowflake':
                opts = dict(sfURL=f"{env('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com", sfUser=env('SNOWFLAKE_USER'),
                            sfPassword=env('SNOWFLAKE_PASSWORD'), sfDatabase=env('SNOWFLAKE_DATABASE'),
                            sfSchema=env('SNOWFLAKE_SCHEMA'), sfWarehouse=env('SNOWFLAKE_WAREHOUSE'), sfRole=env('SNOWFLAKE_ROLE'))
                df.write.format('net.snowflake.spark.snowflake').options(**opts).option('dbtable', s['table']).mode('append').save()
            else:
                df.write.jdbc(f"jdbc:postgresql://warehouse:5432/{env('WH_DB', 'analytics')}", f"raw.{s['table']}", mode='append',
                              properties={'user': env('WH_USER', 'analyst'), 'password': env('WH_PASSWORD', 'analyst'), 'driver': 'org.postgresql.Driver'})
        elif k == 'kafka':
            (df.select(F.to_json(F.struct(*df.columns)).alias('value')).write.format('kafka')
               .option('kafka.bootstrap.servers', 'kafka:9092').option('topic', s['topic']).save())
        elif k == 'console':
            df.show(20, truncate=False)
    log(f'batch {batch_id}: {n} rows')


def jdbc_batch(state=pathlib.Path(f'/chk/{NAME}.watermark')):
    last = state.read_text() if state.exists() else '1970-01-01'
    q = f"(SELECT * FROM {SRC['table']} WHERE updated_at > '{last}') t"
    df = (spark.read.format('jdbc').option('url', f"jdbc:postgresql://{SRC['host']}:5432/{env('PG_DB')}")
          .option('dbtable', q).option('user', env('PG_USER')).option('password', env('PG_PASSWORD'))
          .option('driver', 'org.postgresql.Driver').load())
    if df.count():
        mx = df.agg(F.max('updated_at')).first()[0]
        write_batch(transform(df.withColumn('event_time', F.col('updated_at'))), int(time.time()))
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text(str(mx))


pathlib.Path('/chk').mkdir(exist_ok=True)
if CFG['mode'] == 'stream' and SRC['kind'] != 'jdbc':
    (source_stream().writeStream.foreachBatch(write_batch).option('checkpointLocation', CHK)
        .trigger(processingTime=f"{CFG['trigger']} seconds").start().awaitTermination())
else:
    trig = Trigger(NAME, CFG.get('every') or CFG.get('trigger'))
    while True:
        rid = trig.wait()
        ok = True
        try:
            if SRC['kind'] == 'jdbc':
                jdbc_batch()
            else:
                (source_stream().writeStream.foreachBatch(write_batch).option('checkpointLocation', CHK)
                    .trigger(availableNow=True).start().awaitTermination())
        except Exception as e:
            ok = False
            log('run failed:', e)
        trig.done(rid, ok)
`;

  PY.dbtRun = String.raw`"""Runs 'dbt build' on a schedule or when Airflow triggers it."""
import os, subprocess, sys, time
sys.path.insert(0, '/py')
from common import Trigger, env, log

trig = Trigger(env('SERVICE_NAME', 'dbt'), float(env('EVERY', '30')))
while True:
    rid = trig.wait()
    t0 = time.time()
    # dbt build runs models, snapshots (SCD2) and tests in dependency order
    rc = subprocess.run(['dbt', 'build'], cwd='/dbt').returncode
    log(f'dbt finished in {time.time() - t0:.1f}s with exit code {rc}')
    trig.done(rid, rc == 0)
`;

  /* =====================================================================
   * Build the bundle from the current design
   * ===================================================================== */
  PL.buildExport = function () {
    const nodes = S.nodes.slice(), F = {}, services = {}, volumes = {}, notes = [], urls = [], prompts = [];
    const up = n => PL.upstream(n), down = n => PL.outLinks(n).filter(l => !PL.isControlLink(l)).map(l => PL.byId(l.to));
    const ctrlBy = n => PL.inLinks(n).filter(l => PL.isControlLink(l)).map(l => PL.byId(l.from));
    const airflowOf = n => ctrlBy(n).find(c => c.type === 'airflow');
    const schedOf = n => ctrlBy(n).find(c => c.type === 'schedule');
    const has = t => nodes.some(n => n.type === t);
    if (!nodes.length) return null;

    // unique service names
    const RESERVED = new Set(['kafka', 'kafka-ui', 'kafka-init', 'connect', 'warehouse', 's3', 'init', 'iceberg-rest', 'airflow', 'metabase', 'flow-runner', 'jobmanager', 'taskmanager', 'flink-jars']);
    const svc = {};
    nodes.forEach(n => { let b = san(n.name).replace(/_/g, '-'), s = b, i = 2; while (RESERVED.has(s) || Object.values(svc).includes(s)) s = b + '-' + i++; svc[n.id] = s; });
    const pyImage = { build: { context: '.', dockerfile: 'py/Dockerfile' }, image: 'pipeline-lab-py:latest' };
    let pyBuilt = false;
    // every Python service names the same build, so compose never tries to pull the local image
    const py = extra => { pyBuilt = true; return Object.assign({ build: pyImage.build, image: pyImage.image, env_file: ['.env'], volumes: ['./py:/app'], working_dir: '/app' }, extra); };
    const needTriggers = nodes.some(n => airflowOf(n));
    const trigEnv = n => airflowOf(n) ? { TRIGGER_DIR: '/triggers' } : {};
    const trigVol = n => airflowOf(n) ? ['triggers:/triggers'] : [];
    const dependsInit = { init: { condition: 'service_completed_successfully' } };
    const pyReq = new Set(['requests==2.32.3', 'tabulate==0.9.0', 'kafka-python-ng==2.2.3', 'psycopg[binary]==3.2.3', 'boto3==1.35.36', 'pyarrow==17.0.0']);

    // ---------- topics produced into each Kafka node ----------
    const topicsOf = k => {
      const out = [];
      for (const u of up(k)) {
        if (u.type === 'cdc') { const pre = san(u.name); out.push(`${pre}.public.orders`, `${pre}.public.customers`); }
        else if (u.type === 'app') out.push(san(u.name) + '.events');
        else out.push(san(u.name) + '.out');
      }
      return out.length ? out : [san(k.name)];
    };
    const kafkaTopicsUp = n => up(n).filter(u => u.type === 'kafka').flatMap(topicsOf);

    // ---------- shared infrastructure ----------
    const needKafka = has('kafka') || has('cdc') || has('app');
    const needS3 = has('s3') || has('iceberg');
    const needWh = has('snowflake') || has('dbt') || nodes.some(n => n.type === 'bi');
    const needPgOltp = has('postgres');

    if (needKafka) {
      const k = nodes.find(n => n.type === 'kafka');
      const parts = k ? +k.cfg.partitions : 3;
      if (nodes.filter(n => n.type === 'kafka').length > 1) notes.push('Several Kafka nodes share one local broker; each keeps its own topics.');
      if (k && k.cfg.rf !== '1') notes.push(`Kafka runs as a single local broker, so replication factor ${k.cfg.rf} becomes 1. The partition count (${parts}) is kept.`);
      services.kafka = {
        image: IMG.kafka, hostname: 'kafka', ports: ['29092:29092'],
        environment: {
          KAFKA_NODE_ID: 1, KAFKA_PROCESS_ROLES: 'broker,controller',
          KAFKA_LISTENERS: 'PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092',
          KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:9092,EXTERNAL://localhost:29092',
          KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT',
          KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093', KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
          KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT', KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1,
          KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1, KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1,
          KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0, KAFKA_NUM_PARTITIONS: parts, KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
        },
        healthcheck: { test: ['CMD-SHELL', '/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1'], interval: '5s', timeout: '10s', retries: 30 }
      };
      const topics = nodes.filter(n => n.type === 'kafka').map(k => ({ k, t: topicsOf(k) }));
      if (topics.length) {
        services['kafka-init'] = {
          image: IMG.kafka, depends_on: { kafka: { condition: 'service_healthy' } }, entrypoint: ['/bin/sh', '-c'],
          command: [topics.flatMap(({ k, t }) => t.map(x => `/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic ${x} --partitions ${k.cfg.partitions} --replication-factor 1`)).join(' && ')]
        };
      }
      services['kafka-ui'] = { image: IMG.kafkaUi, ports: ['8080:8080'], depends_on: ['kafka'], environment: { KAFKA_CLUSTERS_0_NAME: 'local', KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: 'kafka:9092', ...(has('cdc') ? { KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: 'debezium', KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: 'http://connect:8083' } : {}) } };
      urls.push(['Kafka UI (topics, partitions, consumer lag)', 'http://localhost:8080']);
    }
    if (needS3) {
      services.s3 = {
        image: IMG.s3, ports: ['8333:8333'], entrypoint: ['/bin/sh', '-c'],
        command: ['printf \'{"identities":[{"name":"lab","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write","List","Tagging"]}]}\' "$$AWS_ACCESS_KEY_ID" "$$AWS_SECRET_ACCESS_KEY" > /tmp/s3.json && weed server -dir=/data -s3 -s3.port=8333 -s3.config=/tmp/s3.json -volume.max=0 -master.volumeSizeLimitMB=256'],
        env_file: ['.env'], volumes: ['s3data:/data'], profiles: ['local-s3']
      };
      volumes.s3data = {};
      urls.push(['S3 endpoint (SeaweedFS, S3-compatible)', 'http://localhost:8333']);
      notes.push('Local S3 is SeaweedFS (MinIO no longer publishes free images). Answer “y” to the AWS question in setup.sh to use real S3 instead.');
    }
    if (needWh) {
      services.warehouse = {
        image: IMG.postgres, environment: { POSTGRES_USER: '${WH_USER}', POSTGRES_PASSWORD: '${WH_PASSWORD}', POSTGRES_DB: '${WH_DB}' },
        ports: ['5442:5432'], volumes: ['whdata:/var/lib/postgresql/data', './warehouse/init.sql:/docker-entrypoint-initdb.d/init.sql:ro'],
        healthcheck: { test: ['CMD-SHELL', 'pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}'], interval: '3s', retries: 30 }
      };
      volumes.whdata = {};
      F['warehouse/init.sql'] = '-- Local stand-in for Snowflake. Raw landing tables go to schema raw, dbt builds into analytics.\nCREATE SCHEMA IF NOT EXISTS raw;\nCREATE SCHEMA IF NOT EXISTS analytics;\nCREATE SCHEMA IF NOT EXISTS snapshots;\n';
      urls.push(['Warehouse (Postgres stand-in for Snowflake)', 'postgres://localhost:5442']);
    }
    // init: buckets + trigger folder
    const bucketsNeeded = needS3 ? ['raw', 'warehouse'] : [];
    if (bucketsNeeded.length || needTriggers) {
      services.init = py({ command: ['python', 'init.py'], environment: { BUCKETS: bucketsNeeded.map(b => '${S3_BUCKET_' + b.toUpperCase() + '}').join(',') }, volumes: ['./py:/app', ...(needTriggers ? ['triggers:/triggers'] : [])], restart: 'no' });
      if (needTriggers) volumes.triggers = {};
    }
    const initDep = services.init ? dependsInit : {};
    const kafkaDep = needKafka ? { kafka: { condition: 'service_healthy' } } : {};
    const whDep = needWh ? { warehouse: { condition: 'service_healthy' } } : {};

    // ---------- per component ----------
    let pgPort = 5433;
    for (const n of nodes) {
      const name = svc[n.id], c = n.cfg;
      switch (n.type) {
        case 'postgres': {
          services[name] = {
            image: IMG.postgres, hostname: name,
            command: ['postgres', '-c', 'wal_level=logical', '-c', 'max_replication_slots=10', '-c', 'max_wal_senders=10'],
            environment: { POSTGRES_USER: '${PG_USER}', POSTGRES_PASSWORD: '${PG_PASSWORD}', POSTGRES_DB: '${PG_DB}' },
            ports: [`${pgPort}:5432`], volumes: [`./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql:ro`],
            healthcheck: { test: ['CMD-SHELL', 'pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}'], interval: '3s', retries: 30 }
          };
          urls.push([`${n.name} (OLTP Postgres)`, `postgres://localhost:${pgPort}`]); pgPort++;
          F['postgres/init.sql'] = PG_INIT;
          const rate = Math.min(+c.rate, 100);
          if (+c.rate > 100) notes.push(`${n.name}: the writer is capped at ${rate} writes/s locally (designed: ${c.rate}/s). Change RATE_${envKey(n.name)} in .env.`);
          prompts.push({ k: 'RATE_' + envKey(n.name), q: `Writes per second for ${n.name}`, d: String(rate), group: 'Load' });
          services[name + '-writer'] = py({ command: ['python', 'pg_writer.py'], environment: { PG_HOST: name, RATE: '${RATE_' + envKey(n.name) + '}', MIX: c.mix }, depends_on: { [name]: { condition: 'service_healthy' } }, restart: 'unless-stopped' });
          break;
        }
        case 'app': {
          const k = down(n).find(d => d.type === 'kafka');
          const rate = Math.min(+c.rate, 300);
          prompts.push({ k: 'RATE_' + envKey(n.name), q: `Events per second for ${n.name}`, d: String(rate), group: 'Load' });
          services[name] = py({ command: ['python', 'events.py'], environment: { TOPIC: san(n.name) + '.events', RATE: '${RATE_' + envKey(n.name) + '}', HOT_KEY: k && k.cfg.skew ? '1' : '0' }, depends_on: { ...kafkaDep }, restart: 'unless-stopped' });
          break;
        }
        case 'cdc': {
          const pg = up(n).find(u => u.type === 'postgres');
          if (!pg) { notes.push(`${n.name} has no Postgres source linked, so it was skipped.`); break; }
          const pre = san(n.name);
          if (c.mode === 'poll') {
            services[name] = py({ command: ['python', 'poller.py'], environment: { PG_HOST: svc[pg.id], TOPIC_PREFIX: pre + '.public', POLL_SECONDS: String(c.poll) }, depends_on: { ...kafkaDep, [svc[pg.id]]: { condition: 'service_healthy' } }, restart: 'unless-stopped' });
            notes.push(`${n.name} uses query polling, exported as a small Python poller (Debezium only does log-based CDC).`);
            break;
          }
          if (!services.connect) {
            services.connect = {
              image: IMG.connect, ports: ['8083:8083'], depends_on: { ...kafkaDep },
              environment: { BOOTSTRAP_SERVERS: 'kafka:9092', GROUP_ID: 'connect', CONFIG_STORAGE_TOPIC: 'connect_configs', OFFSET_STORAGE_TOPIC: 'connect_offsets', STATUS_STORAGE_TOPIC: 'connect_statuses', CONFIG_STORAGE_REPLICATION_FACTOR: 1, OFFSET_STORAGE_REPLICATION_FACTOR: 1, STATUS_STORAGE_REPLICATION_FACTOR: 1 },
              healthcheck: { test: ['CMD-SHELL', 'curl -sf http://localhost:8083/connectors > /dev/null'], interval: '5s', retries: 60 }
            };
            urls.push(['Kafka Connect REST (Debezium)', 'http://localhost:8083/connectors']);
          }
          F[`connect/${name}.sh`] = connectorScript(name, pre, svc[pg.id], c);
          services[name + '-register'] = { image: IMG.curl, env_file: ['.env'], entrypoint: ['/bin/sh', `/scripts/${name}.sh`], volumes: ['./connect:/scripts:ro'], depends_on: { connect: { condition: 'service_healthy' }, [svc[pg.id]]: { condition: 'service_healthy' } }, restart: 'on-failure' };
          break;
        }
        case 'spark': {
          const src = up(n)[0];
          if (!src) { notes.push(`${n.name} has no input, so it was skipped.`); break; }
          const pk = new Set();
          let source;
          if (src.type === 'kafka') { source = { kind: 'kafka', topics: topicsOf(src) }; pk.add(PKG.kafka); }
          else if (src.type === 'postgres') { source = { kind: 'jdbc', host: svc[src.id], table: 'orders' }; pk.add(PKG.pg); }
          else if (src.type === 's3') { const w = up(src)[0]; source = { kind: 'files', path: `s3a://raw/${w ? san(w.name) : 'landing'}/`, format: src.cfg.format }; pk.add(PKG.s3a); }
          else if (src.type === 'iceberg') { source = { kind: 'files', path: 's3a://raw/none/', format: 'parquet' }; pk.add(PKG.s3a); notes.push(`${n.name}: reading from Iceberg is exported as a placeholder; use spark.readStream.table('lake.db.…').`); }
          const sinks = [];
          let iceberg = false;
          for (const d of down(n)) {
            if (d.type === 'iceberg') { sinks.push({ kind: 'iceberg', table: 'lake.db.' + san(d.name), mode: d.cfg.mode, compaction: !!d.cfg.compaction }); iceberg = true; pk.add(PKG.iceberg); }
            else if (d.type === 's3') { sinks.push({ kind: 'files', path: `s3a://raw/${san(n.name)}/`, format: d.cfg.format }); pk.add(PKG.s3a); }
            else if (d.type === 'snowflake') { sinks.push({ kind: 'warehouse', table: 'raw_' + san(n.name) }); pk.add(PKG.pg); }
            else if (d.type === 'kafka') { sinks.push({ kind: 'kafka', topic: san(n.name) + '.out' }); pk.add(PKG.kafka); }
            else sinks.push({ kind: 'console' });
          }
          if (!sinks.length) sinks.push({ kind: 'console' });
          const mode = c.mode === 'batch' || airflowOf(n) ? 'batch' : 'stream';
          const cfg = { name, mode, trigger: +c.trigger, every: schedOf(n) ? +schedOf(n).cfg.every : +c.every, job: c.job, source, sinks, iceberg };
          F[`spark/${name}.json`] = JSON.stringify(cfg, null, 2);
          const cores = Math.min(8, c.executors * c.cores);
          if (c.executors * c.cores > 8) notes.push(`${n.name}: runs Spark in local mode with ${cores} threads on your machine (designed: ${c.executors} executors × ${c.cores} cores).`);
          const deps = { ...kafkaDep, ...initDep, ...(iceberg ? { 'iceberg-rest': { condition: 'service_started' } } : {}), ...(sinks.some(s => s.kind === 'warehouse') ? whDep : {}) };
          if (src.type === 'postgres') deps[svc[src.id]] = { condition: 'service_healthy' };
          services[name] = {
            image: IMG.spark, user: 'root', env_file: ['.env'], environment: { ...trigEnv(n), HOME: '/tmp' },
            command: ['/opt/spark/bin/spark-submit', '--master', `local[${cores}]`, '--driver-memory', '1g', '--conf', 'spark.jars.ivy=/tmp/ivy', '--packages', [...pk].join(',') + '${SPARK_EXTRA_PACKAGES:-}', '/py/spark_job.py', `/spark/${name}.json`],
            volumes: ['./py:/py:ro', './spark:/spark:ro', 'sparkchk:/chk', 'ivy:/tmp/ivy', ...trigVol(n)], depends_on: deps, restart: 'on-failure'
          };
          volumes.sparkchk = {}; volumes.ivy = {};
          break;
        }
        case 'flink': {
          const src = up(n).find(u => u.type === 'kafka');
          if (!src) { notes.push(`${n.name}: only Kafka sources are exported for Flink; skipped.`); break; }
          if (!services.jobmanager) {
            const props = 'jobmanager.rpc.address: jobmanager\nrest.address: jobmanager\nparallelism.default: ' + c.par + '\ntaskmanager.numberOfTaskSlots: ' + (Math.max(c.par, 1) + 2); // +2 spare slots for ad-hoc queries
            const iceJars = nodes.some(x => x.type === 'flink' && down(x).some(d => d.type === 'iceberg'));
            const jarUrls = [FLINK_KAFKA_JAR].concat(iceJars ? FLINK_ICEBERG_JARS : []);
            const jarDep = { 'flink-jars': { condition: 'service_completed_successfully' } };
            // jars land in a volume mounted at lib/ext, so JobManager, TaskManager and SQL client all load them
            services.jobmanager = { image: IMG.flink, command: 'jobmanager', ports: ['8082:8081'], env_file: ['.env'], environment: { FLINK_PROPERTIES: props }, volumes: ['flinkjars:/opt/flink/lib/ext'], depends_on: jarDep };
            services.taskmanager = { image: IMG.flink, command: 'taskmanager', env_file: ['.env'], environment: { FLINK_PROPERTIES: props }, volumes: ['flinkjars:/opt/flink/lib/ext'], depends_on: { ...jarDep, jobmanager: { condition: 'service_started' } } };
            services['flink-jars'] = py({ command: ['python', '-c', `import urllib.request, os\nfor u in ${JSON.stringify(jarUrls)}:\n    p = '/jars/' + u.rsplit('/', 1)[1]\n    if not os.path.exists(p):\n        print('downloading', p, flush=True)\n        urllib.request.urlretrieve(u, p + '.part')\n        os.rename(p + '.part', p)\nprint('jars ready')`], volumes: ['flinkjars:/jars'], restart: 'no' });
            volumes.flinkjars = {};
            urls.push(['Flink dashboard', 'http://localhost:8082']);
          }
          F[`flink/${name}.sql`] = flinkSQL(n, name, topicsOf(src), down(n));
          const toIce = down(n).some(d => d.type === 'iceberg');
          // Flink SQL does not expand env vars: fill bucket and S3 endpoint from .env at start; drop the endpoint line on real AWS
          const run = `until (exec 3<>/dev/tcp/jobmanager/8081) 2>/dev/null; do echo waiting for JobManager; sleep 3; done; sleep 5; sed -e "s#__S3_BUCKET__#$\${S3_BUCKET_WAREHOUSE:-warehouse}#" -e "s#__S3_ENDPOINT__#$\${S3_ENDPOINT}#" -e "/'s3.endpoint' = '',/d" /job/${name}.sql > /tmp/job.sql && exec bin/sql-client.sh -f /tmp/job.sql`;
          services[name] = { image: IMG.flink, env_file: ['.env'], depends_on: { jobmanager: { condition: 'service_started' }, taskmanager: { condition: 'service_started' }, ...kafkaDep, ...(toIce ? { 'iceberg-rest': { condition: 'service_started' } } : {}) }, environment: { FLINK_PROPERTIES: 'jobmanager.rpc.address: jobmanager\nrest.address: jobmanager' }, command: ['bash', '-c', run], volumes: ['./flink:/job:ro', 'flinkjars:/opt/flink/lib/ext'], restart: 'on-failure' };
          if (down(n).some(d => d.type !== 'kafka' && d.type !== 'iceberg')) notes.push(`${n.name}: Flink sinks other than Kafka and Iceberg are exported as a print sink (see the Flink TaskManager log).`);
          break;
        }
        case 'iceberg': {
          if (!services['iceberg-rest']) {
            services['iceberg-rest'] = { image: IMG.iceberg, ports: ['8181:8181'], env_file: ['.env', 'iceberg.env'], environment: { CATALOG_WAREHOUSE: 's3://${S3_BUCKET_WAREHOUSE}/', CATALOG_IO__IMPL: 'org.apache.iceberg.aws.s3.S3FileIO', CATALOG_S3_PATH__STYLE__ACCESS: 'true' }, depends_on: { ...initDep } };
            urls.push(['Iceberg REST catalog', 'http://localhost:8181/v1/config']);
          }
          if (!up(n).some(u => u.type === 'spark')) notes.push(`${n.name}: only Spark writes to Iceberg in the export. Put a Spark node in front of it.`);
          if (down(n).some(d => d.type === 'bi' || d.type === 'snowflake')) {
            if (!has('spark') && has('flink')) F['query-iceberg.sh'] = `#!/usr/bin/env bash
# Query the Iceberg tables with Flink SQL (batch mode) inside the Flink JobManager.
# Usage: ./query-iceberg.sh "SELECT * FROM lake.db.<table> ORDER BY window_start DESC LIMIT 20"
set -e
Q="\${1:-SHOW TABLES}"
docker compose exec -T -e Q="$Q" jobmanager bash -c 'EP=""; [ -n "$S3_ENDPOINT" ] && EP="'"'"'s3.endpoint'"'"' = '"'"'$S3_ENDPOINT'"'"',"
cat > /tmp/q.sql <<SQL
SET '"'"'execution.runtime-mode'"'"' = '"'"'batch'"'"';
SET '"'"'sql-client.execution.result-mode'"'"' = '"'"'tableau'"'"';
CREATE CATALOG lake WITH ('"'"'type'"'"' = '"'"'iceberg'"'"', '"'"'catalog-type'"'"' = '"'"'rest'"'"', '"'"'uri'"'"' = '"'"'http://iceberg-rest:8181'"'"',
  '"'"'warehouse'"'"' = '"'"'s3://\${S3_BUCKET_WAREHOUSE:-warehouse}/'"'"', '"'"'io-impl'"'"' = '"'"'org.apache.iceberg.aws.s3.S3FileIO'"'"', $EP '"'"'s3.path-style-access'"'"' = '"'"'true'"'"');
USE lake.db;
$Q;
SQL
bin/sql-client.sh -f /tmp/q.sql 2>&1 | grep -v "^Flink SQL> *$\\|^$\\|^WARNING\\|jline\\|INFO\\]"'
`;
            else F['query-iceberg.sh'] = `#!/usr/bin/env bash\n# Query the Iceberg tables with Spark SQL inside the Spark container.\nset -e\nSVC=$(docker compose ps --services | grep -E '^(${nodes.filter(x => x.type === 'spark').map(x => svc[x.id]).join('|') || 'spark'})$' | head -1)\ndocker compose exec "$SVC" /opt/spark/bin/spark-sql --packages ${PKG.iceberg} \\\n  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.lake.type=rest \\\n  --conf spark.sql.catalog.lake.uri=http://iceberg-rest:8181 --conf spark.sql.catalog.lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO \\\n  --conf spark.sql.catalog.lake.s3.endpoint=\${S3_ENDPOINT:-http://s3:8333} --conf spark.sql.catalog.lake.s3.path-style-access=true \\\n  -e "\${1:-SHOW TABLES IN lake.db}"\n`;
            notes.push('Dashboards and Snowflake read Iceberg through a query engine (Trino, Snowflake external Iceberg tables). Locally, use ./query-iceberg.sh to run Spark SQL against the lake.');
          }
          break;
        }
        case 's3': {
          const kin = up(n).filter(u => u.type === 'kafka');
          if (kin.length) {
            const cfg = { name: name + '-sink', sources: [{ kind: 'kafka', topics: kin.flatMap(topicsOf) }], target: 's3', bucket: 'raw', prefix: san(n.name), flush_every: 10 };
            F[`py/loaders/${name}.json`] = JSON.stringify(cfg, null, 2);
            services[name + '-sink'] = py({ command: ['python', 'loader.py', `loaders/${name}.json`], depends_on: { ...kafkaDep, ...initDep }, restart: 'unless-stopped' });
          }
          break;
        }
        case 'snowflake': {
          const sources = [];
          for (const u of up(n)) {
            if (u.type === 'kafka') sources.push({ kind: 'kafka', topics: topicsOf(u) });
            if (u.type === 's3') { const w = up(u)[0]; sources.push({ kind: 's3', bucket: 'raw', prefix: w ? san(w.name) : san(u.name) }); }
            if (u.type === 'iceberg') notes.push(`${n.name}: reading Iceberg from Snowflake needs an external volume and catalog integration in your account. See snowflake/iceberg.sql.`);
          }
          if (up(n).some(u => u.type === 'iceberg')) F['snowflake/iceberg.sql'] = SF_ICEBERG;
          if (!sources.length) break;
          const batch = c.ingest === 'copy' || airflowOf(n);
          const cfg = { name: name + '-loader', sources, target: 'warehouse', mode: batch ? 'batch' : 'stream', every: schedOf(n) ? +schedOf(n).cfg.every : +c.every, flush_every: c.ingest === 'pipe' ? 10 : 2 };
          F[`py/loaders/${name}.json`] = JSON.stringify(cfg, null, 2);
          services[name + '-loader'] = py({ command: ['python', 'loader.py', `loaders/${name}.json`], environment: trigEnv(n), volumes: ['./py:/app', ...trigVol(n)], depends_on: { ...kafkaDep, ...whDep, ...initDep }, restart: 'unless-stopped' });
          notes.push(`${n.name}: loads with a Python loader (${batch ? 'batch, like COPY INTO' : 'continuous, like Snowpipe'}) into ${'the local warehouse'} or your Snowflake account.`);
          break;
        }
        case 'dbt': {
          const sf = up(n).find(u => u.type === 'snowflake');
          const rawTables = sf ? rawTablesFor(sf) : [];
          Object.assign(F, dbtProject(n, rawTables));
          services[name] = {
            build: { context: '.', dockerfile: 'dbt/Dockerfile' }, env_file: ['.env'], working_dir: '/dbt',
            environment: { ...trigEnv(n), SERVICE_NAME: name, EVERY: String(schedOf(n) ? schedOf(n).cfg.every : c.every), HAS_SNAPSHOT: c.scd === 'scd2' ? '1' : '0', DBT_PROFILES_DIR: '/dbt' },
            command: ['python', '/py/dbt_runner.py'], volumes: ['./dbt:/dbt', './py:/py:ro', ...trigVol(n)], depends_on: { ...whDep }, restart: 'unless-stopped'
          };
          if (!sf) notes.push(`${n.name} has no Snowflake node upstream; it still runs against the warehouse.`);
          break;
        }
        case 'airflow': {
          const targets = PL.outLinks(n).map(l => PL.byId(l.to));
          F[`airflow/dags/${san(n.name)}.py`] = airflowDag(n, targets, svc, nodes);
          services.airflow = {
            image: IMG.airflow, command: 'standalone', ports: ['8081:8080'],
            environment: { AIRFLOW__CORE__LOAD_EXAMPLES: 'false', AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: '10', AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false' },
            volumes: ['./airflow/dags:/opt/airflow/dags', 'triggers:/triggers'], depends_on: { ...initDep }
          };
          urls.push(['Airflow (user admin, password: docker compose exec airflow cat standalone_admin_password.txt)', 'http://localhost:8081']);
          break;
        }
        case 'bi': {
          if (!services.metabase) {
            services.metabase = { image: IMG.metabase, ports: ['3000:3000'], depends_on: needWh ? ['warehouse'] : [] };
            urls.push(['Metabase (connect to host warehouse, port 5432, database from .env)', 'http://localhost:3000']);
          }
          break;
        }
      }
    }

    // ---------- simple blocks → one flow-runner ----------
    const simple = nodes.filter(n => ['api', 'file', 'script', 'table'].includes(n.type));
    if (simple.length) {
      const order = topo(simple);
      const steps = order.map(n => {
        const s = { id: n.id, slug: svc[n.id], name: n.name, type: n.type, deps: up(n).filter(u => simple.includes(u)).map(u => u.id), sources: [], sinks: [] };
        if (n.type === 'api') { s.url = n.cfg.url; s.path = n.cfg.path; s.headers_env = 'API_HEADERS_' + envKey(n.name); prompts.push({ k: s.headers_env, q: `HTTP headers for ${n.name} as JSON, e.g. {"Authorization": "Bearer …"} (Enter for none)`, d: '', secret: true, group: 'APIs' }); }
        if (n.type === 'file') { const fname = san(n.cfg.fileName || 'cities.csv').replace(/_(csv|json|jsonl|ndjson|txt)$/, '.$1'); s.path = '/app/data/' + fname; F['py/data/' + fname] = n.cfg.fileText || PL.SAMPLE_CSV; }
        if (n.type === 'script') {
          s.limit = +n.cfg.limit; s.code = `/app/steps/${svc[n.id]}.py`;
          F[`py/steps/${svc[n.id]}.py`] = n.cfg.lang === 'js' ? `# This step was written in JavaScript in Pipeline Lab:\n#\n${n.cfg.code.split('\n').map(l => '# ' + l).join('\n')}\n#\n# Port it to Python here. For now it passes the first \`limit\` rows through.\nresult = rows[:limit]\n` : n.cfg.code;
          if (n.cfg.lang === 'js') notes.push(`${n.name} is JavaScript. The export runs Python, so its code is included as a comment to port.`);
        }
        if (n.type === 'table') s.max_rows = +n.cfg.maxRows;
        for (const u of up(n)) {
          if (u.type === 'kafka') s.sources.push({ kind: 'kafka', topics: topicsOf(u), max: n.type === 'script' ? Math.max(+n.cfg.limit, 1) : 200 });
          if (u.type === 'postgres') s.sources.push({ kind: 'oltp', host: svc[u.id], query: `SELECT * FROM orders ORDER BY updated_at DESC LIMIT ${n.type === 'script' ? n.cfg.limit : 200}` });
          if (u.type === 'snowflake') s.sources.push({ kind: 'warehouse', query: 'SELECT * FROM ' + (rawTablesFor(u)[0] ? (u => `raw.${u}`)(rawTablesFor(u)[0]) : 'information_schema.tables') + ' LIMIT 200' });
          if (u.type === 'dbt') s.sources.push({ kind: 'warehouse', query: `SELECT * FROM analytics.${dbtMart(u, rawTablesFor(up(u).find(x => x.type === 'snowflake') || u))} LIMIT 200` });
          if (u.type === 'spark') s.sources.push({ kind: 'warehouse', query: `SELECT * FROM raw.raw_${san(u.name)} LIMIT 200` });
        }
        for (const d of down(n)) {
          if (d.type === 'kafka') s.sinks.push({ kind: 'kafka', topic: san(n.name) + '.out' });
          if (d.type === 's3') s.sinks.push({ kind: 's3', bucket: 'raw', prefix: san(n.name) });
          if (d.type === 'snowflake') s.sinks.push({ kind: 'warehouse', table: 'raw_' + san(n.name) });
          if (d.type === 'postgres') s.sinks.push({ kind: 'oltp', host: svc[d.id], table: san(n.name) });
        }
        return s;
      });
      const sched = simple.map(schedOf).find(Boolean);
      const af = simple.map(airflowOf).find(Boolean);
      F['py/flow.json'] = JSON.stringify({ steps, every: sched ? +sched.cfg.every : null }, null, 2);
      const deps = { ...initDep };
      if (steps.some(s => s.sources.some(x => x.kind === 'kafka') || s.sinks.some(x => x.kind === 'kafka'))) Object.assign(deps, kafkaDep);
      if (steps.some(s => s.sources.concat(s.sinks).some(x => x.kind === 'warehouse'))) Object.assign(deps, whDep);
      steps.forEach(s => s.sources.concat(s.sinks).filter(x => x.kind === 'oltp').forEach(x => { deps[x.host] = { condition: 'service_healthy' }; }));
      services['flow-runner'] = py({ command: ['python', 'runner.py', 'flow.json'], environment: af ? { TRIGGER_DIR: '/triggers' } : {}, volumes: ['./py:/app', './output:/output', ...(af ? ['triggers:/triggers'] : [])], depends_on: deps, restart: sched || af ? 'unless-stopped' : 'no' });
      if (!sched && !af) notes.push('The simple flow (API / File / Script / Table) runs once and exits. Link a Scheduler to the script to keep it running on a timer. Results print in the log and land in ./output as CSV.');
    }

    // ---------- shared files ----------
    if (has('snowflake')) pyReq.add('snowflake-connector-python==3.12.3');
    F['py/requirements.txt'] = [...pyReq].join('\n') + '\n';
    F['py/Dockerfile'] = `FROM python:3.12-slim\nWORKDIR /app\nCOPY py/requirements.txt /tmp/requirements.txt\nRUN pip install --no-cache-dir -r /tmp/requirements.txt\nCOPY py/ /app/\nCMD ["python", "runner.py", "flow.json"]\n`;
    F['py/common.py'] = PY.common; F['py/runner.py'] = PY.runner; F['py/loader.py'] = PY.loader; F['py/pg_writer.py'] = PY.pgwriter;
    F['py/events.py'] = PY.events; F['py/poller.py'] = PY.poller; F['py/init.py'] = PY.init; F['py/spark_job.py'] = PY.spark; F['py/dbt_runner.py'] = PY.dbtRun;
    if (simple.length || pyBuilt) F['output/.gitkeep'] = '';
    if (needS3 || has('iceberg')) F['iceberg.env'] = '# Written by setup.sh\nCATALOG_S3_ENDPOINT=http://s3:8333\n';

    // python services that use S3 depend on local s3 (profile) only in local mode: handled by setup.sh profiles
    const compose = { name: 'pipeline-lab', services };
    if (Object.keys(volumes).length) compose.volumes = volumes;
    F['docker-compose.yml'] = `# Generated by Pipeline Lab from the design "${(PL.SCENARIOS.find(s => s.id === S.scenario) || {}).name || 'custom'}".\n# Start it with ./setup.sh (asks for settings, writes .env, then runs docker compose up).\n\n` + yaml(compose) + '\n';
    F['setup.sh'] = setupScript({ needPgOltp, needS3, needWh, hasSnowflake: has('snowflake'), prompts, urls, hasSpark: has('spark'), hasRunner: !!simple.length });
    F['.env.example'] = '# Copy to .env or run ./setup.sh to create it interactively.\n' + envExample({ needPgOltp, needS3, needWh, prompts });
    F['.gitignore'] = '.env\noutput/*.csv\n';
    if (PL.planner && PL.planner.hasCloud()) {
      Object.assign(F, PL.planner.exportFiles(yaml, n => svc[n.id]));
      notes.push(S.plan ? 'cloud/PLAN.md has the resource plan, with Kubernetes manifests in cloud/k8s/ for components planned on Kubernetes. Locally, Kafka partitions, Spark executors, Flink parallelism and the warehouse size come from the design: press Apply in Resources to make them follow the plan.' : 'cloud/PLAN.md sizes this design for the cloud with default volumes. Open Resources in the app to set your own.');
    }
    F['README.md'] = readme(nodes, svc, urls, notes);
    return { files: F, notes, urls, services: Object.keys(services) };

    /* helpers needing closure */
    function topo(list) {
      const out = [], done = new Set();
      const visit = n => { if (done.has(n.id)) return; done.add(n.id); up(n).filter(u => list.includes(u)).forEach(visit); out.push(n); };
      list.forEach(visit); return out;
    }
    function rawTablesFor(sf) {
      const t = [];
      for (const u of up(sf)) {
        if (u.type === 'kafka') topicsOf(u).forEach(x => t.push('raw_' + san(x)));
        if (u.type === 's3') { const w = up(u)[0]; t.push('raw_' + (w ? san(w.name) : san(u.name))); }
        if (u.type === 'spark' || u.type === 'script') t.push('raw_' + san(u.name));
      }
      return t;
    }
  };

  /* ---------- templates ---------- */
  const PG_INIT = `-- Sample OLTP schema. REPLICA IDENTITY FULL makes updates and deletes carry the old row (the "before" image) in CDC events.
CREATE TABLE customers (
  id INT PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE orders (
  id BIGSERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(id),
  amount NUMERIC(10,2) NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
INSERT INTO customers
SELECT g, 'Customer ' || g, (ARRAY['Lisbon','Berlin','Austin','Osaka','Krakow','Toronto','Nairobi','Lyon'])[1 + g % 8], now()
FROM generate_series(1, 50) g;
INSERT INTO orders (customer_id, amount, status)
SELECT 1 + (random() * 49)::int, round((random() * 300)::numeric, 2), 'new' FROM generate_series(1, 500);
`;

  const SF_ICEBERG = `-- Read the lakehouse's Iceberg tables from Snowflake (run in your account; needs ACCOUNTADMIN or equivalent).
-- Local SeaweedFS is not reachable from Snowflake, so this applies when the lake is on real S3.
CREATE OR REPLACE EXTERNAL VOLUME lake_volume
  STORAGE_LOCATIONS = ((NAME = 'lake' STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://<your-warehouse-bucket>/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account>:role/<snowflake-access-role>'));
CREATE OR REPLACE CATALOG INTEGRATION lake_rest
  CATALOG_SOURCE = ICEBERG_REST TABLE_FORMAT = ICEBERG CATALOG_NAMESPACE = 'db'
  REST_CONFIG = (CATALOG_URI = 'https://<your-public-iceberg-rest-endpoint>')
  REST_AUTHENTICATION = (TYPE = BEARER BEARER_TOKEN = '<token>') ENABLED = TRUE;
-- CREATE ICEBERG TABLE orders EXTERNAL_VOLUME = 'lake_volume' CATALOG = 'lake_rest' CATALOG_TABLE_NAME = 'orders';
`;

  function connectorScript(name, prefix, pgHost, c) {
    return `#!/bin/sh
# Registers the Debezium Postgres connector for ${name}. Runs once; safe to re-run.
set -e
until curl -sf http://connect:8083/connectors > /dev/null; do echo "waiting for Kafka Connect"; sleep 3; done
cat > /tmp/${name}.json <<EOF
{
  "name": "${name}",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "${pgHost}",
    "database.port": "5432",
    "database.user": "\${PG_USER}",
    "database.password": "\${PG_PASSWORD}",
    "database.dbname": "\${PG_DB}",
    "topic.prefix": "${prefix}",
    "slot.name": "${san(name)}_slot",
    "publication.autocreate.mode": "filtered",
    "table.include.list": "public.orders,public.customers",
    "snapshot.mode": "${c.snapshot ? 'always' : 'initial'}",
    "decimal.handling.mode": "string",
    "tombstones.on.delete": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,source.ts_ms,source.lsn"
  }
}
EOF
if curl -sf http://connect:8083/connectors/${name} > /dev/null; then
  echo "connector ${name} already registered"
else
  curl -sf -X POST -H "Content-Type: application/json" --data @/tmp/${name}.json http://connect:8083/connectors
  echo; echo "connector ${name} registered"
fi
`;
  }

  function flinkSQL(n, name, topics, downs) {
    const c = n.cfg, win = c.op === 'window';
    const cols = win ? '`window_start` TIMESTAMP(3), `window_end` TIMESTAMP(3), `events` BIGINT' : '`value` STRING, `ts` TIMESTAMP_LTZ(3)';
    const select = win ? `SELECT window_start, window_end, COUNT(*) AS events
FROM TABLE(TUMBLE(TABLE src, DESCRIPTOR(ts), INTERVAL '10' SECOND))
GROUP BY window_start, window_end` : 'SELECT `value`, `ts` FROM src';
    const ddl = [], targets = [];
    const k = downs.find(d => d.type === 'kafka');
    if (k) {
      ddl.push(`CREATE TABLE kafka_out (${cols}) WITH (
  'connector' = 'kafka', 'topic' = '${san(n.name)}.out', 'properties.bootstrap.servers' = 'kafka:9092', 'format' = 'json'${c.eos ? `,\n  'sink.delivery-guarantee' = 'exactly-once', 'sink.transactional-id-prefix' = '${san(n.name)}', 'properties.transaction.timeout.ms' = '900000'` : ",\n  'sink.delivery-guarantee' = 'at-least-once'"}
);`);
      targets.push('kafka_out');
    }
    const ices = downs.filter(d => d.type === 'iceberg');
    if (ices.length) {
      ddl.push(`-- Iceberg through the same REST catalog Spark uses. Data files are committed on every checkpoint (${c.ckpt}s).
CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://__S3_BUCKET__/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = '__S3_ENDPOINT__',
  's3.path-style-access' = 'true'
);

CREATE DATABASE IF NOT EXISTS lake.db;`);
      ices.forEach(d => {
        ddl.push(`CREATE TABLE IF NOT EXISTS lake.db.${san(d.name)} (${cols}) WITH ('format-version' = '2'${d.cfg.mode === 'mor' ? ", 'write.delete.mode' = 'merge-on-read', 'write.update.mode' = 'merge-on-read'" : ''});`);
        targets.push('lake.db.' + san(d.name));
      });
    }
    if (!targets.length) { ddl.push(`CREATE TABLE print_out (${cols}) WITH ('connector' = 'print');`); targets.push('print_out'); }
    const inserts = targets.map(t => `INSERT INTO ${t}\n${select};`);
    return `-- Flink SQL job generated by Pipeline Lab for "${n.name}".
SET 'pipeline.name' = '${name}';
SET 'execution.checkpointing.interval' = '${c.ckpt}s';
SET 'parallelism.default' = '${c.par}';

CREATE TABLE src (
  \`value\` STRING,
  \`ts\` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR \`ts\` AS \`ts\` - INTERVAL '${win ? c.watermark : 0}' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = '${topics.join(';')}',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-${san(n.name)}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

${ddl.join('\n\n')}

${inserts.length > 1 ? `EXECUTE STATEMENT SET\nBEGIN\n${inserts.join('\n')}\nEND;` : inserts[0]}
`;
  }

  function dbtMart(n, rawTables) {
    if (rawTables.some(t => /_orders$/.test(t))) return 'fct_orders';
    if (rawTables.some(t => /_events$/.test(t))) return 'fct_events_per_minute';
    return rawTables.length ? 'stg_' + rawTables[0].replace(/^raw_/, '') : 'stg_placeholder';
  }

  function dbtProject(n, rawTables) {
    const c = n.cfg, F = {}, mat = c.mat;
    F['dbt/Dockerfile'] = 'FROM python:3.12-slim\nRUN pip install --no-cache-dir dbt-core==1.9.4 dbt-postgres==1.9.1 dbt-snowflake==1.9.4\nWORKDIR /dbt\n';
    F['dbt/dbt_project.yml'] = yaml({
      name: 'pipeline_lab', version: '1.0.0', 'config-version': 2, profile: 'pipeline_lab',
      'model-paths': ['models'], 'snapshot-paths': ['snapshots'], 'macro-paths': ['macros'], 'clean-targets': ['target'],
      models: { pipeline_lab: { staging: { '+materialized': 'view' }, marts: { '+materialized': mat } } }
    }) + '\n';
    F['dbt/profiles.yml'] = `pipeline_lab:
  target: "{{ env_var('DBT_TARGET', 'local') }}"
  outputs:
    local:
      type: postgres
      host: warehouse
      port: 5432
      user: "{{ env_var('WH_USER', 'analyst') }}"
      password: "{{ env_var('WH_PASSWORD', 'analyst') }}"
      dbname: "{{ env_var('WH_DB', 'analytics') }}"
      schema: analytics
      threads: 4
    snowflake:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT', '') }}"
      user: "{{ env_var('SNOWFLAKE_USER', '') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD', '') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', '') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', '') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', '') }}"
      schema: ANALYTICS
      threads: 4
`;
    F['dbt/macros/json_get.sql'] = `{# Read a key from the JSON payload column, on Postgres (jsonb) or Snowflake (variant). #}
{% macro json_get(col, key) -%}
  {%- if target.type == 'snowflake' -%} {{ col }}:"{{ key }}"::string
  {%- else -%} {{ col }}->>'{{ key }}'
  {%- endif -%}
{%- endmacro %}
`;
    F['dbt/macros/schema.sql'] = `{# Use the schema names as written (analytics, snapshots) instead of prefixing them. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
`;
    const tables = rawTables.length ? rawTables : ['raw_placeholder'];
    F['dbt/models/sources.yml'] = `version: 2
sources:
  - name: raw
    schema: "{{ 'raw' if target.type == 'postgres' else env_var('SNOWFLAKE_SCHEMA', 'RAW') }}"
    tables:
${tables.map(t => `      - name: ${t}`).join('\n')}
`;
    const j = (k) => `{{ json_get('payload', '${k}') }}`;
    let marts = [];
    for (const t of tables) {
      const base = t.replace(/^raw_/, '');
      let sql;
      if (/_orders$/.test(t)) sql = `select
    cast(${j('id')} as bigint) as order_id,
    cast(${j('customer_id')} as int) as customer_id,
    cast(${j('amount')} as numeric(10, 2)) as amount,
    ${j('status')} as status,
    cast(${j('updated_at')} as {{ dbt.type_timestamp() }}) as updated_at,
    ${j('__op')} as cdc_op,
    coalesce(${j('__deleted')}, 'false') = 'true' as is_deleted,
    loaded_at
from {{ source('raw', '${t}') }}`;
      else if (/_customers$/.test(t)) sql = `select
    cast(${j('id')} as int) as customer_id,
    ${j('name')} as name,
    ${j('city')} as city,
    cast(${j('updated_at')} as {{ dbt.type_timestamp() }}) as updated_at,
    loaded_at
from {{ source('raw', '${t}') }}`;
      else if (/_events$/.test(t)) sql = `select
    ${j('event_id')} as event_id,
    ${j('user_id')} as user_id,
    ${j('event')} as event,
    ${j('page')} as page,
    cast(${j('ts')} as bigint) as ts_ms,
    loaded_at
from {{ source('raw', '${t}') }}`;
      else sql = `select payload, source, loaded_at\nfrom {{ source('raw', '${t}') }}`;
      F[`dbt/models/staging/stg_${base}.sql`] = `-- Staging: typed columns from the raw JSON landing table.\n${sql}\n`;
    }
    const ordersStg = tables.find(t => /_orders$/.test(t)), custStg = tables.find(t => /_customers$/.test(t)), evStg = tables.find(t => /_events$/.test(t));
    const incr = mat === 'incremental' ? `{{ config(unique_key='order_id', incremental_strategy=('delete+insert' if target.type == 'postgres' else 'merge')) }}\n` : '';
    if (ordersStg) {
      F['dbt/models/marts/fct_orders.sql'] = `-- Latest state of each order. Change events arrive at least once and out of order, so keep the newest per key.
${incr}with changes as (
    select * from {{ ref('stg_${ordersStg.replace(/^raw_/, '')}') }}
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),
ranked as (
    select *, row_number() over (partition by order_id order by updated_at desc, loaded_at desc) as rn
    from changes
)
select order_id, customer_id, amount, status, updated_at, is_deleted, loaded_at
from ranked
where rn = 1
`;
      marts.push('fct_orders');
    }
    if (evStg) {
      F['dbt/models/marts/fct_events_per_minute.sql'] = `select
    date_trunc('minute', loaded_at) as minute,
    event,
    count(*) as events,
    count(distinct user_id) as users
from {{ ref('stg_${evStg.replace(/^raw_/, '')}') }}
group by 1, 2
`;
    }
    if (custStg && c.scd !== 'none') {
      const stg = `stg_${custStg.replace(/^raw_/, '')}`;
      F['dbt/models/staging/stg_customers_latest.sql'] = `select * from (
    select *, row_number() over (partition by customer_id order by updated_at desc, loaded_at desc) as rn
    from {{ ref('${stg}') }}
) t where rn = 1
`;
      if (c.scd === 'scd2') {
        F['dbt/snapshots/customers_snapshot.sql'] = `-- SCD Type 2: each change to name or city closes the current row (dbt_valid_to) and inserts a new version.
{% snapshot customers_snapshot %}
{{ config(target_schema='snapshots', unique_key='customer_id', strategy='check', check_cols=['name', 'city']) }}
select customer_id, name, city, updated_at from {{ ref('stg_customers_latest') }}
{% endsnapshot %}
`;
        F['dbt/models/marts/dim_customer.sql'] = `-- One row per customer version. Join facts on customer_id and event time between valid_from and valid_to.
{{ config(materialized='view') }}
select
    customer_id, name, city,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('customers_snapshot') }}
`;
      } else {
        F['dbt/models/marts/dim_customer.sql'] = `-- SCD Type 1: overwrite in place, no history.
{{ config(materialized='incremental', unique_key='customer_id', incremental_strategy=('delete+insert' if target.type == 'postgres' else 'merge')) }}
select customer_id, name, city, updated_at from {{ ref('stg_customers_latest') }}
`;
      }
    }
    F['dbt/models/schema.yml'] = `version: 2
models:
${ordersStg ? `  - name: fct_orders
    columns:
      - name: order_id
        tests: [unique, not_null]
` : ''}${custStg && c.scd !== 'none' ? `  - name: dim_customer
    columns:
      - name: customer_id
        tests: [not_null]
` : ''}${!ordersStg && !custStg ? '  []\n' : ''}`;
    if (F['dbt/models/schema.yml'].endsWith('  []\n')) F['dbt/models/schema.yml'] = 'version: 2\n';
    return F;
  }

  function airflowDag(n, targets, svc, nodes) {
    const name = san(n.name);
    // order tasks along data links between the targets
    const reach = (a, b) => { const seen = new Set(), st = [a.id]; while (st.length) { const id = st.pop(); if (id === b.id) return true; if (seen.has(id)) continue; seen.add(id); S.links.filter(l => l.from === id && !PL.isControlLink(l)).forEach(l => st.push(l.to)); } return false; };
    const task = t => t.type === 'script' ? 'flow-runner' : t.type === 'snowflake' ? svc[t.id] + '-loader' : svc[t.id];
    const lines = targets.map(t => `    ${san(t.name)} = PythonOperator(task_id='${san(t.name)}', python_callable=trigger, op_kwargs={'service': '${task(t)}'})`);
    const deps = [];
    for (const a of targets) for (const b of targets) if (a !== b && reach(a, b) && !targets.some(m => m !== a && m !== b && reach(a, m) && reach(m, b))) deps.push(`    ${san(a.name)} >> ${san(b.name)}`);
    return `"""DAG generated by Pipeline Lab for "${n.name}".

Each task asks a service to run once and waits until it reports back, using files on a shared volume.
That handshake stands in for operators such as SparkSubmitOperator or DbtCloudRunJobOperator in a real deployment.
"""
import pathlib
import time
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

TRIGGERS = pathlib.Path('/triggers')


def trigger(service, timeout=1800, **context):
    run_id = f"{context['run_id']}-{uuid.uuid4().hex[:6]}"
    (TRIGGERS / f'{service}.req').write_text(run_id)
    started = time.time()
    while time.time() - started < timeout:
        done = TRIGGERS / f'{service}.done'
        if done.exists():
            parts = done.read_text().split()
            if parts and parts[0] == run_id:
                if parts[1:] == ['ok']:
                    print(f'{service} finished in {time.time() - started:.1f}s')
                    return
                raise RuntimeError(f'{service} reported a failure, see: docker compose logs {service}')
        time.sleep(2)
    raise TimeoutError(f'{service} did not finish within {timeout}s')


with DAG(
    dag_id='${name}',
    schedule=timedelta(seconds=${n.cfg.interval}),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=${n.cfg.paused ? 'True' : 'False'},
    default_args={'retries': 1, 'retry_delay': timedelta(seconds=10)},
    tags=['pipeline-lab'],
) as dag:
${lines.join('\n') || '    pass'}
${deps.join('\n')}
`;
  }

  function envExample(o) {
    const L = [];
    if (o.needPgOltp) L.push('PG_USER=app', 'PG_PASSWORD=change-me', 'PG_DB=app');
    if (o.needS3) L.push('S3_MODE=local', 'S3_ENDPOINT=http://s3:8333', 'AWS_ACCESS_KEY_ID=lab', 'AWS_SECRET_ACCESS_KEY=change-me', 'AWS_REGION=us-east-1', 'S3_BUCKET_RAW=raw', 'S3_BUCKET_WAREHOUSE=warehouse', 'COMPOSE_PROFILES=local-s3');
    if (o.needWh) L.push('WAREHOUSE_MODE=local', 'WH_USER=analyst', 'WH_PASSWORD=change-me', 'WH_DB=analytics', 'DBT_TARGET=local',
      '# Snowflake (only when WAREHOUSE_MODE=snowflake)', 'SNOWFLAKE_ACCOUNT=', 'SNOWFLAKE_USER=', 'SNOWFLAKE_PASSWORD=', 'SNOWFLAKE_ROLE=SYSADMIN', 'SNOWFLAKE_WAREHOUSE=COMPUTE_WH', 'SNOWFLAKE_DATABASE=PIPELINE_LAB', 'SNOWFLAKE_SCHEMA=RAW', 'SPARK_EXTRA_PACKAGES=');
    o.prompts.forEach(p => L.push(`${p.k}=${p.secret ? '' : p.d}`));
    return L.join('\n') + '\n';
  }

  function setupScript(o) {
    const q = s => s.replace(/"/g, '\\"');
    const ask = o.prompts.map(p => `ask ${p.k} "${q(p.q)}" "${q(p.d)}"${p.secret ? ' secret' : ''}`).join('\n');
    return `#!/usr/bin/env bash
# Pipeline Lab setup: asks for connection settings, writes .env, starts everything with docker compose.
#   ./setup.sh            interactive
#   ./setup.sh --defaults use local defaults for everything (no questions)
#   ./setup.sh --down     stop and remove containers (add -v to drop data volumes)
set -euo pipefail
cd "$(dirname "$0")"

if [ "\${1:-}" = "--down" ]; then shift; docker compose down "$@"; exit 0; fi
DEFAULTS=0; [ "\${1:-}" = "--defaults" ] && DEFAULTS=1

bold() { printf '\\033[1m%s\\033[0m\\n' "$*"; }
command -v docker >/dev/null 2>&1 || { echo "Docker is not installed. Get it at https://docs.docker.com/get-docker/"; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required (the 'docker compose' command)."; exit 1; }
docker info >/dev/null 2>&1 || { echo "Docker is installed but not running. Start Docker Desktop (or the docker service) and run this again."; exit 1; }

rand() { LC_ALL=C tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c "\${1:-20}" || true; }
: > .env.new
put() { printf '%s=%s\\n' "$1" "$2" >> .env.new; export "$1=$2"; }
ask() { # ask VAR "question" "default" [secret]
  local var="$1" question="$2" def="$3" secret="\${4:-}" val=""
  if [ "$DEFAULTS" = 1 ]; then val="$def"
  elif [ -n "$secret" ]; then read -r -s -p "  $question\${def:+ [keep generated]}: " val; echo
  else read -r -p "  $question\${def:+ [$def]}: " val; fi
  put "$var" "\${val:-$def}"
}
yes_no() { # yes_no "question" -> returns 0 for yes
  [ "$DEFAULTS" = 1 ] && return 1
  local a; read -r -p "  $1 [y/N]: " a; [[ "$a" =~ ^[Yy] ]]
}

if [ -f .env ] && [ "$DEFAULTS" = 0 ]; then
  read -r -p "Found an existing .env. Keep it and just start? [Y/n]: " keep
  if [[ ! "$keep" =~ ^[Nn] ]]; then rm -f .env.new; START_ONLY=1; fi
fi

if [ -z "\${START_ONLY:-}" ]; then
bold "Pipeline Lab setup"
echo "Press Enter to accept the value in [brackets]."
${o.needPgOltp ? `
bold "Source database (Postgres)"
ask PG_USER "User" "app"
ask PG_PASSWORD "Password" "$(rand 16)" secret
ask PG_DB "Database" "app"
` : ''}${o.needS3 ? `
bold "Object storage"
if yes_no "Use real AWS S3 instead of local S3 (SeaweedFS)?"; then
  put S3_MODE aws
  put S3_ENDPOINT ""
  ask AWS_ACCESS_KEY_ID "AWS access key id" ""
  ask AWS_SECRET_ACCESS_KEY "AWS secret access key" "" secret
  ask AWS_REGION "AWS region" "us-east-1"
  ask S3_BUCKET_RAW "Bucket for raw files (must exist or be creatable)" "pipeline-lab-raw-$(rand 6 | tr 'A-Z' 'a-z')"
  ask S3_BUCKET_WAREHOUSE "Bucket for Iceberg tables" "pipeline-lab-lake-$(rand 6 | tr 'A-Z' 'a-z')"
  put COMPOSE_PROFILES ""
  printf 'CATALOG_S3_ENDPOINT=\\n' > iceberg.env
else
  put S3_MODE local
  put S3_ENDPOINT "http://s3:8333"
  put AWS_ACCESS_KEY_ID "lab"
  put AWS_SECRET_ACCESS_KEY "$(rand 24)"
  put AWS_REGION "us-east-1"
  put S3_BUCKET_RAW raw
  put S3_BUCKET_WAREHOUSE warehouse
  put COMPOSE_PROFILES local-s3
  printf 'CATALOG_S3_ENDPOINT=http://s3:8333\\n' > iceberg.env
fi
` : ''}${o.needWh ? `
bold "Warehouse"
echo "  Snowflake is a cloud service and can't run in Docker. A local Postgres plays the warehouse unless you connect your account."
ask WH_USER "Local warehouse user" "analyst"
ask WH_PASSWORD "Local warehouse password" "$(rand 16)" secret
ask WH_DB "Local warehouse database" "analytics"
if yes_no "Load into your real Snowflake account instead?"; then
  put WAREHOUSE_MODE snowflake
  put DBT_TARGET snowflake
  ask SNOWFLAKE_ACCOUNT "Account identifier (e.g. abc12345.eu-central-1 or orgname-accountname)" ""
  ask SNOWFLAKE_USER "User" ""
  ask SNOWFLAKE_PASSWORD "Password" "" secret
  ask SNOWFLAKE_ROLE "Role" "SYSADMIN"
  ask SNOWFLAKE_WAREHOUSE "Warehouse" "COMPUTE_WH"
  ask SNOWFLAKE_DATABASE "Database (must exist)" "PIPELINE_LAB"
  ask SNOWFLAKE_SCHEMA "Schema for raw tables (must exist)" "RAW"
  put SPARK_EXTRA_PACKAGES ",net.snowflake:spark-snowflake_2.12:3.0.0,net.snowflake:snowflake-jdbc:3.19.0"
  echo "  Create them first if needed: CREATE DATABASE IF NOT EXISTS $SNOWFLAKE_DATABASE; CREATE SCHEMA IF NOT EXISTS $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA; CREATE SCHEMA IF NOT EXISTS $SNOWFLAKE_DATABASE.ANALYTICS;"
else
  put WAREHOUSE_MODE local
  put DBT_TARGET local
  put SPARK_EXTRA_PACKAGES ""
fi
` : ''}${o.prompts.length ? `
bold "Components"
${ask}
` : ''}
mv .env.new .env
chmod 600 .env
echo "Saved settings to .env (keep it private)."
fi
[ -f iceberg.env ] || printf 'CATALOG_S3_ENDPOINT=http://s3:8333\\n' > iceberg.env
mkdir -p output

bold "Starting containers (first run downloads images and builds, which takes a few minutes)"
docker compose up -d --build

bold "Running"
docker compose ps --format 'table {{.Service}}\\t{{.State}}\\t{{.Status}}' || docker compose ps
echo
${o.urls.map(([l, u]) => `echo "  ${q(l)}: ${u}"`).join('\n')}
echo
echo "Follow the logs:  docker compose logs -f${o.hasRunner ? ' flow-runner' : ''}"
echo "Stop everything:  ./setup.sh --down"
`;
  }

  function readme(nodes, svc, urls, notes) {
    const rows = nodes.map(n => `| ${n.name} | ${PL.TYPES[n.type].label} | ${svc[n.id]} |`).join('\n');
    return `# ${(PL.SCENARIOS.find(s => s.id === S.scenario) || {}).name || 'Pipeline'} (exported from Pipeline Lab)

This folder runs the pipeline you drew, end to end, on your machine with Docker.

## Run it

\`\`\`bash
chmod +x setup.sh
./setup.sh          # asks for settings, writes .env, runs docker compose up
./setup.sh --down   # stop
\`\`\`

Needs Docker with Compose v2 and about 4 GB of free memory for the bigger pipelines. On Windows run it from WSL or Git Bash.

## Components

| On the canvas | Type | Service(s) |
|---|---|---|
${rows}

## Open

${urls.map(([l, u]) => `- ${l}: ${u}`).join('\n') || '- Results print in `docker compose logs -f flow-runner` and are saved to `./output`.'}

## What differs from the cloud

${notes.map(x => '- ' + x).join('\n') || '- Nothing: every component runs as designed.'}
- Snowflake is SaaS. Locally a Postgres container plays the warehouse; answer “y” in setup.sh to load into your account.
- Kafka is a single broker, so replication settings don’t apply locally.

## Files

- \`docker-compose.yml\`: all services and how they connect.
- \`py/\`: Python services (flow runner, loaders, data generators) built into one image.
- \`spark/\`, \`flink/\`, \`dbt/\`, \`airflow/\`, \`connect/\`: job definitions per component.
- \`output/\`: CSV results of Table blocks.
`;
  }

  /* ---------- export modal ---------- */
  PL.openExport = function () {
    const res = PL.buildExport();
    const ui = PL.ui;
    if (!res) { ui.toast('Add some components first. The export turns the canvas into a docker compose bundle.', true); return; }
    const paths = Object.keys(res.files).sort((a, b) => order(a) - order(b) || a.localeCompare(b));
    function order(p) { return ['setup.sh', 'docker-compose.yml', 'README.md', '.env.example'].indexOf(p) + 1 || 9; }
    let cur = paths[0];
    const card = ui.modal('Export to Docker', `
      <div class="exp-notes">
        <div class="note"><b>Run it:</b> download the zip, unzip it, then run <span class="mono">./setup.sh</span>. It asks for credentials and connection details, writes <span class="mono">.env</span> and starts <span class="mono">docker compose up</span>. ${res.services.length} services, ${paths.length} files.</div>
        ${res.notes.length ? `<div class="note warn">${res.notes.map(x => ui.esc(x)).join('<br>')}</div>` : ''}
      </div>
      <div class="exp-grid">
        <div class="exp-files" id="expFiles" role="listbox" aria-label="Files">${paths.map(p => `<button data-p="${ui.esc(p)}">${ui.esc(p)}</button>`).join('')}</div>
        <pre class="exp-code" id="expCode" tabindex="0"></pre>
      </div>`,
      `<button class="btn" id="expCopy">Copy file</button><button class="btn primary" id="expZip"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 4v11M7 10l5 5 5-5M5 19.5h14"/></svg>Download .zip</button>`);
    const show = p => { cur = p; card.querySelector('#expCode').textContent = res.files[p]; card.querySelectorAll('#expFiles button').forEach(b => b.classList.toggle('on', b.dataset.p === p)); };
    card.querySelector('#expFiles').onclick = e => { const b = e.target.closest('button'); if (b) show(b.dataset.p); };
    card.querySelector('#expCopy').onclick = () => ui.copyText(res.files[cur], 'Copied ' + cur);
    card.querySelector('#expZip').onclick = async () => {
      if (!window.JSZip) { ui.toast('The zip library did not load. Copy the files one by one instead.', true); return; }
      const zip = new JSZip(), root = zip.folder('pipeline-lab-export');
      for (const p of paths) root.file(p, res.files[p], { unixPermissions: /\.sh$/.test(p) ? '755' : '644' });
      const blob = await zip.generateAsync({ type: 'blob', platform: 'UNIX' });
      const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'pipeline-lab-export.zip';
      document.body.appendChild(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(a.href), 5000);
      ui.toast('Downloaded pipeline-lab-export.zip');
    };
    show(cur);
  };
})(window.PL);
