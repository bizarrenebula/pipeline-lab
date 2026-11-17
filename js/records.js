/* Pipeline Lab — record model.
 * The simulation moves counts. This file gives those counts a face: one concrete record
 * (a row change, a click event) and what it looks like on every hop of the pipeline.
 * hop(node, rec) says what a component does to a record that arrives, how long that takes,
 * and what it sends on. Step-through mode (trace.js) and the link data inspector both use it.
 */
window.PL = window.PL || {};
(function (PL) {
  'use strict';
  const S = PL.S;
  const R = PL.REC = {};
  const pick = a => a[Math.floor(Math.random() * a.length)];
  const rint = (a, b) => a + Math.floor(Math.random() * (b - a + 1));
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const clone = o => JSON.parse(JSON.stringify(o));
  // a fixed wall clock for the demo, moved forward by simulation time
  const BASE_MS = Date.UTC(2026, 10, 17, 7, 0, 0);
  const wall = t => BASE_MS + Math.round((t == null ? S.t : t) * 1000);
  const iso = ms => new Date(ms).toISOString().replace('.000Z', 'Z');
  R.iso = iso; R.wall = wall;

  /* ---------- the little world the source database holds ---------- */
  const CUSTOMERS = [
    { id: 'C-104', name: 'Ada Moreno', city: 'Lisbon', tier: 'gold' },
    { id: 'C-221', name: 'Ken Ito', city: 'Berlin', tier: 'silver' },
    { id: 'C-307', name: 'Lena Novak', city: 'Austin', tier: 'bronze' }
  ];
  const CITIES = ['Lisbon', 'Berlin', 'Austin', 'Osaka', 'Krakow', 'Toronto', 'Nairobi', 'Lyon'];
  const STATUS = ['created', 'paid', 'shipped', 'delivered'];
  const EVENTS = ['page_view', 'click', 'add_to_cart', 'search', 'checkout'];
  let lsn = 0x16B3A28, xid = 7410, orderSeq = 48120;
  function nextLsn() { lsn += rint(40, 600); return '0/' + lsn.toString(16).toUpperCase(); }

  // murmur-ish string hash, stable, so the same key always lands on the same partition
  function hash(s) { let h = 0x9747b28c; s = String(s); for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x5bd1e995); h ^= h >>> 15; } return h >>> 0; }
  R.hash = hash;
  R.partitionFor = function (k, key) {
    const p = +k.cfg.partitions;
    if (k.cfg.skew && (String(key).endsWith('4') || hash(key) % 2 === 0)) return 0; // hot key: half of the traffic
    return hash(key) % p;
  };

  /* A source change: what the application did. op is c (insert), u (update) or d (delete). */
  R.sourceChange = function (src, op, table) {
    const t = S.t;
    if (src.type === 'app') {
      const uid = 'u' + rint(1000, 9999);
      return { fmt: 'event', op: 'c', key: uid, table: 'clicks', t,
        data: { event_id: 'e-' + (hash(t + uid) % 1e8).toString(36), user_id: uid, session_id: 's-' + rint(100, 999),
          event: pick(EVENTS), page: pick(['/home', '/product/sku-881', '/cart', '/search?q=boots']), device: pick(['ios', 'android', 'web']), ts: iso(wall(t)) } };
    }
    if (src.type === 'api' || src.type === 'file' || src.type === 'script') {
      const res = PL.RT && PL.RT.results[src.id];
      if (res && res.rows && res.rows.length) { const row = pick(res.rows.slice(0, 50)); return { fmt: 'row', op: 'c', key: row.id != null ? row.id : '#' + rint(1, 99), table: src.name, real: true, t, data: row }; }
      const row = src.type === 'file' ? { city: 'Tokyo', country: 'Japan', population: 37400068 } : { userId: 1, id: rint(1, 100), title: 'sunt aut facere repellat', body: 'quia et suscipit…' };
      return { fmt: 'row', op: 'c', key: row.id || row.city, table: src.name, t, data: row };
    }
    // postgres (or anything else): a row change in orders or customers
    op = op || 'u';
    table = table || 'orders';
    const tx = ++xid;
    if (table === 'customers') {
      const c = Object.assign({}, pick(CUSTOMERS));
      // keep the customer's current city in step with dbt's history table, so before/after line up
      const dbt = S.nodes.find(x => x.type === 'dbt' && x.rt.scd);
      const curRow = dbt && dbt.rt.scd.find(x => x.id === c.id && x.cur);
      if (curRow) c.city = curRow.city;
      let city; do { city = pick(CITIES); } while (city === c.city);
      const before = Object.assign({}, c, { updated_at: iso(wall(t) - rint(3, 90) * 86400000) });
      const after = Object.assign({}, c, { city, updated_at: iso(wall(t)) });
      if (op === 'c') return { fmt: 'change', op, key: 'C-' + rint(400, 999), table, tx, t, before: null, after: Object.assign({}, after, { id: 'C-' + rint(400, 999), name: 'New Customer', tier: 'bronze' }) };
      if (op === 'd') return { fmt: 'change', op, key: c.id, table, tx, t, before, after: null };
      return { fmt: 'change', op, key: c.id, table, tx, t, before, after };
    }
    const id = op === 'c' ? ++orderSeq : rint(46000, orderSeq);
    const cust = pick(CUSTOMERS).id, amount = +(rint(1200, 48000) / 100).toFixed(2);
    const si = rint(0, 2);
    const before = { id, customer_id: cust, status: STATUS[si], amount, currency: 'EUR', updated_at: iso(wall(t) - rint(60, 7200) * 1000) };
    if (op === 'c') return { fmt: 'change', op, key: id, table, tx, t, before: null, after: Object.assign({}, before, { status: 'created', updated_at: iso(wall(t)) }) };
    if (op === 'd') return { fmt: 'change', op, key: id, table, tx, t, before, after: null };
    return { fmt: 'change', op, key: id, table, tx, t, before, after: Object.assign({}, before, { status: STATUS[si + 1], updated_at: iso(wall(t)) }) };
  };

  const OPW = { c: 'INSERT', u: 'UPDATE', d: 'DELETE' };
  const opName = op => OPW[op] || 'EVENT';
  R.opName = opName;
  const rowOf = rec => rec.after || rec.before || rec.data || {};
  R.label = rec => {
    if (!rec) return '';
    if (rec.table === 'clicks') return 'event ' + rec.key;
    if (rec.fmt === 'agg') return 'aggregate';
    if (rec.fmt === 'scd') return 'SCD2 ' + rec.key;
    return { c: 'insert', u: 'update', d: 'delete' }[rec.op] + ' ' + (rec.table === 'customers' ? rec.key : rec.table === 'orders' ? '#' + rec.key : rec.key);
  };
  function topicFor(rec, k) {
    if (rec.table === 'clicks') return k ? (k.name.toLowerCase().includes('click') ? 'clicks' : 'app.events') : 'app.events';
    return 'shop.public.' + rec.table;
  }
  function plainRow(rec) {
    const r = rowOf(rec);
    return Object.assign({}, r);
  }

  /* ---------- what each component does to a record ----------
   * hop(node, rec, from) → { out, title, body, wait, waitText }
   * out null ends that branch (the record is dropped, or the component is a sink).
   */
  const fmtS = s => s < 1 ? Math.round(s * 1000) + ' ms' : s < 60 ? (Math.round(s * 10) / 10) + ' s' : Math.floor(s / 60) + ' min ' + Math.round(s % 60) + ' s';
  R.fmtS = fmtS;
  const orchestrator = n => { const l = PL.inLinks(n).find(x => PL.isControlLink(x)); return l ? PL.byId(l.from) : null; };
  function schedWait(n, every) {
    const o = orchestrator(n);
    if (o) { const iv = +(o.cfg.interval || o.cfg.every || 30); return { wait: iv / 2, text: `waits for ${o.name} to trigger it (every ${iv} s, so ${fmtS(iv / 2)} on average)` }; }
    return { wait: every / 2, text: `waits for the next scheduled run (every ${every} s, so ${fmtS(every / 2)} on average)` };
  }

  const HOP = {
    postgres(n, rec) {
      const L = nextLsn(); rec.lsn = L;
      const r = rowOf(rec);
      const sql = rec.op === 'c' ? `INSERT INTO ${rec.table} VALUES (${rec.key}, …)` : rec.op === 'd' ? `DELETE FROM ${rec.table} WHERE id = ${fmtKey(rec.key)}`
        : `UPDATE ${rec.table} SET ${changedCols(rec).map(c => `${c} = '${r[c]}'`).join(', ')} WHERE id = ${fmtKey(rec.key)}`;
      return { out: Object.assign({}, rec, { fmt: 'wal', lsn: L }),
        title: `The app commits transaction ${rec.tx}`,
        body: `<code>${esc(sql)}</code>. Before the table page changes, Postgres appends the change to its write-ahead log (WAL) at position <b>${L}</b> and flushes it to disk. That log entry is what makes the commit durable, and it is also what CDC reads. With <code>wal_level = logical</code> the entry carries enough to rebuild the whole row.`,
        wait: 0.004, waitText: 'commit and WAL flush, a few milliseconds' };
    },
    app(n, rec) {
      return { out: Object.assign({}, rec, { fmt: 'event' }),
        title: 'A user taps in the app',
        body: `The app’s producer client builds an event and buffers it. It sends a batch when <code>batch.size</code> fills or <code>linger.ms</code> passes. The event is keyed by <b>${esc(rec.key)}</b>, so every event of this user lands in the same partition and stays in order.`,
        wait: 0.01, waitText: 'client batching (linger.ms)' };
    },
    api(n, rec) { return { out: Object.assign({}, rec, { fmt: 'row' }), title: 'An HTTP request returns JSON', body: `${rec.real ? 'This is a real row from your last run.' : 'Press Run flow to see real rows here.'} The block requests <code>${esc(shortUrl(n.cfg.url))}</code>, takes the array of records and passes each one on as a row.`, wait: 0.2, waitText: 'HTTP round trip' }; },
    file(n, rec) { return { out: Object.assign({}, rec, { fmt: 'row' }), title: 'The file is parsed into rows', body: `${rec.real ? 'This is a real row from your file.' : 'Press Run flow to parse the real file.'} Each CSV line becomes an object keyed by the header, with numbers converted.`, wait: 0.05, waitText: 'parse time' }; },
    script(n, rec) { return { out: Object.assign({}, rec, { fmt: 'row' }), title: 'Your script transforms the rows', body: `The script receives <code>rows</code> and returns <code>result</code>. Anything it keeps moves on. ${PL.isOrchestrated(n) ? 'It runs only when its trigger fires.' : ''}`, wait: 0.1, waitText: 'script run' }; },
    table(n, rec) { return { out: null, title: 'The row is shown in a table', body: 'Table output is a sink: rows end here and show in the Runtime panel.', wait: 0, waitText: '' }; },
    cdc(n, rec) {
      if (rec.table === 'clicks' || rec.fmt === 'row') return { out: rec, title: 'Debezium passes it through', body: 'Debezium reads database logs. Events from other sources pass unchanged.', wait: 0.1, waitText: '' };
      const env = { before: rec.before, after: rec.after, source: { version: '2.7.0.Final', connector: 'postgresql', name: 'shop', db: 'shop', schema: 'public', table: rec.table, txId: rec.tx, lsn: lsnNum(rec.lsn), ts_ms: wall(rec.t) }, op: rec.op, ts_ms: wall(rec.t) + 180 };
      if (n.cfg.mode === 'poll') {
        const poll = +n.cfg.poll;
        if (rec.op === 'd') return { out: null, dropped: true,
          title: 'Polling never sees this delete',
          body: `The connector runs <code>SELECT * FROM ${rec.table} WHERE updated_at &gt; :last</code> every ${poll} s. A deleted row is simply gone, so nothing matches. <b>The delete is lost</b> and every table downstream keeps the row forever. Log-based capture reads the WAL instead and does see it.`, wait: poll / 2, waitText: 'next poll' };
        const row = rec.after;
        return { out: Object.assign({}, rec, { fmt: 'debezium', env: { before: null, after: row, source: { connector: 'jdbc-poll', table: rec.table, query_ts: iso(wall(rec.t + poll / 2)) }, op: rec.op === 'c' ? 'c' : 'r', ts_ms: wall(rec.t + poll / 2) } }),
          title: 'A polling query finds the changed row',
          body: `Every ${poll} s the connector asks for rows with <code>updated_at</code> newer than last time. It only sees the row as it is now: <b>no “before” image</b>, and if the row changed twice between polls the first change is lost. ${rec.op === 'u' ? 'It can’t even tell this was an update and not an insert.' : ''}`,
          wait: poll / 2, waitText: `poll interval ${poll} s, ${fmtS(poll / 2)} on average` };
      }
      return { out: Object.assign({}, rec, { fmt: 'debezium', env }),
        title: 'Debezium reads the WAL and emits a change event',
        body: `Debezium holds a <b>replication slot</b>, so Postgres keeps WAL until Debezium confirms it. It decodes the entry at ${rec.lsn} with the <code>pgoutput</code> plugin and wraps it in an envelope: <code>op</code> = <b>${rec.op}</b>, the row <b>before</b> and <b>after</b>, and <code>source</code> with the LSN and transaction id. ${rec.op === 'u' ? 'Compare before and after below: only the changed columns differ.' : rec.op === 'd' ? 'A delete has <code>after: null</code>, and Debezium also sends a tombstone (key with null value) so Kafka compaction can drop the key.' : 'An insert has <code>before: null</code>.'} The key is the primary key, <b>${esc(fmtKey(rec.key))}</b>.`,
        wait: 0.3, waitText: 'decode and send, sub-second' };
    },
    kafka(n, rec) {
      const p = R.partitionFor(n, rec.key);
      const pt = n.rt.parts[p]; const offset = Math.floor(pt ? pt.end : 0) + rint(0, 3);
      const topic = topicFor(rec, n);
      const value = rec.fmt === 'debezium' ? rec.env : rec.fmt === 'event' ? rec.data : plainRow(rec);
      const rf = +n.cfg.rf, acks = n.cfg.acks;
      const out = Object.assign({}, rec, { fmt: 'kafka', kafka: { topic, partition: p, offset, key: rec.key, timestamp: wall(rec.t + 0.3), headers: rec.fmt === 'debezium' ? { '__debezium.op': rec.op } : {}, value } });
      const lat = acks === 'all' ? (rf === 1 ? 0.05 : 0.15) : 0.03;
      let dead = '';
      if (n.cfg.brokerDown) dead = rf === 1 && p % 3 === 2 ? ' <b>Broker 3 is down and this partition has no replica, so the write fails until it returns.</b>' : ' Broker 3 is down: another replica was elected leader, so the write still succeeds.';
      return { out, kafka: out.kafka,
        title: `Kafka appends it to partition ${p} at offset ${offset.toLocaleString('en-US')}`,
        body: `The producer hashes the key (<code>murmur2(${esc(String(rec.key))}) % ${n.cfg.partitions}</code>) to pick partition <b>${p}</b>${n.cfg.skew && p === 0 ? ', the hot partition that gets half of all traffic' : ''}. The partition leader appends it to the end of its log and gives it the next offset. ${acks === 'all' ? `With <code>acks=all</code> the write is confirmed only after all ${rf} in-sync replicas have it.` : 'With <code>acks=1</code> only the leader confirms, which is faster but can lose the record if that broker dies right now.'} Records are not deleted when read: each consumer group just remembers its own offset.${dead}`,
        wait: lat, waitText: `replication with acks=${acks}` };
    },
    spark(n, rec, from) {
      const batch = n.cfg.mode === 'batch';
      const T = batch ? null : +n.cfg.trigger;
      const w = batch ? schedWait(n, +n.cfg.every) : { wait: T / 2 + Math.max(0.4, n.rt.lastDur || 0.4), text: `micro-batch trigger every ${T} s plus the batch itself` };
      const src = rowOf(rec);
      const meta = rec.kafka ? `offsets ${rec.kafka.partition}:${rec.kafka.offset}` : 'the new input';
      const down = PL.outLinks(n).map(l => PL.byId(l.to)).filter(x => x && !PL.isControlLink({ from: n.id, to: x.id }));
      const sinkIce = down.some(d => d.type === 'iceberg');
      if (n.cfg.job === 'agg') {
        const r = rowOf(rec);
        const ws = Math.floor(rec.t / 60) * 60;
        const agg = rec.table === 'clicks'
          ? { window_start: iso(wall(ws)), window_end: iso(wall(ws + 60)), event: r.event, events: rint(400, 2600), users: rint(120, 900) }
          : { window_start: iso(wall(ws)), window_end: iso(wall(ws + 60)), status: r.status, orders: rint(30, 240), revenue_eur: +(rint(200000, 1900000) / 100).toFixed(2) };
        return { out: Object.assign({}, rec, { fmt: 'agg', data: agg }),
          title: `Spark ${batch ? 'batch job' : 'micro-batch #' + (n.rt.runNo + 1)} aggregates it`,
          body: `The record is read as part of ${meta}. Stage 1 parses the JSON and emits <code>(window, ${rec.table === 'clicks' ? 'event' : 'status'})</code> pairs. That needs all records of one key on the same task, so Spark <b>shuffles</b>: it writes them to local disk by key hash and stage 2 pulls them over the network. Stage 2 sums each group. The single record is now one count inside an aggregate row.${batch ? '' : ' Running aggregates are kept in a state store between micro-batches.'}`,
          wait: w.wait, waitText: w.text };
      }
      const row = Object.assign({}, src);
      if (rec.table !== 'clicks') { row._op = rec.op; row._lsn = rec.lsn || null; }
      if (rec.table === 'orders' && row.amount != null) row.amount_usd = +(row.amount * 1.08).toFixed(2);
      if (rec.table === 'clicks') { row.country = pick(['PT', 'DE', 'US', 'JP']); row.is_bot = false; }
      row._ingested_at = iso(wall(rec.t + w.wait));
      row.event_date = iso(wall(rec.t)).slice(0, 10);
      const merge = sinkIce && rec.op !== 'c';
      return { out: Object.assign({}, rec, { fmt: 'row', data: row }),
        title: `Spark ${batch ? 'batch job' : 'micro-batch #' + (n.rt.runNo + 1)} cleans and enriches it`,
        body: `The record is picked up with ${meta}. ${batch ? 'A batch job reads everything that arrived since the last run.' : `Structured Streaming wakes every ${T} s, reads the new offsets from each partition (one task per partition), and processes them as a small batch.`} Parse, cast types, add derived columns: these are narrow transformations, so there is <b>no shuffle</b> and each task works alone. ${merge ? `Because this is an ${rec.op === 'd' ? 'delete' : 'update'}, the sink uses <code>MERGE INTO … ON id</code> instead of a plain append.` : ''} The consumed offsets are written to the checkpoint only after the output commits, which is what makes a restart safe.`,
        wait: w.wait, waitText: w.text };
    },
    flink(n, rec) {
      const ck = +n.cfg.ckpt, eos = n.cfg.eos;
      if (n.cfg.op === 'window') {
        const ws = Math.floor(rec.t / 10) * 10, wm = +n.cfg.watermark;
        const r = rowOf(rec);
        const agg = { window_start: iso(wall(ws)), window_end: iso(wall(ws + 10)), key: r.event || r.status || 'all', count: rint(80, 900) };
        const fire = (ws + 10 + wm) - rec.t;
        return { out: Object.assign({}, rec, { fmt: 'agg', data: agg }),
          title: 'Flink adds it to a 10-second window',
          body: `<code>keyBy</code> sends it to the parallel instance that owns its key, and the event joins the window <b>[${iso(wall(ws)).slice(11, 19)}, ${iso(wall(ws + 10)).slice(11, 19)})</b>. The window is kept in keyed state. It fires when the <b>watermark</b> passes its end; the watermark trails the newest event time by ${wm} s to wait for late events. ${eos ? `Exactly-once: the result is committed with the next checkpoint, up to ${ck} s later.` : ''}`,
          wait: fire + (eos ? ck / 2 : 0), waitText: `window end + ${wm} s watermark${eos ? ' + checkpoint' : ''}` };
      }
      const row = Object.assign({}, rowOf(rec), { _processed_at: iso(wall(rec.t + 0.05)) });
      return { out: Object.assign({}, rec, { fmt: 'row', data: row }),
        title: 'Flink transforms it as it arrives',
        body: `A stateless map handles one record at a time, in milliseconds. ${eos ? `With exactly-once sinks the output sits in an open transaction until the next checkpoint barrier passes, every ${ck} s, so downstream sees it ${fmtS(ck / 2)} later on average.` : 'Without exactly-once the output goes out at once, but after a crash the records since the last checkpoint are sent again.'}`,
        wait: eos ? ck / 2 : 0.05, waitText: eos ? 'wait for checkpoint commit' : 'per-record processing' };
    },
    s3(n, rec) {
      const fmtx = n.cfg.format, part = String(rint(0, 15)).padStart(5, '0');
      const key = `s3://lake/raw/${(rec.table || 'data')}/dt=${iso(wall(rec.t)).slice(0, 10)}/hour=${iso(wall(rec.t)).slice(11, 13)}/part-${part}-${(hash(rec.t) % 1e6).toString(16)}.${fmtx === 'parquet' ? 'snappy.parquet' : fmtx}`;
      const rows = rint(800, 9000), size = rows * { json: 900, csv: 450, parquet: 120 }[fmtx];
      return { out: Object.assign({}, rec, { fmt: 'file', file: { key, format: fmtx, rows, bytes: size }, data: rec.data || plainRow(rec) }),
        title: `The row lands in a ${fmtx.toUpperCase()} file in S3`,
        body: `The writer buffers rows and rolls a new object every 2 s. ${fmtx === 'parquet' ? 'Parquet stores each column separately, compressed, with min/max stats per row group, so readers can skip most of the file.' : `${fmtx.toUpperCase()} is plain text: every reader must parse every byte.`} S3 has no tables, only objects under a prefix like <code>dt=…/hour=…/</code>. Small files pile up quickly, and every file is one more request for the reader.`,
        wait: 1, waitText: 'file roll interval' };
    },
    iceberg(n, rec) {
      const T = +n.cfg.commit, mode = n.cfg.mode, snap = n.rt.snapId + 1;
      const rowLevel = rec.op === 'u' || rec.op === 'd';
      const sid = bigId(snap);
      const commit = { snapshot_id: sid, parent_id: n.rt.snapId ? bigId(n.rt.snapId) : null, sequence_number: snap, operation: rowLevel ? 'overwrite' : 'append',
        summary: { 'added-data-files': 1, 'added-records': rint(800, 6000), 'added-delete-files': rowLevel && mode === 'mor' ? 1 : 0, 'removed-data-files': rowLevel && mode === 'cow' ? 1 : 0 },
        manifest_list: `s3://lake/warehouse/${esc(n.name.replace(/\W+/g, '_').toLowerCase())}/metadata/snap-${sid}-1-${(hash(sid) % 1e8).toString(16)}.avro` };
      const detail = !rowLevel ? 'An append just adds a new data file and lists it in a new manifest.'
        : mode === 'cow' ? `Copy-on-write: the data file that holds row ${esc(fmtKey(rec.key))} is <b>rewritten in full</b> with the new value. Writes are slow, reads stay fast.`
        : `Merge-on-read: the writer adds a small <b>delete file</b> that says “row ${esc(fmtKey(rec.key))} in file X is gone”, plus a data file with the new version. Writes are fast; readers merge the two until compaction cleans up.`;
      return { out: Object.assign({}, rec, { fmt: 'commit', commit, data: rec.data || plainRow(rec) }),
        title: `Iceberg commits it in snapshot ${snap}`,
        body: `The writer collects rows for ${T} s, writes Parquet data files, then writes a manifest (which files, their column stats), a manifest list, and a new <code>metadata.json</code>. The commit is one atomic swap of the catalog pointer to that new metadata file. Readers see either the old snapshot or the new one, never half. ${detail} Old snapshots stay, so you can query <code>FOR VERSION AS OF</code> any of them.`,
        wait: T / 2 + 0.3, waitText: `commit interval ${T} s` };
    },
    snowflake(n, rec, from) {
      const fromIce = from && from.type === 'iceberg', fromKafka = from && from.type === 'kafka', fromS3 = from && from.type === 's3';
      let data, title, body, wait, waitText;
      const r = rowOf(rec.env || rec);
      if (fromIce) {
        data = Object.assign({}, rec.data || r); title = 'Snowflake reads the Iceberg snapshot'; wait = 3; waitText = 'metadata refresh';
        body = 'Snowflake queries the Iceberg table where it lies, through the catalog. No copy is made: it reads the same Parquet files, using the manifest stats to skip files that can’t match.';
      } else if (fromKafka && n.cfg.ingest !== 'copy') {
        data = { RECORD_METADATA: { topic: rec.kafka.topic, partition: rec.kafka.partition, offset: rec.kafka.offset, key: String(rec.kafka.key), CreateTime: rec.kafka.timestamp }, RECORD_CONTENT: rec.kafka.value };
        const st = n.cfg.ingest === 'stream';
        title = st ? 'Snowpipe Streaming inserts the row' : 'Snowpipe loads the file the connector wrote';
        wait = st ? 1.5 : 8; waitText = st ? 'streaming flush' : 'file staging and pipe load';
        body = `The Kafka connector writes the record into a raw table with two VARIANT columns: <code>RECORD_METADATA</code> (topic, partition, offset) and <code>RECORD_CONTENT</code> (the payload as JSON). ${st ? 'Snowpipe Streaming sends rows straight into the table over an SDK channel, no files.' : 'Classic Snowpipe stages small files and loads each one, which adds latency.'} It runs on serverless compute, so the warehouse can stay suspended. The raw table is append-only: the update is a new row, and dbt later decides what the current row is.`;
      } else {
        data = Object.assign({ _loaded_at: iso(wall(rec.t + 2)) }, rec.data || r);
        const w = schedWait(n, +n.cfg.every);
        title = 'COPY INTO loads the batch'; wait = w.wait; waitText = w.text;
        body = `A scheduled <code>COPY INTO raw.${rec.table || 'data'} FROM @stage</code> runs on the warehouse (${n.cfg.size}). ${PL.sfState(n) === 'suspended' ? 'The warehouse is suspended, so it first resumes, which takes a few seconds and starts billing (60 s minimum).' : ''} Snowflake remembers which ${fromS3 ? 'files' : 'records'} it already loaded, so a retry doesn’t create duplicates.`;
      }
      return { out: Object.assign({}, rec, { fmt: 'row', data, sfRaw: !!data.RECORD_CONTENT }), title, body, wait, waitText };
    },
    dbt(n, rec, from, live) {
      const scd = n.cfg.scd, mat = n.cfg.mat;
      const w = schedWait(n, +n.cfg.every);
      const r = Object.assign({}, rowOf(rec.env || (rec.data && rec.data.RECORD_CONTENT) || rec));
      if (rec.table === 'customers' && scd !== 'none' && rec.op === 'u' && rec.before) {
        const b = rec.before, a = rec.after;
        const nowT = rec.t + w.wait;
        if (live) applyScd(n, b.id, a.city, nowT);
        if (scd === 'scd2') {
          const rows = [
            { customer_id: b.id, city: b.city, valid_from: b.updated_at, valid_to: iso(wall(nowT)), is_current: false, dbt_scd_id: (hash(b.id + b.city) % 1e9).toString(16) },
            { customer_id: a.id, city: a.city, valid_from: iso(wall(nowT)), valid_to: null, is_current: true, dbt_scd_id: (hash(a.id + a.city) % 1e9).toString(16) }];
          return { out: Object.assign({}, rec, { fmt: 'scd', rows }),
            title: 'dbt snapshot versions the customer (SCD2)',
            body: `<code>dbt snapshot</code> compares the new row with the current version of <b>${esc(b.id)}</b>. The city changed (${esc(b.city)} → ${esc(a.city)}), so it <b>closes</b> the old row by setting <code>valid_to</code> and <b>inserts</b> a new current row. Facts join on the version that was valid at their event time, so last month’s sales stay in ${esc(b.city)}. Open dbt’s Configure tab: the table there just changed.`,
            wait: w.wait, waitText: w.text };
        }
        return { out: Object.assign({}, rec, { fmt: 'row', data: { customer_id: a.id, city: a.city, updated_at: iso(wall(nowT)) } }),
          title: 'dbt overwrites the customer (SCD1)',
          body: `SCD1 keeps one row per customer: the city becomes ${esc(a.city)} and ${esc(b.city)} is forgotten. Simple and small, but every past order now reports under the new city.`, wait: w.wait, waitText: w.text };
      }
      const how = mat === 'incremental' ? `<code>incremental</code>: it selects only rows newer than the last run (<code>where _loaded_at &gt; (select max(_loaded_at) from {{ this }})</code>) and runs <code>MERGE</code> on <code>order_id</code>, so this ${rec.op === 'd' ? 'delete removes the row' : rec.op === 'u' ? 'update replaces the old row' : 'insert adds a row'}.`
        : mat === 'table' ? '<code>table</code>: it rebuilds the whole table from scratch every run, so cost grows with history, not with change.'
        : '<code>view</code>: nothing is stored. The SQL runs each time the dashboard queries it.';
      const row = rec.table === 'clicks' ? { event_date: (r.ts || '').slice(0, 10), event: r.event || r.key, sessions: rint(80, 400), users: rint(60, 300) }
        : { order_id: r.id, customer_id: r.customer_id, customer_city: (CUSTOMERS.find(c => c.id === r.customer_id) || {}).city || null, status: r.status, amount_eur: r.amount, is_deleted: rec.op === 'd', updated_at: r.updated_at };
      return { out: Object.assign({}, rec, { fmt: 'row', data: row }),
        title: `dbt models it into ${rec.table === 'clicks' ? 'fct_sessions' : 'fct_orders'}`,
        body: `dbt runs SQL in the warehouse in dependency order: <code>stg_${rec.table || 'data'}</code> parses and renames the raw columns${rec.fmt === 'row' && rec.sfRaw ? ' (it pulls fields out of the JSON in RECORD_CONTENT and keeps only the newest version of each key)' : ''}, then the fact model joins the customer dimension. Materialization is ${how}`,
        wait: w.wait, waitText: w.text };
    },
    bi(n, rec) {
      const live = n.cfg.mode === 'live';
      return { out: null, title: 'The dashboard shows it',
        body: live ? 'In live mode every page view sends SQL to the warehouse, so the tile shows the change on its next query. It is always fresh, but it keeps the warehouse awake and billing.'
          : `In extract mode the tool copies the data on a schedule (every ${n.cfg.refresh} s). Between refreshes, readers see an older copy.`,
        wait: live ? 2 : +n.cfg.refresh / 2, waitText: live ? 'next query' : `extract refresh every ${n.cfg.refresh} s` };
    }
  };
  function fmtKey(k) { return typeof k === 'number' ? k : `'${k}'`; }
  function changedCols(rec) { if (!rec.before || !rec.after) return []; return Object.keys(rec.after).filter(k => JSON.stringify(rec.after[k]) !== JSON.stringify(rec.before[k])); }
  R.changedCols = changedCols;
  function lsnNum(L) { if (!L) return null; const p = L.split('/'); return parseInt(p[0], 16) * 4294967296 + parseInt(p[1], 16); }
  function bigId(i) { return String(3051729675574597004n + BigInt(i) * 7919n * 104729n); }
  R.bigId = bigId;
  function shortUrl(u) { try { const x = new URL(u); return x.host + x.pathname; } catch (e) { return u; } }
  function applyScd(n, id, city, t) {
    const r = n.rt; if (!r.scd) PL.scdApply(n, null);
    const cur = r.scd.find(x => x.id === id && x.cur); if (!cur || cur.city === city) return;
    if (n.cfg.scd === 'scd1') { cur.city = city; cur.from = S.t; return; }
    cur.to = S.t; cur.cur = false; r.scd.unshift({ id, name: cur.name, city, from: S.t, to: null, cur: true, traced: true });
    if (r.scd.length > 9) r.scd.length = 9;
  }

  R.applyScd = applyScd;
  R.hop = function (node, rec, from, live) {
    if (node.cfg.down && node.type !== 'kafka') return { out: null, dropped: true, title: `${node.name} is offline`, body: 'The record waits upstream until the component comes back. That is the backlog you see growing on the canvas.', wait: 0, waitText: '' };
    const f = HOP[node.type];
    if (!f) return { out: rec, title: node.name, body: '', wait: 0, waitText: '' };
    return f(node, rec, from, live);
  };

  /* ---------- paths ---------- */
  const isSource = n => !PL.upstream(n).length && !PL.TYPES[n.type].control;
  R.isSource = isSource;
  // the chain of nodes from a source down to n, following the first data input each time
  R.pathTo = function (n) {
    const path = [n]; const seen = new Set([n.id]);
    let cur = n;
    while (true) {
      const up = PL.upstream(cur).filter(u => !seen.has(u.id));
      if (!up.length) break;
      cur = up.find(u => u.type === 'cdc' || u.type === 'kafka') || up[0];
      seen.add(cur.id); path.unshift(cur);
    }
    return path;
  };
  /* A record on link l: start a change at the source, run it through every hop up to l.from. */
  R.sampleOnLink = function (l, op) {
    const a = PL.byId(l.from); if (!a) return null;
    if (PL.isControlLink(l)) {
      const b = PL.byId(l.to);
      const run = a.rt.runs && a.rt.runs[0];
      const dag = a.name.toLowerCase().replace(/\W+/g, '_');
      return { fmt: 'trigger', data: a.type === 'airflow'
        ? { dag_id: dag, run_id: 'scheduled__' + iso(wall(run ? run.t : S.t)), task_id: b.name.toLowerCase().replace(/\W+/g, '_'), try_number: 1, logical_date: iso(wall(run ? run.t : S.t)), state: run && run.tasks[b.id] ? run.tasks[b.id].state : 'queued', operator: { spark: 'SparkSubmitOperator', dbt: 'DbtRunOperator', snowflake: 'SnowflakeOperator', script: 'PythonOperator' }[b.type] || 'BashOperator' }
        : { trigger: 'interval', every_s: +a.cfg.every, fired_at: iso(wall(run ? run.t : S.t)), target: b.name } };
    }
    const path = R.pathTo(a);
    const src = path[0];
    let table = 'orders';
    if (op === 'u' && Math.random() < 0.35) table = 'customers';
    let rec = R.sourceChange(src, op, table);
    for (let i = 0; i < path.length; i++) {
      const h = R.hop(path[i], rec, path[i - 1] || null, false);
      if (!h.out) return h.dropped ? { fmt: 'dropped', op: rec.op, why: h.title } : null;
      rec = h.out;
    }
    return rec;
  };

  /* ---------- rendering a record as HTML ---------- */
  function val(v) {
    if (v === null) return '<span class="pv-null">null</span>';
    if (v === undefined) return '<span class="pv-null">–</span>';
    if (typeof v === 'number') return `<span class="pv-num">${v}</span>`;
    if (typeof v === 'boolean') return `<span class="pv-bool">${v}</span>`;
    if (typeof v === 'object') return `<span class="pv-obj">${esc(JSON.stringify(v))}</span>`;
    return `<span class="pv-str">${esc(String(v).length > 60 ? String(v).slice(0, 58) + '…' : v)}</span>`;
  }
  function kv(o) {
    return `<table class="pv-kv">${Object.keys(o).map(k => `<tr><th>${esc(k)}</th><td>${val(o[k])}</td></tr>`).join('')}</table>`;
  }
  function diff(before, after) {
    const keys = []; [before, after].forEach(o => o && Object.keys(o).forEach(k => { if (!keys.includes(k)) keys.push(k); }));
    return `<table class="pv-diff"><tr><th></th><th>before</th><th>after</th></tr>${keys.map(k => {
      const b = before ? before[k] : undefined, a = after ? after[k] : undefined;
      const ch = !before ? 'add' : !after ? 'del' : JSON.stringify(a) !== JSON.stringify(b) ? 'chg' : '';
      return `<tr class="${ch}"><th>${esc(k)}</th><td>${before ? val(b) : (k === keys[0] ? '<span class="pv-null">null</span>' : '')}</td><td>${after ? val(a) : (k === keys[0] ? '<span class="pv-null">null</span>' : '')}</td></tr>`;
    }).join('')}</table>`;
  }
  function chips(o) { return `<div class="pv-chips">${Object.keys(o).filter(k => o[k] != null && o[k] !== '').map(k => `<span><small>${esc(k)}</small>${esc(o[k])}</span>`).join('')}</div>`; }
  function raw(o) { return `<details class="pv-raw"><summary>Raw JSON</summary><pre>${esc(JSON.stringify(o, null, 2))}</pre></details>`; }
  function envView(env) {
    const op = env.op;
    return `${chips({ op: op + ' (' + ({ c: 'create', u: 'update', d: 'delete', r: 'read' }[op] || op) + ')', table: env.source && env.source.table, lsn: env.source && env.source.lsn, txId: env.source && env.source.txId })}${diff(env.before, env.after)}`;
  }
  function rowsTable(rows) {
    const cols = Object.keys(rows[0]);
    return `<div class="pv-scroll"><table class="pv-rows"><tr>${cols.map(c => `<th>${esc(c)}</th>`).join('')}</tr>${rows.map(r => `<tr class="${r.is_current === false ? 'old' : r.is_current ? 'new' : ''}">${cols.map(c => `<td>${val(r[c])}</td>`).join('')}</tr>`).join('')}</table></div>`;
  }
  R.view = function (rec) {
    if (!rec) return '<div class="note">Nothing crosses here.</div>';
    switch (rec.fmt) {
      case 'dropped': return `<div class="note bad">${esc(rec.why)}. A ${opName(rec.op).toLowerCase()} never reaches this link.</div>`;
      case 'trigger': return `<div class="pv-kind">Trigger message</div>${kv(rec.data)}${raw(rec.data)}`;
      case 'change': return `<div class="pv-kind">${opName(rec.op)} on ${esc(rec.table)}</div>${diff(rec.before, rec.after)}`;
      case 'wal': return `<div class="pv-kind">WAL record</div>${chips({ lsn: rec.lsn, xid: rec.tx, op: opName(rec.op), table: 'public.' + rec.table })}${diff(rec.before, rec.after)}`;
      case 'event': return `<div class="pv-kind">Event</div>${kv(rec.data)}${raw(rec.data)}`;
      case 'debezium': return `<div class="pv-kind">Debezium change event · key ${esc(JSON.stringify({ id: rec.key }))}</div>${envView(rec.env)}${raw({ key: { id: rec.key }, value: rec.env })}`;
      case 'kafka': {
        const k = rec.kafka;
        return `<div class="pv-kind">Kafka record</div>${chips({ topic: k.topic, partition: k.partition, offset: k.offset, key: k.key, timestamp: iso(k.timestamp).slice(11, 19) })}
          ${k.value && k.value.op ? envView(k.value) : kv(k.value || {})}${raw(k)}`;
      }
      case 'row': return `<div class="pv-kind">${rec.sfRaw ? 'Raw table row (VARIANT columns)' : 'Row'}</div>${rec.sfRaw ? `${chips(rec.data.RECORD_METADATA)}${rec.data.RECORD_CONTENT && rec.data.RECORD_CONTENT.op ? envView(rec.data.RECORD_CONTENT) : kv(rec.data.RECORD_CONTENT || {})}` : kv(rec.data || {})}${raw(rec.data)}`;
      case 'agg': return `<div class="pv-kind">Aggregate row</div>${kv(rec.data)}${raw(rec.data)}`;
      case 'file': return `<div class="pv-kind">Object in S3</div>${chips({ format: rec.file.format, rows: rec.file.rows.toLocaleString('en-US'), size: Math.round(rec.file.bytes / 1024) + ' KB' })}<div class="pv-path">${esc(rec.file.key)}</div><div class="pv-sub">one row inside it</div>${kv(rec.data || {})}`;
      case 'commit': return `<div class="pv-kind">Iceberg snapshot commit</div>${kv(Object.assign({ snapshot_id: rec.commit.snapshot_id, operation: rec.commit.operation, sequence_number: rec.commit.sequence_number }, rec.commit.summary))}<div class="pv-path">${esc(rec.commit.manifest_list)}</div><div class="pv-sub">the row, as readers now see it</div>${kv(rec.data || {})}`;
      case 'scd': return `<div class="pv-kind">dim_customer · two rows changed</div>${rowsTable(rec.rows)}`;
    }
    return kv(rec.data || {});
  };
  R.short = function (rec) {
    if (!rec) return '';
    switch (rec.fmt) {
      case 'dropped': return 'lost: ' + opName(rec.op).toLowerCase();
      case 'trigger': return rec.data.run_id || 'trigger';
      case 'kafka': return `p${rec.kafka.partition}@${rec.kafka.offset} key=${rec.kafka.key}`;
      case 'debezium': return `op=${rec.env.op} ${rec.table}#${rec.key}`;
      case 'wal': return `${rec.lsn} ${opName(rec.op)} ${rec.table}`;
      case 'event': return `${rec.data.event} ${rec.data.user_id}`;
      case 'file': return rec.file.key.split('/').pop();
      case 'commit': return `snapshot ${rec.commit.sequence_number} ${rec.commit.operation}`;
      case 'scd': return `SCD2 ${rec.key}`;
      case 'agg': return Object.values(rec.data).slice(2, 4).join(' · ');
      default: { const d = rec.data || {}; const k = Object.keys(d).filter(x => !x.startsWith('_') && x !== 'RECORD_METADATA'); return k.slice(0, 2).map(x => `${x}=${typeof d[x] === 'object' ? '{…}' : d[x]}`).join(' '); }
    }
  };
})(window.PL);
