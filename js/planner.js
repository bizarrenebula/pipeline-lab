/* Pipeline Lab — resource planner for cloud architectures.
 * Turns incoming data volume into sizing for each cloud component (Kafka/MSK, Spark on EMR or Kubernetes,
 * Flink, Snowflake, RDS, Debezium, Airflow, S3/Iceberg), packs Kubernetes pods onto nodes and estimates cost.
 * Every number comes with the reasoning behind it. Users can override any value by hand.
 * Prices are rounded on-demand list prices (us-east-1, 2024–2025) and are meant for comparing designs, not for quotes.
 */
window.PL = window.PL || {};
(function (PL) {
  'use strict';
  const S = PL.S;
  const H = 730; // hours per month
  const ceil = Math.ceil, max = Math.max, min = Math.min;
  const r1 = x => Math.round(x * 10) / 10;
  const up3 = x => ceil(x / 3) * 3;

  /* ---------- price lists ---------- */
  const EC2 = {
    'm6i.xlarge': { cpu: 4, mem: 16, usd: 0.192 }, 'm6i.2xlarge': { cpu: 8, mem: 32, usd: 0.384 }, 'm6i.4xlarge': { cpu: 16, mem: 64, usd: 0.768 },
    'r6i.2xlarge': { cpu: 8, mem: 64, usd: 0.504 }, 'r6i.4xlarge': { cpu: 16, mem: 128, usd: 1.008 }, 'c6i.4xlarge': { cpu: 16, mem: 32, usd: 0.68 }
  };
  const MSK = {
    'kafka.m5.large': { cpu: 2, mem: 8, usd: 0.21, cap: 30 }, 'kafka.m5.xlarge': { cpu: 4, mem: 16, usd: 0.42, cap: 60 },
    'kafka.m5.2xlarge': { cpu: 8, mem: 32, usd: 0.84, cap: 120 }, 'kafka.m5.4xlarge': { cpu: 16, mem: 64, usd: 1.68, cap: 240 }
  };
  const RDS = {
    'db.m6i.large': { cpu: 2, mem: 8, usd: 0.171, wps: 3000 }, 'db.m6i.xlarge': { cpu: 4, mem: 16, usd: 0.342, wps: 6000 },
    'db.m6i.2xlarge': { cpu: 8, mem: 32, usd: 0.684, wps: 12000 }, 'db.m6i.4xlarge': { cpu: 16, mem: 64, usd: 1.368, wps: 25000 }
  };
  const SF = { XS: 1, S: 2, M: 4, L: 8, XL: 16 }; // credits per hour
  const P = { emrUplift: 0.25, eks: 73, ebs: 0.08, s3: 0.023, put: 0.005 / 1000, rdsGb: 0.115, mskGb: 0.10, kpu: 0.11, mcu: 0.11, credit: 3, sfTb: 23, mwaa: 0.49, mwaaWorker: 0.055 };
  PL.PRICES = { EC2, MSK, RDS, SF, P };

  const UNITS = { 'rows/s': 1, 'rows/day': 1 / 86400, 'MB/s': 'mbs', 'GB/day': 'gbd' };
  const DEF_SIZE = { postgres: 300, app: 500, api: 1000, file: 200 };
  const SOURCE_TYPES = ['postgres', 'app', 'api', 'file'];
  const CLOUD = ['kafka', 'spark', 'flink', 'snowflake', 'postgres', 'cdc', 'airflow', 's3', 'iceberg', 'dbt'];
  const TARGETS = {
    kafka: [['msk', 'Amazon MSK'], ['k8s', 'Kubernetes (Strimzi)']],
    spark: [['emr', 'Amazon EMR on EC2'], ['k8s', 'Kubernetes (EMR on EKS, Spark operator)']],
    flink: [['managed', 'Managed Service for Apache Flink'], ['k8s', 'Kubernetes (Flink operator)']],
    postgres: [['rds', 'Amazon RDS'], ['k8s', 'Kubernetes (CloudNativePG)']],
    cdc: [['mskc', 'MSK Connect'], ['k8s', 'Kubernetes (Strimzi Connect)']],
    airflow: [['mwaa', 'Amazon MWAA'], ['k8s', 'Kubernetes (Helm chart)']],
    snowflake: [['saas', 'Snowflake (SaaS)']], s3: [['s3', 'Amazon S3']], iceberg: [['s3', 'Iceberg on S3']], dbt: [['k8s', 'Kubernetes job']]
  };

  function plan() {
    if (!S.plan) S.plan = { workload: { srcs: {}, peak: 3, growth: 30, retention: 7, history: 365, runs: 24, window: 60 }, k8sNode: 'm6i.2xlarge', target: {}, manual: {} };
    return S.plan;
  }
  const isCloud = n => CLOUD.includes(n.type);
  const target = n => plan().target[n.id] || TARGETS[n.type][0][0];

  /* ---------- volume flowing through the design ---------- */
  function srcCfg(n) {
    const w = plan().workload.srcs;
    if (!w[n.id]) w[n.id] = { value: +n.cfg.rate || 100, unit: 'rows/s', size: DEF_SIZE[n.type] || 500 };
    return w[n.id];
  }
  function srcFlow(n) {
    const s = srcCfg(n), u = UNITS[s.unit];
    let rows;
    if (u === 'mbs') rows = s.value * 1e6 / s.size; else if (u === 'gbd') rows = s.value * 1e9 / 86400 / s.size; else rows = s.value * u;
    return { rows, bytes: rows * s.size };
  }
  function flows() {
    const memo = {};
    const out = (n, seen) => {
      if (memo[n.id]) return memo[n.id];
      if (seen.has(n.id)) return { rows: 0, bytes: 0 };
      seen.add(n.id);
      const ups = PL.upstream(n);
      let f;
      if (!ups.length || SOURCE_TYPES.includes(n.type)) f = SOURCE_TYPES.includes(n.type) ? srcFlow(n) : { rows: 0, bytes: 0 };
      else {
        f = ups.reduce((a, u) => { const o = out(u, seen); return { rows: a.rows + o.rows, bytes: a.bytes + o.bytes }; }, { rows: 0, bytes: 0 });
        f.inRows = f.rows; f.inBytes = f.bytes;
        if (n.type === 'cdc') f = { ...f, bytes: f.bytes * 1.3 };
        if (n.type === 'spark' && n.cfg.job === 'agg') f = { ...f, rows: f.rows * 0.05, bytes: f.bytes * 0.05 };
        if (n.type === 'flink' && n.cfg.op === 'window') f = { ...f, rows: f.rows * 0.001, bytes: f.bytes * 0.001 };
      }
      memo[n.id] = f; return f;
    };
    const inflow = {};
    S.nodes.forEach(n => {
      const ups = PL.upstream(n);
      inflow[n.id] = ups.reduce((a, u) => { const o = out(u, new Set()); return { rows: a.rows + o.rows, bytes: a.bytes + o.bytes }; }, { rows: 0, bytes: 0 });
    });
    return inflow;
  }

  /* ---------- per-component sizing ---------- */
  // Each sizer returns { rows: [{k,label,unit,sug,opts?,cap?}], why: [], pods: [], cost: [{label,usd}], apply: {}, head }
  // `v(k)` reads the effective value: the user's manual value, or the suggestion.
  const fmtN = x => x >= 1e6 ? r1(x / 1e6) + 'M' : x >= 1e4 ? Math.round(x / 1e3) + 'k' : x >= 1000 ? r1(x / 1e3) + 'k' : x >= 10 ? Math.round(x) + '' : r1(x) + '';
  const fmtMB = mb => mb >= 1000 ? r1(mb / 1000) + ' GB' : mb >= 1 ? r1(mb) + ' MB' : mb >= 0.001 ? Math.round(mb * 1000) + ' KB' : Math.round(mb * 1e6) + ' B';
  const fmtGB = gb => gb >= 1000 ? r1(gb / 1000) + ' TB' : (gb >= 10 ? Math.round(gb) : r1(gb)) + ' GB';
  const usd = x => '$' + (x >= 100 ? Math.round(x).toLocaleString('en-US') : x.toFixed(2));
  PL.fmtUSD = usd;

  function consumersOf(k) {
    return PL.outLinks(k).filter(l => !PL.isControlLink(l)).map(l => PL.byId(l.to)).map(d =>
      d.type === 'spark' ? d.cfg.executors * d.cfg.cores : d.type === 'flink' ? +d.cfg.par : 1);
  }

  const SZ = {
    kafka(n, f, W, v, tg) {
      const M = W.peak * (1 + W.growth / 100), avgMB = f.bytes / 1e6, peakMB = avgMB * M;
      const cons = consumersOf(n), maxCons = cons.length ? max(...cons) : 1;
      const partSug = up3(max(3, ceil(peakMB / 5), maxCons));
      const tName = Object.keys(MSK).find(t => ceil(peakMB / MSK[t].cap) <= 6) || 'kafka.m5.4xlarge';
      const brokSug = max(3, up3(ceil(peakMB / MSK[tName].cap)));
      const rows = [
        { k: 'partitions', label: 'Partitions per topic', sug: partSug, cap: true },
        { k: 'brokers', label: 'Brokers', sug: brokSug, cap: true },
        { k: 'type', label: tg === 'msk' ? 'Broker instance' : 'Broker pod size (like)', sug: tName, opts: Object.keys(MSK) },
        { k: 'rf', label: 'Replication factor', sug: 3 }
      ];
      const t = MSK[v('type', tName)] || MSK[tName], br = v('brokers', brokSug), rf = v('rf', 3);
      const diskSug = max(10, ceil(avgMB * 86400 * W.retention * rf / 1000 * 1.3 / br));
      rows.push({ k: 'disk', label: 'Disk per broker', unit: 'GB', sug: diskSug, cap: true });
      const disk = v('disk', diskSug);
      const why = [
        `Ingress averages ${fmtMB(avgMB)}/s. Sized for the peak: × ${W.peak} peak factor × ${1 + W.growth / 100} for 12 months of growth = ${fmtMB(peakMB)}/s.`,
        `Partitions: one consumer thread reads one partition, and about 5 MB/s per partition keeps consumers comfortable, so ceil(${r1(peakMB)} / 5) = ${ceil(peakMB / 5)}. Downstream wants up to ${maxCons} parallel readers (Spark cores or Flink parallelism), and partitions cap that parallelism. Take the larger value, at least 3, and round up to a multiple of 3 so partitions spread evenly over 3 brokers: ${partSug}.`,
        `Brokers: a ${tName} takes about ${MSK[tName].cap} MB/s of producer traffic once RF=3 replication is added on top. ${r1(peakMB)} / ${MSK[tName].cap} → ${ceil(peakMB / MSK[tName].cap)}, with a minimum of 3 (one per availability zone, so RF=3 survives losing a zone): ${brokSug}.`,
        `Disk: ${fmtMB(avgMB)}/s × 86,400 s × ${W.retention} days of retention × RF ${rf}, ÷ ${br} brokers, + 30% headroom to keep disks under 70% full = ${diskSug} GB per broker.`,
        'Adding partitions later remaps keys to new partitions and breaks per-key ordering for a while. Removing them is not possible, so size them for the year ahead. Apply never lowers the partition count of the design.'
      ];
      if (diskSug > 3000) why.push(`${diskSug} GB per broker is a lot of local disk. Tiered storage (MSK or Kafka 3.6+) keeps only recent segments on the brokers and the rest in S3, or shorten retention and replay older data from the lake.`);
      let pods = [], cost;
      if (tg === 'msk') cost = [{ label: `${br} × ${v('type', tName)}`, usd: br * t.usd * H }, { label: `${br * disk} GB broker storage`, usd: br * disk * P.mskGb }];
      else {
        pods = [{ name: 'kafka broker', count: br, cpu: t.cpu, mem: t.mem, pvc: disk }, { name: 'kafka controller (KRaft)', count: 3, cpu: 0.5, mem: 2 }];
        cost = [{ label: `${br * disk} GB EBS volumes`, usd: br * disk * P.ebs }];
      }
      return { rows, why, pods, cost, apply: { partitions: min(24, max(+n.cfg.partitions, v('partitions', partSug))), rf: String(min(3, rf)) }, head: `${br} × ${v('type', tName).replace('kafka.', '')} · ${v('partitions', partSug)} partitions` };
    },

    spark(n, f, W, v, tg) {
      const batch = n.cfg.mode === 'batch' || PL.isOrchestrated(n), agg = n.cfg.job === 'agg';
      const G = 1 + W.growth / 100, avgMB = (f.inBytes != null ? f.inBytes : f.bytes) / 1e6;
      const perCore = batch ? (agg ? 12 : 20) : (agg ? 5 : 8); // MB/s one core processes
      let coresSug, runMB = 0, runtimeH = 0;
      const why = [];
      if (batch) {
        runMB = avgMB * 86400 / W.runs * G;
        coresSug = max(2, ceil(runMB / (perCore * W.window * 60 * 0.7)));
        why.push(`Batch: ${W.runs} runs a day, so each run reads ${fmtMB(runMB)} (including ${W.growth}% growth). A core processes about ${perCore} MB/s for a ${agg ? 'shuffle-heavy aggregation' : 'clean and enrich job'} on columnar data. The run must finish in ${W.window} min with 30% left over for retries and skew: ceil(${fmtMB(runMB)} / (${perCore} MB/s × ${W.window * 60} s × 0.7)) = ${coresSug} cores, at least 2.`);
      } else {
        const peakMB = avgMB * W.peak * G;
        coresSug = max(2, ceil(peakMB / perCore));
        why.push(`Streaming: input peaks at ${fmtMB(peakMB)}/s (${fmtMB(avgMB)}/s × ${W.peak} peak × ${G} growth). With JSON parsing and ${agg ? 'stateful aggregation' : 'a clean and enrich step'}, a core handles about ${perCore} MB/s, so ${coresSug} cores keep each micro-batch shorter than its trigger interval.`);
      }
      const ecSug = min(4, coresSug), execSug = ceil(coresSug / ecSug), memPerCore = agg ? 4 : 2;
      const ec = v('ecores', ecSug), ex = v('executors', execSug);
      const emSug = ec * memPerCore, em = v('emem', emSug), ov = max(0.384, r1(em * 0.1));
      const total = ec * ex;
      const shufSug = batch ? max(total * 2, ceil(ceil(runMB * (agg ? 1 : 0.3) / 128) / total) * total) : total * 2;
      const nodeSug = agg ? 'r6i.2xlarge' : 'm6i.2xlarge';
      const rows = [
        { k: 'executors', label: 'Executors', sug: execSug, cap: true },
        { k: 'ecores', label: 'Cores per executor', sug: ecSug },
        { k: 'emem', label: 'Executor memory', unit: 'GB', sug: emSug, cap: true },
        { k: 'shuffle', label: 'spark.sql.shuffle.partitions', sug: shufSug }
      ];
      if (tg === 'emr') rows.push({ k: 'node', label: 'Core node instance', sug: nodeSug, opts: Object.keys(EC2) });
      why.push(`Executors: up to 4 cores each. Fat executors hit GC pauses and HDFS/S3 client contention, and 1-core executors waste memory on per-JVM overhead. ${coresSug} cores → ${execSug} × ${ecSug}.`);
      why.push(`Memory: ${memPerCore} GB per core for ${agg ? 'shuffles and aggregation state' : 'narrow transformations'} → ${emSug} GB heap, plus spark.executor.memoryOverhead of max(384 MB, 10%) = ${ov} GB outside the heap. The container or pod asks for ${r1(em + ov)} GB.`);
      why.push(batch
        ? `Shuffle partitions: aim for about 128 MB per shuffle partition, and at least 2 tasks per core so stragglers do not idle the cluster: ${shufSug}. Adaptive Query Execution (on by default in Spark 3.2+) coalesces small ones.`
        : `Shuffle partitions: the default of 200 makes 200 tiny tasks per micro-batch. Use about 2 per core: ${shufSug}. For a stateful stream, set this before the first run: the state store is partitioned by it and it cannot change while the checkpoint exists.`);
      let pods = [], cost = [];
      const dutyH = batch ? W.runs * (runMB / (total * perCore * 3600) + 0.15) : 24; // +9 min start-up per run
      runtimeH = batch ? runMB / (total * perCore * 3600) : 0;
      if (batch) why.push(`Each run takes about ${r1(runtimeH * 60)} min of compute plus about 9 min of cluster or pod start-up, for ${r1(dutyH)} h a day. Pay only for that with a transient EMR cluster, or with pods that disappear after the job.`);
      if (tg === 'emr') {
        const nt = EC2[v('node', nodeSug)] || EC2[nodeSug];
        const perNode = max(1, min(Math.floor(nt.cpu / ec), Math.floor(nt.mem * 0.75 / (em + ov))));
        const nodes = ceil(ex / perNode);
        why.push(`EMR packing: YARN offers about 75% of a ${v('node', nodeSug)}'s memory to containers, which fits ${perNode} executors per node, so ${nodes} core nodes plus 1 primary node (m6i.xlarge). The EMR fee adds about 25% on top of EC2.`);
        cost.push({ label: `${nodes} × ${v('node', nodeSug)} core nodes, ${batch ? r1(dutyH) + ' h/day' : 'always on'}`, usd: nodes * nt.usd * (1 + P.emrUplift) * dutyH * 30.4 });
        cost.push({ label: '1 × m6i.xlarge primary node', usd: EC2['m6i.xlarge'].usd * (1 + P.emrUplift) * dutyH * 30.4 });
      } else {
        pods = [{ name: 'spark driver', count: 1, cpu: batch ? 1 : 2, mem: 4, duty: dutyH / 24 }, { name: 'spark executor', count: ex, cpu: ec, mem: r1(em + ov), duty: dutyH / 24 }];
        why.push('On Kubernetes, executors are pods: requests.cpu = cores per executor, and the memory request = limit = heap + overhead, so the kernel never OOM-kills a JVM that stays within its settings. Dynamic allocation with shuffle tracking adds and removes executor pods as the backlog changes.');
      }
      return { rows, why, pods, cost, apply: { executors: min(16, ex), cores: min(8, ec) }, head: `${ex} × ${ec} cores · ${em} GB${batch ? ' · batch' : ''}` };
    },

    flink(n, f, W, v, tg) {
      const win = n.cfg.op === 'window', M = W.peak * (1 + W.growth / 100);
      const inRows = f.inRows != null ? f.inRows : f.rows, peakRows = inRows * M;
      const perSlot = (win ? 15000 : 40000) * (n.cfg.eos ? 0.9 : 1);
      const parSug = max(1, ceil(peakRows / perSlot));
      const k = PL.upstream(n).find(u => u.type === 'kafka');
      const rows = [
        { k: 'par', label: 'Parallelism', sug: parSug, cap: true },
        { k: 'slots', label: 'Slots per TaskManager', sug: 2 },
        { k: 'tmmem', label: 'TaskManager process memory', unit: 'GB', sug: win ? 8 : 4, cap: true }
      ];
      const par = v('par', parSug), slots = v('slots', 2), tms = ceil(par / slots), tmmem = v('tmmem', win ? 8 : 4);
      const why = [
        `Input peaks at ${fmtN(peakRows)} rows/s (${fmtN(inRows)} average × ${r1(M)}). One slot handles about ${fmtN(perSlot)} rows/s for a ${win ? 'keyed window' : 'stateless map'}${n.cfg.eos ? ', minus 10% for exactly-once checkpoint barriers' : ''}, so parallelism = ${parSug}.`,
        k ? (k.cfg.partitions < par ? `Kafka has ${k.cfg.partitions} partitions but parallelism is ${par}: ${par - k.cfg.partitions} source subtasks would sit idle. Raise partitions to at least ${par}.` : `Kafka has ${k.cfg.partitions} partitions, enough for ${par} source subtasks.`) : 'No Kafka upstream: parallelism is limited by the source.',
        `${par} slots ÷ ${slots} per TaskManager = ${tms} TaskManagers with ${tmmem} GB each. ${win ? 'Windows keep state, so give managed memory (RocksDB) a larger share. State size is about the keys × window length × aggregate size, and checkpoints copy it to S3 every interval.' : 'Stateless operators need little managed memory, so most of it can go to network buffers.'}`,
        `Exactly-once sinks publish results only when a checkpoint completes, so freshness ≈ checkpoint interval (${n.cfg.ckpt} s in the design).`
      ];
      let pods = [], cost = [];
      if (tg === 'managed') {
        const kpu = ceil(par / 1) + 1;
        why.push(`Managed Flink bills KPUs (1 vCPU, 4 GB each). Parallelism per KPU = 1 → ${par} KPUs, + 1 KPU for the application's orchestration = ${kpu}.`);
        cost.push({ label: `${kpu} KPU always on`, usd: kpu * P.kpu * H }, { label: `${kpu * 50} GB running storage`, usd: kpu * 50 * 0.1 });
      } else {
        pods = [{ name: 'flink jobmanager', count: 1, cpu: 1, mem: 2 }, { name: 'flink taskmanager', count: tms, cpu: slots, mem: tmmem }];
        why.push('With the Flink Kubernetes Operator, a FlinkDeployment sets jobManager and taskManager resources. The operator autoscaler can change parallelism from the observed backlog and busy time.');
      }
      return { rows, why, pods, cost, apply: { par: min(16, par) }, head: `parallelism ${par} · ${tms} TM` };
    },

    snowflake(n, f, W, v) {
      const dbt = PL.outLinks(n).map(l => PL.byId(l.to)).find(d => d.type === 'dbt');
      const G = 1 + W.growth / 100, dailyGB = f.bytes * 86400 / 1e9 * G, histGB = dailyGB * W.history / 4;
      const incGB = dailyGB / W.runs / 4;
      const mat = dbt ? dbt.cfg.mat : 'incremental', models = dbt ? +dbt.cfg.models : 6;
      const scanGB = (mat === 'table' ? histGB : incGB * 3 * max(1, models / 6)) + (dbt && dbt.cfg.scd === 'scd2' ? incGB * 2 : 0);
      const gbPerMin = s => 4 * SF[s]; // compressed GB scanned and rewritten per minute
      const targetMin = min(W.window, 1440 / W.runs); // a run must end before the window closes and before the next run
      const sizeSug = Object.keys(SF).find(s => scanGB / gbPerMin(s) <= targetMin) || 'XL';
      const rows = [{ k: 'size', label: 'Warehouse size', sug: sizeSug, opts: Object.keys(SF) }, { k: 'suspend', label: 'Auto-suspend', unit: 's', sug: 60 }];
      const size = v('size', sizeSug), susp = v('suspend', 60), cr = SF[size];
      const runMin = scanGB / gbPerMin(size);
      const billedMin = max(1, runMin) + susp / 60;
      const crDay = n.cfg.suspend ? W.runs * billedMin / 60 * cr : 24 * cr;
      const ingestCr = n.cfg.ingest === 'copy' ? dailyGB / 40 : dailyGB * 0.02;
      const why = [
        `${fmtGB(dailyGB)} lands per day (with growth), about ${fmtGB(histGB)} compressed after ${W.history} days (Snowflake compresses about 4×).`,
        `Each of the ${W.runs} daily transform runs ${mat === 'table' ? `rebuilds the marts as tables, rescanning about ${fmtGB(scanGB)} of history every time` : `processes about ${fmtGB(scanGB)} of new data (incremental models${dbt && dbt.cfg.scd === 'scd2' ? ', plus the SCD2 snapshot comparing changed rows' : ''})`}.`,
        `An XS warehouse scans and rewrites about 4 GB of compressed data per minute, and every size up doubles both speed and credits per hour. The smallest size that finishes a run within ${Math.round(targetMin)} min (the batch window, or the gap between runs) is ${sizeSug}. A run on ${size} takes about ${r1(runMin)} min.`,
        `Billing is per second with a 60 s minimum on every resume, and the warehouse stays up for the auto-suspend time (${susp} s) after each run. ${n.cfg.suspend ? `${W.runs} runs × ${r1(billedMin)} min × ${cr} credits/h = ${r1(crDay)} credits/day.` : 'Auto-suspend is off in the design, so it bills 24 h a day.'}`,
        `Loading (${n.cfg.ingest === 'copy' ? 'COPY on the warehouse' : 'Snowpipe'}): about ${r1(ingestCr)} credits/day. For many concurrent BI users, scale out (a multi-cluster warehouse) instead of up.`
      ];
      if (mat === 'table') why.push('Switching the marts to incremental cuts the scan from the full history to the new rows only. It is the biggest saving here.');
      const cost = [{ label: `${size} warehouse, ${r1(crDay)} credits/day`, usd: crDay * 30.4 * P.credit }, { label: 'Loading credits', usd: ingestCr * 30.4 * P.credit }, { label: `${fmtGB(histGB)} storage`, usd: histGB / 1000 * P.sfTb }];
      return { rows, why, pods: [], cost, apply: { size, suspend: true }, head: `${size} · ${r1(crDay)} credits/day` };
    },

    postgres(n, f, W, v, tg) {
      const src = srcFlow(n), M = W.peak * (1 + W.growth / 100), peakW = src.rows * M;
      const tSug = Object.keys(RDS).find(t => RDS[t].wps >= peakW) || 'db.m6i.4xlarge';
      const insFrac = { ins: 0.9, mix: 0.5, upd: 0.1 }[n.cfg.mix] || 0.5;
      const cdc = PL.outLinks(n).some(l => PL.byId(l.to).type === 'cdc');
      const walGB = cdc ? src.bytes * 3600 * 6 / 1e9 : 0;
      const keep = min(W.history, 90); // OLTP databases archive old rows; the lake keeps the long history
      const stSug = max(20, ceil(src.rows * insFrac * srcCfg(n).size * 86400 * keep * 2 * 1.3 / 1e9 + walGB));
      const rows = [{ k: 'type', label: tg === 'rds' ? 'Instance' : 'Pod size (like)', sug: tSug, opts: Object.keys(RDS) }, { k: 'storage', label: 'Storage', unit: 'GB', sug: stSug, cap: true }];
      const t = RDS[v('type', tSug)] || RDS[tSug], st = v('storage', stSug);
      const why = [
        `This is the source system: its size is set by the application, but CDC and extracts add load. Peak writes: ${fmtN(peakW)}/s.`,
        `A ${tSug} handles about ${fmtN(RDS[tSug].wps)} small writes/s with gp3 storage.`,
        `Storage: ${Math.round(insFrac * 100)}% of writes are inserts × ${srcCfg(n).size} B × ${keep} days kept in the OLTP database (older rows are archived to the lake) × 2 for indexes, + 30% headroom${cdc ? `, + ${fmtGB(walGB)} of WAL so the replication slot survives a 6 h Debezium outage (an inactive slot keeps WAL until disk fills)` : ''} = ${stSug} GB.`
      ];
      if (peakW > RDS['db.m6i.4xlarge'].wps || stSug > 64000) why.push(`That is past one Postgres instance${stSug > 64000 ? ' (RDS storage stops at 64 TB)' : ''}. Shard by tenant or key (Citus, Aurora Limitless), batch the writes, or archive sooner. Debezium then reads each shard separately.`);
      let pods = [], cost;
      if (tg === 'rds') cost = [{ label: v('type', tSug), usd: t.usd * H }, { label: `${st} GB gp3`, usd: st * P.rdsGb }];
      else { pods = [{ name: 'postgres', count: 1, cpu: t.cpu, mem: t.mem, pvc: st }]; cost = [{ label: `${st} GB EBS`, usd: st * P.ebs }]; }
      return { rows, why, pods, cost, apply: {}, head: `${v('type', tSug).replace('db.', '')} · ${st} GB` };
    },

    cdc(n, f, W, v, tg) {
      const M = W.peak * (1 + W.growth / 100), peak = (f.inRows != null ? f.inRows : f.rows) * M;
      const rows = [{ k: 'workers', label: 'Connect workers', sug: 2 }, { k: 'wmem', label: 'Worker memory', unit: 'GB', sug: 4 }];
      const w = v('workers', 2), wm = v('wmem', 4);
      const why = [
        `Peak change rate: ${fmtN(peak)} rows/s. The Postgres connector reads one replication slot with a single task, good for about 8–10k rows/s.`,
        peak > 8000 ? 'That is above what one task handles: split the tables across several connectors, each with its own slot and publication.' : 'One task is enough at this volume.',
        `${w} Connect workers so the task fails over when a worker dies. ${wm} GB each covers the JVM and the producer buffers.`,
        n.cfg.mode === 'poll' ? 'Query polling puts a full scan load on the source every interval and misses deletes. Prefer log-based capture.' : 'Log-based capture adds almost no query load, but the replication slot holds WAL whenever the connector is down.'
      ];
      let pods = [], cost;
      if (tg === 'mskc') cost = [{ label: `${w} MCU (${w} vCPU, ${w * 4} GB)`, usd: w * P.mcu * H }];
      else { pods = [{ name: 'kafka connect', count: w, cpu: 1, mem: wm }]; cost = []; }
      return { rows, why, pods, cost, apply: {}, head: `${w} workers · 1 task` };
    },

    airflow(n, f, W, v, tg) {
      const tasks = PL.outLinks(n).length, perDay = tasks * W.runs;
      const rows = [{ k: 'workers', label: tg === 'mwaa' ? 'Extra workers' : 'Worker slots (pods at peak)', sug: tg === 'mwaa' ? 0 : max(2, tasks) }];
      const w = v('workers', tg === 'mwaa' ? 0 : max(2, tasks));
      const why = [
        `${tasks} tasks per DAG run × ${W.runs} runs a day = ${perDay} task runs a day. Airflow only triggers the work, and the heavy lifting runs in Spark and the warehouse, so it stays small.`,
        tg === 'mwaa' ? 'An MWAA small environment (up to about 50 DAGs) covers this, and extra workers are only needed for many parallel tasks.' : 'On Kubernetes, the KubernetesExecutor starts one pod per task (0.5 vCPU, 1 GB) and removes it afterwards. The scheduler, webserver and triggerer are always on.'
      ];
      let pods = [], cost;
      if (tg === 'mwaa') cost = [{ label: 'MWAA small environment', usd: P.mwaa * H }, ...(w ? [{ label: `${w} extra workers`, usd: w * P.mwaaWorker * H }] : [])];
      else { pods = [{ name: 'airflow scheduler', count: 1, cpu: 1, mem: 2 }, { name: 'airflow webserver', count: 1, cpu: 1, mem: 2 }, { name: 'airflow triggerer', count: 1, cpu: 0.5, mem: 1 }, { name: 'airflow task pod', count: w, cpu: 0.5, mem: 1, duty: min(1, perDay * 3 / 1440) }]; cost = []; }
      return { rows, why, pods, cost, apply: {}, head: tg === 'mwaa' ? 'MWAA small' : `${3 + w} pods` };
    },

    s3(n, f, W) {
      const comp = { parquet: 4, csv: 1.5, json: 1 }[n.cfg.format] || 1;
      const dailyGB = f.bytes * 86400 / 1e9 * (1 + W.growth / 100) / comp, totGB = dailyGB * W.history;
      const why = [`${fmtGB(dailyGB)} a day after ${n.cfg.format} compression (about ${comp}×), ${fmtGB(totGB)} after ${W.history} days. Lifecycle rules can move old data to cheaper tiers.`];
      if (n.cfg.format !== 'parquet') why.push('Parquet is columnar and compressed: about 4× smaller than JSON and much faster to query.');
      return { rows: [], why, pods: [], cost: [{ label: `${fmtGB(totGB)} S3 Standard`, usd: totGB * P.s3 }], apply: {}, head: fmtGB(totGB) + ' stored' };
    },

    iceberg(n, f, W) {
      const up = PL.upstream(n)[0];
      const writers = up ? (up.type === 'spark' ? up.cfg.executors * up.cfg.cores : up.type === 'flink' ? +up.cfg.par : 1) : 1;
      const commitS = up ? (up.type === 'spark' ? (up.cfg.mode === 'batch' ? 86400 / W.runs : +up.cfg.trigger) : up.type === 'flink' ? +up.cfg.ckpt : +n.cfg.commit) : +n.cfg.commit;
      const dailyGB = f.bytes * 86400 / 1e9 * (1 + W.growth / 100) / 4, totGB = dailyGB * W.history;
      const files = writers * 86400 / commitS, fileMB = dailyGB * 1000 / files;
      const goodCommit = ceil(writers * 128 / max(dailyGB * 1000 / 86400, 1e-6));
      const why = [
        `${fmtGB(dailyGB)} of Parquet a day, ${fmtGB(totGB)} after ${W.history} days.`,
        `${writers} writers commit every ${r1(commitS)} s, which makes about ${fmtN(files)} data files a day averaging ${fmtMB(fileMB)}.`,
        fileMB < 64 ? `That is far below the 128–512 MB that readers like, so every query opens thousands of small files. Keep compaction on (rewrite_data_files) and, if freshness allows, commit less often: about every ${fmtN(goodCommit)} s for 128 MB files. Also expire old snapshots and delete orphan files.` : 'File sizes are healthy.'
      ];
      return { rows: [], why, pods: [], cost: [{ label: `${fmtGB(totGB)} on S3`, usd: totGB * P.s3 }, { label: `${fmtN(files * 30)} PUT requests`, usd: files * 30.4 * P.put }], apply: fileMB < 64 ? { compaction: true } : {}, head: `${fmtMB(fileMB)} avg file` };
    },

    dbt(n, f, W) {
      return { rows: [], why: ['dbt only sends SQL: the work and its cost land on the warehouse (see Snowflake). The runner is a small pod that lives for the length of a run.'], pods: [{ name: 'dbt runner', count: 1, cpu: 0.5, mem: 1, duty: min(1, W.runs * 5 / 1440) }], cost: [], apply: {}, head: 'runs on the warehouse' };
    }
  };

  /* ---------- Kubernetes packing ---------- */
  function packK8s(pods, nodeType) {
    const nt = EC2[nodeType], cpuA = nt.cpu * 0.94 - 0.2, memA = nt.mem * 0.88 - 0.6; // kubelet/system reserve + daemonsets
    const items = [];
    pods.forEach(p => { for (let i = 0; i < p.count; i++) items.push(p); });
    const tooBig = items.filter(p => p.cpu > cpuA || p.mem > memA);
    const pack = list => {
      const bins = [];
      list.filter(p => !tooBig.includes(p)).sort((a, b) => b.cpu * 4 + b.mem - (a.cpu * 4 + a.mem)).forEach(p => {
        let b = bins.find(b => b.cpu + p.cpu <= cpuA && b.mem + p.mem <= memA);
        if (!b) { b = { cpu: 0, mem: 0 }; bins.push(b); }
        b.cpu += p.cpu; b.mem += p.mem;
      });
      return bins;
    };
    const steady = pack(items.filter(p => !(p.duty < 1))), peak = pack(items);
    const reqCpu = items.reduce((a, p) => a + p.cpu, 0), reqMem = items.reduce((a, p) => a + p.mem, 0);
    const burstDuty = items.filter(p => p.duty < 1).reduce((a, p) => max(a, p.duty), 0);
    const minN = max(steady.length, 1), maxN = max(peak.length, minN);
    const cost = (minN + (maxN - minN) * burstDuty) * nt.usd * H + P.eks;
    return { nt, cpuA: r1(cpuA), memA: r1(memA), minN, maxN, reqCpu: r1(reqCpu), reqMem: r1(reqMem), util: r1(reqCpu / (maxN * cpuA) * 100), tooBig, cost, burstDuty };
  }

  /* ---------- whole plan ---------- */
  function compute() {
    const p = plan(), W = p.workload, inflow = flows();
    const comps = [];
    S.nodes.filter(isCloud).forEach(n => {
      const tg = target(n), man = p.manual[n.id] || {};
      const v = (k, s) => { const m = man[k]; if (m === undefined || m === '' || m === null) return s; return typeof s === 'number' ? +m : m; };
      const r = SZ[n.type](n, inflow[n.id], W, v, tg);
      r.rows.forEach(row => {
        row.eff = v(row.k, row.sug); row.manual = man[row.k] !== undefined && man[row.k] !== '';
        row.state = row.cap && typeof row.sug === 'number' ? (row.eff < row.sug * 0.8 ? 'under' : row.eff > row.sug * 2.5 && row.sug > 1 ? 'over' : '') : '';
      });
      r.monthly = r.cost.reduce((a, c) => a + c.usd, 0);
      comps.push({ n, tg, ...r, flow: inflow[n.id] });
    });
    const pods = [];
    comps.forEach(c => c.pods.forEach(pd => pods.push({ ...pd, comp: c.n.name })));
    const k8s = pods.length ? packK8s(pods, p.k8sNode) : null;
    const total = comps.reduce((a, c) => a + c.monthly, 0) + (k8s ? k8s.cost : 0);
    return { comps, pods, k8s, total, W };
  }

  /* differences between the plan and the design's knobs (used by hints and Apply) */
  function diffs() {
    if (!S.plan) return [];
    const out = [];
    compute().comps.forEach(c => Object.entries(c.apply).forEach(([k, val]) => { if (String(c.n.cfg[k]) !== String(val)) out.push({ n: c.n, k, from: c.n.cfg[k], to: val }); }));
    return out;
  }
  function apply() {
    const d = diffs();
    d.forEach(x => { x.n.cfg[x.k] = x.to; });
    PL.ui.syncNodes(); PL.ui.refreshInspector && PL.ui.refreshInspector(); PL.ui.saveSoon();
    return d.length;
  }

  /* ---------- export: sizing doc + Kubernetes manifests ---------- */
  function exportFiles(yaml, svcName) {
    if (!S.nodes.some(isCloud)) return {};
    const R = compute(), F = {}, W = R.W, p = plan();
    const srcLines = S.nodes.filter(n => SOURCE_TYPES.includes(n.type)).map(n => { const s = srcCfg(n); return `- ${n.name}: ${s.value} ${s.unit}, ${s.size} B per record`; });
    let md = `# Resource plan\n\nGenerated by Pipeline Lab. A starting point for running this design in the cloud: the local docker-compose bundle is a functional replica and does not use these sizes.\nPrices are rounded on-demand list prices (us-east-1). Use them to compare options, not as a quote.\n\n## Workload\n\n${srcLines.join('\n') || '- (no sources)'}\n- Peak factor ${W.peak}×, growth ${W.growth}% over 12 months\n- Kafka retention ${W.retention} days, history kept ${W.history} days\n- ${W.runs} scheduled runs a day, batch window ${W.window} min\n\n## Estimated monthly cost: ${usd(R.total)}\n\n`;
    R.comps.forEach(c => {
      md += `## ${c.n.name} (${PL.TYPES[c.n.type].label}, ${TARGETS[c.n.type].find(t => t[0] === c.tg)[1]})\n\n`;
      if (c.rows.length) md += '| Setting | Value | Suggested |\n| --- | --- | --- |\n' + c.rows.map(r => `| ${r.label} | ${r.eff}${r.unit ? ' ' + r.unit : ''}${r.manual ? ' (manual)' : ''} | ${r.sug}${r.unit ? ' ' + r.unit : ''} |`).join('\n') + '\n\n';
      md += c.why.map(w => '- ' + w).join('\n') + '\n\n';
      if (c.cost.length) md += 'Cost: ' + c.cost.map(x => `${x.label} ${usd(x.usd)}`).join(', ') + '\n\n';
    });
    if (R.k8s) {
      const k = R.k8s;
      md += `## Kubernetes cluster (EKS)\n\n- Node type ${p.k8sNode}: ${k.cpuA} vCPU and ${k.memA} GB allocatable after kubelet, system reserve and daemonsets\n- Pods request ${k.reqCpu} vCPU and ${k.reqMem} GB in total at peak\n- Cluster autoscaler or Karpenter: min ${k.minN}, max ${k.maxN} nodes\n- About ${usd(k.cost)} a month including the EKS control plane\n\n| Pod | Count | CPU request | Memory request = limit |\n| --- | --- | --- | --- |\n` + R.pods.map(pd => `| ${pd.comp}: ${pd.name} | ${pd.count} | ${pd.cpu} | ${pd.mem} Gi |`).join('\n') + '\n\nCPU limits are left unset so pods can burst into idle CPU instead of being throttled. Memory limit = request, because a JVM over its limit is OOM-killed.\n';
      if (k.tooBig.length) md += `\nThese pods do not fit a ${p.k8sNode}: ${[...new Set(k.tooBig.map(x => x.comp + ': ' + x.name))].join(', ')}. Choose a larger node type.\n`;
      R.comps.filter(c => c.tg === 'k8s').forEach(c => {
        const name = svcName(c.n), ns = 'data';
        const res = (cpu, mem) => ({ requests: { cpu: String(cpu), memory: mem + 'Gi' }, limits: { memory: mem + 'Gi' } });
        let doc = null;
        if (c.n.type === 'spark') {
          const ex = c.rows.find(r => r.k === 'executors').eff, ec = c.rows.find(r => r.k === 'ecores').eff, em = c.rows.find(r => r.k === 'emem').eff;
          doc = { apiVersion: 'sparkoperator.k8s.io/v1beta2', kind: 'SparkApplication', metadata: { name, namespace: ns }, spec: { type: 'Python', mode: 'cluster', image: 'apache/spark:3.5.3-python3', mainApplicationFile: 's3a://<your-bucket>/jobs/' + name + '.py', sparkVersion: '3.5.3', sparkConf: { 'spark.sql.shuffle.partitions': String(c.rows.find(r => r.k === 'shuffle').eff), 'spark.dynamicAllocation.enabled': 'true', 'spark.dynamicAllocation.shuffleTracking.enabled': 'true' }, driver: { cores: 1, memory: '4g', serviceAccount: 'spark' }, executor: { instances: ex, cores: ec, memory: em + 'g', memoryOverhead: Math.round(max(0.384, em * 0.1) * 1024) + 'm' }, dynamicAllocation: { enabled: true, minExecutors: max(1, Math.floor(ex / W.peak)), maxExecutors: ex } } };
        } else if (c.n.type === 'flink') {
          const par = c.rows.find(r => r.k === 'par').eff, slots = c.rows.find(r => r.k === 'slots').eff, tm = c.rows.find(r => r.k === 'tmmem').eff;
          doc = { apiVersion: 'flink.apache.org/v1beta1', kind: 'FlinkDeployment', metadata: { name, namespace: ns }, spec: { image: 'flink:1.19.1-scala_2.12-java17', flinkVersion: 'v1_19', flinkConfiguration: { 'taskmanager.numberOfTaskSlots': String(slots), 'execution.checkpointing.interval': c.n.cfg.ckpt + 's', 'state.backend.type': 'rocksdb', 'job.autoscaler.enabled': 'true' }, serviceAccount: 'flink', jobManager: { resource: { cpu: 1, memory: '2g' } }, taskManager: { resource: { cpu: slots, memory: tm + 'g' } }, job: { jarURI: 'local:///opt/flink/usrlib/' + name + '.jar', parallelism: par, upgradeMode: 'last-state' } } };
        } else if (c.n.type === 'kafka') {
          const br = c.rows.find(r => r.k === 'brokers').eff, disk = c.rows.find(r => r.k === 'disk').eff, t = MSK[c.rows.find(r => r.k === 'type').eff] || MSK['kafka.m5.large'];
          doc = { apiVersion: 'kafka.strimzi.io/v1beta2', kind: 'KafkaNodePool', metadata: { name: name + '-brokers', namespace: ns, labels: { 'strimzi.io/cluster': name } }, spec: { replicas: br, roles: ['broker'], storage: { type: 'persistent-claim', size: disk + 'Gi', class: 'gp3' }, resources: res(t.cpu, t.mem) } };
        } else if (c.pods.length) {
          doc = { kind: 'ResourceHints', note: 'requests/limits to put on the Helm chart or operator for ' + c.n.name, pods: c.pods.map(pd => ({ name: pd.name, replicas: pd.count, resources: res(pd.cpu, pd.mem) })) };
        }
        if (doc) F[`cloud/k8s/${name}.yaml`] = `# ${c.n.name}: starting manifest from the Pipeline Lab resource plan. Fill in images, paths and secrets.\n` + yaml(doc) + '\n';
      });
    }
    F['cloud/PLAN.md'] = md;
    F['cloud/plan.json'] = JSON.stringify({ workload: W, total_monthly_usd: Math.round(R.total), components: R.comps.map(c => ({ name: c.n.name, type: c.n.type, target: c.tg, monthly_usd: Math.round(c.monthly), settings: Object.fromEntries(c.rows.map(r => [r.k, r.eff])) })), kubernetes: R.k8s && { node_type: p.k8sNode, min_nodes: R.k8s.minN, max_nodes: R.k8s.maxN } }, null, 2);
    return F;
  }

  PL.planner = { plan, compute, diffs, apply, exportFiles, isCloud, TARGETS, UNITS, SOURCE_TYPES, srcCfg, hasCloud: () => S.nodes.some(isCloud) };
})(window.PL);
