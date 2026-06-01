// AI-MONITOR · data layer
// Two interchangeable data sources behind one façade:
//
//   window.AIData = { NODES, MODELS, live, totals, subscribe,
//                     setIntervalMs, pause, resume, mode, switchMode }
//
//   mode  · "mock" — local simulator (default; great for demos & dev)
//         · "live" — subscribes to /api/stream (SSE) on the FastAPI backend
//
// Selection (in order):
//   1. ?mode=live or ?mode=mock in URL
//   2. localStorage("aim.mode")
//   3. auto-probe /api/health — if reachable, "live"; else "mock"

(() => {
  // ── Static topology (matches the backend's NODES list) ──────────────
  const NODES = [
    { id: "node-1",  name: "Workstation", ip: "10.0.0.1",      role: "Gateway · Edge",   class: "GPU host",
      os: "Ubuntu 24.04 LTS", kernel: "6.8.0-49", driver: "NVIDIA 560.35", cuda: "12.6",
      gpu: { name: "GeForce RTX 4090",     mem: 24,  fp16: 165.2, fp4: 660.0 },
      cpu: { model: "Ryzen 9 7950X",       cores: 16, threads: 32 },
      ram: 128, disk: 4096, net: "10 GbE",
      services: ["litellm","vllm","ollama","comfyui","prometheus"] },
    { id: "node-2", name: "Inference-1", ip: "10.0.0.2",     role: "Inference",        class: "GPU node",
      os: "Ubuntu 24.04 LTS (DGX OS)", kernel: "6.8.0-49", driver: "NVIDIA 560.40", cuda: "12.6",
      gpu: { name: "GB10 Grace-Blackwell", mem: 128, fp16: 250.0, fp4: 1000.0 },
      cpu: { model: "Grace 20-core ARM",   cores: 20, threads: 20 },
      ram: 128, disk: 4096, net: "200 GbE ConnectX-7",
      services: ["vllm","node-exporter","dcgm"] },
    { id: "node-3", name: "Inference-2", ip: "10.0.0.3",     role: "Inference",        class: "GPU node",
      os: "Ubuntu 24.04 LTS (DGX OS)", kernel: "6.8.0-49", driver: "NVIDIA 560.40", cuda: "12.6",
      gpu: { name: "GB10 Grace-Blackwell", mem: 128, fp16: 250.0, fp4: 1000.0 },
      cpu: { model: "Grace 20-core ARM",   cores: 20, threads: 20 },
      ram: 128, disk: 4096, net: "200 GbE ConnectX-7",
      services: ["vllm","sglang","node-exporter","dcgm"] },
    { id: "node-4", name: "Inference-3", ip: "10.0.0.4",     role: "Inference",        class: "GPU node",
      os: "Ubuntu 24.04 LTS (DGX OS)", kernel: "6.8.0-49", driver: "NVIDIA 560.40", cuda: "12.6",
      gpu: { name: "GB10 Grace-Blackwell", mem: 128, fp16: 250.0, fp4: 1000.0 },
      cpu: { model: "Grace 20-core ARM",   cores: 20, threads: 20 },
      ram: 128, disk: 4096, net: "200 GbE ConnectX-7",
      services: ["vllm","node-exporter","dcgm"] },
    { id: "node-5", name: "Inference-4", ip: "10.0.0.5",     role: "Inference",        class: "GPU node",
      os: "Ubuntu 24.04 LTS (DGX OS)", kernel: "6.8.0-49", driver: "NVIDIA 560.40", cuda: "12.6",
      gpu: { name: "GB10 Grace-Blackwell", mem: 128, fp16: 250.0, fp4: 1000.0 },
      cpu: { model: "Grace 20-core ARM",   cores: 20, threads: 20 },
      ram: 128, disk: 4096, net: "200 GbE ConnectX-7",
      services: ["vllm","node-exporter","dcgm"] },
  ];

  // 真实模型（镜像后端 MODEL_CATALOG / litellm /v1/models · comfyui mode）。
  // metricsSource: "vllm" 有真实 vLLM 指标；"none" = llama.cpp/停用，诚实无实时指标。
  const MODELS_CATALOG = [
    { id: "qwen3-coder", display: "Qwen3-Coder-Next", vendor: "Alibaba", kind: "chat",
      params: "Coder-Next", quant: "FP8", ctx: 262144, framework: "vLLM", port: 8888,
      route: "litellm/qwen3-coder-next", nodes: ["node-4","node-5"], vram: 0,
      metricsSource: "vllm", state: "idle", tps: 0, ttft: 0, tpot: 0, rps: 0, kv: 0,
      p50: 0, p95: 0, p99: 0, err: 0, tags: ["coding","2xSpark","resident"] },
    { id: "deepseek-v4-flash", display: "DeepSeek-V4-Flash", vendor: "DeepSeek", kind: "chat",
      params: "—", quant: "—", ctx: 262144, framework: "vLLM", port: 8000,
      route: "litellm/deepseek-v4-flash", nodes: ["node-3"], vram: 0,
      metricsSource: "vllm", state: "no-metrics", tps: 0, ttft: 0, tpot: 0, rps: 0, kv: 0,
      p50: 0, p95: 0, p99: 0, err: 0, tags: ["reasoning","stopped"] },
    { id: "gemma-4-31b-abliterated", display: "Gemma-4-31B-abliterated", vendor: "Google", kind: "vision",
      params: "31B", quant: "Q8 GGUF", ctx: 32768, framework: "llama.cpp", port: 8001,
      route: "litellm/gemma-4-31b-abliterated", nodes: ["node-3"], vram: 0,
      metricsSource: "none", state: "no-metrics", tps: 0, ttft: 0, tpot: 0, rps: 0, kv: 0,
      p50: 0, p95: 0, p99: 0, err: 0, tags: ["vision","abliterated"] },
    { id: "qwen3-vl-abliterated", display: "Qwen3-VL-8B-abliterated", vendor: "Alibaba", kind: "vision",
      params: "8B", quant: "Q8 GGUF", ctx: 32768, framework: "llama.cpp", port: 8002,
      route: "litellm/qwen3-vl-abliterated", nodes: ["node-3"], vram: 0,
      metricsSource: "none", state: "no-metrics", tps: 0, ttft: 0, tpot: 0, rps: 0, kv: 0,
      p50: 0, p95: 0, p99: 0, err: 0, tags: ["vision","scoring"] },
  ];

  const HIST  = 60;   // history points retained on the cluster pulse cards
  const SPARK = 32;

  // ── Shared live state · single instance, mutated in place ───────────
  // Consumers (Nav, Hero, sections) destructure `window.AIData.live` once,
  // so the reference must be stable. We never reassign `live`; instead we
  // wipe and re-populate its fields when switching data sources.
  const live = {
    nodes: {}, models: {}, nodeMeta: {},
    cluster: {
      tps: [], rps: [], kv: [], pow: [], temp: [],
      tpsNow: 0, rpsNow: 0, kvNow: 0, powNow: 0, tempNow: 0,
      reqTotal: 0, tokTotal: 0, latP50: 0, latP95: 0, uptimeSec: 0,
    },
    log: [], alerts: [],
  };

  function makeNodeMetrics() {
    const empty = () => ({ now: 0, hist: Array(HIST).fill(0) });
    return {
      up: true,                          // 真实在线状态(live 模式由后端 up 覆盖; mock 默认在线)
      gpu: empty(), vram: empty(), cpu: empty(), mem: empty(),
      power: empty(), tempGpu: empty(), tempCpu: empty(),
      fan: empty(), disk: empty(), netIn: empty(), netOut: empty(),
      smActivity: empty(), pcie: empty(),
      rdmaIn: empty(), rdmaOut: empty(),
    };
  }
  function makeModelMetrics() {
    const empty = () => ({ now: 0, hist: Array(SPARK).fill(0) });
    return { tps: empty(), ttft: empty(), tpot: empty(), rps: empty(), kv: empty() };
  }
  function resetLive() {
    // wipe & reinitialize all keys in place — preserves the `live` reference
    for (const k of Object.keys(live.nodes))    delete live.nodes[k];
    for (const k of Object.keys(live.models))   delete live.models[k];
    for (const k of Object.keys(live.nodeMeta)) delete live.nodeMeta[k];
    NODES.forEach((n)  => { live.nodes[n.id]  = makeNodeMetrics(); live.nodeMeta[n.id] = { temps: [] }; });
    MODELS.forEach((m) => { live.models[m.id] = makeModelMetrics(); });
    Object.assign(live.cluster, {
      tps: [], rps: [], kv: [], pow: [], temp: [],
      tpsNow: 0, rpsNow: 0, kvNow: 0, powNow: 0, tempNow: 0,
      reqTotal: 0, tokTotal: 0, latP50: 0, latP95: 0, uptimeSec: 0,
    });
    live.log.length = 0;
    live.alerts.length = 0;
  }

  const totals = (() => {
    const totalVram = NODES.reduce((a, n) => a + n.gpu.mem, 0);
    const totalRam  = NODES.reduce((a, n) => a + n.ram, 0);
    const totalCores  = NODES.reduce((a, n) => a + n.cpu.cores, 0);
    const totalThreads= NODES.reduce((a, n) => a + n.cpu.threads, 0);
    const totalDisk = NODES.reduce((a, n) => a + n.disk, 0);
    const totalFp16 = NODES.reduce((a, n) => a + n.gpu.fp16, 0);
    const totalFp4  = NODES.reduce((a, n) => a + n.gpu.fp4, 0);
    return { totalVram, totalRam, totalCores, totalThreads, totalDisk,
             totalFp16, totalFp4, pflopsFp4: totalFp4 / 1000,
             nodes: NODES.length, gpus: NODES.length, models: MODELS_CATALOG.length,
             serving: MODELS_CATALOG.filter((m) => m.state === "serving").length };
  })();

  // ── Subscriber bus ──────────────────────────────────────────────────
  const subs = new Set();
  const emit = () => subs.forEach((fn) => fn());
  const subscribe = (fn) => { subs.add(fn); return () => subs.delete(fn); };

  // ── State ───────────────────────────────────────────────────────────
  let MODELS = [...MODELS_CATALOG];
  let mode   = "mock";
  let intervalMs = 1200;
  let mockTimer  = null;
  let sse        = null;
  let connStatus = "init"; // init|connecting|live|mock|reconnecting|error
  let running    = true;

  // First-time structure init
  NODES.forEach((n)  => { live.nodes[n.id]  = makeNodeMetrics(); live.nodeMeta[n.id] = { temps: [] }; });
  MODELS.forEach((m) => { live.models[m.id] = makeModelMetrics(); });

  // ── Mock simulator (same logic as the original v0 prototype) ────────
  const liveSeeds = {
    "node-1":   { gpu: 62, vram: 78, cpu: 41, mem: 58, power: 312, tempGpu: 64, tempCpu: 58, fan: 47, disk: 38, netIn: 220, netOut: 180 },
    "node-2":   { gpu: 78, vram: 64, cpu: 36, mem: 72, power: 168, tempGpu: 62, tempCpu: 55, fan: 38, disk: 22, netIn: 540, netOut: 510 },
    "node-3":   { gpu: 84, vram: 68, cpu: 44, mem: 76, power: 175, tempGpu: 66, tempCpu: 57, fan: 41, disk: 19, netIn: 480, netOut: 460 },
    "node-4":   { gpu: 21, vram: 14, cpu: 12, mem: 22, power:  68, tempGpu: 48, tempCpu: 46, fan: 22, disk: 12, netIn:  20, netOut:  18 },
    "node-5":   { gpu: 55, vram: 42, cpu: 28, mem: 48, power: 132, tempGpu: 58, tempCpu: 52, fan: 32, disk: 14, netIn: 220, netOut: 190 },
  };

  function seedMock() {
    resetLive();
    NODES.forEach((n) => {
      const s = liveSeeds[n.id];
      if (!s) return;          // 防御: 节点无 seed 时跳过, 不崩
      const ns = live.nodes[n.id];
      const seed = (k, v) => { ns[k].now = v; ns[k].hist = Array.from({ length: HIST }, () => v + (Math.random() - 0.5) * 6); };
      seed("gpu", s.gpu); seed("vram", s.vram); seed("cpu", s.cpu); seed("mem", s.mem);
      seed("power", s.power); seed("tempGpu", s.tempGpu); seed("tempCpu", s.tempCpu);
      seed("fan", s.fan); seed("disk", s.disk); seed("netIn", s.netIn); seed("netOut", s.netOut);
      seed("smActivity", s.gpu * 0.92); seed("pcie", s.netIn * 0.45);
      live.nodeMeta[n.id].temps = [
        { module: "CPU",  label: "Package",         chip: "coretemp", celsius: Math.round(s.tempCpu) },
        { module: "NVMe", label: "Composite nvme0", chip: "nvme0",    celsius: Math.round(s.tempCpu - 6 + Math.random() * 4) },
        { module: "NVMe", label: "Composite nvme1", chip: "nvme1",    celsius: Math.round(s.tempCpu - 5 + Math.random() * 4) },
        { module: "网卡", label: "ConnectX MAC",    chip: "mlx5",     celsius: Math.round(s.tempGpu - 8 + Math.random() * 3) },
        { module: "水冷", label: "Coolant temp",    chip: "aio",      celsius: Math.round(s.tempCpu - 12 + Math.random() * 3) },
      ];
    });
    MODELS.forEach((m) => {
      const ms = live.models[m.id];
      const seedM = (k, v) => { ms[k].now = v; ms[k].hist = Array.from({ length: SPARK }, () => Math.max(0, v + (Math.random() - 0.5) * v * 0.25)); };
      seedM("tps", m.tps); seedM("ttft", m.ttft); seedM("tpot", m.tpot); seedM("rps", m.rps); seedM("kv", m.kv);
    });
    live.cluster.reqTotal = 142_840;
    live.cluster.tokTotal = 18_400_000;
    live.cluster.uptimeSec = 86_400 * 12 + 4 * 3600 + 1820;
    live.alerts = [
      { sev: "warn", msg: "Node-2 GPU thermal margin reduced", sub: "85°C peak under load · soft thermal cap", when: "2m ago" },
      { sev: "ok",   msg: "Gateway · LiteLLM rotated TLS cert", sub: "letsencrypt · expires 2026-08-12", when: "14m ago" },
      { sev: "hot",  msg: "a model approaching KV-cache pressure", sub: "71% across inference nodes · consider evicting cold prefixes", when: "26m ago" },
      { sev: "warn", msg: "a node idle > 4h", sub: "candidate for power-save autopilot", when: "1h ago" },
      { sev: "ok",   msg: "Cluster heartbeat green", sub: "5/5 nodes · Prometheus 15s scrape · 0 drops", when: "1h ago" },
    ];
    seedLog();
  }

  function seedLog() {
    const serving = MODELS.filter((m) => m.state === "serving");
    let t0 = new Date();
    for (let i = 0; i < 30; i++) {
      const t = new Date(t0 - i * 1500 - Math.random() * 800);
      live.log.unshift(genLogEntry(t, serving));
    }
  }
  function genLogEntry(t, serving = MODELS.filter((m) => m.state === "serving")) {
    // 真实目录里可能没有 state==="serving" 的模型（v4-flash 多为 idle，
    // llama.cpp 为 no-metrics）→ serving 空。回退到全部 MODELS，再空则安全占位，
    // 否则 m.kind 读 undefined 会在 boot 的 seedMock 直接崩，整个 data.js 不启动。
    const pool = (serving && serving.length) ? serving : MODELS;
    const m = pool[Math.floor(Math.random() * pool.length)];
    if (!m) return { t, model: "—", meth: "POST /v1/chat/completions", lat: 0, status: "200" };
    const meth = m.kind === "embed"  ? "POST /v1/embeddings"
              : m.kind === "speech" ? "POST /v1/audio/transcriptions"
              : m.kind === "image"  ? "POST /v1/images/generations"
              : "POST /v1/chat/completions";
    const lat = Math.round(m.p50 + (Math.random() - .3) * (m.p95 - m.p50));
    const r = Math.random();
    const status = r > .985 ? "5xx" : r > .97 ? "4xx" : "200";
    return { t, model: m.id, meth, lat, status };
  }

  function mockTick() {
    NODES.forEach((n) => {
      const s = liveSeeds[n.id];
      if (!s) return;          // 防御: 节点无 seed 时跳过, 不崩
      const ns = live.nodes[n.id];
      const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
      const drift = (cur, target, amp) => clamp(cur + (target - cur) * 0.06 + (Math.random() - 0.5) * amp, 0, 100);
      const driftRaw = (cur, target, amp, lo, hi) => clamp(cur + (target - cur) * 0.06 + (Math.random() - 0.5) * amp, lo, hi);
      ns.gpu.now = drift(ns.gpu.now, s.gpu, 6);
      ns.vram.now = drift(ns.vram.now, s.vram, 1.2);
      ns.cpu.now = drift(ns.cpu.now, s.cpu, 8);
      ns.mem.now = drift(ns.mem.now, s.mem, 1.0);
      ns.power.now = driftRaw(ns.power.now, s.power * (0.5 + ns.gpu.now/100), 12, 50, /gateway/i.test(n.role) ? 480 : 240);
      ns.tempGpu.now = driftRaw(ns.tempGpu.now, 38 + ns.gpu.now * 0.45, 1.6, 32, 89);
      ns.tempCpu.now = driftRaw(ns.tempCpu.now, 36 + ns.cpu.now * 0.32, 1.4, 30, 84);
      ns.fan.now = driftRaw(ns.fan.now, Math.max(ns.tempGpu.now - 35, 10), 3, 8, 98);
      ns.disk.now = drift(ns.disk.now, s.disk, 1.0);
      ns.netIn.now = Math.max(0, ns.netIn.now + (Math.random() - 0.45) * 80);
      ns.netOut.now = Math.max(0, ns.netOut.now + (Math.random() - 0.48) * 80);
      ns.smActivity.now = drift(ns.smActivity.now, ns.gpu.now * 0.95, 5);
      ns.pcie.now = Math.max(0, ns.pcie.now + (Math.random() - 0.5) * 40);
      Object.keys(ns).forEach((k) => { ns[k].hist.shift(); ns[k].hist.push(ns[k].now); });
    });
    MODELS.forEach((m) => {
      if (m.state !== "serving") return;
      const ms = live.models[m.id];
      const drift = (cur, target, amp) => Math.max(0, cur + (target - cur) * 0.08 + (Math.random() - 0.5) * amp);
      ms.tps.now = drift(ms.tps.now, m.tps, m.tps * 0.18);
      ms.ttft.now = drift(ms.ttft.now, m.ttft, m.ttft * 0.14);
      ms.tpot.now = drift(ms.tpot.now, m.tpot, m.tpot * 0.18);
      ms.rps.now = drift(ms.rps.now, m.rps, m.rps * 0.22);
      ms.kv.now = drift(ms.kv.now, m.kv, 1.8);
      Object.keys(ms).forEach((k) => { ms[k].hist.shift(); ms[k].hist.push(ms[k].now); });
    });
    const totalTps = MODELS.reduce((a, m) => a + (m.state === "serving" ? live.models[m.id].tps.now : 0), 0);
    const totalRps = MODELS.reduce((a, m) => a + (m.state === "serving" ? live.models[m.id].rps.now : 0), 0);
    const serving  = MODELS.filter((m) => m.state === "serving");
    const totalKv  = serving.length ? serving.reduce((a, m) => a + live.models[m.id].kv.now, 0) / serving.length : 0;
    const totalPow = NODES.reduce((a, n) => a + live.nodes[n.id].power.now, 0);
    const peakT    = NODES.reduce((a, n) => Math.max(a, live.nodes[n.id].tempGpu.now), 0);
    live.cluster.tpsNow = totalTps; live.cluster.rpsNow = totalRps;
    live.cluster.kvNow  = totalKv;  live.cluster.powNow = totalPow; live.cluster.tempNow = peakT;
    [["tps", totalTps],["rps", totalRps],["kv", totalKv],["pow", totalPow],["temp", peakT]].forEach(([k, v]) => {
      if (live.cluster[k].length >= HIST) live.cluster[k].shift();
      live.cluster[k].push(v);
    });
    live.cluster.reqTotal += Math.round(totalRps * (intervalMs/1000));
    live.cluster.tokTotal += Math.round(totalTps * (intervalMs/1000));
    live.cluster.uptimeSec += intervalMs/1000;
    const n = 1 + Math.floor(Math.random() * 3);
    for (let i = 0; i < n; i++) live.log.unshift(genLogEntry(new Date()));
    if (live.log.length > 80) live.log.length = 80;
    emit();
  }

  function startMock() {
    stopMock(); stopSSE();
    if (mode !== "mock") seedMock();
    mode = "mock"; connStatus = "mock";
    mockTimer = setInterval(mockTick, intervalMs);
    mockTick(); // seed cluster aggregates
  }
  function stopMock() { if (mockTimer) { clearInterval(mockTimer); mockTimer = null; } }

  // ── Live (SSE) data source ──────────────────────────────────────────
  // The /api/stream endpoint pushes a full payload every ~1.2s; we use
  // the values to update `live.*.now` and shift the rolling history.
  function startLive() {
    stopMock(); stopSSE();
    mode = "live"; connStatus = "connecting";
    resetLive();
    const url = "/api/stream";
    sse = new EventSource(url);
    sse.onopen = () => { connStatus = "live"; emit(); };
    sse.onerror = () => {
      connStatus = "reconnecting";
      // Browsers auto-reconnect EventSource — only fall back if it stays down.
      setTimeout(() => {
        if (connStatus === "reconnecting") { stopSSE(); startMock(); connStatus = "mock-fallback"; emit(); }
      }, 8000);
      emit();
    };
    sse.onmessage = (ev) => {
      try {
        const payload = JSON.parse(ev.data);
        applyLivePayload(payload);
        emit();
      } catch (e) { /* skip */ }
    };
  }
  function stopSSE() { if (sse) { sse.close(); sse = null; } }

  function applyLivePayload(p) {
    // ── cluster ──
    if (p.cluster) {
      Object.assign(live.cluster, {
        tpsNow:  p.cluster.live.tpsNow  || 0,
        rpsNow:  p.cluster.live.rpsNow  || 0,
        kvNow:   p.cluster.live.kvNow   || 0,
        powNow:  p.cluster.live.powNow  || 0,
        tempNow: p.cluster.live.tempNow || 0,
        reqTotal: p.cluster.reqTotal || live.cluster.reqTotal,
        tokTotal: p.cluster.tokTotal || live.cluster.tokTotal,
        latP50: p.cluster.live.latP50 || 0,
        latP95: p.cluster.live.latP95 || 0,
        uptimeSec: p.cluster.uptimeSec || 0,
      });
      // Replace history wholesale with backend's view (5min @ 15s = 20 pts;
      // pad to HIST for chart consistency).
      const pad = (arr, n) => {
        if (!arr || !arr.length) return Array(n).fill(0);
        if (arr.length >= n) return arr.slice(-n);
        return [...Array(n - arr.length).fill(arr[0]), ...arr];
      };
      // pow/temp 有真实 Prometheus range → 整段替换；tps/kv 无 range
      // (vLLM 直采，非 Prometheus)，改由下方模型聚合滚动，勿在此清空。
      live.cluster.rps  = pad(p.cluster.history.rps,  HIST);
      live.cluster.pow  = pad(p.cluster.history.pow,  HIST);
      live.cluster.temp = pad(p.cluster.history.temp, HIST);
      // Home Assistant — optional. Each field may be null (HA exporter
      // absent or sensor stale); the UI treats null as "—", not 0.
      live.power = p.cluster.power || null;
      live.env   = p.cluster.env   || null;
    }
    // ── nodes：后端是运行时权威全集。前端动态对齐——后端发什么节点就显
    //    什么(id 任意, 不要求匹配前端静态目录)。静态 NODES 仅 mock 模式用;
    //    live 模式按 payload 重建 NODES + live.nodes, 否则后端 id(atlas/
    //    spark-NN)与静态 id(node-N)对不上 → 全部节点无数据。与 MODELS 同款。
    if (Array.isArray(p.nodes) && p.nodes.length) {
      const byId = new Map(NODES.map((x) => [x.id, x]));
      const next = [];
      p.nodes.forEach((n) => {
        const lv = n.live || {};
        const e = byId.get(n.id) || { id: n.id, os: "—", kernel: "—", driver: "NVIDIA —", cuda: "—" };
        e.name = n.name || n.id;
        e.ip = n.ip || "";
        e.class = n.class || "GPU host";
        e.role = n.role || "node";
        e.kind = n.kind || "discrete";
        e.gpu = n.gpu || { name: "—", mem: 0 };
        e.cpu = n.cpu || { cores: 0, threads: 0 };
        e.ram = n.ram || 0; e.disk = n.disk || 0; e.net = n.net || "";
        e.services = n.services || [];
        e.gpuPending = !!n.gpuPending;
        if (e.os === undefined) { e.os = "—"; e.kernel = "—"; e.driver = "NVIDIA —"; e.cuda = "—"; }
        if (!live.nodes[n.id]) live.nodes[n.id] = makeNodeMetrics();
        if (!live.nodeMeta[n.id]) live.nodeMeta[n.id] = { temps: [] };
        const ns = live.nodes[n.id];
        ns.up = n.up !== false;          // 后端权威 up → 诚实显示在线/离线
        const upd = (k, v) => { if (v === undefined || v === null) return;
          ns[k].now = v; ns[k].hist.shift(); ns[k].hist.push(v); };
        if (/gateway/i.test(e.role)) upd("gpu", lv.gpu);  // 网关(discrete)用真实 DCGM
        upd("vram", lv.vram);
        upd("tempGpu", lv.tempGpu); upd("tempCpu", lv.tempCpu); upd("power", lv.power);
        upd("cpu", lv.cpu); upd("mem", lv.mem); upd("disk", lv.disk);
        upd("netIn", lv.netIn); upd("netOut", lv.netOut);
        upd("rdmaIn", lv.rdmaIn); upd("rdmaOut", lv.rdmaOut);
        live.nodeMeta[n.id].temps = Array.isArray(lv.temps) ? lv.temps : [];
        next.push(e);
      });
      const keep = new Set(next.map((x) => x.id));
      Object.keys(live.nodes).forEach((id) => {
        if (!keep.has(id)) { delete live.nodes[id]; delete live.nodeMeta[id]; }
      });
      NODES.length = 0; Array.prototype.push.apply(NODES, next);   // 原地替换保持引用
    }
    // ── models：后端(网关 /health 自动发现)是运行时权威全集。
    //    前端动态对齐——后端返回什么就显什么(新模型自动建条目+live槽，
    //    消失的删掉)，顺序随后端(运行中+vLLM 优先)。不再依赖前端静态目录，
    //    部署怎么变监控自动跟，不漏显也不显错。
    if (Array.isArray(p.models) && p.models.length) {
      const byId = new Map(MODELS.map((x) => [x.id, x]));
      const next = [];
      p.models.forEach((m) => {
        const lv = m.live || {};
        let e = byId.get(m.id) ||
          { id: m.id, tps: 0, ttft: 0, tpot: 0, rps: 0, kv: 0,
            p50: 0, p95: 0, p99: 0, err: 0 };
        e.display = m.display || m.id;
        e.vendor = m.vendor || "—";
        e.kind = m.kind || "chat";
        e.params = m.params || "—";
        e.quant = m.quant || "—";
        e.ctx = m.ctx || 0;
        e.framework = m.framework || "—";
        e.nodes = Array.isArray(m.nodes) ? m.nodes : [];
        e.vram = m.vram || 0;
        e.route = m.route || "";
        e.tags = Array.isArray(m.tags) ? m.tags : [];
        e.state = m.state;                       // serving|idle|online|stopped
        e.resident = !!lv.resident;              // 真实驻留探针(后端权威)
        e.metricsSource = lv.metrics || "none";
        if (lv.p50 !== undefined) {
          e.p50 = Math.round(lv.p50);
          e.p95 = Math.round(lv.p95);
          e.p99 = Math.round(lv.p99);
        }
        if (!live.models[m.id]) live.models[m.id] = makeModelMetrics();
        const ms = live.models[m.id];
        const upd = (k, v) => { if (v === undefined || v === null) return;
          ms[k].now = v; ms[k].hist.shift(); ms[k].hist.push(v); };
        upd("tps", lv.tps); upd("rps", lv.rps); upd("kv", lv.kv);
        upd("ttft", lv.ttft); upd("tpot", lv.tpot);
        next.push(e);
      });
      const keep = new Set(next.map((x) => x.id));
      Object.keys(live.models).forEach((id) => {
        if (!keep.has(id)) delete live.models[id];   // 后端已不返回 → 清掉
      });
      MODELS.length = 0;                              // 原地替换, 保持数组引用
      Array.prototype.push.apply(MODELS, next);
    }
    // ── GB10 Spark GPU 环 = 加速器活跃度(驻留就绪基线 + 实时推理)。
    //    GB10 无可靠算力计数器，DCGM_GPU_UTIL/功率滞后失真(实测)。语义改为:
    //    后端真实 resident 探针(vLLM 可达且模型已加载) → 12% 基线
    //      含义="权重已驻留·预热·0 请求在途"，非真闲置；
    //    有真实推理 → 按真实 tps 升至 18~100%。discrete dGPU 节点保留真实 DCGM。
    NODES.forEach((n) => {
      if (/gateway/i.test(n.role)) return;             // 网关(discrete)节点 DCGM util 可靠, 不走推导
      const ns = live.nodes[n.id]; if (!ns) return;
      let act = 0;
      MODELS.forEach((m) => {
        // 任何有真实推理指标的后端(vLLM / llama.cpp) → 都参与节点活跃度推导
        if (!(m.metricsSource === "vllm" || m.metricsSource === "llamacpp" || m.metricsSource === "sglang")) return;
        if (!(m.nodes || []).includes(n.id)) return;
        const mm = live.models[m.id];
        if (!mm) return;
        if (m.resident || m.state === "serving" || m.state === "idle")
          act = Math.max(act, 12);                                   // 驻留就绪基线
        if (m.state === "serving" || mm.tps.now > 0)                 // 真实推理活跃
          act = Math.max(act, Math.min(100, Math.max(18, mm.tps.now / 60 * 100)));
      });
      ns.gpu.now = Math.round(act);
      ns.gpu.hist.shift(); ns.gpu.hist.push(ns.gpu.now);  // 滚动一拍: 节点循环已跳过 Spark 原始DCGM, 此处独占驱动 → 60拍历史连续(热力图/趋势图有数据)
    });
    // ── cluster tps/kv：从 vLLM 真实模型指标聚合滚动（无 Prometheus range 源）──
    if (p.models) {
      const vm = MODELS.filter((m) => (m.metricsSource === "vllm" || m.metricsSource === "llamacpp" || m.metricsSource === "sglang") && live.models[m.id]);
      const cTps = vm.reduce((a, m) => a + (live.models[m.id].tps.now || 0), 0);
      const cKv  = vm.length ? Math.max(...vm.map((m) => live.models[m.id].kv.now || 0)) : 0;
      live.cluster.tpsNow = cTps;
      live.cluster.kvNow  = cKv;
      const rollC = (key, val) => {
        let a = live.cluster[key];
        if (!Array.isArray(a)) { a = []; live.cluster[key] = a; }
        if (a.length < HIST) { while (a.length < HIST) a.push(val); }
        else { a.push(val); a.shift(); }
      };
      rollC("tps", cTps);
      rollC("kv", cKv);
    }
    // ── alerts (replace whole list) ──
    if (p.alerts) live.alerts = p.alerts;
    // ── log (fetched separately, but mirror if attached) ──
    // 后端 log 的 t 是字符串；前端 Telemetry 用 e.t.getTime()/toLocaleTimeString
    // 必须是 Date，否则整段 Telemetry 崩（同 genLogEntry 教训）。归一化。
    if (Array.isArray(p.log)) live.log = p.log.map((e) => ({
      t: new Date(e.t), meth: e.meth || "", model: e.model || "—",
      status: e.status || "200", lat: e.lat,
    }));
  }

  // ── Mode selection ──────────────────────────────────────────────────
  async function pickMode() {
    const url = new URL(location.href);
    const q = url.searchParams.get("mode");
    if (q === "live" || q === "mock") return q;   // 显式 ?mode= 强制覆盖
    // 探活优先：后端健康就 LIVE，绝不被陈旧 localStorage("mock") 卡死
    // （踩坑：Tweaks 选过 Mock 会持久化 aim.mode=mock，旧逻辑优先于探活
    //  导致后端再健康也永远显 MOCK）。健康则顺手清掉陈旧 mock 标记。
    try {
      const r = await fetch("/api/health", { cache: "no-store",
        signal: AbortSignal.timeout(2500) });
      if (r.ok) {
        if (localStorage.getItem("aim.mode") === "mock") localStorage.removeItem("aim.mode");
        return "live";
      }
    } catch (e) {}
    // 仅探活失败时才参考 localStorage 提示，否则 mock
    return localStorage.getItem("aim.mode") === "live" ? "live" : "mock";
  }

  function switchMode(next) {
    if (next === mode) return;
    localStorage.setItem("aim.mode", next);
    if (next === "live") startLive(); else startMock();
    emit();
  }

  // ── Display config (cluster_name, timezone) ────────────────────────
  // Loaded once from /api/config; mutated in place so consumers see fresh
  // values on the next re-render (Nav re-renders every 1s clock tick).
  // timezone=null → browser-local (default). Set via display.timezone in
  // hearth.yaml when you want timestamps pinned to a fixed IANA zone.
  const displayConfig = { cluster_name: "Hearth", timezone: null };
  async function loadConfig() {
    try {
      const r = await fetch("/api/config", { cache: "no-store",
        signal: AbortSignal.timeout(2500) });
      if (r.ok) Object.assign(displayConfig, await r.json());
    } catch (e) { /* keep defaults — browser-local timezone */ }
  }
  const _tzOpt = () => displayConfig.timezone ? { timeZone: displayConfig.timezone } : {};
  function formatTime(d) {
    return d.toLocaleTimeString("en-US", { hour12: false, ..._tzOpt() });
  }
  function formatDate(d) {
    return d.toLocaleDateString("en-US",
      { weekday: "short", month: "short", day: "numeric", ..._tzOpt() });
  }

  // ── Energy trends (24h / 7d / 30d, day-vs-night) ─────────────────
  // Refreshed every 60s — these are aggregates that don't move in
  // sub-minute scale, no point hammering the endpoint at SSE cadence.
  async function loadEnergyTrends() {
    try {
      const r = await fetch("/api/energy/trends", { cache: "no-store",
        signal: AbortSignal.timeout(8000) });
      if (r.ok) { live.energyTrends = await r.json(); emit(); }
    } catch (e) { /* keep last successful snapshot */ }
  }

  // ── Public API ──────────────────────────────────────────────────────
  window.AIData = {
    NODES, MODELS, live, totals, subscribe,
    get mode() { return mode; },
    get status() { return connStatus; },
    get displayConfig() { return displayConfig; },
    formatTime, formatDate,
    switchMode,
    setIntervalMs(ms) {
      intervalMs = ms;
      if (mode === "mock" && running) { stopMock(); mockTimer = setInterval(mockTick, ms); }
      // SSE rate is controlled by the backend's TICK_SEC env var.
    },
    pause()  { running = false; stopMock(); stopSSE(); },
    resume() { running = true; if (mode === "live") startLive(); else startMock(); },
  };

  // Boot
  seedMock();
  loadConfig();   // fire-and-forget; first render uses defaults, ~250ms later switches
  loadEnergyTrends();
  setInterval(loadEnergyTrends, 60_000);
  pickMode().then((m) => {
    if (m === "live") startLive(); else startMock();
  });
})();
