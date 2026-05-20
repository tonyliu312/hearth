// AI-MONITOR · Page sections.
// Hero → Cluster → Nodes → Models → Telemetry → Footer

const { NODES, MODELS, live, totals, subscribe } = window.AIData;

// Tiny helper: subscribe to live tick and re-render
function useLive() {
  const [, force] = useState(0);
  useEffect(() => subscribe(() => force((n) => n + 1)), []);
}

function fmtBytes(gb) {
  if (gb >= 1024) return `${(gb / 1024).toFixed(1)} TB`;
  return `${Math.round(gb)} GB`;
}
function fmtDuration(sec) {
  const d = Math.floor(sec / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  return `${d}d ${String(h).padStart(2,"0")}:${String(m).padStart(2,"0")}`;
}
function fmtNumber(n) {
  if (n >= 1e6) return (n / 1e6).toFixed(2) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return Math.round(n).toString();
}

// ── NAV ─────────────────────────────────────────────────────────────────
function Nav({ onOpenCmd }) {
  const [clock, setClock] = useState(() => new Date());
  const [menuOpen, setMenuOpen] = useState(false);
  const [, force] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setClock(new Date()), 1000);
    return () => clearInterval(id);
  }, []);
  useEffect(() => subscribe(() => force((n) => n + 1)), []);
  // 强制台北时区：用户在台北/偶尔公出中国，不依赖浏览器/机器时区设置
  const time = clock.toLocaleTimeString("en-US", { hour12: false });
  const date = clock.toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric" });
  const mode = window.AIData.mode;
  const status = window.AIData.status;
  const isLive = status === "live";
  const isReconnect = status === "reconnecting" || status === "connecting";
  const { t } = useLang();
  const close = () => setMenuOpen(false);
  return (
    <header className="nav">
      <div className="nav-logo">
        <div className="nav-logo-mark"></div>
        <span>{t("Hearth")}</span>
      </div>
      <nav className={"nav-links" + (menuOpen ? " open" : "")}>
        <a href="#overview" data-active onClick={close}>{t("Overview")}</a>
        <a href="#cluster" onClick={close}>{t("Cluster")}</a>
        <a href="#nodes" onClick={close}>{t("Nodes")}</a>
        <a href="#models" onClick={close}>{t("Models")}</a>
        <a href="#telemetry" onClick={close}>{t("Telemetry")}</a>
        <a href="#fabric" onClick={close}>{t("Fabric")}</a>
      </nav>
      <div className="nav-right">
        <span className={"dot" + (isReconnect ? " warn" : "")} />
        <span className="nav-status-text">
          {isLive ? t("5/5 nodes online")
            : isReconnect ? t("reconnecting…")
            : mode === "mock" ? t("mock data · no backend")
            : t("5/5 nodes online")}
        </span>
        <span className="nav-sep" />
        <span className="nav-mode-badge" title={isLive ? "Streaming /api/stream" : "Local simulator"}
              style={{
                fontFamily: "var(--mono)", fontSize: 9.5, letterSpacing: ".14em",
                padding: "2px 7px", borderRadius: 4,
                background: isLive ? "rgba(48,209,88,.12)" : "rgba(255,214,10,.10)",
                color: isLive ? "var(--ok)" : "var(--warn)",
                border: "0.5px solid " + (isLive ? "rgba(48,209,88,.3)" : "rgba(255,214,10,.3)"),
              }}>
          {isLive ? "LIVE" : "MOCK"}
        </span>
        <span className="nav-sep" />
        <span className="nav-clock">{date} · {time}</span>
        <span className="nav-sep" />
        <LangSwitcher />
        <span className="nav-sep" />
        <button className="nav-search" onClick={onOpenCmd}
          style={{
            display: "inline-flex", alignItems: "center", gap: 8,
            background: "rgba(255,255,255,.04)",
            border: "0.5px solid var(--line)",
            color: "var(--ink-2)",
            font: "500 11px var(--mono)",
            padding: "5px 10px 5px 10px",
            borderRadius: 7,
            cursor: "pointer",
          }}>
          {t("Search")} <span className="kbd">⌘K</span>
        </button>
        {/* 手机端汉堡菜单按钮: 桌面 display:none, 手机 ≤640 显示 */}
        <button className="nav-burger" aria-label="menu" aria-expanded={menuOpen}
          onClick={() => setMenuOpen((v) => !v)}>
          <span /><span /><span />
        </button>
      </div>
    </header>
  );
}

// ── HERO ────────────────────────────────────────────────────────────────
function Hero() {
  useLive();
  const { t } = useLang();
  const cluster = live.cluster;

  // For the headline ticker: rolling running tokens count
  return (
    <section className="hero" id="overview">
      <div className="hero-eyebrow reveal in">
        <span className="live"><span className="dot" />LIVE · 1.2 s</span>
        <span>{t("Home AI Compute Center")} · 192.168.1.0/24</span>
      </div>
      <h1 className="hero-title reveal in">
        {t("Home")}<br />
        <em>{t("Compute Center")}</em>
      </h1>
      <p className="hero-sub reveal in">
        {t("A unified telemetry surface for the home AI fabric — RTX 4090 edge + four DGX Spark inference nodes, served behind a single LiteLLM gateway. Every TFLOP, every token, every watt — in real time.")}
      </p>
      <div className="hero-meta reveal in">
        <span>{t("UPTIME")} <b>{fmtDuration(cluster.uptimeSec)}</b></span>
        <span>{t("REQUESTS")} <b>{fmtNumber(cluster.reqTotal)}</b></span>
        <span>{t("TOKENS SERVED")} <b>{fmtNumber(cluster.tokTotal)}</b></span>
        <span>{t("SCRAPE")} <b>15 s · Prometheus 2.55</b></span>
      </div>

      <div className="stat-row">
        <HeroStat
          label="AI Compute · FP4"
          value={totals.pflopsFp4}
          decimals={2}
          unit="PFLOPS"
          spark={cluster.tps}
          color="var(--accent)"
          sub={<>{Math.round(totals.totalFp16)} <small>TFLOPS BF16</small></>}
        />
        <HeroStat
          label="Unified Memory"
          value={totals.totalVram}
          unit="GB"
          spark={cluster.kv}
          color="var(--violet)"
          sub={<>{NODES.length} GPUs · 1× consumer + 4× datacenter</>}
        />
        <HeroStat
          label="CPU Cores"
          value={totals.totalCores}
          unit="cores"
          spark={[]}
          color="var(--teal)"
          fake={true}
          sub={<>{totals.totalThreads} threads · 1× x86 + 4× ARM</>}
        />
        <HeroStat
          label="System RAM"
          value={totals.totalRam}
          unit="GB"
          spark={[]}
          color="var(--ok)"
          fake={true}
          sub={<>DDR5 + LPDDR5X unified</>}
        />
        <HeroStat
          label="Storage"
          value={totals.totalDisk / 1024}
          unit="TB"
          decimals={0}
          spark={[]}
          color="var(--hot)"
          fake={true}
          sub={<>5× 4 TB NVMe Gen4 · 38% used</>}
        />
      </div>

      <div style={{ marginTop: 24, display: "flex", justifyContent: "space-between", alignItems: "flex-end", flexWrap: "wrap", gap: 16 }}>
        <div className="hero-meta" style={{ marginTop: 0 }}>
          <span><span className="dot" /> Live throughput</span>
          <span>TOKENS/SEC <b className="num">{cluster.tpsNow.toFixed(0)}</b></span>
          <span>REQUESTS/SEC <b className="num">{cluster.rpsNow.toFixed(2)}</b></span>
          <span>KV-CACHE <b className="num">{cluster.kvNow.toFixed(0)}%</b></span>
          <span>CLUSTER POWER <b className="num">{(cluster.powNow / 1000).toFixed(2)} kW</b></span>
          <span>HOTTEST GPU <b className="num">{cluster.tempNow.toFixed(0)}°C</b></span>
        </div>
      </div>
    </section>
  );
}

function HeroStat({ label, value, decimals = 0, unit, spark, color, sub, fake }) {
  const [ref, v] = useCountUp(value, 1400);
  return (
    <div className="stat">
      <div className="stat-label">{label}</div>
      <div ref={ref} className="stat-value">
        {v.toFixed(decimals)} <span className="stat-unit">{unit}</span>
      </div>
      <div className="stat-sub">{sub}</div>
      <div className="stat-spark">
        {fake ? <FakeSpark color={color} /> : <Sparkline data={spark} color={color} />}
      </div>
    </div>
  );
}
// Static "system" sparkline for fields that don't have time-series (cores/ram/disk)
function FakeSpark({ color }) {
  const [ref, { w }] = useElementSize();
  const H = 28;
  const data = useMemo(() => Array.from({ length: 28 }, (_, i) => 50 + Math.sin(i * 0.7) * 6 + Math.cos(i * 0.3) * 4), []);
  if (!w) return <div ref={ref} style={{ width: "100%", height: H }} />;
  return <div ref={ref}><Sparkline data={data} color={color} /></div>;
}

// ── CLUSTER ────────────────────────────────────────────────────────────
function Cluster() {
  useLive();
  const { t } = useLang();
  const [hmMetric, setHmMetric] = useState("GPU");
  const hmKey = hmMetric === "VRAM" ? "vram" : "gpu";
  // Build aggregate utilization series and per-resource breakdown
  // GPU utilization aggregate
  const tps = live.cluster.tps;
  const pow = live.cluster.pow;
  const temp = live.cluster.temp;
  const kv = live.cluster.kv;

  // Resource pool composition (VRAM)
  const usedVram = NODES.reduce((a, n) => a + n.gpu.mem * (live.nodes[n.id].vram.now / 100), 0);
  const freeVram = totals.totalVram - usedVram;

  // Build heatmap rows
  const hmRows = NODES.map((n) => ({
    label: n.name,
    data: live.nodes[n.id][hmKey].hist,
  }));

  return (
    <section className="page reveal" id="cluster">
      <div className="eyebrow"><span className="num">02</span>{t("Cluster · live telemetry")}</div>
      <h2>{t("Real-time pulse of the ")}<em>{t("entire fabric.")}</em></h2>
      <p className="lede">
        {t("Aggregated utilization, throughput, and thermals across all five nodes — sampled at 1.2 s, retained for 7 days in Prometheus, surfaced here as a single coherent picture.")}
      </p>

      <div className="grid g-3" style={{ marginBottom: 16 }}>
        <PulseCard title={t("Token throughput")} sub={t("tokens / sec · all models")} data={tps} color="var(--accent)" unit=" t/s" max={Math.max(...tps) * 1.3 || 1000} />
        <PulseCard title={t("Cluster power draw")} sub={t("watts · all PSUs")} data={pow} color="var(--hot)" unit=" W" max={Math.max(...pow) * 1.3 || 2000} />
        <PulseCard title={t("KV-cache pressure")} sub={t("% utilization, weighted mean")} data={kv} color="var(--violet)" unit="%" max={100} />
      </div>

      <div className="grid" style={{ gridTemplateColumns: "1.4fr 1fr", gap: "var(--gap)" }}>
        <div className="card">
          <div className="card-head">
            <div>
              <div className="card-title">{t("Per-node")} {hmMetric} · {t("heatmap")}</div>
              <div className="card-sub">{hmMetric === "VRAM" ? t("VRAM usage · last 60 ticks · 0–100%") : t("Accelerator activity · last 60 ticks · 0–100%")}</div>
            </div>
            <div className="btn-seg" role="tablist">
              {["GPU","VRAM"].map((k) => (
                <button key={k} data-on={hmMetric === k ? "1" : "0"} onClick={() => setHmMetric(k)}>{k}</button>
              ))}
            </div>
          </div>
          <div className="card-body">
            <Heatmap rows={hmRows} columns={30} />
            <div style={{ display: "flex", justifyContent: "space-between", marginTop: 16, fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>
              <span>60 ticks ago</span>
              <span style={{ display: "flex", gap: 12, alignItems: "center" }}>
                <Swatch hue={145} />0–35
                <Swatch hue={210} />35–65
                <Swatch hue={28}  />65–85
                <Swatch hue={5}   />85+ critical
              </span>
              <span>now</span>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-head">
            <div>
              <div className="card-title">{t("Resource pools")}</div>
              <div className="card-sub">{t("Aggregate capacity vs. live usage")}</div>
            </div>
          </div>
          <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: 18 }}>
            <PoolBar label="GPU Compute (FP4)" used={live.cluster.tpsNow * 0.001 * totals.pflopsFp4} cap={totals.pflopsFp4} unit=" PFLOPS" segs={[
              { label: "qwen3", value: 0.30 * live.cluster.tpsNow * 0.001 * totals.pflopsFp4, color: "var(--accent)" },
              { label: "deepseek", value: 0.24 * live.cluster.tpsNow * 0.001 * totals.pflopsFp4, color: "var(--violet)" },
              { label: "gpt-oss", value: 0.16 * live.cluster.tpsNow * 0.001 * totals.pflopsFp4, color: "var(--teal)" },
              { label: "other", value: 0.12 * live.cluster.tpsNow * 0.001 * totals.pflopsFp4, color: "var(--pink)" },
              { label: "free", value: totals.pflopsFp4 - 0.82 * live.cluster.tpsNow * 0.001 * totals.pflopsFp4, color: "rgba(255,255,255,.08)" },
            ]} />
            <PoolBar label="VRAM / Unified" used={usedVram} cap={totals.totalVram} unit=" GB" segs={[
              ...MODELS.filter((m) => m.state === "serving").map((m, i) => ({
                label: m.id, value: m.vram, color: ["var(--accent)","var(--violet)","var(--teal)","var(--hot)","var(--ok)","var(--pink)","#5ac8fa","#ffd60a"][i % 8],
              })),
              { label: "free", value: Math.max(0, totals.totalVram - MODELS.filter(m=>m.state==="serving").reduce((a,m)=>a+m.vram,0)), color: "rgba(255,255,255,.08)" },
            ]} />
            <PoolBar label="System RAM" used={totals.totalRam * 0.42} cap={totals.totalRam} unit=" GB" segs={[
              { label: "kernel", value: totals.totalRam * 0.06, color: "var(--ink-4)" },
              { label: "containers", value: totals.totalRam * 0.21, color: "var(--accent)" },
              { label: "buffers", value: totals.totalRam * 0.08, color: "var(--teal)" },
              { label: "page-cache", value: totals.totalRam * 0.07, color: "var(--violet)" },
              { label: "free", value: totals.totalRam * 0.58, color: "rgba(255,255,255,.08)" },
            ]} />
            <PoolBar label="Storage" used={totals.totalDisk * 0.38} cap={totals.totalDisk} unit=" GB" segs={[
              { label: "models", value: totals.totalDisk * 0.22, color: "var(--violet)" },
              { label: "datasets", value: totals.totalDisk * 0.09, color: "var(--accent)" },
              { label: "logs/traces", value: totals.totalDisk * 0.04, color: "var(--teal)" },
              { label: "system", value: totals.totalDisk * 0.03, color: "var(--ink-4)" },
              { label: "free", value: totals.totalDisk * 0.62, color: "rgba(255,255,255,.08)" },
            ]} />
          </div>
        </div>
      </div>

      <div className="grid g-3" style={{ marginTop: 16 }}>
        <SmallStat label="Peak GPU temp"     value={cluster_peak()} unit="°C" sub="Spark-02 · deepseek load" color="var(--hot)" />
        <SmallStat label="Avg power / token" value={(live.cluster.powNow / Math.max(1, live.cluster.tpsNow)).toFixed(2)} unit=" J/tok" sub="lower is better · target ≤ 1.4" color="var(--accent)" />
        <SmallStat label="P95 latency (chat)" value={Math.round(weightedP95())} unit=" ms" sub="weighted across serving models" color="var(--violet)" />
      </div>
    </section>
  );
}

function cluster_peak() { return Math.round(Math.max(...NODES.map((n) => live.nodes[n.id].tempGpu.now))); }
function weightedP95() {
  const serving = MODELS.filter((m) => m.state === "serving" && m.kind === "chat");
  const totalRps = serving.reduce((a, m) => a + live.models[m.id].rps.now, 0) || 1;
  return serving.reduce((a, m) => a + m.p95 * live.models[m.id].rps.now / totalRps, 0);
}

function Swatch({ hue }) {
  return <span style={{ display: "inline-block", width: 12, height: 8, borderRadius: 2, background: `oklch(0.62 0.18 ${hue})` }} />;
}

function PulseCard({ title, sub, data, color, unit, max }) {
  const now = data[data.length - 1] || 0;
  return (
    <div className="card">
      <div className="card-head">
        <div>
          <div className="card-title">{title}</div>
          <div className="card-sub">{sub}</div>
        </div>
        <div className="num" style={{ fontSize: 22, fontFamily: "var(--display)", fontWeight: 500, letterSpacing: "-.02em" }}>
          {now.toFixed(unit === "%" ? 0 : 1)}<small style={{ fontSize: 11, color: "var(--ink-3)", fontWeight: 400, fontFamily: "var(--mono)", marginLeft: 3 }}>{unit}</small>
        </div>
      </div>
      <div className="card-body" style={{ padding: "10px 6px 6px" }}>
        <AreaChart series={[data]} colors={[color]} height={140} yMax={max} unit={unit} padding={{l:36,r:14,t:10,b:18}} ticks={3} />
      </div>
    </div>
  );
}

function PoolBar({ label, used, cap, unit, segs }) {
  const pct = (used / cap) * 100;
  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6, alignItems: "baseline" }}>
        <span className="metric-l">{label}</span>
        <span className="num" style={{ fontSize: 12, color: "var(--ink-2)" }}>
          <b style={{ color: "var(--ink)", fontWeight: 500 }}>{used.toFixed(used < 10 ? 2 : 0)}</b>
          <span style={{ color: "var(--ink-4)" }}> / {cap.toFixed(0)}{unit}</span>
          <span style={{ marginLeft: 8, color: "var(--ink-3)" }}>{pct.toFixed(0)}%</span>
        </span>
      </div>
      <StackedBar segments={segs} height={8} total={cap} />
      <div style={{ display: "flex", flexWrap: "wrap", gap: 10, marginTop: 8, fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)" }}>
        {segs.filter((s) => s.label !== "free").map((s, i) => (
          <span key={i} style={{ display: "inline-flex", alignItems: "center", gap: 5 }}>
            <i style={{ width: 8, height: 8, borderRadius: 2, background: s.color }} />
            {s.label} <b style={{ color: "var(--ink-2)", fontWeight: 500 }}>{s.value.toFixed(s.value < 10 ? 1 : 0)}</b>
          </span>
        ))}
      </div>
    </div>
  );
}

function SmallStat({ label, value, unit, sub, color }) {
  return (
    <div className="card" style={{ padding: 18 }}>
      <div className="metric-l" style={{ marginBottom: 12 }}>{label}</div>
      <div className="num" style={{ font: "500 28px/1 var(--display)", letterSpacing: "-.022em", color: "var(--ink)" }}>
        {value}<span style={{ fontSize: 13, color: "var(--ink-3)", marginLeft: 3, fontFamily: "var(--mono)" }}>{unit}</span>
      </div>
      <div style={{ marginTop: 10, fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>{sub}</div>
    </div>
  );
}

Object.assign(window, { Nav, Hero, Cluster, useLive, fmtBytes, fmtDuration, fmtNumber });
