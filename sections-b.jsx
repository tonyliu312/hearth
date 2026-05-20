// AI-MONITOR · Page sections — Nodes, Models, Telemetry

const { NODES: _NODES, MODELS: _MODELS, live: _live, totals: _totals } = window.AIData;

// ── NODES ──────────────────────────────────────────────────────────────
function NodesSection() {
  useLive();
  const { t } = useLang();
  const [active, setActive] = useState(null);
  const [nfilter, setNfilter] = useState("All");
  return (
    <section className="page reveal" id="nodes">
      <div className="eyebrow"><span className="num">03</span>{t("Nodes")}</div>
      <div className="sect-head">
        <div>
          <h2 style={{ margin: 0 }}>{t("Five machines. ")}<em>{t("Each a citizen.")}</em></h2>
          <p className="lede" style={{ margin: "14px 0 0" }}>
            {t("Each host runs the LiteLLM gateway, an inference engine, or both. Click a node for the full forensic view.")}
          </p>
        </div>
        <div className="btn-seg" role="tablist">
          {["All","RTX","DGX","Active"].map((k) => (
            <button key={k} data-on={nfilter === k ? "1" : "0"} onClick={() => setNfilter(k)}>{t(k)}</button>
          ))}
        </div>
      </div>

      {/* auto-fit + minmax(280px,1fr): 每张卡保底 280px(3 个 66px 环 + gap
          一定能放下, CPU 环不再被 .node overflow:hidden 截), 列数随宽度自
          适应——宽屏 5 列、中屏 4/3 列、手机 1 列。container 已 margin:0
          auto 居中, 卡片随网格平铺填满, 左右留白天然等距。 */}
      <div className="grid g-5" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))" }}>
        {_NODES.filter((n) => nfilter === "All" ? true
              : nfilter === "RTX" ? n.id === "atlas"
              : nfilter === "DGX" ? n.id !== "atlas"
              : (_live.nodes[n.id] && _live.nodes[n.id].cpu.now > 0))
          .map((n) => <NodeCard key={n.id} node={n} onClick={() => setActive(n)} />)}
      </div>

      {active && <NodeDetail node={active} onClose={() => setActive(null)} />}
    </section>
  );
}

function NodeCard({ node, onClick }) {
  const { t } = useLang();
  const ns = _live.nodes[node.id];
  const isHost = node.id === "atlas";
  return (
    <article className="node" onClick={onClick}>
      <div className="node-head">
        <div>
          <div className="node-class">{node.class}</div>
          <div className="node-name">{node.name}</div>
          <div className="node-ip">{node.ip}</div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div className="node-status"><span className="dot" />{t("ONLINE")}</div>
          <div style={{ marginTop: 6 }}>
            <span className={"chip " + (isHost ? "accent" : "violet")} style={{ fontSize: 9.5 }}>
              {isHost ? "GATEWAY" : node.role.split(" ")[2] || "NODE"}
            </span>
          </div>
        </div>
      </div>

      <div className="node-rings">
        <div>
          {node.gpuPending
            ? <div style={{ width: 66, height: 66, display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "var(--mono)", fontSize: 8.5, color: "var(--ink-3)", textAlign: "center", lineHeight: 1.35 }}>GPU<br />{t("maint. pending")}</div>
            : <Ring value={ns.gpu.now} size={66} stroke={5} sub="GPU" />}
          <div className="rl">GPU</div>
        </div>
        <div>
          <Ring value={ns.vram.now} size={66} stroke={5} sub="VRAM" />
          <div className="rl">VRAM</div>
        </div>
        <div>
          <Ring value={ns.cpu.now} size={66} stroke={5} sub="CPU" />
          <div className="rl">CPU</div>
        </div>
      </div>

      {!node.gpuPending &&
        <Sparkline data={ns.gpu.hist} height={30} color={ns.gpu.now > 80 ? "var(--hot)" : "var(--accent)"} />}

      <div className="node-metas">
        <div className="k">GPU</div><div className="v">{node.gpu.name.replace("GeForce ","")}</div>
        <div className="k">VRAM</div><div className="v">{node.gpu.mem} GB</div>
        <div className="k">CPU</div><div className="v">{node.cpu.cores}c / {node.cpu.threads}t</div>
        <div className="k">RAM</div><div className="v">{node.ram} GB</div>
        <div className="k">{t("Net")}</div><div className="v">{node.net}</div>
        <div className="k">{t("Power")}</div><div className="v num">{node.gpuPending ? t("GPU pending · maintenance window") : ns.power.now.toFixed(0) + " W"}</div>
        <div className="k">{t("GPU temp")}</div><div className="v num" style={{ color: !node.gpuPending && ns.tempGpu.now > 80 ? "var(--hot)" : "var(--ink)" }}>{node.gpuPending ? "—" : ns.tempGpu.now.toFixed(0) + " °C"}</div>
      </div>
    </article>
  );
}

function NodeDetail({ node, onClose }) {
  const { t } = useLang();
  const ns = _live.nodes[node.id];
  const hostedModels = _MODELS.filter((m) => m.nodes.includes(node.id));
  return (
    <div className="card" style={{ marginTop: 22 }}>
      <div className="card-head">
        <div>
          <div className="card-title" style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span className="num" style={{ color: "var(--ink-3)", fontSize: 12, fontWeight: 400, fontFamily: "var(--mono)" }}>$ ssh root@{node.ip}</span>
            <span>{node.name} · {t("forensic view")}</span>
          </div>
          <div className="card-sub">{node.gpu.name} · {node.os} · kernel {node.kernel} · NVIDIA {node.driver.split(" ")[1]} · CUDA {node.cuda}</div>
        </div>
        <button onClick={onClose} style={{
          appearance: "none", border: 0, background: "rgba(255,255,255,.04)",
          color: "var(--ink-2)", borderRadius: 7, padding: "5px 10px",
          fontFamily: "var(--mono)", fontSize: 11, cursor: "pointer",
        }}>Close ✕</button>
      </div>

      <div style={{ padding: 22, display: "grid", gridTemplateColumns: "1.4fr 1fr 1fr", gap: 22 }}>
        <div>
          <div className="metric-l" style={{ marginBottom: 12 }}>{t("Accelerator activity · 60 ticks")}</div>
          <AreaChart
            series={[ns.gpu.hist]}
            colors={["var(--accent)"]}
            height={180}
            yMax={100}
            unit="%"
          />
          <div style={{ display: "flex", gap: 16, marginTop: 8, fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>
            <span><i style={{ display: "inline-block", width: 8, height: 8, borderRadius: 2, background: "var(--accent)", marginRight: 6 }} />{t("Accelerator activity")}</span>
          </div>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <DetailMetric label={t("GPU")}            value={ns.gpu.now.toFixed(0)} unit="%" bar={ns.gpu.now} />
          <DetailMetric label={t("VRAM")}           value={(node.gpu.mem * ns.vram.now / 100).toFixed(1)} unit={` / ${node.gpu.mem} GB`} bar={ns.vram.now} color="violet" />
          <DetailMetric label={t("GPU temp")}       value={ns.tempGpu.now.toFixed(0)} unit=" °C" bar={ns.tempGpu.now} color={ns.tempGpu.now > 80 ? "hot" : "ok"} />
          <DetailMetric label={t("Power draw")}     value={ns.power.now.toFixed(0)} unit=" W"  bar={Math.min(100, ns.power.now / (node.id === "atlas" ? 4.5 : 2.5))} color="hot" />
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <DetailMetric label={t("CPU")}            value={ns.cpu.now.toFixed(0)} unit="%" bar={ns.cpu.now} color="teal" />
          <DetailMetric label={t("RAM")}            value={(node.ram * ns.mem.now / 100).toFixed(0)} unit={` / ${node.ram} GB`} bar={ns.mem.now} color="violet" />
          <DetailMetric label={t("Disk usage")}     value={(node.disk * ns.disk.now / 100 / 1024).toFixed(1)} unit={` / ${(node.disk/1024).toFixed(0)} TB`} bar={ns.disk.now} color="ok" />
          <DetailMetric label={t("Net In")}         value={(ns.netIn.now).toFixed(0)} unit=" MB/s" bar={Math.min(100, ns.netIn.now / 12)} color="accent" />
          <DetailMetric label={t("Net Out")}        value={(ns.netOut.now).toFixed(0)} unit=" MB/s" bar={Math.min(100, ns.netOut.now / 12)} color="accent" />
        </div>
      </div>

      <div style={{ borderTop: "0.5px solid var(--line)", padding: 22, display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 22 }}>
        <div>
          <div className="metric-l" style={{ marginBottom: 10 }}>{t("Hosted models")}</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {hostedModels.length === 0 && <div style={{ fontFamily: "var(--mono)", fontSize: 11.5, color: "var(--ink-3)" }}>{t("— No models pinned to this node —")}</div>}
            {hostedModels.map((m) => (
              <div key={m.id} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 10px", borderRadius: 7, background: "rgba(255,255,255,.025)", border: "0.5px solid var(--line)" }}>
                <div>
                  <div style={{ fontFamily: "var(--display)", fontSize: 12.5, fontWeight: 590 }}>{m.display}</div>
                  <div style={{ fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>{m.framework} · {m.quant} · :{m.port}</div>
                </div>
                <span className={"chip " + (m.state === "serving" ? "ok" : m.state === "online" ? "ok" : m.state === "idle" ? "warn" : m.state === "stopped" ? "bad" : "ghost")}>
                  {t(m.state)}
                </span>
              </div>
            ))}
          </div>
        </div>

        <div>
          <div className="metric-l" style={{ marginBottom: 10 }}>{t("Running services")}</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {node.services.map((s) => <span key={s} className="chip">{s}</span>)}
          </div>
          <div className="metric-l" style={{ marginTop: 18, marginBottom: 10 }}>{t("System")}</div>
          <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "6px 14px", fontFamily: "var(--mono)", fontSize: 11.5 }}>
            <span style={{ color: "var(--ink-3)" }}>CPU</span><span>{node.cpu.model}</span>
            <span style={{ color: "var(--ink-3)" }}>OS</span><span>{node.os}</span>
            <span style={{ color: "var(--ink-3)" }}>{t("Kernel")}</span><span>{node.kernel}</span>
            <span style={{ color: "var(--ink-3)" }}>{t("Driver")}</span><span>{node.driver}</span>
            <span style={{ color: "var(--ink-3)" }}>CUDA</span><span>{node.cuda}</span>
            <span style={{ color: "var(--ink-3)" }}>{t("Net")}</span><span>{node.net}</span>
          </div>
          <div className="metric-l" style={{ marginTop: 18, marginBottom: 10 }}>{t("Hardware temps")}</div>
          <div style={{ display: "grid", gridTemplateColumns: "auto 1fr auto", gap: "6px 14px", fontFamily: "var(--mono)", fontSize: 11.5 }}>
            {(() => {
              const ts = (_live.nodeMeta[node.id] && _live.nodeMeta[node.id].temps) || [];
              if (!ts.length) return <span style={{ color: "var(--ink-3)", gridColumn: "1 / -1" }}>{t("— no temperature data (node unmanaged) —")}</span>;
              return ts.flatMap((t, i) => {
                const c = t.celsius >= 80 ? "var(--hot)" : t.celsius >= 70 ? "var(--warn)" : "var(--ink)";
                return [
                  <span key={i + "m"} style={{ color: "var(--ink-3)" }}>{t.module}</span>,
                  <span key={i + "l"} style={{ color: "var(--ink-3)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{t.label}</span>,
                  <span key={i + "v"} className="num" style={{ color: c, textAlign: "right" }}>{Math.round(t.celsius)} °C</span>,
                ];
              });
            })()}
          </div>
        </div>

        <div>
          <div className="metric-l" style={{ marginBottom: 10 }}>{t("Quick actions")}</div>
          <div style={{ display: "grid", gap: 8 }}>
            {["Open SSH session","Drain & cordon","Restart daemons","Pin model…","Run nvidia-smi dmon","Reboot"].map((a) => (
              <button key={a} style={{
                appearance: "none", border: "0.5px solid var(--line)",
                background: "rgba(255,255,255,.025)",
                color: "var(--ink)", borderRadius: 7, padding: "9px 12px",
                font: "500 12px var(--display)", letterSpacing: "-.005em", textAlign: "left", cursor: "pointer",
                transition: "background .15s, border-color .15s",
              }}
              onMouseEnter={(e) => { e.currentTarget.style.background = "rgba(255,255,255,.06)"; e.currentTarget.style.borderColor = "var(--line-2)"; }}
              onMouseLeave={(e) => { e.currentTarget.style.background = "rgba(255,255,255,.025)"; e.currentTarget.style.borderColor = "var(--line)"; }}
              >{t(a)}</button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function DetailMetric({ label, value, unit, bar, color = "accent" }) {
  return (
    <div className="metric">
      <div className="metric-h">
        <div className="metric-l">{label}</div>
        <div className="metric-v num">{value}<small>{unit}</small></div>
      </div>
      <div className={"bar " + color}><i style={{ width: `${Math.max(0, Math.min(100, bar))}%` }} /></div>
    </div>
  );
}

// ── MODELS ─────────────────────────────────────────────────────────────
function ModelsSection() {
  useLive();
  const { t } = useLang();
  const [active, setActive] = useState(null);
  const [filter, setFilter] = useState("all");

  const visible = _MODELS.filter((m) => {
    if (filter === "all") return true;
    if (filter === "serving") return m.state === "serving";
    if (filter === "idle") return m.state !== "serving";
    if (filter === "chat") return m.kind === "chat";
    return true;
  });

  return (
    <section className="page reveal" id="models">
      <div className="eyebrow"><span className="num">04</span>{t("Models · LiteLLM gateway")}</div>
      <div className="sect-head">
        <div>
          <h2 style={{ margin: 0 }}>{t("One endpoint. ")}<em>{t("Every model.")}</em></h2>
          <p className="lede" style={{ margin: "14px 0 0" }}>
            {t("All inference is routed through LiteLLM on ")}<span className="num" style={{ color: "var(--accent)" }}>{(_live.cluster && _live.cluster.gatewayHost) || "127.0.0.1:4000"}</span>{t(" — OpenAI-compatible API, smart routing, fallbacks, cost & token accounting. Cold models spin up on demand to fit the VRAM budget.")}
          </p>
        </div>
        <div className="btn-seg">
          {[["all","All"],["serving","Serving"],["idle","Idle"],["chat","Chat"]].map(([k, l]) => (
            <button key={k} data-on={filter === k ? "1" : "0"} onClick={() => setFilter(k)}>{t(l)}</button>
          ))}
        </div>
      </div>

      {/* Gateway header strip */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div style={{ padding: 22, display: "grid", gridTemplateColumns: "1.6fr 1fr 1fr 1fr 1fr", gap: 22, alignItems: "center" }}>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span style={{ width: 28, height: 28, borderRadius: 8, background: "linear-gradient(135deg, #0a84ff, #bf5af2)", display: "inline-flex", alignItems: "center", justifyContent: "center", color: "#fff", font: "600 11px var(--mono)" }}>LL</span>
              <b style={{ fontFamily: "var(--display)", fontWeight: 590, fontSize: 16 }}>LiteLLM Gateway</b>
              <span className="chip ok"><span className="dot" />{t("healthy")}</span>
            </div>
            <div className="num" style={{ fontSize: 11.5, color: "var(--ink-3)" }}>
              litellm · {_live.gatewayHost || "127.0.0.1:4000"} · OpenAI-compatible /v1/*
            </div>
          </div>
          <GatewayStat label={t("Catalog models")} value={_MODELS.length} sub={`${_MODELS.filter(m=>m.metricsSource==="vllm"||m.metricsSource==="llamacpp").length} ${t("with live metrics")}`} />
          <GatewayStat label={t("Live metric sources")} value={_MODELS.filter(m=>m.metricsSource==="vllm"||m.metricsSource==="llamacpp").length} sub={t("vLLM + llama.cpp /metrics")} />
          <GatewayStat label={t("Live throughput")} value={Math.round(_MODELS.filter(m=>m.metricsSource==="vllm"||m.metricsSource==="llamacpp").reduce((a,m)=>a+(_live.models[m.id]?_live.models[m.id].tps.now:0),0))} sub={t("t/s · measured sum")} />
          <GatewayStat label={t("LiteLLM metrics")} value={t("enterprise")} sub={t("OSS not exposed · documented")} />
        </div>
      </div>

      <div className="models">
        <div className="model-headrow">
          <div>{t("Model")}</div>
          <div>{t("Throughput")}</div>
          <div>{t("TTFT / TPOT")}</div>
          <div>{t("KV-cache")}</div>
          <div>{t("P50 / P95 / P99")}</div>
          <div>{t("Placement · framework")}</div>
          <div style={{ textAlign: "right" }}>{t("State")}</div>
        </div>
        {visible.map((m) => (
          <React.Fragment key={m.id}>
            <div className="model" data-active={active === m.id ? "1" : "0"} onClick={() => setActive(active === m.id ? null : m.id)}>
              <div className="model-name">
                <b>{m.display}</b>
                <span>{m.vendor} · {m.params} · {m.quant} · ctx&nbsp;{m.ctx === 0 ? "—" : m.ctx >= 1e6 ? `${(m.ctx/1e6).toFixed(0)}M` : `${(m.ctx/1024).toFixed(0)}K`}</span>
              </div>
              <div>
                {(m.metricsSource === "vllm" || m.metricsSource === "llamacpp") ? (
                  <>
                    <div className="model-bigmetric">{_live.models[m.id].tps.now.toFixed(0)}<small>t/s</small></div>
                    <div className="model-spark"><Sparkline data={_live.models[m.id].tps.hist} color="var(--accent)" height={20} /></div>
                  </>
                ) : <div style={{ fontFamily: "var(--mono)", color: "var(--ink-4)", fontSize: 11 }}>{t("no live metrics source")}<br />· {m.framework} ·</div>}
              </div>
              <div className="num" style={{ fontSize: 12, color: "var(--ink-2)" }}>
                {/* TTFT: vLLM 暴露, llama.cpp 不暴露(诚实显—); TPOT: 两者都有 */}
                {m.metricsSource === "vllm" ? (
                  <div><b style={{ color: "var(--ink)" }}>{_live.models[m.id].ttft.now.toFixed(0)}</b> <small style={{ color: "var(--ink-3)", fontFamily: "var(--mono)" }}>ms TTFT</small></div>
                ) : m.metricsSource === "llamacpp" ? (
                  <div><span style={{ color: "var(--ink-4)" }}>—</span> <small style={{ color: "var(--ink-3)", fontFamily: "var(--mono)" }}>ms TTFT</small></div>
                ) : null}
                {(m.metricsSource === "vllm" || m.metricsSource === "llamacpp") ? (
                  <div><b style={{ color: "var(--ink)" }}>{_live.models[m.id].tpot.now.toFixed(0)}</b> <small style={{ color: "var(--ink-3)", fontFamily: "var(--mono)" }}>ms/tok</small></div>
                ) : <span style={{ color: "var(--ink-4)" }}>—</span>}
              </div>
              <div>
                {m.metricsSource === "vllm" ? (
                  <>
                    <div style={{ marginBottom: 4, fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-2)" }}>
                      {_live.models[m.id].kv.now.toFixed(1)}%
                    </div>
                    <div className={"bar " + (_live.models[m.id].kv.now > 80 ? "bad" : _live.models[m.id].kv.now > 65 ? "hot" : "violet")}>
                      <i style={{ width: `${Math.min(100, _live.models[m.id].kv.now)}%` }} />
                    </div>
                  </>
                ) : <span style={{ color: "var(--ink-4)", fontFamily: "var(--mono)", fontSize: 10.5 }}>—</span>}
              </div>
              <div className="num" style={{ fontSize: 11.5, color: "var(--ink-2)" }}>
                {m.metricsSource === "vllm" ? <>
                  <span style={{ color: "var(--ink)" }}>{m.p50}</span>
                  <span style={{ color: "var(--ink-4)" }}> / </span>
                  <span>{m.p95}</span>
                  <span style={{ color: "var(--ink-4)" }}> / </span>
                  <span style={{ color: m.p99 > 20000 ? "var(--hot)" : "var(--ink-2)" }}>{m.p99}</span>
                  <small style={{ marginLeft: 4, fontFamily: "var(--mono)", color: "var(--ink-3)" }}>ms</small>
                </> : <span style={{ color: "var(--ink-4)", fontFamily: "var(--mono)", fontSize: 10.5 }}>—</span>}
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-2)" }}>{m.framework}</div>
                <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                  {m.nodes.map((nid) => <span key={nid} className="chip" style={{ fontSize: 9.5 }}>{nid}</span>)}
                </div>
              </div>
              <div style={{ textAlign: "right" }}>
                <span className={"chip " + (m.state === "serving" ? "ok" : m.state === "online" ? "ok" : m.state === "loading" ? "warn" : m.state === "stopped" ? "bad" : "ghost")}>
                  {(m.state === "serving" || m.state === "online") ? <><span className="dot" />{t(m.state)}</> : t(m.state)}
                </span>
              </div>
            </div>

            {active === m.id && (
              <ModelDetail model={m} />
            )}
          </React.Fragment>
        ))}
      </div>
    </section>
  );
}

function GatewayStat({ label, value, sub }) {
  return (
    <div>
      <div className="metric-l" style={{ marginBottom: 6 }}>{label}</div>
      <div style={{ font: "500 22px/1 var(--display)", letterSpacing: "-.022em", color: "var(--ink)", fontVariantNumeric: "tabular-nums" }}>{value}</div>
      <div style={{ marginTop: 4, fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>{sub}</div>
    </div>
  );
}

function ModelDetail({ model }) {
  const { t } = useLang();
  const ms = _live.models[model.id];
  return (
    <div className="model-detail">
      <div>
        <div className="metric-l" style={{ marginBottom: 8 }}>{t("Throughput · last 32 ticks")}</div>
        <AreaChart series={[ms.tps.hist]} colors={["var(--accent)"]} height={140} unit={model.kind === "embed" ? " e/s" : " t/s"} padding={{l:42,r:14,t:10,b:18}} ticks={3} />
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
        {model.metricsSource === "vllm" ? <>
          <DetailMetric label={t("TTFT (first token)")} value={ms.ttft.now.toFixed(0)} unit=" ms" bar={Math.min(100, ms.ttft.now / 8)} color="violet" />
          <DetailMetric label={t("TPOT (per token)")}   value={ms.tpot.now.toFixed(0)} unit=" ms" bar={Math.min(100, ms.tpot.now / 5)} color="teal" />
          <DetailMetric label={t("Requests / sec")}     value={ms.rps.now.toFixed(2)} unit="" bar={Math.min(100, ms.rps.now * 6)} color="accent" />
          <DetailMetric label={t("KV-cache")}           value={ms.kv.now.toFixed(1)} unit="%" bar={Math.min(100, ms.kv.now)} color={ms.kv.now > 80 ? "bad" : ms.kv.now > 65 ? "hot" : "violet"} />
        </> : model.metricsSource === "llamacpp" ? <>
          {/* llama.cpp /metrics 暴露 TPOT/throughput/running, 不暴露 TTFT/KV/e2e */}
          <DetailMetric label={t("TPOT (per token)")}   value={ms.tpot.now.toFixed(0)} unit=" ms" bar={Math.min(100, ms.tpot.now / 5)} color="teal" />
          <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)", lineHeight: 1.7 }}>
            {t("llama.cpp /metrics does not expose TTFT / KV% / e2e histograms · shown as — honestly")}
          </div>
        </> : <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)", lineHeight: 1.7 }}>
          {model.framework} {t("backend has no Prometheus /metrics endpoint. No live-metrics source → honestly marked, not faked. Model still serves normally via LiteLLM route")} {model.route}
        </div>}
      </div>
      <div>
        <div className="metric-l" style={{ marginBottom: 8 }}>{t("Route · ")}{model.route}</div>
        <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "5px 12px", fontFamily: "var(--mono)", fontSize: 11.5 }}>
          <span style={{ color: "var(--ink-3)" }}>{t("Framework")}</span><span>{model.framework}</span>
          <span style={{ color: "var(--ink-3)" }}>{t("Port")}</span><span>:{model.port}</span>
          <span style={{ color: "var(--ink-3)" }}>{t("Quant")}</span><span>{model.quant}</span>
          <span style={{ color: "var(--ink-3)" }}>{t("VRAM")}</span><span>{model.vram} GB</span>
          <span style={{ color: "var(--ink-3)" }}>{t("Context")}</span><span>{model.ctx === 0 ? "—" : model.ctx >= 1e6 ? `${(model.ctx/1e6).toFixed(0)}M tokens` : `${(model.ctx/1024).toFixed(0)}K tokens`}</span>
          <span style={{ color: "var(--ink-3)" }}>{t("Placement")}</span><span>{model.nodes.join(", ")}</span>
        </div>
        <div style={{ marginTop: 14, display: "flex", flexWrap: "wrap", gap: 5 }}>
          {model.tags.map((t) => <span key={t} className="chip violet" style={{ fontSize: 9.5 }}>{t}</span>)}
        </div>
        <div style={{ marginTop: 16, display: "flex", gap: 6 }}>
          <button style={modelBtn(true)}>{t("Open Playground")}</button>
          <button style={modelBtn(false)}>{t("Restart")}</button>
          <button style={modelBtn(false)}>{t("Logs")}</button>
        </div>
      </div>
    </div>
  );
}

function modelBtn(primary) {
  return {
    appearance: "none", border: "0.5px solid " + (primary ? "transparent" : "var(--line)"),
    background: primary ? "var(--ink)" : "transparent",
    color: primary ? "var(--bg)" : "var(--ink)",
    borderRadius: 7, padding: "7px 12px",
    font: "500 11.5px var(--display)", letterSpacing: "-.005em", cursor: "pointer",
  };
}

// ── TELEMETRY ──────────────────────────────────────────────────────────
function TelemetrySection() {
  useLive();
  const { t } = useLang();
  return (
    <section className="page reveal" id="telemetry">
      <div className="eyebrow"><span className="num">05</span>{t("Telemetry · alerts")}</div>
      <h2>{t("Signal, not ")}<em>{t("noise.")}</em></h2>
      <p className="lede">
        {t("Every request that lands at the gateway, every anomaly the rules engine catches — surfaced as a quiet, structured stream. No paging unless something actually needs you.")}
      </p>

      <div className="grid" style={{ gridTemplateColumns: "1.5fr 1fr", gap: 16 }}>
        <div className="card">
          <div className="card-head">
            <div>
              <div className="card-title">Request stream</div>
              <div className="card-sub">{t("LiteLLM request log · OSS Postgres")} · {_live.log.length}</div>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span className="chip ok"><span className="dot" />{t("streaming")}</span>
              <span className="num" style={{ fontSize: 11, color: "var(--ink-3)" }}>p50 {_live.cluster.latP50}ms · p95 {_live.cluster.latP95}ms</span>
            </div>
          </div>
          <div className="card-body" style={{ padding: "12px 22px 22px" }}>
            <div className="log">
              {_live.log.slice(0, 18).map((e, i) => {
                const stat = e.status;
                const cls = stat === "200" ? "ok" : stat === "4xx" ? "warn" : "bad";
                return (
                  <div className={"log-row " + cls} key={e.t.getTime() + "-" + i}>
                    <span className="t">{e.t.toLocaleTimeString("en-US", { hour12: false })}</span>
                    <span className="meth">{stat}</span>
                    <span className="log-meta"><span className="log-path">{e.meth.split(" ")[1]} <span style={{ color: "var(--ink-4)" }}>→</span> </span><b style={{ color: "var(--ink)", fontWeight: 500 }}>{e.model}</b></span>
                    <span className="lat">{typeof e.lat === "number" ? e.lat + " ms" : "—"}</span>
                  </div>
                );
              })}
              <div style={{ position: "absolute", inset: "auto 0 0 0", height: 64, background: "linear-gradient(180deg, transparent, var(--bg-1))", pointerEvents: "none" }} />
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-head">
            <div>
              <div className="card-title">Alerts</div>
              <div className="card-sub">Last 24 h · 0 paged · 4 informational</div>
            </div>
            <span className="chip warn">{_live.alerts.filter((a) => a.sev === "warn" || a.sev === "hot").length} open</span>
          </div>
          <div className="card-body" style={{ padding: "4px 0 0" }}>
            {_live.alerts.map((a, i) => (
              <div key={i} className={"alert " + a.sev}>
                <span className="sev" />
                <div className="msg">
                  <b>{a.msg}</b>
                  <span>{a.sub}</span>
                </div>
                <span className="when">{a.when}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}

// ── FABRIC (network topology) ──────────────────────────────────────────
function FabricSection() {
  useLive();
  const { t } = useLang();
  const [ref, { w }] = useElementSize();
  const H = 360;
  // 网关节点(default 第一个 role=gateway 或第一个节点) + 推理节点们
  const GW = _NODES.find((n) => /gateway/i.test(n.role || "")) || _NODES[0];
  const ATL = { x: w * 0.5, y: 70, id: GW.id, label: (GW.name + " · gateway") };
  const peers = _NODES.filter((n) => n.id !== GW.id);
  const peerXs = peers.map((_, i) => w * (peers.length === 1 ? 0.5 : 0.18 + i * (0.64 / Math.max(1, peers.length - 1))));
  const sparks = peers.map((n, i) => ({
    x: peerXs[i], y: H - 70, id: n.id, label: n.name,
  }));

  // Throughput on each link (atlas → spark)
  const linkLoad = (id) => {
    const ns = _live.nodes[id];
    return Math.min(1, (ns.netIn.now + ns.netOut.now) / 1200);
  };
  // 东西向 CX-7 RoCE/RDMA 真实吞吐 (MB/s)，驱动 spark↔spark mesh 强度
  const ewLoad = (id) => {
    const ns = _live.nodes[id];
    return Math.min(1, ((ns.rdmaIn ? ns.rdmaIn.now : 0) + (ns.rdmaOut ? ns.rdmaOut.now : 0)) / 4000);
  };

  return (
    <section className="page reveal" id="fabric">
      <div className="eyebrow"><span className="num">06</span>{t("Fabric · network")}</div>
      <div className="sect-head">
        <div>
          <h2 style={{ margin: 0 }}>{t("The wires ")}<em>{t("between everything.")}</em></h2>
          <p className="lede" style={{ margin: "14px 0 0" }}>
            {t("The gateway host peers with every inference node — link intensity & pulse are driven by real node-exporter throughput. Inter-node fabric (RDMA/InfiniBand) is physical topology only (throughput not instrumented by default).")}
          </p>
        </div>
      </div>

      <div className="card">
        <div className="card-body" ref={ref} style={{ padding: 0 }}>
          <div className="fabric">
            {w > 0 && (
              <svg viewBox={`0 0 ${w} ${H}`}>
                <defs>
                  <linearGradient id="linkg" x1="0" x2="1" y1="0" y2="0">
                    <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.05" />
                    <stop offset="50%" stopColor="var(--accent)" stopOpacity="0.6" />
                    <stop offset="100%" stopColor="var(--accent)" stopOpacity="0.05" />
                  </linearGradient>
                </defs>
                {/* spark <-> spark mesh — 强度由真实 RDMA 吞吐驱动 */}
                {sparks.map((a, i) => sparks.slice(i + 1).map((b, j) => {
                  const ld = Math.max(ewLoad(a.id), ewLoad(b.id));
                  return (
                  <line key={`m-${i}-${j}`} x1={a.x} y1={a.y} x2={b.x} y2={b.y}
                        stroke="var(--violet)" strokeWidth={0.8 + ld * 2.4}
                        strokeOpacity={0.16 + ld * 0.5} strokeDasharray="3 4" />
                  );
                }))}
                {/* atlas -> sparks */}
                {sparks.map((s, i) => {
                  const load = linkLoad(s.id);
                  return (
                    <g key={`a-${i}`}>
                      <line x1={ATL.x} y1={ATL.y} x2={s.x} y2={s.y}
                            stroke="var(--accent)" strokeWidth={1 + load * 2.5} strokeOpacity={0.18 + load * 0.4} />
                      {/* packet pulse */}
                      <circle r="2.5" fill="var(--accent)">
                        <animateMotion dur={`${(3 - load * 1.8).toFixed(1)}s`} repeatCount="indefinite"
                                       path={`M${ATL.x},${ATL.y} L${s.x},${s.y}`} />
                      </circle>
                      <circle r="2" fill="var(--violet)">
                        <animateMotion dur={`${(2.4 - load * 1.6).toFixed(1)}s`} repeatCount="indefinite" begin="1s"
                                       path={`M${s.x},${s.y} L${ATL.x},${ATL.y}`} />
                      </circle>
                    </g>
                  );
                })}
                {/* atlas node */}
                <NodeBlob x={ATL.x} y={ATL.y} label={GATEWAY_NODE.name.toUpperCase() + " · " + GATEWAY_NODE.gpu.name.split(" ").slice(-2).join(" ")} sub={GATEWAY_NODE.ip} color="var(--accent)" util={(_live.nodes[GATEWAY_NODE.id]||{}).gpu?.now || 0} />
                {sparks.map((s) => (
                  <NodeBlob key={s.id} x={s.x} y={s.y}
                            label={s.label.toUpperCase() + " · DGX SPARK"}
                            sub={_NODES.find((n) => n.id === s.id).ip}
                            color="var(--violet)"
                            util={_live.nodes[s.id].gpu.now} />
                ))}
              </svg>
            )}
          </div>
          <div style={{ padding: "0 22px 22px", display: "flex", justifyContent: "space-between", fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>
            <span><span style={{ display: "inline-block", width: 22, height: 1, background: "var(--accent)", verticalAlign: "middle", marginRight: 6 }} />North-South · 10 GbE</span>
            <span><span style={{ display: "inline-block", width: 22, borderTop: "1px dashed var(--violet)", verticalAlign: "middle", marginRight: 6 }} />{t("East-West · 200 GbE ConnectX-7 RDMA (live)")}</span>
            <span>{t("North-South pulse ∝ real network throughput")}</span>
          </div>
        </div>
      </div>
    </section>
  );
}

function NodeBlob({ x, y, label, sub, color, util }) {
  const r = 24;
  return (
    <g>
      <circle cx={x} cy={y} r={r + 8} fill={color} opacity="0.08">
        <animate attributeName="r" values={`${r + 4};${r + 14};${r + 4}`} dur="3s" repeatCount="indefinite" />
      </circle>
      <circle cx={x} cy={y} r={r} fill="var(--bg-1)" stroke={color} strokeWidth="1" />
      <circle cx={x} cy={y} r={r - 4} fill="none" stroke={color} strokeWidth="2"
              strokeDasharray={`${2 * Math.PI * (r-4) * util/100} ${2 * Math.PI * (r-4)}`}
              transform={`rotate(-90 ${x} ${y})`} />
      <text x={x} y={y - 2} textAnchor="middle" style={{ font: "600 11px var(--mono)", fill: "var(--ink)", fontVariantNumeric: "tabular-nums" }}>
        {util.toFixed(0)}%
      </text>
      <text x={x} y={y + r + 18} textAnchor="middle" style={{ font: "600 10px var(--mono)", fill: "var(--ink-2)", letterSpacing: ".06em" }}>
        {label}
      </text>
      <text x={x} y={y + r + 32} textAnchor="middle" style={{ font: "500 9.5px var(--mono)", fill: "var(--ink-4)" }}>
        {sub}
      </text>
    </g>
  );
}

// ── COMMAND PALETTE ────────────────────────────────────────────────────
function CmdK({ open, onClose }) {
  const { t } = useLang();
  const [q, setQ] = useState("");
  const [idx, setIdx] = useState(0);
  const items = useMemo(() => {
    const xs = [
      ..._NODES.map((n) => ({ kind: "node", label: n.name, sub: `${n.class} · ${n.ip}`, href: "#nodes" })),
      ..._MODELS.map((m) => ({ kind: "model", label: m.display, sub: `${m.framework} · ${m.params} · ${m.state}`, href: "#models" })),
      { kind: "view", label: "Overview", sub: "Section · top of page", href: "#overview" },
      { kind: "view", label: "Cluster", sub: "Section · aggregate telemetry", href: "#cluster" },
      { kind: "view", label: "Nodes", sub: "Section · five machines", href: "#nodes" },
      { kind: "view", label: "Models", sub: "Section · LiteLLM gateway", href: "#models" },
      { kind: "view", label: "Telemetry", sub: "Section · alerts & log stream", href: "#telemetry" },
      { kind: "view", label: "Fabric", sub: "Section · network topology", href: "#fabric" },
    ];
    if (!q.trim()) return xs;
    const qq = q.toLowerCase();
    return xs.filter((x) => x.label.toLowerCase().includes(qq) || x.sub.toLowerCase().includes(qq));
  }, [q]);
  useEffect(() => { setIdx(0); }, [q]);
  useEffect(() => {
    if (!open) return;
    const onKey = (e) => {
      if (e.key === "Escape") onClose();
      else if (e.key === "ArrowDown") { e.preventDefault(); setIdx((i) => Math.min(i + 1, items.length - 1)); }
      else if (e.key === "ArrowUp")   { e.preventDefault(); setIdx((i) => Math.max(i - 1, 0)); }
      else if (e.key === "Enter")     { const it = items[idx]; if (it) { window.location.hash = it.href; onClose(); } }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, items, idx, onClose]);

  if (!open) return null;
  return (
    <div className="cmdk-wrap" onClick={onClose}>
      <div className="cmdk" onClick={(e) => e.stopPropagation()}>
        <input autoFocus placeholder={t("Search nodes, models, sections…")}
               value={q} onChange={(e) => setQ(e.target.value)} />
        <div style={{ maxHeight: 360, overflowY: "auto" }}>
          {items.slice(0, 8).map((it, i) => (
            <div key={i} className="cmdk-item" data-on={i === idx ? "1" : "0"}
                 onMouseEnter={() => setIdx(i)}
                 onClick={() => { window.location.hash = it.href; onClose(); }}>
              <div>
                <div style={{ color: "var(--ink)", fontWeight: 500 }}>{t(it.label)}</div>
                <div style={{ fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)", marginTop: 2 }}>{t(it.sub)}</div>
              </div>
              <span className="meta">{it.kind}</span>
            </div>
          ))}
          {items.length === 0 && (
            <div style={{ padding: 22, color: "var(--ink-3)", fontFamily: "var(--mono)", fontSize: 12 }}>{t("No results")}</div>
          )}
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { NodesSection, NodeDetail, ModelsSection, TelemetrySection, FabricSection, CmdK });
