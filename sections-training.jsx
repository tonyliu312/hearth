// AI-MONITOR · Training observability section (Phase 1 — obs-only substrate).
// 数据来自 /api/training(DCGM L1 + node-exporter, 经 obs-prometheus 只读)→ live.training。
// 覆盖 Layer C(GPU 健康)/D(RoCE 互联)/E(静默 stall 检测)。loss·grad-norm·step·
// ETA·MFU 需训练框架信号源(Phase 2),此处诚实标"未接入",不伪造。
// 注:本文件独立 babel 作用域——全局词法环境已占用 live/_live/_NODES,故用 _tLive 等独立名;
// SmallStat/DetailMetric 未导出到 window,故内联 _TStat。可用 window 全局:Ring/Sparkline/useLive/useLang。

const _tLive = window.AIData.live;

function _fmtBw(mbps) {
  if (mbps >= 1000) return (mbps / 1000).toFixed(1) + " GB/s";
  return Math.round(mbps) + " MB/s";
}

function _fmtEta(sec) {
  if (sec == null) return "—";
  const h = Math.floor(sec / 3600), m = Math.round((sec % 3600) / 60);
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

// 训练框架信号(Phase 2):loss·grad-norm·step/ETA·acceptance。来自 leader tfevents,
// 缺失时(当前轮 / 文件不存在)整块不渲染,由 TrainingSection 退回 "awaiting" 提示。
function _SignalBlock({ sig }) {
  const { t } = useLang();
  const loss = sig.lossSeries || [];
  const acc = sig.accSeries || [];
  const macc = sig.meanAcceptSeries || [];
  const lossDelta = loss.length >= 2 ? loss[loss.length - 1] - loss[0] : 0;
  // mean accept length 是 EAGLE/投机解码核心 KPI:≥2.3 达标(对应单流 ≥50 t/s),1.0=无效
  const ma = sig.meanAccept;
  const maTone = ma == null ? "var(--ink-3)" : ma >= 2.3 ? "var(--ok)" : ma >= 1.6 ? "var(--warn)" : "var(--bad)";
  const stale = sig.staleSec != null && sig.staleSec > 600;   // >10min 无写入 = 疑似停
  const pct = Math.round((sig.progress || 0) * 100);
  return (
    <div className="card" style={{ marginBottom: 16, borderColor: stale ? "var(--hot)" : undefined }}>
      <div className="card-body" style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))", gap: 22, alignItems: "center" }}>
        {/* loss — hero, 自动缩放 sparkline 显降势 */}
        <div style={{ gridColumn: "span 2", minWidth: 0 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
            <span style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>{t("train loss")}</span>
            <span style={{ fontFamily: "var(--mono)", fontSize: 11, color: lossDelta < 0 ? "var(--ok)" : "var(--ink-3)" }}>
              {lossDelta < 0 ? "↓" : ""}{lossDelta ? Math.abs(lossDelta).toFixed(3) : ""}
            </span>
          </div>
          <div className="num" style={{ fontSize: 26 }}>{sig.loss != null ? sig.loss.toFixed(3) : "—"}</div>
          <Sparkline data={loss} height={56} color="var(--accent)" />
        </div>
        {/* step + epoch + progress + ETA */}
        <div style={{ minWidth: 0 }}>
          <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>
            {t("step")}{sig.epoch != null ? ` · ep ${sig.epoch}` : ""}
          </div>
          <div className="num" style={{ fontSize: 20 }}>{sig.step}<small style={{ fontSize: 12, opacity: .5 }}>{sig.totalSteps ? ` / ${sig.totalSteps}` : ""}</small></div>
          <div style={{ height: 4, background: "rgba(255,255,255,.08)", borderRadius: 3, margin: "6px 0" }}>
            <div style={{ width: `${pct}%`, height: "100%", background: "var(--accent)", borderRadius: 3 }} />
          </div>
          <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-2)" }}>
            {t("ETA")} {_fmtEta(sig.etaSec)} · {sig.perStepSec != null ? sig.perStepSec.toFixed(2) + "s/it" : "—"}
          </div>
        </div>
        {/* mean accept length — EAGLE/投机解码核心 KPI(≥2.3 达标);acceptance% 作辅 */}
        <div style={{ minWidth: 0 }}>
          <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>
            {t("mean accept")} <small style={{ opacity: .55 }}>{t("≥2.3")}</small>
          </div>
          <div className="num" style={{ fontSize: 20, color: maTone }}>
            {ma != null ? ma.toFixed(2) + "×" : "—"}
            {sig.acceptance != null && <small style={{ fontSize: 11, opacity: .6, color: "var(--ink-3)" }}> · {(sig.acceptance * 100).toFixed(0)}% acc</small>}
          </div>
          <Sparkline data={macc.length ? macc : acc} height={28} color="var(--violet)" />
        </div>
        {/* grad-norm + lr + 状态 */}
        <div style={{ minWidth: 0, display: "flex", flexDirection: "column", gap: 4 }}>
          <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>
            {t("grad norm")} <span style={{ color: "var(--ink-1)" }}>{sig.gradNorm != null ? sig.gradNorm.toFixed(2) : "—"}</span>
          </div>
          <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>
            {t("lr")} <span style={{ color: "var(--ink-1)" }}>{sig.lr != null ? sig.lr.toExponential(1) : "—"}</span>
          </div>
          {stale ? <_TPill label={t("write stalled?")} tone="bad" />
                 : <_TPill label={t("logging")} tone="ok" />}
        </div>
      </div>
    </div>
  );
}

function _TStat({ label, value, unit, sub, color }) {
  return (
    <div>
      <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>{label}</div>
      <div className="num" style={{ fontSize: 18, color: color || "var(--ink-1)" }}>
        {value}{unit && <small style={{ fontSize: 11, opacity: 0.6 }}>{unit}</small>}
      </div>
      {sub && <div style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)" }}>{sub}</div>}
    </div>
  );
}

function _TPill({ label, tone }) {
  const c = { ok: "var(--ok)", warn: "var(--warn)", bad: "var(--bad)", idle: "var(--ink-3)" }[tone] || "var(--ink-3)";
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 6, fontFamily: "var(--mono)",
                   fontSize: 11, color: c, border: `1px solid ${c}`, borderRadius: 999,
                   padding: "3px 10px", opacity: tone === "idle" ? 0.6 : 1 }}>
      <span style={{ width: 6, height: 6, borderRadius: 999, background: c }} />{label}
    </span>
  );
}

function _RankCard({ n, slowest }) {
  const { t } = useLang();
  const fault = n.xid > 0 || n.eccDbe > 0 || n.ibLinkDowned > 0;
  const isSlow = slowest && n.id === slowest;
  const memTone = n.memPct >= 90 ? "var(--bad)" : n.memPct >= 80 ? "var(--hot)" : "var(--ok)";
  const ringColor = n.stallSuspect ? "var(--bad)" : isSlow ? "var(--hot)" : "var(--accent)";
  const border = (n.stallSuspect || fault) ? "var(--bad)" : isSlow ? "var(--hot)" : undefined;
  return (
    <div className="card" style={{ borderColor: border }}>
      <div className="card-body" style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: 14, alignItems: "center" }}>
        <Ring value={n.util} max={100} size={64} stroke={6} color={ringColor}
              label={n.util.toFixed(0)} sub={t("util")} />
        <div style={{ minWidth: 0 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 8 }}>
            <strong style={{ fontSize: 14 }}>{n.id}</strong>
            {n.stallSuspect ? <span style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--bad)" }}>{t("stall?")}</span>
             : isSlow ? <span style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--hot)" }}>{t("straggler")}</span> : null}
          </div>
          {/* unified-memory headroom — OOM-critical on GB10 */}
          <div style={{ marginTop: 8, fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-2)" }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span>{t("mem")}</span><span style={{ color: memTone }}>{n.memPct.toFixed(0)}%</span>
            </div>
            <div style={{ height: 4, background: "rgba(255,255,255,.08)", borderRadius: 3, marginTop: 3 }}>
              <div style={{ width: `${Math.min(100, n.memPct)}%`, height: "100%", background: memTone, borderRadius: 3 }} />
            </div>
          </div>
          <div style={{ marginTop: 8, display: "flex", gap: 14, fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>
            <span>{n.tempGpu.toFixed(0)}°C</span>
            <span>{n.power.toFixed(0)} W</span>
            <span>{_fmtBw(n.roceRxMBps + n.roceTxMBps)}</span>
          </div>
          {(fault || n.stallSuspect) && (
            <div style={{ marginTop: 6, fontFamily: "var(--mono)", fontSize: 10, color: "var(--bad)" }}>
              {n.xid > 0 ? `Xid ${n.xid} ` : ""}{n.eccDbe > 0 ? `ECC-DBE ${n.eccDbe} ` : ""}
              {n.ibLinkDowned > 0 ? `link-down ${n.ibLinkDowned} ` : ""}{n.stallSuspect ? t("comm stalled?") : ""}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function TrainingSection() {
  useLive();
  const { t } = useLang();
  const T = _tLive.training || { nodes: [], summary: {} };
  const S = T.summary || {};
  const nodes = T.nodes || [];
  const sig = T.signal || { present: false };
  const hist = _tLive.trainingHist || { util: [], roce: [] };
  if (!nodes.length) return null;                 // 无 GB10 训练节点 → 不渲染

  const active = S.active || 0;
  const idle = active === 0;
  // straggler:活跃节点里 util 最低的(且偏斜显著 ≥15%)
  let slowest = null;
  if (active > 1 && (S.utilSkew || 0) >= 15) {
    const sorted = nodes.filter((n) => n.util > 0).sort((a, b) => a.util - b.util);
    slowest = sorted.length ? sorted[0].id : null;
  }
  const health = S.stallSuspect ? { tone: "bad", label: t("stall suspected") }
    : (S.anyEccDbe || S.anyXid) ? { tone: "bad", label: t("GPU fault") }
    : idle ? { tone: "idle", label: t("idle") }
    : { tone: "ok", label: t("healthy") };

  return (
    <section className="page reveal" id="training">
      <div className="eyebrow"><span className="num">07</span>{t("Training · distributed")}</div>
      <div className="sect-head">
        <div>
          <h2 style={{ margin: 0 }}>{t("The long ")}<em>{t("run.")}</em></h2>
          <p className="lede" style={{ margin: "14px 0 0" }}>
            {t("Substrate health for the 4×GB10 training cluster — per-rank GPU, unified-memory headroom, CX-7 RoCE collective bandwidth, and a silent-stall heuristic. Read-only from obs; zero training impact.")}
          </p>
        </div>
      </div>

      {/* Phase 2 训练框架信号(loss/step/ETA/acceptance)—— 有 tfevents 才显 */}
      {sig.present && <_SignalBlock sig={sig} />}

      {/* status strip(底座 — 始终显)*/}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="card-body" style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: 18, alignItems: "center" }}>
          <_TStat label={t("Ranks active")} value={`${active}/${S.nodes || nodes.length}`} sub={t("util ≥ 50%")} />
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <Ring value={S.utilAvg || 0} max={100} size={56} stroke={5}
                  color={idle ? "var(--ink-3)" : "var(--accent)"} label={(S.utilAvg || 0).toFixed(0)} sub={t("avg util")} />
            <div style={{ flex: 1, minWidth: 0 }}><Sparkline data={hist.util} height={28} color="var(--accent)" /></div>
          </div>
          <div style={{ minWidth: 0 }}>
            <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>{t("RoCE Σ rx+tx")}</div>
            <div className="num" style={{ fontSize: 18 }}>{_fmtBw((S.roceRxMBps || 0) + (S.roceTxMBps || 0))}</div>
            <Sparkline data={hist.roce} height={22} color="var(--violet)" />
          </div>
          <_TStat label={t("GPU power Σ")} value={(S.powerTotal || 0).toFixed(0)} unit=" W" />
          <div style={{ display: "flex", flexDirection: "column", gap: 6, alignItems: "flex-start" }}>
            <_TPill label={health.label} tone={health.tone} />
            {(S.utilSkew || 0) >= 15 && active > 1 && <_TPill label={`${t("skew")} ${S.utilSkew.toFixed(0)}%`} tone="warn" />}
          </div>
        </div>
      </div>

      {/* per-rank small multiples */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 14 }}>
        {nodes.map((n) => <_RankCard key={n.id} n={n} slowest={slowest} />)}
      </div>

      {/* signal 缺失时的诚实提示;有信号时标注来源 + MFU 仍待接 */}
      {!sig.present ? (
        <div style={{ marginTop: 16, fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)", lineHeight: 1.6 }}>
          {t("Loss · step / ETA · accept rate — awaiting a training signal source (auto-detects JSON / Prometheus / TensorBoard from the leader). Not fabricated. See docs/training-observability.md.")}
        </div>
      ) : (
        <div style={{ marginTop: 16, fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>
          {t("source")}: {sig.source} · {t("refresh ~12s")} · {t("MFU pending GB10 bf16 peak FLOPs.")}
        </div>
      )}
    </section>
  );
}

Object.assign(window, { TrainingSection });
