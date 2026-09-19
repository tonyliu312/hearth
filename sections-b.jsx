// AI-MONITOR · Page sections — Nodes, Models, Telemetry

const { NODES: _NODES, MODELS: _MODELS, live: _live, totals: _totals } = window.AIData;

// 有真实指标源的后端。新增引擎只改这一处 —— 之前 vllm/llamacpp/sglang 在六处
// 各写一遍长条件,oMLX 接进来时漏一处就是"有数据却不显示"。
const LIVE_METRIC_SOURCES = ["vllm", "llamacpp", "sglang", "omlx"];
const hasLiveMetrics = (m) => LIVE_METRIC_SOURCES.includes(m.metricsSource);

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
          <h2 style={{ margin: 0 }}>{t("{n} machines. ", { n: _NODES.length })}<em>{t("Each a citizen.")}</em></h2>
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

      {/* 分组代替"靠顺序暗示": 有实时吞吐数字的一组、集群成员(TP worker/无归属)一组,
          各自带标题与计数。顺序只是隐含信息, 标题才是明示(补丁 3 只排序, 机主反馈
          "视觉上太不明显", 根因是层级不是顺序)。
          ⛔ 不给卡片加彩色边框/阴影/SERVING 徽章: 装饰性强调会让整页更吵, 也与
             09-15 定的"数值统一颜色"冲突。分组靠标题与留白, 不靠颜色。
          列数按【本组】数量取 g-1~g-6, 7+ 用 g-auto; 响应式塌缩仍交给 CSS 媒体查询。 */}
      {(() => {
        const shown = _orderNodes(_NODES.filter((n) => nfilter === "All" ? true
              : nfilter === "RTX" ? (n.kind || "discrete") === "discrete"
              : nfilter === "DGX" ? n.kind === "unified-arm-soc"   // 只算 GB10;apple-silicon 不是 DGX
              : (_live.nodes[n.id] && _live.nodes[n.id].cpu.now > 0)));
        const serving = shown.filter((n) => n.throughputRole === "api");
        const members = shown.filter((n) => n.throughputRole !== "api");
        const grid = (g) => "grid " + (g.length <= 6 ? "g-" + g.length : "g-auto");
        const card = (n) => <NodeCard key={n.id} node={n} onClick={() => setActive(n)} />;
        return (
          <>
            {serving.length > 0 && <>
              <NodeGroupHead label={t("Serving")} count={serving.length} first />
              <div className={grid(serving)}>{serving.map(card)}</div>
            </>}
            {members.length > 0 && <>
              <NodeGroupHead label={t("Cluster members")} count={members.length} />
              <NodeCompactList nodes={members} onPick={setActive} />
            </>}
          </>
        );
      })()}

      {active && <NodeDetail node={active} onClose={() => setActive(null)} />}
    </section>
  );
}

// 一台节点上跑的模型: 主行是官方主名, 变体/量化用次级灰跟在后面, 完整的
// 后端自报名放 title。⛔ 三者都不许丢 —— 主名去掉的是思考档位后缀(那不是模型
// 身份), 变体说的是"实际加载的是哪份权重", 截掉它就是又一次"看着正常但不是真相"。
function _modelLine(models) {
  const arr = (models || []).filter(Boolean);
  if (!arr.length) return null;
  return arr.map((m, i) => (
    <React.Fragment key={(m.served || m.name) + i}>
      {i ? " · " : ""}
      {m.name}
      {m.variant ? <span style={{ color: "var(--ink-4)" }}>{" · "}{m.variant}</span> : null}
    </React.Fragment>
  ));
}

function _servedTitle(models) {
  return (models || []).map((m) => m.served).filter(Boolean).join(", ");
}

// 节点 id → 显示名。⛔ 界面上任何位置都不要直接渲染 id: id 是内部标识(Prometheus
// 标签 / 配置引用 / 落盘 key 都在用, 不改), 显示名可以随时改。2026-09-19 把 atlas
// 的显示名改成 GPU-HOST 后, 只有这台会露馅 —— 四台 Spark 的 id 恰好与显示名一致。
function _nodeName(id) {
  const n = _NODES.find((x) => x.id === id);
  return (n && n.name) || id;
}

// 这台此刻在不在出 token。⛔ 只用已有数据在展示层推导, 不为此改 /api/nodes:
//   实时解码/预填 > 0, 或归属到这台的模型有请求在跑(running > 0)。
//   prefill > 0 也算活跃 —— 正在 prefill 但还没吐出第一个 token 的那几秒不能算空闲。
function _nodeBusy(node) {
  if ((node.decodeTps ?? 0) > 0 || (node.prefillTokPerS ?? 0) > 0) return true;
  return _MODELS.some((m) => (m.nodes || []).includes(node.id)
    && ((_live.models[m.id] && _live.models[m.id].running.now) || 0) > 0);
}

// 分组标题。克制: 只有一行小字 + 计数, 无底色无边框, 靠留白分段。
function NodeGroupHead({ label, count, first }) {
  return (
    <div style={{ display: "flex", alignItems: "baseline", gap: 8,
                  margin: first ? "2px 0 12px" : "34px 0 12px" }}>
      <span style={{ fontFamily: "var(--mono)", fontSize: 10.5, letterSpacing: ".12em",
                     textTransform: "uppercase", color: "var(--ink-3)" }}>{label}</span>
      <span className="num" style={{ fontSize: 10.5, color: "var(--ink-4)" }}>· {count}</span>
    </div>
  );
}

// 卡片顺序: 有实时吞吐【数字】的排前面, 只有归属说明的(TP worker)排后面。
// ⛔ 判据是【能力】不是【数值】: 空闲时 decode 是 0 也算有数字, 仍排前面。
//    按当前数值排会让卡片随负载跳来跳去, 没法看。
// ⛔ 档内不再引入第二层排序, 保持配置顺序 —— 用两个数组拼接而不是 sort,
//    显式稳定, 不依赖引擎的 sort 稳定性。
// ⛔ 只改展示顺序: hearth.yaml 的 nodes 顺序表达拓扑, /api/nodes 的返回顺序也不动。
// 首帧(models 还没到)所有节点都没有 throughputRole → 保持配置顺序, 数据到了重排一次。
function _orderNodes(list) {
  const withNumbers = [], rest = [];
  list.forEach((n) => (n.throughputRole === "api" ? withNumbers : rest).push(n));
  return withNumbers.concat(rest);
}

// 次要内容退后, 不是把主要内容加徽章: TP 成员收成紧凑列表 —— 去环形图、去七行
// 规格表、字号降一级、整体降不透明度。⛔ 但仍然【可点进详情】, 完整规格在详情里
// 一行不少 —— 收起来不等于拿掉。
// ⛔ 单位只在表头出现一次(GPU % / TEMP °C), 行内只放数字。
// ⛔ 列宽写死不用 repeat(): styles.css:891 的窄屏规则会把含 repeat( 的内联 grid 压成单列。
function NodeCompactList({ nodes, onPick }) {
  const { t } = useLang();
  const COLS = "minmax(0,1.1fr) minmax(0,1.5fr) 4.5em 4.5em";
  const head = { fontFamily: "var(--mono)", fontSize: 9.5, letterSpacing: ".1em",
                 textTransform: "uppercase", color: "var(--ink-4)" };
  return (
    <div style={{ border: "0.5px solid var(--line)", borderRadius: "var(--r-md)",
                  background: "var(--bg-1)", overflow: "hidden", opacity: 0.78 }}>
      <div style={{ display: "grid", gridTemplateColumns: COLS, gap: "0 14px",
                    padding: "9px 16px", borderBottom: "0.5px solid var(--line)" }}>
        <div style={head}>{t("Unit")}</div>
        <div style={head}>{t("Belongs to")}</div>
        <div style={{ ...head, textAlign: "right" }}>{t("GPU %")}</div>
        <div style={{ ...head, textAlign: "right" }}>{t("Temp °C")}</div>
      </div>
      {nodes.map((n, i) => {
        const ns = _live.nodes[n.id] || {};
        const noTel = n.gpuTelemetry === false;
        return (
          <div key={n.id} onClick={() => onPick(n)}
               title={[_servedTitle(n.throughputModels), t("open the full forensic view")]
                      .filter(Boolean).join(" · ")}
               style={{ display: "grid", gridTemplateColumns: COLS, gap: "0 14px",
                        padding: "10px 16px", cursor: "pointer", alignItems: "baseline",
                        borderTop: i ? "0.5px solid var(--line)" : "none" }}>
            <div style={{ fontSize: 12, color: "var(--ink-2)", overflow: "hidden",
                          textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {n.name}
              <span style={{ marginLeft: 8, fontFamily: "var(--mono)", fontSize: 10,
                             color: "var(--ink-4)" }}>{n.ip}</span>
            </div>
            <div style={{ fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)",
                          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {n.throughputRole === "worker"
                ? <>{t("worker")} · {_modelLine(n.throughputModels)}</>
                : "—"}
            </div>
            <div className="num" style={{ fontSize: 11.5, color: "var(--ink-2)", textAlign: "right" }}>
              {ns.gpu ? ns.gpu.now.toFixed(0) : "—"}
            </div>
            <div className="num" style={{ fontSize: 11.5, color: "var(--ink-2)", textAlign: "right" }}>
              {noTel || !ns.tempGpu ? "—" : ns.tempGpu.now.toFixed(0)}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function NodeCard({ node, onClick }) {
  const { t } = useLang();
  const ns = _live.nodes[node.id];
  const isHost = /gateway/i.test(node.role || "");
  const offline = ns.up === false;       // 后端权威: 真实离线状态(不再写死 ONLINE)
  return (
    <article className="node" data-offline={offline ? "1" : "0"} onClick={onClick}
             style={offline ? { opacity: 0.55 } : undefined}>
      <div className="node-head">
        <div>
          <div className="node-class">{node.class}</div>
          <div className="node-name">{node.name}</div>
          {/* 设备名下面一行真实的后端模型名(官方主名, 不带思考档位后缀)。
              层级: 设备名是主、模型名是次、IP 最次 —— 不与英雄数字抢注意力,
              不加颜色不加徽章。 */}
          {(node.throughputModels || []).length > 0 && (
            <div title={_servedTitle(node.throughputModels)}
                 style={{ marginTop: 3, fontSize: 11, color: "var(--ink-2)", maxWidth: 215,
                          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {_modelLine(node.throughputModels)}
            </div>
          )}
          <div className="node-ip">{node.ip}</div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div className="node-status" style={offline ? { color: "var(--ink-3)" } : undefined}>
            <span className={"dot" + (offline ? " bad" : "")} />{offline ? t("OFFLINE") : t("ONLINE")}
          </div>
          <div style={{ marginTop: 6 }}>
            <span className={"chip " + (offline ? "ghost" : isHost ? "accent" : "violet")} style={{ fontSize: 9.5 }}>
              {isHost ? "GATEWAY" : node.role.split(" ")[2] || "NODE"}
            </span>
          </div>
        </div>
      </div>

      {/* 英雄数字: 这张卡最该被一眼看到的量。规格表(GPU/VRAM/CPU/Net/功耗/温度)
          降到下面 —— 补丁 3 之前 Decode 是规格表的最后一行, 与 GPU 温度同级,
          排到第一张也还是第七行小字, 主次是反的。
          ⛔ 单位只在标签行出现一次(09-15 机主定的规矩), prefill 副行不再重复。
          ⛔ 等宽 tabular figures + 固定行高: 数值跳动时布局不抖。
          ⛔ 空闲显示 0.0 不隐藏: 判据是能力不是数值(与补丁 3 一致)。 */}
      {node.throughputRole === "api" && (() => {
        // 活跃/空闲两档。⛔ 这【不是】按数值给颜色(09-15 那条规矩禁的是按大小/阈值
        //    上彩虹色), 而是一个状态轴上的两档: 在出 token vs 没在出。
        //    所以只有两档、只有两种颜色、不随 tok/s 高低做任何渐变。
        // ⛔ 脉冲点只在真的在动时出现, 但【占位始终保留】(visibility 而不是条件渲染),
        //    否则状态切换时数字会横向跳。
        const busy = _nodeBusy(node);
        const inkNum = busy ? "var(--ink)" : "var(--ink-3)";
        const inkSub = busy ? "var(--ink-2)" : "var(--ink-4)";
        return (
          <div style={{ margin: "14px 0 4px" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span className="num" style={{ fontSize: 30, lineHeight: "34px", fontWeight: 500,
                                             letterSpacing: "-.02em", color: inkNum }}>
                {(node.decodeTps ?? 0).toFixed(1)}
              </span>
              <span title={busy ? t("emitting tokens now") : ""}
                    style={{ width: 6, height: 6, borderRadius: "50%", flex: "0 0 auto",
                             background: "var(--ink)", visibility: busy ? "visible" : "hidden",
                             animation: "pulse 1.6s ease-in-out infinite" }} />
            </div>
            <div style={{ marginTop: 5, fontFamily: "var(--mono)", fontSize: 10,
                          letterSpacing: ".1em", textTransform: "uppercase", color: "var(--ink-3)" }}>
              {t("tok/s · decode")}
            </div>
            <div className="num" style={{ marginTop: 6, fontSize: 11, color: inkSub }}
                 title={node.prefillTokPerS == null ? t("this backend exposes no realtime prefill counter") : ""}>
              <span style={{ letterSpacing: ".1em", textTransform: "uppercase" }}>{t("prefill")}</span>
              {"  "}
              {node.prefillTokPerS == null ? "—" : node.prefillTokPerS.toLocaleString("en-US")}
            </div>
          </div>
        );
      })()}

      <div className="node-rings">
        <div>
          {node.gpuPending
            ? <div style={{ width: 56, height: 56, display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "var(--mono)", fontSize: 8, color: "var(--ink-3)", textAlign: "center", lineHeight: 1.35 }}>GPU<br />{t("maint. pending")}</div>
            : <Ring value={ns.gpu.now} size={56} stroke={5} sub="GPU" />}
          <div className="rl">GPU</div>
        </div>
        <div>
          <Ring value={ns.vram.now} size={56} stroke={5} sub="VRAM" />
          <div className="rl">VRAM</div>
        </div>
        <div>
          <Ring value={ns.cpu.now} size={56} stroke={5} sub="CPU" />
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
        {/* 没有 GPU 遥测源(node.gpuTelemetry === false)时显示「—」:0 W / 0 °C 会被读成真实读数 */}
        <div className="k">{t("Power")}</div><div className="v num">{node.gpuPending ? t("GPU pending · maintenance window") : node.gpuTelemetry === false ? "—" : ns.power.now.toFixed(0) + " W"}</div>
        <div className="k">{t("GPU temp")}</div><div className="v num" style={{ color: !node.gpuPending && node.gpuTelemetry !== false && ns.tempGpu.now > 80 ? "var(--hot)" : "var(--ink)" }}>{node.gpuPending || node.gpuTelemetry === false ? "—" : ns.tempGpu.now.toFixed(0) + " °C"}</div>
        {/* 实时吞吐(2026-09-19 机主要求放到卡片上)。口径与首屏那块同源:
            墙钟负载, 空闲即 0, 无源显示「—」。
            ⛔ TP 组只有对外提供 API 的那台显示数字(后端按 /v1/models 实测判定),
               其余成员显示归属 —— 四张卡各写一遍同一个数会被读成四倍。
            ⛔ 没有模型归属的节点整两行不渲染: 那种情况下 0 是假数, 不是"空闲"。 */}
        {/* api 节点的吞吐已提到卡片顶部当英雄数字, 这里不再重复一遍。
            worker 只有归属说明, 仍留在规格表里(它不是"数字"这一档)。 */}
        {node.throughputRole === "worker" ? <>
          <div className="k">{t("Throughput")}</div>
          <div className="v" style={{ color: "var(--ink-3)" }}
               title={t("this node is a TP/PP member; the whole group produces one throughput figure, shown on the node that serves the API")}>
            {t("worker")} · {_modelLine(node.throughputModels)}
          </div>
        </> : null}
      </div>
    </article>
  );
}

// 秒 → 人话时长。开机时长以天/小时为主，分钟只在不足一小时时才有意义。
function fmtUptime(sec) {
  const d = Math.floor(sec / 86400), h = Math.floor((sec % 86400) / 3600),
        m = Math.floor((sec % 3600) / 60);
  return d ? `${d}d ${h}h` : h ? `${h}h ${m}m` : `${m}m`;
}

function NodeDetail({ node, onClose }) {
  const { t } = useLang();
  const ns = _live.nodes[node.id];
  // 后端 facts: 逐挂载点存储 / 逐网卡 / 磁盘 IO / 开机时长 / GPU 健康计数。
  // 全部来自已有的 node_exporter 与 DCGM 序列；节点没有 obs 覆盖时为空。
  const facts = node.facts || {};
  const gh = facts.gpuHealth || {};
  const nicTemp = (_live.nodeMeta?.[node.id]?.temps || []).find((x) => x.module === "网卡" || x.module === "NIC")?.celsius || 0;
  const hostedModels = _MODELS.filter((m) => m.nodes.includes(node.id));
  return (
    <div className="card" style={{ marginTop: 22 }}>
      <div className="card-head">
        <div>
          <div className="card-title" style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span className="num" style={{ color: "var(--ink-3)", fontSize: 12, fontWeight: 400, fontFamily: "var(--mono)" }}>$ ssh root@{node.ip}</span>
            <span>{node.name} · {t("forensic view")}</span>
          </div>
          <div className="card-sub">{node.kind === "apple-silicon"
            ? <>{node.gpu.name} · macOS · Metal</>
            : <>{node.gpu.name} · {node.os} · kernel {node.kernel} · NVIDIA {node.driver.split(" ")[1]} · CUDA {node.cuda}</>}</div>
        </div>
        <button onClick={onClose} style={{
          appearance: "none", border: 0, background: "rgba(255,255,255,.04)",
          color: "var(--ink-2)", borderRadius: 7, padding: "5px 10px",
          fontFamily: "var(--mono)", fontSize: 11, cursor: "pointer",
        }}>Close ✕</button>
      </div>

      <div className="nd-grid nd-grid-top" style={{ padding: 22 }}>
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
          {/* ioreg 口径量的是整个 GPU 设备(桌面合成也算), 不是推理独占 —— 必须标注,
              否则 idle 时的 50% 底噪会被读成「模型在忙」。实测见 i18n 注释。 */}
          <DetailMetric label={t("GPU")}            value={ns.gpu.now.toFixed(0)} unit="%" bar={ns.gpu.now}
                        note={node.gpuUtilSource === "ioreg" ? t("whole device · incl. display") : null} />
          <DetailMetric label={t("VRAM")}           value={(node.gpu.mem * ns.vram.now / 100).toFixed(1)} unit={` / ${node.gpu.mem} GB`} bar={ns.vram.now} color="violet"
                        note={node.vramKind === "unified" ? t("GPU-allocated · unified") : null} />
          {node.gpuTelemetry !== false && (
            <DetailMetric label={t("GPU temp")}     value={ns.tempGpu.now.toFixed(0)} unit=" °C" bar={ns.tempGpu.now} color={ns.tempGpu.now > 80 ? "hot" : "ok"} />
          )}
          {nicTemp > 0 && (
            <DetailMetric label={t("NIC temp")}     value={nicTemp.toFixed(0)} unit=" °C" bar={Math.min(100, nicTemp)} color={nicTemp > 85 ? "hot" : nicTemp > 70 ? "warn" : "ok"} />
          )}
          {node.gpuTelemetry !== false && (
            <DetailMetric label={t("Power draw")}   value={ns.power.now.toFixed(0)} unit=" W"  bar={Math.min(100, ns.power.now / (/gateway/i.test(node.role || "") ? 4.5 : 2.5))} color="hot" />
          )}
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <DetailMetric label={t("CPU")}            value={ns.cpu.now.toFixed(0)} unit="%" bar={ns.cpu.now} color="teal" />
          <DetailMetric label={t("RAM")}            value={(node.ram * ns.mem.now / 100).toFixed(0)} unit={` / ${node.ram} GB`} bar={ns.mem.now} color="violet" />
          <DetailMetric label={t("Disk usage")}     value={(node.disk * ns.disk.now / 100 / 1024).toFixed(1)} unit={` / ${(node.disk/1024).toFixed(0)} TB`} bar={ns.disk.now} color="ok" />
          <DetailMetric label={t("Net In")}         value={(ns.netIn.now).toFixed(0)} unit=" MB/s" bar={Math.min(100, ns.netIn.now / 12)} color="accent" />
          <DetailMetric label={t("Net Out")}        value={(ns.netOut.now).toFixed(0)} unit=" MB/s" bar={Math.min(100, ns.netOut.now / 12)} color="accent" />
        </div>
      </div>

      <div className="nd-grid nd-grid-bottom" style={{ borderTop: "0.5px solid var(--line)", padding: 22 }}>
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
            {/* apple-silicon 没有 NVIDIA 驱动 / CUDA:别显示「NVIDIA —」这种错类目 */}
            <span style={{ color: "var(--ink-3)" }}>{t("Driver")}</span><span>{node.kind === "apple-silicon" ? "Metal" : node.driver}</span>
            {node.kind !== "apple-silicon" && <>
              <span style={{ color: "var(--ink-3)" }}>CUDA</span><span>{node.cuda}</span>
            </>}
            <span style={{ color: "var(--ink-3)" }}>{t("Net")}</span><span>{node.net}</span>
            {/* 以下三项来自后端 facts(现有 Prometheus 序列, 被监控机零新增命令)。
                没有 obs 覆盖的节点(如经隧道直采的 MBP)整块缺席, 不显示占位 0。 */}
            {facts.uptimeSec !== undefined ? <>
              <span style={{ color: "var(--ink-3)" }}>{t("Uptime")}</span>
              <span>{fmtUptime(facts.uptimeSec)}</span>
            </> : null}
            {gh.xid !== undefined ? <>
              <span style={{ color: "var(--ink-3)" }}>XID</span>
              <span style={{ color: gh.xid ? "var(--bad)" : "var(--ink)" }}>
                {gh.xid}{gh.xid && gh.xidMsg ? ` · ${gh.xidMsg}` : ""}</span>
              {/* ECC 计数器缺席 = 该 GPU 没有 ECC(GB10 的 LPDDR5X 就没有),
                  不是"0 个错误"。2026-09-19 起采集端不再伪造 0, 这里如实写 n/a。 */}
              <span style={{ color: "var(--ink-3)" }}>ECC</span>
              {(gh.eccSbe !== undefined || gh.eccDbe !== undefined) ? (
                <span style={{ color: gh.eccDbe ? "var(--bad)" : gh.eccSbe ? "var(--hot)" : "var(--ink)" }}>
                  {gh.eccSbe ?? "—"} {t("correctable")} / {gh.eccDbe ?? "—"} {t("uncorrectable")}</span>
              ) : (
                <span style={{ color: "var(--ink-3)" }} title={t("this GPU exposes no ECC counters")}>
                  {t("n/a · no ECC counters")}</span>
              )}
            </> : null}
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

      {/* 逐挂载点存储 / 逐网卡链路 / 磁盘 IO。数据来自已有的 node_exporter 序列
          (node_filesystem_* / node_network_* / node_disk_*)，被监控机上零新增命令。
          节点没有这些序列(如经隧道直采的 MBP)时整块不渲染。 */}
      {(facts.mounts || []).length || (facts.nics || []).length ? (
        <div style={{ borderTop: "0.5px solid var(--line)", padding: 22 }}>
          <div className="metric-l" style={{ marginBottom: 12 }}>{t("Storage & network")}</div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
                        gap: 22, fontFamily: "var(--mono)", fontSize: 11 }}>
            {(facts.mounts || []).length ? (
              <div>
                <div style={{ color: "var(--ink-3)", fontSize: 10, marginBottom: 6 }}>{t("Filesystems")}</div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr auto auto", gap: "4px 10px", maxWidth: 340 }}>
                  {facts.mounts.map((m) => (
                    <div key={m.mount} style={{ display: "contents" }}>
                      <span style={{ color: "var(--ink-2)", overflow: "hidden", textOverflow: "ellipsis" }}
                            title={m.device}>{m.mount}</span>
                      <span style={{ color: "var(--ink-3)", textAlign: "right" }}>
                        {m.availGb >= 1024 ? `${(m.availGb / 1024).toFixed(1)}T` : `${m.availGb.toFixed(0)}G`} {t("free")}</span>
                      {/* 85% 起标热色:与后端 disk 告警同一阈值 */}
                      <span style={{ textAlign: "right",
                                     color: m.usedPct >= 85 ? "var(--hot)" : "var(--ink)" }}>{m.usedPct}%</span>
                    </div>
                  ))}
                </div>
                {(facts.disks || []).length ? (
                  <div style={{ marginTop: 10 }}>
                    <div style={{ color: "var(--ink-3)", fontSize: 10, marginBottom: 6 }}>{t("Disk I/O")}</div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr auto auto", gap: "4px 10px", maxWidth: 340 }}>
                      {facts.disks.map((d) => (
                        <div key={d.device} style={{ display: "contents" }}>
                          <span style={{ color: "var(--ink-2)" }}>{d.device}</span>
                          <span style={{ color: "var(--ink-3)", textAlign: "right" }}>R {d.readMBs}</span>
                          <span style={{ color: "var(--ink-3)", textAlign: "right" }}>W {d.writeMBs} MB/s</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}
              </div>
            ) : null}
            {(facts.nics || []).length ? (
              <div>
                <div style={{ color: "var(--ink-3)", fontSize: 10, marginBottom: 6 }}>{t("Interfaces")}</div>
                <div style={{ display: "grid", gridTemplateColumns: "auto 1fr auto", gap: "4px 10px", maxWidth: 340 }}>
                  {facts.nics.map((c) => (
                    <div key={c.name} style={{ display: "contents" }}>
                      <span style={{ color: c.state === "up" ? "var(--ink-2)" : "var(--ink-3)" }}>{c.name}</span>
                      <span style={{ color: "var(--ink-3)" }}>{c.mac}</span>
                      {/* 链路速率 0 = 网卡 down 或驱动不报, 显示「—」不显示 0 Mbps */}
                      <span style={{ textAlign: "right", color: c.state === "up" ? "var(--ink)" : "var(--ink-3)" }}>
                        {c.speedMbps >= 1000 ? `${c.speedMbps / 1000}G` : c.speedMbps > 0 ? `${c.speedMbps}M` : "—"}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}

      <div style={{ borderTop: "0.5px solid var(--line)", padding: 22 }}>
        <div className="metric-l" style={{ marginBottom: 12 }}>{t("Hardware sensors")}</div>
        <SensorPanel nodeId={node.id} />
      </div>
    </div>
  );
}

// 按硬件模块分组的可展开传感器面板（温度各组 + 风扇组）。
// 后端 live.{temps,fans} 已全量直采；此处只负责"应看尽看"的分组呈现。
function SensorPanel({ nodeId }) {
  const { t } = useLang();
  const meta = _live.nodeMeta[nodeId] || {};
  const temps = meta.temps || [];
  const fans = meta.fans || [];
  const [open, setOpen] = useState({});
  if (!temps.length && !fans.length)
    return <div style={{ fontFamily: "var(--mono)", fontSize: 11.5, color: "var(--ink-3)" }}>{t("— no temperature data (node unmanaged) —")}</div>;

  const tc = (c) => (c >= 80 ? "var(--hot)" : c >= 70 ? "var(--warn)" : "var(--ink)");
  const moreRow = (n) => ({ label: t("+{n} more").replace("{n}", n), value: "", color: "var(--ink-3)" });
  const order = ["CPU", "水冷", "网卡", "NVMe", "SoC", "平台", "其他"];
  const rank = (m) => { const i = order.indexOf(m); return i < 0 ? 99 : i; };

  const groups = {};
  temps.forEach((x) => { (groups[x.module] = groups[x.module] || []).push(x); });
  const cards = Object.keys(groups).sort((a, b) => rank(a) - rank(b)).map((mod) => {
    const list = groups[mod];                         // already hottest-first (global desc sort)
    const expanded = !!open[mod];
    const rows = (expanded ? list : list.slice(0, 3)).map((x) => ({
      label: x.label, value: `${Math.round(x.celsius)} °C`, color: tc(x.celsius),
    }));
    if (!expanded && list.length > 3) rows.push(moreRow(list.length - 3));
    return <SensorGroup key={mod} title={mod} rep={`${Math.round(list[0].celsius)} °C`}
      repColor={tc(list[0].celsius)} rows={rows} expandable={list.length > 3}
      expanded={expanded} onToggle={() => setOpen((o) => ({ ...o, [mod]: !o[mod] }))} />;
  });

  if (fans.length) {
    const expanded = !!open.__fans;
    const repRpm = Math.max(...fans.map((f) => f.rpm));
    const rows = (expanded ? fans : fans.slice(0, 3)).map((f) => ({
      label: f.label, value: f.rpm > 0 ? `${f.rpm} RPM` : t("stopped"),
      color: f.rpm > 0 ? "var(--ink)" : "var(--ink-3)",
    }));
    if (!expanded && fans.length > 3) rows.push(moreRow(fans.length - 3));
    cards.push(<SensorGroup key="__fans" title={t("Fans")} rep={`${repRpm} RPM`} repColor="var(--ink)"
      rows={rows} expandable={fans.length > 3} expanded={expanded}
      onToggle={() => setOpen((o) => ({ ...o, __fans: !o.__fans }))} />);
  }

  return <div className="sensor-grid">{cards}</div>;
}

function SensorGroup({ title, rep, repColor, rows, expandable, expanded, onToggle }) {
  return (
    <div className="sensor-card">
      <button type="button" className="sensor-card-head" onClick={expandable ? onToggle : undefined}
        style={{ cursor: expandable ? "pointer" : "default" }}>
        <span className="sensor-card-title">{title}</span>
        <span className="sensor-card-rep num" style={{ color: repColor }}>{rep}</span>
        {expandable && <span className="sensor-chev" style={{ transform: expanded ? "rotate(90deg)" : "none" }}>›</span>}
      </button>
      <div className="sensor-rows">
        {rows.map((r, i) => (
          <div className="sensor-row" key={i}>
            <span className="sensor-row-l">{r.label}</span>
            <span className="num" style={{ color: r.color }}>{r.value}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function DetailMetric({ label, value, unit, bar, color = "accent", note }) {
  return (
    <div className="metric">
      <div className="metric-h">
        <div className="metric-l">{label}{note ? <span style={{ color: "var(--ink-3)", fontWeight: 400, marginLeft: 6, textTransform: "none", letterSpacing: 0 }}>{note}</span> : null}</div>
        <div className="metric-v num">{value}<small>{unit}</small></div>
      </div>
      <div className={"bar " + color}><i style={{ width: `${Math.max(0, Math.min(100, bar))}%` }} /></div>
    </div>
  );
}

// 一行「均值 + p50/p90/p99」。分位数是业界标准口径(vllm bench serve /
// GenAI-Perf / LLMPerf)，均值同排列出来做对照：样本量小时分位数会偏高，
// 两者背离大时以均值为准。
// 单位只在表头左上角标一次(PctGrid 的 unit)，格子里只放带千分位的毫秒整数。
// ⛔ 因此【不许】再对大值自动换算成秒：表头写 ms、某格却是秒，正是 de26f43 修掉的
//    那种「读的人无从判断单位」。13.9s 就写 13,900。
// ⛔ 也别把单位塞回每一格、别给四列不同亮度：2026-09-15 用户两次反馈「乱」，主因
//    就是逐格 ms + mean 灰 / p50 白 / p99 灰。定稿是用户从三个预览里选的
//    「单位进表头、统一颜色」。只有缺值「—」用最淡色，超过 warn 的 p99 标热色。
function PctRow({ label, mean, p50, p90, p99, warn }) {
  const f = (v) => (v === undefined || v === null)
    ? <span style={{ color: "var(--ink-3)" }}>—</span>
    : Math.round(v).toLocaleString("en-US");
  const num = { textAlign: "right", color: "var(--ink-2)" };
  return (
    <div style={{ display: "contents" }}>
      <span style={{ color: "var(--ink-3)" }}>{label}</span>
      <span style={num}>{f(mean)}</span>
      <span style={num}>{f(p50)}</span>
      <span style={num}>{f(p90)}</span>
      <span style={{ ...num, color: warn && p99 > warn ? "var(--hot)" : num.color }}>{f(p99)}</span>
    </div>
  );
}

// PctRow 的表格外壳(含表头，左上角是单位)。列宽固定而不是 1fr：面板很宽时 1fr 会把
// 四列拉开到一眼扫不过去，数字之间隔着大片空白，看起来像散落的点而不是一张表。
// ⛔ 列宽必须逐列写出，不能写 repeat(4, …)：styles.css 手机断点(≤640px)里有
//    [style*="repeat("] { grid-template-columns: 1fr !important }，本意是把卡片网格
//    压成单列，会误伤这张数字表(2026-09-15 在 420px 宽实测整表错位)。
// minmax(4.4em, max-content)：7 位以内(107,500)放得下，360px 宽手机上整表仍放得下；
// 更长的值只撑宽本列，不会压到邻列上。
function PctGrid({ children, unit = "ms" }) {
  const { t } = useLang();
  const h = { color: "var(--ink-3)", fontSize: 10, textAlign: "right" };
  const col = "minmax(4.4em, max-content)";
  return (
    <div style={{ display: "grid", gridTemplateColumns: `auto ${col} ${col} ${col} ${col}`,
                  justifyContent: "start", gap: "5px 12px", fontFamily: "var(--mono)",
                  fontSize: 11, fontVariantNumeric: "tabular-nums" }}>
      <span style={{ color: "var(--ink-3)", fontSize: 10 }}>{unit}</span>
      <span style={h}>{t("mean")}</span>
      <span style={h}>p50</span>
      <span style={h}>p90</span>
      <span style={h}>p99</span>
      {children}
    </div>
  );
}

// 投机解码：接受率决定吞吐。实测同一引擎五种负载，每步耗时全在 76-80ms 只差
// 5.5%，而端到端吞吐 38→102 tok/s 差 168%，差异几乎全部来自接受率。
function SpecDecode({ spec }) {
  const { t } = useLang();
  const pos = Array.isArray(spec.perPos) ? spec.perPos : [];
  return (
    <div>
      <div className="metric-l" style={{ marginBottom: 8 }}>
        {/* specLen 是每位置数组的长度 = 草稿长度【上限】(llama.cpp 固定 64),不是典型值。
            并排给出实测平均(草稿 token / 草稿步数),否则会把 64 读成"每步草稿 64 个"。 */}
        {t("Speculative decoding")} · {t("draft len")} {spec.specLen}
        {spec.drafts > 0 ? <span style={{ color: "var(--ink-3)", textTransform: "none" }}>
          {" · "}{t("avg")} {(spec.draftTokens / spec.drafts).toFixed(1)}</span> : null}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "auto 1fr",
                    gap: "5px 12px", fontFamily: "var(--mono)", fontSize: 11.5 }}>
        <span style={{ color: "var(--ink-3)" }}>{t("Acceptance rate")}</span>
        <span><b style={{ color: "var(--ink)" }}>{spec.acceptRate}%</b>
          <small style={{ color: "var(--ink-3)" }}> {t("lifetime")}</small>
          {spec.acceptRateNow !== null && spec.acceptRateNow !== undefined
            ? <> · <b style={{ color: "var(--ink)" }}>{spec.acceptRateNow}%</b>
                <small style={{ color: "var(--ink-3)" }}> {t("current window")}</small></>
            : null}
        </span>
        <span style={{ color: "var(--ink-3)" }}>{t("Tokens / step")}</span>
        <span style={{ color: "var(--ink)" }}>{spec.tokensPerStep}</span>
        <span style={{ color: "var(--ink-3)" }}>{t("Drafts")}</span>
        <span>{spec.drafts.toLocaleString()}</span>
        <span style={{ color: "var(--ink-3)" }}>{t("Accepted")}</span>
        <span>{spec.accepted.toLocaleString()} / {spec.draftTokens.toLocaleString()}</span>
      </div>
      {pos.length ? <>
        <div className="metric-l" style={{ margin: "12px 0 6px" }}>{t("Acceptance by draft position")}</div>
        {/* 只画前 12 个位置:llama.cpp 的每位置数组固定 64 长(--draft-max),实测位置 0-2
            占绝大多数、之后长尾极小,64 根柱子读不出信息。数据不裁剪,只裁显示。 */}
        <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          {pos.slice(0, 12).map((v, i) => (
            <div key={i} style={{ display: "grid", gridTemplateColumns: "18px 1fr 42px",
                                  alignItems: "center", gap: 8,
                                  fontFamily: "var(--mono)", fontSize: 10.5 }}>
              <span style={{ color: "var(--ink-3)" }}>{i}</span>
              <span style={{ height: 6, background: "var(--line)", borderRadius: 3, overflow: "hidden" }}>
                <i style={{ display: "block", height: "100%", width: `${Math.max(0, Math.min(100, v))}%`,
                            background: "var(--accent)" }} />
              </span>
              <span style={{ color: "var(--ink-2)", textAlign: "right" }}>{v}%</span>
            </div>
          ))}
          {pos.length > 12 ? (
            <div style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)", marginTop: 2 }}>
              {t("+{n} more").replace("{n}", pos.length - 12)}
            </div>
          ) : null}
        </div>
      </> : null}
    </div>
  );
}

// SLO 达标率。刻意不叫 goodput —— 那个词业界特指「同时满足全部 SLO 的请求
// 占比」，是每请求的联合条件；聚合直方图只能给边缘分布。联合值用
// Fréchet-Hoeffding 边界给严格区间，不做任何独立性假设。
function SloBlock({ model }) {
  const { t } = useLang();
  return (
    <div style={{ marginTop: 14 }}>
      <div className="metric-l" style={{ marginBottom: 6 }}>
        {t("SLO attainment")} · TTFT ≤ {model.sloTtftMs}ms · TPOT ≤ {model.sloTpotMs}ms
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "auto 1fr",
                    gap: "4px 12px", fontFamily: "var(--mono)", fontSize: 11.5 }}>
        <span style={{ color: "var(--ink-3)" }}>TTFT</span>
        <span style={{ color: "var(--ink)" }}>{model.sloTtftRate}%</span>
        <span style={{ color: "var(--ink-3)" }}>TPOT</span>
        <span style={{ color: "var(--ink)" }}>{model.sloTpotRate}%</span>
        <span style={{ color: "var(--ink-3)" }}>{t("both")}</span>
        <span>
          <b style={{ color: model.sloJointWide ? "var(--ink-2)" : "var(--ink)" }}>
            {model.sloJointLower}–{model.sloJointUpper}%
          </b>
          {model.sloJointWide
            ? <small style={{ color: "var(--hot)" }}> {t("wide range · indicative only")}</small>
            : null}
        </span>
      </div>
    </div>
  );
}

// 饱和提示：排队占了 TTFT 的大头 + 持续有请求在等 → 再加负载不划算。
function SatBlock({ model }) {
  const { t } = useLang();
  return (
    <div style={{ marginTop: 12, fontFamily: "var(--mono)", fontSize: 11 }}>
      <span style={{
        display: "inline-block", padding: "2px 8px", borderRadius: 4, fontSize: 10,
        background: model.saturated ? "var(--hot)" : "var(--line)",
        color: model.saturated ? "#000" : "var(--ink-3)",
      }}>
        {model.saturated ? t("SATURATED · adding load costs latency, not throughput")
                         : t("headroom available")}
      </span>
      <span style={{ color: "var(--ink-3)", marginLeft: 8 }}>
        {t("queue is")} {model.queueShareP90}% {t("of TTFT p90")}
        {model.waitingCapacity > 0 ? ` · ${model.waitingCapacity} ${t("waiting on capacity")}` : ""}
      </span>
    </div>
  );
}

// MBU / MFU。回答「还有多少余量」——瓶颈在带宽还是在别处。
function EffBlock({ model }) {
  const { t } = useLang();
  return (
    <div>
      <div className="metric-l" style={{ marginBottom: 6 }}>{t("Efficiency")}</div>
      <div style={{ display: "grid", gridTemplateColumns: "auto 1fr",
                    gap: "4px 12px", fontFamily: "var(--mono)", fontSize: 11.5 }}>
        {model.mbu !== undefined ? <>
          <span style={{ color: "var(--ink-3)" }}>{t("MBU (bandwidth)")}</span>
          <span style={{ color: "var(--ink)" }}>{model.mbu}%</span>
        </> : null}
        {model.mfu !== undefined ? <>
          <span style={{ color: "var(--ink-3)" }}>{t("MFU (compute)")}</span>
          <span><b style={{ color: "var(--ink)" }}>{model.mfu}%</b>
            <small style={{ color: "var(--ink-3)" }}> {t("processed")}</small>
            {" · "}{model.mfuDelivered}%
            <small style={{ color: "var(--ink-3)" }}> {t("delivered")}</small>
          </span>
        </> : null}
        {model.stepsPerSec !== undefined ? <>
          <span style={{ color: "var(--ink-3)" }}>{t("Engine steps")}</span>
          <span>{model.stepsPerSec}/s</span>
        </> : null}
      </div>
      {model.mfuDrafterMissing
        ? <div style={{ marginTop: 5, fontSize: 10, color: "var(--ink-3)",
                        fontFamily: "var(--mono)" }}>
            {t("drafter params not configured → MFU is an underestimate")}
          </div> : null}
    </div>
  );
}

// 响应构成。数据来自 LiteLLM spend logs（带外只读，不碰推理路径），不是引擎
// /metrics —— 引擎侧完全没有 thinking/正文的区分。
function ResponseBlock({ model }) {
  const { t } = useLang();
  const hasEmpty = model.emptyContentRate !== undefined;
  const hasThink = model.thinkingCharShare !== undefined;
  const hasTtfc = model.ttfcNullRate !== undefined || model.ttfcP50 !== undefined;
  if (!hasEmpty && !hasThink && !hasTtfc) return null;
  return (
    <div>
      <div className="metric-l" style={{ marginBottom: 6 }}>
        {t("Response composition")} · {t("last")} {model.spendWindowH}h
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "auto 1fr",
                    gap: "4px 12px", fontFamily: "var(--mono)", fontSize: 11.5 }}>
        {hasEmpty ? <>
          <span style={{ color: "var(--ink-3)" }}>{t("Empty answers")}</span>
          <span>
            <b style={{ color: model.emptyContentRate > 5 ? "var(--hot)" : "var(--ink)" }}>
              {model.emptyContentRate}%
            </b>
            <small style={{ color: "var(--ink-3)" }}>
              {" "}{model.emptyContentN}/{model.emptyContentTotal} · {t("baseline 0%")}
            </small>
          </span>
        </> : null}
        {model.toolCallN !== undefined ? <>
          <span style={{ color: "var(--ink-3)" }}>{t("Tool responses")}</span>
          <span style={{ color: "var(--ink-2)" }}>
            {model.toolCallN}<small style={{ color: "var(--ink-3)", marginLeft: 2 }}>{t("items")}</small>
            {" "}<small style={{ color: "var(--ink-3)" }}>{t("counted separately")}</small>
          </span>
        </> : null}
        {hasThink ? <>
          <span style={{ color: "var(--ink-3)" }}>{t("Thinking share")}</span>
          <span>
            <b style={{ color: "var(--ink)" }}>{model.thinkingCharShare}%</b>
            <small style={{ color: "var(--ink-3)" }}>
              {" "}{t("of characters")} · n={model.thinkingSampleN}
              {model.thinkingTruncatedN > 0 ? ` · ${model.thinkingTruncatedN} ${t("truncated, excluded")}` : ""}
            </small>
          </span>
        </> : null}
      </div>
      {model.ttfcP50 !== undefined || model.ttfcNullRate !== undefined ? <>
        <div className="metric-l" style={{ margin: "12px 0 6px" }}>
          {t("Time to first content")} · {t("gateway stream")} · {t("last")} {model.ttfcWindowH}h
        </div>
        {model.ttfcP50 !== undefined ? <PctGrid>
          {/* TTFT 这一行是【网关口径】，与上面延迟分解里那个 vLLM 口径的 TTFT
              不是一回事：hook 只看走网关的流量，vLLM 看全部含直连。故分别标源，
              ⛔ 不要拿两者互相校验或二选一。 */}
          <PctRow label={t("TTFT (gw)")} p50={model.ttftGwP50} p90={model.ttftGwP90} />
          <PctRow label={t("TTFC")} mean={model.ttfcMean} p50={model.ttfcP50}
                  p90={model.ttfcP90} p99={model.ttfcP99} warn={10000} />
        </PctGrid> : null}
        <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "4px 12px",
                      fontFamily: "var(--mono)", fontSize: 11.5, marginTop: 6 }}>
          {model.ttfcNullRate !== undefined ? <>
            <span style={{ color: "var(--ink-3)" }}>{t("No content at all")}</span>
            <span><b style={{ color: "var(--ink)" }}>{model.ttfcNullRate}%</b>
              <small style={{ color: "var(--ink-3)" }}>
                {" "}{model.ttfcNullN}/{model.ttfcTotalN} · {t("tool calls, expected")}
              </small></span>
          </> : null}
          {model.thinkChunksP50 !== undefined ? <>
            <span style={{ color: "var(--ink-3)" }}>{t("Think chunks first")}</span>
            <span>{model.thinkChunksP50}<small style={{ color: "var(--ink-3)", marginLeft: 2 }}>
              {t("chunks")}</small>{" "}<small style={{ color: "var(--ink-3)" }}>
              {t("median, before first content")}</small></span>
          </> : null}
        </div>
      </> : null}
      {/* ⛔ 判据写在界面上是刻意的：区分「文本响应」与「工具响应」必须用
          tool_calls 是不是数组，不能用 finish_reason —— 该模型发 tool_calls 时
          finish_reason 仍是 'stop'，按它过滤会把工具响应算进文本组，空正文率
          会虚高到 35.6%（那些正文本来就该空）。后人若照 finish_reason 改回去，
          这行字是唯一的拦阻。 */}
      <div style={{ marginTop: 5, fontSize: 10, color: "var(--ink-3)",
                    fontFamily: "var(--mono)", lineHeight: 1.6 }}>
        {t("text vs tool split by tool_calls array, not finish_reason")}
        {hasThink ? <><br />{t("character share, not tokens · truncated responses excluded")}</> : null}
        {model.ttfcP50 !== undefined
          ? <><br />{t("TTFC from gateway stream hook · different population than vLLM TTFT above")}</>
          : null}
      </div>
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
        <div className="gw-strip" style={{ padding: 22, alignItems: "center" }}>
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
          <GatewayStat label={t("Catalog models")} value={_MODELS.length} sub={`${_MODELS.filter(hasLiveMetrics).length} ${t("with live metrics")}`} />
          <GatewayStat label={t("Live metric sources")} value={_MODELS.filter(hasLiveMetrics).length} sub={t("vLLM · llama.cpp · SGLang · oMLX")} />
          <GatewayStat label={t("Live throughput")} value={Math.round(_MODELS.filter(hasLiveMetrics).reduce((a,m)=>a+(_live.models[m.id]?_live.models[m.id].tps.now:0),0))} sub={t("t/s · measured sum")} />
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
                {/* 后端探不到 served-name 且网关同一 endpoint 挂多条候选路由 →
                    名字是按字母序猜的(历史上把 minimax-m3 显成 DeepSeek)。标出来。 */}
                <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap" }}>
                  <b>{m.display}</b>
                  {m.identityUnverified && (
                    <span className="chip warn" style={{ fontFamily: "var(--mono)", fontSize: 9.5, padding: "2px 6px" }}
                          title={`${t("Backend unreachable — name guessed from gateway routes")}: ${(m.identityCandidates || []).join(" / ")}`}>
                      {t("identity unverified")}
                    </span>
                  )}
                </div>
                {/* 后端自报了已加载权重时，副标题优先显示它:这套 oMLX 把 served-name
                    设成了路由名(mbp-none),vendor/params/quant 全是「—」,光看行首认不出
                    载的是什么模型 —— MBP 上 2026-09-18 一天换了三个。 */}
                <span>{m.loadedModel
                  ? m.loadedModel
                  : `${m.vendor} · ${m.params} · ${m.quant}`} · ctx&nbsp;{m.ctx === 0 ? "—" : m.ctx >= 1e6 ? `${(m.ctx/1e6).toFixed(0)}M` : `${(m.ctx/1024).toFixed(0)}K`}</span>
              </div>
              <div>
                {hasLiveMetrics(m) ? (
                  <>
                    <div className="model-bigmetric">{_live.models[m.id].tps.now.toFixed(0)}<small>t/s</small></div>
                    <div className="model-spark"><Sparkline data={_live.models[m.id].tps.hist} color="var(--accent)" height={20} /></div>
                    {/* 并发: tok/s 不带并发数解读不了 (单并发 50t/s vs 4并发 200t/s 截然不同).
                        vLLM/SGLang 暴露 num_requests_running + waiting; llama.cpp 只有 running. */}
                    <div style={{ marginTop: 4, fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>
                      <span style={{ color: "var(--ink-2)" }}>{_live.models[m.id].running.now.toFixed(0)}</span> {t("running")}
                      {_live.models[m.id].waiting.now > 0 && (
                        <> · <span style={{ color: "var(--warn)" }}>{_live.models[m.id].waiting.now.toFixed(0)} {t("queued")}</span></>
                      )}
                    </div>
                  </>
                ) : <div style={{ fontFamily: "var(--mono)", color: "var(--ink-4)", fontSize: 11 }}>{t("no live metrics source")}<br />· {m.framework} ·</div>}
              </div>
              <div className="num" style={{ fontSize: 12, color: "var(--ink-2)" }}>
                {/* TTFT: vLLM/SGLang 暴露, llama.cpp/oMLX 不暴露(诚实显—); TPOT: 除 oMLX 外都有。
                    ⛔ 取 m.ttft / m.tpot(后端窗口值,缺席时 data.js 会删掉)而不是 sparkline 的
                    .now —— 后者在后端停发后会把上一次的值(或初始 0)继续显示成当前值,
                    空闲的 llama.cpp 因此长期显示「0 ms/tok」(2026-09-18 实测)。 */}
                {(m.metricsSource === "vllm" || m.metricsSource === "sglang") ? (
                  <div><b style={{ color: "var(--ink)" }}>{m.ttft !== undefined ? m.ttft.toFixed(0) : "—"}</b> <small style={{ color: "var(--ink-3)", fontFamily: "var(--mono)" }}>ms TTFT</small></div>
                ) : (m.metricsSource === "llamacpp" || m.metricsSource === "omlx") ? (
                  <div><span style={{ color: "var(--ink-4)" }}>—</span> <small style={{ color: "var(--ink-3)", fontFamily: "var(--mono)" }}>ms TTFT</small></div>
                ) : null}
                {(m.metricsSource === "vllm" || m.metricsSource === "llamacpp" || m.metricsSource === "sglang") ? (
                  <div><b style={{ color: "var(--ink)" }}>{m.tpot !== undefined ? m.tpot.toFixed(0) : "—"}</b> <small style={{ color: "var(--ink-3)", fontFamily: "var(--mono)" }}>ms/tok</small></div>
                ) : <span style={{ color: "var(--ink-4)" }}>—</span>}
              </div>
              <div>
                {(m.metricsSource === "vllm" || m.metricsSource === "sglang") ? (
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
                {/* p50/p95/p99 是滑动窗口值：窗口内没有完成的请求就整组缺席，
                    此时显示「—」而不是 undefined 或上一个窗口的残值。 */}
                {(m.metricsSource === "vllm" || m.metricsSource === "sglang")
                  && typeof m.p95 === "number" ? <>
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
                  {m.nodes.map((nid) => <span key={nid} className="chip" style={{ fontSize: 9.5 }}
                                               title={nid}>{_nodeName(nid)}</span>)}
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
        {/* 空态：网关没返回任何路由(刚重启/网关未就绪)。诚实显空，不拿演示目录顶包。 */}
        {visible.length === 0 && (
          <div style={{ padding: "34px 22px", textAlign: "center", fontFamily: "var(--mono)", fontSize: 12, color: "var(--ink-3)" }}>
            {_MODELS.length === 0
              ? <>{t("No models discovered — gateway returned no routes.")}<br />
                  <span style={{ color: "var(--ink-4)", fontSize: 11 }}>{t("Auto-retrying every 25s · nothing is faked while empty")}</span></>
              : t("No model matches this filter.")}
          </div>
        )}
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

// 概览进度条。与吞吐图同属"一眼看"的性质，所以和图放同一列；
// 密集表格(延迟分解/SLO/响应构成)留在中间列，那里给了更多宽度。
function MetricBars({ model, ms }) {
  const { t } = useLang();
  const src = model.metricsSource;
  if (src === "vllm" || src === "sglang") {
    return (
      <>
        {/* 窗口内没有完成的请求 → 显示「—」而不是上一个窗口的残值。
            「延迟未知」和「延迟很低」是两件事，混起来最容易误判。 */}
        <DetailMetric label={t("TTFT (first token)")} value={model.ttft !== undefined ? model.ttft.toFixed(0) : "—"} unit={model.ttft !== undefined ? " ms" : ""} bar={Math.min(100, (model.ttft || 0) / 8)} color="violet" />
        <DetailMetric label={t("TPOT (per token)")}   value={model.tpot !== undefined ? model.tpot.toFixed(0) : "—"} unit={model.tpot !== undefined ? " ms" : ""} bar={Math.min(100, (model.tpot || 0) / 5)} color="teal" />
        <DetailMetric label={t("Concurrency · running")} value={ms.running.now.toFixed(0)} unit={ms.waiting.now > 0 ? ` (+${ms.waiting.now.toFixed(0)} ${t("queued")})` : ""} bar={Math.min(100, ms.running.now * 10)} color={ms.waiting.now > 0 ? "hot" : "ok"} />
        <DetailMetric label={t("Requests / sec")}     value={ms.rps.now.toFixed(2)} unit="" bar={Math.min(100, ms.rps.now * 6)} color="accent" />
        <DetailMetric label={t("KV-cache")}           value={ms.kv.now.toFixed(1)} unit="%" bar={Math.min(100, ms.kv.now)} color={ms.kv.now > 80 ? "bad" : ms.kv.now > 65 ? "hot" : "violet"} />
        {model.kvTokens ? <div style={{ marginTop: -6, fontFamily: "var(--mono)",
                                        fontSize: 10.5, color: "var(--ink-3)" }}>
          {t("pool")} {(model.kvTokens / 1e6).toFixed(2)}M {t("tokens")} · {(model.kvBytes / 2 ** 30).toFixed(1)} GiB · {t("max concurrency")} {model.kvMaxConc}×
        </div> : null}
      </>
    );
  }
  if (src === "omlx") {
    return (
      <>
        {/* oMLX 只有累计计数器:并发/请求率是窗口实测,prefill 吞吐与缓存命中率是它
            自报的【生命周期】均值(标出来,别当此刻值读);没有 TTFT/TPOT/KV 占用。 */}
        <DetailMetric label={t("Concurrency · running")} value={ms.running.now.toFixed(0)} unit={ms.waiting.now > 0 ? ` (+${ms.waiting.now.toFixed(0)} ${t("queued")})` : ""} bar={Math.min(100, ms.running.now * 25)} color={ms.waiting.now > 0 ? "hot" : "ok"} />
        <DetailMetric label={t("Requests / sec")} value={ms.rps.now.toFixed(2)} unit="" bar={Math.min(100, ms.rps.now * 6)} color="accent" />
        {/* oMLX 没有 prefill token 计数器 → 实时口径【无源】。只给引擎自报的历史
            均值, 并明写无实时源, 免得把 2037 当成"此刻在以 2037 tok/s 预填"。 */}
        {model.prefillTokPerSLifetime !== undefined ? (
          <DetailMetric label={t("Prefill · lifetime avg")} value={model.prefillTokPerSLifetime.toLocaleString("en-US")} unit=" tok/s" bar={Math.min(100, model.prefillTokPerSLifetime / 20)} color="violet"
                        note={t("no realtime source")} />
        ) : null}
        {model.cacheHitRateLifetime !== undefined ? (
          <DetailMetric label={t("Prompt cache hit · lifetime avg")} value={model.cacheHitRateLifetime.toFixed(0)} unit="%" bar={model.cacheHitRateLifetime} color="teal"
                        note={t("no realtime source")} />
        ) : null}
      </>
    );
  }
  if (src === "llamacpp") {
    return (
      <>
        {/* llama.cpp 暴露 TPOT/吞吐/并发/步频/投机解码/缓存命中,不暴露 TTFT/KV/e2e 直方图。
            TPOT 是滑动窗口值(计数器只在请求完成时跳);窗口内没有完成的请求就显示「—」,
            不拿 0 顶(0 会被读成"每 token 0 毫秒")。 */}
        <DetailMetric label={t("TPOT (per token)")}   value={model.tpot !== undefined ? model.tpot.toFixed(0) : "—"} unit={model.tpot !== undefined ? " ms" : ""} bar={Math.min(100, (model.tpot || 0) / 5)} color="teal" />
        <DetailMetric label={t("Concurrency · running")} value={ms.running.now.toFixed(0)} unit={ms.waiting.now > 0 ? ` (+${ms.waiting.now.toFixed(0)} ${t("queued")})` : ""} bar={Math.min(100, ms.running.now * 10)} color={ms.waiting.now > 0 ? "hot" : "ok"} />
        {/* ⚠️ 步频是 1.2s 【瞬时】窗口,上面的吞吐/TPOT 是 45s 完成请求窗口 —— 两者
            口径不同,别拿 步频 × 每步产出 去对吞吐(对不上是正常的)。 */}
        {model.stepsPerSec !== undefined ? (
          <DetailMetric label={t("Engine steps")} value={model.stepsPerSec.toFixed(1)} unit={` /s · ${t("instantaneous")}`} bar={Math.min(100, model.stepsPerSec * 10)} color="accent" />
        ) : null}
        {/* prefill 两行分开: 上面是【此刻】(空闲即 0), 下面是【引擎速度】(空闲不掉)。
            2026-09-19 前只有下面那行, 空闲时显示 1699 被当成实时值读。 */}
        {model.prefillTokPerS !== undefined ? (
          <DetailMetric label={t("Prefill · now")} value={model.prefillTokPerS.toLocaleString("en-US")} unit=" tok/s" bar={Math.min(100, model.prefillTokPerS / 20)} color="violet"
                        note={model.prefillWindowSec ? `${t("window")} ${model.prefillWindowSec}s` : null} />
        ) : null}
        {model.prefillTokPerSLifetime !== undefined ? (
          <DetailMetric label={t("Prefill · lifetime avg")} value={model.prefillTokPerSLifetime.toLocaleString("en-US")} unit=" tok/s" bar={Math.min(100, model.prefillTokPerSLifetime / 20)} color="violet" />
        ) : null}
        {/* 命中率两行: 上面是【窗口实时】(窗口内没有 prefill 活动就显示 "—",
            不填 0 也不 latch), 下面是【累计平均】(空闲时依然成立)。 */}
        {model.cacheHitSource ? (
          <DetailMetric label={t("Prompt cache hit · now")}
                        value={model.cacheHitRate !== undefined ? model.cacheHitRate.toFixed(0) : "—"}
                        unit={model.cacheHitRate !== undefined ? "%" : ""}
                        bar={model.cacheHitRate || 0} color="teal"
                        note={model.cacheHitRate === undefined ? t("no prefill in window")
                              : model.cacheHitWindowSec ? `${t("window")} ${model.cacheHitWindowSec}s` : null} />
        ) : null}
        {model.cacheHitRateLifetime !== undefined ? (
          <DetailMetric label={t("Prompt cache hit · lifetime avg")} value={model.cacheHitRateLifetime.toFixed(0)} unit="%" bar={model.cacheHitRateLifetime} color="teal" />
        ) : null}
      </>
    );
  }
  return null;
}

function ModelDetail({ model }) {
  const { t } = useLang();
  const ms = _live.models[model.id];
  return (
    <div className="model-detail">
      <div>
        <div className="metric-l" style={{ marginBottom: 8 }}>
          {t("Throughput · last 32 ticks")}
          {model.tpsWindowSec
            ? <span style={{ color: "var(--ink-3)", textTransform: "none" }}>
                {/* oMLX 的计数器只在请求完成时跳,吞吐本来就是滑动窗口值(不是瞬时),
                    也没有"瞬时 vs 持续"这组对照 —— 标成 instantaneous 会读错。 */}
                {/* llama.cpp 与 oMLX 的生成计数器都只在请求完成时跳 → 吞吐本来就是
                    滑动窗口值,标 instantaneous 会读错。 */}
                {" · "}{(model.metricsSource === "omlx" || model.metricsSource === "llamacpp") ? t("sliding window") : t("instantaneous")} {model.tpsWindowSec}s
                {/* 瞬时窗口只有十几步，方差极大；vllm bench serve 同一次测量
                    Output vs Peak token throughput 就差 2.1 倍(61.13 / 129.00)。
                    并排给出长窗口持续值，避免把峰值读成持续吞吐。 */}
                {(model.metricsSource === "omlx" || model.metricsSource === "llamacpp") ? null
                  : model.tpsSustained !== null && model.tpsSustained !== undefined
                  ? <> · {t("sustained")} <b style={{ color: "var(--ink-2)" }}>{model.tpsSustained}</b> t/s
                      {" "}({model.tpsSustainedWindowSec}s)</>
                  : <> · {t("sustained")} — <small>{t("(warming up)")}</small></>}
              </span>
            : null}
        </div>
        <AreaChart series={[ms.tps.hist]} colors={["var(--accent)"]} height={140} unit={model.kind === "embed" ? " e/s" : " t/s"} padding={{l:42,r:14,t:10,b:18}} ticks={3} />
        <div style={{ display: "flex", flexDirection: "column", gap: 14, marginTop: 18 }}>
          <MetricBars model={model} ms={ms} />
        </div>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
        {(model.metricsSource === "vllm" || model.metricsSource === "sglang") ? <>
          {model.latencyWindowSec !== undefined ? <div>
            <div className="metric-l" style={{ marginBottom: 3 }}>
              {t("Latency breakdown")}
            </div>
            {/* 窗口长度与样本数必须显示：这些是滑动窗口值不是「开机以来」，
                而且读数的人要能判断这个 p99 是几条样本撑起来的。
                单独占一行：接在标题后面时，中等宽度下会从「low sample」中间折行。 */}
            <div style={{ fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)",
                          marginBottom: 8 }}>
              {t("window")} {model.latencyWindowSec}s · n={model.latencySampleN}
              {/* 样本偏少时给值+标注(同 sloJointWide 的做法)；数学上无意义的那些
                  分位数则直接缺席、显示「—」。两者是互补不是二选一：
                  藏掉的是「算不出」，标注的是「算得出但别当准数读」。 */}
              {model.latencyLowSample
                ? <span style={{ color: "var(--hot)" }}> · {t("low sample")}</span>
                : null}
            </div>
            <PctGrid>
              {/* 缺席的分位数由 PctRow 渲染成「—」：样本不足以支撑该分位数时，
                  宁可留白也不给一个看起来正常、实际是桶沿的数字。 */}
              {/* 排队 / prefill / decode 三段分开：prefill 算力受限、decode 内存
                  带宽受限、queue 是容量不够。混着报，"变慢了"看不出该查哪一侧。 */}
              <PctRow label={t("Queue")}   mean={model.queue}   p50={model.queueP50}   p90={model.queueP90}   p99={model.queueP99} />
              <PctRow label={t("Prefill")} mean={model.prefill} p50={model.prefillP50} p90={model.prefillP90} p99={model.prefillP99} />
              {/* SGLang 不导出单请求 decode 耗时与原始到达间隔：这两行对它恒为「—」，
                  直接不画，下方脚注说明原因。 */}
              {model.metricsSource !== "sglang"
                ? <PctRow label={t("Decode")} mean={model.decode} p50={model.decodeP50} p90={model.decodeP90} p99={model.decodeP99} />
                : null}
              {/* 均值用 model.ttft/tpot（窗口 Δsum/Δcount），不用 sparkline 的 now：
                  分位数和均值必须同源同窗，一半窗口一半别的比全错更难查。 */}
              <PctRow label="TTFT"         mean={model.ttft}    p50={model.ttftP50}    p90={model.ttftP90}    p99={model.ttftP99} warn={20000} />
              <PctRow label="TPOT"         mean={model.tpot}    p50={model.tpotP50}    p90={model.tpotP90}    p99={model.tpotP99} />
              {/* ITL ≠ TPOT：TPOT 按请求平均每 token，ITL 是相邻 token 的实际
                  到达间隔分布。投机解码下一次接受多个 token → 一批接近 0 的间隔
                  加少量长间隔，均值抹平，分位数才看得见。 */}
              {model.metricsSource !== "sglang"
                ? <PctRow label="ITL" mean={model.itl} p50={model.itlP50} p90={model.itlP90} p99={model.itlP99} />
                : null}
            </PctGrid>
            <div style={{ marginTop: 10, fontFamily: "var(--mono)", fontSize: 10.5,
                          color: "var(--ink-3)", lineHeight: 1.7 }}>
              {/* prefill 的性能对照是【吞吐】不是耗时：原始耗时随 prompt 长度线性
                  变化，量的是负载不是引擎速度。decode 一侧不加吞吐 —— 它的对照是表里的
                  TPOT 与 ITL，加了是重复。
                  放表下方而不是插在 Prefill 行下面：单位不同(tok/s)、口径不同(生命周期，
                  不是本窗口 —— 窗口差分会错位到虚高几十倍，见 main.py _put_prefill_tps)，
                  插在表里不属于任何一列，是用户说「乱」的原因之一。必须带「累计」字样。
                  「每请求归一化，非墙钟」放 title：用户选定的样式里这行只有数值与累计。 */}
              {model.prefillTokPerS !== undefined ? (
                <div title={t("wall-clock rate over the sliding window; 0 when nothing is prefilling")}>
                  {t("Prefill · now")}{" "}
                  <b style={{ color: "var(--ink-2)" }}>{model.prefillTokPerS.toLocaleString("en-US")}</b>
                  {" "}tok/s{model.prefillWindowSec ? ` · ${t("window")} ${model.prefillWindowSec}s` : ""}
                </div>
              ) : null}
              {model.prefillTokPerSLifetime !== undefined ? (
                <div title={t("per-request normalised, not wall-clock")}>
                  {t("Prefill · lifetime avg")}{" "}
                  <b style={{ color: "var(--ink-2)" }}>{model.prefillTokPerSLifetime.toLocaleString("en-US")}</b>
                  {" "}tok/s
                </div>
              ) : null}
              {/* 前缀缓存命中率。⛔ 分母是 命中+实算, 不是 prompt_tokens_total
                  (后者已含命中, 会把命中率算成约一半)。默认给窗口实时值,
                  窗口内没有 prefill 活动就显示 "—", 不填 0 也不 latch。 */}
              {model.cacheHitSource ? (
                <div title={model.cacheHitRate === undefined ? t("no prefill in window") : ""}>
                  {t("Prompt cache hit · now")}{" "}
                  <b style={{ color: "var(--ink-2)" }}>
                    {model.cacheHitRate !== undefined ? model.cacheHitRate.toFixed(0) + "%" : "—"}</b>
                  {model.cacheHitRate !== undefined && model.cacheHitWindowSec
                    ? ` · ${t("window")} ${model.cacheHitWindowSec}s` : ""}
                </div>
              ) : null}
              {model.cacheHitRateLifetime !== undefined ? (
                <div>
                  {t("Prompt cache hit · lifetime avg")}{" "}
                  <b style={{ color: "var(--ink-2)" }}>{model.cacheHitRateLifetime.toFixed(0)}%</b>
                </div>
              ) : null}
              {/* SGLang 的 TPOT 桶按 token 加权(输出块内均摊)，Decode / ITL 引擎不导出
                  —— 不写明，下一个看到少了两行的人会再查一遍。 */}
              {model.metricsSource === "sglang"
                ? <div>{t("SGLang: TPOT per-token · no Decode / ITL")}</div>
                : null}
            </div>
            {model.sloTtftRate !== undefined ? <SloBlock model={model} /> : null}
            {model.saturated !== undefined ? <SatBlock model={model} /> : null}
          </div> : null}
          {/* 响应构成放第 2 栏：它讲的是延迟与输出构成，与上面的分解同话题；
              第 3 栏留给 路由 / 效率 / 投机解码，两栏高度才不至于一边空一半。 */}
          <ResponseBlock model={model} />
        </> : model.metricsSource === "omlx" ? <>
          <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)", lineHeight: 1.7 }}>
            {t("oMLX exposes counters only (/api/status) · no TTFT / TPOT / KV% / latency histograms")}
          </div>
        </> : model.metricsSource === "llamacpp" ? <>
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
          {/* 后端自报的已加载权重。oMLX 这套把 served-name 设成了路由名(mbp-none),
              光看模型名不知道载的是什么 —— MBP 上 2026-09-18 一天换了三个模型。 */}
          {model.loadedModel ? <>
            <span style={{ color: "var(--ink-3)" }}>{t("Loaded model")}</span>
            <span style={{ color: "var(--ink)" }}>{model.loadedModel}
              {model.weightsGb !== undefined
                ? <small style={{ color: "var(--ink-3)" }}> · {model.weightsGb} GB</small> : null}</span>
          </> : null}
          <span style={{ color: "var(--ink-3)" }}>{t("Port")}</span><span>:{model.port}</span>
          <span style={{ color: "var(--ink-3)" }}>{t("Quant")}</span><span>{model.quant}</span>
          <span style={{ color: "var(--ink-3)" }}>{t("VRAM")}</span><span>{model.vram} GB</span>
          <span style={{ color: "var(--ink-3)" }}>{t("Context")}</span><span>{model.ctx === 0 ? "—" : model.ctx >= 1e6 ? `${(model.ctx/1e6).toFixed(0)}M tokens` : `${(model.ctx/1024).toFixed(0)}K tokens`}</span>
          <span style={{ color: "var(--ink-3)" }}>{t("Placement")}</span>
          <span title={model.nodes.join(", ")}>{model.nodes.map(_nodeName).join(", ")}</span>
        </div>
        <div style={{ marginTop: 14, display: "flex", flexWrap: "wrap", gap: 5 }}>
          {model.tags.map((t) => <span key={t} className="chip violet" style={{ fontSize: 9.5 }}>{t}</span>)}
        </div>
        {model.mbu !== undefined || model.mfu !== undefined
          ? <div style={{ marginTop: 18 }}><EffBlock model={model} /></div> : null}
        {model.spec ? <div style={{ marginTop: 18 }}><SpecDecode spec={model.spec} /></div> : null}
      </div>
    </div>
  );
}

// ── TELEMETRY ──────────────────────────────────────────────────────────
// 某个口径的数据源整体不可用时,不要让它退化成一排「—」——「—」读起来像
// 「这个窗口没数据」,而真相是「这个数据源现在根本不可信」。整行写清楚原因。
function _srcOk(tr, k) {
  return (tr?.sources?.[k]?.available) !== false;
}

function _unavailRows(tr, t) {
  const src = tr?.sources || {};
  const rows = [];
  [["wall", t("Wall avg")], ["ac", t("AC")], ["cabinet", t("Cabinet mean")]].forEach(([k, label]) => {
    const s = src[k];
    if (!s || s.available !== false) return;
    rows.push(
      <tr key={"unavail-" + k}>
        <td><b style={{ color: "var(--ink-3)" }}>{label}</b></td>
        <td colSpan={3} style={{ textAlign: "right", color: "var(--ink-4)", fontFamily: "var(--mono)", fontSize: 10.5 }}>
          {t("data source unavailable")}{s.reason ? " · " + s.reason : ""}
        </td>
      </tr>
    );
  });
  return rows;
}

function _trendCell(v, suffix = "", digits = 1) {
  if (v == null) return "—";
  return (typeof v === "number" ? v.toFixed(digits) : String(v)) + suffix;
}

function TelemetrySection() {
  useLive();
  const { t } = useLang();
  // HA-derived fields are null when the exporter is absent / sensor stale.
  // Render the tm-card strip only when at least one field exists; otherwise
  // the section degrades to its original 2-col grid as if HA never existed.
  const pw = _live.power || null;
  const en = _live.env   || null;
  const haAny = (pw && (pw.wallW != null || pw.tokensPerW != null))
             || (en && (en.rackTempC != null || en.acOn != null));
  // 整机口径(智能插座)与 GPU 口径(DCGM)是两回事:前者挂了不等于没有能耗数据。
  // ⛔ 但也【不能】拿 GPU 数字顶替整机数字 —— 卡片标题与副标必须写清是哪个口径。
  const gpuAny = pw && (pw.gpuW != null || pw.gpuKwh24h != null);
  const wallOk = pw?.wallAvailable !== false && pw?.wallW != null;
  const fmt = (v, unit, digits = 1) =>
    v == null ? "—" : (typeof v === "number" ? v.toFixed(digits) : String(v)) + (v == null ? "" : unit);
  return (
    <section className="page reveal" id="telemetry">
      <div className="eyebrow"><span className="num">05</span>{t("Telemetry · alerts")}</div>
      <h2>{t("Signal, not ")}<em>{t("noise.")}</em></h2>
      <p className="lede">
        {t("Every request that lands at the gateway, every anomaly the rules engine catches — surfaced as a quiet, structured stream. No paging unless something actually needs you.")}
      </p>

      {(haAny || gpuAny) && (
        <div className="grid" style={{ gridTemplateColumns: "repeat(3, minmax(0,1fr))", gap: 14, marginBottom: 18 }}>
          <div className="tm-card" data-metric="wall-power">
            <div className="tm-card-l">{wallOk ? t("Wall power") : t("GPU power")}</div>
            <div className="tm-card-v num">{fmt(wallOk ? pw?.wallW : pw?.gpuW, " W")}</div>
            <div className="tm-card-s">
              {wallOk
                ? <>{t("Σ smart-plug · DCGM GPU ")}{fmt(pw?.gpuW, " W")}</>
                : <>{t("DCGM · GPU only, not whole-machine")}
                    {pw?.gpuKwh24h != null && <> · {t("24h ")}<span className="num">{pw.gpuKwh24h.toFixed(2)}</span> kWh</>}</>}
            </div>
            {!wallOk && pw?.wallUnavailableReason && (
              <div className="tm-card-s" style={{ color: "var(--ink-4)" }}>
                {t("smart-plug")} · {pw.wallUnavailableReason}
              </div>
            )}
          </div>
          <div className="tm-card" data-metric="efficiency">
            <div className="tm-card-l">{t("Efficiency")}</div>
            <div className="tm-card-v num">{fmt(pw?.tokensPerW, "", 2)}
              {pw?.tokensPerW != null && <small> tok·W⁻¹·s⁻¹</small>}
            </div>
            <div className="tm-card-s">
              {pw?.tokensPerW != null ? t("LiteLLM tokens / wall power")
                                      : t("Whole-machine source unavailable")}
            </div>
          </div>
          <div className="tm-card" data-metric="rack-env">
            <div className="tm-card-l">{t("Rack")}</div>
            <div className="tm-card-v num">
              {fmt(en?.rackTempC, " °C")}
              <span style={{ color: "var(--ink-3)", marginLeft: 10 }}>{fmt(en?.rackRH, "%", 0)}</span>
            </div>
            <div className="tm-card-s">
              <span className={"dot " + (en?.acOn === true ? "ok" : en?.acOn === false ? "bad" : "")}
                    style={{ marginRight: 6 }} />
              {t("AC ")}{en?.acOn == null ? "—" : en.acOn ? t("on") : t("off")}
              {en?.acW != null && <span style={{ color: "var(--ink-3)" }}> · {fmt(en.acW, " W")}</span>}
            </div>
          </div>
        </div>
      )}

      {haAny && (
        <div className="card" style={{ marginBottom: 18 }} data-metric="device-power">
          <div className="card-head">
            <div>
              <div className="card-title">{t("Devices · live power draw")}</div>
              <div className="card-sub">{t("Per smart-plug reading from Home Assistant · 15s polled")}</div>
            </div>
            <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>
              Σ <b style={{ color: "var(--ink)" }} className="num">{fmt(pw?.wallW, " W")}</b>
              {pw?.kwh24h != null && <span style={{ marginLeft: 12 }}>· {t("24h ")}
                <b style={{ color: "var(--ink)" }} className="num">{pw.kwh24h.toFixed(2)}<small> kWh</small></b></span>}
              {pw?.kwh30d != null && <span style={{ marginLeft: 8 }}>· {t("30d ")}
                <b style={{ color: "var(--ink)" }} className="num">{pw.kwh30d.toFixed(2)}<small> kWh</small></b></span>}
              {en?.cabinetHeatProxyC != null && <span style={{ marginLeft: 12 }} title="Mean of all spark cuco plug internal temps · proxy for cabinet ambient, ±5°C uncertain">
                · {t("cabinet ~")}
                <b style={{ color: "var(--ink)" }} className="num">{en.cabinetHeatProxyC.toFixed(1)}<small> °C</small></b></span>}
            </div>
          </div>
          <div className="card-body" style={{ padding: "8px 22px 16px" }}>
            <div style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)", marginBottom: 8, letterSpacing: ".06em" }}>
              {t("Energy = sliding-window integral of live wall power, independent from HA/Mi Home counters. 24h ≤ 30d strictly by construction; values grow as the series matures.")}
            </div>
            <table className="tm-devices">
              <thead>
                <tr>
                  <th style={{ width: "23%" }}>{t("Device")}</th>
                  <th style={{ width: "16%" }}>{t("Type")}</th>
                  <th style={{ width: "11%", textAlign: "right" }}>{t("Power")}</th>
                  <th style={{ width: "12%", textAlign: "right" }}>{t("24h")}</th>
                  <th style={{ width: "12%", textAlign: "right" }}>{t("30d")}</th>
                  <th style={{ width: "12%", textAlign: "right" }} title="Smart-plug internal temperature — proxy for ambient air at that location in the rack">{t("Plug °C")}</th>
                  <th style={{ width: "14%" }}>{t("State")}</th>
                </tr>
              </thead>
              <tbody>
                {_NODES.map((n) => {
                  const w = pw?.byNode?.[n.id];
                  const k24 = pw?.byNode24h?.[n.id];
                  const k30 = pw?.byNode30d?.[n.id];
                  const ptemp = en?.byNodePlugTempC?.[n.id];
                  const has = w != null;
                  const hot = ptemp != null && ptemp >= 50;
                  return (
                    <tr key={n.id} data-device={n.id}>
                      <td><b style={{ color: "var(--ink)" }}>{n.name}</b>
                        <span style={{ color: "var(--ink-4)", marginLeft: 8, fontFamily: "var(--mono)", fontSize: 10.5 }}>{n.ip}</span>
                      </td>
                      <td style={{ color: "var(--ink-3)" }}>{n.class || t("Compute node")}</td>
                      <td className="num" style={{ textAlign: "right", color: has ? "var(--ink)" : "var(--ink-4)" }}>
                        {has ? w.toFixed(0) + " W" : "—"}
                      </td>
                      <td className="num" style={{ textAlign: "right", color: k24 != null ? "var(--ink)" : "var(--ink-4)" }}>
                        {k24 != null ? k24.toFixed(2) + " kWh" : "—"}
                      </td>
                      <td className="num" style={{ textAlign: "right", color: k30 != null ? "var(--ink)" : "var(--ink-4)" }}>
                        {k30 != null ? k30.toFixed(2) + " kWh" : "—"}
                      </td>
                      <td className="num" style={{ textAlign: "right", color: ptemp == null ? "var(--ink-4)" : hot ? "var(--hot)" : "var(--ink)" }}>
                        {ptemp != null ? ptemp.toFixed(0) + " °C" : "—"}
                      </td>
                      <td>
                        {has ? (
                          <><span className="dot ok" style={{ marginRight: 6 }} />{t("on")}</>
                        ) : (
                          <span style={{ color: "var(--ink-4)", fontSize: 11 }}>{t("no smart plug")}</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
                {(en?.acW != null || en?.acOn != null) && (
                  <tr data-device="rack-ac">
                    <td><b style={{ color: "var(--ink)" }}>{t("Rack AC")}</b>
                      <span style={{ color: "var(--ink-4)", marginLeft: 8, fontFamily: "var(--mono)", fontSize: 10.5 }}>—</span>
                    </td>
                    <td style={{ color: "var(--ink-3)" }}>{t("Cabinet HVAC")}</td>
                    <td className="num" style={{ textAlign: "right" }}>
                      {en.acW != null ? en.acW.toFixed(0) + " W" : "—"}
                    </td>
                    <td className="num" style={{ textAlign: "right", color: en.acKwh24h != null ? "var(--ink)" : "var(--ink-4)" }}>
                      {en.acKwh24h != null ? en.acKwh24h.toFixed(2) + " kWh" : "—"}
                    </td>
                    <td className="num" style={{ textAlign: "right", color: en.acKwh30d != null ? "var(--ink)" : "var(--ink-4)" }}>
                      {en.acKwh30d != null ? en.acKwh30d.toFixed(2) + " kWh" : "—"}
                    </td>
                    <td className="num" style={{ textAlign: "right", color: "var(--ink-4)" }} title="AC plug runs hot from compressor — not a useful ambient signal">
                      —
                    </td>
                    <td>
                      <span className={"dot " + (en.acOn === true ? "ok" : en.acOn === false ? "bad" : "")} style={{ marginRight: 6 }} />
                      {en.acOn == null ? t("unknown") : en.acOn ? t("on") : t("off")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {_live.energyTrends && (
        <div className="card" style={{ marginBottom: 18 }} data-metric="energy-trends">
          <div className="card-head">
            <div>
              <div className="card-title">{t("Energy trends · A/B for physical changes")}</div>
              <div className="card-sub">
                {t("Day = 06:00–18:00 local · same-window comparison reveals what changes (AC setpoint, sunshade, sensor move) actually moved the bill")}
              </div>
            </div>
          </div>
          <div className="card-body" style={{ padding: "8px 22px 16px" }}>
            <table className="tm-devices">
              <thead>
                <tr>
                  <th style={{ width: "32%" }}>{t("Metric")}</th>
                  <th style={{ width: "22%", textAlign: "right" }}>{t("24h")}</th>
                  <th style={{ width: "22%", textAlign: "right" }}>{t("7d")}</th>
                  <th style={{ width: "24%", textAlign: "right" }}>{t("30d")}</th>
                </tr>
              </thead>
              <tbody>
                {/* GPU —— 唯一还活着的能耗口径(DCGM)。⛔ 只含 GPU, 不是整机功耗。 */}
                <tr><td><b style={{ color: "var(--ink)" }}>{t("GPU avg")}</b>
                  <span style={{ color: "var(--ink-4)", marginLeft: 6 }}>{t("(GPU only)")}</span></td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.gpu?.last24h?.avgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.gpu?.last7d?.avgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.gpu?.last30d?.avgW, " W", 0)}</td></tr>
                <tr><td style={{ paddingLeft: 22, color: "var(--ink-3)" }}>{t("GPU energy")}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-2)" }}>{_trendCell(_live.energyTrends.gpu?.last24h?.kwh, " kWh", 2)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-2)" }}>{_trendCell(_live.energyTrends.gpu?.last7d?.kwh, " kWh", 2)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-2)" }}>{_trendCell(_live.energyTrends.gpu?.last30d?.kwh, " kWh", 2)}</td></tr>
                <tr><td style={{ paddingLeft: 22, color: "var(--ink-3)" }}>↳ {t("day")} (06-18)</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.gpu?.last24h?.dayAvgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.gpu?.last7d?.dayAvgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.gpu?.last30d?.dayAvgW, " W", 0)}</td></tr>
                <tr><td style={{ paddingLeft: 22, color: "var(--ink-3)" }}>↳ {t("night")}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.gpu?.last24h?.nightAvgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.gpu?.last7d?.nightAvgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.gpu?.last30d?.nightAvgW, " W", 0)}</td></tr>
                {/* 不可用的口径:整行写明"数据源不可用 + 原因", 不留一排 0 或一排空 */}
                {_unavailRows(_live.energyTrends, t)}
                {/* AC —— 数据源不可用时整组隐藏, 由上面的 _unavailRows 用一行说明取代 */}
                {_srcOk(_live.energyTrends, "ac") && (<>
                <tr><td><b style={{ color: "var(--ink)" }}>{t("AC energy")}</b></td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.ac?.last24h?.kwh, " kWh", 2)}</td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.ac?.last7d?.kwh, " kWh", 2)}</td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.ac?.last30d?.kwh, " kWh", 2)}</td></tr>
                <tr><td style={{ paddingLeft: 22, color: "var(--ink-3)" }}>{t("avg power")}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-2)" }}>{_trendCell(_live.energyTrends.ac?.last24h?.avgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-2)" }}>{_trendCell(_live.energyTrends.ac?.last7d?.avgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-2)" }}>{_trendCell(_live.energyTrends.ac?.last30d?.avgW, " W", 0)}</td></tr>
                <tr><td style={{ paddingLeft: 22, color: "var(--ink-3)" }}>↳ {t("day")} (06-18)</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.ac?.last24h?.dayAvgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.ac?.last7d?.dayAvgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.ac?.last30d?.dayAvgW, " W", 0)}</td></tr>
                <tr><td style={{ paddingLeft: 22, color: "var(--ink-3)" }}>↳ {t("night")} (18-06)</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.ac?.last24h?.nightAvgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.ac?.last7d?.nightAvgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.ac?.last30d?.nightAvgW, " W", 0)}</td></tr>
                </>)}
                {/* Wall */}
                {_srcOk(_live.energyTrends, "wall") && (
                <tr><td><b style={{ color: "var(--ink)" }}>{t("Wall avg")}</b></td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.wall?.last24h?.avgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.wall?.last7d?.avgW, " W", 0)}</td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.wall?.last30d?.avgW, " W", 0)}</td></tr>
                )}
                {/* Cabinet */}
                {_srcOk(_live.energyTrends, "cabinet") && (<>
                <tr><td><b style={{ color: "var(--ink)" }}>{t("Cabinet mean")}</b></td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.cabinet?.last24h?.meanC, " °C", 1)}</td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.cabinet?.last7d?.meanC, " °C", 1)}</td>
                  <td className="num" style={{ textAlign: "right" }}>{_trendCell(_live.energyTrends.cabinet?.last30d?.meanC, " °C", 1)}</td></tr>
                <tr><td style={{ paddingLeft: 22, color: "var(--ink-3)" }}>{t("min / max")}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.cabinet?.last24h?.minC, "", 1)} / {_trendCell(_live.energyTrends.cabinet?.last24h?.maxC, " °C", 1)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.cabinet?.last7d?.minC, "", 1)} / {_trendCell(_live.energyTrends.cabinet?.last7d?.maxC, " °C", 1)}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-3)" }}>{_trendCell(_live.energyTrends.cabinet?.last30d?.minC, "", 1)} / {_trendCell(_live.energyTrends.cabinet?.last30d?.maxC, " °C", 1)}</td></tr>
                </>)}
                {/* samples 用 GPU 口径 —— 它是当前唯一有数据的源; 原先读 ac 的
                    samples, HA 挂掉后整行恒 0/96, 又是一处"0 冒充无数据" */}
                <tr><td style={{ paddingLeft: 22, color: "var(--ink-4)", fontSize: 10.5, fontFamily: "var(--mono)" }}>{t("samples")}</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-4)", fontSize: 10.5, fontFamily: "var(--mono)" }}>{_live.energyTrends.gpu?.last24h?.samples || 0} / 97</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-4)", fontSize: 10.5, fontFamily: "var(--mono)" }}>{_live.energyTrends.gpu?.last7d?.samples || 0} / 673</td>
                  <td className="num" style={{ textAlign: "right", color: "var(--ink-4)", fontSize: 10.5, fontFamily: "var(--mono)" }}>{_live.energyTrends.gpu?.last30d?.samples || 0} / 2881</td></tr>
              </tbody>
            </table>
            <div style={{ fontSize: 10, color: "var(--ink-4)", marginTop: 8, fontFamily: "var(--mono)", letterSpacing: ".06em" }}>
              {t("samples = real data points actually in the window (full = window fully filled). Same value across windows means the series is younger than 24h.")}
            </div>
          </div>
        </div>
      )}

      {/* 逐单元连通性自检。⛔ 只列【非 pass】的项:全绿时一行带过, 不做成一屏
          绿勾 —— 面板的用处是让异常跳出来, 不是让人逐行确认正常。
          skipped 与 pass 分色:"这台机器本来就没有这个源"不等于"这个源是好的"。 */}
      {_live.selfTest && (
        <div className="card" style={{ marginBottom: 18 }} data-metric="selftest">
          <div className="card-head">
            <div>
              <div className="card-title">{t("Connectivity self-test · per unit")}</div>
              <div className="card-sub">
                {t("Each capability is pass / fail / skipped. skipped = no such source on this unit — not the same as healthy.")}
              </div>
            </div>
            <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>
              <b className="num" style={{ color: "var(--ok)" }}>{_live.selfTest.summary?.pass ?? 0}</b> {t("pass")}
              {" · "}
              <b className="num" style={{ color: (_live.selfTest.summary?.fail ?? 0) > 0 ? "var(--hot)" : "var(--ink-3)" }}>
                {_live.selfTest.summary?.fail ?? 0}</b> {t("fail")}
              {" · "}
              <b className="num">{_live.selfTest.summary?.skipped ?? 0}</b> {t("skipped")}
            </div>
          </div>
          <div className="card-body" style={{ padding: "8px 22px 16px" }}>
            {(() => {
              const rows = (_live.selfTest.checks || []).filter((c) => c.status !== "pass");
              if (!rows.length) {
                return <div style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>
                  {t("all capabilities pass")}</div>;
              }
              return (
                <table className="tm-devices">
                  <thead>
                    <tr>
                      <th style={{ width: "22%" }}>{t("Unit")}</th>
                      <th style={{ width: "16%" }}>{t("Capability")}</th>
                      <th style={{ width: "10%" }}>{t("Status")}</th>
                      <th style={{ width: "52%" }}>{t("Detail · next step")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((c, i) => (
                      <tr key={i}>
                        <td style={{ fontFamily: "var(--mono)", fontSize: 10.5 }} title={c.unit}>{c.unitLabel || c.unit}</td>
                        <td style={{ fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-2)" }}>{c.capability}</td>
                        <td style={{ fontFamily: "var(--mono)", fontSize: 10.5,
                                     color: c.status === "fail" ? "var(--hot)" : "var(--ink-3)" }}>{c.status}</td>
                        <td style={{ fontSize: 11, color: "var(--ink-2)" }}>
                          {c.detail}
                          {c.hint ? <div style={{ color: "var(--ink-4)", fontSize: 10.5, marginTop: 2 }}>↳ {c.hint}</div> : null}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              );
            })()}
          </div>
        </div>
      )}

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
                    <span className="t">{window.AIData.formatTime(e.t)}</span>
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
                <NodeBlob x={ATL.x} y={ATL.y} label={GW.name.toUpperCase() + " · " + (GW.gpu && GW.gpu.name ? GW.gpu.name.split(" ").slice(-2).join(" ") : "")} sub={GW.ip} color="var(--accent)" util={(_live.nodes[GW.id]||{}).gpu?.now || 0} />
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
      { kind: "view", label: "Nodes", sub: t("Section · {n} machines", { n: _NODES.length }), href: "#nodes" },
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
