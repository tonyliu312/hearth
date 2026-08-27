// Hearth · Infrastructure section — 非算力设备(交换机 / NAS / 出口网关)的温度与健康。
// 数据来自 /api/infra(SNMP 或设备自带 node-exporter, 均只读)→ live.infra。
// 设备清单由后端 config 的 `infra:` 声明;为空则整节不渲染(mock 模式即如此,不伪造设备)。
//
// 视觉:复用节点详情面板的 .sensor-card 一族(styles.css)。**正常值一律 var(--ink)**,
// 颜色只留给越线的读数 —— 与 sections-b.jsx 的 SensorPanel 同一条规矩,
// 不要给"正常"上绿色, 否则整节变成一片绿, 与全站 Apple 语汇冲突。
//
// 注:本文件独立 babel 作用域——全局词法环境已被 live/_live/_NODES 占用,故用 _iLive 等
// 独立名;SensorGroup 未导出到 window,故内联 _IGroup(标记/CSS 类与之一致)。

const _iLive = window.AIData.live;

// 温度阈值分两套。芯片/机身沿用全站既有线(sections-b.jsx SensorPanel: 70 warn / 80 hot);
// 硬盘远比芯片脆弱 —— .89 曾于 2026-06-13 因某盘到 61°C 触发 DSM 保护性关机,
// 故收紧到 48/55 给那条实测线留余量。
function _tempColor(celsius, group) {
  const [warn, hot] = group === "disk" ? [48, 55] : [70, 80];
  if (celsius >= hot) return "var(--hot)";
  if (celsius >= warn) return "var(--warn)";
  return "var(--ink)";
}

function _fmtUptime(sec) {
  if (sec == null) return "—";
  const d = Math.floor(sec / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

// 只在异常时出现。正常状态不给任何色块 —— 没有消息就是好消息。
function _IAlert({ label }) {
  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: 6, fontFamily: "var(--mono)",
                   fontSize: 11, color: "var(--bad)", border: "1px solid var(--bad)",
                   borderRadius: 999, padding: "3px 10px" }}>
      <span style={{ width: 6, height: 6, borderRadius: 999, background: "var(--bad)" }} />{label}
    </span>
  );
}

// 与 sections-b.jsx 的 SensorGroup 同结构同 CSS 类,只是不需要展开折叠
function _IGroup({ title, rep, repColor, rows }) {
  return (
    <div className="sensor-card">
      <div className="sensor-card-head">
        <span className="sensor-card-title">{title}</span>
        <span className="sensor-card-rep num" style={{ color: repColor || "var(--ink)" }}>{rep}</span>
      </div>
      <div className="sensor-rows">
        {rows.map((r, i) => (
          <div className="sensor-row" key={i}>
            <span className="sensor-row-l">{r.label}</span>
            <span className="num" style={{ color: r.color || "var(--ink)" }}>{r.value}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function _DeviceCard({ d }) {
  const { t } = useLang();
  const hot = d.hottest;
  const faults = [
    ...d.states.filter((s) => !s.ok).map((s) => s.label),
    ...d.psus.filter((p) => p.ok === false).map((p) => p.label),
    ...d.disks.filter((x) => !x.ok).map((x) => x.name),
  ];
  const hotColor = hot ? _tempColor(hot.celsius, _groupOf(d, hot.label)) : "var(--ink)";
  // 描边只在真出事时亮:设备失联 / 有故障位 / 最热点已越 hot 线
  const border = !d.up || faults.length ? "var(--bad)"
    : hotColor === "var(--hot)" ? "var(--hot)" : undefined;

  const groups = [];
  if (d.temps.length) {
    const sorted = [...d.temps].sort((a, b) => b.celsius - a.celsius);
    groups.push(
      <_IGroup key="temps" title={t("Temperature")}
        rep={`${sorted[0].celsius} °C`} repColor={_tempColor(sorted[0].celsius, sorted[0].group)}
        rows={sorted.map((x) => ({ label: x.label, value: `${x.celsius} °C`,
                                   color: _tempColor(x.celsius, x.group) }))} />);
  }
  if (d.disks.length) {
    const hottest = Math.max(...d.disks.map((x) => x.celsius));
    groups.push(
      <_IGroup key="disks" title={t("Disks")}
        rep={`${hottest} °C`} repColor={_tempColor(hottest, "disk")}
        rows={d.disks.map((x) => ({
          label: x.ok ? x.name : `${x.name} ⚠`,
          value: `${x.celsius} °C`,
          color: x.ok ? _tempColor(x.celsius, "disk") : "var(--bad)" }))} />);
  }
  if (d.fans.length) {
    const top = Math.max(...d.fans.map((f) => f.rpm));
    groups.push(
      <_IGroup key="fans" title={t("Fans")} rep={`${top.toLocaleString()} RPM`}
        rows={d.fans.map((f) => ({ label: f.label,
                                   value: f.rpm > 0 ? `${f.rpm.toLocaleString()} RPM` : t("stopped"),
                                   color: f.rpm > 0 ? "var(--ink)" : "var(--ink-3)" }))} />);
  }
  if (d.psus.length) {
    const total = d.psus.reduce((a, p) => a + (p.watts || 0), 0);
    const rows = [];
    d.psus.forEach((p) => {
      const dead = p.ok === false;
      rows.push({ label: p.label, value: p.watts != null ? `${p.watts.toFixed(1)} W` : "—",
                  color: dead ? "var(--bad)" : "var(--ink)" });
      const sub = [p.volts != null ? `${p.volts.toFixed(0)} V` : null,
                   p.celsius != null ? `${p.celsius} °C` : null,
                   p.rpm != null ? `${Math.round(p.rpm).toLocaleString()} RPM` : null]
                  .filter(Boolean).join(" · ");
      if (sub) rows.push({ label: "", value: sub, color: "var(--ink-3)" });
    });
    groups.push(<_IGroup key="psus" title={t("Power")} rep={`${total.toFixed(1)} W`} rows={rows} />);
  }

  return (
    <div className="card" style={{ borderColor: border }}>
      <div className="card-body">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12 }}>
          <div style={{ minWidth: 0 }}>
            <strong style={{ fontSize: 14 }}>{d.name}</strong>
            {d.critical && (
              <span style={{ marginLeft: 8, fontFamily: "var(--mono)", fontSize: 10,
                             color: "var(--ink-3)" }}>{t("single point")}</span>
            )}
            <div style={{ fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)", marginTop: 3 }}>
              {d.class}
            </div>
            <div style={{ fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>
              {d.ip} · {d.role}
            </div>
          </div>
          <div style={{ textAlign: "right", flexShrink: 0 }}>
            {d.up ? (
              <>
                <div className="num" style={{ fontSize: 26, color: hotColor }}>
                  {hot ? hot.celsius : "—"}<small style={{ fontSize: 12, opacity: 0.5 }}>°C</small>
                </div>
                <div style={{ fontFamily: "var(--mono)", fontSize: 10, color: "var(--ink-3)" }}>
                  {hot ? `${t("hottest")} · ${hot.label}` : ""}
                </div>
              </>
            ) : <_IAlert label={t("unreachable")} />}
          </div>
        </div>

        {!d.up ? (
          <div style={{ marginTop: 14, fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>
            {t("Scrape failed — values withheld rather than shown stale.")}
          </div>
        ) : (
          <>
            {groups.length > 0 && (
              <div className="sensor-grid" style={{ marginTop: 14 }}>{groups}</div>
            )}
            <div style={{ marginTop: 12, display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}>
              {faults.map((f) => <_IAlert key={f} label={f} />)}
              <span style={{ marginLeft: "auto", fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>
                {t("up")} {_fmtUptime(d.uptimeSec)}
              </span>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// hottest 只带回了标签,阈值判定要知道它属于哪一组 —— 回查一次即可
function _groupOf(d, label) {
  const t = d.temps.find((x) => x.label === label);
  if (t) return t.group;
  if (d.disks.some((x) => x.name === label)) return "disk";
  return "default";
}

function InfraSection() {
  useLive();
  const { t } = useLang();
  const devices = _iLive.infra || [];
  if (!devices.length) return null;              // 未声明 infra 设备 → 整节不渲染

  const down = devices.filter((d) => !d.up).length;
  const peak = devices
    .filter((d) => d.up && d.hottest)
    .reduce((a, d) => (a && a.hottest.celsius >= d.hottest.celsius ? a : d), null);

  return (
    <section className="page reveal" id="infra">
      <div className="eyebrow"><span className="num">08</span>{t("Infrastructure · environment")}</div>
      <div className="sect-head">
        <div>
          <h2 style={{ margin: 0 }}>{t("Everything else that ")}<em>{t("can overheat.")}</em></h2>
          <p className="lede" style={{ margin: "14px 0 0" }}>
            {t("The switch, the storage, the gateway — the boxes that carry the cluster but don't compute. Temperatures, fans, redundant PSUs and per-disk health, read straight off each device. No control path here; observation only.")}
          </p>
        </div>
        <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
          {down > 0 && <_IAlert label={`${down} ${t("unreachable")}`} />}
          {peak && (
            <span style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--ink-3)" }}>
              {t("peak")}{" "}
              <span className="num" style={{ color: _tempColor(peak.hottest.celsius, _groupOf(peak, peak.hottest.label)) }}>
                {peak.hottest.celsius} °C
              </span>{" "}
              · {peak.name}
            </span>
          )}
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 14 }}>
        {devices.map((d) => <_DeviceCard key={d.id} d={d} />)}
      </div>
    </section>
  );
}

Object.assign(window, { InfraSection });
