// AI-MONITOR · Reusable SVG chart primitives.
// All charts render from a numeric array (rolling history buffer).

const { useEffect, useMemo, useRef, useState, useLayoutEffect } = React;

// ── useElementSize ─────────────────────────────────────────────────────
function useElementSize() {
  const ref = useRef(null);
  const [size, setSize] = useState({ w: 0, h: 0 });
  useLayoutEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver((entries) => {
      const e = entries[0];
      if (e) setSize({ w: e.contentRect.width, h: e.contentRect.height });
    });
    ro.observe(ref.current);
    setSize({ w: ref.current.clientWidth, h: ref.current.clientHeight });
    return () => ro.disconnect();
  }, []);
  return [ref, size];
}

// ── Sparkline (no axis, no labels — pure shape) ────────────────────────
function Sparkline({ data, height = 32, color = "var(--accent)", fill = true, area = true, smooth = true, style }) {
  const [ref, { w, h }] = useElementSize();
  const gradId = useMemo(() => "g" + Math.random().toString(36).slice(2, 9), []);
  const H = h || height;
  if (!data || data.length < 2 || !w) {
    return <div ref={ref} style={{ width: "100%", height: H, ...style }} />;
  }
  const min = Math.min(...data);
  const max = Math.max(...data);
  const span = max - min || 1;
  const stepX = w / (data.length - 1);
  const yOf = (v) => H - 6 - ((v - min) / span) * (H - 12);

  // Build smooth path with catmull-rom-to-bezier
  let d = "";
  if (smooth) {
    const pts = data.map((v, i) => [i * stepX, yOf(v)]);
    d = `M ${pts[0][0]} ${pts[0][1]}`;
    for (let i = 0; i < pts.length - 1; i++) {
      const p0 = pts[i - 1] || pts[i];
      const p1 = pts[i];
      const p2 = pts[i + 1];
      const p3 = pts[i + 2] || p2;
      const c1x = p1[0] + (p2[0] - p0[0]) / 6;
      const c1y = p1[1] + (p2[1] - p0[1]) / 6;
      const c2x = p2[0] - (p3[0] - p1[0]) / 6;
      const c2y = p2[1] - (p3[1] - p1[1]) / 6;
      d += ` C ${c1x} ${c1y}, ${c2x} ${c2y}, ${p2[0]} ${p2[1]}`;
    }
  } else {
    d = data.map((v, i) => `${i ? "L" : "M"} ${i * stepX} ${yOf(v)}`).join(" ");
  }
  const last = data[data.length - 1];
  const fillD = area ? `${d} L ${w} ${H} L 0 ${H} Z` : null;

  return (
    <div ref={ref} style={{ width: "100%", height: H, position: "relative", ...style }}>
      <svg width="100%" height={H} viewBox={`0 0 ${w} ${H}`} preserveAspectRatio="none">
        <defs>
          <linearGradient id={gradId} x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.32} />
            <stop offset="100%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        {area && fill && <path d={fillD} fill={`url(#${gradId})`} />}
        <path d={d} fill="none" stroke={color} strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
        {/* leading dot */}
        <circle cx={w} cy={yOf(last)} r="2.2" fill={color}>
          <animate attributeName="r" values="2.2;3.6;2.2" dur="1.6s" repeatCount="indefinite" />
        </circle>
      </svg>
    </div>
  );
}

// ── Ring gauge ─────────────────────────────────────────────────────────
function Ring({ value = 0, max = 100, size = 70, stroke = 6, color, label, sub }) {
  const v = Math.max(0, Math.min(max, value));
  const r = (size - stroke) / 2;
  const c = 2 * Math.PI * r;
  const off = c * (1 - v / max);
  const auto = color || (v / max > 0.85 ? "var(--bad)" : v / max > 0.7 ? "var(--hot)" : v / max > 0.4 ? "var(--accent)" : "var(--ok)");
  return (
    <div>
      <svg width={size} height={size}>
        <circle cx={size/2} cy={size/2} r={r} fill="none" stroke="rgba(255,255,255,.08)" strokeWidth={stroke} />
        <circle cx={size/2} cy={size/2} r={r} fill="none" stroke={auto} strokeWidth={stroke}
                strokeLinecap="round"
                strokeDasharray={c}
                strokeDashoffset={off}
                transform={`rotate(-90 ${size/2} ${size/2})`}
                style={{ transition: "stroke-dashoffset .6s cubic-bezier(.2,.7,.2,1), stroke .3s" }} />
        <text x="50%" y="48%" textAnchor="middle" dominantBaseline="central"
              style={{ font: "500 14px/1 var(--display)", fill: "var(--ink)", fontVariantNumeric: "tabular-nums", letterSpacing: "-.02em" }}>
          {Math.round(v)}{max === 100 ? "%" : ""}
        </text>
        {sub && (
          <text x="50%" y="68%" textAnchor="middle" dominantBaseline="central"
                style={{ font: "500 8.5px/1 var(--mono)", fill: "var(--ink-3)", letterSpacing: ".08em", textTransform: "uppercase" }}>
            {sub}
          </text>
        )}
      </svg>
      {label && <div className="rl">{label}</div>}
    </div>
  );
}

// ── Area chart (axes optional) ─────────────────────────────────────────
function AreaChart({ series, height = 160, colors = ["var(--accent)"], yMax, padding = { l: 36, r: 10, t: 14, b: 22 }, ticks = 4, unit = "" }) {
  const [ref, { w }] = useElementSize();
  const gradIds = useMemo(
    () => (series || [0]).map((_, i) => "ag" + Math.random().toString(36).slice(2, 8) + i),
    [series ? series.length : 0]
  );
  const H = height;
  if (!series || !series.length || !series[0].length || !w) {
    return <div ref={ref} style={{ width: "100%", height: H }} />;
  }
  const flat = series.flat();
  const min = 0;
  const max = yMax || Math.max(1, Math.max(...flat) * 1.15);
  // 左 padding 自动撑大以容下最宽 y 轴标签 (整数位 + unit)：9px mono ~ 6px/digit,
  // unit 字符 ~5.5px/字, 标签尾端距 P.l 偏左 8px → 留 12px 余量。
  // 避免"l:36 + 数字 1234 t/s"那种被 viewBox 左缘截断只剩个位的窗口。
  const _labelW = String(Math.round(max)).length * 6 + (unit || "").length * 5.5 + 12;
  const P = { ...padding, l: Math.max(padding.l || 0, Math.ceil(_labelW)) };
  const innerW = w - P.l - P.r;
  const innerH = H - P.t - P.b;
  const span = max - min || 1;
  const stepX = (i, len) => P.l + (i / (len - 1)) * innerW;
  const yOf = (v) => P.t + innerH - ((v - min) / span) * innerH;

  function path(data) {
    const pts = data.map((v, i) => [stepX(i, data.length), yOf(v)]);
    let d = `M ${pts[0][0]} ${pts[0][1]}`;
    for (let i = 0; i < pts.length - 1; i++) {
      const p0 = pts[i - 1] || pts[i];
      const p1 = pts[i];
      const p2 = pts[i + 1];
      const p3 = pts[i + 2] || p2;
      const c1x = p1[0] + (p2[0] - p0[0]) / 6;
      const c1y = p1[1] + (p2[1] - p0[1]) / 6;
      const c2x = p2[0] - (p3[0] - p1[0]) / 6;
      const c2y = p2[1] - (p3[1] - p1[1]) / 6;
      d += ` C ${c1x} ${c1y}, ${c2x} ${c2y}, ${p2[0]} ${p2[1]}`;
    }
    return d;
  }

  const yTicks = Array.from({ length: ticks + 1 }, (_, i) => (max / ticks) * i);

  return (
    <div ref={ref} style={{ width: "100%", height: H }}>
      <svg width="100%" height={H} viewBox={`0 0 ${w} ${H}`}>
        <defs>
          {colors.map((c, i) => (
            <linearGradient key={i} id={gradIds[i]} x1="0" x2="0" y1="0" y2="1">
              <stop offset="0%" stopColor={c} stopOpacity={0.35} />
              <stop offset="100%" stopColor={c} stopOpacity={0} />
            </linearGradient>
          ))}
        </defs>
        {/* grid */}
        {yTicks.map((t, i) => (
          <g key={i}>
            <line x1={P.l} x2={w - P.r} y1={yOf(t)} y2={yOf(t)} stroke="var(--line)" strokeWidth="0.5" />
            <text x={P.l - 8} y={yOf(t)} textAnchor="end" dominantBaseline="central"
                  style={{ font: "500 9px/1 var(--mono)", fill: "var(--ink-4)", letterSpacing: ".06em" }}>
              {Math.round(t)}{unit}
            </text>
          </g>
        ))}
        {series.map((s, i) => (
          <g key={i}>
            <path d={`${path(s)} L ${w - P.r} ${H - P.b} L ${P.l} ${H - P.b} Z`} fill={`url(#${gradIds[i]})`} />
            <path d={path(s)} fill="none" stroke={colors[i] || colors[0]} strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
            <circle cx={w - P.r} cy={yOf(s[s.length - 1])} r="2.8" fill={colors[i] || colors[0]}>
              <animate attributeName="r" values="2.8;4.5;2.8" dur="1.8s" repeatCount="indefinite" />
            </circle>
          </g>
        ))}
      </svg>
    </div>
  );
}

// ── Stacked bar (horizontal) ───────────────────────────────────────────
function StackedBar({ segments, height = 6, total }) {
  const sum = total || segments.reduce((a, s) => a + s.value, 0);
  return (
    <div style={{ display: "flex", height, borderRadius: 3, overflow: "hidden", background: "rgba(255,255,255,.06)" }}>
      {segments.map((s, i) => (
        <div key={i} title={`${s.label}: ${s.value.toFixed(1)}`}
             style={{ width: `${(s.value / sum) * 100}%`, background: s.color, transition: "width .6s cubic-bezier(.2,.7,.2,1)" }} />
      ))}
    </div>
  );
}

// ── Heatmap (per-node × time) ──────────────────────────────────────────
function Heatmap({ rows, columns = 30, accessor }) {
  // rows: [{ label, data: number[] }], values 0–100
  const cellW = 100 / columns;
  return (
    <div style={{ display: "grid", gap: 6 }}>
      {rows.map((r, ri) => (
        <div key={ri} style={{ display: "grid", gridTemplateColumns: "84px 1fr 36px", gap: 10, alignItems: "center" }}>
          <div style={{ fontFamily: "var(--mono)", fontSize: 10.5, color: "var(--ink-3)" }}>{r.label}</div>
          <div style={{ display: "flex", gap: 1.5 }}>
            {r.data.slice(-columns).map((v, i) => {
              const a = Math.min(1, Math.max(0.05, v / 100));
              const hue = v > 85 ? 5 : v > 65 ? 28 : v > 35 ? 210 : 145;
              return <div key={i} style={{
                flex: 1, height: 14, borderRadius: 2,
                background: `oklch(0.62 0.18 ${hue} / ${a})`,
                transition: "background .25s",
              }} />;
            })}
          </div>
          <div className="num" style={{ fontSize: 11, color: "var(--ink-2)", textAlign: "right" }}>
            {Math.round(r.data[r.data.length - 1])}%
          </div>
        </div>
      ))}
    </div>
  );
}

// ── Animated counter on view ───────────────────────────────────────────
function useCountUp(target, duration = 1400, decimals = 0) {
  const [val, setVal] = useState(0);
  const startedRef = useRef(false);
  const ref = useRef(null);
  useEffect(() => {
    if (!ref.current) return;
    const io = new IntersectionObserver((entries) => {
      entries.forEach((e) => {
        if (e.isIntersecting && !startedRef.current) {
          startedRef.current = true;
          const t0 = performance.now();
          const tween = () => {
            const t = Math.min(1, (performance.now() - t0) / duration);
            // ease-out-quint
            const e2 = 1 - Math.pow(1 - t, 5);
            setVal(target * e2);
            if (t < 1) requestAnimationFrame(tween);
          };
          requestAnimationFrame(tween);
        }
      });
    }, { threshold: 0.2 });
    io.observe(ref.current);
    return () => io.disconnect();
  }, [target, duration]);
  return [ref, val];
}

function CountUp({ value, decimals = 0, suffix = "" }) {
  const [ref, v] = useCountUp(value, 1400, decimals);
  return <span ref={ref} className="num">{v.toFixed(decimals)}{suffix}</span>;
}

// ── i18n: hook + 地球仪语言切换器 ───────────────────────────────────
// useLang() 订阅 AII18N，语言切换时强制重渲染；返回翻译函数 t。
function useLang() {
  const [, force] = React.useState(0);
  React.useEffect(() => window.AII18N.subscribe(() => force((n) => n + 1)), []);
  return { t: window.AII18N.t, lang: window.AII18N.lang, setLang: window.AII18N.setLang };
}

function LangSwitcher() {
  const { lang, setLang } = useLang();
  const [open, setOpen] = React.useState(false);
  const LANGS = window.AII18N.LANGS;
  const cur = LANGS.find((l) => l.code === lang) || LANGS[0];
  React.useEffect(() => {
    if (!open) return;
    const close = () => setOpen(false);
    window.addEventListener("click", close);
    return () => window.removeEventListener("click", close);
  }, [open]);
  return (
    <div style={{ position: "relative" }} onClick={(e) => e.stopPropagation()}>
      <button onClick={() => setOpen((v) => !v)} title="Language · 语言 · 語言"
        style={{
          display: "inline-flex", alignItems: "center", gap: 6,
          background: "rgba(255,255,255,.04)", border: "0.5px solid var(--line)",
          color: "var(--ink-2)", font: "500 11px var(--mono)",
          padding: "5px 9px", borderRadius: 7, cursor: "pointer",
        }}>
        <span style={{ fontSize: 13, lineHeight: 1 }}>🌐</span>
        <span>{cur.flag}</span>
      </button>
      {open && (
        <div style={{
          position: "absolute", top: "calc(100% + 6px)", right: 0, zIndex: 50,
          background: "var(--bg-2, #131316)", border: "0.5px solid var(--line-2)",
          borderRadius: 9, padding: 5, minWidth: 132,
          boxShadow: "0 8px 28px rgba(0,0,0,.5)",
        }}>
          {LANGS.map((l) => (
            <button key={l.code} onClick={() => { setLang(l.code); setOpen(false); }}
              style={{
                display: "flex", alignItems: "center", gap: 9, width: "100%",
                background: l.code === lang ? "rgba(255,255,255,.06)" : "transparent",
                border: 0, color: l.code === lang ? "var(--ink)" : "var(--ink-2)",
                font: "500 12px var(--display)", padding: "7px 9px",
                borderRadius: 6, cursor: "pointer", textAlign: "left",
              }}>
              <span style={{ fontSize: 14 }}>{l.flag}</span>{l.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

Object.assign(window, { Sparkline, Ring, AreaChart, StackedBar, Heatmap, useCountUp, CountUp, useElementSize, useLang, LangSwitcher });
