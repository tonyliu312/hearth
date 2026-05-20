// Hearth · i18n (English fallback + zh-CN + zh-TW)
// 设计：t(英文原串) 查表；缺失键回退英文原串（零破坏）。
// 语言：en 🇬🇧 / zh-CN 🇨🇳 / zh-TW 🇹🇼。localStorage 持久化 aim.lang。
(() => {
  const LANGS = [
    { code: "en",    flag: "🇬🇧", label: "English" },
    { code: "zh-CN", flag: "🇨🇳", label: "简体中文" },
    { code: "zh-TW", flag: "🇹🇼", label: "繁體中文" },
  ];

  // 字典：key = 英文原串。en 用原串本身（表里不重复列）。
  const DICT = {
    "zh-CN": {
      // nav
      "Overview": "总览", "Cluster": "集群", "Nodes": "节点", "Models": "模型",
      "Telemetry": "遥测", "Fabric": "网络", "Search": "搜索",
      "5/5 nodes online": "5/5 节点在线", "reconnecting…": "重连中…",
      "mock data · no backend": "模拟数据 · 无后端", "LIVE": "实时", "MOCK": "模拟",
      // hero
      "Home": "家庭", "Compute Center": "智算中心",
      "A unified telemetry surface for the home AI fabric — RTX 4090 edge + four DGX Spark inference nodes, served behind a single LiteLLM gateway. Every TFLOP, every token, every watt — in real time.":
        "家庭 AI 集群的统一遥测面 —— RTX 4090 边缘节点 + 四台 DGX Spark 推理节点，统一经 LiteLLM 网关对外。每一次浮点、每一个 token、每一瓦特，全部实时。",
      "UPTIME": "运行时长", "REQUESTS": "请求数", "TOKENS SERVED": "已服务 Token", "SCRAPE": "抓取",
      // section eyebrows
      "Cluster · live telemetry": "集群 · 实时遥测",
      "Models · LiteLLM gateway": "模型 · LiteLLM 网关",
      "Telemetry · alerts": "遥测 · 告警", "Fabric · network": "网络 · 拓扑",
      // section h2 (拆分 普通 + <em> 两段)
      "Real-time pulse of the ": "实时脉搏 · ", "entire fabric.": "整个集群",
      "Five machines. ": "五台机器 · ", "Each a citizen.": "各为一员",
      "One endpoint. ": "一个入口 · ", "Every model.": "所有模型",
      "Signal, not ": "只见信号 · ", "noise.": "不见噪声",
      "The wires ": "万物 · ", "between everything.": "互联之线",
      // ledes
      "Aggregated utilization, throughput, and thermals across all five nodes — sampled at 1.2 s, retained for 7 days in Prometheus, surfaced here as a single coherent picture.":
        "五节点的利用率、吞吐与热数据聚合 —— 1.2 秒采样，Prometheus 留存 7 天，在此汇成一张连贯全景。",
      "Atlas — the RTX 4090 host — runs the LiteLLM gateway and edge-class workloads. Four DGX Spark boxes carry the heavy inference. Click a node for the full forensic view.":
        "Atlas（RTX 4090 主机）跑 LiteLLM 网关与边缘负载，四台 DGX Spark 承载重推理。点击任意节点查看完整取证视图。",
      "Atlas peers with every Spark over 10 GbE — North-South link intensity & pulse are driven by real node-exporter throughput. The 200 GbE ConnectX-7 Spark-to-Spark mesh is physical topology only (RDMA throughput not instrumented).":
        "Atlas 与各 Spark 经 10 GbE 互联 —— 南北向链路强度/脉冲由真实 node-exporter 网络吞吐驱动；Spark 间 200 GbE ConnectX-7 为物理拓扑（RDMA 吞吐未插桩，仅示意）。",
      // cards / labels
      "Resource pools": "资源池", "Aggregate capacity vs. live usage": "总容量 vs 实时用量",
      "Hosted models": "承载模型", "Running services": "运行服务", "System": "系统",
      "Quick actions": "快捷操作", "LiteLLM Gateway": "LiteLLM 网关",
      "Model": "模型", "Throughput": "吞吐", "State": "状态",
      "Accelerator activity": "加速器活跃度", "VRAM": "显存",
      // footer
      "Prometheus 2.55 · DCGM 3.3 · LiteLLM 1.52 · scrape 15 s · retention 7 d":
        "Prometheus 2.55 · DCGM 3.3 · LiteLLM 1.83 · 抓取 15 秒 · 留存 7 天",
      "All inference is routed through LiteLLM on ": "全部推理统一经 LiteLLM 路由 · ",
      " — OpenAI-compatible API, smart routing, fallbacks, cost & token accounting. Cold models spin up on demand to fit the VRAM budget.":
        " —— OpenAI 兼容 API、智能路由、回退、成本与 Token 计量。冷模型按需拉起以适配显存预算。",
      "Every request that lands at the gateway, every anomaly the rules engine catches — surfaced as a quiet, structured stream. No paging unless something actually needs you.":
        "每一个到达网关的请求、规则引擎捕获的每一处异常 —— 以安静、结构化的流呈现。除非真的需要你，否则不打扰。",
      "Models · LiteLLM gateway": "模型 · LiteLLM 网关",
      // PulseCards
      "Token throughput": "Token 吞吐", "tokens / sec · all models": "tokens/秒 · 全部模型",
      "Cluster power draw": "集群功耗", "watts · all PSUs": "瓦特 · 全部电源",
      "KV-cache pressure": "KV 缓存压力", "% utilization, weighted mean": "% 利用率 · 加权均值",
      // node detail
      "forensic view": "取证视图", "Accelerator activity · 60 ticks": "加速器活跃度 · 近 60 拍",
      "GPU temp": "GPU 温度", "Power draw": "功耗", "Disk usage": "磁盘占用",
      "Net In": "入向流量", "Net Out": "出向流量",
      "Hosted models": "承载模型", "Running services": "运行服务",
      "Kernel": "内核", "Driver": "驱动", "Net": "网络",
      "Hardware temps": "硬件温度",
      "— no temperature data (node unmanaged) —": "— 无温度数据（节点未纳管）—",
      "GPU pending · maintenance window": "GPU 待维护窗口纳管",
      "Open SSH session": "打开 SSH 会话", "Drain & cordon": "排空并隔离",
      "Restart daemons": "重启守护进程", "Pin model…": "固定模型…",
      "Run nvidia-smi dmon": "运行 nvidia-smi dmon", "Reboot": "重启节点",
      // model detail / table
      "Throughput · last 32 ticks": "吞吐 · 近 32 拍",
      "TTFT (first token)": "TTFT（首 token）", "TPOT (per token)": "TPOT（每 token）",
      "Requests / sec": "请求/秒",
      "Route · ": "路由 · ",
      "TTFT / TPOT": "TTFT / TPOT", "KV-cache": "KV 缓存",
      "P50 / P95 / P99": "P50 / P95 / P99", "Placement · framework": "部署 · 框架",
      "no live metrics source": "无实时指标源", "vLLM not exposed": "vLLM 未暴露",
      "vLLM + llama.cpp /metrics": "vLLM + llama.cpp /metrics", "Live throughput": "实时吞吐",
      "llama.cpp /metrics does not expose TTFT / KV% / e2e histograms · shown as — honestly":
        "llama.cpp /metrics 未暴露 TTFT / KV% / e2e 直方图 · 诚实显—，不伪造",
      "TPOT / KV-cache / error-rate: this vLLM build does not expose them → honestly left blank, not faked.":
        "TPOT / KV缓存 / 错误率：此 vLLM 构建 /metrics 未暴露 → 诚实留空，不伪造。",
      "backend has no Prometheus /metrics endpoint. No live-metrics source → honestly marked, not faked. Model still serves normally via LiteLLM route":
        "后端无 Prometheus /metrics 端点。实时指标无来源 → 诚实标注，不伪造。模型经 LiteLLM 路由正常可用",
      // gateway stats
      "Catalog models": "目录模型", "with live metrics": "有实时指标",
      "Live metric sources": "实时指标源", "vLLM native /metrics": "vLLM 原生 /metrics",
      "vLLM throughput": "vLLM 吞吐", "t/s · measured sum": "t/s · 实测合计",
      "LiteLLM metrics": "LiteLLM 指标", "enterprise": "企业版",
      "OSS not exposed · documented": "OSS 不暴露 · 已记录",
      "healthy": "健康", "streaming": "流式传输中",
      // filters / states
      "All": "全部", "RTX": "RTX", "DGX": "DGX", "Active": "活跃",
      "Serving": "服务中", "Idle": "空闲", "Chat": "对话",
      "serving": "服务中", "idle": "空闲", "no-metrics": "无指标",
      "online": "在线", "stopped": "已停",
      "loading": "加载中", "cold": "冷", "down": "离线",
      // cmdk
      "Search nodes, models, sections…": "搜索节点、模型、板块…",
      "Section · top of page": "板块 · 页面顶部",
      "Section · aggregate telemetry": "板块 · 聚合遥测",
      "Section · five machines": "板块 · 五台机器",
      "Section · LiteLLM gateway": "板块 · LiteLLM 网关",
      "Section · alerts & log stream": "板块 · 告警与日志流",
      "Section · network topology": "板块 · 网络拓扑",
      "No results": "无结果",
      // tweaks
      "Tweaks": "偏好设置", "Appearance": "外观", "Theme": "主题",
      "Dark": "深色", "Light": "浅色", "Accent": "强调色", "Density": "密度",
      "Compact": "紧凑", "Regular": "标准", "Comfy": "宽松",
      "Live data": "实时数据", "Source": "数据源", "Auto": "自动",
      "Live": "实时", "Mock": "模拟", "Tick": "刷新间隔",
      "Motion / animations": "动效 / 动画", "Shortcut": "快捷键",
      "Press": "按", "anywhere to open the command palette.": "可在任意处打开命令面板。",
      "Click any node or model row for the forensic view.": "点击任意节点或模型行进入取证视图。",
      "Framework": "框架", "Port": "端口", "Quant": "量化", "Context": "上下文",
      "Placement": "部署", "Open Playground": "打开 Playground",
      "Restart": "重启", "Logs": "日志", "Close": "关闭",
      "— No models pinned to this node —": "— 无模型固定到此节点 —",
      "ONLINE": "在线", "Power": "功率", "maint. pending": "待维护纳管",
      "LiteLLM request log · OSS Postgres": "LiteLLM 请求日志 · OSS Postgres",
      "East-West · 200 GbE ConnectX-7 RDMA (live)": "东西向 · 200 GbE ConnectX-7 RDMA（真实）",
      "Per-node": "各节点", "heatmap": "热力图",
      "Accelerator activity · last 60 ticks · 0–100%": "加速器活跃度 · 近 60 拍 · 0–100%",
      "VRAM usage · last 60 ticks · 0–100%": "显存占用 · 近 60 拍 · 0–100%",
      "East-West · 200 GbE ConnectX-7 (topology only, throughput not instrumented)": "东西向 · 200 GbE ConnectX-7（仅拓扑，吞吐未插桩）",
      "North-South pulse ∝ real network throughput": "南北向脉冲 ∝ 真实网络吞吐",
      "Hearth": "Hearth · 家庭算力监控",
      "Home AI Compute Center": "家庭智算中心",
      "Home AI Compute Monitor": "家庭 AI 算力监控",
      // lang switcher
      "Language": "语言",
    },
    "zh-TW": {
      "Overview": "總覽", "Cluster": "叢集", "Nodes": "節點", "Models": "模型",
      "Telemetry": "遙測", "Fabric": "網路", "Search": "搜尋",
      "5/5 nodes online": "5/5 節點在線", "reconnecting…": "重連中…",
      "mock data · no backend": "模擬資料 · 無後端", "LIVE": "即時", "MOCK": "模擬",
      "Home": "家庭", "Compute Center": "智算中心",
      "A unified telemetry surface for the home AI fabric — RTX 4090 edge + four DGX Spark inference nodes, served behind a single LiteLLM gateway. Every TFLOP, every token, every watt — in real time.":
        "家庭 AI 叢集的統一遙測面 —— RTX 4090 邊緣節點 + 四台 DGX Spark 推理節點，統一經 LiteLLM 閘道對外。每一次浮點、每一個 token、每一瓦特，全部即時。",
      "UPTIME": "運行時長", "REQUESTS": "請求數", "TOKENS SERVED": "已服務 Token", "SCRAPE": "擷取",
      "Cluster · live telemetry": "叢集 · 即時遙測",
      "Models · LiteLLM gateway": "模型 · LiteLLM 閘道",
      "Telemetry · alerts": "遙測 · 告警", "Fabric · network": "網路 · 拓撲",
      "Real-time pulse of the ": "即時脈搏 · ", "entire fabric.": "整個叢集",
      "Five machines. ": "五台機器 · ", "Each a citizen.": "各為一員",
      "One endpoint. ": "一個入口 · ", "Every model.": "所有模型",
      "Signal, not ": "只見訊號 · ", "noise.": "不見雜訊",
      "The wires ": "萬物 · ", "between everything.": "互聯之線",
      "Aggregated utilization, throughput, and thermals across all five nodes — sampled at 1.2 s, retained for 7 days in Prometheus, surfaced here as a single coherent picture.":
        "五節點的利用率、吞吐與熱數據聚合 —— 1.2 秒取樣，Prometheus 留存 7 天，在此匯成一張連貫全景。",
      "Atlas — the RTX 4090 host — runs the LiteLLM gateway and edge-class workloads. Four DGX Spark boxes carry the heavy inference. Click a node for the full forensic view.":
        "Atlas（RTX 4090 主機）跑 LiteLLM 閘道與邊緣負載，四台 DGX Spark 承載重推理。點擊任一節點查看完整取證視圖。",
      "Atlas peers with every Spark over 10 GbE — North-South link intensity & pulse are driven by real node-exporter throughput. The 200 GbE ConnectX-7 Spark-to-Spark mesh is physical topology only (RDMA throughput not instrumented).":
        "Atlas 與各 Spark 經 10 GbE 互聯 —— 南北向鏈路強度/脈衝由真實 node-exporter 網路吞吐驅動；Spark 間 200 GbE ConnectX-7 為實體拓撲（RDMA 吞吐未插樁，僅示意）。",
      "Resource pools": "資源池", "Aggregate capacity vs. live usage": "總容量 vs 即時用量",
      "Hosted models": "承載模型", "Running services": "執行服務", "System": "系統",
      "Quick actions": "快捷操作", "LiteLLM Gateway": "LiteLLM 閘道",
      "Model": "模型", "Throughput": "吞吐", "State": "狀態",
      "Accelerator activity": "加速器活躍度", "VRAM": "顯存",
      "Prometheus 2.55 · DCGM 3.3 · LiteLLM 1.52 · scrape 15 s · retention 7 d":
        "Prometheus 2.55 · DCGM 3.3 · LiteLLM 1.83 · 擷取 15 秒 · 留存 7 天",
      "All inference is routed through LiteLLM on ": "全部推理統一經 LiteLLM 路由 · ",
      " — OpenAI-compatible API, smart routing, fallbacks, cost & token accounting. Cold models spin up on demand to fit the VRAM budget.":
        " —— OpenAI 相容 API、智慧路由、回退、成本與 Token 計量。冷模型按需拉起以適配顯存預算。",
      "Every request that lands at the gateway, every anomaly the rules engine catches — surfaced as a quiet, structured stream. No paging unless something actually needs you.":
        "每一個到達閘道的請求、規則引擎捕獲的每一處異常 —— 以安靜、結構化的流呈現。除非真的需要你，否則不打擾。",
      "Token throughput": "Token 吞吐", "tokens / sec · all models": "tokens/秒 · 全部模型",
      "Cluster power draw": "叢集功耗", "watts · all PSUs": "瓦特 · 全部電源",
      "KV-cache pressure": "KV 快取壓力", "% utilization, weighted mean": "% 使用率 · 加權均值",
      "forensic view": "取證視圖", "Accelerator activity · 60 ticks": "加速器活躍度 · 近 60 拍",
      "GPU temp": "GPU 溫度", "Power draw": "功耗", "Disk usage": "磁碟佔用",
      "Net In": "入向流量", "Net Out": "出向流量",
      "Hosted models": "承載模型", "Running services": "執行服務",
      "Kernel": "核心", "Driver": "驅動", "Net": "網路",
      "Hardware temps": "硬體溫度",
      "— no temperature data (node unmanaged) —": "— 無溫度資料（節點未納管）—",
      "GPU pending · maintenance window": "GPU 待維護窗口納管",
      "Open SSH session": "開啟 SSH 工作階段", "Drain & cordon": "排空並隔離",
      "Restart daemons": "重啟守護程序", "Pin model…": "固定模型…",
      "Run nvidia-smi dmon": "執行 nvidia-smi dmon", "Reboot": "重啟節點",
      "Throughput · last 32 ticks": "吞吐 · 近 32 拍",
      "TTFT (first token)": "TTFT（首 token）", "TPOT (per token)": "TPOT（每 token）",
      "Requests / sec": "請求/秒",
      "Route · ": "路由 · ",
      "TTFT / TPOT": "TTFT / TPOT", "KV-cache": "KV 快取",
      "P50 / P95 / P99": "P50 / P95 / P99", "Placement · framework": "部署 · 框架",
      "no live metrics source": "無即時指標來源", "vLLM not exposed": "vLLM 未暴露",
      "vLLM + llama.cpp /metrics": "vLLM + llama.cpp /metrics", "Live throughput": "即時吞吐",
      "llama.cpp /metrics does not expose TTFT / KV% / e2e histograms · shown as — honestly":
        "llama.cpp /metrics 未暴露 TTFT / KV% / e2e 直方圖 · 誠實顯—,不偽造",
      "TPOT / KV-cache / error-rate: this vLLM build does not expose them → honestly left blank, not faked.":
        "TPOT / KV快取 / 錯誤率：此 vLLM 建置 /metrics 未暴露 → 誠實留空，不偽造。",
      "backend has no Prometheus /metrics endpoint. No live-metrics source → honestly marked, not faked. Model still serves normally via LiteLLM route":
        "後端無 Prometheus /metrics 端點。即時指標無來源 → 誠實標註，不偽造。模型經 LiteLLM 路由正常可用",
      "Catalog models": "目錄模型", "with live metrics": "有即時指標",
      "Live metric sources": "即時指標來源", "vLLM native /metrics": "vLLM 原生 /metrics",
      "vLLM throughput": "vLLM 吞吐", "t/s · measured sum": "t/s · 實測合計",
      "LiteLLM metrics": "LiteLLM 指標", "enterprise": "企業版",
      "OSS not exposed · documented": "OSS 不暴露 · 已記錄",
      "healthy": "健康", "streaming": "串流中",
      "All": "全部", "RTX": "RTX", "DGX": "DGX", "Active": "活躍",
      "Serving": "服務中", "Idle": "閒置", "Chat": "對話",
      "serving": "服務中", "idle": "閒置", "no-metrics": "無指標",
      "online": "在線", "stopped": "已停",
      "loading": "載入中", "cold": "冷", "down": "離線",
      "Search nodes, models, sections…": "搜尋節點、模型、區塊…",
      "Section · top of page": "區塊 · 頁面頂部",
      "Section · aggregate telemetry": "區塊 · 聚合遙測",
      "Section · five machines": "區塊 · 五台機器",
      "Section · LiteLLM gateway": "區塊 · LiteLLM 閘道",
      "Section · alerts & log stream": "區塊 · 告警與日誌流",
      "Section · network topology": "區塊 · 網路拓撲",
      "No results": "無結果",
      "Tweaks": "偏好設定", "Appearance": "外觀", "Theme": "主題",
      "Dark": "深色", "Light": "淺色", "Accent": "強調色", "Density": "密度",
      "Compact": "緊湊", "Regular": "標準", "Comfy": "寬鬆",
      "Live data": "即時資料", "Source": "資料來源", "Auto": "自動",
      "Live": "即時", "Mock": "模擬", "Tick": "刷新間隔",
      "Motion / animations": "動效 / 動畫", "Shortcut": "快捷鍵",
      "Press": "按", "anywhere to open the command palette.": "可在任意處開啟命令面板。",
      "Click any node or model row for the forensic view.": "點擊任一節點或模型列進入取證視圖。",
      "Framework": "框架", "Port": "連接埠", "Quant": "量化", "Context": "上下文",
      "Placement": "部署", "Open Playground": "開啟 Playground",
      "Restart": "重啟", "Logs": "日誌", "Close": "關閉",
      "— No models pinned to this node —": "— 無模型固定到此節點 —",
      "ONLINE": "在線", "Power": "功率", "maint. pending": "待維護納管",
      "LiteLLM request log · OSS Postgres": "LiteLLM 請求日誌 · OSS Postgres",
      "East-West · 200 GbE ConnectX-7 RDMA (live)": "東西向 · 200 GbE ConnectX-7 RDMA（真實）",
      "Per-node": "各節點", "heatmap": "熱力圖",
      "Accelerator activity · last 60 ticks · 0–100%": "加速器活躍度 · 近 60 拍 · 0–100%",
      "VRAM usage · last 60 ticks · 0–100%": "顯存佔用 · 近 60 拍 · 0–100%",
      "East-West · 200 GbE ConnectX-7 (topology only, throughput not instrumented)": "東西向 · 200 GbE ConnectX-7（僅拓撲，吞吐未插樁）",
      "North-South pulse ∝ real network throughput": "南北向脈衝 ∝ 真實網路吞吐",
      "Hearth": "Hearth · 家庭算力监控",
      "Home AI Compute Center": "家庭智算中心",
      "Home AI Compute Monitor": "家庭 AI 算力監控",
      "Language": "語言",
    },
  };

  const saved = localStorage.getItem("aim.lang");
  const navZh = (navigator.language || "en").toLowerCase().startsWith("zh");
  let lang = saved || (navZh ? "zh-CN" : "en");
  if (!LANGS.some((l) => l.code === lang)) lang = "en";

  const subs = new Set();
  function t(s) {
    if (lang === "en" || s == null) return s;
    const m = DICT[lang];
    return (m && m[s] !== undefined) ? m[s] : s;  // 缺失回退英文原串
  }
  function setLang(code) {
    if (!LANGS.some((l) => l.code === code) || code === lang) return;
    lang = code;
    localStorage.setItem("aim.lang", code);
    subs.forEach((fn) => fn(code));
  }
  window.AII18N = {
    LANGS,
    get lang() { return lang; },
    t, setLang,
    subscribe(fn) { subs.add(fn); return () => subs.delete(fn); },
  };
})();
