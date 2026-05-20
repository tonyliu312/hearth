<div align="center">

# Hearth

**家庭 AI 算力叢集的單畫面監控儀表板。**

為在家裡跑 LLM 的人(一台機器或多台節點都可以)而做的自架可觀測性儀表板。
vLLM、llama.cpp、SGLang、Ollama、LiteLLM 閘道——**自動發現、真實指標、誠實標註**。

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#目前狀態)

[English](README.md) · [简体中文](README.zh-CN.md) · **繁體中文**

</div>

---

## 它是什麼

Hearth 在一個面板裡呈現:

- **節點狀態**——GPU / 顯記憶體 / CPU / 記憶體 / 溫度 / 功率,即時更新
- **模型狀態**——哪些在服務、吞吐(t/s)、TTFT、TPOT、KV-cache 佔用、p50/p95/p99,**自動從 LiteLLM 閘道 / vLLM / llama.cpp 的 `/metrics` 探得**
- **閘道流量**——近期請求、錯誤、延遲(**直接讀 LiteLLM 開源版自帶的 Postgres `SpendLogs`,不需要企業版**)
- **誠實標註空缺**——後端不暴露的指標(例如 llama.cpp 沒有 TTFT 直方圖),介面顯示 `—`,**絕不偽造數字**

**目標場景**:家庭算力叢集,1 到 ~10 節點,異構 GPU,可能跑多種推論框架,可能掛在 LiteLLM 閘道後。**單機也能跑**。

## 目前狀態

🏗️ **Alpha 階段——積極開發中。**

本專案最初是為一個 5 節點的家庭叢集而做的個人監控,正逐步通用化以適用一般家用算力場景。設定正從硬寫常數遷移到聲明式 YAML。詳見 [`CHANGELOG.md`](CHANGELOG.md) 與下方路線圖。

如果你想要今天就穩定可用,請等 `v0.1.0`。如果你願意一邊用一邊跟進、貢獻程式碼、提供回饋——歡迎你。

## 快速開始

> **前置條件**:跑 Hearth 的主機上要有 Docker + Docker Compose。可選:在每個 GPU 節點上裝 Prometheus + DCGM exporter(沒裝也能用,Hearth 會優雅降級)。

```bash
git clone https://github.com/tonyliu312/hearth.git
cd hearth/server
cp .env.example .env          # 編輯密鑰(LiteLLM master key 等)
docker compose up -d
open http://localhost:8080
```

多節點設定詳見 [`docs/topology.md`](docs/topology.md)(於 v0.1.0 / P1 提供)。

## 功能

- 📊 **真實指標,絕不偽造**——每一個數字都源自真實後端;缺失的資料誠實標註
- 🔌 **自動發現**——模型、後端、up/down 狀態由 LiteLLM 閘道 `/health` + 直接探測各後端取得(閘道偶發抽風時也不會誤判全部下線)
- 🌍 **多語言**——English、简体中文、繁體中文(歡迎 PR 增加其他語言)
- 📱 **行動裝置友善**——響應式佈局、行動端漢堡選單
- 🎨 **Apple 風格美學**——深色主題、等寬數字、細邊框
- 🔐 **設計上唯讀**——不控制模型、不影響生產(你繼續用現有工具管模型,Hearth 只看)

## 監控範圍(開箱即用)

| 後端類型 | 探測方式 | 指標 |
|---|---|---|
| **vLLM** | 探 `/metrics` 找 `vllm:*` 計數器 | tps、TTFT、TPOT、KV%、p50/p95/p99(e2e)、運行中請求數、等待數、駐留狀態 |
| **llama.cpp** | 探 `/metrics` 找 `llamacpp:*` 計數器 | tps、TPOT、運行中、等待數(TTFT/KV/p* 上游不暴露——顯示為 `—`) |
| **閘道健康但無 `/metrics`** | 退到 `/v1/models` 探活 | "online" 狀態,無詳細指標 |
| **節點 OS** | Prometheus + node_exporter + DCGM | CPU、記憶體、GPU 使用率、顯記憶體、網路、磁碟、溫度、功率 |
| **閘道日誌** | LiteLLM 開源 Postgres `LiteLLM_SpendLogs` | 近期請求、狀態、延遲、模型 |

加新後端類型 = 加一個轉接器檔案即可。詳見 [`docs/adapters.md`](docs/adapters.md)(規劃中)。

## 路線圖

**v0.1.0 — 設定即資料**(進行中)
- [ ] `config/nodes.yaml` + `config/models.yaml` 取代 `server/api/main.py` 內的硬寫常數
- [ ] 節點類型抽象(`discrete` / `unified-arm-soc` / `apple-silicon`),取代 GB10 特殊邏輯
- [ ] 時區改由瀏覽器自動偵測,移除硬寫 `Asia/Taipei`
- [ ] `examples/` 拓樸預設(單 4090 / 雙 A100 / 多節點異構)

**v0.2.0 — 轉接器外掛化**
- 可插拔的指標來源轉接器(vLLM / llama.cpp / SGLang / Ollama / 自訂 HTTP)
- 可插拔的告警通道(Telegram / LINE / Pushover / ntfy / Slack / Discord / 電子郵件)

**v0.3.0 — 拋光**
- mkdocs 文件網站
- 多架構 Docker 映像檔(amd64 + arm64,涵蓋 Jetson / Apple Silicon 主機)
- 語意化版本發佈

## 貢獻

詳見 [`CONTRIBUTING.md`](CONTRIBUTING.md)。簡言之:非瑣碎變更請先開 issue 討論;遵循 [Conventional Commits](https://www.conventionalcommits.org/);善意溝通([`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md))。

## 安全性

發現安全性問題請**不要開公開 issue**。私下回報流程見 [`SECURITY.md`](SECURITY.md)。

## 授權

[MIT](LICENSE) © Tony Liu 與 Hearth 貢獻者。
