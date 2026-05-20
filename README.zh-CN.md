<div align="center">

# Hearth

**家庭 AI 算力集群的一屏式监控面板。**

为在家里跑 LLM 的人(一台机器或多台节点都行)做的自托管可观测性仪表盘。
vLLM、llama.cpp、SGLang、Ollama、LiteLLM 网关——**自动发现、真实指标、诚实标注**。

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#当前状态)

[English](README.md) · **简体中文** · [繁體中文](README.zh-TW.md)

<br>

![Hearth 仪表盘 — Apple Pro Display 风格](docs/screenshots/01-desktop-overview.png)

</div>

## 为什么是 Hearth

大多数家庭实验室监控**要么**通用(Grafana / Netdata 擅长主机指标但对 LLM 服务无感),**要么**专做 LLM 但以云为主(Phoenix、LangSmith)。Hearth 处在交集上:**一个仪表盘同时懂你的主机和你的模型**,为在自家 GPU 上跑 DeepSeek / Qwen / Gemma 的人设计。

视觉语言刻意做成 **Apple Pro Display 控制台**风格:深黑底、等宽数字、环形仪表、细边框。不是为了赶时髦,而是因为「密度 + 克制」是你每天扫五十次的遥测数据应有的语法。

---

## 它是什么

Hearth 在一个面板里给你看:

- **节点状态**——GPU / 显存 / CPU / 内存 / 温度 / 功率,实时刷新
- **模型状态**——哪些在服务、吞吐(t/s)、TTFT、TPOT、KV-cache 占用、p50/p95/p99,**自动从 LiteLLM 网关 / vLLM / llama.cpp 的 `/metrics` 发现**
- **网关流量**——最近请求、错误、延迟(**直接读 LiteLLM 开源版自带的 Postgres `SpendLogs`,不需要企业版**)
- **诚实标注空缺**——后端不暴露的指标(比如 llama.cpp 没有 TTFT 直方图),界面显示 `—`,**绝不伪造数字**

**目标场景**:家庭算力集群,1 到 ~10 节点,异构 GPU,可能跑多种推理框架,可能挂在 LiteLLM 网关后。**单机也能跑**。

## 当前状态

🏗️ **Alpha 阶段——活跃开发中。**

本项目最初是为一个 5 节点家庭集群做的个人监控,正在逐步通用化,以适配普通家庭算力场景。配置正在从硬编码常量迁移到声明式 YAML。详见 [`CHANGELOG.md`](CHANGELOG.md) 和下方路线图。

`v0.1.0-alpha` 已发布 — 配置即数据落地, 用上游的 5 节点真实集群按 YAML 配置端到端复测过, `/api/nodes`、`/api/models`、`/api/cluster` 与硬编码参考实现行为一致。欢迎试用; 边缘拓扑可能还有粗糙处, 待 `v0.2.0` 适配器层完善。

## 快速开始

> **前置条件**:跑 Hearth 的主机上有 Docker + Docker Compose。可选:每个 GPU 节点上装 Prometheus + DCGM exporter(没装也能用,Hearth 会优雅降级)。

```bash
git clone https://github.com/tonyliu312/hearth.git
cd hearth/server
cp .env.example .env          # 编辑 secret(LiteLLM master key 等)
docker compose up -d
open http://localhost:8080
```

多节点配置见 [`docs/topology.md`](docs/topology.md)(将于 v0.1.0 / P1 提供)。

## 功能

- 📊 **真实指标,绝不伪造**——每一个数字都源自真实后端;缺失的数据诚实标注
- 🔌 **自动发现**——模型、后端、up/down 状态从 LiteLLM 网关 `/health` + 直接探测各后端获得(网关抽风时也不会误判全停)
- 🌍 **多语言**——English、简体中文、繁體中文(欢迎 PR 加更多语言)
- 📱 **移动端友好**——响应式布局、移动端汉堡菜单
- 🎨 **Apple 风格审美**——深色主题、等宽数字、细边框
- 🔐 **设计上只读**——不控制模型、不影响生产(你继续用现有工具管模型,Hearth 只看)

## 监控范围(开箱即用)

| 后端 / 来源 | 当前状态 | 指标 | 用户友好度 |
|---|---|---|---|
| **vLLM** `/metrics` | ✅ 完整 | tps · TTFT · TPOT · KV% · p50/p95/p99 · 运行/等待 · 驻留 | 🟢 开箱即用 |
| **llama.cpp** `/metrics` | ✅ 部分(上游限制) | tps · TPOT · 运行/等待(TTFT/KV/p* 上游不暴露 — 显 `—`) | 🟢 开箱即用 |
| **LiteLLM 网关** `/health` + `/model/info` | ✅ 自动发现 | 模型列表、up/down、route → backend | 🟢 开箱即用 |
| **LiteLLM 网关** `LiteLLM_SpendLogs` Postgres | ✅ 只读 SELECT | 每请求日志:模型、状态、延迟、token | 🟢 开箱即用 |
| **网关健康但无 `/metrics`** | ✅ 诚实 "online" | 仅状态,绝不偽造数字 | 🟢 开箱即用 |
| **node_exporter + dcgm-exporter** (Prometheus) | ✅ 走你的 obs 栈 | CPU · 内存 · GPU 利用率 · 显存 · 网络 · 磁盘 · 温度 · 功率 | 🟢 开箱即用 |
| **SGLang** `sglang:*` | 🟡 显示为 "online" | 尚无详细指标 | 🟡 v0.2.0 适配器 |
| **Ollama** 原生 | 🟡 仅 OS 层 | 模型级指标缺失(Ollama 默认不暴露 `/metrics`) | 🟡 v0.2.0 适配器或把 Ollama 挂在 LiteLLM 后面 |
| **告警推送**(Telegram / LINE / ntfy / Slack…) | 🔴 尚未 | 告警规则触发到 UI,无推送渠道 | 🔴 v0.2.0 |

> **alpha 现实期望值**:今天最佳组合是 *LiteLLM 网关 + vLLM 与/或 llama.cpp + node_exporter + dcgm-exporter*。Hearth 就是在这套组合上开发和测过的。其他配置能用,但有上面注的 caveat。

**第一次用?** 看 [`docs/getting-started.md`](docs/getting-started.md) — 5 分钟从 `git clone` 跑到仪表盘的完整教程,含常见踩坑。

加新后端类型 = 加一个适配器文件即可。见 [`docs/adapters.md`](docs/adapters.md)(stub,完整指南 v0.2.0)。

## 截图

<table>
<tr>
<td width="50%"><a href="docs/screenshots/03-desktop-nodes.png"><img src="docs/screenshots/03-desktop-nodes.png" alt="节点视图"/></a><p align="center"><b>节点</b> — GPU / 显存 / CPU 环, 硬件指纹, 每台主机的实时温度和功率</p></td>
<td width="50%"><a href="docs/screenshots/04-desktop-models.png"><img src="docs/screenshots/04-desktop-models.png" alt="模型视图"/></a><p align="center"><b>模型</b> — LiteLLM 自动发现, 从 vLLM + llama.cpp <code>/metrics</code> 取真实 tps / TTFT / TPOT / KV</p></td>
</tr>
<tr>
<td><a href="docs/screenshots/02-desktop-cluster.png"><img src="docs/screenshots/02-desktop-cluster.png" alt="集群总览"/></a><p align="center"><b>集群</b> — Token 吞吐、集群功率、KV-cache 压力 — 脉冲图</p></td>
<td><a href="docs/screenshots/05-desktop-telemetry.png"><img src="docs/screenshots/05-desktop-telemetry.png" alt="遥测"/></a><p align="center"><b>遥测</b> — LiteLLM <code>SpendLogs</code> 请求流 + 告警引擎, "信号而非噪音"</p></td>
</tr>
<tr>
<td colspan="2" align="center">
<a href="docs/screenshots/06-mobile-overview.png"><img src="docs/screenshots/06-mobile-overview.png" alt="移动端总览" width="280"/></a>
<a href="docs/screenshots/07-mobile-cluster.png"><img src="docs/screenshots/07-mobile-cluster.png" alt="移动端集群" width="280"/></a>
<a href="docs/screenshots/08-mobile-models.png"><img src="docs/screenshots/08-mobile-models.png" alt="移动端模型" width="280"/></a>
<p align="center"><b>移动端</b> — 响应式布局, 汉堡菜单, 日志行省略号截断, 状态一眼可见</p>
</td>
</tr>
</table>

> 截图都来自一个真实运行的集群, 拓扑 / 主机名 / IP 已替换为通用占位符 (`Workstation`、`Inference-1..4`、`10.0.0.0/24`)。替换过程可复现 — 见 [`docs/screenshots/_capture.py`](docs/screenshots/_capture.py)。

## 路线图

**v0.1.0 — 配置即数据**(进行中)
- [x] Single `config/hearth.yaml` 取代 `server/api/main.py` 里的硬编码常量
- [x] 节点类型抽象(`discrete` / `unified-arm-soc` / `apple-silicon`),取代 GB10 特殊逻辑
- [x] 时区改由浏览器决定,移除硬编码 `Asia/Taipei`
- [x] `examples/` 拓扑预设(单 4090 / 双 A100 / 多节点异构)

**v0.2.0 — 适配器插件化**
- 可插拔的指标源适配器(vLLM / llama.cpp / SGLang / Ollama / 自定义 HTTP)
- 可插拔的告警渠道(Telegram / LINE / Pushover / ntfy / Slack / Discord / 邮件)

**v0.3.0 — 抛光**
- mkdocs 文档站
- 多架构 Docker 镜像(amd64 + arm64,覆盖 Jetson / Apple Silicon 主机)
- 语义化版本发布

## 贡献

详见 [`CONTRIBUTING.md`](CONTRIBUTING.md)。简言之:非琐碎改动先开 issue 讨论;遵循 [Conventional Commits](https://www.conventionalcommits.org/);善意沟通([`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md))。

## 安全

发现安全问题请**不要开公开 issue**。私下报告流程见 [`SECURITY.md`](SECURITY.md)。

## 许可证

[MIT](LICENSE) © Tony Liu and Hearth contributors。
