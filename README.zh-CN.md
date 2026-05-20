<div align="center">

# Hearth

**家庭 AI 算力集群的一屏式监控面板。**

为在家里跑 LLM 的人(一台机器或多台节点都行)做的自托管可观测性仪表盘。
vLLM、llama.cpp、SGLang、Ollama、LiteLLM 网关——**自动发现、真实指标、诚实标注**。

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#当前状态)

[English](README.md) · **简体中文** · [繁體中文](README.zh-TW.md)

</div>

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

如果你想要今天就稳定可用,请等 `v0.1.0`。如果你想边用边跟、贡献代码、提反馈——欢迎。

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

| 后端类型 | 探测方式 | 指标 |
|---|---|---|
| **vLLM** | 探 `/metrics` 找 `vllm:*` 计数器 | tps、TTFT、TPOT、KV%、p50/p95/p99(e2e)、运行中请求数、等待数、驻留状态 |
| **llama.cpp** | 探 `/metrics` 找 `llamacpp:*` 计数器 | tps、TPOT、运行中、等待数(TTFT/KV/p* 上游不暴露——显示为 `—`) |
| **网关健康但无 `/metrics`** | 退到 `/v1/models` 探活 | "online" 状态,无详细指标 |
| **节点 OS** | Prometheus + node_exporter + DCGM | CPU、内存、GPU 利用率、显存、网络、磁盘、温度、功率 |
| **网关日志** | LiteLLM 开源 Postgres `LiteLLM_SpendLogs` | 最近请求、状态、延迟、模型 |

加新后端类型 = 加一个适配器文件即可。见 [`docs/adapters.md`](docs/adapters.md)(规划中)。

## 路线图

**v0.1.0 — 配置即数据**(进行中)
- [ ] `config/nodes.yaml` + `config/models.yaml` 取代 `server/api/main.py` 里的硬编码常量
- [ ] 节点类型抽象(`discrete` / `unified-arm-soc` / `apple-silicon`),取代 GB10 特殊逻辑
- [ ] 时区自动探测浏览器,移除硬编码 `Asia/Taipei`
- [ ] `examples/` 拓扑预设(单 4090 / 双 A100 / 多节点异构)

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
