<div align="center">

# Hearth

**One-pane-of-glass monitoring for your home AI compute cluster.**

A self-hosted observability dashboard for people who run LLMs at home — one box or many.
vLLM, llama.cpp, SGLang, Ollama, LiteLLM gateway — auto-discovered, real metrics, honestly labeled.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange.svg)](#status)

**English** · [简体中文](README.zh-CN.md) · [繁體中文](README.zh-TW.md)

</div>

---

## What it is

Hearth shows you, in one place:

- **Your nodes** — GPU / VRAM / CPU / RAM / temps / power, real-time
- **Your models** — which are serving, throughput (t/s), TTFT, TPOT, KV-cache, p50/p95/p99 — auto-discovered from your LiteLLM gateway, vLLM, or llama.cpp `/metrics`
- **Your gateway traffic** — recent requests, errors, latencies (reads LiteLLM's OSS Postgres `SpendLogs` directly — no enterprise license needed)
- **Honest gaps** — when a backend doesn't expose a metric (e.g. llama.cpp has no TTFT histogram), it shows `—`, not a fake number

Designed for the **home compute cluster** target: 1 to ~10 nodes, mixed GPUs, mixed serving frameworks, possibly behind a LiteLLM gateway. Single-machine setup works too.

## Status

🏗️ **Alpha — under active development.**

This project was originally built as a personal monitor for a 5-node home cluster and is being progressively generalized for general home-lab use. Configuration is being moved from hard-coded constants to declarative YAML. See [`CHANGELOG.md`](CHANGELOG.md) and the roadmap below.

If you want something stable today, wait for `v0.1.0`. If you want to follow along / contribute / file feedback — welcome.

## Quick start

> **Prerequisites:** Docker + Docker Compose on the host that will run Hearth. Optional: Prometheus + DCGM exporter on each GPU node (Hearth degrades gracefully if absent).

```bash
git clone https://github.com/tonyliu312/hearth.git
cd hearth/server
cp .env.example .env          # edit secrets (LiteLLM master key, etc.)
docker compose up -d
open http://localhost:8080
```

For multi-node configuration, see [`docs/topology.md`](docs/topology.md) (coming in v0.1.0 / P1).

## Features

- 📊 **Real metrics, no fakes** — every number shown is sourced from a real backend; missing data is honestly labeled
- 🔌 **Auto-discovery** — models, backends, and their up/down state are discovered from the LiteLLM gateway `/health` + direct backend probes (resilient if gateway flaps)
- 🌍 **Multi-language** — English, 简体中文, 繁體中文 (PRs welcome for more)
- 📱 **Mobile-friendly** — responsive layout, mobile hamburger nav
- 🎨 **Apple-style aesthetic** — dark theme, tabular numerals, subtle borders
- 🔐 **Read-only by design** — no model control, no production impact (you keep using your existing tools to manage models)

## What it monitors (out of the box)

| Backend type | Detection | Metrics |
|---|---|---|
| **vLLM** | Probes `/metrics` for `vllm:*` counters | tps, TTFT, TPOT, KV%, p50/p95/p99 (e2e), running, waiting, resident |
| **llama.cpp** | Probes `/metrics` for `llamacpp:*` counters | tps, TPOT, running, waiting (TTFT/KV/p* not exposed by upstream — shown as `—`) |
| **Gateway-healthy, no `/metrics`** | Falls back to `/v1/models` reachability | "online" state, no detail metrics |
| **Node OS** | Prometheus + node_exporter + DCGM | CPU, RAM, GPU util, VRAM, network, disk, temperatures, power |
| **Gateway logs** | LiteLLM OSS Postgres `LiteLLM_SpendLogs` | Recent requests, status, latency, model |

Adding a new backend type = one adapter file. See [`docs/adapters.md`](docs/adapters.md) (coming).

## Roadmap

**v0.1.0 — Configuration as data** *(in progress)*
- [ ] `config/nodes.yaml` + `config/models.yaml` replace hard-coded constants in `server/api/main.py`
- [ ] Node-type abstraction (`discrete` / `unified-arm-soc` / `apple-silicon`) instead of GB10 specials
- [ ] Timezone auto-detect from browser instead of hard-coded
- [ ] `examples/` topology presets (single-4090, dual-A100, multi-node-heterogeneous)

**v0.2.0 — Adapter plugins**
- Pluggable metrics-source adapters (vLLM / llama.cpp / SGLang / Ollama / custom HTTP)
- Pluggable alert channels (Telegram / LINE / Pushover / ntfy / Slack / Discord / email)

**v0.3.0 — Polish**
- mkdocs documentation site
- Multi-arch Docker images (amd64 + arm64 for Jetson / Apple Silicon hosts)
- Tagged releases with semantic versioning

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Briefly: open an issue first for non-trivial changes, follow [Conventional Commits](https://www.conventionalcommits.org/), be kind ([`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)).

## Security

If you find a security issue, please **don't open a public issue**. See [`SECURITY.md`](SECURITY.md) for private disclosure.

## License

[MIT](LICENSE) © Tony Liu and Hearth contributors.
