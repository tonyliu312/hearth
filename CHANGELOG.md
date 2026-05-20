# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Configuration as data** — Hearth now loads topology from a single YAML file (`$HEARTH_CONFIG`, defaults to `/etc/hearth/config.yaml`). Single-host localhost default kicks in if the file is absent, so `docker compose up` works on a fresh install. Schema documented in [`docs/topology.md`](docs/topology.md); fully-commented template at [`config/hearth.example.yaml`](config/hearth.example.yaml).
- **Node `kind` abstraction** — every node declares `kind: discrete | unified-arm-soc | apple-silicon`. Hearth picks the right VRAM% source per kind (DCGM FB for discrete, MemAvailable for unified). Replaces the legacy hard-coded `obs_node != "rtx4090-pc"` check.
- **`examples/`** — three preset topologies: single-host, dual-GPU on one host, heterogeneous multi-node cluster (the upstream 5-node shape, generalized).
- **UI screenshots in README** (English / 简体中文 / 繁體中文) — 5 desktop sections + 3 mobile views, captured via a reproducible Playwright + Firefox pipeline with on-the-fly DOM redaction (`docs/screenshots/_capture.py`).
- **Translated READMEs** — `README.zh-CN.md` and `README.zh-TW.md` with locale-appropriate technical vocabulary.
- Initial open-source seed from the upstream personal monitor (5-node home cluster project).
- MIT license, `README.md`, `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, GitHub issue / PR templates, CI workflow.

### Changed

- **Timezone handled by the browser, not the server.** SQL emits ISO 8601 with `Z` suffix; the frontend renders with `toLocaleTimeString()` in the user's own timezone. Removed all `Asia/Taipei` hard-codes from the personal-version seed.
- **Atlas-specific code paths generalized.** What was `if n["id"] == "atlas"` is now `if n.get("node_source") == "direct"` — any node configured with a direct (non-Prometheus) scrape path qualifies, not just one specific host. What was `obs_live.get("rtx4090-pc")` is now driven by the first `kind: discrete` node found in config.
- App title: `"Tony 的家庭智算中心监控系统 API"` → `"Hearth API"` (v0.1.0).

### Pending (next minor)

- vLLM / SGLang / Ollama / llama.cpp adapter plugins (v0.2.0)
- Alert routing plugins (Telegram / LINE / Pushover / ntfy / Slack / Discord / email) (v0.2.0)
- Config hot-reload (v0.2.0)
- mkdocs documentation site + multi-arch Docker images (v0.3.0)

[Unreleased]: https://github.com/tonyliu312/hearth/compare/HEAD...HEAD
