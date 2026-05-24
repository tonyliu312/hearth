# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Alert push channels** — node-down / GPU-overheat / memory / disk / gateway-error alerts now push to ntfy, Telegram, Discord, Slack, or a generic webhook. Fires on state *transition* (healthy→firing, firing→resolved) only — no per-tick spam. State persists across restart. Configure via `alerts:` in `config/hearth.yaml`; secrets via env vars. End-to-end tested through ntfy.sh. Docs: [`docs/alerts.md`](docs/alerts.md).
- Alert rule engine messages translated to English with stable `key`s (node:rule) for transition detection.

### Fixed

- **Honest node online/offline status.** NodeCard previously hard-coded "ONLINE" + a green dot for every node, regardless of reality — a powered-off node still showed ONLINE. Now reads the backend's authoritative `up` flag: offline nodes show "OFFLINE", a red dot, and the card dims. The nav "5/5 nodes online" was likewise hard-coded; it now computes the real online/total count and the status dot goes amber when degraded. (Backend was always honest — `up: false` for unreachable nodes — only the frontend was faking it.)

## [v0.1.1-alpha] — 2026-05-20

**Documentation + usability polish on top of v0.1.0-alpha.** No code change to the API or UI; this release is about lowering the on-ramp.

### Added

- **`docs/getting-started.md`** — A 5-minute walkthrough from `git clone` to a running dashboard, including:
  - The exact `node_exporter` / `dcgm-exporter` install commands
  - A multi-node extension example
  - An "Ollama-only" section that honestly says what works and what doesn't
  - Five common-gotcha entries with concrete fixes

### Changed

- **README "What it monitors" table redesigned** (en / zh-CN / zh-TW). Now a 4-column matrix: backend · today's status · metrics · OSS-user fit. Each row tagged 🟢 drop-in / 🟡 partial-or-workaround / 🔴 not-yet. Honest framing replaces the previous best-case-only table.
- Added "alpha reality check" callout: best fit today is *LiteLLM gateway + vLLM and/or llama.cpp + node_exporter + dcgm-exporter*. Other configurations work with caveats.
- Quick-start link to `docs/getting-started.md` surfaced from each README so new visitors land in the tutorial within one click.

### Why this is a separate release

The post-public audit surfaced that v0.1.0-alpha was *technically* shippable but *practically* under-documented for the long-tail audience (Ollama-only home labs, SGLang, alert-push users). Rather than letting them bounce, this release tells them up front what works and what's planned. The roadmap to fix the underlying gaps (Ollama adapter, SGLang adapter, alert channels) is still v0.2.0.

## [v0.1.0-alpha] — 2026-05-20

First alpha after P1 (configuration as data). End-to-end re-verified against
the upstream 5-node home cluster: `/api/nodes`, `/api/models`, `/api/cluster`
return identical results to the hardcoded reference implementation.


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
