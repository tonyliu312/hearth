# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Topology-driven UI scale — no more hardcoded "5"** — Nodes section grid, hero subtitle, lede and command-palette hint all now render from `NODES.length` instead of the historical 5-node assumption. CSS gains `.g-1` (single-node) and `.g-auto` (7+ nodes auto-fill at 220px minmax) so layouts stay coherent from solo workstation to a 4-Spark farm. i18n's `t()` learned a `{n}` placeholder so translations stay grammatical in zh-CN / zh-TW (e.g. `"{n} machines."` → `"{n} 台机器"` instead of locking "Five machines" into the source string). Hero's "1× consumer + 4× datacenter" subtitle is now computed from each node's `kind` field (`discrete` vs anything else) — drop a single ARM SoC into a `single-host.yaml` and the label updates itself. Existing 5-node deployments render identically; verified end-to-end against the upstream cluster.
- **Cross-host failover watchdog** — `server/deploy/ha-failover-watchdog.sh`. Closes the failure mode the in-host `ha-controller-watchdog.sh` cannot: the controller host dying (kernel hardlockup, PSU drop, motherboard fault) takes both the controller AND the local watchdog with it, leaving the rack AC plug stuck in whatever state Layer 2 last set. The cross-host script runs from cron on a peer node (a Spark, the NAS, or any always-on host that can reach HA REST) every 5 minutes, polls the controller host's `sensor.hearth_atlas_heartbeat` (written by `ha-controller` every ~30 s), and if it's stale beyond `STALE_SECONDS` (default 300) AND the plug is OFF, unconditionally turns it ON via HA REST — handing control back to the AC's Layer-1 thermostat. Zero dependency on the controller host's runtime (bash + curl + python3-only-for-json), so it survives the controller host being literally unplugged. Token loads from `~/.config/ha/token` (chmod 600), never from a committed file. Docs: [`docs/topology.md`](docs/topology.md) `ha.controller` section, fourth watchdog layer.
- **Layer-2 AC controller (opt-in)** — `server/integration/ha-controller.py` (+ systemd unit, external bash watchdog, install script). Reads `max(DCGM_FI_DEV_GPU_TEMP{node=~"spark.+"})` from Prometheus and can de-energize the rack-AC cuco plug via HA REST when GPUs are demonstrably cool, opportunistically saving compressor cycles the AC's built-in thermostat (Layer 1) would have spent on over-cooling. Architecture is strictly additive: Layer 2 can only turn the plug off (override L1's "want to cool"), never override L1's "stop cooling" — so L2-enabled is mathematically ≥ L2-disabled in energy savings, with worst case being silent degradation to L1-only behavior. Three independent watchdog layers (controller fail-safe, max-OFF-duration cap, external cron bash watchdog) guarantee any L2 failure returns control to L1 within 10 minutes max. Default `enabled: false` — opt in via `hearth.yaml > ha.controller`. Hard blocklist (MT6000 router) is unbypassable. Prometheus metrics on `:9106` for dashboard visibility. Docs: [`docs/topology.md`](docs/topology.md) `ha.controller` section.
- **Energy trends rollups** — new `/api/energy/trends` endpoint + Telemetry card showing 24h / 7d / 30d sliding windows with day-vs-night split (06:00–18:00 local). Lets operators A/B physical changes (AC setpoint tweaks, sunshade install, sensor relocation) by comparing same-window-same-period numbers instead of eyeballing graphs. Day/night partition is done Hearth-side using `display.timezone` because PromQL can't bucket by local hour. Frontend refreshes on a 60s timer (not SSE — these aggregates don't need sub-minute cadence).
- **Cabinet ambient proxy** from spark cuco plug internal temperatures. After empirically discovering the originally-configured rack temperature sensor was in a different room, fall back to a signal already present: each cuco plug exposes its own internal `_temperature_p_12_2` thermometer. At a node plug's ~70 W load self-heating is modest (~5 °C above ambient air), so the mean across all spark plugs is a usable PROXY for rack ambient — far better than no signal. New `ha_node_plug_temp_celsius{node=…}` metric, new `/api/cluster.env.cabinetHeatProxyC` + `byNodePlugTempC` fields, new "Plug °C" column in the Devices table, and a "cabinet ~XX.X °C" reading in the card header. ±5 °C uncertain absolute, but trends are trustworthy.
- **Home Assistant integration (optional) — wall power + rack environment.** Fills the gap DCGM (GPU only) and node_exporter (no socket-side W) can't: real per-host wall power from HA smart plugs, `tokens/W` efficiency (LiteLLM throughput / wall power), and rack temperature/humidity/AC status — surfaced in the existing Telemetry section as three terse `tm-card`s. Architecture: a tiny Python exporter (`server/integration/ha-exporter.py`) polls HA REST every 15 s and exposes Prometheus metrics on `:9105`; obs Prometheus scrapes it the same way it scrapes node_exporter. New `ha:` block in `hearth.yaml` plus per-node `sources.ha_plug_id` declare the topology — operators map plugs to nodes in config, not in code. Hard blocklist (MT6000 router by default) is enforced at exporter startup. Cards self-hide cleanly when no HA is configured, so the integration is invisible to users who don't need it. New `/api/config` field surface unchanged; `/api/cluster` gains `power` and `env` sub-objects (every field `null` when stale). Docs: [`docs/requirements/ha-integration.md`](docs/requirements/ha-integration.md), [`docs/topology.md`](docs/topology.md) `ha:` section.
- **`display.timezone` — pin all timestamps to a specific IANA zone.** Hearth has always rendered the nav clock and request-log times in the browser's own locale, which is fine until the same dashboard is viewed from a server-side browser, a remote support seat in another country, or a Pi whose TZ was never set. Add optional `display.timezone: "Asia/Taipei"` (or any IANA zone) to `hearth.yaml` and the frontend formats every timestamp in that zone regardless of viewer locale. Omit it and Hearth keeps the existing browser-local default. New `/api/config` endpoint exposes the value; data.js fetches it once at boot and routes the two render sites (nav clock, request log) through a `formatTime/formatDate` helper. Backend stays TZ-agnostic — SQL still emits ISO-8601 `Z`.
- **Multi-node (tensor/pipeline parallel) topology** — new optional `model_topology` config block. A TP=N deployment (e.g. vLLM `--tensor-parallel-size 4` over Ray across several hosts) exposes only the head node's endpoint to the gateway, so auto-discovery attributed the model — and its GPU-activity rings — to the head alone, making the worker nodes look idle while they were running TP shards at full GPU. Declaring `model_topology: {<model>: {nodes: [...], parallelism: "TP=4"}}` attributes the model and its derived activity to every node in the group. Docs: [`docs/topology.md`](docs/topology.md).
- **SGLang adapter** (`_scrape_sglang`, `sglang:*` metrics) — tps / TTFT / TPOT / KV% / p50-99 / running / waiting, the full rich set (SGLang exposes e2e + TTFT histograms like vLLM). Frontend renders SGLang models exactly like vLLM. ⚠️ Implemented from SGLang's documented metric names but **not yet verified against a live SGLang instance** (dev cluster runs vLLM + llama.cpp only) — please open an issue if metric names differ in your version.
- **Alert push channels** — node-down / GPU-overheat / memory / disk / gateway-error alerts now push to ntfy, Telegram, Discord, Slack, or a generic webhook. Fires on state *transition* (healthy→firing, firing→resolved) only — no per-tick spam. State persists across restart. Configure via `alerts:` in `config/hearth.yaml`; secrets via env vars. End-to-end tested through ntfy.sh. Docs: [`docs/alerts.md`](docs/alerts.md).
- Alert rule engine messages translated to English with stable `key`s (node:rule) for transition detection.

### Fixed

- **Request log no longer surfaces LiteLLM's own health probes.** The Telemetry request log and the Hero `reqTotal` rollup were both reading every row of `LiteLLM_SpendLogs`, including LiteLLM's 27-second background health checks — a real 10+5-token completion sent to each healthy backend, plus an error-path "residue" row written when a probe hits a down backend. In our cluster these were **71% of all rows** (45k success-probes + 5k fail-residue out of 70k total), making an idle gateway look like it was under constant 200-OK load and inflating cumulative request counts. The SQL behind `/api/logs` and `_litellm_rollup` now excludes both probe forms (`api_key='litellm-internal-health-check'` for successes, empty `call_type` + NULL/empty/`"None"` `api_key` for failure residue) so both panels reflect business traffic only.
- **Node metrics now render for any backend-reported node id.** The frontend keyed its live node buffers off the *static* `NODES` catalog ids, so a deployment whose `hearth.yaml` used different ids (e.g. `atlas` / `spark-01`) had every node's CPU/mem/GPU/temp silently dropped — the cards showed blanks. `applyLivePayload` now rebuilds `NODES` + `live.nodes` dynamically from the SSE payload (same reconcile the models list already used), so node cards populate regardless of how operators name their nodes.
- **`node_metrics: direct` override for the obs-host hairpin.** The host running the obs Prometheus stack often can't scrape its own `node_exporter` (Docker bridge hairpin NAT), leaving its CPU/mem rings at 0 even though its GPU (DCGM, same bridge) works. New per-node `sources.node_metrics: obs | direct` forces CPU/mem/disk/net to scrape `:9100` directly while GPU stays on obs DCGM. Documented in [`docs/topology.md`](docs/topology.md) and [`config/hearth.example.yaml`](config/hearth.example.yaml).
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
