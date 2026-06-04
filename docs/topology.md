# Topology configuration

Hearth loads its topology from a single YAML file. By default it looks at
`/etc/hearth/config.yaml`; override with the `HEARTH_CONFIG` environment
variable. If no config is found, Hearth falls back to a single-host
localhost default so `docker compose up` works on a fresh install.

A fully-commented example lives at [`config/hearth.example.yaml`](../config/hearth.example.yaml). Three preset topologies under [`examples/`](../examples/) demonstrate the common shapes:

| Preset | When to use |
|---|---|
| [`single-host.yaml`](../examples/single-host.yaml) | One machine, one or two GPUs, everything local |
| [`dual-gpu.yaml`](../examples/dual-gpu.yaml) | One host with multiple discrete GPUs, vLLM TP=N |
| [`multi-node-heterogeneous.yaml`](../examples/multi-node-heterogeneous.yaml) | Gateway box + N ARM-SoC inference nodes (or any mix) |

## Schema

```yaml
display:
  cluster_name: <string>             # navbar / browser tab label
  timezone: auto                     # reserved; browser-local is used today

gateway:
  type: litellm                      # only "litellm" today
  enabled: <bool>                    # set false if you don't run a gateway
  base_url: <url>                    # e.g. http://127.0.0.1:4000
  master_key_env: <env var name>     # env var NAME — value never in YAML

nodes:
  - id: <stable string>              # short id; used internally
    name: <display name>
    ip: <ipv4>                       # primary address
    role_label: <free string>        # what shows under the node title
    kind: discrete | unified-arm-soc | apple-silicon
    class: <free string>             # short class tag shown above name
    hw:                              # display-only fields
      gpu: <string>
      vram_gb: <int>
      cpu_model: <string>
      cpu_cores: <int>
      cpu_threads: <int>             # defaults to cpu_cores if omitted
      ram_gb: <int>
      disk_gb: <int>
      net: <string>
      fp16_tflops: <float>           # optional, used by Hero cluster aggregate
      fp4_tflops: <float>            # optional
    sources:                         # where Hearth scrapes this node's metrics
      node_exporter: <host:port>     # optional
      dcgm: <host:port>              # optional (NVIDIA only)
      obs_node_label: <string>       # label `node` value in prometheus.yml; if
                                     # set, this node is read FROM the obs
                                     # Prometheus; if omitted, the Hearth API
                                     # scrapes :9100/:9400 directly.
      node_metrics: obs | direct     # optional override. Force CPU/mem/disk/net
                                     # from :9100 even with obs_node_label set
                                     # (GPU still via obs DCGM). See note below.
    services: [<string>, ...]        # display-only chips

model_meta:                          # OPTIONAL display overlay for routes
  <route name>:
    display: <string>
    vendor: <string>
    kind: chat | vision | embed | rerank
    tags: [<string>, ...]

model_topology:                      # OPTIONAL multi-node (TP/PP) declaration
  <model id>:
    nodes: [<node id>, ...]          # every node in the parallel group
    parallelism: <string>            # display label, e.g. "TP=4"
```

## `model_topology` — multi-node tensor/pipeline parallel

A model served with `--tensor-parallel-size N` (or pipeline parallel) across several hosts via Ray/torchrun exposes **one** OpenAI endpoint: the head node. The LiteLLM gateway only knows that one `api_base`, so Hearth's auto-discovery attributes the model to the head node alone — the worker nodes show idle even though they're running TP shards and their GPUs are fully engaged.

Declare the span and Hearth attributes the model, and its derived GPU-activity rings, to **every** node in the group:

```yaml
model_topology:
  deepseek-v4-flash:
    nodes: [spark-01, spark-02, spark-03, spark-04]   # TP=4 across 4 hosts
    parallelism: "TP=4"
```

Node ids must match `nodes[].id`; unknown ids are ignored. The model's live metrics (tps / TTFT / KV / …) still come from the head endpoint — TP ranks compute in lockstep, so the head's throughput reflects the whole group — and that activity now lights up all member nodes instead of just the head.

## The `kind` field — why it matters

The single most important field. It tells Hearth how to interpret VRAM%:

| `kind` | VRAM% source | Used by |
|---|---|---|
| `discrete` | DCGM `FB_USED / (FB_USED + FB_FREE)` | RTX 4090 / A100 / H100 / 3090 / etc. — any GPU with its own VRAM |
| `unified-arm-soc` | `node_memory_MemTotal - MemAvailable` (system memory occupancy) | DGX Spark (GB10), Jetson AGX Orin — GPU shares system RAM |
| `apple-silicon` | Same as unified-arm-soc | M1/M2/M3 Mac (Metal, mlx, Ollama on macOS) |

DCGM's `FB_USED` on unified-memory ARM SoCs is unreliable or empty — Hearth detects this and reads from system memory instead. Get this wrong and the VRAM ring will be flat zero.

## `sources` — where metrics come from

Hearth supports two routes for node-level OS metrics:

1. **Via the obs Prometheus** (recommended for multi-node) — set `obs_node_label` to whatever `labels.node` you use in `prometheus.yml`. The api container then queries `node={obs_node_label}` from Prometheus.

2. **Direct scrape** (single-host or hosts not in your Prometheus job) — leave `obs_node_label` empty. The api container hits `:9100` / `:9400` on the IPs you list.

Both routes can coexist in the same cluster.

### `node_metrics: direct` — the obs-host hairpin case

There's one host where route 1 silently breaks: **the machine running your obs Prometheus stack itself**. A Prometheus container on a Docker bridge network frequently can't scrape its own host's `node_exporter` (hairpin NAT on the bridge), so `node={label}` returns nothing for CPU/mem even though the GPU (DCGM, scraped over the same bridge) works fine — leaving that node's CPU/mem rings flat at 0.

Set `node_metrics: direct` on that node to keep its GPU coming from obs DCGM (via `obs_node_label`) while CPU/mem/disk/net are scraped straight from `:9100`. This is a per-node override; everything else stays on the obs route.

## Adding a new node

1. Add an entry to `config/hearth.yaml` matching one of the example shapes.
2. Make sure node_exporter (and dcgm-exporter if NVIDIA) are running on that host and reachable from the api container.
3. If you want it to show up in your obs Prometheus, also add a target in `server/prometheus/prometheus.yml` with `labels.node` matching the `obs_node_label`.
4. Restart Hearth: `docker compose restart api`.

## Reloading without a restart

Hearth caches config at process start. To pick up topology changes, restart the api container. (Hot-reload is on the v0.2.0 roadmap.)

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Node shows no live data | `obs_node_label` doesn't match a target in `prometheus.yml`, or node_exporter not reachable from api container |
| GPU ring flat zero but node serving | Wrong `kind` (e.g., GB10 set to `discrete` — should be `unified-arm-soc`) |
| Model card shows "no live metrics source" | The backend has no `/metrics` endpoint Hearth recognizes (vLLM / llama.cpp). Verify by `curl http://<host>:<port>/metrics`. |
| All node times shown in wrong timezone | Browser TZ vs server TZ mismatch — Hearth uses *browser* local time. Check your OS/browser TZ. |

## `ha:` — optional Home Assistant integration

Hearth ships an optional Prometheus exporter (`server/integration/ha-exporter.py`) that turns a Home Assistant instance into wall-power and rack-environment metrics. Three lightweight `tm-card`s in the Telemetry section then render: **Wall power** (Σ smart-plug W), **Efficiency** (`tokens·W⁻¹·s⁻¹` = LiteLLM throughput / wall power), **Rack** (temperature / humidity / AC state).

```yaml
ha:
  base_url: "http://homeassistant.local:8123"      # prefer mDNS — survives DHCP drift
  exporter_target: "127.0.0.1:9105"                 # where Prometheus scrapes the exporter
  rack_sensor_id: "miaomiaoc_cn_..._t8"             # OPTIONAL temp/humidity sensor entity prefix
  rack_ac_plug_id: "2027457700"                     # OPTIONAL rack AC's smart-plug id
  blocklist:                                         # plugs the exporter REFUSES to monitor (refuses to start if seen)
    - "2051674991"                                   # MT6000 router default — turning it off kills the LAN

nodes:
  - id: spark-01
    sources:
      ha_plug_id: "2029950736"                      # this node's smart-plug id
```

**Honest degradation**:
- HA exporter absent → `/api/cluster.power` / `.env` come back `null`,前端 three cards are hidden entirely.
- One sensor stale → that field is `null`; siblings still render.
- A node has no `ha_plug_id` → its wall-power column shows `—` (not `0 W`). Atlas with no smart plug is the canonical example.

**Bearer token**: never put the HA long-lived token in `hearth.yaml` or any committed file. The systemd unit at `server/deploy/ha-exporter.service` loads it from a root-only drop-in (`HA_TOKEN=…` in `/etc/systemd/system/ha-exporter.service.d/token.conf`,`chmod 600`).

Full design rationale + open spec handoffs: [`docs/requirements/ha-integration.md`](requirements/ha-integration.md).

### Why Hearth's kWh won't match HA / Mi Home / your utility meter

This is intentional, and worth stating up front:

The energy columns ("24h", "30d") in the Telemetry table are **Hearth's own integral** of the live `ha_node_wall_power_watts` series — that is, every 15s scrape of W × 15s, summed over the window, converted to kWh. They are **not** scraped from any "kWh accumulator" field. **They will not equal** what you see in:

- the Mi Home (米家) / Tuya / Aqara / SmartThings app on your phone,
- the Home Assistant Energy Dashboard,
- your electric utility's smart meter.

The reasons are deliberate and structural:

1. **Vendor clouds are not real-time, lossless, or auditable.** Mi Home (and friends) sync the plug's internal counter to their cloud on their own schedule, occasionally miss samples, sometimes reset on firmware updates, and aggregate into hourly / daily buckets in ways the user can't see. Hearth treats the only thing the device reports honestly — instantaneous wall watts — as the source of truth, and integrates that itself.
2. **HA inherits those gaps.** Empirically, on the upstream cluster, the cuco `power_consumption_p_11_1` entity sat frozen at 0.01 kWh for >24h while the W series moved normally. HA's Energy Dashboard does its own long-term-statistics rollup on top, which can still drift from any of the above.
3. **Sliding window, not calendar window.** "24h" means "the last 24 hours", not "today since midnight". Same for "30d". A calendar boundary would need timezone-aware bucketing on the Prometheus side for very little real-world benefit on a home cluster — "what did this rack actually draw recently" is the question that matters for deciding when to migrate a workload or scale down a node.
4. **Hearth's number is the one you can defend.** It's computed from a single, verifiable input (the live wall-W series), with one trivially-checkable formula. If Hearth says 4.30 kWh, you can replay the underlying Prometheus series and arrive at the same number deterministically. The vendor cloud's number can't be replayed.

**Bottom line for OSS users**: don't try to make Hearth match your phone app. Use Hearth to monitor your *cluster's* energy reality (which is what you can act on), and use the phone app or the utility meter for the bill.

## `ha.controller` — Layer-2 GPU-driven AC override (opt-in)

When the AC plug is wired through HA and Hearth knows the GPU temperatures, you can let Hearth opportunistically de-energize the AC plug when GPUs are demonstrably cool, saving compressor cycles the AC's own thermostat would have spent on over-cooling. This is **strictly additive**: every off-second is a win versus the AC running standalone, and the AC's built-in controller (F01/F02 setpoints) remains the safety baseline whenever the plug is energized.

### Architecture

Two control layers, with explicit roles:

- **Layer 1** — the AC's internal F01/F02/F05 thermostat. Must be configured to safely run the rack on its own; Hearth never modifies these.
- **Layer 2** — `ha-controller` systemd service. Reads `max(DCGM_FI_DEV_GPU_TEMP{node=~"spark.+"})` and toggles the AC plug via HA REST. Default-on (Layer 1 runs); turns plug off only when all of: GPU ≤ close threshold, ≥ 5 min since last switch, not in emergency.

### Four watchdog layers (any L2 failure → L1 takes over within minutes)

1. **Controller fail-safe**: any missing signal (Prometheus down, HA down, GPU temp metric absent) → if plug is off, force on.
2. **Max-OFF-duration cap**: plug cannot stay off longer than `max_off_duration_s` (default 600 s). Bounds L2's blast radius if its logic ever goes wrong.
3. **In-host watchdog** (`server/deploy/ha-controller-watchdog.sh`, runs from cron every 5 min **on the controller host**): if controller's last-decision metric is stale > 120 s and plug is off, force plug on via HA REST. Single-file bash + curl — has zero dependency on Python or Hearth's runtime, so it works even if everything else on the host is broken.
4. **Cross-host failover watchdog** (`server/deploy/ha-failover-watchdog.sh`, runs from cron every 5 min **on a peer host** — a Spark, the NAS, anywhere with HA REST access): covers the harder failure mode that layer 3 cannot — the controller host **itself** dies (kernel hardlockup, PSU drop, motherboard fault), taking the controller AND its in-host watchdog with it. The cross-host script polls Atlas's `sensor.hearth_atlas_heartbeat` (which `ha-controller` writes every ~30 s); if it's stale beyond `STALE_SECONDS` (default 300) AND the plug is OFF, it unconditionally turns the plug ON via HA REST, returning control to Layer 1. Zero dependency on Atlas's runtime — survives Atlas being literally unplugged. Token loads from peer's `~/.config/ha/token` (chmod 600). Install: copy the script + a long-lived HA token to a peer, add a `*/5 * * * *` crontab entry. Recommended for any deployment where the controller host is itself the most-likely-to-die node.

### Schema

```yaml
ha:
  controller:
    enabled: false                  # opt in by setting true
    target_plug_id: "2027457700"    # AC plug only — NOT a node plug
    decision_interval_s: 30
    gpu_open_threshold: 65          # max-GPU ≥ this → plug ON
    gpu_close_threshold: 55         # max-GPU ≤ this → plug may turn OFF
    min_switch_interval_s: 300      # compressor protection
    max_off_duration_s: 600         # safety cap (10 min)
    emergency_open_threshold: 75    # max-GPU ≥ this for 30s → force ON
    emergency_open_dur_s: 30
    fail_safe: "on"                 # default to ON on any failure
```

### Hard safety rails

- `target_plug_id` must **not** be in `ha.blocklist` (operator-defined) or in `HARD_BLOCKLIST` (`2051674991` MT6000 router — hardcoded, cannot be overridden).
- Controller refuses to start if `enabled` is false or if `target_plug_id` is missing.
- The install script (`install-ha-controller.sh`) re-validates all of the above before touching systemd.

### Deployment

```bash
# 0) Set controller.enabled: true and target_plug_id in hearth.yaml
# 1) Install (idempotent)
sudo TOKEN_FILE=/home/$USER/.config/ha/token \
    bash server/deploy/install-ha-controller.sh
# 2) Watch first 24h via Hearth Telemetry → Energy trends card
# 3) Disable any time
sudo systemctl stop ha-controller && sudo systemctl disable ha-controller
```

**Recommended**: install the cross-host failover watchdog on at least one peer node (a Spark, the NAS — any always-on host that can reach HA REST and is independent of the controller host's hardware):

```bash
# On the peer host (e.g. spark-01), as the owning user:
mkdir -p ~/.config/ha && chmod 700 ~/.config/ha
# Paste your HA long-lived token (Bearer ...) into this file:
echo "YOUR_HA_LONG_LIVED_TOKEN" > ~/.config/ha/token && chmod 600 ~/.config/ha/token

# Copy the watchdog (from your Hearth checkout or scp from the controller host)
cp server/deploy/ha-failover-watchdog.sh ~/ha-failover-watchdog.sh
chmod +x ~/ha-failover-watchdog.sh

# Add to crontab — every 5 min
( crontab -l 2>/dev/null; echo "*/5 * * * * HA_URL=http://homeassistant.local:8123 \$HOME/ha-failover-watchdog.sh >> \$HOME/hearth-failover-watchdog.log 2>&1" ) | crontab -
```

`HA_URL` defaults to `http://homeassistant.local:8123`; override to your LAN IP if mDNS isn't reachable from the peer. Logs accumulate in `~/hearth-failover-watchdog.log` — each cron run logs "Atlas alive" silently or "Atlas DOWN AND plug OFF — UNCONDITIONAL TURN ON" when it rescues.

### Operator's mental model

"Layer 1 is the AC doing its normal job. Layer 2 is Hearth saying 'hey, GPUs are cool, take a short break.'  If Hearth gets confused, Layer 1 takes over within 10 minutes max."
