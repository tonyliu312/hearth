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
    services: [<string>, ...]        # display-only chips

model_meta:                          # OPTIONAL display overlay for routes
  <route name>:
    display: <string>
    vendor: <string>
    kind: chat | vision | embed | rerank
    tags: [<string>, ...]
```

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
