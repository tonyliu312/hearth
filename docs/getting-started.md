# Getting started · 5 minutes

This guide gets Hearth running on **one** GPU box. Multi-node setup is a small extension at the bottom.

> **Realistic scope.** Hearth is alpha. The flow below covers the path that's known to work end-to-end (LiteLLM gateway + vLLM or llama.cpp + node_exporter + dcgm-exporter on NVIDIA). If your setup is Ollama-only, see the [Ollama section](#ollama-only) — there are caveats.

## Prerequisites

- Linux host with Docker + Docker Compose (Hearth itself doesn't care about your distro)
- Your inference engine already running somewhere (Hearth is read-only — it watches what you've already deployed)
- 5 minutes of attention

## Step 1 · Clone

```bash
git clone https://github.com/tonyliu312/hearth.git
cd hearth
```

## Step 2 · Configure (one minute)

```bash
cd server
cp .env.example .env
# Edit .env:
#   - Set LITELLM_MASTER_KEY if you use a LiteLLM gateway
#   - (Optional) Set EXTERNAL_HOST to the IP you'll access from a browser
$EDITOR .env
```

If you have multiple machines or want to declare your topology, also create a Hearth config:

```bash
cd ..    # back to repo root
cp config/hearth.example.yaml config/hearth.yaml
$EDITOR config/hearth.yaml
```

The minimum useful change: edit the single `nodes:` entry's `ip` and `hw.gpu` to match reality. Everything else has sensible defaults.

**No config?** Hearth boots with a single-host localhost default. `docker compose up` will work but only show metrics from `host.docker.internal:9100` / `:9400` (Hearth running inside Docker on the same box as the exporters).

## Step 3 · Install the exporters (one-time, per GPU host)

These are industry-standard tools, not Hearth-specific. If you already have a Prometheus stack, you almost certainly have them.

**node_exporter** (CPU / RAM / disk / network / temps):

```bash
# Ubuntu/Debian — bundled in most distros' apt:
sudo apt install prometheus-node-exporter
sudo systemctl enable --now prometheus-node-exporter
# Verify: curl http://localhost:9100/metrics | head
```

**dcgm-exporter** (NVIDIA GPU util / VRAM / temp / power) — only needed on hosts with NVIDIA GPUs:

```bash
# Via Docker (NVIDIA's official image):
docker run -d --gpus all --restart unless-stopped \
  --name dcgm-exporter -p 9400:9400 \
  nvcr.io/nvidia/k8s/dcgm-exporter:3.3.9-3.6.1-ubuntu22.04
# Verify: curl http://localhost:9400/metrics | grep DCGM_FI_DEV_GPU_UTIL
```

For AMD ROCm or Apple Silicon, see the [topology guide](topology.md) — DCGM doesn't apply.

## Step 4 · Boot Hearth

```bash
cd server
docker compose up -d
docker compose ps           # all containers should be Up
```

Open <http://localhost:8080/> in a browser.

## Step 5 · Verify

You should see:

- The **Hero** section with cluster stat ring (uptime, requests, tokens)
- The **Cluster** section with token-throughput / power / KV pulse charts
- The **Nodes** section with cards per host — GPU/VRAM/CPU rings
- The **Models** section listing models discovered from your LiteLLM gateway
- The **Telemetry** section streaming recent requests

If a section is empty:

| Symptom | Most likely cause |
|---|---|
| Cluster pulse charts flat zero | LiteLLM `/metrics` not reachable from the api container — check `LITELLM_URL` in `.env` |
| Node cards show "no data" | node_exporter / dcgm-exporter not reachable on the IP you declared in `nodes.yaml` |
| Models section says "no live metric sources" | Your vLLM / llama.cpp engines aren't exposing `/metrics`, or they're behind your gateway via a route Hearth can't introspect |
| Telemetry has no requests | LiteLLM's Postgres `LiteLLM_SpendLogs` table is empty (no traffic yet) or you didn't wire the api to read it |

## Multi-node setup

Extend `config/hearth.yaml`:

```yaml
nodes:
  - id: gateway
    name: "Workstation"
    ip: 10.0.0.1
    role: gateway
    kind: discrete
    hw: { gpu: "RTX 4090", vram_gb: 24, cpu_cores: 16, ram_gb: 128 }
    sources:
      node_exporter: "10.0.0.1:9100"
      dcgm: "10.0.0.1:9400"
      obs_node_label: "workstation"

  - id: infer-1
    name: "Inference-1"
    ip: 10.0.0.2
    role: inference
    kind: discrete
    hw: { gpu: "RTX 3090", vram_gb: 24, cpu_cores: 8, ram_gb: 64 }
    sources:
      node_exporter: "10.0.0.2:9100"
      dcgm: "10.0.0.2:9400"
      obs_node_label: "infer-1"
```

Until [#yaml-driven-prometheus](https://github.com/tonyliu312/hearth/issues) (v0.2.0) ships, you also need to add matching scrape targets in `server/prometheus/prometheus.yml`. The labels `node:<obs_node_label>` are how Hearth joins Prometheus data to your config nodes.

See [`docs/topology.md`](topology.md) for the full schema reference and three pre-built example topologies in [`examples/`](../examples/).

## Ollama-only

If your only inference engine is Ollama (no LiteLLM, no vLLM, no llama.cpp), Hearth will display:

- ✅ Your node-level OS metrics (CPU, RAM, GPU util via DCGM, temps, power)
- ❌ No per-model throughput / TTFT / TPOT (Ollama doesn't ship Prometheus metrics natively)

A dedicated Ollama adapter is on the [v0.2.0 roadmap](../README.md#roadmap). Until then, consider running Ollama behind a [LiteLLM gateway](https://docs.litellm.ai/) — LiteLLM proxies Ollama as an OpenAI-compatible backend and emits per-request metrics that Hearth reads.

## Common gotchas

1. **`host.docker.internal` doesn't resolve** — your Docker version is too old. Either upgrade, or replace with the host's LAN IP in `nodes.yaml`.
2. **CORS errors in browser console** — set `CORS_ORIGINS=https://your-host` in `.env`.
3. **Dashboard works on localhost but not from another machine** — Docker Compose binds to `0.0.0.0` by default; check your firewall (`ufw status`).
4. **Model card stuck at "online" without numbers** — that engine doesn't speak `vllm:*` or `llamacpp:*` metric names. Honest behavior. File an issue with the engine name; we'll add an adapter (or you can — see [`docs/adapters.md`](adapters.md)).
5. **Timezone in logs looks wrong** — Hearth uses the **browser's** local timezone. Check your OS timezone setting on the device you're viewing from, not the Hearth host.

## Next steps

- Customize topology → [`docs/topology.md`](topology.md)
- Write a backend adapter → [`docs/adapters.md`](adapters.md)
- Run on Tailscale / Cloudflare Tunnel for remote access → see hardening notes in [`SECURITY.md`](../SECURITY.md)

If you hit a wall, file an issue with your topology (number of nodes, GPU types, gateway, engines) and the symptom. We'll triage.
