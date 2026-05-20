# Hearth · server stack

This directory contains the Docker Compose stack and configuration that runs Hearth on the host of your choosing.

> **Status: alpha.** The configuration is currently a mix of `.env`-driven and hard-coded examples. v0.1.0 will move topology to a declarative `config/nodes.yaml` + `config/models.yaml`. Until then, treat the examples here as templates to copy and edit.

## Layout

```
server/
├── docker-compose.yml      Orchestrates the full stack
├── .env.example            Secrets template — copy to .env and fill in
│
├── api/                    FastAPI backend (Python)
│   ├── main.py             Endpoints + adapters (vLLM, llama.cpp, LiteLLM)
│   ├── requirements.txt
│   └── Dockerfile
│
├── nginx/                  Serves the dashboard + proxies /api/*
│   ├── nginx.conf
│   └── Dockerfile
│
├── prometheus/             Scrapes node + GPU exporters + model engines
│   ├── prometheus.yml      ← Edit targets to match your topology
│   └── rules/              Recording + alert rules
│
├── alertmanager/           Routes alerts (no receivers wired by default)
│   └── alertmanager.yml
│
└── litellm/config.yaml     Snippet to merge into your LiteLLM config
```

## Single-host quick start

If you run everything on one box (one GPU host):

```bash
cd server
cp .env.example .env
# Edit .env: set LITELLM_MASTER_KEY (if you use a LiteLLM gateway)
docker compose up -d
```

Open `http://localhost:8080` (or whatever port nginx is mapped to).

The default `prometheus.yml` uses `host.docker.internal` for node-exporter, DCGM, LiteLLM, and a single vLLM example. Edit to match your reality.

## Multi-node setup

For multiple GPU hosts, edit `prometheus/prometheus.yml` to add one target per node. The metric structure used by the dashboard is:

| Layer | Source | What it provides |
|---|---|---|
| Node OS | `node_exporter :9100` on each host | CPU, RAM, disk, network, temperatures |
| GPU | `dcgm-exporter :9400` on NVIDIA hosts | GPU util, VRAM, GPU temp, power |
| Gateway | LiteLLM `:4000` `/metrics` (or Postgres `LiteLLM_SpendLogs`) | Request log, model routing, latency per model |
| Engines | vLLM `:8000`/`:8001`/… `/metrics` | TTFT, TPOT, KV-cache, throughput |
| Engines | llama.cpp `--metrics` `/metrics` | tps, TPOT, running/waiting (TTFT/KV not exposed) |

You install `node_exporter` and `dcgm-exporter` on each GPU host (your distro's package manager + the official NVIDIA installer). Hearth doesn't manage those — you do.

## After editing

```bash
# Prometheus config change:
docker compose restart prometheus
curl -X POST http://localhost:9090/-/reload    # hot-reload

# API code change:
docker compose restart api
curl -s http://localhost:8088/api/health | jq

# Nginx config change:
docker compose restart web

# Check Prometheus targets are UP:
open http://localhost:9090/targets
```

## Verifying it works

```bash
curl -fsS http://localhost:8080/api/health
# {"ok": true, "prometheus": true, "time": "..."}

curl -s http://localhost:8080/api/models | jq '.[] | {id, state, framework, nodes}'
# Expect: list of models discovered from LiteLLM /model/info + /health
```

If the model list is empty, check:

1. Is `LITELLM_URL` reachable from the api container?
2. Is `LITELLM_MASTER_KEY` correct?
3. Does `curl -fsS -H "Authorization: Bearer $KEY" $LITELLM_URL/model/info` from your host return data?

## Security

- The default Docker Compose binds the dashboard to `0.0.0.0:8080` (LAN-reachable). For a multi-user home network, put it behind your VPN (Tailscale `serve --tls`, WireGuard, ZeroTier) — see [`../SECURITY.md`](../SECURITY.md).
- Secrets belong in `.env` (chmod 600). Never commit `.env`.
- LiteLLM master keys give full API access to your models. Treat them like SSH keys.
