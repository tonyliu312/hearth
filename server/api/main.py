# Hearth · FastAPI backend
#
# 双源、全只读、零生产影响：
#   - obs-prometheus (PROMETHEUS_URL)  : DCGM GPU(全 3 节点) + 2 台 Spark 的 node/hwmon
#   - 宿主 node-exporter (NODE_EXPORTER_URL) : host 自身 node_* + 全部 hwmon 温度
#     (if your obs Prometheus has no scrape job for this host, Hearth directly hits :9100/:9400)
#
# 不依赖任何 recording rule —— 聚合在本服务内用原始 PromQL 计算，
# 因此无需改 obs 的 prometheus.yml（严格不越界）。
#
#   GET /api/health /cluster /nodes /nodes/{id} /models /models/{id}
#       /alerts /logs /topology /stream(SSE)

import os
import re
import sys
import time
import json
import asyncio
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# ── Config ─────────────────────────────────────────────────────────
PROM_URL    = os.environ.get("PROMETHEUS_URL",    "http://host.docker.internal:9090")
NODEEXP_URL = os.environ.get("NODE_EXPORTER_URL", "http://host.docker.internal:9100")
AM_URL      = os.environ.get("ALERTMANAGER_URL",  "http://host.docker.internal:9093")
LITELLM_URL = os.environ.get("LITELLM_URL",       "http://host.docker.internal:4000")
LITELLM_KEY = os.environ.get("LITELLM_MASTER_KEY", "")
CORS        = os.environ.get("CORS_ORIGINS",      "*").split(",")
TICK_SEC    = float(os.environ.get("TICK_SEC",    "1.5"))

# ─────────────────────────────────────────────────────────────────────
# Topology loaded from YAML (`$HEARTH_CONFIG`, default /etc/hearth/config.yaml).
# Schema docs: docs/topology.md  ·  example: config/hearth.example.yaml
#
# Each node carries a `kind` field that replaces the legacy GB10-special:
#   - discrete       : dedicated VRAM GPU (use DCGM FB_USED/FB_FREE for VRAM%)
#   - unified-arm-soc: GPU shares system memory (GB10 / Jetson) — use
#                      node_exporter MemAvailable for VRAM%
#   - apple-silicon  : same unified-memory treatment as ARM SoC (mlx, Ollama on Metal)
#
# `node_source`: "obs"   → metrics via the obs Prometheus we scrape from
#                "direct"→ Hearth API scrapes the host's :9100/:9400 itself
#                          (used when this host isn't in the obs Prometheus job)
# Single-host default activates if the YAML is absent — single localhost node.
# ─────────────────────────────────────────────────────────────────────


def _default_config() -> dict:
    """Single-host localhost default — first `docker compose up` works with no YAML."""
    return {
        "display": {"cluster_name": "Home Cluster"},
        "gateway": {"type": "litellm", "enabled": True,
                    "base_url": os.environ.get("LITELLM_URL", "http://host.docker.internal:4000"),
                    "master_key_env": "LITELLM_MASTER_KEY"},
        "nodes": [{
            "id": "node-1", "name": "localhost", "ip": "127.0.0.1",
            "role_label": "node", "kind": "discrete",
            "class": "GPU host",
            "hw": {"gpu": "—", "vram_gb": 0, "cpu_cores": 0, "cpu_threads": 0, "ram_gb": 0},
            "sources": {"node_exporter": "host.docker.internal:9100",
                        "dcgm": "host.docker.internal:9400"},
        }],
        "model_meta": {},
        "model_topology": {},
    }


def _load_config(path: str) -> dict:
    """Load Hearth YAML config; fall back to single-host default if absent/invalid."""
    try:
        import yaml                        # noqa: WPS433 — optional dep, fail soft
    except ImportError:
        print("[hearth] pyyaml not installed; using single-host default", file=sys.stderr)
        return _default_config()
    p = Path(path) if path else None
    if p and p.exists():
        try:
            data = yaml.safe_load(p.read_text()) or {}
            if isinstance(data, dict) and data.get("nodes"):
                return data
            print(f"[hearth] config {path} loaded but has no nodes; using default", file=sys.stderr)
        except Exception as e:
            print(f"[hearth] failed to parse {path}: {e}; using default", file=sys.stderr)
    return _default_config()


def _node_from_yaml(y: dict) -> dict:
    """YAML node entry → internal flat dict (preserves legacy NODES shape so
    the rest of main.py is untouched)."""
    hw = y.get("hw") or {}
    src = y.get("sources") or {}
    obs_label = src.get("obs_node_label")
    return {
        "id": y["id"],
        "name": y.get("name", y["id"]),
        "ip": y.get("ip", ""),
        "class": y.get("class", "GPU host"),
        "role": y.get("role_label", y.get("role", "node")),
        "kind": y.get("kind", "discrete"),
        "obs_node": obs_label,
        # node_metrics: "obs" | "direct" — override for hosts whose node_exporter
        # the obs Prometheus can't reach (e.g. the obs host's own bridge-net
        # hairpin). Such a node keeps obs_node_label (GPU via obs DCGM) but
        # scrapes :9100 directly for CPU/mem/disk/net. Default: obs if labelled.
        "node_source": src.get("node_metrics") or ("obs" if obs_label else "direct"),
        "gpu": {"name": hw.get("gpu", "—"),
                "mem":  hw.get("vram_gb", 0),
                "fp16": hw.get("fp16_tflops", 0),
                "fp4":  hw.get("fp4_tflops", 0)},
        "cpu": {"model":   hw.get("cpu_model", "—"),
                "cores":   hw.get("cpu_cores", 0),
                "threads": hw.get("cpu_threads", hw.get("cpu_cores", 0))},
        "ram":  hw.get("ram_gb", 0),
        "disk": hw.get("disk_gb", 0),
        "net":  hw.get("net", ""),
        "services": y.get("services", []),
    }


HEARTH_CONFIG_PATH = os.environ.get("HEARTH_CONFIG", "/etc/hearth/config.yaml")
HEARTH_CFG = _load_config(HEARTH_CONFIG_PATH)
NODES = [_node_from_yaml(n) for n in HEARTH_CFG.get("nodes", [])] or [
    _node_from_yaml(n) for n in _default_config()["nodes"]
]

NODE_BY_ID  = {n["id"]: n for n in NODES}
OBS_TO_ID   = {n["obs_node"]: n["id"] for n in NODES if n["obs_node"]}
IP_TO_ID    = {n["ip"]: n["id"] for n in NODES}
# kind lookup by obs_node label — replaces the legacy `obs_node != "rtx4090-pc"`
KIND_BY_OBS = {n["obs_node"]: n.get("kind", "discrete") for n in NODES if n["obs_node"]}
# First discrete-kind node's obs label, for cluster-level "the discrete GPU" lookups
DISCRETE_OBS = next((n["obs_node"] for n in NODES
                     if n.get("kind") == "discrete" and n.get("obs_node")), None)

app = FastAPI(title="Hearth API", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=CORS, allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])
client = httpx.AsyncClient(timeout=8.0)


# ── PromQL (obs-prometheus, 只读) ───────────────────────────────────
async def promql(query: str) -> list[dict[str, Any]]:
    try:
        r = await client.get(f"{PROM_URL}/api/v1/query", params={"query": query})
        r.raise_for_status()
        d = r.json()
        if d.get("status") != "success":
            return []
        return [{"metric": i["metric"], "value": float(i["value"][1])}
                for i in d["data"]["result"]]
    except Exception:
        return []


async def promql_range(query: str, minutes: int = 5, step: int = 15):
    now = time.time()
    try:
        r = await client.get(f"{PROM_URL}/api/v1/query_range",
                             params={"query": query, "start": now - minutes * 60,
                                     "end": now, "step": step})
        r.raise_for_status()
        d = r.json()
        if d.get("status") != "success":
            return []
        return [{"metric": i["metric"],
                 "values": [[float(t), float(v)] for t, v in i["values"]]}
                for i in d["data"]["result"]]
    except Exception:
        return []


def _one(rs, default=0.0):
    return rs[0]["value"] if rs else default


def _by(rs, label):
    return {i["metric"].get(label, "?"): i["value"] for i in rs}


# ── Host node-exporter direct scrape (when obs Prometheus has no job for this host) ──
_PROM_LINE = re.compile(r'^([a-zA-Z_:][\w:]*)\{([^}]*)\}\s+([-\d.eE+]+)\s*$')
_PROM_BARE = re.compile(r'^([a-zA-Z_:][\w:]*)\s+([-\d.eE+]+)\s*$')


def _parse_labels(s: str) -> dict[str, str]:
    out = {}
    for m in re.finditer(r'(\w+)="((?:[^"\\]|\\.)*)"', s):
        out[m.group(1)] = m.group(2).replace('\\"', '"').replace('\\\\', '\\')
    return out


async def _scrape_node_exporter() -> dict[str, list[dict]]:
    """抓一次宿主 node-exporter，按指标名归并 [{labels, value}]。"""
    out: dict[str, list[dict]] = {}
    try:
        r = await client.get(f"{NODEEXP_URL}/metrics", timeout=5.0)
        r.raise_for_status()
        for line in r.text.splitlines():
            if not line or line[0] == "#":
                continue
            m = _PROM_LINE.match(line)
            if m:
                name, lbl, val = m.group(1), _parse_labels(m.group(2)), m.group(3)
            else:
                b = _PROM_BARE.match(line)
                if not b:
                    continue
                name, lbl, val = b.group(1), {}, b.group(2)
            try:
                out.setdefault(name, []).append({"labels": lbl, "value": float(val)})
            except ValueError:
                pass
    except Exception:
        pass
    return out


def _sum(rows, pred=lambda l: True):
    return sum(x["value"] for x in rows if pred(x["labels"]))


# 模块归类：把 hwmon 传感器映射成人话硬件模块
def _classify_temp(chip: str, label: str) -> str:
    c, l = chip.lower(), label.lower()
    if "coretemp" in c or "k10temp" in c or "package" in l or "tctl" in l or "tccd" in l:
        return "CPU"
    if c.startswith("nvme") or "nvme" in c:
        return "NVMe"
    if "coolant" in l or "pump" in l or "water" in l:
        return "水冷"
    if "mac temp" in l or "phy temp" in l or "nic" in l or "mlx" in c:
        return "网卡"
    if "soc" in l or "gpu" in l:
        return "SoC"
    return "其他"


def _atlas_temps(scrape: dict) -> list[dict]:
    """从宿主 node-exporter 文本里抽出全部硬件模块温度。"""
    labels_idx = {}
    for x in scrape.get("node_hwmon_sensor_label", []):
        lb = x["labels"]
        labels_idx[(lb.get("chip", ""), lb.get("sensor", ""))] = lb.get("label", "")
    temps = []
    for x in scrape.get("node_hwmon_temp_celsius", []):
        lb = x["labels"]
        chip, sensor = lb.get("chip", ""), lb.get("sensor", "")
        human = labels_idx.get((chip, sensor)) or sensor
        if x["value"] <= 0 or x["value"] > 150:   # 跳过无效/未连接传感器
            continue
        temps.append({"module": _classify_temp(chip, human),
                      "label": human, "chip": chip,
                      "celsius": round(x["value"], 1)})
    # 按模块聚合给一个代表值（取最高）+ 保留明细
    return sorted(temps, key=lambda t: (-t["celsius"]))


# CPU% / network rates need two-sample diff
async def _atlas_node_live() -> dict:
    s1 = await _scrape_node_exporter()
    if not s1:
        return {}
    await asyncio.sleep(0.4)
    s2 = await _scrape_node_exporter()

    def cpu_total(s):
        idle = _sum(s.get("node_cpu_seconds_total", []), lambda l: l.get("mode") == "idle")
        tot = _sum(s.get("node_cpu_seconds_total", []))
        return idle, tot
    i1, t1 = cpu_total(s1)
    i2, t2 = cpu_total(s2)
    cpu = max(0.0, min(100.0, (1 - (i2 - i1) / (t2 - t1)) * 100)) if t2 > t1 else 0.0

    memt = _sum(s2.get("node_memory_MemTotal_bytes", []))
    mema = _sum(s2.get("node_memory_MemAvailable_bytes", []))
    mem = (1 - mema / memt) * 100 if memt else 0.0

    def fs(s, key):
        return _sum(s.get(key, []), lambda l: l.get("mountpoint") == "/")
    dsz = fs(s2, "node_filesystem_size_bytes")
    dav = fs(s2, "node_filesystem_avail_bytes")
    disk = (1 - dav / dsz) * 100 if dsz else 0.0

    def net(s, key):
        return _sum(s.get(key, []),
                    lambda l: not re.match(r"lo|docker|veth|br-", l.get("device", "")))
    rx = (net(s2, "node_network_receive_bytes_total") -
          net(s1, "node_network_receive_bytes_total")) / 0.4 / 1024 / 1024
    tx = (net(s2, "node_network_transmit_bytes_total") -
          net(s1, "node_network_transmit_bytes_total")) / 0.4 / 1024 / 1024

    temps = _atlas_temps(s2)
    cpu_t = next((t["celsius"] for t in temps if t["module"] == "CPU"), 0)
    boot = _sum(s2.get("node_boot_time_seconds", []))
    return {"cpu": round(cpu, 1), "mem": round(mem, 1), "disk": round(disk, 1),
            "netIn": round(max(0, rx), 2), "netOut": round(max(0, tx), 2),
            "tempCpu": cpu_t, "temps": temps,
            "uptimeSec": int(time.time() - boot) if boot else 0}


# ── Health ─────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    try:
        r = await client.get(f"{PROM_URL}/-/healthy")
        prom_ok = r.status_code == 200
    except Exception:
        prom_ok = False
    return {"ok": True, "prometheus": prom_ok,
            "time": datetime.now(timezone.utc).isoformat()}


# ── Nodes ──────────────────────────────────────────────────────────
async def _obs_node_live() -> dict[str, dict]:
    """obs 里 2 台 Spark 的实时指标（DCGM + node + hwmon）。"""
    # 仅查有真实源的指标。SM activity/PCIe = DCGM PROF 类（按纪律未采，
    # 避免生产推理 GPU 上的 profiling 开销）→ 不再产出，前端相应移除（不伪造）。
    (gpu, fb_u, fb_f, gtemp, mtemp, pwr,
     cpu, memr, dsk, nin, nout, ibr, ibt) = await asyncio.gather(
        promql("DCGM_FI_DEV_GPU_UTIL"),
        promql("DCGM_FI_DEV_FB_USED"),
        promql("DCGM_FI_DEV_FB_FREE"),
        promql("DCGM_FI_DEV_GPU_TEMP"),
        promql("DCGM_FI_DEV_MEMORY_TEMP"),
        promql("DCGM_FI_DEV_POWER_USAGE"),
        promql('100 - (avg by (node) (rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)'),
        promql('(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100'),
        promql('(1 - node_filesystem_avail_bytes{mountpoint="/"} / node_filesystem_size_bytes{mountpoint="/"}) * 100'),
        promql('sum by (node) (rate(node_network_receive_bytes_total{device!~"lo|docker.*|veth.*|br-.*"}[1m])) / 1048576'),
        promql('sum by (node) (rate(node_network_transmit_bytes_total{device!~"lo|docker.*|veth.*|br-.*"}[1m])) / 1048576'),
        # 东西向 CX-7 RoCE/RDMA 真实吞吐 (×4 = port_data 单位 lanes→bytes 约定)
        promql('sum by (node) (rate(node_infiniband_port_data_received_bytes_total[1m])) * 4 / 1048576'),
        promql('sum by (node) (rate(node_infiniband_port_data_transmitted_bytes_total[1m])) * 4 / 1048576'),
    )
    g, fu, ff, gt, mt, pw = map(lambda r: _by(r, "node"),
                                [gpu, fb_u, fb_f, gtemp, mtemp, pwr])
    cp, me, dk, ni, no, ir, it = map(lambda r: _by(r, "node"),
                                     [cpu, memr, dsk, nin, nout, ibr, ibt])

    # hwmon 明细温度（带人话 label）按 node 聚
    htemp = await promql(
        'node_hwmon_temp_celsius * on (chip,sensor,node) group_left(label) '
        'node_hwmon_sensor_label')
    per_node_temps: dict[str, list] = {}
    for it in htemp:
        m = it["metric"]
        nd = m.get("node")
        if not nd or it["value"] <= 0 or it["value"] > 150:
            continue
        human = m.get("label") or m.get("sensor", "")
        per_node_temps.setdefault(nd, []).append({
            "module": _classify_temp(m.get("chip", ""), human),
            "label": human, "chip": m.get("chip", ""),
            "celsius": round(it["value"], 1)})

    # 节点键集 = 所有指标并集（GPU_UTIL 此 DCGM 配置可能整体为空，
    # (and a host with no obs node job; relying only on g/cp would drop nodes with only temp/power) 
    universe = set()
    for mp in (g, fu, ff, gt, mt, pw, cp, me, dk, ni, no):
        universe |= set(mp)
    universe |= set(per_node_temps)
    out = {}
    for obs_node in universe:
        vt = (fu.get(obs_node, 0) + ff.get(obs_node, 0)) or 1
        temps = sorted(per_node_temps.get(obs_node, []), key=lambda t: -t["celsius"])
        cpu_t = next((t["celsius"] for t in temps if t["module"] == "CPU"), 0)
        # Node kind drives VRAM% interpretation:
        #   discrete       → DCGM FB_USED/FB_TOTAL (dedicated VRAM)
        #   unified-arm-soc / apple-silicon → node_exporter MemAvailable (shared)
        # The choice is by `kind` field on each node (config-driven), not by
        # any hard-coded host name.
        is_unified = KIND_BY_OBS.get(obs_node, "discrete") != "discrete"
        vram_pct = me.get(obs_node, 0) if is_unified else (fu.get(obs_node, 0) / vt * 100)
        out[obs_node] = {
            "gpu": round(g.get(obs_node, 0), 1),
            "vram": round(vram_pct, 1),
            "vramKind": "unified" if is_unified else "discrete",
            "tempGpu": round(gt.get(obs_node, 0), 1),
            "tempMem": round(mt.get(obs_node, 0), 1),
            "tempCpu": cpu_t,
            "power": round(pw.get(obs_node, 0), 1),
            "cpu": round(cp.get(obs_node, 0), 1),
            "mem": round(me.get(obs_node, 0), 1),
            "disk": round(dk.get(obs_node, 0), 1),
            "netIn": round(ni.get(obs_node, 0), 2),
            "netOut": round(no.get(obs_node, 0), 2),
            "rdmaIn": round(ir.get(obs_node, 0), 2),
            "rdmaOut": round(it.get(obs_node, 0), 2),
            "temps": temps,
        }
    return out


async def _node_payload() -> list[dict]:
    obs_live, direct = await asyncio.gather(_obs_node_live(), _atlas_node_live())
    # `direct` is the host that runs the Hearth api itself (scraped via the
    # api container's own /proc + /sys, not via obs Prometheus). The legacy
    # name "_atlas_node_live" is preserved for now to minimize diff.
    # GPU metrics for that host still go via obs DCGM (if it has one) —
    # pulled out by its discrete-node obs label, set from config.
    discrete_gpu = obs_live.get(DISCRETE_OBS or "", {})
    out = []
    for n in NODES:
        live = {"gpu": 0, "vram": 0, "vramKind": "discrete", "tempGpu": 0,
                "tempMem": 0, "tempCpu": 0, "power": 0, "cpu": 0, "mem": 0,
                "disk": 0, "netIn": 0, "netOut": 0, "rdmaIn": 0,
                "rdmaOut": 0, "temps": []}
        if n.get("node_source") == "direct":
            live.update({k: discrete_gpu.get(k, 0)
                         for k in ("gpu", "vram", "tempGpu", "tempMem", "power")})
            live["vramKind"] = discrete_gpu.get("vramKind", "discrete")
            if direct:
                live.update({k: direct[k] for k in ("cpu", "mem", "disk", "netIn",
                                                    "netOut", "tempCpu", "temps")
                             if k in direct})
            up = bool(direct) or bool(discrete_gpu)
        elif n["obs_node"] and n["obs_node"] in obs_live:
            live.update(obs_live[n["obs_node"]])
            up = True
        else:
            up = False   # node not in obs Prometheus job → honestly mark no-data
        out.append({**{k: v for k, v in n.items() if k != "node_source"},
                    "live": live, "up": up})
    return out


@app.get("/api/nodes")
async def nodes_list():
    return await _node_payload()


@app.get("/api/nodes/{node_id}")
async def node_detail(node_id: str):
    if node_id not in NODE_BY_ID:
        raise HTTPException(404, f"unknown node: {node_id}")
    node = next(n for n in await _node_payload() if n["id"] == node_id)
    obs = NODE_BY_ID[node_id]["obs_node"]
    if obs:
        lbl = f'node="{obs}"'
        gh, ch, mh, th, ph = await asyncio.gather(
            promql_range(f"DCGM_FI_DEV_GPU_UTIL{{{lbl}}}"),
            promql_range(f'100 - (avg(rate(node_cpu_seconds_total{{{lbl},mode="idle"}}[1m]))*100)'),
            promql_range(f'(1 - node_memory_MemAvailable_bytes{{{lbl}}}/node_memory_MemTotal_bytes{{{lbl}}})*100'),
            promql_range(f"DCGM_FI_DEV_GPU_TEMP{{{lbl}}}"),
            promql_range(f"DCGM_FI_DEV_POWER_USAGE{{{lbl}}}"),
        )
        f = lambda r: [v for _, v in r[0]["values"]] if r else []
        node["history"] = {"gpu": f(gh), "cpu": f(ch), "mem": f(mh),
                           "tempGpu": f(th), "power": f(ph)}
    else:
        node["history"] = {}
    node["hostedModels"] = [m for m in await models_list()
                            if node_id in (m.get("nodes") or [])]
    return node


# ── Cluster (聚合在本服务算，不依赖 recording rules) ─────────────────
@app.get("/api/cluster")
async def cluster():
    total_vram = sum(n["gpu"]["mem"] for n in NODES)
    total_ram  = sum(n["ram"] for n in NODES)
    (tps, rps, g_avg, g_max, fb_u, fb_t, pw, gt,
     cpu, memu, memt) = await asyncio.gather(
        promql("sum(rate(litellm_total_tokens_metric[1m]))"),
        promql("sum(rate(litellm_total_requests_metric[1m]))"),
        promql("avg(DCGM_FI_DEV_GPU_UTIL)"),
        promql("max(DCGM_FI_DEV_GPU_UTIL)"),
        promql("sum(DCGM_FI_DEV_FB_USED)/1024"),                                  # atlas 独显 FB (GiB)
        promql("sum(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes)/1073741824"),  # Σ Spark GB10 统一内存已用 (GiB)
        promql("sum(DCGM_FI_DEV_POWER_USAGE)"),
        promql("max(DCGM_FI_DEV_GPU_TEMP)"),
        promql('avg(100 - (avg by (node)(rate(node_cpu_seconds_total{mode="idle"}[1m]))*100))'),
        promql("sum(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes)/1073741824"),
        promql("sum(node_memory_MemTotal_bytes)/1073741824"),
    )
    tps_h, pow_h, temp_h = await asyncio.gather(
        promql_range("sum(rate(litellm_total_tokens_metric[1m]))"),
        promql_range("sum(DCGM_FI_DEV_POWER_USAGE)"),
        promql_range("max(DCGM_FI_DEV_GPU_TEMP)"),
    )
    ser = lambda r: [v for _, v in r[0]["values"]] if r else []
    roll = await _litellm_rollup()   # Hero 累计 + 延迟摘要(LiteLLM OSS Postgres, 非企业版)
    return {
        "totals": {"nodes": len(NODES), "gpus": len(NODES),
                   "totalVram": total_vram, "totalRam": total_ram,
                   "totalCores": sum(n["cpu"]["cores"] for n in NODES),
                   "totalThreads": sum(n["cpu"]["threads"] for n in NODES),
                   "totalDisk": sum(n["disk"] for n in NODES),
                   "totalFp16": sum(n["gpu"]["fp16"] for n in NODES),
                   "totalFp4": sum(n["gpu"]["fp4"] for n in NODES),
                   "pflopsFp4": round(sum(n["gpu"]["fp4"] for n in NODES) / 1000, 2)},
        "live": {"tpsNow": _one(tps), "rpsNow": _one(rps), "kvNow": 0,
                 "powNow": round(_one(pw), 1), "tempNow": round(_one(gt), 1),
                 "gpuAvg": round(_one(g_avg), 1), "gpuMax": round(_one(g_max), 1),
                 "vramUsed": round(_one(fb_u) + _one(fb_t), 1),  # 独显FB + Σ GB10统一内存
                 "vramTotal": total_vram,                         # 目录: 24 + 4×128 = 536
                 "cpuAvg": round(_one(cpu), 1),
                 "memUsed": round(_one(memu), 1),
                 "memTotal": round(_one(memt), 1) or total_ram,
                 "latP50": roll["latP50"], "latP95": roll["latP95"]},
        "history": {"tps": ser(tps_h), "rps": [], "kv": [],
                    "pow": ser(pow_h), "temp": ser(temp_h)},
        "uptimeSec": int(_one(await promql(
            "max(time() - node_boot_time_seconds)")) or 0),
        "reqTotal": roll["reqTotal"],   # vLLM 原生累计(非 litellm 企业版门控)
        "tokTotal": roll["tokTotal"],
    }


# ── Models (真实：直采 vLLM 原生 /metrics；LiteLLM prometheus 企业版门控不可用) ──
# 真实模型来自 litellm /v1/models（comfyui mode 当前态）。仅 vLLM 后端暴露
# 当前真实部署（2026-05-19 核实，部署随用户调整会变；目录须随真相更新）：
#  - qwen3-coder（运行中，主模型）= Qwen3-Coder-Next-FP8，vLLM @ .188:8888
#    + .189:8888（spark-03/04），网关名 qwen3-coder-next，直采 .188:8888/metrics。
#  - deepseek-v4-flash 已停（网关推理 500，路由 .156:8000 无指标）→ metrics_url
#    指其真实后端，停着就诚实显 no-metrics/离线，重启自动恢复；不再盗用别人端点。
#  - gemma / qwen3-vl 仍在网关但走 .156:8001/8002（无 vLLM /metrics）→
#    metrics_url=None 诚实标"无实时指标源"，不伪造数字。
#  - qwen3.5-122b-abliterated 已从网关移除 → 不再列（幽灵条目=信息不对）。
# ── 模型自动发现（LiteLLM 网关驱动，不再手工维护目录）──────────────
# 真相源 = 网关 /model/info(route→backend) + /health(backend up/down)。
# 部署随用户调整，监控自动反映、无需改代码、不会漏显也不会显错。
# MODEL_META 只做"好看"的静态修饰(显示名/厂商/标签)；未知模型自动从
# id 推导，绝不因此漏显或标错状态。
_ALIAS_ROUTES = {"default", "code", "agent", "long", "vision", "fast",
                 "reason", "reasoning", "embed", "embedding", "rerank", "vl"}
MODEL_META = {
    "qwen3-coder-next": {"display": "Qwen3-Coder-Next", "vendor": "Alibaba",
        "kind": "chat", "tags": ["coding"]},
    "deepseek-v4-flash": {"display": "DeepSeek-V4-Flash", "vendor": "DeepSeek",
        "kind": "chat", "tags": ["reasoning"]},
    "deepseek-v4-flash-pp4": {"display": "DeepSeek-V4-Flash · PP4",
        "vendor": "DeepSeek", "kind": "chat", "tags": ["reasoning", "test"]},
    "minimax-m2.7": {"display": "MiniMax-M2.7", "vendor": "MiniMax",
        "kind": "chat", "tags": ["reasoning"]},
    "gemma-4-31b-abliterated": {"display": "Gemma-4-31B-abliterated",
        "vendor": "Google", "kind": "vision", "tags": ["vision", "abliterated"]},
    "qwen3-vl-abliterated": {"display": "Qwen3-VL-8B-abliterated",
        "vendor": "Alibaba", "kind": "vision", "tags": ["vision", "abliterated"]},
}


def _host_of(api_base: str) -> str:
    m = re.search(r"//([^:/]+)", api_base or "")
    return m.group(1) if m else ""


def _meta_for(route: str) -> dict:
    if route in MODEL_META:
        return dict(MODEL_META[route])
    low = route.lower()
    vendor = ("Alibaba" if "qwen" in low else "DeepSeek" if "deepseek" in low
              else "MiniMax" if "minimax" in low else "Google" if "gemma" in low
              else "Meta" if "llama" in low else "—")
    return {"display": route.replace("_", " ").replace("-", " ").title(),
            "vendor": vendor,
            "kind": "vision" if ("vl" in low or "vision" in low) else "chat",
            "tags": []}


async def _gw_get(path: str, timeout: float):
    try:
        r = await client.get(f"{LITELLM_URL}{path}",
                             headers={"Authorization": f"Bearer {LITELLM_KEY}"},
                             timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


async def _ctx_of(base: str) -> int:
    try:
        r = await client.get(f"{base}/v1/models", timeout=3.0)
        r.raise_for_status()
        return int(r.json()["data"][0].get("max_model_len") or 0)
    except Exception:
        return 0


_DISCO = {"ts": 0.0, "models": None}
_DISCO_TTL = 25.0          # 部署拓扑变化慢；富指标(tps/kv)仍每 snapshot 直采
_DISCO_TASK = None


async def _discover() -> list[dict]:
    """网关 /model/info + /health → 逻辑模型列表（按主 route 折叠别名/副本）。
    每条: id/route/display/vendor/kind/tags/nodes/up/vllm_bases/ctx/framework。"""
    info = await _gw_get("/model/info", 8.0) or {}
    # /health 仅作辅助/兜底信号;超时收紧,避免网关 /health 偶发卡顿拖慢
    # 整个发现周期(直接探测各后端才是主判定路径)
    health = await _gw_get("/health", 8.0) or {}
    up_map: dict[str, bool] = {}
    for key, flag in (("healthy_endpoints", True), ("unhealthy_endpoints", False)):
        for e in (health.get(key) or []):
            ab = (e.get("api_base") or "").rstrip("/")
            up_map[ab[:-3] if ab.endswith("/v1") else ab] = flag
    route_bases: dict[str, set] = {}
    for m in (info.get("data") or []):
        rt = m.get("model_name") or ""
        ab = ((m.get("litellm_params") or {}).get("api_base") or "").rstrip("/")
        if not rt or not ab:
            continue
        route_bases.setdefault(rt, set()).add(ab[:-3] if ab.endswith("/v1") else ab)
    base_routes: dict[str, set] = {}
    for rt, bs in route_bases.items():
        for b in bs:
            base_routes.setdefault(b, set()).add(rt)

    def _primary(routes: set) -> str:
        non_alias = sorted(r for r in routes if r not in _ALIAS_ROUTES)
        return non_alias[0] if non_alias else sorted(routes)[0]

    models: dict[str, dict] = {}
    for b, routes in base_routes.items():
        prt = _primary(routes)
        meta = _meta_for(prt)
        mm = models.setdefault(prt, {
            "id": prt, "route": f"litellm/{prt}", "display": meta["display"],
            "vendor": meta["vendor"], "kind": meta["kind"],
            "tags": list(meta["tags"]), "params": "—", "quant": "—",
            "framework": "—", "vram": 0, "ctx": 0,
            "_nodes": set(), "_aliases": set(), "_bases": [],
            "up": False, "vllm_bases": [], "llamacpp_bases": [], "sglang_bases": []})
        mm["_bases"].append(b)
        host = _host_of(b)
        if host in IP_TO_ID:
            mm["_nodes"].add(IP_TO_ID[host])
        for r in routes:
            if r != prt:
                mm["_aliases"].add(r)
        if up_map.get(b):
            mm["up"] = True
    # up 判定:直接探后端为主(/metrics 或 /v1/models 可达即活),/health 仅做
    # 辅助 / 兜底——避免单点故障(网关 /health 偶发超时 22s)把所有模型误标
    # stopped。直接探测自给自足,网关挂了监控仍如实反映后端真相。
    for mm in models.values():
        for b in mm["_bases"]:
            sc = await _scrape_vllm(b)
            if any(str(k).startswith("vllm:") for k in sc) or sc.get("__e2e_buckets"):
                mm["vllm_bases"].append(b); mm["up"] = True
                continue
            sc2 = await _scrape_llamacpp(b)
            if sc2:                              # 有 llamacpp:* 行
                mm["llamacpp_bases"].append(b); mm["up"] = True
                continue
            sc3 = await _scrape_sglang(b)
            if any(str(k).startswith("sglang:") for k in sc3) or sc3.get("__e2e_buckets"):
                mm["sglang_bases"].append(b); mm["up"] = True
                continue
            try:                                 # 无指标但 /v1/models 通 → 在线
                r = await client.get(f"{b}/v1/models", timeout=3.0)
                if r.status_code == 200:
                    mm["up"] = True
            except Exception:
                pass
            if up_map.get(b):                    # 网关健康作辅助证据
                mm["up"] = True
        if mm["vllm_bases"]:
            mm["framework"] = "vLLM"
            mm["ctx"] = await _ctx_of(mm["vllm_bases"][0])
        elif mm["llamacpp_bases"]:
            mm["framework"] = "llama.cpp"
            mm["ctx"] = await _ctx_of(mm["llamacpp_bases"][0])
        elif mm["sglang_bases"]:
            mm["framework"] = "SGLang"
            mm["ctx"] = await _ctx_of(mm["sglang_bases"][0])
        if mm["_aliases"]:
            mm["tags"] = mm["tags"] + ["alias:" + ",".join(sorted(mm["_aliases"]))]
        mm["nodes"] = sorted(mm.pop("_nodes"))
        # Multi-node (tensor/pipeline parallel) override. A TP=N deployment
        # exposes ONE gateway endpoint (the Ray/torchrun head), so auto-
        # discovery only ever attributes the model to the head node — the
        # worker nodes look idle while genuinely running TP shards. When the
        # operator declares the span in `model_topology`, attribute the model
        # to every participating node so node GPU-activity rings (derived from
        # this model's tps) light up across the whole TP group, not just head.
        topo = (HEARTH_CFG.get("model_topology") or {}).get(mm["id"])
        if topo and topo.get("nodes"):
            valid = [n for n in topo["nodes"] if n in IP_TO_ID.values()]
            mm["nodes"] = sorted(set(mm["nodes"]) | set(valid))
            if topo.get("parallelism") and topo["parallelism"] not in mm["tags"]:
                mm["tags"] = mm["tags"] + [topo["parallelism"]]
        mm.pop("_aliases", None)
        mm.pop("_bases", None)
    return sorted(models.values(),
                  key=lambda x: (not x["up"],
                                 not (x["vllm_bases"] or x["llamacpp_bases"]),
                                 x["id"]))


async def _disco_loop():
    while True:
        try:
            d = await _discover()
            _DISCO["models"], _DISCO["ts"] = d, time.time()
        except Exception:
            pass
        await asyncio.sleep(_DISCO_TTL)


async def _disco_cached() -> list[dict]:
    global _DISCO_TASK
    if _DISCO_TASK is None or _DISCO_TASK.done():
        _DISCO_TASK = asyncio.create_task(_disco_loop())
    if _DISCO["models"] is None:
        try:
            _DISCO["models"] = await _discover()
            _DISCO["ts"] = time.time()
        except Exception:
            return []
    return _DISCO["models"]


_VLLM_SCALARS = {
    "vllm:generation_tokens_total", "vllm:prompt_tokens_total",
    "vllm:request_success_total",
    "vllm:time_to_first_token_seconds_sum", "vllm:time_to_first_token_seconds_count",
    "vllm:num_requests_running", "vllm:num_requests_waiting",
    "vllm:request_time_per_output_token_seconds_sum",
    "vllm:request_time_per_output_token_seconds_count",
    "vllm:kv_cache_usage_perc",
    "vllm:e2e_request_latency_seconds_sum", "vllm:e2e_request_latency_seconds_count",
}


def _hquant(buckets: dict, q: float) -> float:
    """Prometheus 式 histogram_quantile（累积桶 + 桶内线性插值）。返回秒。"""
    if not buckets:
        return 0.0
    pts = []
    for le, c in buckets.items():
        le_f = float("inf") if le in ("+Inf", "Inf") else float(le)
        pts.append((le_f, c))
    pts.sort()
    total = pts[-1][1]
    if total <= 0:
        return 0.0
    rank = q * total
    prev_le, prev_c = 0.0, 0.0
    for le_f, c in pts:
        if c >= rank:
            if le_f == float("inf"):
                return prev_le
            if c == prev_c:
                return le_f
            return prev_le + (le_f - prev_le) * ((rank - prev_c) / (c - prev_c))
        prev_le, prev_c = le_f, c
    return pts[-1][0]


_LLAMACPP_SCALARS = {
    "llamacpp:tokens_predicted_total",
    "llamacpp:tokens_predicted_seconds_total",
    "llamacpp:prompt_tokens_total",
    "llamacpp:prompt_seconds_total",
    "llamacpp:requests_processing",
    "llamacpp:requests_deferred",
    "llamacpp:n_decode_total",
    "llamacpp:predicted_tokens_seconds",
    "llamacpp:prompt_tokens_seconds",
}


async def _scrape_llamacpp(base: str) -> dict:
    """直采 llama.cpp 原生 /metrics（与 vLLM 同 Prometheus 文本格式，前缀
    llamacpp:）。返回所需标量。无 e2e/TTFT 直方图（llama.cpp 不暴露）→
    p50/p95/p99/TTFT 留 0 诚实标"未测", 不伪造。"""
    out: dict[str, float] = {}
    try:
        r = await client.get(f"{base}/metrics", timeout=4.0)
        r.raise_for_status()
        for line in r.text.splitlines():
            if not line or line[0] == "#":
                continue
            sp = line.rsplit(" ", 1)
            if len(sp) != 2:
                continue
            name = sp[0].split("{")[0]
            if name not in _LLAMACPP_SCALARS:
                continue
            try:
                v = float(sp[1])
            except ValueError:
                continue
            out[name] = out.get(name, 0.0) + v
    except Exception:
        return {}
    return out


_SGLANG_SCALARS = {
    "sglang:num_running_reqs", "sglang:num_queue_reqs",
    "sglang:gen_throughput",
    "sglang:prompt_tokens_total", "sglang:generation_tokens_total",
    "sglang:time_to_first_token_seconds_sum", "sglang:time_to_first_token_seconds_count",
    "sglang:inter_token_latency_seconds_sum", "sglang:inter_token_latency_seconds_count",
    "sglang:token_usage",
    "sglang:e2e_request_latency_seconds_sum", "sglang:e2e_request_latency_seconds_count",
}


async def _scrape_sglang(base: str) -> dict:
    """直采 SGLang 原生 /metrics（需启动加 --enable-metrics；前缀 sglang:）。
    指标比 llama.cpp 丰富，含 TTFT / inter-token / e2e 直方图，接近 vLLM。

    ⚠️ 注意：基于 SGLang 官方文档的指标名实现，**尚未对 live SGLang 实例
    端到端验证**（开发集群无 SGLang 后端）。若你的 SGLang 版本指标名不同
    导致显示异常，请开 issue 反馈实际 `sglang:*` 名称，我们快速适配。"""
    out: dict[str, float] = {}
    e2e_b: dict[str, float] = {}
    try:
        r = await client.get(f"{base}/metrics", timeout=4.0)
        r.raise_for_status()
        for line in r.text.splitlines():
            if not line or line[0] == "#":
                continue
            sp = line.rsplit(" ", 1)
            if len(sp) != 2:
                continue
            head, name = sp[0], sp[0].split("{")[0]
            try:
                v = float(sp[1])
            except ValueError:
                continue
            if name == "sglang:e2e_request_latency_seconds_bucket":
                mle = re.search(r'le="([^"]+)"', head)
                if mle:
                    e2e_b[mle.group(1)] = e2e_b.get(mle.group(1), 0.0) + v
            elif name in _SGLANG_SCALARS:
                out[name] = out.get(name, 0.0) + v
    except Exception:
        return {}
    out["__e2e_buckets"] = e2e_b
    return out


async def _scrape_vllm(base: str) -> dict:
    """直采 vLLM 原生 /metrics（prom 文本）。含 V1 改名指标 + e2e 直方图桶。"""
    out: dict[str, float] = {}
    e2e_b: dict[str, float] = {}
    try:
        r = await client.get(f"{base}/metrics", timeout=4.0)
        r.raise_for_status()
        for line in r.text.splitlines():
            if not line or line[0] == "#":
                continue
            sp = line.rsplit(" ", 1)
            if len(sp) != 2:
                continue
            head, name = sp[0], sp[0].split("{")[0]
            try:
                v = float(sp[1])
            except ValueError:
                continue
            if name == "vllm:e2e_request_latency_seconds_bucket":
                mle = re.search(r'le="([^"]+)"', head)
                if mle:
                    e2e_b[mle.group(1)] = e2e_b.get(mle.group(1), 0.0) + v
            elif name in _VLLM_SCALARS:
                out[name] = out.get(name, 0.0) + v
    except Exception:
        return {}
    out["__e2e_buckets"] = e2e_b
    return out


def _merge_scrape(dicts: list[dict]) -> dict:
    """多副本 vLLM /metrics 合并：标量相加，e2e 桶相加（总吞吐口径）。"""
    acc: dict = {"__e2e_buckets": {}}
    for d in dicts:
        for k, v in (d or {}).items():
            if k == "__e2e_buckets":
                for le, c in (v or {}).items():
                    acc["__e2e_buckets"][le] = acc["__e2e_buckets"].get(le, 0.0) + c
            else:
                acc[k] = acc.get(k, 0.0) + v
    return acc


@app.get("/api/models")
async def models_list():
    disco = await _disco_cached()
    # 所有"在线且有 vLLM 指标"的后端 → 两次采样算 counter→rate（含多副本）
    vbases = sorted({b for m in disco for b in m.get("vllm_bases", [])})
    lbases = sorted({b for m in disco for b in m.get("llamacpp_bases", [])})
    gbases = sorted({b for m in disco for b in m.get("sglang_bases", [])})
    s1 = {b: await _scrape_vllm(b) for b in vbases}
    l1 = {b: await _scrape_llamacpp(b) for b in lbases}
    g1 = {b: await _scrape_sglang(b) for b in gbases}
    await asyncio.sleep(0.5)
    s2 = {b: await _scrape_vllm(b) for b in vbases}
    l2 = {b: await _scrape_llamacpp(b) for b in lbases}
    g2 = {b: await _scrape_sglang(b) for b in gbases}
    out = []
    for m in disco:
        vb = m.get("vllm_bases") or []
        lb = m.get("llamacpp_bases") or []
        gb = m.get("sglang_bases") or []
        base_keys = ("id", "display", "vendor", "kind", "params", "quant",
                     "ctx", "framework", "nodes", "vram", "route", "tags")
        card = {k: m[k] for k in base_keys}
        if vb:                                  # 真实 vLLM 指标（可能多副本汇总）
            a = _merge_scrape([s1.get(b) or {} for b in vb])
            b = _merge_scrape([s2.get(b) or {} for b in vb])
            dt = 0.5
            tps = max(0.0, (b.get("vllm:generation_tokens_total", 0)
                            - a.get("vllm:generation_tokens_total", 0)) / dt)
            rps = max(0.0, (b.get("vllm:request_success_total", 0)
                            - a.get("vllm:request_success_total", 0)) / dt)
            tcnt = b.get("vllm:time_to_first_token_seconds_count", 0)
            tsum = b.get("vllm:time_to_first_token_seconds_sum", 0)
            ttft = (tsum / tcnt * 1000) if tcnt else 0
            pcnt = b.get("vllm:request_time_per_output_token_seconds_count", 0)
            psum = b.get("vllm:request_time_per_output_token_seconds_sum", 0)
            tpot = (psum / pcnt * 1000) if pcnt else 0
            kv = b.get("vllm:kv_cache_usage_perc", 0) * 100 / max(1, len(vb))
            e2e_b = b.get("__e2e_buckets") or {}
            running = b.get("vllm:num_requests_running", 0)
            waiting = b.get("vllm:num_requests_waiting", 0)
            state = "serving" if running > 0 or tps > 0 else "idle"
            live = {"tps": round(tps, 1), "rps": round(rps, 3),
                    "ttft": round(ttft, 1), "tpot": round(tpot, 1),
                    "kv": round(kv, 1), "running": int(running),
                    "waiting": int(waiting), "metrics": "vllm",
                    # 真实驻留探针：vLLM 可达且模型已加载 → 权重常驻、毫秒级可服务
                    "resident": True,
                    "p50": round(_hquant(e2e_b, 0.50) * 1000, 0),
                    "p95": round(_hquant(e2e_b, 0.95) * 1000, 0),
                    "p99": round(_hquant(e2e_b, 0.99) * 1000, 0)}
        elif lb:                                # llama.cpp 真实指标（可能多副本汇总）
            a = _merge_scrape([l1.get(b) or {} for b in lb])
            b = _merge_scrape([l2.get(b) or {} for b in lb])
            dt = 0.5
            tps = max(0.0, (b.get("llamacpp:tokens_predicted_total", 0)
                            - a.get("llamacpp:tokens_predicted_total", 0)) / dt)
            # tpot: 解码耗时差 / 解码 token 差 → ms/token
            d_tok = max(0.0, b.get("llamacpp:tokens_predicted_total", 0)
                            - a.get("llamacpp:tokens_predicted_total", 0))
            d_sec = max(0.0, b.get("llamacpp:tokens_predicted_seconds_total", 0)
                            - a.get("llamacpp:tokens_predicted_seconds_total", 0))
            tpot = (d_sec / d_tok * 1000) if d_tok > 0 else 0
            running = b.get("llamacpp:requests_processing", 0)
            waiting = b.get("llamacpp:requests_deferred", 0)
            state = "serving" if running > 0 or tps > 0 else "idle"
            # llama.cpp /metrics 不暴露 TTFT/e2e 直方图/KV% → 留 0 诚实标"未测",
            # 不伪造；rps 同理(无 request_success_total)。前端 metrics=llamacpp 可
            # 据此显示"—"代替 0。
            live = {"tps": round(tps, 1), "rps": 0,
                    "ttft": 0, "tpot": round(tpot, 1),
                    "kv": 0, "running": int(running),
                    "waiting": int(waiting), "metrics": "llamacpp",
                    "resident": True,
                    "p50": 0, "p95": 0, "p99": 0}
        elif gb:                                # SGLang 真实指标(含 TTFT/e2e, 接近 vLLM)
            a = _merge_scrape([g1.get(b) or {} for b in gb])
            b = _merge_scrape([g2.get(b) or {} for b in gb])
            dt = 0.5
            tps = max(0.0, (b.get("sglang:generation_tokens_total", 0)
                            - a.get("sglang:generation_tokens_total", 0)) / dt)
            tcnt = b.get("sglang:time_to_first_token_seconds_count", 0)
            tsum = b.get("sglang:time_to_first_token_seconds_sum", 0)
            ttft = (tsum / tcnt * 1000) if tcnt else 0
            icnt = b.get("sglang:inter_token_latency_seconds_count", 0)
            isum = b.get("sglang:inter_token_latency_seconds_sum", 0)
            tpot = (isum / icnt * 1000) if icnt else 0
            kv = b.get("sglang:token_usage", 0) * 100 / max(1, len(gb))
            e2e_b = b.get("__e2e_buckets") or {}
            running = b.get("sglang:num_running_reqs", 0)
            waiting = b.get("sglang:num_queue_reqs", 0)
            state = "serving" if running > 0 or tps > 0 else "idle"
            live = {"tps": round(tps, 1), "rps": 0,
                    "ttft": round(ttft, 1), "tpot": round(tpot, 1),
                    "kv": round(kv, 1), "running": int(running),
                    "waiting": int(waiting), "metrics": "sglang",
                    "resident": True,
                    "p50": round(_hquant(e2e_b, 0.50) * 1000, 0),
                    "p95": round(_hquant(e2e_b, 0.95) * 1000, 0),
                    "p99": round(_hquant(e2e_b, 0.99) * 1000, 0)}
        elif m.get("up"):                       # 网关健康但无可识别 /metrics
            state = "online"                    # 在线·服务中，无详细指标（不伪造）
            live = {"metrics": "none", "resident": True}
        else:                                   # 网关判定后端 down → 已停
            state = "stopped"
            live = {"metrics": "none", "resident": False}
        out.append({**card, "state": state, "live": live})
    return out


@app.get("/api/models/{model_id}")
async def model_detail(model_id: str):
    b = next((m for m in await models_list() if m["id"] == model_id), None)
    if not b:
        raise HTTPException(404, f"unknown model: {model_id}")
    return b


# ── Alerts / Logs (优雅降级) ───────────────────────────────────────
async def _alerts(nodes=None, log=None):
    """Rule engine — derives alerts from already-collected metrics (no
    Alertmanager dependency). nodes/log can be passed in to reuse the SSE
    snapshot's data instead of recomputing _node_payload (expensive).
    Each alert carries a stable `key` (node:rule) so the push notifier can
    detect fire / resolve transitions without spamming on every tick."""
    if nodes is None:
        nodes = await _node_payload()
    if log is None:
        log = await _litellm_request_log(60)
    out = []
    for n in nodes:
        L, nm, nid = n["live"], n["name"], n["id"]
        if not n.get("up"):
            out.append({"key": f"{nid}:offline", "sev": "bad",
                        "msg": f"{nm} offline", "sub": f"{n['ip']} · no metrics", "when": "live"})
            continue
        gt = L.get("tempGpu", 0)
        if gt >= 90:
            out.append({"key": f"{nid}:gpu_temp", "sev": "bad",
                        "msg": f"{nm} GPU overheating {gt:.0f}°C",
                        "sub": "past critical — shed load", "when": "live"})
        elif gt >= 85:
            out.append({"key": f"{nid}:gpu_temp", "sev": "hot",
                        "msg": f"{nm} GPU hot {gt:.0f}°C",
                        "sub": "near thermal throttle", "when": "live"})
        if L.get("mem", 0) >= 95:
            out.append({"key": f"{nid}:mem", "sev": "warn",
                        "msg": f"{nm} memory pressure {L['mem']:.0f}%",
                        "sub": "system/unified memory near limit", "when": "live"})
        if L.get("disk", 0) >= 85:
            out.append({"key": f"{nid}:disk", "sev": "warn",
                        "msg": f"{nm} disk {L['disk']:.0f}%",
                        "sub": "root filesystem filling up", "when": "live"})
    err = sum(1 for e in log[:40] if e.get("status") != "200")
    if err >= 5:
        out.append({"key": "gateway:errors", "sev": "warn",
                    "msg": f"gateway errors · {err}/40",
                    "sub": "LiteLLM 5xx rate elevated recently", "when": "last 40 reqs"})
    up = sum(1 for n in nodes if n.get("up"))
    if not out:
        out.append({"key": None, "sev": "ok",
                    "msg": f"cluster healthy · {up}/{len(nodes)} nodes online",
                    "sub": "all metrics within thresholds", "when": "live"})
    return out[:12]


@app.get("/api/alerts")
async def alerts():
    return await _alerts()


# ── Alert push notifier (pluggable channels) ──────────────────────────
# Fires a notification on healthy→firing transition, again on resolve,
# and (optionally) re-fires while still firing every `repeat_after_minutes`.
# State persists to disk so a restart doesn't re-spam. Config in hearth.yaml
# `alerts:` section. Channel secrets come from env vars (names in YAML),
# never the YAML itself.
_SEV_RANK = {"ok": 0, "warn": 1, "hot": 2, "bad": 3}
_ALERT_STATE_FILE = os.environ.get("HEARTH_ALERT_STATE", "/tmp/hearth-alert-state.json")


def _load_alert_state() -> dict:
    try:
        return json.loads(Path(_ALERT_STATE_FILE).read_text())
    except Exception:
        return {}


def _save_alert_state(s: dict) -> None:
    try:
        Path(_ALERT_STATE_FILE).write_text(json.dumps(s))
    except Exception:
        pass


_ALERT_STATE = _load_alert_state()


def _ch_url(ch: dict, key: str) -> str:
    """Resolve a channel URL/secret: prefer literal `<key>`, else env `<key>_env`."""
    return ch.get(key) or os.environ.get(ch.get(f"{key}_env", ""), "")


async def _push_channel(ch: dict, title: str, body: str, sev: str) -> None:
    typ = ch.get("type", "")
    text = f"{title}\n{body}".strip()
    try:
        if typ == "ntfy":
            url = _ch_url(ch, "url")
            if url:
                # ntfy Title is an HTTP header → must be latin-1. Strip emoji
                # (severity is already conveyed by Priority + Tags); the full
                # text with emoji still goes in the UTF-8 body.
                hdr_title = title.encode("ascii", "ignore").decode("ascii").strip() or "Hearth alert"
                await client.post(url, content=text.encode("utf-8"), timeout=8.0, headers={
                    "Title": hdr_title,
                    "Priority": "urgent" if sev == "bad" else "high" if sev == "hot" else "default",
                    "Tags": "rotating_light" if sev == "bad" else "warning" if sev == "hot"
                            else "white_check_mark" if sev == "ok" else "information_source",
                })
        elif typ == "telegram":
            token = os.environ.get(ch.get("token_env", ""), "")
            chat = str(ch.get("chat_id", ""))
            if token and chat:
                await client.post(f"https://api.telegram.org/bot{token}/sendMessage",
                                  json={"chat_id": chat, "text": text}, timeout=8.0)
        elif typ == "discord":
            url = _ch_url(ch, "webhook_url")
            if url:
                await client.post(url, json={"content": f"**{title}**\n{body}"}, timeout=8.0)
        elif typ == "slack":
            url = _ch_url(ch, "webhook_url")
            if url:
                await client.post(url, json={"text": f"*{title}*\n{body}"}, timeout=8.0)
        elif typ == "webhook":                       # generic JSON POST — escape hatch
            url = _ch_url(ch, "url")
            if url:
                await client.post(url, json={"title": title, "body": body, "severity": sev},
                                  timeout=8.0)
        else:
            print(f"[hearth] unknown alert channel type: {typ}", file=sys.stderr)
    except Exception as e:
        print(f"[hearth] alert push to {typ} failed: {e}", file=sys.stderr)


async def _notify_alerts(alerts_list: list) -> None:
    cfg = HEARTH_CFG.get("alerts") or {}
    if not cfg.get("enabled"):
        return
    channels = cfg.get("channels") or []
    if not channels:
        return
    min_rank = _SEV_RANK.get(cfg.get("min_severity", "warn"), 1)
    repeat_s = float(cfg.get("repeat_after_minutes", 30)) * 60
    now = time.time()
    cname = (HEARTH_CFG.get("display") or {}).get("cluster_name", "Hearth")

    firing = {a["key"]: a for a in alerts_list
              if a.get("key") and _SEV_RANK.get(a["sev"], 0) >= min_rank}
    pushes = []   # (title, body, sev)

    for key, a in firing.items():
        st = _ALERT_STATE.get(key)
        if st is None:
            pushes.append((f"🔴 {a['msg']}", a.get("sub", ""), a["sev"]))
            _ALERT_STATE[key] = {"sev": a["sev"], "msg": a["msg"],
                                 "first": now, "last_push": now}
        elif repeat_s > 0 and now - st.get("last_push", 0) >= repeat_s:
            pushes.append((f"🔴 still firing · {a['msg']}", a.get("sub", ""), a["sev"]))
            st["last_push"] = now

    for key in list(_ALERT_STATE.keys()):
        if key not in firing:
            msg = _ALERT_STATE[key].get("msg", key)
            pushes.append((f"✅ resolved · {msg}", "", "ok"))
            del _ALERT_STATE[key]

    if pushes:
        _save_alert_state(_ALERT_STATE)
        for title, body, sev in pushes:
            full = f"[{cname}] {title}"
            await asyncio.gather(*[_push_channel(ch, full, body, sev) for ch in channels],
                                 return_exceptions=True)


# ── 全量快照缓存：解耦 SSE 发送节奏与重活构建 ───────────────────────
# 一个 tick 重活 ~7s(nodes 双采样+obs 串行 + alerts 旧版重复跑 node)。
# 改为：重活每 _SNAP_TTL 算一次(nodes 只算一次, alerts 复用)，SSE 每
# TICK_SEC 发最新快照 → 前端每 1.5s 收帧平滑重渲染，数据 ~2.5s 新鲜。
_SNAP = {"ts": 0.0, "data": None}
_SNAP_TTL = 2.5


async def _build_snapshot() -> dict:
    nodes = await _node_payload()                 # 贵, 只算一次
    log = await _litellm_request_log(40)
    cl, models = await asyncio.gather(cluster(), models_list())
    al = await _alerts(nodes, log)                # 复用 nodes/log, 不重复跑
    await _notify_alerts(al)                       # 推送渠道(跳变才发, 不阻塞失败)
    return {"ts": time.time(), "cluster": cl, "nodes": nodes,
            "models": models, "alerts": al, "log": log}


_SNAP_TASK = None


async def _snap_loop():
    """后台持续重建快照——SSE 永不在请求路径上阻塞于重活。"""
    while True:
        try:
            d = await _build_snapshot()
            _SNAP["data"] = d
            _SNAP["ts"] = d["ts"]
        except Exception:
            pass
        await asyncio.sleep(_SNAP_TTL)


async def _snapshot() -> dict:
    global _SNAP_TASK
    if _SNAP_TASK is None or _SNAP_TASK.done():
        _SNAP_TASK = asyncio.create_task(_snap_loop())
    if _SNAP["data"] is None:                  # 首帧: 同步建一次避免空
        try:
            _SNAP["data"] = await _build_snapshot()
            _SNAP["ts"] = _SNAP["data"]["ts"]
        except Exception:
            return {"ts": time.time(), "cluster": {}, "nodes": [],
                    "models": [], "alerts": [], "log": []}
    return _SNAP["data"]


# ── LiteLLM 请求日志/累计：直读 OSS 自带 Postgres LiteLLM_SpendLogs ──
# LiteLLM 落库是核心 OSS 功能(非企业版门控)；只读 SELECT，零碰网关/容器。
# 比 docker-logs 富：真实模型名(model_group)+token+request_duration_ms+status。
_LOG_CACHE = {"ts": 0.0, "data": []}
_ROLL_CACHE = {"ts": 0.0, "data": {"reqTotal": 0, "tokTotal": 0,
                                   "latP50": 0, "latP95": 0}}
_DB_TTL = 6.0


async def _psql(sql: str) -> str:
    """只读查 litellm-postgres（docker exec psql，本机已在 docker 组）。"""
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "exec", "litellm-postgres",
            "psql", "-U", "litellm", "-d", "litellm", "-tAc", sql,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        raw, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
        return raw.decode("utf-8", "replace")
    except Exception:
        return ""


async def _litellm_request_log(limit: int = 40) -> list[dict]:
    now = time.time()
    if now - _LOG_CACHE["ts"] < _DB_TTL and _LOG_CACHE["data"]:
        return _LOG_CACHE["data"][:limit]
    sql = (
        "SELECT concat_ws(E'\\t',"
        # startTime is stored naive UTC in LiteLLM_SpendLogs. Emit ISO-8601
        # with a trailing "Z" so JS `new Date()` parses correctly regardless
        # of the user's browser locale; the frontend then formats it with
        # `toLocaleTimeString()` in the browser's own timezone — no Hearth
        # locale lock-in.
        "to_char(\"startTime\",'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),"
        "COALESCE(NULLIF(model_group,''),model,'?'),"
        "COALESCE(total_tokens,0),"
        "COALESCE(request_duration_ms,0),"
        "COALESCE(status,'success'),"
        "COALESCE(call_type,'completion')) "
        "FROM \"LiteLLM_SpendLogs\" ORDER BY \"startTime\" DESC LIMIT 60;")
    out: list[dict] = []
    for line in (await _psql(sql)).splitlines():
        f = line.split("\t")
        if len(f) != 6:
            continue
        ts, model, tok, lat, st, ct = f
        model = model.split("/")[-1]                 # 去 openai/ 前缀
        status = "200" if st.lower() in ("success", "200", "ok") else "5xx"
        try:
            lat_i = int(float(lat))
        except ValueError:
            lat_i = 0
        out.append({"t": ts, "meth": f"POST /{ct}", "model": model,
                    "status": status, "lat": lat_i if lat_i > 0 else None,
                    "tokens": int(tok) if tok.isdigit() else 0})
    if out:
        _LOG_CACHE["ts"] = now
        _LOG_CACHE["data"] = out
    return out[:limit]


async def _litellm_rollup() -> dict:
    """Hero 累计 + 延迟摘要：LiteLLM_SpendLogs 全后端真实聚合（非企业版）。"""
    now = time.time()
    if now - _ROLL_CACHE["ts"] < _DB_TTL:
        return _ROLL_CACHE["data"]
    sql = ("SELECT concat_ws(E'\\t', count(*), COALESCE(sum(total_tokens),0),"
           "COALESCE(round(percentile_cont(0.5) WITHIN GROUP "
           "(ORDER BY request_duration_ms) FILTER (WHERE request_duration_ms>0)),0),"
           "COALESCE(round(percentile_cont(0.95) WITHIN GROUP "
           "(ORDER BY request_duration_ms) FILTER (WHERE request_duration_ms>0)),0)) "
           "FROM \"LiteLLM_SpendLogs\";")
    r = (await _psql(sql)).strip().split("\t")
    if len(r) == 4 and r[0].isdigit():
        d = {"reqTotal": int(r[0]), "tokTotal": int(float(r[1])),
             "latP50": int(float(r[2])), "latP95": int(float(r[3]))}
        _ROLL_CACHE["ts"] = now
        _ROLL_CACHE["data"] = d
    return _ROLL_CACHE["data"]


@app.get("/api/logs")
async def logs(limit: int = 30):
    return await _litellm_request_log(limit)


# ── Topology ───────────────────────────────────────────────────────
@app.get("/api/topology")
async def topology():
    # Pick the gateway node by role (not by literal id) so any topology works.
    gw = next((x["id"] for x in NODES if "gateway" in (x.get("role", "").lower())),
              NODES[0]["id"] if NODES else "node-1")
    return {
        "nodes": [{"id": n["id"], "ip": n["ip"], "name": n["name"],
                   "class": n["class"]} for n in NODES],
        "links": [{"a": gw, "b": n["id"], "kind": "ns", "speed": "10G"}
                  for n in NODES if n["id"] != gw],
    }


# ── SSE ────────────────────────────────────────────────────────────
@app.get("/api/stream")
async def stream(request: Request):
    async def gen():
        while True:
            if await request.is_disconnected():
                return
            try:
                payload = await _snapshot()       # 缓存快照, 快; 重活每 _SNAP_TTL 一次
                yield f"data: {json.dumps(payload, default=float)}\n\n"
            except Exception as e:
                yield f"event: error\ndata: {json.dumps({'err': str(e)})}\n\n"
            await asyncio.sleep(TICK_SEC)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8080, log_level="info")
