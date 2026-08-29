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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tfevents  # 零依赖 tfevents 标量解析(训练信号 Phase 2)

# ── Config ─────────────────────────────────────────────────────────
PROM_URL    = os.environ.get("PROMETHEUS_URL",    "http://host.docker.internal:9090")
NODEEXP_URL = os.environ.get("NODE_EXPORTER_URL", "http://host.docker.internal:9100")
# Atlas 这块 ASUS ROG 板 hwmon collector(asus WMI/EC + nct6798 走 ACPI 串行读)
# 单次 scrape 稳定 3-5s,EC 偶发争用会更久。直采超时必须远高于此,否则后台
# 双采(_atlas_node_live)任一次超时即把 Atlas 误判为 offline。
NODEEXP_TIMEOUT = float(os.environ.get("NODE_EXPORTER_TIMEOUT", "12.0"))
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
# kind lookup by obs_node label — replaces the legacy `obs_node != "gpu-host"`
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
        r = await client.get(f"{NODEEXP_URL}/metrics", timeout=NODEEXP_TIMEOUT)
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
    if "thermal_zone" in c or "acpitz" in c or "pch" in l or "systin" in l or "board" in l:
        return "平台"
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


def _atlas_fans(scrape: dict) -> list[dict]:
    """从宿主 node-exporter 抽出风扇转速（带人话 label，按转速降序）。"""
    labels_idx = {}
    for x in scrape.get("node_hwmon_sensor_label", []):
        lb = x["labels"]
        labels_idx[(lb.get("chip", ""), lb.get("sensor", ""))] = lb.get("label", "")
    fans = []
    for x in scrape.get("node_hwmon_fan_rpm", []):
        lb = x["labels"]
        chip, sensor = lb.get("chip", ""), lb.get("sensor", "")
        human = labels_idx.get((chip, sensor))
        # 跳过既无 label 又 0 转的幽灵通道（主板上未接的风扇头）
        if human is None and x["value"] <= 0:
            continue
        fans.append({"label": human or sensor, "chip": chip,
                     "rpm": int(round(x["value"]))})
    return sorted(fans, key=lambda f: -f["rpm"])


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
    fans = _atlas_fans(s2)
    cpu_t = next((t["celsius"] for t in temps if t["module"] == "CPU"), 0)
    boot = _sum(s2.get("node_boot_time_seconds", []))
    return {"cpu": round(cpu, 1), "mem": round(mem, 1), "disk": round(disk, 1),
            "netIn": round(max(0, rx), 2), "netOut": round(max(0, tx), 2),
            "tempCpu": cpu_t, "temps": temps, "fans": fans,
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
                "rdmaOut": 0, "temps": [], "fans": []}
        if n.get("node_source") == "direct":
            live.update({k: discrete_gpu.get(k, 0)
                         for k in ("gpu", "vram", "tempGpu", "tempMem", "power")})
            live["vramKind"] = discrete_gpu.get("vramKind", "discrete")
            if direct:
                live.update({k: direct[k] for k in ("cpu", "mem", "disk", "netIn",
                                                    "netOut", "tempCpu", "temps", "fans")
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
        # ── HA-derived (OPTIONAL; null fields when ha-exporter is absent) ─
        # Wall power = real socket-side W from smart plugs; tokens·W⁻¹ joins
        # LiteLLM throughput with that. Rack env = temperature/humidity/AC.
        # Every field is null when the underlying ha_* series is stale or
        # missing — frontend treats null as "—", not 0.
        "power": await _ha_power(),
        "env":   await _ha_env(),
    }


# ── Training observability (Phase 1: 纯 obs 只读底座, 对训练零影响) ──────
# 训练节点 = kind "unified-arm-soc"(GB10). 全部指标来自已在采的 DCGM(L1, ~0% GPU
# 开销)+ node-exporter, 经 obs-prometheus 联邦只读 PromQL —— 不碰 Spark 节点、不改
# DCGM、不开 profiling。覆盖体系 Layer C(GPU 健康)/D(RoCE 互联)/E(静默 stall 检测)。
# Layer A/B(loss/grad-norm/step/ETA/MFU)需训练框架信号源, 见 docs/training-observability.md Phase 2。
_GB10_OBS = [n["obs_node"] for n in NODES
             if n.get("kind") == "unified-arm-soc" and n.get("obs_node")]

# ── Phase 2: 训练框架信号 —— 可插拔多源适配器 ──────────────────────────
# 开源项目 / 用户场景多样 → 不绑定单一框架。三类通用来源归一化到一套规范 schema:
#   1) json     — 训练侧 exporter 写的 metrics JSON(原子替换)。最通用的自定义格式。
#   2) prom     — Prometheus textfile / endpoint(node_exporter textfile,任何 prom 兼容框架)。
#   3) tfevents — TensorBoard event 文件(PyTorch/HF/Lightning/Keras 通用)。零依赖解析。
# 全部经免密 SSH 只读 cat / 本地读,对训练零影响。`auto` 按 json→prom→tfevents 顺序探测。
# 字段映射在 _norm_snapshot/_signal_from_tfevents 内做(别名容错),新框架只需补别名或加一个 reader。
# 规范 schema 见 docs/training-observability.md「Phase 2 多源」。
TRAIN_SOURCE = os.environ.get("TRAIN_SOURCE", "auto")          # auto|json|prom|tfevents|off
TRAIN_HOST   = os.environ.get("TRAIN_METRICS_HOST", "user@10.0.0.22")  # ""/"local"=本地
TRAIN_JSON   = os.environ.get("TRAIN_METRICS_JSON", "/home/user/m3-spec-out/train_metrics.json")
TRAIN_PROM   = os.environ.get("TRAIN_METRICS_PROM", "/home/user/m3-spec-out/train_metrics.prom")
TRAIN_TFEVENTS_GLOB = os.environ.get("TRAIN_TFEVENTS_GLOB",
                                     "/home/user/m3-train-out/runs/*.tfevents.*")
# prom 指标前缀(剥离后映射到规范字段);多框架前缀都列上, 命中即剥
TRAIN_PROM_PREFIXES = tuple(p for p in os.environ.get(
    "TRAIN_PROM_PREFIXES", "speculators_train_,train_,train/").split(",") if p)
TRAIN_TOTAL_STEPS = int(os.environ.get("TRAIN_TOTAL_STEPS", "0")) or None  # 0=从 progress 自动推
_TRAIN_SIG = {"ts": 0.0, "data": {"present": False}}
_TRAIN_SIG_TTL = 12.0      # exporter ~10s 刷新; 同步节拍
_TRAIN_HIST = {}           # 快照源(json/prom 只给当前值)→ 跨轮累积历史驱动曲线
_TRAIN_TOTAL_CACHE = None  # 一旦某样本带 progress_pct → 反推 total 锁定, 供后续无 progress 样本复用


async def _read_bytes(spec: str, is_glob: bool = False) -> bytes:
    """读训练侧文件(免密 SSH cat 或本地)。失败/不存在 → b''。二进制安全(tfevents)。"""
    host = TRAIN_HOST
    if host in ("", "local"):
        import glob as _glob
        path = spec
        if is_glob:
            fs = sorted(_glob.glob(spec), key=os.path.getmtime)
            path = fs[-1] if fs else None
        if not path or not os.path.exists(path):
            return b""
        try:
            return Path(path).read_bytes()
        except Exception:
            return b""
    cmd = (f'f=$(ls -t {spec} 2>/dev/null | head -1); [ -n "$f" ] && cat "$f"'
           if is_glob else f'cat {spec} 2>/dev/null')
    try:
        proc = await asyncio.create_subprocess_exec(
            "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=4", host, cmd,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=8.0)
        return out or b""
    except Exception:
        return b""


def _accum(key: str, step, val, cap: int = 120):
    """快照源历史累积:按 step 去重追加;step 回退(新 run)→ 重置该 key。"""
    if val is None or step is None:
        return
    buf = _TRAIN_HIST.setdefault(key, [])
    if buf and step < buf[0][0]:
        buf.clear()
    if buf and buf[-1][0] == step:
        buf[-1] = (step, val)
    elif not buf or step > buf[-1][0]:
        buf.append((step, val))
    if len(buf) > cap:
        del buf[:len(buf) - cap]


def _hist_series(key: str, cap: int = 80):
    vals = [v for _s, v in _TRAIN_HIST.get(key, [])]
    if len(vals) > cap:
        idx = [round(i * (len(vals) - 1) / (cap - 1)) for i in range(cap)]
        vals = [vals[i] for i in idx]
    return [round(v, 4) for v in vals]


def _g(flat: dict, *names):
    """别名容错取值:返回首个存在且非 None 的字段。"""
    for n in names:
        v = flat.get(n)
        if v is not None:
            return v
    return None


def _norm_snapshot(flat: dict, source: str, now: float):
    """json / prom 快照(当前值)→ 规范 schema。字段名做跨框架别名映射。"""
    step = _g(flat, "global_step", "step", "iteration")
    if step is None:
        return None
    step = int(step)
    loss = _g(flat, "loss", "train_loss", "total_loss")
    acc = _g(flat, "cond_acc_0", "acceptance_rate_0", "acceptance", "accept_rate")
    mean_accept = _g(flat, "mean_accept_est", "mean_accept", "accept_length", "avg_accept_len")
    lr = _g(flat, "lr", "learning_rate")
    gn = _g(flat, "grad_norm", "gradnorm", "grad_norm_clip")
    epoch = _g(flat, "epoch")
    sps = _g(flat, "steps_per_sec", "it_per_sec", "iter_per_sec")
    eta = _g(flat, "eta_seconds", "eta", "eta_sec")
    prog = _g(flat, "progress_pct", "progress")
    ts = _g(flat, "ts", "timestamp", "time")
    global _TRAIN_TOTAL_CACHE
    total = TRAIN_TOTAL_STEPS or _TRAIN_TOTAL_CACHE
    if prog and prog > 0:                       # 带 progress 的样本 → 反推并锁定 total
        total = round(step / (prog / 100.0))
        _TRAIN_TOTAL_CACHE = total
    progress = (prog / 100.0) if prog is not None else (step / total if total else None)
    # per-step:优先用源给的 steps_per_sec;否则用累积 (step,ts) 自派生(exporter 常省略)
    per_step = (1.0 / sps) if sps else None
    if per_step is None and ts:
        _accum("_ts", step, ts)
        b = _TRAIN_HIST.get("_ts", [])
        if len(b) >= 2 and b[-1][0] > b[-2][0] and b[-1][1] > b[-2][1]:
            per_step = (b[-1][1] - b[-2][1]) / (b[-1][0] - b[-2][0])
    if eta is None and per_step and total and step < total:
        eta = (total - step) * per_step
    _accum("loss", step, loss)
    _accum("acc", step, acc)
    _accum("meanAccept", step, mean_accept)
    parts = {k.split("_", 1)[1]: round(float(v), 4)
             for k, v in flat.items() if k.startswith("loss_")}
    return {
        "present": True, "source": source,
        "step": step, "totalSteps": int(total) if total else None,
        "progress": round(progress, 4) if progress is not None else None,
        "epoch": int(epoch) if epoch is not None else None,
        "loss": round(loss, 4) if loss is not None else None,
        "lossSeries": _hist_series("loss"),
        "lossParts": parts or None,
        "gradNorm": round(gn, 3) if gn is not None else None,
        "lr": lr,
        "acceptance": round(acc, 4) if acc is not None else None,
        "accSeries": _hist_series("acc"),
        "meanAccept": round(mean_accept, 3) if mean_accept is not None else None,
        "meanAcceptSeries": _hist_series("meanAccept"),
        "perStepSec": round(per_step, 2) if per_step else None,
        "etaSec": int(eta) if eta else None,
        "staleSec": int(now - ts) if ts else None,
        "mfu": _g(flat, "mfu"),
    }


def _parse_prom_text(text: str) -> dict:
    """Prometheus 文本 → {规范字段: float}(剥离 TRAIN_PROM_PREFIXES 前缀)。"""
    flat = {}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        name = parts[0].split("{")[0]
        for p in TRAIN_PROM_PREFIXES:
            if name.startswith(p):
                name = name[len(p):]; break
        try:
            flat[name] = float(parts[1])
        except ValueError:
            pass
    return flat


def _mean_latest(series: dict, prefix: str):
    vals, step = [], None
    for tag, pts in series.items():
        if tag.startswith(prefix) and pts:
            vals.append(pts[-1][2]); step = max(step or 0, pts[-1][0])
    return (sum(vals) / len(vals) if vals else None), step


def _mean_curve(series: dict, prefix: str, cap: int = 80):
    by_step = {}
    for tag, pts in series.items():
        if not tag.startswith(prefix):
            continue
        for step, _w, val in pts:
            by_step.setdefault(step, []).append(val)
    if not by_step:
        return []
    steps = sorted(by_step)
    curve = [sum(by_step[s]) / len(by_step[s]) for s in steps]
    if len(curve) > cap:
        idx = [round(i * (len(curve) - 1) / (cap - 1)) for i in range(cap)]
        curve = [curve[i] for i in idx]
    return [round(v, 4) for v in curve]


def _signal_from_tfevents(raw: bytes, now: float):
    """TensorBoard tfevents → 规范 schema(有完整历史)。EAGLE 接受率 tag = acceptance_rate_*。"""
    try:
        s = tfevents.series(raw)
    except Exception:
        return None
    if not s:
        return None
    loss, _ = _mean_latest(s, "train/ploss")
    if loss is None:
        loss, _ = _mean_latest(s, "train/loss")
    acc, _ = _mean_latest(s, "train/acceptance_rate")
    gn = (s.get("train/grad_norm") or [(0, 0, None)])[-1][2]
    lr = (s.get("train/lr") or [(0, 0, None)])[-1][2]
    step = max([p[-1][0] for p in s.values() if p] or [0])
    ref = s.get("train/grad_norm") or s.get("train/ploss_0") or next(iter(s.values()), [])
    per_step = None
    if len(ref) >= 2 and ref[-1][0] > ref[-2][0]:
        dw, ds = ref[-1][1] - ref[-2][1], ref[-1][0] - ref[-2][0]
        if ds > 0 and dw > 0:
            per_step = dw / ds
    total = TRAIN_TOTAL_STEPS
    eta = ((total - step) * per_step) if (per_step and total and step < total) else None
    last_wall = max([p[-1][1] for p in s.values() if p] or [0])
    loss_curve = _mean_curve(s, "train/ploss") or _mean_curve(s, "train/loss")
    return {
        "present": True, "source": "tfevents",
        "step": int(step), "totalSteps": int(total) if total else None,
        "progress": round(step / total, 4) if total else None,
        "epoch": None,
        "loss": round(loss, 4) if loss is not None else None,
        "lossSeries": loss_curve, "lossParts": None,
        "gradNorm": round(gn, 3) if gn is not None else None, "lr": lr,
        "acceptance": round(acc, 4) if acc is not None else None,
        "accSeries": _mean_curve(s, "train/acceptance_rate"),
        "meanAccept": None, "meanAcceptSeries": [],
        "perStepSec": round(per_step, 2) if per_step else None,
        "etaSec": int(eta) if eta else None,
        "staleSec": int(now - last_wall) if last_wall else None,
        "mfu": None,
    }


async def _reader_json(now):
    raw = await _read_bytes(TRAIN_JSON)
    if not raw:
        return None
    try:
        d = json.loads(raw.decode("utf-8", "replace"))
    except Exception:
        return None
    return _norm_snapshot(d, "json", now) if isinstance(d, dict) else None


async def _reader_prom(now):
    raw = await _read_bytes(TRAIN_PROM)
    if not raw:
        return None
    flat = _parse_prom_text(raw.decode("utf-8", "replace"))
    return _norm_snapshot(flat, "prom", now) if flat else None


async def _reader_tfevents(now):
    raw = await _read_bytes(TRAIN_TFEVENTS_GLOB, is_glob=True)
    if not raw:
        return None
    return _signal_from_tfevents(raw, now)


_TRAIN_READERS = [("json", _reader_json), ("prom", _reader_prom),
                  ("tfevents", _reader_tfevents)]


async def _train_signal() -> dict:
    """多源探测训练信号。auto = 按序首个产出数据者胜;否则用指定源。缺失 → present:False。"""
    now = time.time()
    if now - _TRAIN_SIG["ts"] < _TRAIN_SIG_TTL:
        return _TRAIN_SIG["data"]
    _TRAIN_SIG["ts"] = now
    if TRAIN_SOURCE == "off":
        _TRAIN_SIG["data"] = {"present": False}
        return _TRAIN_SIG["data"]
    order = (_TRAIN_READERS if TRAIN_SOURCE == "auto"
             else [(n, f) for n, f in _TRAIN_READERS if n == TRAIN_SOURCE])
    for _name, fn in order:
        try:
            sig = await fn(now)
        except Exception:
            sig = None
        if sig and sig.get("present"):
            _TRAIN_SIG["data"] = sig
            return sig
    _TRAIN_SIG["data"] = {"present": False}
    return _TRAIN_SIG["data"]


async def _training_payload() -> dict:
    if not _GB10_OBS:
        return {"nodes": [], "summary": {}}
    # obs_node 标签均为安全字符(spark-a 等);不用 re.escape——它会把 `-` 转成 `\-`,
    # 而 Prometheus RE2 拒绝该转义 → 查询报错变空值(踩过)。
    sel = "|".join(_GB10_OBS)
    nf = f'{{node=~"{sel}"}}'
    (util, power, gtemp, mtemp, fb_used, fb_free, xid, ecc_sbe, ecc_dbe,
     mem_pct, roce_rx, roce_tx, ib_down) = await asyncio.gather(
        promql(f"DCGM_FI_DEV_GPU_UTIL{nf}"),
        promql(f"DCGM_FI_DEV_POWER_USAGE{nf}"),
        promql(f"DCGM_FI_DEV_GPU_TEMP{nf}"),
        promql(f"DCGM_FI_DEV_MEMORY_TEMP{nf}"),
        promql(f"DCGM_FI_DEV_FB_USED{nf}"),
        promql(f"DCGM_FI_DEV_FB_FREE{nf}"),
        promql(f"DCGM_FI_DEV_XID_ERRORS{nf}"),
        promql(f"DCGM_FI_DEV_ECC_SBE_VOL_TOTAL{nf}"),
        promql(f"DCGM_FI_DEV_ECC_DBE_VOL_TOTAL{nf}"),
        # GB10 统一内存占用%(OOM 关键 —— 训练贴内存上限跑, 实测 OOM 会硬重启节点)
        promql(f'(1 - node_memory_MemAvailable_bytes{nf} / node_memory_MemTotal_bytes{nf}) * 100'),
        # 东西向 CX-7 RoCE 真实吞吐(IB port_data 计数, ×4 lanes→bytes 约定 → MB/s)
        promql(f"sum by (node) (rate(node_infiniband_port_data_received_bytes_total{nf}[1m])) * 4 / 1048576"),
        promql(f"sum by (node) (rate(node_infiniband_port_data_transmitted_bytes_total{nf}[1m])) * 4 / 1048576"),
        promql(f"sum by (node) (node_infiniband_link_downed_total{nf})"),
    )
    U, P, GT, MT, FU, FF, XID, ES, ED, MEM, RX, TX, IBD = (
        _by(x, "node") for x in (util, power, gtemp, mtemp, fb_used, fb_free,
                                 xid, ecc_sbe, ecc_dbe, mem_pct, roce_rx, roce_tx, ib_down))
    nodes = []
    for o in _GB10_OBS:
        fbu, fbf = FU.get(o, 0), FF.get(o, 0)
        fbt = fbu + fbf
        nodes.append({
            "id": OBS_TO_ID.get(o, o), "obs": o,
            "util": round(U.get(o, 0), 1), "power": round(P.get(o, 0), 1),
            "tempGpu": round(GT.get(o, 0), 1), "tempMem": round(MT.get(o, 0), 1),
            "memPct": round(MEM.get(o, 0), 1),                       # 统一内存占用%(OOM)
            "fbUsedPct": round(fbu / fbt * 100, 1) if fbt else 0,    # GPU framebuffer 占用%
            "xid": int(XID.get(o, 0)), "eccSbe": int(ES.get(o, 0)), "eccDbe": int(ED.get(o, 0)),
            "roceRxMBps": round(RX.get(o, 0), 1), "roceTxMBps": round(TX.get(o, 0), 1),
            "ibLinkDowned": int(IBD.get(o, 0)),
        })
    if not nodes:
        return {"ts": time.time(), "nodes": [], "summary": {}}
    utils = [n["util"] for n in nodes]
    powers = [n["power"] for n in nodes]
    med_p = sorted(powers)[len(powers) // 2]
    # 静默 stall 启发(NCCL 卡死特征: GPU util 满载但功率塌、RoCE 归零, 仪表盘"全绿"却零进度):
    # 高 util + 东西向带宽近零 + 功率显著低于活跃中位 → 嫌疑(保守阈值, 仅提示非硬告警;
    # 精确判活性须 Layer A 步进, 见 Phase 2)。
    for n in nodes:
        n["stallSuspect"] = bool(
            n["util"] >= 90 and (n["roceRxMBps"] + n["roceTxMBps"]) < 2
            and med_p > 0 and n["power"] < med_p * 0.6)
    summary = {
        "nodes": len(nodes),
        "active": sum(1 for u in utils if u >= 50),       # util≥50 视为参与训练
        "utilAvg": round(sum(utils) / len(utils), 1),
        "utilSkew": round(max(utils) - min(utils), 1),    # straggler 粗信号(底座近似)
        "powerTotal": round(sum(powers), 1),
        "roceRxMBps": round(sum(n["roceRxMBps"] for n in nodes), 1),
        "roceTxMBps": round(sum(n["roceTxMBps"] for n in nodes), 1),
        "anyXid": any(n["xid"] for n in nodes),
        "anyEccDbe": any(n["eccDbe"] for n in nodes),
        "stallSuspect": any(n["stallSuspect"] for n in nodes),
    }
    signal = await _train_signal()                 # Phase 2 训练框架信号(缺则 present=False)
    return {"ts": time.time(), "nodes": nodes, "summary": summary, "signal": signal}


@app.get("/api/training")
async def training():
    return await _training_payload()


# ── 基础设施设备(交换机 / NAS)温度与健康 ─────────────────────────────
# 数据链: 设备 SNMP → obs-snmp-exporter → obs-prometheus → 这里只读聚合。
# 设备清单来自 config 的 `infra:`;整块缺失 → 返回空列表, 前端 section 不渲染。
INFRA = HEARTH_CFG.get("infra") or []

# mtxrHealthType → (字段名, 缩放)。1/2/6 是 MikroTik 文档语义;
# 3(0.1V) 与 5(0.1W) 的缩放是实测标定的 —— CLI `/system/health` 显示 26 W / SNMP 262,
# 市电 2270 → 227.0 V。设备换代若单位变了, 这两行是唯一要改的地方。
_MTXR_UNIT = {1: ("celsius", 1.0), 2: ("rpm", 1.0),
              3: ("volts", 0.1), 5: ("watts", 0.1), 6: ("state", 1.0)}

# 传感器名 → 展示分组。RouterOS 的健康表会随型号增减条目,
# 认不出的温度项一律落到 "chip", 不丢数据(诚实降级优于静默丢弃)。
_MTXR_GROUP = {"cpu": "chip", "switch": "chip", "sfp": "chip", "phy": "chip",
               "board": "board"}


def _mtxr_group(sensor: str) -> str:
    head = sensor.split("-", 1)[0]
    return _MTXR_GROUP.get(head, "chip")


def _psu_slot(sensor: str) -> str | None:
    """psu1-temperature → 'PSU1';非 psu 项返回 None。"""
    if not sensor.startswith("psu"):
        return None
    return "PSU" + sensor[3:4]


async def _infra_mikrotik(dev: str) -> dict:
    vals, types = await asyncio.gather(
        promql(f'mtxrHealthValue{{device="{dev}"}}'),
        promql(f'mtxrHealthType{{device="{dev}"}}'),
    )
    tmap = {t["metric"].get("sensor"): int(t["value"]) for t in types}
    temps, fans, states = [], [], []
    psus: dict[str, dict] = {}
    for v in vals:
        sensor = v["metric"].get("sensor")
        if not sensor:
            continue
        field, scale = _MTXR_UNIT.get(tmap.get(sensor, 0), (None, 1.0))
        val = v["value"] * scale
        slot = _psu_slot(sensor)
        if slot:
            p = psus.setdefault(slot, {"label": slot})
            # psuN-state: 0 = ok (与群晖的 1=ok 相反, 别统一化时搞反)
            if field == "state":
                p["ok"] = val == 0
            elif field:
                p[field] = round(val, 1)
            continue
        if field == "celsius":
            temps.append({"label": sensor.replace("-temperature", ""),
                          "celsius": round(val), "group": _mtxr_group(sensor)})
        elif field == "rpm":
            fans.append({"label": sensor.replace("-speed", ""), "rpm": round(val)})
        elif field == "state":
            states.append({"label": sensor.replace("-state", ""), "ok": val == 0})
    fans.sort(key=lambda f: f["label"])
    return {"temps": temps, "fans": fans, "states": states,
            "psus": [psus[k] for k in sorted(psus)], "disks": []}


async def _infra_synology(dev: str) -> dict:
    sysT, diskT, diskS, stat = await asyncio.gather(
        promql(f'synoSystemTemperature{{device="{dev}"}}'),
        promql(f'synoDiskTemperature{{device="{dev}"}}'),
        promql(f'synoDiskStatus{{device="{dev}"}}'),
        promql('{__name__=~"synoSystemStatus|synoPowerStatus|synoSystemFanStatus'
               f'|synoCpuFanStatus",device="{dev}"}}'),
    )
    # 盘位 ≠ 盘符: SNMP 的 diskIndex 0 对应的是 "Disk 3"。一律按 disk 名对齐排序。
    okByDisk = {d["metric"].get("disk"): d["value"] == 1 for d in diskS}
    disks = sorted(
        ({"name": d["metric"].get("disk", "?"), "celsius": round(d["value"]),
          "ok": okByDisk.get(d["metric"].get("disk"), True)} for d in diskT),
        key=lambda d: d["name"])
    temps = [{"label": "system", "celsius": round(s["value"]), "group": "system"}
             for s in sysT]
    states = [{"label": s["metric"]["__name__"]
               .replace("syno", "").replace("Status", ""),
               "ok": s["value"] == 1} for s in stat]
    states.sort(key=lambda s: s["label"])
    return {"temps": temps, "fans": [], "states": states, "psus": [], "disks": disks}


async def _infra_openwrt(dev: str) -> dict:
    """OpenWrt 路由器 — 走 node-exporter 格式而非 SNMP(设备上没有 snmpd)。
    温度由自建 lua collector 提供, 指标名与官方 hwmon collector 一致。"""
    temps, load = await asyncio.gather(
        promql(f'node_hwmon_temp_celsius{{device="{dev}"}}'),
        promql(f'node_load1{{device="{dev}"}}'),
    )
    out = {"temps": [], "fans": [], "states": [], "psus": [], "disks": []}
    for t in temps:
        out["temps"].append({"label": t["metric"].get("sensor", "temp"),
                             "celsius": round(t["value"]), "group": "chip"})
    # 路由器没有风扇/电源/硬盘可读, 用 load1 占一个状态位当"还活着且不过载"的信号
    if load:
        out["states"].append({"label": f"load {load[0]['value']:.2f}",
                              "ok": load[0]["value"] < 4})
    return out


async def _infra_payload() -> list[dict]:
    if not INFRA:
        return []
    ups, uptimes, boots, nows = await asyncio.gather(
        promql('up{job=~"snmp_.*|openwrt"}'),
        promql('hrSystemUptime{job=~"snmp_.*"}'),          # SNMP 设备: 百分之一秒
        promql('node_boot_time_seconds{job="openwrt"}'),   # node-exporter 设备: 开机时刻
        promql('node_time_seconds{job="openwrt"}'),
    )
    upByDev = {u["metric"].get("device"): u["value"] == 1 for u in ups}
    utByDev = {u["metric"].get("device"): u["value"] / 100 for u in uptimes}
    nowByDev = {n["metric"].get("device"): n["value"] for n in nows}
    for b in boots:                                        # 设备侧时钟自洽, 不用服务端时间
        d = b["metric"].get("device")
        if d in nowByDev:
            utByDev[d] = nowByDev[d] - b["value"]

    async def one(y: dict) -> dict:
        dev = y.get("prom_device") or y["id"]
        src = y.get("source", "")
        up = upByDev.get(dev, False)
        blank = {"temps": [], "fans": [], "states": [], "psus": [], "disks": []}
        if not up:
            body = blank                      # 抓不到就诚实留空, 不显示上一次的陈值
        elif src == "mikrotik":
            body = await _infra_mikrotik(dev)
        elif src == "synology":
            body = await _infra_synology(dev)
        elif src == "openwrt":
            body = await _infra_openwrt(dev)
        else:
            body = blank
        # hottest 给卡片头部一个可扫读的单一数字 —— 盘温也参与, 它才是 NAS 的风险点
        pool = ([(t["label"], t["celsius"]) for t in body["temps"]]
                + [(d["name"], d["celsius"]) for d in body["disks"]]
                + [(p["label"], p["celsius"]) for p in body["psus"] if "celsius" in p])
        hottest = max(pool, key=lambda x: x[1]) if pool else None
        return {"id": y["id"], "name": y.get("name", y["id"]), "ip": y.get("ip", ""),
                "class": y.get("class", ""), "role": y.get("role_label", ""),
                "source": src, "critical": bool(y.get("critical")),
                "up": up,
                "uptimeSec": round(utByDev[dev]) if dev in utByDev else None,
                "hottest": {"label": hottest[0], "celsius": hottest[1]} if hottest else None,
                **body}

    return list(await asyncio.gather(*(one(y) for y in INFRA)))


@app.get("/api/infra")
async def infra():
    return {"ts": time.time(), "devices": await _infra_payload()}


# ── HA-derived cluster fields (gracefully null when HA exporter absent) ─
async def _ha_power() -> dict:
    # Direct aggregation in PromQL — works without recording rules so the
    # obs Prometheus needs no extra config beyond the scrape job.
    # `byNode` is the per-device breakdown the Telemetry table needs; the
    # cluster Σ is then derived from byNode in the frontend so the rollup
    # and the table are guaranteed consistent (no drift between PromQL
    # rounds).
    # Per-node W is the live snapshot from Prometheus. The two energy windows
    # are Hearth-side rolling integrals over the trusted W series — the cuco
    # entity's claimed kWh field doesn't actually accumulate (verified empty
    # 24h history), so we ignore it. avg_over_time(W) × hours / 1000 = kWh.
    # 24h window = "last day"; 30d window = "last 30 days" — sliding, not
    # calendar-aligned, but immune to HA-side counter resets / TZ confusion.
    gpu, eff, per_node_w, per_node_24h, per_node_30d = await asyncio.gather(
        promql("sum(DCGM_FI_DEV_POWER_USAGE)"),
        promql("sum(rate(litellm_total_tokens_metric[1m])) "
               "/ clamp_min(sum(ha_node_wall_power_watts), 1)"),
        promql("ha_node_wall_power_watts"),
        # sum_over_time × step / 3600 / 1000 — integrates only the seconds
        # that actually have samples. avg_over_time × window-length would
        # extrapolate a 1-hour average to a 30-day total whenever the series
        # is newly added; this form honestly reports "kWh accumulated since
        # we started polling" and converges to the true window total over
        # time. step = 15s (the obs scrape interval).
        promql("sum_over_time(ha_node_wall_power_watts[24h]) * 15 / 3600 / 1000"),
        promql("sum_over_time(ha_node_wall_power_watts[30d]) * 15 / 3600 / 1000"),
    )
    by_node    = {k: round(float(v), 1) for k, v in _by(per_node_w,   "node").items()}
    by_node_d  = {k: round(float(v), 2) for k, v in _by(per_node_24h, "node").items()}
    by_node_m  = {k: round(float(v), 2) for k, v in _by(per_node_30d, "node").items()}
    wall_total = round(sum(by_node.values()), 1)   if by_node   else None
    kwh_d_tot  = round(sum(by_node_d.values()), 2) if by_node_d else None
    kwh_m_tot  = round(sum(by_node_m.values()), 2) if by_node_m else None
    # Empty PromQL result = metric absent (e.g. exporter not configured for
    # this entity). _one() defaults to 0.0 which would lie ("0 W of GPU"),
    # so guard explicitly and emit null.
    def _f(r, digits=1):
        return None if not r else round(r[0]["value"], digits)
    return {"wallW": wall_total, "gpuW": _f(gpu), "tokensPerW": _f(eff, 2),
            "kwh24h": kwh_d_tot, "kwh30d": kwh_m_tot,
            "byNode": by_node, "byNode24h": by_node_d, "byNode30d": by_node_m}


async def _ha_env() -> dict:
    # Same Hearth-side rolling-window kWh for the rack AC — direct from the
    # ha_rack_ac_power_watts series rather than the unreliable cuco kWh field.
    # cabinetHeatProxyC: each spark cuco plug has an internal temp sensor.
    # At ~70 W load self-heating is ~5°C, so the mean across all spark plugs
    # is a usable PROXY for cabinet ambient — beats no signal at all, and
    # uncalibrated absolute (±5°C) but trends are trustworthy. Replaces the
    # bedroom sensor we accidentally pointed at earlier.
    t, h, ac_w, ac_24h, ac_30d, ac_s, plug_temps = await asyncio.gather(
        promql("ha_rack_temperature_celsius"),
        promql("ha_rack_humidity_percent"),
        promql("ha_rack_ac_power_watts"),
        promql("sum_over_time(ha_rack_ac_power_watts[24h]) * 15 / 3600 / 1000"),
        promql("sum_over_time(ha_rack_ac_power_watts[30d]) * 15 / 3600 / 1000"),
        promql("ha_rack_ac_state"),
        promql("ha_node_plug_temp_celsius"),
    )
    by_node_plug = {k: round(float(v), 1)
                    for k, v in _by(plug_temps, "node").items()}
    proxy = round(sum(by_node_plug.values()) / len(by_node_plug), 1) \
        if by_node_plug else None
    # Empty PromQL result = no such metric (entity not configured). Emit null
    # rather than the 0.0 default, so the frontend can render "—" instead of
    # claiming the rack is at 0°C / off when the sensor simply doesn't exist.
    def _f(r, digits=1):
        return None if not r else round(r[0]["value"], digits)
    return {"rackTempC": _f(t), "rackRH": _f(h, 0),
            "acW": _f(ac_w), "acKwh24h": _f(ac_24h, 2), "acKwh30d": _f(ac_30d, 2),
            "acOn": None if not ac_s else bool(ac_s[0]["value"]),
            "byNodePlugTempC": by_node_plug, "cabinetHeatProxyC": proxy}


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
    # 下面三条与 deepseek-v4-flash 是【同一个引擎实例】(head 节点, TP=4 会漂移,
    # 2026-08-06 在 .188:8000) 的四个入口,
    # 差别只在网关 hook 注入的 thinking/effort 档位, 不是四个部署。
    # 不登记的话 _meta_for 会 .title() 推导成 "Deepseek V4 Flash Think Low",
    # 面板上看起来像"当前部署的是 Think Low 档" —— 2026-08-04 已造成一次误判。
    "deepseek-v4-flash-think-low": {"display": "DeepSeek-V4-Flash \u00b7 Think 低档(同一实例)",
        "vendor": "DeepSeek", "kind": "chat", "tags": ["reasoning", "think"]},
    "deepseek-v4-flash-think": {"display": "DeepSeek-V4-Flash \u00b7 Think 标准档(同一实例)",
        "vendor": "DeepSeek", "kind": "chat", "tags": ["reasoning", "think"]},
    "deepseek-v4-flash-think-max": {"display": "DeepSeek-V4-Flash \u00b7 Think 极限档(同一实例)",
        "vendor": "DeepSeek", "kind": "chat", "tags": ["reasoning", "think"]},
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


async def _served_name(base: str) -> str:
    """后端(vLLM/OpenAI 兼容)自报的 served-model-name = /v1/models .data[0].id。
    这是该 endpoint 真正加载的权重身份,优先于网关上可能过时的路由别名——
    运维换载模型时常只新增网关路由、忘删旧路由,导致一个 base 挂多名,
    若按字母序挑主名会显错(实测 .156:8000 已换 minimax-m3,旧 deepseek-v4-flash
    路由仍在 → 字母序误选 deepseek)。"""
    try:
        r = await client.get(f"{base}/v1/models", timeout=3.0)
        r.raise_for_status()
        return str(r.json()["data"][0].get("id") or "")
    except Exception:
        return ""


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
    # 后端自报的 served-model-name 是"实际加载了什么"的真相,优先于网关别名
    served = {b: await _served_name(b) for b in base_routes}

    def _primary(routes: set, sv: str) -> tuple[str, bool, list]:
        # 返回 (主名, 身份是否已核实, 歧义候选)。
        # 后端实际加载的模型名优先(抗网关路由漂移)。后端 served-name 常带量化
        # 后缀(minimax-m3-awq)而网关路由不带(minimax-m3),故精确匹配之外再做
        # 边界前缀容错:取与 served-name 在 `-` 段边界上互为前缀、且最长的路由。
        non_alias = sorted(r for r in routes if r not in _ALIAS_ROUTES)
        if sv:
            nsv = sv.lower().replace("_", "-")
            # 精确命中优先:served-name 等于某条路由时,那条就是后端身份本身。
            # 下面的前缀容错会"取最长",而更长的路由可能只是同实例的档位别名
            # (deepseek-v4-flash-think-low),不是更精确的身份 —— 不加这一步就会
            # 被别名挤掉主名(2026-08-06 实测显成 "Think 低档(同一实例)")。
            for r in routes:
                if r.lower().replace("_", "-") == nsv:
                    return r, True, []
            best = None
            for r in routes:
                nr = r.lower().replace("_", "-")
                if nsv == nr or nsv.startswith(nr + "-") or nr.startswith(nsv + "-"):
                    if best is None or len(nr) > len(best.lower()):
                        best = r
            if best:
                return best, True, []
        # 后端不可达(模型没拉起 / 刚重启) → 拿不到 served-name。此时若该 endpoint
        # 只挂了一条非别名路由,名字无歧义;挂了多条(运维换载只加路由不删旧的)就
        # 只能按字母序猜,历史上因此把 minimax-m3 显成 DeepSeek。猜可以,但必须
        # 标记 unverified 让 UI 说清楚,不能拿废弃别名冒充当前部署。
        fallback = non_alias[0] if non_alias else sorted(routes)[0]
        return fallback, len(non_alias) <= 1, (non_alias if len(non_alias) > 1 else [])

    models: dict[str, dict] = {}
    for b, routes in base_routes.items():
        prt, verified, cands = _primary(routes, served.get(b, ""))
        meta = _meta_for(prt)
        mm = models.setdefault(prt, {
            "id": prt, "route": f"litellm/{prt}", "display": meta["display"],
            "vendor": meta["vendor"], "kind": meta["kind"],
            "tags": list(meta["tags"]), "params": "—", "quant": "—",
            "framework": "—", "vram": 0, "ctx": 0,
            "identityUnverified": False, "identityCandidates": [],
            "_nodes": set(), "_aliases": set(), "_bases": [],
            "up": False, "vllm_bases": [], "llamacpp_bases": [], "sglang_bases": []})
        if not verified:
            mm["identityUnverified"] = True
            mm["identityCandidates"] = sorted(set(mm["identityCandidates"]) | set(cands))
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
    # 引擎迭代计数。count = 引擎前向次数(每次前向把激活权重完整读一遍 → MBU 的
    # 分子基础); sum = 这些前向里实际处理的 token 位置总数(含 prefill 与投机解码
    # 的 verify 位置 → MFU 的分子基础)。
    # ⛔ 不要拿 spec_decode_num_drafts_total 当步频:drafts 是【每请求】计数,
    #    连续批处理下一次前向会产出 B 个 draft(本机实测 51564/28300 = 1.82),
    #    用它算 MBU 会按批量倍数高估。
    "vllm:iteration_tokens_total_count", "vllm:iteration_tokens_total_sum",
    # 请求耗时分解 + ITL。sum/count 求均值,作为分位数的对照线:样本量小时
    # 分位数会偏高(几十个样本的 p99 基本就是最大值),两者背离大时以均值为准。
    "vllm:request_prefill_time_seconds_sum", "vllm:request_prefill_time_seconds_count",
    "vllm:request_decode_time_seconds_sum", "vllm:request_decode_time_seconds_count",
    "vllm:request_queue_time_seconds_sum", "vllm:request_queue_time_seconds_count",
    "vllm:inter_token_latency_seconds_sum", "vllm:inter_token_latency_seconds_count",
    # 投机解码(speculative decoding)：只取 _total 计数器。同名的 _created 是
    # "该序列首次出现的 unix 时间戳"gauge(1.78e9),混进求和会把接受率炸成
    # 天文数字 —— 精确名匹配天然把它挡在外面,不要改成前缀匹配。
    "vllm:spec_decode_num_drafts_total",
    "vllm:spec_decode_num_draft_tokens_total",
    "vllm:spec_decode_num_accepted_tokens_total",
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


def _hfrac(buckets: dict, x: float) -> float | None:
    """累积桶 → 样本中 <= x 秒的比例(0-1)。_hquant 的反函数,桶内线性插值。

    用于 SLO 达标率:阈值恰好落在桶边界时是精确值(如 TPOT 100ms = le 0.1),
    落在桶中间则是插值近似(如 TTFT 2000ms 落在 [1.0, 2.5] 之间)。
    无样本返回 None —— 不是 0%,也不是 100%,是"没测到"。"""
    if not buckets:
        return None
    pts = sorted((float("inf") if le in ("+Inf", "Inf") else float(le), c)
                 for le, c in buckets.items())
    total = pts[-1][1]
    if total <= 0:
        return None
    prev_le, prev_c = 0.0, 0.0
    for le_f, c in pts:
        if x <= le_f:
            if le_f == float("inf") or le_f == prev_le:
                return prev_c / total
            return (prev_c + (c - prev_c) * ((x - prev_le) / (le_f - prev_le))) / total
        prev_le, prev_c = le_f, c
    return 1.0


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


# name → 累积桶累加到 out 的哪个 __key 下。业界标准口径(vllm bench serve /
# GenAI-Perf / LLMPerf)一律报 p50/p90/p99 —— 交互式场景的验收线是尾延迟,
# 均值会把长尾抹平, 故 TTFT/TPOT 与 e2e 一样走直方图桶。
_VLLM_HISTS = {
    "vllm:e2e_request_latency_seconds_bucket": "__e2e_buckets",
    "vllm:time_to_first_token_seconds_bucket": "__ttft_buckets",
    "vllm:request_time_per_output_token_seconds_bucket": "__tpot_buckets",
    # prefill 算力受限、decode 内存带宽受限,混进一个 tok/s 里两边信息都丢了。
    # 实测同一引擎上下文 4K→1M: decode 只掉 2%(41.2→40.3 tok/s), 而 TTFT 从
    # 3.2s 涨到 452s —— 长上下文的代价全在 prefill 一侧。混着报就看不出该查哪边。
    "vllm:request_prefill_time_seconds_bucket": "__prefill_buckets",
    "vllm:request_decode_time_seconds_bucket": "__decode_buckets",
    # queue: 区分"服务器慢"和"服务器排队"的唯一指标。实测 QPS 1.0→2.0 时
    # TTFT p90 从 1451ms 炸到 16248ms(11 倍), 而 TPOT 只从 90.5 到 96.5ms ——
    # 引擎没变慢,是在排队。没有这一项会把排队误判成模型问题去调参数。
    "vllm:request_queue_time_seconds_bucket": "__queue_buckets",
    # ITL 与 TPOT 不是一回事: TPOT 是整个请求的平均每 token 耗时(按请求计数),
    # ITL 是相邻 token 实际到达间隔的分布(按间隔计数)。本机实测该差异是实的:
    # ITL count 45000 ≈ drafts_total 44720, ITL sum 12373s ≈ decode sum 12362s
    # → 该引擎上 ITL 记的是"每次产出事件"的间隔, 而投机解码一步产出约 3.95 个
    # token, 所以 ITL 均值 275ms ≈ TPOT 均值 77ms × 3.95。一次接受多个 token 会
    # 产生一批接近 0 的间隔加少量长间隔, 均值看不出来, 分位数能。
    "vllm:inter_token_latency_seconds_bucket": "__itl_buckets",
}


async def _scrape_vllm(base: str) -> dict:
    """直采 vLLM 原生 /metrics（prom 文本）。含 V1 改名指标、e2e/TTFT/TPOT
    直方图桶, 以及投机解码每位置接受计数。"""
    out: dict[str, float] = {}
    hists: dict[str, dict[str, float]] = {k: {} for k in _VLLM_HISTS.values()}
    spec_pos: dict[str, float] = {}
    kv_info: dict[str, float] = {}
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
            hkey = _VLLM_HISTS.get(name)
            if hkey:
                mle = re.search(r'le="([^"]+)"', head)
                if mle:
                    hb = hists[hkey]
                    hb[mle.group(1)] = hb.get(mle.group(1), 0.0) + v
            elif name == "vllm:cache_config_info":
                # KV 池绝对值。这是 Prometheus info gauge(值恒为 1,信息全在
                # label 里),**就在已抓的这份文本内** —— 不需要 docker logs、
                # 不需要 ssh、不新增任何网络往返。
                # 不要用 num_gpu_blocks × block_size 反推池大小:该模型
                # 2176 × 2304 = 5,013,504 ≠ kv_cache_size_tokens 4,545,221。
                # 精确关系是 kv_cache_size_tokens == int(kv_cache_max_concurrency
                # × max_model_len):4.334661354581673 × 1048576 = 4,545,221,
                # 精确到个位(源头见 vLLM kv_cache_utils.py 的
                # get_kv_cache_capacity() / update_kv_cache_capacity())。
                # 以 kv_cache_size_tokens 为准。
                for lbl, key in (("kv_cache_size_tokens", "tokens"),
                                 ("kv_cache_memory_bytes", "bytes"),
                                 ("kv_cache_max_concurrency", "maxConc")):
                    mv = re.search(rf'{lbl}="([^"]+)"', head)
                    if not mv:
                        continue
                    try:
                        val = float(mv.group(1))
                    except ValueError:
                        continue
                    if key == "maxConc":
                        # ⛔ maxConc 是【比值】(满窗请求数 = 池容量 / max_model_len)，
                        # 跨 engine 求和无意义 —— DP>1 时会变成 N 倍。取最大值。
                        # tokens / bytes 是【容量】，多 engine 就该相加，故走下面分支。
                        kv_info[key] = max(kv_info.get(key, 0.0), val)
                    else:
                        kv_info[key] = kv_info.get(key, 0.0) + val
            elif name == "vllm:num_requests_waiting_by_reason":
                mr = re.search(r'reason="([^"]+)"', head)
                if mr and mr.group(1) == "capacity":     # 容量性排队 = 真饱和信号
                    out["__waiting_capacity"] = out.get("__waiting_capacity", 0.0) + v
            elif name == "vllm:spec_decode_num_accepted_tokens_per_pos_total":
                mp = re.search(r'position="([^"]+)"', head)
                if mp:
                    spec_pos[mp.group(1)] = spec_pos.get(mp.group(1), 0.0) + v
            elif name in _VLLM_SCALARS:
                out[name] = out.get(name, 0.0) + v
    except Exception:
        return {}
    out.update(hists)
    out["__spec_pos"] = spec_pos
    out["__kv_info"] = kv_info          # 老版本 vLLM 无此指标 → 空 dict → 字段缺席
    return out


def _merge_scrape(dicts: list[dict]) -> dict:
    """多副本 /metrics 合并：标量相加；dict 值（直方图桶 __*_buckets、投机解码
    每位置计数 __spec_pos）按子键相加。累积桶逐 le 相加后仍是合法累积直方图,
    故多副本合并出来的分位数就是全部副本的总口径。llama.cpp / SGLang 的
    scrape 结果键更少, 走同一分支不受影响。"""
    acc: dict = {"__e2e_buckets": {}}
    for d in dicts:
        for k, v in (d or {}).items():
            if isinstance(v, dict):
                sub = acc.setdefault(k, {})
                for sk, c in (v or {}).items():
                    sub[sk] = sub.get(sk, 0.0) + c
            else:
                acc[k] = acc.get(k, 0.0) + v
    return acc


def _rate(a: dict, b: dict, key: str, dt: float) -> float:
    """counter→rate。首采样缺失(scrape 失败 → 空 dict)或 counter reset 时返回 0,
    绝不把整段生命周期累计值当作单个 0.5s 窗口的吞吐——否则 tps 会炸成
    `累计token/0.5` 的天文数字(实测 6798/0.5≈13.6k 的幽灵峰值)。"""
    pa, pb = a.get(key), b.get(key)
    if pa is None or pb is None or pb < pa:
        return 0.0
    return (pb - pa) / dt


def _gauge(a: dict, b: dict, key: str) -> float:
    """瞬时 gauge(并发/排队):取两次采样的峰值,降低短请求落在采样间隙
    被整体漏成 0 的概率(单点采一个每 ~2.5s 才刷的瞬时值代表性差)。"""
    return max(a.get(key, 0.0), b.get(key, 0.0))


# base → (monotonic 时刻, generation_tokens_total)。用于把"瞬时吞吐"和
# "持续吞吐"分开报。vllm bench serve 自己就分两行报 Output token throughput
# 与 Peak output token throughput,同一次测量 61.13 对 129.00,差 2.1 倍 ——
# 只报一个数,看到 120+ 的人会把峰值当成持续吞吐。
_TPS_ANCHOR: dict[str, tuple[float, float]] = {}
_TPS_MIN_WIN = 15.0     # 窗口短于此 → 与瞬时值无异,不值得单列
_TPS_MAX_WIN = 300.0    # 超过此 → 锚点太旧,重置(否则会把很久以前的负载摊进来)


# SLO 阈值与饱和判据。阈值放配置不写死;整段缺失 → 相关字段全部缺席,面板不渲染。
_SLO = HEARTH_CFG.get("slo") or {}
# 效率口径。峰值算力必须用【实测】值:规格书数字与实际可达差很远,用规格书算出来的
# MFU 没有意义。GB10 实测 bf16 matmul burn 95.0 TFLOP/s(来源 tonyd2wild 的
# GLM-5.3-Int4-Int8Mix 仓,排查单节点降频时测得)。
_EFF = HEARTH_CFG.get("efficiency") or {}


def _slo_rates(ttft_b: dict, tpot_b: dict) -> dict | None:
    """SLO 达标率。**刻意不叫 goodput** —— 业界那个词特指「同时满足全部 SLO
    的请求占比」,是每请求的联合条件;我们手上只有聚合直方图,只能给边缘分布,
    拿分量去占用那个名字会误导。

    联合达标率用 Fréchet-Hoeffding 边界给严格区间(不需要任何独立性假设):
        joint ∈ [max(0, a+b-1), min(a, b)]
    区间宽度自己也是信息:a=99% b=60% → [59%,60%] 几乎等于精确值;
    a=80% b=70% → [50%,70%] 就只能当参考。宽度超过 sloJointWidthWarn
    个百分点时前端标"仅供参考"。"""
    t_ms, p_ms = _SLO.get("ttft_ms"), _SLO.get("tpot_ms")
    if not t_ms or not p_ms:
        return None
    a = _hfrac(ttft_b, t_ms / 1000.0)
    b = _hfrac(tpot_b, p_ms / 1000.0)
    if a is None or b is None:
        return None                     # 无样本 → 不报 0% 也不报 100%
    lo, hi = max(0.0, a + b - 1.0), min(a, b)
    return {"sloTtftMs": t_ms, "sloTpotMs": p_ms,
            "sloTtftRate": round(a * 100, 1), "sloTpotRate": round(b * 100, 1),
            "sloJointLower": round(lo * 100, 1), "sloJointUpper": round(hi * 100, 1),
            "sloJointWide": (hi - lo) * 100 > float(_SLO.get("joint_width_warn_pp", 10))}


def _efficiency(meta: dict, steps_ps: float, tok_ps: float,
                proc_tok_ps: float, draft_tok_ps: float) -> dict | None:
    """MBU(内存带宽利用率) / MFU(算力利用率)。需要按模型配激活参数量,
    没配就整块不返回 —— 不估算、不编造。

    MBU: decode 每步把激活权重完整读一遍 → 激活字节数 × 步频 / 理论带宽。
    ⚠️ 这是【纯权重口径】(weight-MBU),不含 KV cache 的读写。长上下文下总线
    实际占用会明显高于这个数,所以别把「100% - MBU」当成可用余量去解读。
    公式不动,只是读数时要知道它量的是什么。
    MFU 给两个口径,回答的是不同问题:
      mfu          verify pass 实际处理的 token(草稿长度+1)。硬件确实为全部
                   位置做了 FLOPs,被拒的草稿也真烧了算力 —— MFU 按定义是
                   「硬件利用率」,这个是主口径。
      mfuDelivered 实际吐出的 token。回答「每个有用 token 花了多少算力」。
    两者比值【不等于】接受率,是它的仿射变换:
        mfuDelivered / mfu = (k × acceptRate + 1) / (k + 1)
    实测 k=7、acceptRate=0.246 时比值 = (1.72+1)/8 = 0.340,而直接观察到的
    吐出/前向位置 = 1047/3080 = 0.340 —— 吻合。把它当成"比值即接受率"去反推,
    反推出来的接受率会偏高约 38%。仍不单列字段(可从两个已有字段直接看出)。
    FLOPs 用前向 MAC 近似 2 × 激活参数 × token 数。"""
    ap = meta.get("active_params_b")
    if not ap:
        return None
    out: dict = {}
    ap_n = float(ap) * 1e9
    # 并行度属于【部署】，硬件常数属于【机器】——两者必须分开。
    # peak_tflops / mem_bw_gbps 是【每节点】的 GB10 常数；active_params_b 是
    # 【全模型】激活量。TP=N 时每个节点只读自己那 1/N 分片、只用自己那份算力，
    # 所以分子是聚合的、分母必须乘上 N，否则系统性高估 N 倍。
    # 未配 tp_size 按 1 处理(单节点部署)。
    tp = float(meta.get("tp_size") or 1) or 1.0
    dp_n = float(meta.get("drafter_params_b") or 0) * 1e9
    # drafter 的每参数字节数【不继承】目标模型：目标是 NVFP4(0.57)，drafter
    # 是 bf16(2.0)，差 3.5 倍，继承会把 drafter 那一项算小。
    dbpp = float(meta.get("drafter_bytes_per_param") or 0)
    bw = _EFF.get("mem_bw_gbps")
    bpp = meta.get("bytes_per_param")
    if bw and bpp and steps_ps > 0:
        # drafter 在 MBU 里不可忽略：本机 1.1711B × 2B/param = 2.342 GB/步，
        # 相对目标模型的 10.26 GB/步 是 22.8%(占两者合计的 18.6%)。
        # 假设(未证实)：drafter 与目标模型同 TP 分片 —— vLLM 对 speculative
        # model 默认沿用同一 parallel config。若实际未分片，drafter 那一项应
        # 单独按 tp 倍算；因该项只占两成、影响可控，不为它加开关。
        step_bytes = ap_n * float(bpp) + dp_n * dbpp
        out["mbu"] = round(step_bytes * steps_ps / (float(bw) * 1e9 * tp) * 100, 1)
    peak = _EFF.get("peak_tflops")
    if peak and proc_tok_ps > 0:
        # FLOPs 用前向 MAC 近似 2 × 激活参数 × token 数。
        # 分子用【实测】的 iteration_tokens_sum 速率,而不是 步频 × (k+1) 估算:
        # 前者是引擎自报的"这些前向里真处理了多少 token 位置",天然涵盖投机解码
        # 的 verify 位置与 prefill 位置,不需要假设每步批量或草稿长度。
        # 代价是 prefill 阶段 mfu 会冲高 —— 那是真实的算力消耗,不是失真。
        peak_f = float(peak) * 1e12 * tp        # 同 MBU：分母按 TP 聚合
        fl = 2 * ap_n * proc_tok_ps
        if dp_n and draft_tok_ps > 0:
            # FLOPs 只数【位置】不数【前向次数】：7 次串行单 token 前向
            # = 7 × (2·P·1) = 14P，1 次并行 7 token 前向 = 2·P·7 = 14P，完全相同。
            # block-diffusion 省的是步时(墙钟)，不是 FLOP 计数。
            # ⛔ 位置数直接用 draft_tokens 速率，不要用 步频 × spec_len：
            # drafts 是【每请求每步】计数，连续批处理下 drafts = B × 步数
            # (实测 385/77 = 批量 5)，用步频算会把 drafter FLOPs 少算 B 倍。
            # 这与 MBU 步频那处是同一类错误(每请求计数 vs 引擎计数)，方向相反。
            # 用速率还有个好处：窗口内没发生投机解码时它自然为 0，与下面
            # verify 位置的回落判据一致，不会一个按生命周期、一个按窗口。
            # 待核：config.json 的 dflash_config.block_size = 8 而
            # num_speculative_tokens = 7。若 drafter 前向实际覆盖 8 个位置，
            # 这一项偏低 14%；但该项仅占总 FLOPs 约 5-6%，净影响 <1%。
            # 未证实前不猜，直接采信引擎自报的 draft_tokens。
            fl += 2 * dp_n * draft_tok_ps
        elif draft_tok_ps > 0:
            out["mfuDrafterMissing"] = True   # 未配 drafter 参数量 → mfu 偏低估
        out["mfu"] = round(fl / peak_f * 100, 1)
        out["mfuDelivered"] = round(2 * ap_n * tok_ps / peak_f * 100, 1)
    return out or None


# ── LiteLLM spend logs（带外只读，不碰推理路径）────────────────────────
# 为什么走这条路：thinking/正文的区分【引擎侧完全没有】—— vLLM /metrics 里没有
# 任何 reasoning 指标；LiteLLM 自己的 prometheus 是企业版门控(见本文件上方注释)。
# 唯一通的是网关落库的响应正文(litellm config 里 store_prompts_in_spend_logs)。
# 这是【只读、带外】查询：不经过推理路径、不给引擎发任何请求，对模型零影响。
_SPEND = HEARTH_CFG.get("spend_logs") or {}
_SPEND_DATA: dict = {"ts": 0.0, "by_model": {}}
_SPEND_TASK = None

# 判据说明(踩过的两个坑，改这段前先读)：
# 1) tool_calls 必须用 jsonb_typeof(...)='array' 判定。键存在但值为 JSON null 时
#    SQL 的 `IS NOT NULL` 仍为真 —— 用它会把纯文本响应误判成工具调用。
# 2) ⛔ 不能用 finish_reason 区分「文本响应」和「工具响应」：GLM-5.3-Flash 发
#    tool_calls 时 finish_reason 仍是 'stop'。实测 422 条工具响应里 323 条藏在
#    'stop' 下，只有 99 条是 'tool_calls'。按 finish_reason 过滤会把工具响应算进
#    文本组，于是「空正文率」虚高到 35.6%（那些正文本来就该是空的，模型用工具
#    作答了）。按 tool_calls 数组判定后，真实文本响应的空正文率是 0%。
# 3) completion_tokens > N 顺带排掉健康探针(实测探针 max 5 token)。
_SPEND_SQL = """
WITH t AS (
  SELECT model, (response::jsonb->'choices'->0->'message') AS msg
  FROM "LiteLLM_SpendLogs"
  WHERE "startTime" > now() - '{win} hours'::interval
    AND completion_tokens > {mintok} AND response IS NOT NULL
), u AS (
  SELECT CASE WHEN position('/' in model)>0 THEN split_part(model,'/',2) ELSE model END AS m,
         jsonb_typeof(msg->'tool_calls')='array' AS is_tool,
         length(COALESCE(msg->>'content','')) c,
         length(COALESCE(msg->>'reasoning_content','')) r
  FROM t
)
SELECT m, count(*) FILTER (WHERE NOT is_tool), count(*) FILTER (WHERE NOT is_tool AND c=0),
       count(*) FILTER (WHERE is_tool),
       count(*) FILTER (WHERE NOT is_tool AND c<{lim} AND r<{lim}),
       count(*) FILTER (WHERE NOT is_tool AND (c>={lim} OR r>={lim})),
       COALESCE(sum(r) FILTER (WHERE NOT is_tool AND c<{lim} AND r<{lim}),0),
       COALESCE(sum(c+r) FILTER (WHERE NOT is_tool AND c<{lim} AND r<{lim}),0)
FROM u GROUP BY m
"""


async def _spend_scrape() -> dict:
    """查一次 spend logs，返回 模型名 → 指标。失败一律返回 {} → 字段缺席。

    只读是【硬保证】不是纪律：PGOPTIONS 强制 default_transaction_read_only=on，
    写操作会被 Postgres 直接拒绝(已实测 CREATE TABLE 报错)。
    走 docker exec 而非 TCP：容器内不需要口令，避免把 DB 密码放进配置。
    postgres 也监听 127.0.0.1:5432，日后要换 TCP 只需改 argv。"""
    if not _SPEND.get("enabled"):
        return {}
    try:
        win = int(_SPEND.get("window_hours", 6))
        mintok = int(_SPEND.get("min_completion_tokens", 20))
        lim = int(_SPEND.get("truncation_char_limit", 2200))
    except (TypeError, ValueError):
        return {}
    sql = _SPEND_SQL.format(win=win, mintok=mintok, lim=lim)
    argv = ["docker", "exec", "-e", "PGOPTIONS=-c default_transaction_read_only=on",
            str(_SPEND.get("container", "litellm-postgres")),
            "psql", "-U", str(_SPEND.get("user", "litellm")),
            "-d", str(_SPEND.get("db", "litellm")),
            "-X", "-A", "-F", "\t", "-t", "-v", "ON_ERROR_STOP=1", "-c", sql]
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, _err = await asyncio.wait_for(proc.communicate(), timeout=20.0)
        if proc.returncode != 0:
            return {}
    except Exception:
        return {}
    by_model: dict = {}
    for line in out.decode("utf-8", "replace").splitlines():
        f = line.split("\t")
        if len(f) != 8:
            continue
        try:
            m = f[0].strip()
            text_n, empty_n, tool_n, untrunc, trunc, think_c, total_c = (int(x) for x in f[1:])
        except ValueError:
            continue
        d: dict = {"spendWindowH": win, "toolCallN": tool_n}
        if text_n > 0:
            d["emptyContentN"] = empty_n
            d["emptyContentTotal"] = text_n
            d["emptyContentRate"] = round(empty_n / text_n * 100, 1)
        if untrunc > 0 and total_c > 0:
            # ⚠️ 字符占比,不是 token 占比 —— token 级切分没落库。字段名带 Char
            # 就是为了不让人当成 token 占比读。
            # 只统计两个字段都未触顶的响应:LiteLLM 落库把文本截断在约 2293 字符,
            # 对全体求平均会因截断【系统性偏低】。
            d["thinkingCharShare"] = round(think_c / total_c * 100, 1)
            d["thinkingSampleN"] = untrunc
            d["thinkingTruncatedN"] = trunc
        by_model[m] = d
    return by_model


async def _spend_loop():
    """独立的长 TTL 刷新循环。⛔ 绝不能放进 2.5s 快照循环:那张表近百万行,
    且这是子进程 + DB 查询。默认 300s 一次 ≈ 0.0033 次/秒。"""
    while True:
        try:
            d = await _spend_scrape()
            if d:
                _SPEND_DATA["by_model"], _SPEND_DATA["ts"] = d, time.time()
        except Exception:
            pass
        await asyncio.sleep(max(60, int(_SPEND.get("refresh_seconds", 300))))


def _spend_for(model_id: str) -> dict:
    """取某模型的 spend 指标。没开、没数据、该模型没样本 → 空 dict → 字段缺席。"""
    global _SPEND_TASK
    if not _SPEND.get("enabled"):
        return {}
    if _SPEND_TASK is None or _SPEND_TASK.done():
        _SPEND_TASK = asyncio.create_task(_spend_loop())
    return (_SPEND_DATA["by_model"] or {}).get(model_id) or {}


def _tps_rollup(samples: dict, now: float) -> dict[str, tuple]:
    """每轮**只调一次**,推进所有 base 的锚点并算出各自的长窗口持续吞吐。

    必须整轮一次性算完、不能放进按模型的循环里:同一个 base 可能被多个模型 id
    命中(LiteLLM 别名),循环里逐模型推锚点会让第二个模型读到刚被自己写成
    age=0 的锚点,持续值恒为 None。

    返回 base → (tok/s 或 None, 实际窗口秒数 或 None)。"""
    out: dict[str, tuple] = {}
    for base, samp in samples.items():
        cur = (samp or {}).get("vllm:generation_tokens_total")
        if cur is None:                         # 抓取失败 → 不动锚点
            out[base] = (None, None)
            continue
        prev = _TPS_ANCHOR.get(base)
        if prev is None:
            _TPS_ANCHOR[base] = (now, cur)
            out[base] = (None, None)
            continue
        age, ptok = now - prev[0], prev[1]
        if cur < ptok or age > _TPS_MAX_WIN:    # 引擎重启 / 锚点过旧 → 重新起锚
            _TPS_ANCHOR[base] = (now, cur)
            out[base] = (None, None)
            continue
        if age < _TPS_MIN_WIN:                  # 窗口还不够长,保留旧锚点继续攒
            out[base] = (None, None)
            continue
        _TPS_ANCHOR[base] = (now, cur)
        out[base] = ((cur - ptok) / age, age)
    return out


def _tps_sustained(bases: list[str], roll: dict) -> tuple:
    """把某模型名下各副本的持续吞吐汇总。要么全部有效要么整体 None ——
    不同窗口的速率相加没有意义,也不拿 1.2s 瞬时值冒充持续值。"""
    tot, win = 0.0, None
    for base in bases:
        r, age = roll.get(base, (None, None))
        if r is None:
            return None, None
        tot += r
        win = age if win is None else max(win, age)
    if win is None:
        return None, None
    return round(tot, 1), round(win, 1)


def _spec_stats(a: dict, b: dict) -> dict | None:
    """投机解码(speculative decoding)派生指标。未开投机解码 → 引擎不暴露这批
    counter → 返回 None,前端据此整块不渲染(而不是显示 0% —— 那是伪造)。

    为什么这几个数值得单列:实测同一引擎五种负载,每步耗时(ms/step)全在
    76-80ms 之间只差 5.5%,而端到端吞吐从 38 到 102 tok/s 差了 168% ——
    差异几乎全部来自接受率。只看 tps/TPOT 看不到决定吞吐的那个自变量。

    口径(两种并存,语义固定,不做模式切换):
      acceptRate    生命周期累计 = accepted_tokens_total / draft_tokens_total
      acceptRateNow 本采样窗口增量,反映"当前这种负载"的接受率;窗口内没有
                    草稿(空闲)时为 None,不拿生命周期值冒充当前值
      tokensPerStep 每步产出 = 1 + accepted_tokens_total / drafts_total
      perPos        每位置接受率 = per_pos_total[i] / drafts_total
    perPos / tokensPerStep 只给生命周期口径:一个窗口约 16 步,窗口内的
    每位置接受率量化粒度是 1/16,噪声比信号大。"""
    drafts = b.get("vllm:spec_decode_num_drafts_total", 0.0)
    dtok = b.get("vllm:spec_decode_num_draft_tokens_total", 0.0)
    acc = b.get("vllm:spec_decode_num_accepted_tokens_total", 0.0)
    pos = b.get("__spec_pos") or {}
    if drafts <= 0:
        return None                     # 未开投机解码 / 开了但零流量 → 不伪造

    d_dtok = _rate(a, b, "vllm:spec_decode_num_draft_tokens_total", 1.0)
    d_acc = _rate(a, b, "vllm:spec_decode_num_accepted_tokens_total", 1.0)
    now = round(d_acc / d_dtok * 100, 1) if d_dtok > 0 else None

    # position 是字符串标签,必须按整数排序 —— 字典序下 "10" < "2",
    # 草稿长度 >=10 时曲线会被打乱。
    keys = sorted((k for k in pos if str(k).isdigit()), key=int)
    per_pos = [round(pos[k] / drafts * 100, 1) for k in keys]
    return {"acceptRate": round(acc / dtok * 100, 1) if dtok > 0 else 0.0,
            "acceptRateNow": now,
            "tokensPerStep": round(1 + acc / drafts, 2),
            "perPos": per_pos,
            "specLen": len(per_pos),
            "drafts": int(drafts), "draftTokens": int(dtok),
            "accepted": int(acc)}


@app.get("/api/models")
async def models_list():
    disco = await _disco_cached()
    # 所有"在线且有 vLLM 指标"的后端 → 两次采样算 counter→rate（含多副本）
    vbases = sorted({b for m in disco for b in m.get("vllm_bases", [])})
    lbases = sorted({b for m in disco for b in m.get("llamacpp_bases", [])})
    gbases = sorted({b for m in disco for b in m.get("sglang_bases", [])})
    # 2026-08-29: dt 原为硬编码 0.5, 但真实间隔 = sleep + 两轮【串行】抓取耗时。
    # 抓取耗时被漏算 -> dt 偏小 -> 速率系统性偏高(实测虚高约 2 倍:
    # 持续 61 tok/s 显示成 129)。改为用 monotonic 实测间隔。
    # sleep 同时 0.5 -> 1.2s: 本地推理一步约 76ms, 0.5s 窗口只装得下 7 步,
    # 而每步产出 1-5 token 方差极大; 1.2s 约 16 步, 方差被抹平且接口仍可接受。
    _t0 = time.monotonic()
    s1 = {b: await _scrape_vllm(b) for b in vbases}
    l1 = {b: await _scrape_llamacpp(b) for b in lbases}
    g1 = {b: await _scrape_sglang(b) for b in gbases}
    await asyncio.sleep(1.2)
    s2 = {b: await _scrape_vllm(b) for b in vbases}
    l2 = {b: await _scrape_llamacpp(b) for b in lbases}
    g2 = {b: await _scrape_sglang(b) for b in gbases}
    _dt_real = max(1e-3, time.monotonic() - _t0)
    _t_now = time.monotonic()           # 持续吞吐锚点时刻,整轮统一
    _tps_roll = _tps_rollup(s2, _t_now)  # 整轮一次性推进锚点(不可下放进循环)
    out = []
    for m in disco:
        vb = m.get("vllm_bases") or []
        lb = m.get("llamacpp_bases") or []
        gb = m.get("sglang_bases") or []
        base_keys = ("id", "display", "vendor", "kind", "params", "quant",
                     "ctx", "framework", "nodes", "vram", "route", "tags",
                     "identityUnverified", "identityCandidates")
        card = {k: m.get(k) for k in base_keys}
        if vb:                                  # 真实 vLLM 指标（可能多副本汇总）
            a = _merge_scrape([s1.get(b) or {} for b in vb])
            b = _merge_scrape([s2.get(b) or {} for b in vb])
            dt = _dt_real
            tps = _rate(a, b, "vllm:generation_tokens_total", dt)
            rps = _rate(a, b, "vllm:request_success_total", dt)
            tcnt = b.get("vllm:time_to_first_token_seconds_count", 0)
            tsum = b.get("vllm:time_to_first_token_seconds_sum", 0)
            ttft = (tsum / tcnt * 1000) if tcnt else 0
            pcnt = b.get("vllm:request_time_per_output_token_seconds_count", 0)
            psum = b.get("vllm:request_time_per_output_token_seconds_sum", 0)
            tpot = (psum / pcnt * 1000) if pcnt else 0
            kv = b.get("vllm:kv_cache_usage_perc", 0) * 100 / max(1, len(vb))
            e2e_b = b.get("__e2e_buckets") or {}
            ttft_b = b.get("__ttft_buckets") or {}
            tpot_b = b.get("__tpot_buckets") or {}
            queue_b = b.get("__queue_buckets") or {}
            prefill_b = b.get("__prefill_buckets") or {}
            decode_b = b.get("__decode_buckets") or {}
            itl_b = b.get("__itl_buckets") or {}
            tps_sus, tps_win = _tps_sustained(vb, _tps_roll)
            running = _gauge(a, b, "vllm:num_requests_running")
            waiting = _gauge(a, b, "vllm:num_requests_waiting")
            state = "serving" if running > 0 or tps > 0 else "idle"
            live = {"tps": round(tps, 1), "rps": round(rps, 3),
                    "ttft": round(ttft, 1), "tpot": round(tpot, 1),
                    "kv": round(kv, 1), "running": int(running),
                    "waiting": int(waiting), "metrics": "vllm",
                    # 真实驻留探针：vLLM 可达且模型已加载 → 权重常驻、毫秒级可服务
                    "resident": True,
                    "p50": round(_hquant(e2e_b, 0.50) * 1000, 0),
                    "p95": round(_hquant(e2e_b, 0.95) * 1000, 0),
                    "p99": round(_hquant(e2e_b, 0.99) * 1000, 0),
                    # TTFT / TPOT 分位数(ms)。上面的 ttft/tpot 均值字段保留不删:
                    # 样本量小时分位数会偏高(几十个样本的 p99 基本就是最大值),
                    # 两者背离大时以均值为准。分位数走 p50/p90/p99 是业界口径
                    # (vllm bench serve / GenAI-Perf / LLMPerf), 与上面 e2e 那组
                    # 历史 p50/p95/p99 刻意不统一 —— 不动既有 e2e 字段。
                    "ttftP50": round(_hquant(ttft_b, 0.50) * 1000, 0),
                    "ttftP90": round(_hquant(ttft_b, 0.90) * 1000, 0),
                    "ttftP99": round(_hquant(ttft_b, 0.99) * 1000, 0),
                    "tpotP50": round(_hquant(tpot_b, 0.50) * 1000, 1),
                    "tpotP90": round(_hquant(tpot_b, 0.90) * 1000, 1),
                    "tpotP99": round(_hquant(tpot_b, 0.99) * 1000, 1),
                    # tps 是 1.2s 瞬时采样窗口 —— 把窗口长度一并暴露出来,
                    # 面板才能标清"瞬时"而不是让人当成持续吞吐。
                    "tpsWindowSec": round(dt, 2),
                    "tpsSustained": tps_sus, "tpsSustainedWindowSec": tps_win}
            # 请求耗时分解:排队 → prefill → decode。三段分开报,"变慢了"才知道
            # 该查哪一侧(排队 = 容量不够 / prefill = 上下文太长 / decode = 带宽)。
            for _k, _bk, _sum, _cnt in (
                    ("queue", queue_b, "vllm:request_queue_time_seconds_sum",
                     "vllm:request_queue_time_seconds_count"),
                    ("prefill", prefill_b, "vllm:request_prefill_time_seconds_sum",
                     "vllm:request_prefill_time_seconds_count"),
                    ("decode", decode_b, "vllm:request_decode_time_seconds_sum",
                     "vllm:request_decode_time_seconds_count"),
                    ("itl", itl_b, "vllm:inter_token_latency_seconds_sum",
                     "vllm:inter_token_latency_seconds_count")):
                _c, _s = b.get(_cnt, 0), b.get(_sum, 0)
                live[_k] = round(_s / _c * 1000, 1) if _c else 0     # 均值对照线
                live[f"{_k}P50"] = round(_hquant(_bk, 0.50) * 1000, 1)
                live[f"{_k}P90"] = round(_hquant(_bk, 0.90) * 1000, 1)
                live[f"{_k}P99"] = round(_hquant(_bk, 0.99) * 1000, 1)
            spec = _spec_stats(a, b)
            if spec:                    # 未开投机解码 → 键整个缺席,前端 if (live.spec)
                live["spec"] = spec
            # SLO 达标率(阈值来自配置 slo:,未配则整组字段缺席)
            slo = _slo_rates(ttft_b, tpot_b)
            if slo:
                live.update(slo)
            # 饱和提示:排队时间占了 TTFT 的大头 + 两次采样都有请求在等 →
            # 再加负载已经不划算(实测 QPS 1.0→2.0 吞吐只涨 25%,而 TTFT p90
            # 涨了一个数量级,多出来的时间几乎全花在排队上)。
            # "持续"在单次调用内只能取两次采样都 > 0 —— 这是本接口能拿到的
            # 最强证据,不做跨调用状态。
            _qr = (live["queueP90"] / live["ttftP90"]) if live["ttftP90"] > 0 else 0.0
            live["queueShareP90"] = round(_qr * 100, 1)
            live["waitingCapacity"] = int(_gauge(a, b, "__waiting_capacity"))
            live["saturated"] = bool(
                min(a.get("vllm:num_requests_waiting", 0),
                    b.get("vllm:num_requests_waiting", 0)) > 0
                and _qr > float(_SLO.get("queue_share_warn", 0.5)))
            # KV 池绝对值。来自 cache_config_info 这个 info gauge,搭现有抓取的
            # 顺风车 —— 零额外往返。老版本 vLLM 无此指标 → 字段缺席。
            _kvi = b.get("__kv_info") or {}
            if _kvi.get("tokens"):
                live["kvTokens"] = int(_kvi["tokens"])
                live["kvBytes"] = int(_kvi.get("bytes", 0))
                live["kvMaxConc"] = round(_kvi.get("maxConc", 0), 2)
            # MBU / MFU。步频:开了投机解码时一步 = 一次 draft,用 drafts 速率;
            # 否则一步出一个 token,用 token 速率。
            _steps = _rate(a, b, "vllm:iteration_tokens_total_count", dt)
            live["stepsPerSec"] = round(_steps, 2)
            # MFU 的分子 = 目标模型【真正做过前向的位置数】。
            # ⛔ 不能直接用 iteration_tokens_total_sum:vLLM 源码
            # (vllm/v1/metrics/loggers.py:1205-1208) 里它 =
            #   prompt_token_stats.computed + num_generation_tokens
            # 也就是「实算 prefill + 吐出的 token」,**不含被拒的草稿位置**。
            # 硬件为 k+1 个位置烧了算力,它只数了吐出的那 ~2.7 个 —— 实测低估
            # 2.94-3.03 倍。(注意 iteration_sum - prompt == generation 是上式
            # 构造出来的恒等式,拿它做验证是循环论证,不构成证据。)
            _gen_ps = tps
            _iter_ps = _rate(a, b, "vllm:iteration_tokens_total_sum", dt)
            # 实算 prefill:天然排除 prefix cache 命中的部分,正是"真的算了的"那些
            _prefill_ps = max(0.0, _iter_ps - _gen_ps)
            _dtok_ps = _rate(a, b, "vllm:spec_decode_num_draft_tokens_total", dt)
            _drafts_ps = _rate(a, b, "vllm:spec_decode_num_drafts_total", dt)
            # verify 位置 = 草稿位置 + 每(请求,步)一个的 bonus token。
            # num_drafts_total 正是「每请求每步 1 个」的计数,恰好补上 bonus。
            # 实测两轮 verify位置/drafts 都精确 = 8.00 = k+1。
            # 未开投机解码时 drafts 为 0,此时 decode 位置就是吐出的 token ——
            # 不能让关掉投机解码的模型算出 0。
            _verify_ps = (_dtok_ps + _drafts_ps) if _drafts_ps > 0 else _gen_ps
            _proc = _prefill_ps + _verify_ps
            eff = _efficiency((HEARTH_CFG.get("model_meta") or {}).get(m["id"]) or {},
                              _steps, tps, _proc, _dtok_ps)
            if eff:
                live.update(eff)
            live.update(_spend_for(m["id"]))   # 带外只读，缺数据即缺字段
        elif lb:                                # llama.cpp 真实指标（可能多副本汇总）
            a = _merge_scrape([l1.get(b) or {} for b in lb])
            b = _merge_scrape([l2.get(b) or {} for b in lb])
            dt = _dt_real
            tps = _rate(a, b, "llamacpp:tokens_predicted_total", dt)
            # tpot: 解码耗时差 / 解码 token 差 → ms/token(两采样齐备才算,缺则留 0)
            d_tok = _rate(a, b, "llamacpp:tokens_predicted_total", 1.0)
            d_sec = _rate(a, b, "llamacpp:tokens_predicted_seconds_total", 1.0)
            tpot = (d_sec / d_tok * 1000) if d_tok > 0 else 0
            running = _gauge(a, b, "llamacpp:requests_processing")
            waiting = _gauge(a, b, "llamacpp:requests_deferred")
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
            dt = _dt_real
            tps = _rate(a, b, "sglang:generation_tokens_total", dt)
            tcnt = b.get("sglang:time_to_first_token_seconds_count", 0)
            tsum = b.get("sglang:time_to_first_token_seconds_sum", 0)
            ttft = (tsum / tcnt * 1000) if tcnt else 0
            icnt = b.get("sglang:inter_token_latency_seconds_count", 0)
            isum = b.get("sglang:inter_token_latency_seconds_sum", 0)
            tpot = (isum / icnt * 1000) if icnt else 0
            kv = b.get("sglang:token_usage", 0) * 100 / max(1, len(gb))
            e2e_b = b.get("__e2e_buckets") or {}
            running = _gauge(a, b, "sglang:num_running_reqs")
            waiting = _gauge(a, b, "sglang:num_queue_reqs")
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
    cl, models, training, infra_dev = await asyncio.gather(
        cluster(), models_list(), _training_payload(), _infra_payload())
    al = await _alerts(nodes, log)                # 复用 nodes/log, 不重复跑
    await _notify_alerts(al)                       # 推送渠道(跳变才发, 不阻塞失败)
    return {"ts": time.time(), "cluster": cl, "nodes": nodes,
            "models": models, "alerts": al, "log": log, "training": training,
            "infra": infra_dev}


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
        "FROM \"LiteLLM_SpendLogs\" "
        # Hide LiteLLM's background health probes — two forms, both pollute the
        # request panel and make an idle cluster look like it's under load:
        #   1. Successful probes: a real 10+5-token completion every 27s,
        #      tagged `api_key='litellm-internal-health-check'` with
        #      `call_type='acompletion'`.
        #   2. Failed probes against down backends: written with empty
        #      `call_type` and api_key as NULL/empty-string/literal-"None"
        #      (LiteLLM's error path), empty messages, 0 tokens / duration.
        # Real gateway calls always have a real call_type and a real api_key;
        # the dual filter cleanly separates business from monitoring noise.
        "WHERE call_type IS NOT NULL AND call_type != '' "
        "  AND COALESCE(api_key,'') NOT IN ('', 'None', 'litellm-internal-health-check') "
        "ORDER BY \"startTime\" DESC LIMIT 60;")
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
           "FROM \"LiteLLM_SpendLogs\" "
           # Same dual filter as _litellm_request_log — see note there.
           "WHERE call_type IS NOT NULL AND call_type != '' "
           "  AND COALESCE(api_key,'') NOT IN ('', 'None', 'litellm-internal-health-check');")
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


@app.get("/api/config")
async def config():
    """Display-only config the frontend needs at boot. Kept tiny so it can be
    fetched once and cached; not for live metrics."""
    d = HEARTH_CFG.get("display") or {}
    # `timezone` is an optional IANA zone (e.g. "Asia/Taipei", "Europe/London").
    # Omit / leave empty → frontend formats timestamps in the browser's own
    # locale. Set it to pin the dashboard to a fixed zone regardless of who's
    # looking — useful when the same UI is shared across regions or viewed
    # from a server-side browser whose TZ isn't your operating zone.
    return {"cluster_name": d.get("cluster_name", "Hearth"),
            "timezone": (d.get("timezone") or "").strip() or None}


# ── Energy trends ─────────────────────────────────────────────────────
# Day/night split + multi-window rollups so the operator can see whether a
# physical change (AC setpoint adjustment, sunshade, sensor relocation,
# workload shift) actually moved the energy bill — without having to write
# PromQL by hand. The split is Hearth-side because PromQL has no clean way
# to bucket samples by local-timezone hour; we pull the raw range and
# partition in Python using display.timezone (falls back to UTC).
#
# Windows: 24h (immediate feedback), 7d (weekly baseline), 30d (monthly
# trend). On a freshly-deployed exporter these windows will be partially
# filled — the rollup honestly reflects only the seconds we actually have,
# the same convention as the per-node kWh figures.
def _local_hour(ts_unix: float, tz_name: str | None) -> int:
    if not tz_name:
        return datetime.utcfromtimestamp(ts_unix).hour
    try:
        from zoneinfo import ZoneInfo
        return datetime.fromtimestamp(ts_unix, tz=ZoneInfo(tz_name)).hour
    except Exception:
        return datetime.utcfromtimestamp(ts_unix).hour


def _split_day_night(series, tz_name: str | None, day_start=6, day_end=18):
    """series = [(ts, value), ...]. Returns (day_vals, night_vals)."""
    day, night = [], []
    for ts, v in series:
        h = _local_hour(ts, tz_name)
        (day if day_start <= h < day_end else night).append(v)
    return day, night


def _agg(series, tz_name, step_seconds=900):
    """Returns dict of avgW / kwh / dayAvgW / nightAvgW / samples for a series.
    step_seconds = query_range step, so sum × step / 3600 / 1000 = kWh."""
    if not series:
        return {"avgW": None, "kwh": None, "dayAvgW": None,
                "nightAvgW": None, "samples": 0}
    vals = [v for _, v in series]
    day, night = _split_day_night(series, tz_name)
    kwh = sum(vals) * step_seconds / 3600 / 1000
    return {
        "avgW":      round(sum(vals) / len(vals), 1),
        "kwh":       round(kwh, 2),
        "dayAvgW":   round(sum(day) / len(day), 1) if day else None,
        "nightAvgW": round(sum(night) / len(night), 1) if night else None,
        "samples":   len(vals),
    }


async def _window_series(promql_str: str, minutes: int, step: int = 900):
    """Pull a single-series range, return [(ts, value)]."""
    raw = await promql_range(promql_str, minutes=minutes, step=step)
    if not raw or not raw[0].get("values"):
        return []
    return [(t, v) for t, v in raw[0]["values"]]


@app.get("/api/energy/trends")
async def energy_trends():
    """Day/night × 24h/7d/30d rollups for AC + total wall power + cabinet temp.
    Lets operators A/B physical changes (e.g. sunshade) by comparing same-
    window-same-period-of-day numbers rather than hand-eyeballing graphs."""
    tz = (HEARTH_CFG.get("display") or {}).get("timezone") or None
    # Step 15min is plenty for trend (we don't need second-level here).
    STEP = 900
    windows = [("last24h", 24 * 60), ("last7d", 7 * 24 * 60), ("last30d", 30 * 24 * 60)]

    out = {"timezone": tz or "UTC",
           "dayDef": "06:00–18:00 local",
           "ac": {}, "wall": {}, "cabinet": {}}

    for name, mins in windows:
        ac_series   = await _window_series("ha_rack_ac_power_watts", mins, STEP)
        wall_series = await _window_series("sum(ha_node_wall_power_watts)", mins, STEP)
        cab_series  = await _window_series("avg(ha_node_plug_temp_celsius)", mins, STEP)
        out["ac"][name]   = _agg(ac_series, tz, STEP)
        out["wall"][name] = _agg(wall_series, tz, STEP)
        # Cabinet temp is a °C measurement, not power — only mean/day/night
        # are meaningful (no "kWh of temperature").
        if cab_series:
            day, night = _split_day_night(cab_series, tz)
            vals = [v for _, v in cab_series]
            out["cabinet"][name] = {
                "meanC":     round(sum(vals) / len(vals), 1),
                "minC":      round(min(vals), 1),
                "maxC":      round(max(vals), 1),
                "dayMeanC":  round(sum(day) / len(day), 1) if day else None,
                "nightMeanC": round(sum(night) / len(night), 1) if night else None,
                "samples":   len(vals),
            }
        else:
            out["cabinet"][name] = {"meanC": None, "minC": None, "maxC": None,
                                    "dayMeanC": None, "nightMeanC": None, "samples": 0}
    return out


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
