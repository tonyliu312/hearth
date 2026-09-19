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
from collections import deque
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
# 按节点直采(sources.node_exporter_url)的超时。这类节点多是经 SSH 反向隧道接入的
# 笔记本:合盖时隧道可能半开,连接能建上但没有数据。超时必须短,否则每个 SSE tick
# 都被拖住几秒。darwin 的 node_exporter 没有慢 hwmon,正常一次 scrape 远低于此值。
NODEEXP_NODE_TIMEOUT = float(os.environ.get("NODE_EXPORTER_NODE_TIMEOUT", "2.5"))
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
        # sources.node_exporter_url 优先:该节点自己的 node_exporter,按节点直采,既不经
        # obs Prometheus、也不是 Hearth 宿主本机(例:MBP 的 :9100 经 SSH 反向隧道映射到
        # 本机 127.0.0.1:19100)。这类节点没有任何 GPU 遥测源。
        "node_source": ("exporter" if src.get("node_exporter_url")
                        else src.get("node_metrics") or ("obs" if obs_label else "direct")),
        "exporter_url": (src.get("node_exporter_url") or "").rstrip("/") or None,
        # gpu_probe_ssh: Apple Silicon 的 GPU 利用率/显存 node_exporter 不导出,
        # 只能问 ioreg。配成 user@host 后走【免密只读 SSH】, 30s 一次(见 _darwin_gpu)。
        "gpu_probe_ssh": (src.get("gpu_probe_ssh") or "").strip() or None,
        # 笔记本会合盖/离家,离线是常态:alert_offline: false → 卡片照常显示 OFFLINE,
        # 但不发 offline 告警(否则每轮一条 bad,还会推送)。
        "alertOffline": y.get("alert_offline", True) is not False,
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


async def _scrape_node_exporter(url: str | None = None,
                                timeout: float | None = None) -> dict[str, list[dict]]:
    """抓一次 node-exporter，按指标名归并 [{labels, value}]。默认是宿主本机。"""
    out: dict[str, list[dict]] = {}
    try:
        r = await client.get(f"{url or NODEEXP_URL}/metrics",
                             timeout=timeout or NODEEXP_TIMEOUT)
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
async def _atlas_node_live(url: str | None = None, timeout: float | None = None) -> dict:
    """两次采样算 CPU% / 网速。url 缺省 = Hearth 宿主本机(legacy 名 atlas);
    给了 url 就是按节点直采(sources.node_exporter_url)。"""
    ts1 = time.monotonic()
    s1 = await _scrape_node_exporter(url, timeout)
    if not s1:
        return {}
    await asyncio.sleep(0.4)
    ts2 = time.monotonic()
    s2 = await _scrape_node_exporter(url, timeout)
    if not s2:
        return {}
    # 采样间隔取【两次请求发起】之差,不是写死 0.4:第一次 scrape 自身的耗时也在
    # 间隔里。宿主的 hwmon collector 一次 3-5s(见 NODEEXP_TIMEOUT 注释),写死 0.4
    # 会把网速放大约 10 倍;经隧道的节点也有几十毫秒往返。
    span = max(0.05, ts2 - ts1)

    def cpu_total(s):
        idle = _sum(s.get("node_cpu_seconds_total", []), lambda l: l.get("mode") == "idle")
        tot = _sum(s.get("node_cpu_seconds_total", []))
        return idle, tot
    i1, t1 = cpu_total(s1)
    i2, t2 = cpu_total(s2)
    cpu = max(0.0, min(100.0, (1 - (i2 - i1) / (t2 - t1)) * 100)) if t2 > t1 else 0.0

    darwin = any(x["labels"].get("sysname") == "Darwin" for x in s2.get("node_uname_info", []))
    memt = _sum(s2.get("node_memory_MemTotal_bytes", []))
    mema = _sum(s2.get("node_memory_MemAvailable_bytes", []))
    if memt:
        mem = (1 - mema / memt) * 100
    elif darwin:
        # macOS 的 node_exporter 没有 MemTotal/MemAvailable，只有 vm_stat 那套页面分类。
        # 按「活动监视器 · 已用内存」口径：App 内存(internal - purgeable) + 联动 + 压缩。
        # ⛔ 不能用 1 - free/total：macOS 把空闲内存拿去做文件缓存，free 常年只有几百 MB，
        #    那样恒接近 100%。联动内存里包含 Metal 常驻的模型权重，正是要看的量。
        mt_total = _sum(s2.get("node_memory_total_bytes", []))
        used = (_sum(s2.get("node_memory_wired_bytes", []))
                + _sum(s2.get("node_memory_compressed_bytes", []))
                + max(0.0, _sum(s2.get("node_memory_internal_bytes", []))
                      - _sum(s2.get("node_memory_purgeable_bytes", []))))
        mem = min(100.0, used / mt_total * 100) if mt_total else 0.0
    else:
        mem = 0.0

    def fs(s, key):
        return _sum(s.get(key, []), lambda l: l.get("mountpoint") == "/")
    dsz = fs(s2, "node_filesystem_size_bytes")
    dav = fs(s2, "node_filesystem_avail_bytes")
    disk = (1 - dav / dsz) * 100 if dsz else 0.0

    # macOS 的 utun(VPN)/awdl/llw/anpi/bridge 等是虚拟口，流量与 en0 重复计数。只对
    # darwin 追加，不改 Linux 节点的既有口径。
    skip = r"lo|docker|veth|br-" + (r"|utun|awdl|llw|anpi|bridge|gif|stf|ap\d" if darwin else "")

    def net(s, key):
        return _sum(s.get(key, []),
                    lambda l: not re.match(skip, l.get("device", "")))
    rx = (net(s2, "node_network_receive_bytes_total") -
          net(s1, "node_network_receive_bytes_total")) / span / 1024 / 1024
    tx = (net(s2, "node_network_transmit_bytes_total") -
          net(s1, "node_network_transmit_bytes_total")) / span / 1024 / 1024

    temps = _atlas_temps(s2)
    fans = _atlas_fans(s2)
    # ⛔ 没有 CPU 温度传感器时是 None, 不是 0 —— macOS 上 node_exporter 免 sudo
    # 拿不到温度(要 powermetrics), 给 0 就成了"实测 0 度"。(2026-09-19 w1W:p1 指出)
    cpu_t = next((t["celsius"] for t in temps if t["module"] == "CPU"), None)
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
        cpu_t = next((t["celsius"] for t in temps if t["module"] == "CPU"), None)
        # Node kind drives VRAM% interpretation:
        #   discrete       → DCGM FB_USED/FB_TOTAL (dedicated VRAM)
        #   unified-arm-soc / apple-silicon → node_exporter MemAvailable (shared)
        # The choice is by `kind` field on each node (config-driven), not by
        # any hard-coded host name.
        is_unified = KIND_BY_OBS.get(obs_node, "discrete") != "discrete"
        vram_pct = me.get(obs_node, 0) if is_unified else (fu.get(obs_node, 0) / vt * 100)
        # ⛔ 序列缺席 → None, 不是 0。原先一律 .get(node, 0):某个 exporter 掉了、
        # 某块卡没有该指标, 面板上都会变成"实测 0 W / 0 °C / 0% 利用率", 而不是
        # "没有数据"。(2026-09-19 与 mbp 的 tempCpu=0 同源, w1W:p1 指出)
        # ⚠️ 已知例外: 四台 Spark 的 DCGM_FI_DEV_MEMORY_TEMP 与 ECC SBE/DBE 是
        #    gb10-gpu-textfile.sh 为兼容 dcgm-exporter 而【写死的 0】(GB10 无显存
        #    温度传感器、LPDDR5X 无 ECC)。那是采集端的假 0, 这一层看不出来,
        #    要治得改那个脚本 —— 已列入待裁定。
        def _v(d, digits=1):
            x = d.get(obs_node)
            return None if x is None else round(x, digits)

        out[obs_node] = {
            "gpu": _v(g),
            "vram": round(vram_pct, 1) if (me.get(obs_node) is not None
                                           or fu.get(obs_node) is not None) else None,
            "vramKind": "unified" if is_unified else "discrete",
            "tempGpu": _v(gt),
            # 显存温度: 五张卡全是 0(2026-09-19 实测 DCGM_FI_DEV_MEMORY_TEMP)。
            # 4090 的 DCGM 对该字段不支持、GB10 那条是 textfile 脚本写死的兼容占位。
            # GPU 本体 29-60°C 时显存 0°C 物理上不可能 → 一律当"无该传感器"处理。
            "tempMem": (lambda x: None if x is None or x <= 0 else x)(_v(mt)),
            "tempCpu": cpu_t,
            "power": _v(pw),
            "cpu": _v(cp),
            "mem": _v(me),
            "disk": _v(dk),
            "netIn": _v(ni, 2),
            "netOut": _v(no, 2),
            "rdmaIn": _v(ir, 2),
            "rdmaOut": _v(it, 2),
            "temps": temps,
        }
    return out


# ── 慢变事实：逐挂载点存储 / 逐网卡链路 / 开机时长 / GPU 健康计数 ──────────
# 这些量分钟级才变，跟着 2.5s 的快照循环查纯属浪费 —— 单独 30s 缓存。
# ⛔ 全部来自【已有】的 node_exporter 与 DCGM 序列，不在被监控机上新增任何命令。
#    这是 2026-09-19 对照 sparkDash 时的判定: 它用 SSH 跑 lsblk/df/ip/journalctl 拿
#    同样的东西, 而本地 Prometheus 里现成就有(实测 node_filesystem_* 18 条、
#    node_network_speed_bytes 54 条、node_network_info 64 条含 MAC、XID/ECC 五台全有)。
_FACTS: dict = {"ts": 0.0, "data": {}}
_FACTS_TTL = 30.0
# 虚拟网卡/伪文件系统不显示: 它们既不是物理链路也不是节点自己的盘。
_NIC_SKIP = r"lo|br-.*|docker.*|veth.*|virbr.*|tun[0-9].*|tailscale.*|oray.*|wg[0-9].*"
_FS_SKIP = r"tmpfs|devtmpfs|overlay|squashfs|ramfs|efivarfs|nfs4|nfs|cifs|fuse.*"


async def _node_facts() -> dict[str, dict]:
    """obs 里各节点的慢变事实, 按 obs node label 归并。失败则返回上一份缓存。"""
    now = time.time()
    if _FACTS["data"] and now - _FACTS["ts"] < _FACTS_TTL:
        return _FACTS["data"]
    fs_sel = f'{{fstype!~"{_FS_SKIP}",mountpoint!~"/boot.*|/snap.*"}}'
    nic_sel = f'{{device!~"{_NIC_SKIP}"}}'
    (fs_sz, fs_av, d_rd, d_wr, nic_sp, nic_info, up, xid, sbe, dbe) = await asyncio.gather(
        promql(f"node_filesystem_size_bytes{fs_sel}"),
        promql(f"node_filesystem_avail_bytes{fs_sel}"),
        promql("rate(node_disk_read_bytes_total[2m])"),
        promql("rate(node_disk_written_bytes_total[2m])"),
        promql(f"node_network_speed_bytes{nic_sel}"),
        promql(f"node_network_info{nic_sel}"),
        promql("time() - node_boot_time_seconds"),
        # XID: Atlas 经 job=dcgm, 四台 Spark 经 job=node(2026-08-02 起 GB10 改用
        # node_exporter textfile collector 导出同名指标) —— 不要按 job 过滤。
        promql("DCGM_FI_DEV_XID_ERRORS"),
        promql("DCGM_FI_DEV_ECC_SBE_VOL_TOTAL"),
        promql("DCGM_FI_DEV_ECC_DBE_VOL_TOTAL"),
    )
    if not (fs_sz or up):                       # Prometheus 整体不可达 → 保留旧值
        return _FACTS["data"]

    def by_node_key(rows, key):
        out: dict[str, dict[str, float]] = {}
        for r in rows:
            nd, k = r["metric"].get("node"), r["metric"].get(key)
            if nd and k:
                out.setdefault(nd, {})[k] = r["value"]
        return out

    sz, av = by_node_key(fs_sz, "mountpoint"), by_node_key(fs_av, "mountpoint")
    rd, wr = by_node_key(d_rd, "device"), by_node_key(d_wr, "device")
    sp = by_node_key(nic_sp, "device")
    dev_of: dict[str, dict[str, str]] = {}      # node -> mountpoint -> 设备名
    for r in fs_sz:
        m = r["metric"]
        if m.get("node") and m.get("mountpoint"):
            dev_of.setdefault(m["node"], {})[m["mountpoint"]] = m.get("device", "—")
    mac: dict[str, dict[str, dict]] = {}
    for r in nic_info:
        m = r["metric"]
        if m.get("node") and m.get("device"):
            mac.setdefault(m["node"], {})[m["device"]] = {
                "mac": m.get("address", ""), "operstate": m.get("operstate", "")}
    upt = {r["metric"].get("node"): r["value"] for r in up if r["metric"].get("node")}

    def gpu_counter(rows):
        out: dict[str, float] = {}
        for r in rows:
            nd = r["metric"].get("node")
            if nd:                               # 多卡则取和
                out[nd] = out.get(nd, 0.0) + r["value"]
        return out

    xids, sbes, dbes = gpu_counter(xid), gpu_counter(sbe), gpu_counter(dbe)
    # XID 的 err_msg label 带人话描述, 非零时一并带出来供排障
    xid_msg = {r["metric"].get("node"): r["metric"].get("err_msg", "")
               for r in xid if r["value"] > 0 and r["metric"].get("node")}

    data: dict[str, dict] = {}
    for nd in set(sz) | set(sp) | set(upt) | set(xids):
        mounts = []
        for mp, total in sorted((sz.get(nd) or {}).items()):
            avail = (av.get(nd) or {}).get(mp, 0.0)
            mounts.append({"mount": mp, "device": (dev_of.get(nd) or {}).get(mp, "—"),
                           "totalGb": round(total / 2 ** 30, 1),
                           "availGb": round(avail / 2 ** 30, 1),
                           "usedPct": round((1 - avail / total) * 100, 1) if total else 0.0})
        nics = []
        for dv, speed in sorted((sp.get(nd) or {}).items()):
            info = (mac.get(nd) or {}).get(dv, {})
            nics.append({"name": dv,
                         # node_exporter 报的是 bytes/s, 链路速率习惯用 Mbps
                         "speedMbps": int(speed * 8 / 1e6) if speed > 0 else 0,
                         "mac": info.get("mac", ""), "state": info.get("operstate", "")})
        disks = []
        for dv in sorted(set(rd.get(nd) or {}) | set(wr.get(nd) or {})):
            disks.append({"device": dv,
                          "readMBs": round((rd.get(nd) or {}).get(dv, 0.0) / 2 ** 20, 2),
                          "writeMBs": round((wr.get(nd) or {}).get(dv, 0.0) / 2 ** 20, 2)})
        d: dict = {"mounts": mounts, "nics": nics, "disks": disks}
        if nd in upt:
            d["uptimeSec"] = int(upt[nd])
        if nd in xids:                           # 有 DCGM 源才给, 否则整块缺席
            # ⛔ ECC 两项【有序列才给】。四台 Spark 的 LPDDR5X 统一内存没有 ECC,
            # 2026-09-19 已把采集端写死的 0 删掉(机主裁定) —— 这里再 .get(nd, 0)
            # 就等于把采集端刚治好的假 0 在 API 层重新造一遍。
            d["gpuHealth"] = {"xid": int(xids[nd])}
            if nd in sbes:
                d["gpuHealth"]["eccSbe"] = int(sbes[nd])
            if nd in dbes:
                d["gpuHealth"]["eccDbe"] = int(dbes[nd])
            if xid_msg.get(nd):
                d["gpuHealth"]["xidMsg"] = xid_msg[nd]
        data[nd] = d
    _FACTS["data"], _FACTS["ts"] = data, now
    return data


# Apple Silicon 的 GPU 遥测:node_exporter 不导出, DCGM 更没有。唯一免 sudo 的来源是
# ioreg 的 PerformanceStatistics。温度与封装功耗仍拿不到(要 root 的 powermetrics),
# 保持缺席不伪造。
# 成本实测(2026-09-19 10:10, 被测机 MBP 侧 node_cpu_seconds_total 差分 A/B, 各 2 臂):
#   对照 291.7% / 探针 343.8% 单核当量, 60 次探针耗时 19.05s
#   → 0.165 CPU 秒/次(含 sshd 建会话, 大头在这里; ioreg 命令本身只有 0.01-0.02s)
#   → 30s 一次 = 单核 0.55%、10 核机 0.055%。
# ⚠️ macOS 没有 cgroup, 这里用 node_exporter 的全核忙碌计数器代替 cgroup A/B;
#    背景噪声约 ±8 个百分点(两个对照臂之差), 远小于 52 个百分点的探针增量。
_GPU_PROBE: dict = {"ts": 0.0, "data": {}}
_GPU_PROBE_TTL = 30.0
_DARWIN_GPU_CMD = (
    "ioreg -r -d 1 -c IOAccelerator 2>/dev/null | "
    "grep -Eo '\"(Device Utilization %|In use system memory|Alloc system memory)\"=[0-9]+' | head -6; "
    "echo ---; sysctl -n hw.memsize")


async def _darwin_gpu(host: str) -> dict:
    """ssh <host> ioreg → {gpu%, vramUsedGb, vramTotalGb, vramPct}。失败返回 {}。"""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=4",
            "-o", "IdentitiesOnly=yes", "-i", os.path.expanduser("~/.ssh/id_ed25519"),
            host, _DARWIN_GPU_CMD,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=8.0)
    except Exception:
        return {}
    txt = out.decode(errors="replace")
    stats, _, memtxt = txt.partition("---")

    def field(label: str):
        m = re.search(rf'"{re.escape(label)}"=(\d+)', stats)
        return float(m.group(1)) if m else None

    util, alloc, inuse = (field("Device Utilization %"), field("Alloc system memory"),
                          field("In use system memory"))
    try:
        total = float(memtxt.strip().split()[0])
    except (ValueError, IndexError):
        total = 0.0
    # Apple 没有独立显存:显存总量就是系统内存; 已用取 Alloc(含已提交未驻留), 回落 In use
    used = alloc if (alloc or 0) > 0 else (inuse or 0.0)
    d: dict = {}
    if util is not None:
        d["gpu"] = round(util, 1)
    if total > 0:
        d["vramUsedGb"] = round(used / 2 ** 30, 2)
        d["vramTotalGb"] = round(total / 2 ** 30, 1)
        d["vram"] = round(used / total * 100, 1)
    return d


async def _gpu_probes() -> dict[str, dict]:
    """按节点跑 GPU 探针(目前只有 Apple Silicon 这一种)。30s 缓存, 不进 2.5s 快照循环。"""
    now = time.time()
    if _GPU_PROBE["data"] and now - _GPU_PROBE["ts"] < _GPU_PROBE_TTL:
        return _GPU_PROBE["data"]
    targets = [(n["id"], n["gpu_probe_ssh"]) for n in NODES if n.get("gpu_probe_ssh")]
    if not targets:
        return {}
    res = await asyncio.gather(*[_darwin_gpu(h) for _, h in targets])
    data = {nid: r for (nid, _), r in zip(targets, res) if r}
    if data:                                  # 全失败时保留上一份, 不闪回 0
        _GPU_PROBE["data"], _GPU_PROBE["ts"] = data, now
    return _GPU_PROBE["data"]


# ── 节点吞吐归属 ────────────────────────────────────────────────────
# 2026-09-19 机主反馈: node card 上看不到 tok/s 与 prefill。吞吐本来只按模型算
# (/api/models), 节点侧没有。这里做【模型 → 节点】的归属, 口径与首屏那块完全同源
# (都读 models 的 live.tps / live.prefillTokPerS), 不引第二套算法。
#
# ⛔ 归属规则的要害: TP 组整组只产出一份吞吐。deepseek-v41-flash 跨四台 Spark,
#    若四张卡各写 91 tok/s, 看起来就是 364。所以只有【真正对外提供 API 的那台】
#    显示数字(apiNodes, 由 /v1/models 实测判定), 其余成员标成 worker + 所属模型。
# ⛔ 空闲显示 0 而不是隐藏整行 —— 卡片要能回答"这台此刻在不在出 token"。
#    但【没有任何模型归属】的节点不渲染这两行: 那种情况下 0 是假数, 不是"空闲"。
_MODELS_LAST: dict = {"ts": 0.0, "data": []}


def _attach_node_throughput(nodes: list, models: list) -> None:
    """把 models 的实时吞吐按节点归属写进 nodes[*]['live']。就地修改。"""
    agg: dict = {}
    for m in models or []:
        lv = m.get("live") or {}
        if (lv.get("metrics") or "none") == "none":
            continue                     # 没有实时指标源的模型不参与
        span = m.get("nodes") or []
        api = set(m.get("apiNodes") or [])
        name = lv.get("loadedModel") or m.get("servedName") or m.get("display") or m.get("id")
        for nid in span:
            d = agg.setdefault(nid, {"role": None, "models": [], "tps": None, "pf": None,
                                     "pfSource": None})
            if name not in d["models"]:
                d["models"].append(name)
            if api and nid not in api:
                # TP/PP 成员但不对外提供 API → 只标归属, 不给数字
                d["role"] = d["role"] or "worker"
                continue
            d["role"] = "api"
            d["tps"] = (d["tps"] or 0.0) + float(lv.get("tps") or 0.0)
            _pf = lv.get("prefillTokPerS")
            if _pf is None:
                # 该后端没有实时 prefill 源(oMLX): 标明无源, 不拿 0 顶替
                d["pfSource"] = d["pfSource"] or lv.get("prefillSource") or "none"
            else:
                d["pf"] = (d["pf"] or 0.0) + float(_pf)
                d["pfSource"] = "window"
    for n in nodes:
        d = agg.get(n["id"])
        if not d or not d["role"]:
            continue
        live = n.setdefault("live", {})
        live["throughputRole"] = d["role"]
        live["throughputModels"] = d["models"]
        if d["role"] == "api":
            live["decodeTps"] = round(d["tps"] or 0.0, 1)
            live["prefillTokPerS"] = None if d["pf"] is None else round(d["pf"], 0)
            live["prefillSource"] = d["pfSource"] or "window"


async def _node_payload() -> list[dict]:
    exp_nodes = [n for n in NODES if n.get("node_source") == "exporter"]
    obs_live, direct, facts, gpu_probe, *exp_lives = await asyncio.gather(
        _obs_node_live(), _atlas_node_live(), _node_facts(), _gpu_probes(),
        *[_atlas_node_live(n["exporter_url"], NODEEXP_NODE_TIMEOUT) for n in exp_nodes])
    exp_live = {n["id"]: lv for n, lv in zip(exp_nodes, exp_lives)}
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
        if n.get("node_source") == "exporter":
            # 按节点直采:只有 CPU/内存/磁盘/网络(+传感器,有才有)。没有任何 GPU 遥测源
            # (无 DCGM;Apple GPU 利用率/功耗 node_exporter 不导出) → GPU 字段保持缺省,
            # 由顶层 gpuTelemetry=False 让前端显示「—」,而不是「0 W / 0 °C」。
            d = exp_live.get(n["id"]) or {}
            live.update({k: d[k] for k in ("cpu", "mem", "disk", "netIn", "netOut",
                                           "tempCpu", "temps", "fans") if k in d})
            if n.get("kind") != "discrete" and d:
                # 统一内存:显存占用就是内存占用(与 GB10 的 unified 同口径)
                live["vramKind"] = "unified"
                live["vram"] = d.get("mem", 0)
            # Apple Silicon 的 GPU 利用率与显存:来自 ioreg 探针(30s)。拿到后显存改用
            # 【GPU 实际分配量】而不是整机内存占用 —— 两者在统一内存上不是一回事。
            # ⛔ 仍然只有利用率与显存:温度/功耗要 root 才拿得到, 继续缺席(gpuTelemetry
            #    保持 false, 面板上功耗与 GPU 温度仍显示「—」)。
            gp = gpu_probe.get(n["id"]) or {}
            if gp:
                live.update({k: gp[k] for k in ("gpu", "vram") if k in gp})
                live["vramKind"] = "unified"
                live["gpuUtilSource"] = "ioreg"
                if "vramUsedGb" in gp:
                    live["vramUsedGb"], live["vramTotalGb"] = gp["vramUsedGb"], gp["vramTotalGb"]
            # ⛔ 没有来源的遥测字段一律 null, 不留默认 0。
            # 界面靠 gpuTelemetry=false 显示「—」只遮住了 UI 这一层:载荷里的 0
            # 会被告警规则 / 导出 / 第三方脚本读成「实测 0 瓦 / 0 度」。
            # (2026-09-19 w1W:p1 提醒, 与 tpsNow 恒 0 是同一类坑)
            for k in ("tempGpu", "tempMem", "power", "rdmaIn", "rdmaOut"):
                live[k] = None
            for k in ("cpu", "mem", "disk", "netIn", "netOut", "tempCpu"):
                if k not in d:
                    live[k] = None
            if "gpu" not in gp:
                live["gpu"] = None
            if "vram" not in gp and not (n.get("kind") != "discrete" and d):
                live["vram"] = None
            up = bool(d)
        elif n.get("node_source") == "direct":
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
        # 慢变事实(30s 缓存)。按 obs label 取 —— 没有 obs 覆盖的节点(如经隧道直采的
        # MBP)这里就是空 dict, 前端相应整块不渲染, 不造假。
        f = facts.get(n["obs_node"] or "") or {}
        out.append({**{k: v for k, v in n.items() if k not in ("node_source", "exporter_url")},
                    "gpuTelemetry": n.get("node_source") != "exporter",
                    "facts": f,
                    "live": live, "up": up})
    return out


@app.get("/api/nodes")
async def nodes_list():
    nodes = await _node_payload()
    # 吞吐用最近一轮 models 缓存(见 _attach_node_throughput 上方注释)。缓存空
    # (进程刚起、还没跑过 models_list)→ 这两行字段整组缺席, 卡片不渲染, 不填 0。
    _attach_node_throughput(nodes, _MODELS_LAST["data"])
    return nodes


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
        # ⛔ 指标名必须带 _total:LiteLLM 用 prometheus_client 的 Counter,导出名是
        # litellm_total_tokens_metric_total。少了后缀 → 查不到序列 → tpsNow/rpsNow 恒 0。
        # 2026-09-19 实测:抓取任务此前还因 401 down 着,两层问题叠在一起,界面看不出来
        # (前端用各模型吞吐自行重算了集群值,见 data.js 的 cluster tps 段)。
        promql("sum(rate(litellm_total_tokens_metric_total[1m]))"),
        promql("sum(rate(litellm_proxy_total_requests_metric_total[1m]))"),
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
        promql_range("sum(rate(litellm_total_tokens_metric_total[1m]))"),
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
# ⛔ HA 导出器在 HA 不可达时【仍然导出 0】而不是把 series 撤掉 ——
#    实测 2026-09-19 10:19: ha_up=0, 但 ha_rack_ac_power_watts=0 每 15s 照常写入,
#    30d 内 max 也是 0。这等于"拿 0 冒充没数据", 跟机主 09-19 点名的那类坑同源。
#    HA 侧不归我们动(机主 09-19: "暂不管, 不要动 HA"), 所以在 Hearth 这一层挡:
#    ha_up != 1 时, 一切 HA 派生字段一律按【无数据】处理, 置 null / available=false。
async def _ha_ok() -> bool:
    r = await promql("ha_up")
    return bool(r) and float(r[0]["value"]) > 0


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
    # ── GPU 侧能耗(2026-09-19 加)────────────────────────────────────
    # ⛔ 口径: 只有 GPU, 不含 CPU/内存/风扇/电源损耗 —— 不是整机功耗, 更不是电表读数。
    #    机主 09-19 裁定: HA 智能插座(整机口径)全部 unavailable 期间, 能耗一律走
    #    DCGM 的 GPU 侧, 并在界面上明标不含整机。
    # 两个口径都算, 由覆盖率决定用哪个, 结果里带 source 字段说明用的是哪一个:
    #   1) 计数器 increase(DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION) —— 精确, 但
    #      这条 series 2026-09-19 才铺开(Atlas 09:47 / 四台 Spark 10:14), 窗口没填满前
    #      只代表"开始计数以来", 会系统性偏小。
    #   2) 功率积分 sum_over_time(POWER_USAGE)*15s —— 有 30d 历史, 但受 15s 采样
    #      粒度限制, 漏掉采样间隔内的尖峰。
    # 覆盖率 >= 98% 用 1), 否则用 2) —— 计数器攒满 24h/30d 后自动切换, 无需改代码。
    gpu, eff, per_node_w, per_node_24h, per_node_30d, \
        g_ctr_d, g_ctr_m, g_int_d, g_int_m, g_cov_d, g_cov_m, g_node_d = await asyncio.gather(
        promql("sum(DCGM_FI_DEV_POWER_USAGE)"),
        promql("sum(rate(litellm_total_tokens_metric_total[1m])) "
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
        promql("sum(increase(DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION[24h])) / 3.6e9"),
        promql("sum(increase(DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION[30d])) / 3.6e9"),
        promql("sum(sum_over_time(DCGM_FI_DEV_POWER_USAGE[24h])) * 15 / 3600 / 1000"),
        promql("sum(sum_over_time(DCGM_FI_DEV_POWER_USAGE[30d])) * 15 / 3600 / 1000"),
        # 覆盖率 = 窗口内真实有数据的采样点 / 窗口应有的点数。子查询步长取
        # 1m/5m 只是为了便宜, 不影响判定(判的是"这条 series 存在多久", 不是精度)。
        promql("count_over_time(sum(DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION)[24h:1m]) / 1440"),
        promql("count_over_time(sum(DCGM_FI_DEV_TOTAL_ENERGY_CONSUMPTION)[30d:5m]) / 8640"),
        promql("sum_over_time(DCGM_FI_DEV_POWER_USAGE[24h]) * 15 / 3600 / 1000"),
    )
    ha_ok = await _ha_ok()
    # HA 挂着的时候这三条全是导出器写的假 0 —— 直接当空处理, 下面的 sum() 就会给 None。
    if not ha_ok:
        per_node_w = per_node_24h = per_node_30d = []
        eff = []
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

    def _gpu_kwh(ctr, integral, cover):
        """→ (kWh, 口径, 覆盖率)。两个都取不到就是 (None, None, None) —— 不填 0。"""
        c, i, cv = _f(ctr, 3), _f(integral, 3), _f(cover, 3)
        if c is not None and cv is not None and cv >= 0.98:
            return c, "dcgm-counter", cv
        if i is not None:
            return i, "dcgm-power-integral", cv
        return (c, "dcgm-counter", cv) if c is not None else (None, None, cv)

    g_kwh_d, g_src_d, g_cv_d = _gpu_kwh(g_ctr_d, g_int_d, g_cov_d)
    g_kwh_m, g_src_m, g_cv_m = _gpu_kwh(g_ctr_m, g_int_m, g_cov_m)
    by_node_gpu_d = {k: round(float(v), 3) for k, v in _by(g_node_d, "node").items()}
    return {"wallW": wall_total, "gpuW": _f(gpu), "tokensPerW": _f(eff, 2),
            "kwh24h": kwh_d_tot, "kwh30d": kwh_m_tot,
            "byNode": by_node, "byNode24h": by_node_d, "byNode30d": by_node_m,
            # GPU 侧能耗。scope 字段是给界面用的:必须标"不含整机"。
            "gpuKwh24h": g_kwh_d, "gpuKwh30d": g_kwh_m,
            "gpuKwhSource24h": g_src_d, "gpuKwhSource30d": g_src_m,
            "gpuKwhCoverage24h": g_cv_d, "gpuKwhCoverage30d": g_cv_m,
            "byNodeGpuKwh24h": by_node_gpu_d,
            "gpuEnergyScope": "gpu-only",
            # 整机口径当前是否可信。false 时上面 wallW/kwh24h/kwh30d/byNode* 全是 null。
            "wallAvailable": ha_ok,
            "wallUnavailableReason": None if ha_ok else "HA exporter down (ha_up=0)"}


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
    if not await _ha_ok():
        t = h = ac_w = ac_24h = ac_30d = ac_s = plug_temps = []
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
            "byNodePlugTempC": by_node_plug, "cabinetHeatProxyC": proxy,
            "haAvailable": bool(by_node_plug) or _f(t) is not None}


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
    # GLM: high/low/max 三个网关路由指向【同一个 vLLM 实例】
    # (2026-09-08 实测 10.0.0.23:8000, 网关 alias 串同时挂
    # agent/code/default/long), 差别只在 hook 注入的 think 档位。
    # ⛔ 主名恒为 -think-high 是 _primary() "取最长路由" 的副产物, 不是"当前
    # 部署了 High 档": 后端 served-name 是不带档位的 glm-5.3-flash, 精确匹配落
    # 空后走前缀容错, 而 -high(24 字符) 比 -low/-max(23) 长一位。所以主名这条
    # display 【不许写档位】, 否则就是 2026-08-04 DeepSeek 那次误判的复刻。
    "glm-5.3-flash-think-high": {"display": "GLM-5.3-Flash",
        "vendor": "Zhipu", "kind": "chat", "tags": ["reasoning", "think"]},
    # 下面两条正常只作为 alias 出现; 一旦哪天被选成主名, 也不会被 .title()
    # 推导成看起来像独立部署的 "Glm 5.3 Flash Think Low"。
    "glm-5.3-flash-think-low": {"display": "GLM-5.3-Flash · Think 低档",
        "vendor": "Zhipu", "kind": "chat", "tags": ["reasoning", "think"]},
    "glm-5.3-flash-think-max": {"display": "GLM-5.3-Flash · Think 极限档",
        "vendor": "Zhipu", "kind": "chat", "tags": ["reasoning", "think"]},
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
              else "Zhipu" if "glm" in low
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


# 直探声明里被跳过的地址 → 原因。给 /api/selftest 用: "声明了却没出现在面板上"
# 必须能查到为什么, 否则运维会以为 Hearth 漏了。
_DIRECT_SKIPPED: dict[str, str] = {}


async def _classify_base(b: str) -> str:
    """探一个 base 是哪种引擎 → vllm/llamacpp/sglang/omlx/ds4/q27/exl3/none。

    顺序即优先级: 前四种是本集群在跑的, 放前面; 后三种(ds4/q27/exl3)本集群
    当前没有实例, 放链尾, 不给生产路径增加无谓请求。
    none = 这个地址不响应任何已知引擎的指标口径(可能是没起来, 也可能是别的服务)。"""
    sc = await _scrape_vllm(b)
    if any(str(k).startswith("vllm:") for k in sc) or sc.get("__e2e_buckets"):
        return "vllm"
    if await _scrape_llamacpp(b):
        return "llamacpp"
    sc3 = await _scrape_sglang(b)
    if any(str(k).startswith("sglang:") for k in sc3) or sc3.get("__e2e_buckets"):
        return "sglang"
    if await _scrape_omlx(b):
        return "omlx"
    if await _scrape_ds4(b):
        return "ds4"
    if await _scrape_q27(b):
        return "q27"
    if await _scrape_exl3(b):
        return "exl3"
    return "none"


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
    known_bases: set = set()        # 已被网关条目占用的 base, 给下面的直探去重用
    for b, routes in base_routes.items():
        prt, verified, cands = _primary(routes, served.get(b, ""))
        meta = _meta_for(prt)
        mm = models.setdefault(prt, {
            "id": prt, "route": f"litellm/{prt}", "source": "gateway",
            "display": meta["display"],
            "vendor": meta["vendor"], "kind": meta["kind"],
            "tags": list(meta["tags"]), "params": "—", "quant": "—",
            "framework": "—", "vram": 0, "ctx": 0,
            "identityUnverified": False, "identityCandidates": [],
            "servedName": "",
            "_nodes": set(), "_aliases": set(), "_bases": [], "_api_nodes": set(),
            "up": False, "vllm_bases": [], "llamacpp_bases": [], "sglang_bases": [],
            "omlx_bases": [], "ds4_bases": [], "q27_bases": [], "exl3_bases": []})
        # 后端自报的 served-model-name。网关路由可能是"档位别名"(DGX-Spark-auto),
        # 而后端实际加载的是 deepseek-v41-flash —— 两者都要留着, 面板上才说得清
        # "你调的是哪条路由 / 它背后是什么模型"。
        if served.get(b) and not mm.get("servedName"):
            mm["servedName"] = served[b]
        # 真正对外提供 OpenAI API 的是哪台: /v1/models 有 data 才算(_served_name
        # 拿得到 id 就意味着 200 且有模型)。⛔ 判据用实测不用配置 —— 四台 Spark 的
        # 8004 全都导出 sglang:* 指标, 但只有 rank0(.188) 的 /v1/models 是 200,
        # 另外三台 404。吞吐是【整个 TP 组产出一份】, 四台各显示一遍会被读成四倍。
        if served.get(b):
            _h = _host_of(b)
            if _h in IP_TO_ID:
                mm["_api_nodes"].add(IP_TO_ID[_h])
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
    # 运维声明的引擎端点(model_topology.<id>.metrics_bases)。网关下一跳不一定是引擎:
    # 2026-09-17 起 DGX-Spark-* 走 LiteLLM → 本机 QoS 调度器(127.0.0.1:8018) → 两个
    # vLLM 池，调度器的 /metrics 只有 qos_* 排队量，也不对外暴露池地址 —— 从网关
    # api_base 自动发现永远摸不到引擎，只能在配置里声明。
    # 只【追加】、不替换：声明的端点挂了(试验容器撤掉)只是分类不上，不会把模型判成
    # down，也不会压掉网关那条 base。不参与上面的 _served_name/_primary —— 身份仍以
    # 网关 base 为准(引擎自报名带 -exl3-trial 之类后缀，匹配不上任何路由)。
    # 多个池走 _merge_scrape 按多副本求和：前提是【一条请求只落在一个池】(调度器
    # elastic-isolated 模式即如此)。若调度器改成 prefill/decode 拆分(strict-pd/hybrid,
    # 同一请求两个池都记一遍)，求和会重复计数，这里必须重新设计。
    for mm in models.values():
        topo = (HEARTH_CFG.get("model_topology") or {}).get(mm["id"]) or {}
        for raw in topo.get("metrics_bases") or []:
            b = str(raw).rstrip("/")
            b = b[:-3] if b.endswith("/v1") else b
            if b in mm["_bases"]:
                continue
            mm["_bases"].append(b)
            host = _host_of(b)
            if host in IP_TO_ID:
                mm["_nodes"].add(IP_TO_ID[host])
    # up 判定:直接探后端为主(/metrics 或 /v1/models 可达即活),/health 仅做
    # 辅助 / 兜底——避免单点故障(网关 /health 偶发超时 22s)把所有模型误标
    # stopped。直接探测自给自足,网关挂了监控仍如实反映后端真相。
    _KIND_BASES = {"vllm": "vllm_bases", "llamacpp": "llamacpp_bases",
                   "sglang": "sglang_bases", "omlx": "omlx_bases",
                   "ds4": "ds4_bases", "q27": "q27_bases", "exl3": "exl3_bases"}
    for mm in models.values():
        for b in mm["_bases"]:
            kind = await _classify_base(b)
            if kind != "none":
                mm[_KIND_BASES[kind]].append(b); mm["up"] = True
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
        elif mm["omlx_bases"]:
            mm["framework"] = "oMLX"
            mm["ctx"] = await _ctx_of(mm["omlx_bases"][0])
        elif mm["ds4_bases"]:
            mm["framework"] = "ds4-server"
            mm["ctx"] = await _ctx_of(mm["ds4_bases"][0])
        elif mm["q27_bases"]:
            mm["framework"] = "q27"
            mm["ctx"] = await _ctx_of(mm["q27_bases"][0])
        elif mm["exl3_bases"]:
            mm["framework"] = "EXL3"
            mm["ctx"] = await _ctx_of(mm["exl3_bases"][0])
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
        known_bases.update(mm.pop("_bases", []))
        _api = mm.pop("_api_nodes", set())
        # 网关下一跳是回环地址(经隧道的 MBP)时映射不到节点 → 单节点模型兜底认它自己;
        # 多节点(TP 组)且判不出 API 在哪时宁可【都不标】, 也不四张卡各显示一遍。
        if not _api and len(mm["nodes"]) == 1:
            _api = set(mm["nodes"])
        mm["apiNodes"] = sorted(_api)

    # ── 未挂网关的引擎(配置里声明要直探的地址)────────────────────────
    # 冲突 3 的解法: 【以 base URL 为主键去重】。网关已经覆盖的地址在这里跳过,
    # 否则同一个引擎会在面板上出现两次(一条带 route, 一条不带)。
    # 这些条目 source=direct、route=None —— 没挂网关就是没挂, 不编一个路由出来。
    # 声明的地址探不通【也要显示】(up=false): 运维声明了却没起来, 正是要看见的事。
    for raw in (HEARTH_CFG.get("direct_engines") or []):
        if isinstance(raw, dict):
            b = str(raw.get("base") or "").rstrip("/")
            want_id, want_node = raw.get("id"), raw.get("node")
        else:
            b, want_id, want_node = str(raw).rstrip("/"), None, None
        if not b:
            continue
        if b.endswith("/v1"):
            b = b[:-3]
        if b in known_bases:
            continue
        kind = await _classify_base(b)
        # ⛔ 有 /metrics ≠ 是一个可服务的端点。2026-09-19 实测: 四台 Spark 的
        #    TP worker(.189:8004 等)也导出 183 行 sglang:* (每阶段延迟/排队/KV),
        #    但没有 generation_tokens_total、/v1/models 返回 404 —— 它们是同一个
        #    部署的分片, 不是独立模型。按 /v1/models 是否 200 来分辨:
        #      200        → 独立服务端点, 建卡
        #      404/其它   → 分片或别的服务, 【不建卡】, 只记进自检结果说明原因
        #      连不上     → 声明了却没起来, 建卡并如实标 offline
        status, reachable = None, False
        try:
            rr = await client.get(f"{b}/v1/models", timeout=3.0)
            status, reachable = rr.status_code, True
        except Exception:
            pass
        if reachable and status != 200:
            _DIRECT_SKIPPED[b] = (f"/v1/models HTTP {status} —— 有 {kind} 指标但不是"
                                  f"独立服务端点(多半是 TP/PP 分片), 不建模型卡")
            known_bases.add(b)
            continue
        _DIRECT_SKIPPED.pop(b, None)
        sv = await _served_name(b)
        mid = str(want_id or sv or f"direct:{_host_of(b)}")
        if mid in models:                       # 主名撞车: 退回地址做 id, 不覆盖网关条目
            mid = f"direct:{_host_of(b)}"
        meta = _meta_for(mid)
        nodes = set()
        host = _host_of(b)
        if want_node:
            nodes.add(str(want_node))
        elif host in IP_TO_ID:
            nodes.add(IP_TO_ID[host])
        entry = {"id": mid, "route": None, "source": "direct",
                 "display": meta["display"], "vendor": meta["vendor"],
                 "kind": meta["kind"], "tags": list(meta["tags"]) + ["direct"],
                 "params": "—", "quant": "—", "framework": "—", "vram": 0, "ctx": 0,
                 "identityUnverified": not bool(sv), "identityCandidates": [],
                 "servedName": sv or "",
                 "apiNodes": sorted(nodes) if sv else [],
                 "nodes": sorted(nodes), "up": kind != "none" or status == 200,
                 "vllm_bases": [], "llamacpp_bases": [], "sglang_bases": [],
                 "omlx_bases": [], "ds4_bases": [], "q27_bases": [], "exl3_bases": []}
        if kind != "none":
            entry[_KIND_BASES[kind]].append(b)
            entry["framework"] = {"vllm": "vLLM", "llamacpp": "llama.cpp",
                                  "sglang": "SGLang", "omlx": "oMLX",
                                  "ds4": "ds4-server", "q27": "q27",
                                  "exl3": "EXL3"}[kind]
            entry["ctx"] = await _ctx_of(b)
        models[mid] = entry
        known_bases.add(b)
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
    # 2026-09-18 补采:以下 llama.cpp 一直在导出,只是一直没接 —— 面板因此缺了
    # prefill 吞吐 / 缓存命中 / 投机解码三块。
    "llamacpp:prompt_tokens_cached_total",
    "llamacpp:spec_decode_num_drafts_total",
    "llamacpp:spec_decode_num_draft_tokens_total",
    "llamacpp:spec_decode_num_accepted_tokens_total",
}


async def _scrape_llamacpp(base: str) -> dict:
    """直采 llama.cpp 原生 /metrics（与 vLLM 同 Prometheus 文本格式，前缀
    llamacpp:）。返回所需标量 + 投机解码每位置接受计数。无 e2e/TTFT 直方图
    （llama.cpp 不暴露）→ 分位数/TTFT 一律【缺席】, 不伪造也不填 0。"""
    out: dict[str, float] = {}
    spec_pos: dict[str, float] = {}
    try:
        r = await client.get(f"{base}/metrics", timeout=4.0)
        r.raise_for_status()
        for line in r.text.splitlines():
            if not line or line[0] == "#":
                continue
            sp = line.rsplit(" ", 1)
            if len(sp) != 2:
                continue
            head = sp[0]
            name = head.split("{")[0]
            try:
                v = float(sp[1])
            except ValueError:
                continue
            if name == "llamacpp:spec_decode_num_accepted_tokens_per_pos_total":
                mp = re.search(r'position="([^"]+)"', head)
                if mp:
                    spec_pos[mp.group(1)] = spec_pos.get(mp.group(1), 0.0) + v
            elif name in _LLAMACPP_SCALARS:
                out[name] = out.get(name, 0.0) + v
    except Exception:
        return {}
    # ⛔ 只在真有 llamacpp:* 标量时才附 __spec_pos:发现阶段用 `if sc2:` 判定这个
    #    endpoint 是不是 llama.cpp,无条件塞一个键会把任何 endpoint 认成 llama.cpp。
    if out and spec_pos:
        out["__spec_pos"] = spec_pos
    return out


_SGLANG_SCALARS = {
    "sglang:num_running_reqs", "sglang:num_queue_reqs",
    "sglang:gen_throughput",
    "sglang:prompt_tokens_total", "sglang:generation_tokens_total",
    "sglang:time_to_first_token_seconds_sum", "sglang:time_to_first_token_seconds_count",
    "sglang:inter_token_latency_seconds_sum", "sglang:inter_token_latency_seconds_count",
    "sglang:token_usage",
    "sglang:e2e_request_latency_seconds_sum", "sglang:e2e_request_latency_seconds_count",
    "sglang:queue_time_seconds_sum", "sglang:queue_time_seconds_count",
}


# 口径已在 live SGLang（10.0.0.23:8004，DSPARK 投机解码）对照源码核过：
#   queue   observe_queue_time(forward_entry - wait_queue_entry)，纯调度排队，
#           与 vllm:request_queue_time_seconds 同义。
#   tpot    ⚠️ 源头叫 inter_token_latency，但【不是】vLLM 那种原始到达间隔：
#           metrics_collector.py observe_inter_token_latency 把一个输出块的间隔
#           除以 num_new_tokens，再按 num_new_tokens 计入桶 —— 每个 token 记一次
#           块内均摊耗时。所以 count ≈ decode token 数(实测 937431 vs 944469)，
#           是【按 token 加权】的每 token 耗时分布，形状是 TPOT 不是 ITL。
#           投机解码"一批近 0 间隔 + 少量长间隔"的特征被均摊抹掉了，拿它填 ITL
#           行就是冒充。ITL 行对 SGLang 保持缺席。
#   e2e / ttft 按 is_streaming 分两条序列，累积桶逐 le 相加仍是合法直方图。
# per_stage_req_latency_seconds 另走下面的 stage 过滤，不在这张表里。
_SGLANG_HISTS = {
    "sglang:e2e_request_latency_seconds_bucket": "__e2e_buckets",
    "sglang:time_to_first_token_seconds_bucket": "__ttft_buckets",
    "sglang:inter_token_latency_seconds_bucket": "__tpot_buckets",
    "sglang:queue_time_seconds_bucket": "__queue_buckets",
}
# prefill 耗时 = per_stage_req_latency_seconds{stage="prefill_forward"}：
# req_time_stats.py set_prefill_finished_time 记的是 last_forward_entry_time →
# prefill 完成，而 last_forward_entry_time 只在首次进 forward(或 retract 后)才写，
# 所以【已覆盖全部 chunk】，与 vllm:request_prefill_time_seconds 同义。
# ⛔ 同一族里还有两个 stage，都不能混进来：
#   chunked_prefill  同一段时间按 chunk 切出来的子片(实测 count 532 < 请求数)，加上就重复计
#   request_process  tokenizer→scheduler 入队那一跳(均值 1.6ms)，不是 prefill
# ⛔ 所以 _sum/_count 也不能走 _SGLANG_SCALARS 按裸名累加 —— 那会把三个 stage 加在一起。
# decode 没有对应量：DECODE_LOOP 未设 metrics_is_observed，/metrics 里根本没有。
_SGLANG_PREFILL_STAGE = re.compile(r'(?:\{|,)stage="prefill_forward"(?:,|\})')
_SGLANG_PREFILL = "__sglang_prefill_seconds"     # _lat_snapshot 会拼 _sum / _count


async def _scrape_sglang(base: str) -> dict:
    """直采 SGLang 原生 /metrics（需启动加 --enable-metrics；前缀 sglang:）。
    含 TTFT / 每 token 耗时 / e2e / 排队 / 分阶段直方图，接近 vLLM。

    优先用实时 decode counter 计算生成中的速度；旧版无该指标时回退到
    generation_tokens_total。prefill_compute / prefill_cache 不计入生成速度。

    queue / per_stage 带 tp_rank 标签。默认只有 tp_rank 0 上报
    (enable_metrics_for_all_schedulers=False)，按标签相加正确；若开了该开关且是
    纯 TP，各 rank 会重复记同一批请求 —— 均值与分位数不变，样本数会虚高 N 倍。"""
    out: dict[str, float] = {}
    hists: dict[str, dict[str, float]] = {k: {} for k in _SGLANG_HISTS.values()}
    hists["__prefill_buckets"] = {}
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
            hkey = _SGLANG_HISTS.get(name)
            if name == "sglang:realtime_tokens_total":
                mm = re.search(r'(?:\{|,)mode="([^"]+)"(?:,|})', head)
                if mm and mm.group(1) == "decode":
                    key = "__sglang_decode_tokens_total"
                    out[key] = out.get(key, 0.0) + v
                elif mm and mm.group(1) == "prefill_compute":
                    # 实算 prefill token，已排除 prefix cache 命中(命中的记在
                    # prefill_cache)，与 vLLM 那边 iteration_sum - generation 同口径
                    key = "__sglang_prefill_compute_tokens_total"
                    out[key] = out.get(key, 0.0) + v
            elif hkey:
                mle = re.search(r'le="([^"]+)"', head)
                if mle:
                    hb = hists[hkey]
                    hb[mle.group(1)] = hb.get(mle.group(1), 0.0) + v
            elif name.startswith("sglang:per_stage_req_latency_seconds_"):
                if not _SGLANG_PREFILL_STAGE.search(head):
                    continue
                suf = name[len("sglang:per_stage_req_latency_seconds"):]
                if suf == "_bucket":
                    mle = re.search(r'le="([^"]+)"', head)
                    if mle:
                        hb = hists["__prefill_buckets"]
                        hb[mle.group(1)] = hb.get(mle.group(1), 0.0) + v
                elif suf in ("_sum", "_count"):
                    key = _SGLANG_PREFILL + suf
                    out[key] = out.get(key, 0.0) + v
            elif name in _SGLANG_SCALARS:
                out[name] = out.get(name, 0.0) + v
    except Exception:
        return {}
    if "__sglang_decode_tokens_total" not in out and "sglang:generation_tokens_total" in out:
        out["__sglang_decode_tokens_total"] = out["sglang:generation_tokens_total"]
    out.update(hists)
    return out


# oMLX(Apple Silicon 上的 MLX 推理服务, MBP 2026-09-17 起用它替掉 llama-server)。
# ⛔ 它【没有】Prometheus 端点:/metrics 返回 404、/admin/api/stats 要登录(401)。
# 免鉴权的 /api/status 是唯一能拿到运行数据的地方,给的是累计计数器 + 自报均值。
# 因此这个后端只能出吞吐/请求率/并发,没有 TTFT/TPOT/延迟分位数/KV 池用量。
_OMLX_KEYS = ("total_requests", "active_requests", "waiting_requests",
              "total_prompt_tokens", "total_completion_tokens", "total_cached_tokens",
              "cache_efficiency", "avg_prefill_tps", "avg_generation_tps",
              "models_loaded", "uptime_seconds",
              "model_memory_used")        # 权重常驻字节数(oMLX 自报)


# oMLX 速率用【滑动窗口】而不是 _tps_rollup 那种重置式锚点:重置式每出一次数就把
# 锚点归零,接下来十几秒没有长窗口值可用,读数会在真值和 0/尖峰之间来回跳
# (2026-09-18 实测:14.4 → 0 → 13.3 → 119.2 → 16.6)。滑动窗口始终以窗口内最早的
# 一份样本为基准,读数连续。
_OMLX_HIST: dict[str, deque] = {}
_OMLX_WIN = 45.0        # 滑动窗口上限(秒):跳变计数器要够长才稳,又不能太旧
_OMLX_MIN = 8.0         # 窗口短于此不出数(一次完成事件就能把短窗口拉成尖峰)
_OMLX_MAX_SNAPS = 200   # 防止高频调用把 deque 撑大


_OMLX_STEP: dict[str, tuple] = {}     # base -> (step, observed_monotonic)


def _omlx_step_rate(base: str, snap: dict) -> float | None:
    """解码步频(步/秒)。没有该端点 / 基线未建 / 快照冻结 → None(调用方保持上次)。

    ⛔ 空闲必须【显式置 0 并重置基线】: 引擎空闲时调度器快照本身冻结, Δstep 与 Δt
       同时为 0, 走"保持上次"会让读数永远卡在最后一次的速率上 —— 请求早结束了面板
       还在报 111 tok/s。这是 2026-09-19 实测踩过的坑。
    ⛔ 分母用快照自带的 observed_monotonic, 不是我们的轮询间隔(见 _scrape_omlx 注释)。"""
    step, mono = snap.get("__omlx_step"), snap.get("__omlx_step_mono")
    if step is None or mono is None:
        return None
    prev = _OMLX_STEP.get(base)
    _OMLX_STEP[base] = (step, mono)
    active = (snap.get("omlx:sched_running", 0.0)
              + snap.get("omlx:sched_prefilling", 0.0))
    if active <= 0:
        return 0.0
    if prev is None:
        return None
    d_step, d_t = step - prev[0], mono - prev[1]
    if d_t <= 0 or d_t > 60 or d_step < 0:      # 冻结 / 基线过旧 / 引擎重启
        return None
    return max(0.0, d_step / d_t)


def _omlx_rates(base: str, snap: dict, now: float) -> tuple:
    """(tok/s, req/s, 窗口秒)。窗口不够长 / 抓取失败 / 计数器回退 → (None, None, None)。"""
    cur_t = snap.get("omlx:total_completion_tokens")
    cur_r = snap.get("omlx:total_requests")
    if cur_t is None or cur_r is None:          # 抓取失败 → 不动历史
        return (None, None, None)
    h = _OMLX_HIST.setdefault(base, deque())
    while h and now - h[0][0] > _OMLX_WIN:
        h.popleft()
    old = h[0] if h else None
    h.append((now, cur_t, cur_r))
    while len(h) > _OMLX_MAX_SNAPS:
        h.popleft()
    if old is None:
        return (None, None, None)
    if cur_t < old[1] or cur_r < old[2]:        # 引擎重启 → 丢历史重来
        h.clear(); h.append((now, cur_t, cur_r))
        return (None, None, None)
    age = now - old[0]
    if age < _OMLX_MIN:
        return (None, None, None)
    return ((cur_t - old[1]) / age, (cur_r - old[2]) / age, round(age, 1))


# llama.cpp 的 tokens_predicted_* 与 oMLX 同病:【只在请求完成时跳】。2026-09-18 实测
# 生成中连抓三次 tokens_predicted_total 纹丝不动(而 n_decode_total 每次 +1),所以
# 1.2s 窗口算出来恒是 0 —— 面板上 tps/TPOT 长期显示 0 就是这么来的。改走滑动窗口。
_LC_HIST: dict[str, deque] = {}
_LC_WIN = 45.0          # 同 _OMLX_WIN:跳变计数器要够长才稳,又不能太旧
_LC_MIN = 8.0
_LC_MAX_SNAPS = 200


def _lc_rates(base: str, snap: dict, now: float) -> tuple:
    """(Δ生成 token, Δ生成耗时秒, 窗口秒)。窗口不够 / 抓取失败 / 计数器回退 → 全 None。

    返回【原始增量】而不是算好的速率:多副本要按 token 数加权合并 TPOT,
    各自算完再平均是错的(短请求多的那台会被算重)。"""
    cur_t = snap.get("llamacpp:tokens_predicted_total")
    cur_s = snap.get("llamacpp:tokens_predicted_seconds_total")
    if cur_t is None or cur_s is None:          # 抓取失败 → 不动历史
        return (None, None, None)
    h = _LC_HIST.setdefault(base, deque())
    while h and now - h[0][0] > _LC_WIN:
        h.popleft()
    old = h[0] if h else None
    h.append((now, cur_t, cur_s))
    while len(h) > _LC_MAX_SNAPS:
        h.popleft()
    if old is None:
        return (None, None, None)
    if cur_t < old[1] or cur_s < old[2]:        # 引擎重启 → 丢历史重来
        h.clear(); h.append((now, cur_t, cur_s))
        return (None, None, None)
    age = now - old[0]
    if age < _LC_MIN:
        return (None, None, None)
    return (cur_t - old[1], cur_s - old[2], round(age, 1))


async def _scrape_omlx(base: str) -> dict:
    """直采 oMLX 的 /api/status。返回 omlx:* 标量;不是 oMLX 就返回 {}。

    ⚠️ total_requests 在【准入】时自增,不是完成时(实测 active_requests=2 时它仍在涨),
    所以它只能算请求率,不能拿来反推每请求的任何量。"""
    try:
        r = await client.get(f"{base}/api/status", timeout=3.0)
        r.raise_for_status()
        d = r.json()
    except Exception:
        return {}
    # 认指标键而不是认 version/owned_by 字段:别的服务也可能有 /api/status
    if not isinstance(d, dict) or "total_completion_tokens" not in d or "loaded_models" not in d:
        return {}
    out: dict = {}
    for k in _OMLX_KEYS:
        try:
            out[f"omlx:{k}"] = float(d.get(k) or 0)
        except (TypeError, ValueError):
            continue
    # 实际加载的权重身份。⛔ 不能靠 /v1/models 的 id:这套部署把 served-name 直接
    # 设成了路由名(实测 2026-09-18 返回 "mbp-none"),按它看永远不知道载的是哪个模型
    # —— 而 MBP 上的模型一天换了三次(Qwen3.6-35B → Qwen3.5-2B → LFM2.5-1.2B)。
    _lm = d.get("default_model") or ((d.get("loaded_models") or [None])[0])
    if _lm:
        out["__omlx_model"] = str(_lm)
    # ── 调度器实时状态(/v1/router/state) ────────────────────────────────
    # /api/status 的 token 计数器只在请求完成时跳, 生成途中吞吐恒 0。调度器快照里的
    # step 是【解码步计数器】, 生成中持续增长, 是这个后端唯一的实时信号。
    # ⛔ 分母必须用快照自带的 observed_monotonic, 不能用我们自己的轮询间隔:
    #    该快照有 TTL, 空闲时会冻结(实测 scheduler_snapshot_age_s 涨到 97s),
    #    拿墙钟去除冻结的计数器会算出假速率。
    # ⛔ fairness 里的 *_ema 与 best_prefill_tps 是引擎的【历史测量】不是当前速率, 不映射。
    # ⛔ oMLX 没有 prefill token 计数器 —— prefill 一律留空, 不借 best_prefill_tps 冒充。
    try:
        r2 = await client.get(f"{base}/v1/router/state", timeout=3.0)
        r2.raise_for_status()
        st = r2.json()
        mdl = next(iter((st.get("models") or {}).values()), None) or {}
        sch = mdl.get("scheduler") or {}
        cnt = sch.get("counts") or {}
        if sch.get("step") is not None and sch.get("observed_monotonic") is not None:
            out["__omlx_step"] = float(sch["step"])
            out["__omlx_step_mono"] = float(sch["observed_monotonic"])
        for src, key in (("running", "omlx:sched_running"),
                         ("prefilling", "omlx:sched_prefilling"),
                         ("waiting", "omlx:sched_waiting")):
            if cnt.get(src) is not None:
                out[key] = float(cnt[src])
        if sch.get("configured_max_concurrency"):
            out["omlx:sched_slots_total"] = float(sch["configured_max_concurrency"])
        if mdl.get("scheduler_snapshot_age_s") is not None:
            out["omlx:snapshot_age_s"] = float(mdl["scheduler_snapshot_age_s"])
    except Exception:
        pass            # 老版本 oMLX 无该端点 → 相关字段缺席, 回落完成计数器窗口
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


# ── ds4 / EXL3 / q27 三种后端的识别与采集 ───────────────────────────
# 口径抄自 sparkDash 已验证的实现(/home/user/dev/sparkDash/server/collectors/
# LlmProbe.js:355-445, 那边有单元测试固定了样本):
#   ds4  : /metrics 里有 ds4_tokens_decoded_total
#   q27  : /metrics 里有 q27_decode_tokens_total(signalnine/q27)
#   exl3 : /health 返回 {backend:"exl3"} 或 {ok:true, busy:<bool>}
#          —— vLLM 的 /health 是空体 200, 不会误判。
# ⚠️ 未经真实实例验证: 本集群 2026-09-19 只跑 SGLang 与 oMLX, 这三种一个都没有。
#    下面的解析只用 sparkDash 的样本做过离线自测(/home/user/dev/hearth/server/api/tests/test_engine_probes.py),
#    真接上实例时必须重新核对字段名, 不要把"能跑通"当成"口径正确"。
_DS4_SCALARS = {
    "ds4_tokens_decoded_total", "ds4_tokens_prefilled_total",
    "ds4_tokens_prefill_computed_total", "ds4_requests_inflight",
    "ds4_decode_tok_s", "ds4_prefill_tok_s",
}
_Q27_SCALARS = {
    "q27_decode_tokens_total", "q27_prompt_tokens_total",
    "q27_prefill_computed_tokens_total", "q27_prefill_cached_tokens_total",
    "q27_requests_total", "q27_requests_errors_total", "q27_requests_inflight",
    "q27_slots_total", "q27_kv_usage_perc", "q27_spec_accept_ratio",
    "q27_preemptions_total",
}


def _prom_parse(text: str, scalars: set, hists: dict, labeled: dict | None = None) -> dict:
    """Prometheus 文本 → {标量名: 求和值} (+ 直方图桶存进 hists 指定的键)。

    同名多 label 的行【求和】(q27 按 api="chat"/"messages" 分组), 与 Hearth
    其它采集器一致。直方图只收 _bucket/_sum/_count 三件套, 留给 _lat_window 差分。"""
    out: dict = {}
    buckets: dict = {}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        sp = line.rsplit(" ", 1)
        if len(sp) != 2:
            continue
        head = sp[0]
        name = head.split("{")[0]
        try:
            v = float(sp[1])
        except ValueError:
            continue
        if labeled and name in labeled:
            # 同名指标按 label 拆语义(ds4 的 kind="computed" 才是实算量)。
            # 这类字段【不能】按名字求和当成一个量用, 见下面 _scrape_ds4 的注释。
            _lbl, _map = labeled[name]
            _m = re.search(rf'{_lbl}="([^"]+)"', head)
            if _m and _m.group(1) in _map:
                _k = _map[_m.group(1)]
                out[_k] = out.get(_k, 0.0) + v
        if name in scalars:
            out[name] = out.get(name, 0.0) + v
            continue
        for hname, key in hists.items():
            if name == hname + "_bucket":
                m = re.search(r'le="([^"]+)"', head)
                if m:
                    d = buckets.setdefault(key, {})
                    d[m.group(1)] = d.get(m.group(1), 0.0) + v
            elif name in (hname + "_sum", hname + "_count"):
                out[name] = out.get(name, 0.0) + v
    out.update(buckets)
    return out


_GEN_HIST: dict[str, deque] = {}
_GEN_WIN = 45.0            # 与 _LC_WIN/_OMLX_WIN 同理:跳变计数器要够长才稳
_GEN_MIN = 8.0
_GEN_MAX_SNAPS = 200


def _gen_rate(key: str, value, now: float, win: float = None, min_age: float = None):
    """(速率/秒, 窗口秒)。窗口不够 / 抓取失败 / 计数器回退 → (None, None)。

    给"只在请求完成时跳"的计数器用(exl3 的 completion_tokens_total 就是)。
    1.2s 双采样在这种计数器上不是 0 就是尖峰, 见 _lc_rates 上方那段实测。
    win/min_age 可覆盖默认窗口: prefill 实时速率用更短的窗口(见 _PREFILL_WIN)。"""
    if value is None:
        return (None, None)
    win = _GEN_WIN if win is None else win
    min_age = _GEN_MIN if min_age is None else min_age
    h = _GEN_HIST.setdefault(key, deque())
    while h and now - h[0][0] > win:
        h.popleft()
    old = h[0] if h else None
    h.append((now, float(value)))
    while len(h) > _GEN_MAX_SNAPS:
        h.popleft()
    if old is None:
        return (None, None)
    if float(value) < old[1]:              # 引擎重启 → 丢历史重来
        h.clear(); h.append((now, float(value)))
        return (None, None)
    age = now - old[0]
    if age < min_age:
        return (None, None)
    return ((float(value) - old[1]) / age, round(age, 1))


async def _scrape_ds4(base: str) -> dict:
    """直采 ds4-server 的 /metrics。不是 ds4 就返回 {}。"""
    try:
        r = await client.get(f"{base}/metrics", timeout=4.0)
        r.raise_for_status()
        txt = r.text
    except Exception:
        return {}
    if not re.search(r"(?m)^ds4_tokens_decoded_total[{\s]", txt):
        return {}
    # ⛔ ds4 的实算 prefill 是【label】不是独立指标名:
    #    ds4_tokens_prefilled_total{kind="computed"} 才是实算,
    #    不带 label 的总量含缓存命中(对照实现:
    #    /home/user/dev/sparkDash/server/collectors/LlmProbe.js:625-632)。
    #    按名字求和会把命中算进去 —— 与 SGLang 的 prompt_tokens_total 同形的坑。
    return _prom_parse(txt, _DS4_SCALARS, {},
                       {"ds4_tokens_prefilled_total":
                        ("kind", {"computed": "__ds4_prefill_computed",
                                  "cached": "__ds4_prefill_cached"})})


async def _scrape_q27(base: str) -> dict:
    """直采 q27 的 /metrics。不是 q27 就返回 {}。TTFT 直方图一并带回。"""
    try:
        r = await client.get(f"{base}/metrics", timeout=4.0)
        r.raise_for_status()
        txt = r.text
    except Exception:
        return {}
    if not re.search(r"(?m)^q27_decode_tokens_total[{\s]", txt):
        return {}
    return _prom_parse(txt, _Q27_SCALARS, {"q27_ttft_seconds": "__ttft_buckets"})


async def _scrape_exl3(base: str) -> dict:
    """探 EXL3(tools/serve_openai.py) 的 /health。不是 exl3 就返回 {}。

    ⛔ EXL3 只给 busy 与两个累计 token 数, 没有直方图也没有队列深度 ——
       延迟分位数/排队一律缺席, 不用 tps 反推(同 llama.cpp/oMLX 的降级口径)。"""
    try:
        r = await client.get(f"{base}/health", timeout=3.0)
        if r.status_code != 200:
            return {}
        d = r.json()
    except Exception:
        return {}
    if not isinstance(d, dict):
        return {}
    busy = d.get("busy")
    if d.get("backend") != "exl3" and not (d.get("ok") is True and isinstance(busy, bool)):
        return {}
    out: dict = {"exl3:busy": 1.0 if busy else 0.0}
    for src, dst in (("completion_tokens_total", "exl3:completion_tokens_total"),
                     ("prompt_tokens_total", "exl3:prompt_tokens_total")):
        v = d.get(src)
        if isinstance(v, (int, float)):
            out[dst] = float(v)
    return out


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
            elif isinstance(v, str):
                # 字符串(oMLX 自报的已加载模型名)不能相加:保留第一个非空的。
                # 多副本理论上载的是同一个权重,不一致时以先抓到的为准,不拼接。
                if not acc.get(k):
                    acc[k] = v
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


# ── 网关 TTFC hook 产出（只读文件，不碰网关）─────────────────────────
# 首个【正文】token 延迟。vLLM 侧给不出:completionStartTime 只是首个 chunk 的
# 时刻,而该模型首个 chunk 是 thinking token 不是正文,库里没有任何 intra-stream
# 的正文起始时刻。网关 hook 在流上直接量,产出 jsonl,我们只读文件。
_TTFC = HEARTH_CFG.get("ttfc") or {}


def _quantile(vals: list, q: float) -> float:
    """原始样本的分位数(线性插值)。与 _hquant 不同 —— 那个吃累积直方图桶,
    这个吃逐条样本。样本量小时分位数会偏高,所以同时给均值和样本数做对照。"""
    if not vals:
        return 0.0
    v = sorted(vals)
    if len(v) == 1:
        return v[0]
    pos = q * (len(v) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


async def _ttfc_scrape() -> dict:
    """读 hook 产出的 jsonl。失败/缺文件一律 {} —— 文件按日期分且容器可写层
    无挂载,重建容器即丢,所以"当天文件不存在"是【正常情况】不是错误。

    取最近两个文件而不是按日期名拼:既跨零点仍能凑满窗口,又不必猜 hook 写
    文件名用的是哪个时区。"""
    if not _TTFC.get("enabled"):
        return {}
    try:
        win = int(_TTFC.get("window_hours", 6))
    except (TypeError, ValueError):
        return {}
    d = str(_TTFC.get("dir", "/tmp/ttfc"))
    argv = ["docker", "exec", str(_TTFC.get("container", "litellm-gateway")), "sh", "-c",
            f"ls -1t {d}/ttfc-*.jsonl 2>/dev/null | head -2 | xargs -r cat"]
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, _e = await asyncio.wait_for(proc.communicate(), timeout=20.0)
        if proc.returncode != 0:
            return {}
    except Exception:
        return {}
    cutoff = time.time() - win * 3600
    agg: dict = {}
    for line in out.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line or line[0] != "{":
            continue
        try:
            r = json.loads(line)
            if float(r.get("ts", 0)) < cutoff:
                continue
            name = str(r.get("model") or "")
        except (ValueError, TypeError):
            continue
        if not name:
            continue
        name = name.split("/", 1)[1] if "/" in name else name
        a = agg.setdefault(name, {"ttfc": [], "ttft": [], "think": [], "n": 0, "null": 0})
        a["n"] += 1
        c = r.get("ttfc_ms")
        # ⛔ ttfc_ms 为 null 是【合法值】不是缺数据:工具调用响应本来就没有正文。
        # 必须先过滤再算分位数,否则要么崩要么把 null 当 0(会把分位数拉到地板)。
        if c is None:
            a["null"] += 1
        else:
            try:
                a["ttfc"].append(float(c))
            except (TypeError, ValueError):
                pass
        for key, fld in (("ttft", "ttft_ms"), ("think", "think_chunks_before_content")):
            v = r.get(fld)
            if v is not None:
                try:
                    a[key].append(float(v))
                except (TypeError, ValueError):
                    pass
    out_by_model: dict = {}
    for name, a in agg.items():
        if not a["n"]:
            continue
        d2: dict = {"ttfcWindowH": win, "ttfcTotalN": a["n"],
                    "ttfcNullN": a["null"],
                    "ttfcNullRate": round(a["null"] / a["n"] * 100, 1)}
        if a["ttfc"]:
            d2.update({"ttfcSampleN": len(a["ttfc"]),
                       "ttfcMean": round(sum(a["ttfc"]) / len(a["ttfc"]), 0),
                       "ttfcP50": round(_quantile(a["ttfc"], 0.50), 0),
                       "ttfcP90": round(_quantile(a["ttfc"], 0.90), 0),
                       "ttfcP99": round(_quantile(a["ttfc"], 0.99), 0)})
        if a["ttft"]:
            # ⛔ 与 vLLM 侧的 ttftP50 【口径不同】,不要互相校验或二选一:
            # hook 只看走网关的流量,vLLM 看全部(含直连)。故字段名带 Gw 标源。
            d2["ttftGwP50"] = round(_quantile(a["ttft"], 0.50), 0)
            d2["ttftGwP90"] = round(_quantile(a["ttft"], 0.90), 0)
        if a["think"]:
            # ⛔ 这是首个正文【之前】的 thinking 分片数,不是全程累计 ——
            # hook 在首个正文到达后完全短路(那正是它零开销的前提)。
            d2["thinkChunksP50"] = round(_quantile(a["think"], 0.50), 0)
        out_by_model[name] = d2
    return out_by_model


async def _spend_loop():
    """独立的长 TTL 刷新循环。⛔ 绝不能放进 2.5s 快照循环:那张表近百万行,
    且这是子进程 + DB 查询。默认 300s 一次 ≈ 0.0033 次/秒。"""
    while True:
        merged: dict = {}
        for fn in (_spend_scrape, _ttfc_scrape):
            try:                        # 两个数据源互不牵连:一个挂了另一个照常
                for k, v in (await fn() or {}).items():
                    merged.setdefault(k, {}).update(v)
            except Exception:
                pass
        if merged:
            _SPEND_DATA["by_model"], _SPEND_DATA["ts"] = merged, time.time()
        await asyncio.sleep(max(60, int(_SPEND.get("refresh_seconds", 300))))


def _spend_for(model_id: str) -> dict:
    """取某模型的 spend 指标。没开、没数据、该模型没样本 → 空 dict → 字段缺席。"""
    global _SPEND_TASK
    if not (_SPEND.get("enabled") or _TTFC.get("enabled")):
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


# ── 延迟直方图的滑动窗口差分 ───────────────────────────────────────────
# ⛔ 直接把第二次抓取的【累积】桶喂给 _hquant，报出来的是「开机以来」不是
# 「当前」。业界标准是 histogram_quantile(q, rate(bucket[5m])) —— 关键是
# rate() 那一步。实测虚高幅度(引擎已跑 11.9 小时、累积 1386 条样本时)：
#     E2E  p50 15871ms → 真值 800ms (19.8x)   E2E  p90 44438 → 2200 (20x)
#     TTFT p50  1640ms → 真值 406ms ( 4.0x)   TTFT p90  4974 → 1975 (2.5x)
#     TPOT p50    40ms → 真值  38ms ( 1.05x)  ← 每 token 的量对请求长短不敏感
# 偏差随引擎运行时长单调增大：跑得越久，"当前延迟"越像是在报历史平均。
_LAT_HIST: dict[str, deque] = {}
_LAT_MAX_WIN = 300.0        # 窗口上限(秒)。取窗口内【最老】的一份做基准，
                            # 所以稳态下窗口自然趋近这个值，样本量最大。
                            #
                            # 2026-09-04 曾改 900 想让 p99 凑够 100 样本，当天回退。
                            # 拉长窗口【不产生信息】，只是把不同负载状态的样本混成
                            # 一个总体，还把十分钟前的结果当"当前"报出去。
                            #
                            # ⚠️ 理由要写对：第一版注释写的是"突发期有排队、空闲期
                            # 无排队，混算会串味"，那是【错的】—— 实测本机几乎不排队
                            # (vllm:request_queue_time_seconds le=0.3 占 99.91%,
                            # 平均排队 20.2ms)。真实机制是 e2e 由【输出 token 数】
                            # 主导，空闲期问的往往是长问题、反而更慢，混算会让结论反号。
                            # 结论不变，但别照着错理由去改代码。
                            #
                            # 更根本的:实测 24h 内 287 个 300 秒窗口, n>=100 的只有
                            # 9 个(3.1%), n=1 的占 69% —— 这个流量下 p99 在数学上
                            # 就不可算, 缺席是正确终态, 不需要任何改动让它出现。
_LAT_MAX_SNAPS = 400        # 防止高频调用把 deque 撑大(按时间 trim 之外的兜底)
_LAT_BUCKETS = ("__e2e_buckets", "__ttft_buckets", "__tpot_buckets",
                "__queue_buckets", "__prefill_buckets", "__decode_buckets",
                "__itl_buckets")
# 均值也必须做差 —— sum/count 同样是生命周期累计
_LAT_MEANS = {
    "ttft": "vllm:time_to_first_token_seconds",
    "tpot": "vllm:request_time_per_output_token_seconds",
    "queue": "vllm:request_queue_time_seconds",
    "prefill": "vllm:request_prefill_time_seconds",
    "decode": "vllm:request_decode_time_seconds",
    "itl": "vllm:inter_token_latency_seconds",
    "e2e": "vllm:e2e_request_latency_seconds",
}


# SGLang 暴露的是同类指标、不同前缀，走同一套窗口差分。
# 2026-09-15 已在 live SGLang 上对照源码核过口径(见 _SGLANG_HISTS 上方注释)。
# 此前这里只配了 ttft/tpot/e2e，分解表的 Queue/Prefill 与全部分位数因此恒为「—」——
# 不是引擎不导出，是没接。decode / itl 才是引擎真的不导出，字段缺席。
_LAT_MEANS_SGLANG = {
    "ttft": "sglang:time_to_first_token_seconds",
    "tpot": "sglang:inter_token_latency_seconds",   # 按 token 加权，见 _SGLANG_HISTS
    "queue": "sglang:queue_time_seconds",
    "prefill": _SGLANG_PREFILL,
    "e2e": "sglang:e2e_request_latency_seconds",
}


# q27 只导出 TTFT 一个直方图(2026-09-19 按 sparkDash 的样本, 未经真实实例验证)。
# decode/ITL/排队一律缺席 —— 不拿 tps 反推。
_LAT_MEANS_Q27 = {"ttft": "q27_ttft_seconds"}


# (短名, 差分桶键, 小数位)。按引擎只列【实际导出】的族，不在表里的族字段整个缺席。
_LAT_FAMILIES_VLLM = (("ttft", "__ttft_buckets", 0), ("tpot", "__tpot_buckets", 1),
                      ("queue", "__queue_buckets", 1), ("prefill", "__prefill_buckets", 1),
                      ("decode", "__decode_buckets", 1), ("itl", "__itl_buckets", 1))
_LAT_FAMILIES_SGLANG = (("ttft", "__ttft_buckets", 0), ("tpot", "__tpot_buckets", 1),
                        ("queue", "__queue_buckets", 1), ("prefill", "__prefill_buckets", 1))
_LAT_FAMILIES_Q27 = (("ttft", "__ttft_buckets", 0),)


def _lat_snapshot(merged: dict, means: dict) -> dict:
    """从一次(已跨副本合并的)抓取里抠出算延迟要用的全部累积量。"""
    snap: dict = {"b": {}, "s": {}}
    for k in _LAT_BUCKETS:
        snap["b"][k] = dict(merged.get(k) or {})
    for base in means.values():
        for suf in ("_sum", "_count"):
            snap["s"][base + suf] = float(merged.get(base + suf, 0.0) or 0.0)
    return snap


def _lat_window(key: str, merged: dict, now: float, means: dict | None = None,
                e2e_prefix: str = "vllm") -> dict | None:
    """滑动窗口差分。返回 差分桶 + 窗口均值 + 窗口长度 + 样本数；不可用返回 None。

    ⛔ 锚点建在【_merge_scrape 之后】的口径上（跨副本已求和）。副作用：某个副本
    掉线会让合并后的和【下降】，被下面的重置检测判成引擎重启 → 丢锚重来。这个
    退化是有意接受的（代价是损失一个窗口），不是 bug —— 不要把它"修"成允许负差分。

    返回 None 的三种情况，调用方一律让相关字段【整组缺席】而不是填 0：
      1. 还没有窗口内的基准（刚启动 / 刚重置）
      2. 检测到 counter 重置（引擎重启、副本掉线）
      3. 窗口内 0 条样本（模型空闲）—— 报 0 会被读成「延迟 0 毫秒」，
         这是所有误读里最危险的一种
    """
    means = means or _LAT_MEANS
    hist = _LAT_HIST.setdefault(key, deque())
    cur = _lat_snapshot(merged, means)
    base = None
    # 取窗口内最老的一份做基准；顺手 trim 掉过期的
    while hist and now - hist[0][0] > _LAT_MAX_WIN:
        hist.popleft()
    if hist:
        base = hist[0]
    hist.append((now, cur))
    while len(hist) > _LAT_MAX_SNAPS:
        hist.popleft()
    if base is None:
        return None
    b_ts, b_snap = base

    # 陷阱 1：counter 重置要【逐桶】判 —— _rate() 那套单值判据不够用。
    # 任一 le 桶或任一 sum/count 变小 → 引擎重启/副本掉线 → 丢弃全部历史重新起锚，
    # 绝不能让差分出现负数（负数喂进 _hquant 会算出无意义的分位数）。
    for k in _LAT_BUCKETS:
        cb, bb = cur["b"][k], b_snap["b"][k]
        for le, v in bb.items():
            if cb.get(le, 0.0) < v:
                hist.clear()
                hist.append((now, cur))
                return None
    for name, v in b_snap["s"].items():
        if cur["s"].get(name, 0.0) < v:
            hist.clear()
            hist.append((now, cur))
            return None

    diff_b: dict = {}
    for k in _LAT_BUCKETS:
        cb, bb = cur["b"][k], b_snap["b"][k]
        d = {le: cb[le] - bb.get(le, 0.0) for le in cb}
        diff_b[k] = d if max(d.values(), default=0.0) > 0 else {}

    # ⚠️ 累加器不能叫 means —— 那会遮蔽同名入参，循环立刻在空字典上迭代，
    # 所有窗口均值静默丢成 0（而 0 恰好会被读成「延迟 0 毫秒」）。
    out_means: dict = {}
    counts: dict = {}
    for short, base_name in means.items():
        dc = cur["s"][base_name + "_count"] - b_snap["s"][base_name + "_count"]
        ds = cur["s"][base_name + "_sum"] - b_snap["s"][base_name + "_sum"]
        counts[short] = int(dc)
        if dc > 0:
            out_means[short] = ds / dc * 1000.0
    # 样本数用 e2e(按请求计)；ITL 是按 token 间隔计的，量级不同，不拿它当请求数
    _e2e_cnt = f"{e2e_prefix}:e2e_request_latency_seconds_count"
    n = int(cur["s"].get(_e2e_cnt, 0.0) - b_snap["s"].get(_e2e_cnt, 0.0))
    if n <= 0 and not out_means:
        return None                     # 窗口内没有完成的请求 → 整组缺席
    return {"b": diff_b, "mean": out_means, "n": counts,
            "windowSec": round(now - b_ts, 1), "sampleN": max(0, n)}


# 小样本下分位数无意义 —— 而且失效方式很有欺骗性:样本不够时 _hquant 走
# `if c == prev_c: return le` 那条路,直接吐【桶沿】。实测窗口内只有 4 条样本时
# e2e p50 报 5000.0ms(真实约 700ms,误差 7 倍),因为 5.0 正是 e2e 直方图的桶边界。
# 它看起来是个完全正常的数字,不看样本数根本发现不了。
#
# 判据用样本数不用窗口时长:300 秒窗口里只有 2 条请求同样不可信。
# 分位数 q 只有在窗口内至少有一个样本能落到尾部时才有意义:
#     n * (1 - q) >= 1   →   p50 需 n>=2, p90 需 n>=10, p95 需 n>=20, p99 需 n>=100
# 【逐个分位数分别判定】:不够的那个单独缺席,够的照常出(n=12 时 p50/p90 出、
# p99 缺席),比整组一起藏或一起出都更准确。
_LOW_SAMPLE_N = 20          # 低于此值给值但标注可疑(照 sloJointWide 的先例)


def _q_ok(n: int, q: float) -> bool:
    # 容差不是装饰:1-0.9 在二进制里是 0.09999999999999998,10*(1-0.9) < 1,
    # 不给容差的话 p90 会要求 n>=11 而不是判据说的 10 —— 只差一个样本,
    # 但那正好是"刚够"与"不够"的分界,会让边界情形静默地少一个分位数。
    return n * (1.0 - q) >= 1.0 - 1e-9


def _put_q(live: dict, name: str, buckets: dict, n: int, q: float, nd: int) -> None:
    """样本量够才写入该分位数;不够则字段【缺席】,不给一个像模像样的桶沿。"""
    if _q_ok(n, q):
        live[name] = round(_hquant(buckets, q) * 1000, nd)


def _put_latency(live: dict, lat: dict, families: tuple) -> None:
    """窗口元信息 + e2e 分位数 + 各族 均值/p50/p90/p99。vLLM 与 SGLang 共用 ——
    以前两个分支各写一份，SGLang 那份只抄了 ttft/tpot 均值，分解表因此整片空着。"""
    _lb, _lm, _ln = lat["b"], lat["mean"], lat["n"]
    # 窗口长度与样本数必须暴露:读数的人要能判断这个 p99 是几条样本
    # 撑起来的。样本数按【请求】计(e2e),ITL 是按 token 间隔计的,
    # 量级不同,不拿它冒充请求数。
    live["latencyWindowSec"] = lat["windowSec"]
    live["latencySampleN"] = lat["sampleN"]
    # 样本偏少时仍给值,但标注可疑 —— 与 sloJointWide 同一套做法
    # (给值+标注,而不是藏起来)。数学上无意义的那些分位数才真的缺席。
    live["latencyLowSample"] = lat["sampleN"] < _LOW_SAMPLE_N
    # e2e 这组历史字段沿用 p50/p95/p99 命名(不动),但语义已从
    # 「开机以来」修正为「窗口内」。
    for _q, _n in ((0.50, "p50"), (0.95, "p95"), (0.99, "p99")):
        _put_q(live, _n, _lb["__e2e_buckets"], _ln.get("e2e", 0), _q, 0)
    # 均值同样是窗口差分(Δsum/Δcount),与分位数同源同窗 —— 一半窗口
    # 一半生命周期比全错更难查。⛔ 均值缺失时也不能填 0
    # (同「延迟 0 毫秒」那类误读),没有就整个不给。
    # 逐族取各自的样本数:ITL 是按 token 间隔计的,比请求数大两个数量级,
    # 用请求数去判定它会把本来足够可信的 ITL 分位数误藏掉。
    for _k, _bk, _nd in families:
        if _k in _lm:
            live[_k] = round(_lm[_k], 1)
        for _q, _suf in ((0.50, "P50"), (0.90, "P90"), (0.99, "P99")):
            _put_q(live, _k + _suf, _lb[_bk], _ln.get(_k, 0), _q, _nd)


def _put_saturation(live: dict, waiting_min: float) -> None:
    """饱和提示:排队时间占了 TTFT 的大头 + 两次采样都有请求在等 →
    再加负载已经不划算(实测 QPS 1.0→2.0 吞吐只涨 25%,而 TTFT p90
    涨了一个数量级,多出来的时间几乎全花在排队上)。
    "持续"在单次调用内只能取两次采样都 > 0(调用方传两次采样的较小值) ——
    这是本接口能拿到的最强证据,不做跨调用状态。

    queueP90/ttftP90 可能因样本不足而缺席 → 派生量跟着缺席,
    不用 0 顶替(那会让"排队占比 0%"看起来像系统很健康)。"""
    if live.get("queueP90") is None or not live.get("ttftP90"):
        return
    _qr = live["queueP90"] / live["ttftP90"]
    live["queueShareP90"] = round(_qr * 100, 1)
    live["saturated"] = bool(waiting_min > 0
                             and _qr > float(_SLO.get("queue_share_warn", 0.5)))


# ── prefill 的两个口径 ──────────────────────────────────────────────
# 2026-09-19 机主反馈: 首页要能一眼看出"现在哪台在 prefill、多快"。原先只有
# 【生命周期累计平均】一个口径, 三台全空闲时它仍显示 1699 / 1011 / 2037 ——
# 这是"值在但不动"的另一种形态(恒定的大数, 比恒 0 更能骗人)。
# 现在分成两个字段, 名字自带口径:
#   prefillTokPerS          实时(墙钟)速率, 空闲就是 0。量的是【此刻负载】。
#   prefillTokPerSLifetime  累计 prefill token / 累计 prefill 耗时, 每请求归一化。
#                           量的是【引擎速度】, 与负载无关, 空闲时不会掉。
# ⛔ 两者不可互相顶替, 也不可共用一个标签(界面上分别标"实时""累计平均")。
# 窗口取 30s: 太短(1.2s)在 prefill 这种突发量上不是 0 就是尖峰; 太长(45s+)空闲后
# 要等半分多钟才回落到 0, 首页看起来就像"还在跑"。min_age 5s 是为了刚启动时
# 给 None(界面显示"—")而不是把半个窗口当成一整个窗口算。
_PREFILL_WIN = 30.0
_PREFILL_MIN = 5.0


def _put_prefill_rt(live: dict, model_id: str, tokens, now: float,
                    source: str = "window") -> None:
    """实时 prefill 速率(tok/s, 墙钟口径)。计数器缺席 → 字段整个缺席, 不填 0。"""
    if tokens is None:
        return
    r, w = _gen_rate(f"prefill:{model_id}", tokens, now,
                     win=_PREFILL_WIN, min_age=_PREFILL_MIN)
    if r is None:                      # 窗口还没攒够 → 缺席, 界面显示"—"
        return
    live["prefillTokPerS"] = round(max(0.0, r), 0)
    live["prefillWindowSec"] = w
    live["prefillSource"] = source


def _put_prefill_tps(live: dict, tokens: float, seconds: float) -> None:
    """prefill 吞吐(tok/s)，只给【生命周期】口径(与 _spec_stats 的 perPos 同理)。
    入参是第二次抓取的累积值：实算 prefill token 总数、prefill 耗时总和(秒)。

    ⛔ 不能做滑动窗口差分：分子【每个 chunk 算完就累加】，分母要等整个 prefill
    【结束才落一次】。1M 上下文的一次 prefill 要 100-170s，与 300s 窗口同一量级 ——
    窗口末尾若有一条还在 prefill，token 进来了、耗时没进来，读数就炸。
    2026-09-15 实测 SGLang 窗口 n=5 时报 34,059 tok/s，同期 70 分钟长窗口 1177、
    生命周期约 1300，虚高 25 倍以上；耗时落地的下一个窗口又会反向偏低。
    vLLM 同病：iteration_tokens 每步累加，request_prefill_time 请求结束才记。
    这两个计数器之间没有按请求对齐的办法，样本数门槛也挡不住(一条在途长 prefill
    就够把几十条短请求的比值带偏)。
    也不用 Δtoken / 窗口秒数：那个没有错位，但量的是负载(空闲时趋近 0)，不是引擎速度。

    ⚠️ 分母是【跨请求求和】的：并发时各请求在墙钟上重叠，所以这是【每请求归一化】
    的速率，不是墙钟吞吐。别拿它跟 tps 比 —— 界面上也标了这句。"""
    if tokens > 0 and seconds > 0:
        live["prefillTokPerSLifetime"] = round(tokens / seconds, 0)


def _spec_stats(a: dict, b: dict, prefix: str = "vllm") -> dict | None:
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
    drafts = b.get(f"{prefix}:spec_decode_num_drafts_total", 0.0)
    dtok = b.get(f"{prefix}:spec_decode_num_draft_tokens_total", 0.0)
    acc = b.get(f"{prefix}:spec_decode_num_accepted_tokens_total", 0.0)
    pos = b.get("__spec_pos") or {}
    if drafts <= 0:
        return None                     # 未开投机解码 / 开了但零流量 → 不伪造

    d_dtok = _rate(a, b, f"{prefix}:spec_decode_num_draft_tokens_total", 1.0)
    d_acc = _rate(a, b, f"{prefix}:spec_decode_num_accepted_tokens_total", 1.0)
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
    obases = sorted({b for m in disco for b in m.get("omlx_bases", [])})
    dbases = sorted({b for m in disco for b in m.get("ds4_bases", [])})
    qbases = sorted({b for m in disco for b in m.get("q27_bases", [])})
    xbases = sorted({b for m in disco for b in m.get("exl3_bases", [])})
    # 2026-08-29: dt 原为硬编码 0.5, 但真实间隔 = sleep + 两轮【串行】抓取耗时。
    # 抓取耗时被漏算 -> dt 偏小 -> 速率系统性偏高(实测虚高约 2 倍:
    # 持续 61 tok/s 显示成 129)。改为用 monotonic 实测间隔。
    # sleep 同时 0.5 -> 1.2s: 本地推理一步约 76ms, 0.5s 窗口只装得下 7 步,
    # 而每步产出 1-5 token 方差极大; 1.2s 约 16 步, 方差被抹平且接口仍可接受。
    _t0 = time.monotonic()
    s1 = {b: await _scrape_vllm(b) for b in vbases}
    l1 = {b: await _scrape_llamacpp(b) for b in lbases}
    g1 = {b: await _scrape_sglang(b) for b in gbases}
    o1 = {b: await _scrape_omlx(b) for b in obases}
    d1 = {b: await _scrape_ds4(b) for b in dbases}
    q1 = {b: await _scrape_q27(b) for b in qbases}
    x1 = {b: await _scrape_exl3(b) for b in xbases}
    await asyncio.sleep(1.2)
    s2 = {b: await _scrape_vllm(b) for b in vbases}
    l2 = {b: await _scrape_llamacpp(b) for b in lbases}
    g2 = {b: await _scrape_sglang(b) for b in gbases}
    o2 = {b: await _scrape_omlx(b) for b in obases}
    d2 = {b: await _scrape_ds4(b) for b in dbases}
    q2 = {b: await _scrape_q27(b) for b in qbases}
    x2 = {b: await _scrape_exl3(b) for b in xbases}
    _dt_real = max(1e-3, time.monotonic() - _t0)
    _t_now = time.monotonic()           # 持续吞吐锚点时刻,整轮统一
    _tps_roll = _tps_rollup(s2, _t_now)  # 整轮一次性推进锚点(不可下放进循环)
    # oMLX 的计数器【只在请求完成时跳一次】(实测 active=2 时连续几秒 Δ=0,完成瞬间
    # 一次 +47/+168) → 走滑动窗口,整轮一次性推进(同 _tps_rollup 的理由)。
    _omlx_rate = {b: _omlx_rates(b, o2.get(b) or {}, _t_now) for b in obases}
    # 调度器步频: 生成中唯一的实时信号(完成计数器要等请求结束才跳)
    _omlx_srate = {b: _omlx_step_rate(b, o2.get(b) or {}) for b in obases}
    # llama.cpp 同理(生成 token 计数器只在请求完成时跳),整轮一次性推进
    _lc_rate = {b: _lc_rates(b, l2.get(b) or {}, _t_now) for b in lbases}
    out = []
    for m in disco:
        vb = m.get("vllm_bases") or []
        lb = m.get("llamacpp_bases") or []
        gb = m.get("sglang_bases") or []
        ob = m.get("omlx_bases") or []
        db = m.get("ds4_bases") or []
        qb = m.get("q27_bases") or []
        xb = m.get("exl3_bases") or []
        base_keys = ("id", "display", "vendor", "kind", "params", "quant",
                     "ctx", "framework", "nodes", "vram", "route", "tags",
                     "identityUnverified", "identityCandidates", "source",
                     "servedName", "apiNodes")
        card = {k: m.get(k) for k in base_keys}
        if vb:                                  # 真实 vLLM 指标（可能多副本汇总）
            a = _merge_scrape([s1.get(b) or {} for b in vb])
            b = _merge_scrape([s2.get(b) or {} for b in vb])
            dt = _dt_real
            tps = _rate(a, b, "vllm:generation_tokens_total", dt)
            rps = _rate(a, b, "vllm:request_success_total", dt)
            kv = b.get("vllm:kv_cache_usage_perc", 0) * 100 / max(1, len(vb))
            # 全部延迟量走【滑动窗口差分】。None = 刚起/引擎重启/窗口内无请求,
            # 此时整组延迟字段缺席 —— 不填 0(会被读成"延迟 0 毫秒")。
            lat = _lat_window(m["id"], b, _t_now)
            tps_sus, tps_win = _tps_sustained(vb, _tps_roll)
            running = _gauge(a, b, "vllm:num_requests_running")
            waiting = _gauge(a, b, "vllm:num_requests_waiting")
            state = "serving" if running > 0 or tps > 0 else "idle"
            live = {"tps": round(tps, 1), "rps": round(rps, 3),
                    "kv": round(kv, 1), "running": int(running),
                    "waiting": int(waiting), "metrics": "vllm",
                    # 真实驻留探针：vLLM 可达且模型已加载 → 权重常驻、毫秒级可服务
                    "resident": True,
                    # tps 是 1.2s 瞬时采样窗口 —— 把窗口长度一并暴露出来,
                    # 面板才能标清"瞬时"而不是让人当成持续吞吐。
                    "tpsWindowSec": round(dt, 2),
                    "tpsSustained": tps_sus, "tpsSustainedWindowSec": tps_win}
            if lat:
                # 窗口元信息 / e2e / 各族均值与分位数，口径注释都在 _put_latency 里
                _put_latency(live, lat, _LAT_FAMILIES_VLLM)
            # prefill 吞吐(tok/s)。业界不用【阶段耗时】衡量 prefill/decode 性能:
            # 原始耗时随 prompt/输出长度线性变化，量的是负载不是引擎速度
            # (实测 code 那条输出 1907 token 用约 19s、structured 那条 414 token
            #  用约 5s，decode 耗时差 4 倍而速度几乎一样 99.8 vs 87.7 t/s)。
            # prefill 看吞吐，decode 看 ms/token —— decode 一侧的对照就是同表下面
            # 的 TPOT 与 ITL，不在 decode 行重复造一个吞吐。
            #
            # ⛔ 分子必须用 iteration_sum - generation(vLLM 的
            # prompt_token_stats.computed，见 loggers.py:1205-1208)，这是【实算的】
            # prefill token，天然排除 prefix cache 命中。
            # ⛔ 不能用 prompt_tokens_total：本机实测 prefix cache 命中率 93.4%，
            # 该口径 61.6M 对实算 4.09M，会把吞吐夸大约 15 倍。这不是"日后才会
            # 分叉"，是当前就错。实算值恰好等于
            # prompt_tokens_total - prompt_tokens_cached_total，两条路径互为佐证。
            #
            # 为什么只给生命周期口径、为什么是每请求归一化 —— 见 _put_prefill_tps。
            _pf_num = (b.get("vllm:iteration_tokens_total_sum", 0.0)
                       - b.get("vllm:generation_tokens_total", 0.0))
            _put_prefill_tps(live, _pf_num,
                             b.get("vllm:request_prefill_time_seconds_sum", 0.0))
            # 实时口径用同一个分子(实算 prefill token), 分母换成墙钟 → 空闲即 0。
            _put_prefill_rt(live, m["id"], _pf_num, _t_now)
            spec = _spec_stats(a, b)
            if spec:                    # 未开投机解码 → 键整个缺席,前端 if (live.spec)
                live["spec"] = spec
            # SLO 达标率。⛔ 必须用【同一份差分桶】—— 它建在 TTFT/TPOT 之上,
            # 若这里还吃生命周期桶,就成了"一半窗口一半生命周期",比全错更难查。
            slo = (_slo_rates(lat["b"]["__ttft_buckets"], lat["b"]["__tpot_buckets"])
                   if lat else None)
            if slo:
                live.update(slo)
            live["waitingCapacity"] = int(_gauge(a, b, "__waiting_capacity"))
            _put_saturation(live, min(a.get("vllm:num_requests_waiting", 0),
                                      b.get("vllm:num_requests_waiting", 0)))
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
            # 吞吐与 TPOT 走滑动窗口(见 _lc_rates):1.2s 窗口在生成过程中恒为 0。
            _r = [_lc_rate.get(x) or (None, None, None) for x in lb]
            _ok = [x for x in _r if x[0] is not None]
            _full = len(_ok) == len(lb) and bool(lb)   # 有副本没攒够窗口 → 整体不出数
            _d_tok = sum(x[0] for x in _ok) if _full else 0.0
            _d_sec = sum(x[1] for x in _ok) if _full else 0.0
            _win = max((x[2] for x in _ok), default=None) if _full else None
            tps = (_d_tok / _win) if _win else 0.0
            running = _gauge(a, b, "llamacpp:requests_processing")
            waiting = _gauge(a, b, "llamacpp:requests_deferred")
            state = "serving" if running > 0 or tps > 0 else "idle"
            # llama.cpp /metrics 没有 TTFT/e2e 直方图/KV 占用 → 这些字段【整个缺席】。
            # ⛔ 旧版填 0(ttft/p50/p95/p99),面板上会被读成"TTFT 0ms、p99 0ms"。
            # rps 同理没有请求计数器,保持 0(前端 metrics=llamacpp 显示"—")。
            live = {"tps": round(tps, 1), "rps": 0,
                    "kv": 0, "running": int(running),
                    "waiting": int(waiting), "metrics": "llamacpp",
                    "resident": True, "tpsWindowSec": _win}
            if _d_tok > 0:              # 窗口内有请求完成才有每 token 耗时
                live["tpot"] = round(_d_sec / _d_tok * 1000, 1)
            # 引擎步频:n_decode_total 每次 llama_decode 调用都涨(实测生成中连抓三次
            # +1/+1),是这个后端唯一能在 1.2s 窗口上反映"此刻在不在动"的信号。
            live["stepsPerSec"] = round(_rate(a, b, "llamacpp:n_decode_total", dt), 2)
            # prefill 吞吐 / 提示词缓存命中:都是【生命周期】累计比值(口径同 oMLX 那条)。
            # prompt_tokens_total 是实算的(命中的另记在 prompt_tokens_cached_total),
            # 所以这个比值不会被缓存命中夸大 —— 与 vLLM 侧那条口径一致。
            _put_prefill_tps(live, b.get("llamacpp:prompt_tokens_total", 0.0),
                             b.get("llamacpp:prompt_seconds_total", 0.0))
            # 实时: prompt_tokens_total 的墙钟差分。这个计数器与 tokens_predicted
            # 一样【只在请求完成时跳】, 所以必须走窗口, 不能用 1.2s 双采样。
            _put_prefill_rt(live, m["id"], b.get("llamacpp:prompt_tokens_total"), _t_now)
            # 命中率的分母必须是【实算 + 命中】, 两者不可重叠。
            # ✅ 2026-09-19 16:42 在 .20:8001 实测验证过 llama.cpp 的这两个计数器互斥:
            #    同一个 prompt 连发两次 —— 冷跑 Δprompt_tokens_total=1500 / Δcached=0,
            #    热跑 Δprompt_tokens_total=0 / Δcached=2000。即 prompt_tokens_total
            #    【不含】命中, 所以 cached/(cached+prompt) 没有重复计数。
            # ⛔ 别照抄到别的引擎: SGLang 的 sglang:prompt_tokens_total 【含】命中
            #    (同日实测 20s 窗口 732 对实算 115), 那边要用
            #    realtime_tokens_total{mode=prefill_compute} 当实算分子。
            #    sparkDash 就是在这一步把命中率算成了 0.4895(真值 0.937)。
            _pc = b.get("llamacpp:prompt_tokens_cached_total", 0.0)
            _pp = b.get("llamacpp:prompt_tokens_total", 0.0)
            if _pc + _pp > 0:
                live["cacheHitRate"] = round(_pc / (_pc + _pp) * 100, 1)
            spec = _spec_stats(a, b, "llamacpp")
            if spec:                    # 未开投机解码 → 键缺席,前端整块不渲染
                live["spec"] = spec
        elif gb:                                # SGLang 真实指标(含 TTFT/e2e, 接近 vLLM)
            a = _merge_scrape([g1.get(b) or {} for b in gb])
            b = _merge_scrape([g2.get(b) or {} for b in gb])
            dt = _dt_real
            tps = _rate(a, b, "__sglang_decode_tokens_total", dt)
            kv = b.get("sglang:token_usage", 0) * 100 / max(1, len(gb))
            # 与 vLLM 同一修正：延迟量走滑动窗口差分，不再报"开机以来"
            lat = _lat_window(m["id"], b, _t_now, means=_LAT_MEANS_SGLANG,
                              e2e_prefix="sglang")
            running = _gauge(a, b, "sglang:num_running_reqs")
            waiting = _gauge(a, b, "sglang:num_queue_reqs")
            state = "serving" if running > 0 or tps > 0 else "idle"
            live = {"tps": round(tps, 1), "rps": 0,
                    "kv": round(kv, 1), "running": int(running),
                    "waiting": int(waiting), "metrics": "sglang",
                    "resident": True}
            if lat:                     # None → 整组延迟字段缺席，不填 0
                _put_latency(live, lat, _LAT_FAMILIES_SGLANG)
            # prefill 吞吐：分子 realtime_tokens_total{mode=prefill_compute}(实算，已排除
            # prefix cache 命中)，分母 prefill_forward 耗时总和。SGLang 在
            # report_prefill_stats 里【每个 prefill 批次】累加分子，正是窗口差分会炸的
            # 那种错位 —— 只给生命周期口径，理由见 _put_prefill_tps。
            _put_prefill_tps(live, b.get("__sglang_prefill_compute_tokens_total", 0.0),
                             b.get(_SGLANG_PREFILL + "_sum", 0.0))
            # 实时: 同一个分子(每个 prefill 批次就累加, 天然适合墙钟差分), 分母是墙钟。
            # 这正是生命周期口径【不能】做窗口差分的那个分子 —— 换成墙钟分母就没有
            # 分子分母错位的问题了(见 _put_prefill_tps 的说明)。
            _put_prefill_rt(live, m["id"],
                            b.get("__sglang_prefill_compute_tokens_total"), _t_now)
            # ⛔ SLO 达标率对 SGLang 【不给】：_slo_rates 的联合区间(Fréchet 边界)
            # 要求 TTFT 与 TPOT 是同一批【请求】上的边缘分布，而 SGLang 的 TPOT 桶
            # 按 token 加权(长请求权重大)，两个分量总体不同，区间不成立。
            _put_saturation(live, min(a.get("sglang:num_queue_reqs", 0),
                                      b.get("sglang:num_queue_reqs", 0)))
        elif ob:                                # oMLX:只有累计计数器,没有直方图
            a = _merge_scrape([o1.get(b) or {} for b in ob])
            b = _merge_scrape([o2.get(b) or {} for b in ob])
            dt = _dt_real
            # 滑动窗口速率。窗口没攒够(刚重启 8 秒内)→ 全为 None,此时吞吐/请求率
            # 字段整组给 0 而不是拿 1.2s 窗口顶 —— 跳变计数器在 1.2s 上不是 0 就是尖峰。
            _rates = [_omlx_rate.get(x) or (None, None, None) for x in ob]
            _ok = [r for r in _rates if r[0] is not None]
            tps = sum(r[0] for r in _ok) if len(_ok) == len(ob) and ob else 0.0
            rps = sum(r[1] for r in _ok) if len(_ok) == len(ob) and ob else 0.0
            _tps_win = max((r[2] for r in _ok), default=None)
            # 调度器计数优先于 /api/status 的 active_requests: 前者是调度器自己的
            # 实时账本, 还区分 running / prefilling / waiting。
            _sched_run = _gauge(a, b, "omlx:sched_running")
            _sched_pre = _gauge(a, b, "omlx:sched_prefilling")
            _has_sched = ("omlx:sched_running" in b) or ("omlx:sched_running" in a)
            running = (_sched_run + _sched_pre) if _has_sched else _gauge(a, b, "omlx:active_requests")
            waiting = (_gauge(a, b, "omlx:sched_waiting") if _has_sched
                       else _gauge(a, b, "omlx:waiting_requests"))
            # 步频 → 实时吞吐。单流下一步即一 token(2026-09-19 实测: 步频 116-126/s,
            # 同期引擎自报 solo_decode_tps_ema 112-118, 吻合)。
            # ⛔ 并发 >1 时一步产出多个 token, step 不再等于 token 数 —— 这时【不】拿它
            #    当 tok/s, 回落到完成计数器的 45s 窗口值, 并用 tpsSource 标明来源。
            _sr = [_omlx_srate.get(x) for x in ob]
            _steps = (sum(x for x in _sr if x is not None)
                      if any(x is not None for x in _sr) else None)
            if _steps is not None:
                live_steps = round(_steps, 2)
            else:
                live_steps = None
            if _steps is not None and running <= 1:
                tps, _tps_src = _steps, "steps"
            else:
                _tps_src = "window"
            state = "serving" if running > 0 or tps > 0 else "idle"
            # ⛔ kv 留 0:oMLX 只给 model_memory_used/max(权重+KV 对内存上限),那不是
            #    KV 池占用率,填进 kv 会被读成"KV 用了 75%"。
            # ⛔ 不拿 tps 反推 TPOT:并发 2 时 1000/tps 会把每 token 耗时算少一半。
            #    ttft/tpot/p50/p95/p99 一律缺席(与 llama.cpp 同样的诚实降级)。
            live = {"tps": round(tps, 1), "rps": round(rps, 3), "kv": 0,
                    "running": int(running), "waiting": int(waiting),
                    "metrics": "omlx",
                    # 真实驻留探针:引擎自报已加载模型数 > 0
                    "resident": b.get("omlx:models_loaded", 0) > 0,
                    "tpsWindowSec": _tps_win,
                    # steps = 调度器步频实时值; window = 完成计数器的 45s 滑动窗口
                    "tpsSource": _tps_src}
            if live_steps is not None:
                live["stepsPerSec"] = live_steps
            if b.get("omlx:sched_slots_total"):
                live["slotsTotal"] = int(b["omlx:sched_slots_total"])
            # prefill 吞吐与缓存命中率只有 oMLX 自报的【生命周期】均值 —— 不做窗口差分:
            # Δ(prompt-cached)/Δt 是墙钟吞吐,与面板上"每请求归一化"的 prefill 口径
            # 不是一回事,同名不同义比缺失更糟。
            # oMLX 没有 prefill token 计数器(/api/status 与 /v1/router/state 都没有),
            # 只有引擎自报的【历史均值】avg_prefill_tps。⛔ 不拿 best_prefill_tps 或
            # fairness 的 ema 顶替实时值 —— 那些同样是历史量。实时口径在此【无源】。
            _pf = b.get("omlx:avg_prefill_tps", 0.0)
            if _pf > 0:
                live["prefillTokPerSLifetime"] = round(_pf, 0)
            live["prefillSource"] = "none"
            _ce = b.get("omlx:cache_efficiency", 0.0)
            if _ce > 0:
                live["cacheHitRate"] = round(_ce, 1)
            # 已加载权重的真身与常驻大小:路由名(mbp-none)看不出载的是什么模型
            _lm = b.get("__omlx_model")
            if _lm:
                live["loadedModel"] = _lm
            _mw = b.get("omlx:model_memory_used", 0.0)
            if _mw > 0:
                live["weightsGb"] = round(_mw / 2 ** 30, 2)
        elif db:                                # ds4-server(未经真实实例验证)
            a = _merge_scrape([d1.get(b) or {} for b in db])
            b = _merge_scrape([d2.get(b) or {} for b in db])
            # ⛔ 不用引擎自报的 ds4_decode_tok_s / ds4_prefill_tok_s 当实时值:
            #    它们是 ~60s 窗口 gauge, 请求结束后还会【长时间维持非零】(对照实现
            #    LlmProbe.js:616-620 明确写了 "do not use them for the live panel")。
            #    那正是"值在但不动"的假数形态 —— 一律走计数器差分。
            tps = _rate(a, b, "ds4_tokens_decoded_total", _dt_real)
            running = _gauge(a, b, "ds4_requests_inflight")
            state = "serving" if running > 0 or tps > 0 else "idle"
            live = {"tps": round(tps, 1), "rps": 0.0, "kv": 0,
                    "running": int(running), "waiting": 0,
                    "metrics": "ds4", "resident": True, "tpsSource": "window1.2s"}
            # 实算 prefill 只认 kind="computed" 那条。取不到时:
            #   有 kind="cached" → 用 总量 - 命中 推导(两个都是精确量, 相减仍精确,
            #     不是"换成语义更宽的近似值"; 这条是 w1W:p1 2026-09-19 的改法, 采纳)
            #   连 cached 也没有 → 字段【整个缺席】, 绝不退回含命中的总量
            _ds4_pf = b.get("__ds4_prefill_computed")
            _ds4_ca = b.get("__ds4_prefill_cached")
            if _ds4_pf is None and _ds4_ca is not None:
                _tot = b.get("ds4_tokens_prefilled_total")
                if _tot is not None and _tot >= _ds4_ca:
                    _ds4_pf = _tot - _ds4_ca
            if _ds4_pf is None:
                live["prefillSource"] = "none"
            else:
                _put_prefill_rt(live, m["id"], _ds4_pf, _t_now)
            if _ds4_pf is not None and _ds4_ca is not None and (_ds4_pf + _ds4_ca) > 0:
                live["cacheHitRate"] = round(_ds4_ca / (_ds4_ca + _ds4_pf) * 100, 1)
        elif qb:                                # q27(未经真实实例验证)
            a = _merge_scrape([q1.get(x) or {} for x in qb])
            b = _merge_scrape([q2.get(x) or {} for x in qb])
            tps = _rate(a, b, "q27_decode_tokens_total", _dt_real)
            rps = _rate(a, b, "q27_requests_total", _dt_real)
            running = _gauge(a, b, "q27_requests_inflight")
            kv = b.get("q27_kv_usage_perc", 0.0) * 100 / max(1, len(qb))
            lat = _lat_window(m["id"], b, _t_now, _LAT_MEANS_Q27, e2e_prefix="q27")
            state = "serving" if running > 0 or tps > 0 else "idle"
            live = {"tps": round(tps, 1), "rps": round(rps, 3), "kv": round(kv, 1),
                    "running": int(running), "waiting": 0,
                    "metrics": "q27", "resident": True, "tpsSource": "window1.2s"}
            if b.get("q27_slots_total"):
                live["slotsTotal"] = int(b["q27_slots_total"])
            # 前缀缓存命中 = 命中 token / (命中 + 实算) —— 两个都是累计量, 给的是
            # 生命周期口径(与 llama.cpp/oMLX 同), 不做窗口差分。
            _cc = b.get("q27_prefill_cached_tokens_total", 0.0)
            _cp = b.get("q27_prefill_computed_tokens_total", 0.0)
            if _cc + _cp > 0:
                live["cacheHitRate"] = round(_cc / (_cc + _cp) * 100, 1)
            _put_prefill_rt(live, m["id"],
                            b.get("q27_prefill_computed_tokens_total"), _t_now)
            _sa = b.get("q27_spec_accept_ratio")
            if _sa is not None:
                live["specAcceptRate"] = round(float(_sa) * 100, 1)
            if lat:
                _put_latency(live, lat, _LAT_FAMILIES_Q27)
        elif xb:                                # EXL3(未经真实实例验证)
            a = _merge_scrape([x1.get(x) or {} for x in xb])
            b = _merge_scrape([x2.get(x) or {} for x in xb])
            # ⛔ EXL3 的 completion_tokens_total 只在请求完成时跳 → 走 45s 滑动窗口,
            #    1.2s 双采样在这种计数器上不是 0 就是尖峰。
            _r, _w = _gen_rate("exl3:" + m["id"], b.get("exl3:completion_tokens_total"), _t_now)
            tps = _r if _r is not None else 0.0
            busy = b.get("exl3:busy", 0.0) > 0
            state = "serving" if busy or tps > 0 else "idle"
            live = {"tps": round(tps, 1), "rps": 0.0, "kv": 0,
                    "running": 1 if busy else 0, "waiting": 0,
                    "metrics": "exl3", "resident": True,
                    "tpsSource": "window", "tpsWindowSec": _w}
            _put_prefill_rt(live, m["id"], b.get("exl3:prompt_tokens_total"), _t_now)
        elif m.get("up"):                       # 网关健康但无可识别 /metrics
            state = "online"                    # 在线·服务中，无详细指标（不伪造）
            live = {"metrics": "none", "resident": True}
        else:                                   # 网关判定后端 down → 已停
            state = "stopped"
            live = {"metrics": "none", "resident": False}
        # 后端自报名 → 卡片副标题。路由名可能只是档位别名(DGX-Spark-auto),
        # 面板上必须能看出它背后到底载着什么模型。oMLX 分支已自己填过就不覆盖。
        _sv = m.get("servedName")
        if _sv and not live.get("loadedModel") and _sv != m.get("id"):
            live["loadedModel"] = _sv
        out.append({**card, "state": state, "live": live})
    # 最近一轮结果缓存: /api/nodes 要按模型给节点算吞吐, 但 models_list 每次调用
    # 都做两轮 1.2s 采样(贵), 不能在节点路径上再跑一次。SSE 快照里两者同轮生成,
    # 走 _build_snapshot 注入; 单独访问 /api/nodes 时用这份最近缓存(最旧 2.5s)。
    _MODELS_LAST["ts"], _MODELS_LAST["data"] = time.time(), out
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
            if n.get("alertOffline") is False:
                continue        # 笔记本等:离线是常态,卡片已显示 OFFLINE,不发告警
            out.append({"key": f"{nid}:offline", "sev": "bad",
                        "msg": f"{nm} offline", "sub": f"{n['ip']} · no metrics", "when": "live"})
            continue
        # ⛔ 这些字段可能是 None(该节点没有对应遥测源)。None 一律【跳过判定】——
        #    把「没数据」当成 0 会得出「GPU 0°C 很安全」这种假结论。
        gt, mem, dsk = L.get("tempGpu"), L.get("mem"), L.get("disk")
        if gt is not None and gt >= 90:
            out.append({"key": f"{nid}:gpu_temp", "sev": "bad",
                        "msg": f"{nm} GPU overheating {gt:.0f}°C",
                        "sub": "past critical — shed load", "when": "live"})
        elif gt is not None and gt >= 85:
            out.append({"key": f"{nid}:gpu_temp", "sev": "hot",
                        "msg": f"{nm} GPU hot {gt:.0f}°C",
                        "sub": "near thermal throttle", "when": "live"})
        if mem is not None and mem >= 95:
            out.append({"key": f"{nid}:mem", "sev": "warn",
                        "msg": f"{nm} memory pressure {mem:.0f}%",
                        "sub": "system/unified memory near limit", "when": "live"})
        if dsk is not None and dsk >= 85:
            out.append({"key": f"{nid}:disk", "sev": "warn",
                        "msg": f"{nm} disk {dsk:.0f}%",
                        "sub": "root filesystem filling up", "when": "live"})
        # 逐挂载点容量。原先只有 live.disk(根分区)一条, 数据盘/模型盘写满看不见 ——
        # 五台机器上 /data、/mnt/models 这类分区才是真会被撑爆的。
        # ⛔ 用的是现有 node_filesystem_* 序列, 零新增探针。
        # ⛔ 不依赖任何持久化状态(机主 2026-09-19 裁定): 只看当前值, 不做"增长速率"
        #    判断 —— 那需要历史基线, 而历史归 Prometheus 管, 不归告警规则。
        # ⛔ 按【设备】去重, 不是按挂载点: Atlas 的 /dev/nvme3n1p1 同时挂在 /data、
        #    /out、/workspace/artifacts/... 四处(bind mount), 按挂载点报会把一个
        #    满盘刷成四条告警, 把真正的其它问题挤出列表。代表挂载点取最短路径。
        _fs: dict = {}
        for mnt in (n.get("facts") or {}).get("mounts") or []:
            up_, mp = mnt.get("usedPct"), mnt.get("mount") or ""
            if up_ is None or mp == "/" or up_ < 90:
                continue              # 根分区上面那条 disk 规则已经覆盖, 不重复报
            dv = mnt.get("device") or mp
            cur = _fs.get(dv)
            if cur is None or len(mp) < len(cur["mount"]):
                _fs[dv] = {**mnt, "mount": mp, "n": (cur or {}).get("n", 0) + 1}
            else:
                cur["n"] += 1
        for dv, mnt in _fs.items():
            more = f" (+{mnt['n'] - 1} more mounts)" if mnt["n"] > 1 else ""
            out.append({"key": f"{nid}:fs:{dv}",
                        "sev": "bad" if mnt["usedPct"] >= 95 else "warn",
                        "msg": f"{nm} {mnt['mount']} {mnt['usedPct']:.0f}%{more}",
                        "sub": f"{mnt.get('availGb')} GB left on {dv}",
                        "when": "live"})
        # GPU 硬件健康。XID 是 NVIDIA 驱动报的致命/非致命错误码, 非零一律当事故看;
        # ECC 双比特(DBE)不可纠, 单比特(SBE)可纠但持续增长说明显存在退化。
        # 这三个计数器现有 DCGM 采集里就有(五台全覆盖), 不额外增加被监控机负担。
        gh = (n.get("facts") or {}).get("gpuHealth") or {}
        if gh.get("xid"):
            sub = gh.get("xidMsg") or "check dmesg / nvidia-bug-report"
            out.append({"key": f"{nid}:xid", "sev": "bad",
                        "msg": f"{nm} GPU XID error {gh['xid']}",
                        "sub": sub, "when": "live"})
        if gh.get("eccDbe"):
            out.append({"key": f"{nid}:ecc_dbe", "sev": "bad",
                        "msg": f"{nm} GPU ECC double-bit {gh['eccDbe']}",
                        "sub": "uncorrectable — retire page / RMA check", "when": "live"})
        elif gh.get("eccSbe"):
            out.append({"key": f"{nid}:ecc_sbe", "sev": "warn",
                        "msg": f"{nm} GPU ECC single-bit {gh['eccSbe']}",
                        "sub": "correctable, but a rising count means degradation", "when": "live"})
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
# ── 日峰值落盘 ──────────────────────────────────────────────────────
# 机主 2026-09-19 裁定: 只落「每日最高解码/预填 tok/s」这类小标量, 写独立 JSON,
# 不混进 hearth.yaml; 曲线历史继续交给 Prometheus(job=vllm 存 180 天),
# Hearth 其余部分保持无状态。
# ⛔ 文件可删: 删掉后 Hearth 必须照常启动, 只是日峰值从头开始。所以这里【所有】
#    读写异常都就地降级成"内存里记着", 绝不往上抛。
# ⛔ 告警规则不许依赖这份状态(同一条裁定) —— 本模块只被 _build_snapshot 与
#    /api/peaks 使用, _alerts 不读它。
PEAKS_FILE = os.environ.get(
    "HEARTH_PEAKS_FILE", os.path.expanduser("~/.local/state/hearth/daily-peaks.json"))
PEAKS_KEEP_DAYS = 14
_PEAKS_WRITE_EVERY = 60.0        # 峰值变了也最多每分钟落一次盘, 避免 2.5s 写一次
# error    = 最近一次【写入】失败的原因(写成功就清掉)
# load_note = 启动时【读取】发现的问题(文件损坏/没权限), 写成功【不】清 —— 那是
#             两件事: "这次写进去了"不代表"上次那份没丢"。实测踩过: 损坏文件被
#             丢弃后第一次落盘成功, note 被清成 None, 外面就看不出丢过数据。
_PEAKS: dict = {"data": None, "dirty": False, "last_write": 0.0,
                "persisted": True, "error": None, "load_note": None}


def _peaks_day() -> str:
    """按 display.timezone 算"今天"。配置没给时区就用 UTC —— 不猜浏览器时区,
    否则同一份落盘文件会因为看的人不同而跨日错位。"""
    tz = (HEARTH_CFG.get("display") or {}).get("timezone")
    if tz:
        try:
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo(tz)).strftime("%Y-%m-%d")
        except Exception:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _peaks_load() -> dict:
    """读落盘文件。文件不存在 / 损坏 / 没权限 → 空结构 + 记下原因, 不抛。"""
    if _PEAKS["data"] is not None:
        return _PEAKS["data"]
    data = {"version": 1, "days": {}}
    try:
        with open(PEAKS_FILE, "r") as f:
            got = json.load(f)
        if isinstance(got, dict) and isinstance(got.get("days"), dict):
            data = {"version": 1, "days": got["days"]}
    except FileNotFoundError:
        pass                                   # 正常: 第一次跑, 或被人删了
    except Exception as e:
        _PEAKS["load_note"] = (f"上次的落盘文件读不出来({e.__class__.__name__}), "
                               f"已丢弃并从头开始: {PEAKS_FILE}")
    # 迁移: 2026-09-19 之前 prefillTps 记的是【生命周期累计平均】(空闲也有值),
    # 改名后实时值另存 prefillTpsRealtime。旧键留着会让面板/接口把 1700 当成
    # "今天最高预填 1700 tok/s" —— 同一类假数, 读到就丢掉, 不做换算。
    for _d in (data.get("days") or {}).values():
        for _mb in (_d.get("models") or {}).values():
            _mb.pop("prefillTps", None)
    _PEAKS["data"] = data
    return data


def _peaks_save(force: bool = False) -> None:
    now = time.time()
    if not _PEAKS["dirty"]:
        return
    if not force and now - _PEAKS["last_write"] < _PEAKS_WRITE_EVERY:
        return
    try:
        os.makedirs(os.path.dirname(PEAKS_FILE) or ".", exist_ok=True)
        tmp = f"{PEAKS_FILE}.tmp"
        with open(tmp, "w") as f:
            json.dump(_PEAKS["data"], f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, PEAKS_FILE)            # 原子替换: 半截文件读起来是损坏
        _PEAKS["dirty"], _PEAKS["last_write"] = False, now
        _PEAKS["persisted"], _PEAKS["error"] = True, None
    except Exception as e:
        # 写不进去(只读挂载 / 无权限)不是致命错: 面板照常跑, 只是重启后丢。
        _PEAKS["persisted"] = False
        _PEAKS["error"] = f"写入失败({e.__class__.__name__}): {PEAKS_FILE}"


def _peak_bump(bucket: dict, key: str, value, when: str) -> bool:
    """value 更高才覆盖。⛔ None / 非数 / <=0 一律不记 —— 0 不是峰值, 记进去
    会让"今天还没跑过任何请求"看起来像"今天峰值 0 tok/s"。"""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    if v <= 0:
        return False
    old = bucket.get(key)
    if isinstance(old, dict) and float(old.get("value", 0)) >= v:
        return False
    bucket[key] = {"value": round(v, 1), "at": when}
    return True


def _record_peaks(models: list, cl: dict) -> None:
    """每轮快照更新当日峰值。只记小标量, 不记曲线。"""
    data = _peaks_load()
    day = _peaks_day()
    d = data["days"].setdefault(day, {})
    when = datetime.now(timezone.utc).isoformat(timespec="seconds")
    # ⛔ 两个集群口径【不是一回事】, 分开存, 名字里写清楚:
    #   gatewayTokensTps = sum(rate(litellm_total_tokens_metric_total[1m])),
    #     LiteLLM 记的是每请求【总 token(prompt+completion)】, 长 prompt 会让它
    #     远高于真实解码速度(实测 874.8 vs 引擎侧解码 70)。
    #   decodeTps = 各模型引擎侧 live.tps 之和, 才是真的"每秒吐出多少 token"。
    changed = _peak_bump(d, "gatewayTokensTps", (cl.get("live") or {}).get("tpsNow"), when)
    _dec = sum(float((m.get("live") or {}).get("tps") or 0) for m in (models or []))
    changed |= _peak_bump(d, "decodeTps", _dec, when)
    per = d.setdefault("models", {})
    for m in models or []:
        lv = m.get("live") or {}
        mid = m.get("id")
        if not mid:
            continue
        mb = per.setdefault(mid, {})
        changed |= _peak_bump(mb, "decodeTps", lv.get("tps"), when)
        # 键名自带口径: 记的是【实时】prefill 速率的当日最高值, 不是累计平均。
        # (2026-09-19 改名; 旧键 prefillTps 在历史文件里最多再留 14 天自然过期)
        changed |= _peak_bump(mb, "prefillTpsRealtime", lv.get("prefillTokPerS"), when)
        if not mb:                             # 一天下来一个峰值都没有 → 不留空壳
            per.pop(mid, None)
    if len(data["days"]) > PEAKS_KEEP_DAYS:    # 只留最近 N 天, 文件恒定大小
        for k in sorted(data["days"])[:-PEAKS_KEEP_DAYS]:
            data["days"].pop(k, None)
        changed = True
    if changed:
        _PEAKS["dirty"] = True
    _peaks_save()


@app.get("/api/peaks")
async def peaks():
    """最近 PEAKS_KEEP_DAYS 天的日峰值。文件被删 → days 为空, 不是错误。"""
    data = _peaks_load()
    days = data.get("days") or {}
    best = {}
    for day, d in days.items():
        for k, v in d.items():
            if k == "models" or not isinstance(v, dict):
                continue
            if float(v.get("value", 0)) > float((best.get(k) or {}).get("value", 0)):
                best[k] = {"value": v["value"], "day": day, "at": v.get("at")}
    return {"today": _peaks_day(), "keepDays": PEAKS_KEEP_DAYS,
            "days": days, "best": best,
            # 落盘状态如实暴露: persisted=false 表示这批峰值重启会丢。
            "file": PEAKS_FILE, "persisted": _PEAKS["persisted"],
            "note": _PEAKS["error"], "loadNote": _PEAKS["load_note"]}


# TICK_SEC 发最新快照 → 前端每 1.5s 收帧平滑重渲染，数据 ~2.5s 新鲜。
_SNAP = {"ts": 0.0, "data": None}
_SNAP_TTL = 2.5


async def _build_snapshot() -> dict:
    nodes = await _node_payload()                 # 贵, 只算一次
    log = await _litellm_request_log(40)
    cl, models, training, infra_dev = await asyncio.gather(
        cluster(), models_list(), _training_payload(), _infra_payload())
    _attach_node_throughput(nodes, models)         # 同轮数据, 节点与模型口径一致
    al = await _alerts(nodes, log)                # 复用 nodes/log, 不重复跑
    await _notify_alerts(al)                       # 推送渠道(跳变才发, 不阻塞失败)
    try:
        _record_peaks(models, cl)                  # 日峰值落盘; 失败只降级不影响快照
    except Exception:
        pass
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


@app.on_event("startup")
async def _start_background_sampling() -> None:
    """进程起来就开始采样，不等第一个浏览器。

    ⛔ 这不是优化, 是正确性问题: tps / 延迟分位数全是【滑动窗口】量, 窗口靠
    _snap_loop 每 2.5s 调一次 _build_snapshot 来推进。而 _snapshot() 是惰性启动的,
    只有 /api/stream 被访问时才创建这个 task —— 重启后没人开页面, 窗口就一直是空的,
    第一个访问者看到的是"warming up"而不是真实基线(2026-09-19 核查发现)。
    启动即拉起后, 页面一打开就是攒好的窗口。

    on_event 在 FastAPI 0.115 上已标记 deprecated 但仍生效; 换 lifespan 需要在
    app 创建处就持有函数对象, 而 _snap_loop 定义在其后, 故沿用 on_event。"""
    global _SNAP_TASK
    if _SNAP_TASK is None or _SNAP_TASK.done():
        _SNAP_TASK = asyncio.create_task(_snap_loop())


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
           "ac": {}, "wall": {}, "cabinet": {}, "gpu": {}}

    for name, mins in windows:
        ac_series   = await _window_series("ha_rack_ac_power_watts", mins, STEP)
        wall_series = await _window_series("sum(ha_node_wall_power_watts)", mins, STEP)
        cab_series  = await _window_series("avg(ha_node_plug_temp_celsius)", mins, STEP)
        # GPU 侧口径: 五张卡的 DCGM 功率之和。⛔ 不含 CPU/内存/风扇/电源损耗,
        # 与上面 wall(智能插座整机口径)不可互换, 界面上必须分开标。
        gpu_series  = await _window_series("sum(DCGM_FI_DEV_POWER_USAGE)", mins, STEP)
        out["ac"][name]   = _agg(ac_series, tz, STEP)
        out["wall"][name] = _agg(wall_series, tz, STEP)
        out["gpu"][name]  = _agg(gpu_series, tz, STEP)
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

    # ⛔ 数据源自述。界面据此在【某个口径完全没数据】时明写"数据源不可用",
    #    而不是留一排 0 或一排空格让人以为"这段时间没耗电"。
    #    HA 五个插座实体自 2026-08-08/09 起全部 unavailable → wall/ac/cabinet
    #    现在就是 available=false, 这不是故障, 是如实反映。
    ha_ok = await _ha_ok()

    def _avail(block):
        return any((block.get(w) or {}).get("samples", 0) > 0 for w, _ in windows)
    out["sources"] = {
        "gpu":     {"metric": "DCGM_FI_DEV_POWER_USAGE", "scope": "gpu-only",
                    "note": "GPU 功耗, 不含整机", "available": _avail(out["gpu"])},
        "wall":    {"metric": "ha_node_wall_power_watts", "scope": "whole-machine",
                    "note": "智能插座整机功耗",
                    "available": ha_ok and _avail(out["wall"]),
                    "reason": None if ha_ok else "HA exporter down (ha_up=0)"},
        "ac":      {"metric": "ha_rack_ac_power_watts", "scope": "rack-ac",
                    "note": "机架空调",
                    "available": ha_ok and _avail(out["ac"]),
                    "reason": None if ha_ok else "HA exporter down (ha_up=0)"},
        "cabinet": {"metric": "ha_node_plug_temp_celsius", "scope": "cabinet-proxy",
                    "note": "插座内部温度, 机柜环境温度的代理量",
                    "available": ha_ok and any((out["cabinet"].get(w) or {}).get("samples", 0) > 0
                                               for w, _ in windows),
                    "reason": None if ha_ok else "HA exporter down (ha_up=0)"},
    }
    # HA 挂着时把 ac/wall/cabinet 的数字整块清成 null —— 留着 0.0 W / 0.00 kWh
    # 就是拿 0 冒充无数据(机主 09-19 明令禁止)。
    if not ha_ok:
        for blk in ("ac", "wall"):
            for w, _ in windows:
                out[blk][w] = {"avgW": None, "kwh": None, "dayAvgW": None,
                               "nightAvgW": None, "samples": 0}
        for w, _ in windows:
            out["cabinet"][w] = {"meanC": None, "minC": None, "maxC": None,
                                 "dayMeanC": None, "nightMeanC": None, "samples": 0}
    return out


# ── 逐单元连通性自检 ────────────────────────────────────────────────
# 面板上「没数据」有十几种原因:exporter 挂了、标签配错、隧道断了、网关 401、
# 声明的地址是 TP 分片……现在只能看到空白。这个接口把每个单元的每项能力拆成
# pass / fail / skipped 三态,并给出【下一步查什么】。
#
# ⛔ 只复用已有的探测路径(已经在跑的 Prometheus 查询、已发现的模型、已缓存的
#    探针结果),不新增任何对被监控机的命令或请求 —— 自检本身不能变成负载。
# ⛔ skipped ≠ pass。"这台机器本来就没有 GPU 遥测源"是 skipped,要和"有源但探
#    不到"分开,否则自检会把缺能力伪装成健康。
def _chk(unit: str, cap: str, status: str, detail: str, hint: str = "") -> dict:
    return {"unit": unit, "capability": cap, "status": status,
            "detail": detail, "hint": hint}


@app.get("/api/selftest")
async def selftest():
    nodes = await _node_payload()
    disco = await _disco_cached()
    out: list[dict] = []

    # ── 基础设施 ────────────────────────────────────────────────
    try:
        r = await client.get(f"{PROM_URL}/-/healthy", timeout=4.0)
        ok = r.status_code == 200
    except Exception as e:
        ok, r = False, None
        out.append(_chk("obs", "prometheus", "fail", f"{PROM_URL} 不可达: {e}",
                        "确认 obs 栈在跑: docker ps | grep prometheus"))
    if ok:
        out.append(_chk("obs", "prometheus", "pass", f"{PROM_URL} healthy"))
    elif r is not None:
        out.append(_chk("obs", "prometheus", "fail", f"{PROM_URL} HTTP {r.status_code}", ""))

    gw = await _gw_get("/health", 6.0)
    out.append(_chk("gateway", "litellm", "pass" if gw is not None else "fail",
                    "LiteLLM /health 可达" if gw is not None else f"{LITELLM_URL}/health 无响应或鉴权失败",
                    "" if gw is not None else "查 LITELLM_MASTER_KEY 与容器状态"))

    # 网关指标是 tpsNow/rpsNow 的唯一来源。2026-09-19 就因为抓取任务 401 + 指标名
    # 少了 _total 后缀, 这两个值恒 0 而界面看不出来 —— 自检必须能抓到这种"静默 0"。
    lt = await promql("sum(litellm_total_tokens_metric_total)")
    out.append(_chk("gateway", "litellm_metrics", "pass" if lt else "fail",
                    "litellm_total_tokens_metric_total 有序列" if lt
                    else "Prometheus 里没有 litellm_total_tokens_metric_total",
                    "" if lt else "查 obs 的 litellm 抓取任务(常见: 401 缺 Bearer、指标名少 _total)"))

    # ── 逐节点 ─────────────────────────────────────────────────
    probe_cfg = {n["id"]: n.get("gpu_probe_ssh") for n in NODES}
    for n in nodes:
        u, lv = f"node:{n['id']}", n.get("live") or {}
        src = next((x.get("node_source") for x in NODES if x["id"] == n["id"]), None)
        out.append(_chk(u, "reachable", "pass" if n.get("up") else "fail",
                        f"来源={src or 'obs'} · up={n.get('up')}",
                        "" if n.get("up") else
                        ("直采节点: 查 exporter 与隧道(hearth.yaml 的 sources.node_exporter_url)"
                         if src == "exporter" else
                         "obs 节点: 查 node_exporter 与 file_sd 里的 node= 标签是否与 obs_node_label 一致")))
        # GPU 遥测: 三态分明
        if n.get("gpuTelemetry") is False and not lv.get("gpuUtilSource"):
            out.append(_chk(u, "gpu_telemetry", "skipped", "该节点没有 GPU 遥测源(无 DCGM)",
                            "Apple Silicon 可配 sources.gpu_probe_ssh 走 ioreg"))
        elif lv.get("gpu") is None:
            out.append(_chk(u, "gpu_telemetry", "fail", "有遥测源但取不到 GPU 利用率",
                            "查 DCGM_FI_DEV_GPU_UTIL 是否还有该 node= 的序列"))
        else:
            out.append(_chk(u, "gpu_telemetry", "pass",
                            f"gpu={lv['gpu']}% 来源={lv.get('gpuUtilSource') or 'dcgm'}"))
        # 功耗(能耗口径的输入)
        if lv.get("power") is None:
            out.append(_chk(u, "power", "skipped" if n.get("gpuTelemetry") is False
                            else "fail", "没有功耗读数",
                            "macOS 需 root 才能读 GPU 功耗, 属已知限制"
                            if n.get("gpuTelemetry") is False else
                            "查 DCGM_FI_DEV_POWER_USAGE 该节点序列"))
        else:
            out.append(_chk(u, "power", "pass", f"{lv['power']} W"))
        # 慢变事实(存储/网卡/开机时长/GPU 健康)
        f = n.get("facts") or {}
        if f:
            out.append(_chk(u, "node_facts", "pass",
                            f"挂载点 {len(f.get('mounts') or [])} · 网卡 {len(f.get('nics') or [])}"))
        else:
            out.append(_chk(u, "node_facts", "skipped" if src == "exporter" else "fail",
                            "无节点事实数据",
                            "直采节点不在 obs 里, 这些事实查不到, 属预期"
                            if src == "exporter" else "查 node_filesystem_* / node_network_info 序列"))
        # GPU 硬件健康。ECC 计数器缺席【不是故障】: GB10 的 LPDDR5X 没有 ECC,
        # 采集端 2026-09-19 起不再伪造 0。只要 XID 在, 这条信号就是全的。
        gh = (n.get("facts") or {}).get("gpuHealth") or {}
        if gh:
            has_ecc = ("eccSbe" in gh) or ("eccDbe" in gh)
            out.append(_chk(u, "gpu_health", "pass",
                            f"XID={gh.get('xid')} · ECC 计数器"
                            + ("有" if has_ecc else "无(该 GPU 不导出, 非故障)")))
        elif n.get("gpuTelemetry") is False:
            out.append(_chk(u, "gpu_health", "skipped", "该节点没有 GPU 健康数据源"))
        else:
            out.append(_chk(u, "gpu_health", "fail", "有 GPU 遥测源但取不到 XID",
                            "查 DCGM_FI_DEV_XID_ERRORS 该 node= 的序列"))
        # SSH GPU 探针(只有配了的节点才判)
        if probe_cfg.get(n["id"]):
            got = (_GPU_PROBE.get("data") or {}).get(n["id"])
            out.append(_chk(u, "gpu_probe_ssh", "pass" if got else "fail",
                            f"ioreg 探针 {'有' if got else '无'}数据 · 目标 {probe_cfg[n['id']]}",
                            "" if got else "查免密 SSH(BatchMode)与主机是否休眠; 笔记本合盖会失败"))

    # ── 逐模型 ─────────────────────────────────────────────────
    for m in disco:
        u = f"model:{m['id']}"
        kinds = [k for k in ("vllm_bases", "llamacpp_bases", "sglang_bases", "omlx_bases",
                             "ds4_bases", "q27_bases", "exl3_bases") if m.get(k)]
        out.append(_chk(u, "engine_metrics", "pass" if kinds else ("fail" if m.get("up") else "skipped"),
                        f"framework={m.get('framework')} · 识别到的端点组={kinds or '无'}",
                        "" if kinds else ("在线但没有任何已知引擎指标口径: 确认引擎类型与 /metrics 路径"
                                          if m.get("up") else "模型未拉起 → 指标缺席属预期")))
        if m.get("source") == "direct":
            out.append(_chk(u, "gateway_route", "skipped", "直探条目, 没挂网关 → 无路由",
                            "要走网关就在 LiteLLM 里加一条 model 配置"))
        else:
            out.append(_chk(u, "gateway_route", "pass" if m.get("route") else "fail",
                            f"route={m.get('route')}"))
        if m.get("identityUnverified"):
            sv = m.get("servedName")
            if sv:
                # 后端报了名字, 只是与网关路由对不上 —— 常见于"档位别名"型路由
                # (DGX-Spark-auto/high/low...), 主名只能按字母序猜。不是故障。
                out.append(_chk(u, "identity", "skipped",
                                f"后端自报 '{sv}', 与网关路由对不上 → 主名按字母序取自 "
                                f"{m.get('identityCandidates') or '—'}",
                                "想让面板显示真名: 在网关加一条与 served-model-name 同名的路由, "
                                "或在 hearth.yaml 的 model_meta 里给该主名配 display"))
            else:
                out.append(_chk(u, "identity", "fail",
                                "身份未核实: 后端没报 served-model-name(多半没拉起)",
                                f"候选: {m.get('identityCandidates') or '—'} · 后端起来后会自动核实"))

    # ── 直探声明里被跳过的地址 ──────────────────────────────────
    for b, why in _DIRECT_SKIPPED.items():
        out.append(_chk(f"direct:{b}", "serving_endpoint", "skipped", why,
                        "这是预期行为: TP/PP 分片不该当成独立模型; 若想看分片指标请用节点视图"))

    summary = {k: sum(1 for c in out if c["status"] == k) for k in ("pass", "fail", "skipped")}
    return {"generatedAt": datetime.now(timezone.utc).isoformat(),
            "summary": summary, "checks": out}


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
