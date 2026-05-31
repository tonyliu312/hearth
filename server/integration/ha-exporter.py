#!/usr/bin/env python3
"""Hearth × Home Assistant Prometheus exporter.

Reads `hearth.yaml` (path from $HEARTH_CONFIG, default /etc/hearth/config.yaml)
to build the node→cuco-plug mapping at startup — operators declare topology in
config, not in code. Exposes `ha_*` Prometheus metrics on :9105/metrics so the
obs Prometheus can scrape and the FastAPI cluster payload can join `ha_*` with
`node_*` / `DCGM_*` by `node` label.

Polls Home Assistant REST API (`GET /api/states/<entity_id>`) every
HA_POLL_INTERVAL seconds; failures don't emit a sample (lets Prometheus go
stale rather than fake a 0).

Hard safety:
  - blocklist enforced at startup — if any configured plug_id is in the
    blocklist (MT6000 router by default) the exporter refuses to start
  - only reads HA state; never POSTs to /api/services/*
"""
from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

import requests
from prometheus_client import Gauge, start_http_server

log = logging.getLogger("ha-exporter")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

HEARTH_CONFIG_PATH = os.environ.get("HEARTH_CONFIG", "/etc/hearth/config.yaml")
HA_URL_ENV = os.environ.get("HA_URL")
HA_TOKEN = os.environ.get("HA_TOKEN", "")
PORT = int(os.environ.get("HA_EXPORTER_PORT", "9105"))
INTERVAL = float(os.environ.get("HA_POLL_INTERVAL", "15"))
TIMEOUT = float(os.environ.get("HA_HTTP_TIMEOUT", "5"))

# MT6000 router is never monitored regardless of operator config — turning it
# off would isolate the whole LAN. The exporter refuses to start if any
# configured plug_id matches this list.
HARD_BLOCKLIST = {"2051674991"}


def load_config(path: str) -> dict:
    try:
        import yaml
    except ImportError:
        log.error("pyyaml not installed — pip install pyyaml")
        sys.exit(2)
    p = Path(path)
    if not p.is_file():
        log.error("HEARTH_CONFIG not found: %s", path)
        sys.exit(2)
    try:
        return yaml.safe_load(p.read_text()) or {}
    except Exception as e:
        log.error("failed to parse %s: %s", path, e)
        sys.exit(2)


def build_runtime_config(cfg: dict) -> dict:
    """Pluck the HA-related fields from hearth.yaml; validate against blocklist."""
    ha = cfg.get("ha") or {}
    base_url = (HA_URL_ENV or ha.get("base_url")
                or "http://homeassistant.local:8123").rstrip("/")
    blocklist = set(ha.get("blocklist") or []) | HARD_BLOCKLIST
    node_plugs: dict[str, str] = {}
    for n in cfg.get("nodes") or []:
        pid = ((n.get("sources") or {}).get("ha_plug_id") or "").strip()
        if not pid:
            continue
        if pid in blocklist:
            log.error("node %s.sources.ha_plug_id=%s is in blocklist — refusing to start",
                      n.get("id"), pid)
            sys.exit(3)
        node_plugs[n["id"]] = pid
    rack_sensor = (ha.get("rack_sensor_id") or "").strip() or None
    rack_ac = (ha.get("rack_ac_plug_id") or "").strip() or None
    if rack_ac and rack_ac in blocklist:
        log.error("ha.rack_ac_plug_id=%s in blocklist — refusing to start", rack_ac)
        sys.exit(3)
    return {"base_url": base_url, "node_plugs": node_plugs,
            "rack_sensor": rack_sensor, "rack_ac_plug": rack_ac}


# ── Metrics ─────────────────────────────────────────────────────────────
g_up            = Gauge("ha_up", "HA REST reachability (1=ok, 0=down)")
g_wall_power    = Gauge("ha_node_wall_power_watts",
                        "Per-node wall-socket power (W) measured by HA smart plug",
                        ["node"])
g_plug_state    = Gauge("ha_node_plug_state",
                        "Per-node smart-plug state (1=on, 0=off)", ["node"])
g_ac_power      = Gauge("ha_rack_ac_power_watts", "Rack AC power (W)")
g_ac_state      = Gauge("ha_rack_ac_state", "Rack AC on/off (1=on, 0=off)")
g_ac_inner_temp = Gauge("ha_rack_ac_plug_temp_celsius",
                        "Rack AC plug internal temperature (°C)")
g_rack_temp     = Gauge("ha_rack_temperature_celsius",
                        "Rack environmental temperature (°C)")
g_rack_humidity = Gauge("ha_rack_humidity_percent",
                        "Rack environmental relative humidity (%)")
g_rack_battery  = Gauge("ha_rack_sensor_battery_percent",
                        "Rack temp/humidity sensor battery (%)")


def make_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}"})
    return s


def get_state(session: requests.Session, base_url: str, entity_id: str):
    try:
        r = session.get(f"{base_url}/api/states/{entity_id}", timeout=TIMEOUT)
        r.raise_for_status()
        d = r.json()
        return d.get("state"), d.get("attributes") or {}
    except Exception as e:
        log.warning("get_state(%s) failed: %s", entity_id, e)
        return None, {}


def as_float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def scrape_once(session: requests.Session, rt: dict) -> None:
    ok = True
    base = rt["base_url"]

    for node, cid in rt["node_plugs"].items():
        pw, _ = get_state(session, base,
                          f"sensor.cuco_cn_{cid}_v3_electric_power_p_11_2")
        sw, _ = get_state(session, base, f"switch.cuco_cn_{cid}_v3_on_p_2_1")
        v = as_float(pw)
        if v is not None:
            g_wall_power.labels(node=node).set(v)
        else:
            ok = False
        if sw in ("on", "off"):
            g_plug_state.labels(node=node).set(1 if sw == "on" else 0)

    if rt["rack_ac_plug"]:
        cid = rt["rack_ac_plug"]
        pw, _ = get_state(session, base,
                          f"sensor.cuco_cn_{cid}_v3_electric_power_p_11_2")
        sw, _ = get_state(session, base, f"switch.cuco_cn_{cid}_v3_on_p_2_1")
        tp, _ = get_state(session, base,
                          f"sensor.cuco_cn_{cid}_v3_temperature_p_12_2")
        if (v := as_float(pw)) is not None:
            g_ac_power.set(v)
        if sw in ("on", "off"):
            g_ac_state.set(1 if sw == "on" else 0)
        if (v := as_float(tp)) is not None:
            g_ac_inner_temp.set(v)

    if rt["rack_sensor"]:
        sid = rt["rack_sensor"]
        t, _ = get_state(session, base, f"sensor.{sid}_temperature_p_3_1001")
        h, _ = get_state(session, base, f"sensor.{sid}_relative_humidity_p_3_1002")
        b, _ = get_state(session, base, f"sensor.{sid}_battery_level_p_2_1003")
        if (v := as_float(t)) is not None:
            g_rack_temp.set(v)
        if (v := as_float(h)) is not None:
            g_rack_humidity.set(v)
        if (v := as_float(b)) is not None:
            g_rack_battery.set(v)

    g_up.set(1 if ok else 0)


def main() -> None:
    if not HA_TOKEN:
        log.error("HA_TOKEN not set — read from ~/.config/ha/token "
                  "and pass via env (root-only systemd drop-in recommended)")
        sys.exit(2)

    cfg = load_config(HEARTH_CONFIG_PATH)
    rt = build_runtime_config(cfg)
    if not rt["node_plugs"] and not rt["rack_sensor"] and not rt["rack_ac_plug"]:
        log.warning("no HA plug/sensor configured in %s — exporter will run "
                    "but emit no metrics. Set nodes[].sources.ha_plug_id or "
                    "ha.rack_sensor_id / ha.rack_ac_plug_id to enable.",
                    HEARTH_CONFIG_PATH)

    log.info("Hearth HA exporter — base_url=%s port=%d interval=%.0fs "
             "nodes=%d ac=%s sensor=%s",
             rt["base_url"], PORT, INTERVAL, len(rt["node_plugs"]),
             "y" if rt["rack_ac_plug"] else "n",
             "y" if rt["rack_sensor"] else "n")
    session = make_session(HA_TOKEN)
    start_http_server(PORT)
    while True:
        try:
            scrape_once(session, rt)
        except Exception as e:
            log.error("scrape_once error: %s", e)
            g_up.set(0)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
