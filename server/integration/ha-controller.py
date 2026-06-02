#!/usr/bin/env python3
"""Hearth · GPU-temperature-driven AC controller (Layer 2 override).

ARCHITECTURE — two-layer thermal control

  Layer 1 (AC built-in):  F01/F02 setpoints on the AC LCD panel.  Whenever
    the plug has power, the AC's own controller decides when to run the
    compressor based on return-air temperature.  This Layer 1 must work
    correctly on its own; it is the safety baseline whenever Layer 2 is
    absent or has failed.

  Layer 2 (this script):  Reads max(DCGM_FI_DEV_GPU_TEMP) — the temperature
    of what we actually care about (the GPUs) — and can ONLY DE-ENERGIZE
    the AC plug to save power when the GPUs are demonstrably cool.  It
    cannot force-cool: the only way Layer 2 turns the AC "on" is by
    energizing the plug, after which Layer 1 decides whether to run the
    compressor.  This guarantees Layer 2 never wastes power that Layer 1
    wouldn't have spent.

This asymmetry — "L2 can only turn the plug off, never override L1's stop"
— is what mathematically makes "L2 enabled" ≥ "L2 disabled" in energy
savings: each OFF second is a strict win, and worst case L2 keeps the
plug on forever and degrades cleanly to L1-only behavior.

SAFETY RAILS (all unconditional)

  - target_plug_id must not be in `ha.blocklist` or in HARD_BLOCKLIST.
  - Minimum switch interval (default 5 min) — compressor protection.
  - Maximum OFF duration (default 10 min) — even if L2 logic believes the
    plug should stay off, force it back on after this window.  Bounds
    L2's damage if its decision logic ever goes wrong: at most 10 minutes
    without cooling before L1 retakes control.
  - Emergency open: max_gpu ≥ emergency_open_threshold for ≥30s → force
    plug on regardless of minimum-interval lock.
  - Fail-safe: any missing signal (max_gpu, plug state, HA unreachable)
    → if plug is OFF, force ON; if plug is ON, leave alone.  Default
    failure mode of an HVAC controller must never be "no cooling".

State persists across restarts in HC_STATE_FILE so the minimum-interval
lock + max-OFF-duration timer survive a process bounce.

Exposes Prometheus metrics on :9106/metrics for Hearth's dashboard.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import sys
import time

import httpx
from prometheus_client import Counter, Gauge, start_http_server

log = logging.getLogger("ha-controller")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

HEARTH_CONFIG_PATH = os.environ.get("HEARTH_CONFIG", "/etc/hearth/config.yaml")
HA_URL = os.environ.get("HA_URL", "http://homeassistant.local:8123").rstrip("/")
HA_TOKEN = os.environ.get("HA_TOKEN", "")
PROM_URL = os.environ.get("PROMETHEUS_URL", "http://127.0.0.1:9090").rstrip("/")
PORT = int(os.environ.get("HA_CONTROLLER_PORT", "9106"))
HC_STATE_FILE = pathlib.Path(os.environ.get(
    "HEARTH_CONTROLLER_STATE", "/var/lib/hearth/ac-controller.json"))

# Hard blocklist (cannot be overridden by config; permanent constants).
HARD_BLOCKLIST = {"2051674991"}                # MT6000 router

# Metrics.
g_decision   = Gauge("hearth_ac_controller_decision",
                     "Current decision: 1=should be on, 0=should be off")
g_actual     = Gauge("hearth_ac_controller_actual_state",
                     "Actual plug state read from HA: 1=on, 0=off")
g_max_gpu    = Gauge("hearth_ac_controller_max_gpu_celsius",
                     "max(DCGM_FI_DEV_GPU_TEMP) used in last decision")
g_in_state_s = Gauge("hearth_ac_controller_in_state_seconds",
                     "Seconds since last state transition")
g_alive      = Gauge("hearth_ac_controller_alive",
                     "1 if controller loop ran successfully in last interval")
c_switches   = Counter("hearth_ac_controller_switches_total",
                       "Total plug switches", ["direction"])
c_emergency  = Counter("hearth_ac_controller_emergency_opens_total",
                       "Times the emergency-open path fired")
c_failsafe   = Counter("hearth_ac_controller_failsafe_total",
                       "Times the fail-safe path fired (signal missing)")
c_maxoff     = Counter("hearth_ac_controller_max_off_forced_total",
                       "Times max-OFF-duration forced the plug back on")
g_last_dec_ts = Gauge("hearth_ac_controller_last_decision_ts",
                      "Unix timestamp of last successful decide() loop")


def _load_config() -> dict:
    try:
        import yaml
    except ImportError:
        log.error("pyyaml not installed")
        sys.exit(2)
    p = pathlib.Path(HEARTH_CONFIG_PATH)
    if not p.is_file():
        log.error("HEARTH_CONFIG not found: %s", HEARTH_CONFIG_PATH)
        sys.exit(2)
    return yaml.safe_load(p.read_text()) or {}


def _read_state() -> dict:
    try:
        return json.loads(HC_STATE_FILE.read_text())
    except Exception:
        return {"last_switch_ts": 0.0, "emergency_first_ts": 0.0,
                "last_decision": None}


def _write_state(state: dict) -> None:
    try:
        HC_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        HC_STATE_FILE.write_text(json.dumps(state))
    except Exception as e:
        log.warning("state file write failed: %s", e)


class Controller:
    def __init__(self, cfg: dict):
        self.target_plug = cfg["target_plug_id"]
        # Whitelist of cuco IDs Hearth is allowed to ever write to.  Hard
        # blocklist always wins over any operator configuration.
        if self.target_plug in HARD_BLOCKLIST:
            log.error("REFUSING to start: target_plug_id=%s is in HARD_BLOCKLIST",
                      self.target_plug)
            sys.exit(3)
        operator_blocklist = set(cfg.get("blocklist") or [])
        if self.target_plug in operator_blocklist:
            log.error("REFUSING to start: target_plug_id=%s is in operator blocklist",
                      self.target_plug)
            sys.exit(3)
        self.entity = f"switch.cuco_cn_{self.target_plug}_v3_on_p_2_1"

        self.gpu_open  = float(cfg.get("gpu_open_threshold", 65))
        self.gpu_close = float(cfg.get("gpu_close_threshold", 55))
        self.min_int   = float(cfg.get("min_switch_interval_s", 300))
        self.max_off   = float(cfg.get("max_off_duration_s", 600))
        self.emerg     = float(cfg.get("emergency_open_threshold", 75))
        self.emerg_dur = float(cfg.get("emergency_open_dur_s", 30))
        self.interval  = float(cfg.get("decision_interval_s", 30))
        self.fail_safe = (cfg.get("fail_safe") or "on").lower()
        if self.gpu_open <= self.gpu_close:
            log.error("gpu_open_threshold (%s) must be > gpu_close_threshold (%s)",
                      self.gpu_open, self.gpu_close)
            sys.exit(3)
        self.client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {HA_TOKEN}"}, timeout=10.0)
        self.state = _read_state()

    async def _max_gpu(self) -> float | None:
        try:
            r = await self.client.get(f"{PROM_URL}/api/v1/query",
                params={"query": 'max(DCGM_FI_DEV_GPU_TEMP{node=~"spark.+"})'})
            r.raise_for_status()
            d = r.json()
            if d.get("status") == "success" and d["data"]["result"]:
                return float(d["data"]["result"][0]["value"][1])
        except Exception as e:
            log.warning("max_gpu query failed: %s", e)
        return None

    async def _plug_state(self) -> str | None:
        """Read the plug's actual state from HA REST (not Prometheus —
        Prometheus has 15s scrape lag which can race the control loop)."""
        try:
            r = await self.client.get(f"{HA_URL}/api/states/{self.entity}")
            r.raise_for_status()
            s = r.json().get("state")
            return s if s in ("on", "off") else None
        except Exception as e:
            log.warning("plug state query failed: %s", e)
        return None

    async def _set_plug(self, on: bool) -> bool:
        action = "turn_on" if on else "turn_off"
        try:
            r = await self.client.post(
                f"{HA_URL}/api/services/switch/{action}",
                json={"entity_id": self.entity})
            r.raise_for_status()
            log.info("→ plug %s OK (target=%s)", action, self.target_plug)
            return True
        except Exception as e:
            log.error("→ plug %s FAILED: %s", action, e)
            return False

    async def decide(self) -> None:
        max_gpu = await self._max_gpu()
        actual  = await self._plug_state()
        now = time.time()
        in_state_for = now - self.state.get("last_switch_ts", 0.0)
        g_in_state_s.set(in_state_for)

        # ── Fail-safe: missing signal ───────────────────────────────────
        # Either GPU temp or plug state unreadable → controller cannot
        # safely decide. Default-on policy: if plug is currently off, force
        # it on. If it's already on, leave it.  Never let a sensor outage
        # silently strand the rack without cooling.
        if max_gpu is None or actual is None:
            c_failsafe.inc()
            log.warning("FAIL-SAFE: missing signal (max_gpu=%s actual=%s) → "
                        "policy=%s", max_gpu, actual, self.fail_safe)
            if self.fail_safe == "on" and actual == "off":
                ok = await self._set_plug(True)
                if ok:
                    self.state["last_switch_ts"] = now
                    self.state["last_decision"] = "ON (fail-safe)"
                    c_switches.labels(direction="failsafe_on").inc()
                    _write_state(self.state)
            return

        g_max_gpu.set(max_gpu)
        g_actual.set(1 if actual == "on" else 0)

        # ── Emergency open ──────────────────────────────────────────────
        # If GPU sustained above emergency threshold for emerg_dur seconds,
        # turn plug on regardless of minimum-interval lock.
        if max_gpu >= self.emerg:
            if self.state.get("emergency_first_ts", 0.0) == 0.0:
                self.state["emergency_first_ts"] = now
                _write_state(self.state)
            elif (now - self.state["emergency_first_ts"] >= self.emerg_dur
                  and actual != "on"):
                log.warning("EMERGENCY: max_gpu=%.1f °C for ≥%.0fs → force ON",
                            max_gpu, self.emerg_dur)
                c_emergency.inc()
                ok = await self._set_plug(True)
                if ok:
                    self.state["last_switch_ts"] = now
                    self.state["last_decision"] = "EMERGENCY ON"
                    self.state["emergency_first_ts"] = 0.0
                    c_switches.labels(direction="emergency_on").inc()
                    _write_state(self.state)
                return
        else:
            if self.state.get("emergency_first_ts", 0.0) != 0.0:
                self.state["emergency_first_ts"] = 0.0
                _write_state(self.state)

        # ── Max-OFF-duration safety: cap how long L2 can hold the plug off
        # No matter what L2 logic thinks, if the plug has been OFF longer than
        # max_off_duration_s, force ON and let L1 take over.  Bounds L2's
        # blast radius if its decision logic is buggy.
        if actual == "off" and in_state_for >= self.max_off:
            log.warning("MAX-OFF: plug has been off for %.0fs (≥ %.0fs cap) → "
                        "force ON, return control to Layer 1",
                        in_state_for, self.max_off)
            c_maxoff.inc()
            ok = await self._set_plug(True)
            if ok:
                self.state["last_switch_ts"] = now
                self.state["last_decision"] = "MAX-OFF FORCED ON"
                c_switches.labels(direction="maxoff_on").inc()
                _write_state(self.state)
            return

        # ── Normal hysteresis ────────────────────────────────────────────
        should_on: bool | None = None
        if (actual == "off" and max_gpu >= self.gpu_open
                and in_state_for >= self.min_int):
            should_on = True
        elif (actual == "on" and max_gpu <= self.gpu_close
                and in_state_for >= self.min_int):
            should_on = False

        # Surface "what would I do if I were free" even when locked.
        if should_on is True:
            g_decision.set(1)
        elif should_on is False:
            g_decision.set(0)
        else:
            # Holding current state — decision == actual.
            g_decision.set(1 if actual == "on" else 0)

        if should_on is None:
            log.debug("hold | actual=%s max_gpu=%.1f °C in_state_for=%.0fs",
                      actual, max_gpu, in_state_for)
            return

        ok = await self._set_plug(should_on)
        if ok:
            self.state["last_switch_ts"] = now
            self.state["last_decision"] = "ON" if should_on else "OFF"
            c_switches.labels(direction="on" if should_on else "off").inc()
            log.info("decision: %s | max_gpu=%.1f °C in_state_for=%.0fs",
                     "→ON" if should_on else "→OFF", max_gpu, in_state_for)
            _write_state(self.state)

    async def _heartbeat(self) -> None:
        """Write a unix-timestamp heartbeat to HA so peer hosts can detect
        when this controller (or the whole Atlas) is dead.  Any number of
        cross-host failover watchdogs can poll the same sensor entity and
        unconditionally turn the AC on if the heartbeat goes stale.

        Uses the REST-only state API (POST /api/states/<entity>); the
        sensor doesn't need to be pre-declared in HA configuration.yaml."""
        try:
            await self.client.post(
                f"{HA_URL}/api/states/sensor.hearth_atlas_heartbeat",
                json={"state": str(int(time.time())),
                      "attributes": {
                          "friendly_name": "Hearth Atlas controller heartbeat",
                          "device_class": "timestamp_unix",
                          "unit_of_measurement": "s"}})
        except Exception as e:
            log.debug("heartbeat write failed (non-fatal): %s", e)

    async def run(self) -> None:
        log.info("Controller START | target=%s gpu_open=%.0f°C gpu_close=%.0f°C "
                 "min_interval=%.0fs max_off=%.0fs emergency=%.0f°C(%.0fs) "
                 "failsafe=%s interval=%.0fs",
                 self.target_plug, self.gpu_open, self.gpu_close,
                 self.min_int, self.max_off, self.emerg, self.emerg_dur,
                 self.fail_safe, self.interval)
        while True:
            try:
                await self.decide()
                g_alive.set(1)
                g_last_dec_ts.set(time.time())
                # Write cross-host heartbeat AFTER a successful decide(),
                # so a stale heartbeat means the controller has actually
                # failed (not just slow or starting up).
                await self._heartbeat()
            except Exception as e:
                g_alive.set(0)
                log.exception("decide() crashed: %s", e)
            await asyncio.sleep(self.interval)


def main() -> None:
    if not HA_TOKEN:
        log.error("HA_TOKEN not set — set via systemd drop-in")
        sys.exit(2)
    cfg = _load_config()
    ha_cfg = cfg.get("ha") or {}
    ctrl_cfg = ha_cfg.get("controller") or {}
    if not ctrl_cfg.get("enabled"):
        log.error("ha.controller.enabled is false in %s — set to true to opt in",
                  HEARTH_CONFIG_PATH)
        sys.exit(2)
    if "target_plug_id" not in ctrl_cfg:
        log.error("ha.controller.target_plug_id is required")
        sys.exit(2)
    ctrl_cfg["blocklist"] = ha_cfg.get("blocklist") or []
    start_http_server(PORT)
    asyncio.run(Controller(ctrl_cfg).run())


if __name__ == "__main__":
    main()
