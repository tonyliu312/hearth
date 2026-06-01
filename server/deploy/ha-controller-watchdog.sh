#!/usr/bin/env bash
# Hearth · external watchdog for ha-controller
#
# Third safety layer (after ha-controller's internal fail-safe and
# max-off-duration cap).  Runs from cron every 5 minutes.  If the
# controller is silent (no recent decision metric OR process not
# running) AND the cuco plug is OFF, force the plug ON via HA REST so
# Layer 1 takes over.  Never touches the plug if it's already ON.
#
# Deliberately a single short bash script (not Python): if the entire
# Python/Hearth runtime is broken, this still works as long as bash +
# curl exist.  No dependencies on Hearth's venv, no shared state.
#
# Install (cron, runs as tony):
#   */5 * * * * /home/tony/dev/hearth/server/deploy/ha-controller-watchdog.sh \
#               >> /var/log/hearth-watchdog.log 2>&1
#
# Tunables (override via env):
#   STALE_SECONDS  — if no controller decision in this many seconds, treat
#                    as dead (default 120s = controller's normal cadence
#                    is 30s, so 4× missed = dead).
#   TARGET_PLUG_ID — cuco plug id the controller manages.  MUST match
#                    hearth.yaml's ha.controller.target_plug_id.
#   HA_TOKEN_FILE  — path to HA long-lived token (default ~/.config/ha/token).

set -euo pipefail

CONTROLLER_METRICS=${CONTROLLER_METRICS:-http://127.0.0.1:9106/metrics}
STALE_SECONDS=${STALE_SECONDS:-120}
TARGET_PLUG_ID=${TARGET_PLUG_ID:-2027457700}
HA_URL=${HA_URL:-http://homeassistant.local:8123}
HA_TOKEN_FILE=${HA_TOKEN_FILE:-$HOME/.config/ha/token}

log() { printf '%(%Y-%m-%d %H:%M:%S)T watchdog: %s\n' -1 "$*"; }

[[ -f "$HA_TOKEN_FILE" ]] || { log "no HA token at $HA_TOKEN_FILE, cannot rescue"; exit 0; }
HA_TOKEN=$(cat "$HA_TOKEN_FILE")
PLUG_ENTITY="switch.cuco_cn_${TARGET_PLUG_ID}_v3_on_p_2_1"

# 1) Read controller's last-decision timestamp.  Default to 0 (dead) if
# the metrics endpoint is unreachable.
last_ts=$(curl -fsS --max-time 4 "$CONTROLLER_METRICS" 2>/dev/null \
          | awk '/^hearth_ac_controller_last_decision_ts/ { print int($2); exit }')
last_ts=${last_ts:-0}
now=$(date +%s)
age=$((now - last_ts))

if (( age <= STALE_SECONDS )); then
  # Controller is alive and decided recently — nothing to do.
  exit 0
fi

log "controller silent for ${age}s (threshold ${STALE_SECONDS}s) — checking plug"

# 2) Read plug state from HA.  If we can't reach HA, we can't help anyway.
plug=$(curl -fsS --max-time 5 -H "Authorization: Bearer $HA_TOKEN" \
       "$HA_URL/api/states/$PLUG_ENTITY" 2>/dev/null \
       | python3 -c 'import sys,json; print(json.load(sys.stdin).get("state",""))' \
       2>/dev/null || true)

case "$plug" in
  on)
    log "controller silent but plug already ON — Layer 1 is in control, no action"
    ;;
  off)
    log "controller silent AND plug OFF — forcing plug ON to restore Layer 1"
    if curl -fsS --max-time 6 -X POST \
        -H "Authorization: Bearer $HA_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"entity_id\":\"$PLUG_ENTITY\"}" \
        "$HA_URL/api/services/switch/turn_on" >/dev/null 2>&1; then
      log "  → turn_on OK"
    else
      log "  → turn_on FAILED — operator intervention required"
      exit 1
    fi
    ;;
  *)
    log "could not read plug state from HA (got '$plug') — no action possible"
    exit 1
    ;;
esac
