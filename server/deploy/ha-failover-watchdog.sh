#!/usr/bin/env bash
# Hearth · CROSS-HOST failover watchdog
#
# Designed to run on a PEER host (typically a spark or a NAS — anywhere
# that has HA REST access but is independent of Atlas).  The local
# `ha-controller-watchdog.sh` already covers in-Atlas failures
# (controller crash, OOM kill, etc.); this script covers the harder
# case: Atlas itself dies (PSU, kernel panic, hardware fault,
# administrator mistake) and takes both the controller AND the local
# watchdog with it.
#
# Mechanism: Atlas's ha-controller writes a unix-timestamp heartbeat
# to HA's REST state API every ~30s (sensor.hearth_atlas_heartbeat).
# This script polls that sensor; if the heartbeat is stale beyond
# STALE_SECONDS AND the rack-AC plug is currently OFF, it
# unconditionally turns the plug ON via HA REST, returning control to
# the AC's built-in Layer-1 thermostat.
#
# Independence: this script depends only on:
#   - bash + curl + python3 (already on every Linux host)
#   - HA REST being reachable from the peer host
#   - the HA long-lived token file
# It does NOT depend on the controller, Prometheus, Hearth API, or any
# other Atlas-side service.  If HA itself is down everything is broken
# anyway.
#
# Install (on the peer host, e.g. spark-01, as user tony):
#   1) cp ~/.config/ha/token  (copy from Atlas, chmod 600)
#   2) install this script: /home/tony/bin/ha-failover-watchdog.sh
#   3) crontab -e, add:
#        */5 * * * * /home/tony/bin/ha-failover-watchdog.sh \
#                    >> /var/log/hearth-failover-watchdog.log 2>&1
#
# Tunables (env override):
#   HA_URL              — default http://homeassistant.local:8123
#   HA_TOKEN_FILE       — default ~/.config/ha/token
#   HEARTBEAT_ENTITY    — default sensor.hearth_atlas_heartbeat
#   TARGET_PLUG_ID      — default 2027457700 (rack AC cuco)
#   STALE_SECONDS       — default 300 (5 min — wider than local watchdog's
#                         120s, since this script's worst-case detection
#                         delay is one cron cycle anyway)
#
# Exit code semantics:
#   0 — Atlas alive, OR Atlas dead but plug already on (Layer 1 happy)
#   0 — Atlas dead, plug was off, rescue succeeded
#   1 — Could not reach HA, or rescue attempt failed (operator review)

# NOT `set -e` for the heartbeat fetch — when HA or Atlas is down the
# curl will fail, which is the exact signal this script needs to act on.
set -uo pipefail

HA_URL=${HA_URL:-http://homeassistant.local:8123}
HA_TOKEN_FILE=${HA_TOKEN_FILE:-$HOME/.config/ha/token}
HEARTBEAT_ENTITY=${HEARTBEAT_ENTITY:-sensor.hearth_atlas_heartbeat}
TARGET_PLUG_ID=${TARGET_PLUG_ID:-2027457700}
STALE_SECONDS=${STALE_SECONDS:-300}

log() { printf '%(%Y-%m-%d %H:%M:%S)T failover-watchdog[%s]: %s\n' -1 "$(hostname -s)" "$*"; }

if [[ ! -f "$HA_TOKEN_FILE" ]]; then
  log "no HA token at $HA_TOKEN_FILE — cannot rescue (operator install task)"
  exit 1
fi
HA_TOKEN=$(cat "$HA_TOKEN_FILE")
PLUG_ENTITY="switch.cuco_cn_${TARGET_PLUG_ID}_v3_on_p_2_1"

# 1) Read heartbeat sensor state.  curl/python failures all collapse to
# beat=0 which is ∞-stale, falling into the rescue path.
beat=$(curl -fsS --max-time 5 -H "Authorization: Bearer $HA_TOKEN" \
       "$HA_URL/api/states/$HEARTBEAT_ENTITY" 2>/dev/null \
       | python3 -c 'import sys,json; print(json.load(sys.stdin).get("state","0"))' \
       2>/dev/null || echo 0)
beat=${beat:-0}
# Guard against non-numeric state (e.g. "unavailable", "never")
[[ "$beat" =~ ^[0-9]+$ ]] || beat=0
now=$(date +%s)
age=$((now - beat))

if (( age <= STALE_SECONDS )); then
  # Atlas's controller is alive and recent — nothing to do.
  exit 0
fi

log "Atlas heartbeat STALE (age=${age}s > ${STALE_SECONDS}s threshold) — checking rack-AC plug"

# 2) Read plug state. If HA is unreachable from this peer, we can't help anyway.
plug=$(curl -fsS --max-time 5 -H "Authorization: Bearer $HA_TOKEN" \
       "$HA_URL/api/states/$PLUG_ENTITY" 2>/dev/null \
       | python3 -c 'import sys,json; print(json.load(sys.stdin).get("state",""))' \
       2>/dev/null || echo "")

case "$plug" in
  on)
    log "Atlas appears down but plug already ON — Layer 1 has control, no action"
    ;;
  off)
    log "Atlas DOWN AND plug OFF — UNCONDITIONAL TURN ON (returning to Layer 1)"
    if curl -fsS --max-time 6 -X POST \
        -H "Authorization: Bearer $HA_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"entity_id\":\"$PLUG_ENTITY\"}" \
        "$HA_URL/api/services/switch/turn_on" >/dev/null 2>&1; then
      log "  → turn_on OK · Layer 1 will start cooling within ~4 min"
    else
      log "  → turn_on FAILED · operator intervention required"
      exit 1
    fi
    ;;
  *)
    log "could not read plug state from HA (got '$plug') — cannot decide"
    exit 1
    ;;
esac
