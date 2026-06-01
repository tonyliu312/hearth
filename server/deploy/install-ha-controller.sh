#!/usr/bin/env bash
# Install Hearth's Layer-2 AC controller (ha-controller) + external watchdog.
# Idempotent. Reads HEARTH_CONFIG to validate ha.controller.enabled=true
# before doing anything.
#
# This is a WRITE-side install — the controller will toggle the cuco plug
# named in hearth.yaml.  Verify your config and physical setup before
# enabling.  After install, the controller starts immediately; to disable,
# `sudo systemctl stop ha-controller && sudo systemctl disable ha-controller`.
set -euo pipefail

REPO=${REPO:-/home/tony/dev/hearth}
HEARTH_CFG=${HEARTH_CFG:-$REPO/config/hearth.yaml}
TOKEN_FILE=${TOKEN_FILE:-$HOME/.config/ha/token}

if [[ ! -f "$HEARTH_CFG" ]]; then
  echo "ERROR: hearth.yaml not found at $HEARTH_CFG"
  exit 1
fi
if [[ ! -f "$TOKEN_FILE" ]]; then
  echo "ERROR: HA token not found at $TOKEN_FILE"
  exit 1
fi
if [[ ! -x "$REPO/.venv/bin/python" ]]; then
  echo "ERROR: $REPO/.venv/bin/python missing"
  exit 1
fi

# Validate config: ha.controller.enabled must be true + target_plug_id set
"$REPO/.venv/bin/python" - <<PY
import sys, yaml, pathlib
cfg = yaml.safe_load(pathlib.Path("$HEARTH_CFG").read_text()) or {}
ctrl = ((cfg.get("ha") or {}).get("controller") or {})
if not ctrl.get("enabled"):
    print("ERROR: hearth.yaml ha.controller.enabled is not true — opt in first")
    sys.exit(1)
tgt = ctrl.get("target_plug_id")
if not tgt:
    print("ERROR: hearth.yaml ha.controller.target_plug_id is required")
    sys.exit(1)
HARD_BLOCK = {"2051674991"}
op_block = set((cfg.get("ha") or {}).get("blocklist") or [])
if tgt in HARD_BLOCK or tgt in op_block:
    print(f"ERROR: target_plug_id {tgt} is in blocklist")
    sys.exit(1)
print(f"✓ config valid: target_plug_id={tgt}")
PY

echo "[1/5] Creating state directory /var/lib/hearth ..."
sudo install -d -o tony -g tony -m 755 /var/lib/hearth

echo "[2/5] Installing controller systemd unit ..."
sudo install -m 644 "$REPO/server/deploy/ha-controller.service" \
  /etc/systemd/system/ha-controller.service

echo "[3/5] Sharing token drop-in with ha-exporter (chmod 600) ..."
# Reuse the same HA_TOKEN drop-in we created for ha-exporter (operator's
# token file is the single source of truth).  Create our own drop-in
# directory and point it at the same Environment= line.
sudo install -d -m 700 /etc/systemd/system/ha-controller.service.d
printf '[Service]\nEnvironment=HA_TOKEN=%s\n' "$(cat "$TOKEN_FILE")" \
  | sudo tee /etc/systemd/system/ha-controller.service.d/token.conf >/dev/null
sudo chmod 600 /etc/systemd/system/ha-controller.service.d/token.conf

echo "[4/5] Installing watchdog cron (every 5 min, as tony) ..."
CRON_LINE="*/5 * * * * $REPO/server/deploy/ha-controller-watchdog.sh >> /var/log/hearth-watchdog.log 2>&1"
# Ensure log file exists and writable
sudo touch /var/log/hearth-watchdog.log
sudo chown tony:tony /var/log/hearth-watchdog.log
# Add to tony's crontab without disturbing existing entries
( crontab -l 2>/dev/null | grep -v 'ha-controller-watchdog' ; echo "$CRON_LINE" ) | crontab -

echo "[5/5] Enabling and starting ha-controller ..."
sudo systemctl daemon-reload
sudo systemctl enable --now ha-controller

sleep 3
echo
echo "── status ───────────────────────────────────────────────"
sudo systemctl status ha-controller --no-pager -n 8 || true
echo
echo "── metrics smoke ────────────────────────────────────────"
if curl -fsS --max-time 4 http://127.0.0.1:9106/metrics | grep -E '^hearth_ac_controller_' | head -8; then
  echo "✓ controller serving metrics on :9106"
else
  echo "✗ controller not responding on :9106 — check 'journalctl -u ha-controller -n 50'"
  exit 1
fi
echo
echo "Next:"
echo "  - Watch first 24h via Hearth Telemetry → Energy trends card"
echo "  - Watchdog log: tail -f /var/log/hearth-watchdog.log"
echo "  - Disable any time: sudo systemctl stop ha-controller && sudo systemctl disable ha-controller"
