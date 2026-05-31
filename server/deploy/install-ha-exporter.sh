#!/usr/bin/env bash
# Install Hearth's Home Assistant exporter on this host. Idempotent.
#
# Pre-reqs:
#   - hearth-api venv at /home/tony/dev/hearth/.venv with requests + pyyaml
#   - HA long-lived token saved at ~/.config/ha/token (chmod 600)
#   - hearth.yaml has `ha:` block and at least one node with ha_plug_id
#
# After install:
#   systemctl is-active ha-exporter           # should be active
#   curl http://127.0.0.1:9105/metrics | grep ha_  # should show samples
#   Then add the scrape job to your obs prometheus.yml + reload.
set -euo pipefail

REPO=${REPO:-/home/tony/dev/hearth}
TOKEN_FILE=${TOKEN_FILE:-$HOME/.config/ha/token}

if [[ ! -f "$TOKEN_FILE" ]]; then
  echo "ERROR: HA token not found at $TOKEN_FILE"
  echo "  Create one in Home Assistant: profile → Long-Lived Access Tokens,"
  echo "  save to $TOKEN_FILE and 'chmod 600 $TOKEN_FILE'."
  exit 1
fi

if [[ ! -x "$REPO/.venv/bin/python" ]]; then
  echo "ERROR: $REPO/.venv/bin/python not found — set up the hearth-api venv first."
  exit 1
fi

echo "[1/4] Ensuring prometheus_client is installed in the venv …"
"$REPO/.venv/bin/pip" install -q prometheus_client requests pyyaml

echo "[2/4] Installing systemd unit …"
sudo install -m 644 "$REPO/server/deploy/ha-exporter.service" /etc/systemd/system/ha-exporter.service

echo "[3/4] Writing root-only token drop-in (chmod 600) …"
sudo install -d -m 700 /etc/systemd/system/ha-exporter.service.d
printf '[Service]\nEnvironment=HA_TOKEN=%s\n' "$(cat "$TOKEN_FILE")" \
  | sudo tee /etc/systemd/system/ha-exporter.service.d/token.conf >/dev/null
sudo chmod 600 /etc/systemd/system/ha-exporter.service.d/token.conf

echo "[4/4] Enabling and starting ha-exporter …"
sudo systemctl daemon-reload
sudo systemctl enable --now ha-exporter

sleep 2
echo
echo "── status ───────────────────────────────────────────────"
sudo systemctl status ha-exporter --no-pager -n 5 || true
echo
echo "── smoke test ───────────────────────────────────────────"
if curl -fsS --max-time 4 http://127.0.0.1:9105/metrics | grep -E '^ha_' | head -8; then
  echo "✓ ha-exporter is serving metrics"
else
  echo "✗ no ha_* metrics yet — check 'journalctl -u ha-exporter -n 50'"
  exit 1
fi

echo
echo "Next: add the scrape job to your obs prometheus.yml:"
echo "  - job_name: home_assistant"
echo "    scrape_interval: 15s"
echo "    static_configs:"
echo "      - targets: [127.0.0.1:9105]"
echo "        labels: { source: home-assistant }"
echo "Then: curl -X POST http://<obs-host>:9090/-/reload"
