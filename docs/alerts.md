# Alert push

Hearth's rule engine evaluates your metrics every snapshot tick and surfaces alerts in the dashboard. With a few lines of config it also **pushes** them to your phone or chat — once when a node goes unhealthy, once when it recovers, and (optionally) a reminder while it's still firing.

Notifications fire only on **state transitions**, not every tick — no spam. The fired-state persists to disk, so restarting Hearth doesn't re-send everything.

## What triggers an alert

| Rule | Severity | Key |
|---|---|---|
| Node offline (no metrics) | `bad` | `<node>:offline` |
| GPU ≥ 90 °C | `bad` | `<node>:gpu_temp` |
| GPU ≥ 85 °C | `hot` | `<node>:gpu_temp` |
| Memory ≥ 95 % | `warn` | `<node>:mem` |
| Disk ≥ 85 % | `warn` | `<node>:disk` |
| Gateway 5xx ≥ 5 / last 40 requests | `warn` | `gateway:errors` |

The `key` is what the notifier uses to dedupe. A GPU warming `hot → bad` updates the same key (no double-fire).

## Configure

Add an `alerts:` block to `config/hearth.yaml`:

```yaml
alerts:
  enabled: true
  min_severity: warn          # warn | hot | bad — push this level and above
  repeat_after_minutes: 30    # re-notify if still firing (0 = fire once, never repeat)
  channels:
    - type: ntfy
      url: "https://ntfy.sh/your-private-topic"
```

Then restart: `docker compose restart api` (or `systemctl restart hearth-api`).

## Channels

Secrets (bot tokens, webhook URLs you'd rather not commit) come from **environment variables** — you give the env-var name in YAML, the value lives in your `.env` / systemd drop-in.

### ntfy (easiest — free, no account, self-hostable)

```yaml
- type: ntfy
  url: "https://ntfy.sh/your-topic"      # or  url_env: NTFY_URL
```

Install the [ntfy app](https://ntfy.sh/), subscribe to your topic, done. Pick a long random topic name (anyone who knows it can read it) or self-host ntfy.

### Telegram

```yaml
- type: telegram
  token_env: "TELEGRAM_BOT_TOKEN"        # from @BotFather
  chat_id: "123456789"                   # your chat id (message the bot, read getUpdates)
```

### Discord / Slack (incoming webhook)

```yaml
- type: discord
  webhook_url_env: "DISCORD_WEBHOOK_URL"
- type: slack
  webhook_url_env: "SLACK_WEBHOOK_URL"
```

Create the webhook in your server/workspace settings, put the URL in `.env`.

### Generic webhook (escape hatch)

```yaml
- type: webhook
  url_env: "MY_WEBHOOK_URL"
```

POSTs JSON: `{"title": ..., "body": ..., "severity": "warn"}`. Wire it to PagerDuty, Home Assistant, a Lambda, whatever.

You can list **multiple** channels — every alert goes to all of them.

## Behaviour

- **Fire**: when an alert appears that wasn't firing → one push (`🔴 <message>`).
- **Resolve**: when a firing alert clears → one push (`✅ resolved · <message>`).
- **Repeat**: if `repeat_after_minutes > 0` and the alert is still firing after that interval → a reminder (`🔴 still firing · …`).
- **Restart-safe**: state is written to `${HEARTH_ALERT_STATE:-/tmp/hearth-alert-state.json}`. After a restart, already-fired alerts aren't re-sent.

## Notes on rendering

- The notification **body** is full UTF-8 (emoji preserved).
- For ntfy, the **title** is an HTTP header so it's sent ASCII-only (emoji/symbols stripped) — severity is conveyed by ntfy's Priority + Tags (🚨 / ⚠️ / ✅) instead.

## Not yet (v0.2.x)

- LINE Messaging API (the simple LINE Notify webhook was sunset 2025-03-31; the Messaging-API path needs an Official Account + token + userId — heavier setup, planned)
- Email / SMTP
- Per-channel severity routing (e.g., `bad` → phone, `warn` → Slack only)
- Quiet hours / maintenance windows
