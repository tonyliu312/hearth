# Security Policy

## Reporting a vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**

Instead, email the maintainer privately:

- **Contact:** open a [private security advisory](https://github.com/tonyliu312/hearth/security/advisories/new) on GitHub

Include:

- Description of the issue and its impact
- Steps to reproduce (proof-of-concept welcome)
- Affected version(s) / commit SHA
- Any suggested mitigation

You can expect:

- Acknowledgement within **3 business days**
- A first assessment within **7 days**
- Coordinated disclosure: fix + advisory published together; CVE requested if applicable

## Scope

In scope:

- Authentication / authorization issues (when auth is added)
- Server-side request forgery, remote code execution, command injection
- Cross-site scripting (XSS), CSRF in the dashboard
- Secret exposure (env, logs, error messages)
- Container escape from provided Docker images
- Dependency vulnerabilities with practical exploitability

Out of scope (low priority, not refused but lower urgency):

- Findings requiring physical access to the monitoring host
- Self-XSS / clickjacking on read-only dashboard
- Reports from automated scanners without practical impact
- Vulnerabilities in third-party services Hearth integrates with — report those upstream

## Supported versions

Until `v1.0.0`, only the latest tagged release receives security fixes. Pre-1.0 the project is alpha — operate at your own risk.

## Hardening notes for operators

- Run Hearth behind your home VPN (Tailscale, WireGuard, ZeroTier) or LAN-only — do **not** expose to the public internet without an authenticating reverse proxy (e.g., Cloudflare Tunnel + Zero Trust, oauth2-proxy)
- Secrets (LiteLLM master key, etc.) belong in `.env` (chmod 600), never in committed files
- The included Docker compose binds to `127.0.0.1` by default; if you change that, ensure your firewall is configured
