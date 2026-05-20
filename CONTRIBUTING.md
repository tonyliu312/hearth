# Contributing to Hearth

Thanks for your interest! Hearth is alpha and welcoming early contributors.

## Quick orientation

| Layer | Files | Tech |
|---|---|---|
| Frontend | `*.jsx`, `data.js`, `i18n.js`, `index.html`, `styles.css` | React 18, in-browser Babel (no bundler), CSS custom-props |
| Backend | `server/api/main.py` | FastAPI + httpx, reads Prometheus + LiteLLM Postgres + direct backend `/metrics` |
| Infra | `server/{docker-compose.yml,nginx,prometheus,alertmanager}` | Docker Compose, nginx (host net), Prometheus, Alertmanager |

The frontend has **no build step** — files are served as-is and Babel transpiles in the browser. That's a feature for hackability; a build step is a future option (P3) but not a requirement.

## Before you start

For **non-trivial changes** (new feature, refactor, breaking change): please open an issue first to discuss. For typo fixes, small bugs, dependency bumps — go straight to a PR.

## Workflow

1. **Fork** the repo, create a branch from `main`: `git checkout -b feat/your-thing`
2. **Code**. Match the surrounding style (it's not strict — pragmatic Apple-product-page aesthetic, light comments explaining *why*, not *what*)
3. **Test manually** — there's no test suite yet. Run the stack locally (`docker compose up` in `server/`), reload the browser, hit the affected endpoints
4. **Commit** following [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat(api): add llamacpp adapter`
   - `fix(ui): nav burger active state X transform`
   - `docs(readme): clarify quickstart`
   - `chore(deps): bump httpx 0.27 → 0.28`
   - Sign off your commits: `git commit -s -m "..."` (DCO)
5. **Open a PR** against `main`. Fill out the template. Link any related issue.

## Code style

- **Frontend JSX**: per-file scoped style objects (never name them just `styles`). Use `terminalStyles`, `heroStyles`, etc. — different files collide.
- **Comments**: explain *why*, not *what*. Code should be self-evident.
- **Honesty principle**: if a metric isn't available from a backend, show `—`, not a fake `0`. The contract is "every visible number is real or honestly absent".
- **Don't break the data shape**: frontend reads `live.nodes[id].<metric>.{now, hist}` and `live.models[id].<metric>.{now, hist}`. Both mock and live writers must respect this.

## Adding a metrics-source adapter

Until v0.2.0 introduces a formal plugin API, adapters live in `server/api/main.py` as `_scrape_<kind>` functions. Pattern: take a base URL, return a dict of normalized fields (`tps`, `ttft`, `tpot`, `kv`, `running`, `waiting`, `p50`, `p95`, `p99`, `resident`). Missing fields → `0` *and* the model's `metrics` label tells the frontend which fields the source actually exposes (so the UI can show `—`). See `_scrape_vllm` and `_scrape_llamacpp` as references.

## Adding a UI language

`i18n.js` has dictionaries keyed by English. Add a new language object beside `zh-CN` / `zh-TW`. Use UI placement to test (the lang switcher is in the nav).

## Reporting issues

Use the GitHub issue templates. Include:

- Hearth version (commit SHA if not tagged)
- Your topology (1 node? cluster? what GPUs? what backends?)
- What you expected vs what you saw (screenshot if UI)
- Steps to reproduce

## Code of Conduct

We follow the [Contributor Covenant 2.1](CODE_OF_CONDUCT.md). Be respectful. Disagreements are fine; personal attacks are not.

## License

By contributing, you agree your work is licensed under the [MIT License](LICENSE) of this project.
