# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial open-source seed from the upstream personal monitor (5-node home cluster project)
- MIT license, `README.md`, `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`
- GitHub issue / pull-request templates, CI workflow skeleton

### Pending (next minor)

- `config/nodes.yaml` + `config/models.yaml` — replace hard-coded topology constants in `server/api/main.py` with declarative configuration (P1)
- Node-type abstraction — generalize the GB10 unified-memory specials to a `kind` field (`discrete` / `unified-arm-soc` / `apple-silicon`) (P1)
- Timezone — auto-detect from browser, remove `Asia/Taipei` hard-codes (P1)
- `examples/` — preset topology files for common home-lab shapes (P1)

[Unreleased]: https://github.com/tonyliu312/hearth/compare/HEAD...HEAD
