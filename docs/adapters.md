# Adapter authoring guide *(stub — full guide arrives in v0.2.0)*

Hearth discovers metrics by probing each backend's `/metrics` endpoint. Today there are two adapters:

| Adapter | Prefix | What it returns |
|---|---|---|
| `_scrape_vllm` | `vllm:*` | tps, TTFT, TPOT, KV%, p50/95/99 (e2e), running, waiting |
| `_scrape_llamacpp` | `llamacpp:*` | tps, TPOT, running, waiting |
| `_scrape_sglang` | `sglang:*` | tps, TTFT, TPOT, KV%, p50/95/99, running, waiting *(untested against live SGLang — please report)* |

Both live in [`server/api/main.py`](../server/api/main.py). They share a normalized output shape (the `live` dict) so the frontend renders them identically.

## Adding a new backend (informal until v0.2.0 plugin API)

1. Add a `_LLAMACPP_SCALARS`-style set of metric names you care about.
2. Write `_scrape_<kind>(base) -> dict` that fetches `{base}/metrics` and returns the normalized fields.
3. In `_discover()` add a probe step: if the backend exposes your prefix, append to `<kind>_bases` on the model.
4. In `models_list()` add a branch that computes the `live` dict from your scrape + sets `metrics: "<kind>"`.
5. In `data.js` GB10 derivation, the `metricsSource` check generalizes (already accepts vllm + llamacpp; just add yours).

v0.2.0 will formalize this as a Python entry-point plugin so you can ship adapters as separate packages without forking Hearth.

## Wishlist

- Ollama `ollama:*` adapter (Ollama doesn't ship Prometheus metrics natively yet — could wrap its `/api/ps`)
- TensorRT-LLM Triton adapter
