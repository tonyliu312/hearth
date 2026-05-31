# 04 Architecture / 集成架构

> 详见 [`ha-integration.md` §4](./ha-integration.md) — 完整数据流图与组件契合理由。
> 数据流: HA REST(只读 15s 轮询) → ha-exporter (`:9105`/metrics) → Prometheus scrape job → recording rules → FastAPI `/api/cluster` → 前端 Telemetry 区。
