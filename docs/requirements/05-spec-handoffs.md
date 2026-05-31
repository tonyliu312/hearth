# 05 Spec Handoffs / 交付 Phase 6 spec 的开放点

> 详见 [`ha-integration.md` §9](./ha-integration.md) — 完整 7 个开放设计点(2 CRITICAL + 4 HIGH + 1 MEDIUM)。
>
> 关键开放点(Phase 6 spec 必须解决):
> - 9.1 `hearth.yaml` schema 扩展(`nodes[].sources.ha_plug_id` 等)
> - 9.2 `/api/cluster` payload 的 `power` / `env` 子对象字段定义
> - 9.3 exporter `node` 标签 ⇄ `obs_node_label` 一致性
> - 9.4 HA 离线时 SSE 降级语义(null vs 0 vs stale)
> - 9.5 exporter 服务发现机制
