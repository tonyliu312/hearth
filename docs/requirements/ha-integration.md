# Hearth × Home Assistant 集成需求

> 把家庭 Home Assistant 里的智能插座 / 机柜环境传感器接入 Hearth,
> 给监控补上 **每节点整机墙功率** 和 **机房/机柜环境(温湿度、空调)** ——
> 这是 DCGM(只有 GPU 功率)和 node-exporter(没有墙功率)都拿不到的数据。
>
> 最后核实 HA 侧:2026-05-31 · 集成方维护:本文档 + `ha-exporter.py`
> 镜像自 `AI-Monitor/server/integration/home-assistant.md` 作为 Hearth Phase 1 输入。

---

## 0. Actors(参与者)

| Actor | 角色 | 与本集成的关系 |
|---|---|---|
| **集群运维者(自托管用户)** | 终端用户 | 在 Hearth UI 上看墙功率/能效/机柜环境,据此决策(开关空调、迁移负载、定位高耗能模型) |
| **ha-exporter 进程** | 自动化采集者 | 在 Atlas 宿主上常驻,以只读 REST 轮询 HA,产出 Prometheus 文本指标 |
| **Home Assistant 实例** | 上游数据源 | 提供 cuco 插座功率/通断、温湿度计 Pro、机柜空调插座状态 |
| **Prometheus(obs 栈)** | 中间存储/聚合层 | 抓 exporter 指标、运行 recording rules、触发 alerts |
| **Hearth FastAPI** | UI 数据网关 | 把 `ha_*` 与 `cluster:*` 指标拼进 `/api/cluster` SSE payload |
| **Hearth 前端(浏览器)** | 可视化消费者 | 在「05 Telemetry」区渲染墙功率/tokens·W⁻¹/机柜温湿度卡片 |
| ⛔ MT6000 路由器 | **非 actor(显式排除)** | 永不监控、永不控制(关了全网失联) |

---

## 0.5 核心使用场景 / Use Cases

> 4 个主要 scenario,驱动本集成的功能需求。

### UC-1 整机墙功率监控(Wall Power Monitoring)

**触发**:运维者查看 Hearth 仪表板。
**主流程**:exporter 每 15s 从 HA 拉 4 个 spark 节点的 cuco 插座功率 → Prometheus 入库 → FastAPI 拼进 `/api/cluster` → 前端 Telemetry 区显示节点墙功率列。
**验收**:atlas 无插座 → 显「—」(诚实降级);4 个 spark 显实际瓦数;HA 掉线 → `ha_up=0` 告警但 UI 其余数据不受影响。

### UC-2 能效比 tokens/W 监控(Efficiency Curve)

**触发**:运维者评估推理能效。
**主流程**:Prometheus recording rule `cluster:tokens_per_watt = sum(rate(litellm_total_tokens_metric[1m])) / clamp_min(sum(ha_node_wall_power_watts), 1)` → 前端能效卡片。
**验收**:DeepSeek TP=4 负载下能算出合理 tokens·W⁻¹;无负载时分子 0、不报错;墙功率全无时分母用 clamp_min 防除零。

### UC-3 机柜环境告警(Rack Environmental Alerts)

**触发**:机柜温度 > 38°C 持续 5 分钟,或空调关但集群在跑。
**主流程**:`RackOverTemp`/`RackACOff` 规则触发 → Prometheus alert → (可选 Alertmanager 推送) → Hearth UI 显示。
**验收**:温度告警 5 分钟去抖、不抖动告警风暴;空调-负载交叉规则(空调关 AND 总墙功率 > 150W)正确判定。

### UC-4 节点失电检测(Node Power Loss Detection)

**触发**:某节点插座被人手动关闭或 HA 报 `off`。
**主流程**:exporter 看到 `switch.*on*` = "off" → `ha_node_plug_state=0` → `NodePowerLost` 告警 → UI 节点卡片显示离线 + 失电状态。
**验收**:配合既有 `ns.up` 真相源,失电节点不仅 OFFLINE,还能区分「机器死机」vs「物理断电」。

---

## 1. 为什么要接 HA(它补了什么盲区)

| 监控维度 | 现有数据源 | 盲区 | HA 补上 |
|---|---|---|---|
| GPU 功率 | DCGM `DCGM_FI_DEV_POWER_USAGE` | 只算 GPU,不含 CPU/风扇/PSU 损耗 | — |
| **整机墙功率** | ❌ 无 | 真实从插座拉的整机功耗 | ✅ cuco 插座 `electric_power` |
| **能效 tokens/watt** | ❌ 无(分母缺墙功率) | 真实能效比 | ✅ LiteLLM tokens ÷ 墙功率 |
| **节点供电状态** | `up{job=node}`(只知进程在不在) | 是否真的通电 | ✅ 插座 `on/off` |
| **机柜环境温湿度** | ❌ 无 | 机房过热预警 | ✅ 温湿度计 Pro |
| **机柜空调** | ❌ 无 | 散热是否在工作 | ✅ 机柜空调插座功率/通断 |

一句话:**DCGM 告诉你 GPU 烧了多少瓦,HA 告诉你墙上插座实际拉了多少瓦、机柜多热、空调开没开。** 两者一 join,才有真实的 tokens/watt 整机能效曲线和热-电闭环。

---

## 2. HA 实例信息(数据源)

| 项 | 值 |
|---|---|
| 地址(首选) | `http://homeassistant.local:8123`(mDNS,**抗 DHCP 漂移**) |
| 当前 IP | `192.168.1.228`(曾 `.100`→`.71`→`.228`,别认死 IP) |
| 认证 | long-lived token,`~/.config/ha/token`(chmod 600,exp 2036) |
| 协议 | REST `GET /api/states/<entity_id>`,只读轮询 |

> ⚠️ **IP 漂移**:HA IP 会因 DHCP 续约变化。exporter 的 `HA_URL` **务必用 mDNS 域名**
> `homeassistant.local`,不要硬编码 IP。Atlas 上验证 mDNS 可达:
> `getent hosts homeassistant.local`。

完整设备/部署清单见 `~/dev/HomeAssistant/docs/HA-部署与设备清单.md`。

---

## 3. 插座 ↔ 节点映射(集成的关键,认 cuco ID 别认标签)

cuco 智能插座(`Gosund cuco.plug.v3`),entity 命名规律:
- 功率:`sensor.cuco_cn_<ID>_v3_electric_power_p_11_2`(单位 W)
- 主开关:`switch.cuco_cn_<ID>_v3_on_p_2_1`(`on`/`off`)

| AI-Monitor `node` | 节点 IP | cuco ID | 说明 |
|---|---|---|---|
| `spark-01` | .151 | `2029950736` | ray-head |
| `spark-02` | .156 | `2051676838` | |
| `spark-03` | .188 | `2029941045` | ✅ 2026-05-27 实测验证 |
| `spark-04` | .189 | `2029938288` | ✅ 2026-05-27 实测验证 |
| `atlas` | .20 | **无** | RTX4090-PC 不在 cuco 插座上 → 无墙功率(诚实降级) |
| —(机柜空调) | — | `2027457700` | 🆕 非节点;另有插座内温 `_temperature_p_12_2` |
| —(MT6000 路由器) | — | `2051674991` | ⛔ **永不监控/永不控制**,关了全网失联 |

> 🔴 **历史踩坑**:.188/.189 的插座**标签曾在 HA 里对调**,导致一度断错开关、误判"插座失效"。
> 上表是 2026-05-27 用 reboot 时间戳实证后的**物理映射**。**永远认 cuco ID,别认设备显示名。**
> 现 HA 设备名已改名修正,但代码里仍以 ID 为准最安全。

机柜环境温湿度计 Pro(`miaomiaoce.sensor_ht.t8`,ID `miaomiaoc_cn_blt_3_1p6fkmb844g00_t8`):
- 温度 `sensor.<ID>_temperature_p_3_1001` · 湿度 `_relative_humidity_p_3_1002` · 电量 `_battery_level_p_2_1003`

---

## 4. 架构:HA → Prometheus exporter(契合现有管线)

沿用 AI-Monitor 既定数据流,**新增一个 scrape job,不碰任何现有组件**:

```
HA (192.168.1.228:8123)
  │  REST /api/states (只读轮询, 15s)
  ▼
ha-exporter.py  (Atlas 宿主 venv, :9105/metrics)   ← 本集成新增
  │  Prometheus 文本格式, node 标签与现有约定一致
  ▼
Prometheus (job=home_assistant)  ──►  recording rules  ──►  FastAPI /api/*  ──►  前端
```

为什么用 exporter 而不是在 FastAPI 里直连 HA:
1. **契合约定** —— CLAUDE.md 明确"新派生指标先做成 recording rule,再经 FastAPI 暴露"。指标进了 Prometheus 才能 join `node_*`/`DCGM_*` 算能效、进 recording rule、进告警。
2. **零生产影响** —— 独立 job + 独立进程,符合 `verify-no-prod-impact.sh` 纪律,不触碰 obs-*/litellm。
3. **抗 HA 抖动** —— HA 重启/掉线只让 `ha_up=0`,不阻塞 SSE 主链路。

> 关于大陆容器出网被墙:与 ai-monitor-api 同样**走宿主 venv 直跑**,不进 docker build(见 `VERIFICATION.md`)。

---

## 5. 落地步骤

### 5.1 部署 exporter(Atlas 192.168.1.20)

```bash
cd /home/tony/dev/AI-Monitor/server/integration
# 复用 ai-monitor-api 的 venv(已含 requests),仅补 prometheus_client
/home/tony/dev/AI-Monitor/server/runtime/venv/bin/pip install \
  -i https://pypi.tuna.tsinghua.edu.cn/simple prometheus_client

# 冒烟:用 mDNS 抗漂移
HA_URL=http://homeassistant.local:8123 \
HA_TOKEN=$(cat ~/.config/ha/token) \
  /home/tony/dev/AI-Monitor/server/runtime/venv/bin/python ha-exporter.py &
curl -s http://127.0.0.1:9105/metrics | grep ha_node_wall_power_watts
# 期望: ha_node_wall_power_watts{node="spark-01"} 66.0  等
```

### 5.2 systemd 常驻(token 走 root-only drop-in,不入 git)

与 `ai-monitor-api.service` 同样的 token 保护套路。`/etc/systemd/system/ha-exporter.service`:

```ini
[Unit]
Description=AI-Monitor · Home Assistant exporter
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=tony
WorkingDirectory=/home/tony/dev/AI-Monitor/server/integration
Environment=HA_URL=http://homeassistant.local:8123
Environment=HA_EXPORTER_PORT=9105
# HA_TOKEN 由 root-only drop-in 提供(chmod 600,不入 git):
#   /etc/systemd/system/ha-exporter.service.d/token.conf
#     [Service]
#     Environment=HA_TOKEN=<long-lived token>
ExecStart=/home/tony/dev/AI-Monitor/server/runtime/venv/bin/python ha-exporter.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
sudo install -d -m 700 /etc/systemd/system/ha-exporter.service.d
printf '[Service]\nEnvironment=HA_TOKEN=%s\n' "$(cat ~/.config/ha/token)" \
  | sudo tee /etc/systemd/system/ha-exporter.service.d/token.conf >/dev/null
sudo chmod 600 /etc/systemd/system/ha-exporter.service.d/token.conf
sudo systemctl daemon-reload && sudo systemctl enable --now ha-exporter
```

### 5.3 加 Prometheus scrape job

`server/prometheus/prometheus.yml` 追加(沿用 `node` 标签约定,让 ha_* 能和 node_*/DCGM_* join):

```yaml
  # ── Home Assistant: 整机墙功率 + 机柜环境(经 ha-exporter) ──
  - job_name: home_assistant
    scrape_interval: 15s
    static_configs:
      - targets: [192.168.1.20:9105]
        labels: { source: home-assistant }
```

> exporter 已在指标内自带 `node` 标签(spark-01..04),故这里 job 级不再贴 node。

reload Prometheus:`curl -X POST http://192.168.1.20:9090/-/reload`,
然后 Targets 页应见 `home_assistant` UP。

### 5.4 加 recording rules(派生能效指标)

`server/prometheus/rules/recording.yml` 的 `cluster` 组追加:

```yaml
    # ── 墙功率 / 能效(来自 HA 插座) ────────────────────────────
    - record: cluster:wall_power_w:sum
      expr: sum(ha_node_wall_power_watts)
    - record: cluster:gpu_power_w:sum            # 对照:DCGM GPU 功率
      expr: sum(DCGM_FI_DEV_POWER_USAGE)
    # 整机能效:每瓦墙功率产出的 tokens/s(分母去掉机柜空调与路由器,只算节点)
    - record: cluster:tokens_per_watt
      expr: sum(rate(litellm_total_tokens_metric[1m])) / clamp_min(sum(ha_node_wall_power_watts), 1)
    # 单节点 PUE 近似:整机墙功率 / GPU 功率(>1,越接近 1 说明 GPU 占比越高)
    - record: node:power_overhead_ratio
      expr: ha_node_wall_power_watts / clamp_min(on(node) DCGM_FI_DEV_POWER_USAGE, 1)
```

### 5.5 加告警(机柜过热 / 空调失效 / 节点失电)

`server/prometheus/rules/alerts.yml` 新增组:

```yaml
- name: home_assistant
  interval: 30s
  rules:
    - alert: HAExporterDown
      expr: up{job="home_assistant"} == 0 or ha_up == 0
      for: 3m
      labels: { severity: warning }
      annotations:
        summary: "HA exporter 不可达,墙功率/机柜环境数据缺失"

    - alert: RackOverTemp
      expr: ha_rack_temperature_celsius > 38
      for: 5m
      labels: { severity: critical }
      annotations:
        summary: "机柜温度 {{ $value | printf \"%.1f\" }}°C 超 38°C"
        description: "检查机柜空调(ha_rack_ac_state)与节点负载。"

    - alert: RackACOff
      expr: ha_rack_ac_state == 0 and cluster:wall_power_w:sum > 150
      for: 2m
      labels: { severity: critical }
      annotations:
        summary: "机柜在跑负载但空调已关闭 → 过热风险"

    - alert: NodePowerLost
      expr: ha_node_plug_state == 0
      for: 1m
      labels: { severity: critical }
      annotations:
        summary: "节点 {{ $labels.node }} 插座已断电"

    - alert: RackSensorLowBattery
      expr: ha_rack_sensor_battery_percent < 15
      for: 30m
      labels: { severity: info }
      annotations:
        summary: "机柜温湿度计 Pro 电量 {{ $value }}%"
```

### 5.6(可选)FastAPI 暴露 + 前端

`server/api/main.py` 把新指标拼进 `/api/cluster` 或 `/api/stream` 的 payload
(薄 PromQL→JSON,沿用既有 `_one`/`_by` helper):

```python
# 在组装 cluster payload 处补:
power = {
    "wallW":      _one(promql('cluster:wall_power_w:sum')),
    "gpuW":       _one(promql('cluster:gpu_power_w:sum')),
    "tokensPerW": _one(promql('cluster:tokens_per_watt')),
}
env = {
    "rackTempC":  _one(promql('ha_rack_temperature_celsius')),
    "rackRH":     _one(promql('ha_rack_humidity_percent')),
    "acW":        _one(promql('ha_rack_ac_power_watts')),
    "acOn":       bool(_one(promql('ha_rack_ac_state'))),
}
# payload["power"] = power; payload["env"] = env
```

前端按 CLAUDE.md 设计语言用现成 ring/metric block 渲染(Telemetry 区加
"墙功率 / tokens·W⁻¹ / 机柜温湿度"卡片),颜色读 CSS 自定义属性,数字用 tabular + 单位 `<small>`。

---

## 6. 指标速查(exporter 暴露)

| 指标 | 标签 | 含义 |
|---|---|---|
| `ha_up` | — | HA REST 可达性(1/0) |
| `ha_node_wall_power_watts` | `node` | 节点整机墙功率(W) |
| `ha_node_plug_state` | `node` | 节点插座通断(1=on) |
| `ha_rack_ac_power_watts` | — | 机柜空调功率(W) |
| `ha_rack_ac_state` | — | 机柜空调通断(1=on) |
| `ha_rack_ac_plug_temp_celsius` | — | 机柜空调插座内部温度 |
| `ha_rack_temperature_celsius` | — | 机柜环境温度 |
| `ha_rack_humidity_percent` | — | 机柜环境湿度 |
| `ha_rack_sensor_battery_percent` | — | 温湿度计 Pro 电量 |

`unavailable`/`unknown` 的传感器**不打点**(让 Prometheus 显 stale,不伪造 0)。

---

## 7. ⛔ 电源控制(可选,高危,默认不启用)

cuco 插座**可写**(`POST /api/services/switch/turn_off`),理论上 AI-Monitor 能做
一键 power-cycle 解锁频。但这是**断市电**操作,受家庭运维硬纪律约束:

> **用户锁死原则**:"除非 SSH 无法登录,否则必须安全关机后再断电。"

若要在 AI-Monitor 暴露控制动作,必须满足全部:
1. **只读监控与控制分离** —— exporter 永远只读;控制走独立的、需显式鉴权的 FastAPI action 端点,**绝不自动触发**。
2. **断电前强制安全关机** —— `docker stop -t 20 vllm_node` → `ssh tony@<ip> 'sudo -n shutdown -h now'` → ping 确认下电 → 才 `turn_off`。唯一例外:SSH 登不上(机器卡死)。
3. **插座白名单 + ID 校验** —— 只允许 `spark-01..04` 对应的 cuco ID;**硬编码拒绝** `2051674991`(MT6000 路由器)和 `2027457700`(机柜空调)。
4. **来电自启已验证** —— Spark BIOS 已开 power-on-after-AC,来电 ~24s 自起,无需 WoL。

> 控制流参考实现见 `~/dev/HomeAssistant/docs/HA-部署与设备清单.md` 第 9 节。
> **本集成默认只做监控(只读),控制端点不随 exporter 部署。**

---

## 8. 零生产影响 / 验证

- exporter 对 HA 是**只读** REST 轮询,对 AI-Monitor 是**独立 job + 独立进程**,
  不触碰 obs-*/litellm/daemon —— 部署前后跑 `verify-no-prod-impact.sh diff` 应全 PASS。
- 验收口径(诚实降级):
  - `atlas` 无墙功率(不在 cuco 插座上)—— 前端该节点功率显"—"而非 0。
  - HA 掉线/IP 漂移未及时解析 → `ha_up=0` + `HAExporterDown` 告警,其余监控不受影响。
  - 传感器 `unavailable` → 对应指标 stale,不伪造数值。

---

## 9. Hearth 迁移要点(OSS 适配,Phase 6 spec 需明确)

> 本文档是从 AI-Monitor PoC 迁移过来的需求,以下是把 PoC「config-as-data 化」必须在 Phase 6 spec 解决的开放设计点。本节是**需求层的交接清单**,具体 schema/JSON 形状交给 spec.md 决定。

### 9.1 ⛔ CRITICAL — `hearth.yaml` schema 扩展

PoC 的 `NODE_PLUGS = {"spark-01": "2029950736", ...}` 硬码节点↔cuco ID 映射,与 Hearth「节点 id 操作者自定义」原则冲突。**Phase 6 spec 必须定义**:

| 字段 | 位置 | 类型 | 验证规则 |
|---|---|---|---|
| `ha_plug_id` | `nodes[].sources.ha_plug_id` | string(cuco ID 数字) | 可选;不存在 → 该节点无墙功率,Hero / Telemetry 显「—」 |
| `ha_sensor_id` | `display.ha_rack_sensor_id` 或 `nodes[].sources.ha_sensor_id` | string | 可选;一个集群通常只 1 个机柜传感器 |
| `ha_ac_plug_id` | `display.ha_rack_ac_plug_id` | string | 可选;同上 |
| `ha.blocklist` | 顶层 `ha.blocklist: [<cuco_id>, ...]` | string[] | **MT6000 路由器 ID `2051674991` 必须默认带,exporter 启动时硬检** |
| `ha.base_url` | `ha.base_url` | string | 默认 `http://homeassistant.local:8123`,可被 env `HA_URL` 覆盖 |

exporter 启动时读取 `hearth.yaml`,把 `nodes[]` 里所有有 `ha_plug_id` 的节点装入运行时映射表。**任何 plug_id 命中 `blocklist` → 拒绝启动并日志报错**。

### 9.2 ⛔ CRITICAL — `/api/cluster` payload 的 `power` / `env` 子对象 schema

Phase 6 spec 必须明确(本节只列字段名 + 单位 + 缺数据降级,不定 JSON 嵌套结构):

| 字段 | 单位 | 精度 | HA 缺数据时 |
|---|---|---|---|
| `power.wallW` | 瓦(W) | 1 位小数 | `null`(前端显「—」) |
| `power.gpuW` | 瓦(W) | 1 位小数 | `null`(前端显「—」) |
| `power.tokensPerW` | tokens·W⁻¹ | 2 位小数 | `null`(前端显「—」),分母 clamp_min 1 防 0 除 |
| `env.rackTempC` | 摄氏度 | 1 位小数 | `null` |
| `env.rackRH` | % | 0 位小数 | `null` |
| `env.acW` | 瓦 | 1 位小数 | `null` |
| `env.acOn` | bool | — | `null`(三态:on/off/unknown,不强制 false) |

**注意**:`null` 不等同 `0`;前端必须区分「数据流通但确为 0」与「无数据」。

### 9.3 🟡 HIGH — exporter 的 `node` 标签 ⇄ hearth.yaml 的 `obs_node_label` 一致性

exporter 自带 `node="spark-01"` 等标签,与 Prometheus scrape 后聚合一致。但 hearth.yaml `nodes[].sources.obs_node_label` 是该节点在 obs Prometheus 里的真实标签(如 `spark-5135`)。**Phase 6 spec 必须规定**:exporter 标签值 = `nodes[].id`(Hearth 内部 id);PromQL recording rule 在 `join` 时用 `on(node)` 还是 `on(obs_node_label)` 由 spec 决定。当前推荐:exporter 用 `nodes[].id` 作 `node` 标签值,与 Hearth 内部命名一致;join 时 `on(node)`。

### 9.4 🟡 HIGH — HA 离线时的 SSE 主链路降级语义

文档 §8 说「ha_up=0 不阻塞 SSE 主链路」,**Phase 6 spec 需明确**:
- exporter 失联时,Prometheus 看到的 `ha_*` 指标 **stale**(默认 stale 5 分钟后视为不存在);
- FastAPI 拼装 `/api/cluster` 时,`promql('cluster:wall_power_w:sum')` 返回 NaN → `_one` helper 返回 `None` → JSON 序列化为 `null`;
- 前端拿 `null` → 显「—」,**不**用历史值 backfill(诚实降级)。

### 9.5 🟡 HIGH — Prometheus 找不到 exporter 时的发现机制

PoC 在 `prometheus.yml` 硬码 `targets: [192.168.1.20:9105]`。Atlas IP 不会漂(它是网关本机);但开源版用户若不在 Atlas 跑,需文档化。**Phase 6 spec 决定**:
- v1:硬码 IP/host(简单,适用单 Atlas);
- v2:支持 `ha.exporter_target` 配置字段。

v1 默认 `127.0.0.1:9105`(同主机跑),example.yaml 注释说明。

### 9.6 🟢 MEDIUM — 部署前 entity_id ⇄ 物理节点对应验证清单

历史教训:.188/.189 标签在 HA UI 里曾对调。**Phase 6 spec 应附验证 checklist**(部署 ha-exporter 前必须做):
1. 在 HA UI 找到 cuco 设备,记下 ID 与「显示名」;
2. 对每个 cuco ID,在 HA UI 点开关 → ssh 该节点 → `uptime` 看是否真的断电重启;
3. 实测验证后再写进 `hearth.yaml`,**永远以 cuco ID 为权威**,设备显示名仅作辅助;
4. 验证完成后,在 `hearth.yaml` 注释里记录验证日期(类似 PoC 的 `# ✓2026-05-27 实测验证`)。

### 9.7 🟢 MEDIUM — 前端 TelemetrySection 渲染规格(Phase 6 spec UI 子项)

Phase 6 spec 应给出 wireframe(文字描述或 ASCII)+ Playwright 验收用的 DOM 选择器约定。最低限度:
- 「墙功率」卡片:`.tm-card[data-metric="wall-power"]`,显 `power.wallW` + 节点细分小表;
- 「能效」卡片:`.tm-card[data-metric="efficiency"]`,显 `power.tokensPerW` + sparkline;
- 「机柜温湿度」卡片:`.tm-card[data-metric="rack-env"]`,显 `env.rackTempC` / `env.rackRH` / `env.acOn` 状态点;
- 复用现有 `metric-l` / `ring` / `Sparkline` 组件,不引入新设计语言。

---

## 10. 变更历史

- **2026-05-31** 初版(AI-Monitor)。基于 HA 2026.5.4、554→583 entity、本日新增"机柜空调"插座
  (`2027457700`)与"温湿度计 Pro"(`miaomiaoce...t8`)。插座↔节点映射采用 2026-05-27 实测验证版。
- **2026-05-31 (later)** 镜像至 Hearth `docs/requirements/ha-integration.md`,新增 §0 Actors / §0.5 UCs / §9 Hearth 迁移要点,作为 Hearth 项目 Phase 1 需求输入。
