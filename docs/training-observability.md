# 分布式训练可观测性 — 4×GB10

> 研究 + 落地方案。背景:4 台 DGX Spark(GB10,128GB 统一内存)经 Ray 跑分布式训练,
> 节点间走 CX-7 RoCE 200G(`enp1s0f1np1`,内网 10.10.0.x)。Hearth 现有监控面向推理,
> 训练场景零覆盖。本文给出该监控什么、用户最关注什么、如何呈现,以及对训练**零影响**的落地路径。

## 0. 框架:训练监控 ≠ 推理监控

推理问"快不快、在不在线";训练问 **"还在进步吗?效率高吗?会不会悄悄发散/卡死?"**。
训练是数小时到数周的长作业,**没及时发现问题的代价 = 烧掉的 GPU-天**。重点从"当前值"
转向"轨迹"和"活性"。**原始 GPU util 在训练场景具有误导性**(见 Layer E 的静默 stall)。

## 1. 指标体系(按层)

### Layer A — 训练进度/正确性(ML 信号,用户盯得最紧)
- **Loss**(每步 + EMA 平滑)— 第一指标,必须曲线
- **Grad norm** — 不稳定早期预警;业界把 loss/grad-norm **5× 跳变或 NaN** 直接判故障
- **Learning rate**(warmup/decay)— loss 只有配 LR 才可解释
- **Val loss / eval 指标**、**tokens/samples 已处理**、**epoch 进度**、**loss scale**(混精)
- 来源:**训练框架**(wandb/tensorboard/console),DCGM 看不到 → Hearth 最大盲区

### Layer B — 吞吐与效率
- **Throughput**:tokens/s、samples/s、steps/s(per-GPU)
- **Step time + 拆解**(compute/comm/data/optimizer)— 最强单一效率诊断
- **MFU**(Model FLOPs Utilization)— 效率头条,衡量集群有没有被浪费
- **ETA** — step_time × 剩余步数,用户真正想问的

### Layer C — GPU 健康(DCGM 已有大部分)
- 显存 used/free — 训练贴内存上限跑,**OOM = run 死**(GB10 已实测 OOM 硬重启)
- 功率/温度/时钟 — **热降频悄悄拖慢训练**
- **ECC / Xid** — 单个 Xid 能中途搞死节点、损坏权重

### Layer D — 分布式/互联(多节点独有)
- **RoCE 带宽 tx/rx**(all-reduce 走 CX-7 200G)+ RDMA/IB 错误计数
- **通信占比**:step 里 comm 的比例,comm 主导 = 扩展性坏
- **Straggler / rank 偏斜**:per-rank step-time 方差。研究:大作业 **42.5%–59% 遇 fail-slow
  straggler,拖慢 ~10–35% GPU-小时**,主因多为负载不均(pipeline 偏斜、GC)非硬件

### Layer E — 作业活性/病理检测
- **步进心跳看门狗**:step 还在涨吗?**NCCL 静默挂死** = GPU util 100%、**功率却低(~70W)、
  零进度,仪表盘全绿却烧预算**。检测必须基于**任务级步进**,不能信 util
- **NaN/Inf 探测**、吞吐突降、**最近 checkpoint 年龄**(崩了别丢几小时)

## 2. 用户最关注什么(排序)

**Tier 1(每几分钟扫一眼 / 该告警)**
1. Loss 曲线(还在降?NaN?)
2. 步进 + ETA(活着?在推进?还要多久?)
3. 显存余量(OOM = 死)
4. 吞吐 + MFU(有没有浪费集群)

**Tier 2(觉得不对时查)**
5. Grad norm · 6. per-rank step-time 偏斜(straggler)· 7. comm/compute 占比 · 8. 温度/功率/降频、Xid/ECC

**Tier 3(深挖/复盘)**
9. per-layer 梯度/权重统计、LR 调度、loss scale、单链路带宽、网络错误计数

> **核心洞察**:Tier 1 全是"活着/在进步/没 OOM/效率",**不是原始 GPU util**(推理思维,训练里会骗你)。

## 3. 如何呈现(Hearth Apple-console 语言)

独立 **Training run-card**(区别于 inference Models),绑定当前作业:
- **顶栏**:模型 · #GPU · `step X/Y` · elapsed/**ETA** · 状态 pill(running / **stalled** / diverged)
- **主视觉 = Loss 曲线**(大 AreaChart,可选 log-y,叠 EMA);**下方副轴叠 grad-norm**
- **进度块**:大 step 计数 + 进度条 + tokens-seen + **ETA**
- **效率条**:tokens/s · **MFU% 环**(训练版"唯一重要的环",替推理 tps 环)· step-time compute/comm/data 堆叠微条
- **per-rank 小多图(4×GB10)**:每节点**显存(OOM 关键)** + 温度 + step-time,**最慢 rank 染色**
- **互联面板**:RoCE tx/rx sparkline + comm 占比 + 链路错误徽标
- **健康 lane**:NaN · **stall(N 秒无步进)** · Xid/ECC · 降频 · checkpoint 年龄

呈现原则:① **曲线 > 瞬时值**(与推理相反)② **"卡死"一眼可辨**——用步进非 util 判活性
③ **ETA 最显眼** ④ loss EMA + raw 叠加平滑。

## 4. 数据源 & 落地(对训练零影响)

### Phase 1 — 纯底座,零接触训练作业
全部来自**已经在采**的 DCGM(L1,~0% GPU 开销)+ node-exporter,经 obs-prometheus 联邦,
Hearth 只读 PromQL,**不碰 Spark 节点、不改 DCGM、不开 profiling**:
- per-rank GB10:显存余量 / 温度 / 功率 / **Xid·ECC** / util
- **RoCE 带宽 + IB 错误计数**(node-exporter `node_network_*{device="enp1s0f1np1"}` rate +
  `node_infiniband_*`;接上 Hearth 现有 `rdmaIn/rdmaOut` 占位)
- **★ 静默 stall 探测器(高性价比)**:`高 util + 异常低功率(~70W) + RoCE 带宽掉零` =
  大概率 NCCL 卡死。**不需训练框架任何数据,DCGM 现有信号即可算** → Tier-1 告警
- **straggler 提示**:跨 4 节点的 util/功率/带宽偏斜(底座近似;精确值需 Layer A 步进)

### Phase 2 — 需一个只读训练信号源(需用户指定,不碰作业)
Loss / grad-norm / step / LR / MFU / ETA 必须从训练框架已有日志读。只读接法:
- TensorBoard event 文件 / wandb export / JSON-lines 日志 → Hearth 加解析器
- 或训练框架暴露 prometheus endpoint → Hearth 直采
- MFU 从 step-time + 模型 FLOPs/step 推算(给常数)

> 只需用户告知"训练把指标写到哪",不翻作业配置、不改训练。

## 5. 路线

先上 **Phase 1**(本仓已实现:`/api/training` + Training section,纯 obs 只读,训练零影响)。
待用户给出训练信号源,叠 **Phase 2** 补全 loss/ETA/MFU。

---
## 参考
- MegaScale: Scaling LLM Training to >10,000 GPUs — arxiv 2402.15627
- Robust LLM Training Infrastructure at ByteDance — ACM 3731569.3764838
- From Detection to Recovery: LLM Pre-training with 504 GPUs (Lablup) — arxiv 2605.09370
- Efficient Training of LLMs on Distributed Infrastructures: A Survey — arxiv 2407.20018
- NVIDIA Dev Forums: Deterministic Pre-Collapse Signal for NCCL Silent Stall Detection (~t-30)
- NCCL Troubleshooting (official) · Crusoe: NCCL hangs from failed fabricmanager
