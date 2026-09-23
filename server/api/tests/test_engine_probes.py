"""ds4 / q27 / EXL3 三种后端解析的离线自测。

为什么是离线: 本集群 2026-09-19 没有这三种实例(只有 SGLang + oMLX), 接不上真实
端点。样本取自 sparkDash 的单元测试固定值
(sparkDash 的 __tests__/LlmProbe.q27.test.js 等),
那边是对着真实引擎抓下来的。

⛔ 通过这些用例只证明【解析没写错】, 不证明【口径对】。真接上实例时必须重新核对
   字段语义(尤其 ds4 的两个 tok_s gauge 是不是 60s 窗口、q27 的 ttft 是否含排队)。

跑法: python3 server/api/tests/test_engine_probes.py   # 在仓库根目录跑
"""
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("HEARTH_CONFIG", os.environ.get("HEARTH_CONFIG", ""))

import main  # noqa: E402

Q27_METRICS = """# TYPE q27_requests_total counter
q27_requests_total{api="chat"} 3
q27_requests_total{api="messages"} 1
# TYPE q27_prompt_tokens_total counter
q27_prompt_tokens_total{api="chat"} 200
q27_prompt_tokens_total{api="messages"} 50
# TYPE q27_prefill_computed_tokens_total counter
q27_prefill_computed_tokens_total{api="chat"} 150
q27_prefill_computed_tokens_total{api="messages"} 50
# TYPE q27_prefill_cached_tokens_total counter
q27_prefill_cached_tokens_total{api="chat"} 50
# TYPE q27_decode_tokens_total counter
q27_decode_tokens_total{api="chat"} 400
q27_decode_tokens_total{api="messages"} 100
# TYPE q27_requests_inflight gauge
q27_requests_inflight 2
# TYPE q27_slots_total gauge
q27_slots_total 4
# TYPE q27_kv_usage_perc gauge
q27_kv_usage_perc 0.42
# TYPE q27_spec_accept_ratio gauge
q27_spec_accept_ratio 0.87
# TYPE q27_ttft_seconds histogram
q27_ttft_seconds_bucket{api="chat",le="0.010"} 0
q27_ttft_seconds_bucket{api="chat",le="0.500"} 2
q27_ttft_seconds_bucket{api="chat",le="+Inf"} 2
q27_ttft_seconds_sum{api="chat"} 0.9
q27_ttft_seconds_count{api="chat"} 2
"""

# ds4 的实算 prefill 是 label 不是独立指标名(对照 LlmProbe.js:625-632):
# kind="computed" 才是实算, kind="cached" 是命中, 不带 label 求和 = 两者之和。
DS4_METRICS = """# TYPE ds4_tokens_decoded_total counter
ds4_tokens_decoded_total 12345
# TYPE ds4_tokens_prefilled_total counter
ds4_tokens_prefilled_total{kind="computed"} 1000
ds4_tokens_prefilled_total{kind="cached"} 9000
# TYPE ds4_requests_inflight gauge
ds4_requests_inflight 1
# TYPE ds4_decode_tok_s gauge
ds4_decode_tok_s 61.5
# TYPE ds4_prefill_tok_s gauge
ds4_prefill_tok_s 1830.0
"""

DS4_METRICS_NOLABEL = """# TYPE ds4_tokens_decoded_total counter
ds4_tokens_decoded_total 12345
# TYPE ds4_tokens_prefilled_total counter
ds4_tokens_prefilled_total 10000
"""

# 只有 cached label 的构建: computed 要能由 总量 - cached 精确推导出来。
# ⚠️ fixture 必须贴近真实 exporter: 真实 exporter 不会对同一指标名【同时】发
#    labeled 与 unlabeled 两条序列(w1W:p1 2026-09-19 写测试时踩过, 记此备忘)。
DS4_METRICS_CACHED_ONLY = """# TYPE ds4_tokens_decoded_total counter
ds4_tokens_decoded_total 12345
# TYPE ds4_tokens_prefilled_total counter
ds4_tokens_prefilled_total{kind="cached"} 9000
ds4_tokens_prefilled_total{kind="other"} 1000
"""

DS4_LABELED = {"ds4_tokens_prefilled_total":
               ("kind", {"computed": "__ds4_prefill_computed",
                         "cached": "__ds4_prefill_cached"})}

VLLM_METRICS = 'vllm:generation_tokens_total{model_name="x"} 10.0\n'

fails = []


def check(name, cond, got=None):
    if cond:
        print(f"  通过  {name}")
    else:
        print(f"  失败  {name}  实际={got!r}")
        fails.append(name)


print("q27 解析")
q = main._prom_parse(Q27_METRICS, main._Q27_SCALARS, {"q27_ttft_seconds": "__ttft_buckets"})
check("同名多 label 求和 decode=500", q.get("q27_decode_tokens_total") == 500.0, q.get("q27_decode_tokens_total"))
check("requests_total=4", q.get("q27_requests_total") == 4.0, q.get("q27_requests_total"))
check("kv_usage_perc=0.42", q.get("q27_kv_usage_perc") == 0.42, q.get("q27_kv_usage_perc"))
check("slots_total=4", q.get("q27_slots_total") == 4.0, q.get("q27_slots_total"))
check("TTFT 桶进 __ttft_buckets", (q.get("__ttft_buckets") or {}).get("0.500") == 2.0, q.get("__ttft_buckets"))
check("TTFT sum/count 落到标量", q.get("q27_ttft_seconds_sum") == 0.9 and q.get("q27_ttft_seconds_count") == 2.0,
      (q.get("q27_ttft_seconds_sum"), q.get("q27_ttft_seconds_count")))
hit = q["q27_prefill_cached_tokens_total"] / (q["q27_prefill_cached_tokens_total"] + q["q27_prefill_computed_tokens_total"])
check("前缀缓存命中 50/(50+200)=20%", round(hit * 100, 1) == 20.0, round(hit * 100, 1))

print("ds4 解析")
d = main._prom_parse(DS4_METRICS, main._DS4_SCALARS, {}, DS4_LABELED)
check("decoded=12345", d.get("ds4_tokens_decoded_total") == 12345.0, d.get("ds4_tokens_decoded_total"))
check("decode_tok_s=61.5", d.get("ds4_decode_tok_s") == 61.5, d.get("ds4_decode_tok_s"))
check("kind=computed 单独取出 1000", d.get("__ds4_prefill_computed") == 1000.0, d.get("__ds4_prefill_computed"))
check("kind=cached 单独取出 9000", d.get("__ds4_prefill_cached") == 9000.0, d.get("__ds4_prefill_cached"))
check("不带 label 的总量仍是两者之和 10000",
      d.get("ds4_tokens_prefilled_total") == 10000.0, d.get("ds4_tokens_prefilled_total"))
check("命中率 9000/(9000+1000)=90%",
      round(d["__ds4_prefill_cached"] / (d["__ds4_prefill_cached"] + d["__ds4_prefill_computed"]) * 100, 1) == 90.0)
# 旧构建没有 kind label: 实算量取不到 → 整个缺席, 不许退回含命中的总量
d2 = main._prom_parse(DS4_METRICS_NOLABEL, main._DS4_SCALARS, {}, DS4_LABELED)
check("无 label 时实算量缺席(不退回总量)", d2.get("__ds4_prefill_computed") is None, d2.get("__ds4_prefill_computed"))
check("无 label 时总量仍可读=10000", d2.get("ds4_tokens_prefilled_total") == 10000.0, d2.get("ds4_tokens_prefilled_total"))
# 只有 cached label: computed = 总量 - cached, 精确推导(不是近似)
d3 = main._prom_parse(DS4_METRICS_CACHED_ONLY, main._DS4_SCALARS, {}, DS4_LABELED)
_tot, _ca = d3.get("ds4_tokens_prefilled_total"), d3.get("__ds4_prefill_cached")
check("只有 cached label 时 computed 缺席", d3.get("__ds4_prefill_computed") is None)
check("总量-cached 推出 computed=1000", _tot is not None and _ca is not None and _tot - _ca == 1000.0,
      (_tot, _ca))

print("识别标记(与 sparkDash 同一条判据)")
check("ds4 标记命中", bool(re.search(r"(?m)^ds4_tokens_decoded_total[{\s]", DS4_METRICS)))
check("q27 标记命中", bool(re.search(r"(?m)^q27_decode_tokens_total[{\s]", Q27_METRICS)))
check("vLLM 不会被认成 ds4", not re.search(r"(?m)^ds4_tokens_decoded_total[{\s]", VLLM_METRICS))
check("vLLM 不会被认成 q27", not re.search(r"(?m)^q27_decode_tokens_total[{\s]", VLLM_METRICS))

print("EXL3 /health 判据")


def looks_exl3(d0):
    return isinstance(d0, dict) and (
        d0.get("backend") == "exl3" or (d0.get("ok") is True and isinstance(d0.get("busy"), bool)))


check("{backend:exl3} → 是", looks_exl3({"backend": "exl3"}))
check("{ok:true,busy:false} → 是", looks_exl3({"ok": True, "busy": False}))
check("vLLM 空 /health → 否", not looks_exl3({}))
check("{status:ok} → 否", not looks_exl3({"status": "ok"}))

print("通用滑动窗口速率 _gen_rate")
main._GEN_HIST.clear()
check("第一拍无基准 → None", main._gen_rate("t", 100.0, 1000.0) == (None, None))
check("窗口不足 8s → None", main._gen_rate("t", 200.0, 1003.0) == (None, None))
r, w = main._gen_rate("t", 1100.0, 1010.0)
check("10s 内 +1000 → 100/s", r == 100.0 and w == 10.0, (r, w))
main._GEN_HIST.clear()
main._gen_rate("t2", 500.0, 2000.0)
check("计数器回退(引擎重启) → None", main._gen_rate("t2", 10.0, 2010.0) == (None, None))

print()
if fails:
    print(f"失败 {len(fails)} 项: {fails}")
    sys.exit(1)
print("全部通过")
