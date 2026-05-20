"""Hearth screenshot capture — Playwright + Firefox.

Loads the locally-running monitor (http://127.0.0.1/), injects DOM
substitutions to replace private identifiers (real IPs, real host
names, "Tony's" branding) with generic Hearth-flavored placeholders,
then captures desktop + mobile screenshots for the README.

This script is committed for reproducibility (anyone with Playwright
installed can re-run it).  Edit the SUBSTITUTIONS dict if you want
different generic names.

Run:
    /tmp/hearth-shot-venv/bin/python docs/screenshots/_capture.py
"""
from __future__ import annotations
import asyncio, pathlib, sys
from playwright.async_api import async_playwright

URL = "http://127.0.0.1/"
OUT = pathlib.Path(__file__).parent

# Privacy redaction: real → generic (case-sensitive whole-substring match)
SUBSTITUTIONS = {
    "192.168.1.20":  "10.0.0.1",
    "192.168.1.151": "10.0.0.2",
    "192.168.1.156": "10.0.0.3",
    "192.168.1.188": "10.0.0.4",
    "192.168.1.189": "10.0.0.5",
    "Atlas":         "Workstation",
    "Spark-01":      "Inference-1",
    "Spark-02":      "Inference-2",
    "Spark-03":      "Inference-3",
    "Spark-04":      "Inference-4",
    "spark-01":      "inference-1",
    "spark-02":      "inference-2",
    "spark-03":      "inference-3",
    "spark-04":      "inference-4",
    "spark-34d3":    "infer-host-1",
    "spark-5135":    "infer-host-2",
    "spark-1475":    "infer-host-3",
    "spark-25c1":    "infer-host-4",
    "rtx4090-pc":    "workstation-host",
    "Tony 的家庭智算中心监控系统": "Hearth · Home AI Compute Monitor",
    "Tony 的家庭智算中心監控系統": "Hearth · Home AI Compute Monitor",
    "Tony 的家庭智算中心":          "Hearth",
    "Tony's AI Center":             "Hearth",
    "Tony's Home AI Compute Center · Monitor": "Home AI Compute Monitor",
    "DeepSeek-V4-Flash":            "Llama-Inferno-70B",   # placeholder model names
    "deepseek-v4-flash":            "llama-inferno-70b",
    "Qwen3-Coder-Next":             "Qwen-Coder-32B",
    "qwen3-coder-next":             "qwen-coder-32b",
    "MiniMax-M2.7":                 "Yi-Reasoner-34B",
    "Gemma-4-31B-abliterated":      "Gemma-3-27B",
    "gemma-4-31b-abliterated":      "gemma-3-27b",
    "Qwen3-VL-8B-abliterated":      "Qwen-VL-7B",
    # Subnet / IP-range notation
    "192.168.1.0/24":               "10.0.0.0/24",
    "192.168.1.":                    "10.0.0.",                # safety net
    # Hero descriptor phrase (private topology details)
    "A unified telemetry surface for the home AI fabric — RTX 4090 edge + four DGX Spark inference nodes, served behind a single LiteLLM gateway. Every TFLOP, every token, every watt — in real time.":
        "A unified telemetry surface for your home AI compute cluster — auto-discovers models, surfaces real metrics from vLLM / llama.cpp / LiteLLM, honestly labels what backends don't expose. Every token, every watt — in real time.",
    "RTX 4090 edge + four DGX Spark inference nodes": "mixed GPU cluster, one gateway",
    "served behind a single LiteLLM gateway":          "behind a LiteLLM gateway",
    # Section headlines / descriptions that leak topology specifics
    "Five machines. ":                                 "Your fleet. ",
    "Atlas — the RTX 4090 host — runs the LiteLLM gateway and edge-class workloads. Four DGX Spark boxes carry the heavy inference. Click a node for the full forensic view.":
        "Each host runs the LiteLLM gateway, an inference engine, or both. Click a node for the full forensic view.",
    "DGX Spark":                                       "GPU Node",
    "DGX SPARK":                                       "GPU NODE",
}

# CSS that disables the scroll-reveal so full-page screenshots show all sections.
DISABLE_REVEAL = """
() => {
  const s = document.createElement('style');
  s.textContent = `
    .reveal, .reveal.in { opacity: 1 !important; transform: none !important; }
    *, *::before, *::after { animation-duration: 0s !important; transition-duration: 0s !important; }
  `;
  document.head.appendChild(s);
}
"""

# JS that walks all text nodes and rewrites them in-place.
INJECT = """
(subs) => {
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  const keys = Object.keys(subs).sort((a, b) => b.length - a.length);  // longest first
  let n;
  while ((n = walker.nextNode())) {
    let t = n.nodeValue;
    let changed = false;
    for (const k of keys) {
      if (t.includes(k)) { t = t.split(k).join(subs[k]); changed = true; }
    }
    if (changed) n.nodeValue = t;
  }
  // Title
  if (document.title) {
    let t = document.title;
    for (const k of keys) if (t.includes(k)) t = t.split(k).join(subs[k]);
    document.title = t;
  }
}
"""


async def shoot(page, name, full_page=True):
    await page.wait_for_timeout(800)                    # SSE settle
    await page.evaluate(INJECT, SUBSTITUTIONS)
    await page.wait_for_timeout(300)
    path = OUT / f"{name}.png"
    await page.screenshot(path=str(path), full_page=full_page)
    print(f"  ✓ {path.name}  ({path.stat().st_size // 1024} KB)")


async def main():
    async with async_playwright() as pw:
        browser = await pw.firefox.launch(headless=True)

        # ── Desktop ─────────────────────────────────────────────────
        ctx = await browser.new_context(
            viewport={"width": 1480, "height": 900},
            device_scale_factor=2,                       # retina-quality
        )
        page = await ctx.new_page()
        await page.goto(URL, wait_until="domcontentloaded", timeout=20000)
        await page.evaluate(DISABLE_REVEAL)
        await page.wait_for_timeout(2500)                # SSE first frame + render settle
        # Hero 截首屏(1480x900), 而非全页(过长不适合 README)
        await page.evaluate(INJECT, SUBSTITUTIONS)
        await page.wait_for_timeout(200)
        await page.screenshot(path=str(OUT / "01-desktop-overview.png"),
                              clip={"x": 0, "y": 0, "width": 1480, "height": 900})
        print("  ✓ 01-desktop-overview.png  (viewport hero shot)")

        # 截某些 section 局部 (滚动到 anchor 再裁视口)
        for anchor, name in [
            ("#cluster",   "02-desktop-cluster"),
            ("#nodes",     "03-desktop-nodes"),
            ("#models",    "04-desktop-models"),
            ("#telemetry", "05-desktop-telemetry"),
        ]:
            try:
                await page.evaluate(f"document.querySelector('{anchor}')?.scrollIntoView({{behavior:'instant',block:'start'}})")
                await page.wait_for_timeout(800)
                await page.evaluate(INJECT, SUBSTITUTIONS)
                await page.wait_for_timeout(200)
                await page.screenshot(path=str(OUT / f"{name}.png"), full_page=False,
                                       clip={"x": 0, "y": 0, "width": 1480, "height": 900})
                print(f"  ✓ {name}.png")
            except Exception as e:
                print(f"  ✗ {name}: {e}")
        await ctx.close()

        # ── Mobile (iPhone 14 Pro 视口) ─────────────────────────────
        ctx_m = await browser.new_context(
            viewport={"width": 390, "height": 844},
            device_scale_factor=3,
        )
        page_m = await ctx_m.new_page()
        await page_m.goto(URL, wait_until="domcontentloaded", timeout=20000)
        await page_m.evaluate(DISABLE_REVEAL)
        await page_m.wait_for_timeout(2500)
        # Mobile 首屏(390x844)做 hero, 显示 nav + 漢堡 + Hero
        await page_m.evaluate(INJECT, SUBSTITUTIONS)
        await page_m.wait_for_timeout(200)
        await page_m.screenshot(path=str(OUT / "06-mobile-overview.png"),
                                clip={"x": 0, "y": 0, "width": 390, "height": 844})
        print("  ✓ 06-mobile-overview.png  (viewport hero shot)")

        # Mobile 局部:nav + hero, 模型列表, telemetry
        for anchor, name in [
            ("#cluster",   "07-mobile-cluster"),
            ("#models",    "08-mobile-models"),
        ]:
            try:
                await page_m.evaluate(f"document.querySelector('{anchor}')?.scrollIntoView({{behavior:'instant',block:'start'}})")
                await page_m.wait_for_timeout(600)
                await page_m.evaluate(INJECT, SUBSTITUTIONS)
                await page_m.wait_for_timeout(200)
                await page_m.screenshot(path=str(OUT / f"{name}.png"), full_page=False)
                print(f"  ✓ {name}.png")
            except Exception as e:
                print(f"  ✗ {name}: {e}")

        await ctx_m.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
