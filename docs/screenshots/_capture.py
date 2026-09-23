"""Hearth screenshot capture — Playwright + Firefox.

Loads the locally-running monitor (http://127.0.0.1/), injects DOM
substitutions to replace private identifiers (real IPs, host names,
personal branding) with generic placeholders. The real mapping lives
outside the repo (see load_substitutions below),
then captures desktop + mobile screenshots for the README.

This script is committed for reproducibility (anyone with Playwright
installed can re-run it).  Put your own mapping in _local/redactions.json
(or point HEARTH_REDACTIONS at one).

Run:
    /tmp/hearth-shot-venv/bin/python docs/screenshots/_capture.py
"""
from __future__ import annotations
import asyncio, pathlib, sys
from playwright.async_api import async_playwright

URL = "http://127.0.0.1/"
OUT = pathlib.Path(__file__).parent

# Privacy redaction: real → generic (case-sensitive whole-substring match)
# ── 隐私脱敏表 ────────────────────────────────────────────────────────
# ⛔ 真值【不进仓库】: 这里只留一份格式示例。真实映射放本地文件, 默认
#    <repo>/_local/redactions.json(该目录 gitignore), 也可用环境变量
#    HEARTH_REDACTIONS=<path> 指定别的位置。
#    没有该文件时按下面的示例跑 —— 截图里就不会被替换, 请自行补表再发布。
# 格式: {"要替换的真值": "公开用的占位符"}, 大小写敏感的整子串匹配。
EXAMPLE_SUBSTITUTIONS = {
    "10.0.0.10":            "10.0.0.1",          # 真实 IP → 文档用网段
    "my-gpu-host":          "workstation-host",  # 真实主机名 → 通用名
    "My Home AI Center":    "Hearth",            # 个人化标题 → 项目名
    "Some-Private-Model":   "Llama-Inferno-70B", # 未公开的模型名 → 占位模型名
}


def load_substitutions() -> dict:
    """真实脱敏表: 环境变量 > _local/redactions.json > 内置示例。"""
    import json as _json, os as _os
    cand = _os.environ.get("HEARTH_REDACTIONS", "")
    paths = [pathlib.Path(cand)] if cand else []
    paths.append(pathlib.Path(__file__).resolve().parents[2] / "_local" / "redactions.json")
    for f in paths:
        try:
            if f.is_file():
                m = _json.loads(f.read_text())
                if isinstance(m, dict) and m:
                    print(f"[capture] 脱敏表: {f} ({len(m)} 条)")
                    return m
        except Exception as e:
            print(f"[capture] 读不出脱敏表 {f}: {e}")
    print("[capture] 未找到本地脱敏表, 使用内置示例 —— 截图可能仍含真实标识, 发布前请检查")
    return dict(EXAMPLE_SUBSTITUTIONS)


SUBSTITUTIONS = load_substitutions()

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
