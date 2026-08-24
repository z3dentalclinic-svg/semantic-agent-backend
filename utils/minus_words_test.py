"""
minus_words_test.py — стенд минус-слов. build: mw_0.2

Конвейер Andrew (4 вызова ПОСЛЕДОВАТЕЛЬНО, без парсинга и фильтров):
  1. FINDER — лёгкая модель, ОДИН раз идёт в интернет: «найди самый полный список минус-слов»
  2. модель 2 получает список + сид + регион → дополняет
  3. модель 3 получает УЖЕ ДОПОЛНЕННЫЙ список → дополняет
  4. модель 4 получает список после 3 → дополняет
  Вызовы 2-4 в интернет не ходят.

Регистрация в main.py:
    from minus_words_test import register_minus_words_test
    register_minus_words_test(app)

Эндпоинты:
    GET  /minus-test          — HTML стенд (файл minus_test.html рядом)
    POST /api/minus-test      — {"seed", "region", "finder", "extenders": [..], "thinking"}

Ключи из окружения: GEMINI_API_KEY, OPENAI_API_KEY, ANTHROPIC_API_KEY
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

BUILD = "mw_0.2"

# ─── реестр моделей: цена $ за 1M токенов (in, out); поправь под актуальный прайс ───
MODELS: dict[str, dict] = {
    "gemini-3.7-flash":      {"vendor": "gemini",    "price": (0.30, 2.50), "search": True},
    "gemini-3.6-flash":      {"vendor": "gemini",    "price": (0.30, 2.50), "search": True},
    "gemini-3.1-flash-lite": {"vendor": "gemini",    "price": (0.10, 0.40), "search": True},
    "gpt-5.6-sol":           {"vendor": "openai",    "price": (1.25, 10.0), "search": True},
    "claude-fable-5":        {"vendor": "anthropic", "price": (5.0, 25.0),  "search": True},
    "claude-opus-5":         {"vendor": "anthropic", "price": (15.0, 75.0), "search": True},
}
SEARCH_PRICE_PER_CALL = {"gemini": 0.035, "openai": 0.01, "anthropic": 0.01}

DEFAULT_FINDER = "gemini-3.1-flash-lite"
DEFAULT_EXTENDERS = ["gemini-3.7-flash", "claude-fable-5", "gpt-5.6-sol"]  # порядок = порядок цепочки

# ─── промпты (формулировки Andrew, дословно) ───
FINDER_PROMPT = (
    "Найди пожалуйста самый полный список минус слов для рекламы Google Ads для этого сида: «{seed}».\n"
    "Ответ: одно минус-слово на строку, без нумерации и пояснений."
)
EXTENDER_PROMPT = (
    "Вот список минус слов:\n{found}\n\n"
    "Вот сид: «{seed}»\nВот регион поиска: {region}\n"
    "Дополни этот список недостающими минус словами.\n"
    "Ответ: только новые слова, одно на строку, без нумерации и пояснений."
)


# ══════════════════════════ вызовы вендоров ══════════════════════════

async def _call_gemini(model: str, prompt: str, search: bool, thinking: str) -> dict:
    key = os.environ["GEMINI_API_KEY"]
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    body: dict = {"contents": [{"role": "user", "parts": [{"text": prompt}]}]}
    gen: dict = {}
    if thinking != "off":
        gen["thinkingConfig"] = {"thinkingLevel": thinking}
    if gen:
        body["generationConfig"] = gen
    if search:
        body["tools"] = [{"google_search": {}}]
    async with httpx.AsyncClient(timeout=180) as c:
        r = await c.post(url, json=body)
        r.raise_for_status()
        d = r.json()
    text = "".join(p.get("text", "") for p in d["candidates"][0]["content"].get("parts", []))
    um = d.get("usageMetadata", {})
    sources = []
    gm = d["candidates"][0].get("groundingMetadata", {})
    for ch in gm.get("groundingChunks", []):
        w = ch.get("web", {})
        if w.get("uri"):
            sources.append({"title": w.get("title", ""), "uri": w["uri"]})
    return {
        "text": text,
        "in": um.get("promptTokenCount", 0),
        "out": um.get("candidatesTokenCount", 0),
        "think": um.get("thoughtsTokenCount", 0),
        "sources": sources,
        "searched": bool(gm),
    }


async def _call_openai(model: str, prompt: str, search: bool, thinking: str) -> dict:
    key = os.environ["OPENAI_API_KEY"]
    body: dict = {"model": model, "input": prompt}
    if thinking != "off":
        body["reasoning"] = {"effort": thinking}
    if search:
        body["tools"] = [{"type": "web_search"}]
    async with httpx.AsyncClient(timeout=180) as c:
        r = await c.post("https://api.openai.com/v1/responses",
                         headers={"Authorization": f"Bearer {key}"}, json=body)
        r.raise_for_status()
        d = r.json()
    text, sources, searched = "", [], False
    for item in d.get("output", []):
        if item.get("type") == "web_search_call":
            searched = True
        if item.get("type") == "message":
            for ct in item.get("content", []):
                if ct.get("type") == "output_text":
                    text += ct.get("text", "")
                    for a in ct.get("annotations", []):
                        if a.get("url"):
                            sources.append({"title": a.get("title", ""), "uri": a["url"]})
    u = d.get("usage", {})
    return {"text": text, "in": u.get("input_tokens", 0), "out": u.get("output_tokens", 0),
            "think": u.get("output_tokens_details", {}).get("reasoning_tokens", 0),
            "sources": sources, "searched": searched}


async def _call_anthropic(model: str, prompt: str, search: bool, thinking: str) -> dict:
    key = os.environ["ANTHROPIC_API_KEY"]
    body: dict = {"model": model, "max_tokens": 4000,
                  "messages": [{"role": "user", "content": prompt}]}
    if thinking != "off":
        body["thinking"] = {"type": "enabled", "budget_tokens": {"low": 1024, "medium": 4000, "high": 8000}[thinking]}
        body["max_tokens"] = body["thinking"]["budget_tokens"] + 4000
    if search:
        body["tools"] = [{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}]
    async with httpx.AsyncClient(timeout=180) as c:
        r = await c.post("https://api.anthropic.com/v1/messages",
                         headers={"x-api-key": key, "anthropic-version": "2023-06-01"}, json=body)
        r.raise_for_status()
        d = r.json()
    text, sources, searched = "", [], False
    for blk in d.get("content", []):
        t = blk.get("type")
        if t == "text":
            text += blk.get("text", "")
        elif t == "server_tool_use":
            searched = True
        elif t == "web_search_tool_result":
            for res in blk.get("content", []) if isinstance(blk.get("content"), list) else []:
                if res.get("url"):
                    sources.append({"title": res.get("title", ""), "uri": res["url"]})
    u = d.get("usage", {})
    return {"text": text, "in": u.get("input_tokens", 0), "out": u.get("output_tokens", 0),
            "think": 0, "sources": sources, "searched": searched}


_CALLERS = {"gemini": _call_gemini, "openai": _call_openai, "anthropic": _call_anthropic}


async def call_model(model: str, prompt: str, *, search: bool, thinking: str) -> dict:
    meta = MODELS[model]
    t0 = time.perf_counter()
    try:
        res = await _CALLERS[meta["vendor"]](model, prompt, search, thinking)
        err = None
    except Exception as e:  # noqa: BLE001
        res, err = {"text": "", "in": 0, "out": 0, "think": 0, "sources": [], "searched": False}, f"{type(e).__name__}: {e}"
    pin, pout = meta["price"]
    cost = (res["in"] * pin + (res["out"] + res["think"]) * pout) / 1_000_000
    if search and res["searched"]:
        cost += SEARCH_PRICE_PER_CALL[meta["vendor"]]
    res.update({"model": model, "wall": round(time.perf_counter() - t0, 2),
                "cost": round(cost, 5), "error": err, "search": search})
    return res


# ══════════════════════════ разбор и слияние ══════════════════════════

_STRIP = re.compile(r"^[\s\-\*\•\d\.\)\]]+|[\s\-\*\•]+$")


def parse_list(text: str) -> list[str]:
    out, seen = [], set()
    for line in text.splitlines():
        w = _STRIP.sub("", line).strip().strip('"«»').lower()
        if not w or len(w) > 60 or ":" in w and len(w.split()) > 4:
            continue
        w = re.sub(r"\s+", " ", w)
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out


def merge(stages: list[tuple[str, list[str]]]) -> list[dict]:
    """stages: [(model, слова, которые эта модель ДОБАВИЛА)] в порядке цепочки"""
    rows, seen = [], set()
    for i, (model, ws) in enumerate(stages):
        for w in ws:
            if w in seen:
                continue
            seen.add(w)
            rows.append({"word": w, "stage": i + 1, "by": model})
    return rows


# ══════════════════════════ конвейер ══════════════════════════

class MinusReq(BaseModel):
    seed: str
    region: str = "Украина"
    finder: str = DEFAULT_FINDER
    extenders: list[str] = DEFAULT_EXTENDERS
    thinking: str = "low"          # off | low | medium | high


async def run_minus(req: MinusReq) -> dict:
    t0 = time.perf_counter()
    seed = req.seed.strip()

    # 1. finder — единственный вызов с поиском
    finder = await call_model(req.finder, FINDER_PROMPT.format(seed=seed), search=True, thinking=req.thinking)
    current = parse_list(finder["text"])
    stages: list[tuple[str, list[str]]] = [(finder["model"], list(current))]
    calls = [finder]

    # 2-4. цепочка: каждая модель получает список, дополненный предыдущей
    for m in req.extenders:
        prompt = EXTENDER_PROMPT.format(found="\n".join(current) or "(пусто)", seed=seed, region=req.region)
        r = await call_model(m, prompt, search=False, thinking=req.thinking)
        calls.append(r)
        added = [w for w in parse_list(r["text"]) if w not in set(current)]
        stages.append((m, added))
        current = current + added

    rows = merge(stages)
    stats = {
        "build": BUILD,
        "seed": seed, "region": req.region, "thinking": req.thinking,
        "total_cost": round(sum(c["cost"] for c in calls), 5),
        "total_wall": round(time.perf_counter() - t0, 2),
        "finder_count": len(stages[0][1]),
        "added_by_stage": {m: len(ws) for m, ws in stages[1:]},
        "merged_count": len(rows),
        "calls": [{k: v for k, v in c.items() if k != "text"} for c in calls],
    }
    return {"rows": rows, "list": current, "stats": stats,
            "raw": {c["model"]: c["text"] for c in calls}}


# ══════════════════════════ регистрация ══════════════════════════

def register_minus_words_test(app: FastAPI) -> None:
    html_path = Path(__file__).with_name("minus_test.html")

    @app.get("/minus-test", response_class=HTMLResponse)
    async def minus_page():
        return html_path.read_text(encoding="utf-8")

    @app.post("/api/minus-test")
    async def minus_api(req: MinusReq):
        unknown = [m for m in [req.finder, *req.extenders] if m not in MODELS]
        if unknown:
            return JSONResponse({"error": f"unknown models: {unknown}"}, status_code=400)
        return await run_minus(req)

    @app.get("/api/minus-test/models")
    async def minus_models():
        return {"models": list(MODELS), "finder": DEFAULT_FINDER, "extenders": DEFAULT_EXTENDERS, "build": BUILD}
