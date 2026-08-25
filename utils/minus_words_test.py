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

BUILD = "mw_1.0"

# ─── реестр моделей: цена $ за 1M токенов (in, out); поправь под актуальный прайс ───
MODELS: dict[str, dict] = {
    "gemini-3.7-flash":      {"vendor": "gemini",    "price": (0.30, 2.50), "search": True},
    "gemini-3.6-flash":      {"vendor": "gemini",    "price": (0.30, 2.50), "search": True},
    "gemini-3.1-flash-lite": {"vendor": "gemini",    "price": (0.10, 0.40), "search": True},
    "gpt-5.6-sol":           {"vendor": "openai",    "price": (4.0, 20.0),  "search": True},
    "gpt-5.6-terra":         {"vendor": "openai",    "price": (2.0, 12.0),  "search": True},
    "gpt-5.6-luna":          {"vendor": "openai",    "price": (0.20, 1.20), "search": True},
    "claude-sonnet-4-6":     {"vendor": "anthropic", "price": (3.0, 15.0),  "search": True},
    "claude-fable-5":        {"vendor": "anthropic", "price": (5.0, 25.0),  "search": True},
    "claude-opus-5":         {"vendor": "anthropic", "price": (15.0, 75.0), "search": True},
}
SEARCH_PRICE_PER_CALL = {"gemini": 0.035, "openai": 0.01, "anthropic": 0.01}   # openai: за каждый поиск + 8k input-токенов
OPENAI_SEARCH_INPUT_TOKENS = 8000

# Регион → ISO-код страны для user_location поиска OpenAI (конфиг, дополнять по мере надобности)
REGION_CODES = {"украина": "UA", "ukraine": "UA", "россия": "RU", "russia": "RU", "казахстан": "KZ",
                "kazakhstan": "KZ", "польша": "PL", "poland": "PL", "германия": "DE", "germany": "DE",
                "беларусь": "BY", "belarus": "BY", "ирландия": "IE", "ireland": "IE", "сша": "US", "usa": "US"}


def region_code(region: str) -> str | None:
    r = region.strip().lower()
    if len(r) == 2 and r.isalpha():
        return r.upper()
    return REGION_CODES.get(r)

DEFAULT_FINDER = "gemini-3.7-flash"  # Luna «искал» без источников (0.7); Gemini 3.7 реально открывал страницы (0.4)
DEFAULT_EXTENDERS = ["gemini-3.7-flash", "claude-sonnet-4-6", "gpt-5.6-luna"]  # порядок = порядок цепочки
DEFAULT_CENSOR = "gemini-3.7-flash"

# ─── промпты mw_0.7: широкая генерация (как в 0.5) + цензор (PRUNE из 0.6) ───
FINDER_PROMPT = (
    "Найди пожалуйста самый полный список минус слов для рекламы Google Ads для этого сида: «{seed}». "
    "Регион: {region}.\n"
    "Сделай один поиск в интернете и собери слова из найденных опубликованных списков. "
    "Ничего не придумывай сам: если слова нет в найденных источниках, не пиши его.\n"
    "Ответ: одно минус-слово на строку, без нумерации и пояснений."
)
EXTENDER_PROMPT = (
    "Вот список минус слов:\n{found}\n\n"
    "Вот сид: «{seed}»\nВот регион поиска: {region}\n"
    "Дополни этот список недостающими минус словами.\n"
    "Ответ: только новые слова, одно на строку, без нумерации и пояснений."
)
RELATE_PROMPT = (
    "Сид: «{seed}». Регион: {region}.\n"
    "Ниже пронумерованный список слов. Для каждого слова ответь на вопрос: "
    "фраза «{seed} + слово» — это уточнение или расширение сида?\n"
    "Ответ: номера слов, для которых ДА, через запятую. Ничего кроме номеров.\n\n{numbered}"
)
PRUNE_PROMPT = (
    "Регион: {region}. Фраза: «{seed}».\n"
    "Ниже пронумерованный список слов-кандидатов в минус-слова для рекламы по этой фразе.\n"
    "Проверь каждое: запрос «{seed} + слово» реально набирают, и человек в нём не является клиентом того, "
    "кто рекламируется по этой фразе. Слово, уточняющее или выбирающее тот же товар или услугу, не подходит. "
    "Проверка: убери слово из запроса — если человек остался тем же клиентом, слово не подходит.\n"
    "Ничего не добавляй. Ответ: номера слов, которые ОСТАВИТЬ, через запятую. Ничего кроме номеров.\n\n{numbered}"
)
# GEN_PROMPT из 0.6 (узкий генератор) снят — точность без широты; см. git-историю для отката


# ══════════════════════════ вызовы вендоров ══════════════════════════

GEMINI_SEARCH_SYSTEM = (
    "Сегодняшняя дата: {today}. Твоя внутренняя база устарела. "
    "Ответ обязан строиться только на результатах Google Search, выполненного сейчас."
)


async def _call_gemini(model: str, prompt: str, search: bool, thinking: str, country: str | None = None) -> dict:
    key = os.environ["GEMINI_API_KEY"]
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"

    def _body(search_tool: dict | None) -> dict:
        body: dict = {"contents": [{"role": "user", "parts": [{"text": prompt}]}]}
        gen: dict = {}
        if thinking != "off":
            gen["thinkingConfig"] = {"thinkingLevel": thinking}
        if search_tool is not None:
            gen["temperature"] = 0.0          # детерминированное решение «искать»
            body["system_instruction"] = {"parts": [{"text": GEMINI_SEARCH_SYSTEM.format(
                today=time.strftime("%Y-%m-%d"))}]}
            body["tools"] = [search_tool]
        if gen:
            body["generationConfig"] = gen
        return body

    # порядок попыток: legacy google_search_retrieval с порогом 0.0 (по совету Gemini) → google_search
    variants = ([("retrieval_t0", {"google_search_retrieval": {"dynamic_retrieval_config": {
                    "mode": "MODE_DYNAMIC", "dynamic_threshold": 0.0}}}),
                 ("google_search", {"google_search": {}})] if search else [("none", None)])
    used = "none"
    async with httpx.AsyncClient(timeout=180) as c:
        for name, tool in variants:
            r = await c.post(url, json=_body(tool))
            if r.status_code == 400 and name == "retrieval_t0":
                continue                      # инструмент не поддержан моделью → следующий вариант
            r.raise_for_status()
            d = r.json()
            used = name
            break
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
        "n_search": len(gm.get("webSearchQueries", [])) or (1 if gm else 0),
        "search_tool": used,
    }


async def _call_openai(model: str, prompt: str, search: bool, thinking: str, country: str | None = None) -> dict:
    key = os.environ["OPENAI_API_KEY"]
    body: dict = {"model": model, "input": prompt}
    if thinking != "off":
        body["reasoning"] = {"effort": thinking}
    if search:
        tool: dict = {"type": "web_search"}
        if country:
            tool["user_location"] = {"type": "approximate", "country": country}
        body["tools"] = [tool]
        body["tool_choice"] = {"type": "web_search"}   # принудительный поиск
    async with httpx.AsyncClient(timeout=180) as c:
        r = await c.post("https://api.openai.com/v1/responses",
                         headers={"Authorization": f"Bearer {key}"}, json=body)
        r.raise_for_status()
        d = r.json()
    text, sources, searched, n_search = "", [], False, 0
    for item in d.get("output", []):
        if item.get("type") == "web_search_call":
            searched = True
            n_search += 1
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
            "sources": sources, "searched": searched, "n_search": n_search}


async def _call_anthropic(model: str, prompt: str, search: bool, thinking: str, country: str | None = None) -> dict:
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


async def call_model(model: str, prompt: str, *, search: bool, thinking: str, country: str | None = None) -> dict:
    meta = MODELS[model]
    t0 = time.perf_counter()
    try:
        res = await _CALLERS[meta["vendor"]](model, prompt, search, thinking, country)
        err = None
    except Exception as e:  # noqa: BLE001
        res, err = {"text": "", "in": 0, "out": 0, "think": 0, "sources": [], "searched": False}, f"{type(e).__name__}: {e}"
    pin, pout = meta["price"]
    cost = (res["in"] * pin + (res["out"] + res["think"]) * pout) / 1_000_000
    if search and res["searched"]:
        n = res.get("n_search", 1) or 1
        cost += SEARCH_PRICE_PER_CALL[meta["vendor"]] * n
        if meta["vendor"] == "openai":
            cost += OPENAI_SEARCH_INPUT_TOKENS * n * pin / 1_000_000
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


_NUMS = re.compile(r"\d+")


def parse_keep(text: str, n: int) -> set[int] | None:
    """Номера оставить (1-based). None = ответ не разобран → fail-open, список не трогаем."""
    nums = {int(x) for x in _NUMS.findall(text)}
    nums = {x for x in nums if 1 <= x <= n}
    if not nums:            # пустой или нечисловой ответ = fail-open (срез «всё» кодом не признаём)
        return None
    return nums


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


# ══════════════════════════ фильтры парсера (без LLM) над фразами «сид + слово» ══════════════════════════

def run_parser_filters(words: list[str], seed: str, country: str, language: str, filters: str) -> dict:
    """Синхронно: main.apply_filters_traced над фразами. Возвращает раскладку по словам."""
    import main as _main                                     # lazy: main импортирует этот модуль
    phrases = {f"{seed} {w}": w for w in words}
    result = {"seed": seed, "method": "minus-test", "keywords": list(phrases), "anchors": [],
              "count": len(phrases), "anchors_count": 0}
    l2_config = _main._build_l2_config(None, None, None)
    result = _main.apply_filters_traced(result, seed=seed, country=country, method="minus-test",
                                        language=language, enabled_filters=filters, l2_config=l2_config)

    def _kw(k):  # ключ может быть str или {"query": ...}
        return (k if isinstance(k, str) else k.get("query", "")).lower().strip()

    valid = [phrases[_kw(k)] for k in result.get("keywords", []) if _kw(k) in phrases]
    grey = [phrases[_kw(k)] for k in result.get("keywords_grey", []) if _kw(k) in phrases]
    blocked = (result.get("_trace") or {}).get("blocked_keywords", {}) or {}
    trash: list[dict] = []
    seen = set(valid) | set(grey)
    for ph, info in blocked.items():
        w = phrases.get(ph.lower().strip())
        if w and w not in seen:
            seen.add(w)
            trash.append({"word": w, "by": info.get("blocked_by", "?"), "reason": info.get("reason", "")})
    for a in result.get("anchors", []):                     # то, что не попало в blocked_keywords трейсера
        ph = a if isinstance(a, str) else a.get("query", a.get("keyword", ""))
        w = phrases.get(str(ph).lower().strip())
        if w and w not in seen:
            seen.add(w)
            trash.append({"word": w, "by": (a.get("anchor_reason", "anchor") if isinstance(a, dict) else "anchor"), "reason": ""})
    for w in words:                                         # на всякий случай — ничего не терять
        if w not in seen:
            trash.append({"word": w, "by": "unknown", "reason": "не найдено ни в одном ведре"})
    by_filter: dict[str, int] = {}
    for t in trash:
        by_filter[t["by"]] = by_filter.get(t["by"], 0) + 1
    return {"valid": valid, "grey": grey, "trash": trash, "by_filter": by_filter,
            "timings": result.get("_filter_timings", {})}


# ══════════════════════════ конвейер ══════════════════════════

class MinusReq(BaseModel):
    seed: str
    region: str = "Украина"
    finder: str = DEFAULT_FINDER
    extenders: list[str] = DEFAULT_EXTENDERS
    censor: str = DEFAULT_CENSOR
    thinking: str = "low"          # off | low | medium | high
    filters: str = "pre,geo,bpf,l0,l15v2,l2"   # бесплатные фильтры парсера над фразами «сид + слово»; "" = выкл
    country: str = "ua"
    language: str = "ru"
    run_relate: bool = True        # LLM-1: слово — уточнение/расширение сида? нет → мусор
    relate_model: str = DEFAULT_CENSOR
    run_censor: bool = True        # LLM-2: среди уточнений — что минус (PRUNE)


async def run_minus(req: MinusReq) -> dict:
    t0 = time.perf_counter()
    seed = req.seed.strip()

    # 1. finder — единственный вызов с поиском (принудительный у OpenAI, страна из региона)
    finder = await call_model(req.finder, FINDER_PROMPT.format(seed=seed, region=req.region),
                              search=True, thinking=req.thinking, country=region_code(req.region))
    finder["role"] = "finder"
    current = parse_list(finder["text"])
    stages: list[tuple[str, list[str]]] = [(finder["model"], list(current))]
    calls = [finder]

    # 2-4. цепочка «дополни»: каждый получает список, дополненный предыдущим
    for m in req.extenders:
        prompt = EXTENDER_PROMPT.format(found="\n".join(current) or "(пусто)", seed=seed, region=req.region)
        r = await call_model(m, prompt, search=False, thinking=req.thinking)
        r["role"] = "extend"
        calls.append(r)
        added = [w for w in parse_list(r["text"]) if w not in set(current)]
        stages.append((m, added))
        current = current + added
    before_filters = list(current)

    # 5. бесплатные фильтры парсера над фразами «сид + слово»
    filt: dict = {"valid": current, "grey": [], "trash": [], "by_filter": {}, "timings": {}, "error": None}
    if req.filters.strip():
        t1 = time.perf_counter()
        try:
            filt = await asyncio.to_thread(run_parser_filters, current, seed, req.country, req.language, req.filters)
            filt["error"] = None
        except Exception as e:  # noqa: BLE001
            filt["error"] = f"{type(e).__name__}: {e}"
        filt["wall"] = round(time.perf_counter() - t1, 2)
        current = filt["valid"] + filt["grey"]           # в цензор/итог идёт всё, что не TRASH

    # 6. LLM-1 — «уточнение/расширение сида?»: нет → мусор
    unrelated: list[dict] = []
    if req.run_relate and current:
        numbered = "\n".join(f"{i+1}. {w}" for i, w in enumerate(current))
        rl = await call_model(req.relate_model, RELATE_PROMPT.format(seed=seed, region=req.region, numbered=numbered),
                              search=False, thinking=req.thinking)
        rl["role"] = "relate"
        calls.append(rl)
        keep = parse_keep(rl["text"], len(current))
        if keep is None:
            rl["error"] = (rl["error"] or "") + " | parse fail → list untouched"
        else:
            unrelated = [{"word": w, "by": rl["model"]} for i, w in enumerate(current) if (i + 1) not in keep]
            current = [w for i, w in enumerate(current) if (i + 1) in keep]
    after_relate = list(current)

    # 7. LLM-2 — цензор среди уточнений: что минус (только удаление)
    removed: list[dict] = []
    if req.run_censor and current:
        numbered = "\n".join(f"{i+1}. {w}" for i, w in enumerate(current))
        cz = await call_model(req.censor, PRUNE_PROMPT.format(seed=seed, region=req.region, numbered=numbered),
                              search=False, thinking=req.thinking)
        cz["role"] = "censor"
        calls.append(cz)
        keep = parse_keep(cz["text"], len(current))
        if keep is None:                          # fail-open
            cz["error"] = (cz["error"] or "") + " | parse fail → list untouched"
        else:
            removed = [{"word": w, "by": cz["model"]} for i, w in enumerate(current) if (i + 1) not in keep]
            current = [w for i, w in enumerate(current) if (i + 1) in keep]

    origin = {w: (m, i + 1) for i, (m, ws) in enumerate(stages) for w in ws}
    rows = [{"word": w, "stage": origin[w][1], "by": origin[w][0]} for w in current]
    stats = {
        "build": BUILD,
        "seed": seed, "region": req.region, "thinking": req.thinking,
        "total_cost": round(sum(c["cost"] for c in calls), 5),
        "total_wall": round(time.perf_counter() - t0, 2),
        "finder_count": len(stages[0][1]),
        "added_by_stage": {m: len(ws) for m, ws in stages[1:]},
        "before_filters": len(before_filters),
        "filters": {"valid": len(filt["valid"]), "grey": len(filt["grey"]), "trash": len(filt["trash"]),
                    "by_filter": filt["by_filter"], "wall": filt.get("wall"), "error": filt.get("error"),
                    "timings": filt.get("timings", {})},
        "unrelated_by_llm1": len(unrelated),
        "after_relate": len(after_relate),
        "removed_by_censor": len(removed),
        "final_count": len(current),
        "calls": [{k: v for k, v in c.items() if k != "text"} for c in calls],
    }
    grey_set = set(filt["grey"])
    for r in rows:
        r["bucket"] = "grey" if r["word"] in grey_set else "valid"
    return {"rows": rows, "list": current, "removed": removed, "unrelated": unrelated, "trash": filt["trash"],
            "stats": stats,
            "raw": {f"{i+1}. {c.get('role', 'gen')} {c['model']}": c["text"] for i, c in enumerate(calls)}}


# ══════════════════════════ регистрация ══════════════════════════

def register_minus_words_test(app: FastAPI) -> None:
    html_path = Path(__file__).with_name("minus_test.html")

    @app.get("/minus-test", response_class=HTMLResponse)
    async def minus_page():
        return html_path.read_text(encoding="utf-8")

    @app.post("/api/minus-test")
    async def minus_api(req: MinusReq):
        unknown = [m for m in [req.finder, *req.extenders, req.censor] if m not in MODELS]
        if unknown:
            return JSONResponse({"error": f"unknown models: {unknown}"}, status_code=400)
        return await run_minus(req)

    @app.get("/api/minus-test/models")
    async def minus_models():
        return {"models": list(MODELS), "finder": DEFAULT_FINDER, "extenders": DEFAULT_EXTENDERS,
                "censor": DEFAULT_CENSOR, "build": BUILD}
