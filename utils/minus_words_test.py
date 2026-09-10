"""
minus_words_test.py — стенд минус-слов. build: ms_0.1 (минуса из своей семантики)

ms_0.1 — вход: готовый autopilot JSON + ключи, ВЫБРАННЫЕ человеком для рекламы; из остатка кодом три потока
  (geo — выброс, info_intent — фразовые минуса, остальное — пословно → шит словами выбранных → цензор PRUNE_SEM).
  Полный пайплайн не гоняется. Старая цепочка на памяти моделей (mw_1.3) закомментирована с меткой [MEM-MODE mw_1.3].
  Эндпоинты: GET /minus-semantics (minus_semantics.html рядом), POST /api/minus-semantics, GET /api/minus-semantics/models

[MEM-MODE mw_1.3] ниже — описание старой цепочки:

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
    [MEM-MODE mw_1.3] GET /minus-test, POST /api/minus-test — выключены в ms_0.1

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

BUILD = "ms_0.2"   # [MEM-MODE mw_1.3] было: BUILD = "mw_1.3"

# ─── реестр моделей: цена $ за 1M токенов (in, out); поправь под актуальный прайс ───
MODELS: dict[str, dict] = {
    "gemini-3.8-flash":      {"vendor": "gemini",    "price": (0.75, 3.75), "search": True},   # ms_0.1: вводная цена до 31.12.2026, потом 1.5/7.5
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

# ┌── [MEM-MODE mw_1.3] дефолты finder/extenders/censor — закомментировано в ms_0.1 (готовая цепочка на памяти моделей, совместить позже) ──
# DEFAULT_FINDER = "gemini-3.7-flash"  # Luna «искал» без источников (0.7); Gemini 3.7 реально открывал страницы (0.4)
# DEFAULT_EXTENDERS = ["gemini-3.7-flash", "claude-sonnet-4-6", "gpt-5.6-luna"]  # порядок = порядок цепочки
# DEFAULT_CENSOR = "gemini-3.7-flash"
# └── [MEM-MODE mw_1.3] конец: дефолты finder/extenders/censor ──
DEFAULT_CENSOR = "gemini-3.8-flash"   # ms_0.1: было gemini-3.7-flash

# ┌── [MEM-MODE mw_1.3] промпты FINDER/EXTENDER/RELATE/PRUNE — закомментировано в ms_0.1 (готовая цепочка на памяти моделей, совместить позже) ──
# # ─── промпты mw_0.7: широкая генерация (как в 0.5) + цензор (PRUNE из 0.6) ───
# FINDER_PROMPT = (
#     "Найди пожалуйста самый полный список минус слов для рекламы Google Ads для этого сида: «{seed}». "
#     "Регион: {region}.\n"
#     "Сделай один поиск в интернете и собери слова из найденных опубликованных списков. "
#     "Ничего не придумывай сам: если слова нет в найденных источниках, не пиши его.\n"
#     "Ответ: одно минус-слово на строку, без нумерации и пояснений."
# )
# EXTENDER_PROMPT = (
#     "Вот список минус слов:\n{found}\n\n"
#     "Вот сид: «{seed}»\nВот регион поиска: {region}\n"
#     "Дополни этот список недостающими минус словами.\n"
#     "Ответ: только новые слова, одно на строку, без нумерации и пояснений."
# )
# # mw_1.3: вопрос LLM-1 сменён с «уточнение/расширение» на реальную встречаемость запроса —
# #         старый вопрос терял реальные минуса (симптомы/причины/фото/бесплатно), они не «уточняют» сид
# RELATE_PROMPT = (
#     "Сид: «{seed}». Регион: {region}.\n"
#     "Ниже пронумерованный список слов. Для каждого слова ответь на вопрос: "
#     "люди реально набирают в поиске запрос, в котором есть этот сид (или его часть) и это слово?\n"
#     "Ответ: номера слов, для которых ДА, через запятую. Ничего кроме номеров.\n\n{numbered}"
# )
# # mw_1.1: определение клиента (разбор 28 ложных срезов Andrew → 4 типа, корень один — «клиент» читался как «в теме»).
# #         Формулировка коммерческая; для инфо-сидов Andrew ожидает другую историю — пока так.
# PRUNE_PROMPT = (
#     "Регион: {region}. Фраза: «{seed}».\n"
#     "Ниже пронумерованный список слов-кандидатов в минус-слова для рекламы по этой фразе.\n"
#     "Клиент — человек, который прямо сейчас покупает новый товар или заказывает услугу по сиду у этого "
#     "рекламодателя. Не клиент: изучает тему или выбирает, ищет другое состояние товара, другой канал покупки, "
#     "смежный товар или услугу.\n"
#     "Проверь каждое слово: запрос «{seed} + слово» реально набирают, и человек в нём — не клиент. "
#     "Слово, уточняющее или выбирающее тот же новый товар или услугу у этого рекламодателя, не подходит.\n"
#     "Ничего не добавляй. Ответ: номера слов, которые ОСТАВИТЬ, через запятую. Ничего кроме номеров.\n\n{numbered}"
# )
# # GEN_PROMPT из 0.6 (узкий генератор) снят — точность без широты; см. git-историю для отката
# └── [MEM-MODE mw_1.3] конец: промпты FINDER/EXTENDER/RELATE/PRUNE ──


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

# ┌── [MEM-MODE mw_1.3] parse_list (разбор списков генератора) — закомментировано в ms_0.1 (готовая цепочка на памяти моделей, совместить позже) ──
# _STRIP = re.compile(r"^[\s\-\*\•\d\.\)\]]+|[\s\-\*\•]+$")
#
#
# def parse_list(text: str) -> list[str]:
#     out, seen = [], set()
#     for line in text.splitlines():
#         w = _STRIP.sub("", line).strip().strip('"«»').lower()
#         if not w or len(w) > 60 or ":" in w and len(w.split()) > 4:
#             continue
#         w = re.sub(r"\s+", " ", w)
#         if w not in seen:
#             seen.add(w)
#             out.append(w)
#     return out
# └── [MEM-MODE mw_1.3] конец: parse_list (разбор списков генератора) ──


_NUMS = re.compile(r"\d+")


def parse_keep(text: str, n: int) -> set[int] | None:
    """Номера оставить (1-based). None = ответ не разобран → fail-open, список не трогаем."""
    nums = {int(x) for x in _NUMS.findall(text)}
    nums = {x for x in nums if 1 <= x <= n}
    if not nums:            # пустой или нечисловой ответ = fail-open (срез «всё» кодом не признаём)
        return None
    return nums


# ┌── [MEM-MODE mw_1.3] merge (слияние стадий генератора) — закомментировано в ms_0.1 (готовая цепочка на памяти моделей, совместить позже) ──
# def merge(stages: list[tuple[str, list[str]]]) -> list[dict]:
#     """stages: [(model, слова, которые эта модель ДОБАВИЛА)] в порядке цепочки"""
#     rows, seen = [], set()
#     for i, (model, ws) in enumerate(stages):
#         for w in ws:
#             if w in seen:
#                 continue
#             seen.add(w)
#             rows.append({"word": w, "stage": i + 1, "by": model})
#     return rows
# └── [MEM-MODE mw_1.3] конец: merge (слияние стадий генератора) ──


# ┌── [MEM-MODE mw_1.3] run_parser_filters (бесплатные фильтры над «сид + слово») — закомментировано в ms_0.1 (готовая цепочка на памяти моделей, совместить позже) ──
# # ══════════════════════════ фильтры парсера (без LLM) над фразами «сид + слово» ══════════════════════════
#
# def run_parser_filters(words: list[str], seed: str, country: str, language: str, filters: str) -> dict:
#     """Синхронно: main.apply_filters_traced над фразами. Возвращает раскладку по словам."""
#     import main as _main                                     # lazy: main импортирует этот модуль
#
#     def norm(t: str) -> str:
#         return re.sub(r"\s+", " ", str(t).lower()).strip()
#
#     phrases = {norm(f"{seed} {w}"): w for w in words}
#     result = {"seed": seed, "method": "minus-test", "keywords": list(phrases), "anchors": [],
#               "count": len(phrases), "anchors_count": 0}
#     l2_config = _main._build_l2_config(None, None, None)
#     result = _main.apply_filters_traced(result, seed=seed, country=country, method="minus-test",
#                                         language=language, enabled_filters=filters, l2_config=l2_config)
#
#     def _kw(k):  # ключ может быть str или {"query": ...}
#         return norm(k if isinstance(k, str) else k.get("query", ""))
#
#     valid = [phrases[_kw(k)] for k in result.get("keywords", []) if _kw(k) in phrases]
#     grey = [phrases[_kw(k)] for k in result.get("keywords_grey", []) if _kw(k) in phrases]
#     blocked = (result.get("_trace") or {}).get("blocked_keywords", {}) or {}
#     trash: list[dict] = []
#     seen = set(valid) | set(grey)
#     for ph, info in blocked.items():
#         w = phrases.get(norm(ph))
#         if w and w not in seen:
#             seen.add(w)
#             trash.append({"word": w, "by": info.get("blocked_by", "?"), "reason": info.get("reason", "")})
#     for a in result.get("anchors", []):                     # то, что не попало в blocked_keywords трейсера
#         ph = a if isinstance(a, str) else a.get("query", a.get("keyword", ""))
#         w = phrases.get(norm(ph))
#         if w and w not in seen:
#             seen.add(w)
#             trash.append({"word": w, "by": (a.get("anchor_reason", "anchor") if isinstance(a, dict) else "anchor"), "reason": ""})
#     for w in words:                                         # на всякий случай — ничего не терять
#         if w not in seen:
#             trash.append({"word": w, "by": "unknown", "reason": "не найдено ни в одном ведре"})
#     by_filter: dict[str, int] = {}
#     for t in trash:
#         by_filter[t["by"]] = by_filter.get(t["by"], 0) + 1
#     return {"valid": valid, "grey": grey, "trash": trash, "by_filter": by_filter,
#             "timings": result.get("_filter_timings", {})}
# └── [MEM-MODE mw_1.3] конец: run_parser_filters (бесплатные фильтры над «сид + слово») ──


# ══════════════════════════ конвейер ══════════════════════════

# ┌── [MEM-MODE mw_1.3] MinusReq (запрос старого стенда) — закомментировано в ms_0.1 (готовая цепочка на памяти моделей, совместить позже) ──
# class MinusReq(BaseModel):
#     seed: str
#     region: str = "Украина"
#     finder: str = DEFAULT_FINDER
#     extenders: list[str] = DEFAULT_EXTENDERS
#     censor: str = DEFAULT_CENSOR
#     thinking: str = "low"          # off | low | medium | high
#     filters: str = "pre,geo,bpf,l0,l15v2,l2"   # бесплатные фильтры парсера над фразами «сид + слово»; "" = выкл
#     country: str = "ua"
#     language: str = "ru"
#     run_relate: bool = True        # LLM-1: слово — уточнение/расширение сида? нет → мусор
#     relate_model: str = DEFAULT_CENSOR
#     run_censor: bool = True        # LLM-2: среди уточнений — что минус (PRUNE)
# └── [MEM-MODE mw_1.3] конец: MinusReq (запрос старого стенда) ──


_SEED_JUNK = re.compile(r"^[\s\d\.\)\-•*]+")


def clean_seed(seed: str) -> str:
    """Срезает нумерацию/маркеры из вставленного списка и схлопывает пробелы/табы."""
    return re.sub(r"\s+", " ", _SEED_JUNK.sub("", seed)).strip()


# ┌── [MEM-MODE mw_1.3] run_minus (конвейер finder → дополнители → фильтры → LLM-1 → цензор) — закомментировано в ms_0.1 (готовая цепочка на памяти моделей, совместить позже) ──
# async def run_minus(req: MinusReq) -> dict:
#     t0 = time.perf_counter()
#     seed = clean_seed(req.seed)
#
#     # 1. finder — единственный вызов с поиском (принудительный у OpenAI, страна из региона)
#     finder = await call_model(req.finder, FINDER_PROMPT.format(seed=seed, region=req.region),
#                               search=True, thinking=req.thinking, country=region_code(req.region))
#     finder["role"] = "finder"
#     current = parse_list(finder["text"])
#     stages: list[tuple[str, list[str]]] = [(finder["model"], list(current))]
#     calls = [finder]
#
#     # 2-4. цепочка «дополни»: каждый получает список, дополненный предыдущим
#     for m in req.extenders:
#         prompt = EXTENDER_PROMPT.format(found="\n".join(current) or "(пусто)", seed=seed, region=req.region)
#         r = await call_model(m, prompt, search=False, thinking=req.thinking)
#         r["role"] = "extend"
#         calls.append(r)
#         added = [w for w in parse_list(r["text"]) if w not in set(current)]
#         stages.append((m, added))
#         current = current + added
#     before_filters = list(current)
#
#     # 5. бесплатные фильтры парсера над фразами «сид + слово»
#     filt: dict = {"valid": current, "grey": [], "trash": [], "by_filter": {}, "timings": {}, "error": None}
#     if req.filters.strip():
#         t1 = time.perf_counter()
#         try:
#             filt = await asyncio.to_thread(run_parser_filters, current, seed, req.country, req.language, req.filters)
#             filt["error"] = None
#         except Exception as e:  # noqa: BLE001
#             filt["error"] = f"{type(e).__name__}: {e}"
#         filt["wall"] = round(time.perf_counter() - t1, 2)
#         mapped = filt["valid"] + filt["grey"]
#         unknown_all = filt["trash"] and all(t["by"] == "unknown" for t in filt["trash"]) and not mapped
#         if unknown_all:                                   # сопоставление фраза→слово не сработало → fail-open
#             filt["error"] = (filt.get("error") or "") + " | mapping fail → list untouched"
#             filt["trash"], filt["by_filter"] = [], {}
#             filt["valid"] = list(current)
#         else:
#             current = mapped                              # в цензор/итог идёт всё, что не TRASH
#
#     # 6. LLM-1 — «уточнение/расширение сида?»: нет → мусор
#     unrelated: list[dict] = []
#     if req.run_relate and current:
#         numbered = "\n".join(f"{i+1}. {w}" for i, w in enumerate(current))
#         rl = await call_model(req.relate_model, RELATE_PROMPT.format(seed=seed, region=req.region, numbered=numbered),
#                               search=False, thinking=req.thinking)
#         rl["role"] = "relate"
#         calls.append(rl)
#         keep = parse_keep(rl["text"], len(current))
#         if keep is None:
#             rl["error"] = (rl["error"] or "") + " | parse fail → list untouched"
#         else:
#             unrelated = [{"word": w, "by": rl["model"]} for i, w in enumerate(current) if (i + 1) not in keep]
#             current = [w for i, w in enumerate(current) if (i + 1) in keep]
#     after_relate = list(current)
#
#     # 7. LLM-2 — цензор среди уточнений: что минус (только удаление)
#     removed: list[dict] = []
#     if req.run_censor and current:
#         numbered = "\n".join(f"{i+1}. {w}" for i, w in enumerate(current))
#         cz = await call_model(req.censor, PRUNE_PROMPT.format(seed=seed, region=req.region, numbered=numbered),
#                               search=False, thinking=req.thinking)
#         cz["role"] = "censor"
#         calls.append(cz)
#         keep = parse_keep(cz["text"], len(current))
#         if keep is None:                          # fail-open
#             cz["error"] = (cz["error"] or "") + " | parse fail → list untouched"
#         else:
#             removed = [{"word": w, "by": cz["model"]} for i, w in enumerate(current) if (i + 1) not in keep]
#             current = [w for i, w in enumerate(current) if (i + 1) in keep]
#
#     origin = {w: (m, i + 1) for i, (m, ws) in enumerate(stages) for w in ws}
#     rows = [{"word": w, "stage": origin[w][1], "by": origin[w][0]} for w in current]
#     stats = {
#         "build": BUILD,
#         "seed": seed, "region": req.region, "thinking": req.thinking,
#         "total_cost": round(sum(c["cost"] for c in calls), 5),
#         "total_wall": round(time.perf_counter() - t0, 2),
#         "finder_count": len(stages[0][1]),
#         "added_by_stage": {m: len(ws) for m, ws in stages[1:]},
#         "before_filters": len(before_filters),
#         "filters": {"valid": len(filt["valid"]), "grey": len(filt["grey"]), "trash": len(filt["trash"]),
#                     "by_filter": filt["by_filter"], "wall": filt.get("wall"), "error": filt.get("error"),
#                     "timings": filt.get("timings", {})},
#         "unrelated_by_llm1": len(unrelated),
#         "after_relate": len(after_relate),
#         "removed_by_censor": len(removed),
#         "final_count": len(current),
#         "calls": [{k: v for k, v in c.items() if k != "text"} for c in calls],
#     }
#     grey_set = set(filt["grey"])
#     for r in rows:
#         r["bucket"] = "grey" if r["word"] in grey_set else "valid"
#     return {"rows": rows, "list": current, "removed": removed, "unrelated": unrelated, "trash": filt["trash"],
#             "stats": stats,
#             "raw": {f"{i+1}. {c.get('role', 'gen')} {c['model']}": c["text"] for i, c in enumerate(calls)}}
# └── [MEM-MODE mw_1.3] конец: run_minus (конвейер finder → дополнители → фильтры → LLM-1 → цензор) ──


# ══════════════════════════ ms_0.1: минуса из своей семантики ══════════════════════════
#
# Вход — готовый autopilot JSON (полный пайплайн не гоняется) + ключи, которые человек ВЫБРАЛ для рекламы
# (чекбоксы по каждому ключу, из любых кластеров). Остаток = невыбранные VALID, кодом делится на три потока
# по groups.by_group из JSON (разметка L0-группировки, у каждого ключа ровно одна группа):
#   geo         → выбрасывается: своё гео = валид, чужое режут фильтры парсера; минусом гео не бывает
#   info_intent → ФРАЗОВЫЕ минуса кодом: хвост ключа без сида. Пословно не режем — многозначность
#                 («где купить» / «где стоит», «сколько стоит» / «где стоит») код не разрешит
#   остальное   → хвосты пословно → шит (слово из выбранного ключа минусом быть не может — заблокирует
#                 свой же показ) → цензор PRUNE_SEM одним вызовом, в промпте ВЫБРАННЫЕ ключи как рамка
# Рамка от выбранных ключей вместо сида: для сида «аккумулятор на скутер» чужие бренды/модели скутеров и
# 48/60/72В/электро — валид, для рекламодателя «Yamaha 12В гель» — минус (моделирование на JSON Andrew:
# рамка от сида пропускала ~40 из ~75 минусов).

BUILD_SEM = "ms_0.2"
DEFAULT_CENSOR_SEM = "gemini-3.8-flash"
# ms_0.2: contacts («где находится») и action («своими руками») тоже фразами — пословно «где» блокировал «где купить»
INFO_GROUPS = ("info_intent", "contacts", "action")   # группы L0 → фразовый поток
# INFO_GROUPS = ("info_intent",)   # ms_0.1
GEO_GROUPS = ("geo",)

# ms_0.1 формулировка (откат): «…другой бренд, модель, характеристику, смежный товар или услугу, которых в запросах рекламодателя нет.»
PRUNE_SEM_PROMPT = (
    "Регион: {region}. Тема: «{seed}».\n"
    "Рекламодатель показывает объявления только по этим запросам:\n{selected}\n\n"
    "Ниже пронумерованный список слов из других запросов по той же теме — кандидаты в минус-слова.\n"
    "Клиент — человек, который прямо сейчас покупает новый товар или заказывает услугу у этого рекламодателя, "
    "то есть ищет то, что описано в его запросах выше. Не клиент: изучает тему или выбирает, ищет другое "
    "состояние товара, другой канал покупки, чужой бренд, смежный товар или услугу.\n"
    "Характеристика или модель — минус только если она несовместима с товаром из запросов рекламодателя "
    "(другое напряжение, другой тип), а не просто в них не названа.\n"
    "Проверь каждое слово: запрос «{seed} + слово» реально набирают, и человек в нём — не клиент этого рекламодателя. "
    "Слово, уточняющее тот же товар или услугу из запросов рекламодателя, не подходит.\n"
    "Ничего не добавляй. Ответ: номера слов, которые ОСТАВИТЬ, через запятую. Ничего кроме номеров.\n\n{numbered}"
)

_TOKEN = re.compile(r"[^\W_]+", re.UNICODE)


def tokens(s: str) -> list[str]:
    return [t.lower() for t in _TOKEN.findall(s or "")]


def same_stem(a: str, b: str) -> bool:
    """Одна основа без словарей: более короткое слово ≥ 4 символов, общий префикс ≥ 3 и покрывает короткое
    без последних двух (окончания прилагательных/падежей). Числовые токены — только точное совпадение.
    скутер/скутера/скутере, гелевый/гелевого, аккумулятор/аккумуляторная, купить/купити, цена/цены — да;
    12/125, 150/1500, скутер/электроскутер, на/над, как/какой, акб/аккумулятор — нет."""
    if a == b:
        return True
    if a[0].isdigit() or b[0].isdigit():
        return False
    n = 0
    for x, y in zip(a, b):
        if x != y:
            break
        n += 1
    short = min(len(a), len(b))
    return short >= 4 and n >= 3 and n >= short - 2


def _stem_hit(t: str, pool: list[str]) -> str | None:
    for p in pool:
        if same_stem(t, p):
            return p
    return None


def tail_tokens(keyword: str, seed_toks: list[str]) -> list[str]:
    return [t for t in tokens(keyword) if _stem_hit(t, seed_toks) is None]


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower()).strip()


class SemReq(BaseModel):
    autopilot: dict                 # весь autopilot JSON (нужны seed, keywords, groups.by_group, clusters.region/language)
    selected: list[str]             # ключи, по которым рекламодатель показывается
    censor: str = DEFAULT_CENSOR_SEM
    thinking: str = "low"           # off | low | medium | high
    run_censor: bool = True         # выкл → неявный остаток отдаётся сырым списком без LLM
    region: str = ""                # пусто → из JSON (clusters.region → _trace.country)


def split_streams(ap: dict, selected: list[str]) -> dict:
    """Детерминированная часть: потоки, фразы, кандидаты, шит. Без LLM."""
    seed = clean_seed(ap.get("seed", ""))
    seed_toks = tokens(seed)
    keywords = [k if isinstance(k, str) else k.get("query", "") for k in ap.get("keywords", [])]
    by_group = ((ap.get("groups") or {}).get("by_group") or {})
    group_of: dict[str, str] = {}
    for g, kws in by_group.items():
        for k in kws:
            group_of[_norm(k)] = g

    sel_norm = {_norm(s) for s in selected}
    sel_keys = [k for k in keywords if _norm(k) in sel_norm]
    sel_toks: list[str] = []
    for k in sel_keys:
        for t in tokens(k):
            if t not in sel_toks:
                sel_toks.append(t)

    rest = [k for k in keywords if _norm(k) not in sel_norm]
    geo, info, other = [], [], []
    for k in rest:
        g = group_of.get(_norm(k), "?")
        (geo if g in GEO_GROUPS else info if g in INFO_GROUPS else other).append(k)

    # info → фразы (хвост без сида); фраза, целиком сидящая в выбранном ключе, — в шит
    phrases: dict[str, list[str]] = {}
    shielded: list[dict] = []
    no_minus: list[str] = []
    for k in info:
        ph = " ".join(tail_tokens(k, seed_toks))
        if not ph:
            no_minus.append(k)
            continue
        hit = next((s for s in sel_keys if ph in _norm(s)), None)
        if hit:
            shielded.append({"word": ph, "kind": "phrase", "by": hit, "keys": [k]})
            continue
        phrases.setdefault(ph, []).append(k)

    # остальное → слова; шит по основе со словами выбранных ключей
    cand: dict[str, list[str]] = {}
    sh_words: dict[str, dict] = {}
    for k in other:
        tt = tail_tokens(k, seed_toks)
        got = False
        for t in tt:
            hit = _stem_hit(t, sel_toks)
            if hit is not None:
                e = sh_words.setdefault(t, {"word": t, "kind": "word", "by": hit, "keys": []})
                if k not in e["keys"]:
                    e["keys"].append(k)
                continue
            lst = cand.setdefault(t, [])
            if k not in lst:
                lst.append(k)
            got = True
        if not got:
            no_minus.append(k)
    shielded.extend(sh_words.values())

    return {
        "seed": seed, "keywords": keywords, "group_of": group_of,
        "selected": sel_keys, "rest": rest, "geo": geo, "info": info, "other": other,
        "phrases": phrases, "candidates": cand, "shielded": shielded, "no_minus": no_minus,
    }


async def run_semantics(req: SemReq) -> dict:
    t0 = time.perf_counter()
    ap = req.autopilot
    cl = ap.get("clusters") or {}
    region = req.region.strip() or cl.get("region") or (ap.get("_trace") or {}).get("country") or ""
    s = split_streams(ap, req.selected)
    seed = s["seed"]
    cand = s["candidates"]
    words = list(cand)

    removed: list[dict] = []
    calls: list[dict] = []
    prompt = ""
    if req.run_censor and words:
        numbered = "\n".join(f"{i+1}. {w}" for i, w in enumerate(words))
        prompt = PRUNE_SEM_PROMPT.format(region=region or "не указан", seed=seed,
                                         selected="\n".join(s["selected"]) or "(пусто)", numbered=numbered)
        cz = await call_model(req.censor, prompt, search=False, thinking=req.thinking)
        cz["role"] = "censor"
        calls.append(cz)
        keep = parse_keep(cz["text"], len(words))
        if keep is None:                                  # fail-open: список не трогаем, ошибка в отчёте
            cz["error"] = (cz["error"] or "") + " | parse fail → list untouched"
        else:
            removed = [{"word": w, "keys": cand[w]} for i, w in enumerate(words) if (i + 1) not in keep]
            words = [w for i, w in enumerate(words) if (i + 1) in keep]

    minus_words = [{"word": w, "keys": cand[w]} for w in words]
    info_phrases = [{"phrase": p, "keys": ks} for p, ks in s["phrases"].items()]
    stats = {
        "build": BUILD_SEM, "seed": seed, "region": region,
        "censor": req.censor if req.run_censor else None, "thinking": req.thinking,
        "valid_total": len(s["keywords"]), "selected": len(s["selected"]), "rest": len(s["rest"]),
        "geo_dropped": len(s["geo"]), "info_keys": len(s["info"]), "info_phrases": len(info_phrases),
        "other_keys": len(s["other"]), "candidates": len(cand),
        "shielded": len(s["shielded"]), "no_minus": len(s["no_minus"]),
        "minus_words": len(minus_words), "removed_by_censor": len(removed),
        "total_cost": round(sum(c["cost"] for c in calls), 5),
        "total_wall": round(time.perf_counter() - t0, 2),
        "calls": [{k: v for k, v in c.items() if k != "text"} for c in calls],
    }
    return {"minus_words": minus_words, "info_phrases": info_phrases, "removed": removed,
            "shielded": s["shielded"], "geo": s["geo"], "no_minus": s["no_minus"],
            "stats": stats, "prompt": prompt,
            "raw": {f"{i+1}. {c.get('role', '?')} {c['model']}": c["text"] for i, c in enumerate(calls)}}


# ══════════════════════════ регистрация ══════════════════════════

def register_minus_words_test(app: FastAPI) -> None:
    html_sem = Path(__file__).with_name("minus_semantics.html")

    @app.get("/minus-semantics", response_class=HTMLResponse)
    async def minus_semantics_page():
        return html_sem.read_text(encoding="utf-8")

    @app.post("/api/minus-semantics")
    async def minus_semantics_api(req: SemReq):
        if req.censor not in MODELS:
            return JSONResponse({"error": f"unknown model: {req.censor}"}, status_code=400)
        if not req.autopilot.get("keywords"):
            return JSONResponse({"error": "autopilot JSON без keywords"}, status_code=400)
        return await run_semantics(req)

    @app.get("/api/minus-semantics/models")
    async def minus_semantics_models():
        return {"models": list(MODELS), "censor": DEFAULT_CENSOR_SEM, "build": BUILD_SEM}


# ┌── [MEM-MODE mw_1.3] register_minus_words_test (старые роуты /minus-test, /api/minus-test) — закомментировано в ms_0.1 (готовая цепочка на памяти моделей, совместить позже) ──
# def register_minus_words_test(app: FastAPI) -> None:
#     html_path = Path(__file__).with_name("minus_test.html")
#
#     @app.get("/minus-test", response_class=HTMLResponse)
#     async def minus_page():
#         return html_path.read_text(encoding="utf-8")
#
#     @app.post("/api/minus-test")
#     async def minus_api(req: MinusReq):
#         unknown = [m for m in [req.finder, *req.extenders, req.censor] if m not in MODELS]
#         if unknown:
#             return JSONResponse({"error": f"unknown models: {unknown}"}, status_code=400)
#         return await run_minus(req)
#
#     @app.get("/api/minus-test/models")
#     async def minus_models():
#         return {"models": list(MODELS), "finder": DEFAULT_FINDER, "extenders": DEFAULT_EXTENDERS,
#                 "censor": DEFAULT_CENSOR, "build": BUILD}
# └── [MEM-MODE mw_1.3] конец: register_minus_words_test (старые роуты /minus-test, /api/minus-test) ──
