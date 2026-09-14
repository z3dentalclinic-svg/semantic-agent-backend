"""
intent_map.py — карта интентов по VALID-ключам: конвейер трёх моделей, строго друг за другом.

Регистрация в main.py (две строки, ничего больше):
    from intent_map import router as intent_map_router
    app.include_router(intent_map_router)

POST /api/intent-map
  {"seed": str, "keywords": [str|{"query"|"keyword": str}], "country": str, "language": str, "city": str?}
  → {"intents": [...], "stages": [...], "stats": {...}, "build": ...}

Схема (Andrew, 2026-09-14):
  проход 1 — Gemini: полный список интентов из ключей + из базы знаний модели;
  проход 2 — Claude: получает ключи + список прохода 1, отдаёт ТОЛЬКО добавленные интенты;
  проход 3 — GPT:    получает ключи + объединённый список, отдаёт ТОЛЬКО добавленные.
  Параллели нет: каждая следующая модель видит всё, что нашли до неё. Код сливает, дедуп по имени интента.
  Формат строки: «интент | пример 1; пример 2» (минимум 2 примера ключей на интент).
  Ошибка прохода не роняет цепочку: список остаётся как был, ошибка фиксируется в stages.

Модуль самодостаточен: свой реестр моделей и свои вызовы вендоров (НЕ импортирует minus_words_test —
правится отдельно, не ломая другие модули). Ключи из окружения: GEMINI_API_KEY, OPENAI_API_KEY, ANTHROPIC_API_KEY.
"""
from __future__ import annotations

import os
import re
import time

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

BUILD = "im_0.1"

# ─── реестр моделей: цена $ за 1M токенов (in, out). Правка цен — только здесь. ───
MODELS: dict[str, dict] = {
    "gemini-3.8-flash": {"vendor": "gemini",    "price": (0.75, 3.75)},   # вводная цена до 31.12.2026, потом 1.5/7.5
    "claude-sonnet-5":  {"vendor": "anthropic", "price": (2.0, 10.0)},
    "gpt-5.6-sol":      {"vendor": "openai",    "price": (4.0, 20.0)},
}

# Конвейер: порядок = порядок проходов. thinking: off | low | medium | high
# Andrew 2026-09-14: старт medium на всех трёх; после живого прогона — A/B low vs medium на одном VALID.
CHAIN: list[tuple[str, str]] = [
    ("gemini-3.8-flash", "medium"),
    ("claude-sonnet-5",  "medium"),
    ("gpt-5.6-sol",      "medium"),
]

HTTP_TIMEOUT = 240
ANTHROPIC_MAX_TOKENS = 16000   # общий лимит thinking + ответ (адаптивный режим 5-й серии)

# ─── промпты (формулировка Andrew) ───
FIRST_PROMPT = (
    "Вот список ключевых слов, собранных из подсказок Google по запросу «{seed}».\n"
    "Регион: {region}. Язык: {language}.\n"
    "Составь по ним список поисковых интентов на основе этих ключей и дополни интентами, "
    "которые есть в твоей базе знаний по этой теме, но в списке ключей не встретились.\n"
    "Формат ответа: одна строка на интент — «интент | пример 1; пример 2» "
    "(минимум 2 примера ключевых слов на интент). Без нумерации и пояснений.\n\n"
    "Ключевые слова:\n{keys}"
)
EXTEND_PROMPT = (
    "Вот список ключевых слов, собранных из подсказок Google по запросу «{seed}».\n"
    "Регион: {region}. Язык: {language}.\n\n"
    "Ключевые слова:\n{keys}\n\n"
    "Вот уже составленный по этим ключам список интентов:\n{intents}\n\n"
    "Расширь его: добавь интенты, которые пропущены в этом списке — из ключей и из твоей базы знаний по этой теме.\n"
    "Формат ответа: только НОВЫЕ интенты, одна строка на интент — «интент | пример 1; пример 2» "
    "(минимум 2 примера ключевых слов на интент). Без нумерации и пояснений. "
    "Если добавить нечего — ответь одним словом: нет."
)


# ══════════════════════════ вызовы вендоров (без поиска) ══════════════════════════
# Возвращают {"text", "in", "out", "think"}; "out" — ВСЕ оплачиваемые выходные токены (включая thinking),
# "think" — справочно. У Gemini candidatesTokenCount thinking не содержит → складываем сами.

async def _call_gemini(model: str, prompt: str, thinking: str) -> dict:
    key = os.environ["GEMINI_API_KEY"]
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    body: dict = {"contents": [{"role": "user", "parts": [{"text": prompt}]}]}
    if thinking != "off":
        body["generationConfig"] = {"thinkingConfig": {"thinkingLevel": thinking}}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
        r = await c.post(url, json=body)
        r.raise_for_status()
        d = r.json()
    text = "".join(p.get("text", "") for p in d["candidates"][0]["content"].get("parts", []))
    um = d.get("usageMetadata", {})
    think = um.get("thoughtsTokenCount", 0)
    return {"text": text, "in": um.get("promptTokenCount", 0),
            "out": um.get("candidatesTokenCount", 0) + think, "think": think}


async def _call_openai(model: str, prompt: str, thinking: str) -> dict:
    key = os.environ["OPENAI_API_KEY"]
    body: dict = {"model": model, "input": prompt}
    if thinking != "off":
        body["reasoning"] = {"effort": thinking}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
        r = await c.post("https://api.openai.com/v1/responses",
                         headers={"Authorization": f"Bearer {key}"}, json=body)
        r.raise_for_status()
        d = r.json()
    text = ""
    for item in d.get("output", []):
        if item.get("type") == "message":
            for ct in item.get("content", []):
                if ct.get("type") == "output_text":
                    text += ct.get("text", "")
    u = d.get("usage", {})
    # output_tokens в Responses API уже включает reasoning_tokens
    return {"text": text, "in": u.get("input_tokens", 0), "out": u.get("output_tokens", 0),
            "think": u.get("output_tokens_details", {}).get("reasoning_tokens", 0)}


async def _call_anthropic(model: str, prompt: str, thinking: str) -> dict:
    # 5-я серия: только адаптивный режим — thinking.type="adaptive" + output_config.effort;
    # budget_tokens на Sonnet 5 возвращает 400. max_tokens — общий лимит thinking + ответ.
    key = os.environ["ANTHROPIC_API_KEY"]
    body: dict = {"model": model, "max_tokens": ANTHROPIC_MAX_TOKENS,
                  "messages": [{"role": "user", "content": prompt}]}
    if thinking == "off":
        body["thinking"] = {"type": "disabled"}
    else:
        body["thinking"] = {"type": "adaptive"}
        body["output_config"] = {"effort": thinking}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
        r = await c.post("https://api.anthropic.com/v1/messages",
                         headers={"x-api-key": key, "anthropic-version": "2023-06-01"}, json=body)
        r.raise_for_status()
        d = r.json()
    text = "".join(blk.get("text", "") for blk in d.get("content", []) if blk.get("type") == "text")
    u = d.get("usage", {})
    # output_tokens у Anthropic включает thinking; отдельного счётчика в usage нет
    return {"text": text, "in": u.get("input_tokens", 0), "out": u.get("output_tokens", 0), "think": 0}


_CALLERS = {"gemini": _call_gemini, "openai": _call_openai, "anthropic": _call_anthropic}


async def call_model(model: str, prompt: str, thinking: str) -> dict:
    meta = MODELS[model]
    t0 = time.perf_counter()
    try:
        res = await _CALLERS[meta["vendor"]](model, prompt, thinking)
        err = None
    except Exception as e:  # noqa: BLE001
        res, err = {"text": "", "in": 0, "out": 0, "think": 0}, f"{type(e).__name__}: {e}"
    pin, pout = meta["price"]
    cost = (res["in"] * pin + res["out"] * pout) / 1_000_000
    res.update({"model": model, "thinking": thinking, "wall": round(time.perf_counter() - t0, 2),
                "cost": round(cost, 5), "error": err})
    return res


# ══════════════════════════ разбор и слияние ══════════════════════════

_LEAD = re.compile(r"^[\s\-\*\•\d\.\)\]]+")
_WS = re.compile(r"\s+")


def _norm(s: str) -> str:
    return _WS.sub(" ", s.strip().strip('"«»\'').lower())


def parse_intents(text: str) -> tuple[list[dict], int]:
    """Строки «интент | пример 1; пример 2» → [{"intent", "examples"}]. Второе число — строк без «|» (не разобраны)."""
    out, seen, unparsed = [], set(), 0
    for line in text.splitlines():
        line = _LEAD.sub("", line).strip().strip("`")
        if not line:
            continue
        if _norm(line) == "нет":
            continue
        if "|" not in line:
            unparsed += 1
            continue
        head, _, tail = line.partition("|")
        intent = _WS.sub(" ", head.strip().strip('"«»*').strip())
        if not intent:
            unparsed += 1
            continue
        examples, ex_seen = [], set()
        for ex in re.split(r"[;|]", tail):
            ex = _WS.sub(" ", ex.strip().strip('"«»*').strip())
            if ex and _norm(ex) not in ex_seen:
                ex_seen.add(_norm(ex))
                examples.append(ex)
        k = _norm(intent)
        if k in seen:
            continue
        seen.add(k)
        out.append({"intent": intent, "examples": examples})
    return out, unparsed


def merge_stage(current: list[dict], found: list[dict], stage: int, model: str) -> tuple[list[dict], int, int]:
    """Добавляет к current интенты из found, которых там ещё нет (дедуп по имени). → (список, добавлено, дублей)."""
    have = {_norm(x["intent"]) for x in current}
    added, dupes = 0, 0
    for it in found:
        k = _norm(it["intent"])
        if k in have:
            dupes += 1
            continue
        have.add(k)
        current.append({"intent": it["intent"], "examples": it["examples"], "stage": stage, "by": model})
        added += 1
    return current, added, dupes


def _intents_block(intents: list[dict]) -> str:
    return "\n".join(f"{x['intent']} | {'; '.join(x['examples'])}" for x in intents)


# ══════════════════════════ конвейер ══════════════════════════

class IntentReq(BaseModel):
    seed: str
    keywords: list           # VALID: строки или объекты с query/keyword
    country: str = ""
    language: str = ""
    city: str = ""


def _kw_strings(keywords: list) -> list[str]:
    out, seen = [], set()
    for k in keywords:
        s = k if isinstance(k, str) else (k.get("query") or k.get("keyword") or "") if isinstance(k, dict) else ""
        s = _WS.sub(" ", str(s).strip())
        if s and _norm(s) not in seen:
            seen.add(_norm(s))
            out.append(s)
    return out


async def run_intent_map(req: IntentReq) -> dict:
    t0 = time.perf_counter()
    seed = _WS.sub(" ", req.seed.strip())
    keys = _kw_strings(req.keywords)
    region = req.country.strip() + (f" / {req.city.strip()}" if req.city.strip() else "")
    ctx = {"seed": seed, "region": region or "не указан", "language": req.language or "не указан",
           "keys": "\n".join(keys)}

    intents: list[dict] = []
    stages: list[dict] = []
    for i, (model, thinking) in enumerate(CHAIN, start=1):
        # пустой текущий список (первый проход или упавший первый проход) → полное построение
        mode = "build" if not intents else "extend"
        prompt = FIRST_PROMPT.format(**ctx) if mode == "build" else EXTEND_PROMPT.format(**ctx, intents=_intents_block(intents))
        r = await call_model(model, prompt, thinking)
        found, unparsed = parse_intents(r["text"]) if not r["error"] else ([], 0)
        intents, added, dupes = merge_stage(intents, found, i, model)
        stages.append({
            "stage": i, "model": model, "thinking": thinking, "mode": mode,
            "found": len(found), "added": added, "dupes": dupes, "unparsed": unparsed, "total_after": len(intents),
            "in": r["in"], "out": r["out"], "think": r["think"], "cost": r["cost"], "wall": r["wall"],
            "error": r["error"], "raw": r["text"],
        })

    total_cost = round(sum(s["cost"] for s in stages), 5)
    return {
        "seed": seed, "region": region, "language": req.language, "keywords_in": len(keys),
        "intents": intents,
        "stages": stages,
        "stats": {"intents_total": len(intents), "total_cost": total_cost,
                  "total_wall": round(time.perf_counter() - t0, 2),
                  "errors": [s["model"] for s in stages if s["error"]]},
        "build": BUILD,
    }


router = APIRouter()


@router.post("/api/intent-map")
async def intent_map_endpoint(req: IntentReq):
    if not req.seed.strip():
        return JSONResponse({"error": "seed пустой"}, status_code=400)
    if not req.keywords:
        return JSONResponse({"error": "keywords пустой — нет VALID для карты интентов"}, status_code=400)
    return await run_intent_map(req)


@router.get("/api/intent-map/models")
async def intent_map_models():
    return {"chain": [{"model": m, "thinking": t, "price": MODELS[m]["price"]} for m, t in CHAIN], "build": BUILD}
