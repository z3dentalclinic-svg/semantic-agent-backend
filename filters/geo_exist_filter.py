"""
geo_exist_filter.py — LLM-проверка существования гео-элементов. build: ge_1.0

Задача одна (решение Andrew): на сидах с гео проверять, существует ли реально
гео-элемент из хвоста ключа В ПРЕДЕЛАХ локации сида. Склейки подсказок типа
«заправка картриджей полтава ялтинская улица» → если Ялтинской улицы в Полтаве
нет — ключ в TRASH (anchor_reason="GEO_EXIST_TRASH").

Место в цепочке: сразу после geo_garbage_filter (до платных узлов).
Триггер: гео в сиде. Локация берётся из result["_geo_seed_cities"]
(кладёт geo_garbage_filter), фолбэк — Geox-разбор токенов сида.
Проверка существования — целиком LLM, кодом только отбор кандидатов и разбор ответа.
Ошибка вызова/разбора → fail-open, ключи не трогаем.

Стиль вызова, ключ, трейс — как l2_5_filter.py.
"""
from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

MODEL = "gemini-3.1-flash-lite"
API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
PRICE_IN = 0.25    # $/1M, июль 2026; thinking биллится как output
PRICE_OUT = 1.50
GEO_EXIST_BUILD = "ge_1.0 lin-numbers, 2026-08-30"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

# Промпт утверждён Andrew (итерации: не «город» — сиды бывают Буковель/Закарпатье;
# без «общих обозначений места»; без «рядом с ней» — только внутри локации).
SYSTEM_PROMPT = "Ты проверяешь географию поисковых запросов. Отвечаешь только номерами."
USER_PROMPT = (
    "Локация: {location}, {country}.\n"
    "Ниже пронумерованный список гео-элементов (улицы, районы, населённые пункты, курорты, "
    "ориентиры) из поисковых запросов с этой локацией.\n"
    "Для каждого ответь на вопрос: существует ли реально такой гео-элемент в пределах этой локации?\n"
    "Ответ: номера элементов, которые СУЩЕСТВУЮТ, через запятую. Ничего кроме номеров.\n\n{numbered}"
)

# Маркеры ТИПА гео-элемента — только для ОТБОРА кандидатов на LLM-проверку
# (решение о срезе всегда за LLM). Лингвистический признак, не blacklist.
_GEO_TYPE_MARKERS = (
    "улиц", "вулиц", "проспект", "просп", "переулок", "провулок", "бульвар",
    "площад", "площ", "шоссе", "шосе", "вокзал", "станц", "метро", "район",
    "микрорайон", "мікрорайон", "набережн", "массив", "масив", "жилмассив",
)

_TOKEN_RE = re.compile(r"[а-яёіїєґa-z0-9\-']+")
_NUM_RE = re.compile(r"\d+")


@dataclass
class GeoExistConfig:
    api_key: str = ""
    country: str = "ua"
    timeout: int = 60
    thinking_level: str = "low"
    model: str = MODEL


def _kw_str(kw: Any) -> str:
    return (kw if isinstance(kw, str) else kw.get("query", "")).strip()


def _tokens(text: str) -> List[str]:
    return _TOKEN_RE.findall(text.lower())


def _seed_location(result: Dict[str, Any], seed: str) -> str:
    """Локация сида: из _geo_seed_cities (geo_garbage_filter), фолбэк — Geox по токенам сида."""
    cities = result.get("_geo_seed_cities") or []
    if cities:
        return " ".join(str(c) for c in cities)
    try:
        from .geo_garbage_filter import _has_geo_parse  # тот же разбор, не второй способ
    except ImportError:
        from geo_garbage_filter import _has_geo_parse
    geo_toks = [t for t in _tokens(seed) if _has_geo_parse(t)]
    return " ".join(geo_toks)


def _geo_tail(kw: str, seed_toks: set) -> Optional[str]:
    """Хвост ключа после вычитания токенов сида, если в нём есть гео-признак."""
    tail = [t for t in _tokens(kw) if t not in seed_toks]
    if not tail:
        return None
    has_marker = any(any(t.startswith(m) for m in _GEO_TYPE_MARKERS) for t in tail)
    if not has_marker:
        try:
            from .geo_garbage_filter import _has_geo_parse
        except ImportError:
            from geo_garbage_filter import _has_geo_parse
        has_marker = any(_has_geo_parse(t) for t in tail)
    if not has_marker:
        return None
    return " ".join(tail)


def _call_gemini(api_key: str, user_prompt: str, timeout: int,
                 thinking_level: str, model: str) -> Tuple[str, Dict[str, Any]]:
    import requests
    # Ключ ТОЛЬКО в заголовке (не в URL — утекает в логи HTTP-клиентов).
    url = f"{API_BASE}/{model}:generateContent"
    payload = {
        "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
        "generationConfig": {
            "temperature": 0,
            "thinkingConfig": {"thinkingLevel": thinking_level},
        },
    }
    try:
        response = requests.post(
            url,
            headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
            json=payload,
            timeout=timeout,
        )
    except requests.exceptions.RequestException as e:
        raise Exception(f"GeoExist network error: {type(e).__name__}: {e}")
    if response.status_code != 200:
        raise Exception(f"GeoExist API error {response.status_code}: {response.text[:500]}")
    data = response.json()
    cands = data.get("candidates")
    if not cands:
        raise Exception(f"GeoExist no candidates: {str(data)[:300]}")
    parts = (cands[0].get("content") or {}).get("parts") or []
    text = "".join(p.get("text", "") for p in parts if isinstance(p, dict)).strip()
    um = data.get("usageMetadata", {}) or {}
    diag = {
        "prompt_tokens": um.get("promptTokenCount", 0),
        "output_tokens": um.get("candidatesTokenCount", 0),
        "thinking_tokens": um.get("thoughtsTokenCount", 0),
    }
    if not text:
        raise Exception(f"GeoExist empty text (finishReason={cands[0].get('finishReason')})")
    return text, diag


def apply_geo_exist_filter(
    result: Dict[str, Any],
    seed: str,
    enable_geo_exist: bool = True,
    config: Optional[GeoExistConfig] = None,
) -> Dict[str, Any]:
    """LLM-проверка гео-хвостов. Интерфейс как apply_l2_5_filter: срезанные → anchors."""
    if not enable_geo_exist:
        return result
    if config is None:
        config = GeoExistConfig()
    config.api_key = os.environ.get("GEMINI_API_KEY", "").strip() or config.api_key or GEMINI_API_KEY

    t0 = time.time()
    stats: Dict[str, Any] = {"build": GEO_EXIST_BUILD, "model": config.model,
                             "checked": 0, "trash": 0, "skipped": True}
    result["geo_exist_stats"] = stats
    result["_geo_exist_trace"] = []

    location = _seed_location(result, seed)
    if not location:            # в сиде нет гео → фильтр молчит
        stats["reason"] = "no_geo_in_seed"
        return result
    if not config.api_key:
        stats["reason"] = "no_api_key"
        logger.warning("[GEO_EXIST] GEMINI_API_KEY не задан — skipping")
        return result

    keywords = result.get("keywords", [])
    seed_toks = set(_tokens(seed))
    # хвост → ключи с этим хвостом (дедуп хвостов, один элемент проверяется один раз)
    tail_to_kws: Dict[str, List[Any]] = {}
    for kw in keywords:
        tail = _geo_tail(_kw_str(kw), seed_toks)
        if tail:
            tail_to_kws.setdefault(tail, []).append(kw)
    if not tail_to_kws:
        stats["reason"] = "no_geo_tails"
        return result

    tails = list(tail_to_kws)
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(tails))
    prompt = USER_PROMPT.format(location=location, country=config.country, numbered=numbered)

    stats.update({"skipped": False, "location": location, "checked": len(tails)})
    try:
        text, diag = _call_gemini(config.api_key, prompt, config.timeout,
                                  config.thinking_level, config.model)
    except Exception as e:      # fail-open: ключи не трогаем
        stats["error"] = str(e)[:300]
        stats["wall"] = round(time.time() - t0, 2)
        logger.error(f"[GEO_EXIST] {e} — fail-open, ключи не тронуты")
        return result

    nums = {int(x) for x in _NUM_RE.findall(text)}
    exists = {tails[i] for i in range(len(tails)) if (i + 1) in nums}
    if not nums:                # нечисловой ответ = fail-open («ничего не существует» кодом не признаём)
        stats["error"] = f"parse fail: {text[:120]}"
        stats["wall"] = round(time.time() - t0, 2)
        return result

    if "anchors" not in result:
        result["anchors"] = []
    kept: List[Any] = []
    trace: List[Dict[str, Any]] = []
    n_trash = 0
    trash_kw_set = set()
    for tail, kws in tail_to_kws.items():
        ok = tail in exists
        trace.append({"tail": tail, "exists": ok, "keywords": [_kw_str(k) for k in kws]})
        if not ok:
            for k in kws:
                trash_kw_set.add(_kw_str(k).lower())
                result["anchors"].append({
                    "keyword": _kw_str(k),
                    "anchor_reason": "GEO_EXIST_TRASH",
                    "geo_exist": {"tail": tail, "location": location, "source": config.model},
                })
                n_trash += 1
    for kw in keywords:
        if _kw_str(kw).lower() not in trash_kw_set:
            kept.append(kw)

    result["keywords"] = kept
    if "count" in result:
        result["count"] = len(kept)
    result["_geo_exist_trace"] = trace
    cost = (diag["prompt_tokens"] * PRICE_IN
            + (diag["output_tokens"] + diag["thinking_tokens"]) * PRICE_OUT) / 1_000_000
    stats.update({"trash": n_trash, "kept": len(kept), "wall": round(time.time() - t0, 2),
                  "tokens": diag, "cost_usd": round(cost, 6)})
    logger.info(f"[GEO_EXIST] location='{location}' tails={len(tails)} trash={n_trash} "
                f"cost=${cost:.5f} wall={stats['wall']}s")
    return result
