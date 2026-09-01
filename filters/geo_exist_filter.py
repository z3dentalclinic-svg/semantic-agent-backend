"""
geo_exist_filter.py — гео-фильтр склеек. build: ge_2.0 (максимальная сборка по своду консилиума)

Схема: LLM выдвигает гипотезы → код приносит факты → LLM судит по фактам. Память модели не источник истины.

Этапы (каждый выключается конфигом — для теста «что работает, что нет»):
  A. Классификатор (lite, промпт-основа ge_1.2 «склейки» в 2 шага):
     JSON с полем reasoning (CoT будит фактологию lite) и статусом на каждый хвост:
     NOT_GEO | GEO_LOCAL | GEO_FOREIGN | GEO_UNCERTAIN | RENAMED
  B. Точечный судья настоящести (gemini-3.7-flash + google_search) ТОЛЬКО для GEO_UNCERTAIN:
     один хвост = один вызов, параллельно; вопрос «настоящий запрос или случайная склейка» (or_1.2:
     36/40 верных вердиктов, 0 UNKNOWN на полтавском корпусе). Nominatim снят — судья его покрывает.
Политика среза (асимметричная):
     TRASH = GEO_FOREIGN этапа A (у «склеечного» промпта 0 ложных за серию) ∪ GLUE судьи.
     NOT_GEO / GEO_LOCAL / REAL / UNKNOWN / RENAMED → KEEP. Fail-open на любом сбое/схеме.
RENAMED → KEEP: решение Andrew по переименованным (гагарина/лермонтова) — отдельной политики пока нет.

Вызов как раньше: apply_geo_exist_filter(result, seed, ...) после geo_garbage_filter,
локация из result["_geo_seed_cities"]. Срезы → anchors GEO_EXIST_TRASH.
"""
from __future__ import annotations

import json as _json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

MODEL_CLASSIFIER = "gemini-3.1-flash-lite"
MODEL_ADJUDICATOR = "gemini-3.1-flash-lite"
API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
PRICES = {  # $/1M (in, out); thinking биллится как output
    "gemini-3.1-flash-lite": (0.25, 1.50),
    "gemini-3.7-flash": (0.30, 2.50),
}
GEO_EXIST_BUILD = "ge_3.0 lite-router -> pointwise gemini judge, 2026-09-01"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

SYSTEM_PROMPT = "Ты проверяешь географию поисковых запросов. Отвечаешь строго в заданном формате."

# Этап A — основа ge_1.2 (лучший по серии: 12 верных/0 ложных), статусы вместо номеров.
CLASSIFY_PROMPT = (
    "Локация: {location}, {country}.\n"
    "Ниже пронумерованные фрагменты из поисковых запросов с этой локацией.\n"
    "Шаг 1. Определи, какие фрагменты являются гео-элементами (улица, район, населённый пункт, ТЦ, "
    "ориентир). Остальным ставь статус NOT_GEO.\n"
    "Шаг 2. Для каждого гео-элемента реши: это реальное гео-уточнение этой локации — или случайная "
    "склейка подсказок (гео другого города либо место, которого в этой локации нет)?\n"
    "Статусы:\n"
    "NOT_GEO — фрагмент не является гео-элементом (сервисные и коммерческие слова).\n"
    "GEO_LOCAL — реальный гео-объект этой локации.\n"
    "GEO_FOREIGN — гео-объект другого города, случайная склейка.\n"
    "GEO_UNCERTAIN — гео-объект, но принадлежность этой локации не уверена.\n"
    "RENAMED — переименованный или исторический объект этой локации.\n"
    "GEO_LOCAL ставь только если ТВЁРДО знаешь, что объект есть именно в этой локации; "
    "при любом сомнении — GEO_UNCERTAIN. Если сомневаешься между GEO_FOREIGN и GEO_UNCERTAIN — "
    "тоже GEO_UNCERTAIN.\n"
    "Ответ строго JSON без текста вокруг:\n"
    '{{"reasoning": "краткое обоснование по сомнительным (1-3 предложения)", '
    '"items": [{{"n": 1, "status": "..."}}]}}\n\n{numbered}'
)

# Этап B — точечный судья настоящести (промпт or_1.2 + строка про услугу внутри объекта: кейс
# эпицентр/екватор — модель судила наличие услуги, а не настоящесть запроса).
JUDGE_PROMPT = (
    "Локация: {location}, {country}. Поисковый запрос: «{seed} {tail}».\n"
    "Проверь через поиск Google: это настоящий запрос, который мог ввести живой человек, ищущий "
    "«{seed}» — или случайная склейка подсказок (гео-элемент не относится к локации, не существует, "
    "или такой связки никто не ищет)?\n"
    "Не оценивай, оказывается ли услуга внутри названного объекта: человек ищет услугу рядом "
    "с ориентиром, и это настоящий запрос.\n"
    "Первая строка ответа — строго одно слово: REAL или GLUE или UNKNOWN. "
    "Вторая строка — причина в 5-10 слов."
)

_TOKEN_RE = re.compile(r"[а-яёіїєґa-z0-9\-']+")
_STATUSES = {"NOT_GEO", "GEO_LOCAL", "GEO_FOREIGN", "GEO_UNCERTAIN", "RENAMED"}
_VERDICTS = {"LOCAL", "FOREIGN", "UNKNOWN"}


@dataclass
class GeoExistConfig:
    api_key: str = ""
    country: str = "ua"
    timeout: int = 60
    thinking_level: str = "low"
    classifier_model: str = MODEL_CLASSIFIER
    judge_model: str = "gemini-3.7-flash"   # точечный судья, с google_search
    use_judge: bool = True                  # False = только этап A (его FOREIGN)
    judge_limit: int = 8                    # максимум сомнительных на судью за прогон
    judge_parallel: int = 6                 # параллельность точечных вызовов


def _kw_str(kw: Any) -> str:
    return (kw if isinstance(kw, str) else kw.get("query", "")).strip()


def _tokens(text: str) -> List[str]:
    return _TOKEN_RE.findall(text.lower())


def _seed_location(result: Dict[str, Any], seed: str) -> str:
    cities = result.get("_geo_seed_cities") or []
    if cities:
        return " ".join(str(c) for c in cities)
    try:
        from .geo_garbage_filter import _has_geo_parse
    except ImportError:
        from geo_garbage_filter import _has_geo_parse
    return " ".join(t for t in _tokens(seed) if _has_geo_parse(t))


def _geo_tail(kw: str, seed_toks: set) -> Optional[str]:
    tail = [t for t in _tokens(kw) if t not in seed_toks]
    return " ".join(tail) if tail else None


# ─────────────── LLM ───────────────

def _call_gemini(api_key: str, model: str, user_prompt: str, timeout: int, thinking_level: str,
                 json_mime: bool = True, search: bool = False) -> Tuple[str, Dict]:
    import requests
    url = f"{API_BASE}/{model}:generateContent"
    gen: Dict[str, Any] = {"temperature": 0, "thinkingConfig": {"thinkingLevel": thinking_level}}
    if json_mime:
        gen["responseMimeType"] = "application/json"
    payload: Dict[str, Any] = {
        "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
        "generationConfig": gen,
    }
    if search:
        payload["tools"] = [{"google_search": {}}]
    try:
        r = requests.post(url, headers={"Content-Type": "application/json", "x-goog-api-key": api_key},
                          json=payload, timeout=timeout)
    except requests.exceptions.RequestException as e:
        raise Exception(f"GeoExist network error: {type(e).__name__}: {e}")
    if r.status_code != 200:
        raise Exception(f"GeoExist API error {r.status_code}: {r.text[:400]}")
    d = r.json()
    cands = d.get("candidates") or []
    if not cands:
        raise Exception(f"GeoExist no candidates: {str(d)[:300]}")
    text = "".join(p.get("text", "") for p in (cands[0].get("content") or {}).get("parts", [])
                   if isinstance(p, dict)).strip()
    um = d.get("usageMetadata", {}) or {}
    if not text:
        raise Exception(f"GeoExist empty text (finishReason={cands[0].get('finishReason')})")
    return text, {"in": um.get("promptTokenCount", 0), "out": um.get("candidatesTokenCount", 0),
                  "think": um.get("thoughtsTokenCount", 0), "model": model,
                  "searched": bool(cands[0].get("groundingMetadata"))}


def _parse_json(text: str) -> Optional[dict]:
    t = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    try:
        return _json.loads(t)
    except Exception:
        m = re.search(r"\{.*\}", t, re.S)
        if m:
            try:
                return _json.loads(m.group(0))
            except Exception:
                return None
    return None


def _cost(diags: List[Dict]) -> float:
    total = 0.0
    for dg in diags:
        pin, pout = PRICES.get(dg.get("model", ""), (0.30, 2.50))
        total += (dg["in"] * pin + (dg["out"] + dg["think"]) * pout) / 1_000_000
    return round(total, 6)


# ─────────────── Точечный судья (параллельно) ───────────────

def _judge_tails(tails: List[str], seed: str, location: str, cfg: "GeoExistConfig",
                 diags: List[Dict]) -> Dict[str, Tuple[str, str, bool]]:
    """tail → (verdict REAL|GLUE|UNKNOWN, reason, searched). Ошибка вызова → UNKNOWN (fail-open)."""
    from concurrent.futures import ThreadPoolExecutor

    def one(tail: str):
        try:
            text, dg = _call_gemini(cfg.api_key, cfg.judge_model,
                                    JUDGE_PROMPT.format(seed=seed, tail=tail, location=location,
                                                        country=cfg.country),
                                    cfg.timeout, cfg.thinking_level, json_mime=False, search=True)
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            v = lines[0].split()[0].upper() if lines else "UNKNOWN"
            if v not in ("REAL", "GLUE", "UNKNOWN"):
                v = "UNKNOWN"
            return tail, v, (lines[1] if len(lines) > 1 else "")[:120], dg
        except Exception as e:  # noqa: BLE001
            return tail, "UNKNOWN", f"err: {str(e)[:80]}", None

    out: Dict[str, Tuple[str, str, bool]] = {}
    with ThreadPoolExecutor(max_workers=max(1, cfg.judge_parallel)) as ex:
        for tail, v, reason, dg in ex.map(one, tails):
            if dg:
                diags.append(dg)
            out[tail] = (v, reason, bool(dg and dg.get("searched")))
    return out


# ─────────────── Основной вход ───────────────

def apply_geo_exist_filter(
    result: Dict[str, Any],
    seed: str,
    enable_geo_exist: bool = True,
    config: Optional[GeoExistConfig] = None,
) -> Dict[str, Any]:
    if not enable_geo_exist:
        return result
    if config is None:
        config = GeoExistConfig()
    config.api_key = os.environ.get("GEMINI_API_KEY", "").strip() or config.api_key or GEMINI_API_KEY

    t0 = time.time()
    stats: Dict[str, Any] = {"build": GEO_EXIST_BUILD, "checked": 0, "trash": 0,
                             "skipped": True, "stages": {}}
    result["geo_exist_stats"] = stats
    result["_geo_exist_trace"] = []

    location = _seed_location(result, seed)
    if not location:
        stats["reason"] = "no_geo_in_seed"
        return result
    if not config.api_key:
        stats["reason"] = "no_api_key"
        return result

    keywords = result.get("keywords", [])
    seed_toks = set(_tokens(seed))
    tail_to_kws: Dict[str, List[Any]] = {}
    for kw in keywords:
        tail = _geo_tail(_kw_str(kw), seed_toks)
        if tail:
            tail_to_kws.setdefault(tail, []).append(kw)
    if not tail_to_kws:
        stats["reason"] = "no_geo_tails"
        return result

    tails = list(tail_to_kws)
    stats.update({"skipped": False, "location": location, "checked": len(tails)})
    diags: List[Dict] = []

    # ── Этап A: классификатор со статусами и reasoning
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(tails))
    status: Dict[str, str] = {}
    try:
        text, dg = _call_gemini(config.api_key, config.classifier_model,
                                CLASSIFY_PROMPT.format(location=location, country=config.country,
                                                       numbered=numbered),
                                config.timeout, config.thinking_level)
        diags.append(dg)
        parsed = _parse_json(text)
        if not parsed or "items" not in parsed:
            raise Exception(f"classifier parse fail: {text[:150]}")
        for it in parsed["items"]:
            n, st = it.get("n"), str(it.get("status", "")).upper()
            if isinstance(n, int) and 1 <= n <= len(tails) and st in _STATUSES:
                status[tails[n - 1]] = st
        stats["stages"]["classifier"] = {
            "reasoning": str(parsed.get("reasoning", ""))[:600],
            "counts": {}}
    except Exception as e:                               # fail-open всего фильтра
        stats["error"] = str(e)[:300]
        stats["wall"] = round(time.time() - t0, 2)
        logger.error(f"[GEO_EXIST] stage A: {e} — fail-open")
        return result
    for t in tails:
        status.setdefault(t, "GEO_UNCERTAIN")            # не размечен → в сомнительные, не в треш
    stats["stages"]["classifier"]["counts"] = {s: sum(1 for v in status.values() if v == s) for s in _STATUSES}

    # ── Этап B: точечный судья настоящести — только сомнительные
    need = [t for t in tails if status[t] == "GEO_UNCERTAIN"][: config.judge_limit]
    judged: Dict[str, Tuple[str, str, bool]] = {}
    if config.use_judge and need:
        judged = _judge_tails(need, seed, location, config, diags)
        stats["stages"]["judge"] = {
            "asked": len(need),
            "verdicts": {v: sum(1 for x in judged.values() if x[0] == v) for v in ("REAL", "GLUE", "UNKNOWN")},
            "searched": sum(1 for x in judged.values() if x[2])}

    # ── Политика среза: FOREIGN этапа A + GLUE судьи; всё остальное живёт
    trash_tails = set()
    for t in tails:
        if status[t] == "GEO_FOREIGN":
            trash_tails.add(t)
        elif judged.get(t, ("",))[0] == "GLUE":
            trash_tails.add(t)

    if "anchors" not in result:
        result["anchors"] = []
    trace, trash_kw = [], set()
    n_trash = 0
    for t in tails:
        keep = t not in trash_tails
        jv = judged.get(t)
        trace.append({"tail": t, "status": status[t],
                      "verdict": jv[0] if jv else None, "reason": jv[1] if jv else None,
                      "exists": keep, "keywords": [_kw_str(k) for k in tail_to_kws[t]]})
        if not keep:
            for k in tail_to_kws[t]:
                trash_kw.add(_kw_str(k).lower())
                jv = judged.get(t)
                result["anchors"].append({"keyword": _kw_str(k), "anchor_reason": "GEO_EXIST_TRASH",
                                          "geo_exist": {"tail": t, "status": status[t],
                                                        "verdict": jv[0] if jv else None,
                                                        "reason": jv[1] if jv else None,
                                                        "location": location}})
                n_trash += 1
    result["keywords"] = [kw for kw in keywords if _kw_str(kw).lower() not in trash_kw]
    if "count" in result:
        result["count"] = len(result["keywords"])
    result["_geo_exist_trace"] = trace
    stats.update({"trash": n_trash, "kept": len(result["keywords"]),
                  "wall": round(time.time() - t0, 2), "cost_usd": _cost(diags),
                  "llm_calls": len(diags), "tokens": diags})
    logger.info(f"[GEO_EXIST] {GEO_EXIST_BUILD} loc='{location}' tails={len(tails)} trash={n_trash} "
                f"cost=${stats['cost_usd']} wall={stats['wall']}s")
    return result
