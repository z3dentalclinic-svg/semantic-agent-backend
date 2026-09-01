"""
geo_exist_filter.py — гео-фильтр склеек. build: ge_2.0 (максимальная сборка по своду консилиума)

Схема: LLM выдвигает гипотезы → код приносит факты → LLM судит по фактам. Память модели не источник истины.

Этапы (каждый выключается конфигом — для теста «что работает, что нет»):
  A. Классификатор (lite, промпт-основа ge_1.2 «склейки» в 2 шага):
     JSON с полем reasoning (CoT будит фактологию lite) и статусом на каждый хвост:
     NOT_GEO | GEO_LOCAL | GEO_FOREIGN | GEO_UNCERTAIN | RENAMED
  B. Evidence кодом для GEO_UNCERTAIN (+подтверждение GEO_FOREIGN при confirm_foreign):
     OpenStreetMap Nominatim (бесплатно): найден ли объект в локации; где найден вообще.
  C. Судья (lite) по карточкам доказательств: LOCAL | FOREIGN | UNKNOWN.
Политика среза (асимметричная, консенсус консилиума):
     TRASH = GEO_FOREIGN этапа A (при confirm_foreign=False) ∪ FOREIGN судьи по доказательствам.
     NOT_GEO / GEO_LOCAL / UNKNOWN / RENAMED → KEEP. Fail-open на любом сбое или несоблюдении схемы.
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
GEO_EXIST_BUILD = "ge_2.0 hypothesis->evidence->adjudicator, 2026-09-01"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_UA = "semantic-agent-geo-exist/2.0"

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
    "Если сомневаешься между GEO_FOREIGN и GEO_UNCERTAIN — ставь GEO_UNCERTAIN.\n"
    "Ответ строго JSON без текста вокруг:\n"
    '{{"reasoning": "краткое обоснование по сомнительным (1-3 предложения)", '
    '"items": [{{"n": 1, "status": "..."}}]}}\n\n{numbered}'
)

# Этап C — судья по доказательствам, не по памяти. FOREIGN требует положительного подтверждения
# чужой локации; недостаток данных = UNKNOWN, не FOREIGN.
ADJUDICATE_PROMPT = (
    "Локация исходного запроса: {location}, {country}.\n"
    "Ниже гео-кандидаты и результаты внешней проверки по справочнику карт (OpenStreetMap).\n"
    "Для каждого кандидата определи отношение к локации ТОЛЬКО по приведённым данным:\n"
    "LOCAL — данные подтверждают объект в этой локации.\n"
    "FOREIGN — данные подтверждают объект в другой локации, а в этой он не найден.\n"
    "UNKNOWN — данных недостаточно или они неоднозначны.\n"
    "Не используй собственные знания об улицах и объектах, если они не подтверждаются данными. "
    "Совпадение названия в другом городе само по себе не делает объект FOREIGN, если он найден и в этой "
    "локации. Для FOREIGN нужно положительное подтверждение другой локации. При сомнении — UNKNOWN.\n"
    'Ответ строго JSON: {{"items": [{{"n": 1, "verdict": "LOCAL|FOREIGN|UNKNOWN"}}]}}\n\n{cards}'
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
    adjudicator_model: str = MODEL_ADJUDICATOR
    # тумблеры этапов
    use_reasoning_json: bool = True     # A: JSON+reasoning (False — резерв, поведение не меняет: формат A всегда JSON)
    use_evidence: bool = True           # B: Nominatim для GEO_UNCERTAIN
    confirm_foreign: bool = False       # True = GEO_FOREIGN этапа A тоже резать только по доказательству
    use_adjudicator: bool = True        # C: судья LLM (False = решает код: найден в локации → LOCAL и т.д.)
    nominatim_limit: int = 12           # максимум карточек за прогон (rate limit OSM ~1 rps → ~2 c на карточку)


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

def _call_gemini(api_key: str, model: str, user_prompt: str, timeout: int, thinking_level: str) -> Tuple[str, Dict]:
    import requests
    url = f"{API_BASE}/{model}:generateContent"
    payload = {
        "systemInstruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
        "generationConfig": {"temperature": 0, "thinkingConfig": {"thinkingLevel": thinking_level},
                             "responseMimeType": "application/json"},
    }
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
                  "think": um.get("thoughtsTokenCount", 0), "model": model}


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


# ─────────────── Evidence: Nominatim (OSM, бесплатно) ───────────────

def _nominatim(query: str, country: str, timeout: int = 10) -> List[Dict]:
    import requests
    try:
        r = requests.get(NOMINATIM_URL,
                         params={"q": query, "format": "json", "limit": 5,
                                 "countrycodes": country, "accept-language": "uk,ru"},
                         headers={"User-Agent": NOMINATIM_UA}, timeout=timeout)
        if r.status_code != 200:
            return []
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _evidence_card(tail: str, location: str, country: str) -> Dict[str, Any]:
    """Найден ли объект в локации; в каких населённых пунктах найден вообще."""
    local = _nominatim(f"{tail}, {location}", country)
    time.sleep(1.05)                                   # rate limit OSM ~1 rps
    anywhere = _nominatim(tail, country)
    time.sleep(1.05)
    loc_low = location.lower()
    local_hits = [h.get("display_name", "")[:120] for h in local
                  if loc_low in h.get("display_name", "").lower()]
    other = [h.get("display_name", "")[:120] for h in anywhere
             if h.get("display_name") and loc_low not in h.get("display_name", "").lower()]
    return {"tail": tail, "found_in_location": bool(local_hits),
            "local_hits": local_hits[:3], "found_elsewhere": other[:3]}


def _cards_text(cards: List[Dict]) -> str:
    lines = []
    for i, c in enumerate(cards):
        lines.append(f"{i+1}. «{c['tail']}»")
        lines.append("   найден в локации: " + ("да — " + "; ".join(c["local_hits"]) if c["found_in_location"] else "нет"))
        lines.append("   найден в других местах: " + ("; ".join(c["found_elsewhere"]) if c["found_elsewhere"] else "нет"))
    return "\n".join(lines)


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

    # ── Этап B: evidence для сомнительных (и FOREIGN при confirm_foreign)
    need = [t for t in tails if status[t] == "GEO_UNCERTAIN"]
    if config.confirm_foreign:
        need += [t for t in tails if status[t] == "GEO_FOREIGN"]
    need = need[:config.nominatim_limit]
    cards: List[Dict] = []
    if config.use_evidence and need:
        for t in need:
            cards.append(_evidence_card(t, location, config.country))
        stats["stages"]["evidence"] = {"cards": len(cards),
                                       "found_local": sum(1 for c in cards if c["found_in_location"])}

    # ── Этап C: судья
    verdict: Dict[str, str] = {}
    if cards:
        if config.use_adjudicator:
            try:
                text, dg = _call_gemini(config.api_key, config.adjudicator_model,
                                        ADJUDICATE_PROMPT.format(location=location, country=config.country,
                                                                 cards=_cards_text(cards)),
                                        config.timeout, config.thinking_level)
                diags.append(dg)
                parsed = _parse_json(text)
                for it in (parsed or {}).get("items", []):
                    n, v = it.get("n"), str(it.get("verdict", "")).upper()
                    if isinstance(n, int) and 1 <= n <= len(cards) and v in _VERDICTS:
                        verdict[cards[n - 1]["tail"]] = v
            except Exception as e:                        # сбой судьи → сомнительные остаются KEEP
                stats["stages"]["adjudicator_error"] = str(e)[:200]
        else:                                             # решает код по карточке
            for c in cards:
                if c["found_in_location"]:
                    verdict[c["tail"]] = "LOCAL"
                elif c["found_elsewhere"]:
                    verdict[c["tail"]] = "FOREIGN"
                else:
                    verdict[c["tail"]] = "UNKNOWN"

    # ── Политика среза: только доказанное чужое
    trash_tails = set()
    for t in tails:
        if status[t] == "GEO_FOREIGN" and not config.confirm_foreign:
            trash_tails.add(t)
        elif verdict.get(t) == "FOREIGN":
            trash_tails.add(t)

    if "anchors" not in result:
        result["anchors"] = []
    trace, trash_kw = [], set()
    n_trash = 0
    for t in tails:
        keep = t not in trash_tails
        trace.append({"tail": t, "status": status[t], "verdict": verdict.get(t),
                      "exists": keep, "keywords": [_kw_str(k) for k in tail_to_kws[t]]})
        if not keep:
            for k in tail_to_kws[t]:
                trash_kw.add(_kw_str(k).lower())
                result["anchors"].append({"keyword": _kw_str(k), "anchor_reason": "GEO_EXIST_TRASH",
                                          "geo_exist": {"tail": t, "status": status[t],
                                                        "verdict": verdict.get(t), "location": location}})
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
