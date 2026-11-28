"""
keyword_planner.py — планировщик ключевых слов Google (Google Ads API): идеи как источник ключей + замер ключей.

Регистрация в main.py:
    from utils.keyword_planner import router as keyword_planner_router
    app.include_router(keyword_planner_router)
и две вставки в /api/apply-filters (planner_before_filters / planner_after_filters) — прямой режим.
Релевантный режим: utils/relevant_worker.py 0.7 берёт planner_ideas / planner_metrics отсюда.

kp_0.1 (Andrew, 2026-11-28). Решения Andrew:
  • идеи Google — ещё один источник ключей; идут через ВСЮ цепочку фильтров наравне с подсказками
    («мы за чистоту»: что срежет — то срежет), фильтруются с исходным сидом;
  • трассировка: по каждому ключу видно, откуда он пришёл (парсер / планировщик / оба) и каким слоем срезан;
  • замер (объём, сезонность, конкуренция, вилка ставок, средняя цена клика) — для ключей, прошедших фильтры.
Проверено живым запросом 28.11.2026 (скрипт fgs_ads_check 0.6): API v26, доступ Basic у проекта parser,
запросы идут от имени управляющего аккаунта (customer = MCC), отдельный рекламный аккаунт не нужен;
developer-token не требуется (уровень доступа привязан к проекту Google Cloud OAuth-клиента).

Переменные окружения (Render). Пока нет хотя бы одной из первых четырёх — модуль ВЫКЛЮЧЕН:
эндпоинты отвечают configured=false, прямой и релевантный режимы работают ровно как до модуля.
    GOOGLE_ADS_CLIENT_ID        OAuth-клиент (тип Desktop) проекта Google Cloud
    GOOGLE_ADS_CLIENT_SECRET
    GOOGLE_ADS_REFRESH_TOKEN
    GOOGLE_ADS_CUSTOMER_ID      номер аккаунта, от имени которого идут запросы (можно с дефисами)
    GOOGLE_ADS_LOGIN_CUSTOMER_ID  необязательно; по умолчанию = GOOGLE_ADS_CUSTOMER_ID
    KEYWORD_PLANNER_OFF=1       рубильник: выключить модуль, не удаляя ключи

GET  /api/keyword-planner/status                                   → {configured, build, ...}
POST /api/keyword-ideas    {"seed", "country", "language"}         → {"ideas": [...], "stats": {...}}
POST /api/keyword-metrics  {"keywords": [...], "country", "language"} → {"metrics": {ключ: {...}}, "stats": {...}}

Запросы к Google бесплатны: stats.total_cost = 0 (общий контракт ворот).
Модуль самодостаточен: httpx + стандартная библиотека, фильтры и парсеры не импортирует.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time

import httpx
from fastapi import APIRouter
from pydantic import BaseModel

BUILD = "kp_0.1"
logger = logging.getLogger("keyword_planner")

API_HOST = "https://googleads.googleapis.com"
TOKEN_URL = "https://oauth2.googleapis.com/token"
API_VERSIONS = ["v26", "v25", "v24"]      # берётся первая, которая не отвечает 404; запоминается
HTTP_TIMEOUT = 60
MIN_INTERVAL = 1.1                        # с между запросами: у планировщика строгий лимит запросов
RETRY_SLEEP = 5                           # с перед единственным повтором при 429 / RESOURCE_EXHAUSTED
IDEAS_PAGE_SIZE = 1000
IDEAS_MAX_PAGES = 3
METRICS_BATCH = 5000                      # лимит API — 10 000 ключей на запрос
NETWORK = "GOOGLE_SEARCH"

router = APIRouter()

_state = {"token": None, "token_exp": 0.0, "version": None, "last_call": 0.0, "currency": None}
_geo_cache: dict = {}
_lang_cache: dict = {}
_lock: asyncio.Lock | None = None         # создаётся в работающем loop (uvicorn — один loop)


# ───────────────────────── конфигурация ─────────────────────────

def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _customer_id() -> str:
    return re.sub(r"\D", "", _env("GOOGLE_ADS_CUSTOMER_ID"))


def _login_customer_id() -> str:
    return re.sub(r"\D", "", _env("GOOGLE_ADS_LOGIN_CUSTOMER_ID")) or _customer_id()


def is_configured() -> bool:
    if _env("KEYWORD_PLANNER_OFF") in ("1", "true", "yes"):
        return False
    return bool(_env("GOOGLE_ADS_CLIENT_ID") and _env("GOOGLE_ADS_CLIENT_SECRET")
                and _env("GOOGLE_ADS_REFRESH_TOKEN") and _customer_id())


def norm_key(kw) -> str:
    """Ключ сопоставления строк планировщика и парсера: регистр, ё→е, пробелы."""
    return " ".join(str(kw).lower().replace("ё", "е").split())


def _kw_text(kw) -> str:
    if isinstance(kw, str):
        return kw
    if isinstance(kw, dict):
        return str(kw.get("query") or kw.get("keyword") or "")
    return str(kw)


class PlannerError(Exception):
    pass


# ───────────────────────── доступ к API ─────────────────────────

def _get_lock() -> asyncio.Lock:
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


async def _access_token(client: httpx.AsyncClient) -> str:
    if _state["token"] and time.time() < _state["token_exp"] - 60:
        return _state["token"]
    r = await client.post(TOKEN_URL, data={
        "grant_type": "refresh_token",
        "client_id": _env("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": _env("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": _env("GOOGLE_ADS_REFRESH_TOKEN"),
    }, timeout=HTTP_TIMEOUT)
    try:
        d = r.json()
    except ValueError:
        d = {}
    if r.status_code != 200 or not d.get("access_token"):
        raise PlannerError("вход Google не удался: HTTP %s %s" % (r.status_code, str(d.get("error") or "")[:80]))
    _state["token"] = d["access_token"]
    _state["token_exp"] = time.time() + float(d.get("expires_in") or 3000)
    return _state["token"]


def _error_text(data) -> str:
    if not isinstance(data, dict):
        return str(data)[:300]
    err = data.get("error") or {}
    parts = [str(err.get("status") or ""), str(err.get("message") or "")[:200]]
    for det in err.get("details") or []:
        for e in (det.get("errors") or []) if isinstance(det, dict) else []:
            parts.append("%s: %s" % (e.get("errorCode"), str(e.get("message") or "")[:200]))
    return " | ".join(p for p in parts if p)


async def _post(client: httpx.AsyncClient, path: str, body: dict) -> dict:
    """POST к Google Ads API: один запрос за раз, пауза между запросами, один повтор при исчерпании лимита."""
    async with _get_lock():
        versions = [_state["version"]] if _state["version"] else list(API_VERSIONS)
        last = "нет ответа"
        for version in versions:
            for attempt in (1, 2):
                wait = MIN_INTERVAL - (time.time() - _state["last_call"])
                if wait > 0:
                    await asyncio.sleep(wait)
                token = await _access_token(client)
                headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json",
                           "login-customer-id": _login_customer_id()}
                r = await client.post("%s/%s/%s" % (API_HOST, version, path), headers=headers,
                                      json=body, timeout=HTTP_TIMEOUT)
                _state["last_call"] = time.time()
                try:
                    data = r.json()
                except ValueError:
                    data = {"error": {"status": "HTTP %s" % r.status_code, "message": r.text[:200]}}
                if r.status_code == 200:
                    _state["version"] = version
                    return data
                last = "HTTP %s: %s" % (r.status_code, _error_text(data))
                if r.status_code == 401 and attempt == 1:
                    _state["token"] = None                      # токен протух раньше срока — взять новый
                    continue
                if (r.status_code == 429 or "RESOURCE_EXHAUSTED" in last) and attempt == 1:
                    await asyncio.sleep(RETRY_SLEEP)
                    continue
                break
            if not last.startswith("HTTP 404"):
                break                                           # 404 = версии API нет → пробуем следующую
        raise PlannerError(last)


async def _search(client: httpx.AsyncClient, query: str) -> list:
    data = await _post(client, "customers/%s/googleAds:search" % _customer_id(), {"query": query})
    return data.get("results") or []


async def _geo_constant(client: httpx.AsyncClient, country: str):
    """Код страны ('ua') → 'geoTargetConstants/2804'. Берётся у Google, кэшируется. None = без гео."""
    code = re.sub(r"[^A-Za-z]", "", str(country or "")).upper()
    if len(code) != 2:
        return None
    if code not in _geo_cache:
        try:
            rows = await _search(client, "SELECT geo_target_constant.id, geo_target_constant.target_type "
                                         "FROM geo_target_constant WHERE geo_target_constant.country_code = '%s' "
                                         "AND geo_target_constant.target_type = 'Country'" % code)
        except PlannerError as e:                               # не нашли страну — работаем без гео, в stats будет geo=None
            logger.error("[PLANNER] geo lookup error for '%s': %s", code, e)
            return None
        gid = None
        for row in rows:
            gid = (row.get("geoTargetConstant") or {}).get("id")
            if gid:
                break
        _geo_cache[code] = "geoTargetConstants/%s" % gid if gid else None
    return _geo_cache[code]


async def _lang_constant(client: httpx.AsyncClient, language: str):
    """Код языка ('ru') → 'languageConstants/1031'. Берётся у Google, кэшируется. None = все языки."""
    code = re.sub(r"[^A-Za-z]", "", str(language or "")).lower()
    if len(code) != 2:
        return None                                             # 'auto' и названия языков — без языкового фильтра
    if code not in _lang_cache:
        try:
            rows = await _search(client, "SELECT language_constant.id, language_constant.code FROM language_constant "
                                         "WHERE language_constant.code = '%s'" % code)
        except PlannerError as e:                               # не нашли язык — работаем без языкового фильтра
            logger.error("[PLANNER] language lookup error for '%s': %s", code, e)
            return None
        lid = None
        for row in rows:
            lid = (row.get("languageConstant") or {}).get("id")
            if lid:
                break
        _lang_cache[code] = "languageConstants/%s" % lid if lid else None
    return _lang_cache[code]


async def _currency(client: httpx.AsyncClient):
    if _state["currency"] is None:
        try:
            rows = await _search(client, "SELECT customer.currency_code FROM customer LIMIT 1")
            _state["currency"] = ((rows[0].get("customer") or {}).get("currencyCode") if rows else "") or ""
        except PlannerError:
            _state["currency"] = ""
    return _state["currency"]


async def _targeting(client: httpx.AsyncClient, country: str, language: str) -> dict:
    geo = await _geo_constant(client, country)
    lang = await _lang_constant(client, language)
    body = {"keywordPlanNetwork": NETWORK}
    if geo:
        body["geoTargetConstants"] = [geo]
    if lang:
        body["language"] = lang
    return body


# ───────────────────────── разбор ответа ─────────────────────────

def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _units(micros):
    v = _int(micros)
    return None if v is None else round(v / 1e6, 2)


_MONTHS = {"JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4, "MAY": 5, "JUNE": 6, "JULY": 7,
           "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12}


def _metric_row(m: dict) -> dict:
    """Метрики одной строки планировщика → плоский словарь. Суммы — в валюте аккаунта."""
    m = m or {}
    monthly = []
    for x in m.get("monthlySearchVolumes") or []:
        y, mo, n = _int(x.get("year")), _MONTHS.get(str(x.get("month") or "").upper()), _int(x.get("monthlySearches"))
        if y and mo and n is not None:
            monthly.append([y, mo, n])
    return {
        "volume": _int(m.get("avgMonthlySearches")),
        "monthly": monthly,
        "competition": m.get("competition"),
        "competition_index": _int(m.get("competitionIndex")),
        "bid_low": _units(m.get("lowTopOfPageBidMicros")),
        "bid_high": _units(m.get("highTopOfPageBidMicros")),
        "cpc": _units(m.get("averageCpcMicros")),
    }


# ───────────────────────── идеи и замер ─────────────────────────

async def planner_ideas(seed: str, country: str = "ua", language: str = "ru") -> dict:
    """Идеи планировщика от одной затравки. {"skipped": True} — модуль выключен.
    Иначе {"keywords": [...], "ideas": [{keyword, ...метрики}], "stats": {...}} либо {"error": "...", "keywords": []}."""
    if not is_configured():
        return {"skipped": True, "keywords": [], "ideas": []}
    t0 = time.time()
    try:
        async with httpx.AsyncClient() as client:
            base = await _targeting(client, country, language)
            base["keywordSeed"] = {"keywords": [seed]}
            base["pageSize"] = IDEAS_PAGE_SIZE
            ideas, seen, token, pages = [], set(), None, 0
            while pages < IDEAS_MAX_PAGES:
                body = dict(base)
                if token:
                    body["pageToken"] = token
                data = await _post(client, "customers/%s:generateKeywordIdeas" % _customer_id(), body)
                pages += 1
                for item in data.get("results") or []:
                    text = " ".join(str(item.get("text") or "").split())
                    k = norm_key(text)
                    if not k or k in seen:
                        continue
                    seen.add(k)
                    row = _metric_row(item.get("keywordIdeaMetrics"))
                    row["keyword"] = text
                    ideas.append(row)
                token = data.get("nextPageToken")
                if not token:
                    break
            currency = await _currency(client)
        return {"keywords": [i["keyword"] for i in ideas], "ideas": ideas,
                "stats": {"total_cost": 0, "ideas": len(ideas), "pages": pages, "currency": currency,
                          "geo": base.get("geoTargetConstants"), "language": base.get("language"),
                          "api_version": _state["version"], "wall": round(time.time() - t0, 2), "build": BUILD}}
    except Exception as e:                                     # сеть, лимиты, отказ Google — конвейер не роняем
        logger.error("[PLANNER] ideas error for '%s': %s", seed, e)
        return {"error": str(e)[:300], "keywords": [], "ideas": [],
                "stats": {"total_cost": 0, "wall": round(time.time() - t0, 2), "build": BUILD}}


async def planner_metrics(keywords, country: str = "ua", language: str = "ru") -> dict:
    """Замер списка ключей. {"skipped": True} — модуль выключен.
    Иначе {"metrics": {исходный_ключ: {...}}, "stats": {...}}; ключа нет в metrics = Google не дал по нему данных.
    Google склеивает близкие формы: строка ответа относится и к своему text, и к closeVariants."""
    if not is_configured():
        return {"skipped": True, "metrics": {}}
    t0 = time.time()
    originals: dict = {}
    for kw in keywords or []:
        text = _kw_text(kw)
        k = norm_key(text)
        if k:
            originals.setdefault(k, []).append(text)
    uniq = list(originals)
    out: dict = {}
    merged_rows = 0
    try:
        async with httpx.AsyncClient() as client:
            base = await _targeting(client, country, language)
            base["historicalMetricsOptions"] = {"includeAverageCpc": True}
            for i in range(0, len(uniq), METRICS_BATCH):
                body = dict(base)
                body["keywords"] = uniq[i:i + METRICS_BATCH]
                data = await _post(client, "customers/%s:generateKeywordHistoricalMetrics" % _customer_id(), body)
                for item in data.get("results") or []:
                    row = _metric_row(item.get("keywordMetrics"))
                    forms = [str(item.get("text") or "")] + [str(v) for v in (item.get("closeVariants") or [])]
                    hit = [f for f in forms if norm_key(f) in originals]
                    if len(forms) > 1:
                        merged_rows += 1
                    for f in hit:
                        for original in originals[norm_key(f)]:
                            r = dict(row)
                            others = [x for x in forms if norm_key(x) != norm_key(f)]
                            if others:
                                r["merged_with"] = others      # формы, которые Google считает тем же ключом
                            out[original] = r
            currency = await _currency(client)
        with_volume = sum(1 for r in out.values() if r.get("volume"))
        return {"metrics": out,
                "stats": {"total_cost": 0, "requested": len(uniq), "returned": len(out), "with_volume": with_volume,
                          "merged_rows": merged_rows, "currency": currency,
                          "geo": base.get("geoTargetConstants"), "language": base.get("language"),
                          "api_version": _state["version"], "wall": round(time.time() - t0, 2), "build": BUILD}}
    except Exception as e:
        logger.error("[PLANNER] metrics error: %s", e)
        return {"error": str(e)[:300], "metrics": out,
                "stats": {"total_cost": 0, "requested": len(uniq), "returned": len(out),
                          "wall": round(time.time() - t0, 2), "build": BUILD}}


# ───────────────────────── прямой режим: /api/apply-filters ─────────────────────────

SRC_PARSER = "parser"
SRC_PLANNER = "planner"


async def planner_before_filters(seed: str, keywords, country: str = "ua", language: str = "ru"):
    """До цепочки фильтров: идеи планировщика добавляются к ключам парсера.
    None — модуль выключен (ключи не трогаются). Иначе {"keywords": объединённый список, "sources": {...}, ...}.
    Ошибка Google не роняет прогон: ключи парсера идут в фильтры как есть, ошибка попадает в отчёт."""
    if not is_configured():
        return None
    keywords = list(keywords or [])
    sources: dict = {}
    index: dict = {}
    for kw in keywords:
        text = _kw_text(kw)
        k = norm_key(text)
        if k and k not in index:
            index[k] = text
            sources[text] = [SRC_PARSER]
    res = await planner_ideas(seed, country, language)
    added, both = [], 0
    for text in res.get("keywords") or []:
        k = norm_key(text)
        if k in index:
            if SRC_PLANNER not in sources[index[k]]:
                sources[index[k]].append(SRC_PLANNER)
                both += 1
        else:
            index[k] = text
            sources[text] = [SRC_PLANNER]
            added.append(text)
    return {"keywords": keywords + added, "sources": sources, "added": added, "both": both,
            "parser_count": len(keywords), "ideas": res.get("ideas") or [],
            "ideas_stats": res.get("stats") or {}, "error": res.get("error")}


async def planner_after_filters(result: dict, kp, country: str = "ua", language: str = "ru") -> dict:
    """После цепочки фильтров: в ответ добавляются источники ключей, сводка «что откуда пришло и где срезано»
    и замер прошедших ключей. Существующие поля ответа не меняются, кроме пометки source у срезанных."""
    if not kp or not isinstance(result, dict):
        return result
    sources = kp["sources"]
    by_norm = {norm_key(k): v for k, v in sources.items()}

    def src_of(kw):
        return by_norm.get(norm_key(_kw_text(kw))) or [SRC_PARSER]

    valid = [_kw_text(k) for k in (result.get("keywords") or [])]
    grey = [_kw_text(k) for k in (result.get("keywords_grey") or [])]
    blocked = ((result.get("_trace") or {}).get("blocked_keywords") or {}) if isinstance(result.get("_trace"), dict) else {}
    summary = {s: {"valid": 0, "grey": 0, "blocked": 0, "blocked_by": {}} for s in (SRC_PARSER, SRC_PLANNER, "both")}

    def bucket(kw):
        s = src_of(kw)
        return "both" if len(s) > 1 else s[0]

    for kw in valid:
        summary[bucket(kw)]["valid"] += 1
    for kw in grey:
        summary[bucket(kw)]["grey"] += 1
    for kw, info in blocked.items():
        b = summary[bucket(kw)]
        b["blocked"] += 1
        layer = str((info or {}).get("blocked_by") or "?") if isinstance(info, dict) else "?"
        b["blocked_by"][layer] = b["blocked_by"].get(layer, 0) + 1
        if isinstance(info, dict):
            info["source"] = src_of(kw)                        # откуда пришёл срезанный ключ
    result["keywords_sources"] = sources
    met = await planner_metrics(valid + grey, country, language)
    result["planner"] = {
        "build": BUILD,
        "ideas_total": len(kp.get("ideas") or []),
        "ideas_new": len(kp.get("added") or []),               # идеи, которых не было у парсера
        "ideas_also_in_parser": kp.get("both", 0),
        "parser_count": kp.get("parser_count", 0),
        "by_source": summary,
        "ideas_error": kp.get("error"),
        "ideas_stats": kp.get("ideas_stats") or {},
        "metrics": met.get("metrics") or {},
        "metrics_error": met.get("error"),
        "metrics_stats": met.get("stats") or {},
        "currency": (met.get("stats") or {}).get("currency") or (kp.get("ideas_stats") or {}).get("currency") or "",
    }
    return result


# ───────────────────────── эндпоинты ─────────────────────────

class IdeasRequest(BaseModel):
    seed: str
    country: str = "ua"
    language: str = "ru"


class MetricsRequest(BaseModel):
    keywords: list
    country: str = "ua"
    language: str = "ru"


@router.get("/api/keyword-planner/status")
async def keyword_planner_status():
    return {"configured": is_configured(), "build": BUILD, "api_version": _state["version"],
            "currency": _state["currency"], "stats": {"total_cost": 0}}


@router.post("/api/keyword-ideas")
async def keyword_ideas_endpoint(req: IdeasRequest):
    res = await planner_ideas(" ".join(str(req.seed).split()), req.country, req.language)
    if res.get("skipped"):
        return {"configured": False, "ideas": [], "stats": {"total_cost": 0, "build": BUILD}}
    res.pop("keywords", None)
    res["configured"] = True
    return res


@router.post("/api/keyword-metrics")
async def keyword_metrics_endpoint(req: MetricsRequest):
    res = await planner_metrics(req.keywords, req.country, req.language)
    if res.get("skipped"):
        return {"configured": False, "metrics": {}, "stats": {"total_cost": 0, "build": BUILD}}
    res["configured"] = True
    return res
