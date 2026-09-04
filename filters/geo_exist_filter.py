"""
geo_exist_filter.py — гео-фильтр склеек. build: ge_4.3 (проход 2 на GPT-5.6 Sol, low)

ge_4.3: проход 2 — gpt-5.6-sol, reasoning_effort low (решение Andrew): клиент OpenAI перенесён из
        l3_filter._call_openai дословно (Chat Completions, max_completion_tokens, без temperature;
        completion_tokens уже включает reasoning → биллинг по нему). Ключ env OPENAI_API_KEY. Модель
        прохода 2 выбирается по префиксу: gpt-* → OpenAI, иначе Gemini. Нет ключа / сбой → AREA остаются.
ge_4.2: stats["process_calls"] — журнал ВСЕХ вызовов фильтра за процесс (call_no, location, хвосты,
        wall/tokens/cost по проходам) — чтобы разбивка первого вызова не терялась при перезаписи
        stats вторым. Временно (area_why_detail=True): 3.7 пишет по каждому AREA 1-2 предложения —
        что за объект, где в локации, откуда уверенность — чтобы увидеть причину ошибок по тем,
        кого он оставляет; после разбора выключить флагом.
Схема ge_4.1 — два прохода последовательно, без HTTP-оракулов:
  Проход 1 — lite, thinking low (классификация; high дал 30/30 PLACE, но ×10 thinking-токенов и +10 с).
  Проход 2 — gemini-3.7-flash, thinking low (ge_4.0: lite на «есть ли район в городе» оставил левый
             берег, юбилейный, солнечный ×2, западня с уверенным «микрорайон Полтавы» — закон 6 на районах).
  Счётчики (ge_4.1): на каждый проход wall/tokens/cost в stats["stages"], плюс call_no и накопленные
             итоги процесса stats["process_totals"] — чтобы измерить §8.4 (сколько раз фильтр
             вызывается за прогон автопилота).
  Промпт CLASS: правило «слово может быть и сервисным, и названием заведения → SERVICE»
             (ge_4.0 срезал «форум» как ТЦ).
Схема ge_4.0 — два lite-прохода последовательно, thinking high, без HTTP-оракулов:
  Проход 1 (CLASS, батч по всем хвостам, вопрос «что это за слово», знание города не нужно):
      SERVICE — сервисное/коммерческое слово (цена, отзывы, на дому)      → пропуск (вето, как NOT_GEO)
      PLACE   — улица, ТЦ, магазин, заведение, любой адресный объект     → OUT
      AREA    — район, ориентир-местность, населённый пункт, страна      → на проход 2
      не распознано / нет в ответе                                        → на проход 2 (не в срез)
  Проход 2 (AREA, точечный список только по AREA): «есть ли это в локации или её округе».
      NO → OUT; YES/UNKNOWN → остаётся.
Обоснование (эталон v2, сид «заправка картриджей полтава»): щорса и пушкина внутри сида неотличимы —
оба из городского пула Google; различает их только сервис на месте, а он снаружи. Правило
«улица/объект → OUT» снимает все 28 склеек эталона ценой 8 уличных ключей с сервисом (объём ~0).
Ялтинская-класс (уверенная ошибка памяти, закон 6) исчезает: вопрос «есть ли в городе» задаётся
только районам и населённым пунктам, а улицы уходят классом.
Сбой прохода 1 = фильтр молчит (fail-open). Сбой прохода 2 = AREA остаются.

Схема ge_3.3 (ансамбль GLUE/STATUS/ATTRIB + выключенный судья) сохранена в коде закомментированной
для отката — см. блок «# ge_3.3 ENSEMBLE (rollback)».

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
    "gpt-5.6-sol": (5.00, 30.00),      # ge_4.3; у OpenAI reasoning уже внутри completion_tokens
}
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
GEO_EXIST_BUILD = "ge_4.3 conveyor, area on gpt-5.6-sol low, 2026-09-05"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()

# ge_4.1: счётчики процесса — сколько раз фильтр вызван за жизнь процесса и что это стоило (§8.4)
_PROCESS_TOTALS: Dict[str, Any] = {"calls": 0, "wall": 0.0, "cost_usd": 0.0, "llm_calls": 0}
_PROCESS_CALLS: List[Dict[str, Any]] = []   # ge_4.2: журнал вызовов, попадает в stats["process_calls"]

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

# Постановка 1 — GLUE: промпт ge_1.2 ДОСЛОВНО (лучший по серии, менять запрещено без прогона).
GLUE_PROMPT = (
    "Локация: {location}, {country}.\n"
    "Ниже пронумерованные фрагменты из поисковых запросов с этой локацией.\n"
    "Шаг 1. Определи, какие фрагменты являются гео-элементами (улица, район, населённый пункт, "
    "ТЦ, ориентир). Остальные пропусти и не выводи.\n"
    "Шаг 2. Для каждого гео-элемента реши: это реальное гео-уточнение этой локации — или случайная "
    "склейка подсказок (гео другого города либо место, которого в этой локации нет)?\n"
    "Ответ: номера случайных склеек через запятую. Если их нет — 0. Ничего кроме номеров.\n\n{numbered}"
)

# Постановка 3 — ATTRIB: позитивная привязка к городу (ялтинская → Киев lite знает уверенно).
ATTRIB_PROMPT = (
    "Локация запроса: {location}, {country}.\n"
    "Ниже пронумерованные фрагменты из поисковых запросов.\n"
    "Для каждого фрагмента, который является названием гео-объекта (улица, район, ТЦ, ориентир, "
    "населённый пункт), определи город Украины, к которому этот объект относится В ПЕРВУЮ ОЧЕРЕДЬ.\n"
    "Указывай город только если уверен. Фрагменты, которые не являются гео-объектами, а также "
    "объекты, существующие во многих городах, и объекты самой локации — НЕ включай в ответ.\n"
    # ge_3.3: строка «во многих городах» ВЕРНУЛАСЬ — без неё ATTRIB срезал вул. Лесі Українки
    # (есть в Полтаве, индекс 36002): пан-украинские имена улиц привязываются к самому известному
    # городу. Строка несущая, не убирать без прогона по эталону.\n
    'Ответ строго JSON: {{"items": [{{"n": 1, "city": "Киев"}}]}}. Если таких нет: {{"items": []}}\n\n{numbered}'
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


# ── ge_4.0 ──────────────────────────────────────────────────────────────────────
# Проход 1 — CLASS. Знание города не требуется: вопрос «что это за тип слова». При сомнении — AREA,
# чтобы сомнительное шло на проверку, а не в срез (ложный срез хуже пропуска).
CLASS_PROMPT = (
    "Локация запроса: {location}, {country}.\n"
    "Ниже пронумерованные фрагменты из поисковых запросов с этой локацией. Для КАЖДОГО фрагмента "
    "определи тип слова. Знать локацию для этого не нужно — оценивай только, чем является фрагмент.\n"
    "Классы:\n"
    "SERVICE — сервисное или коммерческое слово, не гео (цена, отзывы, на дому, адреса, недорого, "
    "фото, обучение, работа, онлайн и т.п.).\n"
    "PLACE — конкретный адресный объект: улица, переулок, проспект, бульвар, площадь-улица, ТЦ, ТРЦ, "
    "магазин, рынок, заведение, институт, вокзал-здание, любое место с адресом. Фамилия или имя "
    "человека в роли названия (щорса, гагарина, лесі українки) — это улица → PLACE.\n"
    "AREA — территория без адреса: район города, микрорайон, часть города (центр, левый берег, "
    "возле вокзала, в районе ...), населённый пункт, область, страна.\n"
    "Если слово может быть и сервисным словом, и названием заведения (форум, галерея, планета) — SERVICE.\n"
    "Если не уверен между PLACE и AREA — ставь AREA. Если фрагмент непонятен — AREA.\n"
    "Ответ строго JSON без текста вокруг: "
    '{{"items": [{{"n": 1, "class": "SERVICE|PLACE|AREA"}}]}}\n\n{numbered}'
)

# Проход 2 — AREA. Точечный список (обычно 3-10 фрагментов), только территории.
AREA_PROMPT = (
    "Локация запроса: {location}, {country}.\n"
    "Ниже пронумерованные фрагменты — районы, части города, населённые пункты или страна из поисковых "
    "запросов с этой локацией. Для каждого ответь: относится ли эта территория к локации или её "
    "ближайшей округе (районы и части самого города; соседние населённые пункты, откуда или куда "
    "логично ехать; область и страна локации)?\n"
    "YES — да, относится. NO — твёрдо знаешь, что такой территории в локации и её округе нет "
    "(это район или город в другом месте страны, либо такого объекта не существует). "
    "UNKNOWN — не уверен.\n"
    "NO ставь только при твёрдой уверенности; при сомнении — UNKNOWN.\n"
    "{why_rule}"
    "Ответ строго JSON без текста вокруг: "
    '{{"items": [{{"n": 1, "answer": "YES|NO|UNKNOWN", "why": "{why_fmt}"}}]}}\n\n{numbered}'
)
# ge_4.2: два режима пояснений — короткий (штатный) и подробный (временно, для разбора ошибок)
_WHY_SHORT = ("", "3-8 слов")
_WHY_DETAIL = ("В поле why по КАЖДОМУ фрагменту 1-2 предложения: что это за объект (район, парк, село, "
               "микрорайон), где именно он находится и в каком городе, и откуда уверенность — "
               "это важно и для YES, и для UNKNOWN.\n", "1-2 предложения")
_CLASSES = {"SERVICE", "PLACE", "AREA"}
_AREA_ANSWERS = {"YES", "NO", "UNKNOWN"}
# ────────────────────────────────────────────────────────────────────────────────

_TOKEN_RE = re.compile(r"[а-яёіїєґa-z0-9\-']+")
_STATUSES = {"NOT_GEO", "GEO_LOCAL", "GEO_FOREIGN", "GEO_UNCERTAIN", "RENAMED"}
_VERDICTS = {"LOCAL", "FOREIGN", "UNKNOWN"}


@dataclass
class GeoExistConfig:
    api_key: str = ""
    country: str = "ua"
    timeout: int = 60
    thinking_level: str = "low"    # ge_4.1: обратно low (ge_4.0 high — ×10 thinking, +10 с, 30/30 и на low)
    # area_model: str = "gemini-3.7-flash"   # ge_4.1–4.2: 3.7 low — тоже конструирует районы (левый берег, солнечный)
    area_model: str = "gpt-5.6-sol"          # ge_4.3: проход 2 на Sol; префикс gpt- → OpenAI-клиент
    area_effort: str = "low"                 # ge_4.3: reasoning_effort для OpenAI (none|low|medium|high|xhigh)
    area_why_detail: bool = True            # ge_4.2: ВРЕМЕННО подробные why от 3.7; после разбора → False
    classifier_model: str = MODEL_CLASSIFIER
    judge_model: str = "gemini-3.7-flash"   # точечный судья, с google_search
    use_judge: bool = False                 # ge_3.1: судья выключен (время), ансамбль решает
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


def _call_openai(api_key: str, model: str, user_prompt: str, timeout: int, effort: str) -> Tuple[str, Dict]:
    """ge_4.3: OpenAI Chat Completions — перенос l3_filter._call_openai. Возвращает (text, diag) в формате
    _call_gemini: in/out/think/model; think=0, т.к. reasoning уже входит в completion_tokens."""
    import requests
    payload: Dict[str, Any] = {
        "model": model,
        "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                     {"role": "user", "content": user_prompt}],
        "max_completion_tokens": 8192,
        "stream": False,
        "reasoning_effort": effort,
        "response_format": {"type": "json_object"},
    }
    try:
        r = requests.post(OPENAI_API_URL, headers={"Content-Type": "application/json",
                                                    "Authorization": f"Bearer {api_key}"},
                          json=payload, timeout=timeout)
    except requests.exceptions.RequestException as e:
        raise Exception(f"GeoExist OpenAI network error: {type(e).__name__}: {e}")
    if r.status_code != 200:
        raise Exception(f"GeoExist OpenAI API error {r.status_code}: {r.text[:400]}")
    d = r.json()
    choices = d.get("choices") or []
    if not choices:
        raise Exception(f"GeoExist OpenAI no choices: {str(d)[:300]}")
    text = ((choices[0].get("message") or {}).get("content") or "").strip()
    if not text:
        raise Exception(f"GeoExist OpenAI empty text (finish_reason={choices[0].get('finish_reason')})")
    um = d.get("usage", {}) or {}
    return text, {"in": um.get("prompt_tokens", 0) or 0, "out": um.get("completion_tokens", 0) or 0,
                  "think": 0, "model": model, "searched": False,
                  "reasoning_tokens": ((um.get("completion_tokens_details") or {}).get("reasoning_tokens")),
                  "effort_sent": effort}


def _call_area_model(cfg: "GeoExistConfig", user_prompt: str) -> Tuple[str, Dict]:
    """ge_4.3: проход 2 — OpenAI для gpt-*, иначе Gemini."""
    if cfg.area_model.startswith("gpt-"):
        key = os.environ.get("OPENAI_API_KEY", "").strip()
        if not key:
            raise Exception("no OPENAI_API_KEY")
        return _call_openai(key, cfg.area_model, user_prompt, cfg.timeout, cfg.area_effort)
    return _call_gemini(cfg.api_key, cfg.area_model, user_prompt, cfg.timeout, cfg.thinking_level)


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


def _cost1(dg: Dict) -> float:
    return _cost([dg])


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

    # ── ge_3.3 ENSEMBLE (rollback): раскомментировать блок и убрать конвейер ge_4.0 ниже
    # # ── Ансамбль: три lite-постановки параллельно
    # numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(tails))
    # from concurrent.futures import ThreadPoolExecutor
    #
    # def run_glue():
    #     text, dg = _call_gemini(config.api_key, config.classifier_model,
    #                             GLUE_PROMPT.format(location=location, country=config.country,
    #                                                numbered=numbered),
    #                             config.timeout, config.thinking_level, json_mime=False)
    #     nums = {int(x) for x in re.findall(r"\d+", text)}
    #     if not nums:
    #         raise Exception(f"glue parse fail: {text[:100]}")
    #     return {tails[i] for i in range(len(tails)) if (i + 1) in nums}, dg
    #
    # def run_status():
    #     text, dg = _call_gemini(config.api_key, config.classifier_model,
    #                             CLASSIFY_PROMPT.format(location=location, country=config.country,
    #                                                    numbered=numbered),
    #                             config.timeout, config.thinking_level)
    #     parsed = _parse_json(text)
    #     if not parsed or "items" not in parsed:
    #         raise Exception(f"status parse fail: {text[:100]}")
    #     st = {}
    #     for it in parsed["items"]:
    #         n, v = it.get("n"), str(it.get("status", "")).upper()
    #         if isinstance(n, int) and 1 <= n <= len(tails) and v in _STATUSES:
    #             st[tails[n - 1]] = v
    #     return st, dg
    #
    # def run_attrib():
    #     text, dg = _call_gemini(config.api_key, config.classifier_model,
    #                             ATTRIB_PROMPT.format(location=location, country=config.country,
    #                                                  numbered=numbered),
    #                             config.timeout, config.thinking_level)
    #     parsed = _parse_json(text)
    #     if parsed is None or "items" not in parsed:
    #         raise Exception(f"attrib parse fail: {text[:100]}")
    #     loc_low = location.lower()
    #     out = {}
    #     for it in parsed["items"]:
    #         n, city = it.get("n"), str(it.get("city", "")).strip()
    #         if isinstance(n, int) and 1 <= n <= len(tails) and city and city.lower() not in loc_low and loc_low not in city.lower():
    #             out[tails[n - 1]] = city
    #     return out, dg
    #
    # glue_cut: set = set()
    # status: Dict[str, str] = {}
    # attrib: Dict[str, str] = {}
    # errors = []
    # with ThreadPoolExecutor(max_workers=3) as ex:
    #     futs = {"glue": ex.submit(run_glue), "status": ex.submit(run_status), "attrib": ex.submit(run_attrib)}
    #     for name, fut in futs.items():
    #         try:
    #             val, dg = fut.result()
    #             diags.append(dg)
    #             if name == "glue":
    #                 glue_cut = val
    #             elif name == "status":
    #                 status = val
    #             else:
    #                 attrib = val
    #         except Exception as e:  # noqa: BLE001 — сбой постановки = её вклад пуст
    #             errors.append(f"{name}: {str(e)[:120]}")
    # if errors:
    #     stats["stage_errors"] = errors
    # if len(errors) == 3:                                  # все три упали → фильтр молчит
    #     stats["error"] = "all ensemble calls failed"
    #     stats["wall"] = round(time.time() - t0, 2)
    #     logger.error("[GEO_EXIST] ансамбль целиком упал — fail-open")
    #     return result
    #
    # status_foreign = {t for t, v in status.items() if v == "GEO_FOREIGN"}
    # protected = {t for t, v in status.items() if v in ("NOT_GEO", "RENAMED")}   # вето Andrew
    # stats["stages"] = {
    #     "glue": {"cut": len(glue_cut)},
    #     "status": {"counts": {v: sum(1 for x in status.values() if x == v) for v in _STATUSES}},
    #     "attrib": {"foreign": len(attrib)},
    # }
    #
    # # ── Этап B: точечный судья — по умолчанию ВЫКЛЮЧЕН (use_judge=False), оставлен для стенда
    # need = [t for t in tails if status.get(t) == "GEO_UNCERTAIN"][: config.judge_limit]
    # judged: Dict[str, Tuple[str, str, bool]] = {}
    # if config.use_judge and need:
    #     judged = _judge_tails(need, seed, location, config, diags)
    #     stats["stages"]["judge"] = {
    #         "asked": len(need),
    #         "verdicts": {v: sum(1 for x in judged.values() if x[0] == v) for v in ("REAL", "GLUE", "UNKNOWN")},
    #         "searched": sum(1 for x in judged.values() if x[2])}
    #
    # # ── Политика: объединение срезов минус вето
    # trash_tails = set()
    # for t in tails:
    #     if t in protected:
    #         continue
    #     if t in glue_cut or t in status_foreign or t in attrib or judged.get(t, ("",))[0] == "GLUE":
    #         trash_tails.add(t)

    # ── ge_4.0 КОНВЕЙЕР ─────────────────────────────────────────────────────────
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(tails))

    # Проход 1 — CLASS (батч по всем хвостам)
    cls: Dict[str, str] = {}
    t_cls = time.time()
    try:
        text, dg = _call_gemini(config.api_key, config.classifier_model,
                                CLASS_PROMPT.format(location=location, country=config.country,
                                                    numbered=numbered),
                                config.timeout, config.thinking_level)
        diags.append(dg)
        parsed = _parse_json(text)
        if not parsed or "items" not in parsed:
            raise Exception(f"class parse fail: {text[:100]}")
        for it in parsed["items"]:
            n, v = it.get("n"), str(it.get("class", "")).upper()
            if isinstance(n, int) and 1 <= n <= len(tails) and v in _CLASSES:
                cls[tails[n - 1]] = v
    except Exception as e:  # noqa: BLE001 — сбой прохода 1 = фильтр молчит (fail-open)
        stats["error"] = f"class: {str(e)[:160]}"
        stats["wall"] = round(time.time() - t0, 2)
        stats["cost_usd"] = _cost(diags)
        _PROCESS_TOTALS["calls"] += 1
        _PROCESS_TOTALS["wall"] += stats["wall"]
        stats["process_totals"] = dict(_PROCESS_TOTALS)
        _PROCESS_CALLS.append({"call_no": _PROCESS_TOTALS["calls"], "location": location,
                               "tails": len(tails), "error": stats["error"], "wall": stats["wall"]})
        stats["process_calls"] = list(_PROCESS_CALLS)
        logger.error(f"[GEO_EXIST] проход CLASS упал — fail-open: {stats['error']}")
        return result
    cls_diag = diags[-1]

    place = {t for t, v in cls.items() if v == "PLACE"}
    service = {t for t, v in cls.items() if v == "SERVICE"}
    area = [t for t in tails if t not in place and t not in service]   # AREA + нераспознанные
    stats["stages"] = {"class": {"counts": {v: sum(1 for x in cls.values() if x == v) for v in _CLASSES},
                                 "unclassified": sum(1 for t in tails if t not in cls),
                                 "model": cls_diag["model"], "wall": round(time.time() - t_cls, 2),
                                 "tokens": {"in": cls_diag["in"], "out": cls_diag["out"], "think": cls_diag["think"]},
                                 "cost_usd": _cost1(cls_diag)}}

    # Проход 2 — AREA (точечный список); сбой = AREA остаются
    area_ans: Dict[str, Tuple[str, str]] = {}
    area_diag: Optional[Dict] = None
    if area:
        numbered_area = "\n".join(f"{i+1}. {t}" for i, t in enumerate(area))
        t_area = time.time()
        try:
            text, dg = _call_area_model(config,
                                        AREA_PROMPT.format(location=location, country=config.country,
                                                           numbered=numbered_area,
                                                           why_rule=(_WHY_DETAIL if config.area_why_detail else _WHY_SHORT)[0],
                                                           why_fmt=(_WHY_DETAIL if config.area_why_detail else _WHY_SHORT)[1]))
            diags.append(dg)
            area_diag = dg
            parsed = _parse_json(text)
            if not parsed or "items" not in parsed:
                raise Exception(f"area parse fail: {text[:100]}")
            for it in parsed["items"]:
                n, v = it.get("n"), str(it.get("answer", "")).upper()
                if isinstance(n, int) and 1 <= n <= len(area) and v in _AREA_ANSWERS:
                    area_ans[area[n - 1]] = (v, str(it.get("why", ""))[:400 if config.area_why_detail else 120])
        except Exception as e:  # noqa: BLE001
            stats["stage_errors"] = [f"area: {str(e)[:160]}"]
            logger.warning(f"[GEO_EXIST] проход AREA упал — AREA остаются: {e}")
        stats["stages"]["area"] = {"asked": len(area),
                                   "answers": {v: sum(1 for x in area_ans.values() if x[0] == v)
                                               for v in _AREA_ANSWERS},
                                   "model": config.area_model, "effort": (config.area_effort if config.area_model.startswith("gpt-") else config.thinking_level),
                                   "reasoning_tokens": (area_diag or {}).get("reasoning_tokens"),
                                   "wall": round(time.time() - t_area, 2),
                                   "tokens": ({"in": area_diag["in"], "out": area_diag["out"],
                                               "think": area_diag["think"]} if area_diag else None),
                                   "cost_usd": _cost1(area_diag) if area_diag else 0.0}

    # Политика: PLACE → OUT; AREA+NO → OUT; всё остальное остаётся
    trash_tails = set(place) | {t for t, (v, _) in area_ans.items() if v == "NO"}
    # ────────────────────────────────────────────────────────────────────────────

    if "anchors" not in result:
        result["anchors"] = []
    # ge_3.3 trace (rollback):
    # trace, trash_kw = [], set()
    # n_trash = 0
    # for t in tails:
    #     keep = t not in trash_tails
    #     jv = judged.get(t)
    #     by = [src for src, hit in (("glue", t in glue_cut), ("status", t in status_foreign),
    #                                ("attrib", t in attrib), ("judge", jv[0] == "GLUE" if jv else False)) if hit]
    #     trace.append({"tail": t, "status": status.get(t), "by": by, "city": attrib.get(t),
    #                   "verdict": jv[0] if jv else None, "reason": jv[1] if jv else None,
    #                   "exists": keep, "keywords": [_kw_str(k) for k in tail_to_kws[t]]})
    #     if not keep:
    #         for k in tail_to_kws[t]:
    #             trash_kw.add(_kw_str(k).lower())
    #             jv = judged.get(t)
    #             result["anchors"].append({"keyword": _kw_str(k), "anchor_reason": "GEO_EXIST_TRASH",
    #                                       "geo_exist": {"tail": t, "status": status.get(t), "by": by, "city": attrib.get(t),
    #                                                     "verdict": jv[0] if jv else None,
    #                                                     "reason": jv[1] if jv else None,
    #                                                     "location": location}})
    #             n_trash += 1
    trace, trash_kw = [], set()
    n_trash = 0
    for t in tails:
        keep = t not in trash_tails
        av = area_ans.get(t)
        by = "PLACE" if t in place else ("AREA_NO" if (av and av[0] == "NO") else None)
        rec = {"tail": t, "class": cls.get(t), "area": av[0] if av else None,
               "why": av[1] if av else None, "by": by, "exists": keep,
               "keywords": [_kw_str(k) for k in tail_to_kws[t]]}
        trace.append(rec)
        if not keep:
            for k in tail_to_kws[t]:
                trash_kw.add(_kw_str(k).lower())
                result["anchors"].append({"keyword": _kw_str(k), "anchor_reason": "GEO_EXIST_TRASH",
                                          "geo_exist": {"tail": t, "class": cls.get(t), "by": by,
                                                        "area": av[0] if av else None,
                                                        "why": av[1] if av else None,
                                                        "location": location}})
                n_trash += 1
    result["keywords"] = [kw for kw in keywords if _kw_str(kw).lower() not in trash_kw]
    if "count" in result:
        result["count"] = len(result["keywords"])
    result["_geo_exist_trace"] = trace
    stats.update({"trash": n_trash, "kept": len(result["keywords"]),
                  "wall": round(time.time() - t0, 2), "cost_usd": _cost(diags),
                  "llm_calls": len(diags), "tokens": diags})
    _PROCESS_TOTALS["calls"] += 1
    _PROCESS_TOTALS["wall"] = round(_PROCESS_TOTALS["wall"] + stats["wall"], 2)
    _PROCESS_TOTALS["cost_usd"] = round(_PROCESS_TOTALS["cost_usd"] + stats["cost_usd"], 6)
    _PROCESS_TOTALS["llm_calls"] += len(diags)
    stats["process_totals"] = dict(_PROCESS_TOTALS)
    st = stats["stages"]
    _PROCESS_CALLS.append({
        "call_no": _PROCESS_TOTALS["calls"], "location": location, "tails": len(tails), "trash": n_trash,
        "wall": stats["wall"], "cost_usd": stats["cost_usd"],
        "class": {k: st["class"].get(k) for k in ("counts", "wall", "tokens", "cost_usd")},
        "area": ({k: st["area"].get(k) for k in ("asked", "answers", "wall", "tokens", "cost_usd")}
                 if "area" in st else None),
        "trash_tails": sorted(trash_tails),
        "area_kept": {t: list(v) for t, v in area_ans.items() if v[0] != "NO"},
    })
    stats["process_calls"] = list(_PROCESS_CALLS)
    logger.info(f"[GEO_EXIST] {GEO_EXIST_BUILD} call#{_PROCESS_TOTALS['calls']} loc='{location}' "
                f"tails={len(tails)} trash={n_trash} "
                f"class={st['class']['wall']}s/${st['class']['cost_usd']} "
                f"area={st.get('area', {}).get('wall', 0)}s/${st.get('area', {}).get('cost_usd', 0)} "
                f"total={stats['wall']}s/${stats['cost_usd']} "
                f"process: calls={_PROCESS_TOTALS['calls']} wall={_PROCESS_TOTALS['wall']}s "
                f"cost=${_PROCESS_TOTALS['cost_usd']}")
    return result
