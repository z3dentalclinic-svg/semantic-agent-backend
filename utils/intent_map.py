"""
intent_map.py — карта интентов по VALID-ключам: конвейер трёх моделей, строго друг за другом.

Регистрация в main.py (две строки, ничего больше):
    from utils.intent_map import router as intent_map_router
    app.include_router(intent_map_router)

POST /api/intent-map
  {"seed": str, "keywords": [str|{"query"|"keyword": str}], "country": str, "language": str, "city": str?}
  → {"map": {...}, "intents": [...], "stages": [...], "stats": {...}, "build": ...}

im_0.2 — стадия A (Andrew, 2026-09-14): унифицированный каркас, одинаковый для любой ниши.
  LLM отдаёт СТРУКТУРУ, не список: предмет сида, варианты предмета (с написаниями и признаками),
  города/языки региона, группы интентов (макро → под-группа → шаблоны с плейсхолдерами
  {variant} {attr} {city}, тип, scope variant|common). Код размножает шаблоны по осям.
  Конвейер тот же: проход 1 строит карту (Gemini), проходы 2–3 (Claude, GPT) отдают ТОЛЬКО добавления
  в той же JSON-структуре, код сливает. Ошибка/непарс прохода не роняет цепочку.
  Промпты не содержат слов ниши — ниша заполняет абстракции сама (правило «алгоритм, не списки»).

im_0.3 (Andrew, 2026-09-14): один язык (из формы), размножение только по каноническому имени варианта
  (написания не размножаются), реальные ключи VALID раскладываются моделью по под-группам (номера),
  якорь предмета + верификатор: шаблон без слова предмета в группе без ключей → бинарный вопрос
  gemini-lite ×3 (большинство), 0 → строка удаляется молча (без блока аудита — решение принимает модуль).
  Написания — только формы имени; коды/версии/комплектации — признаки. Потолок размножения → предупреждение.

im_0.4 (Andrew, 2026-09-15): плейсхолдеров нет — модель пишет только реальные запросы. Размножение = подстановка
  кодом: в под-группе scope=variant место, где назван предмет (леммы, как в якоре), заменяется на каноническое
  имя каждого варианта (запрос, где вариант уже назван, не размножается); запрос с городом из оси — по остальным
  городам. Признаки не размножаются (ось данных). journey — этапы пути клиента как чек-лист для проходов 2–3.
  Тип не из шкалы → «информационный».

im_0.5 (Andrew, 2026-09-15): подстановка городов отключена (город в запросе несёт смысл — «одесса порт»;
  города остаются осью данных для serviceArea); один запрос на под-группу; Claude и GPT — thinking low
  (medium: долго, добавляет мало), Gemini medium.

im_0.6 (Andrew, 2026-09-16): поле notes — «особенности ниши/региона» от специалиста (необязательное), подмешивается
  в промпты всех проходов как контекст; GPT-проход — gpt-5.6-terra вместо sol (в 2 раза дешевле).

im_0.1 — плоский формат «интент | примеры» — блок сохранён внизу файла как точка отката.

Модуль самодостаточен: свой реестр моделей и свои вызовы вендоров (НЕ импортирует minus_words_test —
правится отдельно, не ломая другие модули). Ключи из окружения: GEMINI_API_KEY, OPENAI_API_KEY, ANTHROPIC_API_KEY.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

BUILD = "im_0.6"

# ─── реестр моделей: цена $ за 1M токенов (in, out). Правка цен — только здесь. ───
MODELS: dict[str, dict] = {
    "gemini-3.8-flash": {"vendor": "gemini",    "price": (0.75, 3.75)},   # вводная цена до 31.12.2026, потом 1.5/7.5
    "claude-sonnet-5":  {"vendor": "anthropic", "price": (2.0, 10.0)},
    "gpt-5.6-sol":      {"vendor": "openai",    "price": (4.0, 20.0)},    # акция до 21.11.2026, потом 5/30
    "gpt-5.6-terra":    {"vendor": "openai",    "price": (2.0, 12.0)},
    "gpt-5.6-luna":     {"vendor": "openai",    "price": (0.20, 1.20)},   # кандидат на A/B
    "gemini-3.1-flash-lite": {"vendor": "gemini", "price": (0.10, 0.40)},   # верификатор
}

# Конвейер: порядок = порядок проходов. thinking: off | low | medium | high
# Andrew 2026-09-14: старт medium на всех трёх; после живого прогона — A/B low vs medium на одном VALID.
CHAIN: list[tuple[str, str]] = [
    ("gemini-3.8-flash", "medium"),
    ("claude-sonnet-5",  "low"),      # im_0.5: было medium — 36 с, +15 запросов
    ("gpt-5.6-terra",    "low"),      # im_0.6: было gpt-5.6-sol low (41 с, $0.077); im_0.5: sol medium — 106 с, 73% цены
]

VERIFY: tuple[str, str] = ("gemini-3.1-flash-lite", "low")   # верификатор потока «без якоря»
VERIFY_VOTES = 3                                              # вызовов на голосование, большинство

HTTP_TIMEOUT = 240
ANTHROPIC_MAX_TOKENS = 16000   # общий лимит thinking + ответ (адаптивный режим 5-й серии)
MAX_EXPANDED = 20000           # потолок размноженных интентов — только предупреждение в stats.capped

TYPES = ("коммерческий", "информационный", "навигационный", "инструмент", "сравнение", "локальный")

# ─── промпты im_0.2: каркас Andrew (5 пунктов), без слов ниши ───
FRAMEWORK = (
    "Определи:\n"
    "1. Предмет сида — объект или услуга. Если слово сида совпадает с названием бренда или модели — "
    "это бренд, не нарицательное слово. Укажи написания предмета, которыми его набирают в поиске.\n"
    "2. Варианты предмета — на что предмет делится у покупателя (модели, виды услуги, типы товара, направления). "
    "Для каждого — написания того же имени, которыми его набирают (кириллица, латиница, разговорное), в порядке "
    "употребимости в поиске. Коды, версии, поколения и комплектации — это не написания, а признаки.\n"
    "3. Признаки вариантов — что внутри варианта меняет выбор (поколение, версия, размер, объём, год, класс). "
    "Признак привязан к своему варианту; если у признака есть период — укажи.\n"
    "4. Этапы пути клиента (journey) — сквозные темы, одинаковые для всех вариантов (выбор, цена, оформление, "
    "доставка, проверка, оплата, риски, сервис, сравнение и другие, характерные для этой темы). Перечисли этапы списком.\n"
    "5. Города региона.\n\n"
    "Интенты записывай группами: макро-группа → под-группа → один запрос, самый типичный для этой под-группы. "
    "Запрос — реальная поисковая речь, как люди набирают, только на языке {language}; без шаблонов и подстановочных "
    "скобок. Если под-группа повторяется для каждого варианта предмета, пиши запрос с предметом сида (код сам "
    "подставит варианты). Запрос — запрос человека, который решает задачу сида, в контексте сида; запрос про "
    "другой предмет или без связи с сидом не годится.\n"
    "Для каждой под-группы: тип (" + " / ".join(TYPES) + "), scope: \"variant\" — содержание зависит от варианта "
    "предмета, \"common\" — общий этап, одинаковый для всех вариантов, и keys — номера ключевых слов из списка, "
    "которые относятся к этой под-группе (ключ относится к одной под-группе; если ни один — пустой список).\n"
)
JSON_SHAPE = (
    '{"subject": "...", "subject_is_brand": true, "subject_aliases": ["..."],\n'
    ' "variants": [{"name": "...", "aliases": ["..."], "attrs": [{"name": "...", "period": "..."}]}],\n'
    ' "journey": ["..."], "cities": ["..."],\n'
    ' "groups": [{"macro": "...", "sub": "...", "type": "...", "scope": "variant", "keys": [1, 5], "queries": ["..."]}]}'
)
FIRST_PROMPT = (
    "Сид: «{seed}». Регион: {region}. Язык: {language}.{notes}\n"
    "Ниже ключевые слова, собранные из подсказок Google по этому сиду.\n\n"
    "Задача — полная карта поисковых интентов по этой теме: по ключам плюс из твоей базы знаний (то, чего в ключах нет). "
    "Работай в реалиях региона: местные термины, правила, каналы покупки.\n\n"
    + FRAMEWORK +
    "\nОтвет — только JSON без пояснений:\n" + JSON_SHAPE + "\n\n"
    "Ключевые слова:\n{keys}"
)
EXTEND_PROMPT = (
    "Сид: «{seed}». Регион: {region}. Язык: {language}.{notes}\n"
    "Ниже ключевые слова, собранные из подсказок Google по этому сиду, и уже составленная карта интентов по этой теме.\n\n"
    "Расширь карту: добавь то, чего в ней нет — написания предмета, варианты предмета, признаки вариантов, города, "
    "этапы пути клиента, под-группы и запросы; из ключей и из твоей базы знаний в реалиях региона (местные термины, "
    "правила, каналы покупки). Пройди по списку journey как по чек-листу: каждый этап должен быть закрыт хотя бы одной "
    "под-группой — незакрытые этапы закрой, отсутствующие этапы добавь. Ключи, которые ещё не отнесены ни к одной "
    "под-группе, отнеси к существующей или новой.\n\n"
    + FRAMEWORK +
    "\nОтвет — только JSON той же структуры и ТОЛЬКО с добавлениями: новые варианты целиком; новые написания и признаки — "
    "под именем существующего варианта; номера ключей — под существующими macro и sub; новые под-группы целиком, по одному запросу. "
    "Пустые списки допустимы. Если добавить нечего — {}.\n" + JSON_SHAPE + "\n\n"
    "Ключевые слова:\n{keys}\n\n"
    "Текущая карта:\n{map}"
)
# Верификатор потока «без якоря» — бинарный вопрос в духе кросс-промпта relevant_search (отношение к сиду, не слова)
VERIFY_PROMPT = (
    "Сид: «{seed}». Регион: {region}.\n"
    "Ниже пронумерованные поисковые запросы. Для каждого ответь на вопрос: это запрос человека, который решает "
    "ту же задачу, что человек с сидом, на любом этапе его пути (выбор, покупка, оформление, доставка, проверка, "
    "оплата, сервис)? Запрос про другую тему или другую задачу — нет.\n"
    "Ответ: номера запросов, для которых ДА, через запятую. Ничего кроме номеров.\n\n{numbered}"
)


def _fill(tpl: str, **kw) -> str:
    """Подстановка полей промпта заменой, НЕ str.format: в промптах есть литеральные {variant}/{attr}/{city} и JSON-скобки."""
    for k, v in kw.items():
        tpl = tpl.replace("{" + k + "}", str(v))
    return tpl


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


# ══════════════════════════ разбор JSON и слияние карты ══════════════════════════

_WS = re.compile(r"\s+")
_EMPTY_COUNTS = {"variants": 0, "aliases": 0, "attrs": 0, "cities": 0, "journey": 0, "groups": 0, "queries": 0, "keys": 0}


def _norm(s) -> str:
    return _WS.sub(" ", str(s if s is not None else "").strip().strip('"«»\'').lower())


def _clean(s) -> str:
    return _WS.sub(" ", str(s if s is not None else "").strip().strip('"«»*').strip())


def parse_json(text: str) -> dict | None:
    """JSON из ответа модели: снимает ```-ограждения, берёт от первой { до последней }. None = не разобрано."""
    t = text.strip()
    t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
    t = re.sub(r"\s*```$", "", t)
    a, b = t.find("{"), t.rfind("}")
    if a < 0 or b <= a:
        return None
    try:
        d = json.loads(t[a:b + 1])
    except json.JSONDecodeError:
        return None
    return d if isinstance(d, dict) else None


def empty_map() -> dict:
    return {"subject": "", "subject_is_brand": None, "subject_aliases": [], "variants": [], "journey": [], "cities": [], "groups": []}


def _as_list(x) -> list:
    return x if isinstance(x, list) else ([] if x is None else [x])


def merge_map(cur: dict, add: dict, stage: int, model: str, keys: list[str] | None = None) -> dict:
    """Сливает ответ прохода в карту. Дедуп: варианты по имени/написанию, признаки внутри варианта,
    города, группы по (macro, sub), шаблоны внутри группы; ключи по номерам, ключ — в одну группу (первое
    отнесение выигрывает). Возвращает счётчики добавленного."""
    c = dict(_EMPTY_COUNTS)
    if not isinstance(add, dict):
        return c
    keys = keys or []
    if not cur["subject"] and _clean(add.get("subject")):
        cur["subject"] = _clean(add.get("subject"))
    if cur["subject_is_brand"] is None and isinstance(add.get("subject_is_brand"), bool):
        cur["subject_is_brand"] = add["subject_is_brand"]
    have_sa = {_norm(x) for x in cur["subject_aliases"]} | {_norm(cur["subject"])}
    for x in _as_list(add.get("subject_aliases")):
        xs = _clean(x)
        if xs and _norm(xs) not in have_sa:
            have_sa.add(_norm(xs))
            cur["subject_aliases"].append(xs)

    # варианты предмета: ключ = любое из написаний
    name_index: dict[str, dict] = {}
    for v in cur["variants"]:
        for n in [v["name"]] + v["aliases"]:
            name_index[_norm(n)] = v
    for raw in _as_list(add.get("variants")):
        if isinstance(raw, str):
            raw = {"name": raw}
        if not isinstance(raw, dict):
            continue
        name = _clean(raw.get("name"))
        if not name:
            continue
        aliases = [_clean(a) for a in _as_list(raw.get("aliases")) if _clean(a)]
        v = next((name_index[_norm(n)] for n in [name] + aliases if _norm(n) in name_index), None)
        if v is None:
            v = {"name": name, "aliases": [], "attrs": [], "stage": stage, "by": model}
            cur["variants"].append(v)
            name_index[_norm(name)] = v
            c["variants"] += 1
        for a in aliases:
            if _norm(a) != _norm(v["name"]) and _norm(a) not in name_index:
                v["aliases"].append(a)
                name_index[_norm(a)] = v
                c["aliases"] += 1
        have_attrs = {_norm(x["name"]) for x in v["attrs"]}
        for ar in _as_list(raw.get("attrs")):
            if isinstance(ar, str):
                ar = {"name": ar}
            if not isinstance(ar, dict) or not _clean(ar.get("name")):
                continue
            an = _clean(ar.get("name"))
            if _norm(an) in have_attrs:
                continue
            have_attrs.add(_norm(an))
            v["attrs"].append({"name": an, "period": _clean(ar.get("period")), "stage": stage, "by": model})
            c["attrs"] += 1

    for key in ("cities", "journey"):
        have = {_norm(x) for x in cur[key]}
        for x in _as_list(add.get(key)):
            xs = _clean(x)
            if xs and _norm(xs) not in have:
                have.add(_norm(xs))
                cur[key].append(xs)
                c[key] += 1

    # группы: ключ (macro, sub)
    gindex = {(_norm(g["macro"]), _norm(g["sub"])): g for g in cur["groups"]}
    assigned = {_norm(k) for g in cur["groups"] for k in g["keys"]}
    for raw in _as_list(add.get("groups")):
        if not isinstance(raw, dict):
            continue
        macro, sub = _clean(raw.get("macro")), _clean(raw.get("sub"))
        if not macro and not sub:
            continue
        sub = sub or macro
        macro = macro or sub
        g = gindex.get((_norm(macro), _norm(sub)))
        if g is None:
            typ = _norm(raw.get("type"))
            g = {"macro": macro, "sub": sub, "type": typ if typ in TYPES else "информационный",   # im_0.4: не из шкалы → информационный
                 "scope": "common" if _norm(raw.get("scope")) == "common" else "variant",
                 "queries": [], "keys": [], "stage": stage, "by": model}
            cur["groups"].append(g)
            gindex[(_norm(macro), _norm(sub))] = g
            c["groups"] += 1
        have_q = {_norm(q["q"]) for q in g["queries"]}
        for q in _as_list(raw.get("queries")) + _as_list(raw.get("templates")):   # templates — совместимость с ответом старого формата
            qs = _clean(q).replace("{variant}", cur["subject"]).replace("{city}", cur["cities"][0] if cur["cities"] else "").replace("{attr}", "")
            qs = _WS.sub(" ", qs).strip()
            if qs and _norm(qs) not in have_q:
                have_q.add(_norm(qs))
                g["queries"].append({"q": qs, "stage": stage, "by": model})
                c["queries"] += 1
        for n in _as_list(raw.get("keys")):
            try:
                idx = int(n)
            except (TypeError, ValueError):
                continue
            if 1 <= idx <= len(keys) and _norm(keys[idx - 1]) not in assigned:
                assigned.add(_norm(keys[idx - 1]))
                g["keys"].append(keys[idx - 1])
                c["keys"] += 1
    return c


def map_for_prompt(cur: dict, keys_index: dict[str, int] | None = None) -> str:
    """Карта в компактном JSON для промпта расширения (без служебных stage/by); ключи — номерами списка."""
    slim = {
        "subject": cur["subject"], "subject_is_brand": cur["subject_is_brand"], "subject_aliases": cur["subject_aliases"],
        "variants": [{"name": v["name"], "aliases": v["aliases"],
                      "attrs": [{"name": a["name"], "period": a["period"]} for a in v["attrs"]]} for v in cur["variants"]],
        "journey": cur["journey"], "cities": cur["cities"],
        "groups": [{"macro": g["macro"], "sub": g["sub"], "type": g["type"], "scope": g["scope"],
                    "keys": [keys_index.get(_norm(k), 0) for k in g["keys"]] if keys_index else [],
                    "queries": [q["q"] for q in g["queries"]]} for g in cur["groups"]],
    }
    return json.dumps(slim, ensure_ascii=False)


# ══════════════════════════ якорь предмета + верификатор ══════════════════════════

try:
    import pymorphy3
    _MORPH = pymorphy3.MorphAnalyzer()
except Exception:  # noqa: BLE001 — без pymorphy: префиксное совпадение (словоформа = форма + ≤2 символа)
    _MORPH = None

_TOK = re.compile(r"\w+")
_CYR = re.compile(r"[а-яёіїєґ]")


def _lemma(tok: str) -> str:
    if _MORPH is not None and _CYR.search(tok):
        return _MORPH.parse(tok)[0].normal_form
    return tok


def _lemmas(s: str) -> list[str]:
    return [_lemma(t) for t in _TOK.findall(_norm(s))]


def anchor_forms(cur: dict) -> list[list[str]]:
    """Все написания предмета и вариантов из самой карты (леммами) — списков в коде нет."""
    forms = [cur["subject"]] + cur["subject_aliases"]
    for v in cur["variants"]:
        forms += [v["name"]] + v["aliases"]
    out, seen = [], set()
    for f in forms:
        lem = _lemmas(f)
        if lem and tuple(lem) not in seen:
            seen.add(tuple(lem))
            out.append(lem)
    return out


def _tok_match(a: str, b: str) -> bool:
    if a == b:
        return True
    if _MORPH is None:   # префиксный запасной вариант: джип → джипа/джипов, но не джипами
        return len(a) >= 3 and b.startswith(a) and len(b) - len(a) <= 2
    return False


def _form_lists(names: list[str]) -> list[list[str]]:
    out, seen = [], set()
    for f in names:
        lem = _lemmas(f)
        if lem and tuple(lem) not in seen:
            seen.add(tuple(lem))
            out.append(lem)
    return out


def find_span(text: str, forms: list[list[str]]) -> tuple[int, int] | None:
    """Первое вхождение любого из написаний (леммами) — как срез символов исходной строки.
    Формы пробуются от короткой к длинной: «jeep» раньше «jeep usa», чтобы подстановка не съедала контекст."""
    toks = list(_TOK.finditer(_norm(text)))
    tl = [_lemma(m.group()) for m in toks]
    for f in sorted(forms, key=len):
        n = len(f)
        for i in range(len(tl) - n + 1):
            if all(_tok_match(f[j], tl[i + j]) for j in range(n)):
                return toks[i].start(), toks[i + n - 1].end()
    return None


def has_anchor(query: str, forms: list[list[str]]) -> bool:
    """Предмет назван: последовательность лемм одного из написаний внутри запроса."""
    return find_span(query, forms) is not None


_NUMS = re.compile(r"\d+")


def parse_yes(text: str, n: int) -> set[int] | None:
    """Номера «ДА» (1-based). None = ответ не разобран → голос не считается."""
    nums = {int(x) for x in _NUMS.findall(text)}
    nums = {x for x in nums if 1 <= x <= n}
    return nums if nums or _norm(text) in ("", "нет", "none", "0") else None


async def verify_templates(seed: str, region: str, items: list[str]) -> tuple[set[int], dict]:
    """Поток «без якоря»: VERIFY_VOTES параллельных вызовов, большинство. Ошибки/непарс — fail-open по этому голосу;
    если ни один голос не разобран — оставляем всё. → (индексы оставить, stats)."""
    numbered = "\n".join(f"{i + 1}. {t}" for i, t in enumerate(items))
    prompt = _fill(VERIFY_PROMPT, seed=seed, region=region, numbered=numbered)
    model, thinking = VERIFY
    t0 = time.perf_counter()
    calls = await asyncio.gather(*[call_model(model, prompt, thinking) for _ in range(VERIFY_VOTES)])
    votes: list[set[int]] = []
    for r in calls:
        if r["error"]:
            continue
        yes = parse_yes(r["text"], len(items))
        if yes is not None:
            votes.append({i - 1 for i in yes})
    if not votes:
        keep = set(range(len(items)))
    else:
        need = len(votes) // 2 + 1
        keep = {i for i in range(len(items)) if sum(i in v for v in votes) >= need}
    st = {"stage": "verify", "model": model, "thinking": thinking, "candidates": len(items), "kept": len(keep),
          "cut": len(items) - len(keep), "votes_valid": len(votes),
          "in": sum(r["in"] for r in calls), "out": sum(r["out"] for r in calls),
          "cost": round(sum(r["cost"] for r in calls), 5), "wall": round(time.perf_counter() - t0, 2),
          "error": (None if votes else ("; ".join(r["error"] for r in calls if r["error"]) or "все голоса не разобраны")),
          "vote_errors": [r["error"] for r in calls if r["error"]],
          "cut_items": [items[i] for i in range(len(items)) if i not in keep]}
    return keep, st


# ══════════════════════════ размножение шаблонов по осям (код) ══════════════════════════

def expand_map(cur: dict) -> tuple[list[dict], bool]:
    """Подстановка по осям. scope=variant: место предмета в запросе → каноническое имя каждого варианта;
    запрос, где вариант уже назван, не размножается. Города (im_0.5) и признаки не размножаются.
    Дедуп по тексту. → (интенты, упёрлись в потолок)."""
    out, seen = [], set()
    subj_forms = _form_lists([cur["subject"]] + cur["subject_aliases"])
    var_forms = _form_lists([n for v in cur["variants"] for n in [v["name"]] + v["aliases"]])
    cities = cur["cities"] or []
    city_forms = [(c, _form_lists([c])) for c in cities]

    def _norm_text(t: str) -> str:
        return _WS.sub(" ", t).strip()

    for g in cur["groups"]:
        for qd in g["queries"]:
            q = qd["q"]
            base = _norm(q)   # подстановка идёт в нормализованной строке (find_span считает срез по ней)
            # ── вариант
            rows: list[tuple[str, str]] = [(base, "")]
            if g["scope"] == "variant" and cur["variants"] and find_span(base, var_forms) is None:
                span = find_span(base, subj_forms)
                if span is not None:
                    a, b = span
                    rows = [(_norm_text(base[:a] + v["name"] + base[b:]), v["name"]) for v in cur["variants"]]
            # ── город: im_0.5 подстановка ОТКЛЮЧЕНА (Andrew: «одесса порт» → «Львов порт»; города — ось данных).
            #    Город в запросе только помечается (поле city), для отката — блок ниже.
            rows2: list[tuple[str, str, str]] = []
            for text, vname in rows:
                cspan, cname = None, ""
                for c, cf in city_forms:
                    cspan = find_span(text, cf)
                    if cspan is not None:
                        cname = c
                        break
                rows2.append((text, vname, cname if cspan is not None else ""))
                # ── im_0.4 (подстановка городов, откат):
                # if cspan is None or text[cspan[0]:cspan[1]] != _norm(cname):
                #     rows2.append((text, vname, cname if cspan is not None else ""))
                # else:
                #     a, b = cspan
                #     for c in cities:
                #         rows2.append((_norm_text(text[:a] + c + text[b:]), vname, c))
            for text, vname, cname in rows2:
                k = _norm(text)
                if not text or k in seen:
                    continue
                seen.add(k)
                out.append({"intent": text, "macro": g["macro"], "sub": g["sub"], "type": g["type"],
                            "scope": g["scope"], "source": q, "variant": vname, "city": cname,
                            "stage": qd["stage"], "by": qd["by"]})
                if len(out) >= MAX_EXPANDED:
                    return out, True
    return out, False


# ══════════════════════════ конвейер ══════════════════════════

class IntentReq(BaseModel):
    seed: str
    keywords: list           # VALID: строки или объекты с query/keyword
    country: str = ""
    language: str = ""
    city: str = ""
    notes: str = ""          # im_0.6: особенности ниши/региона от специалиста — контекст для моделей


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
    notes = _WS.sub(" ", req.notes.strip())
    ctx = {"seed": seed, "region": region or "не указан", "language": req.language or "ru",
           "notes": (f"\nОсобенности ниши и региона от специалиста (учитывай при построении карты): {notes}" if notes else ""),
           "keys": "\n".join(f"{i + 1}. {k}" for i, k in enumerate(keys))}
    keys_index = {_norm(k): i + 1 for i, k in enumerate(keys)}

    cur = empty_map()
    stages: list[dict] = []
    for i, (model, thinking) in enumerate(CHAIN, start=1):
        # пустая карта (первый проход или упавший первый проход) → полное построение
        mode = "build" if not cur["groups"] else "extend"
        prompt = _fill(FIRST_PROMPT, **ctx) if mode == "build" else _fill(EXTEND_PROMPT, **ctx, map=map_for_prompt(cur, keys_index))
        r = await call_model(model, prompt, thinking)
        err = r["error"]
        parsed = parse_json(r["text"]) if not err else None
        if not err and parsed is None:
            err = "parse: ответ не JSON"
        counts = merge_map(cur, parsed, i, model, keys) if parsed is not None else dict(_EMPTY_COUNTS)
        stages.append({
            "stage": i, "model": model, "thinking": thinking, "mode": mode,
            "added": counts, "groups_after": len(cur["groups"]),
            "queries_after": sum(len(g["queries"]) for g in cur["groups"]),
            "in": r["in"], "out": r["out"], "think": r["think"], "cost": r["cost"], "wall": r["wall"],
            "error": err, "raw": r["text"],
        })

    # ── якорь предмета: шаблон без слова предмета в группе без ключей → верификатор; 0 → удаляется
    forms = anchor_forms(cur)
    candidates: list[tuple[dict, dict]] = []
    n_anchor = n_keybacked = 0
    for g in cur["groups"]:
        for t in g["queries"]:
            if has_anchor(t["q"], forms):
                n_anchor += 1
            elif g["keys"]:
                n_keybacked += 1
            else:
                candidates.append((g, t))
    verify_stats = None
    if candidates:
        keep, verify_stats = await verify_templates(seed, ctx["region"], [t["q"] for _, t in candidates])
        drop = {id(t) for i, (_, t) in enumerate(candidates) if i not in keep}
        for g in cur["groups"]:
            g["queries"] = [t for t in g["queries"] if id(t) not in drop]
        cur["groups"] = [g for g in cur["groups"] if g["queries"] or g["keys"]]
        stages.append(verify_stats)

    intents, capped = expand_map(cur)
    total_cost = round(sum(s["cost"] for s in stages), 5)
    return {
        "seed": seed, "region": region, "language": req.language, "notes": notes, "keywords_in": len(keys),
        "map": cur,
        "intents": intents,
        "stages": stages,
        "stats": {
            "groups": len(cur["groups"]),
            "queries": sum(len(g["queries"]) for g in cur["groups"]),
            "journey": len(cur["journey"]),
            "variants": len(cur["variants"]),
            "attrs": sum(len(v["attrs"]) for v in cur["variants"]),
            "cities": len(cur["cities"]),
            "keys_assigned": sum(len(g["keys"]) for g in cur["groups"]),
            "anchor": n_anchor, "key_backed": n_keybacked,
            "verified": (verify_stats or {}).get("kept", 0), "cut": (verify_stats or {}).get("cut", 0),
            "intents_total": len(intents), "capped": capped,
            "total_cost": total_cost, "total_wall": round(time.perf_counter() - t0, 2),
            "errors": [s["model"] for s in stages if s["error"]],
        },
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


# ══════════════════════════ im_0.1 — плоский формат (точка отката, не вызывается) ══════════════════════════
# FIRST_PROMPT_01 = (
#     "Вот список ключевых слов, собранных из подсказок Google по запросу «{seed}».\n"
#     "Регион: {region}. Язык: {language}.\n"
#     "Составь по ним список поисковых интентов на основе этих ключей и дополни интентами, "
#     "которые есть в твоей базе знаний по этой теме, но в списке ключей не встретились.\n"
#     "Формат ответа: одна строка на интент — «интент | пример 1; пример 2» "
#     "(минимум 2 примера ключевых слов на интент). Без нумерации и пояснений.\n\n"
#     "Ключевые слова:\n{keys}"
# )
# EXTEND_PROMPT_01 = (
#     "Вот список ключевых слов, собранных из подсказок Google по запросу «{seed}».\n"
#     "Регион: {region}. Язык: {language}.\n\n"
#     "Ключевые слова:\n{keys}\n\n"
#     "Вот уже составленный по этим ключам список интентов:\n{intents}\n\n"
#     "Расширь его: добавь интенты, которые пропущены в этом списке — из ключей и из твоей базы знаний по этой теме.\n"
#     "Формат ответа: только НОВЫЕ интенты, одна строка на интент — «интент | пример 1; пример 2» "
#     "(минимум 2 примера ключевых слов на интент). Без нумерации и пояснений. "
#     "Если добавить нечего — ответь одним словом: нет."
# )
# Разбор строк «интент | пример 1; пример 2» (parse_intents) и merge_stage по имени интента — файл im_0.1 в git-истории.
