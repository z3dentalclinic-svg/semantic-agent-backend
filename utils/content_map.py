"""
content_map.py — карта контента (стадия B): дерево страниц из карты интентов (intent_map.py, стадия A).

Регистрация в main.py (две строки):
    from utils.content_map import router as content_map_router
    app.include_router(content_map_router)

POST /api/content-map
  {"seed": str, "region": str, "language": str, "map": <map из /api/intent-map>, "intents": <intents из /api/intent-map>}
  → {"nodes": [...], "assign": [...], "stats": {...}, "stage": {...}, "build": ...}

Правила Andrew (2026-09-14, вычитаны из jeep_content_map.xlsx), реализованы кодом:
  1. Вариант предмета → своя страница. Поколение (признак kind=поколение с периодом) → отдельная страница только
     у варианта с текущим поколением: текущее + предыдущее — страницы, старше — блоки на странице варианта.
     Вариант без текущего поколения (легаси) — одна страница. Год/имя поколения в запросе определяют страницу;
     запрос без них → текущее поколение.
  2. Общий этап (scope=common) → один генерик на тему; «вариант × общий этап» сворачивается в генерик, а не на
     страницу варианта. Сравнения (тип «сравнение», scope=variant) → страницы сравнений.
  3. Города → не страницы, а serviceArea (список городов у узла «локальный»).
  4. Язык → не ось страниц.
  5. Волны по счёту интентов+ключей на узел: хаб — волна 1; верхняя треть — 1; счёт ≤ 2 и легаси — 3; остальное — 2.
Модель (один вызов) нужна только для того, что кодом не сделать: слить дубли под-групп с разных проходов в темы
страниц, дать страницам имена и url-слаги. Падение вызова → fail-open: каждая common-подгруппа = своя страница.

cm_0.2 (Andrew, 2026-09-17): под-группы с общим спросом по предмету (запрос ≈ сид) → хаб (kind=hub в ответе модели);
  одиночная под-группа без ключей — не страница, присоединяется к соседней по этапу (в промпт передаётся число ключей);
  легаси-вариант: не волна 1; волна 3 только без ключей, с ключами — волна 2.

Модуль самодостаточен (не импортирует intent_map.py и minus_words_test.py).
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import date

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

BUILD = "cm_0.2"

MODELS: dict[str, dict] = {
    "gemini-3.8-flash": {"vendor": "gemini", "price": (0.75, 3.75)},
    "gemini-3.1-flash-lite": {"vendor": "gemini", "price": (0.10, 0.40)},
}
CONSOLIDATE: tuple[str, str] = ("gemini-3.8-flash", "low")   # слияние тем + имена/слаги
HTTP_TIMEOUT = 180
WAVE_TOP_SHARE = 1 / 3      # доля узлов (по счёту) в волне 1
WAVE_LOW_COUNT = 2          # счёт ≤ этого → волна 3

CONSOLIDATE_PROMPT = (
    "Сид: «{seed}». Регион: {region}. Язык: {language}.\n"
    "Ниже под-группы карты поисковых интентов по этой теме. У каждой — номер, тип и один типичный запрос.\n\n"
    "Собери из них страницы сайта. Одна страница = одна тема, которую раскрывает отдельная статья или раздел; "
    "под-группы об одном и том же (в том числе дубли с разными названиями) — на одну страницу. "
    "Под-группы с общим спросом по предмету сида (запрос — это сам сид или сид плюс «купить», «цена», «под ключ» "
    "и подобное без отдельной темы) — это главная страница: kind = hub, одна на всю карту. "
    "Под-группы типа «сравнение» — страницы сравнений (kind = compare), остальные — kind = generic. "
    "У каждой под-группы указано число реальных ключей (кл.): под-группа с 0 ключей и без соседей по теме — не "
    "отдельная страница, присоедини её к ближайшей по этапу пути клиента; отдельная страница — там, где есть ключи "
    "или несколько под-групп одной темы. Каждая под-группа должна попасть ровно на одну страницу. "
    "Название страницы — короткое, на языке {language}; slug — латиницей, через дефис.\n"
    "Отдельно дай слаги (латиницей, через дефис) для предмета сида, вариантов и поколений из списка ниже.\n\n"
    "Ответ — только JSON:\n"
    '{{"pages": [{{"name": "...", "slug": "...", "kind": "generic", "groups": [1, 5]}}],\n'
    ' "slugs": {{"<имя из списка>": "slug"}}}}\n\n'
    "Под-группы:\n{groups}\n\n"
    "Предмет, варианты и поколения:\n{names}"
)


# ══════════════════════════ вызов модели ══════════════════════════

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


_CALLERS = {"gemini": _call_gemini}


async def call_model(model: str, prompt: str, thinking: str) -> dict:
    meta = MODELS[model]
    t0 = time.perf_counter()
    try:
        res = await _CALLERS[meta["vendor"]](model, prompt, thinking)
        err = None
    except Exception as e:  # noqa: BLE001
        res, err = {"text": "", "in": 0, "out": 0, "think": 0}, f"{type(e).__name__}: {e}"
    pin, pout = meta["price"]
    res.update({"model": model, "thinking": thinking, "wall": round(time.perf_counter() - t0, 2),
                "cost": round((res["in"] * pin + res["out"] * pout) / 1_000_000, 5), "error": err})
    return res


def parse_json(text: str) -> dict | None:
    t = re.sub(r"^```[a-zA-Z]*\s*", "", text.strip())
    t = re.sub(r"\s*```$", "", t)
    a, b = t.find("{"), t.rfind("}")
    if a < 0 or b <= a:
        return None
    try:
        d = json.loads(t[a:b + 1])
    except json.JSONDecodeError:
        return None
    return d if isinstance(d, dict) else None


# ══════════════════════════ текст: леммы, поиск форм ══════════════════════════

try:
    import pymorphy3
    _MORPH = pymorphy3.MorphAnalyzer()
except Exception:  # noqa: BLE001
    _MORPH = None

_WS = re.compile(r"\s+")
_TOK = re.compile(r"\w+")
_CYR = re.compile(r"[а-яёіїєґ]")
_YEAR = re.compile(r"(?<!\d)(19[89]\d|20[0-4]\d)(?!\d)")
_NOW = re.compile(r"н\.?\s?в\.?|наст|now|present|сейчас|по\s?сей", re.I)


def _norm(s) -> str:
    return _WS.sub(" ", str(s if s is not None else "").strip().strip('"«»\'').lower())


def _lemma(tok: str) -> str:
    if _MORPH is not None and _CYR.search(tok):
        return _MORPH.parse(tok)[0].normal_form
    return tok


def _lemmas(s: str) -> list[str]:
    return [_lemma(t) for t in _TOK.findall(_norm(s))]


def _forms(names: list[str]) -> list[list[str]]:
    out, seen = [], set()
    for f in names:
        lem = _lemmas(f)
        if lem and tuple(lem) not in seen:
            seen.add(tuple(lem))
            out.append(lem)
    return out


def _tok_match(a: str, b: str) -> bool:
    if a == b:
        return True
    if _MORPH is None:
        return len(a) >= 3 and b.startswith(a) and len(b) - len(a) <= 2
    return False


def contains(text: str, forms: list[list[str]]) -> bool:
    tl = _lemmas(text)
    for f in forms:
        n = len(f)
        for i in range(len(tl) - n + 1):
            if all(_tok_match(f[j], tl[i + j]) for j in range(n)):
                return True
    return False


_TRANSLIT = dict(zip("абвгдеёжзийклмнопрстуфхцчшщъыьэюяіїєґ",
                     ["a", "b", "v", "g", "d", "e", "e", "zh", "z", "i", "y", "k", "l", "m", "n", "o", "p", "r", "s", "t",
                      "u", "f", "h", "ts", "ch", "sh", "sch", "", "y", "", "e", "yu", "ya", "i", "yi", "ye", "g"]))


def slugify(s: str) -> str:
    """Запасной слаг, если модель не дала: транслит кириллицы посимвольно, латиница/цифры, остальное — дефис."""
    t = "".join(_TRANSLIT.get(ch, ch) for ch in _norm(s))
    t = re.sub(r"[^a-z0-9]+", "-", t.encode("ascii", "ignore").decode()).strip("-")
    return t or "page"


# ══════════════════════════ поколения ══════════════════════════

def period_years(period: str) -> tuple[int | None, int | None, bool]:
    """«2011–2021» → (2011, 2021, False); «2022-н.в.» → (2022, None, True). Без чисел → (None, None, False)."""
    ys = [int(y) for y in _YEAR.findall(period or "")]
    current = bool(_NOW.search(period or ""))
    if not ys:
        return None, None, current
    start = min(ys)
    end = None if current else (max(ys) if len(ys) > 1 else None)
    if not current and len(ys) == 1:
        end = None   # «2019» одиночный год — открытый конец не считаем текущим
    return start, end, current


def generations(variant: dict) -> list[dict]:
    """Поколения варианта (kind=поколение с периодом), по возрасту: новейшее первым."""
    gens = []
    for a in variant.get("attrs", []):
        if _norm(a.get("kind")) not in ("поколение", "generation"):
            continue
        start, end, cur = period_years(a.get("period", ""))
        if start is None:
            continue
        gens.append({"name": a["name"], "period": a.get("period", ""), "start": start, "end": end, "current": cur})
    gens.sort(key=lambda g: (g["current"], g["start"]), reverse=True)
    return gens


# ══════════════════════════ дерево ══════════════════════════

class ContentReq(BaseModel):
    seed: str
    region: str = ""
    language: str = ""
    map: dict
    intents: list


def _node(nid: str, kind: str, name: str, parent: str | None, **extra) -> dict:
    d = {"id": nid, "kind": kind, "name": name, "parent": parent, "slug": "", "variant": "", "generation": "",
         "period": "", "blocks": [], "wave": 2, "count": 0, "keys": 0, "intents": [], "cities": []}
    d.update(extra)
    return d


def build_skeleton(seed: str, m: dict) -> tuple[list[dict], dict]:
    """Хаб + страницы вариантов/поколений (правило 1). → (узлы, индекс вариант → {'node', 'gens': {gen_name: node_id}, 'default'})."""
    nodes = [_node("hub", "hub", seed, None)]
    vindex: dict[str, dict] = {}
    for i, v in enumerate(m.get("variants", [])):
        vid = f"v{i}"
        gens = generations(v)
        has_current = any(g["current"] for g in gens)
        entry = {"node": vid, "gens": {}, "default": vid, "legacy": not has_current and bool(gens)}
        if has_current and len(gens) >= 2:
            # текущее + предыдущее — страницы; старше — блоки на странице варианта
            nodes.append(_node(vid, "variant", v["name"], "hub", variant=v["name"]))
            for k, g in enumerate(gens):
                if k < 2:
                    gid = f"{vid}g{k}"
                    nodes.append(_node(gid, "generation", f"{v['name']} {g['name']}", vid, variant=v["name"],
                                       generation=g["name"], period=g["period"]))
                    entry["gens"][g["name"]] = gid
                else:
                    nodes[[n["id"] for n in nodes].index(vid)]["blocks"].append(f"{g['name']} {g['period']}".strip())
            entry["default"] = entry["gens"][gens[0]["name"]]   # запрос без поколения → текущее
        else:
            label = v["name"] if not gens else f"{v['name']} ({gens[0]['name']}{', ' + gens[0]['period'] if gens[0]['period'] else ''})"
            nd = _node(vid, "variant", label, "hub", variant=v["name"],
                       generation=gens[0]["name"] if gens else "", period=gens[0]["period"] if gens else "")
            nd["blocks"] = [f"{g['name']} {g['period']}".strip() for g in gens[1:]]
            nodes.append(nd)
            for g in gens:
                entry["gens"][g["name"]] = vid
        vindex[_norm(v["name"])] = entry
        for a in v.get("aliases", []):
            vindex.setdefault(_norm(a), entry)
    return nodes, vindex


def resolve_generation(text: str, variant: dict, entry: dict) -> str:
    """Страница варианта для запроса: имя поколения в тексте → его узел; год в периоде → его узел; иначе — default."""
    if not entry["gens"]:
        return entry["default"]
    gens = generations(variant)
    for g in gens:
        if contains(text, _forms([g["name"]])):
            return entry["gens"].get(g["name"], entry["default"])
    for y in (int(x) for x in _YEAR.findall(text)):
        for g in gens:
            if g["start"] <= y and (g["end"] is None or y <= g["end"]):
                return entry["gens"].get(g["name"], entry["default"])
    return entry["default"]


async def consolidate(seed: str, region: str, language: str, groups: list[dict], m: dict) -> tuple[dict | None, dict]:
    """Один вызов модели: common-подгруппы → страницы (слияние дублей), слаги. → (parsed|None, stats)."""
    lines = []
    for i, g in enumerate(groups):
        q = g["queries"][0]["q"] if g.get("queries") else ""
        lines.append(f"{i + 1}. [{g.get('type', '')}, {len(g.get('keys', []))} кл.] {g['macro']} → {g['sub']}: {q}")
    names = [m.get("subject") or seed] + [v["name"] for v in m.get("variants", [])]
    for v in m.get("variants", []):
        names += [f"{v['name']} {g['name']}" for g in generations(v)]
    prompt = CONSOLIDATE_PROMPT
    for k, v in {"seed": seed, "region": region or "не указан", "language": language or "ru",
                 "groups": "\n".join(lines), "names": "\n".join(names)}.items():
        prompt = prompt.replace("{" + k + "}", v)
    prompt = prompt.replace("{{", "{").replace("}}", "}")
    model, thinking = CONSOLIDATE
    r = await call_model(model, prompt, thinking)
    parsed = parse_json(r["text"]) if not r["error"] else None
    err = r["error"] or (None if parsed is not None else "parse: ответ не JSON")
    st = {"model": model, "thinking": thinking, "in": r["in"], "out": r["out"], "cost": r["cost"],
          "wall": r["wall"], "error": err, "raw": r["text"]}
    return (parsed if err is None else None), st


def waves(nodes: list[dict], vindex: dict) -> None:
    legacy_nodes = {e["node"] for e in vindex.values() if e["legacy"]}
    leaves = [n for n in nodes if n["kind"] != "hub" and not (n["kind"] == "variant" and any(c["parent"] == n["id"] for c in nodes))]
    ranked = sorted(leaves, key=lambda n: (n["keys"], n["count"]), reverse=True)   # реальные ключи — главный сигнал спроса
    top = max(1, int(len(ranked) * WAVE_TOP_SHARE))
    for k, n in enumerate(ranked):
        legacy = n["id"] in legacy_nodes or n["parent"] in legacy_nodes
        if n["count"] <= WAVE_LOW_COUNT or (legacy and n["keys"] == 0):
            n["wave"] = 3
        elif k < top and not legacy:          # cm_0.2: легаси не выше волны 2
            n["wave"] = 1
        else:
            n["wave"] = 2
    for n in nodes:
        if n["kind"] == "hub":
            n["wave"] = 1
        elif n["kind"] == "variant":
            kids = [c["wave"] for c in nodes if c["parent"] == n["id"]]
            if kids:
                n["wave"] = min(kids)   # родитель-вариант — заголовок над страницами поколений


async def run_content_map(req: ContentReq) -> dict:
    t0 = time.perf_counter()
    m, intents = req.map, req.intents
    seed = _WS.sub(" ", req.seed.strip())
    groups = m.get("groups", [])
    nodes, vindex = build_skeleton(seed, m)
    variants_by_name = {_norm(v["name"]): v for v in m.get("variants", [])}
    var_forms_all = _forms([n for v in m.get("variants", []) for n in [v["name"]] + v.get("aliases", [])])

    # ── темы генериков и сравнений — модель; fail-open: каждая common-подгруппа = страница
    common_idx = [i for i, g in enumerate(groups) if g.get("scope") == "common" or _norm(g.get("type")) == "сравнение"]
    parsed, st = await consolidate(seed, req.region, req.language, groups, m)
    by_hub = nodes[0]
    group_node: dict[int, str] = {}
    slugs: dict[str, str] = {}
    if parsed:
        slugs = {_norm(k): str(v) for k, v in (parsed.get("slugs") or {}).items() if isinstance(v, str)}
        for p_i, pg in enumerate(parsed.get("pages") or []):
            if not isinstance(pg, dict):
                continue
            kind = _norm(pg.get("kind"))
            kind = kind if kind in ("compare", "hub") else "generic"
            if kind == "hub":                                   # cm_0.2: общий спрос по предмету → хаб
                nd, nid = by_hub, "hub"
            else:
                nid = f"p{p_i}"
                nd = _node(nid, kind, str(pg.get("name") or f"Страница {p_i + 1}"), "hub", slug=str(pg.get("slug") or ""))
            members = []
            for n in pg.get("groups") or []:
                try:
                    gi = int(n) - 1
                except (TypeError, ValueError):
                    continue
                if 0 <= gi < len(groups) and gi not in group_node:
                    group_node[gi] = nid
                    members.append(gi)
            if members:
                nd["groups"] = nd.get("groups", []) + [f"{groups[gi]['macro']} → {groups[gi]['sub']}" for gi in members]
                if kind != "hub":
                    nodes.append(nd)
    # под-группы, которые модель не разложила (или вызов упал) — каждая своей страницей
    for gi in common_idx:
        if gi not in group_node:
            g = groups[gi]
            nid = f"g{gi}"
            kind = "compare" if _norm(g.get("type")) == "сравнение" else "generic"
            nodes.append(_node(nid, kind, g["sub"], "hub", groups=[f"{g['macro']} → {g['sub']}"]))
            group_node[gi] = nid
    by_id = {n["id"]: n for n in nodes}

    # ── слаги
    for n in nodes:
        if n["slug"]:
            continue
        if n["kind"] == "generation":
            n["slug"] = slugs.get(_norm(f"{n['variant']} {n['generation']}")) or \
                (slugs.get(_norm(n["variant"]), slugify(n["variant"])) + "-" + slugify(n["generation"]))
        elif n["kind"] == "variant":
            n["slug"] = slugs.get(_norm(n["variant"])) or slugify(n["variant"])
        else:
            n["slug"] = slugs.get(_norm(n["name"])) or slugify(n["name"])
    by_id["hub"]["slug"] = slugs.get(_norm(m.get("subject") or seed)) or by_id["hub"]["slug"] or slugify(seed)

    # ── распределение: интенты (размноженные запросы) и реальные ключи
    assign: list[dict] = []
    group_index = {(_norm(g["macro"]), _norm(g["sub"])): i for i, g in enumerate(groups)}

    def place(text: str, gi: int | None, variant_name: str, source: str, macro: str, sub: str, typ: str) -> None:
        g = groups[gi] if gi is not None else None
        scope = g.get("scope") if g else "common"
        gtype = _norm(g.get("type")) if g else ""
        nid = None
        if gi is not None and (scope == "common" or gtype == "сравнение"):
            nid = group_node.get(gi)                       # правило 2: общий этап / сравнение → генерик, даже с вариантом
        if nid is None:
            vn = _norm(variant_name)
            entry = vindex.get(vn)
            if entry is None:                               # вариант не передан — ищем в тексте
                for name, e in vindex.items():
                    if contains(text, _forms([name])):
                        entry, vn = e, name
                        break
            if entry is not None:
                v = variants_by_name.get(_norm(m["variants"][int(entry["node"][1:])]["name"]))
                nid = resolve_generation(text, v, entry)  # правило 1: поколение по имени/году, иначе текущее
            elif gi is not None and group_node.get(gi):
                nid = group_node[gi]
            else:
                nid = "hub"
        node = by_id.get(nid, by_id["hub"])
        node["count"] += 1
        if source == "key":
            node["keys"] += 1
        node["intents"].append({"intent": text, "source": source, "macro": macro, "sub": sub, "type": typ})
        assign.append({"intent": text, "source": source, "macro": macro, "sub": sub, "type": typ,
                       "node": node["id"], "node_name": node["name"], "slug": node["slug"]})

    for gi, g in enumerate(groups):
        for k in g.get("keys", []):
            vn = next((name for name in vindex if contains(k, _forms([name]))), "")
            place(k, gi, vn, "key", g["macro"], g["sub"], g.get("type", ""))
    for x in intents:
        gi = group_index.get((_norm(x.get("macro")), _norm(x.get("sub"))))
        place(x.get("intent", ""), gi, x.get("variant", ""), f"проход {x.get('stage', '')}", x.get("macro", ""),
              x.get("sub", ""), x.get("type", ""))

    # ── правило 3: города → serviceArea на узлах локальных под-групп
    cities = m.get("cities", [])
    for n in nodes:
        if any(_norm(t) == "локальный" for t in (groups[gi].get("type", "") for gi in group_node if group_node[gi] == n["id"])):
            n["cities"] = cities

    # ── узлы без единого интента — не страницы
    nodes = [n for n in nodes if n["kind"] in ("hub", "variant", "generation") or n["count"] > 0]
    waves(nodes, vindex)

    return {
        "seed": seed, "nodes": nodes, "assign": assign,
        "stats": {"nodes": len(nodes), "hub": 1,
                  "variant": sum(n["kind"] == "variant" for n in nodes),
                  "generation": sum(n["kind"] == "generation" for n in nodes),
                  "generic": sum(n["kind"] == "generic" for n in nodes),
                  "compare": sum(n["kind"] == "compare" for n in nodes),
                  "assigned": len(assign), "keys": sum(a["source"] == "key" for a in assign),
                  "wave1": sum(n["wave"] == 1 for n in nodes), "wave2": sum(n["wave"] == 2 for n in nodes),
                  "wave3": sum(n["wave"] == 3 for n in nodes),
                  "total_cost": st["cost"], "total_wall": round(time.perf_counter() - t0, 2),
                  "errors": [st["error"]] if st["error"] else []},
        "stage": st, "build": BUILD,
    }


router = APIRouter()


@router.post("/api/content-map")
async def content_map_endpoint(req: ContentReq):
    if not req.seed.strip():
        return JSONResponse({"error": "seed пустой"}, status_code=400)
    if not req.map or not req.map.get("groups"):
        return JSONResponse({"error": "карта интентов пустая — сначала «Список интентов»"}, status_code=400)
    return await run_content_map(req)


@router.get("/api/content-map/models")
async def content_map_models():
    return {"consolidate": {"model": CONSOLIDATE[0], "thinking": CONSOLIDATE[1]}, "build": BUILD}
