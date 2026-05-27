"""
L1.5 v3 — TRASH filter (двухосевая логика).

КОНЦЕПЦИЯ:
- Default = TRASH
- Spasaem v GREY если ДОКАЗАНЫ ОБЕ оси: action И object
- Никогда не возвращает VALID — это работа L2/L3
- Для одноосевого seed (только object или только action) — одной оси достаточно

ОСИ:
- AXIS_OBJECT: substring | прямая лемма | RuWordNet synonym | E5 hyponym (cos≥0.78 + в neighbors)
- AXIS_ACTION: substring | прямая лемма | RuWordNet synonym | E5 synonym (cos≥0.85)

Двойной фильтр для гипонимов (cos + neighbors) спасает от false friends:
- роза→цвет: cos 0.8, есть в L0_VALID neighbors → hyponym ✓
- подставка→доставка: cos 0.86, но не в neighbors → false friend ✗
- пятёрочка→цвет: cos 0.55, не в neighbors → false friend ✗

LONG SEEDS (3+ content_lemmas):
Все content_lemmas обязательны. action_anchor + object_anchor через все методы,
"прочие" content_lemmas (например, geo `буковель`) — только substring/lemma/synonym.
"""

import functools
import logging
import re
from typing import List, Dict, Set, Tuple, Optional, Any
from collections import Counter

logger = logging.getLogger(__name__)

# ─── pymorphy3 ───────────────────────────────────────────────────────────
try:
    import pymorphy3
    _morph = pymorphy3.MorphAnalyzer()
except Exception as e:
    _morph = None
    logger.error(f"[L1.5/v3] pymorphy3 not available: {e}")

# ─── RuWordNet (REMOVED) ─────────────────────────────────────────────────
# Раньше здесь был download БД RuWordNet с GitHub + инициализация ORM.
# Удалено: теперь все 4 RWN-функции (_get_synonyms, _get_cohyponyms,
# _has_verb_derivation, _count_hyponyms) читают данные из prebuilt index'ов
# `rwn_*.json.gz` (см. блок PREBUILT RWN INDEX ниже).
#
# Преимущества удаления:
#   - -100-200 MB RAM (нет sqlite ORM в памяти)
#   - -1-2s startup (нет download/init/sanity-check)
#   - -1 зависимость в requirements.txt (ruwordnet больше не нужен)
#   - Чище код (нет fallback-веток `if _rwn is None`)
#
# Файлы индексов собираются скриптом `build_rwn_index.py` локально — он
# использует ruwordnet ОДИН РАЗ при сборке. В проде ruwordnet не нужен.


# ─── PREBUILT RWN INDEX — мгновенный dict-lookup вместо RWN walks ────────
# Если рядом с проектом лежат .json.gz файлы со предкомпьюченными индексами
# (созданными скриптом build_rwn_index.py), грузим их в RAM и используем
# для всех RWN-запросов вместо ORM-обхода SQLite.
#
# Цель: убить ~18s RWN-нагрузки с холодного прогона.
# Старая RWN-логика остаётся как fallback если файлов нет / битые / отключено.
#
# Файлы (~12 MB суммарно):
#   rwn_synonyms.json.gz     — {лемма: [синонимы]}
#   rwn_cohyponyms.json.gz   — {лемма: [cohyponyms]}  (с лимитом 50)
#   rwn_meta.json.gz         — {лемма: {hypo_count: N, has_verb_derivation: bool}}

import gzip as _gzip
import json as _json
import os as _os

_USE_PREBUILT_INDEX = False
_SYNONYMS_INDEX: Dict[str, List[str]] = {}
_COHYPONYMS_INDEX: Dict[str, List[str]] = {}
_META_INDEX: Dict[str, Dict[str, Any]] = {}


def _find_index_dir() -> Optional[str]:
    """Ищет папку с rwn_*.json.gz файлами по нескольким стандартным местам."""
    # __file__ = .../filters/l1_5_filter_v2.py → родительская папка = корень проекта
    _filter_dir = _os.path.dirname(_os.path.abspath(__file__))
    _project_root = _os.path.dirname(_filter_dir)
    candidates = [
        _project_root,                                # /opt/render/project/src
        _filter_dir,                                  # /opt/render/project/src/filters
        _os.getcwd(),                                 # текущая директория процесса
        _os.path.join(_project_root, "data"),         # на случай если положишь в /data
    ]
    for d in candidates:
        path = _os.path.join(d, "rwn_synonyms.json.gz")
        if _os.path.exists(path):
            return d
    return None


def _load_prebuilt_indexes() -> bool:
    """Грузит 3 .json.gz файла в глобальные dict'ы. True если всё ок."""
    global _SYNONYMS_INDEX, _COHYPONYMS_INDEX, _META_INDEX
    index_dir = _find_index_dir()
    if not index_dir:
        logger.info("[L1.5/v3] Prebuilt RWN index files NOT FOUND → fallback to RWN-walk mode")
        return False

    import time as _time
    t0 = _time.perf_counter()
    try:
        with _gzip.open(_os.path.join(index_dir, "rwn_synonyms.json.gz"), "rt", encoding="utf-8") as f:
            _SYNONYMS_INDEX = _json.load(f)
        with _gzip.open(_os.path.join(index_dir, "rwn_cohyponyms.json.gz"), "rt", encoding="utf-8") as f:
            _COHYPONYMS_INDEX = _json.load(f)
        with _gzip.open(_os.path.join(index_dir, "rwn_meta.json.gz"), "rt", encoding="utf-8") as f:
            _META_INDEX = _json.load(f)
    except Exception as e:
        logger.warning(f"[L1.5/v3] Prebuilt index load FAILED: {e} → fallback to RWN-walk mode")
        _SYNONYMS_INDEX = {}
        _COHYPONYMS_INDEX = {}
        _META_INDEX = {}
        return False

    dt = _time.perf_counter() - t0
    logger.info(
        f"[L1.5/v3] Prebuilt RWN index loaded from {index_dir} in {dt:.2f}s "
        f"(synonyms={len(_SYNONYMS_INDEX)}, cohyponyms={len(_COHYPONYMS_INDEX)}, "
        f"meta={len(_META_INDEX)})"
    )
    return True


_USE_PREBUILT_INDEX = _load_prebuilt_indexes()


# ─── E5 model — НЕ молча глотаем ошибки импорта ─────────────────────────
import sys as _sys

def _filter_diag(msg: str) -> None:
    """Диагностический print для startup-логов (logger.info на module-level
    может уходить в никуда т.к. uvicorn ещё не настроил logging)."""
    print(f"[L1.5/DIAG] {msg}", file=_sys.stderr, flush=True)


_E5_IMPORT_OK = False
get_e5_word_embedding = None
e5_cosine_sim = None
get_e5_model = None
warm_e5_word_cache = None

_filter_diag("starting e5_model import")
try:
    from .e5_model import (
        get_e5_word_embedding as _gee,
        e5_cosine_sim as _ecs,
        get_e5_model as _gem,
        warm_e5_word_cache as _wwc,
    )
    _filter_diag(f"relative import: _gee={_gee}, _gem={_gem}, _ecs={_ecs}, _wwc={_wwc}")
    # SANITY: все 4 имени должны быть вызываемыми. Если хоть одно None —
    # импорт частично сломан, fallback не сработал, raise чтобы явно увидеть.
    assert callable(_gee), f"get_e5_word_embedding is not callable: {_gee!r}"
    assert callable(_ecs), f"e5_cosine_sim is not callable: {_ecs!r}"
    assert callable(_gem), f"get_e5_model is not callable: {_gem!r}"
    assert callable(_wwc), f"warm_e5_word_cache is not callable: {_wwc!r}"
    get_e5_word_embedding = _gee
    e5_cosine_sim = _ecs
    get_e5_model = _gem
    warm_e5_word_cache = _wwc
    _E5_IMPORT_OK = True
    _filter_diag("E5 module imported via relative path, all callables OK")
except Exception as e_rel:
    _filter_diag(f"relative import failed: {type(e_rel).__name__}: {e_rel}")
    try:
        from e5_model import (
            get_e5_word_embedding as _gee,
            e5_cosine_sim as _ecs,
            get_e5_model as _gem,
            warm_e5_word_cache as _wwc,
        )
        _filter_diag(f"absolute import: _gee={_gee}, _gem={_gem}, _ecs={_ecs}, _wwc={_wwc}")
        assert callable(_gee), f"get_e5_word_embedding is not callable: {_gee!r}"
        assert callable(_ecs), f"e5_cosine_sim is not callable: {_ecs!r}"
        assert callable(_gem), f"get_e5_model is not callable: {_gem!r}"
        assert callable(_wwc), f"warm_e5_word_cache is not callable: {_wwc!r}"
        get_e5_word_embedding = _gee
        e5_cosine_sim = _ecs
        get_e5_model = _gem
        warm_e5_word_cache = _wwc
        _E5_IMPORT_OK = True
        _filter_diag("E5 module imported via absolute path, all callables OK")
    except Exception as e_abs:
        _filter_diag(f"absolute import also failed: {type(e_abs).__name__}: {e_abs}")
        _filter_diag("E5 unavailable — fallback to no-semantic mode (substring/lemma/RWN only)")

        def get_e5_word_embedding(w):
            return None

        def e5_cosine_sim(a, b):
            return 0.0

        def get_e5_model():
            return None

        def warm_e5_word_cache(words, batch_size=64):
            return 0

# Финальный assert после всего: даже после fallback функции должны быть вызываемыми
assert callable(get_e5_word_embedding), \
    f"FATAL: get_e5_word_embedding is None after import block. _E5_IMPORT_OK={_E5_IMPORT_OK}"
assert callable(get_e5_model), \
    f"FATAL: get_e5_model is None after import block. _E5_IMPORT_OK={_E5_IMPORT_OK}"
_filter_diag(f"final state: _E5_IMPORT_OK={_E5_IMPORT_OK}, all E5 callables OK")

# ─── Тюнинг — пороги привязаны к backend модели ──────────────────────────
# Распределение cos зависит от модели → пороги тоже зависят. При смене
# EMBEDDING_BACKEND в e5_model.py пороги переключаются автоматически.
#
# MIN_OBJECT_LEMMA_LEN от backend НЕ зависит (это длина строки).
#
# При миграции на новый backend:
#   1. Стартуем с тех же значений что у предыдущего backend
#   2. Прогон → смотрим distribution cos и регрессии в counts
#   3. Калибруем пороги по контрольным парам (см. MIGRATION_TO_MINILM.md §4)

_THRESHOLDS_BY_BACKEND = {
    "e5_large": {
        # Откалибровано на baseline 'доставка цветов' / 'купить айфон 16'
        "COS_OBJECT_HIGH":             0.78,
        "COS_ACTION_HIGH":             0.87,
        "COS_GAP_MIN":                 0.05,
        "COS_QUALIFIER_NUMERIC_HIGH":  0.82,
    },
    "e5_small": {
        # СТАРТ: те же значения для прогона baseline на новой модели.
        # После прогона на 'доставка цветов' / 'купить айфон 16' калибруем
        # под фактическое распределение cos в e5-small. Ожидаемое смещение
        # ~0.02-0.05 от значений e5-large (то же семейство, но меньше параметров).
        "COS_OBJECT_HIGH":             0.78,
        "COS_ACTION_HIGH":             0.87,
        "COS_GAP_MIN":                 0.05,
        "COS_QUALIFIER_NUMERIC_HIGH":  0.82,
    },
}

# Импортируем активный backend из e5_model (одна точка истины)
try:
    from .e5_model import EMBEDDING_BACKEND as _ACTIVE_BACKEND
except Exception:
    try:
        from e5_model import EMBEDDING_BACKEND as _ACTIVE_BACKEND
    except Exception:
        _ACTIVE_BACKEND = "e5_large"  # fallback на дефолт
        logger.warning(
            "[L1.5/v3] Could not import EMBEDDING_BACKEND from e5_model, "
            "falling back to e5_large thresholds"
        )

if _ACTIVE_BACKEND not in _THRESHOLDS_BY_BACKEND:
    logger.warning(
        f"[L1.5/v3] Unknown backend {_ACTIVE_BACKEND!r}, falling back to e5_large thresholds"
    )
    _ACTIVE_BACKEND = "e5_large"

_T = _THRESHOLDS_BY_BACKEND[_ACTIVE_BACKEND]
logger.info(f"[L1.5/v3] Active embedding backend: {_ACTIVE_BACKEND}, thresholds: {_T}")

COS_OBJECT_HIGH            = _T["COS_OBJECT_HIGH"]    # порог cos для гипонимов object (метод 4, с neighbors+gap)
COS_ACTION_HIGH            = _T["COS_ACTION_HIGH"]    # порог cos для синонимов action (метод 4, без neighbors)
COS_GAP_MIN                = _T["COS_GAP_MIN"]        # мин разница (cos_obj - cos_act) в методе 4 object
COS_QUALIFIER_NUMERIC_HIGH = _T["COS_QUALIFIER_NUMERIC_HIGH"]  # порог cos '16' ↔ 'шестнадцатый'

MIN_OBJECT_LEMMA_LEN = 3  # минимальная длина леммы object-кандидата в методе 4.
                          # Защита от предлогов-омонимов: pymorphy парсит 'в'/'с'
                          # как буквы алфавита NOUN → попадали в кандидаты.
                          # От backend НЕ зависит.
NEIGHBOR_WINDOW = 2
NEIGHBOR_MIN_FREQ = 2

# Non-content POS (фильтруем при extraction content_lemmas)
_NON_CONTENT_POS = {
    'PREP', 'CONJ', 'PRCL', 'INTJ',
    'ADVB', 'COMP', 'NUMR', 'NPRO',
}

# Global parses cache (uniq tokens per request)
_parses_cache: Dict[str, Any] = {}
_all_parses_cache: Dict[str, List[Any]] = {}


# ─── Утилиты ─────────────────────────────────────────────────────────────

def _is_content_word(parse) -> bool:
    """POS-фильтр без хардкод-списка стопвордов."""
    pos = parse.tag.POS
    if not pos or pos in _NON_CONTENT_POS:
        return False
    if 'Apro' in str(parse.tag):  # местоименные прилагательные (весь/тот/мой/свой)
        return False
    return True


def _tokenize(text: str) -> List[str]:
    return re.findall(r'[a-zа-яёіїєґ0-9]+', text.lower())


def _parse_top(word: str):
    """Top pymorphy3 parse с кешированием."""
    if _morph is None:
        return None
    if word in _parses_cache:
        return _parses_cache[word]
    parses = _morph.parse(word)
    p = parses[0] if parses else None
    _parses_cache[word] = p
    return p


def _parse_all(word: str) -> List[Any]:
    """Все pymorphy парсы слова. Нужно для устойчивости к омонимам:
    например 'цветов' имеет парсы 'цвет' (окраска) и 'цветок' (растение)."""
    if _morph is None:
        return []
    if word in _all_parses_cache:
        return _all_parses_cache[word]
    parses = _morph.parse(word) or []
    _all_parses_cache[word] = parses
    return parses


def _token_lemmas(word: str, pos_filter: Optional[Set[str]] = None) -> Set[str]:
    """Все возможные normal_form для токена, с опциональным POS-фильтром.

    Решает проблему омонимов: 'цветов' -> {'цвет', 'цветок'}. Все сравнения
    лемм должны идти через эту функцию, не через _parse_top().normal_form.
    """
    out: Set[str] = set()
    for p in _parse_all(word):
        if p.normal_form is None:
            continue
        if pos_filter is None or p.tag.POS in pos_filter:
            out.add(p.normal_form)
    return out


@functools.lru_cache(maxsize=1024)
def _get_synonyms(lemma: str) -> Set[str]:
    """Лексика близкая к лемме из RuWordNet (через prebuilt index).

    Включает (через build_rwn_index.py):
    1. Synonyms — леммы того же synset (доставка ↔ привоз)
    2. Hyponyms — дочерние synset (цветок → роза, тюльпан, букет, эустома)
    3. POS-synonyms — мост между частями речи (доставка ↔ доставлять)
    4. Derivations — словообразовательные пары на уровне sense
       (доставлять → доставка, доставщик)

    Если prebuilt index не загружен — возвращает пустой set (фильтр работает
    без RWN-методов через E5+substring+lemma).
    """
    if not _USE_PREBUILT_INDEX or not lemma:
        return set()
    return set(_SYNONYMS_INDEX.get(lemma.lower(), []))


@functools.lru_cache(maxsize=2048)
def _has_verb_derivation(lemma: str) -> bool:
    """Есть ли у леммы VERB/INFN-производное в RuWordNet (через prebuilt index)?

    Используется для определения action-noun: отглагольные существительные
    (доставка, ремонт, продажа) имеют verb-counterpart, природные предметы
    (цветок, квартира) — нет.

    Если prebuilt index не загружен — возвращает False (caller использует
    fallback в логике определения action-anchor).
    """
    if not _USE_PREBUILT_INDEX or not lemma:
        return False
    meta = _META_INDEX.get(lemma.lower())
    return bool(meta and meta.get("has_verb_derivation", False))


# Лимит размера cohyponym-набора. Если у object_anchor через ближайший
# hypernym с 2 уровнями вниз набирается БОЛЬШЕ COHYPONYM_MAX_SIZE лемм —
# таксономия слишком общая и расширение опасно (для 'цветок' → 'растение'
# попадают деревья/кустарники/травы, 566 лемм). Защита baseline.
# Узкие таксономии типа 'скутер' (36 лемм через 'мототранспортное средство')
# проходят и спасают семантически родственные термины (мопед, байк, мотоцикл).
COHYPONYM_MAX_SIZE = 50


@functools.lru_cache(maxsize=1024)
def _get_cohyponyms(lemma: str) -> Set[str]:
    """Co-hyponyms (семантические братья) леммы из prebuilt RWN index.

    Через build_rwn_index.py:
      - Поднимаемся на 1 уровень к hypernym'у
      - Опускаемся на 2 уровня вниз
      - Если набор > COHYPONYM_MAX_SIZE — пустой (защита от широких таксономий)

    Если prebuilt index не загружен — пустой set (cohyponym-метод выключен,
    остаются substring/lemma/synonym/E5 как fallback).
    """
    if not _USE_PREBUILT_INDEX or not lemma:
        return set()
    return set(_COHYPONYMS_INDEX.get(lemma.lower(), []))


# ─── Разбор seed ─────────────────────────────────────────────────────────

@functools.lru_cache(maxsize=1024)
def _count_hyponyms(lemma: str) -> int:
    """Суммарное число лемм-hyponyms у леммы (через prebuilt index).

    Возвращаемые значения:
        -1: леммы нет в RWN, либо prebuilt index не загружен
         0: лемма есть в RWN, но hyponyms нет (лист дерева)
         N: лемма есть, у неё N hyponyms-лемм суммарно по всем sense'ам

    Используется в `_pick_token_parse` для эвристики выбора 'предметного'
    NOUN при омонимии: цветок → много hyponyms (роза, тюльпан, ...),
    цвет-окраска → 0 или меньше.
    """
    if not _USE_PREBUILT_INDEX or not lemma:
        return -1
    meta = _META_INDEX.get(lemma.lower())
    if meta is None:
        return -1
    return int(meta.get("hypo_count", -1))


def _pick_token_parse(token: str) -> Optional[Any]:
    """Выбор parse для одного токена seed с учётом RuWordNet.

    Pymorphy top даёт самый частотный смысл. Для омонимов (цветов →
    цвет/цветок, замок → замок/замок) этого недостаточно: для слова 'цветов'
    оба парса имеют равный score 0.5 и top — недетерминированный.

    Эвристика выбора:
      1. Если RWN доступен и среди парсов есть несколько NOUN с поддержкой
         в RWN — выбираем тот у кого больше hyponyms. Это устойчивый признак
         "конкретного" существительного (цветок имеет {роза, тюльпан, букет,
         эустома, ...}, цвет-окраска имеет существенно меньше или 0).
      2. Если только один NOUN с поддержкой — берём его.
      3. Если RWN нет или ни один не в RWN — берём top content-парс.
    """
    parses = _parse_all(token)
    if not parses:
        return None

    content_parses = [p for p in parses if _is_content_word(p)]
    if not content_parses:
        return parses[0]

    if _USE_PREBUILT_INDEX:
        # Собираем NOUN-парсы найденные в RWN с числом их hyponyms
        rwn_noun_candidates: List[Tuple[Any, int]] = []
        seen_lemmas: Set[str] = set()
        for p in content_parses:
            if p.tag.POS != 'NOUN':
                continue
            if p.normal_form in seen_lemmas:
                continue
            seen_lemmas.add(p.normal_form)
            hypo_count = _count_hyponyms(p.normal_form)
            if hypo_count < 0:
                # Леммы нет в RWN — пропускаем кандидата (поведение как раньше)
                continue
            rwn_noun_candidates.append((p, hypo_count))

        if rwn_noun_candidates:
            # Берём с максимальным hypo_count. Если ничья — первый в списке
            # (parses уже отсортированы pymorphy по score).
            rwn_noun_candidates.sort(key=lambda x: -x[1])
            return rwn_noun_candidates[0][0]

    # Fallback: top content parse
    return content_parses[0]


def _extract_seed_structure(seed: str) -> Dict[str, Any]:
    """
    Возвращает:
      content_lemmas: List[str] — все content-леммы seed
      action_anchor:  Optional[str] — VERB/INFN или action-noun (отглагольный)
      object_anchor:  Optional[str] — предметный NOUN
      qualifier:      Optional[str] — число из seed (если есть)

    Выбор anchor'ов:
    - action = первый VERB/INFN. Если нет — NOUN с verb-derivation в RWN
      (доставка → доставлять, ремонт → ремонтировать). Это устойчиво к
      порядку слов: "доставка цветов" и "цветов доставка" дадут одно.
    - object = NOUN ≠ action, без verb-derivation если возможно.
    - Fallback (если RWN недоступен) — позиционный: первый NOUN=action,
      последний NOUN=object.
    """
    tokens = _tokenize(seed)
    content_parses = []
    for tok in tokens:
        p = _pick_token_parse(tok)
        if p is None:
            continue
        if _is_content_word(p):
            content_parses.append(p)

    content_lemmas = [p.normal_form for p in content_parses]

    action_anchor: Optional[str] = None
    object_anchor: Optional[str] = None

    # 1. Явный VERB/INFN → action
    for p in content_parses:
        if p.tag.POS in {'VERB', 'INFN'}:
            action_anchor = p.normal_form
            break

    # Только NOUN среди content-парсов
    noun_parses = [p for p in content_parses if p.tag.POS == 'NOUN']

    if action_anchor is None and noun_parses:
        # 2. Action = NOUN с verb-derivation (отглагольный), если prebuilt index доступен.
        # Если несколько кандидатов с derivation — берём первый по позиции.
        if _USE_PREBUILT_INDEX:
            for p in noun_parses:
                if _has_verb_derivation(p.normal_form):
                    action_anchor = p.normal_form
                    break

        # 3. Fallback: первый NOUN
        if action_anchor is None:
            action_anchor = noun_parses[0].normal_form

    # Object = NOUN ≠ action. Предпочитаем NOUN без verb-derivation
    # (это "предметные" существительные: цветок, квартира). Если все NOUN
    # отглагольные — берём последний ≠ action.
    object_candidates = [p for p in noun_parses if p.normal_form != action_anchor]
    if object_candidates:
        non_action_noun = None
        if _USE_PREBUILT_INDEX:
            for p in object_candidates:
                if not _has_verb_derivation(p.normal_form):
                    non_action_noun = p
                    break
        if non_action_noun is not None:
            object_anchor = non_action_noun.normal_form
        else:
            object_anchor = object_candidates[-1].normal_form

    # Single-content seed: единственная лемма = object, action=None
    if object_anchor is None and len(content_parses) == 1:
        object_anchor = content_parses[0].normal_form
        action_anchor = None

    nums = re.findall(r'\d+', seed)
    qualifier = nums[0] if nums else None

    return {
        'content_lemmas': content_lemmas,
        'action_anchor': action_anchor,
        'object_anchor': object_anchor,
        'qualifier': qualifier,
    }


# ─── qualifier-проверка ─────────────────────────────────────────────────

def _qualifier_in_tokens(qualifier: str, tokens: List[str]) -> Tuple[bool, str]:
    """
    Проверяет что keyword выражает qualifier (число из seed) в одной из форм.

    Возвращает (matched, method). method ∈ {
        'exact'        — точное совпадение токена ('16' in tokens)
        'digit_prefix' — цифро-буквенный токен с тем же числом в префиксе
                         ('16е', '16e', '16-й', '16й', '100мл', '50кг', '12в')
        'word_numeric' — словесная форма числа через E5 сравнение
                         ('шестнадцатый' → cos с '16' высокий)
        'none'
    }

    Логика трёх уровней:
    1. Точное совпадение — самый быстрый и строгий путь.
    2. Префиксное совпадение через regex: токен начинается с qualifier и
       продолжается нецифровым суффиксом. Покрывает универсально любые
       единицы измерения и модификации модели (16е, 50кг, 12в, 100мл).
       Защита: ``(?!\\d)`` — суффикс не должен быть цифрой, иначе '16' дало
       бы match для '160' / '1605'.
    3. Если qualifier — чисто цифровой и в keyword есть Anum/NUMR токен —
       сравниваем cos между qualifier и normal_form этого токена.
       Используем E5 без всякого словаря: модель обучена на корпусах где
       'шестнадцатый' и '16' часто встречались в контексте друг друга.
       Порог COS_QUALIFIER_NUMERIC_HIGH консервативный.
    """
    if not qualifier:
        return True, 'no_qualifier'

    # Уровень 1: точное совпадение
    if qualifier in tokens:
        return True, 'exact'

    # Уровень 2: цифровой префикс ('16е' для qualifier='16')
    # Используем negative lookahead для границы: '16е' — да, '160' — нет.
    pattern = re.compile(r'^' + re.escape(qualifier) + r'(?!\d)')
    for tok in tokens:
        if pattern.match(tok):
            return True, 'digit_prefix'

    # Уровень 3: словесная форма числа через E5.
    # Только если qualifier чисто цифровой (10/16/50/100), и E5 доступен.
    if qualifier.isdigit() and _E5_IMPORT_OK:
        qual_emb = get_e5_word_embedding(qualifier)
        if qual_emb is not None:
            for tok in tokens:
                p = _parse_top(tok)
                if p is None:
                    continue
                grams = p.tag.grammemes
                # Anum = числительное-прилагательное (шестнадцатый/первый/сотый)
                # NUMR = количественное числительное (шестнадцать/двадцать)
                if 'Anum' not in grams and 'NUMR' not in grams:
                    continue
                # Получаем embedding нормальной формы — это снимает падежи
                # ('шестнадцатого' → 'шестнадцатый')
                lemma_to_check = p.normal_form
                if lemma_to_check is None:
                    continue
                tok_emb = get_e5_word_embedding(lemma_to_check)
                if tok_emb is None:
                    continue
                cos = e5_cosine_sim(qual_emb, tok_emb)
                if cos >= COS_QUALIFIER_NUMERIC_HIGH:
                    return True, f'word_numeric:{lemma_to_check}({cos:.2f})'

    return False, 'none'


# ─── object_neighbors из L0_VALID ────────────────────────────────────────

def _build_object_neighbors(
    l0_valid_keywords: List[str],
    object_anchor: str,
    excluded_lemmas: Set[str],
    window: int = NEIGHBOR_WINDOW,
    min_freq: int = NEIGHBOR_MIN_FREQ,
) -> Set[str]:
    """
    Леммы NOUN которые встречались в окне ±window от object_anchor
    минимум в min_freq L0_VALID ключах.

    excluded_lemmas — содержательные леммы seed (action_anchor, others),
    их исключаем из neighbors, чтобы action не пролезал как object_hyponym.

    Match anchor → token идёт через ВСЕ возможные леммы токена (омонимия):
    'цветов' → {цвет, цветок}, anchor='цветок' матчится. Substring-match
    убран — он давал false positives на коротких anchor (для 'цвет'
    срабатывал на 'соцветие', 'разноцветный', 'цветовой').
    """
    if _morph is None or not object_anchor:
        return set()

    counter: Counter = Counter()
    for kw in l0_valid_keywords:
        tokens = _tokenize(kw)
        if not tokens:
            continue
        # Все леммы каждого токена (омонимия: цветов → {цвет, цветок}).
        token_lemma_sets: List[Set[str]] = [_token_lemmas(t) for t in tokens]
        # NOUN-леммы отдельно для соседей (фильтруем по POS).
        token_noun_lemmas: List[Set[str]] = [
            _token_lemmas(t, pos_filter={'NOUN'}) for t in tokens
        ]

        # позиции anchor: токен у которого среди возможных лемм есть anchor
        anchor_positions = [
            i for i, lem_set in enumerate(token_lemma_sets)
            if object_anchor in lem_set
        ]
        if not anchor_positions:
            continue

        # уникальные леммы NOUN в окне для этого kw (избегаем двойного счёта).
        # Для каждого токена-соседа берём ВСЕ его NOUN-леммы (омонимия даёт
        # больше шансов матча в дальнейшем prove_object).
        seen_in_kw: Set[str] = set()
        for pos in anchor_positions:
            for j in range(max(0, pos - window), min(len(tokens), pos + window + 1)):
                if j == pos:
                    continue
                for lem in token_noun_lemmas[j]:
                    if lem == object_anchor or lem in excluded_lemmas:
                        continue
                    seen_in_kw.add(lem)
        for lem in seen_in_kw:
            counter[lem] += 1

    return {lem for lem, freq in counter.items() if freq >= min_freq}


def _build_action_neighbors(
    l0_valid_keywords: List[str],
    action_anchor: str,
    excluded_lemmas: Set[str],
    window: int = NEIGHBOR_WINDOW,
    min_freq: int = NEIGHBOR_MIN_FREQ,
) -> Set[str]:
    """
    ОТКЛЮЧЕНО (но оставлено в коде на будущее).

    Логика co-occurrence в окне ±window от action_anchor — на сидах вида
    "доставка X" даёт object-domain леммы (цвет/букет/роза...) которые
    не являются синонимами action. См. историю фиксов: попытка V5 привела
    к 24 FP в 'доставка цветов'.

    Вместо этого используется _build_advb_question_whitelist — узкое
    правило только для ADVB-лемм встречающихся в начале L0 VALID с
    action_anchor (info-intent позиция).
    """
    return set()


def _build_action_question_anchors(
    l0_valid_keywords: List[str],
    action_anchor: str,
    min_freq: int = 2,
) -> Set[str]:
    """
    Набор ADVB-лемм встречающихся на позиции 0 в L0 VALID-keyword'ах
    содержащих action_anchor. Минимум min_freq повторений.

    Построен per-seed из данных L0 — не хардкод-список.

    Зачем: L0 пропускает info-вопросы которые валидны для конкретного
    seed-намерения. Например для 'купить айфон 16' L0 пропускает
    'сколько стоит купить айфон 16', 'где купить айфон 16', 'как выгодно
    купить айфон 16'. Это значит 'сколько'/'где'/'как' — релевантные
    action-сигналы для коммерческого intent этого seed.

    Узкое правило (только ADVB, только pos 0, min_freq=2) защищает
    baseline 'доставка цветов': L0 НЕ пропускает 'где доставка цветов'
    (info про другие магазины, не про доставку), поэтому 'где' не
    попадает в набор. Не задеёт 'где купить цветы в днепре' (TRASH).

    Симметрично object_neighbors: оба используют co-occurrence в L0 VALID
    для построения per-seed набора релевантных слов.
    """
    if _morph is None or not action_anchor:
        return set()

    counter: Counter = Counter()
    for kw in l0_valid_keywords:
        tokens = _tokenize(kw)
        if not tokens:
            continue
        # Проверяем что keyword содержит action_anchor среди лемм токенов
        # (через _token_lemmas чтобы учесть омонимы)
        has_anchor = any(action_anchor in _token_lemmas(t) for t in tokens)
        if not has_anchor:
            continue
        # Берём pos 0 — info-intent позиция
        first_tok = tokens[0]
        for p in _parse_all(first_tok):
            if 'ADVB' in p.tag.grammemes:
                if p.normal_form:
                    counter[p.normal_form] += 1
                break  # один ADVB-парс достаточно

    return {lem for lem, freq in counter.items() if freq >= min_freq}


# ─── Доказательство осей ─────────────────────────────────────────────────

def _prove_object(
    kw_tokens: List[str],
    kw_lemmas: List[Optional[str]],
    kw_parses: List[Any],
    object_anchor: Optional[str],
    object_neighbors: Set[str],
    object_synonyms: Set[str],
    excluded_lemmas: Set[str],
    action_anchor: Optional[str],
    seed_content_lemmas: Set[str],
    object_cohyponyms: Optional[Set[str]] = None,
) -> Tuple[bool, dict]:
    """
    Возвращает (proven, diag).
    diag = {
        'method': 'substring|lemma|ruwordnet|ruwordnet_cohyponym|hyponym|abbrev_pos0|none',
        'matched_lemma': str | None,
        'best_cos': float,           # cos к object_anchor у лучшего кандидата
        'best_cos_act': float,       # cos к action_anchor у того же кандидата
        'best_gap': float,           # best_cos - best_cos_act
        'all_cos': [{'lemma':..., 'cos_obj':..., 'cos_act':..., 'gap':...,
                     'in_neighbors':bool, 'in_seed':bool}, ...],
        'reason': human-readable
    }
    """
    diag = {
        'method': 'none',
        'matched_lemma': None,
        'best_cos': None,
        'best_cos_act': None,
        'best_gap': None,
        'all_cos': [],
        'reason': '',
    }

    if not object_anchor:
        diag['method'] = 'no_object_in_seed'
        diag['reason'] = 'no_object_in_seed'
        return True, diag

    kw_low = ' '.join(kw_tokens)

    # 1. substring
    if object_anchor in kw_low:
        diag['method'] = 'substring'
        diag['matched_lemma'] = object_anchor
        diag['reason'] = f'substring:{object_anchor}'
        return True, diag

    # 2. прямая лемма (через ВСЕ парсы токена — омонимы).
    #    'цветов' → {цвет, цветок}, anchor='цветок' → match.
    for tok in kw_tokens:
        if object_anchor in _token_lemmas(tok):
            diag['method'] = 'lemma'
            diag['matched_lemma'] = object_anchor
            diag['reason'] = f'lemma:{object_anchor}'
            return True, diag

    # 3. RuWordNet synonym/hyponym (через все леммы токена)
    for tok in kw_tokens:
        for lem in _token_lemmas(tok):
            if lem in object_synonyms:
                diag['method'] = 'ruwordnet'
                diag['matched_lemma'] = lem
                diag['reason'] = f'ruwordnet:{lem}'
                return True, diag

    # 3.5. RuWordNet co-hyponym (sibling через ближайший hypernym + 2 уровня
    # вниз). Ловит семантически родственные термины которые НЕ являются
    # гипонимом друг друга, но имеют общего предка таксономии.
    # Пример: 'скутер' и 'мопед' — оба ребёнки/внуки МОТОТРАНСПОРТНОЕ СРЕДСТВО.
    # Метод 3 (hyponyms скутера) такого не ловит — мопед не ребёнок скутера.
    # _get_cohyponyms возвращает пустой set для широких таксономий
    # (>COHYPONYM_MAX_SIZE лемм) — защита baseline 'доставка цветов'.
    if object_cohyponyms:
        for tok in kw_tokens:
            for lem in _token_lemmas(tok):
                if lem in object_cohyponyms:
                    diag['method'] = 'ruwordnet_cohyponym'
                    diag['matched_lemma'] = lem
                    diag['reason'] = f'ruwordnet_cohyponym:{lem}'
                    return True, diag

    # 4. E5 hyponym.
    # cos_obj порог + neighbors + gap >= COS_GAP_MIN отсекают коммерческие
    # атрибуты процесса (цена -0.029, отзыв -0.014, заказ -0.060) и гео
    # (одесса -0.028, днепр -0.019). Гипонимы цвета (роза, тюльпан) уже
    # ловятся методом 3 через RuWordNet hyponyms — gap не убьёт их даже при
    # значениях 0.02-0.04, потому что метод 4 для них не достигается.
    # MIN_OBJECT_LEMMA_LEN отсекает предлоги-омонимы 'в'/'с' (pymorphy
    # парсит букву как NOUN — побочный парс).
    # BYPASS: лемма из seed_content_lemmas — принимаем без neighbors/gap.
    anchor_emb = get_e5_word_embedding(object_anchor)
    action_emb = get_e5_word_embedding(action_anchor) if action_anchor else None

    if anchor_emb is not None:
        best_cos_obj = -1.0
        best_lem_overall = None
        best_cos_act_overall = 0.0

        # Уникальные NOUN-леммы среди ВСЕХ парсов всех токенов keyword
        # (омонимия: цветов → цвет/цветок). Фильтруем короткие леммы (предлоги).
        cand_lemmas: Set[str] = set()
        for tok in kw_tokens:
            for lem in _token_lemmas(tok, pos_filter={'NOUN'}):
                if lem == object_anchor or lem in excluded_lemmas:
                    continue
                if len(lem) < MIN_OBJECT_LEMMA_LEN:
                    continue
                cand_lemmas.add(lem)

        for lem in cand_lemmas:
            cand_emb = get_e5_word_embedding(lem)
            if cand_emb is None:
                continue
            cos_obj = e5_cosine_sim(anchor_emb, cand_emb)
            cos_act = e5_cosine_sim(action_emb, cand_emb) if action_emb is not None else 0.0
            gap = cos_obj - cos_act
            in_n = lem in object_neighbors
            in_seed = lem in seed_content_lemmas

            diag['all_cos'].append({
                'lemma': lem,
                'cos_obj': round(cos_obj, 3),
                'cos_act': round(cos_act, 3),
                'gap': round(gap, 3),
                'in_neighbors': in_n,
                'in_seed': in_seed,
            })

            # BYPASS для лемм из seed (для 3+ word seeds где content_lemmas
            # содержит "прочие" non-anchor слова). Без gap/neighbors.
            if in_seed and cos_obj >= COS_OBJECT_HIGH:
                if cos_obj > best_cos_obj:
                    best_cos_obj = cos_obj
                    best_lem_overall = lem
                    best_cos_act_overall = cos_act
                continue

            # Базовая проверка: cos выше порога + neighbors + gap.
            if not in_n:
                continue
            if cos_obj < COS_OBJECT_HIGH:
                continue
            if gap < COS_GAP_MIN:
                continue

            if cos_obj > best_cos_obj:
                best_cos_obj = cos_obj
                best_lem_overall = lem
                best_cos_act_overall = cos_act

        if best_lem_overall:
            diag['best_cos'] = round(best_cos_obj, 3)
            diag['best_cos_act'] = round(best_cos_act_overall, 3)
            diag['best_gap'] = round(best_cos_obj - best_cos_act_overall, 3)
            diag['method'] = 'hyponym'
            diag['matched_lemma'] = best_lem_overall
            diag['reason'] = (
                f'hyponym:{best_lem_overall}'
                f'(obj={best_cos_obj:.2f},act={best_cos_act_overall:.2f})'
            )
            return True, diag

    # 5. abbrev_pos0 — короткое NOUN на pos 0 + в neighbors.
    # GAP-тест откатан (см. метод 4). Защита от шума — neighbors-фильтр.
    if kw_parses and kw_parses[0] is not None:
        p0 = kw_parses[0]
        # Берём все возможные NOUN-леммы первого токена (омонимы)
        first_noun_lemmas = _token_lemmas(kw_tokens[0], pos_filter={'NOUN'})
        if first_noun_lemmas and len(kw_tokens[0]) <= 4:
            for lem0 in first_noun_lemmas:
                if lem0 in object_neighbors and lem0 not in excluded_lemmas:
                    diag['method'] = 'abbrev_pos0'
                    diag['matched_lemma'] = lem0
                    diag['reason'] = f'abbrev_pos0:{lem0}'
                    return True, diag

    diag['reason'] = 'no_object_proof'
    return False, diag


def _prove_action(
    kw_tokens: List[str],
    kw_lemmas: List[Optional[str]],
    kw_parses: List[Any],
    action_anchor: Optional[str],
    action_synonyms: Set[str],
    action_question_anchors: Optional[Set[str]] = None,
) -> Tuple[bool, dict]:
    """Возвращает (proven, diag). Структура diag — как у _prove_object.

    action_question_anchors — набор ADVB-лемм встречающихся в pos 0 L0 VALID
    с action_anchor (минимум 2 раза). Построено per-seed из данных L0 — не
    хардкод. Используется методом 3.5 для info-intent keyword'ов
    ('сколько стоит ...', 'как заказать ...').
    """
    diag = {
        'method': 'none',
        'matched_lemma': None,
        'best_cos': None,
        'all_cos': [],
        'reason': '',
    }

    if not action_anchor:
        diag['method'] = 'no_action_in_seed'
        diag['reason'] = 'no_action_in_seed'
        return True, diag

    kw_low = ' '.join(kw_tokens)

    # 1. substring
    if action_anchor in kw_low:
        diag['method'] = 'substring'
        diag['matched_lemma'] = action_anchor
        diag['reason'] = f'substring:{action_anchor}'
        return True, diag

    # 2. прямая лемма (через ВСЕ парсы токена)
    for tok in kw_tokens:
        if action_anchor in _token_lemmas(tok):
            diag['method'] = 'lemma'
            diag['matched_lemma'] = action_anchor
            diag['reason'] = f'lemma:{action_anchor}'
            return True, diag

    # 3. RuWordNet synonym (через все парсы)
    for tok in kw_tokens:
        for lem in _token_lemmas(tok):
            if lem in action_synonyms:
                diag['method'] = 'ruwordnet'
                diag['matched_lemma'] = lem
                diag['reason'] = f'ruwordnet:{lem}'
                return True, diag

    # 3.5. ADVB-question anchor. Для info-intent keyword'ов: проверяем
    # есть ли в keyword токен-наречие с леммой из per-seed набора
    # action_question_anchors (построен из pos 0 L0 VALID c action_anchor).
    #
    # Спасает: 'сколько стоит купить айфон 16', 'где лучше купить айфон',
    # 'как выгодно купить ...'. Это валидные info-вопросы для коммерческого
    # seed-намерения которые L0 пропускает.
    #
    # Не задевает baseline 'доставка цветов': L0 не пропускает 'где
    # доставка...', поэтому 'где' не в action_question_anchors → keyword
    # 'где купить цветы днепр' остаётся TRASH (action не доказан, 'купить'
    # не наречие, 'где' не в наборе).
    if action_question_anchors:
        for tok in kw_tokens:
            for p in _parse_all(tok):
                if 'ADVB' not in p.tag.grammemes:
                    continue
                lem = p.normal_form
                if lem and lem in action_question_anchors:
                    diag['method'] = 'advb_question'
                    diag['matched_lemma'] = lem
                    diag['reason'] = f'advb_question:{lem}'
                    return True, diag
                break  # для токена нашли ADVB парс — выходим из inner loop

    # 4. E5 synonym (cos≥COS_ACTION_HIGH, без neighbors).
    # Кандидаты: NOUN/VERB/INFN — содержательные слова, ADVB — вопросительные
    # наречия типа 'сколько'/'почём' которые семантически близки к
    # коммерческим action ('купить', 'заказать'). Без ADVB они отсекались
    # ещё до E5-сравнения.
    # MIN_OBJECT_LEMMA_LEN защищает от предлогов 'в'/'с' через омонимы.
    anchor_emb = get_e5_word_embedding(action_anchor)
    if anchor_emb is not None:
        best_cos = 0.0
        best_lem = None
        cand_lemmas: Set[str] = set()
        for tok in kw_tokens:
            for lem in _token_lemmas(tok, pos_filter={'NOUN', 'VERB', 'INFN', 'ADVB'}):
                if lem == action_anchor:
                    continue
                if len(lem) < MIN_OBJECT_LEMMA_LEN:
                    continue
                cand_lemmas.add(lem)

        for lem in cand_lemmas:
            cand_emb = get_e5_word_embedding(lem)
            if cand_emb is None:
                continue
            cos = e5_cosine_sim(anchor_emb, cand_emb)
            diag['all_cos'].append({'lemma': lem, 'cos': round(cos, 3)})
            if cos > best_cos:
                best_cos = cos
                best_lem = lem
        diag['best_cos'] = round(best_cos, 3) if best_lem else None
        if best_lem and best_cos >= COS_ACTION_HIGH:
            diag['method'] = 'action_syn'
            diag['matched_lemma'] = best_lem
            diag['reason'] = f'action_syn:{best_lem}({best_cos:.2f})'
            return True, diag

    diag['reason'] = 'no_action_proof'
    return False, diag


def _prove_other_lemma(
    lemma: str,
    kw_tokens: List[str],
    kw_lemmas: List[Optional[str]],
    synonyms: Set[str],
) -> Tuple[bool, str]:
    """
    Для содержательных лемм seed (не action, не object) — например, гео `буковель`.
    Только substring/lemma/synonym. Без cos (нет смысла для гео).
    """
    kw_low = ' '.join(kw_tokens)
    if lemma in kw_low:
        return True, f'substring:{lemma}'
    for tok in kw_tokens:
        token_lems = _token_lemmas(tok)
        if lemma in token_lems:
            return True, f'lemma:{lemma}'
        if token_lems & synonyms:
            matched = next(iter(token_lems & synonyms))
            return True, f'ruwordnet:{matched}'
    return False, f'no_proof:{lemma}'


# ─── Main entry ──────────────────────────────────────────────────────────

def apply_l1_5_filter_v2(prev_result: dict, seed: str) -> dict:
    """
    TRASH-filter. Прибирает GREY-список из prev_result.
    Никогда не модифицирует keywords (L0 VALID) и не добавляет туда новых.
    """
    grey_keywords: List[str] = prev_result.get('keywords_grey', []) or []
    prev_result.setdefault('_l1_5_trace', [])

    if not grey_keywords:
        logger.info("[L1.5/v3] empty GREY input — nothing to do")
        return prev_result

    # ── Профилирование (стиль L0): фиксируем время каждого этапа.
    import time as _pf_time
    _t_stage: Dict[str, float] = {}
    _t_total = _pf_time.perf_counter()

    # ── E5 warmup + диагностика. Грузим модель один раз ДО прогона.
    # Без warmup — chicken-egg, модель не загрузится никогда.
    _t = _pf_time.perf_counter()
    e5_status = "DISABLED (import failed)"
    if _E5_IMPORT_OK:
        try:
            model = get_e5_model()
            if model is not None:
                # Прогрев на тест-слове чтобы убедиться что embed работает
                test_emb = get_e5_word_embedding("тест")
                if test_emb is not None:
                    e5_status = f"OK (dim={len(test_emb)})"
                else:
                    e5_status = "LOADED but embed returned None"
            else:
                e5_status = "FAILED (get_e5_model returned None)"
        except Exception as e:
            e5_status = f"ERROR: {e}"
    _t_stage['e5_warmup'] = _pf_time.perf_counter() - _t
    logger.info(f"[L1.5/v3] E5 status: {e5_status}")
    logger.info(
        f"[L1.5/v3] RWN index status: "
        f"{'OK (' + str(len(_SYNONYMS_INDEX)) + ' lemmas)' if _USE_PREBUILT_INDEX else 'DISABLED'}"
    )

    # ── Парсинг seed
    _t = _pf_time.perf_counter()
    seed_struct = _extract_seed_structure(seed)
    _t_stage['extract_seed'] = _pf_time.perf_counter() - _t
    content_lemmas = seed_struct['content_lemmas']
    action_anchor = seed_struct['action_anchor']
    object_anchor = seed_struct['object_anchor']
    qualifier = seed_struct['qualifier']

    # other_lemmas: content_lemmas минус action и object
    other_lemmas = [
        lem for lem in content_lemmas
        if lem != action_anchor and lem != object_anchor
    ]

    # excluded_lemmas: action + others. Эти леммы не должны ловиться
    # как object_hyponym (action часто в neighbors с высокой частотой —
    # риск что action_anchor пролезет как object).
    excluded_lemmas: Set[str] = set()
    if action_anchor:
        excluded_lemmas.add(action_anchor)
    excluded_lemmas.update(other_lemmas)

    logger.info(
        f"[L1.5/v3] seed={seed!r} action={action_anchor!r} object={object_anchor!r} "
        f"other={other_lemmas} qualifier={qualifier!r} excluded={excluded_lemmas}"
    )

    # ── Подготовка ресурсов
    _t = _pf_time.perf_counter()
    l0_valid = prev_result.get('keywords', []) or []
    object_neighbors = (
        _build_object_neighbors(l0_valid, object_anchor, excluded_lemmas)
        if object_anchor else set()
    )
    _t_stage['build_neighbors'] = _pf_time.perf_counter() - _t

    _t = _pf_time.perf_counter()
    # action_question_anchors: набор ADVB-лемм в pos 0 L0 VALID c action_anchor.
    # Per-seed построение для info-intent keywords (см. _build_action_question_anchors).
    action_question_anchors = (
        _build_action_question_anchors(l0_valid, action_anchor)
        if action_anchor else set()
    )
    _t_stage['build_action_q_anchors'] = _pf_time.perf_counter() - _t

    _t = _pf_time.perf_counter()
    object_synonyms = _get_synonyms(object_anchor) if object_anchor else set()
    _t_stage['rwn_obj_syn'] = _pf_time.perf_counter() - _t

    _t = _pf_time.perf_counter()
    object_cohyponyms = _get_cohyponyms(object_anchor) if object_anchor else set()
    _t_stage['rwn_obj_cohyp'] = _pf_time.perf_counter() - _t

    _t = _pf_time.perf_counter()
    action_synonyms = _get_synonyms(action_anchor) if action_anchor else set()
    _t_stage['rwn_act_syn'] = _pf_time.perf_counter() - _t

    _t = _pf_time.perf_counter()
    other_synonyms = {lem: _get_synonyms(lem) for lem in other_lemmas}
    _t_stage['rwn_other_syn'] = _pf_time.perf_counter() - _t

    logger.info(
        f"[L1.5/v3] neighbors({object_anchor})={len(object_neighbors)} "
        f"act_q_anchors({action_anchor})={sorted(action_question_anchors)} "
        f"obj_syn={len(object_synonyms)} obj_cohyp={len(object_cohyponyms)} "
        f"act_syn={len(action_synonyms)}"
    )

    # ── Batch warm E5 cache ─────────────────────────────────────────────
    # КРИТИЧНО для скорости: индивидуальные get_e5_word_embedding вызывают
    # ONNX runtime на каждое слово (~50ms на E5-large CPU). Собираем все
    # уникальные леммы которые понадобятся в проверках, считаем один батч.
    # Дальше get_e5_word_embedding() в _prove_* мгновенно возвращает из кеша.
    _t = _pf_time.perf_counter()
    _t_warm_collect = 0.0
    n_warmed = 0
    n_total = 0
    if _E5_IMPORT_OK and warm_e5_word_cache is not None:
        _tc = _pf_time.perf_counter()
        words_to_warm: Set[str] = set()
        # anchors
        if object_anchor:
            words_to_warm.add(object_anchor)
        if action_anchor:
            words_to_warm.add(action_anchor)
        # qualifier нужен для уровня 3 _qualifier_in_tokens (E5 cos с
        # словесными числительными типа 'шестнадцатый').
        if qualifier:
            words_to_warm.add(qualifier)
        # все NOUN/VERB/INFN-леммы из всех keywords (через омонимы)
        # + Anum/NUMR леммы для qualifier-проверки.
        for kw in grey_keywords:
            for tok in _tokenize(kw):
                # для object-кандидатов нужны NOUN, для action — NOUN+VERB+INFN+ADVB
                # (ADVB добавлен для вопросительных наречий типа 'сколько'/'почём')
                for lem in _token_lemmas(tok, pos_filter={'NOUN', 'VERB', 'INFN', 'ADVB'}):
                    if len(lem) >= MIN_OBJECT_LEMMA_LEN:
                        words_to_warm.add(lem)
                # Anum/NUMR — для qualifier word_numeric проверки.
                # Берём только если есть qualifier-цифра в seed.
                if qualifier and qualifier.isdigit():
                    p = _parse_top(tok)
                    if p is not None and (
                        'Anum' in p.tag.grammemes or 'NUMR' in p.tag.grammemes
                    ):
                        if p.normal_form:
                            words_to_warm.add(p.normal_form)
        _t_warm_collect = _pf_time.perf_counter() - _tc
        n_total = len(words_to_warm)
        n_warmed = warm_e5_word_cache(words_to_warm)
    _t_stage['e5_warm_collect'] = _t_warm_collect
    _t_stage['e5_warm_total'] = _pf_time.perf_counter() - _t
    _t_stage['_e5_warm_new'] = n_warmed
    _t_stage['_e5_warm_unique'] = n_total
    logger.info(
        f"[L1.5/v3] E5 batch warm: {n_warmed} new embeddings "
        f"(total {n_total} unique words) in {_t_stage['e5_warm_total']:.2f}s "
        f"(collect={_t_warm_collect:.2f}s)"
    )

    # ── Прогон GREY.
    # _l1_5_trace — только TRASH (для UI, чтобы GREY не показывались как заблокированные).
    # _l1_5_diag  — полный диагностический trace по всем ключам (для калибровки порогов).
    new_grey: List[str] = []
    trash_traces: List[dict] = []
    full_diag: List[dict] = []

    # Аккумуляторы времени внутри основного цикла
    _t_loop_start = _pf_time.perf_counter()
    _t_tokenize_total = 0.0
    _t_parse_total = 0.0
    _t_prove_obj_total = 0.0
    _t_prove_act_total = 0.0
    _t_prove_other_total = 0.0
    _slow_kw: List[Tuple[float, str]] = []  # top-N самых медленных ключей

    for kw in grey_keywords:
        _kw_t0 = _pf_time.perf_counter()

        _t = _pf_time.perf_counter()
        tokens = _tokenize(kw)
        _t_tokenize_total += _pf_time.perf_counter() - _t

        if not tokens:
            trash_traces.append({
                'keyword': kw, 'label': 'TRASH', 'decided_by': 'l1_5_v3',
                'reason': 'empty_tokens', 'signals': [],
            })
            full_diag.append({
                'keyword': kw, 'label': 'TRASH',
                'reason': 'empty_tokens', 'obj': None, 'act': None,
            })
            continue

        # QUALIFIER_HARD — расширенная проверка с тремя уровнями
        # (exact / digit_prefix / word_numeric через E5).
        if qualifier:
            q_ok, q_method = _qualifier_in_tokens(qualifier, tokens)
            if not q_ok:
                r = f'qualifier_missing:{qualifier}'
                trash_traces.append({
                    'keyword': kw, 'label': 'TRASH', 'decided_by': 'l1_5_v3',
                    'reason': r, 'signals': [],
                })
                full_diag.append({
                    'keyword': kw, 'label': 'TRASH',
                    'reason': r, 'obj': None, 'act': None,
                })
                continue

        _t = _pf_time.perf_counter()
        parses = [_parse_top(t) for t in tokens]
        lemmas = [p.normal_form if p else None for p in parses]
        _t_parse_total += _pf_time.perf_counter() - _t

        # ОСИ
        _t = _pf_time.perf_counter()
        obj_ok, obj_diag = _prove_object(
            tokens, lemmas, parses, object_anchor, object_neighbors, object_synonyms,
            excluded_lemmas, action_anchor, set(content_lemmas),
            object_cohyponyms=object_cohyponyms,
        )
        _t_prove_obj_total += _pf_time.perf_counter() - _t

        _t = _pf_time.perf_counter()
        act_ok, act_diag = _prove_action(
            tokens, lemmas, parses, action_anchor, action_synonyms,
            action_question_anchors=action_question_anchors,
        )
        _t_prove_act_total += _pf_time.perf_counter() - _t

        # OTHER (для 3+ word seeds) — без cos, просто substring/lemma/synonym
        _t = _pf_time.perf_counter()
        other_results: List[Tuple[bool, str]] = []
        for lem in other_lemmas:
            ok, reason = _prove_other_lemma(lem, tokens, lemmas, other_synonyms.get(lem, set()))
            other_results.append((ok, reason))
        all_other_ok = all(ok for ok, _ in other_results)
        other_reasons = [r for _, r in other_results]
        _t_prove_other_total += _pf_time.perf_counter() - _t

        if obj_ok and act_ok and all_other_ok:
            new_grey.append(kw)
            label = 'GREY'
            reason = 'all_axes_proven'
        elif obj_ok and act_ok and not all_other_ok:
            # GREY_SOFT: основные оси (obj+act) доказаны, но other-лемма
            # (3-я+ content-слово seed, например 'цена' для seed
            # "установка кондиционера цена") не найдена в keyword.
            # Семантически это keyword с тем же intent но без явного маркера
            # дополнительной леммы. Не TRASH — пользователь решит включать
            # или нет через UI-группировку по label.
            # Не задеёт seeds с ≤2 content-словами (там other_lemmas пустой
            # → all_other_ok=True всегда).
            new_grey.append(kw)
            label = 'GREY_SOFT'
            missing = [r for ok, r in other_results if not ok]
            reason = 'main_axes_proven_other_missing:' + ','.join(missing)
        else:
            label = 'TRASH'
            failed: List[str] = []
            if not obj_ok:
                failed.append('obj')
            if not act_ok:
                failed.append('act')
            for ok, r in other_results:
                if not ok:
                    failed.append(f'other:{r}')
            reason = 'axis_unproven:' + ','.join(failed)
            # короткие signals для UI
            ui_signals = [f'obj={obj_diag["reason"]}', f'act={act_diag["reason"]}']
            for r in other_reasons:
                ui_signals.append(f'other={r}')
            trash_traces.append({
                'keyword': kw, 'label': 'TRASH', 'decided_by': 'l1_5_v3',
                'reason': reason, 'signals': ui_signals,
            })

        # полный диагностический trace для калибровки
        full_diag.append({
            'keyword': kw,
            'label': label,
            'reason': reason,
            'obj': obj_diag,           # method, matched_lemma, best_cos, all_cos
            'act': act_diag,           # same
            'other': other_reasons,
        })

        _kw_dt = _pf_time.perf_counter() - _kw_t0
        # Сохраняем top-5 самых медленных ключей
        if len(_slow_kw) < 5 or _kw_dt > min(t for t, _ in _slow_kw):
            _slow_kw.append((_kw_dt, kw))
            _slow_kw.sort(key=lambda x: -x[0])
            _slow_kw = _slow_kw[:5]

    _t_stage['grey_loop_total'] = _pf_time.perf_counter() - _t_loop_start
    _t_stage['_in_loop_tokenize'] = _t_tokenize_total
    _t_stage['_in_loop_parse_top'] = _t_parse_total
    _t_stage['_in_loop_prove_obj'] = _t_prove_obj_total
    _t_stage['_in_loop_prove_act'] = _t_prove_act_total
    _t_stage['_in_loop_prove_other'] = _t_prove_other_total

    # ── Обновление prev_result
    prev_result['keywords_grey'] = new_grey
    prev_result['keywords_grey_count'] = len(new_grey)
    prev_result['_l1_5_trace'].extend(trash_traces)
    prev_result.setdefault('_l1_5_diag', []).extend(full_diag)

    grey_n = sum(1 for t in full_diag if t['label'] == 'GREY')
    grey_soft_n = sum(1 for t in full_diag if t['label'] == 'GREY_SOFT')
    trash_n = sum(1 for t in full_diag if t['label'] == 'TRASH')
    logger.info(
        f"[L1.5/v3] {len(grey_keywords)} → GREY={grey_n}, GREY_SOFT={grey_soft_n}, TRASH={trash_n}"
    )

    # ── Финальный профиль ──────────────────────────────────────────────
    _t_stage['_total'] = _pf_time.perf_counter() - _t_total
    # Сортируем по убыванию (только positive stages, без счётчиков)
    _stage_items = [(k, v) for k, v in _t_stage.items() if not k.startswith('_') and isinstance(v, (int, float))]
    _stage_items.sort(key=lambda x: -x[1])
    _stage_str = " | ".join(f"{k}={v:.2f}s" for k, v in _stage_items)
    logger.info(f"[L1.5/stage] {_stage_str}")
    logger.info(
        f"[L1.5/stage] in_loop: tokenize={_t_tokenize_total:.2f}s "
        f"parse_top={_t_parse_total:.2f}s prove_obj={_t_prove_obj_total:.2f}s "
        f"prove_act={_t_prove_act_total:.2f}s prove_other={_t_prove_other_total:.2f}s"
    )
    if _slow_kw:
        _slow_str = " | ".join(f"{t:.2f}s:{kw!r}" for t, kw in _slow_kw)
        logger.info(f"[L1.5/slowest] {_slow_str}")

    # Сохраняем в result для UI/JSON ответа (как L0 делает с _filter_timings)
    prev_result.setdefault('_l1_5_stage_timings', {}).update({
        k: round(v, 4) for k, v in _t_stage.items()
        if not k.startswith('_') and isinstance(v, (int, float))
    })

    return prev_result
