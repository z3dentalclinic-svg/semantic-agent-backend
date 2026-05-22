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

# ─── RuWordNet (optional) ────────────────────────────────────────────────
# Пакет ruwordnet>=0.0.4 НЕ содержит БД в комплекте. БД нужно скачивать
# отдельно командой `python -m ruwordnet download`, которая качает файл в
# `<package>/static/ruwordnet-2021.db`. Эта папка пересоздаётся при каждом
# деплое Render → download пришлось бы повторять. Поэтому качаем БД сами в
# persistent disk `/var/data/models/`, при первом старте сервиса.

_RWN_DB_DIR = "/var/data/models"
_RWN_DB_PATH = f"{_RWN_DB_DIR}/ruwordnet-2021.db"
_RWN_DB_URL = (
    "https://github.com/avidale/python-ruwordnet/releases/download/"
    "0.0.4/ruwordnet-2021.db"
)


def _ensure_ruwordnet_db() -> bool:
    """
    Гарантирует наличие БД RuWordNet на persistent disk.
    Возвращает True если файл готов к использованию.
    """
    import os
    import urllib.request

    if os.path.exists(_RWN_DB_PATH):
        try:
            size_mb = os.path.getsize(_RWN_DB_PATH) / 1e6
            logger.info(f"[L1.5/v3] RuWordNet DB found at {_RWN_DB_PATH} ({size_mb:.1f} MB)")
        except Exception:
            pass
        return True

    try:
        os.makedirs(_RWN_DB_DIR, exist_ok=True)
    except Exception as e:
        logger.error(f"[L1.5/v3] Cannot create dir {_RWN_DB_DIR}: {e}")
        return False

    tmp_path = _RWN_DB_PATH + ".tmp"
    try:
        logger.info(f"[L1.5/v3] RuWordNet DB not found, downloading from {_RWN_DB_URL}")
        # Тот же подход что и в `python -m ruwordnet download` (urlretrieve).
        # Сохраняем во временный файл и атомарно переименовываем — если
        # download прерван, повреждённый файл не останется как валидный.
        urllib.request.urlretrieve(_RWN_DB_URL, tmp_path)
        size_mb = os.path.getsize(tmp_path) / 1e6
        os.rename(tmp_path, _RWN_DB_PATH)
        logger.info(f"[L1.5/v3] RuWordNet DB downloaded: {_RWN_DB_PATH} ({size_mb:.1f} MB)")
        return True
    except Exception as e:
        logger.error(f"[L1.5/v3] Failed to download RuWordNet DB: {e}")
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return False


def _init_ruwordnet():
    """Возвращает экземпляр RuWordNet или None (если БД не доступна)."""
    try:
        from ruwordnet import RuWordNet
    except ImportError as e:
        logger.warning(f"[L1.5/v3] RuWordNet package not installed: {e}")
        return None

    if not _ensure_ruwordnet_db():
        return None

    try:
        rwn = RuWordNet(filename_or_session=_RWN_DB_PATH)
        # Sanity-check: пробуем простой запрос. Если БД повреждена — здесь упадёт.
        _ = rwn.get_senses("тест")
        logger.info(f"[L1.5/v3] RuWordNet loaded from {_RWN_DB_PATH}")
        return rwn
    except Exception as e:
        logger.error(f"[L1.5/v3] RuWordNet init failed: {e}")
        return None


_rwn = _init_ruwordnet()

# ─── E5 model — НЕ молча глотаем ошибки импорта ─────────────────────────
_E5_IMPORT_OK = False
get_e5_word_embedding = None
e5_cosine_sim = None
get_e5_model = None
warm_e5_word_cache = None

try:
    from .e5_model import (
        get_e5_word_embedding as _gee,
        e5_cosine_sim as _ecs,
        get_e5_model as _gem,
        warm_e5_word_cache as _wwc,
    )
    get_e5_word_embedding = _gee
    e5_cosine_sim = _ecs
    get_e5_model = _gem
    warm_e5_word_cache = _wwc
    _E5_IMPORT_OK = True
    logger.info("[L1.5/v3] E5 module imported via relative path")
except Exception as e_rel:
    logger.warning(f"[L1.5/v3] relative import of e5_model failed: {e_rel}")
    try:
        from e5_model import (
            get_e5_word_embedding as _gee,
            e5_cosine_sim as _ecs,
            get_e5_model as _gem,
            warm_e5_word_cache as _wwc,
        )
        get_e5_word_embedding = _gee
        e5_cosine_sim = _ecs
        get_e5_model = _gem
        warm_e5_word_cache = _wwc
        _E5_IMPORT_OK = True
        logger.info("[L1.5/v3] E5 module imported via absolute path")
    except Exception as e_abs:
        logger.error(f"[L1.5/v3] absolute import of e5_model also failed: {e_abs}")
        logger.error("[L1.5/v3] E5 unavailable — semantic axes will use only substring/lemma/ruwordnet/neighbors")

        def get_e5_word_embedding(w):
            return None

        def e5_cosine_sim(a, b):
            return 0.0

        def get_e5_model():
            return None

        def warm_e5_word_cache(words, batch_size=64):
            return 0

# ─── Тюнинг (откалибровать после первого прогона) ────────────────────────
COS_OBJECT_HIGH = 0.78    # порог cos для гипонимов object (с двойным фильтром neighbors)
COS_ACTION_HIGH = 0.88    # порог cos для синонимов action (без neighbors, выше).
                          # Был 0.85 — пропускал "купить", "вокзал", "фото"
                          # как синонимы "доставка" в группе D FP.
                          # 0.90 был слишком строгим (терял "посылка"=0.882,
                          # "заказ"=0.899). 0.88 возвращает их, сохраняя
                          # отсечение "купить"=0.861, "вокзал"=0.850, "дом"=0.868.
COS_GAP_MIN = 0.05        # мин разница (cos_obj - cos_act) для метода 4 object.
                          # Был 0.15 — убивал все валидные гипонимы (роза gap=0.029,
                          # букет gap=0.091, тюльпан gap=0.035). Сейчас розы/тюльпаны
                          # ловятся через RuWordNet (метод 3), а в методе 4 0.05
                          # отсекает коммерческие FP (цена -0.029, отзыв -0.014,
                          # одесса -0.028, дом -0.030, заказ -0.060).
MIN_OBJECT_LEMMA_LEN = 3  # минимальная длина леммы object-кандидата в методе 4.
                          # Защита от предлогов-омонимов: pymorphy парсит 'в'/'с'
                          # как буквы алфавита NOUN → попадали в кандидаты.
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


def _get_synonyms(lemma: str) -> Set[str]:
    """Лексика близкая к лемме из RuWordNet (если доступен).

    Берём:
    1. Synonyms — леммы того же synset (доставка ↔ привоз)
    2. Hyponyms — дочерние synset (цветок → роза, тюльпан, букет, эустома)
    3. POS-synonyms — мост между частями речи (доставка ↔ доставлять)
    4. Derivations — словообразовательные пары на уровне sense
       (доставлять → доставка, доставщик)

    Расширение synset.senses одного синсета само по себе бесполезно для нашей
    задачи (для 'цветок' оно даёт {цвет} — близкий smysl, но не гипонимы).
    Главную ценность дают hyponyms.
    """
    if _rwn is None or not lemma:
        return set()
    try:
        syns: Set[str] = set()
        senses = _rwn.get_senses(lemma)
        for sense in senses:
            # 1. Synonyms (одного synset)
            for s in sense.synset.senses:
                if s.lemma and s.lemma.lower() != lemma:
                    syns.add(s.lemma.lower())
            # 2. Hyponyms (дочерние synset — роза для цветок)
            for hypo_synset in sense.synset.hyponyms:
                for s in hypo_synset.senses:
                    if s.lemma:
                        syns.add(s.lemma.lower())
            # 3. POS-synonyms (мост между частями речи)
            for ps_synset in sense.synset.pos_synonyms:
                for s in ps_synset.senses:
                    if s.lemma and s.lemma.lower() != lemma:
                        syns.add(s.lemma.lower())
            # 4. Derivations (sense-level словообразование)
            for deriv in sense.derivations:
                if deriv.lemma and deriv.lemma.lower() != lemma:
                    syns.add(deriv.lemma.lower())
        return syns
    except Exception as e:
        logger.warning(f"[L1.5/v3] _get_synonyms({lemma!r}) failed: {e}")
        return set()


def _has_verb_derivation(lemma: str) -> bool:
    """Есть ли у леммы VERB/INFN-производное в RuWordNet?

    Используется для определения action-noun: отглагольные существительные
    (доставка, ремонт, продажа) имеют verb-counterpart, природные предметы
    (цветок, квартира) — нет.

    Возвращает False если RuWordNet недоступен → caller использует fallback.
    """
    if _rwn is None or not lemma:
        return False
    try:
        senses = _rwn.get_senses(lemma)
        for sense in senses:
            # Проверяем POS-synonyms (synset мостит части речи)
            for ps_synset in sense.synset.pos_synonyms:
                pos = (ps_synset.part_of_speech or '').upper()
                if pos.startswith('V'):  # V / VERB
                    return True
            # Проверяем sense-level derivations
            for deriv in sense.derivations:
                if deriv.synset is None:
                    continue
                pos = (deriv.synset.part_of_speech or '').upper()
                if pos.startswith('V'):
                    return True
        return False
    except Exception:
        return False


# ─── Разбор seed ─────────────────────────────────────────────────────────

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

    if _rwn is not None:
        # Собираем NOUN-парсы найденные в RWN с числом их hyponyms
        rwn_noun_candidates: List[Tuple[Any, int]] = []
        seen_lemmas: Set[str] = set()
        for p in content_parses:
            if p.tag.POS != 'NOUN':
                continue
            if p.normal_form in seen_lemmas:
                continue
            seen_lemmas.add(p.normal_form)
            try:
                senses = _rwn.get_senses(p.normal_form)
            except Exception:
                continue
            if not senses:
                continue
            # Считаем суммарное число hyponyms по всем sense'ам
            hypo_count = 0
            try:
                for sense in senses:
                    for hypo in sense.synset.hyponyms:
                        hypo_count += len(hypo.senses)
            except Exception:
                pass
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
        # 2. Action = NOUN с verb-derivation (отглагольный), если RWN доступен.
        # Если несколько кандидатов с derivation — берём первый по позиции.
        if _rwn is not None:
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
        if _rwn is not None:
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
) -> Tuple[bool, dict]:
    """
    Возвращает (proven, diag).
    diag = {
        'method': 'substring|lemma|ruwordnet|hyponym|abbrev_pos0|none',
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
) -> Tuple[bool, dict]:
    """Возвращает (proven, diag). Структура diag — как у _prove_object."""
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

    # 4. E5 synonym (cos≥COS_ACTION_HIGH, без neighbors)
    anchor_emb = get_e5_word_embedding(action_anchor)
    if anchor_emb is not None:
        best_cos = 0.0
        best_lem = None
        # Кандидаты — все леммы NOUN/VERB/INFN среди парсов токенов
        cand_lemmas: Set[str] = set()
        for tok in kw_tokens:
            for lem in _token_lemmas(tok, pos_filter={'NOUN', 'VERB', 'INFN'}):
                if lem != action_anchor:
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
    logger.info(f"[L1.5/v3] RuWordNet status: {'OK' if _rwn is not None else 'DISABLED'}")

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
    object_synonyms = _get_synonyms(object_anchor) if object_anchor else set()
    _t_stage['rwn_obj_syn'] = _pf_time.perf_counter() - _t

    _t = _pf_time.perf_counter()
    action_synonyms = _get_synonyms(action_anchor) if action_anchor else set()
    _t_stage['rwn_act_syn'] = _pf_time.perf_counter() - _t

    _t = _pf_time.perf_counter()
    other_synonyms = {lem: _get_synonyms(lem) for lem in other_lemmas}
    _t_stage['rwn_other_syn'] = _pf_time.perf_counter() - _t

    logger.info(
        f"[L1.5/v3] neighbors({object_anchor})={len(object_neighbors)} "
        f"obj_syn={len(object_synonyms)} act_syn={len(action_synonyms)}"
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
        # все NOUN/VERB/INFN-леммы из всех keywords (через омонимы)
        for kw in grey_keywords:
            for tok in _tokenize(kw):
                # для object-кандидатов нужны NOUN, для action — NOUN+VERB+INFN
                for lem in _token_lemmas(tok, pos_filter={'NOUN', 'VERB', 'INFN'}):
                    if len(lem) >= MIN_OBJECT_LEMMA_LEN:
                        words_to_warm.add(lem)
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

        # QUALIFIER_HARD
        if qualifier and qualifier not in tokens:
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
            excluded_lemmas, action_anchor, set(content_lemmas)
        )
        _t_prove_obj_total += _pf_time.perf_counter() - _t

        _t = _pf_time.perf_counter()
        act_ok, act_diag = _prove_action(
            tokens, lemmas, parses, action_anchor, action_synonyms
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
    trash_n = sum(1 for t in full_diag if t['label'] == 'TRASH')
    logger.info(
        f"[L1.5/v3] {len(grey_keywords)} → GREY={grey_n}, TRASH={trash_n}"
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
