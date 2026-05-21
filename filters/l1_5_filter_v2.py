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

try:
    from .e5_model import (
        get_e5_word_embedding as _gee,
        e5_cosine_sim as _ecs,
        get_e5_model as _gem,
    )
    get_e5_word_embedding = _gee
    e5_cosine_sim = _ecs
    get_e5_model = _gem
    _E5_IMPORT_OK = True
    logger.info("[L1.5/v3] E5 module imported via relative path")
except Exception as e_rel:
    logger.warning(f"[L1.5/v3] relative import of e5_model failed: {e_rel}")
    try:
        from e5_model import (
            get_e5_word_embedding as _gee,
            e5_cosine_sim as _ecs,
            get_e5_model as _gem,
        )
        get_e5_word_embedding = _gee
        e5_cosine_sim = _ecs
        get_e5_model = _gem
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

# ─── Тюнинг (откалибровать после первого прогона) ────────────────────────
COS_OBJECT_HIGH = 0.78    # порог cos для гипонимов object (с двойным фильтром neighbors)
COS_ACTION_HIGH = 0.85    # порог cos для синонимов action (без neighbors, выше)
COS_GAP_MIN = 0.15        # минимальная разница (cos_obj - cos_act) — отсекает атрибуты процесса
NEIGHBOR_WINDOW = 2
NEIGHBOR_MIN_FREQ = 2

# Non-content POS (фильтруем при extraction content_lemmas)
_NON_CONTENT_POS = {
    'PREP', 'CONJ', 'PRCL', 'INTJ',
    'ADVB', 'COMP', 'NUMR', 'NPRO',
}

# Global parses cache (uniq tokens per request)
_parses_cache: Dict[str, Any] = {}


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


def _get_synonyms(lemma: str) -> Set[str]:
    """Synonyms из RuWordNet (если доступен)."""
    if _rwn is None or not lemma:
        return set()
    try:
        syns = set()
        senses = _rwn.get_senses(lemma)
        for sense in senses:
            for s in sense.synset.senses:
                if s.lemma and s.lemma.lower() != lemma:
                    syns.add(s.lemma.lower())
        return syns
    except Exception:
        return set()


# ─── Разбор seed ─────────────────────────────────────────────────────────

def _extract_seed_structure(seed: str) -> Dict[str, Any]:
    """
    Возвращает:
      content_lemmas: List[str] — все content-леммы seed
      action_anchor:  Optional[str] — первая VERB/INFN или первое content NOUN
      object_anchor:  Optional[str] — последнее content NOUN (отличное от action)
      qualifier:      Optional[str] — число из seed (если есть)
    """
    tokens = _tokenize(seed)
    content_parses = []
    for tok in tokens:
        p = _parse_top(tok)
        if p is None:
            continue
        if _is_content_word(p):
            content_parses.append(p)

    content_lemmas = [p.normal_form for p in content_parses]

    # Action: первый VERB/INFN, иначе первый NOUN-кандидат
    action_anchor = None
    for p in content_parses:
        if p.tag.POS in {'VERB', 'INFN'}:
            action_anchor = p.normal_form
            break
    if action_anchor is None and content_parses:
        action_anchor = content_parses[0].normal_form

    # Object: последний NOUN ≠ action
    object_anchor = None
    for p in reversed(content_parses):
        if p.tag.POS == 'NOUN' and p.normal_form != action_anchor:
            object_anchor = p.normal_form
            break

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
    """
    if _morph is None or not object_anchor:
        return set()

    counter: Counter = Counter()
    for kw in l0_valid_keywords:
        tokens = _tokenize(kw)
        if not tokens:
            continue
        parses = [_parse_top(t) for t in tokens]
        lemmas = [p.normal_form if p else None for p in parses]

        # позиции anchor (substring или лемма)
        anchor_positions = [
            i for i, (tok, lem) in enumerate(zip(tokens, lemmas))
            if (object_anchor in tok) or (lem == object_anchor)
        ]
        if not anchor_positions:
            continue

        # уникальные леммы NOUN в окне для этого kw (избегаем двойного счёта)
        seen_in_kw: Set[str] = set()
        for pos in anchor_positions:
            for j in range(max(0, pos - window), min(len(tokens), pos + window + 1)):
                if j == pos:
                    continue
                p = parses[j]
                if p is None or p.tag.POS != 'NOUN':
                    continue
                lem = p.normal_form
                if not lem or lem == object_anchor or lem in excluded_lemmas:
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

    # 2. прямая лемма
    for lem in kw_lemmas:
        if lem == object_anchor:
            diag['method'] = 'lemma'
            diag['matched_lemma'] = object_anchor
            diag['reason'] = f'lemma:{object_anchor}'
            return True, diag

    # 3. RuWordNet synonym
    for lem in kw_lemmas:
        if lem and lem in object_synonyms:
            diag['method'] = 'ruwordnet'
            diag['matched_lemma'] = lem
            diag['reason'] = f'ruwordnet:{lem}'
            return True, diag

    # 4. E5 hyponym с GAP-тестом.
    # Для каждого NOUN-кандидата считаем cos и к object_anchor и к action_anchor.
    # Гипоним object имеет ВЫСОКИЙ cos_obj и НИЗКИЙ cos_act → большой gap.
    # Атрибут процесса (цена, отзыв, оплата) имеет высокий cos к обоим → маленький gap → skip.
    # BYPASS: если лемма из seed_content_lemmas — принимаем сразу (не атрибут seed-юзера).
    anchor_emb = get_e5_word_embedding(object_anchor)
    action_emb = get_e5_word_embedding(action_anchor) if action_anchor else None

    if anchor_emb is not None:
        best_score = -1.0  # ранжируем по gap
        best_lem_overall = None
        best_cos_obj = 0.0
        best_cos_act = 0.0

        for p, lem in zip(kw_parses, kw_lemmas):
            if p is None or lem is None:
                continue
            if p.tag.POS != 'NOUN':
                continue
            if lem == object_anchor or lem in excluded_lemmas:
                continue
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

            # BYPASS для лемм из seed: принимаем независимо от gap
            if in_seed and cos_obj >= COS_OBJECT_HIGH:
                if cos_obj > best_score:
                    best_score = cos_obj
                    best_lem_overall = lem
                    best_cos_obj = cos_obj
                    best_cos_act = cos_act
                continue

            # Обычная проверка: cos≥порог, в neighbors, gap≥COS_GAP_MIN
            if not in_n:
                continue
            if cos_obj < COS_OBJECT_HIGH:
                continue
            if gap < COS_GAP_MIN:
                continue

            if gap > best_score:
                best_score = gap
                best_lem_overall = lem
                best_cos_obj = cos_obj
                best_cos_act = cos_act

        if best_lem_overall:
            diag['best_cos'] = round(best_cos_obj, 3)
            diag['best_cos_act'] = round(best_cos_act, 3)
            diag['best_gap'] = round(best_cos_obj - best_cos_act, 3)
            diag['method'] = 'hyponym'
            diag['matched_lemma'] = best_lem_overall
            diag['reason'] = (
                f'hyponym:{best_lem_overall}'
                f'(obj={best_cos_obj:.2f},act={best_cos_act:.2f},gap={best_cos_obj-best_cos_act:+.2f})'
            )
            return True, diag

    # 5. abbrev_pos0 — короткое NOUN на pos 0 + в neighbors + gap-тест против action
    # (без gap может пропустить атрибуты типа "цена" в ключе "цены доставки").
    if kw_parses and kw_parses[0] is not None:
        p0 = kw_parses[0]
        lem0 = kw_lemmas[0]
        if p0.tag.POS == 'NOUN' and lem0 and len(kw_tokens[0]) <= 4:
            if lem0 in object_neighbors and lem0 not in excluded_lemmas:
                # gap-тест: проверяем что лемма ближе к object чем к action
                cand_emb0 = get_e5_word_embedding(lem0)
                if cand_emb0 is not None and anchor_emb is not None:
                    cos_obj0 = e5_cosine_sim(anchor_emb, cand_emb0)
                    cos_act0 = (e5_cosine_sim(action_emb, cand_emb0)
                                if action_emb is not None else 0.0)
                    if (cos_obj0 - cos_act0) >= COS_GAP_MIN or lem0 in seed_content_lemmas:
                        diag['method'] = 'abbrev_pos0'
                        diag['matched_lemma'] = lem0
                        diag['reason'] = (
                            f'abbrev_pos0:{lem0}'
                            f'(obj={cos_obj0:.2f},act={cos_act0:.2f})'
                        )
                        return True, diag
                else:
                    # без E5 — старая логика (но это редко: E5 должен работать)
                    diag['method'] = 'abbrev_pos0'
                    diag['matched_lemma'] = lem0
                    diag['reason'] = f'abbrev_pos0:{lem0}(no_e5)'
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

    # 2. прямая лемма
    for lem in kw_lemmas:
        if lem == action_anchor:
            diag['method'] = 'lemma'
            diag['matched_lemma'] = action_anchor
            diag['reason'] = f'lemma:{action_anchor}'
            return True, diag

    # 3. RuWordNet synonym
    for lem in kw_lemmas:
        if lem and lem in action_synonyms:
            diag['method'] = 'ruwordnet'
            diag['matched_lemma'] = lem
            diag['reason'] = f'ruwordnet:{lem}'
            return True, diag

    # 4. E5 synonym (cos≥COS_ACTION_HIGH, без neighbors)
    anchor_emb = get_e5_word_embedding(action_anchor)
    if anchor_emb is not None:
        best_cos = 0.0
        best_lem = None
        for p, lem in zip(kw_parses, kw_lemmas):
            if p is None or lem is None:
                continue
            if p.tag.POS not in {'NOUN', 'VERB', 'INFN'}:
                continue
            if lem == action_anchor:
                continue
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
    for lem in kw_lemmas:
        if lem == lemma:
            return True, f'lemma:{lemma}'
    for lem in kw_lemmas:
        if lem and lem in synonyms:
            return True, f'ruwordnet:{lem}'
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

    # ── E5 warmup + диагностика. Грузим модель один раз ДО прогона.
    # Без warmup — chicken-egg, модель не загрузится никогда.
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
    logger.info(f"[L1.5/v3] E5 status: {e5_status}")
    logger.info(f"[L1.5/v3] RuWordNet status: {'OK' if _rwn is not None else 'DISABLED'}")

    # ── Парсинг seed
    seed_struct = _extract_seed_structure(seed)
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
    l0_valid = prev_result.get('keywords', []) or []
    object_neighbors = (
        _build_object_neighbors(l0_valid, object_anchor, excluded_lemmas)
        if object_anchor else set()
    )
    object_synonyms = _get_synonyms(object_anchor) if object_anchor else set()
    action_synonyms = _get_synonyms(action_anchor) if action_anchor else set()
    other_synonyms = {lem: _get_synonyms(lem) for lem in other_lemmas}

    logger.info(
        f"[L1.5/v3] neighbors({object_anchor})={len(object_neighbors)} "
        f"obj_syn={len(object_synonyms)} act_syn={len(action_synonyms)}"
    )

    # ── Прогон GREY.
    # _l1_5_trace — только TRASH (для UI, чтобы GREY не показывались как заблокированные).
    # _l1_5_diag  — полный диагностический trace по всем ключам (для калибровки порогов).
    new_grey: List[str] = []
    trash_traces: List[dict] = []
    full_diag: List[dict] = []

    for kw in grey_keywords:
        tokens = _tokenize(kw)
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

        parses = [_parse_top(t) for t in tokens]
        lemmas = [p.normal_form if p else None for p in parses]

        # ОСИ
        obj_ok, obj_diag = _prove_object(
            tokens, lemmas, parses, object_anchor, object_neighbors, object_synonyms,
            excluded_lemmas, action_anchor, set(content_lemmas)
        )
        act_ok, act_diag = _prove_action(
            tokens, lemmas, parses, action_anchor, action_synonyms
        )

        # OTHER (для 3+ word seeds) — без cos, просто substring/lemma/synonym
        other_results: List[Tuple[bool, str]] = []
        for lem in other_lemmas:
            ok, reason = _prove_other_lemma(lem, tokens, lemmas, other_synonyms.get(lem, set()))
            other_results.append((ok, reason))
        all_other_ok = all(ok for ok, _ in other_results)
        other_reasons = [r for _, r in other_results]

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

    return prev_result
