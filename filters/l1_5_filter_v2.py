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
try:
    from ruwordnet import RuWordNet
    _rwn = RuWordNet()
except Exception:
    _rwn = None

# ─── E5 model ────────────────────────────────────────────────────────────
try:
    from .e5_model import get_e5_word_embedding, e5_cosine_sim, is_e5_loaded
except Exception:
    try:
        from e5_model import get_e5_word_embedding, e5_cosine_sim, is_e5_loaded
    except Exception:
        def get_e5_word_embedding(w): return None
        def e5_cosine_sim(a, b): return 0.0
        def is_e5_loaded(): return False

# ─── Тюнинг (откалибровать после первого прогона) ────────────────────────
COS_OBJECT_HIGH = 0.78    # порог cos для гипонимов object (с двойным фильтром neighbors)
COS_ACTION_HIGH = 0.85    # порог cos для синонимов action (без neighbors, выше)
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
    window: int = NEIGHBOR_WINDOW,
    min_freq: int = NEIGHBOR_MIN_FREQ,
) -> Set[str]:
    """
    Леммы NOUN которые встречались в окне ±window от object_anchor
    минимум в min_freq L0_VALID ключах.
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
                if not lem or lem == object_anchor:
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
) -> Tuple[bool, str]:
    """Возвращает (proven, reason). Если object_anchor=None — ось не нужна."""
    if not object_anchor:
        return True, 'no_object_in_seed'

    kw_low = ' '.join(kw_tokens)

    # 1. substring
    if object_anchor in kw_low:
        return True, f'substring:{object_anchor}'

    # 2. прямая лемма
    for lem in kw_lemmas:
        if lem == object_anchor:
            return True, f'lemma:{object_anchor}'

    # 3. RuWordNet synonym
    for lem in kw_lemmas:
        if lem and lem in object_synonyms:
            return True, f'ruwordnet:{lem}'

    # 4. E5 hyponym (cos≥0.78 + в neighbors)
    if is_e5_loaded() and object_neighbors:
        anchor_emb = get_e5_word_embedding(object_anchor)
        if anchor_emb is not None:
            best_cos = 0.0
            best_lem = None
            for p, lem in zip(kw_parses, kw_lemmas):
                if p is None or lem is None:
                    continue
                if p.tag.POS != 'NOUN':
                    continue
                if lem == object_anchor or lem not in object_neighbors:
                    continue
                cand_emb = get_e5_word_embedding(lem)
                if cand_emb is None:
                    continue
                cos = e5_cosine_sim(anchor_emb, cand_emb)
                if cos > best_cos:
                    best_cos = cos
                    best_lem = lem
            if best_lem and best_cos >= COS_OBJECT_HIGH:
                return True, f'hyponym:{best_lem}({best_cos:.2f})'

    # 5. abbrev_pos0: NOUN ≤4 буквы на pos 0 + в neighbors (slot match для аббревиатур)
    if kw_parses and kw_parses[0] is not None:
        p0 = kw_parses[0]
        lem0 = kw_lemmas[0]
        if p0.tag.POS == 'NOUN' and lem0 and len(kw_tokens[0]) <= 4:
            if lem0 in object_neighbors:
                return True, f'abbrev_pos0:{lem0}'

    return False, 'no_object_proof'


def _prove_action(
    kw_tokens: List[str],
    kw_lemmas: List[Optional[str]],
    kw_parses: List[Any],
    action_anchor: Optional[str],
    action_synonyms: Set[str],
) -> Tuple[bool, str]:
    """Возвращает (proven, reason). Если action_anchor=None — ось не нужна."""
    if not action_anchor:
        return True, 'no_action_in_seed'

    kw_low = ' '.join(kw_tokens)

    # 1. substring
    if action_anchor in kw_low:
        return True, f'substring:{action_anchor}'

    # 2. прямая лемма
    for lem in kw_lemmas:
        if lem == action_anchor:
            return True, f'lemma:{action_anchor}'

    # 3. RuWordNet synonym
    for lem in kw_lemmas:
        if lem and lem in action_synonyms:
            return True, f'ruwordnet:{lem}'

    # 4. E5 synonym (cos≥0.85, без neighbors — higher bar)
    if is_e5_loaded():
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
                if cos > best_cos:
                    best_cos = cos
                    best_lem = lem
            if best_lem and best_cos >= COS_ACTION_HIGH:
                return True, f'action_syn:{best_lem}({best_cos:.2f})'

    return False, 'no_action_proof'


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

    logger.info(
        f"[L1.5/v3] seed={seed!r} action={action_anchor!r} object={object_anchor!r} "
        f"other={other_lemmas} qualifier={qualifier!r}"
    )

    # ── Подготовка ресурсов
    l0_valid = prev_result.get('keywords', []) or []
    object_neighbors = _build_object_neighbors(l0_valid, object_anchor) if object_anchor else set()
    object_synonyms = _get_synonyms(object_anchor) if object_anchor else set()
    action_synonyms = _get_synonyms(action_anchor) if action_anchor else set()
    other_synonyms = {lem: _get_synonyms(lem) for lem in other_lemmas}

    logger.info(
        f"[L1.5/v3] neighbors({object_anchor})={len(object_neighbors)} "
        f"obj_syn={len(object_synonyms)} act_syn={len(action_synonyms)}"
    )

    # ── Прогон GREY
    new_grey: List[str] = []
    grey_promoted_traces: List[dict] = []
    trash_traces: List[dict] = []

    for kw in grey_keywords:
        tokens = _tokenize(kw)
        if not tokens:
            trash_traces.append({
                'keyword': kw, 'label': 'TRASH', 'decided_by': 'l1_5_v3',
                'reason': 'empty_tokens', 'signals': [],
            })
            continue

        # QUALIFIER_HARD
        if qualifier and qualifier not in tokens:
            trash_traces.append({
                'keyword': kw, 'label': 'TRASH', 'decided_by': 'l1_5_v3',
                'reason': f'qualifier_missing:{qualifier}', 'signals': [],
            })
            continue

        parses = [_parse_top(t) for t in tokens]
        lemmas = [p.normal_form if p else None for p in parses]

        # ОСИ
        obj_ok, obj_reason = _prove_object(
            tokens, lemmas, parses, object_anchor, object_neighbors, object_synonyms
        )
        act_ok, act_reason = _prove_action(
            tokens, lemmas, parses, action_anchor, action_synonyms
        )

        # OTHER (для 3+ word seeds)
        other_results: List[Tuple[bool, str]] = []
        for lem in other_lemmas:
            ok, reason = _prove_other_lemma(lem, tokens, lemmas, other_synonyms.get(lem, set()))
            other_results.append((ok, reason))

        all_other_ok = all(ok for ok, _ in other_results)

        if obj_ok and act_ok and all_other_ok:
            new_grey.append(kw)
            grey_promoted_traces.append({
                'keyword': kw, 'label': 'GREY', 'decided_by': 'l1_5_v3',
                'reason': 'all_axes_proven',
                'signals': [f'obj={obj_reason}', f'act={act_reason}']
                          + [f'other={r}' for _, r in other_results],
            })
        else:
            failed: List[str] = []
            if not obj_ok:
                failed.append(f'obj={obj_reason}')
            if not act_ok:
                failed.append(f'act={act_reason}')
            for ok, reason in other_results:
                if not ok:
                    failed.append(f'other={reason}')
            trash_traces.append({
                'keyword': kw, 'label': 'TRASH', 'decided_by': 'l1_5_v3',
                'reason': 'axis_unproven', 'signals': failed,
            })

    # ── Обновление prev_result
    prev_result['keywords_grey'] = new_grey
    prev_result['keywords_grey_count'] = len(new_grey)
    prev_result['_l1_5_trace'].extend(grey_promoted_traces)
    prev_result['_l1_5_trace'].extend(trash_traces)

    logger.info(
        f"[L1.5/v3] {len(grey_keywords)} → GREY={len(new_grey)}, TRASH={len(trash_traces)}"
    )

    return prev_result
