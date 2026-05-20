"""
L1.5 v2 — Semantic Anchor Filter с инвертированной логикой.

ВАЖНО: этот фильтр использует E5-large (intfloat/multilingual-e5-large) — 
отдельную модель, более качественную чем MiniLM. Остальные фильтры (L0, L2, 
CategoryMismatch) продолжают использовать MiniLM через shared_model.py.

АРХИТЕКТУРА:
  Default = TRASH. Ключ должен ДОКАЗАТЬ свою валидность через сигналы.

ШАГИ:
  1. HARD GATES — мгновенный TRASH:
     - QUALIFIER_HARD: если seed имеет число и его нет в kw
     - SEED_LEMMA_HARD: если в kw нет ни одной content-леммы seed 
       И нет substring object_anchor
  
  2. HARD PASS — мгновенный VALID:
     - Все seed-content-леммы присутствуют + есть action
  
  3. EVIDENCE-BASED для borderline кейсов:
     - 3-class классификация L0_VALID лемм через slot_ratio + dispersion:
       CORE_OBJECT / GENERIC_CONTEXT / TAIL_NOISE
     - Сбор strong/weak сигналов для каждого NOUN-кандидата
     - Adaptive policy по signal_density (broad vs tech domain)
  
  4. РЕШЕНИЕ:
     - 2+ strong signals + action → VALID
     - 1 strong + 1 weak + action → GREY (борделайн → L3)
     - Иначе → TRASH

ПОРОГИ для E5-large:
  E5 даёт значения в районе 0.75-0.95 для семантически близких пар.
  Это ВЫШЕ чем MiniLM (0.4-0.6). Поэтому пороги COS_HIGH/MID повышены.
"""

from __future__ import annotations
import logging
import re
import time
from collections import Counter, defaultdict
from typing import Optional

import pymorphy3
import numpy as np

# E5-large — отдельная модель для L1.5
from .e5_model import get_e5_word_embedding, e5_cosine_sim

logger = logging.getLogger(__name__)
morph = pymorphy3.MorphAnalyzer(lang='ru')


# ────────────────────────────────────────────────────────────────────────────
# Константы
# ────────────────────────────────────────────────────────────────────────────

_STOPWORDS = {
    'купить', 'заказать', 'цена', 'стоимость', 'отзывы', 'продажа', 'доставка',
    'в', 'на', 'с', 'и', 'или', 'без', 'для', 'под', 'через', 'у', 'к', 'от',
    'из', 'о', 'об', 'как', 'где', 'почему', 'что', 'это', 'до', 'по', 'за',
    'про', 'со', 'но', 'же', 'ли', 'бы', 'не', 'ни', 'весь', 'все', 'всё',
}

# Предлоги исключаются из seed_content_lemmas
_PREP_POS = {'PREP', 'CONJ', 'PRCL', 'INTJ'}

# L0 positive signals которые мы используем как weak booster.
# ИСКЛЮЧАЕМ geo — он шумит (например 'доставка подарков на дом киев' получает geo)
_USEFUL_L0_SIGNALS = {
    'commerce', 'action', 'brand', 'reputation', 'info_intent',
    'contacts', 'location', 'type_spec', 'verb_modifier', 'conjunctive',
}

# Пороги для 3-class классификации
SLOT_RATIO_CORE = 0.35       # лемма в object-slot >=35% — CORE
DISPERSION_GENERIC = 3       # лемма в 3+ разных контекстах — GENERIC

# Пороги MiniLM cos — старые значения для справки
# MiniLM:  COS_HIGH = 0.55, COS_MID = 0.40, COS_ACTION_SYN = 0.55
# 
# E5-large даёт значения значительно выше для близких пар:
# - Истинные синонимы (мопед/скутер): ожидаем 0.85-0.92
# - Гипонимы (роза/цветок): ожидаем 0.78-0.88
# - Связанные слова (дом/доставка): ожидаем 0.70-0.80
# - Несвязанные: 0.60-0.70 (E5 даёт высокий baseline для всех русских слов)
#
# Поэтому пороги для E5 поднимаем. Будем калибровать на проде.
COS_HIGH = 0.82              # сильный сигнал — синоним
COS_MID = 0.75               # слабый сигнал — связанное слово
COS_ACTION_SYN = 0.82        # для action синонимов

# Adaptive policy
SIGNAL_DENSITY_BROAD = 0.10  # >=10% L0 positive — broad domain

# Position fallback
MAX_ABBREV_LEN = 4           # короткое слово ≤4 буквы — кандидат на аббревиатуру

# Финальные пороги для решения
STRONG_FOR_VALID = 2         # 2+ strong → VALID
WEAK_FOR_GREY = 2            # 1 strong + 1 weak (или 2 weak) → GREY


# ────────────────────────────────────────────────────────────────────────────
# Утилиты
# ────────────────────────────────────────────────────────────────────────────

def extract_lemmas(text: str, exclude: Optional[set] = None) -> tuple[list[tuple[str, str, str, int]], set[str]]:
    """
    Парсит kw, возвращает:
      positions: [(lemma, POS, word, index)] — позиции и леммы
      lemmas: set всех лемм (для быстрых проверок in)
    """
    if exclude is None:
        exclude = set()
    
    text_low = text.lower() if isinstance(text, str) else ''
    words = re.findall(r'[а-яёa-z]+', text_low)
    
    positions = []
    lemmas = set()
    for idx, w in enumerate(words):
        if len(w) <= 2:
            continue
        p = morph.parse(w)[0]
        lemma = p.normal_form
        pos = str(p.tag.POS) if p.tag.POS else ''
        positions.append((lemma, pos, w, idx))
        if w not in exclude:
            lemmas.add(lemma)
    
    return positions, lemmas


def extract_seed_content_lemmas(seed: str) -> set[str]:
    """
    Леммы content-слов seed: NOUN/INFN/VERB/ADJF/PRTF, исключая стопворды/предлоги.
    """
    result = set()
    for sw in seed.lower().split():
        if sw in _STOPWORDS or len(sw) <= 2:
            continue
        p = morph.parse(sw)[0]
        if p.tag.POS and p.tag.POS not in _PREP_POS and p.tag.POS != 'NUMR':
            result.add(p.normal_form)
    return result


def extract_qualifier(seed: str) -> tuple[Optional[str], list[str]]:
    """
    Извлекает qualifier (число) и его текстовые формы.
    """
    seed_low = seed.lower()
    m = re.search(r'(?<!\d)(\d+)(?!\d)', seed_low)
    if not m:
        return None, []
    return m.group(1), []


def extract_object_anchor(seed: str) -> Optional[str]:
    """
    Object anchor = последний NOUN в seed.
    """
    result = None
    for sw in seed.lower().split():
        if sw in _STOPWORDS or len(sw) <= 2:
            continue
        p = morph.parse(sw)[0]
        if p.tag.POS == 'NOUN':
            result = p.normal_form
    return result


def extract_action_anchor(seed: str) -> tuple[Optional[str], Optional[str]]:
    """
    Action anchor = первое значимое слово seed (NOUN/INFN/VERB).
    action_root = первые 5 символов action_anchor.
    """
    for sw in seed.lower().split():
        if sw in _STOPWORDS or len(sw) <= 2:
            continue
        p = morph.parse(sw)[0]
        if p.tag.POS in ('NOUN', 'INFN', 'VERB'):
            return p.normal_form, p.normal_form[:5]
    return None, None


# ────────────────────────────────────────────────────────────────────────────
# 3-class классификация лемм
# ────────────────────────────────────────────────────────────────────────────

def classify_lemmas(
    l0_valid_kws: list[str],
    seed_words: set,
    object_anchor: str,
) -> tuple[set[str], set[str], dict]:
    """
    Классифицирует леммы из L0_VALID на 3 класса:
      CORE_OBJECT — лемма в object-slot часто (slot_ratio >= SLOT_RATIO_CORE)
                   И dispersion низкая (< DISPERSION_GENERIC)
      GENERIC_CONTEXT — высокая dispersion, низкий slot_ratio
      TAIL_NOISE — всё остальное (одна встреча, единичный slot)
    
    Object-slot определяем как:
      - Позиция следующая после object_anchor (если object в kw)
      - Позиция аналогичная object_anchor по индексу в seed-структуре
    
    Возвращает:
      core_object: set лемм-кандидатов в object
      generic: set generic слов (для штрафа)
      stats: dict с метриками для логирования
    """
    # Считаем для каждой леммы:
    # - total_count: всего встреч
    # - object_slot_count: встреч в позиции object 
    # - distinct_positions: уникальные индексы позиций (нормированных)
    # - distinct_heads: уникальные соседи слева
    
    total = Counter()
    object_slot = Counter()
    positions_set = defaultdict(set)  # lemma → {норм_позиция}
    heads_set = defaultdict(set)      # lemma → {лемма_слева}
    
    for kw in l0_valid_kws:
        positions, _ = extract_lemmas(kw)
        if not positions:
            continue
        # max_idx — максимальный исходный индекс среди positions (для определения 
        # "последней позиции в kw"). idx это позиция в исходном kw, не в positions.
        max_idx = max(idx for _, _, _, idx in positions)
        
        # Найти anchor-позиции (где находится object_anchor или substring)
        anchor_indices = [
            idx for lemma, pos, w, idx in positions
            if object_anchor and (lemma == object_anchor or lemma.startswith(object_anchor)
                                  or object_anchor in lemma)
        ]
        
        for lemma, pos, w, idx in positions:
            if w in seed_words or w in _STOPWORDS or len(w) <= 2:
                continue
            if pos != 'NOUN':
                continue
            
            total[lemma] += 1
            positions_set[lemma].add(idx)
            
            # Объект слот = позиция РЯДОМ с anchor (соседи)
            for a_idx in anchor_indices:
                if abs(idx - a_idx) <= 1 and idx != a_idx:
                    object_slot[lemma] += 1
                    break
            
            # Если в kw нет anchor — лемма может занимать позицию object 
            # сама по себе. Считаем object_slot если лемма на последней позиции.
            if not anchor_indices and idx == max_idx:
                object_slot[lemma] += 1
            
            # Head = соседняя лемма слева (по idx в kw, не индексу списка)
            if idx > 0:
                for other_lemma, _, _, other_idx in positions:
                    if other_idx == idx - 1:
                        heads_set[lemma].add(other_lemma)
                        break
    
    core_object = set()
    generic_context = set()
    tail_noise = set()
    
    metrics = {}
    
    for lemma, cnt in total.items():
        if cnt < 2:
            tail_noise.add(lemma)
            continue
        slot_ratio = object_slot[lemma] / cnt if cnt else 0
        dispersion = len(positions_set[lemma]) + len(heads_set[lemma])
        
        metrics[lemma] = {
            'total': cnt,
            'object_slot': object_slot[lemma],
            'slot_ratio': round(slot_ratio, 2),
            'dispersion': dispersion,
        }
        
        # CORE_OBJECT: высокий slot_ratio
        if slot_ratio >= SLOT_RATIO_CORE:
            core_object.add(lemma)
        # GENERIC_CONTEXT: высокая dispersion И низкий slot_ratio
        elif dispersion >= DISPERSION_GENERIC and slot_ratio < SLOT_RATIO_CORE:
            generic_context.add(lemma)
        # TAIL_NOISE: всё остальное (низкая частота)
        else:
            tail_noise.add(lemma)
    
    stats = {
        'core_object': sorted(core_object),
        'generic_context': sorted(generic_context),
        'tail_noise_size': len(tail_noise),
        'metrics_sample': {l: metrics[l] for l in sorted(metrics)[:20]},
    }
    
    return core_object, generic_context, stats


# ────────────────────────────────────────────────────────────────────────────
# Сбор object_neighbors для weak booster
# ────────────────────────────────────────────────────────────────────────────

def build_object_neighbors(
    l0_valid_kws: list[str],
    seed_words: set,
    object_anchor: str,
    window: int = 2,
    min_freq: int = 2,
) -> set[str]:
    """
    Леммы которые встречаются в окне ±window от object_anchor в L0_VALID 
    хотя бы в min_freq разных ключах.
    """
    counter: Counter = Counter()
    
    for kw in l0_valid_kws:
        positions, _ = extract_lemmas(kw)
        if not positions:
            continue
        
        anchor_indices = [
            idx for lemma, pos, w, idx in positions
            if object_anchor and (lemma == object_anchor or lemma.startswith(object_anchor))
        ]
        if not anchor_indices:
            continue
        
        kw_neighbors = set()
        for a_idx in anchor_indices:
            for lemma, pos, w, idx in positions:
                if abs(idx - a_idx) > window or idx == a_idx:
                    continue
                if w in seed_words or w in _STOPWORDS or len(w) <= 2:
                    continue
                if pos == 'NOUN':
                    kw_neighbors.add(lemma)
        
        for n in kw_neighbors:
            counter[n] += 1
    
    return {n for n, c in counter.items() if c >= min_freq}


# ────────────────────────────────────────────────────────────────────────────
# L0 signals анализ
# ────────────────────────────────────────────────────────────────────────────

def build_l0_signals_map(l0_trace: list) -> tuple[dict, float]:
    """
    Строит map kw → list of useful L0 positive signals.
    Возвращает (map, signal_density) где signal_density = доля GREY с useful signals.
    """
    signals_map = {}
    grey_count = 0
    grey_with_useful = 0
    
    for t in l0_trace:
        if not isinstance(t, dict):
            continue
        kw = t.get('keyword', '')
        label = t.get('label', '')
        sigs = t.get('signals', [])
        
        if not sigs:
            continue
        
        useful = [s for s in sigs if not s.startswith('-') and s in _USEFUL_L0_SIGNALS]
        if useful:
            signals_map[kw] = useful
        
        if label == 'GREY':
            grey_count += 1
            if useful:
                grey_with_useful += 1
    
    density = grey_with_useful / grey_count if grey_count else 0.0
    return signals_map, density


# ────────────────────────────────────────────────────────────────────────────
# Action check
# ────────────────────────────────────────────────────────────────────────────

def has_action(
    kw: str,
    action_anchor: Optional[str],
    action_root: Optional[str],
    action_set: set[str],
    object_anchor: Optional[str] = None,
) -> tuple[bool, str]:
    """
    A. lemma.startswith(action_root) — однокоренные
    B. lemma in action_set — синонимы
    C. position fallback — короткое NOUN (2-4 буквы) на pos 0 + object substring в kw
    """
    if not action_anchor or not action_root:
        return True, 'no_action_extracted'
    
    kw_low = kw.lower()
    
    for w in re.findall(r'[а-яёa-z]+', kw_low):
        p = morph.parse(w)[0]
        lemma = p.normal_form
        if lemma.startswith(action_root):
            return True, f'action_root:{lemma}'
        if lemma in action_set:
            return True, f'action_lemma:{lemma}'
    
    if object_anchor and object_anchor in kw_low:
        words = re.findall(r'[а-яёa-z]+', kw_low)
        if words:
            first = words[0]
            if 2 <= len(first) <= MAX_ABBREV_LEN:
                p_first = morph.parse(first)[0]
                first_lemma = p_first.normal_form
                if (p_first.tag.POS == 'NOUN'
                    and first_lemma != object_anchor
                    and not first_lemma.startswith(object_anchor)
                    and object_anchor not in first_lemma):
                    return True, f'action_abbrev_pos0:{first_lemma}'
    
    return False, 'no_action'


def build_action_set(
    action_anchor: str,
    action_root: str,
    l0_valid_kws: list[str],
    object_anchor: Optional[str],
) -> set[str]:
    """
    Action set = action_anchor + VERB/INFN из L0_VALID с freq>=3.
    
    Без MiniLM-расширения NOUN — оно даёт массу FP в action_set 
    (букет/подставка/телек/пойзона/купянск близки к 'доставка' по cos).
    """
    if not action_anchor:
        return set()
    
    action_set = {action_anchor}
    counter: Counter = Counter()
    
    for kw in l0_valid_kws:
        kw_low = kw.lower() if isinstance(kw, str) else ''
        for w in re.findall(r'[а-яёa-z]+', kw_low):
            if w in _STOPWORDS or len(w) <= 3:
                continue
            p = morph.parse(w)[0]
            pos = p.tag.POS
            lemma = p.normal_form
            
            if object_anchor and (lemma == object_anchor or object_anchor in lemma):
                continue
            if action_root and lemma.startswith(action_root):
                continue
            
            if pos in ('INFN', 'VERB'):
                counter[lemma] += 1
    
    for lemma, count in counter.most_common(30):
        if count >= 3:
            action_set.add(lemma)
    
    return action_set


# ────────────────────────────────────────────────────────────────────────────
# Сбор evidence для кандидата
# ────────────────────────────────────────────────────────────────────────────

def collect_evidence(
    kw: str,
    object_anchor: str,
    action_anchor: Optional[str],
    seed_words: set,
    core_object: set,
    generic_context: set,
    object_neighbors: set,
    object_emb: Optional[np.ndarray],
    action_emb: Optional[np.ndarray],
    ruwordnet_synonyms: set,
    is_broad_domain: bool,
    kw_l0_signals: list,
) -> tuple[list, list]:
    """
    Собирает strong и weak сигналы для kw.
    
    STRONG сигналы:
      S1. CORE_OBJECT — лемма-кандидат имеет высокий slot_ratio в L0_VALID
      S2. high_cos_object (≥0.55) И (в neighbors ИЛИ tech domain)
      S3. RuWordNet synonym
      S4. action_synonym через MiniLM cos (≥0.55 к action_anchor) + защита от object-side
      S5. brand-like latin token (для iphone-like)
    
    WEAK сигналы:
      W1. mid_cos_object (0.40-0.55)
      W2. в neighbors (но не CORE)
      W3. L0 positive signal (commerce/brand/etc, без geo)
    
    GENERIC_CONTEXT — антисигнал (если кандидат там — игнорируем)
    """
    strong = []
    weak = []
    
    kw_low = kw.lower()
    positions, kw_lemmas = extract_lemmas(kw, exclude=seed_words | _STOPWORDS)
    
    # NOUN-кандидаты
    candidates = [(lemma, w, idx) for lemma, pos, w, idx in positions 
                  if pos == 'NOUN' and lemma not in seed_words and lemma not in _STOPWORDS]
    
    for cand_lemma, cand_word, cand_idx in candidates:
        # Пропускаем generic слова
        if cand_lemma in generic_context:
            continue
        
        # S3: RuWordNet
        if cand_lemma in ruwordnet_synonyms or cand_word in ruwordnet_synonyms:
            strong.append(('ruwordnet', cand_lemma))
            continue  # уже сильный, другие сигналы не нужны
        
        # S1: CORE_OBJECT
        if cand_lemma in core_object:
            strong.append(('core_object', cand_lemma))
        
        # MiniLM cos к object
        cos_obj = 0.0
        if object_emb is not None:
            emb = get_e5_word_embedding(cand_lemma)
            if emb is not None:
                cos_obj = e5_cosine_sim(emb, object_emb)
        
        # S2: high_cos_object
        if cos_obj >= COS_HIGH:
            # Для broad domain — требуем подтверждения через neighbors
            # Для tech domain — high_cos сам по себе strong (т.к. L0 не помогает)
            if cand_lemma in object_neighbors:
                strong.append(('high_cos_neigh', cand_lemma, round(cos_obj, 2)))
            elif not is_broad_domain:  # tech domain
                strong.append(('high_cos_tech', cand_lemma, round(cos_obj, 2)))
            else:  # broad domain без neighbors — подозрительно (false friend)
                weak.append(('high_cos_unverified', cand_lemma, round(cos_obj, 2)))
        elif cos_obj >= COS_MID:
            # W1: mid_cos
            weak.append(('mid_cos', cand_lemma, round(cos_obj, 2)))
        
        # W2: в neighbors но не CORE
        if cand_lemma in object_neighbors and cand_lemma not in core_object:
            # уже могло быть добавлено через high_cos_neigh
            if not any(s[0] == 'high_cos_neigh' and s[1] == cand_lemma for s in strong):
                weak.append(('in_neighbors', cand_lemma))
        
        # S4: action_synonym через MiniLM
        if action_emb is not None and cos_obj < COS_HIGH:
            emb_cand = get_e5_word_embedding(cand_lemma)
            if emb_cand is not None:
                cos_act = e5_cosine_sim(emb_cand, action_emb)
                if cos_act >= COS_ACTION_SYN and cos_act > cos_obj:
                    # Защита от object-side: если cand ближе к object чем к action — пропускаем
                    strong.append(('action_synonym', cand_lemma, round(cos_act, 2)))
    
    # W3: L0 positive signals
    if kw_l0_signals:
        weak.append(('l0_positive', kw_l0_signals))
    
    return strong, weak


# ────────────────────────────────────────────────────────────────────────────
# Главная функция фильтра
# ────────────────────────────────────────────────────────────────────────────

def classify_kw(
    kw: str,
    seed_words: set,
    seed_content_lemmas: set,
    qualifier: Optional[str],
    qualifier_text: list[str],
    object_anchor: str,
    action_anchor: Optional[str],
    action_root: Optional[str],
    action_set: set,
    core_object: set,
    generic_context: set,
    object_neighbors: set,
    object_emb: Optional[np.ndarray],
    action_emb: Optional[np.ndarray],
    ruwordnet_synonyms: set,
    is_broad_domain: bool,
    kw_l0_signals: list,
) -> tuple[str, str]:
    """
    Классифицирует kw → ('VALID' | 'GREY' | 'TRASH', signal_str)
    """
    kw_low = kw.lower()
    
    # ═══ ШАГ 1: HARD GATES (мгновенный TRASH) ═══
    
    # 1A: qualifier-required
    if qualifier:
        has_qual = bool(re.search(rf'(?<!\d){qualifier}(?!\d)', kw_low))
        if not has_qual and qualifier_text:
            for txt in qualifier_text:
                if txt in kw_low:
                    has_qual = True
                    break
        if not has_qual:
            return 'TRASH', f'no_qualifier:{qualifier}'
    
    # 1B: seed-lemma-required
    positions, kw_lemmas = extract_lemmas(kw)
    has_any_seed_lemma = bool(kw_lemmas & seed_content_lemmas)
    has_object_substring = object_anchor and object_anchor in kw_low
    
    if not has_any_seed_lemma and not has_object_substring:
        return 'TRASH', 'no_seed_lemma'
    
    # ═══ ШАГ 2: HARD PASS (все seed-слова присутствуют + action) ═══
    
    # Проверяем каждую seed-content-лемму на присутствие в kw (substring или лемма)
    all_seed_present = True
    for seed_lemma in seed_content_lemmas:
        if seed_lemma in kw_lemmas:
            continue
        # substring check (для морфо-вариантов)
        if any(seed_lemma in w for _, _, w, _ in positions):
            continue
        all_seed_present = False
        break
    
    action_ok, action_signal = has_action(
        kw, action_anchor, action_root, action_set, object_anchor=object_anchor
    )
    
    if all_seed_present and action_ok:
        return 'VALID', f'all_seed_words|{action_signal}'
    
    # ═══ ШАГ 3: EVIDENCE-BASED ═══
    
    if not action_ok:
        # Без action даже сильные object-сигналы не дают VALID
        # Но если все seed-слова есть — может быть GREY
        if all_seed_present:
            return 'GREY', f'all_seed|no_action'
        return 'TRASH', f'partial_seed|{action_signal}'
    
    # Собираем evidence
    strong, weak = collect_evidence(
        kw, object_anchor, action_anchor, seed_words,
        core_object, generic_context, object_neighbors,
        object_emb, action_emb, ruwordnet_synonyms,
        is_broad_domain, kw_l0_signals,
    )
    
    # Также проверка object_anchor substring/лемма как strong сигнал
    if has_object_substring or object_anchor in kw_lemmas:
        strong.append(('object_anchor',))
    
    # ═══ ШАГ 4: РЕШЕНИЕ ═══
    
    n_strong = len(strong)
    n_weak = len(weak)
    
    signal_str = f'strong={strong}|weak={weak}'
    
    # VALID: 2+ strong
    if n_strong >= STRONG_FOR_VALID:
        return 'VALID', f'multi_strong|{signal_str}'
    
    # VALID: 1 strong + action есть + это object_anchor сам
    if n_strong >= 1 and any(s[0] == 'object_anchor' for s in strong):
        return 'VALID', f'object_strong|{signal_str}'
    
    # GREY: 1 strong (не object) или 2 weak
    if n_strong >= 1 or n_weak >= WEAK_FOR_GREY:
        return 'GREY', f'borderline|{signal_str}'
    
    return 'TRASH', f'insufficient_evidence|{signal_str}'


# ────────────────────────────────────────────────────────────────────────────
# Главная функция фильтра — entry point
# ────────────────────────────────────────────────────────────────────────────

def apply_l1_5_filter_v2(data: dict, seed: str) -> dict:
    """
    L1.5 v2 — Default TRASH, спасаем через evidence.
    """
    t_start = time.time()
    
    # Подготовка из seed
    seed_content_lemmas = extract_seed_content_lemmas(seed)
    qualifier, qualifier_text = extract_qualifier(seed)
    object_anchor = extract_object_anchor(seed)
    action_anchor, action_root = extract_action_anchor(seed)
    seed_words = set(seed.lower().split())
    
    logger.info(
        f"[L1.5/v2] seed='{seed}' object='{object_anchor}' "
        f"action='{action_anchor}' root='{action_root}' "
        f"qualifier='{qualifier}' "
        f"seed_content_lemmas={sorted(seed_content_lemmas)}"
    )
    
    # L0 trace
    l0_trace = data.get('_l0_trace', [])
    l0_valid_kws = [t['keyword'] for t in l0_trace 
                    if isinstance(t, dict) and t.get('label') == 'VALID']
    
    # L0 signals map + density
    l0_signals_map, signal_density = build_l0_signals_map(l0_trace)
    is_broad_domain = signal_density >= SIGNAL_DENSITY_BROAD
    
    logger.info(
        f"[L1.5/v2] L0 signal_density={signal_density:.2f}, "
        f"domain_type={'broad' if is_broad_domain else 'tech'}"
    )
    
    # 3-class классификация лемм
    if object_anchor:
        core_object, generic_context, lemma_stats = classify_lemmas(
            l0_valid_kws, seed_words, object_anchor
        )
        logger.info(
            f"[L1.5/v2] lemma classes: CORE={lemma_stats['core_object'][:20]}, "
            f"GENERIC={lemma_stats['generic_context'][:20]}, "
            f"TAIL_size={lemma_stats['tail_noise_size']}"
        )
    else:
        core_object, generic_context = set(), set()
    
    # object_neighbors (window-based)
    object_neighbors = build_object_neighbors(
        l0_valid_kws, seed_words, object_anchor
    ) if object_anchor else set()
    
    # action_set
    action_set = set()
    if action_anchor and action_root:
        action_set = build_action_set(action_anchor, action_root, l0_valid_kws, object_anchor)
    
    # Эмбеддинги
    object_emb = get_e5_word_embedding(object_anchor) if object_anchor else None
    action_emb = get_e5_word_embedding(action_anchor) if action_anchor else None
    
    # RuWordNet synonyms — пока пустые (если есть data['_anchors_synonyms'] — берём оттуда)
    ruwordnet_synonyms = set(data.get('_anchors_synonyms', []))
    
    # GREY input
    grey = data.get('keywords_grey', [])
    if not grey:
        logger.info(f"[L1.5/v2] no GREY to filter, skipping")
        return data
    
    new_valid = []
    new_grey = []
    new_trash = []
    
    trace_entries = []
    
    for kw_item in grey:
        kw = kw_item if isinstance(kw_item, str) else kw_item.get('keyword', '')
        if not kw:
            continue
        
        kw_l0_signals = l0_signals_map.get(kw, [])
        
        label, signal = classify_kw(
            kw, seed_words, seed_content_lemmas, qualifier, qualifier_text,
            object_anchor, action_anchor, action_root, action_set,
            core_object, generic_context, object_neighbors,
            object_emb, action_emb, ruwordnet_synonyms,
            is_broad_domain, kw_l0_signals,
        )
        
        if label == 'VALID':
            new_valid.append(kw_item)
        elif label == 'GREY':
            new_grey.append(kw_item)
        else:  # TRASH
            new_trash.append(kw_item)
            trace_entries.append({
                'keyword': kw,
                'label': 'TRASH',
                'decided_by': 'l1_5_v2',
                'reason': f"l1_5_v2 ({label})",
                'signals': [signal[:200]],
            })
    
    # Промотируем VALID в data['keywords'] (это новая версия — Pass даёт VALID не GREY)
    existing_keywords = list(data.get('keywords', []))
    existing_keywords.extend(new_valid)
    data['keywords'] = existing_keywords
    data['keywords_grey'] = new_grey
    
    # Trace для отладки
    data.setdefault('_l1_5_trace', []).extend(trace_entries)
    
    logger.info(
        f"[L1.5/v2] grey: {len(grey)} → "
        f"VALID promoted: {len(new_valid)}, "
        f"GREY remaining: {len(new_grey)}, "
        f"TRASH: {len(new_trash)}, "
        f"elapsed: {time.time()-t_start:.2f}s"
    )
    
    return data
