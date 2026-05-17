"""
L1.5 Domain Anchor Filter

Размещается между L0 и L2 в пайплайне:
    L0 → [L1.5] → L2 → L3

Назначение: отсечь явный off-topic мусор из GREY-зоны L0 ДО передачи в L2/L3.

Логика: для каждого ключа в keywords_grey проверяет наличие domain anchor.
Anchor = главный объект seed (object_anchor) ± числовой qualifier (для tech-сидов).
Если anchor отсутствует → перемещение в anchors (TRASH).

Принципы:
- Алгоритмически, без хардкодов и whitelists
- Cross-niche (любой seed)
- БЕЗ LLM
- Минимизация FP важнее максимизации TRASH recall

Уровни сигналов (от мягких к жёстким, можно расширять):
- L1: substring match (object_anchor in kw)
- L2: + qualifier check (число + текстовая форма)
- L3: + лемматизация (pymorphy3)
- L4+: cross-script, L0_VALID expansion, RuWordNet (опционально, см. handoff)
"""

from __future__ import annotations

import logging
import re
import time
from typing import Optional

from .shared_morph import morph

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Конфигурация
# ──────────────────────────────────────────────────────────────────────────────

# Текстовые формы чисел для qualifier-проверки (например seed "купить айфон 16"
# должен матчиться на "купить шестнадцатый iphone")
_NUM_TO_TEXT: dict[str, list[str]] = {
    '1': ['первый'], '2': ['второй'], '3': ['третий'], '4': ['четвертый'],
    '5': ['пятый'], '6': ['шестой'], '7': ['седьмой'], '8': ['восьмой'],
    '9': ['девятый'], '10': ['десятый'], '11': ['одиннадцатый'],
    '12': ['двенадцатый'], '13': ['тринадцатый'], '14': ['четырнадцатый'],
    '15': ['пятнадцатый'], '16': ['шестнадцатый'], '17': ['семнадцатый'],
    '18': ['восемнадцатый'], '19': ['девятнадцатый'], '20': ['двадцатый'],
}


# ──────────────────────────────────────────────────────────────────────────────
# Извлечение object_anchor из seed
# ──────────────────────────────────────────────────────────────────────────────

def extract_object_anchor(seed: str) -> tuple[Optional[str], Optional[str], list[str]]:
    """
    Извлекает domain anchor из seed.
    
    Returns:
        (object_anchor, qualifier, qualifier_text)
        
        object_anchor: лемма главного существительного. Приоритет:
            1. NOUN в gent (родительный падеж) — объект действия
               "доставка цветов" → "цветов" (gent) → лемма "цвет"
            2. NOUN после предлога 'на'/'для'/'в'
               "аккумулятор на скутер" → "скутер"
            3. NOUN в accs (винительный)
            4. Последний NOUN в seed
        
        qualifier: число если есть ('16' для 'купить айфон 16')
        qualifier_text: словесные формы числа ['шестнадцатый']
    
    Примеры:
        "доставка цветов" → ('цвет', None, [])
        "купить айфон 16" → ('айфон', '16', ['шестнадцатый'])
        "аккумулятор на скутер" → ('скутер', None, [])
        "имплантация зубов" → ('зуб', None, [])
        "установка кондиционера цена" → ('кондиционер', None, [])
    """
    words = seed.lower().split()
    qualifier: Optional[str] = None
    qualifier_text: list[str] = []
    nouns: list[tuple[str, Optional[str], bool]] = []  # (lemma, case, after_prep)
    after_prep = False
    
    for w in words:
        # Числовой qualifier (16, 8 — отдельный токен)
        if re.match(r'^\d+$', w):
            qualifier = w
            qualifier_text = _NUM_TO_TEXT.get(w, [])
            after_prep = False
            continue
        
        p = morph.parse(w)[0]
        if p.tag.POS == 'NOUN':
            case = str(p.tag.case) if p.tag.case else None
            nouns.append((p.normal_form, case, after_prep))
        after_prep = (p.tag.POS == 'PREP')
    
    if not nouns:
        return None, qualifier, qualifier_text
    
    # Единственный NOUN — он object
    if len(nouns) == 1:
        return nouns[0][0], qualifier, qualifier_text
    
    # Priority 1: NOUN в gent
    for lemma, case, _ap in nouns:
        if case == 'gent':
            return lemma, qualifier, qualifier_text
    
    # Priority 2: NOUN после предлога
    for lemma, _case, ap in nouns:
        if ap:
            return lemma, qualifier, qualifier_text
    
    # Priority 3: NOUN в accs
    for lemma, case, _ap in nouns:
        if case == 'accs':
            return lemma, qualifier, qualifier_text
    
    # Default: последний NOUN
    return nouns[-1][0], qualifier, qualifier_text


# ──────────────────────────────────────────────────────────────────────────────
# Проверка anchor в kw
# ──────────────────────────────────────────────────────────────────────────────

def has_anchor(
    kw: str,
    object_anchor: Optional[str],
    qualifier: Optional[str],
    qualifier_text: list[str],
) -> tuple[bool, str]:
    """
    Проверяет наличие domain anchor в kw.
    
    Уровни проверки (от мягкого к жёсткому):
    L1: substring match object_anchor в kw_lower
    L2: qualifier (число или текстовая форма)
    L3: лемматизация каждого слова kw, сравнение с object_anchor
    
    Returns:
        (has_anchor, signal_name)
        signal_name — какой именно сигнал сработал (для trace)
    """
    if not object_anchor:
        # Не удалось извлечь object — пропускаем (не трашим)
        return True, 'no_object_extracted'
    
    kw_low = kw.lower()
    
    # L1: Substring object_anchor
    if object_anchor in kw_low:
        return True, 'substring'
    
    # L2: Qualifier (число)
    if qualifier:
        # Точное число как отдельный токен (не часть 161, 166)
        if re.search(rf'(?<!\d){qualifier}(?!\d)', kw_low):
            return True, 'qualifier_digit'
        # Текстовая форма ('шестнадцатый')
        for txt in qualifier_text:
            if txt in kw_low:
                return True, 'qualifier_text'
    
    # L3: Лемматизация — для слов где substring не сработал (опечатки, формы)
    for w in re.findall(r'[а-яёa-z]+', kw_low):
        p = morph.parse(w)[0]
        if p.normal_form == object_anchor:
            return True, 'lemma'
    
    return False, 'no_anchor'


# ──────────────────────────────────────────────────────────────────────────────
# Главная функция фильтра
# ──────────────────────────────────────────────────────────────────────────────

def apply_l1_5_filter(data: dict, seed: str) -> dict:
    """
    Применяет L1.5 Domain Anchor Filter к keywords_grey.
    
    Каждый ключ в keywords_grey проверяется на наличие domain anchor.
    Если anchor отсутствует — ключ перемещается в anchors (TRASH).
    
    Безопасность:
    - Не трогает keywords (VALID от L0) и existing anchors (TRASH от L0)
    - Если object_anchor не извлёкся из seed — фильтр пропускает GREY без изменений
    - Минимум 10 ключей в GREY чтобы запускаться (на маленьких наборах не имеет смысла)
    
    Args:
        data: dict с keywords/keywords_grey/anchors/_l0_trace
        seed: строка seed
    
    Returns:
        Обновлённый data с:
        - keywords_grey: уменьшен
        - anchors: увеличен (бывшие GREY → TRASH)
        - _l1_5_trace: список trace для отладки (опционально)
        - _filter_timings['l1_5']: время работы
    """
    t0 = time.perf_counter()
    
    grey = data.get('keywords_grey', [])
    if len(grey) < 10:
        # Слишком мало — пропускаем
        logger.info(f"[L1.5] skipped (only {len(grey)} grey keywords)")
        data.setdefault('_filter_timings', {})['l1_5'] = round(time.perf_counter() - t0, 4)
        return data
    
    # Извлекаем anchor из seed
    object_anchor, qualifier, qualifier_text = extract_object_anchor(seed)
    
    if not object_anchor:
        logger.info(f"[L1.5] skipped (no object_anchor from seed='{seed}')")
        data.setdefault('_filter_timings', {})['l1_5'] = round(time.perf_counter() - t0, 4)
        return data
    
    logger.info(
        f"[L1.5] seed='{seed}' → object='{object_anchor}', qualifier='{qualifier}'"
    )
    
    new_grey: list = []
    new_trash: list = []
    trace_records: list = []
    
    # _l0_trace для контекста (опционально)
    l0_trace = data.get('_l0_trace', [])
    l0_map = {t['keyword']: t for t in l0_trace if isinstance(t, dict)}
    
    for kw_item in grey:
        # kw может быть строкой или dict
        kw = kw_item if isinstance(kw_item, str) else kw_item.get('keyword', '')
        
        ok, signal = has_anchor(kw, object_anchor, qualifier, qualifier_text)
        
        if ok:
            new_grey.append(kw_item)
        else:
            new_trash.append(kw_item)
            trace_records.append({
                'keyword': kw,
                'label': 'TRASH',
                'decided_by': 'l1_5',
                'reason': f'no_domain_anchor (object={object_anchor})',
                'signals': [],
            })
    
    # Обновляем data
    data['keywords_grey'] = new_grey
    data.setdefault('anchors', []).extend(new_trash)
    
    # anchors_count для UI (если поле используется)
    if 'anchors_count' in data:
        data['anchors_count'] = len(data['anchors'])
    
    # Trace для отладки — список всех ключей которые мы затрешили
    # (parser.tracer в main.py сам подхватит этот блок через after_filter с reasons)
    existing_trace = data.get('_l1_5_trace', [])
    data['_l1_5_trace'] = existing_trace + trace_records
    
    elapsed = round(time.perf_counter() - t0, 4)
    data.setdefault('_filter_timings', {})['l1_5'] = elapsed
    
    logger.info(
        f"[L1.5] grey: {len(grey)} → {len(new_grey)}, "
        f"to_trash: {len(new_trash)}, elapsed: {elapsed}s"
    )
    
    return data
