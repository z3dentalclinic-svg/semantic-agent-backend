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
import os
import re
import time
import urllib.request
from typing import Optional

from .shared_morph import morph

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# RuWordNet — taxonomy для synonyms/hyponyms (Уровень 4 расширения anchor)
# ──────────────────────────────────────────────────────────────────────────────

_RUWORDNET_DB_URL = "https://github.com/avidale/python-ruwordnet/releases/download/0.0.4/ruwordnet-2021.db"
_RUWORDNET_DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "_data",
    "ruwordnet.db",
)

_wn = None  # singleton, инициализируется при первом обращении


def _ensure_ruwordnet_db() -> Optional[str]:
    """Скачивает БД RuWordNet один раз. Возвращает путь или None при ошибке."""
    if os.path.exists(_RUWORDNET_DB_PATH):
        return _RUWORDNET_DB_PATH
    
    try:
        os.makedirs(os.path.dirname(_RUWORDNET_DB_PATH), exist_ok=True)
        logger.info(f"[L1.5] Downloading RuWordNet DB (~96MB) from {_RUWORDNET_DB_URL}")
        t0 = time.time()
        urllib.request.urlretrieve(_RUWORDNET_DB_URL, _RUWORDNET_DB_PATH)
        size_mb = os.path.getsize(_RUWORDNET_DB_PATH) / 1024 / 1024
        logger.info(f"[L1.5] RuWordNet DB downloaded ({size_mb:.1f}MB) in {time.time()-t0:.1f}s")
        return _RUWORDNET_DB_PATH
    except Exception as e:
        logger.warning(f"[L1.5] RuWordNet DB download failed: {e}")
        return None


def _get_wn():
    """Lazy singleton — RuWordNet object."""
    global _wn
    if _wn is not None:
        return _wn if _wn is not False else None
    
    try:
        from ruwordnet import RuWordNet
        db_path = _ensure_ruwordnet_db()
        if db_path is None:
            _wn = False
            return None
        _wn = RuWordNet(filename=db_path)
        logger.info("[L1.5] RuWordNet initialized")
        return _wn
    except ImportError:
        logger.warning("[L1.5] ruwordnet package not installed — synonyms disabled")
        _wn = False
        return None
    except Exception as e:
        logger.warning(f"[L1.5] RuWordNet init failed: {e} — synonyms disabled")
        _wn = False
        return None


# Кеш synonyms по object_anchor (один раз на сид)
_synonyms_cache: dict[str, set[str]] = {}


def get_synonyms_for(object_anchor: str) -> set[str]:
    """
    Возвращает синонимы / гипонимы / гиперонимы object_anchor через RuWordNet.
    
    Для 'скутер' вернёт {'мопед', 'мотороллер', 'электроскутер', ...}
    Для 'цветок' вернёт {'роза', 'тюльпан', 'эустома', 'букет', ...}
    
    Если RuWordNet недоступен — пустое множество.
    Кешируется per-anchor.
    """
    if not object_anchor:
        return set()
    
    if object_anchor in _synonyms_cache:
        return _synonyms_cache[object_anchor]
    
    wn = _get_wn()
    if not wn:
        _synonyms_cache[object_anchor] = set()
        return set()
    
    related: set[str] = set()
    try:
        senses = wn.get_senses(object_anchor)
        for sense in senses:
            synset = sense.synset
            # Гипонимы (виды) — наследники
            for h in synset.hyponyms:
                for s in h.senses:
                    related.add(s.name.lower())
            # Синонимы (тот же synset)
            for s in synset.senses:
                related.add(s.name.lower())
            # Гиперонимы (родители) — обобщения (для подстраховки)
            for h in synset.hypernyms:
                for s in h.senses:
                    related.add(s.name.lower())
    except Exception as e:
        logger.debug(f"[L1.5] RuWordNet lookup error for '{object_anchor}': {e}")
    
    related.discard(object_anchor.lower())
    _synonyms_cache[object_anchor] = related
    
    if related:
        logger.info(f"[L1.5] RuWordNet synonyms for '{object_anchor}' ({len(related)}): {sorted(related)[:10]}")
    else:
        logger.info(f"[L1.5] RuWordNet: no synonyms for '{object_anchor}'")
    
    return related


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
    synonyms: Optional[set] = None,
) -> tuple[bool, str]:
    """
    Проверяет наличие domain anchor в kw.
    
    Уровни проверки (от мягкого к жёсткому):
    L1: substring match object_anchor в kw_lower
    L2: qualifier (число или текстовая форма)
    L3: лемматизация каждого слова kw, сравнение с object_anchor
    L4: RuWordNet synonyms (если переданы) — substring + лемма
    """
    if not object_anchor:
        return True, 'no_object_extracted'
    
    kw_low = kw.lower()
    
    # L1: Substring object_anchor
    if object_anchor in kw_low:
        return True, 'substring'
    
    # L2: Qualifier (число)
    if qualifier:
        if re.search(rf'(?<!\d){qualifier}(?!\d)', kw_low):
            return True, 'qualifier_digit'
        for txt in qualifier_text:
            if txt in kw_low:
                return True, 'qualifier_text'
    
    # L3: Лемматизация — собираем леммы kw
    kw_words = re.findall(r'[а-яёa-z]+', kw_low)
    kw_lemmas = set()
    for w in kw_words:
        p = morph.parse(w)[0]
        kw_lemmas.add(p.normal_form)
        if p.normal_form == object_anchor:
            return True, 'lemma'
    
    # L4: RuWordNet synonyms (substring + лемма)
    if synonyms:
        for syn in synonyms:
            if syn in kw_low:
                return True, f'synonym_substring:{syn}'
        for lemma in kw_lemmas:
            if lemma in synonyms:
                return True, f'synonym_lemma:{lemma}'
    
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
    
    # Уровень 4: synonyms/hyponyms через RuWordNet (per-seed cache)
    synonyms = get_synonyms_for(object_anchor)
    
    logger.info(
        f"[L1.5] seed='{seed}' → object='{object_anchor}', qualifier='{qualifier}', "
        f"synonyms({len(synonyms)})={sorted(synonyms)[:5] if synonyms else '[]'}"
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
        
        ok, signal = has_anchor(kw, object_anchor, qualifier, qualifier_text, synonyms)
        
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
