"""
keyword_grouping.py — группировка VALID ключей по детекторным сигналам.

Использует _l0_trace для получения позитивных сигналов каждого ключа.
Присваивает primary group по иерархии приоритетов (geo > brand > commerce > ...).

Ключи без L0 сигналов (promoted L2/L3) идут в группу "other".

Стоимость: O(n) проход по VALID + O(n log n) сортировка. Миллисекунды на 500 ключей.

Формат output:
    result["groups"] = {
        "order": ["geo", "brand", "commerce", ...],  # только непустые группы в порядке приоритета
        "by_group": {
            "geo":      ["имплантация зубов киев", ...],
            "commerce": ["имплантация зубов цена", ...],
            ...
            "other":    ["имплантация зубов плюсы и минусы", ...],  # L2/L3 promoted без L0 сигналов
        },
        "summary": {"geo": 45, "commerce": 78, ..., "other": 35},
    }

Оригинальный массив result["keywords"] не меняется — группировка добавляется рядом.
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# ПРИОРИТЕТЫ ГРУПП
# ═══════════════════════════════════════════════════════════════════════════

# Коммерческий seed (по умолчанию): сначала geo/brand, потом commerce, потом info
# Порядок от специфичного/коммерческого к общему/информационному
COMMERCIAL_PRIORITY = [
    'geo',           # город/страна
    'brand',         # бренд/модель
    'commerce',      # купить/цена/стоимость
    'location',      # рядом/на дому/с выездом
    'contacts',      # телефон/сайт/адрес
    'reputation',    # отзывы/форум/рейтинг
    'type_spec',     # спецификация типа
    'premod_adj',    # прилагательное до seed (базальная/лазерная)
    'postmod_adj',   # прилагательное после seed (жевательных/передних)
    'prep_modifier', # PREP+NOUN условия (без боли/для детей/при диабете)
    'info_intent',   # вопросительные (что такое/как/когда)
    'conjunctive',   # сложные конструкции с 'и'
    'time',          # временные маркеры
    'verb_modifier', # способы действия
    'action',        # паттерны действий
]

# Информационный seed (как принимать X, что делать с Y):
# info_intent должен быть важнее commerce
INFO_PRIORITY = [
    'info_intent',
    'prep_modifier',
    'time',
    'conjunctive',
    'location',
    'geo',
    'reputation',
    'commerce',
    'brand',
    'type_spec',
    'premod_adj',
    'postmod_adj',
    'contacts',
    'verb_modifier',
    'action',
]

# Маркеры определения типа seed
_INFO_MARKERS = frozenset({
    'как', 'что', 'почему', 'где', 'когда', 'зачем', 'куда', 'чем',
    'сколько', 'какой', 'какая', 'какие', 'кто',
})


def _get_priority_for_seed(seed: str) -> List[str]:
    """
    Возвращает иерархию приоритетов для конкретного типа seed.
    
    Info-seed: содержит вопросительное слово ("как принимать нимесил",
    "что делать с Y") → приоритет info_intent.
    
    Иначе — commercial seed ("имплантация зубов", "ремонт пылесосов",
    "доставка цветов") → приоритет commerce/geo/brand.
    """
    if not seed:
        return COMMERCIAL_PRIORITY
    seed_words = set(seed.lower().split())
    if seed_words & _INFO_MARKERS:
        return INFO_PRIORITY
    return COMMERCIAL_PRIORITY


# ═══════════════════════════════════════════════════════════════════════════
# ОСНОВНАЯ ФУНКЦИЯ
# ═══════════════════════════════════════════════════════════════════════════

def group_valid_keywords(result: Dict[str, Any], seed: str) -> Dict[str, Any]:
    """
    Группирует VALID ключи по детекторным сигналам из _l0_trace.
    
    Добавляет result["groups"] — структурированную группировку.
    Оригинальный result["keywords"] не меняется.
    
    Args:
        result: pipeline результат с полями "keywords" (VALID) и "_l0_trace"
        seed: базовый запрос (для выбора приоритета)
    
    Returns:
        result с добавленным полем "groups"
    """
    valid_keywords = result.get("keywords", [])
    l0_trace = result.get("_l0_trace", [])
    
    if not valid_keywords:
        result["groups"] = {"order": [], "by_group": {}, "summary": {}}
        return result
    
    # Индекс: keyword (lower) → позитивные сигналы
    # Нормализуем keyword через .lower().strip() для надёжного lookup
    kw_to_signals: Dict[str, List[str]] = {}
    for rec in l0_trace:
        kw = rec.get("keyword", "")
        if not kw:
            continue
        kw_lower = kw.lower().strip()
        signals = rec.get("signals", [])
        # Только позитивные (без префикса '-')
        positives = [s for s in signals if not s.startswith('-')]
        kw_to_signals[kw_lower] = positives
    
    priority = _get_priority_for_seed(seed)
    
    # Для каждого VALID ключа определяем primary group
    by_group: Dict[str, List[Any]] = {}
    group_counts: Dict[str, int] = {}
    
    for kw_item in valid_keywords:
        # Извлекаем строку keyword
        if isinstance(kw_item, dict):
            kw_str = kw_item.get("keyword", kw_item.get("query", ""))
        else:
            kw_str = str(kw_item)
        
        if not kw_str:
            continue
        
        kw_lower = kw_str.lower().strip()
        signals = kw_to_signals.get(kw_lower, [])
        
        # Определяем primary group по приоритету
        group = None
        for sig in priority:
            if sig in signals:
                group = sig
                break
        
        # Ключ без L0 сигналов (L2/L3 promoted, или exact_seed) → "other"
        if group is None:
            # Специальный случай: запрос точно равен seed
            if 'exact_seed' in signals:
                group = 'exact_seed'
            else:
                group = 'other'
        
        if group not in by_group:
            by_group[group] = []
            group_counts[group] = 0
        by_group[group].append(kw_item)
        group_counts[group] += 1
    
    # Упорядочиваем группы: сначала по приоритету, потом 'exact_seed', потом 'other'
    order = []
    for sig in priority:
        if sig in by_group:
            order.append(sig)
    if 'exact_seed' in by_group:
        order.insert(0, 'exact_seed')  # самый верх — запросы равные seed
    if 'other' in by_group:
        order.append('other')  # самый низ — несортированные
    
    result["groups"] = {
        "order": order,
        "by_group": by_group,
        "summary": group_counts,
    }
    
    logger.info(
        "[GROUPING] seed='%s' | total_valid=%d | groups=%d | %s",
        seed, len(valid_keywords), len(order),
        ', '.join(f"{g}={group_counts[g]}" for g in order)
    )
    
    return result


# ═══════════════════════════════════════════════════════════════════════════
# ЧИТАБЕЛЬНЫЕ ЛЕЙБЛЫ ДЛЯ UI
# ═══════════════════════════════════════════════════════════════════════════
# Русские подписи для группировки в интерфейсе.
# Используются для отображения "зелёной полосы" в HTML output.

GROUP_LABELS_RU = {
    'exact_seed':    'Точное совпадение с seed',
    'geo':           'География',
    'brand':         'Бренды и модели',
    'commerce':      'Коммерческий интент',
    'location':      'Местоположение (рядом / на дому)',
    'contacts':      'Контактная информация',
    'reputation':    'Отзывы и рейтинги',
    'type_spec':     'Технические характеристики',
    'premod_adj':    'Тип (прилагательное перед seed)',
    'postmod_adj':   'Уточнение (прилагательное после seed)',
    'prep_modifier': 'Условия и ограничения',
    'info_intent':   'Информационные запросы',
    'conjunctive':   'Сложные конструкции',
    'time':          'Временные характеристики',
    'verb_modifier': 'Способы действия',
    'action':        'Действия',
    'other':         'Прочее (требует ручного разбора)',
}


def get_group_label(group: str, lang: str = 'ru') -> str:
    """Возвращает человекочитаемый лейбл группы."""
    if lang == 'ru':
        return GROUP_LABELS_RU.get(group, group)
    return group
