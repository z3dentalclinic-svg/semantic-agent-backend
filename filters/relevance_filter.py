"""
Relevance filter - filters keywords by relevance to seed query
IMPROVED VERSION: более мягкая фильтрация, разрешает локальные гео-запросы
"""

import re
from typing import List
from difflib import SequenceMatcher


def is_nearly_same(s1: str, s2: str) -> bool:
    """Проверка схожести слов для защиты от падежей и опечаток"""
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio() > 0.8


async def filter_relevant_keywords(keywords: List[str], seed: str, language: str = 'ru') -> List[str]:
    """
    Улучшенный фильтр релевантности: оставляет ключевые слова релевантные seed
    
    ИЗМЕНЕНИЯ:
    - Убраны стоп-слова из проверки (не фильтруем по "в", "на", "от")
    - Разрешены фразы с дополнениями типа "на дому", "своими руками"
    - Проверка схожести слов через SequenceMatcher для падежей
    
    Args:
        keywords: Список ключевых слов
        seed: Базовый запрос
        language: Код языка
        
    Returns:
        Отфильтрованный список релевантных ключевых слов
    """
    
    # Извлекаем слова из seed (минимум 2 символа)
    seed_words = [w.lower() for w in re.findall(r'\w+', seed) if len(w) > 2]
    
    # Убираем короткие слова (предлоги, союзы) - оставляем только важные
    # Если после фильтрации ничего не осталось - берём все слова
    important_seed = [w for w in seed_words if len(w) > 3] or seed_words
    
    filtered = []
    
    for kw in keywords:
        kw_lower = kw.lower()
        kw_words = kw_lower.split()
        matches = 0
        
        for s_word in important_seed:
            # Проверяем: слово из сида есть в ключе ИЛИ очень похоже (падеж/опечатка)
            if any(s_word in kw_w or is_nearly_same(s_word, kw_w) for kw_w in kw_words):
                matches += 1
        
        # Если ВСЕ важные слова из сида найдены - ключ ПРОХОДИТ
        # Дополнительные слова типа "на дому", "своими руками" НЕ мешают
        if matches >= len(important_seed):
            filtered.append(kw)
    
    return filtered
