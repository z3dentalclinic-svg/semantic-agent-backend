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
    # Очищаем сид до базовых корней (ремонт, пылесос)
    seed_words = [w.lower()[:5] for w in re.findall(r'[а-яёa-z]+', seed) if len(w) > 3]
    
    filtered = []
    for kw in keywords:
        kw_lower = kw.lower()
        # Если хотя бы ОДИН корень из сида есть в ключе - оставляем!
        # Это спасет "ремонтам", "ремонтами" и т.д.
        if any(root in kw_lower for root in seed_words):
            filtered.append(kw)
        else:
            # Если это бренд или район, но основные слова потерялись - в якоря
            pass 
            
    return filtered
