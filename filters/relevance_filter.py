"""
Relevance filter - filters keywords by relevance to seed query
IMPROVED VERSION: более мягкая фильтрация, разрешает локальные гео-запросы
"""

import re
import logging
from typing import List
from difflib import SequenceMatcher

logger = logging.getLogger("RelevanceFilter")


def is_nearly_same(s1: str, s2: str) -> bool:
    """Проверка схожести слов для защиты от падежей и опечаток"""
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio() > 0.8


async def filter_relevant_keywords(keywords: List[str], seed: str, language: str = 'ru') -> List[str]:
    seed_roots = [w.lower()[:5] for w in re.findall(r'[а-яёa-z]+', seed) if len(w) > 3]
    filtered = []
    
    for kw in keywords:
        has_core = any(root in kw.lower() for root in seed_roots)
        if has_core:
            filtered.append(kw)
        else:
            logger.warning(f"!!! [RELEVANCE_DROP] Ключ: '{kw}' | Не найдены корни сида: {seed_roots}")
            
    return filtered
