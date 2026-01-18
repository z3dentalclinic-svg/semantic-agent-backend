"""
Infix filter - removes garbage single-letter words from keywords
"""

from typing import List


async def filter_infix_results(keywords: List[str], language: str) -> List[str]:
    """
    Фильтр INFIX результатов: убирает мусорные одиночные буквы
    
    Args:
        keywords: List of keyword strings
        language: Language code (ru, uk, en)
        
    Returns:
        Filtered list of keywords without garbage single letters
    """
    
    if language.lower() == 'ru':
        valid = {'в', 'на', 'у', 'к', 'от', 'из', 'по', 'о', 'об', 'с', 'со', 'за', 'для', 'и', 'а', 'но'}
    elif language.lower() == 'uk':
        valid = {'в', 'на', 'у', 'до', 'від', 'з', 'по', 'про', 'для', 'і', 'та', 'або'}
    elif language.lower() == 'en':
        valid = {'in', 'on', 'at', 'to', 'from', 'with', 'for', 'by', 'o', 'and', 'or', 'a', 'i'}
    else:
        valid = set()

    filtered = []

    for keyword in keywords:
        keyword_lower = keyword.lower()
        words = keyword_lower.split()

        has_garbage = False
        for i in range(1, len(words)):
            word = words[i]
            if len(word) == 1 and word not in valid:
                has_garbage = True
                break
                

        if not has_garbage:
            filtered.append(keyword)

    return filtered
