import logging
from typing import List
from difflib import SequenceMatcher

logger = logging.getLogger("GoldenNormalizer")


def simple_normalize_keyword(keyword: str, seed: str) -> str:
    """
    Нормализует keyword, заменяя слова похожие на seed на точную форму из seed.
    Использует SequenceMatcher для определения похожести.
    """
    if not keyword or not seed:
        return keyword
    
    # Токены из seed и keyword
    seed_tokens = seed.lower().split()
    kw_tokens = keyword.lower().split()
    
    # Словарь замен: {слово_из_ключа: слово_из_сида}
    replacements = {}
    
    for s in seed_tokens:
        for k in kw_tokens:
            # 1) Точное совпадение
            if k == s:
                replacements[k] = s
                continue
            
            # 2) Похожесть по SequenceMatcher (порог 0.8)
            sim = SequenceMatcher(None, k, s).ratio()
            if sim >= 0.8:
                replacements[k] = s
    
    # Применяем замены к исходному keyword
    result = []
    changes = []
    
    for token in keyword.split():
        low = token.lower()
        if low in replacements:
            new = replacements[low]
            if new != token:
                changes.append((token, new))
            result.append(new)
        else:
            result.append(token)
    
    normalized = " ".join(result)
    
    # DEBUG лог только если были изменения
    if changes:
        logger.debug(
            f"[SIMPLE_NORMALIZER] seed='{seed}' | "
            f"keyword='{keyword}' → '{normalized}' | "
            f"changes={changes}"
        )
    
    return normalized


def normalize_keywords(keywords: List[str], language: str, seed: str) -> List[str]:
    """
    Простой нормализатор: для каждого ключа заменяет слова,
    похожие на слова из seed, на точную форму из seed.
    language пока не используется, оставлен для совместимости.
    """
    return [simple_normalize_keyword(kw, seed) for kw in keywords]
