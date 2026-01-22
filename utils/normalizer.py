from typing import List
from difflib import SequenceMatcher


def simple_normalize_keyword(keyword: str, seed: str) -> str:
    if not keyword or not seed:
        return keyword
    
    seed_tokens = seed.lower().split()
    kw_tokens = keyword.lower().split()
    
    replacements = {}
    
    for s in seed_tokens:
        for k in kw_tokens:
            if k == s:
                replacements[k] = s
                continue
            
            prefix_len = 5
            if len(k) >= prefix_len and len(s) >= prefix_len and k[:prefix_len] == s[:prefix_len]:
                replacements[k] = s
                continue
            
            sim = SequenceMatcher(None, k, s).ratio()
            if sim >= 0.7:
                replacements[k] = s
    
    result = []
    
    for token in keyword.split():
        low = token.lower()
        if low in replacements:
            result.append(replacements[low])
        else:
            result.append(token)
    
    return " ".join(result)


def normalize_keywords(keywords: List[str], language: str, seed: str) -> List[str]:
    return [simple_normalize_keyword(kw, seed) for kw in keywords]
