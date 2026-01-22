import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()
    
    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        if not golden_seed or not keyword:
            return keyword

        # 1. ВСЕ ФОРМЫ слов сида → эталон сида
        seed_forms = {}
        for w in re.findall(r'\w+', golden_seed.lower()):
            try:
                parsed = self.morph.parse(w)
                if not parsed:
                    continue
                seed_form = w  # форма ИЗ сида (эталон!)
                
                # Все формы этого слова → эталон сида
                for form_obj in parsed[0].lexeme:
                    seed_forms[form_obj.word.lower()] = seed_form
            except:
                continue

        # 2. Нормализация ключа
        tokens = keyword.split()
        result = []

        for token in tokens:
            if not token:
                continue
            clean_token = token.lower().strip('.,!?() ')
            if not clean_token:
                result.append(token)
                continue

            try:
                # Пробуем точное совпадение формы
                if clean_token in seed_forms:
                    result.append(seed_forms[clean_token])
                    continue
                    
                # Пробуем лемматизацию
                parsed = self.morph.parse(clean_token)
                if parsed:
                    token_form = parsed[0].word.lower()
                    if token_form in seed_forms:
                        result.append(seed_forms[token_form])
                    else:
                        result.append(token)
                else:
                    result.append(token)
            except:
                result.append(token)

        return " ".join(result)
    
    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        if not keywords or not golden_seed:
            return keywords
        return [self.normalize_by_golden_seed(kw, golden_seed) for kw in keywords]

_normalizer = None

def get_normalizer():
    global _normalizer
    if _normalizer is None:
        _normalizer = GoldenNormalizer()
    return _normalizer

def normalize_keywords(keywords: List[str], language: str, seed: str) -> List[str]:
    n = get_normalizer()
    return n.process_batch(keywords, seed)
