import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        if not golden_seed or not keyword:
            return keyword
        
        # 1. Составляем карту основ из ОРИГИНАЛЬНОГО сида
        # Мы запоминаем, какая начальная форма соответствует слову из сида
        seed_words = re.findall(r'\w+', golden_seed.lower())
        seed_map = {}
        for sw in seed_words:
            p = self.morph.parse(sw)
            if p:
                base = p[0].normal_form
                seed_map[base] = sw  # Например: {'ремонт': 'ремонт', 'пылесос': 'пылесосов'}

        # 2. Обрабатываем ключ по словам, сохраняя структуру 1-в-1
        tokens = keyword.split()
        normalized_tokens = []
        
        for t in tokens:
            # Очищаем только от крайних знаков препинания для поиска основы
            t_clean = t.lower().strip(".,!?;:()")
            if not t_clean:
                normalized_tokens.append(t)
                continue
                
            p_token = self.morph.parse(t_clean)
            if p_token:
                t_base = p_token[0].normal_form
                # Если основа слова есть в нашем сиде - меняем на форму из сида
                if t_base in seed_map:
                    normalized_tokens.append(seed_map[t_base])
                else:
                    # Если это город, отзыв или другое слово - оставляем оригинал
                    normalized_tokens.append(t)
            else:
                normalized_tokens.append(t)

        # Собираем обратно. Количество слов всегда равно исходному!
        return " ".join(normalized_tokens)

    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        if not keywords: return []
        # Возвращаем список нормализованных фраз
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
