import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        # Составляем карту основ ОРИГИНАЛЬНОГО сида
        seed_words = golden_seed.lower().split()
        seed_map = {}
        for sw in seed_words:
            base = self.morph.parse(sw)[0].normal_form
            seed_map[base] = sw

        tokens = keyword.split()
        normalized_tokens = []
        for t in tokens:
            # Ищем основу без жесткой очистки
            t_clean = t.lower().strip(".,!")
            t_base = self.morph.parse(t_clean)[0].normal_form
            if t_base in seed_map:
                normalized_tokens.append(seed_map[t_base])
            else:
                normalized_tokens.append(t)
        return " ".join(normalized_tokens)

    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        if not keywords or not golden_seed: return keywords
        
        print(f"🔍 Normalization IN: {len(keywords)} keywords, seed: '{golden_seed}'")
        
        # Нормализуем каждый ключ
        normalized = [self.normalize_by_golden_seed(kw, golden_seed) for kw in keywords]
        
        # Проверяем пустые результаты
        empty_count = sum(1 for n in normalized if not n or not n.strip())
        if empty_count > 0:
            print(f"⚠️ ПУСТЫЕ результаты: {empty_count} из {len(normalized)}")
        
        print(f"🔍 Normalization OUT: {len(normalized)} keywords")
        
        # Возвращаем полный список (даже если есть дубликаты)
        return normalized


# Global instance
_normalizer = None


def get_normalizer():
    global _normalizer
    if _normalizer is None:
        _normalizer = GoldenNormalizer()
    return _normalizer


def normalize_keywords(keywords: List[str], language: str = 'ru', seed: str = '') -> List[str]:
    if not seed:
        return keywords
    normalizer = get_normalizer()
    return normalizer.process_batch(keywords, seed)
