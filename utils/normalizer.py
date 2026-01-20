import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        # 1. Берем основы слов из СИДА (например: ремонт, пылесос)
        seed_bases = {}
        for w in re.findall(r'\w+', golden_seed.lower()):
            base = self.morph.parse(w)[0].normal_form
            seed_bases[base] = w  # Запоминаем: для базы "ремонт" эталон — "ремонт"

        # 2. Разбиваем ключ на токены, сохраняя всё остальное
        tokens = keyword.split()
        result = []

        for token in tokens:
            # Очищаем только для проверки (штиль!, (штиль) -> штиль)
            clean_token = token.lower().strip('.,!?() ')
            p = self.morph.parse(clean_token)[0]
            base = p.normal_form

            if base in seed_bases:
                # Если слово из сида — приводим к форме сида
                result.append(seed_bases[base])
            else:
                # ВАЖНО: Если слова НЕТ в сиде (штиль, xiaomi, авито) — 
                # возвращаем его КАК ЕСТЬ, не меняя ни единой буквы!
                result.append(token)

        return " ".join(result)

    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        if not keywords or not golden_seed: return keywords
        # Нормализуем каждый ключ
        normalized = [self.normalize_by_golden_seed(kw, golden_seed) for kw in keywords]
        # Убираем дубликаты, которые стали идентичными после правки окончаний
        return list(dict.fromkeys(normalized))


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
