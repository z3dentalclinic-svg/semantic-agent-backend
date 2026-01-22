import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        # 0. Защита от пустых
        if not golden_seed or not keyword:
            return keyword

        # 1. База лемм из seed: "ремонты пылесосов" → {"ремонт": "ремонт", "пылесос": "пылесосов"}
        seed_bases = {}
        for w in re.findall(r'\w+', golden_seed.lower()):
            try:
                parsed = self.morph.parse(w)
                if not parsed:
                    continue
                base = parsed[0].normal_form
                seed_bases[base] = w
            except Exception:
                # если морфология не справилась — просто игнор seed-слово
                continue

        # 2. Токены ключа (сохраняем оригинал)
        tokens = keyword.split()
        result = []

        for token in tokens:
            if not token:
                continue

            # чистим только для анализа, не для вывода
            clean_token = token.lower().strip('.,!?() ')
            if not clean_token:
                # всё стерлось (например, один знак препинания) — оставляем оригинал
                result.append(token)
                continue

            try:
                parsed = self.morph.parse(clean_token)
                if not parsed:
                    # морфология не знает это слово — не трогаем
                    result.append(token)
                    continue

                base = parsed[0].normal_form

                if base in seed_bases:
                    # слово из сида → приводим к форме сида
                    result.append(seed_bases[base])
                else:
                    # не seed-слово → оставляем как есть (авито, youtube и т.п.)
                    result.append(token)
            except Exception:
                # ЛЮБОЙ сбой морфологии → возвращаем исходный токен
                result.append(token)

        return " ".join(result)

    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        if not keywords or not golden_seed:
            return keywords
        return [self.normalize_by_golden_seed(kw, golden_seed) for kw in keywords]
