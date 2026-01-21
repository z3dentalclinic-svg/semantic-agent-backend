import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        # Проверка на None или пустую строку
        if not golden_seed or not keyword:
            return keyword
        
        # Логирование входного ключа
        tokens_in = keyword.split()
            
        # 1. Берем основы слов из СИДА
        seed_bases = {}
        for w in re.findall(r'\w+', golden_seed.lower()):
            try:
                parsed = self.morph.parse(w)
                if parsed:
                    base = parsed[0].normal_form
                    seed_bases[base] = w
                else:
                    seed_bases[w] = w  # fallback
            except Exception:
                seed_bases[w] = w  # fallback при любой ошибке

        # 2. Разбиваем ключ на токены
        tokens = keyword.split()
        result = []

        for token in tokens:
            if not token:  # пропускаем пустые токены
                continue
            
            try:
                # Очищаем только для проверки
                clean_token = token.lower().strip('.,!?() ')
                if not clean_token:
                    result.append(token)
                    continue
                
                # Парсим с проверкой
                parsed = self.morph.parse(clean_token)
                if not parsed:
                    # Если pymorphy не распознал - оставляем как есть
                    result.append(token)
                    continue
                
                base = parsed[0].normal_form

                if base in seed_bases:
                    # Если слово из сида — приводим к форме сида
                    result.append(seed_bases[base])
                else:
                    # Если слова НЕТ в сиде - возвращаем КАК ЕСТЬ
                    result.append(token)
            except Exception:
                # При любой ошибке - оставляем оригинальный токен
                result.append(token)

        final_result = " ".join(result)
        
        # Логирование если токены потерялись
        tokens_out = final_result.split() if final_result else []
        if len(tokens_in) != len(tokens_out):
            print(f"⚠️ ПОТЕРЯ ТОКЕНОВ: IN({len(tokens_in)}): '{keyword}' → OUT({len(tokens_out)}): '{final_result}'")
        
        return final_result

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
