import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        if not golden_seed or not keyword:
            return keyword
        
        # 1. Готовим словарь основ СИДА (оригинального, без принудительных основ от Клода!)
        seed_bases = {}
        for w in re.findall(r'\w+', golden_seed.lower()):
            base = self.morph.parse(w)[0].normal_form
            seed_bases[base] = w  # Сопоставляем основу с формой, которую хочет юзер

        # 2. Обработка ключа
        tokens = keyword.split()
        new_tokens = []

        for token in tokens:
            # Очищаем от знаков препинания для поиска основы
            clean_word = re.sub(r'[^\w]', '', token.lower())
            if not clean_word:
                new_tokens.append(token)
                continue
                
            parsed = self.morph.parse(clean_word)[0]
            base = parsed.normal_form

            # ЕСЛИ ОСНОВА ЕСТЬ В СИДЕ - МЕНЯЕМ ПАДЕЖ НА ТОТ, ЧТО В СИДЕ
            if base in seed_bases:
                new_tokens.append(seed_bases[base])
            # ЕСЛИ НЕТ (это город, отзыв и т.д.) - ОСТАВЛЯЕМ КАК БЫЛО
            else:
                new_tokens.append(token)

        # ГАРАНТИЯ: Количество слов на выходе ВСЕГДА равно количеству на входе
        return " ".join(new_tokens)

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
