import re
import pymorphy3
from typing import List
from nltk.stem import SnowballStemmer

class GoldenNormalizer:
    def __init__(self):
        self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
        self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk')
        self.stemmers = {'en': SnowballStemmer('english'), 'de': SnowballStemmer('german')}

    def _get_base(self, word: str, lang: str) -> str:
        w = word.lower().strip().strip('.,!?()') # Чистим только для поиска базы
        if not w: return ""
        if lang == 'ru': return self.morph_ru.parse(w)[0].normal_form
        if lang == 'uk': return self.morph_uk.parse(w)[0].normal_form
        if lang in self.stemmers: return self.stemmers[lang].stem(w)
        return w

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str, lang: str = 'ru') -> str:
        if not golden_seed: return keyword
        
        # 1. Создаем карту основ из сида
        seed_words = golden_seed.lower().split()
        golden_map = {}
        for sw in seed_words:
            base = self._get_base(sw, lang)
            if base: golden_map[base] = sw

        # 2. Обрабатываем ключевое слово БЕЗ потери элементов
        # Используем split() чтобы сохранить количество слов 1-в-1
        kw_words = keyword.split() 
        final_result = []

        for kw_word in kw_words:
            base = self._get_base(kw_word, lang)
            # Если это слово из сида - правим его
            if base in golden_map:
                final_result.append(golden_map[base])
            else:
                # ВАЖНО: Если слова нет в сиде - возвращаем его ЦЕЛИКОМ (как было в оригинале)
                final_result.append(kw_word)

        return " ".join(final_result)

    def process_batch(self, keywords: List[str], golden_seed: str, lang: str = 'ru') -> List[str]:
        if not keywords or not golden_seed: return keywords
        # 1. Нормализуем (длина списка и структура строк сохраняются)
        normalized = [self.normalize_by_golden_seed(kw, golden_seed, lang) for kw in keywords]
        # 2. Удаляем только ПОЛНЫЕ дубликаты
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
    return normalizer.process_batch(keywords, seed, language)
