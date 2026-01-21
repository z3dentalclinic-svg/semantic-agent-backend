import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        # Оставляем pymorphy только для определения основы слова
        self.morph = pymorphy3.MorphAnalyzer()

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        if not golden_seed or not keyword:
            return keyword
        
        # 1. Создаем карту основ из сида (ремонт пылесосов -> ремонт, пылесос)
        seed_words = re.findall(r'[а-яёА-ЯЁa-zA-Z]+', golden_seed.lower())
        seed_map = {}
        
        for sw in seed_words:
            p = self.morph.parse(sw)
            if p:
                # Каждой нормальной форме (пылесос) сопоставляем форму из сида (пылесосов)
                seed_map[p[0].normal_form] = sw

        # 2. Обрабатываем слова в ключе
        tokens = keyword.split()
        normalized_tokens = []
        
        for t in tokens:
            # Очищаем слово от знаков препинания только для поиска в словаре
            t_clean = re.sub(r'[^а-яёА-ЯЁa-zA-Z]', '', t).lower()
            
            if not t_clean:
                normalized_tokens.append(t)
                continue
            
            p_token = self.morph.parse(t_clean)
            if p_token:
                t_base = p_token[0].normal_form
                # Если основа слова есть в нашем сиде - меняем на форму из сида
                if t_base in seed_map:
                    # Важно: заменяем только само слово, сохраняя символы (если были)
                    # Например "пылесоса," станет "пылесосов,"
                    new_word = t.lower().replace(t_clean, seed_map[t_base])
                    normalized_tokens.append(new_word)
                else:
                    normalized_tokens.append(t)
            else:
                normalized_tokens.append(t)

        return " ".join(normalized_tokens)

    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        # ВАЖНО: Мы просто идем циклом. Никаких set() и фильтров.
        # Если на входе 204 слова, на выходе БУДЕТ 204 слова.
        result = []
        for kw in keywords:
            try:
                normalized = self.normalize_by_golden_seed(kw, golden_seed)
                result.append(normalized)
            except:
                # Если вдруг ошибка — добавляем оригинальное слово, чтобы не потерять строку
                result.append(kw)
        return result

_normalizer = None

def normalize_keywords(keywords: List[str], language: str, seed: str) -> List[str]:
    global _normalizer
    if _normalizer is None:
        _normalizer = GoldenNormalizer()
    return _normalizer.process_batch(keywords, seed)
