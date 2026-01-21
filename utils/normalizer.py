import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()

    def get_base(self, word: str) -> str:
        """Получает начальную форму слова (лемму)"""
        clean_word = re.sub(r'[^а-яёА-ЯЁa-zA-Z]', '', word).lower()
        if not clean_word: return ""
        p = self.morph.parse(clean_word)
        return p[0].normal_form if p else clean_word

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        if not golden_seed or not keyword:
            return keyword
        
        # 1. Разбиваем СИД на слова и создаем карту соответствий (база -> оригинал сида)
        # Пример: {"ремонт": "ремонт", "пылесос": "пылесосов"}
        seed_words = golden_seed.split()
        seed_map = {}
        for sw in seed_words:
            base = self.get_base(sw)
            if base:
                seed_map[base] = sw

        # 2. Разбиваем КЛЮЧ на слова
        # Пример: ["ремонта", "пылесоса", "днепр"]
        tokens = keyword.split()
        normalized_tokens = []
        
        for t in tokens:
            # Очищаем слово от знаков препинания для сравнения
            t_clean_full = re.sub(r'[^а-яёА-ЯЁa-zA-Z]', '', t).lower()
            t_base = self.get_base(t)
            
            # 3. Простое сравнение: если база слова из ключа есть в базе сида
            if t_base in seed_map:
                # Берем форму из сида
                target_word = seed_map[t_base]
                
                # Если в оригинальном токене были знаки препинания (например, "пылесоса,"), 
                # пытаемся их сохранить (заменяем только буквы)
                if t_clean_full:
                    new_token = t.lower().replace(t_clean_full, target_word)
                    normalized_tokens.append(new_token)
                else:
                    normalized_tokens.append(target_word)
            else:
                # Если слова нет в сиде (например, "днепр"), оставляем как есть
                normalized_tokens.append(t)

        # 4. Собираем обратно. Количество слов и строк НЕ меняется.
        return " ".join(normalized_tokens)

    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        # Возвращаем ровно столько строк, сколько зашло. Никаких set()!
        return [self.normalize_by_golden_seed(kw, golden_seed) for kw in keywords]

_normalizer = None

def normalize_keywords(keywords: List[str], language: str, seed: str) -> List[str]:
    global _normalizer
    if _normalizer is None:
        _normalizer = GoldenNormalizer()
    return _normalizer.process_batch(keywords, seed)
