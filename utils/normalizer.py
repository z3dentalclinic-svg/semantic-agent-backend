import re
import pymorphy3
from typing import List
from nltk.stem import SnowballStemmer

class GoldenNormalizer:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()
        self.stemmer_ru = SnowballStemmer("russian")

    def get_stem(self, word: str) -> str:
        """Получает корень слова для железного сравнения"""
        return self.stemmer_ru.stem(word.lower())

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        if not golden_seed or not keyword:
            return keyword
        
        seed_words = re.findall(r'[а-яёА-ЯЁa-zA-Z]+', golden_seed.lower())
        seed_map = {} # Карта лемм
        stem_map = {} # Карта корней (запасной вариант)
        
        for sw in seed_words:
            # Вариант 1: По морфологии
            for p in self.morph.parse(sw):
                seed_map[p.normal_form] = sw
            # Вариант 2: По корню (Stemming)
            stem_map[self.get_stem(sw)] = sw

        tokens = keyword.split()
        normalized_tokens = []
        
        for t in tokens:
            match = re.match(r'^([^а-яёА-ЯЁa-zA-Z]*)([а-яёА-ЯЁa-zA-Z]+)([^а-яёА-ЯЁa-zA-Z]*)$', t)
            if not match:
                normalized_tokens.append(t); continue
                
            prefix, word_body, suffix = match.groups()
            word_lower = word_body.lower()
            
            # 1. Пробуем через лемму (точно)
            found = False
            for p_token in self.morph.parse(word_lower):
                if p_token.normal_form in seed_map:
                    normalized_tokens.append(f"{prefix}{seed_map[p_token.normal_form]}{suffix}")
                    found = True; break
            
            # 2. Если не вышло — пробуем через корень (жестко)
            if not found:
                word_stem = self.get_stem(word_lower)
                if word_stem in stem_map:
                    normalized_tokens.append(f"{prefix}{stem_map[word_stem]}{suffix}")
                    found = True
            
            if not found:
                normalized_tokens.append(t)

        return " ".join(normalized_tokens)

    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        # Возвращаем список один-в-один, без удаления дублей здесь!
        return [self.normalize_by_golden_seed(kw, golden_seed) for kw in keywords]

_normalizer = None
def get_normalizer():
    global _normalizer
    if _normalizer is None: _normalizer = GoldenNormalizer()
    return _normalizer

def normalize_keywords(keywords: List[str], language: str, seed: str) -> List[str]:
    return get_normalizer().process_batch(keywords, seed)
