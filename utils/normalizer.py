import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        # Инициализируем анализатор один раз для экономии памяти
        self.morph = pymorphy3.MorphAnalyzer()

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        if not golden_seed or not keyword:
            return keyword
        
        # 1. Составляем карту основ из ОРИГИНАЛЬНОГО сида
        seed_words = re.findall(r'\w+', golden_seed.lower())
        seed_map = {}
        
        for sw in seed_words:
            # Берем все варианты разбора слова из сида
            parses = self.morph.parse(sw)
            for p in parses:
                # Каждой возможной нормальной форме сопоставляем слово из сида
                # Например, для "пылесосов" нормальная форма "пылесос"
                seed_map[p.normal_form] = sw

        # 2. Обрабатываем ключ по словам
        tokens = keyword.split()
        normalized_tokens = []
        
        for t in tokens:
            # Сохраняем знаки препинания, если они приклеены к слову (например, "пылесос?")
            match = re.match(r'^([^а-яёА-ЯЁa-zA-Z]*)([а-яёА-ЯЁa-zA-Z]+)([^а-яёА-ЯЁa-zA-Z]*)$', t)
            
            if not match:
                normalized_tokens.append(t)
                continue
                
            prefix, word_body, suffix = match.groups()
            word_lower = word_body.lower()
            
            # Проверяем все варианты разбора текущего слова
            p_token_list = self.morph.parse(word_lower)
            found_in_seed = False
            
            for p_token in p_token_list:
                t_base = p_token.normal_form
                if t_base in seed_map:
                    # Если нашли основу в сиде, заменяем тело слова на форму из сида
                    # Сохраняем оригинальные префиксы/суффиксы (знаки препинания)
                    normalized_tokens.append(f"{prefix}{seed_map[t_base]}{suffix}")
                    found_in_seed = True
                    break
            
            if not found_in_seed:
                # Если слова нет в сиде (город, спецслово), оставляем как было
                normalized_tokens.append(t)

        return " ".join(normalized_tokens)

    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        if not keywords: return []
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
