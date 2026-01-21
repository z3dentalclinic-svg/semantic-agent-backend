import re
import pymorphy3
from typing import List

class GoldenNormalizer:
    def __init__(self):
        # Инициализация анализатора (делается один раз)
        self.morph = pymorphy3.MorphAnalyzer()

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str) -> str:
        """
        Принудительно меняет формы слов в ключе на те, что указаны в golden_seed.
        Пример: если в сиде 'пылесосов', а в ключе 'пылесоса' -> станет 'пылесосов'.
        """
        if not golden_seed or not keyword:
            return keyword
        
        # 1. Составляем карту основ из ОРИГИНАЛЬНОГО сида (например, 'ремонт пылесосов')
        # Извлекаем только слова (буквы), игнорируя символы
        seed_words = re.findall(r'[а-яёА-ЯЁa-zA-Z]+', golden_seed.lower())
        seed_map = {}
        
        for sw in seed_words:
            # Получаем все возможные грамматические разборы слова из сида
            parses = self.morph.parse(sw)
            for p in parses:
                # Каждой возможной нормальной форме (лемме) сопоставляем 
                # именно то написание, которое ввел пользователь в сиде.
                # 'пылесос' -> 'пылесосов'
                seed_map[p.normal_form] = sw

        # 2. Разбиваем поисковую подсказку на токены (слова)
        tokens = keyword.split()
        normalized_tokens = []
        
        for t in tokens:
            # Регулярка для отделения знаков препинания от самого слова
            # Группа 1: символы в начале, Группа 2: буквы, Группа 3: символы в конце
            match = re.match(r'^([^а-яёА-ЯЁa-zA-Z]*)([а-яёА-ЯЁa-zA-Z]+)([^а-яёА-ЯЁa-zA-Z]*)$', t)
            
            if not match:
                # Если в токене нет букв (например, просто цифра или символ), оставляем как есть
                normalized_tokens.append(t)
                continue
                
            prefix, word_body, suffix = match.groups()
            word_lower = word_body.lower()
            
            # Проверяем все варианты разбора текущего слова из ключа
            p_token_list = self.morph.parse(word_lower)
            found_in_seed = False
            
            for p_token in p_token_list:
                t_base = p_token.normal_form # Нормальная форма (например, 'пылесос')
                
                if t_base in seed_map:
                    # Если лемма слова совпала с леммой из сида — 
                    # подменяем тело слова на форму из сида ('пылесосов')
                    # Сохраняем знаки препинания (prefix/suffix), если они были
                    normalized_tokens.append(f"{prefix}{seed_map[t_base]}{suffix}")
                    found_in_seed = True
                    break
            
            if not found_in_seed:
                # Если слова нет в сиде (например, это город 'Киев'), оставляем оригинал
                normalized_tokens.append(t)

        # Собираем фразу обратно. Количество слов не меняется.
        return " ".join(normalized_tokens)

    def process_batch(self, keywords: List[str], golden_seed: str) -> List[str]:
        """Обрабатывает список ключей, удаляя только полные дубликаты строк."""
        if not keywords:
            return []
        
        # Удаляем идентичные строки перед обработкой для скорости
        unique_raw = list(dict.fromkeys(keywords))
        
        return [self.normalize_by_golden_seed(kw, golden_seed) for kw in unique_raw]

# Синглтон для работы в API
_normalizer = None

def get_normalizer():
    global _normalizer
    if _normalizer is None:
        _normalizer = GoldenNormalizer()
    return _normalizer

def normalize_keywords(keywords: List[str], language: str, seed: str) -> List[str]:
    """Главная точка входа для main.py"""
    n = get_normalizer()
    return n.process_batch(keywords, seed)
