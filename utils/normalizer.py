"""
Keyword normalizer - converts keywords to their base forms
Handles morphology normalization for multiple languages
"""

import re
from typing import List

try:
    import pymorphy3
    PYMORPHY_AVAILABLE = True
except ImportError:
    PYMORPHY_AVAILABLE = False

from nltk.stem import SnowballStemmer


# Морфологические анализаторы (ленивая инициализация)
_morph_ru = None
_morph_uk = None
_stemmers = {}


def _get_morph_ru():
    """Ленивая инициализация morph_ru"""
    global _morph_ru
    if _morph_ru is None and PYMORPHY_AVAILABLE:
        _morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
    return _morph_ru


def _get_morph_uk():
    """Ленивая инициализация morph_uk"""
    global _morph_uk
    if _morph_uk is None and PYMORPHY_AVAILABLE:
        _morph_uk = pymorphy3.MorphAnalyzer(lang='uk')
    return _morph_uk


def _get_stemmer(language: str):
    """Ленивая инициализация стеммеров"""
    if language not in _stemmers:
        lang_map = {
            'en': 'english',
            'de': 'german',
            'fr': 'french',
            'es': 'spanish',
            'it': 'italian'
        }
        _stemmers[language] = SnowballStemmer(lang_map.get(language, 'english'))
    return _stemmers[language]


def normalize_word(word: str, language: str = 'ru') -> str:
    """
    Нормализует одно слово в начальную форму
    
    Args:
        word: Слово для нормализации
        language: Код языка (ru, uk, en, de, fr, es, it)
        
    Returns:
        Нормализованное слово
    """
    word_lower = word.lower()
    
    # Для ru/uk используем pymorphy3
    if language in ['ru', 'uk'] and PYMORPHY_AVAILABLE:
        morph = _get_morph_ru() if language == 'ru' else _get_morph_uk()
        if morph:
            try:
                parsed = morph.parse(word_lower)
                if parsed:
                    return parsed[0].normal_form
            except:
                pass
    
    # Для западных языков используем Snowball Stemmer
    elif language in ['en', 'de', 'fr', 'es', 'it']:
        stemmer = _get_stemmer(language)
        try:
            return stemmer.stem(word_lower)
        except:
            pass
    
    # Fallback - возвращаем как есть
    return word_lower


def normalize_keyword(keyword: str, language: str = 'ru') -> str:
    """
    Нормализует ключевое слово (фразу)
    
    Примеры:
        "ремонту пылесосов керхєр" → "ремонт пылесос керхер"
        "ремонты стиральных машин" → "ремонт стиральный машина"
    
    Args:
        keyword: Ключевое слово/фраза
        language: Код языка
        
    Returns:
        Нормализованное ключевое слово
    """
    # Разбиваем на слова
    words = re.findall(r'\w+', keyword.lower())
    
    # Нормализуем каждое слово
    normalized_words = [normalize_word(word, language) for word in words]
    
    # Собираем обратно в строку
    return ' '.join(normalized_words)


def normalize_keywords(keywords: List[str], language: str = 'ru') -> List[str]:
    """
    Нормализует список ключевых слов
    
    Args:
        keywords: Список ключевых слов
        language: Код языка
        
    Returns:
        Список нормализованных ключевых слов
    """
    return [normalize_keyword(kw, language) for kw in keywords]
