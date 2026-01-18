"""
Relevance filter - filters keywords by relevance to seed query
"""

import re
from typing import List, Set
from difflib import SequenceMatcher

try:
    import pymorphy3
    PYMORPHY_AVAILABLE = True
except ImportError:
    PYMORPHY_AVAILABLE = False

from nltk.stem import SnowballStemmer


# Stop words по языкам
STOP_WORDS = {
    'ru': {'и', 'в', 'во', 'не', 'на', 'с', 'от', 'для', 'по', 'о', 'об', 'к', 'у', 'за',
           'из', 'со', 'до', 'при', 'без', 'над', 'под', 'а', 'но', 'да', 'или', 'чтобы',
           'что', 'как', 'где', 'когда', 'куда', 'откуда', 'почему'},
    'uk': {'і', 'в', 'на', 'з', 'від', 'для', 'по', 'о', 'до', 'при', 'без', 'над', 'під',
           'а', 'але', 'та', 'або', 'що', 'як', 'де', 'коли', 'куди', 'звідки', 'чому'},
    'en': {'a', 'an', 'the', 'in', 'on', 'at', 'to', 'for', 'o', 'with', 'by', 'from',
           'up', 'about', 'into', 'through', 'during', 'and', 'or', 'but', 'i', 'when',
           'where', 'how', 'why', 'what'},
}

# Морфологические анализаторы (инициализируются при первом использовании)
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


def _normalize_with_pymorphy(text: str, language: str) -> Set[str]:
    """Нормализация через Pymorphy"""
    morph = _get_morph_ru() if language == 'ru' else _get_morph_uk()
    
    if not morph:
        return set()
    
    stop_words = STOP_WORDS.get(language, STOP_WORDS['ru'])
    words = re.findall(r'\w+', text.lower())
    meaningful = [w for w in words if w not in stop_words and len(w) > 1]
    
    lemmas = set()
    for word in meaningful:
        try:
            parsed = morph.parse(word)
            if parsed:
                lemmas.add(parsed[0].normal_form)
        except:
            lemmas.add(word)
    
    return lemmas


def _normalize_with_snowball(text: str, language: str) -> Set[str]:
    """Нормализация через Snowball Stemmer"""
    stemmer = _get_stemmer(language)
    stop_words = STOP_WORDS.get(language, STOP_WORDS['en'])
    words = re.findall(r'\w+', text.lower())
    meaningful = [w for w in words if w not in stop_words and len(w) > 1]
    stems = {stemmer.stem(w) for w in meaningful}
    return stems


def _normalize(text: str, language: str = 'ru') -> Set[str]:
    """Нормализация текста (лемматизация/стемминг)"""
    if language in ['ru', 'uk']:
        return _normalize_with_pymorphy(text, language)
    elif language in ['en', 'de', 'fr', 'es', 'it']:
        return _normalize_with_snowball(text, language)
    else:
        words = re.findall(r'\w+', text.lower())
        stop_words = STOP_WORDS.get('en', set())
        meaningful = [w for w in words if w not in stop_words and len(w) > 1]
        return set(meaningful)


def is_grammatically_valid(seed_word: str, kw_word: str, language: str = 'ru') -> bool:
    """Проверка грамматической корректности"""
    if language not in ['ru', 'uk']:
        return True
    
    try:
        morph = _get_morph_ru() if language == 'ru' else _get_morph_uk()
        
        if not morph:
            return True
        
        parsed_seed = morph.parse(seed_word)
        parsed_kw = morph.parse(kw_word)
        
        if not parsed_seed or not parsed_kw:
            return True
        
        seed_form = parsed_seed[0]
        kw_form = parsed_kw[0]
        
        if seed_form.normal_form != kw_form.normal_form:
            return True
        
        invalid_tags = {'datv', 'ablt', 'loct'}
        
        if 'plur' in kw_form.tag and any(tag in kw_form.tag for tag in invalid_tags):
            return False
        
        return True
    
    except Exception as e:
        return True


async def filter_relevant_keywords(keywords: List[str], seed: str, language: str = 'ru') -> List[str]:
    """
    Фильтр релевантности: оставляет только ключевые слова релевантные seed
    
    Args:
        keywords: List of keyword strings
        seed: Seed query
        language: Language code
        
    Returns:
        Filtered list of relevant keywords
    """
    
    seed_lemmas = _normalize(seed, language)
    
    if not seed_lemmas:
        return keywords
    
    seed_lower = seed.lower()
    seed_words_original = [w.lower() for w in re.findall(r'\w+', seed) if len(w) > 2]
    
    stop_words = STOP_WORDS.get(language, STOP_WORDS['ru'])
    seed_important_words = [w for w in seed_words_original if w not in stop_words]
    
    if not seed_important_words:
        seed_important_words = seed_words_original
    
    filtered = []
    
    for keyword in keywords:
        kw_lower = keyword.lower()
        
        kw_lemmas = _normalize(keyword, language)
        if not seed_lemmas.issubset(kw_lemmas):
            continue
        
        kw_words = kw_lower.split()
        matches = 0
        grammatically_valid = True
        
        for seed_word in seed_important_words:
            found_match = False
            
            for kw_word in kw_words:
                if seed_word in kw_word:
                    if is_grammatically_valid(seed_word, kw_word, language):
                        found_match = True
                        break
                    else:
                        grammatically_valid = False
                        break
            
            if found_match:
                matches += 1
        
        if not grammatically_valid:
            continue
        
        if len(seed_important_words) > 0:
            match_ratio = matches / len(seed_important_words)
            if match_ratio < 1.0:
                continue
        
        first_seed_word = seed_important_words[0]
        first_word_position = -1
        
        for i, kw_word in enumerate(kw_words):
            if first_seed_word in kw_word:
                first_word_position = i
                break
        
        if first_word_position > 1:
            continue
        
        last_index = -1
        order_correct = True
        
        for seed_word in seed_important_words:
            found_at = -1
            for i, kw_word in enumerate(kw_words):
                if i > last_index and seed_word in kw_word:
                    found_at = i
                    break
            
            if found_at == -1:
                order_correct = False
                break
            
            last_index = found_at
        
        if order_correct:
            filtered.append(keyword)
    
    return filtered
