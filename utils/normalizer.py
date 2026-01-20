"""
Smart Normalizer v9.2 - Golden Seed Mask
Normalizes only words from seed, keeps brands/cities/details intact
Supports multiple languages: ru, uk, en, de, fr, es, it
"""

import re
import pymorphy3
from typing import List

try:
    from nltk.stem import SnowballStemmer
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False


class GoldenNormalizer:
    def __init__(self):
        self.morph_ru = None
        self.morph_uk = None
        self.stemmers = {}
    
    def _get_morph(self, language: str):
        """Get morphology analyzer for ru/uk"""
        if language == 'ru':
            if self.morph_ru is None:
                self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
            return self.morph_ru
        elif language == 'uk':
            if self.morph_uk is None:
                self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk')
            return self.morph_uk
        return None
    
    def _get_stemmer(self, language: str):
        """Get stemmer for western languages"""
        if not NLTK_AVAILABLE:
            return None
        if language not in self.stemmers:
            lang_map = {
                'en': 'english',
                'de': 'german',
                'fr': 'french',
                'es': 'spanish',
                'it': 'italian'
            }
            if language in lang_map:
                self.stemmers[language] = SnowballStemmer(lang_map[language])
        return self.stemmers.get(language)
    
    def _get_lemma(self, word: str, language: str) -> str:
        """Get lemma (base form) of word based on language"""
        clean_word = word.lower().strip()
        
        # For ru/uk use pymorphy3
        if language in ['ru', 'uk']:
            morph = self._get_morph(language)
            if morph:
                try:
                    return morph.parse(clean_word)[0].normal_form
                except:
                    pass
        
        # For western languages use NLTK Snowball Stemmer
        elif language in ['en', 'de', 'fr', 'es', 'it']:
            stemmer = self._get_stemmer(language)
            if stemmer:
                try:
                    return stemmer.stem(clean_word)
                except:
                    pass
        
        # Fallback - return as is
        return clean_word

    def normalize_by_golden_seed(self, keyword: str, golden_seed: str, language: str = 'ru') -> str:
        """
        Golden Seed Mask: normalizes only words from seed,
        keeps everything else (brands, cities) untouched.
        """
        # 1. Create lemma map from seed
        seed_tokens = re.findall(r'[а-яёa-z0-9]+', golden_seed.lower())
        golden_map = {}
        for token in seed_tokens:
            lemma = self._get_lemma(token, language)
            golden_map[lemma] = token

        # 2. Process keyword word by word
        # Regex catches ALL words including latin (dreame, bosch) and numbers
        kw_tokens = re.findall(r'[а-яёa-z0-9]+', keyword.lower())
        result_tokens = []
        
        for word in kw_tokens:
            lemma = self._get_lemma(word, language)
            if lemma in golden_map:
                # If word is part of seed (ремонту -> ремонт), use form from seed
                result_tokens.append(golden_map[lemma])
            else:
                # If it's brand (dreame), city, or detail - keep as is
                result_tokens.append(word)
        
        return " ".join(result_tokens)

    def process_batch(self, keywords: List[str], golden_seed: str, language: str = 'ru') -> List[str]:
        """Normalization + deduplication of real duplicates"""
        if not keywords or not golden_seed:
            return keywords
            
        normalized = [self.normalize_by_golden_seed(kw, golden_seed, language) for kw in keywords]
        
        # Remove duplicates, preserve order (dict.fromkeys faster and more stable than set)
        return list(dict.fromkeys(normalized))


# Global instance for use in main.py
_normalizer = GoldenNormalizer()


def normalize_keywords(keywords: List[str], language: str = 'ru', seed: str = '') -> List[str]:
    """Normalize keywords using Golden Seed approach"""
    if not seed:
        return keywords
    return _normalizer.process_batch(keywords, seed, language)
