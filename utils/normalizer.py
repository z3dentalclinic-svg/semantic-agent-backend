"""
Keyword normalizer - Golden Seed Mask approach
Normalizes keywords by mapping them to the exact forms used in the seed query
"""

import re
from typing import List

try:
    import pymorphy3
    PYMORPHY_AVAILABLE = True
except ImportError:
    PYMORPHY_AVAILABLE = False

from nltk.stem import SnowballStemmer


class GoldenNormalizer:
    """Normalizes keywords using the Golden Seed Mask approach"""
    
    def __init__(self):
        self.morph_ru = None
        self.morph_uk = None
        self.stemmers = {}
    
    def _get_morph(self, language: str):
        """Get morphology analyzer for language"""
        if not PYMORPHY_AVAILABLE:
            return None
            
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
        """Get stemmer for language"""
        if language not in self.stemmers:
            lang_map = {
                'en': 'english',
                'de': 'german',
                'fr': 'french',
                'es': 'spanish',
                'it': 'italian'
            }
            self.stemmers[language] = SnowballStemmer(lang_map.get(language, 'english'))
        return self.stemmers[language]
    
    def _get_lemma(self, word: str, language: str = 'ru') -> str:
        """Get lemma (normal form) of a word"""
        clean_word = word.lower().strip()
        
        # For ru/uk use pymorphy3
        if language in ['ru', 'uk']:
            morph = self._get_morph(language)
            if morph:
                try:
                    return morph.parse(clean_word)[0].normal_form
                except:
                    pass
        
        # For western languages use stemmer
        elif language in ['en', 'de', 'fr', 'es', 'it']:
            stemmer = self._get_stemmer(language)
            try:
                return stemmer.stem(clean_word)
            except:
                pass
        
        # Fallback
        return clean_word
    
    def normalize_by_golden_seed(self, keyword: str, golden_seed: str, language: str = 'ru') -> str:
        """
        Golden Seed Mask: normalize keyword using exact forms from seed
        
        Example:
            golden_seed: "ремонт пылесосов"
            keyword: "днепр ремонту пылесоса"
            result: "днепр ремонт пылесосов"
        
        Args:
            keyword: Keyword to normalize
            golden_seed: Reference seed with correct forms
            language: Language code
            
        Returns:
            Normalized keyword
        """
        # 1. Create golden map: {lemma: correct_form} from seed
        seed_tokens = re.findall(r'[а-яёa-z0-9]+', golden_seed.lower())
        golden_map = {}
        for token in seed_tokens:
            lemma = self._get_lemma(token, language)
            golden_map[lemma] = token
        
        # 2. Split keyword into words
        kw_tokens = re.findall(r'[а-яёa-z0-9]+', keyword.lower())
        
        result_tokens = []
        for word in kw_tokens:
            word_lemma = self._get_lemma(word, language)
            
            # 3. If word's lemma is in seed - replace with golden form
            if word_lemma in golden_map:
                result_tokens.append(golden_map[word_lemma])
            else:
                # 4. If word not in seed (city, brand, etc) - keep as is
                result_tokens.append(word)
        
        return " ".join(result_tokens)
    
    def process_batch(self, keywords: List[str], golden_seed: str, language: str = 'ru') -> List[str]:
        """
        Process batch of keywords and remove duplicates
        
        Args:
            keywords: List of keywords
            golden_seed: Reference seed
            language: Language code
            
        Returns:
            Normalized and deduplicated keywords
        """
        normalized = [self.normalize_by_golden_seed(kw, golden_seed, language) for kw in keywords]
        # Remove duplicates while preserving order
        return list(dict.fromkeys(normalized))


# Global instance
_normalizer = None

def get_normalizer():
    """Get global normalizer instance"""
    global _normalizer
    if _normalizer is None:
        _normalizer = GoldenNormalizer()
    return _normalizer


# Convenience functions
def normalize_keyword(keyword: str, language: str = 'ru', seed: str = '') -> str:
    """Normalize single keyword"""
    if not seed:
        return keyword.lower()
    normalizer = get_normalizer()
    return normalizer.normalize_by_golden_seed(keyword, seed, language)


def normalize_keywords(keywords: List[str], language: str = 'ru', seed: str = '') -> List[str]:
    """Normalize list of keywords"""
    if not seed:
        return keywords
    normalizer = get_normalizer()
    return normalizer.process_batch(keywords, seed, language)
