"""
Universal Normalizer v9.5 - Multilingual Golden Seed
Supports RU/UK (pymorphy3), EN/DE/FR/ES/IT (SnowballStemmer)
Preserves brands, cities, and unique tokens
"""

import re
from typing import List

try:
    import pymorphy3
    PYMORPHY_AVAILABLE = True
except ImportError:
    PYMORPHY_AVAILABLE = False

try:
    from nltk.stem import SnowballStemmer
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False


class GoldenNormalizer:
    def __init__(self):
        """Initialize morphology analyzers and stemmers"""
        # Pymorphy3 for Slavic languages
        self.morph_ru = None
        self.morph_uk = None
        
        # SnowballStemmer for Western languages
        self.stemmers = {}
        
        if PYMORPHY_AVAILABLE:
            self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
            self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk')
        
        if NLTK_AVAILABLE:
            lang_map = {
                'en': 'english',
                'de': 'german',
                'fr': 'french',
                'es': 'spanish',
                'it': 'italian'
            }
            for code, name in lang_map.items():
                try:
                    self.stemmers[code] = SnowballStemmer(name)
                except:
                    pass
    
    def _get_base(self, word: str, lang: str) -> str:
        """
        Get base form of word depending on language
        
        - RU/UK: normal_form via pymorphy3
        - EN/DE/FR/ES/IT: stem via SnowballStemmer
        - Others: word.lower()
        """
        clean_word = word.lower().strip()
        
        # Slavic languages - use pymorphy3
        if lang == 'ru' and self.morph_ru:
            try:
                return self.morph_ru.parse(clean_word)[0].normal_form
            except:
                pass
        elif lang == 'uk' and self.morph_uk:
            try:
                return self.morph_uk.parse(clean_word)[0].normal_form
            except:
                pass
        
        # Western languages - use SnowballStemmer
        elif lang in self.stemmers:
            try:
                return self.stemmers[lang].stem(clean_word)
            except:
                pass
        
        # Fallback - return as is
        return clean_word
    
    def normalize_by_golden_seed(self, keyword: str, golden_seed: str, language: str = 'ru') -> str:
        """
        Golden Seed Mask: normalize only words from seed
        
        CRITICAL: Preserve ALL characters (dashes, dots, etc)
        Use split() instead of regex to avoid losing structure
        
        Examples:
            lang='ru', seed='ремонт пылесосов', kw='dreame ремонту пылесоса'
            -> 'dreame ремонт пылесосов'
            
            lang='en', seed='vacuum repair', kw='london vacuums repairs'
            -> 'london vacuum repair'
        """
        # 1. Create base dictionary from seed using \w+ (letters/digits only for map)
        seed_tokens = re.findall(r'\w+', golden_seed.lower())
        golden_map = {}
        for token in seed_tokens:
            base = self._get_base(token, language)
            golden_map[base] = token
        
        # 2. CRITICAL: Process ENTIRE string without losing ANY characters
        # Split by spaces to preserve dashes, dots, and structure
        words = keyword.split()
        result = []
        
        for word in words:
            # Clean word ONLY for base lookup, but preserve original in result
            clean_word = re.sub(r'[^\w]', '', word.lower())
            
            if not clean_word:
                # If word is only punctuation (like "-"), keep it
                result.append(word)
                continue
            
            base = self._get_base(clean_word, language)
            
            if base in golden_map:
                # If word is from seed - replace with golden form
                result.append(golden_map[base])
            else:
                # CRITICAL: If word NOT in seed (Dreame, Bosch, Адреса)
                # Return it EXACTLY as it was in original keyword!
                result.append(word)
        
        return " ".join(result)
    
    def process_batch(self, keywords: List[str], golden_seed: str, language: str = 'ru') -> List[str]:
        """
        Normalize batch and remove duplicates
        
        Args:
            keywords: List of keywords to normalize
            golden_seed: Reference seed with correct forms
            language: Language code (ru, uk, en, de, fr, es, it)
            
        Returns:
            Normalized and deduplicated keywords (preserves order)
        """
        if not keywords or not golden_seed:
            return keywords
        
        # Normalize all keywords
        normalized = [
            self.normalize_by_golden_seed(kw, golden_seed, language) 
            for kw in keywords
        ]
        
        # Remove duplicates, preserve order (dict.fromkeys is faster than set)
        return list(dict.fromkeys(normalized))


# Global instance
_normalizer = None


def get_normalizer():
    """Get or create global normalizer instance"""
    global _normalizer
    if _normalizer is None:
        _normalizer = GoldenNormalizer()
    return _normalizer


def normalize_keywords(keywords: List[str], language: str = 'ru', seed: str = '') -> List[str]:
    """
    Normalize keywords using Golden Seed approach
    
    Args:
        keywords: List of keywords
        language: Language code (ru, uk, en, de, fr, es, it)
        seed: Reference seed (golden form)
        
    Returns:
        Normalized and deduplicated keywords
    """
    if not seed:
        return keywords
    
    normalizer = get_normalizer()
    return normalizer.process_batch(keywords, seed, language)
