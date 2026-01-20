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
        
        CRITICAL: If word base is NOT in seed dictionary (brand, city, detail),
        return word in original form. DO NOT remove it!
        
        Examples:
            lang='ru', seed='ремонт пылесосов', kw='dreame ремонту пылесоса'
            -> 'dreame ремонт пылесосов'
            
            lang='en', seed='vacuum repair', kw='london vacuums repairs'
            -> 'london vacuum repair'
        """
        # 1. Create base dictionary from seed: {base: original_form}
        seed_tokens = re.findall(r'[а-яёa-z0-9]+', golden_seed.lower())
        golden_map = {}
        for token in seed_tokens:
            base = self._get_base(token, language)
            golden_map[base] = token
        
        # 2. Split keyword into words
        # Regex catches ALL words: cyrillic, latin, numbers
        kw_tokens = re.findall(r'[а-яёa-z0-9]+', keyword.lower())
        
        result_tokens = []
        for word in kw_tokens:
            word_base = self._get_base(word, language)
            
            # 3. If base exists in seed dictionary - replace with seed form
            if word_base in golden_map:
                result_tokens.append(golden_map[word_base])
            else:
                # 4. CRITICAL: If base NOT in seed (brand/city/detail) - keep original
                result_tokens.append(word)
        
        return " ".join(result_tokens)
    
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
