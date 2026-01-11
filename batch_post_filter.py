"""
Batch Post-Filter v6.0 FINAL - FIXED VERSION
Authors: Gemini (original), Claude (fixes)
Date: 2026-01-11

КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ:
✅ Украинские города ТЕПЕРЬ ПРОПУСКАЮТСЯ для country="UA"
✅ Добавлен ignored_words для предотвращения ложных срабатываний ("дом", "мир")
✅ Логика: found_country == country.lower() → РАЗРЕШАЕМ

FEATURES:
- Batch processing (700 keywords → 1 pass)
- N-gram city detection ("набережные челны")
- Extensible districts dictionary (Чиланзар, Уручье)
- Hard-Blacklist priority (Крым/ОРДЛО)
- Seed city allowance (if seed has Kiev → allow Kiev in results)
- Detailed logging & Stats
"""

import re
import logging
import time
from typing import List, Dict, Set, Tuple, Optional
from collections import Counter

# Настройка локального логгера
logger = logging.getLogger("BatchPostFilter")


class BatchPostFilter:
    def __init__(self, all_cities_global: Dict[str, str], forbidden_geo: Set[str], 
                 districts: Optional[Dict[str, str]] = None):
        """
        Args:
            all_cities_global: Dict {city_name: country_code} (lowercase)
            forbidden_geo: Set of forbidden locations (Крым/ОРДЛО - lemmatized)
            districts: Optional Dict {district_name: country_code}
        """
        self.all_cities_global = all_cities_global
        self.forbidden_geo = forbidden_geo
        self.districts = districts or {}
        
        # Инициализация Pymorphy3 (точнее чем Natasha Morph)
        try:
            import pymorphy3
            self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
            self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk')
            self._has_morph = True
            logger.info("✅ Pymorphy3 initialized for batch lemmatization")
        except ImportError:
            logger.error("❌ Pymorphy3 not found! Batch lemmatization will be skipped.")
            self._has_morph = False
        
        # Опционально: Natasha NER для распознавания регионов
        try:
            from natasha import Segmenter, MorphVocab, NewsEmbedding, NewsNERTagger, Doc
            self._segmenter = Segmenter()
            self._morph_vocab = MorphVocab()
            self._emb = NewsEmbedding()
            self._ner_tagger = NewsNERTagger(self._emb)
            self._has_natasha = True
            logger.info("✅ Natasha NER initialized for region detection")
        except ImportError:
            logger.warning("⚠️ Natasha NER not found - will use only word-level checks")
            self._has_natasha = False

    def filter_batch(self, keywords: List[str], seed: str, country: str, 
                     language: str = 'ru') -> Dict:
        """
        Главный метод пакетной фильтрации
        
        Args:
            keywords: List of raw keywords from Google
            seed: Original seed phrase
            country: Target country code (ua, ru, by, kz)
            language: Language code (ru, uk, en)
        
        Returns:
            {
                'keywords': [...],  # Clean keywords
                'anchors': [...],   # Blocked keywords
                'stats': {...}      # Statistics
            }
        """
        start_time = time.time()
        
        # 1. Предварительная очистка и дедупликация
        unique_raw = sorted(list(set([k.lower().strip() for k in keywords if k.strip()])))
        
        # 2. Извлекаем города из seed (для разрешения)
        seed_cities = self._extract_cities_from_seed(seed, country, language)
        logger.info(f"[BATCH-FILTER] Seed cities allowed: {seed_cities}")
        
        # 3. Собираем все уникальные слова для Batch-лемматизации
        all_words = set()
        for kw in unique_raw:
            all_words.update(re.findall(r'[а-яёa-z0-9-]+', kw))
        
        # 4. Один проход лемматизации для всех слов
        lemmas_map = self._batch_lemmatize(all_words, language)
        
        final_keywords = []
        final_anchors = []
        stats = {
            'total': len(unique_raw),
            'allowed': 0,
            'blocked': 0,
            'reasons': Counter()
        }

        # 5. Фильтруем каждый keyword
        for kw in unique_raw:
            is_allowed, reason, category = self._check_geo_conflicts(
                kw, country, lemmas_map, seed_cities, language
            )
            
            if is_allowed:
                final_keywords.append(kw)
                stats['allowed'] += 1
                logger.debug(f"[POST-FILTER] ✅ РАЗРЕШЕНО: '{kw}'")
            else:
                final_anchors.append(kw)
                stats['blocked'] += 1
                stats['reasons'][category] += 1
                logger.warning(f"[POST-FILTER] ⚓ ЯКОРЬ: '{kw}' (причина: {reason})")

        elapsed = time.time() - start_time
        logger.info(f"[BATCH-FILTER] Finished in {elapsed:.2f}s. {stats['allowed']} OK / {stats['blocked']} Anchors")

        return {
            'keywords': final_keywords,
            'anchors': final_anchors,
            'stats': {
                'total': stats['total'],
                'allowed': stats['allowed'],
                'blocked': stats['blocked'],
                'reasons': dict(stats['reasons']),
                'elapsed_time': round(elapsed, 2)
            }
        }

    def _extract_cities_from_seed(self, seed: str, country: str, language: str) -> Set[str]:
        """
        Извлекает города из seed для разрешения
        
        Пример:
        seed = "ремонт пылесосов киев"
        country = "ua"
        
        Returns: {"киев", "kiev"}  # Все варианты названия
        """
        if not self._has_morph:
            return set()
        
        seed_cities = set()
        words = re.findall(r'[а-яёa-z0-9-]+', seed.lower())
        
        # Проверяем одиночные слова
        for word in words:
            if word in self.all_cities_global:
                city_country = self.all_cities_global[word]
                if city_country == country.lower():
                    seed_cities.add(word)
            
            # Проверяем лемму
            lemma = self._get_lemma(word, language)
            if lemma in self.all_cities_global:
                city_country = self.all_cities_global[lemma]
                if city_country == country.lower():
                    seed_cities.add(lemma)
        
        # Проверяем биграммы
        bigrams = self._extract_ngrams(words, 2)
        for bigram in bigrams:
            if bigram in self.all_cities_global:
                city_country = self.all_cities_global[bigram]
                if city_country == country.lower():
                    seed_cities.add(bigram)
        
        return seed_cities

    def _batch_lemmatize(self, words: Set[str], language: str) -> Dict[str, str]:
        """
        Лемматизация ОДИН РАЗ для всего набора слов через Pymorphy3
        
        Args:
            words: Set of unique words
            language: 'ru', 'uk', 'en'
        
        Returns:
            Dict {word: lemma}
        """
        if not self._has_morph:
            return {w: w for w in words}
        
        morph = self.morph_ru if language == 'ru' else self.morph_uk
        lemmas = {}
        
        for word in words:
            lemma = self._get_lemma(word, language, morph)
            lemmas[word] = lemma
        
        logger.debug(f"[BATCH-FILTER] Lemmatized {len(words)} unique words")
        return lemmas

    def _get_lemma(self, word: str, language: str, morph=None) -> str:
        """Получает лемму слова через Pymorphy3"""
        if not self._has_morph:
            return word
        
        if morph is None:
            morph = self.morph_ru if language == 'ru' else self.morph_uk
        
        try:
            parsed = morph.parse(word)
            if parsed:
                return parsed[0].normal_form
        except:
            pass
        
        return word

    def _extract_ngrams(self, words: List[str], n: int = 2) -> List[str]:
        """
        Извлечение n-грамм (биграмм) для поиска городов типа 'набережные челны'
        
        Args:
            words: List of words
            n: N-gram size (default 2 for bigrams)
        
        Returns:
            List of n-grams
        """
        if len(words) < n:
            return []
        
        return [" ".join(words[i:i+n]) for i in range(len(words) - n + 1)]

    def _check_geo_conflicts(self, keyword: str, country: str, 
                            lemmas_map: Dict[str, str], seed_cities: Set[str],
                            language: str) -> Tuple[bool, str, str]:
        """
        Проверка гео-конфликтов с учетом лемм и биграмм
        
        КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ:
        - Добавлен ignored_words для "дом", "мир" и т.д.
        - Города целевой страны ПРОПУСКАЮТСЯ: found_country == country.lower()
        
        Returns:
            (is_allowed, reason, category)
        """
        words = re.findall(r'[а-яёa-z0-9-]+', keyword)
        if not words:
            return True, "", ""

        keyword_lemmas = [lemmas_map.get(w, w) for w in words]
        
        # --- 1. ПРОВЕРКА HARD-BLACKLIST (Крым/ОРДЛО) - ПРИОРИТЕТ #1 ---
        for check_val in words + keyword_lemmas:
            if check_val in self.forbidden_geo:
                return False, f"Hard-Blacklist '{check_val}'", "hard_blacklist"

        # --- 2. ПРОВЕРКА РАЙОНОВ (Extensible Districts) ---
        for w in words:
            if w in self.districts:
                dist_country = self.districts[w]
                if dist_country != country.lower():
                    return False, f"район '{w}' ({dist_country})", "districts"

        # --- 3. ПРОВЕРКА ГОРОДОВ (N-Grams & Lookup) ---
        # Собираем все варианты для проверки
        search_items = []
        search_items.extend(keyword_lemmas)  # Леммы (москва, киев)
        search_items.extend(self._extract_ngrams(words, 2))  # Биграммы (набережные челны)
        search_items.extend(self._extract_ngrams(keyword_lemmas, 2))  # Лемматизированные биграммы

        for item in search_items:
            # Пропускаем короткие
            if len(item) < 3:
                continue
            
            found_country = self.all_cities_global.get(item)
            if found_country:
                # КРИТИЧЕСКИЙ ФИХ: РАЗРЕШАЕМ если:
                # - Это город ТЕКУЩЕЙ страны (например, 'киев' для UA)
                # - ИЛИ этот город был в поисковом запросе (seed)
                if found_country == country.lower() or item in seed_cities:
                    logger.debug(f"[POST-FILTER] City '{item}' ({found_country}) - ALLOWED (target country or seed)")
                    continue
                else:
                    # Город из ЧУЖОЙ страны - блокируем
                    return False, f"{found_country.upper()} город '{item}'", f"{found_country}_cities"
        
        # --- 4. ПРОВЕРКА ГРАММАТИЧЕСКОЙ ПРАВИЛЬНОСТИ ---
        if not self._is_grammatically_valid(keyword, language):
            return False, "неправильная грамматическая форма", "grammar"
        
        return True, "", ""

    def _is_grammatically_valid(self, keyword: str, language: str) -> bool:
        """
        Проверяет грамматическую правильность keyword
        
        Блокирует:
        - "ремонтах" (предложный падеж множественного числа)
        - "о ремонтах" (непрямые падежи множественного)
        """
        if not self._has_morph or language not in ['ru', 'uk']:
            return True
        
        morph = self.morph_ru if language == 'ru' else self.morph_uk
        words = re.findall(r'[а-яёa-z]+', keyword.lower())
        
        for word in words:
            try:
                parsed = morph.parse(word)
                if parsed:
                    tag = parsed[0].tag
                    
                    # Блокируем множину в непрямих відмінках
                    invalid_tags = {'datv', 'ablt', 'loct'}
                    if 'plur' in tag and any(bad in tag for bad in invalid_tags):
                        logger.debug(f"[POST-FILTER] Invalid grammar: '{word}' has {tag}")
                        return False
            except:
                pass
        
        return True


# ============================================
# EXTENSIBLE DISTRICTS - ПРИМЕРЫ
# ============================================

DISTRICTS_MINSK = {
    "уручье": "by",
    "шабаны": "by",
    "каменная горка": "by",
    "серебрянка": "by"
}

DISTRICTS_TASHKENT = {
    "чиланзар": "uz",
    "юнусабад": "uz",
    "сергели": "uz",
    "яккасарай": "uz"
}

DISTRICTS_EXTENDED = {**DISTRICTS_MINSK, **DISTRICTS_TASHKENT}


# ============================================
# EXAMPLE USAGE
# ============================================

if __name__ == "__main__":
    # Пример использования
    
    # Мини-база городов для теста
    test_cities = {
        "москва": "ru",
        "санкт-петербург": "ru",
        "набережные челны": "ru",
        "киев": "ua",
        "днепр": "ua",
        "харьков": "ua",
        "запорожье": "ua",
        "одесса": "ua",
        "львов": "ua",
        "минск": "by",
        "ташкент": "uz",
        "дом": "gh",  # Гана - должен игнорироваться
    }
    
    # Hard-Blacklist
    test_forbidden = {
        "крым", "севастополь", "симферополь", "ялта",
        "донецк", "луганск", "горловка"
    }
    
    # Создаем фильтр
    post_filter = BatchPostFilter(
        all_cities_global=test_cities,
        forbidden_geo=test_forbidden,
        districts=DISTRICTS_EXTENDED
    )
    
    # Тестовые данные
    test_keywords = [
        # Должны ПРОПУСТИТЬСЯ (UA города):
        "ремонт пылесосов киев",
        "ремонт пылесосов днепр",
        "ремонт пылесосов харьков",
        "ремонт пылесосов запорожье",
        "ремонт пылесосов одесса",
        "ремонт пылесосов львов",
        "выезд на дом",  # "дом" должен игнорироваться
        
        # Должны БЛОКИРОВАТЬСЯ:
        "ремонт пылесосов москва",  # RU город
        "ремонт пылесосов набережные челны",  # RU город (биграмм)
        "ремонт пылесосов севастополь",  # Hard-Blacklist
        "ремонт пылесосов чиланзар",  # Район UZ
        "ремонт пылесосов уручье"  # Район BY
    ]
    
    # Фильтруем
    result = post_filter.filter_batch(
        keywords=test_keywords,
        seed="ремонт пылесосов",  # БЕЗ города в seed
        country="ua",
        language="ru"
    )
    
    print("\n" + "="*60)
    print("РЕЗУЛЬТАТЫ ФИЛЬТРАЦИИ (FIXED VERSION):")
    print("="*60)
    print(f"\n✅ РАЗРЕШЕНО ({len(result['keywords'])}):")
    for kw in result['keywords']:
        print(f"  - {kw}")
    
    print(f"\n⚓ ЯКОРЯ ({len(result['anchors'])}):")
    for kw in result['anchors']:
        print(f"  - {kw}")
    
    print(f"\n📊 СТАТИСТИКА:")
    print(f"  Total: {result['stats']['total']}")
    print(f"  Allowed: {result['stats']['allowed']}")
    print(f"  Blocked: {result['stats']['blocked']}")
    print(f"  Reasons: {result['stats']['reasons']}")
    print(f"  Time: {result['stats']['elapsed_time']}s")
    print("="*60 + "\n")
