"""
Batch Post-Filter v8.1 - CRITICAL FIX: Seed Protection
BUILD: 2026-01-17-01:30

🔥 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ v8.1:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ПРОБЛЕМА v8.0:
  - Seed protection (строки 427-433) пропускала города из seed
  - Если seed = "ремонт пылесосов ждановичи"
  - То слово "ждановичи" автоматически разрешалось
  - Результат: БЛОКИРОВКА НЕ РАБОТАЛА

РЕШЕНИЕ v8.1:
  - Seed protection теперь НЕ применяется к городам
  - Если слово найдено в all_cities_global И страна != target
  - То оно БЛОКИРУЕТСЯ независимо от наличия в seed
  - Seed protection остаётся только для районов целевой страны
  
✅ РЕЗУЛЬТАТ:
  - "ремонт пылесосов ждановичи" → БЛОКИРУЕТСЯ (BY != UA)
  - "киев ремонт пылесосов" → разрешается (seed city = UA)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Base: v8.0 TWO-LEVEL GEO DATABASE SUPPORT
"""

import re
import logging
import time
from typing import List, Dict, Set, Tuple, Optional
from collections import Counter

# Настройка локального логгера
logger = logging.getLogger("BatchPostFilter")


class BatchPostFilter:
    def __init__(self, 
                 all_cities_global: Dict[str, str], 
                 forbidden_geo: Set[str], 
                 districts: Optional[Dict[str, str]] = None,
                 population_threshold: int = 5000):
        """
        v8.1 Constructor
        
        Args:
            all_cities_global: Dict {city_name: country_code} (lowercase)
            forbidden_geo: Set of forbidden locations (Крым/ОРДЛО - lemmatized)
            districts: Optional Dict {district_name: country_code}
            population_threshold: Minimum city population to consider (default: 5000)
        """
        self.forbidden_geo = forbidden_geo
        self.districts = districts or {}
        self.population_threshold = population_threshold
        
        # v8.1: КРИТИЧЕСКИ ВАЖНО - база городов должна быть lowercase
        # Проверяем и нормализуем если нужно
        self.all_cities_global = {}
        for city, country in all_cities_global.items():
            normalized_city = str(city).lower().strip()
            normalized_country = str(country).lower().strip()
            if normalized_city and normalized_country:
                self.all_cities_global[normalized_city] = normalized_country
        
        logger.warning(f"🔍 v8.1: Loaded {len(self.all_cities_global)} cities (normalized)")
        
        # Дополнительные словари
        self.city_abbreviations = self._get_city_abbreviations()
        self.regions = self._get_regions()
        self.countries = self._get_countries()
        self.manual_small_cities = self._get_manual_small_cities()
        
        # Ignored words - обычные слова которые НЕ являются городами
        self.ignored_words = {
            "дом",      # Ghana (GH) - "выезд на дом"
            "мир",      # Russia villages - "мир цен"
            "бор",      # Serbia - "сосновый бор"  
            "нива",     # Villages - "автомобиль нива"
            "балка",    # Villages - "овражная балка"
            "луч",      # Villages - "солнечный луч"
            "спутник",  # Villages - "спутниковое тв"
            "работа",   # Может быть городом - "ищу работу"
            "цена",     # Может быть городом - "лучшая цена"
            "выезд",    # Может быть городом - "выезд мастера"
        }
        
        # 🔥 КРИТИЧЕСКИЙ ТЕСТ v8.1
        logger.error("="*60)
        logger.error("🔥 v8.1 CRITICAL TEST - Problem Cities Check")
        logger.error("="*60)
        
        test_problem_cities = {
            'ждановичи': 'by',
            'жданович': 'by',
            'барановичи': 'by',
            'лошица': 'by',
            'актобе': 'kz',
            'талдыкорган': 'kz',
        }
        
        all_ok = True
        for city, expected in test_problem_cities.items():
            in_dict = city in self.all_cities_global
            actual = self.all_cities_global.get(city, 'NOT_FOUND')
            status = "✅" if (in_dict and actual == expected) else "❌"
            
            if not (in_dict and actual == expected):
                all_ok = False
            
            logger.error(f"{status} '{city}': in_dict={in_dict}, value={actual}, expected={expected}")
        
        if all_ok:
            logger.error("🚀 ✅ ALL TESTS PASSED - Filter is READY")
        else:
            logger.error("⚠️ ❌ SOME TESTS FAILED")
        
        logger.error("="*60)
        
        # Инициализация Pymorphy3
        try:
            import pymorphy3
            self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
            self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk')
            self._has_morph = True
            logger.info("✅ Pymorphy3 initialized for v8.1")
        except ImportError:
            logger.error("❌ Pymorphy3 not found!")
            self._has_morph = False
    
    def _get_city_abbreviations(self) -> Dict[str, str]:
        """Популярные сокращения городов"""
        return {
            # РФ
            'екб': 'ru', 'екат': 'ru',  # Екатеринбург
            'спб': 'ru', 'питер': 'ru',  # Санкт-Петербург
            'мск': 'ru',  # Москва
            'нск': 'ru',  # Новосибирск
            'нн': 'ru', 'ннов': 'ru',  # Нижний Новгород
            'влад': 'ru',  # Владивосток
            'ростов': 'ru',  # Ростов-на-Дону
            'краснодар': 'ru',
            
            # BY
            'мн': 'by',  # Минск
            
            # KZ
            'алматы': 'kz',
            'астана': 'kz',
            
            # UZ
            'ташкент': 'uz',
        }
    
    def _get_regions(self) -> Dict[str, str]:
        """Регионы РФ, BY, KZ, UZ"""
        return {
            # РФ регионы/республики
            'ингушетия': 'ru',
            'чечня': 'ru', 'чеченская республика': 'ru',
            'дагестан': 'ru',
            'татарстан': 'ru',
            'башкортостан': 'ru',
            'удмуртия': 'ru',
            'мордовия': 'ru',
            'марий эл': 'ru',
            'чувашия': 'ru',
            'якутия': 'ru', 'саха': 'ru',
            'бурятия': 'ru',
            'тыва': 'ru',
            'хакасия': 'ru',
            'алтай': 'ru',
            'карелия': 'ru',
            'коми': 'ru',
            'калмыкия': 'ru',
            'адыгея': 'ru',
            'кабардино-балкария': 'ru',
            'карачаево-черкесия': 'ru',
            'северная осетия': 'ru',
            'крым': 'ru',
            
            # BY области
            'брестская область': 'by',
            'витебская область': 'by',
            'гомельская область': 'by',
            'гродненская область': 'by',
            'минская область': 'by',
            'могилёвская область': 'by',
            
            # KZ области
            'акмолинская область': 'kz',
            'актюбинская область': 'kz',
            'алматинская область': 'kz',
            'восточно-казахстанская область': 'kz',
        }
    
    def _get_countries(self) -> Dict[str, str]:
        """Страны мира"""
        return {
            # СНГ
            'россия': 'ru', 'russia': 'ru', 'рф': 'ru',
            'беларусь': 'by', 'belarus': 'by', 'белоруссия': 'by',
            'казахстан': 'kz', 'kazakhstan': 'kz',
            'узбекистан': 'uz', 'uzbekistan': 'uz',
            
            # Европа
            'польша': 'pl', 'poland': 'pl',
            'литва': 'lt', 'lithuania': 'lt',
            'латвия': 'lv', 'latvia': 'lv',
            'эстония': 'ee', 'estonia': 'ee',
            
            # Другие
            'израиль': 'il', 'israel': 'il',
            'турция': 'tr', 'turkey': 'tr',
        }
    
    def _get_manual_small_cities(self) -> Dict[str, str]:
        """Малые города СНГ"""
        return {
            # BY малые города
            'фаниполь': 'by', 'фанипаль': 'by', 'fanipol': 'by',
            'ошмяны': 'by', 'ashmyany': 'by',
            'узда': 'by', 'uzda': 'by',
            
            # KZ малые города
            'узынагаш': 'kz', 'uzynagash': 'kz',
            'ош': 'kg',  # Кыргызстан
        }
    
    def filter_batch(self, keywords: List[str], seed: str, country: str, language: str) -> Dict:
        """
        v8.1: Batch filtering with FIXED seed protection
        
        🔥 КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ v8.1:
        Seed protection НЕ применяется к городам из других стран!
        """
        start_time = time.time()
        
        stats = {
            'total': len(keywords),
            'allowed': 0,
            'blocked': 0,
            'reasons': Counter()
        }
        
        final_keywords = []
        final_anchors = []
        
        # Extract words from keywords for batch lemmatization
        all_words = set()
        for kw in keywords:
            words = re.findall(r'[а-яёa-z0-9-]+', kw.lower())
            all_words.update(words)
        
        # Batch lemmatization
        lemmas_map = self._batch_lemmatize(all_words, language)
        
        # Extract seed cities ONLY from target country
        seed_cities = self._extract_cities_from_seed(seed, country, language)
        
        logger.info(f"[v8.1] Extracted {len(seed_cities)} seed cities from target country: {seed_cities}")
        
        # Process each keyword
        for keyword in keywords:
            is_ok, reason, reason_tag = self._check_geo_conflicts_v81(
                keyword, country, lemmas_map, seed_cities, language
            )
            
            if is_ok:
                final_keywords.append(keyword)
                stats['allowed'] += 1
            else:
                final_anchors.append(keyword)
                stats['blocked'] += 1
                stats['reasons'][reason_tag] += 1
                
                # Debug logging для критичных городов
                if any(city in keyword.lower() for city in ['ждановичи', 'лошица', 'барановичи']):
                    logger.warning(f"[v8.1] ⚓ BLOCKED (EXPECTED): '{keyword}' → {reason}")
        
        elapsed = time.time() - start_time
        logger.info(f"[v8.1] Finished in {elapsed:.2f}s. {stats['allowed']} OK / {stats['blocked']} Blocked")
        
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

    def _check_geo_conflicts_v81(self, keyword: str, country: str, 
                                  lemmas_map: Dict[str, str], seed_cities: Set[str],
                                  language: str) -> Tuple[bool, str, str]:
        """
        v8.1: КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ - Seed protection НЕ работает для городов
        
        СТАРАЯ ЛОГИКА v8.0 (НЕПРАВИЛЬНАЯ):
          if слово_в_seed_cities → auto-allow
          
        НОВАЯ ЛОГИКА v8.1 (ПРАВИЛЬНАЯ):
          if слово_в_базе_городов AND страна != target:
             БЛОКИРУЕМ независимо от seed!
          
          Seed protection остаётся только для районов СВОЕЙ страны
        """
        words = re.findall(r'[а-яёa-z0-9-]+', keyword.lower())
        if not words:
            return True, "", ""

        keyword_lemmas = [lemmas_map.get(w, w) for w in words]
        
        # --- 1. HARD-BLACKLIST (приоритет #1) ---
        for check_val in words + keyword_lemmas:
            if check_val in self.forbidden_geo:
                return False, f"Hard-Blacklist '{check_val}'", "hard_blacklist"

        # --- 2. РАЙОНЫ (с seed protection) ---
        # Seed protection работает ТОЛЬКО для районов своей страны
        words_set = set(words + keyword_lemmas)
        for w in words:
            if w in self.districts:
                dist_country = self.districts[w]
                
                # Если это район НАШЕЙ страны И он есть в seed → разрешаем
                if dist_country == country.lower() and w in seed_cities:
                    logger.debug(f"[v8.1] District '{w}' in seed_cities → ALLOWED")
                    continue
                
                # Если это район ЧУЖОЙ страны → блокируем
                if dist_country != country.lower():
                    return False, f"район '{w}' ({dist_country})", "districts"
        
        # --- 3. СОКРАЩЕНИЯ ГОРОДОВ ---
        for w in words + keyword_lemmas:
            if w in self.city_abbreviations:
                abbr_country = self.city_abbreviations[w]
                if abbr_country != country.lower():
                    return False, f"сокращение города '{w}' ({abbr_country})", f"{abbr_country}_abbreviations"
        
        # --- 4. РЕГИОНЫ ---
        check_regions = words + keyword_lemmas + self._extract_ngrams(words, 2)
        for item in check_regions:
            if item in self.regions:
                region_country = self.regions[item]
                if region_country != country.lower():
                    return False, f"регион '{item}' ({region_country})", f"{region_country}_regions"
        
        # --- 5. СТРАНЫ ---
        for w in words + keyword_lemmas:
            if w in self.countries:
                ctry_code = self.countries[w]
                if ctry_code != country.lower():
                    return False, f"страна '{w}' ({ctry_code})", f"{ctry_code}_countries"
        
        # --- 6. МАЛЫЕ ГОРОДА СНГ ---
        for w in words + keyword_lemmas:
            if w in self.manual_small_cities:
                city_country = self.manual_small_cities[w]
                if city_country == 'unknown':
                    return False, f"неизвестный объект '{w}'", "unknown"
                if city_country != country.lower():
                    return False, f"малый город '{w}' ({city_country})", f"{city_country}_small_cities"

        # --- 7. ГОРОДА (ГЛАВНАЯ ПРОВЕРКА) ---
        # 🔥 v8.1 КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: 
        # Seed protection НЕ РАБОТАЕТ для городов из других стран!
        
        search_items = []
        search_items.extend(words)
        search_items.extend(keyword_lemmas)
        
        # Биграммы
        bigrams = self._extract_ngrams(words, 2)
        search_items.extend(bigrams)
        search_items.extend([bg.replace(' ', '-') for bg in bigrams])
        
        lemma_bigrams = self._extract_ngrams(keyword_lemmas, 2)
        search_items.extend(lemma_bigrams)
        search_items.extend([bg.replace(' ', '-') for bg in lemma_bigrams])
        
        # Триграммы
        trigrams = self._extract_ngrams(words, 3)
        search_items.extend(trigrams)
        search_items.extend([tg.replace(' ', '-') for tg in trigrams])

        for item in search_items:
            if len(item) < 3:
                continue
            
            # Пропускаем ignored_words
            if item in self.ignored_words:
                logger.debug(f"[v8.1] '{item}' in ignored_words, skipping")
                continue
            
            # Debug для проблемных городов
            debug_cities = ['ждановичи', 'zhdanovichi', 'жданович', 'лошица', 'losica', 'барановичи']
            is_debug_city = any(dc in item.lower() for dc in debug_cities)
            
            if is_debug_city:
                logger.warning(f"🔍 [v8.1 DEBUG] Processing '{item}'")
            
            # Нормализуем слово (склонения → базовая форма)
            item_normalized = self._get_lemma(item, language)
            
            if is_debug_city:
                logger.warning(f"🔍 [v8.1 DEBUG] Normalized: '{item}' → '{item_normalized}'")
            
            # Ищем в базе
            found_country = self.all_cities_global.get(item_normalized)
            
            if is_debug_city:
                logger.warning(f"🔍 [v8.1 DEBUG] Lookup (normalized): '{item_normalized}' → {found_country}")
            
            if not found_country:
                # Пробуем оригинал
                found_country = self.all_cities_global.get(item)
                
                if is_debug_city:
                    logger.warning(f"🔍 [v8.1 DEBUG] Lookup (original): '{item}' → {found_country}")
                
                if found_country:
                    item_normalized = item
            
            # ========== КРИТИЧЕСКАЯ ЛОГИКА v8.1 ==========
            if found_country:
                if is_debug_city:
                    logger.warning(f"🔍 [v8.1 DEBUG] FOUND: '{item_normalized}' = {found_country.upper()}")
                    logger.warning(f"🔍 [v8.1 DEBUG] Target: {country.lower()}")
                
                # Проверка 1: Это наш целевой город?
                if found_country == country.lower():
                    logger.debug(f"[v8.1] City '{item_normalized}' ({found_country}) - ALLOWED (target country)")
                    continue
                
                # 🔥 КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ v8.1:
                # Проверка 2: Seed protection УДАЛЕНА для городов!
                # 
                # СТАРЫЙ КОД v8.0 (НЕПРАВИЛЬНО):
                # if item_normalized in seed_cities:
                #     logger.debug(f"City '{item_normalized}' in seed_cities - ALLOWED")
                #     continue
                #
                # НОВЫЙ КОД v8.1 (ПРАВИЛЬНО):
                # Seed protection НЕ работает для городов из других стран
                # Блокируем независимо от наличия в seed!
                
                # Проверка 3: Город из другой страны → БЛОКИРУЕМ ВСЕГДА
                if is_debug_city:
                    logger.warning(f"🔍 [v8.1 DEBUG] ⚓ BLOCKING: '{item_normalized}' ({found_country.upper()} != {country.upper()})")
                
                logger.warning(f"[v8.1] ⚓ BLOCKING foreign city: '{item}' → '{item_normalized}' ({found_country.upper()})")
                return False, f"{found_country.upper()} город '{item_normalized}'", f"{found_country}_cities"
            
            # ========== МОРФОЛОГИЯ = ВТОРИЧНА (только для слов ВНЕ базы) ==========
            else:
                # Слово не в базе городов - возможно это обычное слово?
                if self._is_common_noun(item_normalized, language):
                    logger.debug(f"[v8.1] '{item_normalized}' NOT in geo database + common noun - ALLOWED")
                    continue
        
        # --- 8. ГРАММАТИКА ---
        if not self._is_grammatically_valid(keyword, language):
            return False, "неправильная грамматическая форма", "grammar"
        
        return True, "", ""

    def _extract_cities_from_seed(self, seed: str, country: str, language: str) -> Set[str]:
        """Извлекает города из seed (ТОЛЬКО из целевой страны)"""
        if not self._has_morph:
            return set()
        
        seed_cities = set()
        words = re.findall(r'[а-яёa-z0-9-]+', seed.lower())
        
        for word in words:
            if word in self.all_cities_global:
                city_country = self.all_cities_global[word]
                if city_country == country.lower():
                    seed_cities.add(word)
            
            lemma = self._get_lemma(word, language)
            if lemma in self.all_cities_global:
                city_country = self.all_cities_global[lemma]
                if city_country == country.lower():
                    seed_cities.add(lemma)
        
        bigrams = self._extract_ngrams(words, 2)
        for bigram in bigrams:
            if bigram in self.all_cities_global:
                city_country = self.all_cities_global[bigram]
                if city_country == country.lower():
                    seed_cities.add(bigram)
        
        return seed_cities

    def _batch_lemmatize(self, words: Set[str], language: str) -> Dict[str, str]:
        """Batch лемматизация"""
        if not self._has_morph:
            return {w: w for w in words}
        
        morph = self.morph_ru if language == 'ru' else self.morph_uk
        lemmas = {}
        
        for word in words:
            lemma = self._get_lemma(word, language, morph)
            lemmas[word] = lemma
        
        return lemmas

    def _get_lemma(self, word: str, language: str, morph=None) -> str:
        """Получает лемму слова"""
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
        """Извлекает n-граммы"""
        if len(words) < n:
            return []
        return [" ".join(words[i:i+n]) for i in range(len(words) - n + 1)]

    def _is_grammatically_valid(self, keyword: str, language: str) -> bool:
        """Проверяет грамматическую правильность"""
        if not self._has_morph or language not in ['ru', 'uk']:
            return True
        
        morph = self.morph_ru if language == 'ru' else self.morph_uk
        words = re.findall(r'[а-яёa-z]+', keyword.lower())
        
        for word in words:
            try:
                parsed = morph.parse(word)
                if parsed:
                    tag = parsed[0].tag
                    invalid_tags = {'datv', 'ablt', 'loct'}
                    if 'plur' in tag and any(bad in tag for bad in invalid_tags):
                        return False
            except:
                pass
        
        return True

    def _is_common_noun(self, word: str, language: str) -> bool:
        """Smart disambiguation с приоритетом Geox"""
        if not self._has_morph or language not in ['ru', 'uk']:
            return False
        
        morph = self.morph_ru if language == 'ru' else self.morph_uk
        
        try:
            parsed = morph.parse(word)
            if parsed:
                for parse_variant in parsed:
                    tag = parse_variant.tag
                    
                    if 'Geox' in tag:
                        logger.debug(f"[v8.1] '{word}' is Geox, NOT common noun")
                        return False
                    
                    if 'Name' in tag:
                        return False
                
                first_tag = parsed[0].tag
                if 'NOUN' in first_tag and word.islower():
                    logger.debug(f"[v8.1] '{word}' is common noun")
                    return True
        except:
            pass
        
        return False


# ============================================
# DISTRICTS
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
