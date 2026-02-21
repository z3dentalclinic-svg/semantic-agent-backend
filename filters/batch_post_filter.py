"""
Batch Post-Filter v7.9 - FUNDAMENTAL FIX: GEO DATABASE PRIORITY
Based on Gemini's recommendations for 187 countries support
"""

import re
import logging
import time
from typing import List, Dict, Set, Tuple, Optional
from collections import Counter

logger = logging.getLogger("BatchPostFilter")


class BatchPostFilter:
    def __init__(self, 
                 all_cities_global: Dict[str, str], 
                 forbidden_geo: Set[str], 
                 districts: Optional[Dict[str, str]] = None,
                 population_threshold: int = 5000):
        self.forbidden_geo = forbidden_geo
        self.districts = districts or {}
        self.population_threshold = population_threshold
        
        self.city_abbreviations = self._get_city_abbreviations()
        self.regions = self._get_regions()
        self.countries = self._get_countries()
        self.manual_small_cities = self._get_manual_small_cities()
        
        self.ignored_words = {
            "дом", "мир", "бор", "нива", "балка", "луч", "спутник", "работа", "цена", "выезд",
        }
        
        # Список крупных городов/столиц которые ВСЕГДА блокируются (не бренды)
        self.forbidden_major_cities = {
            # Россия
            "москва", "moscow", "санкт-петербург", "petersburg", "питер", "spb",
            "новосибирск", "екатеринбург", "казань", "нижний новгород",
            "челябинск", "самара", "омск", "ростов", "уфа", "красноярск",
            # Беларусь (если таргет не BY)
            "минск", "minsk", "гомель", "могилев", "витебск", "гродно", "брест",
            # Казахстан (если таргет не KZ)
            "алматы", "almaty", "астана", "nur-sultan", "шымкент",
            # Другие страны
            "киев", "kiev", "харьков", "одесса", "днепр", "львов", "lviv", # UA
            "варшава", "warsaw", "краков", "krakow",  # PL
            "берлин", "berlin", "мюнхен", "munich",  # DE
            "париж", "paris", "лондон", "london",  # FR, GB
            "рим", "rome", "милан", "milan",  # IT
            "мадрид", "madrid", "барселона", "barcelona",  # ES
        }

        
        base_index = {k.lower().strip(): v for k, v in (all_cities_global or {}).items()}
        geo_index = self._build_filtered_geo_index()
        
        for k, v in geo_index.items():
            if k not in base_index:
                base_index[k] = v
        
        self.all_cities_global = base_index
        
        forced_by_cities = {
            "барановичи": "by",
            "baranovichi": "by",
            "ждановичи": "by",
            "zhdanovichi": "by",
            "лошица": "by",
        }
        
        for name, code in forced_by_cities.items():
            if name not in self.all_cities_global:
                self.all_cities_global[name] = code
        
        # Украинские города (КРИТИЧНО: на случай если geonamescache не загружен)
        forced_ua_cities = {
            "львов": "ua",
            "львів": "ua", 
            "lviv": "ua",
            "lvov": "ua",
            "lemberg": "ua",
            "киев": "ua",
            "київ": "ua",
            "kyiv": "ua",
            "kiev": "ua",
            "харьков": "ua",
            "харків": "ua",
            "kharkiv": "ua",
            "одесса": "ua",
            "одеса": "ua",
            "odessa": "ua",
            "днепр": "ua",
            "дніпро": "ua",
            "dnipro": "ua",
            "запорожье": "ua",
            "запоріжжя": "ua",
            "zaporizhzhia": "ua",
        }
        
        for name, code in forced_ua_cities.items():
            self.all_cities_global[name] = code
        
        try:
            import pymorphy3
            self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
            self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk')
            self._has_morph = True
        except ImportError:
            self._has_morph = False
    
    def _get_city_abbreviations(self) -> Dict[str, str]:
        return {
            'екб': 'ru', 'екат': 'ru', 'спб': 'ru', 'питер': 'ru', 'мск': 'ru',
            'нск': 'ru', 'нн': 'ru', 'ннов': 'ru', 'влад': 'ru', 'ростов': 'ru',
            'краснодар': 'ru', 'мн': 'by', 'алматы': 'kz', 'астана': 'kz', 'ташкент': 'uz',
        }
    
    def _get_regions(self) -> Dict[str, str]:
        return {
            'ингушетия': 'ru', 'чечня': 'ru', 'чеченская республика': 'ru',
            'дагестан': 'ru', 'татарстан': 'ru', 'башкортостан': 'ru',
            'удмуртия': 'ru', 'мордовия': 'ru', 'марий эл': 'ru',
            'чувашия': 'ru', 'якутия': 'ru', 'саха': 'ru', 'бурятия': 'ru',
            'тыва': 'ru', 'хакасия': 'ru', 'алтай': 'ru', 'карелия': 'ru',
            'коми': 'ru', 'калмыкия': 'ru', 'адыгея': 'ru', 'кабардино-балкария': 'ru',
            'карачаево-черкесия': 'ru', 'северная осетия': 'ru', 'крым': 'ru',
            'московская область': 'ru', 'ленинградская область': 'ru',
            'новосибирская область': 'ru', 'свердловская область': 'ru',
            'минская область': 'by', 'гомельская область': 'by',
            'могилевская область': 'by', 'витебская область': 'by',
            'гродненская область': 'by', 'брестская область': 'by',
            'алматинская область': 'kz', 'южно-казахстанская область': 'kz',
            'ташкентская область': 'uz', 'самаркандская область': 'uz',
        }
    
    def _get_countries(self) -> Dict[str, str]:
        return {
            'россия': 'ru', 'россии': 'ru', 'рф': 'ru',
            'беларусь': 'by', 'белоруссия': 'by',
            'казахстан': 'kz', 'казахстане': 'kz',
            'узбекистан': 'uz', 'узбекистане': 'uz',
            'украина': 'ua', 'украине': 'ua',
            'израиль': 'il', 'израиле': 'il',
            'польша': 'pl', 'польше': 'pl',
            'германия': 'de', 'германии': 'de',
            'сша': 'us', 'америка': 'us', 'америке': 'us',
        }
    
    def _get_manual_small_cities(self) -> Dict[str, str]:
        return {
            'ош': 'kg',
            'узынагаш': 'kz',
            'щелкино': 'ru',
            'щёлкino': 'ru',
            'йота': 'unknown',
        }
    
    def _build_filtered_geo_index(self) -> Dict[str, str]:
        try:
            import geonamescache
            gc = geonamescache.GeonamesCache()
            
            # КРИТИЧНО: Устанавливаем порог 5000 для загрузки cities5000.json (65k городов)
            # По умолчанию загружается cities15000.json (32k городов)
            gc.min_city_population = self.population_threshold  # 5000
            
            cities = gc.get_cities()
            
            filtered_index = {}
            
            for city_id, city_data in cities.items():
                country = city_data['countrycode'].lower()
                population = city_data.get('population', 0)
                
                if population < self.population_threshold:
                    continue
                
                name = city_data['name'].lower().strip()
                filtered_index[name] = country
                
                for alt in city_data.get('alternatenames', []):
                    alt = alt.strip()
                    if not (3 <= len(alt) <= 50):
                        continue
                    if not any(c.isalpha() for c in alt):
                        continue
                    
                    is_latin_cyrillic = all(
                        ('\u0000' <= c <= '\u007F') or
                        ('\u0400' <= c <= '\u04FF') or
                        c in ['-', "'", ' ']
                        for c in alt
                    )
                    if not is_latin_cyrillic:
                        continue
                    
                    alt_lower = alt.lower()
                    
                    has_cyr = any('\u0400' <= c <= '\u04FF' for c in alt_lower)
                    has_lat = any('a' <= c <= 'z' for c in alt_lower)
                    
                    if has_cyr and not has_lat:
                        if alt_lower not in filtered_index:
                            filtered_index[alt_lower] = country
                    
                    if alt_lower not in filtered_index:
                        filtered_index[alt_lower] = country
                    
                    alt_dash = alt_lower.replace(' ', '-')
                    if alt_dash != alt_lower and alt_dash not in filtered_index:
                        filtered_index[alt_dash] = country
            
            return filtered_index
            
        except ImportError:
            return {
                'москва': 'ru', 'санкт-петербург': 'ru', 
                'киев': 'ua', 'харьков': 'ua', 'одесса': 'ua',
                'минск': 'by', 'алматы': 'kz', 'ташкент': 'uz'
            }

    def _find_in_country(self, word: str, target_country: str) -> bool:
        """
        PRIORITY 1: Проверка - является ли слово городом ЦЕЛЕВОЙ страны
        
        Возвращает True, если слово найдено в базе именно как город target_country
        """
        word_lower = word.lower()
        
        # Прямой поиск в базе
        found_country = self.all_cities_global.get(word_lower)
        if found_country and found_country == target_country.lower():
            return True
        
        # Поиск с лемматизацией (на случай падежей)
        if self._has_morph:
            lemma_ru = self._get_lemma(word_lower, 'ru')
            lemma_uk = self._get_lemma(word_lower, 'uk')
            
            for lemma in [lemma_ru, lemma_uk]:
                if lemma != word_lower:
                    found_country = self.all_cities_global.get(lemma)
                    if found_country and found_country == target_country.lower():
                        return True
        
        return False
    
    def _is_real_city_not_brand(self, word: str, found_country: str) -> bool:
        """
        Проверяет, является ли слово РЕАЛЬНЫМ городом (а не брендом)
        
        УНИВЕРСАЛЬНАЯ ЛОГИКА без хардкод списков:
        - Кириллица → ГОРОД
        - Латиница → возможный БРЕНД
        - Известные бренды → БРЕНД
        """
        word_lower = word.lower()
        
        # Известные бренды НЕ считаются реальными городами
        known_brands = {
            "редмонд", "redmond", "горенье", "gorenje", "бош", "bosch",
            "самсунг", "samsung", "филипс", "philips", "браун", "braun",
            "панасоник", "panasonic", "сименс", "siemens", "миле", "miele",
            "электролюкс", "electrolux", "аег", "aeg", "занусси", "zanussi",
            "индезит", "indesit", "аристон", "ariston", "канди", "candy",
            "беко", "beko", "хотпоинт", "hotpoint", "вирпул", "whirlpool",
            "дайсон", "dyson", "керхер", "karcher", "витек", "vitek",
            "поларис", "polaris", "скарлет", "scarlett", "тефаль", "tefal",
            "мулинекс", "moulinex", "крупс", "krups", "делонги", "delonghi",
            "филко", "philco", "томас", "thomas", "зелмер", "zelmer",
        }
        
        if word_lower in known_brands:
            return False
        
        # Латинские слова скорее бренды, чем города
        if word_lower.isascii() and word_lower.isalpha():
            # Короткие латинские слова (≤4) - точно бренды
            if len(word_lower) <= 4:
                return False
        
        # Очень короткие слова (1-2 буквы) - скорее аббревиатуры/бренды
        if len(word_lower) <= 2:
            return False
        
        # ═══════════════════════════════════════════════════════════
        # УНИВЕРСАЛЬНАЯ ЛОГИКА для кириллицы
        # Если слово на кириллице - это скорее всего ГОРОД
        # ═══════════════════════════════════════════════════════════
        
        # Проверяем - это кириллица?
        if not word_lower.isascii():
            # Кириллица 3+ букв - это ГОРОД
            # Примеры: уфа(3), омск(4), рига(4), тула(4), ейск(4), курск(5)
            if len(word_lower) >= 3:
                return True
        
        # Латинские длинные слова (5+) могут быть городами
        # Примеры: Paris, London, Berlin
        if len(word_lower) >= 5:
            return True
        
        # В остальных случаях - возможный бренд
        return False

    def _is_brand_like(self, word: str) -> bool:
        """Определяет, может ли слово быть брендом (спорное слово)"""
        word_lower = word.lower()
        
        # Слова в ignored_words считаются не-городами
        if word_lower in self.ignored_words:
            return True
        
        # Известные бренды техники (кириллица и латиница)
        known_brands = {
            # Бренды техники (кириллица)
            "редмонд", "redmond", "горенье", "gorenje", "бош", "bosch",
            "самсунг", "samsung", "филипс", "philips", "браун", "braun",
            "панасоник", "panasonic", "сименс", "siemens", "миле", "miele",
            "электролюкс", "electrolux", "аег", "aeg", "занусси", "zanussi",
            "индезит", "indesit", "аристон", "ariston", "канди", "candy",
            "беко", "beko", "хотпоинт", "hotpoint", "вирпул", "whirlpool",
            "дайсон", "dyson", "керхер", "karcher", "витек", "vitek",
            "поларис", "polaris", "скарлет", "scarlett", "тефаль", "tefal",
            "мулинекс", "moulinex", "крупс", "krups", "делонги", "delonghi",
            "филко", "philco", "томас", "thomas", "зелмер", "zelmer",
            # Добавить другие по необходимости
        }
        
        if word_lower in known_brands:
            return True
        
        # Латинские слова скорее бренды чем города
        if word.isascii() and word.isalpha():
            return True
        
        # Короткие слова (3-4 буквы) могут быть брендами
        if len(word) <= 4:
            return True
        
        return False

    def _has_seed_cores(self, keyword: str, seed: str) -> bool:
        """Проверяет наличие корней из сида в ключе (первые 5 букв)"""
        seed_roots = [w.lower()[:5] for w in re.findall(r'[а-яёa-z]+', seed) if len(w) > 3]
        keyword_lower = keyword.lower()
        return any(root in keyword_lower for root in seed_roots)

    def filter_batch(self, keywords: List[str], seed: str, country: str, 
                     language: str = 'ru') -> Dict:
        start_time = time.time()
        
        logger.info(f"[BPF] START filter_batch | country={country} | lang={language}")
        logger.info(f"[BPF] RAW keywords ({len(keywords)}): {keywords}")
        
        unique_raw = sorted(list(set([k.lower().strip() for k in keywords if k.strip()])))
        logger.info(f"[BPF] UNIQUE_RAW ({len(unique_raw)}): {unique_raw}")
        
        seed_cities = self._extract_cities_from_seed(seed, country, language)
        logger.info(f"[BPF] SEED='{seed}' | seed_cities={seed_cities}")
        
        all_words = set()
        for kw in unique_raw:
            all_words.update(re.findall(r'[а-яёa-z0-9-]+', kw))
        
        lemmas_map = self._batch_lemmatize(all_words, language)
        
        final_keywords = []
        final_anchors = []
        stats = {
            'total': len(unique_raw),
            'allowed': 0,
            'blocked': 0,
            'reasons': Counter()
        }

        for kw in unique_raw:
            is_allowed, reason, category = self._check_geo_conflicts_v75(
                kw, country, lemmas_map, seed_cities, language, seed
            )
            
            if is_allowed:
                final_keywords.append(kw)
                stats['allowed'] += 1
            else:
                final_anchors.append(kw)
                stats['blocked'] += 1
                stats['reasons'][category] += 1

        elapsed = time.time() - start_time
        logger.info(f"[BPF] FINISH {elapsed:.2f}s | "
                    f"allowed={len(final_keywords)} | anchors={len(final_anchors)} | "
                    f"reasons={dict(stats['reasons'])}")

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

    def _check_geo_conflicts_v75(self, keyword: str, country: str, 
                                  lemmas_map: Dict[str, str], seed_cities: Set[str],
                                  language: str, seed: str = "") -> Tuple[bool, str, str]:
        # Проверяем наличие корней сида в ключе (ПРИОРИТЕТ СИДА)
        has_seed = self._has_seed_cores(keyword, seed) if seed else False
        
        logger.debug(f"[BPF] CHECK keyword='{keyword}' | country={country} | "
                     f"has_seed={has_seed} | seed_cities={seed_cities}")
        
        words = re.findall(r'[а-яёa-z0-9-]+', keyword)
        if not words:
            return True, "", ""

        keyword_lemmas = [lemmas_map.get(w, w) for w in words]
        
        words_set = set(words + keyword_lemmas)
        if any(city in words_set for city in seed_cities):
            logger.debug(f"[BPF] ALLOW by seed_cities | keyword='{keyword}'")
            return True, "", ""
        
        for check_val in words + keyword_lemmas:
            if check_val in self.forbidden_geo:
                return False, f"Hard-Blacklist '{check_val}'", "hard_blacklist"

        for w in words:
            if w in self.districts:
                dist_country = self.districts[w]
                if dist_country != country.lower():
                    # Проверяем: есть ли в запросе город ЦЕЛЕВОЙ страны?
                    # "харьков алексеевка" → "харьков" = UA → не блокируем "алексеевка" (RU)
                    has_target_city = any(
                        self.all_cities_global.get(other_w) == country.lower()
                        for other_w in set(words + keyword_lemmas) - {w}
                    )
                    if has_target_city:
                        logger.debug(f"[BPF] ALLOW district '{w}' ({dist_country}) — "
                                     f"keyword has target city ({country})")
                        continue
                    return False, f"район '{w}' ({dist_country})", "districts"
        
        for w in words + keyword_lemmas:
            if w in self.city_abbreviations:
                abbr_country = self.city_abbreviations[w]
                if abbr_country != country.lower():
                    return False, f"сокращение города '{w}' ({abbr_country})", f"{abbr_country}_abbreviations"
        
        check_regions = words + keyword_lemmas + self._extract_ngrams(words, 2)
        for item in check_regions:
            if item in self.regions:
                region_country = self.regions[item]
                if region_country != country.lower():
                    return False, f"регион '{item}' ({region_country})", f"{region_country}_regions"
        
        for w in words + keyword_lemmas:
            if w in self.countries:
                ctry_code = self.countries[w]
                if ctry_code != country.lower():
                    return False, f"страна '{w}' ({ctry_code})", f"{ctry_code}_countries"
        
        for w in words + keyword_lemmas:
            if w in self.manual_small_cities:
                city_country = self.manual_small_cities[w]
                if city_country == 'unknown':
                    return False, f"неизвестный объект '{w}'", "unknown"
                if city_country != country.lower():
                    return False, f"малый город '{w}' ({city_country})", f"{city_country}_small_cities"

        search_items = []
        search_items.extend(words)
        search_items.extend(keyword_lemmas)
        
        bigrams = self._extract_ngrams(words, 2)
        search_items.extend(bigrams)
        search_items.extend([bg.replace(' ', '-') for bg in bigrams])
        
        lemma_bigrams = self._extract_ngrams(keyword_lemmas, 2)
        search_items.extend(lemma_bigrams)
        search_items.extend([bg.replace(' ', '-') for bg in lemma_bigrams])
        
        trigrams = self._extract_ngrams(words, 3)
        search_items.extend(trigrams)
        search_items.extend([tg.replace(' ', '-') for tg in trigrams])

        # FIX: Собираем леммы слов которые являются городами НАШЕЙ страны
        # "львов" → UA → лемма "лев" → не блокировать "лев" как город BF
        our_city_lemmas = set()
        for w in words:
            if self._find_in_country(w, country):
                lemma = self._get_lemma(w, language)
                if lemma != w:
                    our_city_lemmas.add(lemma)
                    logger.debug(f"[BPF] our_city_lemma: '{w}' → '{lemma}'")

        for item in search_items:
            # ШАГ 0: Пропускаем короткие слова и ignored_words
            if len(item) < 3 or item in self.ignored_words:
                if item in self.ignored_words:
                    logger.info(f"[GEO_SKIP] Слово '{item}' в ignored_words")
                continue
            
            item_normalized = self._get_lemma(item, language)
            
            # ═══════════════════════════════════════════════════════════
            # ЖЕСТКАЯ ИЕРАРХИЯ ПРИОРИТЕТОВ (v11)
            # ═══════════════════════════════════════════════════════════
            
            # ┌─────────────────────────────────────────────────────────┐
            # │ PRIORITY 1: СВОЙ ГОРОД (целевая страна)                 │
            # │ Проверяем ПЕРВЫМ делом - это наш город?                 │
            # └─────────────────────────────────────────────────────────┘
            
            is_our_city = self._find_in_country(item, country)
            if not is_our_city:
                # Проверяем также нормализованную форму
                is_our_city = self._find_in_country(item_normalized, country)
            
            if is_our_city:
                logger.info(f"[GEO_ALLOW] ✓ PRIORITY 1: Город '{item}' найден в целевой стране {country.upper()}")
                continue  # Львов спасен!
            
            # ┌─────────────────────────────────────────────────────────┐
            # │ PRIORITY 2: ЧУЖОЙ ГОРОД (другая страна)                 │
            # │ Если город найден в другой стране - блокируем           │
            # └─────────────────────────────────────────────────────────┘
            
            found_country = self.all_cities_global.get(item_normalized) or self.all_cities_global.get(item)
            
            if found_country:
                logger.info(f"[GEO_DEBUG] Слово '{item}' опознано как город страны: {found_country.upper()}")
                
                # Это город ДРУГОЙ страны (мы уже проверили нашу выше)
                if found_country != country.lower():
                    
                    # FIX: Это лемма нашего города? "лев" ← "львов" (UA)
                    if item in our_city_lemmas or item_normalized in our_city_lemmas:
                        logger.info(f"[GEO_ALLOW] ✓ Слово '{item}' — лемма города нашей страны, пропускаем")
                        continue
                    
                    # FIX: Это обычное слово языка? "дом", "белая", "гора"
                    if self._is_common_noun(item_normalized, language) or self._is_common_noun(item, language):
                        logger.info(f"[GEO_ALLOW] ✓ Слово '{item}' — обычное существительное, не город")
                        continue
                    
                    # Проверка seed_cities (города из сида всегда разрешены)
                    if item_normalized in seed_cities or item in seed_cities:
                        logger.info(f"[GEO_ALLOW] Город '{item}' разрешен (есть в сиде)")
                        continue
                    
                    # Проверяем - это РЕАЛЬНЫЙ город или возможный бренд?
                    is_real_city = self._is_real_city_not_brand(item, found_country)
                    
                    if is_real_city:
                        # Это явно РЕАЛЬНЫЙ город (Рига, Ейск, Ишим)
                        # БЛОКИРУЕМ даже если есть seed!
                        reason = f"Слово '{item}' — это город в {found_country.upper()}, а мы парсим {country.upper()}"
                        logger.warning(f"!!! [GEO_ANCHOR] ✗ PRIORITY 2: Ключ отправлен в якоря: '{keyword}' | Причина: {reason} (реальный чужой город)")
                        return False, reason, f"{found_country}_cities"
                    
                    # ┌─────────────────────────────────────────────────────┐
                    # │ PRIORITY 3: СПОРНОЕ СЛОВО (возможный бренд)         │
                    # │ Короткое слово или латиница - может быть брендом   │
                    # └─────────────────────────────────────────────────────┘
                    
                    if has_seed:
                        # Слово похоже на бренд И есть seed - разрешаем
                        logger.info(f"[GEO_ALLOW] ✓ PRIORITY 3: Город '{item}' ({found_country.upper()}) разрешен (спорное слово + есть seed)")
                        continue
                    else:
                        # Нет seed - блокируем даже спорные слова
                        reason = f"Слово '{item}' — это город в {found_country.upper()}, а мы парсим {country.upper()}"
                        logger.warning(f"!!! [GEO_ANCHOR] Ключ отправлен в якоря: '{keyword}' | Причина: {reason}")
                        return False, reason, f"{found_country}_cities"
            
            # Если город не найден нигде - проверяем на обычное существительное
            if self._is_common_noun(item_normalized, language):
                continue
        
        if not self._is_grammatically_valid(keyword, language):
            return False, "неправильная грамматическая форма", "grammar"
        
        return True, "", ""

    def _is_common_noun(self, word: str, language: str) -> bool:
        """
        Проверяет, является ли слово обычным словом языка (не гео-названием).
        Проверяет ПЕРВЫЙ (самый вероятный) вариант парсинга.
        """
        if not self._has_morph or language not in ['ru', 'uk']:
            return False
        
        morph = self.morph_ru if language == 'ru' else self.morph_uk
        
        try:
            parsed = morph.parse(word)
            if not parsed:
                return False
            
            first = parsed[0]
            tag_str = str(first.tag)
            
            # Если первый вариант — гео-название, это НЕ обычное слово
            if 'Geox' in tag_str:
                return False
            
            # Если первый вариант — NOUN или ADJF без Geox → обычное слово
            if ('NOUN' in tag_str or 'ADJF' in tag_str) and word.islower():
                return True
        except:
            pass
        
        return False

    def _extract_cities_from_seed(self, seed: str, country: str, language: str) -> Set[str]:
        """🔥 FIX: Извлекает города из seed БЕЗ фильтра по стране"""
        if not self._has_morph:
            return set()
        
        seed_cities = set()
        words = re.findall(r'[а-яёa-z0-9-]+', seed.lower())
        
        for word in words:
            # БЕЗ ПРОВЕРКИ country!
            if word in self.all_cities_global:
                logger.debug(f"[BPF] seed_city WORD '{word}' -> {self.all_cities_global[word]}")
                seed_cities.add(word)
            
            lemma = self._get_lemma(word, language)
            if lemma in self.all_cities_global:
                logger.debug(f"[BPF] seed_city LEMMA '{lemma}' <- '{word}' "
                             f"-> {self.all_cities_global[lemma]}")
                seed_cities.add(lemma)
        
        bigrams = self._extract_ngrams(words, 2)
        for bigram in bigrams:
            if bigram in self.all_cities_global:
                logger.debug(f"[BPF] seed_city BIGRAM '{bigram}' -> {self.all_cities_global[bigram]}")
                seed_cities.add(bigram)
        
        return seed_cities

    def _batch_lemmatize(self, words: Set[str], language: str) -> Dict[str, str]:
        if not self._has_morph:
            return {w: w for w in words}
        
        morph = self.morph_ru if language == 'ru' else self.morph_uk
        lemmas = {}
        
        for word in words:
            lemma = self._get_lemma(word, language, morph)
            lemmas[word] = lemma
        
        return lemmas

    def _get_lemma(self, word: str, language: str, morph=None) -> str:
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
        if len(words) < n:
            return []
        return [" ".join(words[i:i+n]) for i in range(len(words) - n + 1)]

    def _is_grammatically_valid(self, keyword: str, language: str) -> bool:
        return True  # Временная полная амнистия, чтобы спасти ключи


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
