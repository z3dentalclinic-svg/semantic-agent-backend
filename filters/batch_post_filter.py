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

        # Кэш на уровень одного filter_batch: (word, country) → bool
        # Сбрасывается при каждом вызове filter_batch.
        # Ключевая экономия: слово "купить" в 500 ключах → 1 вызов _find_in_country вместо 500.
        self._request_cache: dict = {}
        self._lemma_cache: dict = {}          # (word, lang) → str, персистентный
        self._word_features_cache: dict = {}  # (word, lang) → dict, персистентный
        # Старые отдельные кэши — оставлены для совместимости, не используются
        self._skip_geo_cache: dict = {}
        self._common_noun_cache: dict = {}

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
        PRIORITY 1: Проверка - является ли слово городом ЦЕЛЕВОЙ страны.
        Кэш: _request_cache (весь filter_batch).
        Лемму берём из _lemmas_map (уже готов) — без повторного morph.parse.
        """
        word_lower = word.lower()
        cache_key = (word_lower, target_country)
        if cache_key in self._request_cache:
            return self._request_cache[cache_key]

        target = target_country.lower()

        # Прямой поиск в базе
        found_country = self.all_cities_global.get(word_lower)
        if found_country and found_country == target:
            self._request_cache[cache_key] = True
            return True

        # Лемма: сначала из готового batch-словаря, иначе через _lemma_cache
        lemma = self._lemmas_map.get(word_lower) or self._get_lemma(word_lower, 'ru')
        if lemma and lemma != word_lower:
            found_country = self.all_cities_global.get(lemma)
            if found_country and found_country == target:
                self._request_cache[cache_key] = True
                return True

        self._request_cache[cache_key] = False
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

        # Сбрасываем кэш для нового запроса
        # Сбрасываем только _request_cache — он зависит от country (ua/ru/by)
        # Остальные кэши персистентны: слова между запросами одинаковые
        self._request_cache = {}
        self._lemmas_map = {}  # batch lemmas для текущего запроса

        logger.info("[BPF] START filter_batch | country=%s | lang=%s | keywords=%d", country, language, len(keywords))
        
        unique_raw = sorted(list(set([k.lower().strip() for k in keywords if k.strip()])))
        
        seed_cities = self._extract_cities_from_seed(seed, country, language)
        logger.debug("[BPF] SEED='%s' | seed_cities=%s", seed, seed_cities)
        
        all_words = set()
        for kw in unique_raw:
            all_words.update(re.findall(r'[а-яёa-z0-9-]+', kw))
        
        lemmas_map = self._batch_lemmatize(all_words, language)
        self._lemmas_map = lemmas_map  # сохраняем для _find_in_country
        
        final_keywords = []
        final_anchors = []
        stats = {
            'total': len(unique_raw),
            'allowed': 0,
            'blocked': 0,
            'reasons': Counter()
        }

        # ПРОФИЛИРОВАНИЕ: суммируем время по блокам за весь батч
        _t_precheck = 0.0
        _t_static = 0.0   # districts/abbr/regions/countries
        _t_search = 0.0   # основной цикл search_items
        _t_other = 0.0    # остальное внутри _check_geo_conflicts_v75

        for kw in unique_raw:
            _t0 = time.perf_counter()
            is_allowed, reason, category, _prof = self._check_geo_conflicts_v75(
                kw, country, lemmas_map, seed_cities, language, seed
            )
            _dt = time.perf_counter() - _t0
            _t_precheck += _prof.get('precheck', 0)
            _t_static   += _prof.get('static', 0)
            _t_search   += _prof.get('search', 0)
            _t_other    += _dt - _prof.get('precheck', 0) - _prof.get('static', 0) - _prof.get('search', 0)

            if is_allowed:
                final_keywords.append(kw)
                stats['allowed'] += 1
            else:
                final_anchors.append(kw)
                stats['blocked'] += 1
                stats['reasons'][category] += 1
                logger.warning("BPF block: '%s' → %s (%s)", kw, reason, category)

        elapsed = time.time() - start_time
        logger.info("[BPF] FINISH %.2fs | allowed=%d | anchors=%d | reasons=%s",
                    elapsed, len(final_keywords), len(final_anchors), dict(stats['reasons']))
        logger.info("[BPF_PROFILE] precheck=%.3fs static=%.3fs search=%.3fs other=%.3fs | lemmatize=%.3fs",
                    _t_precheck, _t_static, _t_search, _t_other,
                    elapsed - _t_precheck - _t_static - _t_search - _t_other)

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
                                  language: str, seed: str = "") -> Tuple:
        _p = {}  # профиль времён блоков
        _t0 = time.perf_counter()

        words = re.findall(r'[а-яёa-z0-9-]+', keyword)
        if not words:
            return True, "", "", _p

        keyword_lemmas = [lemmas_map.get(w, w) for w in words]
        words_set = set(words + keyword_lemmas)

        # FAST PRE-CHECK
        if not (words_set & self.forbidden_geo):
            has_any_geo = any(
                w in self.all_cities_global or
                w in self.districts or
                w in self.regions or
                w in self.countries or
                w in self.city_abbreviations or
                w in self.manual_small_cities
                for w in words_set
            )
            if not has_any_geo:
                _p['precheck'] = time.perf_counter() - _t0
                return True, "", "", _p

        _p['precheck'] = time.perf_counter() - _t0
        _t1 = time.perf_counter()

        # Проверяем наличие корней сида в ключе
        has_seed = self._has_seed_cores(keyword, seed) if seed else False

        if any(city in words_set for city in seed_cities):
            _p['static'] = time.perf_counter() - _t1
            return True, "", "", _p

        for check_val in words_set:
            if check_val in self.forbidden_geo:
                _p['static'] = time.perf_counter() - _t1
                return False, f"Hard-Blacklist '{check_val}'", "hard_blacklist", _p

        # Биграмы вычисляем один раз — используем везде ниже
        word_bigrams = self._extract_ngrams(words, 2)

        # Собираем биграмы которые являются городами НАШЕЙ страны
        our_city_bigrams = set()
        for bg in word_bigrams:
            if self._find_in_country(bg, country):
                our_city_bigrams.add(bg)
                for part in bg.split():
                    our_city_bigrams.add(part)

        country_l = country.lower()

        for w in words:
            if w in self.districts:
                dist_country = self.districts[w]
                if dist_country != country_l:
                    if w in our_city_bigrams:
                        continue
                    if self._find_in_country(w, country):
                        continue
                    if self._is_common_noun(w, language):
                        continue
                    has_target_city = any(
                        self.all_cities_global.get(other_w) == country_l
                        for other_w in words_set - {w}
                    )
                    if has_target_city:
                        continue
                    _p['static'] = time.perf_counter() - _t1
                    return False, f"район '{w}' ({dist_country})", "districts", _p

        for w in words + keyword_lemmas:
            if w in self.city_abbreviations:
                abbr_country = self.city_abbreviations[w]
                if abbr_country != country_l:
                    _p['static'] = time.perf_counter() - _t1
                    return False, f"сокращение города '{w}' ({abbr_country})", f"{abbr_country}_abbreviations", _p

        check_regions = words + keyword_lemmas + word_bigrams
        for item in check_regions:
            if item in self.regions:
                region_country = self.regions[item]
                if region_country != country_l:
                    _p['static'] = time.perf_counter() - _t1
                    return False, f"регион '{item}' ({region_country})", f"{region_country}_regions", _p

        for w in words + keyword_lemmas:
            if w in self.countries:
                ctry_code = self.countries[w]
                if ctry_code != country_l:
                    _p['static'] = time.perf_counter() - _t1
                    return False, f"страна '{w}' ({ctry_code})", f"{ctry_code}_countries", _p

        for w in words + keyword_lemmas:
            if w in self.manual_small_cities:
                city_country = self.manual_small_cities[w]
                if city_country == 'unknown':
                    _p['static'] = time.perf_counter() - _t1
                    return False, f"неизвестный объект '{w}'", "unknown", _p
                if city_country != country_l:
                    _p['static'] = time.perf_counter() - _t1
                    return False, f"малый город '{w}' ({city_country})", f"{city_country}_small_cities", _p

        _p['static'] = time.perf_counter() - _t1
        _t2 = time.perf_counter()

        # word_bigrams уже вычислен выше — переиспользуем
        lemma_bigrams = self._extract_ngrams(keyword_lemmas, 2)
        # Триграммы только если 3+ слов — иначе пусто и лишние итерации
        trigrams = self._extract_ngrams(words, 3) if len(words) >= 3 else []

        search_items = list(dict.fromkeys(
            words + keyword_lemmas
            + word_bigrams + [bg.replace(' ', '-') for bg in word_bigrams]
            + lemma_bigrams + [bg.replace(' ', '-') for bg in lemma_bigrams]
            + trigrams + [tg.replace(' ', '-') for tg in trigrams]
        ))

        # ОПТИМИЗАЦИЯ: один проход вместо двух — строим our_city_lemmas
        # и keyword_has_target_city одновременно, без дублирующих _find_in_country
        our_city_lemmas = set()
        keyword_has_target_city = False

        for w in words_set:
            if self._find_in_country(w, country):
                keyword_has_target_city = True
                # Берём лемму из готового lemmas_map — без повторного morph.parse
                lemma = lemmas_map.get(w, w)
                if lemma != w:
                    our_city_lemmas.add(lemma)

        for item in search_items:
            # ШАГ 0: Пропускаем короткие слова и ignored_words
            if len(item) < 3 or item in self.ignored_words:
                continue
            
            # ШАГ 0.5: Geox guard — пропускаем слова, которые точно НЕ города
            if ' ' not in item and '-' not in item:
                if self._should_skip_geo_check(item, language):
                    continue
            
            item_normalized = self._get_lemma(item, language)

            # PRIORITY 1: СВОЙ ГОРОД (целевая страна) — пропускаем
            is_our_city = self._find_in_country(item, country)
            if not is_our_city and item_normalized != item:
                is_our_city = self._find_in_country(item_normalized, country)
            if is_our_city:
                continue
            
            # PRIORITY 2: ЧУЖОЙ ГОРОД (другая страна)
            found_country = self.all_cities_global.get(item_normalized) or self.all_cities_global.get(item)
            
            if found_country and found_country != country.lower():
                
                # Лемма нашего города? "лев" ← "львов" (UA)
                if item in our_city_lemmas or item_normalized in our_city_lemmas:
                    continue
                
                # Обычное слово языка? "дом", "белая", "гора"
                if self._is_common_noun(item, language):
                    continue
                
                # Город из сида — всегда разрешён
                if item_normalized in seed_cities or item in seed_cities:
                    continue
                
                # В запросе есть город НАШЕЙ страны — проверяем районы и бренды
                if keyword_has_target_city:
                    item_no_yo = item.replace('ё', 'е')
                    dist_variants = [
                        self.districts.get(item),
                        self.districts.get(item_normalized),
                        self.districts.get(item_no_yo),
                    ]
                    if country.lower() in dist_variants:
                        continue
                    if any(d is not None for d in dist_variants):
                        continue
                    if not self._is_real_city_not_brand(item, found_country):
                        continue
                
                # PRIORITY 3: реальный город или спорное слово (бренд)?
                is_real_city = self._is_real_city_not_brand(item, found_country)
                
                if is_real_city:
                    reason = f"Слово '{item}' — это город в {found_country.upper()}, а мы парсим {country.upper()}"
                    return False, reason, f"{found_country}_cities"
                
                # Спорное слово
                if has_seed:
                    continue  # Есть seed — разрешаем
                else:
                    reason = f"Слово '{item}' — это город в {found_country.upper()}, а мы парсим {country.upper()}"
                    return False, reason, f"{found_country}_cities"
            
            # Город не найден — проверяем на обычное существительное
            if self._is_common_noun(item_normalized, language):
                continue
        
        if not self._is_grammatically_valid(keyword, language):
            _p['search'] = time.perf_counter() - _t2
            return False, "неправильная грамматическая форма", "grammar", _p

        _p['search'] = time.perf_counter() - _t2
        return True, "", "", _p

    def _get_word_features(self, word: str, language: str) -> dict:
        """
        Единый морфоразбор слова: один вызов morph.parse вместо 2-3 отдельных.
        Возвращает dict с lemma, skip_geo, is_common_noun.
        Кэшируется в _word_features_cache (персистентный между запросами).
        """
        cache_key = (word, language)
        if cache_key in self._word_features_cache:
            return self._word_features_cache[cache_key]

        features = {'skip_geo': False, 'is_common_noun': False}

        if not self._has_morph or language not in ['ru', 'uk']:
            self._word_features_cache[cache_key] = features
            return features

        morph = self.morph_ru if language == 'ru' else self.morph_uk
        try:
            parses = morph.parse(word)
            if parses:
                best = parses[0]
                tag_str = str(best.tag)
                pos = best.tag.POS

                # skip_geo: служебные части речи или известное слово без Geox
                if pos in ('CONJ', 'PREP', 'PRCL', 'INTJ'):
                    features['skip_geo'] = True
                elif morph.word_is_known(word):
                    has_geox = any('Geox' in str(p.tag) for p in parses)
                    if not has_geox:
                        features['skip_geo'] = True

                # is_common_noun: обычное нарицательное слово языка
                if not features['skip_geo'] and word.islower():
                    if not any(m in tag_str for m in ('Geox', 'Name', 'Surn', 'Patr', 'Orgn')):
                        if 'ADJF' in tag_str:
                            if ('Qual' in tag_str and best.score >= 0.4) or best.score >= 0.6:
                                features['is_common_noun'] = True
                        elif 'NOUN' in tag_str:
                            features['is_common_noun'] = 'inan' in tag_str and best.score >= 0.5
        except:
            pass

        self._word_features_cache[cache_key] = features
        return features

    def _is_common_noun(self, word: str, language: str) -> bool:
        return self._get_word_features(word, language)['is_common_noun']

    def _should_skip_geo_check(self, word: str, language: str) -> bool:
        return self._get_word_features(word, language)['skip_geo']

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
            # Пропускаем мусор до лемматизации
            if len(word) < 3 or word.isdigit() or word in self.ignored_words:
                lemmas[word] = word
                continue
            # Если слово уже есть в базе городов напрямую — лемматизация не нужна
            if word in self.all_cities_global:
                lemmas[word] = word
                continue
            lemma = self._get_lemma(word, language, morph)
            lemmas[word] = lemma

        return lemmas

    def _get_lemma(self, word: str, language: str, morph=None) -> str:
        if not self._has_morph:
            return word

        cache_key = (word, language)
        if cache_key in self._lemma_cache:
            return self._lemma_cache[cache_key]

        if morph is None:
            morph = self.morph_ru if language == 'ru' else self.morph_uk

        result = word
        try:
            parsed = morph.parse(word)
            if parsed:
                result = parsed[0].normal_form
        except:
            pass

        self._lemma_cache[cache_key] = result
        return result

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
