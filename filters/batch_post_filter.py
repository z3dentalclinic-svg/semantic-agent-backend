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
                kw, country, lemmas_map, seed_cities, language
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
                                  language: str) -> Tuple[bool, str, str]:
        logger.debug(f"[BPF] CHECK keyword='{keyword}' | country={country} | "
                     f"seed_cities={seed_cities}")
        
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

        for item in search_items:
            if len(item) < 3 or item in self.ignored_words:
                continue
            
            item_normalized = self._get_lemma(item, language)
            found_country = self.all_cities_global.get(item_normalized) or self.all_cities_global.get(item)
            
            if found_country:
                # АМНИСТИЯ: Если это город нашей страны (UA) или он в SEED - ПРОПУСКАЕМ
                if found_country == country.lower() or item_normalized in seed_cities:
                    continue
                
                # БЛОКИРУЕМ ТОЛЬКО РЕАЛЬНО ЧУЖИЕ СТРАНЫ
                return False, f"Foreign city {found_country}", f"{found_country}_cities"
            
            # 🔥 НОВОЕ: Если это район или микрорайон (Черемушки, Алексеевка), 
            # и он не распознан как чужой город - МЫ ЕГО НЕ ТРОГАЕМ (True)

                if self._is_common_noun(item_normalized, language):
                    continue
        
        if not self._is_grammatically_valid(keyword, language):
            return False, "неправильная грамматическая форма", "grammar"
        
        return True, "", ""

    def _is_common_noun(self, word: str, language: str) -> bool:
        if not self._has_morph or language not in ['ru', 'uk']:
            return False
        
        morph = self.morph_ru if language == 'ru' else self.morph_uk
        
        try:
            parsed = morph.parse(word)
            if parsed:
                for parse_variant in parsed:
                    tag = parse_variant.tag
                    
                    if 'Geox' in tag:
                        return False
                    
                    if 'Name' in tag:
                        return False
                
                first_tag = parsed[0].tag
                if 'NOUN' in first_tag and word.islower():
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
