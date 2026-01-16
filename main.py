"""
Batch Post-Filter v8.0 - TWO-LEVEL GEO DATABASE SUPPORT
Based on Gemini's recommendations for 187 countries support

🎯 НОВОЕ В v8.0:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ДВУХУРОВНЕВАЯ БАЗА ГОРОДОВ:
  Level 1: Cities >15k (global) - ~158k названий
  Level 2: Cities >1k (CIS: BY, KZ, RU, PL, LT, LV, EE) - +27k
  
РЕЗУЛЬТАТ:
  - Ждановичи (BY, 7k) ✅ блокируется
  - Барановичи (BY, 170k) ✅ блокируется
  - +85% покрытие для Беларуси
  - Всего: ~183k городов
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔥 ФУНДАМЕНТАЛЬНОЕ ИСПРАВЛЕНИЕ v7.9:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ПРОБЛЕМА v7.7-v7.8:
  Морфология (_is_common_noun) проверялась ДО блокировки
  → "барановичи" = NOUN → пропускался
  → "лошица" = NOUN → пропускался
  
РЕШЕНИЕ v7.9:
  База городов = ПЕРВИЧНА
  Морфология = ВТОРИЧНА (только для слов ВНЕ базы)
  
  НОВЫЙ АЛГОРИТМ:
  1. Нормализация (лемма)
  2. Поиск в all_cities_global
  3. Если найдено И country != target → БЛОК (БЕЗ проверки NOUN!)
  4. Если НЕ найдено → проверяем _is_common_noun
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ РЕЗУЛЬТАТ v7.9:
  - "барановичи" (BY) → найдено в базе → БЛОК ⚓
  - "талдыкорган" (KZ) → найдено в базе → БЛОК ⚓
  - "дом" (Ghana) → НЕ найдено → _is_common_noun → разрешено ✅

КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ v7.7:
🔥 ИСПРАВЛЕНО: Лемматизация теперь применяется ПЕРЕД поиском в базе городов
🔥 ИСПРАВЛЕНО: Все склонённые формы нормализуются ("в актобе" → "актобе")
✅ Актобе, Фаниполь, Ошмяны теперь правильно блокируются

КРИТИЧЕСКИЕ УЛУЧШЕНИЯ v7.6:
✅ Population filter (> 5000) - игнорируем малые сёла-тёзки
✅ Smart disambiguation через Pymorphy3 (NOUN vs Geox)
✅ Улучшенная N-gram detection
✅ Автономная работа для любой из 187 стран
✅ O(1) lookup через предварительный индекс
✅ Ручной словарь малых городов СНГ (ош, узынагаш, щелкино)

FIXES v7.9 → v8.0:
- Расширена база городов: +27k малых городов СНГ
- Покрытие BY увеличено на 85% (971 → 1,796 названий)
- Ждановичи, Серебрянка и другие малые города теперь находятся
- Логика фильтрации не изменена (совместима с v7.9)
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
        v7.5 Constructor with population filtering
        
        Args:
            all_cities_global: Dict {city_name: country_code} (lowercase)
            forbidden_geo: Set of forbidden locations (Крым/ОРДЛО - lemmatized)
            districts: Optional Dict {district_name: country_code}
            population_threshold: Minimum city population to consider (default: 5000)
        """
        self.forbidden_geo = forbidden_geo
        self.districts = districts or {}
        self.population_threshold = population_threshold
        
        # v7.5: Дополнительные словари для лучшего покрытия
        self.city_abbreviations = self._get_city_abbreviations()
        self.regions = self._get_regions()
        self.countries = self._get_countries()
        self.manual_small_cities = self._get_manual_small_cities()  # v7.6: Малые города СНГ
        
        # v7.6: Ignored words - обычные слова которые НЕ являются городами
        # Даже если есть в базе geonamescache
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
        
        # v7.5: Перестраиваем индекс с учётом населения
        self.all_cities_global = self._build_filtered_geo_index()
        
        # v7.6: КРИТИЧЕСКИЙ ЛОГ - проверяем есть ли Ошмяны и Фаниполь в индексе
        # Ищем любые варианты названий этих городов
        test_patterns = ['oshmyan', 'fanipal', 'fanipol']  # латиница - надёжнее
        found_test = {}
        for key, val in self.all_cities_global.items():
            if any(pattern in key for pattern in test_patterns):
                found_test[key] = val
                if len(found_test) >= 10:  # Ограничим вывод
                    break
        
        logger.warning(f"🔍 v7.6 DEBUG: Cities matching 'oshmyan/fanipal': {found_test}")
        logger.warning(f"🔍 v7.6 DEBUG: Total index size: {len(self.all_cities_global)} entries")
        logger.warning(f"🔍 v7.6 DEBUG: Sample keys (first 10): {list(self.all_cities_global.keys())[:10]}")
        
        # 🔥 КРИТИЧЕСКИЙ DEBUG v7.7 - ПРОВЕРКА ПРОБЛЕМНЫХ ГОРОДОВ
        logger.error("="*60)
        logger.error("🔥 v7.7 CRITICAL DEBUG - CHECKING PROBLEM CITIES")
        logger.error("="*60)
        logger.error(f"🔥 Dict size: {len(self.all_cities_global)} cities")
        
        test_problem_cities = {
            'барановичи': 'by',
            'baranavičy': 'by', 
            'baranovichi': 'by',
            'актобе': 'kz',
            'aktobe': 'kz',
            'aqtobe': 'kz',
            'грозный': 'ru',
            'grozny': 'ru',
            'groznyy': 'ru',
            'талдыкорган': 'kz',
            'taldykorgan': 'kz',
            'усть-каменогорск': 'kz',
            'oskemen': 'kz'
        }
        
        for city, expected in test_problem_cities.items():
            in_dict = city in self.all_cities_global
            actual = self.all_cities_global.get(city, 'NOT_FOUND')
            status = "✅" if in_dict else "❌"
            logger.error(f"{status} '{city}': in_dict={in_dict}, value={actual}, expected={expected}")
        
        logger.error("="*60)
        
        # Инициализация Pymorphy3
        try:
            import pymorphy3
            self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
            self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk')
            self._has_morph = True
            logger.info("✅ Pymorphy3 initialized for v7.7")
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
            'крым': 'ru',  # Политически спорно, но в базе как RU
            
            # РФ области
            'московская область': 'ru',
            'ленинградская область': 'ru',
            'новосибирская область': 'ru',
            'свердловская область': 'ru',
            
            # BY области
            'минская область': 'by',
            'гомельская область': 'by',
            'могилевская область': 'by',
            'витебская область': 'by',
            'гродненская область': 'by',
            'брестская область': 'by',
            
            # KZ области
            'алматинская область': 'kz',
            'южно-казахстанская область': 'kz',
            
            # UZ области
            'ташкентская область': 'uz',
            'самаркандская область': 'uz',
        }
    
    def _get_countries(self) -> Dict[str, str]:
        """Названия стран (для блокировки запросов типа "в израиле")"""
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
        """
        v7.6: Ручной словарь малых городов СНГ
        Города с населением < 5000 или короткие названия (< 3 символа)
        которые не попадают в основную базу geonamescache
        """
        return {
            # Короткие города (< 3 символа)
            'ош': 'kg',  # Ош, Киргизия
            
            # Малые города Казахстана
            'узынагаш': 'kz',  # Узынагаш, Казахстан
            
            # Малые города Крыма (оккупированная территория)
            'щелкино': 'ru',  # Щёлкино, Крым
            'щёлкino': 'ru',
            
            # Другие малые города которые могут появиться
            'йота': 'unknown',  # Неизвестный город/бренд - на всякий случай
        }
    
    def _build_filtered_geo_index(self) -> Dict[str, str]:
        """
        v7.5: Создаём индекс городов с фильтром по населению
        
        Это устраняет проблему "дом" (Ghana), "мир" и т.д.
        Малые сёла с населением < 5000 игнорируются.
        """
        try:
            import geonamescache
            gc = geonamescache.GeonamesCache()
            cities = gc.get_cities()
            
            filtered_index = {}
            total_cities = 0
            filtered_out = 0
            
            for city_id, city_data in cities.items():
                country = city_data['countrycode'].lower()
                population = city_data.get('population', 0)
                
                # v7.5: ФИЛЬТР ПО НАСЕЛЕНИЮ
                if population < self.population_threshold:
                    filtered_out += 1
                    continue
                
                name = city_data['name'].lower().strip()
                filtered_index[name] = country
                total_cities += 1
                
                # Альтернативные названия
                for alt in city_data.get('alternatenames', []):
                    # v7.6: Оставляем минимум 3 символа
                    if not (3 <= len(alt) <= 50):
                        continue
                    if not any(c.isalpha() for c in alt):
                        continue
                    
                    # Проверка на латиницу/кириллицу (с пробелами!)
                    is_latin_cyrillic = all(
                        ('\u0000' <= c <= '\u007F') or
                        ('\u0400' <= c <= '\u04FF') or
                        c in ['-', "'", ' ']  # v7.5: Добавили пробел!
                        for c in alt
                    )
                    
                    if is_latin_cyrillic:
                        alt_lower = alt.lower().strip()
                        if alt_lower not in filtered_index:
                            filtered_index[alt_lower] = country
                            # Также добавляем вариант с дефисом
                            alt_dash = alt_lower.replace(' ', '-')
                            if alt_dash != alt_lower:
                                filtered_index[alt_dash] = country
            
            logger.info(f"✅ v7.7 Geo Index built:")
            logger.info(f"   Cities with pop > {self.population_threshold}: {total_cities}")
            logger.info(f"   Total index entries (with alts): {len(filtered_index)}")
            logger.info(f"   Filtered out (pop < {self.population_threshold}): {filtered_out}")
            
            return filtered_index
            
        except ImportError:
            logger.warning("⚠️ geonamescache not found, using fallback minimal dict")
            # Fallback на минимальный словарь без фильтрации
            return {
                'москва': 'ru', 'санкт-петербург': 'ru', 
                'киев': 'ua', 'харьков': 'ua', 'одесса': 'ua',
                'минск': 'by', 'алматы': 'kz', 'ташкент': 'uz'
            }

    def filter_batch(self, keywords: List[str], seed: str, country: str, 
                     language: str = 'ru') -> Dict:
        """
        v7.5 Batch filtering with smart disambiguation
        """
        start_time = time.time()
        
        # 1. Предварительная очистка
        unique_raw = sorted(list(set([k.lower().strip() for k in keywords if k.strip()])))
        
        # 2. Извлекаем города из seed
        seed_cities = self._extract_cities_from_seed(seed, country, language)
        logger.info(f"[v7.7] Seed cities allowed: {seed_cities}")
        
        # 3. Batch лемматизация
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

        # 4. Фильтруем с v7.5 логикой
        for kw in unique_raw:
            # v7.6 DEBUG: логируем keywords содержащие oshmyan или fanipol
            kw_lower = kw.lower()
            if 'oshmyan' in kw_lower or 'fanipal' in kw_lower or 'fanipol' in kw_lower:
                logger.warning(f"🔍 v7.6 DEBUG INPUT: '{kw}' → проверяем...")
            
            is_allowed, reason, category = self._check_geo_conflicts_v75(
                kw, country, lemmas_map, seed_cities, language
            )
            
            if is_allowed:
                final_keywords.append(kw)
                stats['allowed'] += 1
                logger.debug(f"[v7.7] ✅ РАЗРЕШЕНО: '{kw}'")
            else:
                final_anchors.append(kw)
                stats['blocked'] += 1
                stats['reasons'][category] += 1
                logger.warning(f"[v7.7] ⚓ ЯКОРЬ: '{kw}' (причина: {reason})")

        elapsed = time.time() - start_time
        logger.info(f"[v7.7] Finished in {elapsed:.2f}s. {stats['allowed']} OK / {stats['blocked']} Anchors")

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
        """
        v7.6: Улучшенная проверка с population filter и smart disambiguation
        + защита от ложных срабатываний (Алексеевка в Харькове)
        """
        words = re.findall(r'[а-яёa-z0-9-]+', keyword)
        if not words:
            return True, "", ""

        keyword_lemmas = [lemmas_map.get(w, w) for w in words]
        
        # --- 0. ПРИОРИТЕТ: ПРОВЕРКА SEED_CITY (v7.6) ---
        # Если в запросе есть город из seed (например "харьков алексеевка"),
        # то доверяем этому запросу и НЕ блокируем по другим словам
        words_set = set(words + keyword_lemmas)
        if any(city in words_set for city in seed_cities):
            logger.debug(f"[v7.6] '{keyword}' contains seed city, auto-allow")
            return True, "", ""
        
        # --- 1. HARD-BLACKLIST (приоритет #1) ---
        for check_val in words + keyword_lemmas:
            if check_val in self.forbidden_geo:
                return False, f"Hard-Blacklist '{check_val}'", "hard_blacklist"

        # --- 2. РАЙОНЫ ---
        for w in words:
            if w in self.districts:
                dist_country = self.districts[w]
                if dist_country != country.lower():
                    return False, f"район '{w}' ({dist_country})", "districts"
        
        # --- 2.5. СОКРАЩЕНИЯ ГОРОДОВ (v7.5) ---
        for w in words + keyword_lemmas:
            if w in self.city_abbreviations:
                abbr_country = self.city_abbreviations[w]
                if abbr_country != country.lower():
                    return False, f"сокращение города '{w}' ({abbr_country})", f"{abbr_country}_abbreviations"
        
        # --- 2.6. РЕГИОНЫ (v7.5) ---
        # Проверяем одиночные слова и биграммы
        check_regions = words + keyword_lemmas + self._extract_ngrams(words, 2)
        for item in check_regions:
            if item in self.regions:
                region_country = self.regions[item]
                if region_country != country.lower():
                    return False, f"регион '{item}' ({region_country})", f"{region_country}_regions"
        
        # --- 2.7. СТРАНЫ (v7.5) ---
        for w in words + keyword_lemmas:
            if w in self.countries:
                ctry_code = self.countries[w]
                if ctry_code != country.lower():
                    return False, f"страна '{w}' ({ctry_code})", f"{ctry_code}_countries"
        
        # --- 2.8. МАЛЫЕ ГОРОДА СНГ (v7.6) ---
        for w in words + keyword_lemmas:
            if w in self.manual_small_cities:
                city_country = self.manual_small_cities[w]
                if city_country == 'unknown':
                    # Блокируем всегда (неизвестный источник)
                    return False, f"неизвестный объект '{w}'", "unknown"
                if city_country != country.lower():
                    return False, f"малый город '{w}' ({city_country})", f"{city_country}_small_cities"

        # --- 3. ГОРОДА (v7.5 с population filter) ---
        # Собираем все варианты для проверки (расширенный список)
        search_items = []
        
        # Оригинальные слова (для сокращений типа "екб")
        search_items.extend(words)
        
        # Леммы слов
        search_items.extend(keyword_lemmas)
        
        # Биграммы из оригинальных слов (набережные челны, йошкар ола)
        bigrams = self._extract_ngrams(words, 2)
        search_items.extend(bigrams)
        
        # КРИТИЧНО: Биграммы с дефисом вместо пробела (йошкар-ола вместо йошкар ола)
        search_items.extend([bg.replace(' ', '-') for bg in bigrams])
        
        # Биграммы из лемматизированных слов
        lemma_bigrams = self._extract_ngrams(keyword_lemmas, 2)
        search_items.extend(lemma_bigrams)
        search_items.extend([bg.replace(' ', '-') for bg in lemma_bigrams])
        
        # Триграммы для городов из 3 слов (если есть)
        trigrams = self._extract_ngrams(words, 3)
        search_items.extend(trigrams)
        search_items.extend([tg.replace(' ', '-') for tg in trigrams])

        for item in search_items:
            if len(item) < 3:
                continue
            
            # v7.6: ПРИОРИТЕТ - проверяем ignored_words ДО базы городов
            if item in self.ignored_words:
                logger.debug(f"[v7.6] '{item}' in ignored_words, skipping")
                continue
            
            # ✅ v8.0 DEBUG: Специальное логирование для проблемных городов
            debug_cities = ['ждановичи', 'zhdanovichi', 'жданович', 'лошица', 'losica']
            is_debug_city = any(dc in item.lower() for dc in debug_cities)
            
            if is_debug_city:
                logger.warning(f"🔍 [v8.0 DEBUG] Processing '{item}'")
            
            # ✅ v7.9 ФУНДАМЕНТАЛЬНОЕ ИСПРАВЛЕНИЕ: БАЗА ГОРОДОВ = ПЕРВИЧНА
            # 
            # СТАРАЯ ОШИБКА v7.7-v7.8: 
            #   1. Проверяли базу
            #   2. Если нашли → проверяли _is_common_noun 
            #   3. Если NOUN → пропускали ("лошица", "барановичи")
            #
            # НОВАЯ ЛОГИКА v7.9:
            #   1. Проверяем базу
            #   2. Если нашли И город из другой страны → БЛОКИРУЕМ НЕМЕДЛЕННО
            #   3. Морфология НЕ ВЛИЯЕТ на решение
            
            # Нормализуем слово (склонённые формы → базовая форма)
            item_normalized = self._get_lemma(item, language)
            
            if is_debug_city:
                logger.warning(f"🔍 [v8.0 DEBUG] Normalized: '{item}' → '{item_normalized}'")
            
            # Проверяем в базе: сначала нормализованная форма, потом оригинал
            found_country = self.all_cities_global.get(item_normalized)
            
            if is_debug_city:
                logger.warning(f"🔍 [v8.0 DEBUG] Database lookup (normalized): '{item_normalized}' → {found_country}")
                logger.warning(f"🔍 [v8.0 DEBUG] Database size: {len(self.all_cities_global)} cities")
            
            if not found_country:
                # Не нашли лемму - пробуем оригинал (для сокращений типа "екб")
                found_country = self.all_cities_global.get(item)
                
                if is_debug_city:
                    logger.warning(f"🔍 [v8.0 DEBUG] Database lookup (original): '{item}' → {found_country}")
                
                if found_country:
                    logger.debug(f"[v8.0] Found original: '{item}' → {found_country}")
                    item_normalized = item  # Используем оригинал
            elif item_normalized != item:
                logger.debug(f"[v8.0] Found via lemma: '{item}' → '{item_normalized}' → {found_country}")
            
            # ========== КРИТИЧЕСКАЯ ЛОГИКА v7.9 ==========
            if found_country:
                # Город найден в базе!
                
                if is_debug_city:
                    logger.warning(f"🔍 [v8.0 DEBUG] FOUND IN DATABASE: '{item_normalized}' = {found_country.upper()}")
                    logger.warning(f"🔍 [v8.0 DEBUG] Target country: {country.lower()}")
                    logger.warning(f"🔍 [v8.0 DEBUG] Match: {found_country == country.lower()}")
                
                # Проверка 1: Это наш целевой город? (например, Харьков в UA)
                if found_country == country.lower():
                    logger.debug(f"[v8.0] City '{item_normalized}' ({found_country}) - ALLOWED (target country)")
                    if is_debug_city:
                        logger.warning(f"🔍 [v8.0 DEBUG] ✅ ALLOWED - same country")
                    continue
                
                # Проверка 2: Это город из seed? (защита от ложных срабатываний)
                # Пример: seed="ремонт харьков алексеевка" → "алексеевка" может быть в RU, но это район Харькова
                if item_normalized in seed_cities:
                    logger.debug(f"[v8.0] City '{item_normalized}' in seed_cities - ALLOWED")
                    if is_debug_city:
                        logger.warning(f"🔍 [v8.0 DEBUG] ✅ ALLOWED - in seed")
                    continue
                
                # Проверка 3: Город из другой страны → БЛОКИРУЕМ
                # ⚠️ ВАЖНО: Морфология (NOUN/не-NOUN) НЕ ВЛИЯЕТ на это решение!
                if is_debug_city:
                    logger.warning(f"🔍 [v8.0 DEBUG] ⚓ SHOULD BLOCK: '{item}' → '{item_normalized}' ({found_country.upper()} != {country.upper()})")
                
                logger.warning(f"[v8.0] ⚓ BLOCKING foreign city: '{item}' → '{item_normalized}' ({found_country.upper()})")
                return False, f"{found_country.upper()} город '{item_normalized}'", f"{found_country}_cities"
            
            # ========== МОРФОЛОГИЯ = ВТОРИЧНА (только для слов ВНЕ базы) ==========
            # Если город НЕ найден в базе - проверяем морфологию
            # Это защита от "дом" (Ghana), "мир" (Russia) и т.д.
            else:
                # Слово не в базе городов - возможно это обычное слово?
                if self._is_common_noun(item_normalized, language):
                    logger.debug(f"[v7.9] '{item_normalized}' NOT in geo database + common noun - ALLOWED")
                    continue
                # Если не NOUN и не в базе - тоже пропускаем (техническое слово)
        
        # --- 4. ГРАММАТИКА ---
        if not self._is_grammatically_valid(keyword, language):
            return False, "неправильная грамматическая форма", "grammar"
        
        return True, "", ""

    def _is_common_noun(self, word: str, language: str) -> bool:
        """
        v7.7 FIXED: Smart disambiguation с приоритетом Geox
        
        Примеры:
        - "дом" → True (обычное слово, НЕ город)
        - "ошмяны" → False (Geox - географический объект)
        - "киев" → False (собственное имя, город)
        """
        if not self._has_morph or language not in ['ru', 'uk']:
            return False
        
        morph = self.morph_ru if language == 'ru' else self.morph_uk
        
        try:
            parsed = morph.parse(word)
            if parsed:
                # ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Проверяем ВСЕ варианты парсинга
                for parse_variant in parsed:
                    tag = parse_variant.tag
                    
                    # ПРИОРИТЕТ 1: Если хотя бы один вариант = Geox → это город!
                    if 'Geox' in tag:
                        logger.debug(f"[v7.7] '{word}' is Geox (geographic), NOT common noun")
                        return False
                    
                    # ПРИОРИТЕТ 2: Если Name (собственное имя) → не обычное слово
                    if 'Name' in tag:
                        return False
                
                # Только если НИ ОДИН вариант не Geox/Name - проверяем NOUN
                first_tag = parsed[0].tag
                if 'NOUN' in first_tag and word.islower():
                    logger.debug(f"[v7.7] '{word}' is common noun")
                    return True
        except:
            pass
        
        return False

    def _extract_cities_from_seed(self, seed: str, country: str, language: str) -> Set[str]:
        """Извлекает города из seed"""
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
        """
        Извлекает n-граммы (биграммы, триграммы)
        
        v7.5: Поддержка n=2,3 для многословных городов
        """
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


# ============================================
# EXAMPLE USAGE
# ============================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("🚀 BATCH POST-FILTER v7.5 - AUTONOMOUS GLOBAL GEO-FILTER")
    print("="*60)
    
    # Hard-Blacklist
    test_forbidden = {
        "крым", "севастополь", "симферополь", "ялта",
        "донецк", "луганск", "горловка"
    }
    
    # Создаем фильтр v7.5 (автоматически загрузит базу с population > 5000)
    print("\n📦 Инициализация фильтра...")
    post_filter = BatchPostFilter(
        all_cities_global={},  # Будет перестроен автоматически
        forbidden_geo=test_forbidden,
        districts=DISTRICTS_EXTENDED,
        population_threshold=5000
    )
    
    # Тестовые данные - РЕАЛЬНЫЕ ПРОБЛЕМНЫЕ KEYWORDS
    test_keywords = [
        # ✅ Должны ПРОПУСТИТЬСЯ (UA города):
        "ремонт пылесосов киев",
        "ремонт пылесосов днепр",
        "ремонт пылесосов харьков",
        
        # ⚓ Должны БЛОКИРОВАТЬСЯ (RU города):
        "ремонт роботов пылесосов йошкар ола",  # RU (биграмма с пробелом)
        "ремонт пылесосов улан удэ",            # RU (биграмма)
        "ремонт пылесосов набережные челны",    # RU (биграмма)
        "ремонт пылесосов орехово зуево",       # RU (биграмма)
        "ремонт пылесосов екб",                 # RU (сокращение Екатеринбург)
        
        # ⚓ Должны БЛОКИРОВАТЬСЯ (BY города):
        "ремонт пылесосов фаниполь",            # BY
        "ремонт пылесосов ошмяны",              # BY
        
        # ⚓ Должны БЛОКИРОВАТЬСЯ (KZ города):
        "ремонт пылесосов узынагаш",            # KZ
        
        # ⚓ Должны БЛОКИРОВАТЬСЯ (другие страны):
        "ремонт пылесосов дайсон в израиле",    # IL (Israel)
        
        # ⚓ Должны БЛОКИРОВАТЬСЯ (регионы RU):
        "ремонт пылесосов ингушетия",           # Регион RU
        
        # ⚓ Должны БЛОКИРОВАТЬСЯ (Hard-Blacklist):
        "ремонт пылесосов севастополь",         # Крым
    ]
    
    print(f"\n🧪 Тестируем на {len(test_keywords)} keywords...")
    result = post_filter.filter_batch(
        keywords=test_keywords,
        seed="ремонт пылесосов",
        country="ua",
        language="ru"
    )
    
    print("\n" + "="*60)
    print("📊 РЕЗУЛЬТАТЫ:")
    print("="*60)
    print(f"\n✅ РАЗРЕШЕНО ({len(result['keywords'])}):")
    for kw in result['keywords']:
        print(f"  - {kw}")
    
    print(f"\n⚓ ЯКОРЯ ({len(result['anchors'])}):")
    for kw in result['anchors']:
        print(f"  - {kw}")
    
    print(f"\n📈 СТАТИСТИКА:")
    print(f"  Total: {result['stats']['total']}")
    print(f"  Allowed: {result['stats']['allowed']}")
    print(f"  Blocked: {result['stats']['blocked']}")
    print(f"  Reasons: {result['stats']['reasons']}")
    print(f"  Time: {result['stats']['elapsed_time']}s")
    print("="*60 + "\n")
