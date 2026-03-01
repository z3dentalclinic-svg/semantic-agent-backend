"""
FGS Parser API - Semantic keyword research with geo-filtering
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import List, Dict
import httpx
import asyncio
import time
import random
import re
import logging
from difflib import SequenceMatcher

from filters import (
    BatchPostFilter, 
    DISTRICTS_EXTENDED,
    filter_infix_results,
    filter_relevant_keywords,
    filter_geo_garbage,
    apply_pre_filter,  # ← санитарная очистка парсинга (ДО гео-фильтра)
    apply_l0_filter,   # ← L0 классификатор хвостов (ПОСЛЕ всех фильтров)
    apply_l2_filter,   # ← L2 Tri-Signal классификатор (PMI + Centroid + L0 signals)
)
from geo import generate_geo_blacklist_full
from config import USER_AGENTS, WHITELIST_TOKENS, MANUAL_RARE_CITIES, FORBIDDEN_GEO
from utils.normalizer import normalize_keywords
from utils.tracer import FilterTracer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

normalizer_logger = logging.getLogger("GoldenNormalizer")
normalizer_logger.setLevel(logging.DEBUG)
normalizer_logger.propagate = True

import nltk
from nltk.stem import SnowballStemmer

try:
    from natasha import Segmenter, MorphVocab, NewsEmbedding, NewsNERTagger, Doc
    NATASHA_AVAILABLE = True
except ImportError:
    NATASHA_AVAILABLE = False
    print("⚠️ Natasha не установлена. EntityLogicManager будет работать только с жёстким кешем.")

try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
except Exception as e:
    print(f"Warning: Could not download NLTK data: {e}")

import pymorphy3

app = FastAPI(
    title="FGS Parser API",
    version="8.0.0",
    description="6 методов | 3 sources | Batch Post-Filter | L0 + L2 Classifiers | v8.0 DUAL COSINE"
)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex='.*',  # Разрешает любые источники, включая локальные файлы
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.options("/{rest_of_path:path}")
async def preflight_handler():
    return {}

# === ЗАКОММЕНТИРОВАНО: дубль geo/blacklist.py, используется импорт (строка 25) ===
# def generate_geo_blacklist_full():
#     """
#     """
#     try:
#         from geonamescache import GeonamesCache
# 
#         gc = GeonamesCache()
#         cities = gc.get_cities()
# 
#         all_cities_global = {}  # {город: код_страны}
# 
#         for city_id, city_data in cities.items():
#             country = city_data['countrycode'].lower()  # 'RU', 'UA', 'BY' → 'ru', 'ua', 'by'
# 
#             name = city_data['name'].lower().strip()
#             all_cities_global[name] = country
# 
#             for alt in city_data.get('alternatenames', []):
#                 if ' ' in alt:
#                     continue
# 
#                 if not (3 <= len(alt) <= 30):
#                     continue
# 
#                 if not any(c.isalpha() for c in alt):
#                     continue
# 
#                 alt_clean = alt.replace('-', '').replace("'", "")
#                 if alt_clean.isalpha():
#                     is_latin_cyrillic = all(
#                         ('\u0000' <= c <= '\u007F') or  # ASCII (латиница)
#                         ('\u0400' <= c <= '\u04FF') or  # Кириллица
#                         c in ['-', "'"]
#                         for c in alt
#                     )
# 
#                     if is_latin_cyrillic:
#                         alt_lower = alt.lower().strip()
#                         if alt_lower not in all_cities_global:
#                             all_cities_global[alt_lower] = country
# 
#         print("✅ v5.6.0 TURBO: O(1) WORD BOUNDARY LOOKUP - Гео-Фильтрация инициализирована")
#         print(f"   ALL_CITIES_GLOBAL: {len(all_cities_global)} городов с привязкой к странам")
#         
#         from collections import Counter
#         country_stats = Counter(all_cities_global.values())
#         print(f"   Топ-5 стран: {dict(country_stats.most_common(5))}")
# 
#         return all_cities_global
# 
#     except ImportError:
#         print("⚠️ geonamescache не установлен, используется минимальный словарь")
#         
#         all_cities_global = {
#             'москва': 'ru', 'мск': 'ru', 'спб': 'ru', 'питер': 'ru', 
#             'санкт-петербург': 'ru', 'екатеринбург': 'ru', 'казань': 'ru',
#             'новосибирск': 'ru', 'челябинск': 'ru', 'омск': 'ru',
#             'минск': 'by', 'гомель': 'by', 'витебск': 'by', 'могилев': 'by',
#             'алматы': 'kz', 'астана': 'kz', 'караганда': 'kz',
#             'киев': 'ua', 'харьков': 'ua', 'одесса': 'ua', 'днепр': 'ua',
#             'львов': 'ua', 'запорожье': 'ua', 'кривой рог': 'ua',
#             'николаев': 'ua', 'винница': 'ua', 'херсон': 'ua',
#             'полтава': 'ua', 'чернигов': 'ua', 'черкассы': 'ua',
#             'днепропетровск': 'ua', 'kyiv': 'ua', 'kiev': 'ua',
#             'kharkiv': 'ua', 'odessa': 'ua', 'lviv': 'ua', 'dnipro': 'ua',
#         }
#         
#         return all_cities_global

ALL_CITIES_GLOBAL = generate_geo_blacklist_full()

# Базы для L0 классификатора — используем то что УЖЕ загружено на сервере
# GEO_DB: Dict[str, Set[str]] — город → множество ISO-кодов стран
# detect_geo() проверяет geo_db[word] чтобы узнать в какой стране город
GEO_DB = {}
for city_name, country_code in ALL_CITIES_GLOBAL.items():
    GEO_DB.setdefault(city_name, set()).add(country_code.upper())
for district_name, country_code in DISTRICTS_EXTENDED.items():
    GEO_DB.setdefault(district_name, set()).add(country_code.upper())
logger.info(f"[L0] GEO_DB: {len(GEO_DB)} записей (cities: {len(ALL_CITIES_GLOBAL)}, districts: {len(DISTRICTS_EXTENDED)})")

# BRAND_DB: грузим отдельно (маленькая, ~100 записей)
try:
    from databases import load_brands_db
    BRAND_DB = load_brands_db()
    logger.info(f"[L0] BRAND_DB: {len(BRAND_DB)} записей")
except ImportError:
    BRAND_DB = set()
    logger.warning("[L0] databases.py not found, BRAND_DB пуст")


def deduplicate_final_results(data: dict) -> dict:
    """
    Строгая дедупликация финального списка ключевых слов.
    Удаляет повторы, игнорируя регистр и лишние пробелы.
    """
    if not data or "keywords" not in data:
        return data

    seen = set()
    unique_keywords = []

    for item in data["keywords"]:
        # Проверяем, является ли item строкой или словарём
        if isinstance(item, str):
            raw_query = item
        elif isinstance(item, dict):
            raw_query = item.get("query", "")
        else:
            continue
            
        # Нормализация для сравнения: нижний регистр и удаление лишних пробелов
        norm_query = " ".join(raw_query.lower().split())
        
        if norm_query not in seen:
            seen.add(norm_query)
            unique_keywords.append(item)
    
    data["keywords"] = unique_keywords
    if "total_count" in data:
        data["total_count"] = len(unique_keywords)
    if "count" in data:
        data["count"] = len(unique_keywords)
    if "total_unique_keywords" in data:
        data["total_unique_keywords"] = len(unique_keywords)
    
    return data


class AdaptiveDelay:
    """Автоматическая оптимизация задержек между запросами"""

    def __init__(self, initial_delay: float = 0.2, min_delay: float = 0.1, max_delay: float = 1.0):
        self.delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay

    def get_delay(self) -> float:
        return self.delay

    def on_success(self):
        self.delay = max(self.min_delay, self.delay * 0.95)

    def on_rate_limit(self):
        self.delay = min(self.max_delay, self.delay * 1.5)

class GoogleAutocompleteParser:
    def __init__(self):
        self.adaptive_delay = AdaptiveDelay()


        self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
        self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk')
        
        if NATASHA_AVAILABLE:
            try:
                self.segmenter = Segmenter()
                self.morph_vocab = MorphVocab()
                self.emb = NewsEmbedding()
                self.ner_tagger = NewsNERTagger(self.emb)
                self.natasha_ready = True
                print("✅ Natasha NER initialized for geo-filtering")
            except Exception as e:
                print(f"⚠️ Natasha initialization failed: {e}")
                self.natasha_ready = False
        else:
            self.natasha_ready = False
        
        self.forbidden_geo = FORBIDDEN_GEO

        self.stemmers = {
            'en': SnowballStemmer("english"),
            'de': SnowballStemmer("german"),
            'fr': SnowballStemmer("french"),
            'es': SnowballStemmer("spanish"),
            'it': SnowballStemmer("italian"),
        }

        self.stop_words = {
            'ru': {'и', 'в', 'во', 'не', 'на', 'с', 'от', 'для', 'по', 'о', 'об', 'к', 'у', 'за', 
                   'из', 'со', 'до', 'при', 'без', 'над', 'под', 'а', 'но', 'да', 'или', 'чтобы', 
                   'что', 'как', 'где', 'когда', 'куда', 'откуда', 'почему'},
            'uk': {'і', 'в', 'на', 'з', 'від', 'для', 'по', 'о', 'до', 'при', 'без', 'над', 'під', 
                   'а', 'але', 'та', 'або', 'що', 'як', 'де', 'коли', 'куди', 'звідки', 'чому'},
            'en': {'a', 'an', 'the', 'in', 'on', 'at', 'to', 'for', 'o', 'with', 'by', 'from', 
                   'up', 'about', 'into', 'through', 'during', 'and', 'or', 'but', 'i', 'when', 
                   'where', 'how', 'why', 'what'},
            'de': {'der', 'die', 'das', 'den', 'dem', 'des', 'ein', 'eine', 'einen', 'einem', 
                   'und', 'oder', 'aber', 'in', 'au', 'von', 'zu', 'mit', 'für', 'bei', 'nach',
                   'wie', 'wo', 'wann', 'warum', 'was', 'wer'},
            'fr': {'le', 'la', 'les', 'un', 'une', 'des', 'de', 'du', 'et', 'ou', 'mais', 'dans',
                   'sur', 'avec', 'pour', 'par', 'à', 'en', 'au', 'aux', 'ce', 'qui', 'que',
                   'comment', 'où', 'quand', 'pourquoi', 'quoi'},
            'es': {'el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas', 'de', 'del', 'y', 'o',
                   'pero', 'en', 'con', 'por', 'para', 'a', 'al', 'como', 'que', 'quien',
                   'donde', 'cuando', 'porque', 'qué'},
            'it': {'il', 'lo', 'la', 'i', 'gli', 'le', 'un', 'uno', 'una', 'di', 'da', 'e', 'o',
                   'ma', 'in', 'su', 'con', 'per', 'a', 'come', 'che', 'chi', 'dove', 'quando',
                   'perché', 'cosa'},
            'pl': {'i', 'w', 'na', 'z', 'do', 'dla', 'po', 'o', 'przy', 'bez', 'nad', 'pod',
                   'a', 'ale', 'lub', 'czy', 'że', 'jak', 'gdzie', 'kiedy', 'dlaczego', 'co'}
        }
        
        # Исправлена критическая ошибка: раньше передавался пустой словарь {}
        self.post_filter = BatchPostFilter(
            all_cities_global=ALL_CITIES_GLOBAL,  # ✅ ИСПРАВЛЕНО: передаём загруженную базу
            forbidden_geo=self.forbidden_geo,
            districts=DISTRICTS_EXTENDED,
            population_threshold=5000
        )
        logger.info("✅ Batch Post-Filter v7.9 initialized with REAL cities database")
        logger.info(f"   Database contains {len(ALL_CITIES_GLOBAL)} cities")
        logger.info("   GEO DATABASE = PRIMARY, morphology = secondary")

        # Трассировщик фильтрации
        self.tracer = FilterTracer(enabled=True)
        
        # Флаг для отключения relevance_filter через ?filters=
        self.skip_relevance_filter = False

    def is_city_allowed(self, word: str, target_country: str) -> bool:
        """
        """
        try:
            parsed = self.morph_ru.parse(word.lower())[0]
            lemma = parsed.normal_form
        except:
            lemma = word.lower()
        
        if lemma not in ALL_CITIES_GLOBAL:
            return True
        
        city_country = ALL_CITIES_GLOBAL.get(lemma)  # получаем код страны (напр. 'ru', 'kz', 'ua')
        
        if city_country == target_country.lower():
            return True  # Город нашей страны — разрешаем
        
        return False  # Город чужой страны — блокируем
    
    def strip_geo_to_anchor(self, text: str, seed: str, target_country: str) -> str:
        """
        """
        import re
        
        seed_words = re.findall(r'[а-яёa-z0-9-]+', seed.lower())
        seed_lemmas = set()
        
        for word in seed_words:
            if len(word) < 2:
                continue
            try:
                if any(c in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя' for c in word):
                    lemma = self.morph_ru.parse(word)[0].normal_form
                    seed_lemmas.add(lemma)
                else:
                    seed_lemmas.add(word)  # Латиница как есть
            except:
                seed_lemmas.add(word)
        
        text_words = re.findall(r'[а-яёa-z0-9-]+', text.lower())
        
        # NEW: если в тексте есть город из той же страны, что и target_country,
        # НЕ превращаем этот текст в anchor вообще
        has_local_city = False
        for word in text_words:
            if len(word) < 3:
                continue
            city_country = ALL_CITIES_GLOBAL.get(word)
            if city_country and city_country == target_country.lower():
                has_local_city = True
                break

        if has_local_city:
            logger.info(
                f"ANCHOR BLOCKED (local city present): text='{text}' | seed='{seed}' | country={target_country}"
            )
            return ""
        
        remaining_words = []
        
        for word in text_words:
            if len(word) < 2:
                remaining_words.append(word)
                continue
            
            try:
                if any(c in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя' for c in word):
                    word_lemma = self.morph_ru.parse(word)[0].normal_form
                else:
                    word_lemma = word
            except:
                word_lemma = word
            
            if word_lemma in seed_lemmas:
                logger.info(f"🗑️ SEED REMOVED: '{word}' (lemma: {word_lemma}) from '{text}'")
                continue
            
            remaining_words.append(word)
        
        clean_words = []
        
        for word in remaining_words:
            if len(word) < 2:
                clean_words.append(word)
                continue
            
            try:
                if any(c in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя' for c in word):
                    lemma = self.morph_ru.parse(word)[0].normal_form
                else:
                    lemma = word
            except:
                lemma = word
            
            city_country_word = ALL_CITIES_GLOBAL.get(word)
            city_country_lemma = ALL_CITIES_GLOBAL.get(lemma)
            
            if city_country_word and city_country_word != target_country.lower():
                logger.info(f"🧼 CITY REMOVED: '{word}' (city of {city_country_word}) from anchor")
                continue
            
            if city_country_lemma and city_country_lemma != target_country.lower():
                logger.info(f"🧼 CITY REMOVED: '{word}' (lemma '{lemma}' city of {city_country_lemma}) from anchor")
                continue
            
            clean_words.append(word)
        
        anchor = " ".join(clean_words).strip()
        
        if anchor and anchor != text.lower():
            logger.warning(f"⚓ ANCHOR CREATED: '{text}' → '{anchor}'")
        
        return anchor

    def detect_seed_language(self, seed: str) -> str:
        """Автоопределение языка seed"""
        if any('\u0400' <= char <= '\u04FF' for char in seed):
            if any(char in 'іїєґ' for char in seed.lower()):
                return 'uk'
            return 'ru'
        return 'en'

    def get_modifiers(self, language: str, use_numbers: bool, seed: str, cyrillic_only: bool = False) -> List[str]:
        """Получить модификаторы для языка с умной фильтрацией"""
        modifiers = []

        seed_lower = seed.lower()
        has_cyrillic = any('\u0400' <= c <= '\u04FF' for c in seed_lower)
        has_latin = any('a' <= c <= 'z' for c in seed_lower)

        if language.lower() == 'ru':
            modifiers.extend(list("абвгдежзийклмнопрстуфхцчшщэюя"))
        elif language.lower() == 'uk':
            modifiers.extend(list("абвгдежзийклмнопрстуфхцчшщюяіїєґ"))

        if not cyrillic_only:
            if has_cyrillic and not has_latin and language.lower() not in ['en', 'de', 'fr', 'es', 'pl']:
                pass
            else:
                modifiers.extend(list("abcdefghijklmnopqrstuvwxyz"))

        if use_numbers:
            modifiers.extend([str(i) for i in range(10)])

        return modifiers

    def get_morphological_forms(self, word: str, language: str) -> List[str]:
        """Получить морфологические формы слова через pymorphy3"""
        forms = set([word])

        if language.lower() in ['ru', 'uk']:
            try:
                import pymorphy3
                morph = pymorphy3.MorphAnalyzer()
                parsed = morph.parse(word)

                if parsed:
                    for form in parsed[0].lexeme:
                        pos = form.tag.POS
                        if pos not in ['PRTS', 'PRTF', 'GRND']:
                            forms.add(form.word)
            except:
                pass
        return sorted(list(forms))

    def is_query_allowed(self, query: str, seed: str, country: str) -> bool:
        """
        v7.6: ПОЛНОСТЬЮ ОТКЛЮЧЕН - фильтрация теперь только через BatchPostFilter
        Всегда возвращает True
        
        Старая логика закомментирована ниже - можно вернуть если понадобится
        """
        return True
        
        # ============================================
        # Раскомментируй если нужно вернуть старую фильтрацию
        # ============================================
        # import re
        # 
        # q_lower = query.lower().strip()
        # target_country = country.lower()
        # 
        # for forbidden in self.forbidden_geo:
        #     if forbidden in q_lower:
        #         logger.warning(f"🚫 HARD-BLACKLIST: '{query}' contains '{forbidden}'")
        #         return False
        # 
        # words = re.findall(r'[а-яёa-z0-9-]+', q_lower)
        # lemmas = set()
        # 
        # for word in words:
        #     if len(word) < 3:
        #         lemmas.add(word)
        #         continue
        #     
        #     try:
        #         if any(c in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя' for c in word):
        #             lemma = self.morph_ru.parse(word)[0].normal_form
        #             lemmas.add(lemma)
        #         else:
        #             lemmas.add(word)
        #     except:
        #         lemmas.add(word)
        # 
        # for forbidden in self.forbidden_geo:
        #     if forbidden in lemmas:
        #         logger.warning(f"🚫 HARD-BLACKLIST (lemma): '{query}' → lemma '{forbidden}'")
        #         return False
        # 
        # stopwords = ['израиль', 'россия', 'казахстан', 'узбекистан', 'беларусь', 'молдова']
        # if any(stop in q_lower for stop in stopwords):
        #     if target_country == 'ua' and 'украина' not in q_lower:
        #         logger.warning(f"🚫 COUNTRY BLOCK: '{query}' contains {[s for s in stopwords if s in q_lower]}")
        #         return False
        # 
        # for word in words:
        #     if len(word) < 3:
        #         continue
        #     
        #     city_country_word = ALL_CITIES_GLOBAL.get(word)
        #     
        #     if city_country_word and city_country_word != target_country:
        #         logger.warning(f"🚫 FAST BLOCK: '{word}' ({city_country_word}) in '{query}'")
        #         return False
        # 
        # for lemma in lemmas:
        #     if len(lemma) < 3:
        #         continue
        #     
        #     city_country_lemma = ALL_CITIES_GLOBAL.get(lemma)
        #     
        #     if city_country_lemma and city_country_lemma != target_country:
        #         logger.warning(f"🚫 FAST BLOCK (lemma): '{lemma}' ({city_country_lemma}) in '{query}'")
        #         return False
        # 
        # if self.natasha_ready and NATASHA_AVAILABLE:
        #     try:
        #         from natasha import Doc
        #         
        #         doc = Doc(query)
        #         doc.segment(self.segmenter)
        #         doc.tag_ner(self.ner_tagger)
        #         
        #         for span in doc.spans:
        #             if span.type == 'LOC':
        #                 span.normalize(self.morph_vocab)
        #                 loc_name = span.normal.lower()
        #                 
        #                 if loc_name in ALL_CITIES_GLOBAL:
        #                     loc_country = ALL_CITIES_GLOBAL[loc_name]
        #                     if loc_country != target_country:
        #                         logger.warning(f"📍 NATASHA BLOCKED: '{loc_name}' ({loc_country}) in '{query}'")
        #                         return False
        #                 else:
        #                     loc_words = loc_name.split()
        #                     for loc_word in loc_words:
        #                         if len(loc_word) < 3:
        #                             continue
        #                         word_country = ALL_CITIES_GLOBAL.get(loc_word)
        #                         if word_country and word_country != target_country:
        #                             logger.warning(f"📍 NATASHA BLOCKED (word): '{loc_word}' ({word_country}) in '{loc_name}'")
        #                             return False
        #                 
        #     except Exception as e:
        #         logger.debug(f"Natasha NER error: {e}")
        # 
        # logger.info(f"✅ ALLOWED: {query}")
        # return True
    
    async def autocorrect_text(self, text: str, language: str) -> Dict:
        """Автокоррекция через Yandex Speller (ru/uk/en) или LanguageTool (остальные)"""

        if language.lower() in ['ru', 'uk', 'en']:
            url = "https://speller.yandex.net/services/spellservice.json/checkText"
            lang_map = {'ru': 'ru', 'uk': 'uk', 'en': 'en'}
            yandex_lang = lang_map.get(language.lower(), 'ru')

            params = {"text": text, "lang": yandex_lang, "options": 0}

            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.get(url, params=params)

                    if response.status_code == 200:
                        errors = response.json()

                        if not errors:
                            return {"original": text, "corrected": text, "corrections": [], "has_errors": False}

                        corrected = text
                        corrections = []
                        errors_sorted = sorted(errors, key=lambda x: x.get('pos', 0), reverse=True)

                        for error in errors_sorted:
                            word = error.get('word', '')
                            suggestions = error.get('s', [])

                            if suggestions:
                                suggestion = suggestions[0]
                                pos = error.get('pos', 0)
                                corrected = corrected[:pos] + suggestion + corrected[pos + len(word):]
                                corrections.append({"word": word, "suggestion": suggestion})

                        return {
                            "original": text,
                            "corrected": corrected,
                            "corrections": corrections,
                            "has_errors": True
                        }
            except:
                pass
        return await self.autocorrect_languagetool(text, language)

    async def autocorrect_languagetool(self, text: str, language: str) -> Dict:
        """Автокоррекция через LanguageTool API (30+ языков)"""
        url = "https://api.languagetool.org/v2/check"

        data = {
            "text": text,
            "language": language.lower(),
            "enabledOnly": "false"
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, data=data)

                if response.status_code == 200:
                    result = response.json()
                    matches = result.get('matches', [])

                    if not matches:
                        return {"original": text, "corrected": text, "corrections": [], "has_errors": False}

                    corrected = text
                    corrections = []

                    for match in reversed(matches):
                        offset = match.get('offset', 0)
                        length = match.get('length', 0)
                        replacements = match.get('replacements', [])

                        if replacements:
                            suggestion = replacements[0].get('value', '')
                            word = text[offset:offset+length]
                            corrected = corrected[:offset] + suggestion + corrected[offset+length:]
                            corrections.append({"word": word, "suggestion": suggestion})

                    return {
                        "original": text,
                        "corrected": corrected,
                        "corrections": corrections,
                        "has_errors": True
                    }
        except:
            pass
        return {"original": text, "corrected": text, "corrections": [], "has_errors": False}

    async def fetch_suggestions(self, query: str, country: str, language: str, client: httpx.AsyncClient) -> List[str]:
        """Google Autocomplete"""
        url = "https://www.google.com/complete/search"
        params = {"q": query, "client": "firefox", "hl": language, "gl": country}
        headers = {"User-Agent": random.choice(USER_AGENTS)}

        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0)

            if response.status_code == 429:
                self.adaptive_delay.on_rate_limit()
                return []

            self.adaptive_delay.on_success()

            if response.status_code == 200:
                data = response.json()
                return data[1] if len(data) > 1 else []
        except:
            pass
        return []

    async def fetch_suggestions_yandex(self, query: str, language: str, region_id: int, client: httpx.AsyncClient) -> List[str]:
        """Yandex Suggest"""
        url = "https://suggest-maps.yandex.ru/suggest-geo"

        params = {
            "v": "9",
            "search_type": "tp",
            "part": query,
            "lang": language,
            "n": "10",
            "geo": str(region_id),
            "fullpath": "1"
        }

        headers = {"User-Agent": random.choice(USER_AGENTS)}

        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0)

            if response.status_code == 429:
                self.adaptive_delay.on_rate_limit()
                return []

            self.adaptive_delay.on_success()

            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                return [item.get('text', '') for item in results if item.get('text')]
        except:
            pass
        return []

    async def fetch_suggestions_bing(self, query: str, language: str, country: str, client: httpx.AsyncClient) -> List[str]:
        """Bing Autosuggest"""
        url = "https://www.bing.com/AS/Suggestions"

        params = {
            "q": query,
            "mkt": f"{language}-{country}",
            "cvid": "0",
            "qry": query
        }

        headers = {"User-Agent": random.choice(USER_AGENTS)}

        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0)

            if response.status_code == 429:
                self.adaptive_delay.on_rate_limit()
                return []

            self.adaptive_delay.on_success()

            if response.status_code == 200:
                data = response.json()
                suggestion_groups = data.get('AS', {}).get('Results', [])

                suggestions = []
                for group in suggestion_groups:
                    for item in group.get('Suggests', []):
                        text = item.get('Txt', '')
                        if text:
                            suggestions.append(text)

                return suggestions
        except:
            pass
        return []

    async def parse_with_semaphore(self, queries: List[str], country: str, language: str, 
                                   parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """Парсинг с ограничением параллельности и выбором источника"""

        semaphore = asyncio.Semaphore(parallel_limit)
        all_keywords = set()
        success_count = 0
        failed_count = 0

        async def fetch_with_limit(query: str, client: httpx.AsyncClient):
            nonlocal success_count, failed_count

            async with semaphore:
                await asyncio.sleep(self.adaptive_delay.get_delay())

                if source == "google":
                    results = await self.fetch_suggestions(query, country, language, client)
                elif source == "yandex":
                    results = await self.fetch_suggestions_yandex(query, language, region_id, client)
                elif source == "bing":
                    results = await self.fetch_suggestions_bing(query, language, country, client)
                else:
                    results = []

                if results:
                    all_keywords.update(results)
                    success_count += 1
                else:
                    failed_count += 1

                return results

        async with httpx.AsyncClient() as client:
            tasks = [fetch_with_limit(q, client) for q in queries]
            await asyncio.gather(*tasks)

        return {
            "keywords": sorted(list(all_keywords)),
            "success": success_count,
            "failed": failed_count
        }

    async def parse_suffix(self, seed: str, country: str, language: str, use_numbers: bool, 
                          parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """SUFFIX метод: seed + модификатор"""
        start_time = time.time()

        modifiers = self.get_modifiers(language, use_numbers, seed)
        queries = [f"{seed} {mod}" for mod in modifiers]

        result_raw = await self.parse_with_semaphore(queries, country, language, parallel_limit, source, region_id)

        keywords = set()
        internal_anchors = set()
        
        for kw in result_raw['keywords']:
            if not self.is_query_allowed(kw, seed, country):
                anchor = self.strip_geo_to_anchor(kw, seed, country)
                logger.debug(
                    f"[SUFFIX] BLOCK by is_query_allowed | kw='{kw}' | anchor='{anchor}' "
                    f"| seed='{seed}' | country={country}"
                )
                if anchor and anchor != seed.lower() and len(anchor) > 5:
                    internal_anchors.add(anchor)
                continue  # НЕ добавляем мусор в keywords
            
            keywords.add(kw)
        
        all_with_anchors = keywords | internal_anchors
        if not self.skip_relevance_filter:
            filtered = await filter_relevant_keywords(list(all_with_anchors), seed, language)
        else:
            filtered = list(all_with_anchors)
        
        filtered_set = set(filtered)
        final_keywords = sorted(list(keywords & filtered_set))
        final_anchors = sorted(list(internal_anchors & filtered_set))
        
        logger.info(
            f"[SUFFIX] BEFORE BPF | seed='{seed}' | country={country} | "
            f"final_keywords={len(final_keywords)} | final_anchors={len(final_anchors)}"
        )
        logger.debug(f"[SUFFIX] final_keywords={final_keywords}")
        logger.debug(f"[SUFFIX] final_anchors={final_anchors}")
        
        # === BPF ПЕРЕНЕСЁН В apply_filters_traced (endpoint) ===
        # batch_result = self.post_filter.filter_batch(
        #     keywords=final_keywords,
        #     seed=seed,
        #     country=country,
        #     language=language
        # )
        # combined_anchors = set(final_anchors) | set(batch_result['anchors'])

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "suffix",
            "source": source,
            "keywords": final_keywords,
            "anchors": sorted(list(final_anchors)),
            "count": len(final_keywords),
            "anchors_count": len(final_anchors),
            "queries": len(queries),
            "elapsed_time": round(elapsed, 2),
            "batch_stats": {}
        }

    async def parse_infix(self, seed: str, country: str, language: str, use_numbers: bool, 
                         parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """INFIX метод: вставка модификаторов между словами"""
        start_time = time.time()

        words = seed.strip().split()

        if len(words) < 2:
            return {"error": "INFIX требует минимум 2 слова", "seed": seed}

        modifiers = self.get_modifiers(language, use_numbers, seed, cyrillic_only=True)
        queries = []

        for i in range(1, len(words)):
            for mod in modifiers:
                query = ' '.join(words[:i]) + f' {mod} ' + ' '.join(words[i:])
                queries.append(query)

        result_raw = await self.parse_with_semaphore(queries, country, language, parallel_limit, source, region_id)

        keywords = set()
        internal_anchors = set()
        
        for kw in result_raw['keywords']:
            if not self.is_query_allowed(kw, seed, country):
                anchor = self.strip_geo_to_anchor(kw, seed, country)
                logger.debug(
                    f"[INFIX] BLOCK by is_query_allowed | kw='{kw}' | anchor='{anchor}' "
                    f"| seed='{seed}' | country={country}"
                )
                if anchor and anchor != seed.lower() and len(anchor) > 5:
                    internal_anchors.add(anchor)
                continue  # НЕ добавляем мусор в keywords
            
            keywords.add(kw)
        
        all_with_anchors = keywords | internal_anchors
        filtered_1 = await filter_infix_results(list(all_with_anchors), language)

        if not self.skip_relevance_filter:
            filtered_2 = await filter_relevant_keywords(filtered_1, seed, language)
        else:
            filtered_2 = filtered_1
        
        filtered_set = set(filtered_2)
        final_keywords = sorted(list(keywords & filtered_set))
        final_anchors = sorted(list(internal_anchors & filtered_set))
        
        logger.info(
            f"[INFIX] BEFORE BPF | seed='{seed}' | country={country} | "
            f"final_keywords={len(final_keywords)} | final_anchors={len(final_anchors)}"
        )
        logger.debug(f"[INFIX] final_keywords={final_keywords}")
        logger.debug(f"[INFIX] final_anchors={final_anchors}")
        
        # === BPF ПЕРЕНЕСЁН В apply_filters_traced (endpoint) ===
        # batch_result = self.post_filter.filter_batch(
        #     keywords=final_keywords,
        #     seed=seed,
        #     country=country,
        #     language=language
        # )
        # combined_anchors = set(final_anchors) | set(batch_result['anchors'])

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "infix",
            "source": source,
            "keywords": final_keywords,
            "anchors": sorted(list(final_anchors)),
            "count": len(final_keywords),
            "anchors_count": len(final_anchors),
            "queries": len(queries),
            "elapsed_time": round(elapsed, 2),
            "batch_stats": {}
        }

    async def parse_morphology(self, seed: str, country: str, language: str, use_numbers: bool, 
                               parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """MORPHOLOGY метод: модификация форм существительных"""
        start_time = time.time()

        words = seed.strip().split()

        nouns_to_modify = []

        if language.lower() in ['ru', 'uk']:
            try:
                import pymorphy3
                morph = pymorphy3.MorphAnalyzer()
                for idx, word in enumerate(words):
                    parsed = morph.parse(word)
                    if parsed and parsed[0].tag.POS == 'NOUN':
                        nouns_to_modify.append({
                            'index': idx,
                            'word': word,
                            'forms': self.get_morphological_forms(word, language)
                        })

                if not nouns_to_modify:
                    last_word = words[-1]
                    nouns_to_modify.append({
                        'index': len(words) - 1,
                        'word': last_word,
                        'forms': self.get_morphological_forms(last_word, language)
                    })
            except:
                last_word = words[-1]
                nouns_to_modify.append({
                    'index': len(words) - 1,
                    'word': last_word,
                    'forms': self.get_morphological_forms(last_word, language)
                })
        else:
            last_word = words[-1]
            nouns_to_modify.append({
                'index': len(words) - 1,
                'word': last_word,
                'forms': self.get_morphological_forms(last_word, language)
            })

        all_seeds = []
        if len(nouns_to_modify) >= 1:
            noun = nouns_to_modify[0]
            for form in noun['forms']:
                new_words = words.copy()
                new_words[noun['index']] = form
                all_seeds.append(' '.join(new_words))

        unique_seeds = list(set(all_seeds))

        all_keywords = set()
        modifiers = self.get_modifiers(language, use_numbers, seed)

        for seed_variant in unique_seeds:
            queries = [f"{seed_variant} {mod}" for mod in modifiers]
            result = await self.parse_with_semaphore(queries, country, language, parallel_limit, source, region_id)
            all_keywords.update(result['keywords'])

        keywords = set()
        internal_anchors = set()
        
        for kw in all_keywords:
            if not self.is_query_allowed(kw, seed, country):
                anchor = self.strip_geo_to_anchor(kw, seed, country)
                logger.debug(
                    f"[MORPH] BLOCK by is_query_allowed | kw='{kw}' | anchor='{anchor}' "
                    f"| seed='{seed}' | country={country}"
                )
                if anchor and anchor != seed.lower() and len(anchor) > 5:
                    internal_anchors.add(anchor)
                continue  # НЕ добавляем мусор в keywords
            
            keywords.add(kw)
        
        all_with_anchors = keywords | internal_anchors
        if not self.skip_relevance_filter:
            filtered = await filter_relevant_keywords(sorted(list(all_with_anchors)), seed, language)
        else:
            filtered = sorted(list(all_with_anchors))
        
        filtered_set = set(filtered)
        final_keywords = sorted(list(keywords & filtered_set))
        final_anchors = sorted(list(internal_anchors & filtered_set))
        
        logger.info(
            f"[MORPH] BEFORE BPF | seed='{seed}' | country={country} | "
            f"final_keywords={len(final_keywords)} | final_anchors={len(final_anchors)}"
        )
        logger.debug(f"[MORPH] final_keywords={final_keywords}")
        logger.debug(f"[MORPH] final_anchors={final_anchors}")
        
        # === BPF ПЕРЕНЕСЁН В apply_filters_traced (endpoint) ===
        # batch_result = self.post_filter.filter_batch(
        #     keywords=final_keywords,
        #     seed=seed,
        #     country=country,
        #     language=language
        # )
        # combined_anchors = set(final_anchors) | set(batch_result['anchors'])

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "morphology",
            "source": source,
            "keywords": final_keywords,
            "anchors": sorted(list(final_anchors)),
            "count": len(final_keywords),
            "anchors_count": len(final_anchors),
            "elapsed_time": round(elapsed, 2),
            "batch_stats": {}
        }

    async def parse_light_search(self, seed: str, country: str, language: str, use_numbers: bool, 
                                 parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """LIGHT SEARCH: быстрый поиск (SUFFIX + INFIX)"""
        start_time = time.time()

        suffix_result = await self.parse_suffix(seed, country, language, use_numbers, parallel_limit, source, region_id)
        infix_result = await self.parse_infix(seed, country, language, use_numbers, parallel_limit, source, region_id)

        all_keywords = set(suffix_result["keywords"]) | set(infix_result.get("keywords", []))
        all_anchors = set(suffix_result.get("anchors", [])) | set(infix_result.get("anchors", []))

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "light_search",
            "source": source,
            "keywords": sorted(list(all_keywords)),
            "anchors": sorted(list(all_anchors)),
            "count": len(all_keywords),
            "anchors_count": len(all_anchors),
            "suffix_count": len(suffix_result["keywords"]),
            "infix_count": len(infix_result.get("keywords", [])),
            "elapsed_time": round(elapsed, 2)
        }

    async def parse_adaptive_prefix(self, seed: str, country: str, language: str, use_numbers: bool, 
                                    parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """ADAPTIVE PREFIX метод: извлечение слов из SUFFIX + PREFIX проверка"""
        start_time = time.time()

        seed_words = set(seed.lower().split())

        prefixes = ["", "купить", "цена", "отзывы"]
        queries = []
        for p in prefixes:
            q = f"{p} {seed}".strip()
            if self.is_query_allowed(q, seed, country):
                queries.append(q)
        
        alphabet = self.get_modifiers(language, use_numbers, seed, cyrillic_only=True)
        for char in alphabet:
            q_ext = f"{seed} {char}".strip()
            if self.is_query_allowed(q_ext, seed, country):
                queries.append(q_ext)

        result_raw = await self.parse_with_semaphore(queries, country, language, parallel_limit, source, region_id)

        from collections import Counter
        word_counter = Counter()

        for result in result_raw['keywords']:
            result_words = result.lower().split()
            for word in result_words:
                if word not in seed_words and len(word) > 2:
                    word_counter[word] += 1

        candidates = {w for w, count in word_counter.items() if count >= 2}

        keywords = set()
        internal_anchors = set()
        verified_prefixes = []

        for candidate in sorted(candidates):
            query = f"{candidate} {seed}"

            if not self.is_query_allowed(query, seed, country):
                continue

            result = await self.parse_with_semaphore([query], country, language, parallel_limit, source, region_id)
            
            if result['keywords']:
                verified_prefixes.append(candidate)
                
                for kw in result['keywords']:
                    if not self.is_query_allowed(kw, seed, country):
                        anchor = self.strip_geo_to_anchor(kw, seed, country)
                        logger.debug(
                            f"[ADAPTIVE_PREFIX] BLOCK by is_query_allowed | kw='{kw}' | anchor='{anchor}' "
                            f"| seed='{seed}' | country={country}"
                        )
                        if anchor and anchor != seed.lower() and len(anchor) > 5:
                            internal_anchors.add(anchor)
                        continue  # НЕ добавляем мусор в keywords
                    
                    keywords.add(kw)
        
        all_with_anchors = keywords | internal_anchors
        if not self.skip_relevance_filter:
            filtered = await filter_relevant_keywords(sorted(list(all_with_anchors)), seed, language)
        else:
            filtered = sorted(list(all_with_anchors))
        
        filtered_set = set(filtered)
        final_keywords = sorted(list(keywords & filtered_set))
        final_anchors = sorted(list(internal_anchors & filtered_set))
        
        logger.info(
            f"[ADAPTIVE_PREFIX] BEFORE BPF | seed='{seed}' | country={country} | "
            f"final_keywords={len(final_keywords)} | final_anchors={len(final_anchors)}"
        )
        logger.debug(f"[ADAPTIVE_PREFIX] final_keywords={final_keywords}")
        logger.debug(f"[ADAPTIVE_PREFIX] final_anchors={final_anchors}")
        
        # === BPF ПЕРЕНЕСЁН В apply_filters_traced (endpoint) ===
        # batch_result = self.post_filter.filter_batch(
        #     keywords=final_keywords,
        #     seed=seed,
        #     country=country,
        #     language=language
        # )
        # combined_anchors = set(final_anchors) | set(batch_result['anchors'])

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "adaptive_prefix",
            "source": source,
            "keywords": final_keywords,
            "anchors": sorted(list(final_anchors)),
            "count": len(final_keywords),
            "anchors_count": len(final_anchors),
            "candidates_found": len(candidates),
            "verified_prefixes": verified_prefixes,
            "elapsed_time": round(elapsed, 2),
            "batch_stats": {}
        }

    async def parse_deep_search(self, seed: str, country: str, region_id: int, language: str, 
                                use_numbers: bool, parallel_limit: int, include_keywords: bool) -> Dict:
        """DEEP SEARCH: глубокий поиск (все 4 метода ИЗ ВСЕХ 3 ИСТОЧНИКОВ)"""

        correction = await self.autocorrect_text(seed, language)
        original_seed = seed

        if correction.get("has_errors"):
            seed = correction["corrected"]

        start_time = time.time()
        
        sources = ["google", "yandex", "bing"]
        all_keywords_by_source = {}
        all_anchors_by_source = {}
        
        for source in sources:
            suffix_result = await self.parse_suffix(seed, country, language, use_numbers, parallel_limit, source, region_id)
            infix_result = await self.parse_infix(seed, country, language, use_numbers, parallel_limit, source, region_id)
            morph_result = await self.parse_morphology(seed, country, language, use_numbers, parallel_limit, source, region_id)
            prefix_result = await self.parse_adaptive_prefix(seed, country, language, use_numbers, parallel_limit, source, region_id)
            
            all_keywords_by_source[source] = {
                "suffix": set(suffix_result["keywords"]),
                "infix": set(infix_result.get("keywords", [])),
                "morphology": set(morph_result["keywords"]),
                "adaptive_prefix": set(prefix_result["keywords"])
            }
            
            all_anchors_by_source[source] = {
                "suffix": set(suffix_result.get("anchors", [])),
                "infix": set(infix_result.get("anchors", [])),
                "morphology": set(morph_result.get("anchors", [])),
                "adaptive_prefix": set(prefix_result.get("anchors", []))
            }
        
        all_unique_keywords = set()
        all_unique_anchors = set()
        
        for source in sources:
            for method_kw in all_keywords_by_source[source].values():
                all_unique_keywords |= method_kw
            for method_anchors in all_anchors_by_source[source].values():
                all_unique_anchors |= method_anchors
        
        logger.info(f"[Deep Search] Before final filter: {len(all_unique_keywords)} keywords")
        
        # === BPF ПЕРЕНЕСЁН В apply_filters_traced (endpoint) ===
        # final_filter = self.post_filter.filter_batch(
        #     keywords=list(all_unique_keywords),
        #     seed=seed,
        #     country=country,
        #     language=language
        # )
        # all_unique_keywords = set(final_filter['keywords'])
        # all_unique_anchors = set(final_filter['anchors']) | all_unique_anchors
        
        logger.info(f"[Deep Search] After merge: {len(all_unique_keywords)} keywords, {len(all_unique_anchors)} anchors")

        final_keywords = sorted(list(all_unique_keywords))
        normalized_keywords = normalize_keywords(final_keywords, language, seed)

        elapsed = time.time() - start_time

        response = {
            "seed": original_seed,
            "corrected_seed": seed if correction.get("has_errors") else None,
            "corrections": correction.get("corrections", []) if correction.get("has_errors") else [],
            "keywords": normalized_keywords,
            "anchors": sorted(list(all_unique_anchors)),
            "count": len(normalized_keywords),
            "anchors_count": len(all_unique_anchors),
            "sources": sources,
            "total_unique_keywords": len(normalized_keywords),
            "total_anchors": len(all_unique_anchors),
            "results_by_source": {
                source: {
                    "count": sum(len(kw) for kw in all_keywords_by_source[source].values())
                }
                for source in sources
            },
            "sources_stats": {
                source: {
                    "keywords": sum(len(kw) for kw in all_keywords_by_source[source].values()),
                    "anchors": sum(len(anch) for anch in all_anchors_by_source[source].values())
                }
                for source in sources
            },
            "elapsed_time": round(elapsed, 2)
        }

        if include_keywords:
            response["keywords_detailed"] = {
                **{
                    source: {
                        "all": sorted(list(set.union(*all_keywords_by_source[source].values()))),
                        "suffix": sorted(list(all_keywords_by_source[source]["suffix"])),
                        "infix": sorted(list(all_keywords_by_source[source]["infix"])),
                        "morphology": sorted(list(all_keywords_by_source[source]["morphology"])),
                        "adaptive_prefix": sorted(list(all_keywords_by_source[source]["adaptive_prefix"]))
                    }
                    for source in sources
                }
            }
            response["anchors_detailed"] = {
                **{
                    source: {
                        "all": sorted(list(set.union(*all_anchors_by_source[source].values()))),
                        "suffix": sorted(list(all_anchors_by_source[source]["suffix"])),
                        "infix": sorted(list(all_anchors_by_source[source]["infix"])),
                        "morphology": sorted(list(all_anchors_by_source[source]["morphology"])),
                        "adaptive_prefix": sorted(list(all_anchors_by_source[source]["adaptive_prefix"]))
                    }
                    for source in sources
                }
            }

        return response

parser = GoogleAutocompleteParser()

def apply_smart_fix(result: dict, seed: str, language: str):
    """
    Финальная нормализация результатов
    
    УЛУЧШЕНИЯ:
    - Лемматизация seed перед нормализацией (golden base)
    - Удаление дубликатов через dict.fromkeys
    """
    if result.get("keywords"):
        raw_keywords = result["keywords"]
        
        # Лемматизируем seed для создания golden base
        golden_seed = seed
        if language in ['ru', 'uk']:
            try:
                import pymorphy3
                morph = pymorphy3.MorphAnalyzer(lang=language)
                lemmatized_words = []
                for word in seed.split():
                    parsed = morph.parse(word)
                    if parsed:
                        lemmatized_words.append(parsed[0].normal_form)
                    else:
                        lemmatized_words.append(word)
                golden_seed = " ".join(lemmatized_words)
            except:
                golden_seed = seed
        
        # Нормализация с golden base
        norm_keywords = normalize_keywords(raw_keywords, language, golden_seed)
        
        # Убираем дубликаты (сохраняя порядок)
        result["keywords"] = list(dict.fromkeys(norm_keywords))
        
        total = len(result["keywords"])
        if "count" in result: result["count"] = total
        if "total_count" in result: result["total_count"] = total
        if "total_unique_keywords" in result: result["total_unique_keywords"] = total
            
    return result

@app.get("/")
async def root():
    """Главная страница"""
    return FileResponse('static/index.html')


def _build_l2_config(pmi_valid=None, centroid_valid=None, centroid_trash=None):
    """Собирает L2 config из query параметров (None = использовать дефолт)."""
    from filters.l2_filter import L2Config
    
    config = L2Config()
    
    if pmi_valid is not None:
        config.pmi_valid_threshold = pmi_valid
    if centroid_valid is not None:
        config.centroid_valid_threshold = centroid_valid
    if centroid_trash is not None:
        config.centroid_trash_threshold = centroid_trash
    
    return config


def apply_filters_traced(result: dict, seed: str, country: str, 
                          method: str, language: str = "ru", deduplicate: bool = False,
                          enabled_filters: str = "pre,geo,bpf", l2_config = None) -> dict:
    """
    Применяет цепочку фильтров с трассировкой.
    Порядок: pre_filter → geo_garbage → BPF → deduplicate → L0 → L2
    Заблокированные ключи добавляются в result["anchors"] с указанием фильтра.
    
    enabled_filters: через запятую какие фильтры включены.
        "pre"  = pre_filter
        "geo"  = geo_garbage_filter  
        "bpf"  = batch_post_filter
        "l0"   = L0 tail classifier
        "l2"   = L2 Tri-Signal classifier (PMI + Centroid + L0 signals)
        "none" = все выключены (сырые данные)
        "all" или "pre,geo,bpf,l0,l2" = все включены (по умолчанию)
    """
    # Парсим флаги
    ef = enabled_filters.lower().strip()
    if ef == "all":
        ef = "pre,geo,bpf,l0,l2"
    parts = [x.strip() for x in ef.split(",")]
    run_pre = "pre" in parts
    run_geo = "geo" in parts
    run_bpf = "bpf" in parts
    run_l0 = "l0" in parts
    run_l2 = "l2" in parts
    
    logger.info(f"[FILTERS] enabled_filters='{enabled_filters}' → pre={run_pre} geo={run_geo} bpf={run_bpf} l0={run_l0} l2={run_l2}")
    
    parser.tracer.start_request(seed=seed, country=country, method=method)
    
    if "anchors" not in result:
        result["anchors"] = []
    
    before_set = set(k.lower().strip() if isinstance(k, str) else k.get("query","").lower().strip() for k in result.get("keywords", []))
    
    # PRE-ФИЛЬТР
    if run_pre:
        parser.tracer.before_filter("pre_filter", result.get("keywords", []))
        result = apply_pre_filter(result, seed=seed)
        parser.tracer.after_filter("pre_filter", result.get("keywords", []))
        
        after_set = set(k.lower().strip() if isinstance(k, str) else k.get("query","").lower().strip() for k in result.get("keywords", []))
        for kw in (before_set - after_set):
            result["anchors"].append(kw)
        before_set = after_set
    
    # ГЕО-ФИЛЬТР
    if run_geo:
        parser.tracer.before_filter("geo_garbage_filter", result.get("keywords", []))
        result = filter_geo_garbage(result, seed=seed, target_country=country)
        parser.tracer.after_filter("geo_garbage_filter", result.get("keywords", []))
        
        after_set = set(k.lower().strip() if isinstance(k, str) else k.get("query","").lower().strip() for k in result.get("keywords", []))
        for kw in (before_set - after_set):
            result["anchors"].append(kw)
        before_set = after_set
    
    # BATCH POST-FILTER
    if run_bpf:
        parser.tracer.before_filter("batch_post_filter", result.get("keywords", []))
        bpf_result = parser.post_filter.filter_batch(
            keywords=result.get("keywords", []),
            seed=seed,
            country=country,
            language=language
        )
        result["keywords"] = bpf_result["keywords"]
        parser.tracer.after_filter("batch_post_filter", result.get("keywords", []))
        
        after_set = set(k.lower().strip() if isinstance(k, str) else k.get("query","").lower().strip() for k in result.get("keywords", []))
        for kw in (before_set - after_set):
            result["anchors"].append(kw)
    
    # ДЕДУПЛИКАЦИЯ (опционально)
    if deduplicate:
        parser.tracer.before_filter("deduplicate", result.get("keywords", []))
        result = deduplicate_final_results(result)
        parser.tracer.after_filter("deduplicate", result.get("keywords", []))
    
    # L0 КЛАССИФИКАТОР (последний в цепочке)
    if run_l0:
        parser.tracer.before_filter("l0_filter", result.get("keywords", []))
        
        result = apply_l0_filter(
            result,
            seed=seed,
            target_country=country,
            geo_db=GEO_DB,
            brand_db=BRAND_DB,
        )
        
        # Трейсер L0 — три исхода
        l0_trace = result.get("_l0_trace", [])
        l0_trash = [r["keyword"] for r in l0_trace if r.get("label") == "TRASH"]
        
        parser.tracer.after_l0_filter(
            valid=result.get("keywords", []),
            trash=l0_trash,
            grey=result.get("keywords_grey", []),
            l0_trace=l0_trace,
        )
    
    # L2 СЕМАНТИЧЕСКИЙ КЛАССИФИКАТОР (после L0, обрабатывает GREY)
    if run_l2 and result.get("keywords_grey"):
        parser.tracer.before_filter("l2_filter", result.get("keywords_grey", []))
        
        result = apply_l2_filter(
            result,
            seed=seed,
            enable_l2=True,
            config=l2_config,
        )
        
        # Логируем результаты L2
        l2_stats = result.get("l2_stats", {})
        l2_trace = result.get("_l2_trace", [])
        
        logger.info(
            f"[L2] VALID: {l2_stats.get('l2_valid', 0)}, "
            f"TRASH: {l2_stats.get('l2_trash', 0)}, "
            f"GREY remaining: {l2_stats.get('l2_grey', 0)} "
            f"({l2_stats.get('reduction_pct', 0)}% reduction)"
        )
        
        # Трейсер L2 — три исхода
        parser.tracer.after_l2_filter(
            valid=result.get("keywords", []),
            trash=[a for a in result.get("anchors", []) if isinstance(a, dict) and a.get("anchor_reason") == "L2_TRASH"],
            grey=result.get("keywords_grey", []),
            l2_stats=l2_stats,
            l2_trace=l2_trace,
        )
    
    # Дедупликация anchors
    seen = set()
    unique_anchors = []
    for a in result["anchors"]:
        if isinstance(a, str):
            key = a.lower().strip()
        elif isinstance(a, dict):
            key = (a.get("keyword") or a.get("query") or "").lower().strip()
        else:
            key = ""
        if key and key not in seen:
            seen.add(key)
            unique_anchors.append(a)
    result["anchors"] = unique_anchors  # НЕ сортируем — dict'ы не сортируются
    result["anchors_count"] = len(unique_anchors)
    
    result["_trace"] = parser.tracer.finish_request()
    result["_filters_enabled"] = {"pre": run_pre, "geo": run_geo, "bpf": run_bpf, "l0": run_l0, "l2": run_l2, "rel": not parser.skip_relevance_filter}
    return result


@app.get("/api/trace/last")
async def get_last_trace():
    """Возвращает последний отчёт трассировки"""
    return parser.tracer.finish_request() if parser.tracer.stages else {"message": "No trace available"}


@app.get("/api/trace/keyword")
async def trace_keyword(keyword: str = Query(..., description="Ключевое слово для трассировки")):
    """Трассировка конкретного ключевого слова через все фильтры"""
    return parser.tracer.get_keyword_trace(keyword)


@app.get("/api/trace/toggle")
async def toggle_tracer(enabled: bool = Query(True, description="Включить/выключить трассировку")):
    """Включение/выключение трассировки"""
    parser.tracer.enabled = enabled
    return {"tracer_enabled": enabled}


@app.get("/debug/l2-diag")
async def l2_diagnostic():
    """Возвращает L2 diagnostic dump (centroid distances, PMI, decisions)."""
    import json as _json
    try:
        with open("l2_diagnostic.json", "r", encoding="utf-8") as f:
            return _json.load(f)
    except FileNotFoundError:
        return {"error": "l2_diagnostic.json not found — run a search with L2 enabled first"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/l0-trace")
async def l0_trace_endpoint(
    label: str = Query("all", description="Фильтр: all / valid / trash / grey / no_seed"),
    tail: str = Query(None, description="Поиск по tail (подстрока)"),
    keyword: str = Query(None, description="Поиск по keyword (подстрока)"),
):
    """
    Возвращает L0 diagnostic trace — tail extraction + detector signals для каждого ключа.
    
    Примеры:
        /debug/l0-trace                     — все ключи
        /debug/l0-trace?label=trash         — только TRASH
        /debug/l0-trace?tail=бу             — ключи с tail содержащим "бу"
        /debug/l0-trace?keyword=авито       — ключи с keyword содержащим "авито"
        /debug/l0-trace?label=no_seed       — ключи где seed не найден
    """
    import json as _json
    try:
        with open("l0_diagnostic.json", "r", encoding="utf-8") as f:
            diag = _json.load(f)
    except FileNotFoundError:
        return {"error": "l0_diagnostic.json not found — run a search with L0 enabled first"}
    except Exception as e:
        return {"error": str(e)}
    
    traces = diag.get("trace", [])
    
    # Фильтрация по label
    if label != "all":
        if label == "no_seed":
            traces = [t for t in traces if t.get("tail") is None]
        else:
            traces = [t for t in traces if t.get("label", "").lower() == label.lower()]
    
    # Фильтрация по tail подстроке
    if tail:
        tail_lower = tail.lower()
        traces = [t for t in traces if t.get("tail") and tail_lower in t["tail"].lower()]
    
    # Фильтрация по keyword подстроке
    if keyword:
        kw_lower = keyword.lower()
        traces = [t for t in traces if kw_lower in t.get("keyword", "").lower()]
    
    return {
        "seed": diag.get("seed"),
        "target_country": diag.get("target_country"),
        "stats": diag.get("stats"),
        "filter": {"label": label, "tail": tail, "keyword": keyword},
        "filtered_count": len(traces),
        "trace": traces,
    }


@app.get("/api/light-search")
async def light_search_endpoint(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны"),
    region_id: int = Query(143, description="ID региона для Yandex"),
    language: str = Query("auto", description="Язык"),
    use_numbers: bool = Query(False, description="Добавить цифры"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    source: str = Query("google", description="Источник: google/yandex/bing"),
    filters: str = Query("all", description="Фильтры: all / none / pre,geo,bpf,rel,l0,l2"),
    # L2 пороги (Tri-Signal)
    l2_pmi_valid: float = Query(None, description="L2: PMI VALID threshold"),
    l2_centroid_valid: float = Query(None, description="L2: Centroid VALID threshold"),
    l2_centroid_trash: float = Query(None, description="L2: Centroid TRASH threshold"),
):
    """LIGHT SEARCH: быстрый поиск (SUFFIX + INFIX)"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    # Управление relevance_filter (работает внутри parse-методов)
    ef = filters.lower().strip()
    parser.skip_relevance_filter = ("rel" not in ef) and (ef != "all")

    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]

    result = await parser.parse_light_search(seed, country, language, use_numbers, parallel_limit, source, region_id)
    
    # Собираем L2 config из параметров
    l2_config = _build_l2_config(l2_pmi_valid, l2_centroid_valid, l2_centroid_trash)
    
    result = apply_filters_traced(result, seed, country, method="light-search", language=language, enabled_filters=filters, l2_config=l2_config)

    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])

    return apply_smart_fix(result, seed, language)

@app.get("/api/deep-search")
async def deep_search_endpoint(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны (ua/us/de...)"),
    region_id: int = Query(143, description="ID региона для Yandex (143=Киев)"),
    language: str = Query("auto", description="Язык (auto/ru/uk/en)"),
    use_numbers: bool = Query(False, description="Добавить цифры 0-9"),
    parallel_limit: int = Query(10, description="Параллельных запросов", alias="parallel"),
    include_keywords: bool = Query(True, description="Включить список ключей"),
    filters: str = Query("all", description="Фильтры: all / none / pre,geo,bpf,rel,l0,l2"),
    # L2 пороги (Tri-Signal)
    l2_pmi_valid: float = Query(None, description="L2: PMI VALID threshold"),
    l2_centroid_valid: float = Query(None, description="L2: Centroid VALID threshold"),
    l2_centroid_trash: float = Query(None, description="L2: Centroid TRASH threshold"),
):
    """DEEP SEARCH: глубокий поиск (все 4 метода ИЗ ВСЕХ 3 ИСТОЧНИКОВ)"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    ef = filters.lower().strip()
    parser.skip_relevance_filter = ("rel" not in ef) and (ef != "all")

    result = await parser.parse_deep_search(seed, country, region_id, language, use_numbers, parallel_limit, include_keywords)
    
    # Используем исправленный seed для фильтров (parse_deep_search корректирует через Yandex Speller)
    filter_seed = result.get("corrected_seed") or seed
    
    # Собираем L2 config из параметров
    l2_config = _build_l2_config(l2_pmi_valid, l2_centroid_valid, l2_centroid_trash)
    
    result = apply_filters_traced(result, filter_seed, country, method="deep-search", language=language, deduplicate=True, enabled_filters=filters, l2_config=l2_config)
    
    return result

@app.get("/api/compare")
async def compare_methods(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны (ua/us/de...)"),
    region_id: int = Query(143, description="ID региона для Yandex (143=Киев)"),
    language: str = Query("auto", description="Язык (auto/ru/uk/en)"),
    use_numbers: bool = Query(False, description="Добавить цифры 0-9"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    include_keywords: bool = Query(True, description="Включить список ключей"),
    source: str = Query("google", description="Источник: google/yandex/bing")
):
    """[DEPRECATED] Используйте /api/deep-search"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    return await parser.parse_deep_search(seed, country, region_id, language, use_numbers, parallel_limit, include_keywords, source)

@app.get("/api/parse/suffix")
async def parse_suffix_endpoint(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны"),
    region_id: int = Query(143, description="ID региона для Yandex"),
    language: str = Query("auto", description="Язык"),
    use_numbers: bool = Query(False, description="Добавить цифры"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    source: str = Query("google", description="Источник: google/yandex/bing"),
    filters: str = Query("all", description="Фильтры: all / none / pre,geo,bpf,rel")
):
    """Только SUFFIX метод"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    ef = filters.lower().strip()
    parser.skip_relevance_filter = ("rel" not in ef) and (ef != "all")

    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]

    result = await parser.parse_suffix(seed, country, language, use_numbers, parallel_limit, source, region_id)
    
    result = apply_filters_traced(result, seed, country, method="suffix", language=language, enabled_filters=filters)

    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])

    return apply_smart_fix(result, seed, language)

@app.get("/api/parse/infix")
async def parse_infix_endpoint(
    seed: str = Query(..., description="Базовый запрос (минимум 2 слова)"),
    country: str = Query("ua", description="Код страны"),
    region_id: int = Query(143, description="ID региона для Yandex"),
    language: str = Query("auto", description="Язык"),
    use_numbers: bool = Query(False, description="Добавить цифры"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    source: str = Query("google", description="Источник: google/yandex/bing"),
    filters: str = Query("all", description="Фильтры: all / none / pre,geo,bpf,rel")
):
    """Только INFIX метод"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    ef = filters.lower().strip()
    parser.skip_relevance_filter = ("rel" not in ef) and (ef != "all")

    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]

    result = await parser.parse_infix(seed, country, language, use_numbers, parallel_limit, source, region_id)
    
    result = apply_filters_traced(result, seed, country, method="infix", language=language, enabled_filters=filters)

    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])

    return apply_smart_fix(result, seed, language)

@app.get("/api/parse/morphology")
async def parse_morphology_endpoint(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны"),
    region_id: int = Query(143, description="ID региона для Yandex"),
    language: str = Query("auto", description="Язык"),
    use_numbers: bool = Query(False, description="Добавить цифры"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    source: str = Query("google", description="Источник: google/yandex/bing"),
    filters: str = Query("all", description="Фильтры: all / none / pre,geo,bpf,rel")
):
    """Только MORPHOLOGY метод"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    ef = filters.lower().strip()
    parser.skip_relevance_filter = ("rel" not in ef) and (ef != "all")

    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]

    result = await parser.parse_morphology(seed, country, language, use_numbers, parallel_limit, source, region_id)
    
    result = apply_filters_traced(result, seed, country, method="morphology", language=language, enabled_filters=filters)

    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])

    return apply_smart_fix(result, seed, language)

@app.get("/api/parse/adaptive-prefix")
async def parse_adaptive_prefix_endpoint(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны"),
    region_id: int = Query(143, description="ID региона для Yandex"),
    language: str = Query("auto", description="Язык"),
    use_numbers: bool = Query(False, description="Добавить цифры"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    source: str = Query("google", description="Источник: google/yandex/bing"),
    filters: str = Query("all", description="Фильтры: all / none / pre,geo,bpf,rel")
):
    """ADAPTIVE PREFIX метод (находит PREFIX запросы типа 'киев ремонт пылесосов')"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    ef = filters.lower().strip()
    parser.skip_relevance_filter = ("rel" not in ef) and (ef != "all")

    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]

    result = await parser.parse_adaptive_prefix(seed, country, language, use_numbers, parallel_limit, source, region_id)
    
    result = apply_filters_traced(result, seed, country, method="adaptive-prefix", language=language, enabled_filters=filters)

    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])

    return apply_smart_fix(result, seed, language)


# === Memory Audit Endpoint ===
@app.get("/debug/memory-audit")
def memory_audit():
    import sys, psutil, os
    process = psutil.Process(os.getpid())
    mem = process.memory_info()
    
    # Все загруженные модули и их размеры
    big_modules = {}
    for name, mod in sorted(sys.modules.items()):
        if hasattr(mod, '__file__') and mod.__file__:
            try:
                size = os.path.getsize(mod.__file__)
                top = name.split('.')[0]
                big_modules[top] = big_modules.get(top, 0) + size
            except:
                pass
    
    top_modules = sorted(big_modules.items(), key=lambda x: -x[1])[:20]
    
    return {
        "ram_mb": round(mem.rss / 1024 / 1024, 1),
        "ram_percent": round(process.memory_percent(), 1),
        "system_total_mb": round(psutil.virtual_memory().total / 1024 / 1024, 1),
        "system_available_mb": round(psutil.virtual_memory().available / 1024 / 1024, 1),
        "loaded_modules_top20": {name: f"{size/1024/1024:.1f}MB" for name, size in top_modules},
        "torch_loaded": "torch" in sys.modules,
        "transformers_loaded": "transformers" in sys.modules,
        "fastembed_loaded": "fastembed" in sys.modules,
        "natasha_loaded": "natasha" in sys.modules,
        "google_ads_loaded": "google.ads" in sys.modules,
    }
