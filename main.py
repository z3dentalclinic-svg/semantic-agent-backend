"""
FGS Parser API v7.9 FUNDAMENTAL FIX - GEO DATABASE PRIORITY
Batch Post-Filter + O(1) Lookups + 3 Sources

🔥 ФУНДАМЕНТАЛЬНОЕ ИСПРАВЛЕНИЕ v7.9:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ПРОБЛЕМА v7.7-v7.8:
  Морфология (_is_common_noun) блокировала города из базы
  
РЕШЕНИЕ v7.9:
  База городов = ПЕРВИЧНА, морфология = ВТОРИЧНА
  Любой город из базы с country != target → БЛОКИРУЕТСЯ
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ v7.8:
🔥 ИСПРАВЛЕНО: Убрана "умная" проверка для городов СНГ
🔥 РЕЗУЛЬТАТ: Барановичи, Лошица, Ждановичи, Талдыкорган блокируются
🔥 ЛОГИКА: СНГ→UA = жесткая блокировка по коду страны

КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ v7.7:
🔥 ИСПРАВЛЕНО: В BatchPostFilter теперь передаётся РЕАЛЬНАЯ база городов
🔥 ИСПРАВЛЕНО: Раньше передавался пустой словарь {} - фильтр не работал!
🔥 РЕЗУЛЬТАТ: Актобе, Фаниполь, Ошмяны теперь правильно блокируются
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

from filters import BatchPostFilter, DISTRICTS_EXTENDED
from geo import generate_geo_blacklist_full
from config import USER_AGENTS, WHITELIST_TOKENS, MANUAL_RARE_CITIES, FORBIDDEN_GEO

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
    version="7.9.0",
    description="6 методов | 3 sources | Batch Post-Filter | O(1) lookups | v7.9 GEO DB PRIORITY"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def generate_geo_blacklist_full():
    """
    """
    try:
        from geonamescache import GeonamesCache

        gc = GeonamesCache()
        cities = gc.get_cities()

        all_cities_global = {}  # {город: код_страны}

        for city_id, city_data in cities.items():
            country = city_data['countrycode'].lower()  # 'RU', 'UA', 'BY' → 'ru', 'ua', 'by'

            name = city_data['name'].lower().strip()
            all_cities_global[name] = country

            for alt in city_data.get('alternatenames', []):
                if ' ' in alt:
                    continue

                if not (3 <= len(alt) <= 30):
                    continue

                if not any(c.isalpha() for c in alt):
                    continue

                alt_clean = alt.replace('-', '').replace("'", "")
                if alt_clean.isalpha():
                    is_latin_cyrillic = all(
                        ('\u0000' <= c <= '\u007F') or  # ASCII (латиница)
                        ('\u0400' <= c <= '\u04FF') or  # Кириллица
                        c in ['-', "'"]
                        for c in alt
                    )

                    if is_latin_cyrillic:
                        alt_lower = alt.lower().strip()
                        if alt_lower not in all_cities_global:
                            all_cities_global[alt_lower] = country

        print("✅ v5.6.0 TURBO: O(1) WORD BOUNDARY LOOKUP - Гео-Фильтрация инициализирована")
        print(f"   ALL_CITIES_GLOBAL: {len(all_cities_global)} городов с привязкой к странам")
        
        from collections import Counter
        country_stats = Counter(all_cities_global.values())
        print(f"   Топ-5 стран: {dict(country_stats.most_common(5))}")

        return all_cities_global

    except ImportError:
        print("⚠️ geonamescache не установлен, используется минимальный словарь")
        
        all_cities_global = {
            'москва': 'ru', 'мск': 'ru', 'спб': 'ru', 'питер': 'ru', 
            'санкт-петербург': 'ru', 'екатеринбург': 'ru', 'казань': 'ru',
            'новосибирск': 'ru', 'челябинск': 'ru', 'омск': 'ru',
            'минск': 'by', 'гомель': 'by', 'витебск': 'by', 'могилев': 'by',
            'алматы': 'kz', 'астана': 'kz', 'караганда': 'kz',
            'киев': 'ua', 'харьков': 'ua', 'одесса': 'ua', 'днепр': 'ua',
            'львов': 'ua', 'запорожье': 'ua', 'кривой рог': 'ua',
            'николаев': 'ua', 'винница': 'ua', 'херсон': 'ua',
            'полтава': 'ua', 'чернигов': 'ua', 'черкассы': 'ua',
            'днепропетровск': 'ua', 'kyiv': 'ua', 'kiev': 'ua',
            'kharkiv': 'ua', 'odessa': 'ua', 'lviv': 'ua', 'dnipro': 'ua',
        }
        
        return all_cities_global

ALL_CITIES_GLOBAL = generate_geo_blacklist_full()

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

class EntityLogicManager:
    """
    """

    def __init__(self):
        self.cache = {}

        try:
            import pymorphy3
            self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
            self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk')
            self.morph_available = True
        except Exception as e:
            print(f"⚠️ Pymorphy3 не доступен для EntityLogicManager: {e}")
            self.morph_available = False

        self.hard_cache = {
            'LOC': {
                "киев", "днепр", "днепропетровск", "харьков", "одесса", "львов", 
                "запорожье", "донецк", "кривой рог", "николаев", "луганск", 
                "винница", "херсон", "полтава", "чернигов", "черкассы",
                "житомир", "сумы", "хмельницкий", "ровно", "ивано-франковск",
                "тернополь", "луцк", "ужгород", "черновцы",
                "днепропетровская область", "днепропетровская",
                "харьковская область", "харьковская",
                "киевская область", "киевская",
                "одесская область", "одесская",
                "львовская область", "львовская",
                "запорожская область", "запорожская",
                "донецкая область", "донецкая",
                "луганская область", "луганская",
                "крым", "донбасс", "закарпатье",
                "москва", "спб", "питер", "санкт-петербург", "екатеринбург", 
                "новосибирск", "казань", "краснодар", "воронеж", "самара", 
                "ростов", "уфа", "челябинск", "нижний новгород", "омск",
                "красноярск", "пермь", "волгоград", "саратов", "тюмень",
                "московская область", "московская",
                "ленинградская область", "ленинградская",
                "краснодарский край", "краснодарский",
                "подмосковье",
                "минск", "гомель", "могилев", "витебск", "гродно", "брест",
                "астана", "алматы", "ташкент", "тбилиси", "ереван", "баку",
                "кишинев", "рига", "таллин", "вильнюс", "прага", "варшава",
                "симферополь", "севастополь", "ялта", "евпатория", "керчь", "феодосия",
                "лимассол", "никосия", "ларнака", "пафос"
            },
            'ORG': {
                "apple", "samsung", "xiaomi", "lg", "sony", "bosch",
                "philips", "panasonic", "nokia", "huawei", "lenovo",
                "dell", "hp", "asus", "acer", "msi", "intel", "amd",
                "dyson", "karcher", "thomas", "electrolux", "siemens",
                "ariston", "indesit", "candy", "zanussi", "beko",
                "gorenje", "whirlpool", "hotpoint", "miele", "aeg"
            }
        }

        self.natasha_available = NATASHA_AVAILABLE
        if self.natasha_available:
            try:
                self.segmenter = Segmenter()
                self.morph_vocab = MorphVocab()
                self.emb = NewsEmbedding()
                self.ner_tagger = NewsNERTagger(self.emb)
            except Exception as e:
                print(f"⚠️ Ошибка инициализации Natasha: {e}")
                self.natasha_available = False

    def get_entities(self, text: str, lang: str = 'ru') -> Dict[str, set]:
        """
        """
        cache_key = f"{text}_{lang}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        text_lower = text.lower()
        entities = {'LOC': set(), 'ORG': set()}

        words_original = set(re.findall(r'\w+', text_lower))

        for category, items in self.hard_cache.items():
            intersection = words_original & items
            if intersection:
                entities[category].update(intersection)

        if not any(entities.values()) and self.morph_available and lang in ['ru', 'uk']:
            morph = self.morph_ru if lang == 'ru' else self.morph_uk
            words_normalized = set()

            for word in words_original:
                try:
                    parsed = morph.parse(word)
                    if parsed:
                        words_normalized.add(parsed[0].normal_form)
                except:
                    words_normalized.add(word)

            for category, items in self.hard_cache.items():
                intersection = words_normalized & items
                if intersection:
                    entities[category].update(intersection)

        if not any(entities.values()) and lang == 'ru' and self.natasha_available:
            try:
                doc = Doc(text)
                doc.segment(self.segmenter)
                doc.tag_ner(self.ner_tagger)

                for span in doc.spans:
                    if span.type == 'LOC':
                        entities['LOC'].add(span.text.lower())
                    elif span.type == 'ORG':
                        entities['ORG'].add(span.text.lower())
            except Exception as e:
                pass  # NER может упасть на некорректном тексте

        self.cache[cache_key] = entities
        return entities

    def check_conflict(self, seed: str, keyword: str, lang: str = 'ru') -> bool:
        """
        """
        seed_entities = self.get_entities(seed, lang)
        kw_entities = self.get_entities(keyword, lang)

        for entity_type in ['LOC', 'ORG']:
            seed_set = seed_entities[entity_type]
            kw_set = kw_entities[entity_type]

            if seed_set and (kw_set - seed_set):
                return True  # КОНФЛИКТ!

        return False  # Нет конфликта

class GoogleAutocompleteParser:
    def __init__(self):
        self.adaptive_delay = AdaptiveDelay()

        # v7.6: ЗАКОММЕНТИРОВАНО - не используется, фильтрация только через BatchPostFilter
        # self.entity_manager = EntityLogicManager()

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
        
        # v7.7 CRITICAL FIX: Передаём реальную базу городов ALL_CITIES_GLOBAL
        # Исправлена критическая ошибка: раньше передавался пустой словарь {}
        self.post_filter = BatchPostFilter(
            all_cities_global=ALL_CITIES_GLOBAL,  # ✅ ИСПРАВЛЕНО: передаём загруженную базу
            forbidden_geo=self.forbidden_geo,
            districts=DISTRICTS_EXTENDED,
            population_threshold=5000  # v7.6: Фильтр по населению
        )
        logger.info("✅ Batch Post-Filter v7.9 initialized with REAL cities database")
        logger.info(f"   Database contains {len(ALL_CITIES_GLOBAL)} cities")
        logger.info("   GEO DATABASE = PRIMARY, morphology = secondary")

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

    def _normalize_with_pymorphy(self, text: str, language: str) -> set:
        """
        """
        morph = self.morph_ru if language == 'ru' else self.morph_uk

        stop_words = self.stop_words.get(language, self.stop_words['ru'])

        words = re.findall(r'\w+', text.lower())

        meaningful = [w for w in words if w not in stop_words and len(w) > 1]

        lemmas = set()
        for word in meaningful:
            try:
                parsed = morph.parse(word)
                if parsed:
                    lemmas.add(parsed[0].normal_form)
            except:
                lemmas.add(word)

        return lemmas

    def _normalize_with_snowball(self, text: str, language: str) -> set:
        """
        """
        stemmer = self.stemmers.get(language, self.stemmers['en'])

        stop_words = self.stop_words.get(language, self.stop_words['en'])

        words = re.findall(r'\w+', text.lower())

        meaningful = [w for w in words if w not in stop_words and len(w) > 1]

        stems = {stemmer.stem(w) for w in meaningful}

        return stems

    def _are_words_similar(self, word1: str, word2: str, threshold: float = 0.85) -> bool:
        """
        """
        if len(word1) <= 4 or len(word2) <= 4:
            return False

        similarity = SequenceMatcher(None, word1, word2).ratio()

        return similarity >= threshold

    def _normalize(self, text: str, language: str = 'ru') -> set:
        """
        """

        if language in ['ru', 'uk']:
            return self._normalize_with_pymorphy(text, language)

        elif language in ['en', 'de', 'fr', 'es', 'it']:
            return self._normalize_with_snowball(text, language)

        else:
            words = re.findall(r'\w+', text.lower())
            stop_words = self.stop_words.get('en', set())  # fallback на английские
            meaningful = [w for w in words if w not in stop_words and len(w) > 1]
            return set(meaningful)

    def is_grammatically_valid(self, seed_word: str, kw_word: str, language: str = 'ru') -> bool:
        """
        """
        if language not in ['ru', 'uk']:
            return True

        try:
            morph = self.morph_ru if language == 'ru' else self.morph_uk

            parsed_seed = morph.parse(seed_word)
            parsed_kw = morph.parse(kw_word)

            if not parsed_seed or not parsed_kw:
                return True  # Если не распарсилось - пропускаем

            seed_form = parsed_seed[0]
            kw_form = parsed_kw[0]

            if seed_form.normal_form != kw_form.normal_form:
                return True  # Разные слова - не наша проблема

            invalid_tags = {'datv', 'ablt', 'loct'}

            if 'plur' in kw_form.tag and any(tag in kw_form.tag for tag in invalid_tags):
                return False  # Отсеиваем грамматический мусор!

            return True  # Форма допустимая

        except Exception as e:
            return True

    def is_query_allowed(self, query: str, seed: str, country: str) -> bool:
        """
        v7.6: ПОЛНОСТЬЮ ОТКЛЮЧЕН - фильтрация теперь только через BatchPostFilter
        Всегда возвращает True
        
        Старая логика закомментирована ниже - можно вернуть если понадобится
        """
        return True  # v7.6: Пропускаем всё, фильтрация в BatchPostFilter
        
        # ============================================
        # v7.6: СТАРАЯ ЛОГИКА ЗАКОММЕНТИРОВАНА
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
    
    def post_filter_cities(self, keywords: set, country: str) -> set:
        """
        """
        import re
        
        cleaned = set()
        removed_count = 0
        
        for keyword in keywords:
            should_remove = False
            kw_lower = keyword.lower()
            
            words = re.findall(r'[а-яёa-z0-9-]+', kw_lower)
            
            for word in words:
                if len(word) < 3:
                    continue
                
                if not self.is_city_allowed(word, country):
                    logger.info(f"🧹 POST-FILTER removed (v5.4.0): '{keyword}' | City '{word}' not allowed for {country.upper()}")
                    should_remove = True
                    removed_count += 1
                    break
            
            if not should_remove:
                cleaned.add(keyword)
        
        if removed_count > 0:
            logger.warning(f"🧹 POST-FILTER: Removed {removed_count} keywords with non-{country.upper()} cities")
        
        return cleaned

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

    async def filter_infix_results(self, keywords: List[str], language: str) -> List[str]:
        """Фильтр INFIX результатов: убирает мусорные одиночные буквы"""

        if language.lower() == 'ru':
            valid = {'в', 'на', 'у', 'к', 'от', 'из', 'по', 'о', 'об', 'с', 'со', 'за', 'для', 'и', 'а', 'но'}
        elif language.lower() == 'uk':
            valid = {'в', 'на', 'у', 'до', 'від', 'з', 'по', 'про', 'для', 'і', 'та', 'або'}
        elif language.lower() == 'en':
            valid = {'in', 'on', 'at', 'to', 'from', 'with', 'for', 'by', 'o', 'and', 'or', 'a', 'i'}
        else:
            valid = set()

        filtered = []

        for keyword in keywords:
            keyword_lower = keyword.lower()
            words = keyword_lower.split()

            has_garbage = False
            for i in range(1, len(words)):
                word = words[i]
                if len(word) == 1 and word not in valid:
                    has_garbage = True
                    break

            if not has_garbage:
                filtered.append(keyword)

        return filtered

    async def filter_relevant_keywords(self, keywords: List[str], seed: str, language: str = 'ru') -> List[str]:
        """
        """

        seed_lemmas = self._normalize(seed, language)

        if not seed_lemmas:
            return keywords

        seed_lower = seed.lower()
        seed_words_original = [w.lower() for w in re.findall(r'\w+', seed) if len(w) > 2]

        stop_words = self.stop_words.get(language, self.stop_words['ru'])

        seed_important_words = [w for w in seed_words_original if w not in stop_words]

        if not seed_important_words:
            seed_important_words = seed_words_original

        filtered = []

        for keyword in keywords:
            kw_lower = keyword.lower()

            kw_lemmas = self._normalize(keyword, language)
            if not seed_lemmas.issubset(kw_lemmas):
                continue  # Не про то - отсеиваем

            kw_words = kw_lower.split()
            matches = 0
            grammatically_valid = True

            for seed_word in seed_important_words:
                found_match = False

                for kw_word in kw_words:
                    if seed_word in kw_word:
                        if self.is_grammatically_valid(seed_word, kw_word, language):
                            found_match = True
                            break
                        else:
                            grammatically_valid = False
                            break

                if found_match:
                    matches += 1

            if not grammatically_valid:
                continue

            if len(seed_important_words) > 0:
                match_ratio = matches / len(seed_important_words)
                if match_ratio < 1.0:  # Если НЕ 100% - отсеиваем
                    continue

            first_seed_word = seed_important_words[0]
            first_word_position = -1

            for i, kw_word in enumerate(kw_words):
                if first_seed_word in kw_word:
                    first_word_position = i
                    break

            if first_word_position > 1:
                continue

            last_index = -1
            order_correct = True

            for seed_word in seed_important_words:
                found_at = -1
                for i, kw_word in enumerate(kw_words):
                    if i > last_index and seed_word in kw_word:
                        found_at = i
                        break

                if found_at == -1:
                    order_correct = False
                    break

                last_index = found_at

            if order_correct:
                filtered.append(keyword)

        # ============================================
        # v7.6 FIX: ЗАКОММЕНТИРОВАНО - ИСПОЛЬЗУЕМ ТОЛЬКО BatchPostFilter
        # Старая логика EntityLogicManager создавала дубли и пропускала города
        # ============================================
        # filtered_final = []
        #
        # for keyword in filtered:
        #     is_conflict = await asyncio.to_thread(
        #         self.entity_manager.check_conflict,
        #         seed,
        #         keyword,
        #         language
        #     )
        #
        #     if not is_conflict:
        #         filtered_final.append(keyword)
        #
        # return filtered_final
        
        # v7.6: Возвращаем filtered напрямую - фильтрация теперь только через BatchPostFilter
        return filtered

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
                if anchor and anchor != seed.lower() and len(anchor) > 5:
                    internal_anchors.add(anchor)
                continue  # НЕ добавляем мусор в keywords
            
            keywords.add(kw)
        
        all_with_anchors = keywords | internal_anchors
        filtered = await self.filter_relevant_keywords(list(all_with_anchors), seed, language)
        
        filtered_set = set(filtered)
        final_keywords = sorted(list(keywords & filtered_set))
        final_anchors = sorted(list(internal_anchors & filtered_set))
        
        batch_result = self.post_filter.filter_batch(
            keywords=final_keywords,
            seed=seed,
            country=country,
            language=language
        )
        
        # Объединяем якоря (старые + новые от batch_filter)
        combined_anchors = set(final_anchors) | set(batch_result['anchors'])

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "suffix",
            "source": source,
            "keywords": batch_result['keywords'],  # Очищенные через batch_filter
            "anchors": sorted(list(combined_anchors)),
            "count": len(batch_result['keywords']),
            "anchors_count": len(combined_anchors),
            "queries": len(queries),
            "elapsed_time": round(elapsed, 2),
            "batch_stats": batch_result['stats']  # Статистика фильтрации
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
                if anchor and anchor != seed.lower() and len(anchor) > 5:
                    internal_anchors.add(anchor)
                continue  # НЕ добавляем мусор в keywords
            
            keywords.add(kw)
        
        all_with_anchors = keywords | internal_anchors
        filtered_1 = await self.filter_infix_results(list(all_with_anchors), language)

        filtered_2 = await self.filter_relevant_keywords(filtered_1, seed, language)
        
        filtered_set = set(filtered_2)
        final_keywords = sorted(list(keywords & filtered_set))
        final_anchors = sorted(list(internal_anchors & filtered_set))
        
        batch_result = self.post_filter.filter_batch(
            keywords=final_keywords,
            seed=seed,
            country=country,
            language=language
        )
        
        combined_anchors = set(final_anchors) | set(batch_result['anchors'])

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "infix",
            "source": source,
            "keywords": batch_result['keywords'],
            "anchors": sorted(list(combined_anchors)),
            "count": len(batch_result['keywords']),
            "anchors_count": len(combined_anchors),
            "queries": len(queries),
            "elapsed_time": round(elapsed, 2),
            "batch_stats": batch_result['stats']
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
                if anchor and anchor != seed.lower() and len(anchor) > 5:
                    internal_anchors.add(anchor)
                continue  # НЕ добавляем мусор в keywords
            
            keywords.add(kw)
        
        all_with_anchors = keywords | internal_anchors
        filtered = await self.filter_relevant_keywords(sorted(list(all_with_anchors)), seed, language)
        
        filtered_set = set(filtered)
        final_keywords = sorted(list(keywords & filtered_set))
        final_anchors = sorted(list(internal_anchors & filtered_set))
        
        batch_result = self.post_filter.filter_batch(
            keywords=final_keywords,
            seed=seed,
            country=country,
            language=language
        )
        combined_anchors = set(final_anchors) | set(batch_result['anchors'])

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "morphology",
            "source": source,
            "keywords": batch_result['keywords'],
            "anchors": sorted(list(combined_anchors)),
            "count": len(batch_result['keywords']),
            "anchors_count": len(combined_anchors),
            "elapsed_time": round(elapsed, 2),
            "batch_stats": batch_result['stats']
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
                        if anchor and anchor != seed.lower() and len(anchor) > 5:
                            internal_anchors.add(anchor)
                        continue  # НЕ добавляем мусор в keywords
                    
                    keywords.add(kw)
        
        all_with_anchors = keywords | internal_anchors
        filtered = await self.filter_relevant_keywords(sorted(list(all_with_anchors)), seed, language)
        
        filtered_set = set(filtered)
        final_keywords = sorted(list(keywords & filtered_set))
        final_anchors = sorted(list(internal_anchors & filtered_set))
        
        batch_result = self.post_filter.filter_batch(
            keywords=final_keywords,
            seed=seed,
            country=country,
            language=language
        )
        combined_anchors = set(final_anchors) | set(batch_result['anchors'])

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "adaptive_prefix",
            "source": source,
            "keywords": batch_result['keywords'],
            "anchors": sorted(list(combined_anchors)),
            "count": len(batch_result['keywords']),
            "anchors_count": len(combined_anchors),
            "candidates_found": len(candidates),
            "verified_prefixes": verified_prefixes,
            "elapsed_time": round(elapsed, 2),
            "batch_stats": batch_result['stats']
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
        
        # ✅ КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Финальная фильтрация всех собранных keywords
        # Каждый метод фильтрует свои результаты, но при объединении могут просочиться мусорные города
        logger.info(f"[Deep Search] Before final filter: {len(all_unique_keywords)} keywords")
        
        final_filter = self.post_filter.filter_batch(
            keywords=list(all_unique_keywords),
            seed=seed,
            country=country,
            language=language
        )
        
        all_unique_keywords = set(final_filter['keywords'])
        all_unique_anchors = set(final_filter['anchors']) | all_unique_anchors
        
        logger.info(f"[Deep Search] After final filter: {len(all_unique_keywords)} keywords, {len(all_unique_anchors)} anchors")

        elapsed = time.time() - start_time

        response = {
            "seed": original_seed,
            "corrected_seed": seed if correction.get("has_errors") else None,
            "corrections": correction.get("corrections", []) if correction.get("has_errors") else [],
            "keywords": sorted(list(all_unique_keywords)),  # Для фронтенда
            "anchors": sorted(list(all_unique_anchors)),    # Для фронтенда
            "count": len(all_unique_keywords),              # Для фронтенда
            "anchors_count": len(all_unique_anchors),       # Для фронтенда
            "sources": sources,
            "total_unique_keywords": len(all_unique_keywords),
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

@app.get("/")
async def root():
    """Главная страница"""
    return FileResponse('static/index.html')

@app.get("/api/light-search")
async def light_search_endpoint(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны"),
    region_id: int = Query(143, description="ID региона для Yandex"),
    language: str = Query("auto", description="Язык"),
    use_numbers: bool = Query(False, description="Добавить цифры"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    source: str = Query("google", description="Источник: google/yandex/bing")
):
    """LIGHT SEARCH: быстрый поиск (SUFFIX + INFIX)"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]

    result = await parser.parse_light_search(seed, country, language, use_numbers, parallel_limit, source, region_id)

    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])

    return result

@app.get("/api/deep-search")
async def deep_search_endpoint(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны (ua/us/de...)"),
    region_id: int = Query(143, description="ID региона для Yandex (143=Киев)"),
    language: str = Query("auto", description="Язык (auto/ru/uk/en)"),
    use_numbers: bool = Query(False, description="Добавить цифры 0-9"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    include_keywords: bool = Query(True, description="Включить список ключей")
):
    """DEEP SEARCH: глубокий поиск (все 4 метода ИЗ ВСЕХ 3 ИСТОЧНИКОВ)"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    return await parser.parse_deep_search(seed, country, region_id, language, use_numbers, parallel_limit, include_keywords)

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
    source: str = Query("google", description="Источник: google/yandex/bing")
):
    """Только SUFFIX метод"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]

    result = await parser.parse_suffix(seed, country, language, use_numbers, parallel_limit, source, region_id)

    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])

    return result

@app.get("/api/parse/infix")
async def parse_infix_endpoint(
    seed: str = Query(..., description="Базовый запрос (минимум 2 слова)"),
    country: str = Query("ua", description="Код страны"),
    region_id: int = Query(143, description="ID региона для Yandex"),
    language: str = Query("auto", description="Язык"),
    use_numbers: bool = Query(False, description="Добавить цифры"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    source: str = Query("google", description="Источник: google/yandex/bing")
):
    """Только INFIX метод"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]

    result = await parser.parse_infix(seed, country, language, use_numbers, parallel_limit, source, region_id)

    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])

    return result

@app.get("/api/parse/morphology")
async def parse_morphology_endpoint(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны"),
    region_id: int = Query(143, description="ID региона для Yandex"),
    language: str = Query("auto", description="Язык"),
    use_numbers: bool = Query(False, description="Добавить цифры"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    source: str = Query("google", description="Источник: google/yandex/bing")
):
    """Только MORPHOLOGY метод"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]

    result = await parser.parse_morphology(seed, country, language, use_numbers, parallel_limit, source, region_id)

    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])

    return result

@app.get("/api/parse/adaptive-prefix")
async def parse_adaptive_prefix_endpoint(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны"),
    region_id: int = Query(143, description="ID региона для Yandex"),
    language: str = Query("auto", description="Язык"),
    use_numbers: bool = Query(False, description="Добавить цифры"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    source: str = Query("google", description="Источник: google/yandex/bing")
):
    """ADAPTIVE PREFIX метод (находит PREFIX запросы типа 'киев ремонт пылесосов')"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]

    result = await parser.parse_adaptive_prefix(seed, country, language, use_numbers, parallel_limit, source, region_id)

    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])

    return result

