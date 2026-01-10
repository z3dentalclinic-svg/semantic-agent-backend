
"""
FGS Parser API - Version 5.3.1 PRODUCTION
Deployed: 2026-01-10
Fixed: 
- Корректная обработка английских брендов и русских городов в is_query_allowed
- Убраны лишние пробелы в parse_adaptive_prefix
- Удалены отладочные принты
All functionality preserved
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
from difflib import SequenceMatcher

# NLTK для стемминга (v5.2.0)
import nltk
from nltk.stem import SnowballStemmer

# Natasha для NER (v5.2.4)
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

# Pymorphy3 для лемматизации RU/UK (v5.2.0)
import pymorphy3

app = FastAPI(
    title="FGS Parser API",
    version="5.3.1",
    description="6 методов | 3 источника | Pre-filter: fixed brands/cities logic | Level 2"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# WHITELIST ДЛЯ ПРЕ-ФИЛЬТРА (v5.3.0)

WHITELIST_TOKENS = {
    "филипс", "philips",
    "самсунг", "samsung",
    "бош", "bosch",
    "lg",
    "electrolux", "электролюкс",
    "dyson", "дайсон",
    "xiaomi", "сяоми",
    "karcher", "керхер",
    "tefal", "тефаль",
    "rowenta", "ровента",

    "желтые воды", "жёлтые воды", "zhovti vody",
    "новомосковск", "новомосковськ",  # Украина, НЕ Подмосковье!
}

# GEO BLACKLIST ДЛЯ ПРЕ-ФИЛЬТРА (v5.3.0)

MANUAL_RARE_CITIES = {
    "ua": {
        "щёлкино", "щелкino", "shcholkino",
        "армянск", "армjansk",
        "красноперекопск", "krasnoperekopsk",
        "джанкой", "dzhankoi",

        "коммунарка", "kommunarka",
        "московский", "moskovskiy",
    },

    "ru": {
        "жёлтые воды", "желтые воды", "zhovti vody",
        "вознесенск", "voznesensk",
    },

    "by": set(),

    "kz": set(),
}

def generate_geo_blacklist_full():
    """
    Returns:
    """
    try:
        from geonamescache import GeonamesCache

        gc = GeonamesCache()
        cities = gc.get_cities()

        cities_by_country = {}

        for city_id, city_data in cities.items():
            country = city_data['countrycode']

            if country not in cities_by_country:
                cities_by_country[country] = set()

            name = city_data['name'].lower()
            cities_by_country[country].add(name)

            for alt in city_data.get('alternatenames', []):
                if ' ' in alt:
                    continue

                if not (3 <= len(alt) <= 30):
                    continue

                if not any(c.isalpha() for c in alt):
                    continue

                alt_clean = alt.replace('-', '').replace("'", "")  # Убираем дефисы/апострофы для проверки
                if alt_clean.isalpha():  # Только буквы
                    is_latin_cyrillic = all(
                        ('\u0000' <= c <= '\u007F') or  # ASCII (латиница)
                        ('\u0400' <= c <= '\u04FF') or  # Кириллица
                        c in ['-', "'"]                  # Дефис и апостроф OK
                        for c in alt
                    )

                    if is_latin_cyrillic:
                        cities_by_country[country].add(alt.lower())

        blacklist = {}

        blacklist['ua'] = (
            cities_by_country.get('RU', set()) |  # Россия
            cities_by_country.get('BY', set()) |  # Беларусь
            cities_by_country.get('KZ', set()) |  # Казахстан
            cities_by_country.get('PL', set()) |  # Польша (Щецин!)
            cities_by_country.get('LT', set()) |  # Литва
            cities_by_country.get('LV', set()) |  # Латвия
            cities_by_country.get('EE', set())    # Эстония
        )

        blacklist['ru'] = (
            cities_by_country.get('UA', set()) |
            cities_by_country.get('BY', set()) |
            cities_by_country.get('KZ', set())
        )

        blacklist['by'] = (
            cities_by_country.get('RU', set()) |
            cities_by_country.get('UA', set())
        )

        blacklist['kz'] = (
            cities_by_country.get('RU', set()) |
            cities_by_country.get('UA', set()) |
            cities_by_country.get('BY', set())
        )

        for country, cities_set in blacklist.items():
            manual_cities = MANUAL_RARE_CITIES.get(country, set())
            blacklist[country] = cities_set | manual_cities

        print("✅ GEO_BLACKLIST сгенерирован из geonames + ручной список:")
        for country, cities_set in blacklist.items():
            geonames_count = len(cities_set - MANUAL_RARE_CITIES.get(country, set()))
            manual_count = len(MANUAL_RARE_CITIES.get(country, set()))
            print(f"   {country.upper()}: {len(cities_set)} городов (geonames: {geonames_count}, ручных: {manual_count})")

        return blacklist

    except ImportError:
        print("⚠️ geonamescache не установлен, используется минимальный blacklist")
        return {
            "ua": {"москва", "мск", "спб", "питер", "санкт-петербург", "минск"} | MANUAL_RARE_CITIES.get("ua", set()),
            "ru": {"киев", "харьков", "днепр", "львов", "одесса"} | MANUAL_RARE_CITIES.get("ru", set()),
            "by": {"москва", "спб", "киев", "харьков"} | MANUAL_RARE_CITIES.get("by", set()),
            "kz": {"москва", "спб", "киев"} | MANUAL_RARE_CITIES.get("kz", set()),
        }

GEO_BLACKLIST = generate_geo_blacklist_full()

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

# ENTITY CONFLICT DETECTION (v5.2.4)
class EntityLogicManager:
    """
    """

    def __init__(self):
        self.cache = {}

        # Pymorphy3 для нормализации слов (v5.2.5)
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
                # Украина - области и регионы (v5.2.6)
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
                # Россия - регионы (v5.2.6)
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
        Args:
        Returns:
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
        Args:
        Returns:
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

        # ENTITY CONFLICT DETECTION (v5.2.4)
        self.entity_manager = EntityLogicManager()

        # НОРМАЛИЗАЦИЯ ДЛЯ ФИЛЬТРАЦИИ (v5.2.0)

        self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru')
        self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk')

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
        Args:
        Returns:
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
        Args:
        Returns:
        """
        stemmer = self.stemmers.get(language, self.stemmers['en'])

        stop_words = self.stop_words.get(language, self.stop_words['en'])

        words = re.findall(r'\w+', text.lower())

        meaningful = [w for w in words if w not in stop_words and len(w) > 1]

        stems = {stemmer.stem(w) for w in meaningful}

        return stems

    def _are_words_similar(self, word1: str, word2: str, threshold: float = 0.85) -> bool:
        """
        Args:
        Returns:
        """
        if len(word1) <= 4 or len(word2) <= 4:
            return False

        similarity = SequenceMatcher(None, word1, word2).ratio()

        return similarity >= threshold

    def _normalize(self, text: str, language: str = 'ru') -> set:
        """
        Args:
        Returns:
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
        Args:
        Returns:
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
        Пре-фильтр v5.3.1: корректная обработка английских брендов и русских городов
        """
        import re
        
        q_lower = query.lower().strip()
        
        # 1. Если в запросе есть бренд из Whitelist - ПРОВЕРКА ОКОНЧЕНА (разрешаем)
        if any(white in q_lower for white in WHITELIST_TOKENS):
            return True
        
        # 2. Разбиваем на слова и проверяем каждое
        words = re.findall(r'[а-яёa-z0-9]+', q_lower)
        blacklist = GEO_BLACKLIST.get(country.lower(), set())
        
        for word in words:
            if len(word) < 2:
                continue  # Игнорируем предлоги
            
            # Проверка оригинала
            if word in blacklist:
                return False
            
            # Проверка леммы (только для кириллицы)
            if any(c in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя' for c in word):
                if hasattr(self, 'morph_ru'):
                    try:
                        lemma = self.morph_ru.parse(word)[0].normal_form
                        if lemma in blacklist and lemma not in WHITELIST_TOKENS:
                            return False
                    except:
                        pass
        
        return True

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
                        # ПРОВЕРКА 2.5: Грамматическая валидность (v5.2.3 - Gemini)
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
                continue  # "дому ремонт пылесосов" → позиция 1, но это уже мусор

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

        # ЭТАП 2: ENTITY CONFLICTS (v5.2.4)
        filtered_final = []

        for keyword in filtered:
            is_conflict = await asyncio.to_thread(
                self.entity_manager.check_conflict,
                seed,
                keyword,
                language
            )

            if not is_conflict:
                filtered_final.append(keyword)

        return filtered_final

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

        # Фильтр релевантности (v5.2.0: subset matching)
        filtered = await self.filter_relevant_keywords(result_raw['keywords'], seed, language)

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "suffix",
            "source": source,
            "keywords": filtered,
            "count": len(filtered),
            "queries": len(queries),
            "elapsed_time": round(elapsed, 2)
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

        filtered_1 = await self.filter_infix_results(result_raw['keywords'], language)

        # Фильтр 2: релевантность (v5.2.0: subset matching)
        filtered_2 = await self.filter_relevant_keywords(filtered_1, seed, language)

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "infix",
            "source": source,
            "keywords": filtered_2,
            "count": len(filtered_2),
            "queries": len(queries),
            "elapsed_time": round(elapsed, 2)
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

        filtered = await self.filter_relevant_keywords(sorted(list(all_keywords)), seed, language)

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "morphology",
            "source": source,
            "keywords": filtered,
            "count": len(filtered),
            "elapsed_time": round(elapsed, 2)
        }

    async def parse_light_search(self, seed: str, country: str, language: str, use_numbers: bool, 
                                 parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """LIGHT SEARCH: быстрый поиск (SUFFIX + INFIX)"""
        start_time = time.time()

        suffix_result = await self.parse_suffix(seed, country, language, use_numbers, parallel_limit, source, region_id)
        infix_result = await self.parse_infix(seed, country, language, use_numbers, parallel_limit, source, region_id)

        all_keywords = set(suffix_result["keywords"]) | set(infix_result.get("keywords", []))

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "light_search",
            "source": source,
            "keywords": sorted(list(all_keywords)),
            "count": len(all_keywords),
            "suffix_count": len(suffix_result["keywords"]),
            "infix_count": len(infix_result.get("keywords", [])),
            "elapsed_time": round(elapsed, 2)
        }

    async def parse_adaptive_prefix(self, seed: str, country: str, language: str, use_numbers: bool, 
                                    parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """ADAPTIVE PREFIX метод: извлечение слов из SUFFIX + PREFIX проверка"""
        start_time = time.time()

        seed_words = set(seed.lower().split())

        # Генерация префиксов без лишних пробелов
        prefixes = ["", "купить", "цена", "отзывы"]
        queries = []
        for p in prefixes:
            q = f"{p} {seed}".strip()
            if self.is_query_allowed(q, seed, country):
                queries.append(q)
        
        # Добавляем алфавитные модификаторы
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

        all_keywords = set()
        verified_prefixes = []

        for candidate in sorted(candidates):
            query = f"{candidate} {seed}"

            if not self.is_query_allowed(query, seed, country):
                continue

            result = await self.parse_with_semaphore([query], country, language, parallel_limit, source, region_id)
            if result['keywords']:
                all_keywords.update(result['keywords'])
                verified_prefixes.append(candidate)

        filtered = await self.filter_relevant_keywords(sorted(list(all_keywords)), seed, language)

        elapsed = time.time() - start_time

        return {
            "seed": seed,
            "method": "adaptive_prefix",
            "source": source,
            "keywords": filtered,
            "count": len(filtered),
            "candidates_found": len(candidates),
            "verified_prefixes": verified_prefixes,
            "elapsed_time": round(elapsed, 2)
        }

    async def parse_deep_search(self, seed: str, country: str, region_id: int, language: str, 
                                use_numbers: bool, parallel_limit: int, include_keywords: bool, 
                                source: str = "google") -> Dict:
        """DEEP SEARCH: глубокий поиск (все 4 метода)"""

        correction = await self.autocorrect_text(seed, language)
        original_seed = seed

        if correction.get("has_errors"):
            seed = correction["corrected"]

        start_time = time.time()

        suffix_result = await self.parse_suffix(seed, country, language, use_numbers, parallel_limit, source, region_id)
        infix_result = await self.parse_infix(seed, country, language, use_numbers, parallel_limit, source, region_id)
        morph_result = await self.parse_morphology(seed, country, language, use_numbers, parallel_limit, source, region_id)
        prefix_result = await self.parse_adaptive_prefix(seed, country, language, use_numbers, parallel_limit, source, region_id)

        suffix_kw = set(suffix_result["keywords"])
        infix_kw = set(infix_result.get("keywords", []))
        morph_kw = set(morph_result["keywords"])
        prefix_kw = set(prefix_result["keywords"])

        all_unique = suffix_kw | infix_kw | morph_kw | prefix_kw

        elapsed = time.time() - start_time

        response = {
            "seed": original_seed,
            "corrected_seed": seed if correction.get("has_errors") else None,
            "corrections": correction.get("corrections", []) if correction.get("has_errors") else [],
            "source": source,
            "total_unique_keywords": len(all_unique),
            "methods": {
                "suffix": {"count": len(suffix_kw)},
                "infix": {"count": len(infix_kw)},
                "morphology": {"count": len(morph_kw)},
                "adaptive_prefix": {"count": len(prefix_kw)}
            },
            "elapsed_time": round(elapsed, 2)
        }

        if include_keywords:
            response["keywords"] = {
                "all": sorted(list(all_unique)),
                "suffix": sorted(list(suffix_kw)),
                "infix": sorted(list(infix_kw)),
                "morphology": sorted(list(morph_kw)),
                "adaptive_prefix": sorted(list(prefix_kw))
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
    include_keywords: bool = Query(True, description="Включить список ключей"),
    source: str = Query("google", description="Источник: google/yandex/bing")
):
    """DEEP SEARCH: глубокий поиск (все 4 метода)"""

    if language == "auto":
        language = parser.detect_seed_language(seed)

    return await parser.parse_deep_search(seed, country, region_id, language, use_numbers, parallel_limit, include_keywords, source)

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

