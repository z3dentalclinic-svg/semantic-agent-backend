"""
Semantic Agent Backend
FastAPI server with Google Ads API integration
Credentials from environment variables

ФИНАЛЬНАЯ ВЕРСИЯ:
- SUFFIX парсинг (a-z + а-я + 0-9) = 65 модификаторов
- INFIX парсинг (только кириллица а-я) = 29 модификаторов
- MORPH парсинг (все формы слов через pymorphy2)
- /api/test-parser/single - тестирование одиночных запросов
- /api/test-parser/full - полный парсинг
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Set
import os
import yaml
import httpx
import asyncio
import time
import random

# Морфологические анализаторы
try:
    import pymorphy3
    PYMORPHY_AVAILABLE = True
except ImportError:
    PYMORPHY_AVAILABLE = False
    print("⚠️ pymorphy3 не установлен! Морфологический парсинг недоступен.")

try:
    import inflect
    INFLECT_AVAILABLE = True
except ImportError:
    INFLECT_AVAILABLE = False
    print("⚠️ inflect не установлен! Морфология для английского недоступна.")

app = FastAPI(title="Semantic Agent API", version="2.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create google-ads.yaml from environment variables
def create_google_ads_config():
    """Create google-ads.yaml from environment variables"""
    config = {
        'developer_token': os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
        'client_id': os.getenv('GOOGLE_ADS_CLIENT_ID'),
        'client_secret': os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
        'refresh_token': os.getenv('GOOGLE_ADS_REFRESH_TOKEN', ''),
        'login_customer_id': os.getenv('GOOGLE_ADS_CUSTOMER_ID'),
        'use_proto_plus': True
    }
    
    # Write to file
    with open('google-ads.yaml', 'w') as f:
        yaml.dump(config, f)
    
    return config

# ============================================
# GOOGLE AUTOCOMPLETE PARSER
# ============================================

class AutocompleteParser:
    """Парсер Google Autocomplete"""
    
    def __init__(self):
        self.base_url = "http://suggestqueries.google.com/complete/search"
        
        # Базовые модификаторы (для всех языков)
        self.base_modifiers = list("abcdefghijklmnopqrstuvwxyz0123456789")
        
        # Языковые модификаторы (специфичные символы)
        self.language_modifiers = {
            'en': [],  # Английский - только базовые
            'ru': list("абвгдежзийклмнопрстуфхцчшщэюя"),  # Русский (29 букв без ё,ъ,ы,ь)
            'uk': list("абвгдежзийклмнопрстуфхцчшщьюяіїєґ"),  # Украинский
            'de': list("äöüß"),  # Немецкий
            'fr': list("àâäæçéèêëïîôùûüÿ"),  # Французский
            'es': list("áéíñóúü"),  # Испанский
            'pl': list("ąćęłńóśźż"),  # Польский
            'it': list("àèéìíîòóùú"),  # Итальянский
        }
        
        # Список разных User-Agent для ротации
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0",
        ]
        
        # Морфологические анализаторы
        self.morph_ru = None
        self.morph_en = None
        
        if PYMORPHY_AVAILABLE:
            try:
                self.morph_ru = pymorphy3.MorphAnalyzer()
                print("✅ pymorphy3 (русский) инициализирован")
            except Exception as e:
                print(f"⚠️ Ошибка инициализации pymorphy3: {e}")
        
        if INFLECT_AVAILABLE:
            try:
                self.morph_en = inflect.engine()
                print("✅ inflect (английский) инициализирован")
            except Exception as e:
                print(f"⚠️ Ошибка инициализации inflect: {e}")
    
    def get_modifiers(self, language: str) -> List[str]:
        """
        Получить модификаторы для конкретного языка
        
        Args:
            language: Код языка (en, ru, uk, de, fr, es, pl, it)
            
        Returns:
            List[str]: Базовые (a-z + 0-9) + языковые модификаторы
        """
        modifiers = self.base_modifiers.copy()
        
        # Добавляем языковые модификаторы если есть
        lang_mods = self.language_modifiers.get(language.lower(), [])
        modifiers.extend(lang_mods)
        
        return modifiers
        
    def get_word_forms_ru(self, word: str) -> Set[str]:
        """
        Получить все морфологические формы русского слова
        
        Args:
            word: Русское слово
            
        Returns:
            Set[str]: Все формы слова (пылесос, пылесоса, пылесосу, ...)
        """
        if not self.morph_ru:
            print(f"⚠️ morph_ru не инициализирован!")
            return {word}
        
        forms = set()
        try:
            parsed = self.morph_ru.parse(word)
            print(f"🔍 Разбор слова '{word}': найдено {len(parsed)} вариантов")
            
            if parsed:
                # Берем первый вариант разбора
                p = parsed[0]
                print(f"🔍 Первый разбор: {p.tag}")
                
                # Получаем все формы из лексемы
                for form in p.lexeme:
                    forms.add(form.word)
                
                print(f"✅ Получено {len(forms)} форм: {list(forms)[:5]}...")
        except Exception as e:
            print(f"⚠️ Ошибка получения форм для '{word}': {e}")
            forms.add(word)
        
        if len(forms) == 0:
            print(f"⚠️ Не получено ни одной формы для '{word}', возвращаем исходное")
            forms.add(word)
        
        return forms
    
    def get_word_forms_en(self, word: str) -> Set[str]:
        """
        Получить формы английского слова (singular/plural)
        
        Args:
            word: Английское слово
            
        Returns:
            Set[str]: Единственное и множественное число
        """
        if not self.morph_en:
            return {word}
        
        forms = {word}
        try:
            # Множественное число
            plural = self.morph_en.plural(word)
            if plural:
                forms.add(plural)
            
            # Единственное число (если дали plural)
            singular = self.morph_en.singular_noun(word)
            if singular:
                forms.add(singular)
        except Exception as e:
            print(f"⚠️ Ошибка получения форм для '{word}': {e}")
        
        return forms
    
    def get_seed_variations(self, seed: str, language: str) -> List[str]:
        """
        Получить вариации seed фразы с разными морфологическими формами
        
        Args:
            seed: Исходная фраза (например "ремонт пылесосов")
            language: Язык (ru, en)
            
        Returns:
            List[str]: Все вариации фразы
        """
        print(f"🔍 get_seed_variations вызван: seed='{seed}', language='{language}'")
        
        words = seed.split()
        print(f"🔍 Слов в seed: {len(words)}")
        
        if len(words) < 2:
            print(f"⚠️ Меньше 2 слов, возвращаем исходный seed")
            return [seed]
        
        # Получаем формы для последнего слова (обычно существительное)
        last_word = words[-1]
        print(f"🔍 Последнее слово: '{last_word}'")
        
        if language.lower() == 'ru':
            print(f"🔍 Вызываем get_word_forms_ru('{last_word}')")
            word_forms = self.get_word_forms_ru(last_word)
            print(f"🔍 Получено форм от get_word_forms_ru: {len(word_forms)}")
        elif language.lower() == 'en':
            print(f"🔍 Вызываем get_word_forms_en('{last_word}')")
            word_forms = self.get_word_forms_en(last_word)
            print(f"🔍 Получено форм от get_word_forms_en: {len(word_forms)}")
        else:
            print(f"⚠️ Неизвестный язык '{language}', возвращаем исходный seed")
            return [seed]
        
        # Создаем вариации
        variations = []
        base = ' '.join(words[:-1])  # все слова кроме последнего
        
        for form in word_forms:
            variations.append(f"{base} {form}")
        
        print(f"✅ Создано вариаций: {len(variations)}")
        print(f"   Первые 3: {variations[:3]}")
        
        return variations
        
    async def fetch_suggestions(
        self, 
        query: str, 
        country: str = "US", 
        language: str = "en"
    ) -> List[str]:
        """Получить подсказки для одного запроса"""
        params = {
            "client": "firefox",
            "q": query,
            "gl": country.upper(),
            "hl": language.lower()
        }
        
        # Случайный User-Agent для каждого запроса
        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "application/json",
            "Accept-Language": f"{language.lower()},{language.lower()}-{country.upper()};q=0.9,en;q=0.8",
        }
        
        try:
            async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
                response = await client.get(self.base_url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if len(data) >= 2 and isinstance(data[1], list):
                    return data[1]
                
                return []
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    async def parse_with_modifiers(
        self,
        seed: str,
        country: str = "US",
        language: str = "en",
        use_numbers: bool = False,
        use_morphology: bool = False
    ) -> List[str]:
        """
        Парсинг с модификаторами (SUFFIX + INFIX + MORPH)
        
        МЕТОД 1: SUFFIX латиница/цифры - "seed модификатор" (БЕЗ морфологии)
        МЕТОД 2: SUFFIX кириллица - "seed_form модификатор" (С морфологией если включена)
        МЕТОД 3: INFIX кириллица - "слово1 модификатор слово2" (БЕЗ морфологии)
        """
        all_keywords = set()
        
        # Получаем модификаторы для выбранного языка
        all_modifiers = self.get_modifiers(language)
        
        # Если use_numbers=False, убираем цифры из базовых
        if not use_numbers:
            all_modifiers = [m for m in all_modifiers if not m.isdigit()]
        
        # Разделяем модификаторы на латиницу/цифры и кириллицу
        language_specific = self.language_modifiers.get(language.lower(), [])
        cyrillic_modifiers = [m for m in all_modifiers if m in language_specific]
        latin_digit_modifiers = [m for m in all_modifiers if m not in language_specific]
        
        # МОРФОЛОГИЯ ЗАКОММЕНТИРОВАНА ДЛЯ ТЕСТИРОВАНИЯ PREFIX
        # Seed вариации ТОЛЬКО если включена морфология
        # seed_variations = [seed]
        # if use_morphology:
        #     seed_variations = self.get_seed_variations(seed, language)
        #     print(f"🔤 MORPH mode: ENABLED | Seed variations: {len(seed_variations)}")
        #     for var in seed_variations[:5]:
        #         print(f"   - {var}")
        #     if len(seed_variations) > 5:
        #         print(f"   ... и ещё {len(seed_variations) - 5}")
        
        # ВРЕМЕННО: используем только исходный seed
        seed_variations = [seed]
        if use_morphology:
            print(f"⚠️ MORPH mode: ВРЕМЕННО ОТКЛЮЧЕНА ДЛЯ ТЕСТИРОВАНИЯ PREFIX")
        
        seed_words = seed.split()
        
        print(f"🌍 Language: {language.upper()}")
        print(f"📊 Modifiers: Latin/Digits={len(latin_digit_modifiers)}, Cyrillic={len(cyrillic_modifiers)}")
        print(f"📍 INFIX mode: {'ENABLED' if len(cyrillic_modifiers) > 0 and len(seed_words) >= 2 else 'DISABLED'}")
        print(f"📍 PREFIX mode: {'ENABLED' if len(cyrillic_modifiers) > 0 else 'DISABLED'}")
        
        # ========================================
        # 1. SUFFIX с ЛАТИНИЦЕЙ и ЦИФРАМИ - ЗАКОММЕНТИРОВАНО ДЛЯ ТЕСТА
        # ========================================
        # print(f"\n{'='*60}")
        # print(f"🔤 [1/4] SUFFIX Latin/Digits (исходный seed только)")
        # print(f"{'='*60}")
        # print(f"Пример запроса: '{seed} a'")
        # print(f"Модификаторов: {len(latin_digit_modifiers)}")
        # 
        # latin_results = 0
        # for i, modifier in enumerate(latin_digit_modifiers):
        #     query = f"{seed} {modifier}"
        #     suggestions = await self.fetch_suggestions(query, country, language)
        #     all_keywords.update(suggestions)
        #     latin_results += len(suggestions)
        #     
        #     delay = random.uniform(0.5, 2.0)
        #     if i < 3 or len(suggestions) > 0:
        #         print(f"[{i+1}/{len(latin_digit_modifiers)}] '{query}' → {len(suggestions)} results (wait {delay:.1f}s)")
        #     await asyncio.sleep(delay)
        # 
        # print(f"✅ SUFFIX Latin/Digits завершен: {latin_results} результатов")
        
        print(f"\n⚠️ SUFFIX Latin/Digits ОТКЛЮЧЕН ДЛЯ ТЕСТА REVERSE")
        latin_results = 0
        
        # ========================================
        # 2. SUFFIX с КИРИЛЛИЦЕЙ - ЗАКОММЕНТИРОВАНО ДЛЯ ТЕСТА
        # ========================================
        # print(f"\n{'='*60}")
        # print(f"🔤 [2/4] SUFFIX Cyrillic (БЕЗ морфологии - ВРЕМЕННО)")
        # print(f"{'='*60}")
        # print(f"Seed вариаций: {len(seed_variations)}")
        # print(f"Модификаторов на вариацию: {len(cyrillic_modifiers)}")
        # print(f"Всего запросов: {len(seed_variations)} × {len(cyrillic_modifiers)} = {len(seed_variations) * len(cyrillic_modifiers)}")
        # 
        # cyrillic_results = 0
        # for var_idx, current_seed in enumerate(seed_variations):
        #     if use_morphology and var_idx > 0:
        #         print(f"\n🔄 Вариация {var_idx + 1}/{len(seed_variations)}: '{current_seed}'")
        #     elif var_idx == 0:
        #         print(f"Пример запроса: '{current_seed} а'")
        #     
        #     for i, modifier in enumerate(cyrillic_modifiers):
        #         query = f"{current_seed} {modifier}"
        #         suggestions = await self.fetch_suggestions(query, country, language)
        #         all_keywords.update(suggestions)
        #         cyrillic_results += len(suggestions)
        #         
        #         delay = random.uniform(0.5, 2.0)
        #         if i < 3 or len(suggestions) > 0:
        #             print(f"[{i+1}/{len(cyrillic_modifiers)}] '{query}' → {len(suggestions)} results (wait {delay:.1f}s)")
        #         await asyncio.sleep(delay)
        # 
        # print(f"✅ SUFFIX Cyrillic завершен: {cyrillic_results} результатов")
        
        print(f"\n⚠️ SUFFIX Cyrillic ОТКЛЮЧЕН ДЛЯ ТЕСТА REVERSE")
        cyrillic_results = 0
        
        # ========================================
        # 3. INFIX с КИРИЛЛИЦЕЙ - ЗАКОММЕНТИРОВАНО ДЛЯ ТЕСТА
        # ========================================
        # if len(cyrillic_modifiers) > 0 and len(seed_words) >= 2:
        #     print(f"\n{'='*60}")
        #     print(f"🔤 [3/4] INFIX Cyrillic (исходный seed только)")
        #     print(f"{'='*60}")
        #     print(f"Исходный seed: '{seed}'")
        #     print(f"Слов в seed: {len(seed_words)}")
        #     print(f"Шаблон: '{seed_words[0]} [модификатор] {' '.join(seed_words[1:])}'")
        #     print(f"Пример запроса: '{seed_words[0]} а {' '.join(seed_words[1:])}'")
        #     print(f"Модификаторов: {len(cyrillic_modifiers)}")
        #     
        #     infix_results = 0
        #     for i, modifier in enumerate(cyrillic_modifiers):
        #         infix_query = f"{seed_words[0]} {modifier} {' '.join(seed_words[1:])}"
        #         infix_suggestions = await self.fetch_suggestions(infix_query, country, language)
        #         all_keywords.update(infix_suggestions)
        #         infix_results += len(infix_suggestions)
        #         
        #         delay = random.uniform(0.5, 2.0)
        #         if i < 3 or len(infix_suggestions) > 0:
        #             print(f"[{i+1}/{len(cyrillic_modifiers)}] '{infix_query}' → {len(infix_suggestions)} results (wait {delay:.1f}s)")
        #         await asyncio.sleep(delay)
        #     
        #     print(f"✅ INFIX завершен: {infix_results} результатов")
        # else:
        #     print(f"\n⚠️ INFIX DISABLED (требуется: кириллические модификаторы + seed из 2+ слов)")
        
        print(f"\n⚠️ INFIX ОТКЛЮЧЕН ДЛЯ ТЕСТА REVERSE")
        infix_results = 0
        
        # ========================================
        # 4. PREFIX - ЗАКОММЕНТИРОВАНО ДЛЯ ТЕСТА
        # ========================================
        # if len(cyrillic_modifiers) > 0:
        #     print(f"\n{'='*60}")
        #     print(f"🔤 [4/4] PREFIX Cyrillic (исходный seed только)")
        #     print(f"{'='*60}")
        #     print(f"Исходный seed: '{seed}'")
        #     print(f"Шаблон: '[модификатор] {seed}'")
        #     print(f"Пример запроса: 'а {seed}'")
        #     print(f"Модификаторов: {len(cyrillic_modifiers)}")
        #     
        #     prefix_results = 0
        #     for i, modifier in enumerate(cyrillic_modifiers):
        #         prefix_query = f"{modifier} {seed}"
        #         prefix_suggestions = await self.fetch_suggestions(prefix_query, country, language)
        #         all_keywords.update(prefix_suggestions)
        #         prefix_results += len(prefix_suggestions)
        #         
        #         delay = random.uniform(0.5, 2.0)
        #         if i < 3 or len(prefix_suggestions) > 0:
        #             print(f"[{i+1}/{len(cyrillic_modifiers)}] '{prefix_query}' → {len(prefix_suggestions)} results (wait {delay:.1f}s)")
        #         await asyncio.sleep(delay)
        #     
        #     print(f"✅ PREFIX завершен: {prefix_results} результатов")
        # else:
        #     print(f"\n⚠️ PREFIX DISABLED (требуется: кириллические модификаторы)")
        
        print(f"\n⚠️ PREFIX ОТКЛЮЧЕН ДЛЯ ТЕСТА REVERSE")
        prefix_results = 0
        
        # ========================================
        # 5. REVERSE SUFFIX с КИРИЛЛИЦЕЙ (БЕЗ морфологии!) - ТЕСТ!
        # ========================================
        if len(cyrillic_modifiers) > 0:
            # Создаем обратный seed: "пылесосов ремонт" вместо "ремонт пылесосов"
            reversed_seed = ' '.join(reversed(seed_words))
            
            print(f"\n{'='*60}")
            print(f"🔤 [ТЕСТ] REVERSE SUFFIX Cyrillic - НОВЫЙ МЕТОД!")
            print(f"{'='*60}")
            print(f"Исходный seed: '{seed}'")
            print(f"Обратный seed: '{reversed_seed}'")
            print(f"Шаблон: '{reversed_seed} [модификатор]'")
            print(f"Пример запроса: '{reversed_seed} а'")
            print(f"Модификаторов: {len(cyrillic_modifiers)}")
            
            reverse_results = 0
            for i, modifier in enumerate(cyrillic_modifiers):
                # Делаем SUFFIX с обратным seed
                reverse_query = f"{reversed_seed} {modifier}"
                reverse_suggestions = await self.fetch_suggestions(reverse_query, country, language)
                all_keywords.update(reverse_suggestions)
                reverse_results += len(reverse_suggestions)
                
                delay = random.uniform(0.5, 2.0)
                if i < 3 or len(reverse_suggestions) > 0:
                    print(f"[{i+1}/{len(cyrillic_modifiers)}] '{reverse_query}' → {len(reverse_suggestions)} results (wait {delay:.1f}s)")
                await asyncio.sleep(delay)
            
            print(f"✅ REVERSE SUFFIX завершен: {reverse_results} результатов")
        else:
            print(f"\n⚠️ REVERSE SUFFIX DISABLED (требуется: кириллические модификаторы)")
            reverse_results = 0
        
        print(f"\n{'='*60}")
        print(f"🎉 ПАРСИНГ ЗАВЕРШЕН")
        print(f"{'='*60}")
        print(f"Всего уникальных ключевых слов: {len(all_keywords)}")
        
        return list(all_keywords)


# ============================================
# MODELS
# ============================================

class LocationRequest(BaseModel):
    country_code: str

class LocationResponse(BaseModel):
    id: str
    name: str
    type: str

class ParseRequest(BaseModel):
    seed: str
    country: str = "IE"
    language: str = "en"
    use_numbers: bool = False
    use_morphology: bool = False

class ParseResponse(BaseModel):
    seed: str
    keywords: List[str]
    count: int
    requests_made: int
    parsing_time: float


# ============================================
# ENDPOINTS
# ============================================

@app.get("/")
async def root():
    credentials_loaded = all([
        os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
        os.getenv('GOOGLE_ADS_CLIENT_ID'),
        os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
        os.getenv('GOOGLE_ADS_CUSTOMER_ID')
    ])
    
    # Проверяем доступность морфологии
    morph_status = {
        "russian": {
            "available": PYMORPHY_AVAILABLE,
            "library": "pymorphy2",
            "features": "все падежи и числа" if PYMORPHY_AVAILABLE else "не установлено"
        },
        "english": {
            "available": INFLECT_AVAILABLE,
            "library": "inflect",
            "features": "singular/plural" if INFLECT_AVAILABLE else "не установлено"
        }
    }
    
    return {
        "service": "Semantic Agent API",
        "version": "3.0.0 (SUFFIX + INFIX + MORPHOLOGY)",
        "status": "running",
        "credentials_loaded": credentials_loaded,
        "morphology": {
            "status": "enabled" if (PYMORPHY_AVAILABLE or INFLECT_AVAILABLE) else "disabled",
            "languages": morph_status
        },
        "parsing_modes": {
            "suffix": "seed + modifier (all modifiers)",
            "infix": "word1 + modifier + word2 (cyrillic only, 1-char)",
            "morphology": "all word forms (pymorphy2 for RU, inflect for EN)"
        },
        "endpoints": {
            "health": "/health",
            "locations": "/api/locations/{country_code}",
            "countries": "/api/countries",
            "test_parser_single": "/api/test-parser/single?query={query}&country={country}&language={language}",
            "test_parser_quick": "/api/test-parser/quick?query={query}&country={country}&language={language}",
            "test_parser_full": "/api/test-parser/full?seed={seed}&country={country}&language={language}&use_numbers={bool}&use_morphology={bool}"
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "credentials": "loaded" if os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN") else "missing",
        "parser": "enabled (SUFFIX + INFIX + MORPHOLOGY)",
        "morphology": {
            "russian": "enabled ✅" if PYMORPHY_AVAILABLE else "disabled ❌",
            "english": "enabled ✅" if INFLECT_AVAILABLE else "disabled ❌"
        }
    }

@app.get("/debug")
async def debug():
    """Отладочная информация о библиотеках"""
    debug_info = {
        "pymorphy3": {
            "imported": PYMORPHY_AVAILABLE,
            "error": None
        },
        "inflect": {
            "imported": INFLECT_AVAILABLE,
            "error": None
        }
    }
    
    # Пытаемся импортировать и проверить версии
    try:
        import pymorphy3
        debug_info["pymorphy3"]["version"] = pymorphy3.__version__ if hasattr(pymorphy3, '__version__') else "unknown"
        debug_info["pymorphy3"]["module_path"] = str(pymorphy3.__file__)
    except Exception as e:
        debug_info["pymorphy3"]["error"] = str(e)
    
    try:
        import inflect
        debug_info["inflect"]["version"] = inflect.__version__ if hasattr(inflect, '__version__') else "unknown"
        debug_info["inflect"]["module_path"] = str(inflect.__file__)
    except Exception as e:
        debug_info["inflect"]["error"] = str(e)
    
    # Проверяем pkg_resources
    try:
        import pkg_resources
        debug_info["pkg_resources"] = {
            "available": True,
            "version": pkg_resources.__version__ if hasattr(pkg_resources, '__version__') else "unknown"
        }
    except Exception as e:
        debug_info["pkg_resources"] = {
            "available": False,
            "error": str(e)
        }
    
    # Проверяем setuptools
    try:
        import setuptools
        debug_info["setuptools"] = {
            "available": True,
            "version": setuptools.__version__ if hasattr(setuptools, '__version__') else "unknown"
        }
    except Exception as e:
        debug_info["setuptools"] = {
            "available": False,
            "error": str(e)
        }
    
    return debug_info

@app.get("/api/countries")
async def get_countries():
    countries = [
        {"code": "IE", "name": "Ireland", "flag": "🇮🇪"},
        {"code": "UA", "name": "Україна", "flag": "🇺🇦"},
        {"code": "US", "name": "United States", "flag": "🇺🇸"},
        {"code": "GB", "name": "United Kingdom", "flag": "🇬🇧"},
        {"code": "DE", "name": "Deutschland", "flag": "🇩🇪"},
        {"code": "FR", "name": "France", "flag": "🇫🇷"},
        {"code": "ES", "name": "España", "flag": "🇪🇸"},
        {"code": "IT", "name": "Italia", "flag": "🇮🇹"},
        {"code": "PL", "name": "Polska", "flag": "🇵🇱"},
        {"code": "RU", "name": "Россия", "flag": "🇷🇺"},
    ]
    return {"countries": countries}

@app.get("/api/locations/{country_code}")
async def get_locations(country_code: str):
    """Get locations from Google Ads API"""
    try:
        # Create config from env vars
        create_google_ads_config()
        
        # Import Google Ads service
        from google_ads_service import get_locations_for_country
        
        locations = get_locations_for_country(country_code)
        return {
            "country_code": country_code,
            "locations": locations,
            "source": "google_ads_api"
        }
    except Exception as e:
        # Fallback to mock data
        print(f"Error: {e}")
        
        mock_data = {
            "IE": {
                "regions": [
                    {"id": "1007321", "name": "Carlow", "type": "County"},
                    {"id": "1007322", "name": "Cavan", "type": "County"},
                    {"id": "1007323", "name": "Clare", "type": "County"},
                    {"id": "1007324", "name": "Cork", "type": "County"},
                    {"id": "1007325", "name": "Donegal", "type": "County"},
                    {"id": "1007326", "name": "Dublin", "type": "County"},
                ],
                "cities": [
                    {"id": "1007340", "name": "Dublin", "type": "City"},
                    {"id": "1007341", "name": "Cork", "type": "City"},
                    {"id": "1007342", "name": "Galway", "type": "City"},
                ]
            },
            "UA": {
                "regions": [
                    {"id": "21135", "name": "Дніпропетровська", "type": "Oblast"},
                    {"id": "21136", "name": "Київська", "type": "Oblast"},
                    {"id": "21137", "name": "Львівська", "type": "Oblast"},
                ],
                "cities": [
                    {"id": "1012864", "name": "Дніпро", "type": "City"},
                    {"id": "1011969", "name": "Київ", "type": "City"},
                    {"id": "1009902", "name": "Львів", "type": "City"},
                ]
            }
        }
        
        if country_code.upper() in mock_data:
            return {
                "country_code": country_code.upper(),
                "locations": mock_data[country_code.upper()],
                "source": "mock_fallback",
                "error": str(e)
            }
        else:
            return {
                "country_code": country_code.upper(),
                "locations": {"regions": [], "cities": []},
                "source": "mock_fallback",
                "error": str(e)
            }


# ============================================
# PARSER TEST ENDPOINTS
# ============================================

@app.get("/api/test-parser/single")
async def single_test(
    query: str = Query(..., description="Search query to test"),
    country: str = Query("UA", description="Country code (e.g., UA, US)"),
    language: str = Query("ru", description="Language code (e.g., ru, en)")
):
    """
    Тест одиночного запроса к Google Autocomplete
    
    Пример: 
    GET /api/test-parser/single?query=купить%20бе%20вино&country=UA&language=ru
    GET /api/test-parser/single?query=ремонт%20а%20пылесосов&country=UA&language=ru
    """
    parser = AutocompleteParser()
    
    suggestions = await parser.fetch_suggestions(
        query=query,
        country=country,
        language=language
    )
    
    return {
        "query": query,
        "country": country,
        "language": language,
        "suggestions": suggestions,
        "count": len(suggestions),
        "status": "success" if suggestions else "no_results"
    }


@app.get("/api/test-parser/quick")
async def quick_test(
    query: str = "vacuum repair",
    country: str = "IE",
    language: str = "en"
):
    """
    Быстрый тест парсера - один запрос к Google Autocomplete
    
    Пример: GET /api/test-parser/quick?query=ремонт пылесосов&country=UA&language=ru
    """
    parser = AutocompleteParser()
    
    suggestions = await parser.fetch_suggestions(
        query=query,
        country=country,
        language=language
    )
    
    return {
        "query": query,
        "country": country,
        "language": language,
        "suggestions": suggestions,
        "count": len(suggestions),
        "status": "success" if suggestions else "no_results"
    }


@app.get("/api/test-parser/full")
async def full_test(
    seed: str = "vacuum repair",
    country: str = "IE",
    language: str = "en",
    use_numbers: bool = True,
    use_morphology: bool = False
):
    """
    Полный парсинг с модификаторами (SUFFIX + INFIX + MORPH)
    
    SUFFIX: seed + модификатор (все модификаторы a-z + а-я + 0-9)
    INFIX: слово1 + модификатор + слово2 (только кириллица а-я)
    MORPH: парсинг всех морфологических форм seed фразы
    
    Пример: GET /api/test-parser/full?seed=ремонт пылесосов&country=UA&language=ru&use_numbers=true&use_morphology=true
    """
    parser = AutocompleteParser()
    
    # Получаем список модификаторов для информации
    all_modifiers = parser.get_modifiers(language)
    if not use_numbers:
        all_modifiers = [m for m in all_modifiers if not m.isdigit()]
    
    # Разделяем модификаторы
    language_specific = parser.language_modifiers.get(language.lower(), [])
    cyrillic_modifiers = [m for m in all_modifiers if m in language_specific]
    latin_digit_modifiers = [m for m in all_modifiers if m not in language_specific]
    seed_words = seed.split()
    
    # Получаем seed вариации если морфология включена
    seed_variations = 1
    morph_available = False
    # МОРФОЛОГИЯ ЗАКОММЕНТИРОВАНА ДЛЯ ТЕСТИРОВАНИЯ PREFIX  
    # if use_morphology:
    #     if language.lower() == 'ru' and PYMORPHY_AVAILABLE:
    #         morph_available = True
    #     elif language.lower() == 'en' and INFLECT_AVAILABLE:
    #         morph_available = True
    #     
    #     if morph_available:
    #         variations = parser.get_seed_variations(seed, language)
    #         seed_variations = len(variations)
    
    # ПРАВИЛЬНЫЙ РАСЧЕТ (МОРФОЛОГИЯ ВРЕМЕННО ОТКЛЮЧЕНА):
    # 1. SUFFIX латиница/цифры (БЕЗ морфологии)
    suffix_latin_requests = len(latin_digit_modifiers)
    
    # 2. SUFFIX кириллица (БЕЗ морфологии - ВРЕМЕННО!)
    suffix_cyrillic_requests = len(cyrillic_modifiers) * seed_variations
    
    # 3. INFIX кириллица (БЕЗ морфологии!)
    infix_requests = len(cyrillic_modifiers) if len(seed_words) >= 2 else 0
    
    # 4. PREFIX кириллица (БЕЗ морфологии!) - НОВОЕ!
    prefix_requests = len(cyrillic_modifiers)
    
    # ВСЕГО запросов
    total_requests = suffix_latin_requests + suffix_cyrillic_requests + infix_requests + prefix_requests

    
    start_time = time.time()
    
    keywords = await parser.parse_with_modifiers(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers,
        use_morphology=use_morphology
    )
    
    parsing_time = time.time() - start_time
    
    return {
        "seed": seed,
        "country": country,
        "language": language,
        "modifiers_info": {
            "total_modifiers": len(all_modifiers),
            "latin_digit_modifiers": len(latin_digit_modifiers),
            "cyrillic_modifiers": len(cyrillic_modifiers),
            "base": "a-z" + (" + 0-9" if use_numbers else ""),
            "language_specific": "".join(language_specific) or "none"
        },
        "morphology_info": {
            "enabled": use_morphology,
            "available": morph_available,
            "seed_variations": seed_variations if morph_available else 1,
            "library": "pymorphy2" if language.lower() == 'ru' else "inflect" if language.lower() == 'en' else "none"
        },
        "requests_info": {
            "suffix_latin_digit": suffix_latin_requests,
            "suffix_cyrillic": suffix_cyrillic_requests,
            "infix": infix_requests,
            "prefix": prefix_requests,
            "total_requests": total_requests,
            "formula": f"{suffix_latin_requests} (latin/digit) + {suffix_cyrillic_requests} (cyrillic×{seed_variations}) + {infix_requests} (infix) + {prefix_requests} (prefix) = {total_requests}"
        },
        "keywords": keywords,
        "count": len(keywords),
        "requests_made": total_requests,
        "parsing_time": round(parsing_time, 2)
    }


@app.post("/api/test-parser", response_model=ParseResponse)
async def test_parser(request: ParseRequest):
    """
    Полный парсинг с модификаторами (a-z, опционально 0-9, морфология)
    
    Пример запроса:
    POST /api/test-parser
    {
        "seed": "ремонт пылесосов",
        "country": "UA",
        "language": "ru",
        "use_numbers": false,
        "use_morphology": true
    }
    """
    parser = AutocompleteParser()
    
    start_time = time.time()
    
    keywords = await parser.parse_with_modifiers(
        seed=request.seed,
        country=request.country,
        language=request.language,
        use_numbers=request.use_numbers,
        use_morphology=request.use_morphology
    )
    
    parsing_time = time.time() - start_time
    
    # Получаем модификаторы
    all_modifiers = parser.get_modifiers(request.language)
    if not request.use_numbers:
        all_modifiers = [m for m in all_modifiers if not m.isdigit()]
    
    # Разделяем модификаторы
    language_specific = parser.language_modifiers.get(request.language.lower(), [])
    cyrillic_modifiers = [m for m in all_modifiers if m in language_specific]
    latin_digit_modifiers = [m for m in all_modifiers if m not in language_specific]
    seed_words = request.seed.split()
    
    # МОРФОЛОГИЯ ЗАКОММЕНТИРОВАНА ДЛЯ ТЕСТИРОВАНИЯ PREFIX
    # Seed вариации если морфология включена
    seed_variations = 1
    # if request.use_morphology:
    #     morph_available = (request.language.lower() == 'ru' and PYMORPHY_AVAILABLE) or \
    #                      (request.language.lower() == 'en' and INFLECT_AVAILABLE)
    #     if morph_available:
    #         variations = parser.get_seed_variations(request.seed, request.language)
    #         seed_variations = len(variations)
    
    # Расчет запросов (МОРФОЛОГИЯ ОТКЛЮЧЕНА, ДОБАВЛЕН PREFIX)
    suffix_latin = len(latin_digit_modifiers)
    suffix_cyrillic = len(cyrillic_modifiers) * seed_variations
    infix = len(cyrillic_modifiers) if len(seed_words) >= 2 else 0
    prefix = len(cyrillic_modifiers)  # НОВОЕ!
    total_requests = suffix_latin + suffix_cyrillic + infix + prefix
    
    return ParseResponse(
        seed=request.seed,
        keywords=keywords,
        count=len(keywords),
        requests_made=total_requests,
        parsing_time=round(parsing_time, 2)
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
