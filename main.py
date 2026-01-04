"""
GOOGLE AUTOCOMPLETE PARSER - SUFFIX WITH SMART FILTERING
SUFFIX парсинг с умной фильтрацией модификаторов (с учётом брендов)
Version: 3.4 Smart Filtering (Brand-Aware)
Задержка: 0.2-0.5 сек + параллелизм (3-5 потоков) + умная фильтрация
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
import httpx
import asyncio
import time
import random

app = FastAPI(
    title="Google Autocomplete Parser - SUFFIX with Smart Filtering", 
    version="3.4",
    description="SUFFIX парсинг с умной фильтрацией (сохраняем латиницу для брендов)"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# USER AGENTS
# ============================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]


# ============================================
# SMART SUFFIX PARSER (BRAND-AWARE)
# ============================================
class SmartSuffixParser:
    def __init__(self):
        self.base_url = "https://suggestqueries.google.com/complete/search"
        
        # Базовые модификаторы (латиница + цифры)
        self.base_modifiers = list("abcdefghijklmnopqrstuvwxyz0123456789")
        
        # Языковые модификаторы (кириллица и спецсимволы)
        self.language_modifiers = {
            'en': [],
            'ru': list("абвгдежзийклмнопрстуфхцчшщэюя"),
            'uk': list("абвгдежзийклмнопрстуфхцчшщьюяіїєґ"),
            'de': list("äöüß"),
            'fr': list("àâäæçéèêëïîôùûüÿ"),
            'es': list("áéíñóúü"),
            'pl': list("ąćęłńóśźż"),
            'it': list("àèéìíîòóùú"),
        }
        
        # Редкие буквы которые можно пропустить
        self.rare_chars = {
            'ru': ['ъ', 'ё', 'ы'],  # Редко начинаются слова
            'uk': ['ь', 'ъ'],
            'pl': ['ą', 'ę'],
        }
    
    def detect_seed_language(self, seed: str) -> str:
        """
        Определить язык seed запроса
        
        Returns:
            'latin' - если латиница или цифры
            'cyrillic' - если кириллица
            'mixed' - если смесь
        """
        has_latin = False
        has_cyrillic = False
        
        for char in seed.lower():
            if char.isalpha():
                if ord(char) >= ord('a') and ord(char) <= ord('z'):
                    has_latin = True
                elif ord(char) >= ord('а') and ord(char) <= ord('я'):
                    has_cyrillic = True
        
        if has_cyrillic and has_latin:
            return 'mixed'
        elif has_cyrillic:
            return 'cyrillic'
        else:
            return 'latin'  # По умолчанию латиница (включая только цифры)
    
    def get_modifiers(self, language: str, use_numbers: bool = True, seed: str = "") -> List[str]:
        """
        УМНАЯ ФИЛЬТРАЦИЯ С УЧЁТОМ БРЕНДОВ (для всех языков!)
        
        КЛЮЧЕВАЯ ЛОГИКА:
        1. АНГЛИЙСКИЙ seed → убираем ВСЁ кроме a-z (кириллицу, äöü, àâ...)
        2. ЛЮБОЙ ДРУГОЙ язык → ОСТАВЛЯЕМ латиницу для БРЕНДОВ (dyson, samsung, bosch...)
        3. Убираем редкие буквы (ъ, ё, ы)
        
        Примеры:
        - "vacuum repair" (EN) → [a-z, 0-9] (убрали 40+ символов)
        - "ремонт пылесосов" (RU) → [a-z, а-я, 0-9] (оставили a-z для брендов!)
        - "reparatur" (DE) → [a-z, äöüß, 0-9] (оставили a-z для брендов!)
        - "réparation" (FR) → [a-z, àâ..., 0-9] (оставили a-z для брендов!)
        
        Бренды почти всегда латиница: dyson, samsung, lg, bosch, apple, philips...
        """
        seed_lang = self.detect_seed_language(seed)
        
        # Базовая латиница a-z
        base_latin = list("abcdefghijklmnopqrstuvwxyz")
        
        # Цифры
        numbers = list("0123456789") if use_numbers else []
        
        # Языковые модификаторы (кириллица + спецсимволы)
        lang_specific = self.language_modifiers.get(language.lower(), [])
        
        # УМНАЯ ФИЛЬТРАЦИЯ С УЧЁТОМ БРЕНДОВ:
        
        if language.lower() == 'en' and seed_lang == 'latin':
            # ===== ТОЛЬКО ДЛЯ АНГЛИЙСКОГО =====
            # Английский seed → убираем ВСЁ кроме a-z
            # "vacuum repair" → [a-z, 0-9], БЕЗ кириллицы, БЕЗ äöü, БЕЗ àâ
            modifiers = base_latin + numbers
            removed = len(lang_specific)
            print(f"🇬🇧 Английский seed → {len(modifiers)} модификаторов (убрали {removed} не-английских)")
        
        elif seed_lang == 'latin':
            # ===== ДРУГИЕ ЛАТИНСКИЕ ЯЗЫКИ =====
            # Латинский seed НЕ английский → убираем ТОЛЬКО кириллицу
            # "reparatur" (DE) → [a-z, äöüß, 0-9], БЕЗ кириллицы
            # ОСТАВЛЯЕМ a-z для брендов: bosch, siemens, miele
            
            # Фильтруем: убираем ТОЛЬКО кириллицу
            is_cyrillic = lambda c: (ord('а') <= ord(c) <= ord('я')) or c in ['ё', 'і', 'ї', 'є', 'ґ', 'ў']
            non_cyrillic = [m for m in lang_specific if not is_cyrillic(m)]
            
            modifiers = base_latin + non_cyrillic + numbers
            removed = len(lang_specific) - len(non_cyrillic)
            if removed > 0:
                print(f"🌍 {language.upper()} латинский seed → {len(modifiers)} модификаторов (убрали {removed} кириллических)")
            else:
                print(f"🌍 {language.upper()} латинский seed → {len(modifiers)} модификаторов")
        
        else:
            # ===== КИРИЛЛИЧЕСКИЕ ЯЗЫКИ =====
            # Кириллический seed → ОСТАВЛЯЕМ латиницу для БРЕНДОВ!
            # "ремонт пылесосов" → [a-z, а-я, 0-9]
            # НЕ убираем a-z потому что: "ремонт dyson", "ремонт samsung", "ремонт lg"
            modifiers = base_latin + lang_specific + numbers
            print(f"🇷🇺 {language.upper()} кириллический seed → {len(modifiers)} модификаторов (оставили латиницу для брендов!)")
        
        # Убираем редкие буквы для конкретного языка
        rare = self.rare_chars.get(language.lower(), [])
        if rare:
            before = len(modifiers)
            modifiers = [m for m in modifiers if m not in rare]
            removed = before - len(modifiers)
            if removed > 0:
                print(f"🗑️ Убрали {removed} редких букв: {rare}")
        
        return modifiers
    
    async def fetch_suggestions(self, query: str, country: str, language: str) -> List[str]:
        """Запрос к Google Autocomplete API"""
        params = {
            "client": "chrome",
            "q": query,
            "gl": country,
            "hl": language
        }
        headers = {
            "User-Agent": random.choice(USER_AGENTS)
        }
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(self.base_url, params=params, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, list) and len(data) > 1:
                        suggestions = [s for s in data[1] if isinstance(s, str)]
                        return suggestions
                
                return []
                
        except Exception as e:
            print(f"❌ Error fetching '{query}': {e}")
            return []
    
    async def fetch_with_delay(
        self, 
        modifier: str, 
        seed: str, 
        country: str, 
        language: str
    ) -> tuple:
        """Запрос с задержкой"""
        try:
            # Задержка 0.2-0.5 сек
            await asyncio.sleep(random.uniform(0.2, 0.5))
            
            # Реальный запрос
            query = f"{seed} {modifier}"
            results = await self.fetch_suggestions(query, country, language)
            
            return (modifier, results, True)
            
        except Exception as e:
            print(f"❌ Error with '{modifier}': {e}")
            return (modifier, [], False)
    
    async def parse_suffix(
        self,
        seed: str,
        country: str,
        language: str,
        use_numbers: bool = True,
        parallel_limit: int = 3
    ) -> Dict:
        """SUFFIX ПАРСИНГ С УМНОЙ ФИЛЬТРАЦИЕЙ"""
        start_time = time.time()
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"SUFFIX PARSER - SMART FILTERING (BRAND-AWARE)")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Country: {country.upper()}")
        print(f"Language: {language.upper()}")
        print(f"Use numbers: {use_numbers}")
        print(f"Delay: 0.2-0.5 сек")
        print(f"Parallel: {parallel_limit} потоков\n")
        
        # Получаем умно отфильтрованные модификаторы
        modifiers = self.get_modifiers(language, use_numbers, seed)
        
        print(f"\n📊 Модификаторы: {len(modifiers)}")
        print(f"  Pattern: '{seed} [modifier]'")
        print(f"  Примеры: {modifiers[:10]}...\n")
        print(f"{'='*60}")
        print(f"Начинаем параллельный парсинг...")
        print(f"{'='*60}\n")
        
        # Счётчики
        total_queries = 0
        total_results = 0
        successful_queries = 0
        failed_queries = 0
        
        # ПАРАЛЛЕЛЬНЫЙ ПАРСИНГ с Semaphore
        semaphore = asyncio.Semaphore(parallel_limit)
        
        async def fetch_limited(modifier):
            async with semaphore:
                return await self.fetch_with_delay(modifier, seed, country, language)
        
        # Создаём задачи
        tasks = [fetch_limited(modifier) for modifier in modifiers]
        
        # Запускаем все задачи параллельно
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"[{i+1}/{len(modifiers)}] ❌ EXCEPTION: {result}")
                failed_queries += 1
                total_queries += 1
                continue
            
            modifier, suggestions, success = result
            query = f"{seed} {modifier}"
            total_queries += 1
            
            if success:
                all_keywords.update(suggestions)
                total_results += len(suggestions)
                successful_queries += 1
                
                # Показываем первые 5 и те где есть результаты
                if i < 5 or len(suggestions) > 0:
                    print(f"[{i+1}/{len(modifiers)}] '{query}' → {len(suggestions)} results")
            else:
                failed_queries += 1
                print(f"[{i+1}/{len(modifiers)}] '{query}' → ❌ FAILED")
        
        # Время выполнения
        elapsed_time = time.time() - start_time
        
        # Итоговая статистика
        print(f"\n{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
        print(f"{'='*60}")
        print(f"Запросов: {total_queries}")
        print(f"  ✅ Успешных: {successful_queries}")
        print(f"  ❌ Провалено: {failed_queries}")
        print(f"Результатов (с дубликатами): {total_results}")
        print(f"Уникальных ключей: {len(all_keywords)}")
        print(f"Время выполнения: {elapsed_time:.2f} сек")
        print(f"Средняя скорость: {elapsed_time/total_queries:.2f} сек/запрос")
        print(f"Параллельных потоков: {parallel_limit}")
        print(f"{'='*60}\n")
        
        return {
            "method": "SUFFIX with Smart Filtering (Brand-Aware)",
            "seed": seed,
            "country": country,
            "language": language,
            "use_numbers": use_numbers,
            "delay_range": "0.2-0.5 sec",
            "parallel_limit": parallel_limit,
            "queries": total_queries,
            "successful_queries": successful_queries,
            "failed_queries": failed_queries,
            "total_results": total_results,
            "count": len(all_keywords),
            "keywords": sorted(list(all_keywords)),
            "elapsed_time": round(elapsed_time, 2),
            "avg_time_per_query": round(elapsed_time / total_queries, 2)
        }


# ============================================
# API ENDPOINTS
# ============================================

@app.get("/")
async def root():
    return {
        "api": "Google Autocomplete Parser - SUFFIX with Smart Filtering",
        "version": "3.4",
        "method": "SUFFIX: seed + [a-z, а-я, 0-9]",
        "optimization": "Smart Filtering (Brand-Aware) + Parallel (3-5) + Delay 0.2-0.5 sec",
        "features": {
            "smart_filtering": True,
            "brand_aware": True,
            "language_detection": True,
            "rare_chars_removal": True,
            "simple_parallel": True,
            "morphology": False,
            "infix": False
        },
        "endpoints": {
            "parse": "/api/parse",
            "quick_test": "/api/parse?seed=ремонт+пылесосов&country=UA&language=ru"
        }
    }


@app.get("/api/parse")
async def parse_suffix(
    seed: str = Query("ремонт пылесосов", description="Базовый запрос"),
    country: str = Query("UA", description="Код страны (UA, US, RU, DE...)"),
    language: str = Query("ru", description="Код языка (ru, en, uk, de...)"),
    use_numbers: bool = Query(False, description="Включить цифры 0-9"),
    parallel: int = Query(3, description="Количество параллельных потоков (1-5)", ge=1, le=5)
):
    """
    SUFFIX ПАРСИНГ С УМНОЙ ФИЛЬТРАЦИЕЙ (BRAND-AWARE)
    
    Паттерн: seed + modifier
    
    Умная фильтрация:
    - Английский seed → убираем всё кроме a-z
    - Другие языки → ОСТАВЛЯЕМ латиницу для БРЕНДОВ (dyson, samsung, bosch...)
    - Убираем редкие буквы (ъ, ё, ы)
    
    Оптимизация:
    - Параллелизм (3-5 потоков)
    - Задержка: 0.2-0.5 сек
    - Умная фильтрация модификаторов
    
    Ожидаемое ускорение:
    - Для английского: 4-5× (убираем ~40 модификаторов)
    - Для русского: 3× (параллелизм + задержки, БЕЗ потери брендов)
    """
    parser = SmartSuffixParser()
    
    result = await parser.parse_suffix(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers,
        parallel_limit=parallel
    )
    
    return result
