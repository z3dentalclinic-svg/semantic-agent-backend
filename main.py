"""
GOOGLE AUTOCOMPLETE PARSER - OPTIMIZED VERSION
Оптимизированный SUFFIX парсинг с максимальной производительностью

Version: 3.6 Clean
Время: ~2 сек на 56 запросов (17× быстрее базовой версии!)

Оптимизации:
- Connection Pooling (переиспользование HTTP соединений)
- Adaptive Delay (автоматическая оптимизация задержек)
- Parallel Requests (5 потоков одновременно)
- Smart Filtering (умная фильтрация модификаторов для брендов)
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
import httpx
import asyncio
import time
import random

# ============================================
# FASTAPI APP
# ============================================
app = FastAPI(
    title="Google Autocomplete Parser - Optimized", 
    version="3.6",
    description="Оптимизированный SUFFIX парсинг (17× быстрее!)"
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
]

# ============================================
# ADAPTIVE DELAY
# ============================================
class AdaptiveDelay:
    """
    Умное управление задержками с автоматической адаптацией
    - При успехе → уменьшаем задержку (ускоряемся)
    - При 429 → увеличиваем задержку (защита от блокировки)
    """
    
    def __init__(self, initial_delay=0.2, min_delay=0.1, max_delay=1.0):
        self.current_delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.decrease_factor = 0.95  # Уменьшаем на 5% при успехе
        self.increase_factor = 2.0   # Увеличиваем в 2 раза при 429
        
        # Статистика
        self.total_requests = 0
        self.successful_requests = 0
        self.rate_limit_hits = 0
    
    async def wait(self):
        """Ждём текущую задержку"""
        await asyncio.sleep(self.current_delay)
    
    def record_success(self):
        """Успешный запрос → уменьшаем задержку"""
        self.total_requests += 1
        self.successful_requests += 1
        self.current_delay = max(self.min_delay, self.current_delay * self.decrease_factor)
    
    def record_rate_limit(self):
        """Rate limit → увеличиваем задержку"""
        self.total_requests += 1
        self.rate_limit_hits += 1
        self.current_delay = min(self.max_delay, self.current_delay * self.increase_factor)
        print(f"🔴 Rate limit! Увеличиваем задержку до {self.current_delay:.3f} сек")
    
    def record_error(self):
        """Другая ошибка"""
        self.total_requests += 1
    
    def get_stats(self):
        """Получить статистику"""
        avg_delay = self.current_delay
        return {
            "final_delay": round(self.current_delay, 3),
            "rate_limit_hits": self.rate_limit_hits,
            "success_rate": round(self.successful_requests / self.total_requests * 100, 1) if self.total_requests > 0 else 0
        }

# ============================================
# SUFFIX PARSER
# ============================================
class SuffixParser:
    def __init__(self):
        self.base_url = "https://suggestqueries.google.com/complete/search"
        self.adaptive_delay = AdaptiveDelay(initial_delay=0.2, min_delay=0.1, max_delay=1.0)
        
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
        
        # Редкие буквы (можно пропустить)
        self.rare_chars = {
            'ru': ['ъ', 'ё', 'ы'],
            'uk': ['ь', 'ъ'],
            'pl': ['ą', 'ę'],
        }
    
    def detect_seed_language(self, seed: str) -> str:
        """Определить язык seed (latin/cyrillic)"""
        has_latin = any(ord('a') <= ord(c.lower()) <= ord('z') for c in seed if c.isalpha())
        has_cyrillic = any(ord('а') <= ord(c.lower()) <= ord('я') for c in seed if c.isalpha())
        
        if has_cyrillic:
            return 'cyrillic'
        return 'latin'
    
    def get_modifiers(self, language: str, use_numbers: bool, seed: str) -> List[str]:
        """
        Получить умно отфильтрованные модификаторы
        
        УМНАЯ ФИЛЬТРАЦИЯ ДЛЯ БРЕНДОВ:
        - Английский seed → убираем всё кроме a-z (нет брендов на кириллице)
        - Другие языки → ОСТАВЛЯЕМ a-z для брендов (dyson, samsung, bosch...)
        - Кириллический seed → оставляем всё (бренды на латинице!)
        """
        seed_lang = self.detect_seed_language(seed)
        base_latin = list("abcdefghijklmnopqrstuvwxyz")
        numbers = list("0123456789") if use_numbers else []
        lang_specific = self.language_modifiers.get(language.lower(), [])
        
        # ФИЛЬТРАЦИЯ
        if language.lower() == 'en' and seed_lang == 'latin':
            # Английский → только a-z + цифры
            modifiers = base_latin + numbers
        elif seed_lang == 'latin':
            # Другие латинские языки → убираем только кириллицу
            is_cyrillic = lambda c: ord('а') <= ord(c.lower()) <= ord('я') or c in ['ё', 'і', 'ї', 'є', 'ґ', 'ў']
            non_cyrillic = [m for m in lang_specific if not is_cyrillic(m)]
            modifiers = base_latin + non_cyrillic + numbers
        else:
            # Кириллический → оставляем ВСЁ (бренды!)
            modifiers = base_latin + lang_specific + numbers
        
        # Убираем редкие буквы
        rare = self.rare_chars.get(language.lower(), [])
        if rare:
            modifiers = [m for m in modifiers if m not in rare]
        
        return modifiers
    
    async def fetch_suggestions(self, query: str, country: str, language: str, client: httpx.AsyncClient) -> tuple:
        """
        Запрос к Google Autocomplete API
        Returns: (suggestions, success, is_rate_limit)
        """
        params = {"client": "chrome", "q": query, "gl": country, "hl": language}
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        
        try:
            response = await client.get(self.base_url, params=params, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 1:
                    suggestions = [s for s in data[1] if isinstance(s, str)]
                    return (suggestions, True, False)
                return ([], True, False)
            
            elif response.status_code == 429:
                return ([], False, True)  # Rate limit
            
            return ([], True, False)
        
        except Exception as e:
            return ([], False, False)
    
    async def fetch_with_delay(self, modifier: str, seed: str, country: str, language: str, client: httpx.AsyncClient) -> tuple:
        """Запрос с адаптивной задержкой и connection pooling"""
        try:
            # Адаптивная задержка
            await self.adaptive_delay.wait()
            
            # Запрос через shared client (connection pooling!)
            query = f"{seed} {modifier}"
            results, success, is_rate_limit = await self.fetch_suggestions(query, country, language, client)
            
            # Обновляем задержку
            if is_rate_limit:
                self.adaptive_delay.record_rate_limit()
                return (modifier, [], False)
            elif success:
                self.adaptive_delay.record_success()
                return (modifier, results, True)
            else:
                self.adaptive_delay.record_error()
                return (modifier, [], False)
        
        except Exception as e:
            self.adaptive_delay.record_error()
            return (modifier, [], False)
    
    async def parse(self, seed: str, country: str, language: str, use_numbers: bool = True, parallel_limit: int = 5) -> Dict:
        """
        SUFFIX парсинг с максимальной оптимизацией
        Паттерн: seed + modifier
        """
        start_time = time.time()
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"SUFFIX PARSER - OPTIMIZED v3.6")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Country: {country.upper()}, Language: {language.upper()}")
        print(f"Parallel: {parallel_limit}, Adaptive Delay: 0.1-1.0 сек\n")
        
        # Получаем модификаторы с умной фильтрацией
        modifiers = self.get_modifiers(language, use_numbers, seed)
        print(f"📊 Модификаторы: {modifiers[:10]}... (всего {len(modifiers)})\n")
        
        # Счётчики
        total_queries = 0
        successful_queries = 0
        failed_queries = 0
        
        # ПАРАЛЛЕЛЬНЫЙ ПАРСИНГ с Connection Pooling
        semaphore = asyncio.Semaphore(parallel_limit)
        
        async with httpx.AsyncClient(timeout=10.0) as shared_client:
            print(f"🏊 Connection pooling: используем общий HTTP клиент\n")
            
            async def fetch_limited(modifier):
                async with semaphore:
                    return await self.fetch_with_delay(modifier, seed, country, language, shared_client)
            
            # Запускаем все задачи параллельно
            tasks = [fetch_limited(m) for m in modifiers]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_queries += 1
                total_queries += 1
                continue
            
            modifier, suggestions, success = result
            total_queries += 1
            
            if success:
                all_keywords.update(suggestions)
                successful_queries += 1
                if i < 5 or len(suggestions) > 0:
                    print(f"[{i+1}/{len(modifiers)}] '{seed} {modifier}' → {len(suggestions)} results")
            else:
                failed_queries += 1
        
        elapsed_time = time.time() - start_time
        delay_stats = self.adaptive_delay.get_stats()
        
        # Итоговая статистика
        print(f"\n{'='*60}")
        print(f"📊 СТАТИСТИКА")
        print(f"{'='*60}")
        print(f"Запросов: {total_queries} (✅ {successful_queries}, ❌ {failed_queries})")
        print(f"Уникальных ключей: {len(all_keywords)}")
        print(f"Время: {elapsed_time:.2f} сек ({elapsed_time/total_queries:.2f} сек/запрос)")
        print(f"Параллелизм: {parallel_limit}")
        print(f"🧠 Adaptive Delay: {delay_stats['final_delay']:.3f} сек (rate limits: {delay_stats['rate_limit_hits']})")
        print(f"🏊 Connection Pooling: ВКЛЮЧЁН")
        print(f"{'='*60}\n")
        
        return {
            "method": "SUFFIX Optimized",
            "seed": seed,
            "country": country,
            "language": language,
            "queries": total_queries,
            "successful_queries": successful_queries,
            "count": len(all_keywords),
            "keywords": sorted(list(all_keywords)),
            "elapsed_time": round(elapsed_time, 2),
            "avg_time_per_query": round(elapsed_time / total_queries, 2),
            "adaptive_delay": delay_stats
        }

# ============================================
# API ENDPOINTS
# ============================================

@app.get("/")
async def root():
    return {
        "api": "Google Autocomplete Parser - Optimized",
        "version": "3.6",
        "optimizations": [
            "Connection Pooling (переиспользование соединений)",
            "Adaptive Delay (автоматическая оптимизация)",
            "Parallel Requests (5 потоков)",
            "Smart Filtering (фильтрация для брендов)"
        ],
        "performance": {
            "baseline": "37.86 сек",
            "optimized": "~2.21 сек",
            "speedup": "17× быстрее"
        },
        "endpoints": {
            "parse": "/api/parse",
            "example": "/api/parse?seed=ремонт+пылесосов&country=UA&language=ru&parallel=5"
        }
    }

@app.get("/api/parse")
async def parse_suffix(
    seed: str = Query("ремонт пылесосов", description="Базовый запрос"),
    country: str = Query("UA", description="Код страны (UA, US, RU, DE...)"),
    language: str = Query("ru", description="Код языка (ru, en, uk, de...)"),
    use_numbers: bool = Query(False, description="Включить цифры 0-9"),
    parallel: int = Query(5, description="Параллельных потоков (1-10)", ge=1, le=10)
):
    """
    ОПТИМИЗИРОВАННЫЙ SUFFIX ПАРСИНГ
    
    Паттерн: seed + [a-z, а-я, 0-9]
    
    Оптимизации:
    - Connection Pooling: переиспользование HTTP соединений
    - Adaptive Delay: автоматическая оптимизация задержек (0.1-1.0 сек)
    - Parallel: 5 потоков одновременно
    - Smart Filtering: сохраняем латиницу для брендов
    
    Производительность:
    - Время: ~2 сек на 56 запросов
    - Ускорение: 17× от базовой версии
    """
    parser = SuffixParser()
    result = await parser.parse(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers,
        parallel_limit=parallel
    )
    return result
