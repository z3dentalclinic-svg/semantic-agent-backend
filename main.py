"""
GOOGLE AUTOCOMPLETE PARSER - SUFFIX WITH SIMPLE PARALLEL
SUFFIX парсинг с простым параллелизмом (БЕЗ адаптации)
Version: 3.2 Simple Parallel
Задержка: 0.3-0.7 сек + фиксированный параллелизм (3 потока)
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
import httpx
import asyncio
import time
import random

app = FastAPI(
    title="Google Autocomplete Parser - SUFFIX with Simple Parallel", 
    version="3.2",
    description="SUFFIX парсинг с простым параллелизмом (3 потока)"
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
# SIMPLE SUFFIX PARSER WITH PARALLEL
# ============================================
class SimpleSuffixParser:
    def __init__(self):
        self.base_url = "https://suggestqueries.google.com/complete/search"
        
        # Базовые модификаторы (латиница + цифры)
        self.base_modifiers = list("abcdefghijklmnopqrstuvwxyz0123456789")
        
        # Языковые модификаторы (кириллица и др.)
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
    
    def get_modifiers(self, language: str, use_numbers: bool = True) -> List[str]:
        """Получить все модификаторы для языка"""
        modifiers = self.base_modifiers.copy()
        
        # Добавляем языковые модификаторы
        lang_mods = self.language_modifiers.get(language.lower(), [])
        modifiers.extend(lang_mods)
        
        # Убираем цифры если нужно
        if not use_numbers:
            modifiers = [m for m in modifiers if not m.isdigit()]
        
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
            # Задержка 0.3-0.7 сек
            await asyncio.sleep(random.uniform(0.3, 0.7))
            
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
        """
        SUFFIX ПАРСИНГ С ПРОСТЫМ ПАРАЛЛЕЛИЗМОМ
        """
        start_time = time.time()
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"SUFFIX PARSER - SIMPLE PARALLEL")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Country: {country.upper()}")
        print(f"Language: {language.upper()}")
        print(f"Use numbers: {use_numbers}")
        print(f"Delay: 0.3-0.7 сек")
        print(f"Parallel: {parallel_limit} потоков\n")
        
        # Получаем модификаторы
        modifiers = self.get_modifiers(language, use_numbers)
        
        print(f"📊 Модификаторы: {len(modifiers)}")
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
            "method": "SUFFIX with Simple Parallel",
            "seed": seed,
            "country": country,
            "language": language,
            "use_numbers": use_numbers,
            "delay_range": "0.3-0.7 sec",
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
        "api": "Google Autocomplete Parser - SUFFIX with Simple Parallel",
        "version": "3.2",
        "method": "SUFFIX: seed + [a-z, а-я, 0-9]",
        "optimization": "Simple Parallel (3 потока) + Delay 0.3-0.7 sec",
        "features": {
            "simple_parallel": True,
            "fixed_semaphore": 3,
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
    SUFFIX ПАРСИНГ С ПРОСТЫМ ПАРАЛЛЕЛИЗМОМ
    
    Паттерн: seed + modifier
    Оптимизация: 
    - Фиксированный параллелизм (по умолчанию 3 потока)
    - Задержка: 0.3-0.7 сек
    - Без сложной адаптации
    
    Ожидаемое ускорение: 3× при parallel=3
    """
    parser = SimpleSuffixParser()
    
    result = await parser.parse_suffix(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers,
        parallel_limit=parallel
    )
    
    return result
