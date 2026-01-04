"""
GOOGLE AUTOCOMPLETE PARSER - SUFFIX ONLY
Только SUFFIX парсинг без морфологии и INFIX
Version: 3.1 Clean SUFFIX
Задержка: 0.3-0.7 сек (оптимизированная)
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
import httpx
import asyncio
import time
import random

app = FastAPI(
    title="Google Autocomplete Parser - SUFFIX Only", 
    version="3.1",
    description="Чистый SUFFIX парсинг: seed + [a-z, а-я, 0-9]"
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
# SUFFIX PARSER CLASS
# ============================================
class SuffixParser:
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
        """
        Получить все модификаторы для языка
        
        Args:
            language: код языка (ru, en, uk, de...)
            use_numbers: включить цифры 0-9
            
        Returns:
            список модификаторов [a-z, а-я, 0-9] в зависимости от языка
        """
        modifiers = self.base_modifiers.copy()
        
        # Добавляем языковые модификаторы
        lang_mods = self.language_modifiers.get(language.lower(), [])
        modifiers.extend(lang_mods)
        
        # Убираем цифры если нужно
        if not use_numbers:
            modifiers = [m for m in modifiers if not m.isdigit()]
        
        return modifiers
    
    async def fetch_suggestions(self, query: str, country: str, language: str) -> List[str]:
        """
        Базовый запрос к Google Autocomplete API
        
        Args:
            query: поисковый запрос
            country: код страны (UA, US, RU, DE...)
            language: код языка (ru, en, uk, de...)
            
        Returns:
            список подсказок Google
        """
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
                    
                    # Формат ответа: ["query", ["suggestion1", "suggestion2", ...]]
                    if isinstance(data, list) and len(data) > 1:
                        suggestions = [s for s in data[1] if isinstance(s, str)]
                        return suggestions
                
                return []
                
        except Exception as e:
            print(f"❌ Error fetching '{query}': {e}")
            return []
    
    async def parse_suffix(
        self,
        seed: str,
        country: str,
        language: str,
        use_numbers: bool = True
    ) -> Dict:
        """
        SUFFIX ПАРСИНГ БЕЗ МОРФОЛОГИИ
        
        Паттерн: "seed + modifier"
        
        Args:
            seed: базовый запрос (например "ремонт пылесосов")
            country: код страны (UA, US, RU, DE...)
            language: код языка (ru, en, uk, de...)
            use_numbers: включить цифры 0-9 в модификаторы
            
        Returns:
            dict с результатами парсинга
        """
        start_time = time.time()
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"SUFFIX PARSER - ЧИСТЫЙ (БЕЗ МОРФОЛОГИИ И INFIX)")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Country: {country.upper()}")
        print(f"Language: {language.upper()}")
        print(f"Use numbers: {use_numbers}")
        print(f"Delay: 0.3-0.7 сек\n")
        
        # Получаем модификаторы
        modifiers = self.get_modifiers(language, use_numbers)
        
        print(f"📊 Модификаторы: {len(modifiers)}")
        print(f"  Pattern: '{seed} [modifier]'")
        print(f"  Примеры: {modifiers[:10]}... (показано первые 10)\n")
        print(f"{'='*60}")
        print(f"Начинаем парсинг...")
        print(f"{'='*60}\n")
        
        # Счётчики
        total_queries = 0
        total_results = 0
        
        # SUFFIX парсинг
        for i, modifier in enumerate(modifiers):
            query = f"{seed} {modifier}"
            
            # Запрос к Google Autocomplete
            results = await self.fetch_suggestions(query, country, language)
            
            # Добавляем результаты
            all_keywords.update(results)
            total_results += len(results)
            total_queries += 1
            
            # Логирование (показываем первые 5 и те где есть results)
            if i < 5 or len(results) > 0:
                print(f"[{i+1}/{len(modifiers)}] '{query}' → {len(results)} results")
            
            # ЗАДЕРЖКА 0.3-0.7 сек (оптимизированная!)
            await asyncio.sleep(random.uniform(0.3, 0.7))
        
        # Время выполнения
        elapsed_time = time.time() - start_time
        
        # Итоговая статистика
        print(f"\n{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
        print(f"{'='*60}")
        print(f"Запросов: {total_queries}")
        print(f"Результатов (с дубликатами): {total_results}")
        print(f"Уникальных ключей: {len(all_keywords)}")
        print(f"Время выполнения: {elapsed_time:.2f} сек")
        print(f"Средняя скорость: {elapsed_time/total_queries:.2f} сек/запрос")
        print(f"{'='*60}\n")
        
        return {
            "method": "SUFFIX (no morphology, no INFIX)",
            "seed": seed,
            "country": country,
            "language": language,
            "use_numbers": use_numbers,
            "delay_range": "0.3-0.7 sec",
            "queries": total_queries,
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
        "api": "Google Autocomplete Parser - SUFFIX Only",
        "version": "3.1",
        "method": "SUFFIX: seed + [a-z, а-я, 0-9]",
        "optimization": "Delay: 0.3-0.7 sec (оптимизированная)",
        "features": {
            "morphology": False,
            "infix": False,
            "prefix": False
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
    use_numbers: bool = Query(False, description="Включить цифры 0-9")
):
    """
    ЧИСТЫЙ SUFFIX ПАРСИНГ
    
    Паттерн: seed + modifier
    - seed = "ремонт пылесосов"
    - modifiers = [a, b, c, ..., z, а, б, в, ..., я, 0, 1, ..., 9]
    - queries = ["ремонт пылесосов a", "ремонт пылесосов b", ...]
    
    БЕЗ морфологии
    БЕЗ INFIX
    ЗАДЕРЖКА: 0.3-0.7 сек
    """
    parser = SuffixParser()
    
    result = await parser.parse_suffix(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers
    )
    
    return result
