"""
GOOGLE AUTOCOMPLETE PARSER - SUFFIX ONLY WITH ADAPTIVE PARALLEL
Только SUFFIX парсинг с умным параллелизмом
Version: 3.2 Adaptive Parallel
Задержка: 0.3-0.7 сек + адаптивная параллелизация (3-10 потоков)
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
import httpx
import asyncio
import time
import random

app = FastAPI(
    title="Google Autocomplete Parser - SUFFIX with Adaptive Parallel", 
    version="3.2",
    description="SUFFIX парсинг с адаптивным параллелизмом: автоматически находит безопасный предел"
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
# ADAPTIVE SEMAPHORE CLASS
# ============================================
class AdaptiveSemaphore:
    """
    Умный семафор который автоматически регулирует количество параллельных запросов
    
    Логика:
    - Начинаем с initial_limit (3 параллельных)
    - При успехах → постепенно увеличиваем до max_limit (10)
    - При ошибках → быстро снижаем до min_limit (1)
    - Автоматически находит оптимальный баланс скорость/безопасность
    """
    
    def __init__(self, initial_limit=3, min_limit=1, max_limit=10):
        self.current_limit = initial_limit
        self.min_limit = min_limit
        self.max_limit = max_limit
        self._semaphore = asyncio.Semaphore(initial_limit)
        self._lock = asyncio.Lock()
        
        # Счётчики для адаптации
        self.success_streak = 0  # Подряд успешных запросов
        self.error_count = 0     # Ошибки в текущем окне
        self.total_requests = 0
        
        # Параметры адаптации
        self.increase_threshold = 15  # После 15 успехов увеличиваем
        self.decrease_threshold = 3   # После 3 ошибок снижаем
    
    async def acquire(self):
        """Захватить семафор"""
        await self._semaphore.acquire()
    
    def release(self):
        """Освободить семафор"""
        self._semaphore.release()
    
    async def record_success(self):
        """Записать успешный запрос"""
        async with self._lock:
            self.success_streak += 1
            self.error_count = 0  # Сбрасываем ошибки
            self.total_requests += 1
            
            # Увеличиваем параллелизм если много успехов подряд
            if self.success_streak >= self.increase_threshold and self.current_limit < self.max_limit:
                await self._increase_limit()
                self.success_streak = 0
    
    async def record_error(self):
        """Записать ошибку"""
        async with self._lock:
            self.error_count += 1
            self.success_streak = 0  # Сбрасываем успехи
            self.total_requests += 1
            
            # Снижаем параллелизм если много ошибок
            if self.error_count >= self.decrease_threshold and self.current_limit > self.min_limit:
                await self._decrease_limit()
                self.error_count = 0
    
    async def _increase_limit(self):
        """Увеличить лимит параллельных запросов"""
        old_limit = self.current_limit
        self.current_limit = min(self.current_limit + 1, self.max_limit)
        
        # Пересоздаём семафор с новым лимитом
        # Ждём освобождения всех текущих слотов
        for _ in range(old_limit):
            await self._semaphore.acquire()
        
        self._semaphore = asyncio.Semaphore(self.current_limit)
        
        print(f"✅ Увеличиваем параллелизм: {old_limit} → {self.current_limit}")
    
    async def _decrease_limit(self):
        """Снизить лимит параллельных запросов"""
        old_limit = self.current_limit
        self.current_limit = max(self.current_limit - 1, self.min_limit)
        
        # Пересоздаём семафор с новым лимитом
        for _ in range(old_limit):
            await self._semaphore.acquire()
        
        self._semaphore = asyncio.Semaphore(self.current_limit)
        
        print(f"⚠️ Снижаем параллелизм: {old_limit} → {self.current_limit}")
    
    def get_stats(self):
        """Получить статистику"""
        return {
            "current_limit": self.current_limit,
            "total_requests": self.total_requests,
            "success_streak": self.success_streak,
            "error_count": self.error_count
        }


# ============================================
# SUFFIX PARSER CLASS WITH ADAPTIVE PARALLEL
# ============================================
class AdaptiveSuffixParser:
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
        
        # Adaptive Semaphore (начинаем с 3, можем до 10)
        self.adaptive_sem = AdaptiveSemaphore(
            initial_limit=3,
            min_limit=1,
            max_limit=10
        )
    
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
        """
        Запрос к Google Autocomplete API с retry логикой
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
        
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.get(self.base_url, params=params, headers=headers)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if isinstance(data, list) and len(data) > 1:
                            suggestions = [s for s in data[1] if isinstance(s, str)]
                            return suggestions
                    
                    elif response.status_code == 429:  # Too Many Requests
                        if attempt < max_retries - 1:
                            wait_time = (2 ** attempt)  # 1, 2, 4 секунды
                            print(f"⚠️ Rate limit! Ждём {wait_time} сек...")
                            await asyncio.sleep(wait_time)
                            continue
                    
                    return []
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                else:
                    print(f"❌ Error fetching '{query}': {e}")
                    return []
        
        return []
    
    async def fetch_with_semaphore(
        self, 
        modifier: str, 
        seed: str, 
        country: str, 
        language: str
    ) -> tuple:
        """
        Запрос с использованием Adaptive Semaphore
        
        Returns:
            (modifier, results, success)
        """
        # Захватываем слот семафора
        await self.adaptive_sem.acquire()
        
        try:
            # Задержка 0.3-0.7 сек
            await asyncio.sleep(random.uniform(0.3, 0.7))
            
            # Реальный запрос
            query = f"{seed} {modifier}"
            results = await self.fetch_suggestions(query, country, language)
            
            # Записываем успех
            await self.adaptive_sem.record_success()
            
            return (modifier, results, True)
            
        except Exception as e:
            # Записываем ошибку
            await self.adaptive_sem.record_error()
            print(f"❌ Error with '{modifier}': {e}")
            return (modifier, [], False)
            
        finally:
            # Освобождаем слот
            self.adaptive_sem.release()
    
    async def parse_suffix(
        self,
        seed: str,
        country: str,
        language: str,
        use_numbers: bool = True
    ) -> Dict:
        """
        SUFFIX ПАРСИНГ С ADAPTIVE PARALLEL
        """
        start_time = time.time()
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"SUFFIX PARSER - ADAPTIVE PARALLEL")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Country: {country.upper()}")
        print(f"Language: {language.upper()}")
        print(f"Use numbers: {use_numbers}")
        print(f"Delay: 0.3-0.7 сек")
        print(f"Parallel: адаптивно от 1 до 10 потоков\n")
        
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
        
        # ПАРАЛЛЕЛЬНЫЙ ПАРСИНГ с Adaptive Semaphore
        tasks = [
            self.fetch_with_semaphore(modifier, seed, country, language)
            for modifier in modifiers
        ]
        
        # Запускаем все задачи параллельно
        results = await asyncio.gather(*tasks)
        
        # Обрабатываем результаты
        for i, (modifier, suggestions, success) in enumerate(results):
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
        
        # Статистика семафора
        sem_stats = self.adaptive_sem.get_stats()
        
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
        print(f"\n🧠 ADAPTIVE SEMAPHORE:")
        print(f"  Финальный лимит: {sem_stats['current_limit']} параллельных потоков")
        print(f"  Успехов подряд: {sem_stats['success_streak']}")
        print(f"  Ошибок в окне: {sem_stats['error_count']}")
        print(f"{'='*60}\n")
        
        return {
            "method": "SUFFIX with Adaptive Parallel",
            "seed": seed,
            "country": country,
            "language": language,
            "use_numbers": use_numbers,
            "delay_range": "0.3-0.7 sec",
            "parallel": {
                "type": "adaptive",
                "final_limit": sem_stats['current_limit'],
                "min_limit": 1,
                "max_limit": 10
            },
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
        "api": "Google Autocomplete Parser - SUFFIX with Adaptive Parallel",
        "version": "3.2",
        "method": "SUFFIX: seed + [a-z, а-я, 0-9]",
        "optimization": "Adaptive Parallel (1-10 потоков) + Delay 0.3-0.7 sec",
        "features": {
            "adaptive_semaphore": True,
            "auto_throttling": True,
            "retry_logic": True,
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
    use_numbers: bool = Query(False, description="Включить цифры 0-9")
):
    """
    SUFFIX ПАРСИНГ С ADAPTIVE PARALLEL
    
    Паттерн: seed + modifier
    Оптимизация: 
    - Адаптивный параллелизм (1-10 потоков)
    - Автоматическое снижение при ошибках
    - Автоматическое увеличение при успехах
    - Задержка: 0.3-0.7 сек
    - Retry логика при rate limits
    
    Ожидаемое ускорение: 3-8× по сравнению с последовательным
    """
    parser = AdaptiveSuffixParser()
    
    result = await parser.parse_suffix(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers
    )
    
    return result
