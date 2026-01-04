"""
GOOGLE AUTOCOMPLETE PARSER - SUFFIX WITH ADAPTIVE DELAY
SUFFIX парсинг с адаптивной задержкой и умной фильтрацией
Version: 3.5 Adaptive Delay
Задержка: 0.1-1.0 сек (адаптивная) + параллелизм (3-5 потоков) + умная фильтрация
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
import httpx
import asyncio
import time
import random

app = FastAPI(
    title="Google Autocomplete Parser - SUFFIX with Adaptive Delay", 
    version="3.5",
    description="SUFFIX парсинг с адаптивной задержкой (автоматически находит оптимум)"
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
# ADAPTIVE DELAY CLASS
# ============================================
class AdaptiveDelay:
    """
    Умное управление задержками с автоматической адаптацией
    
    Логика:
    - Начинаем с initial_delay (0.2 сек)
    - При успехе → уменьшаем на 5% (× 0.95)
    - При 429 → увеличиваем в 2 раза (× 2.0)
    - Границы: min_delay (0.1) до max_delay (1.0)
    - Автоматически находит оптимальную скорость!
    """
    
    def __init__(self, initial_delay=0.2, min_delay=0.1, max_delay=1.0):
        self.current_delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.initial_delay = initial_delay
        
        # Статистика
        self.total_requests = 0
        self.successful_requests = 0
        self.rate_limit_hits = 0
        self.delay_history = []
        
        # Параметры адаптации
        self.decrease_factor = 0.95  # При успехе уменьшаем на 5%
        self.increase_factor = 2.0   # При 429 увеличиваем в 2 раза
    
    async def wait(self):
        """Ждём текущую задержку и записываем в историю"""
        await asyncio.sleep(self.current_delay)
        self.delay_history.append(self.current_delay)
    
    def record_success(self):
        """Записать успешный запрос → УМЕНЬШАЕМ задержку"""
        self.total_requests += 1
        self.successful_requests += 1
        
        # Постепенно уменьшаем задержку (работаем быстрее)
        old_delay = self.current_delay
        self.current_delay = max(
            self.min_delay,
            self.current_delay * self.decrease_factor
        )
        
        # Логируем только существенные изменения
        if old_delay - self.current_delay > 0.05:
            print(f"🟢 Ускоряемся: {old_delay:.3f} → {self.current_delay:.3f} сек")
    
    def record_rate_limit(self):
        """Записать rate limit (429) → УВЕЛИЧИВАЕМ задержку"""
        self.total_requests += 1
        self.rate_limit_hits += 1
        
        # Резко увеличиваем задержку (защита)
        old_delay = self.current_delay
        self.current_delay = min(
            self.max_delay,
            self.current_delay * self.increase_factor
        )
        
        print(f"🔴 Rate limit! Замедляемся: {old_delay:.3f} → {self.current_delay:.3f} сек")
    
    def record_error(self):
        """Записать другую ошибку"""
        self.total_requests += 1
    
    def get_stats(self):
        """Получить статистику адаптивной задержки"""
        avg_delay = sum(self.delay_history) / len(self.delay_history) if self.delay_history else 0
        min_delay_used = min(self.delay_history) if self.delay_history else 0
        max_delay_used = max(self.delay_history) if self.delay_history else 0
        
        return {
            "initial_delay": self.initial_delay,
            "final_delay": self.current_delay,
            "avg_delay": round(avg_delay, 3),
            "min_delay_used": round(min_delay_used, 3),
            "max_delay_used": round(max_delay_used, 3),
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "rate_limit_hits": self.rate_limit_hits,
            "success_rate": round(self.successful_requests / self.total_requests * 100, 1) if self.total_requests > 0 else 0
        }


# ============================================
# SMART SUFFIX PARSER (BRAND-AWARE)
# ============================================
class SmartSuffixParser:
    def __init__(self):
        self.base_url = "https://suggestqueries.google.com/complete/search"
        
        # Adaptive Delay (начинаем с 0.2 сек, можем до 0.1 сек)
        self.adaptive_delay = AdaptiveDelay(
            initial_delay=0.2,
            min_delay=0.1,
            max_delay=1.0
        )
        
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
    
    async def fetch_suggestions(self, query: str, country: str, language: str) -> tuple:
        """
        Запрос к Google Autocomplete API с адаптивной задержкой
        
        Returns:
            (suggestions, success, is_rate_limit)
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
                            return (suggestions, True, False)
                        
                        return ([], True, False)
                    
                    elif response.status_code == 429:  # Too Many Requests
                        # Rate limit! Возвращаем специальный флаг
                        if attempt < max_retries - 1:
                            # Ждём с exponential backoff
                            wait_time = (2 ** attempt)  # 1, 2, 4 секунды
                            print(f"⚠️ Rate limit (попытка {attempt+1}/{max_retries}). Ждём {wait_time} сек...")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            # Последняя попытка - возвращаем rate limit флаг
                            return ([], False, True)
                    
                    return ([], True, False)
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                else:
                    print(f"❌ Error fetching '{query}': {e}")
                    return ([], False, False)
        
        return ([], False, False)
    
    async def fetch_with_delay(
        self, 
        modifier: str, 
        seed: str, 
        country: str, 
        language: str
    ) -> tuple:
        """Запрос с АДАПТИВНОЙ задержкой"""
        try:
            # АДАПТИВНАЯ задержка
            await self.adaptive_delay.wait()
            
            # Реальный запрос
            query = f"{seed} {modifier}"
            results, success, is_rate_limit = await self.fetch_suggestions(query, country, language)
            
            # Обновляем адаптивную задержку
            if is_rate_limit:
                # Rate limit → увеличиваем задержку
                self.adaptive_delay.record_rate_limit()
                return (modifier, [], False)
            elif success:
                # Успех → уменьшаем задержку
                self.adaptive_delay.record_success()
                return (modifier, results, True)
            else:
                # Другая ошибка
                self.adaptive_delay.record_error()
                return (modifier, [], False)
            
        except Exception as e:
            self.adaptive_delay.record_error()
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
        print(f"Delay: 0.1-1.0 сек (адаптивная)")
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
        
        # Статистика адаптивной задержки
        delay_stats = self.adaptive_delay.get_stats()
        
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
        print(f"\n🧠 ADAPTIVE DELAY:")
        print(f"  Начальная задержка: {delay_stats['initial_delay']:.3f} сек")
        print(f"  Финальная задержка: {delay_stats['final_delay']:.3f} сек")
        print(f"  Средняя задержка: {delay_stats['avg_delay']:.3f} сек")
        print(f"  Диапазон: {delay_stats['min_delay_used']:.3f} - {delay_stats['max_delay_used']:.3f} сек")
        print(f"  Rate limit hits: {delay_stats['rate_limit_hits']}")
        print(f"  Success rate: {delay_stats['success_rate']}%")
        print(f"{'='*60}\n")
        
        return {
            "method": "SUFFIX with Smart Filtering (Brand-Aware) + Adaptive Delay",
            "seed": seed,
            "country": country,
            "language": language,
            "use_numbers": use_numbers,
            "adaptive_delay": delay_stats,
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
        "api": "Google Autocomplete Parser - SUFFIX with Adaptive Delay",
        "version": "3.5",
        "method": "SUFFIX: seed + [a-z, а-я, 0-9]",
        "optimization": "Adaptive Delay + Smart Filtering + Parallel (3-5)",
        "features": {
            "adaptive_delay": True,
            "auto_throttling": True,
            "exponential_backoff": True,
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


@app.get("/api/test-batching")
async def test_batching():
    """
    ЭКСПЕРИМЕНТАЛЬНЫЙ ТЕСТ БАТЧИНГА
    Проверяем все возможные способы батчинга Google Autocomplete API
    """
    results = {}
    base_url = "https://suggestqueries.google.com/complete/search"
    headers = {"User-Agent": USER_AGENTS[0]}
    
    # КОНТРОЛЬ: Обычный запрос
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                base_url,
                params={"client": "chrome", "q": "ремонт а", "gl": "UA", "hl": "ru"},
                headers=headers
            )
            results["control"] = {
                "method": "Обычный запрос (контроль)",
                "status": response.status_code,
                "works": response.status_code == 200,
                "response_sample": response.text[:200] if response.status_code == 200 else response.text
            }
            await asyncio.sleep(0.5)
    except Exception as e:
        results["control"] = {"method": "Обычный запрос", "error": str(e), "works": False}
    
    # МЕТОД 1: Массив в параметре q
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                base_url,
                params={"client": "chrome", "q": ["ремонт а", "ремонт б"], "gl": "UA", "hl": "ru"},
                headers=headers
            )
            results["array"] = {
                "method": "Массив в q",
                "status": response.status_code,
                "works": response.status_code == 200,
                "response_sample": response.text[:200]
            }
            await asyncio.sleep(0.5)
    except Exception as e:
        results["array"] = {"method": "Массив в q", "error": str(e), "works": False}
    
    # МЕТОД 2: Разделитель |
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                base_url,
                params={"client": "chrome", "q": "ремонт а|ремонт б|ремонт в", "gl": "UA", "hl": "ru"},
                headers=headers
            )
            results["pipe"] = {
                "method": "Разделитель |",
                "status": response.status_code,
                "works": response.status_code == 200,
                "response_sample": response.text[:200]
            }
            await asyncio.sleep(0.5)
    except Exception as e:
        results["pipe"] = {"method": "Разделитель |", "error": str(e), "works": False}
    
    # МЕТОД 3: POST запрос
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                base_url,
                json={"queries": ["ремонт а", "ремонт б"], "client": "chrome", "gl": "UA", "hl": "ru"},
                headers=headers
            )
            results["post"] = {
                "method": "POST запрос",
                "status": response.status_code,
                "works": response.status_code == 200,
                "response_sample": response.text[:200]
            }
    except Exception as e:
        results["post"] = {"method": "POST запрос", "error": str(e), "works": False}
    
    # Проверяем есть ли работающие методы батчинга
    batching_works = any(
        result.get("works") and result.get("method") != "Обычный запрос (контроль)" 
        for result in results.values()
    )
    
    return {
        "batching_supported": batching_works,
        "tested_methods": results,
        "conclusion": "БАТЧИНГ ПОДДЕРЖИВАЕТСЯ!" if batching_works else "Батчинг НЕ поддерживается - только 1 запрос за раз"
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
    SUFFIX ПАРСИНГ С АДАПТИВНОЙ ЗАДЕРЖКОЙ
    
    Паттерн: seed + modifier
    
    Адаптивная задержка:
    - Начало: 0.2 сек
    - При успехах → уменьшается до 0.1 сек (ускоряемся!)
    - При 429 → увеличивается до 1.0 сек (защита!)
    - Автоматически находит оптимум!
    
    Умная фильтрация:
    - Английский seed → убираем всё кроме a-z
    - Другие языки → ОСТАВЛЯЕМ латиницу для БРЕНДОВ (dyson, samsung, bosch...)
    - Убираем редкие буквы (ъ, ё, ы)
    
    Оптимизация:
    - Адаптивная задержка (0.1-1.0 сек)
    - Параллелизм (3-5 потоков)
    - Умная фильтрация модификаторов
    - Exponential backoff при rate limits
    
    Ожидаемое ускорение:
    - Для английского: 5-6×
    - Для русского: 3.5-4×
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
