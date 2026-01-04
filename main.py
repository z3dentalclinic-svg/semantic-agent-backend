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
# KEYWORD PARSER (SUFFIX + INFIX)
# ============================================
class KeywordParser:
    def __init__(self):
        self.base_url = "https://suggestqueries.google.com/complete/search"
        self.adaptive_delay = AdaptiveDelay(initial_delay=0.2, min_delay=0.1, max_delay=1.0)
        
        # Языковые модификаторы (кириллица и спецсимволы)
        self.language_modifiers = {
            'en': [],
            'ru': list("абвгдеёжзийклмнопрстуфхцчшщъыьэюя"),  # Полный русский алфавит!
            'uk': list("абвгдежзийклмнопрстуфхцчшщьюяіїєґ"),
            'de': list("äöüß"),
            'fr': list("àâäæçéèêëïîôùûüÿ"),
            'es': list("áéíñóúü"),
            'pl': list("ąćęłńóśźż"),
            'it': list("àèéìíîòóùú"),
        }
        
        # Редкие буквы (можно пропустить)
        self.rare_chars = {
            'ru': ['ъ', 'ё', 'ы'],  # Убрал 'ь' чтобы было 56 как вчера
            'uk': ['ъ'],  # Убрал 'ь' чтобы не терять модификаторы
            'pl': ['ą', 'ę'],
        }
    
    def detect_seed_language(self, seed: str) -> str:
        """Определить язык seed (latin/cyrillic)"""
        has_latin = any(ord('a') <= ord(c.lower()) <= ord('z') for c in seed if c.isalpha())
        has_cyrillic = any(ord('а') <= ord(c.lower()) <= ord('я') for c in seed if c.isalpha())
        
        if has_cyrillic:
            return 'cyrillic'
        return 'latin'
    
    def get_modifiers(self, language: str, use_numbers: bool, seed: str, cyrillic_only: bool = False) -> List[str]:
        """
        Получить умно отфильтрованные модификаторы
        
        УМНАЯ ФИЛЬТРАЦИЯ ДЛЯ БРЕНДОВ:
        - Английский seed → убираем всё кроме a-z (нет брендов на кириллице)
        - Другие латинские seed → убираем кириллицу (но оставляем спецсимволы языка)
        - Кириллический seed → ОСТАВЛЯЕМ ВСЁ (латиницу для брендов + кириллицу!)
        
        cyrillic_only: Только кириллица (для INFIX - латиница в середине бесполезна)
        """
        seed_lang = self.detect_seed_language(seed)
        base_latin = list("abcdefghijklmnopqrstuvwxyz")
        numbers = list("0123456789") if use_numbers else []
        lang_specific = self.language_modifiers.get(language.lower(), [])
        
        # ФИЛЬТРАЦИЯ
        if language.lower() == 'en' and seed_lang == 'latin':
            # Английский seed → только a-z + цифры
            modifiers = base_latin + numbers
            
        elif seed_lang == 'latin':
            # Латинский seed (не английский) → убираем ТОЛЬКО кириллицу
            is_cyrillic = lambda c: ord('а') <= ord(c.lower()) <= ord('я') or c in ['ё', 'і', 'ї', 'є', 'ґ', 'ў']
            non_cyrillic = [m for m in lang_specific if not is_cyrillic(m)]
            modifiers = base_latin + non_cyrillic + numbers
            
        else:
            # КИРИЛЛИЧЕСКИЙ seed → оставляем ВСЁ (латиницу для брендов!)
            modifiers = base_latin + lang_specific + numbers
        
        # Убираем редкие буквы
        rare = self.rare_chars.get(language.lower(), [])
        if rare:
            modifiers = [m for m in modifiers if m not in rare]
        
        # ТОЛЬКО КИРИЛЛИЦА (для INFIX)
        if cyrillic_only and seed_lang == 'cyrillic':
            # Убираем латиницу и цифры
            is_cyrillic = lambda c: ord('а') <= ord(c.lower()) <= ord('я') or c in ['ё', 'і', 'ї', 'є', 'ґ', 'ў', 'ь']
            modifiers = [m for m in modifiers if is_cyrillic(m)]
        
        return modifiers
    
    def get_morphological_forms(self, word: str, language: str) -> List[str]:
        """
        Получить морфологические формы слова для разных языков
        
        Поддерживаемые языки:
        - RU, UK: pymorphy3 (все формы из лексемы)
        - EN: lemminflect (единственное/множественное + притяжательные формы)
        - Другие: базовая морфология (простые правила)
        """
        forms = set([word])  # Всегда включаем исходную форму
        
        try:
            if language.lower() in ['ru', 'uk']:
                # Русский и Украинский - pymorphy3
                try:
                    import pymorphy3
                    
                    # Создаём анализатор
                    morph = pymorphy3.MorphAnalyzer()
                    
                    # Парсим слово
                    parsed = morph.parse(word)
                    
                    if parsed:
                        # Берем первый вариант разбора
                        p = parsed[0]
                        
                        # Получаем все формы из лексемы
                        for form in p.lexeme:
                            forms.add(form.word)
                    
                    print(f"📖 Морфология (RU/UK): '{word}' → {len(forms)} форм через pymorphy3")
                    
                except ImportError:
                    print(f"⚠️ pymorphy3 не установлен. Используем только базовую форму.")
                    print(f"   Установите: pip install pymorphy3 pymorphy3-dicts-ru")
            
            elif language.lower() == 'en':
                # Английский - lemminflect или базовые правила
                try:
                    import lemminflect
                    
                    # Получаем различные формы через lemminflect
                    # Множественное число
                    plurals = lemminflect.getAllInflections(word, upos='NOUN')
                    if plurals and 'NNS' in plurals:
                        forms.update(plurals['NNS'])
                    
                    # Притяжательная форма
                    if not word.endswith('s'):
                        forms.add(word + "'s")
                        forms.add(word + "s")
                    
                    print(f"📖 Морфология (EN): '{word}' → {len(forms)} форм (lemminflect)")
                    
                except ImportError:
                    # Fallback - базовые правила
                    forms.add(word)  # singular
                    
                    # Множественное число (простые правила)
                    if word.endswith('y') and len(word) > 1 and word[-2] not in 'aeiou':
                        plural = word[:-1] + 'ies'  # baby → babies
                    elif word.endswith(('s', 'x', 'z', 'ch', 'sh')):
                        plural = word + 'es'  # box → boxes
                    elif word.endswith('o') and len(word) > 1 and word[-2] not in 'aeiou':
                        plural = word + 'es'  # hero → heroes
                    elif word.endswith('f'):
                        plural = word[:-1] + 'ves'  # leaf → leaves
                    elif word.endswith('fe'):
                        plural = word[:-2] + 'ves'  # knife → knives
                    else:
                        plural = word + 's'  # regular
                    
                    forms.add(plural)
                    
                    # Притяжательная форма
                    if not word.endswith('s'):
                        forms.add(word + "'s")
                    
                    print(f"📖 Морфология (EN): '{word}' → {len(forms)} форм (базовые правила)")
                    print(f"   Рекомендуется установить: pip install lemminflect")
            
            else:
                # Другие языки - только базовая форма
                print(f"📖 Морфология ({language.upper()}): '{word}' → 1 форма (морфология не поддерживается)")
                print(f"   Для полной поддержки требуется spacy + языковая модель")
        
        except Exception as e:
            print(f"⚠️ Ошибка морфологии: {e}")
        
        return sorted(list(forms))
    
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
    
    async def fetch_with_delay(self, modifier: str, seed: str, country: str, language: str, client: httpx.AsyncClient, custom_query: str = None) -> tuple:
        """Запрос с адаптивной задержкой и connection pooling"""
        try:
            # Адаптивная задержка
            await self.adaptive_delay.wait()
            
            # Запрос через shared client (connection pooling!)
            query = custom_query if custom_query else f"{seed} {modifier}"
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
            "modifiers_count": len(modifiers),  # ДОБАВИЛИ!
            "modifiers_sample": modifiers[:10],  # ДОБАВИЛИ!
            "queries": total_queries,
            "successful_queries": successful_queries,
            "count": len(all_keywords),
            "keywords": sorted(list(all_keywords)),
            "elapsed_time": round(elapsed_time, 2),
            "avg_time_per_query": round(elapsed_time / total_queries, 2),
            "adaptive_delay": delay_stats
        }
    
    async def parse_infix(self, seed: str, country: str, language: str, use_numbers: bool = True, parallel_limit: int = 5) -> Dict:
        """
        INFIX парсинг с максимальной оптимизацией
        Паттерн: word1 + modifier + word2
        
        Пример:
        Seed: "ремонт пылесосов"
        Разбивается на: ["ремонт", "пылесосов"]
        Запросы: "ремонт а пылесосов", "ремонт б пылесосов", ...
        """
        start_time = time.time()
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"INFIX PARSER - OPTIMIZED v3.6")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Country: {country.upper()}, Language: {language.upper()}")
        print(f"Parallel: {parallel_limit}, Adaptive Delay: 0.1-1.0 сек\n")
        
        # Разбиваем seed на слова
        words = seed.strip().split()
        
        if len(words) < 2:
            return {
                "error": "INFIX требует минимум 2 слова в seed",
                "example": "ремонт пылесосов (2 слова) ✅",
                "your_seed": f"{seed} ({len(words)} слов) ❌"
            }
        
        # Получаем модификаторы (ТОЛЬКО КИРИЛЛИЦА для INFIX!)
        modifiers = self.get_modifiers(language, use_numbers=False, seed=seed, cyrillic_only=True)
        print(f"📊 Модификаторы: {modifiers[:10]}... (всего {len(modifiers)})")
        print(f"📊 Паттерн INFIX: '{words[0]}' + modifier + '{' '.join(words[1:])}'")
        print(f"📊 Пример: '{words[0]} {modifiers[0] if modifiers else 'а'} {' '.join(words[1:])}'")
        
        # Счётчики
        total_queries = 0
        successful_queries = 0
        failed_queries = 0
        
        # Сбрасываем адаптивную задержку для нового парсинга
        self.adaptive_delay = AdaptiveDelay(initial_delay=0.2, min_delay=0.1, max_delay=1.0)
        
        # ПАРАЛЛЕЛЬНЫЙ ПАРСИНГ с Connection Pooling
        semaphore = asyncio.Semaphore(parallel_limit)
        
        async with httpx.AsyncClient(timeout=10.0) as shared_client:
            print(f"🏊 Connection pooling: используем общий HTTP клиент\n")
            
            async def fetch_limited(modifier):
                async with semaphore:
                    # INFIX паттерн: word1 + modifier + word2 word3...
                    infix_seed = f"{words[0]} {modifier} {' '.join(words[1:])}"
                    return await self.fetch_with_delay(modifier, words[0] + " _", country, language, shared_client, custom_query=infix_seed)
            
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
                infix_query = f"{words[0]} {modifier} {' '.join(words[1:])}"
                if i < 5 or len(suggestions) > 0:
                    print(f"[{i+1}/{len(modifiers)}] '{infix_query}' → {len(suggestions)} results")
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
            "method": "INFIX Optimized",
            "seed": seed,
            "pattern": f"'{words[0]}' + modifier + '{' '.join(words[1:])}'",
            "country": country,
            "language": language,
            "modifiers_count": len(modifiers),
            "modifiers_sample": modifiers[:10],
            "queries": total_queries,
            "successful_queries": successful_queries,
            "count": len(all_keywords),
            "keywords": sorted(list(all_keywords)),
            "elapsed_time": round(elapsed_time, 2),
            "avg_time_per_query": round(elapsed_time / total_queries, 2),
            "adaptive_delay": delay_stats
        }
    
    async def parse_morphology(self, seed: str, country: str, language: str, use_numbers: bool = True, parallel_limit: int = 5) -> Dict:
        """
        SUFFIX ПАРСИНГ С МОРФОЛОГИЕЙ
        
        Автоматически определяет морфологические формы последнего слова в seed
        и парсит каждую форму отдельно.
        
        Пример:
        Seed: "ремонт пылесосов"
        Формы: ["пылесос", "пылесоса", "пылесосу", "пылесосом", "пылесосе", 
                "пылесосы", "пылесосов", "пылесосам", "пылесосами", "пылесосах"]
        
        Для каждой формы делаем SUFFIX парсинг:
        - "ремонт пылесоса а", "ремонт пылесоса б", ...
        - "ремонт пылесосу а", "ремонт пылесосу б", ...
        - ...
        
        Поддержка языков:
        - RU, UK: полная морфология (10+ форм)
        - EN: единственное/множественное (2 формы)
        - Другие: только базовая форма
        """
        start_time = time.time()
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"MORPHOLOGY PARSER - SUFFIX + AUTO MORPHOLOGY")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Country: {country.upper()}, Language: {language.upper()}")
        print(f"Parallel: {parallel_limit}\n")
        
        # Разбиваем seed на слова
        words = seed.strip().split()
        
        if len(words) == 0:
            return {"error": "Seed не может быть пустым"}
        
        # Извлекаем последнее слово для морфологии
        base_word = words[-1]
        prefix = " ".join(words[:-1]) if len(words) > 1 else ""
        
        # Получаем морфологические формы
        word_forms = self.get_morphological_forms(base_word, language)
        
        print(f"📚 Базовое слово: '{base_word}'")
        print(f"📚 Морфологические формы: {word_forms}")
        print(f"📚 Всего форм: {len(word_forms)}\n")
        
        # Счётчики
        total_queries = 0
        successful_queries = 0
        failed_queries = 0
        forms_results = {}
        
        # Сбрасываем адаптивную задержку
        self.adaptive_delay = AdaptiveDelay(initial_delay=0.2, min_delay=0.1, max_delay=1.0)
        
        # Парсим каждую морфологическую форму
        for form_idx, word_form in enumerate(word_forms, 1):
            # Создаём новый seed с этой формой
            if prefix:
                current_seed = f"{prefix} {word_form}"
            else:
                current_seed = word_form
            
            print(f"\n{'─'*60}")
            print(f"📖 ФОРМА {form_idx}/{len(word_forms)}: '{word_form}'")
            print(f"📖 Seed: '{current_seed}'")
            print(f"{'─'*60}\n")
            
            # Получаем модификаторы
            modifiers = self.get_modifiers(language, use_numbers, current_seed)
            
            # ПАРАЛЛЕЛЬНЫЙ ПАРСИНГ с Connection Pooling
            semaphore = asyncio.Semaphore(parallel_limit)
            form_keywords = set()
            
            async with httpx.AsyncClient(timeout=10.0) as shared_client:
                async def fetch_limited(modifier):
                    async with semaphore:
                        return await self.fetch_with_delay(modifier, current_seed, country, language, shared_client)
                
                # Запускаем все задачи параллельно
                tasks = [fetch_limited(m) for m in modifiers]
                results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Обрабатываем результаты этой формы
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    failed_queries += 1
                    total_queries += 1
                    continue
                
                modifier, suggestions, success = result
                total_queries += 1
                
                if success:
                    form_keywords.update(suggestions)
                    all_keywords.update(suggestions)
                    successful_queries += 1
                    if i < 3 or len(suggestions) > 0:
                        print(f"  [{i+1}/{len(modifiers)}] '{current_seed} {modifier}' → {len(suggestions)} results")
                else:
                    failed_queries += 1
            
            # Сохраняем результаты этой формы
            forms_results[word_form] = {
                "seed": current_seed,
                "keywords_count": len(form_keywords),
                "unique_keywords": len(form_keywords - (all_keywords - form_keywords))
            }
            
            print(f"\n📊 Форма '{word_form}': {len(form_keywords)} ключей")
        
        elapsed_time = time.time() - start_time
        delay_stats = self.adaptive_delay.get_stats()
        
        # Итоговая статистика
        print(f"\n{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
        print(f"{'='*60}")
        print(f"Морфологических форм: {len(word_forms)}")
        print(f"Запросов: {total_queries} (✅ {successful_queries}, ❌ {failed_queries})")
        print(f"Уникальных ключей: {len(all_keywords)}")
        print(f"Время: {elapsed_time:.2f} сек ({elapsed_time/total_queries:.3f} сек/запрос)")
        print(f"Параллелизм: {parallel_limit}")
        print(f"🧠 Adaptive Delay: {delay_stats['final_delay']:.3f} сек (rate limits: {delay_stats['rate_limit_hits']})")
        print(f"🏊 Connection Pooling: ВКЛЮЧЁН")
        print(f"{'='*60}\n")
        
        return {
            "method": "SUFFIX + Morphology",
            "seed": seed,
            "base_word": base_word,
            "word_forms": word_forms,
            "forms_count": len(word_forms),
            "country": country,
            "language": language,
            "queries": total_queries,
            "successful_queries": successful_queries,
            "count": len(all_keywords),
            "keywords": sorted(list(all_keywords)),
            "forms_breakdown": forms_results,
            "elapsed_time": round(elapsed_time, 2),
            "avg_time_per_query": round(elapsed_time / total_queries, 3),
            "adaptive_delay": delay_stats
        }

# ============================================
# API ENDPOINTS
# ============================================

@app.get("/")
async def root():
    return {
        "api": "Google Autocomplete Parser - Optimized",
        "version": "3.7",
        "methods": {
            "suffix": "seed + modifier (ремонт пылесосов + а) - латиница + кириллица",
            "infix": "word1 + modifier + word2 (ремонт + а + пылесосов) - только кириллица",
            "morphology": "SUFFIX для всех морфологических форм слова (автоматически)"
        },
        "optimizations": [
            "Connection Pooling (переиспользование соединений)",
            "Adaptive Delay (автоматическая оптимизация)",
            "Parallel Requests (5 потоков)",
            "Smart Filtering (фильтрация для брендов)",
            "Auto Morphology (автоматическая морфология для RU/UK/EN)"
        ],
        "performance": {
            "baseline": "37.86 сек",
            "optimized_suffix": "~2.48 сек (470 ключей)",
            "optimized_morphology": "~20 сек (700-900 ключей для RU/UK)",
            "speedup": "15× быстрее"
        },
        "endpoints": {
            "suffix": "/api/parse",
            "infix": "/api/parse-infix",
            "morphology": "/api/parse-morphology",
            "compare": "/api/compare",
            "examples": {
                "suffix": "/api/parse?seed=ремонт+пылесосов&country=UA&language=ru&parallel=5",
                "infix": "/api/parse-infix?seed=ремонт+пылесосов&country=UA&language=ru&parallel=5",
                "morphology": "/api/parse-morphology?seed=ремонт+пылесосов&country=UA&language=ru&parallel=5",
                "compare": "/api/compare?seed=ремонт+пылесосов&country=UA&language=ru&parallel=5"
            }
        },
        "morphology_support": {
            "ru": "✅ Полная (через pymorphy3.lexeme)",
            "uk": "✅ Полная (через pymorphy3.lexeme)",
            "en": "✅ Улучшенная (через lemminflect) или базовая",
            "other": "⚠️ Только базовая форма"
        },
        "required_packages": {
            "ru_uk": "pip install pymorphy3 pymorphy3-dicts-ru",
            "en": "pip install lemminflect (опционально)",
            "other": "Не требуется (базовые правила)"
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
    parser = KeywordParser()
    result = await parser.parse(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers,
        parallel_limit=parallel
    )
    return result

@app.get("/api/parse-infix")
async def parse_infix(
    seed: str = Query("ремонт пылесосов", description="Базовый запрос (минимум 2 слова)"),
    country: str = Query("UA", description="Код страны (UA, US, RU, DE...)"),
    language: str = Query("ru", description="Код языка (ru, en, uk, de...)"),
    use_numbers: bool = Query(False, description="Включить цифры 0-9"),
    parallel: int = Query(5, description="Параллельных потоков (1-10)", ge=1, le=10)
):
    """
    ОПТИМИЗИРОВАННЫЙ INFIX ПАРСИНГ
    
    Паттерн: word1 + modifier + word2
    
    Пример:
    Seed: "ремонт пылесосов" → "ремонт а пылесосов", "ремонт б пылесосов", ...
    
    Требования:
    - Seed должен содержать минимум 2 слова
    
    Особенности INFIX:
    - Использует ТОЛЬКО кириллицу (латиница в середине бесполезна)
    - БЕЗ цифр (цифры в середине не встречаются)
    - ~30 модификаторов вместо 56 (быстрее!)
    
    Оптимизации:
    - Connection Pooling: переиспользование HTTP соединений
    - Adaptive Delay: автоматическая оптимизация задержек (0.1-1.0 сек)
    - Parallel: 5 потоков одновременно
    - Cyrillic Only: только кириллица (латиница в середине не работает)
    
    Производительность:
    - Время: ~1.5 сек на 30 запросов
    - Дополнительные ключи: +15 (~3%)
    """
    parser = KeywordParser()
    result = await parser.parse_infix(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers,
        parallel_limit=parallel
    )
    return result

@app.get("/api/parse-morphology")
async def parse_morphology(
    seed: str = Query("ремонт пылесосов", description="Базовый запрос"),
    country: str = Query("UA", description="Код страны (UA, US, RU, DE...)"),
    language: str = Query("ru", description="Код языка (ru, en, uk, de...)"),
    use_numbers: bool = Query(False, description="Включить цифры 0-9"),
    parallel: int = Query(5, description="Параллельных потоков (1-10)", ge=1, le=10)
):
    """
    SUFFIX ПАРСИНГ С АВТОМАТИЧЕСКОЙ МОРФОЛОГИЕЙ
    
    Автоматически определяет все морфологические формы последнего слова
    и парсит каждую форму через SUFFIX метод.
    
    Пример:
    Seed: "ремонт пылесосов"
    
    Морфологические формы (RU):
    - Через pymorphy3.lexeme получает ВСЕ формы слова
    - пылесос, пылесоса, пылесосу, пылесосом, пылесосе (ед.ч.)
    - пылесосы, пылесосов, пылесосам, пылесосами, пылесосах (мн.ч.)
    
    Для каждой формы:
    - "ремонт пылесоса а", "ремонт пылесоса б", ...
    - "ремонт пылесосу а", "ремонт пылесосу б", ...
    - ...
    
    Поддержка языков:
    - RU, UK: полная морфология через pymorphy3 (10+ форм)
    - EN: единственное/множественное число через lemminflect (2-3 формы)
    - Другие: только базовая форма (1 форма)
    
    Производительность:
    - RU/UK: ~10 форм × 56 запросов = 560 запросов (~20 сек)
    - EN: ~2-3 формы × 56 запросов = 112-168 запросов (~4-6 сек)
    
    Результат:
    - Намного больше уникальных ключей (+50-100%)
    - Находит варианты с разными падежами
    """
    parser = KeywordParser()
    result = await parser.parse_morphology(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers,
        parallel_limit=parallel
    )
    return result

@app.get("/api/compare")
async def compare_methods(
    seed: str = Query("ремонт пылесосов", description="Базовый запрос"),
    country: str = Query("UA", description="Код страны"),
    language: str = Query("ru", description="Код языка"),
    parallel: int = Query(5, description="Параллельных потоков", ge=1, le=10)
):
    """
    СРАВНЕНИЕ SUFFIX vs INFIX
    
    Запускает оба метода и сравнивает результаты:
    - Количество уникальных ключей
    - Время выполнения
    - Пересечения результатов
    - Уникальные ключи каждого метода
    """
    parser = KeywordParser()
    
    print("\n🔄 СРАВНЕНИЕ МЕТОДОВ: SUFFIX vs INFIX\n")
    
    # SUFFIX
    suffix_result = await parser.parse(
        seed=seed,
        country=country,
        language=language,
        use_numbers=False,
        parallel_limit=parallel
    )
    
    # Сбрасываем adaptive delay между методами
    parser.adaptive_delay = AdaptiveDelay(initial_delay=0.2, min_delay=0.1, max_delay=1.0)
    
    # INFIX
    infix_result = await parser.parse_infix(
        seed=seed,
        country=country,
        language=language,
        use_numbers=False,  # INFIX без цифр
        parallel_limit=parallel
    )
    
    # Сравнение
    if "error" in infix_result:
        return {
            "error": "INFIX недоступен для этого seed",
            "reason": infix_result["error"],
            "suffix_result": suffix_result
        }
    
    suffix_keywords = set(suffix_result["keywords"])
    infix_keywords = set(infix_result["keywords"])
    
    intersection = suffix_keywords & infix_keywords
    suffix_only = suffix_keywords - infix_keywords
    infix_only = infix_keywords - suffix_keywords
    total_unique = suffix_keywords | infix_keywords
    
    return {
        "seed": seed,
        "comparison": {
            "suffix": {
                "count": len(suffix_keywords),
                "time": suffix_result["elapsed_time"],
                "queries": suffix_result["queries"]
            },
            "infix": {
                "count": len(infix_keywords),
                "time": infix_result["elapsed_time"],
                "queries": infix_result["queries"]
            },
            "overlap": {
                "count": len(intersection),
                "percentage": round(len(intersection) / len(total_unique) * 100, 1) if total_unique else 0
            },
            "unique_to_suffix": {
                "count": len(suffix_only),
                "sample": sorted(list(suffix_only))[:10]
            },
            "unique_to_infix": {
                "count": len(infix_only),
                "sample": sorted(list(infix_only))[:10]
            },
            "total_unique": len(total_unique)
        },
        "winner": {
            "by_count": "INFIX" if len(infix_keywords) > len(suffix_keywords) else "SUFFIX",
            "by_speed": "INFIX" if infix_result["elapsed_time"] < suffix_result["elapsed_time"] else "SUFFIX",
            "recommendation": "Используйте оба метода для максимального покрытия" if len(total_unique) > max(len(suffix_keywords), len(infix_keywords)) * 1.3 else "Достаточно одного метода"
        }
    }
