"""
Google Autocomplete Parser API
Версия: 4.0 Clean
Оптимизированный парсинг с поддержкой трёх методов
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
    title="Google Autocomplete Parser API",
    version="4.0",
    description="SUFFIX + INFIX + MORPHOLOGY"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# КОНСТАНТЫ
# ============================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


# ============================================
# ADAPTIVE DELAY CLASS
# ============================================
class AdaptiveDelay:
    """Автоматическая оптимизация задержек между запросами"""
    
    def __init__(self, initial_delay: float = 0.2, min_delay: float = 0.1, max_delay: float = 1.0):
        self.delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.success_count = 0
        self.rate_limit_hits = 0
    
    def get_delay(self) -> float:
        return self.delay
    
    def on_success(self):
        self.success_count += 1
        if self.success_count >= 10:
            self.delay = max(self.min_delay, self.delay * 0.9)
            self.success_count = 0
    
    def on_rate_limit(self):
        self.rate_limit_hits += 1
        self.delay = min(self.max_delay, self.delay * 2)
        self.success_count = 0


# ============================================
# KEYWORD PARSER CLASS
# ============================================
class KeywordParser:
    """Основной класс для парсинга ключевых слов"""
    
    def __init__(self):
        self.adaptive_delay = AdaptiveDelay(initial_delay=0.2, min_delay=0.1, max_delay=1.0)
        
        # Модификаторы для разных языков
        self.language_modifiers = {
            'ru': list("абвгдежзийклмнопрстуфхцчшщъыьэюя"),
            'uk': list("абвгґдеєжзиіїйклмнопрстуфхцчшщьюя"),
            'en': list("abcdefghijklmnopqrstuvwxyz"),
            'pl': list("aąbcćdeęfghijklłmnńoóprsśtuwyzźż"),
            'de': list("abcdefghijklmnopqrstuvwxyzäöüß"),
            'fr': list("abcdefghijklmnopqrstuvwxyzàâæçéèêëïîôùûüÿœ"),
            'es': list("abcdefghijklmnñopqrstuvwxyzáéíóúü"),
        }
    
    def detect_seed_language(self, seed: str) -> str:
        """Определить язык seed (latin/cyrillic)"""
        has_cyrillic = any(ord('а') <= ord(c.lower()) <= ord('я') for c in seed if c.isalpha())
        return 'cyrillic' if has_cyrillic else 'latin'
    
    def get_modifiers(self, language: str, use_numbers: bool, seed: str, cyrillic_only: bool = False) -> List[str]:
        """
        Получить модификаторы с умной фильтрацией
        
        ФИЛЬТРАЦИЯ:
        - Английский seed → только a-z
        - Латинский seed → убираем кириллицу
        - Кириллический seed → оставляем ВСЁ (латиница для брендов!)
        - cyrillic_only=True → только кириллица (для INFIX)
        """
        seed_lang = self.detect_seed_language(seed)
        base_latin = list("abcdefghijklmnopqrstuvwxyz")
        numbers = list("0123456789") if use_numbers else []
        lang_specific = self.language_modifiers.get(language.lower(), [])
        
        if cyrillic_only:
            # INFIX: только кириллица
            is_cyrillic = lambda c: ord('а') <= ord(c.lower()) <= ord('я') or c in ['ё', 'і', 'ї', 'є', 'ґ', 'ў']
            return [m for m in lang_specific if is_cyrillic(m)]
        
        if language.lower() == 'en' and seed_lang == 'latin':
            # Английский → только a-z + цифры
            return base_latin + numbers
        
        if seed_lang == 'latin':
            # Латинский → убираем кириллицу
            is_cyrillic = lambda c: ord('а') <= ord(c.lower()) <= ord('я') or c in ['ё', 'і', 'ї', 'є', 'ґ', 'ў']
            non_cyrillic = [m for m in lang_specific if not is_cyrillic(m)]
            return base_latin + non_cyrillic + numbers
        
        # Кириллический → всё (латиница для брендов!)
        return base_latin + lang_specific + numbers
    
    def get_morphological_forms(self, word: str, language: str) -> List[str]:
        """Получить морфологические формы слова"""
        forms = set([word])
        
        if language.lower() in ['ru', 'uk']:
            try:
                import pymorphy3
                morph = pymorphy3.MorphAnalyzer()
                parsed = morph.parse(word)
                
                if parsed:
                    for form in parsed[0].lexeme:
                        # Фильтруем причастия и деепричастия
                        # Они создают странные комбинации типа "купившего rgb"
                        pos = form.tag.POS
                        if pos not in ['PRTS', 'PRTF', 'GRND']:  # participle short, participle full, gerund
                            forms.add(form.word)
                
                print(f"📖 Морфология: '{word}' → {len(forms)} форм (без причастий)")
            except ImportError:
                print(f"⚠️ pymorphy3 не установлен")
            except Exception as e:
                print(f"⚠️ Ошибка: {e}")
        
        elif language.lower() == 'en':
            try:
                import lemminflect
                plurals = lemminflect.getAllInflections(word, upos='NOUN')
                if plurals and 'NNS' in plurals:
                    forms.update(plurals['NNS'])
                
                if not word.endswith('s'):
                    forms.add(word + "'s")
                    forms.add(word + "s")
            except:
                pass
        
        return sorted(list(forms))
    
    async def fetch_suggestions(self, query: str, country: str, language: str, client: httpx.AsyncClient) -> List[str]:
        """Получить подсказки от Google Autocomplete"""
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
                if isinstance(data, list) and len(data) > 1:
                    return [s for s in data[1] if isinstance(s, str)]
            
            return []
            
        except Exception:
            return []
    
    async def fetch_suggestions_yandex(self, query: str, language: str, region_id: int, client: httpx.AsyncClient) -> List[str]:
        """Получить подсказки от Yandex Suggest"""
        url = "https://suggest.yandex.ru/suggest-ff.cgi"
        params = {
            "part": query,
            "uil": language,
            "v": "3",
            "lr": region_id  # 0=без региона, 143=Киев, 213=Москва, 20544=Харьков
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
                # Yandex возвращает: [query, [suggestions], ...]
                if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
                    return [s for s in data[1] if isinstance(s, str)]
            
            return []
            
        except Exception:
            return []
    
    async def parse_with_semaphore(self, queries: List[str], country: str, language: str, parallel_limit: int, use_yandex: bool = False, region_id: int = 0) -> Dict:
        """Парсинг с ограничением параллельности"""
        semaphore = asyncio.Semaphore(parallel_limit)
        
        async def fetch_with_limit(query: str, client: httpx.AsyncClient):
            async with semaphore:
                await asyncio.sleep(self.adaptive_delay.get_delay())
                
                if use_yandex:
                    # Запрашиваем оба источника параллельно
                    google_task = self.fetch_suggestions(query, country, language, client)
                    yandex_task = self.fetch_suggestions_yandex(query, language, region_id, client)
                    google_results, yandex_results = await asyncio.gather(google_task, yandex_task)
                    
                    # Объединяем результаты
                    combined = list(set(google_results + yandex_results))
                    return combined
                else:
                    # Только Google
                    return await self.fetch_suggestions(query, country, language, client)
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = [fetch_with_limit(q, client) for q in queries]
            results = await asyncio.gather(*tasks)
        
        all_keywords = set()
        for suggestions in results:
            all_keywords.update(suggestions)
        
        success_count = sum(1 for r in results if r)
        fail_count = len(results) - success_count
        
        return {
            "keywords": sorted(list(all_keywords)),
            "success": success_count,
            "failed": fail_count
        }
    
    # ============================================
    # DUAL METHOD (GOOGLE + YANDEX)
    # ============================================
    async def parse_dual(self, seed: str, country: str, region_id: int, language: str, use_numbers: bool, parallel_limit: int) -> Dict:
        """DUAL метод: Google + Yandex параллельно"""
        start_time = time.time()
        print(f"\n🔵🔴 DUAL (Google + Yandex): {seed}")
        
        modifiers = self.get_modifiers(language, use_numbers, seed)
        queries = [f"{seed} {mod}" for mod in modifiers]
        
        # Запрашиваем оба источника параллельно для каждого запроса
        semaphore = asyncio.Semaphore(parallel_limit)
        
        async def fetch_dual(query: str, client: httpx.AsyncClient):
            async with semaphore:
                await asyncio.sleep(self.adaptive_delay.get_delay())
                
                # Параллельно Google + Yandex
                google_task = self.fetch_suggestions(query, country, language, client)
                yandex_task = self.fetch_suggestions_yandex(query, language, region_id, client)
                google_results, yandex_results = await asyncio.gather(google_task, yandex_task)
                
                # Объединяем
                combined = list(set(google_results + yandex_results))
                
                return {
                    "google": google_results,
                    "yandex": yandex_results,
                    "combined": combined
                }
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = [fetch_dual(q, client) for q in queries]
            results = await asyncio.gather(*tasks)
        
        # Собираем статистику
        all_keywords = set()
        google_only_keywords = set()
        yandex_only_keywords = set()
        
        for result in results:
            all_keywords.update(result["combined"])
            
            google_set = set(result["google"])
            yandex_set = set(result["yandex"])
            
            google_only_keywords.update(google_set - yandex_set)
            yandex_only_keywords.update(yandex_set - google_set)
        
        elapsed_time = time.time() - start_time
        
        google_total = len(all_keywords) - len(yandex_only_keywords)
        yandex_total = len(all_keywords) - len(google_only_keywords)
        overlap = google_total + yandex_total - len(all_keywords)
        
        print(f"✅ ИТОГО: {len(all_keywords)} ключей")
        print(f"🔵 Google: {google_total} ({len(google_only_keywords)} уникальных)")
        print(f"🔴 Yandex: {yandex_total} ({len(yandex_only_keywords)} уникальных)")
        print(f"⏱️ Время: {elapsed_time:.2f} сек")
        
        return {
            "seed": seed,
            "method": "dual",
            "sources": ["Google Autocomplete", "Yandex Suggest"],
            "keywords": sorted(list(all_keywords)),
            "count": len(all_keywords),
            "queries": len(queries),
            "elapsed_time": round(elapsed_time, 2),
            "breakdown": {
                "google": {
                    "total": google_total,
                    "unique": len(google_only_keywords)
                },
                "yandex": {
                    "total": yandex_total,
                    "unique": len(yandex_only_keywords)
                },
                "overlap": overlap,
                "yandex_gain": f"+{round(len(yandex_only_keywords) / google_total * 100, 1)}%" if google_total > 0 else "0%"
            }
        }
    
    # ============================================
    # YANDEX METHOD
    # ============================================
    async def parse_yandex(self, seed: str, region_id: int, language: str, use_numbers: bool, parallel_limit: int) -> Dict:
        """YANDEX ТОЛЬКО метод - для тестирования ценности Yandex"""
        start_time = time.time()
        print(f"\n🔴 YANDEX: {seed}")
        
        modifiers = self.get_modifiers(language, use_numbers, seed)
        queries = [f"{seed} {mod}" for mod in modifiers]
        
        # Используем только Yandex
        semaphore = asyncio.Semaphore(parallel_limit)
        
        async def fetch_yandex_only(query: str, client: httpx.AsyncClient):
            async with semaphore:
                await asyncio.sleep(self.adaptive_delay.get_delay())
                return await self.fetch_suggestions_yandex(query, language, region_id, client)
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = [fetch_yandex_only(q, client) for q in queries]
            results = await asyncio.gather(*tasks)
        
        all_keywords = set()
        for suggestions in results:
            all_keywords.update(suggestions)
        
        success_count = sum(1 for r in results if r)
        elapsed_time = time.time() - start_time
        
        print(f"✅ {len(all_keywords)} ключей за {elapsed_time:.2f} сек")
        
        return {
            "seed": seed,
            "method": "yandex",
            "source": "Yandex Suggest",
            "keywords": sorted(list(all_keywords)),
            "count": len(all_keywords),
            "queries": len(queries),
            "region_id": region_id,
            "elapsed_time": round(elapsed_time, 2)
        }
    
    # ============================================
    # SUFFIX METHOD
    # ============================================
    async def parse(self, seed: str, country: str, language: str, use_numbers: bool, parallel_limit: int) -> Dict:
        """SUFFIX метод"""
        start_time = time.time()
        print(f"\n⚡ SUFFIX: {seed}")
        
        modifiers = self.get_modifiers(language, use_numbers, seed)
        queries = [f"{seed} {mod}" for mod in modifiers]
        
        result = await self.parse_with_semaphore(queries, country, language, parallel_limit)
        elapsed_time = time.time() - start_time
        
        print(f"✅ {len(result['keywords'])} ключей за {elapsed_time:.2f} сек")
        
        return {
            "seed": seed,
            "method": "suffix",
            "keywords": result["keywords"],
            "count": len(result["keywords"]),
            "queries": len(queries),
            "elapsed_time": round(elapsed_time, 2)
        }
    
    # ============================================
    # INFIX METHOD
    # ============================================
    async def parse_infix(self, seed: str, country: str, language: str, use_numbers: bool, parallel_limit: int) -> Dict:
        """INFIX метод"""
        start_time = time.time()
        print(f"\n🔄 INFIX: {seed}")
        
        words = seed.strip().split()
        
        if len(words) < 2:
            return {"error": "INFIX требует минимум 2 слова", "seed": seed}
        
        modifiers = self.get_modifiers(language, use_numbers, seed, cyrillic_only=True)
        queries = []
        
        for i in range(1, len(words)):
            for mod in modifiers:
                query = ' '.join(words[:i]) + f' {mod} ' + ' '.join(words[i:])
                queries.append(query)
        
        result = await self.parse_with_semaphore(queries, country, language, parallel_limit)
        elapsed_time = time.time() - start_time
        
        print(f"✅ {len(result['keywords'])} ключей за {elapsed_time:.2f} сек")
        
        return {
            "seed": seed,
            "method": "infix",
            "keywords": result["keywords"],
            "count": len(result["keywords"]),
            "queries": len(queries),
            "elapsed_time": round(elapsed_time, 2)
        }
    
    # ============================================
    # MORPHOLOGY METHOD
    # ============================================
    async def parse_morphology(self, seed: str, country: str, language: str, use_numbers: bool, parallel_limit: int) -> Dict:
        """MORPHOLOGY метод - модифицирует ВСЕ существительные в запросе"""
        start_time = time.time()
        print(f"\n🚀 MORPHOLOGY: {seed}")
        
        words = seed.strip().split()
        
        # Находим все существительные в запросе
        nouns_to_modify = []
        
        if language.lower() in ['ru', 'uk']:
            try:
                import pymorphy3
                morph = pymorphy3.MorphAnalyzer()
                
                for idx, word in enumerate(words):
                    parsed = morph.parse(word)
                    if parsed:
                        # Проверяем является ли слово существительным
                        pos = parsed[0].tag.POS
                        if pos == 'NOUN':
                            nouns_to_modify.append({
                                'index': idx,
                                'word': word,
                                'forms': self.get_morphological_forms(word, language)
                            })
                            print(f"📌 Существительное #{idx}: '{word}' → {len(self.get_morphological_forms(word, language))} форм")
                
                if not nouns_to_modify:
                    print(f"⚠️ Существительные не найдены, модифицируем последнее слово")
                    last_word = words[-1]
                    nouns_to_modify.append({
                        'index': len(words) - 1,
                        'word': last_word,
                        'forms': self.get_morphological_forms(last_word, language)
                    })
                
            except ImportError:
                print(f"⚠️ pymorphy3 не установлен, модифицируем последнее слово")
                last_word = words[-1]
                nouns_to_modify.append({
                    'index': len(words) - 1,
                    'word': last_word,
                    'forms': [last_word]
                })
        else:
            # Для не-русских языков модифицируем последнее слово
            last_word = words[-1]
            nouns_to_modify.append({
                'index': len(words) - 1,
                'word': last_word,
                'forms': self.get_morphological_forms(last_word, language)
            })
        
        print(f"📚 Будем модифицировать: {len(nouns_to_modify)} слов(а)")
        
        # Генерируем все комбинации форм
        all_seeds = []
        
        if len(nouns_to_modify) == 1:
            # Одно существительное - просто меняем формы
            noun = nouns_to_modify[0]
            for form in noun['forms']:
                new_words = words.copy()
                new_words[noun['index']] = form
                all_seeds.append(' '.join(new_words))
        
        else:
            # Несколько существительных - модифицируем ПЕРВОЕ (обычно это главное слово)
            # Например: "ремонт телефонов" → модифицируем "ремонт"
            noun = nouns_to_modify[0]
            print(f"🎯 Модифицируем первое существительное: '{noun['word']}'")
            
            for form in noun['forms']:
                new_words = words.copy()
                new_words[noun['index']] = form
                all_seeds.append(' '.join(new_words))
        
        unique_seeds = list(set(all_seeds))
        print(f"📋 Уникальных вариантов seed: {len(unique_seeds)}")
        
        # Парсим каждый вариант
        async def parse_single_seed(seed_variant: str) -> Dict:
            modifiers = self.get_modifiers(language, use_numbers, seed)
            queries = [f"{seed_variant} {mod}" for mod in modifiers]
            result = await self.parse_with_semaphore(queries, country, language, parallel_limit)
            return {"keywords": result["keywords"], "queries": len(queries)}
        
        tasks = [parse_single_seed(s) for s in unique_seeds]
        seed_results = await asyncio.gather(*tasks)
        
        all_keywords = set()
        total_queries = 0
        
        for seed_result in seed_results:
            all_keywords.update(seed_result["keywords"])
            total_queries += seed_result["queries"]
        
        elapsed_time = time.time() - start_time
        print(f"✅ {len(all_keywords)} ключей за {elapsed_time:.2f} сек")
        
        return {
            "seed": seed,
            "method": "morphology",
            "keywords": sorted(list(all_keywords)),
            "count": len(all_keywords),
            "forms_count": len(unique_seeds),
            "nouns_modified": len(nouns_to_modify),
            "queries": total_queries,
            "elapsed_time": round(elapsed_time, 2)
        }
    
    # ============================================
    # COMPARE METHOD
    # ============================================
    async def compare_all(self, seed: str, country: str, language: str, use_numbers: bool, parallel_limit: int, include_keywords: bool) -> Dict:
        """COMPARE: сравнение всех методов"""
        print(f"\n🔥 COMPARE: {seed}")
        
        suffix_result = await self.parse(seed, country, language, use_numbers, parallel_limit)
        self.adaptive_delay = AdaptiveDelay()
        
        infix_result = await self.parse_infix(seed, country, language, use_numbers, parallel_limit)
        self.adaptive_delay = AdaptiveDelay()
        
        morphology_result = await self.parse_morphology(seed, country, language, use_numbers, parallel_limit)
        
        suffix_kw = set(suffix_result["keywords"])
        infix_kw = set(infix_result.get("keywords", []))
        morphology_kw = set(morphology_result["keywords"])
        all_unique = suffix_kw | infix_kw | morphology_kw
        
        response = {
            "seed": seed,
            "comparison": {
                "suffix": {
                    "count": len(suffix_kw),
                    "time": suffix_result["elapsed_time"],
                    "queries": suffix_result["queries"]
                },
                "infix": {
                    "count": len(infix_kw),
                    "time": infix_result.get("elapsed_time", 0),
                    "queries": infix_result.get("queries", 0)
                },
                "morphology": {
                    "count": len(morphology_kw),
                    "time": morphology_result["elapsed_time"],
                    "queries": morphology_result["queries"],
                    "forms": morphology_result["forms_count"]
                },
                "total_unique": len(all_unique),
                "total_time": suffix_result["elapsed_time"] + infix_result.get("elapsed_time", 0) + morphology_result["elapsed_time"]
            }
        }
        
        if include_keywords:
            response["keywords"] = {"all_unique": sorted(list(all_unique))}
        
        return response


# ============================================
# API ENDPOINTS
# ============================================

@app.get("/")
async def root():
    return {"status": "ok", "version": "4.0", "methods": ["suffix", "infix", "morphology", "compare"]}


@app.get("/api/parse")
async def parse_suffix(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru"),
    parallel: int = Query(5, ge=1, le=10)
):
    parser = KeywordParser()
    return await parser.parse(seed, country, language, False, parallel)


@app.get("/api/parse-infix")
async def parse_infix(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru"),
    parallel: int = Query(5, ge=1, le=10)
):
    parser = KeywordParser()
    return await parser.parse_infix(seed, country, language, False, parallel)


@app.get("/api/parse-dual")
async def parse_dual_sources(
    seed: str = Query("ремонт пылесосов", description="Ключевое слово"),
    country: str = Query("UA", description="Код страны для Google"),
    region: int = Query(187, description="Yandex Region ID (187=Украина)"),
    language: str = Query("ru", description="Код языка"),
    parallel: int = Query(5, ge=1, le=10, description="Параллельных потоков")
):
    """DUAL парсинг: Google + Yandex параллельно"""
    parser = KeywordParser()
    return await parser.parse_dual(seed, country, region, language, False, parallel)


@app.get("/api/parse-yandex")
async def parse_yandex_only(
    seed: str = Query("ремонт пылесосов", description="Ключевое слово"),
    region: int = Query(0, description="Yandex Region ID (0=все, 143=Киев, 213=Москва, 20544=Харьков)"),
    language: str = Query("ru", description="Код языка"),
    parallel: int = Query(5, ge=1, le=10, description="Параллельных потоков")
):
    """YANDEX парсинг (для тестирования)"""
    parser = KeywordParser()
    return await parser.parse_yandex(seed, region, language, False, parallel)


@app.get("/api/parse-morphology")
async def parse_morphology(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru"),
    parallel: int = Query(5, ge=1, le=10)
):
    parser = KeywordParser()
    return await parser.parse_morphology(seed, country, language, False, parallel)


@app.get("/api/compare")
async def compare_methods(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru"),
    parallel: int = Query(5, ge=1, le=10),
    include_keywords: bool = Query(True)
):
    parser = KeywordParser()
    return await parser.compare_all(seed, country, language, False, parallel, include_keywords)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
