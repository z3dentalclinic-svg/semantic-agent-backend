"""
FGS Parser API - Clean Version 4.5.1
Пять методов парсинга: SUFFIX + INFIX + MORPHOLOGY + MORPHOLOGY ADAPTIVE + ADAPTIVE PREFIX
Три источника: Google + Yandex + Bing
Автокоррекция: Yandex Speller + LanguageTool

Последнее обновление: 2026-01-05
+ Улучшен ADAPTIVE PREFIX (извлекает ВСЕ слова, не только последнее)
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import List, Dict
import httpx
import asyncio
import time
import random

# ============================================
# FASTAPI APP
# ============================================
app = FastAPI(
    title="FGS Parser API",
    version="4.5.1",
    description="5 методов: SUFFIX + INFIX + MORPHOLOGY + MORPHOLOGY ADAPTIVE + ADAPTIVE PREFIX"
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
# ADAPTIVE DELAY
# ============================================
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


# ============================================
# PARSER CLASS
# ============================================
class GoogleAutocompleteParser:
    def __init__(self):
        self.adaptive_delay = AdaptiveDelay()
    
    # ============================================
    # LANGUAGE & MODIFIERS
    # ============================================
    def detect_seed_language(self, seed: str) -> str:
        """Автоопределение языка seed"""
        if any('\u0400' <= char <= '\u04FF' for char in seed):
            if any(char in 'іїєґ' for char in seed.lower()):
                return 'uk'
            return 'ru'
        return 'en'
    
    def get_modifiers(self, language: str, use_numbers: bool, seed: str, cyrillic_only: bool = False) -> List[str]:
        """Получить модификаторы для языка"""
        modifiers = []
        
        # Кириллица (БЕЗ ъ, ь, ы)
        if language.lower() == 'ru':
            modifiers.extend(list("абвгдежзийклмнопрстуфхцчшщэюя"))
        elif language.lower() == 'uk':
            modifiers.extend(list("абвгдежзийклмнопрстуфхцчшщюяіїєґ"))
        
        # Латиница
        if not cyrillic_only:
            modifiers.extend(list("abcdefghijklmnopqrstuvwxyz"))
        
        # Цифры
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
                        # Фильтруем причастия и деепричастия
                        if pos not in ['PRTS', 'PRTF', 'GRND']:
                            forms.add(form.word)
            except:
                pass
        
        return sorted(list(forms))
    
    # ============================================
    # AUTOCORRECTION
    # ============================================
    async def autocorrect_text(self, text: str, language: str) -> Dict:
        """Автокоррекция через Yandex Speller (ru/uk/en) или LanguageTool (остальные)"""
        
        # Yandex Speller для ru/uk/en
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
        
        # LanguageTool fallback для всех языков
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
    
    # ============================================
    # FILTERS
    # ============================================
    async def filter_infix_results(self, keywords: List[str], language: str) -> List[str]:
        """Фильтр INFIX результатов: убирает мусорные одиночные буквы"""
        
        # Whitelist предлогов/союзов
        if language.lower() == 'ru':
            valid = {'в', 'на', 'у', 'к', 'от', 'из', 'по', 'о', 'об', 'с', 'со', 'за', 'для', 'и', 'а', 'но'}
        elif language.lower() == 'uk':
            valid = {'в', 'на', 'у', 'до', 'від', 'з', 'по', 'про', 'для', 'і', 'та', 'або'}
        elif language.lower() == 'en':
            valid = {'in', 'on', 'at', 'to', 'from', 'with', 'for', 'by', 'of', 'and', 'or', 'a', 'i'}
        else:
            valid = set()
        
        filtered = []
        
        for keyword in keywords:
            keyword_lower = keyword.lower()
            words = keyword_lower.split()
            
            # Проверяем ВСЕ слова после первого
            has_garbage = False
            for i in range(1, len(words)):
                word = words[i]
                if len(word) == 1 and word not in valid:
                    has_garbage = True
                    break
            
            if not has_garbage:
                filtered.append(keyword)
        
        return filtered
    
    async def filter_relevant_keywords(self, keywords: List[str], seed: str) -> List[str]:
        """Улучшенный фильтр релевантности: проверяет ВСЕ важные слова из seed"""
        
        seed_words = set(seed.lower().split())
        
        # Стоп-слова (игнорируем)
        stop_words = {'в', 'на', 'для', 'с', 'о', 'по', 'из', 'к', 'от', 'у',
                     'купить', 'заказать', 'цена', 'недорого', 'где', 'как', 'что'}
        
        # Важные слова seed
        important_words = [w for w in seed_words if w not in stop_words and len(w) > 2]
        
        if not important_words:
            return keywords
        
        # НОВАЯ ЛОГИКА: проверяем сколько важных слов присутствует в ключе
        filtered = []
        
        for keyword in keywords:
            keyword_lower = keyword.lower()
            
            # Считаем сколько важных слов из seed есть в ключе
            matches = sum(1 for word in important_words if word in keyword_lower)
            
            # Если seed короткий (1-2 слова) - требуем хотя бы 1 совпадение
            # Если seed длинный (3+ слова) - требуем хотя бы 2 совпадения
            if len(important_words) <= 2:
                required_matches = 1
            else:
                required_matches = min(2, len(important_words))  # Минимум 2, но не больше чем есть
            
            if matches >= required_matches:
                filtered.append(keyword)
        
        return filtered
    
    # ============================================
    # FETCH SUGGESTIONS (3 источника)
    # ============================================
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
    
    # ============================================
    # PARSING WITH SEMAPHORE
    # ============================================
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
                
                # Выбор источника
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
    
    # ============================================
    # SUFFIX METHOD
    # ============================================
    async def parse_suffix(self, seed: str, country: str, language: str, use_numbers: bool, 
                          parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """SUFFIX метод: seed + модификатор"""
        start_time = time.time()
        
        modifiers = self.get_modifiers(language, use_numbers, seed)
        queries = [f"{seed} {mod}" for mod in modifiers]
        
        result_raw = await self.parse_with_semaphore(queries, country, language, parallel_limit, source, region_id)
        
        # Фильтр релевантности
        filtered = await self.filter_relevant_keywords(result_raw['keywords'], seed)
        
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
    
    # ============================================
    # INFIX METHOD
    # ============================================
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
        
        # Фильтр 1: одиночные буквы
        filtered_1 = await self.filter_infix_results(result_raw['keywords'], language)
        
        # Фильтр 2: релевантность
        filtered_2 = await self.filter_relevant_keywords(filtered_1, seed)
        
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
    
    # ============================================
    # MORPHOLOGY METHOD
    # ============================================
    async def parse_morphology(self, seed: str, country: str, language: str, use_numbers: bool, 
                               parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """MORPHOLOGY метод: модификация форм существительных"""
        start_time = time.time()
        
        words = seed.strip().split()
        
        # Находим существительные
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
        
        # Генерируем варианты seed
        all_seeds = []
        if len(nouns_to_modify) >= 1:
            noun = nouns_to_modify[0]
            for form in noun['forms']:
                new_words = words.copy()
                new_words[noun['index']] = form
                all_seeds.append(' '.join(new_words))
        
        unique_seeds = list(set(all_seeds))
        
        # Парсим все формы
        all_keywords = set()
        modifiers = self.get_modifiers(language, use_numbers, seed)
        
        for seed_variant in unique_seeds:
            queries = [f"{seed_variant} {mod}" for mod in modifiers]
            result = await self.parse_with_semaphore(queries, country, language, parallel_limit, source, region_id)
            all_keywords.update(result['keywords'])
        
        # Фильтр релевантности
        filtered = await self.filter_relevant_keywords(sorted(list(all_keywords)), seed)
        
        elapsed = time.time() - start_time
        
        return {
            "seed": seed,
            "method": "morphology",
            "source": source,
            "keywords": filtered,
            "count": len(filtered),
            "elapsed_time": round(elapsed, 2)
        }
    
    # ============================================
    # MORPHOLOGICAL ADAPTIVE METHOD
    # ============================================
    async def parse_morphological_adaptive(self, seed: str, country: str, language: str, use_numbers: bool, 
                                           parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """MORPHOLOGICAL ADAPTIVE метод: морфология + частотный анализ + PREFIX проверка"""
        start_time = time.time()
        
        words = seed.strip().split()
        seed_words = set(seed.lower().split())
        
        # ЭТАП 1: Генерация морфологических форм
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
        
        # Генерируем варианты seed
        all_seeds = []
        if len(nouns_to_modify) >= 1:
            noun = nouns_to_modify[0]
            for form in noun['forms']:
                new_words = words.copy()
                new_words[noun['index']] = form
                all_seeds.append(' '.join(new_words))
        
        unique_seeds = list(set(all_seeds))
        
        # ЭТАП 2: SUFFIX парсинг всех форм
        all_suffix_results = []
        modifiers = self.get_modifiers(language, use_numbers, seed)
        
        for seed_variant in unique_seeds:
            queries = [f"{seed_variant} {mod}" for mod in modifiers]
            result = await self.parse_with_semaphore(queries, country, language, parallel_limit, source, region_id)
            all_suffix_results.extend(result['keywords'])
        
        # ЭТАП 3: Частотный анализ - извлечение слов-кандидатов
        from collections import Counter
        word_counter = Counter()
        
        for result in all_suffix_results:
            result_words = result.lower().split()
            for word in result_words:
                # Только если слово не из seed и длина > 2
                if word not in seed_words and len(word) > 2:
                    word_counter[word] += 1
        
        # Фильтрация: слова встречающиеся ≥2 раза
        candidates = {w for w, count in word_counter.items() if count >= 2}
        
        # ЭТАП 4: PREFIX проверка кандидатов
        all_keywords = set()
        
        for candidate in sorted(candidates):
            query = f"{candidate} {seed}"
            result = await self.parse_with_semaphore([query], country, language, parallel_limit, source, region_id)
            if result['keywords']:
                all_keywords.update(result['keywords'])
        
        # Фильтр релевантности
        filtered = await self.filter_relevant_keywords(sorted(list(all_keywords)), seed)
        
        elapsed = time.time() - start_time
        
        return {
            "seed": seed,
            "method": "morphology_adaptive",
            "source": source,
            "keywords": filtered,
            "count": len(filtered),
            "candidates_found": len(candidates),
            "elapsed_time": round(elapsed, 2)
        }
    
    # ============================================
    # ADAPTIVE PREFIX METHOD
    # ============================================
    async def parse_adaptive_prefix(self, seed: str, country: str, language: str, use_numbers: bool, 
                                    parallel_limit: int, source: str = "google", region_id: int = 0) -> Dict:
        """ADAPTIVE PREFIX метод: извлечение слов из SUFFIX + PREFIX проверка"""
        start_time = time.time()
        
        seed_words = set(seed.lower().split())
        
        # ЭТАП 1: SUFFIX парсинг для извлечения кандидатов
        # Используем только кириллицу (PREFIX работает только с кириллицей)
        modifiers = self.get_modifiers(language, use_numbers, seed, cyrillic_only=True)
        queries = [f"{seed} {mod}" for mod in modifiers]
        
        result_raw = await self.parse_with_semaphore(queries, country, language, parallel_limit, source, region_id)
        
        # ЭТАП 2: Извлечение ВСЕХ новых слов из результатов
        from collections import Counter
        word_counter = Counter()
        
        for result in result_raw['keywords']:
            result_words = result.lower().split()
            for word in result_words:
                # Только если слово не из seed и длина > 2
                if word not in seed_words and len(word) > 2:
                    word_counter[word] += 1
        
        # Фильтрация: слова встречающиеся ≥2 раза
        candidates = {w for w, count in word_counter.items() if count >= 2}
        
        # ЭТАП 3: PREFIX проверка - проверяем "{слово} {seed}"
        all_keywords = set()
        verified_prefixes = []
        
        for candidate in sorted(candidates):
            query = f"{candidate} {seed}"
            result = await self.parse_with_semaphore([query], country, language, parallel_limit, source, region_id)
            if result['keywords']:
                all_keywords.update(result['keywords'])
                verified_prefixes.append(candidate)
        
        # Фильтр релевантности
        filtered = await self.filter_relevant_keywords(sorted(list(all_keywords)), seed)
        
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
    
    # ============================================
    # COMPARE ALL
    # ============================================
    async def compare_all(self, seed: str, country: str, region_id: int, language: str, 
                         use_numbers: bool, parallel_limit: int, include_keywords: bool, 
                         source: str = "google") -> Dict:
        """Сравнение всех трёх методов с автокоррекцией"""
        
        # Автокоррекция seed
        correction = await self.autocorrect_text(seed, language)
        original_seed = seed
        
        if correction.get("has_errors"):
            seed = correction["corrected"]
        
        start_time = time.time()
        
        # Запускаем все три метода
        suffix_result = await self.parse_suffix(seed, country, language, use_numbers, parallel_limit, source, region_id)
        infix_result = await self.parse_infix(seed, country, language, use_numbers, parallel_limit, source, region_id)
        morph_result = await self.parse_morphology(seed, country, language, use_numbers, parallel_limit, source, region_id)
        
        # Собираем результаты
        suffix_kw = set(suffix_result["keywords"])
        infix_kw = set(infix_result.get("keywords", []))
        morph_kw = set(morph_result["keywords"])
        
        all_unique = suffix_kw | infix_kw | morph_kw
        
        # Пересечения
        suffix_only = suffix_kw - infix_kw - morph_kw
        infix_only = infix_kw - suffix_kw - morph_kw
        morph_only = morph_kw - suffix_kw - infix_kw
        all_three = suffix_kw & infix_kw & morph_kw
        
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
                "morphology": {"count": len(morph_kw)}
            },
            "unique_to_method": {
                "suffix_only": len(suffix_only),
                "infix_only": len(infix_only),
                "morph_only": len(morph_only),
                "all_three": len(all_three)
            },
            "elapsed_time": round(elapsed, 2)
        }
        
        if include_keywords:
            response["keywords"] = {
                "all": sorted(list(all_unique)),
                "suffix": sorted(list(suffix_kw)),
                "infix": sorted(list(infix_kw)),
                "morphology": sorted(list(morph_kw))
            }
        
        return response


# ============================================
# API ENDPOINTS
# ============================================
parser = GoogleAutocompleteParser()

@app.get("/")
async def root():
    """Главная страница"""
    return FileResponse('static/index.html')

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
    """Сравнение всех методов (ОСНОВНОЙ ENDPOINT)"""
    
    if language == "auto":
        language = parser.detect_seed_language(seed)
    
    return await parser.compare_all(seed, country, region_id, language, use_numbers, parallel_limit, include_keywords, source)

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
    
    # Автокоррекция
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
    
    # Автокоррекция
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
    
    # Автокоррекция
    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]
    
    result = await parser.parse_morphology(seed, country, language, use_numbers, parallel_limit, source, region_id)
    
    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])
    
    return result

@app.get("/api/parse/morphology-adaptive")
async def parse_morphology_adaptive_endpoint(
    seed: str = Query(..., description="Базовый запрос"),
    country: str = Query("ua", description="Код страны"),
    region_id: int = Query(143, description="ID региона для Yandex"),
    language: str = Query("auto", description="Язык"),
    use_numbers: bool = Query(False, description="Добавить цифры"),
    parallel_limit: int = Query(10, description="Параллельных запросов"),
    source: str = Query("google", description="Источник: google/yandex/bing")
):
    """MORPHOLOGY ADAPTIVE метод (улучшенный с частотным анализом)"""
    
    if language == "auto":
        language = parser.detect_seed_language(seed)
    
    # Автокоррекция
    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]
    
    result = await parser.parse_morphological_adaptive(seed, country, language, use_numbers, parallel_limit, source, region_id)
    
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
    
    # Автокоррекция
    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]
    
    result = await parser.parse_adaptive_prefix(seed, country, language, use_numbers, parallel_limit, source, region_id)
    
    if correction.get("has_errors"):
        result["original_seed"] = correction["original"]
        result["corrections"] = correction.get("corrections", [])
    
    return result
