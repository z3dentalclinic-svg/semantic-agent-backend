"""
GOOGLE AUTOCOMPLETE PARSER - ФИНАЛЬНАЯ ВЕРСИЯ
Все протестированные и работающие методы объединены
Version: 3.0 Final
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Optional
from collections import Counter
from pydantic import BaseModel
import os
import httpx
import asyncio
import time
import random

app = FastAPI(title="Google Autocomplete Parser - Final v3.0", version="3.0")

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
# AUTOCOMPLETE PARSER CLASS
# ============================================
class AutocompleteParser:
    def __init__(self):
        self.base_url = "https://suggestqueries.google.com/complete/search"
        
        # Базовые модификаторы (для всех языков)
        self.base_modifiers = list("abcdefghijklmnopqrstuvwxyz0123456789")
        
        # Языковые модификаторы
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
    
    def get_modifiers(self, language: str) -> List[str]:
        """Получить модификаторы для языка"""
        modifiers = self.base_modifiers.copy()
        lang_mods = self.language_modifiers.get(language.lower(), [])
        modifiers.extend(lang_mods)
        return modifiers
    
    def get_seed_variations(self, seed: str, language: str) -> List[str]:
        """
        Генерация морфологических форм seed (для русского языка)
        Для других языков возвращает только исходный seed
        """
        if language.lower() != 'ru':
            return [seed]
        
        # Для русского - создаём морфологические формы вручную
        # В production можно использовать pymorphy3
        words = seed.split()
        if len(words) != 2:
            return [seed]
        
        # Примеры для "ремонт пылесосов"
        variations = [
            seed,                           # "ремонт пылесосов" (именительный)
            f"{words[0]}а {words[1]}",     # "ремонта пылесосов" (родительный)
            f"по {words[0]}у {words[1]}",  # "по ремонту пылесосов" (дательный с предлогом)
        ]
        
        return variations
    
    async def fetch_suggestions(self, query: str, country: str, language: str) -> List[str]:
        """Базовый запрос к Google Autocomplete API"""
        params = {"client": "chrome", "q": query, "gl": country, "hl": language}
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(self.base_url, params=params, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 1:
                        return [s for s in data[1] if isinstance(s, str)]
                return []
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    # ========================================
    # МЕТОД 1: SUFFIX + INFIX + MORPHOLOGY
    # ========================================
    async def method_suffix_infix_morph(
        self,
        seed: str,
        country: str,
        language: str,
        use_numbers: bool = False,
        use_morphology: bool = True
    ) -> Dict:
        """
        КОМБИНИРОВАННЫЙ МЕТОД: SUFFIX + INFIX + MORPHOLOGY
        
        Для латиницы/цифр:
          - SUFFIX: "seed modifier"
        
        Для кириллицы:
          - SUFFIX: "seed_form modifier" (с морфологией если включена)
          - INFIX: "word1 modifier word2" (без морфологии)
        """
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"МЕТОД 1: SUFFIX + INFIX + MORPHOLOGY")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Language: {language.upper()}")
        print(f"Use numbers: {use_numbers}")
        print(f"Use morphology: {use_morphology}\n")
        
        # Получаем модификаторы
        all_modifiers = self.get_modifiers(language)
        
        if not use_numbers:
            all_modifiers = [m for m in all_modifiers if not m.isdigit()]
        
        # Разделяем на латиницу/цифры и кириллицу
        language_specific = self.language_modifiers.get(language.lower(), [])
        cyrillic_modifiers = [m for m in all_modifiers if m in language_specific]
        latin_digit_modifiers = [m for m in all_modifiers if m not in language_specific]
        
        # Морфологические формы seed
        seed_variations = [seed]
        if use_morphology and language.lower() == 'ru':
            seed_variations = self.get_seed_variations(seed, language)
            print(f"📝 Морфологические формы ({len(seed_variations)}):")
            for var in seed_variations:
                print(f"  • {var}")
            print()
        
        seed_words = seed.split()
        
        print(f"📊 Модификаторы:")
        print(f"  Latin/Digits: {len(latin_digit_modifiers)}")
        print(f"  Cyrillic: {len(cyrillic_modifiers)}")
        print(f"📍 INFIX: {'ENABLED' if len(cyrillic_modifiers) > 0 and len(seed_words) >= 2 else 'DISABLED'}")
        print()
        
        total_queries = 0
        latin_results = 0
        cyrillic_results = 0
        infix_results = 0
        
        # ========================================
        # 1. SUFFIX Latin/Digits
        # ========================================
        if len(latin_digit_modifiers) > 0:
            print(f"{'='*60}")
            print(f"[1/3] SUFFIX Latin/Digits")
            print(f"{'='*60}")
            print(f"Pattern: '{seed} [a-z, 0-9]'")
            print(f"Modifiers: {len(latin_digit_modifiers)}\n")
            
            for i, modifier in enumerate(latin_digit_modifiers):
                query = f"{seed} {modifier}"
                results = await self.fetch_suggestions(query, country, language)
                all_keywords.update(results)
                latin_results += len(results)
                total_queries += 1
                
                if i < 3 or len(results) > 0:
                    print(f"[{i+1}/{len(latin_digit_modifiers)}] '{query}' → {len(results)} results")
                
                await asyncio.sleep(random.uniform(0.5, 2.0))
            
            print(f"\n✅ SUFFIX Latin/Digits: {latin_results} results\n")
        
        # ========================================
        # 2. SUFFIX Cyrillic (с морфологией)
        # ========================================
        if len(cyrillic_modifiers) > 0:
            print(f"{'='*60}")
            print(f"[2/3] SUFFIX Cyrillic (с морфологией)")
            print(f"{'='*60}")
            print(f"Seed variations: {len(seed_variations)}")
            print(f"Modifiers: {len(cyrillic_modifiers)}\n")
            
            for seed_var in seed_variations:
                print(f"--- Форма: '{seed_var}' ---")
                
                for i, modifier in enumerate(cyrillic_modifiers):
                    query = f"{seed_var} {modifier}"
                    results = await self.fetch_suggestions(query, country, language)
                    all_keywords.update(results)
                    cyrillic_results += len(results)
                    total_queries += 1
                    
                    if i < 3 or len(results) > 0:
                        print(f"[{i+1}/{len(cyrillic_modifiers)}] '{query}' → {len(results)} results")
                    
                    await asyncio.sleep(random.uniform(0.5, 2.0))
                
                print()
            
            print(f"✅ SUFFIX Cyrillic: {cyrillic_results} results\n")
        
        # ========================================
        # 3. INFIX (только кириллица, без морфологии)
        # ========================================
        if len(cyrillic_modifiers) > 0 and len(seed_words) >= 2:
            print(f"{'='*60}")
            print(f"[3/3] INFIX (кириллица)")
            print(f"{'='*60}")
            print(f"Pattern: '{seed_words[0]} [modifier] {' '.join(seed_words[1:])}'")
            print(f"Modifiers: {len(cyrillic_modifiers)}\n")
            
            for i, modifier in enumerate(cyrillic_modifiers):
                infix_query = f"{seed_words[0]} {modifier} {' '.join(seed_words[1:])}"
                results = await self.fetch_suggestions(infix_query, country, language)
                all_keywords.update(results)
                infix_results += len(results)
                total_queries += 1
                
                if i < 3 or len(results) > 0:
                    print(f"[{i+1}/{len(cyrillic_modifiers)}] '{infix_query}' → {len(results)} results")
                
                await asyncio.sleep(random.uniform(0.5, 2.0))
            
            print(f"\n✅ INFIX: {infix_results} results\n")
        
        # ========================================
        # ИТОГОВАЯ СТАТИСТИКА
        # ========================================
        print(f"{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
        print(f"{'='*60}")
        print(f"Запросов: {total_queries}")
        print(f"  - SUFFIX Latin/Digits: {len(latin_digit_modifiers)}")
        print(f"  - SUFFIX Cyrillic: {len(cyrillic_modifiers) * len(seed_variations)}")
        print(f"  - INFIX: {len(cyrillic_modifiers) if len(seed_words) >= 2 else 0}")
        print(f"")
        print(f"Результатов:")
        print(f"  - SUFFIX Latin/Digits: {latin_results}")
        print(f"  - SUFFIX Cyrillic: {cyrillic_results}")
        print(f"  - INFIX: {infix_results}")
        print(f"")
        print(f"Уникальных ключей: {len(all_keywords)}")
        print(f"{'='*60}\n")
        
        return {
            "method": "SUFFIX + INFIX + MORPHOLOGY",
            "queries": total_queries,
            "results": {
                "latin": latin_results,
                "cyrillic": cyrillic_results,
                "infix": infix_results
            },
            "keywords": list(all_keywords),
            "count": len(all_keywords)
        }
    
    # ========================================
    # МЕТОД 2: ADAPTIVE PREFIX
    # ========================================
    async def method_adaptive_prefix(
        self,
        seed: str,
        country: str,
        language: str
    ) -> Dict:
        """
        ADAPTIVE PREFIX - Двухэтапный метод
        
        ЭТАП 1: SUFFIX парсинг для извлечения потенциальных PREFIX слов
        ЭТАП 2: PREFIX проверка извлечённых слов
        """
        all_keywords = set()
        seed_words_set = set(seed.lower().split())
        
        print(f"\n{'='*60}")
        print(f"МЕТОД 2: ADAPTIVE PREFIX")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Language: {language.upper()}\n")
        
        # Получаем кириллические модификаторы
        language_specific = self.language_modifiers.get(language.lower(), [])
        
        if not language_specific:
            print(f"⚠️ ADAPTIVE PREFIX работает только с кириллицей!")
            print(f"⚠️ Для языка '{language}' нет кириллических модификаторов\n")
            return {
                "method": "ADAPTIVE PREFIX",
                "queries": 0,
                "stage1": {"queries": 0, "results": 0, "words_extracted": 0},
                "stage2": {"queries": 0, "results": 0, "valid_prefix": 0},
                "keywords": [],
                "count": 0
            }
        
        cyrillic_modifiers = language_specific
        
        # ========================================
        # ЭТАП 1: SUFFIX парсинг
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 1: SUFFIX парсинг (извлечение кандидатов)")
        print(f"{'='*60}")
        print(f"Pattern: '{seed} [а-я]'")
        print(f"Modifiers: {len(cyrillic_modifiers)}\n")
        
        potential_prefix_words = set()
        stage1_keywords = set()
        stage1_count = 0
        
        for i, modifier in enumerate(cyrillic_modifiers):
            query = f"{seed} {modifier}"
            suggestions = await self.fetch_suggestions(query, country, language)
            stage1_keywords.update(suggestions)
            stage1_count += len(suggestions)
            
            # Извлекаем последнее слово из каждого результата
            for suggestion in suggestions:
                words = suggestion.split()
                if len(words) > len(seed.split()):
                    last_word = words[-1].lower()
                    # Фильтр: длина > 2, только буквы
                    if len(last_word) > 2 and last_word.replace('-', '').isalpha():
                        if last_word not in seed_words_set:
                            potential_prefix_words.add(last_word)
            
            if i < 3 or len(suggestions) > 0:
                print(f"[{i+1}/{len(cyrillic_modifiers)}] '{query}' → {len(suggestions)} results")
            
            await asyncio.sleep(random.uniform(0.5, 2.0))
        
        print(f"\n✅ ЭТАП 1 завершён!")
        print(f"Результатов: {stage1_count}")
        print(f"Извлечено слов: {len(potential_prefix_words)}\n")
        
        if len(potential_prefix_words) > 0:
            print(f"Примеры извлечённых слов:")
            for word in sorted(potential_prefix_words)[:15]:
                print(f"  • {word}")
            if len(potential_prefix_words) > 15:
                print(f"  ... и ещё {len(potential_prefix_words) - 15}")
        print()
        
        # ========================================
        # ЭТАП 2: PREFIX проверка
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 2: PREFIX проверка (обратные запросы)")
        print(f"{'='*60}")
        print(f"Pattern: '[word] {seed}'")
        print(f"Слов для проверки: {len(potential_prefix_words)}\n")
        
        stage2_keywords = set()
        stage2_count = 0
        successful_prefix = []
        
        for i, word in enumerate(sorted(potential_prefix_words)):
            prefix_query = f"{word} {seed}"
            prefix_suggestions = await self.fetch_suggestions(prefix_query, country, language)
            
            # Проверяем реальные PREFIX расширения
            real_prefix = []
            for suggestion in prefix_suggestions:
                if suggestion.lower().startswith(word) and seed.lower() in suggestion.lower():
                    real_prefix.append(suggestion)
            
            if len(real_prefix) > 0:
                stage2_keywords.update(real_prefix)
                all_keywords.update(real_prefix)
                stage2_count += len(real_prefix)
                successful_prefix.append(word)
                
                print(f"[{i+1}/{len(potential_prefix_words)}] '{prefix_query}' → ✅ {len(real_prefix)} PREFIX!")
                for exp in real_prefix[:3]:
                    print(f"    • {exp}")
            elif i < 5:
                print(f"[{i+1}/{len(potential_prefix_words)}] '{prefix_query}' → ❌")
            
            await asyncio.sleep(random.uniform(0.5, 2.0))
        
        print(f"\n✅ ЭТАП 2 завершён!")
        print(f"Успешных PREFIX слов: {len(successful_prefix)}")
        print(f"PREFIX запросов: {stage2_count}\n")
        
        # ========================================
        # ИТОГОВАЯ СТАТИСТИКА
        # ========================================
        total_queries = len(cyrillic_modifiers) + len(potential_prefix_words)
        
        print(f"{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
        print(f"{'='*60}")
        print(f"ЭТАП 1:")
        print(f"  Запросов: {len(cyrillic_modifiers)}")
        print(f"  Результатов: {stage1_count}")
        print(f"  Извлечено слов: {len(potential_prefix_words)}")
        print(f"")
        print(f"ЭТАП 2:")
        print(f"  Запросов: {len(potential_prefix_words)}")
        print(f"  Успешных PREFIX: {len(successful_prefix)}")
        print(f"  PREFIX запросов: {stage2_count}")
        print(f"")
        print(f"ВСЕГО:")
        print(f"  Запросов: {total_queries}")
        print(f"  Уникальных ключей: {len(all_keywords)}")
        print(f"{'='*60}\n")
        
        if len(successful_prefix) > 0:
            print(f"🎉 Успешные PREFIX слова:")
            for word in successful_prefix[:20]:
                print(f"  • {word}")
            if len(successful_prefix) > 20:
                print(f"  ... и ещё {len(successful_prefix) - 20}\n")
        
        return {
            "method": "ADAPTIVE PREFIX",
            "queries": total_queries,
            "stage1": {
                "queries": len(cyrillic_modifiers),
                "results": stage1_count,
                "words_extracted": len(potential_prefix_words)
            },
            "stage2": {
                "queries": len(potential_prefix_words),
                "results": stage2_count,
                "valid_prefix": len(successful_prefix)
            },
            "keywords": list(all_keywords),
            "count": len(all_keywords)
        }
    
    # ========================================
    # МЕТОД 3: MORPHOLOGICAL ADAPTIVE (из main__12_.py)
    # ========================================
    async def method_morphological_adaptive(
        self,
        seed: str,
        country: str,
        language: str
    ) -> Dict:
        """
        MORPHOLOGICAL ADAPTIVE
        
        ЭТАП 1: Генерация морфологических форм
        ЭТАП 2: SUFFIX парсинг каждой формы
        ЭТАП 3: Извлечение слов-кандидатов (частотный анализ)
        ЭТАП 4: PREFIX проверка кандидатов
        """
        all_keywords = set()
        seed_words = set(seed.lower().split())
        
        print(f"\n{'='*60}")
        print(f"МЕТОД 3: MORPHOLOGICAL ADAPTIVE")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Language: {language.upper()}\n")
        
        # ЭТАП 1: Морфологические формы
        if language.lower() == 'ru':
            forms = [
                seed,
                "ремонта пылесосов",
                "по ремонту пылесосов"
            ]
        else:
            forms = [seed]
        
        print(f"ЭТАП 1: Морфологические формы ({len(forms)})")
        for form in forms:
            print(f"  • {form}")
        print()
        
        # ЭТАП 2: SUFFIX парсинг
        alphabet = "абвгдежзийклмнопрстуфхцчшщъыьэюя" if language.lower() == 'ru' else "abcdefghijklmnopqrstuvwxyz"
        
        print(f"ЭТАП 2: SUFFIX парсинг")
        all_suffix_results = []
        suffix_count = 0
        
        for form_idx, form in enumerate(forms, 1):
            print(f"--- Форма {form_idx}: '{form}' ---")
            
            for letter in alphabet:
                query = f"{form} {letter}"
                results = await self.fetch_suggestions(query, country, language)
                all_suffix_results.extend(results)
                suffix_count += 1
                await asyncio.sleep(random.uniform(1.0, 2.0))
            
            print(f"Результатов: {len([r for r in all_suffix_results if form in r])}\n")
        
        print(f"✅ SUFFIX: {suffix_count} запросов, {len(all_suffix_results)} результатов\n")
        
        # ЭТАП 3: Извлечение кандидатов
        print(f"ЭТАП 3: Извлечение слов-кандидатов")
        
        word_counter = Counter()
        for result in all_suffix_results:
            words = result.lower().split()
            for word in words:
                if word not in seed_words and len(word) > 2:
                    word_counter[word] += 1
        
        all_candidates = {w for w, count in word_counter.items() if count >= 2}
        
        print(f"Уникальных слов: {len(word_counter)}")
        print(f"После фильтрации (≥2): {len(all_candidates)}")
        print(f"\nТоп-20:")
        for word, count in word_counter.most_common(20):
            print(f"  • '{word}' ({count})")
        print()
        
        # ЭТАП 4: PREFIX проверка
        print(f"ЭТАП 4: PREFIX проверка")
        
        prefix_count = 0
        verified_count = 0
        
        for candidate in sorted(all_candidates):
            query = f"{candidate} {seed}"
            results = await self.fetch_suggestions(query, country, language)
            prefix_count += 1
            
            if results:
                all_keywords.update(results)
                verified_count += 1
                if verified_count <= 10:
                    print(f"✅ '{query}' → {len(results)}")
            
            await asyncio.sleep(random.uniform(1.0, 2.0))
        
        print(f"\nПроверено: {prefix_count}")
        print(f"Валидных PREFIX: {verified_count}\n")
        
        # СТАТИСТИКА
        total_queries = suffix_count + prefix_count
        
        print(f"{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
        print(f"{'='*60}")
        print(f"SUFFIX: {suffix_count} запросов")
        print(f"PREFIX проверка: {prefix_count} запросов")
        print(f"────────────────────────────────")
        print(f"ВСЕГО: {total_queries} запросов")
        print(f"")
        print(f"Кандидатов: {len(all_candidates)}")
        print(f"Валидных PREFIX: {verified_count}")
        print(f"Финальных ключей: {len(all_keywords)}")
        print(f"{'='*60}\n")
        
        return {
            "method": "MORPHOLOGICAL ADAPTIVE",
            "queries": total_queries,
            "suffix_queries": suffix_count,
            "prefix_queries": prefix_count,
            "candidates": len(all_candidates),
            "valid_prefix": verified_count,
            "keywords": list(all_keywords),
            "count": len(all_keywords)
        }


# ============================================
# PYDANTIC MODELS
# ============================================
class ParseRequest(BaseModel):
    seed: str
    country: str = "UA"
    language: str = "ru"
    use_numbers: bool = False
    use_morphology: bool = True
    method: str = "all"  # "suffix_infix", "adaptive_prefix", "morph_adaptive", "all"


# ============================================
# API ENDPOINTS
# ============================================

@app.get("/")
async def root():
    return {
        "api": "Google Autocomplete Parser v3.0",
        "version": "3.0",
        "methods": {
            "1": "SUFFIX + INFIX + MORPHOLOGY",
            "2": "ADAPTIVE PREFIX",
            "3": "MORPHOLOGICAL ADAPTIVE"
        },
        "endpoints": {
            "suffix_infix": "/api/parse/suffix-infix",
            "adaptive_prefix": "/api/parse/adaptive-prefix",
            "morph_adaptive": "/api/parse/morph-adaptive",
            "all": "/api/parse/all"
        }
    }


@app.get("/api/parse/suffix-infix")
async def parse_suffix_infix(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru"),
    use_numbers: bool = Query(False),
    use_morphology: bool = Query(True)
):
    """МЕТОД 1: SUFFIX + INFIX + MORPHOLOGY"""
    parser = AutocompleteParser()
    start = time.time()
    result = await parser.method_suffix_infix_morph(seed, country, language, use_numbers, use_morphology)
    result["time"] = round(time.time() - start, 2)
    return result


@app.get("/api/parse/adaptive-prefix")
async def parse_adaptive_prefix(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru")
):
    """МЕТОД 2: ADAPTIVE PREFIX"""
    parser = AutocompleteParser()
    start = time.time()
    result = await parser.method_adaptive_prefix(seed, country, language)
    result["time"] = round(time.time() - start, 2)
    return result


@app.get("/api/parse/morph-adaptive")
async def parse_morph_adaptive(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru")
):
    """МЕТОД 3: MORPHOLOGICAL ADAPTIVE"""
    parser = AutocompleteParser()
    start = time.time()
    result = await parser.method_morphological_adaptive(seed, country, language)
    result["time"] = round(time.time() - start, 2)
    return result


@app.post("/api/parse/all")
async def parse_all(request: ParseRequest):
    """ВСЕ МЕТОДЫ СРАЗУ"""
    parser = AutocompleteParser()
    start = time.time()
    
    results = {}
    all_keywords = set()
    
    # МЕТОД 1
    if request.method in ["suffix_infix", "all"]:
        result1 = await parser.method_suffix_infix_morph(
            request.seed,
            request.country,
            request.language,
            request.use_numbers,
            request.use_morphology
        )
        results["suffix_infix"] = result1
        all_keywords.update(result1["keywords"])
    
    # МЕТОД 2
    if request.method in ["adaptive_prefix", "all"]:
        result2 = await parser.method_adaptive_prefix(
            request.seed,
            request.country,
            request.language
        )
        results["adaptive_prefix"] = result2
        all_keywords.update(result2["keywords"])
    
    # МЕТОД 3
    if request.method in ["morph_adaptive", "all"]:
        result3 = await parser.method_morphological_adaptive(
            request.seed,
            request.country,
            request.language
        )
        results["morph_adaptive"] = result3
        all_keywords.update(result3["keywords"])
    
    total_time = round(time.time() - start, 2)
    
    return {
        "seed": request.seed,
        "methods_used": request.method,
        "results": results,
        "total_keywords": len(all_keywords),
        "total_time": total_time,
        "all_keywords": list(all_keywords)
    }


# ============================================
# DELAY TESTER (оптимизация задержек)
# ============================================
from fastapi import BackgroundTasks, HTTPException
from datetime import datetime
from typing import Tuple

# Global state для тестирования задержек
delay_test_state = {
    "is_running": False,
    "current_scenario": 0,
    "total_scenarios": 0,
    "progress": 0,
    "last_results": None,
    "start_time": None,
    "error": None
}


class DelayTester:
    """Тестировщик оптимальных задержек между запросами"""
    
    def __init__(self):
        self.base_url = "https://suggestqueries.google.com/complete/search"
        self.modifiers = list("абвгдежзийклмнопрстуфхцчшщэюя")
    
    async def fetch_suggestions_test(
        self, 
        query: str, 
        country: str = "UA", 
        language: str = "ru"
    ) -> Tuple[bool, int, float]:
        """Тестовый запрос к Google Autocomplete"""
        params = {
            "client": "chrome",
            "q": query,
            "gl": country,
            "hl": language
        }
        headers = {
            "User-Agent": random.choice(USER_AGENTS)
        }
        
        start = time.time()
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(self.base_url, params=params, headers=headers)
                elapsed = time.time() - start
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, list) and len(data) > 1:
                        results_count = len([s for s in data[1] if isinstance(s, str)])
                        return (True, results_count, elapsed)
                    else:
                        return (True, 0, elapsed)
                
                return (False, 0, elapsed)
                
        except Exception as e:
            elapsed = time.time() - start
            return (False, 0, elapsed)
    
    async def test_delay_range(
        self,
        min_delay: float,
        max_delay: float,
        num_requests: int,
        seed: str,
        country: str,
        language: str
    ) -> Dict:
        """Тестирование одного диапазона задержек"""
        
        successes = 0
        failures = 0
        total_results = 0
        response_times = []
        
        start_time = time.time()
        
        for i in range(num_requests):
            modifier = self.modifiers[i % len(self.modifiers)]
            query = f"{seed} {modifier}"
            
            success, results, resp_time = await self.fetch_suggestions_test(query, country, language)
            
            if success:
                successes += 1
                total_results += results
            else:
                failures += 1
            
            response_times.append(resp_time)
            
            if i < num_requests - 1:
                delay = random.uniform(min_delay, max_delay)
                await asyncio.sleep(delay)
        
        total_time = time.time() - start_time
        success_rate = (successes / num_requests) * 100
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        avg_results_per_request = total_results / num_requests if num_requests > 0 else 0
        
        return {
            "delay_range": [min_delay, max_delay],
            "num_requests": num_requests,
            "successes": successes,
            "failures": failures,
            "success_rate": round(success_rate, 2),
            "total_results": total_results,
            "avg_results_per_request": round(avg_results_per_request, 2),
            "total_time": round(total_time, 2),
            "avg_response_time": round(avg_response_time, 3),
            "avg_delay": round((min_delay + max_delay) / 2, 2)
        }
    
    async def test_all_scenarios(
        self,
        scenarios: List[Tuple[float, float]],
        num_requests_per_scenario: int,
        pause_between_scenarios: float,
        seed: str,
        country: str,
        language: str
    ) -> Dict:
        """Тестирование всех сценариев задержек"""
        
        global delay_test_state
        
        results = []
        delay_test_state["total_scenarios"] = len(scenarios)
        delay_test_state["start_time"] = datetime.now().isoformat()
        
        for i, (min_delay, max_delay) in enumerate(scenarios):
            delay_test_state["current_scenario"] = i + 1
            delay_test_state["progress"] = int((i / len(scenarios)) * 100)
            
            result = await self.test_delay_range(
                min_delay=min_delay,
                max_delay=max_delay,
                num_requests=num_requests_per_scenario,
                seed=seed,
                country=country,
                language=language
            )
            
            results.append(result)
            
            if i < len(scenarios) - 1:
                await asyncio.sleep(pause_between_scenarios)
        
        delay_test_state["progress"] = 100
        
        # Рекомендация
        recommendation = self.get_recommendation(results)
        
        final_result = {
            "test_timestamp": datetime.now().isoformat(),
            "test_summary": {
                "total_scenarios": len(results),
                "total_requests": sum(r['num_requests'] for r in results),
                "total_time": round(sum(r['total_time'] for r in results), 2)
            },
            "scenarios": results,
            "recommendation": recommendation
        }
        
        return final_result
    
    def get_recommendation(self, results: List[Dict]) -> Dict:
        """Получить рекомендацию по оптимальной задержке"""
        safe_results = [r for r in results if r['success_rate'] >= 95]
        
        if safe_results:
            fastest = min(safe_results, key=lambda x: x['total_time'])
            return {
                "optimal_delay_range": fastest['delay_range'],
                "success_rate": fastest['success_rate'],
                "total_time": fastest['total_time'],
                "avg_results_per_request": fastest['avg_results_per_request'],
                "status": "found",
                "message": f"Оптимальная задержка: {fastest['delay_range'][0]}-{fastest['delay_range'][1]} сек"
            }
        else:
            best = max(results, key=lambda x: x['success_rate'])
            return {
                "optimal_delay_range": best['delay_range'],
                "success_rate": best['success_rate'],
                "total_time": best['total_time'],
                "avg_results_per_request": best['avg_results_per_request'],
                "status": "no_safe_option_found",
                "message": "Безопасный диапазон не найден, используйте консервативные настройки"
            }


class DelayTestRequest(BaseModel):
    """Параметры для теста задержек"""
    scenarios: Optional[List[List[float]]] = None  # [[0.5, 2.0], [0.4, 1.0], ...]
    num_requests_per_scenario: int = 50
    pause_between_scenarios: float = 30.0
    seed: str = "ремонт пылесосов"
    country: str = "UA"
    language: str = "ru"


async def run_delay_test_background(request: DelayTestRequest):
    """Фоновая задача для тестирования задержек"""
    global delay_test_state
    
    try:
        delay_test_state["is_running"] = True
        delay_test_state["error"] = None
        
        tester = DelayTester()
        
        # Сценарии по умолчанию (от консервативного к агрессивному)
        scenarios = request.scenarios or [
            [0.5, 2.0],
            [0.5, 1.5],
            [0.4, 1.0],
            [0.3, 0.7],
            [0.2, 0.5],
            [0.1, 0.3],
        ]
        
        # Конвертируем в кортежи
        scenarios_tuples = [(s[0], s[1]) for s in scenarios]
        
        results = await tester.test_all_scenarios(
            scenarios=scenarios_tuples,
            num_requests_per_scenario=request.num_requests_per_scenario,
            pause_between_scenarios=request.pause_between_scenarios,
            seed=request.seed,
            country=request.country,
            language=request.language
        )
        
        delay_test_state["last_results"] = results
        
    except Exception as e:
        delay_test_state["error"] = str(e)
    
    finally:
        delay_test_state["is_running"] = False
        delay_test_state["current_scenario"] = 0
        delay_test_state["progress"] = 0


# ============================================
# ENDPOINTS ДЛЯ ТЕСТИРОВАНИЯ ЗАДЕРЖЕК
# ============================================

@app.post("/api/test-delays")
async def start_delay_test(
    request: DelayTestRequest,
    background_tasks: BackgroundTasks
):
    """
    Запустить тест оптимальных задержек в фоновом режиме
    
    Тестирует разные диапазоны задержек между запросами к Google Autocomplete
    чтобы найти минимальную безопасную задержку (без блокировок)
    """
    global delay_test_state
    
    if delay_test_state["is_running"]:
        raise HTTPException(status_code=400, detail="Тест уже выполняется")
    
    # Запускаем в фоне
    background_tasks.add_task(run_delay_test_background, request)
    
    scenarios_count = len(request.scenarios) if request.scenarios else 6
    
    return {
        "status": "started",
        "message": "Тест задержек запущен в фоновом режиме",
        "estimated_time_minutes": (scenarios_count * 2 + 5)
    }


@app.get("/api/test-delays/status")
async def get_delay_test_status():
    """Получить статус текущего теста задержек"""
    global delay_test_state
    
    return {
        "is_running": delay_test_state["is_running"],
        "current_scenario": delay_test_state["current_scenario"],
        "total_scenarios": delay_test_state["total_scenarios"],
        "progress": delay_test_state["progress"],
        "start_time": delay_test_state["start_time"],
        "error": delay_test_state["error"]
    }


@app.get("/api/test-delays/results")
async def get_delay_test_results():
    """Получить результаты последнего теста задержек"""
    global delay_test_state
    
    if delay_test_state["is_running"]:
        raise HTTPException(status_code=400, detail="Тест ещё выполняется. Проверьте статус через /api/test-delays/status")
    
    if delay_test_state["last_results"] is None:
        raise HTTPException(status_code=404, detail="Результаты не найдены. Запустите тест через POST /api/test-delays")
    
    return delay_test_state["last_results"]


# ============================================
# ГЛАВНАЯ СТРАНИЦА (ОБНОВЛЁННАЯ)
# ============================================

@app.get("/")
async def root():
    """Главная страница API"""
    return {
        "service": "Google Autocomplete Parser + Delay Optimizer",
        "version": "3.1",
        "endpoints": {
            "parser": {
                "suffix_infix": "GET /api/parse/suffix-infix",
                "adaptive_prefix": "GET /api/parse/adaptive-prefix",
                "morph_adaptive": "GET /api/parse/morph-adaptive",
                "all_methods": "POST /api/parse/all"
            },
            "delay_optimizer": {
                "start_test": "POST /api/test-delays",
                "check_status": "GET /api/test-delays/status",
                "get_results": "GET /api/test-delays/results"
            }
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
