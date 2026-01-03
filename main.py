"""
MORPHOLOGICAL ADAPTIVE TEST - ИСПРАВЛЕННАЯ ВЕРСИЯ
С User-Agent ротацией и задержками
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from collections import Counter
import os
import httpx
import asyncio
import time
import random

app = FastAPI(title="Morphological ADAPTIVE Test Fixed", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# User-Agent ротация
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

class AutocompleteParser:
    def __init__(self):
        self.base_url = "https://suggestqueries.google.com/complete/search"
    
    async def fetch_suggestions(self, query: str, country: str, language: str) -> List[str]:
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
    
    async def morphological_adaptive_test(self, seed: str, country: str, language: str) -> List[str]:
        all_keywords = set()
        seed_words = set(seed.lower().split())
        
        print(f"\n{'='*60}")
        print(f"🔬 MORPHOLOGICAL ADAPTIVE (FIXED)")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"✅ User-Agent ротация включена")
        print(f"✅ Задержки 1-2 сек между запросами\n")
        
        # ЭТАП 1: Генерация морфологических форм
        print(f"{'='*60}")
        print(f"ЭТАП 1: Генерация морфологических форм")
        print(f"{'='*60}\n")
        
        # Для "ремонт пылесосов" создаём формы вручную
        forms = [
            seed,                           # "ремонт пылесосов"
            "ремонта пылесосов",           # родительный
            "по ремонту пылесосов"         # предлог + дательный
        ]
        
        print(f"Форм: {len(forms)}")
        for i, form in enumerate(forms, 1):
            print(f"  {i}. '{form}'")
        print()
        
        # ЭТАП 2: SUFFIX парсинг для каждой формы
        print(f"{'='*60}")
        print(f"ЭТАП 2: SUFFIX парсинг")
        print(f"{'='*60}\n")
        
        alphabet = "абвгдежзийклмнопрстуфхцчшщъыьэюя"
        all_suffix_results = []
        suffix_count = 0
        
        for form_idx, form in enumerate(forms, 1):
            print(f"--- Форма {form_idx}: '{form}' ---")
            form_results = []
            
            for letter in alphabet:
                query = f"{form} {letter}"
                results = await self.fetch_suggestions(query, country, language)
                form_results.extend(results)
                all_suffix_results.extend(results)
                suffix_count += 1
                await asyncio.sleep(random.uniform(1.0, 2.0))
            
            print(f"Результатов: {len(form_results)}")
            if form_results:
                for r in form_results[:3]:
                    print(f"  • {r}")
            print()
        
        print(f"SUFFIX запросов: {suffix_count}")
        print(f"Всего результатов: {len(all_suffix_results)}\n")
        
        # ЭТАП 3: Извлечение слов-кандидатов
        print(f"{'='*60}")
        print(f"ЭТАП 3: Извлечение слов-кандидатов")
        print(f"{'='*60}\n")
        
        word_counter = Counter()
        
        for result in all_suffix_results:
            words = result.lower().split()
            for word in words:
                if word not in seed_words and len(word) > 2:
                    word_counter[word] += 1
        
        # Частотная фильтрация
        all_candidates = {w for w, count in word_counter.items() if count >= 2}
        
        print(f"Уникальных слов: {len(word_counter)}")
        print(f"После фильтрации (≥2): {len(all_candidates)}")
        print(f"\nТоп-20:")
        for word, count in word_counter.most_common(20):
            print(f"  • '{word}' ({count})")
        print()
        
        # ЭТАП 4: Анализ новых слов
        print(f"{'='*60}")
        print(f"ЭТАП 4: Анализ НОВЫХ слов от морфологии")
        print(f"{'='*60}\n")
        
        base_form_words = set()
        morpho_form_words = set()
        
        # Слова от базовой формы (первая треть результатов)
        base_count = len(all_suffix_results) // 3
        for result in all_suffix_results[:base_count]:
            words = result.lower().split()
            for word in words:
                if word not in seed_words and len(word) > 2:
                    base_form_words.add(word)
        
        # Слова от морфологических форм
        for result in all_suffix_results[base_count:]:
            words = result.lower().split()
            for word in words:
                if word not in seed_words and len(word) > 2:
                    morpho_form_words.add(word)
        
        new_from_morphology = morpho_form_words - base_form_words
        
        print(f"От базовой формы: {len(base_form_words)}")
        print(f"От морфо форм: {len(morpho_form_words)}")
        print(f"НОВЫХ от морфологии: {len(new_from_morphology)}")
        
        if new_from_morphology:
            print(f"\nНовые слова:")
            for word in sorted(list(new_from_morphology)[:20]):
                print(f"  • {word}")
        print()
        
        # ЭТАП 5: PREFIX проверка
        print(f"{'='*60}")
        print(f"ЭТАП 5: PREFIX проверка")
        print(f"{'='*60}\n")
        
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
        print(f"Валидных PREFIX: {verified_count}")
        print()
        
        # СТАТИСТИКА
        total_queries = suffix_count + prefix_count
        
        print(f"{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
        print(f"{'='*60}")
        print(f"SUFFIX: {suffix_count} запросов")
        print(f"  - Базовая форма: 29")
        print(f"  - Морфо формы: {suffix_count - 29}")
        print(f"PREFIX проверка: {prefix_count} запросов")
        print(f"──────────────────────────────────")
        print(f"ВСЕГО: {total_queries} запросов")
        print(f"")
        print(f"Кандидатов: {len(all_candidates)}")
        print(f"Валидных PREFIX: {verified_count}")
        print(f"Финальных ключей: {len(all_keywords)}")
        print(f"")
        
        if len(all_candidates) > 0:
            print(f"ЭФФЕКТИВНОСТЬ:")
            print(f"  Кандидатов на запрос: {len(all_candidates)/suffix_count:.2f}")
            print(f"  Валидация: {verified_count}/{len(all_candidates)} = {verified_count/len(all_candidates)*100:.1f}%")
            print(f"  Ключей на запрос: {len(all_keywords)/total_queries:.2f}")
        print(f"")
        
        if len(new_from_morphology) > 0:
            print(f"✅ МОРФОЛОГИЯ ДАЛА РЕЗУЛЬТАТ!")
            print(f"Новых слов: {len(new_from_morphology)} (+{len(new_from_morphology)/len(base_form_words)*100:.1f}%)")
        else:
            print(f"⚠️ Морфология не дала новых слов")
        
        print(f"{'='*60}\n")
        
        return list(all_keywords)


@app.get("/api/test-parser/morphology")
async def test_morphology(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru")
):
    parser = AutocompleteParser()
    start = time.time()
    keywords = await parser.morphological_adaptive_test(seed, country, language)
    return {
        "seed": seed,
        "method": "Morphological ADAPTIVE (Fixed)",
        "keywords": keywords,
        "count": len(keywords),
        "time": round(time.time() - start, 2)
    }


@app.get("/")
async def root():
    return {
        "api": "Morphological ADAPTIVE Test (Fixed)",
        "url": "/api/test-parser/morphology?seed=ремонт пылесосов&country=UA&language=ru"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
