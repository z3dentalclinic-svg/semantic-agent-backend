"""
ChatGPT PPM TEST - PREFIX Projection Method
Статистическая реконструкция PREFIX через анализ n-грамм
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

app = FastAPI(title="ChatGPT PPM Test", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AutocompleteParser:
    def __init__(self):
        self.base_url = "https://suggestqueries.google.com/complete/search"
    
    async def fetch_suggestions(self, query: str, country: str, language: str) -> List[str]:
        params = {"client": "chrome", "q": query, "gl": country, "hl": language}
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(self.base_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 1:
                        return [s for s in data[1] if isinstance(s, str)]
                return []
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    async def chatgpt_ppm_test(self, seed: str, country: str, language: str) -> List[str]:
        all_keywords = set()
        seed_words = set(seed.lower().split())
        
        print(f"\n{'='*60}")
        print(f"🔬 ChatGPT PPM - PREFIX Projection Method")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Метод: Статистическая реконструкция через n-граммы\n")
        
        # ========================================
        # ЭТАП 1: Базовый SUFFIX парсинг
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 1: Базовый SUFFIX парсинг")
        print(f"{'='*60}\n")
        
        alphabet = "абвгдежзийклмнопрстуфхцчшщъыьэюя"
        suffix_results = []
        
        for letter in alphabet:
            query = f"{seed} {letter}"
            results = await self.fetch_suggestions(query, country, language)
            suffix_results.extend(results)
            await asyncio.sleep(random.uniform(0.3, 0.6))
        
        print(f"Базовый SUFFIX: 29 запросов")
        print(f"Получено SUFFIX ключей: {len(suffix_results)}\n")
        
        # ========================================
        # ЭТАП 2: Отбор топ-30 SUFFIX для вторичного расширения
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 2: Отбор топ-30 SUFFIX")
        print(f"{'='*60}\n")
        
        # Берём первые 30 результатов (самые релевантные)
        top_suffix = suffix_results[:30] if len(suffix_results) >= 30 else suffix_results
        
        print(f"Отобрано для вторичного расширения: {len(top_suffix)}")
        print(f"Примеры:")
        for s in top_suffix[:5]:
            print(f"  • {s}")
        print()
        
        # ========================================
        # ЭТАП 3: Вторичное расширение (КЛЮЧЕВОЙ ЭТАП!)
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 3: Вторичное расширение топ SUFFIX")
        print(f"{'='*60}")
        print(f"Цель: найти ДЛИННЫЕ цепочки запросов\n")
        
        # Целевые буквы для расширения (топ-8 по частоте)
        expansion_letters = ["а", "б", "в", "г", "с", "м", "н", "к"]
        all_expansions = []
        expansion_count = 0
        
        for suffix_key in top_suffix:
            for letter in expansion_letters:
                query = f"{suffix_key} {letter}"
                results = await self.fetch_suggestions(query, country, language)
                all_expansions.extend(results)
                expansion_count += 1
                await asyncio.sleep(random.uniform(0.3, 0.6))
        
        print(f"Вторичное расширение: {expansion_count} запросов")
        print(f"Получено расширенных результатов: {len(all_expansions)}\n")
        
        # ========================================
        # ЭТАП 4: Частотный анализ n-грамм
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 4: Частотный анализ n-грамм")
        print(f"{'='*60}\n")
        
        bigrams = Counter()
        trigrams = Counter()
        
        for result in all_expansions:
            words = result.lower().split()
            
            # Биграммы
            for i in range(len(words) - 1):
                bigram = f"{words[i]} {words[i+1]}"
                # Только если НЕ часть seed
                if words[i] not in seed_words and words[i+1] not in seed_words:
                    bigrams[bigram] += 1
            
            # Триграммы
            for i in range(len(words) - 2):
                trigram = f"{words[i]} {words[i+1]} {words[i+2]}"
                if words[i] not in seed_words:
                    trigrams[trigram] += 1
        
        # Фильтруем частотные
        frequent_bigrams = {k: v for k, v in bigrams.items() if v >= 3}
        frequent_trigrams = {k: v for k, v in trigrams.items() if v >= 2}
        
        print(f"Найдено частотных биграмм (≥3 раз): {len(frequent_bigrams)}")
        print(f"Топ-10 биграмм:")
        for bigram, freq in sorted(frequent_bigrams.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  • '{bigram}' ({freq} раз)")
        
        print(f"\nНайдено частотных триграмм (≥2 раз): {len(frequent_trigrams)}")
        print(f"Топ-10 триграмм:")
        for trigram, freq in sorted(frequent_trigrams.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  • '{trigram}' ({freq} раз)")
        print()
        
        # ========================================
        # ЭТАП 5: Математическая проекция PREFIX
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 5: Математическая проекция PREFIX")
        print(f"{'='*60}\n")
        
        prefix_candidates = set()
        projection_count = 0
        
        # Проверяем биграммы
        for ngram in frequent_bigrams.keys():
            test_query = f"{ngram} {seed}"
            results = await self.fetch_suggestions(test_query, country, language)
            projection_count += 1
            
            if results:
                prefix_candidates.add(ngram)
                print(f"✅ Биграмма '{ngram}' → PREFIX подтверждён")
            
            await asyncio.sleep(random.uniform(0.3, 0.6))
        
        # Проверяем триграммы
        for ngram in list(frequent_trigrams.keys())[:20]:  # Ограничиваем топ-20
            test_query = f"{ngram} {seed}"
            results = await self.fetch_suggestions(test_query, country, language)
            projection_count += 1
            
            if results:
                prefix_candidates.add(ngram)
                print(f"✅ Триграмма '{ngram}' → PREFIX подтверждён")
            
            await asyncio.sleep(random.uniform(0.3, 0.6))
        
        print(f"\nПроекция: {projection_count} запросов")
        print(f"Найдено PREFIX кандидатов: {len(prefix_candidates)}\n")
        
        # ========================================
        # ЭТАП 6: Сбор финальных PREFIX ключей
        # ========================================
        if len(prefix_candidates) > 0:
            print(f"{'='*60}")
            print(f"ЭТАП 6: Сбор финальных PREFIX ключей")
            print(f"{'='*60}\n")
            
            for candidate in prefix_candidates:
                query = f"{candidate} {seed}"
                results = await self.fetch_suggestions(query, country, language)
                
                if results:
                    all_keywords.update(results)
                    print(f"'{candidate}' → {len(results)} ключей")
                
                await asyncio.sleep(random.uniform(0.3, 0.6))
        
        # ========================================
        # ФИНАЛЬНАЯ СТАТИСТИКА
        # ========================================
        total_queries = 29 + expansion_count + projection_count
        
        print(f"\n{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА PPM")
        print(f"{'='*60}")
        print(f"Базовый SUFFIX: 29 запросов")
        print(f"Вторичное расширение: {expansion_count} запросов")
        print(f"Математическая проекция: {projection_count} запросов")
        print(f"──────────────────────────────────")
        print(f"ВСЕГО запросов: {total_queries}")
        print(f"")
        print(f"Найдено PREFIX кандидатов: {len(prefix_candidates)}")
        print(f"Финальных PREFIX ключей: {len(all_keywords)}")
        print(f"")
        
        if len(all_keywords) > 0:
            print(f"🎉 PPM РАБОТАЕТ!")
            print(f"Статистическая реконструкция нашла PREFIX запросы!")
        else:
            print(f"❌ PPM не дал результатов")
            print(f"Частотные паттерны не содержат PREFIX слов")
        
        print(f"{'='*60}\n")
        
        return list(all_keywords)


@app.get("/api/test-parser/chatgpt-ppm")
async def test_chatgpt_ppm(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru")
):
    parser = AutocompleteParser()
    start = time.time()
    keywords = await parser.chatgpt_ppm_test(seed, country, language)
    return {
        "seed": seed,
        "method": "ChatGPT PPM",
        "keywords": keywords,
        "count": len(keywords),
        "time": round(time.time() - start, 2)
    }


@app.get("/")
async def root():
    return {
        "api": "ChatGPT PPM Test",
        "url": "/api/test-parser/chatgpt-ppm?seed=ремонт пылесосов&country=UA&language=ru"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
