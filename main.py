"""
GEMINI BIGRAM TEST - ИСПРАВЛЕННАЯ ВЕРСИЯ
С User-Agent ротацией и задержками
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os
import httpx
import asyncio
import time
import random

app = FastAPI(title="Gemini Bigram Test Fixed", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# User-Agent ротация для избежания блокировки
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
    
    async def gemini_bigram_test(self, seed: str, country: str, language: str) -> List[str]:
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"🔬 GEMINI BIGRAM TEST (FIXED)")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"✅ User-Agent ротация включена")
        print(f"✅ Задержки 1-2 сек между запросами\n")
        
        first_word = seed.split()[0]
        print(f"Первое слово: '{first_word}'\n")
        
        print(f"{'='*60}")
        print(f"ТЕСТ: Топ-20 биграмм")
        print(f"{'='*60}\n")
        
        top_bigrams = {
            "се": "сервис, сервисный",
            "ср": "срочный",
            "гд": "где",
            "ма": "мастер, мастерская",
            "не": "недорогой",
            "де": "дешевый",
            "пр": "профессиональный",
            "ка": "как, качественный",
            "сл": "сложный",
            "от": "отличный",
            "ск": "сколько",
            "со": "современный",
            "це": "центр, цена",
            "ча": "частный",
            "ко": "коммерческий",
            "ме": "мелкий",
            "бе": "бесплатный",
            "ре": "ремонт",
            "на": "надежный",
            "ку": "купить"
        }
        
        discovered_words = set()
        total_queries = 0
        
        for bigram, expected in top_bigrams.items():
            query = f"{bigram} {first_word}"
            results = await self.fetch_suggestions(query, country, language)
            total_queries += 1
            
            print(f"'{query}' (ожидаем: {expected})")
            print(f"  Результатов: {len(results)}")
            
            if len(results) > 0:
                found_expansions = []
                
                for result in results:
                    if result.lower().startswith(bigram.lower()):
                        after_bigram = result[len(bigram):].strip()
                        
                        if first_word.lower() in after_bigram.lower():
                            word_pos = after_bigram.lower().find(first_word.lower())
                            if word_pos > 0:
                                expanded_word = after_bigram[:word_pos].strip()
                                if expanded_word:
                                    found_expansions.append(expanded_word)
                                    discovered_words.add(expanded_word)
                
                if len(found_expansions) > 0:
                    print(f"  ✅ НАЙДЕНЫ РАСШИРЕНИЯ:")
                    for word in set(found_expansions):
                        print(f"     🎯 '{word}'")
                else:
                    print(f"  ❌ Расширения НЕ найдены")
                    for r in results[:2]:
                        print(f"     • {r}")
            else:
                print(f"  ❌ Нет результатов")
            
            print()
            # ВАЖНО: Задержка между запросами!
            await asyncio.sleep(random.uniform(1.0, 2.0))
        
        print(f"{'='*60}")
        print(f"✅ ТЕСТ завершён")
        print(f"{'='*60}")
        print(f"Запросов: {total_queries}")
        print(f"Найдено расширений: {len(discovered_words)}\n")
        
        if len(discovered_words) > 0:
            print(f"🎉 БИГРАММЫ РАБОТАЮТ!")
            for word in sorted(discovered_words):
                print(f"  • {word}")
            
            print(f"\n{'='*60}")
            print(f"ПРОВЕРКА PREFIX")
            print(f"{'='*60}\n")
            
            for word in sorted(discovered_words):
                full_query = f"{word} {seed}"
                results = await self.fetch_suggestions(full_query, country, language)
                total_queries += 1
                
                if len(results) > 0:
                    all_keywords.update(results)
                    print(f"✅ '{full_query}' → {len(results)} ключей")
                else:
                    print(f"❌ '{full_query}' → нет")
                
                await asyncio.sleep(random.uniform(1.0, 2.0))
        else:
            print(f"❌ БИГРАММЫ НЕ РАБОТАЮТ")
        
        print(f"\n{'='*60}")
        print(f"ИТОГО ключей: {len(all_keywords)}")
        print(f"{'='*60}\n")
        
        return list(all_keywords)


@app.get("/api/test-parser/gemini-bigram")
async def test_gemini_bigram(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru")
):
    parser = AutocompleteParser()
    start = time.time()
    keywords = await parser.gemini_bigram_test(seed, country, language)
    return {
        "seed": seed,
        "method": "Gemini Bigram (Fixed)",
        "keywords": keywords,
        "count": len(keywords),
        "time": round(time.time() - start, 2)
    }


@app.get("/")
async def root():
    return {
        "api": "Gemini Bigram Test (Fixed)",
        "url": "/api/test-parser/gemini-bigram?seed=ремонт пылесосов&country=UA&language=ru"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
