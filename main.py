"""
CSG (Context-Shift Graph) TEST - метод от ChatGPT
ЦЕЛЬ: Найти "сервисный центр ремонт пылесосов"
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os
import httpx
import asyncio
import time
import random

app = FastAPI(title="CSG Test", version="1.0")

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
    
    async def csg_test(self, seed: str, country: str, language: str) -> List[str]:
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"🔬 CSG - CONTEXT-SHIFT GRAPH")
        print(f"{'='*60}")
        print(f"Seed: '{seed}' | ЦЕЛЬ: 'сервисный центр ремонт пылесосов'\n")
        
        test_anchors = ["киев", "москва", "астана"]
        print(f"Якоря: {', '.join(test_anchors)}\n")
        
        # ЭТАП 1: Context Shift
        print(f"{'='*60}")
        print(f"ЭТАП 1: Context Shift")
        print(f"{'='*60}\n")
        
        for anchor in test_anchors:
            query = f"{anchor} {seed}"
            results = await self.fetch_suggestions(query, country, language)
            print(f"'{query}' → {len(results)} результатов")
            for s in results[:3]:
                print(f"  • {s}")
            await asyncio.sleep(random.uniform(0.5, 1.5))
        
        print(f"\n✅ Этап 1 завершён\n")
        
        # ЭТАП 2: Вторичное расширение
        print(f"{'='*60}")
        print(f"ЭТАП 2: Вторичное расширение")
        print(f"{'='*60}")
        print(f"Проверка: Google вставляет слова МЕЖДУ якорем и seed?\n")
        
        target_letters = {
            "с": "'сервис', 'сервисный', 'срочный'",
            "се": "'сервис', 'сервисный'",
            "сер": "'сервис', 'сервисный'",
            "серв": "'сервисный'",
            "г": "'где', 'гарантийный'",
            "н": "'недорогой'",
            "ц": "'центр'",
            "м": "'мастер'",
        }
        
        discovered = set()
        total_queries = 3
        
        for anchor in test_anchors:
            print(f"\n--- Якорь: '{anchor}' ---")
            
            for letter, desc in target_letters.items():
                query = f"{anchor} {seed} {letter}"
                results = await self.fetch_suggestions(query, country, language)
                total_queries += 1
                
                inserted = []
                for s in results:
                    if s.lower().startswith(anchor.lower()):
                        after = s[len(anchor):].strip()
                        if seed.lower() in after.lower():
                            pos = after.lower().find(seed.lower())
                            if pos > 0:
                                before = after[:pos].strip()
                                if before:
                                    inserted.append(before)
                                    discovered.add(before)
                
                status = "✅ ВСТАВКА!" if inserted else "❌ нет"
                print(f"  '{query}' ({desc})")
                print(f"    {len(results)} результатов | {status}")
                
                if inserted:
                    print(f"    ВСТАВКИ:")
                    for w in set(inserted):
                        print(f"      🎯 '{w}'")
                        for s in results:
                            if w in s:
                                print(f"         {s}")
                                break
                
                await asyncio.sleep(random.uniform(0.5, 1.5))
        
        print(f"\n{'='*60}")
        print(f"✅ Этап 2 завершён")
        print(f"{'='*60}")
        print(f"Запросов: {total_queries} | PREFIX слов: {len(discovered)}")
        
        if discovered:
            print(f"\n🎉 CSG РАБОТАЕТ! Найдены:")
            for w in sorted(discovered):
                print(f"  • {w}")
            
            # ЭТАП 3: Проверка PREFIX
            print(f"\n{'='*60}")
            print(f"ЭТАП 3: Проверка PREFIX")
            print(f"{'='*60}\n")
            
            prefix_kw = set()
            for w in sorted(discovered):
                query = f"{w} {seed}"
                results = await self.fetch_suggestions(query, country, language)
                total_queries += 1
                
                if results:
                    prefix_kw.update(results)
                    all_keywords.update(results)
                    print(f"✅ '{query}' → {len(results)} PREFIX")
                    for s in results[:3]:
                        print(f"    • {s}")
                else:
                    print(f"❌ '{query}' → нет")
                
                await asyncio.sleep(random.uniform(0.5, 1.5))
            
            print(f"\n{'='*60}")
            print(f"📊 СТАТИСТИКА")
            print(f"{'='*60}")
            print(f"Запросов: {total_queries}")
            print(f"PREFIX слов: {len(discovered)}")
            print(f"PREFIX ключей: {len(prefix_kw)}")
            
            if "сервисный центр" in discovered or "сервис" in discovered:
                print(f"\n🎯 ЦЕЛЬ ДОСТИГНУТА!")
            else:
                print(f"\n❌ Цель НЕ достигнута")
        else:
            print(f"\n❌ CSG НЕ РАБОТАЕТ!")
            print(f"Google НЕ вставляет слова между якорем и seed")
        
        print(f"\n{'='*60}")
        print(f"ИТОГО ключей: {len(all_keywords)}")
        print(f"{'='*60}\n")
        
        return list(all_keywords)


@app.get("/api/test-parser/csg")
async def test(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru")
):
    parser = AutocompleteParser()
    start = time.time()
    keywords = await parser.csg_test(seed, country, language)
    return {
        "seed": seed,
        "method": "CSG",
        "keywords": keywords,
        "count": len(keywords),
        "time": round(time.time() - start, 2)
    }


@app.get("/")
async def root():
    return {"api": "CSG Test", "url": "/api/test-parser/csg?seed=ремонт пылесосов&country=UA&language=ru"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
