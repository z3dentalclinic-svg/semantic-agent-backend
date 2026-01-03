"""
GEMINI BIGRAM TEST - Двухбуквенные префиксы
Тестирование: работает ли "се ремонт" → "сервисный ремонт"?
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os
import httpx
import asyncio
import time
import random

app = FastAPI(title="Gemini Bigram Test", version="1.0")

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
    
    async def gemini_bigram_test(self, seed: str, country: str, language: str) -> List[str]:
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"🔬 GEMINI BIGRAM TEST")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Гипотеза: 'се ремонт' → 'сервисный ремонт'\n")
        
        # Берём только ПЕРВОЕ слово из seed
        first_word = seed.split()[0]
        print(f"Первое слово: '{first_word}'\n")
        
        # ========================================
        # ЭТАП 1: Тест топ-20 биграмм
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 1: Топ-20 биграмм (быстрый тест)")
        print(f"{'='*60}\n")
        
        # Топ частотные биграммы для русского языка
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
            # Биграммный запрос
            query = f"{bigram} {first_word}"
            results = await self.fetch_suggestions(query, country, language)
            total_queries += 1
            
            print(f"'{query}' (ожидаем: {expected})")
            print(f"  Результатов: {len(results)}")
            
            if len(results) == 0:
                print(f"  ❌ Нет результатов\n")
                continue
            
            # Анализируем результаты
            found_expansions = []
            
            for result in results:
                # Проверяем начинается ли с биграммы
                if result.lower().startswith(bigram.lower()):
                    # Убираем биграмму
                    after_bigram = result[len(bigram):].strip()
                    
                    # Проверяем есть ли наше первое слово
                    if first_word.lower() in after_bigram.lower():
                        # Извлекаем что между биграммой и first_word
                        word_pos = after_bigram.lower().find(first_word.lower())
                        if word_pos > 0:
                            expanded_word = after_bigram[:word_pos].strip()
                            if expanded_word:
                                found_expansions.append(expanded_word)
                                discovered_words.add(expanded_word)
            
            # Показываем результаты
            if len(found_expansions) > 0:
                print(f"  ✅ НАЙДЕНЫ РАСШИРЕНИЯ:")
                for word in set(found_expansions):
                    print(f"     🎯 '{word}'")
                    for r in results:
                        if word in r:
                            print(f"        Пример: {r}")
                            break
            else:
                print(f"  ❌ Расширения НЕ найдены")
                print(f"  Примеры результатов:")
                for r in results[:3]:
                    print(f"     • {r}")
            
            print()
            await asyncio.sleep(random.uniform(0.3, 0.8))
        
        print(f"{'='*60}")
        print(f"✅ ЭТАП 1 завершён")
        print(f"{'='*60}")
        print(f"Запросов: {total_queries}")
        print(f"Найдено расширений: {len(discovered_words)}")
        
        if len(discovered_words) > 0:
            print(f"\n🎉 БИГРАММЫ РАБОТАЮТ!")
            print(f"Найдены слова:\n")
            for word in sorted(discovered_words):
                print(f"  • {word}")
            
            # ========================================
            # ЭТАП 2: Верификация с полным seed
            # ========================================
            print(f"\n{'='*60}")
            print(f"ЭТАП 2: Верификация PREFIX")
            print(f"{'='*60}\n")
            
            verified_keywords = set()
            
            for word in sorted(discovered_words):
                full_query = f"{word} {seed}"
                results = await self.fetch_suggestions(full_query, country, language)
                total_queries += 1
                
                if len(results) > 0:
                    verified_keywords.update(results)
                    all_keywords.update(results)
                    print(f"✅ '{full_query}' → {len(results)} ключей")
                    for r in results[:3]:
                        print(f"    • {r}")
                else:
                    print(f"❌ '{full_query}' → нет")
                
                await asyncio.sleep(random.uniform(0.3, 0.8))
            
            print(f"\n{'='*60}")
            print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
            print(f"{'='*60}")
            print(f"Запросов: {total_queries}")
            print(f"Найдено слов: {len(discovered_words)}")
            print(f"Верифицированных PREFIX: {len(verified_keywords)}")
            
            if "сервис" in discovered_words or "срочный" in discovered_words:
                print(f"\n🎯 ЦЕЛЬ ДОСТИГНУТА! Нашли 'сервис' или 'срочный'!")
            
        else:
            print(f"\n❌ БИГРАММЫ НЕ РАБОТАЮТ!")
            print(f"Google НЕ расширяет двухбуквенные префиксы")
            print(f"Метод от Gemini не применим")
        
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
        "method": "Gemini Bigram",
        "keywords": keywords,
        "count": len(keywords),
        "time": round(time.time() - start, 2)
    }


@app.get("/")
async def root():
    return {
        "api": "Gemini Bigram Test",
        "url": "/api/test-parser/gemini-bigram?seed=ремонт пылесосов&country=UA&language=ru"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
