"""
PREPOSITIONAL BRIDGE TEST - метод от Gemini
КРИТИЧЕСКИЙ ТЕСТ: Работает ли триграммное расширение?

Проверяем: "с ремонт" → "срочный ремонт", "сервисный ремонт"?
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import os
import httpx
import asyncio
import time
import random

app = FastAPI(title="Gemini Trigram Test", version="1.0")

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
    
    async def gemini_trigram_test(self, seed: str, country: str, language: str) -> List[str]:
        all_keywords = set()
        
        print(f"\n{'='*60}")
        print(f"🔬 PREPOSITIONAL BRIDGE - метод от Gemini")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"КРИТИЧЕСКИЙ ТЕСТ: Работает ли триграммное расширение?\n")
        
        # Берём только ПЕРВОЕ слово из seed
        first_word = seed.split()[0]
        print(f"Первое слово: '{first_word}'")
        print(f"Тестируем: '[буква] {first_word}'\n")
        
        # ========================================
        # КРИТИЧЕСКИЙ ТЕСТ: Триграммы
        # ========================================
        print(f"{'='*60}")
        print(f"ТЕСТ: Триграммное расширение")
        print(f"{'='*60}\n")
        
        # Тестовые буквы для поиска целевых PREFIX
        test_letters = {
            "с": "ожидаем: 'срочный', 'сервисный', 'сервис'",
            "г": "ожидаем: 'где', 'гарантийный'",
            "а": "ожидаем: 'авито'",
            "м": "ожидаем: 'мастер', 'мастерская'",
            "н": "ожидаем: 'недорогой'",
            "ц": "ожидаем: 'центр'",
            "ч": "ожидаем: 'частный'",
            "к": "ожидаем: 'качественный', 'как'",
        }
        
        discovered_prefixes = set()
        total_queries = 0
        
        for letter, expectation in test_letters.items():
            # Триграммный запрос: буква + первое_слово
            trigram_query = f"{letter} {first_word}"
            results = await self.fetch_suggestions(trigram_query, country, language)
            total_queries += 1
            
            print(f"'{trigram_query}' ({expectation})")
            print(f"  Результатов: {len(results)}")
            
            if len(results) == 0:
                print(f"  ❌ Нет результатов\n")
                continue
            
            # Анализируем результаты
            found_prefix_words = []
            
            for result in results:
                # Проверяем: начинается ли с нашей буквы?
                if result.lower().startswith(letter.lower()):
                    # Убираем букву и смотрим что осталось
                    after_letter = result[1:].strip()
                    
                    # Проверяем: есть ли наше первое слово?
                    if first_word.lower() in after_letter.lower():
                        # Извлекаем что ПЕРЕД первым словом
                        word_position = after_letter.lower().find(first_word.lower())
                        if word_position > 0:
                            # Есть слово между буквой и first_word!
                            prefix_word = after_letter[:word_position].strip()
                            if prefix_word:
                                found_prefix_words.append(prefix_word)
                                discovered_prefixes.add(prefix_word)
            
            # Показываем что нашли
            if len(found_prefix_words) > 0:
                print(f"  ✅ НАЙДЕНЫ PREFIX слова:")
                for word in set(found_prefix_words):
                    print(f"     🎯 '{word}'")
                    # Показываем пример
                    for r in results:
                        if word in r:
                            print(f"        Пример: {r}")
                            break
            else:
                print(f"  ❌ PREFIX слова НЕ найдены")
                print(f"  Примеры результатов:")
                for r in results[:3]:
                    print(f"     • {r}")
            
            print()
            await asyncio.sleep(random.uniform(0.5, 1.5))
        
        print(f"{'='*60}")
        print(f"✅ ТЕСТ ЗАВЕРШЁН")
        print(f"{'='*60}")
        print(f"Запросов: {total_queries}")
        print(f"Найдено PREFIX слов: {len(discovered_prefixes)}")
        
        if len(discovered_prefixes) > 0:
            print(f"\n🎉 МЕТОД GEMINI РАБОТАЕТ!")
            print(f"Триграммное расширение находит PREFIX слова:\n")
            for word in sorted(discovered_prefixes):
                print(f"  • {word}")
            
            # ========================================
            # ЭТАП 2: Верификация полного seed
            # ========================================
            print(f"\n{'='*60}")
            print(f"ЭТАП 2: Верификация с полным seed")
            print(f"{'='*60}\n")
            
            verified_keywords = set()
            
            for prefix_word in sorted(discovered_prefixes):
                # Проверяем полный запрос: prefix_word + полный seed
                full_query = f"{prefix_word} {seed}"
                results = await self.fetch_suggestions(full_query, country, language)
                total_queries += 1
                
                if len(results) > 0:
                    verified_keywords.update(results)
                    all_keywords.update(results)
                    print(f"✅ '{full_query}' → {len(results)} ключей")
                    for r in results[:3]:
                        print(f"    • {r}")
                else:
                    print(f"❌ '{full_query}' → нет результатов")
                
                await asyncio.sleep(random.uniform(0.5, 1.5))
            
            print(f"\n{'='*60}")
            print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
            print(f"{'='*60}")
            print(f"Всего запросов: {total_queries}")
            print(f"PREFIX слов найдено: {len(discovered_prefixes)}")
            print(f"Верифицированных ключей: {len(verified_keywords)}")
            
            if "сервис" in discovered_prefixes or "срочный" in discovered_prefixes:
                print(f"\n🎯 ЦЕЛЬ ДОСТИГНУТА!")
                print(f"Нашли целевые PREFIX: 'сервис' / 'срочный'")
            
        else:
            print(f"\n❌ МЕТОД GEMINI НЕ РАБОТАЕТ!")
            print(f"Триграммное расширение НЕ находит PREFIX слова")
            print(f"Google возвращает только морфологию или другие варианты")
        
        print(f"\n{'='*60}")
        print(f"ИТОГО ключей: {len(all_keywords)}")
        print(f"{'='*60}\n")
        
        return list(all_keywords)


@app.get("/api/test-parser/gemini")
async def test_gemini(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru")
):
    parser = AutocompleteParser()
    start = time.time()
    keywords = await parser.gemini_trigram_test(seed, country, language)
    return {
        "seed": seed,
        "method": "PREPOSITIONAL BRIDGE (Gemini)",
        "keywords": keywords,
        "count": len(keywords),
        "time": round(time.time() - start, 2)
    }


@app.get("/")
async def root():
    return {
        "api": "Gemini Trigram Test",
        "url": "/api/test-parser/gemini?seed=ремонт пылесосов&country=UA&language=ru"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
