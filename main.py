"""
MORPHOLOGICAL ADAPTIVE TEST - Консенсус AI
Морфологическое расширение ADAPTIVE метода
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

app = FastAPI(title="Morphological ADAPTIVE Test", version="1.0")

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
    
    async def morphological_adaptive_test(self, seed: str, country: str, language: str) -> List[str]:
        all_keywords = set()
        seed_words = set(seed.lower().split())
        
        print(f"\n{'='*60}")
        print(f"🔬 MORPHOLOGICAL ADAPTIVE - Консенсус AI")
        print(f"{'='*60}")
        print(f"Seed: '{seed}'")
        print(f"Метод: ADAPTIVE + морфологические формы\n")
        
        # ========================================
        # ЭТАП 1: Генерация морфологических форм
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 1: Генерация морфологических форм")
        print(f"{'='*60}\n")
        
        # Для "ремонт пылесосов" создаём формы вручную
        # В реальной реализации можно использовать pymorphy3
        forms = [
            seed,                           # "ремонт пылесосов" (именительный)
            "ремонта пылесосов",           # родительный падеж
            "по ремонту пылесосов"         # предлог + дательный
        ]
        
        print(f"Сгенерировано форм: {len(forms)}")
        for i, form in enumerate(forms, 1):
            print(f"  {i}. '{form}'")
        print()
        
        # ========================================
        # ЭТАП 2: SUFFIX парсинг для каждой формы
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 2: SUFFIX парсинг каждой формы")
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
                await asyncio.sleep(random.uniform(0.3, 0.6))
            
            print(f"Результатов для формы {form_idx}: {len(form_results)}")
            if form_results:
                print(f"Примеры:")
                for r in form_results[:3]:
                    print(f"  • {r}")
            print()
        
        print(f"Всего SUFFIX запросов: {suffix_count}")
        print(f"Всего SUFFIX результатов: {len(all_suffix_results)}\n")
        
        # ========================================
        # ЭТАП 3: Извлечение слов-кандидатов
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 3: Извлечение слов-кандидатов")
        print(f"{'='*60}\n")
        
        word_counter = Counter()
        
        for result in all_suffix_results:
            words = result.lower().split()
            for word in words:
                # Только если слово не из seed
                if word not in seed_words and len(word) > 2:
                    word_counter[word] += 1
        
        # Частотная фильтрация (слова встречающиеся 2+ раза)
        all_candidates = {w for w, count in word_counter.items() if count >= 2}
        
        print(f"Всего уникальных слов: {len(word_counter)}")
        print(f"Слов после фильтрации (≥2 раз): {len(all_candidates)}")
        print(f"\nТоп-20 частотных слов:")
        for word, count in word_counter.most_common(20):
            print(f"  • '{word}' ({count} раз)")
        print()
        
        # ========================================
        # ЭТАП 4: Сравнение с базовым ADAPTIVE
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 4: Анализ НОВЫХ слов от морфологии")
        print(f"{'='*60}\n")
        
        # Отдельно считаем слова от базовой формы
        base_form_words = set()
        for result in all_suffix_results[:len(all_suffix_results)//3]:  # Первая треть = базовая форма
            words = result.lower().split()
            for word in words:
                if word not in seed_words and len(word) > 2:
                    base_form_words.add(word)
        
        # Новые слова = все кандидаты минус базовые
        new_from_morphology = all_candidates - base_form_words
        
        print(f"Слов от базовой формы: {len(base_form_words)}")
        print(f"Слов от морфологических форм: {len(all_candidates)}")
        print(f"НОВЫХ слов благодаря морфологии: {len(new_from_morphology)}")
        
        if new_from_morphology:
            print(f"\nНовые слова:")
            for word in sorted(list(new_from_morphology)[:20]):
                print(f"  • {word}")
        print()
        
        # ========================================
        # ЭТАП 5: PREFIX проверка
        # ========================================
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
                if verified_count <= 10:  # Показываем первые 10
                    print(f"✅ '{query}' → {len(results)} ключей")
            
            await asyncio.sleep(random.uniform(0.3, 0.6))
        
        print(f"\nПроверено кандидатов: {prefix_count}")
        print(f"Валидных PREFIX: {verified_count}")
        print()
        
        # ========================================
        # ФИНАЛЬНАЯ СТАТИСТИКА
        # ========================================
        total_queries = suffix_count + prefix_count
        
        print(f"{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА")
        print(f"{'='*60}")
        print(f"SUFFIX парсинг: {suffix_count} запросов")
        print(f"  - Базовая форма: 29 запросов")
        print(f"  - Морфо формы: {suffix_count - 29} запросов")
        print(f"PREFIX проверка: {prefix_count} запросов")
        print(f"──────────────────────────────────")
        print(f"ВСЕГО запросов: {total_queries}")
        print(f"")
        print(f"Найдено кандидатов: {len(all_candidates)}")
        print(f"Валидных PREFIX: {verified_count}")
        print(f"Финальных ключей: {len(all_keywords)}")
        print(f"")
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
            print(f"Результат аналогичен базовому ADAPTIVE")
        
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
        "method": "Morphological ADAPTIVE",
        "keywords": keywords,
        "count": len(keywords),
        "time": round(time.time() - start, 2)
    }


@app.get("/")
async def root():
    return {
        "api": "Morphological ADAPTIVE Test",
        "url": "/api/test-parser/morphology?seed=ремонт пылесосов&country=UA&language=ru"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
