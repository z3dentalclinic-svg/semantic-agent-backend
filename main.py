"""
CSG (Context-Shift Graph) TEST
Тестирование метода от ChatGPT для поиска PREFIX запросов

ЦЕЛЬ: Найти "сервисный центр ремонт пылесосов"

МЕТОД CSG:
1. Используем якорь (город): "киев"
2. Делаем Context Shift: "киев ремонт пылесосов"
3. Вторичное расширение: "киев ремонт пылесосов с/се/сер/серв..."
4. Ожидаем: "киев сервисный центр ремонт пылесосов"
5. Извлекаем: "сервисный центр"
6. Проверяем: "сервисный центр ремонт пылесосов"
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Set
import os
import yaml
import httpx
import asyncio
import time
import random

# Морфологические анализаторы
try:
    import pymorphy3
    PYMORPHY_AVAILABLE = True
except ImportError:
    PYMORPHY_AVAILABLE = False
    print("⚠️ pymorphy3 не установлен! Морфологический парсинг недоступен.")

try:
    import inflect
    INFLECT_AVAILABLE = True
except ImportError:
    INFLECT_AVAILABLE = False
    print("⚠️ inflect не установлен! Морфология для английского недоступна.")

app = FastAPI(title="Semantic Agent API", version="2.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create google-ads.yaml from environment variables
def create_google_ads_config():
    """Create google-ads.yaml from environment variables"""
    config = {
        'developer_token': os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
        'client_id': os.getenv('GOOGLE_ADS_CLIENT_ID'),
        'client_secret': os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
        'refresh_token': os.getenv('GOOGLE_ADS_REFRESH_TOKEN', ''),
        'login_customer_id': os.getenv('GOOGLE_ADS_CUSTOMER_ID'),
        'use_proto_plus': True
    }
    
    # Write to file
    with open('google-ads.yaml', 'w') as f:
        yaml.dump(config, f)
    
    return config

# ============================================
# GOOGLE AUTOCOMPLETE PARSER
# ============================================

class AutocompleteParser:
    """Парсер Google Autocomplete"""
    
    def __init__(self):
        self.base_url = "http://suggestqueries.google.com/complete/search"
        
        # Базовые модификаторы (для всех языков)
        self.base_modifiers = list("abcdefghijklmnopqrstuvwxyz0123456789")
        
        # Языковые модификаторы (специфичные символы)
        self.language_modifiers = {
            'en': [],  # Английский - только базовые
            'ru': list("абвгдежзийклмнопрстуфхцчшщэюя"),  # Русский (29 букв без ё,ъ,ы,ь)
            'uk': list("абвгдежзийклмнопрстуфхцчшщьюяіїєґ"),  # Украинский
            'de': list("äöüß"),  # Немецкий
            'fr': list("àâäæçéèêëïîôùûüÿ"),  # Французский
            'es': list("áéíñóúü"),  # Испанский
            'pl': list("ąćęłńóśźż"),  # Польский
            'it': list("àèéìíîòóùú"),  # Итальянский
        }
        
        # Список разных User-Agent для ротации
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0",
        ]
        
        # Морфологические анализаторы
        self.morph_ru = None
        self.morph_en = None
        
        if PYMORPHY_AVAILABLE:
            try:
                self.morph_ru = pymorphy3.MorphAnalyzer()
                print("✅ pymorphy3 (русский) инициализирован")
            except Exception as e:
                print(f"⚠️ Ошибка инициализации pymorphy3: {e}")
        
        if INFLECT_AVAILABLE:
            try:
                self.morph_en = inflect.engine()
                print("✅ inflect (английский) инициализирован")
            except Exception as e:
                print(f"⚠️ Ошибка инициализации inflect: {e}")
    
    def get_modifiers(self, language: str) -> List[str]:
        """
        Получить модификаторы для конкретного языка
        
        Args:
            language: Код языка (en, ru, uk, de, fr, es, pl, it)
            
        Returns:
            List[str]: Базовые (a-z + 0-9) + языковые модификаторы
        """
        modifiers = self.base_modifiers.copy()
        
        # Добавляем языковые модификаторы если есть
        lang_mods = self.language_modifiers.get(language.lower(), [])
        modifiers.extend(lang_mods)
        
        return modifiers
        
    def get_word_forms_ru(self, word: str) -> Set[str]:
        """
        Получить все морфологические формы русского слова
        
        Args:
            word: Русское слово
            
        Returns:
            Set[str]: Все формы слова (пылесос, пылесоса, пылесосу, ...)
        """
        if not self.morph_ru:
            print(f"⚠️ morph_ru не инициализирован!")
            return {word}
        
        forms = set()
        try:
            parsed = self.morph_ru.parse(word)
            print(f"🔍 Разбор слова '{word}': найдено {len(parsed)} вариантов")
            
            if parsed:
                # Берем первый вариант разбора
                p = parsed[0]
                print(f"🔍 Первый разбор: {p.tag}")
                
                # Получаем все формы из лексемы
                for form in p.lexeme:
                    forms.add(form.word)
                
                print(f"✅ Получено {len(forms)} форм: {list(forms)[:5]}...")
        except Exception as e:
            print(f"⚠️ Ошибка получения форм для '{word}': {e}")
            forms.add(word)
        
        if len(forms) == 0:
            print(f"⚠️ Не получено ни одной формы для '{word}', возвращаем исходное")
            forms.add(word)
        
        return forms
    
    def get_word_forms_en(self, word: str) -> Set[str]:
        """
        Получить формы английского слова (singular/plural)
        
        Args:
            word: Английское слово
            
        Returns:
            Set[str]: Единственное и множественное число
        """
        if not self.morph_en:
            return {word}
        
        forms = {word}
        try:
            # Множественное число
            plural = self.morph_en.plural(word)
            if plural:
                forms.add(plural)
            
            # Единственное число (если дали plural)
            singular = self.morph_en.singular_noun(word)
            if singular:
                forms.add(singular)
        except Exception as e:
            print(f"⚠️ Ошибка получения форм для '{word}': {e}")
        
        return forms
    
    def get_seed_variations(self, seed: str, language: str) -> List[str]:
        """
        Получить вариации seed фразы с разными морфологическими формами
        
        Args:
            seed: Исходная фраза (например "ремонт пылесосов")
            language: Язык (ru, en)
            
        Returns:
            List[str]: Все вариации фразы
        """
        print(f"🔍 get_seed_variations вызван: seed='{seed}', language='{language}'")
        
        words = seed.split()
        print(f"🔍 Слов в seed: {len(words)}")
        
        if len(words) < 2:
            print(f"⚠️ Меньше 2 слов, возвращаем исходный seed")
            return [seed]
        
        # Получаем формы для последнего слова (обычно существительное)
        last_word = words[-1]
        print(f"🔍 Последнее слово: '{last_word}'")
        
        if language.lower() == 'ru':
            print(f"🔍 Вызываем get_word_forms_ru('{last_word}')")
            word_forms = self.get_word_forms_ru(last_word)
            print(f"🔍 Получено форм от get_word_forms_ru: {len(word_forms)}")
        elif language.lower() == 'en':
            print(f"🔍 Вызываем get_word_forms_en('{last_word}')")
            word_forms = self.get_word_forms_en(last_word)
            print(f"🔍 Получено форм от get_word_forms_en: {len(word_forms)}")
        else:
            print(f"⚠️ Неизвестный язык '{language}', возвращаем исходный seed")
            return [seed]
        
        # Создаем вариации
        variations = []
        base = ' '.join(words[:-1])  # все слова кроме последнего
        
        for form in word_forms:
            variations.append(f"{base} {form}")
        
        print(f"✅ Создано вариаций: {len(variations)}")
        print(f"   Первые 3: {variations[:3]}")
        
        return variations
        
    async def fetch_suggestions(
        self, 
        query: str, 
        country: str = "US", 
        language: str = "en"
    ) -> List[str]:
        """Получить подсказки для одного запроса"""
        params = {
            "client": "firefox",
            "q": query,
            "gl": country.upper(),
            "hl": language.lower()
        }
        
        # Случайный User-Agent для каждого запроса
        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "application/json",
            "Accept-Language": f"{language.lower()},{language.lower()}-{country.upper()};q=0.9,en;q=0.8",
        }
        
        try:
            async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
                response = await client.get(self.base_url, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if len(data) >= 2 and isinstance(data[1], list):
                    return data[1]
                
                return []
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    async def parse_with_modifiers(
        self,
        seed: str,
        country: str = "US",
        language: str = "en",
        use_numbers: bool = False,
        use_morphology: bool = False
    ) -> List[str]:
        """
        Парсинг с модификаторами (SUFFIX + INFIX + MORPH)
        
        МЕТОД 1: SUFFIX латиница/цифры - "seed модификатор" (БЕЗ морфологии)
        МЕТОД 2: SUFFIX кириллица - "seed_form модификатор" (С морфологией если включена)
        МЕТОД 3: INFIX кириллица - "слово1 модификатор слово2" (БЕЗ морфологии)
        """
        all_keywords = set()
        
        # Получаем модификаторы для выбранного языка
        all_modifiers = self.get_modifiers(language)
        
        # Если use_numbers=False, убираем цифры из базовых
        if not use_numbers:
            all_modifiers = [m for m in all_modifiers if not m.isdigit()]
        
        # Разделяем модификаторы на латиницу/цифры и кириллицу
        language_specific = self.language_modifiers.get(language.lower(), [])
        cyrillic_modifiers = [m for m in all_modifiers if m in language_specific]
        latin_digit_modifiers = [m for m in all_modifiers if m not in language_specific]
        
        # МОРФОЛОГИЯ ЗАКОММЕНТИРОВАНА ДЛЯ ТЕСТИРОВАНИЯ PREFIX
        # Seed вариации ТОЛЬКО если включена морфология
        # seed_variations = [seed]
        # if use_morphology:
        #     seed_variations = self.get_seed_variations(seed, language)
        #     print(f"🔤 MORPH mode: ENABLED | Seed variations: {len(seed_variations)}")
        #     for var in seed_variations[:5]:
        #         print(f"   - {var}")
        #     if len(seed_variations) > 5:
        #         print(f"   ... и ещё {len(seed_variations) - 5}")
        
        # ВРЕМЕННО: используем только исходный seed
        seed_variations = [seed]
        if use_morphology:
            print(f"⚠️ MORPH mode: ВРЕМЕННО ОТКЛЮЧЕНА ДЛЯ ТЕСТИРОВАНИЯ PREFIX")
        
        seed_words = seed.split()
        
        print(f"🌍 Language: {language.upper()}")
        print(f"📊 Modifiers: Latin/Digits={len(latin_digit_modifiers)}, Cyrillic={len(cyrillic_modifiers)}")
        print(f"📍 INFIX mode: {'ENABLED' if len(cyrillic_modifiers) > 0 and len(seed_words) >= 2 else 'DISABLED'}")
        print(f"📍 PREFIX mode: {'ENABLED' if len(cyrillic_modifiers) > 0 else 'DISABLED'}")
        
        # ========================================
        # CSG (CONTEXT-SHIFT GRAPH) TEST - ОТ CHATGPT
        # ========================================
        print(f"\n{'='*60}")
        print(f"🔬 [ТЕСТ] CSG - CONTEXT-SHIFT GRAPH (от ChatGPT)")
        print(f"{'='*60}")
        print(f"Исходный seed: '{seed}'")
        print(f"")
        print(f"ЦЕЛЬ: Найти 'сервисный центр ремонт пылесосов'")
        print(f"")
        print(f"МЕТОД CSG:")
        print(f"1. Якорь: 'киев' (город)")
        print(f"2. Context Shift: 'киев ремонт пылесосов'")
        print(f"3. Вторичное расширение: 'киев ремонт пылесосов с/се/сер...'")
        print(f"4. Ожидаем вставку: 'киев [сервисный центр] ремонт пылесосов'")
        print(f"5. Извлекаем PREFIX: 'сервисный центр'")
        print(f"")
        
        # ========================================
        # ЭТАП 0: Тестовые якоря
        # ========================================
        test_anchors = ["киев", "москва", "астана"]
        
        print(f"Тестовые якоря: {', '.join(test_anchors)}\n")
        
        # ========================================
        # ЭТАП 1: Context Shift с якорями
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 1: Context Shift (якорь + seed)")
        print(f"{'='*60}\n")
        
        context_shift_results = {}
        
        for anchor in test_anchors:
            context_query = f"{anchor} {seed}"
            context_suggestions = await self.fetch_suggestions(context_query, country, language)
            context_shift_results[anchor] = context_suggestions
            
            print(f"'{context_query}' → {len(context_suggestions)} результатов")
            if len(context_suggestions) > 0:
                for s in context_suggestions[:3]:
                    print(f"  • {s}")
            
            delay = random.uniform(0.5, 1.5)
            await asyncio.sleep(delay)
        
        print(f"\n✅ Context Shift завершён\n")
        
        # ========================================
        # ЭТАП 2: Вторичное расширение (КРИТИЧЕСКИЙ ТЕСТ!)
        # ========================================
        print(f"{'='*60}")
        print(f"ЭТАП 2: Вторичное расширение (между якорем и seed)")
        print(f"{'='*60}")
        print(f"Проверяем: может ли Google вставить слова МЕЖДУ якорем и seed?\n")
        
        # Целевые буквы для поиска "сервис"
        target_letters = {
            "с": "ищем 'сервис', 'сервисный', 'срочный'",
            "се": "ищем 'сервис', 'сервисный'", 
            "сер": "ищем 'сервис', 'сервисный'",
            "серв": "ищем 'сервис', 'сервисный'",
            "г": "ищем 'где', 'гарантийный'",
            "н": "ищем 'недорогой'",
            "ц": "ищем 'центр'",
            "м": "ищем 'мастер', 'мастерская'",
        }
        
        csg_discovered_words = set()
        csg_total_queries = 0
        
        for anchor in test_anchors:
            print(f"\n--- Якорь: '{anchor}' ---")
            
            for letter, description in target_letters.items():
                # CSG вторичное расширение
                csg_query = f"{anchor} {seed} {letter}"
                csg_suggestions = await self.fetch_suggestions(csg_query, country, language)
                csg_total_queries += 1
                
                # Анализируем результаты: есть ли вставка МЕЖДУ якорем и seed?
                inserted_words = []
                
                for suggestion in csg_suggestions:
                    # Удаляем якорь из начала
                    if suggestion.lower().startswith(anchor.lower()):
                        after_anchor = suggestion[len(anchor):].strip()
                        
                        # Проверяем есть ли seed в остатке
                        if seed.lower() in after_anchor.lower():
                            # Извлекаем что ПЕРЕД seed
                            seed_position = after_anchor.lower().find(seed.lower())
                            if seed_position > 0:
                                # Есть слова между якорем и seed!
                                before_seed = after_anchor[:seed_position].strip()
                                if before_seed:
                                    inserted_words.append(before_seed)
                                    csg_discovered_words.add(before_seed)
                
                # Показываем результаты
                status = "✅ ВСТАВКА!" if len(inserted_words) > 0 else "❌ нет вставки"
                print(f"  '{csg_query}' ({description})")
                print(f"    Результатов: {len(csg_suggestions)} | {status}")
                
                if len(inserted_words) > 0:
                    print(f"    НАЙДЕНЫ ВСТАВКИ:")
                    for word in set(inserted_words):
                        print(f"      🎯 '{word}'")
                        # Показываем полный пример
                        for s in csg_suggestions:
                            if word in s:
                                print(f"         Пример: {s}")
                                break
                
                delay = random.uniform(0.5, 1.5)
                await asyncio.sleep(delay)
        
        print(f"\n{'='*60}")
        print(f"✅ ЭТАП 2 завершён")
        print(f"{'='*60}")
        print(f"Запросов сделано: {csg_total_queries}")
        print(f"Обнаружено уникальных PREFIX слов: {len(csg_discovered_words)}")
        
        if len(csg_discovered_words) > 0:
            print(f"\n🎉 CSG РАБОТАЕТ! Найдены PREFIX слова:")
            for word in sorted(csg_discovered_words):
                print(f"  • {word}")
            
            # ========================================
            # ЭТАП 3: Проверка извлечённых PREFIX
            # ========================================
            print(f"\n{'='*60}")
            print(f"ЭТАП 3: Проверка извлечённых PREFIX")
            print(f"{'='*60}\n")
            
            csg_prefix_keywords = set()
            
            for word in sorted(csg_discovered_words):
                prefix_query = f"{word} {seed}"
                prefix_suggestions = await self.fetch_suggestions(prefix_query, country, language)
                csg_total_queries += 1
                
                if len(prefix_suggestions) > 0:
                    csg_prefix_keywords.update(prefix_suggestions)
                    all_keywords.update(prefix_suggestions)
                    
                    print(f"✅ '{prefix_query}' → {len(prefix_suggestions)} PREFIX найдено!")
                    for s in prefix_suggestions[:3]:
                        print(f"    • {s}")
                else:
                    print(f"❌ '{prefix_query}' → нет результатов")
                
                delay = random.uniform(0.5, 1.5)
                await asyncio.sleep(delay)
            
            print(f"\n{'='*60}")
            print(f"📊 ИТОГОВАЯ СТАТИСТИКА CSG")
            print(f"{'='*60}")
            print(f"Всего запросов: {csg_total_queries}")
            print(f"Обнаружено PREFIX слов: {len(csg_discovered_words)}")
            print(f"Найдено PREFIX ключей: {len(csg_prefix_keywords)}")
            print(f"")
            
            if "сервисный центр" in csg_discovered_words or "сервис" in csg_discovered_words:
                print(f"🎯 ЦЕЛЬ ДОСТИГНУТА! Нашли 'сервис' или 'сервисный центр'!")
            else:
                print(f"❌ Цель НЕ достигнута: 'сервисный центр' не найден")
            
        else:
            print(f"\n❌ CSG НЕ РАБОТАЕТ!")
            print(f"Google НЕ делает вставки между якорем и seed")
            print(f"Метод от ChatGPT не применим к Autocomplete API")
        
        print(f"\n{'='*60}")
        print(f"🎉 CSG ТЕСТ ЗАВЕРШЕН")
        print(f"{'='*60}")
        print(f"Всего уникальных ключевых слов: {len(all_keywords)}")
        
        latin_results = 0
        cyrillic_results = 0
        
            
            # Добавляем к общим результатам
            all_keywords.update(wildcard_suggestions)
            wildcard_all_keywords.update(wildcard_suggestions)
            wildcard_total_results += len(wildcard_suggestions)
            
            # Анализируем длину результатов
            short_results = [s for s in wildcard_suggestions if len(s.split()) <= 3]  # seed + 1 слово
            long_results = [s for s in wildcard_suggestions if len(s.split()) > 3]    # seed + 2+ слова
            
            delay = random.uniform(0.5, 2.0)
            
            print(f"[{i+1}/{len(wildcard_symbols)}] '{wildcard_query}'")
            print(f"    {description}")
            print(f"    Всего: {len(wildcard_suggestions)} | Коротких: {len(short_results)} | Длинных: {len(long_results)}")
            
            if len(wildcard_suggestions) > 0:
                print(f"    Примеры коротких:")
                for exp in short_results[:3]:
                    print(f"      • {exp}")
                
                if len(long_results) > 0:
                    print(f"    Примеры длинных:")
                    for exp in long_results[:3]:
                        print(f"      • {exp}")
            else:
                print(f"    ❌ Нет результатов")
            
            print(f"    Задержка: {delay:.1f}s")
            print()
            await asyncio.sleep(delay)
        
        # Анализ всех результатов
        all_short = [s for s in wildcard_all_keywords if len(s.split()) <= 3]
        all_long = [s for s in wildcard_all_keywords if len(s.split()) > 3]
        
        print(f"{'='*60}")
        print(f"✅ WILDCARD SUFFIX комбо-тест завершен!")
        print(f"{'='*60}")
        print(f"Всего результатов: {wildcard_total_results}")
        print(f"Уникальных ключей: {len(wildcard_all_keywords)}")
        print(f"  - Коротких (seed + 1 слово): {len(all_short)}")
        print(f"  - Длинных (seed + 2+ слова): {len(all_long)}")
        print(f"")
        print(f"📊 СРАВНЕНИЕ С МОДИФИКАТОРАМИ:")
        print(f"  Старый метод: 29 запросов → ~250 результатов → ~30-40 сек")
        print(f"  Новый метод:  {len(wildcard_symbols)} запросов → {len(wildcard_all_keywords)} результатов → ~{len(wildcard_symbols)*1.5:.0f} сек")
        print(f"")
        
        if len(wildcard_all_keywords) >= 200:
            efficiency = 29 / len(wildcard_symbols)
            print(f"🎉 WILDCARD СУПЕР-ЭФФЕКТИВЕН!")
            print(f"⚡ Ускорение: в {efficiency:.1f}x раз меньше запросов!")
            print(f"✅ Покрытие: {len(wildcard_all_keywords)} ключей (отлично!)")
        elif len(wildcard_all_keywords) >= 100:
            print(f"✅ WILDCARD РАБОТАЕТ ХОРОШО!")
            print(f"⚡ Меньше запросов, хорошее покрытие")
            print(f"⚠️ Но немного меньше результатов чем модификаторы")
        else:
            print(f"⚠️ WILDCARD ДАЁТ МАЛО РЕЗУЛЬТАТОВ")
            print(f"❌ Лучше использовать модификаторы а/б/в...")
        
        print(f"")
        print(f"Примеры ДЛИННЫХ запросов:")
        for kw in sorted(all_long, key=lambda x: len(x), reverse=True)[:10]:
            word_count = len(kw.split())
            print(f"  [{word_count} слов] {kw}")
        
        latin_results = 0
        cyrillic_results = 0
        
        # ========================================
        # ADAPTIVE PREFIX - ОТКЛЮЧЕН ДЛЯ CSG ТЕСТА
        # ========================================
        print(f"\n⚠️ ADAPTIVE PREFIX ОТКЛЮЧЕН ДЛЯ CSG ТЕСТА")
        
        # ========================================
        # INFIX - ОТКЛЮЧЕН ДЛЯ CSG ТЕСТА
        # ========================================
        print(f"\n⚠️ INFIX ОТКЛЮЧЕН ДЛЯ CSG ТЕСТА")
        infix_results = 0
        
        # ========================================
        # PREFIX - ОТКЛЮЧЕН ДЛЯ CSG ТЕСТА
        # ========================================
        print(f"\n⚠️ PREFIX ОТКЛЮЧЕН ДЛЯ CSG ТЕСТА")
        prefix_results = 0
        
            
            delay = random.uniform(0.5, 2.0)
            if i < 3 or len(suggestions) > 0:
                print(f"[{i+1}/{len(cyrillic_modifiers)}] '{query}' → {len(suggestions)} results (wait {delay:.1f}s)")
            await asyncio.sleep(delay)
        
        print(f"\n✅ ЭТАП 1 завершен!")
        print(f"Найдено SUFFIX результатов: {stage1_count}")
        print(f"Извлечено потенциальных PREFIX слов: {len(potential_prefix_words)}")
        print(f"\nПримеры извлечённых слов:")
        for word in sorted(potential_prefix_words)[:15]:
            print(f"  • {word}")
        if len(potential_prefix_words) > 15:
            print(f"  ... и ещё {len(potential_prefix_words) - 15}")
        
        # ========================================
        # ЭТАП 2: PREFIX проверка извлечённых слов
        # ========================================
        print(f"\n{'='*60}")
        print(f"ЭТАП 2: PREFIX проверка (обратные запросы)")
        print(f"{'='*60}")
        print(f"Проверяем: '[слово] {seed}'")
        print(f"Слов для проверки: {len(potential_prefix_words)}\n")
        
        stage2_keywords = set()
        stage2_count = 0
        successful_prefix = []
        
        for i, word in enumerate(sorted(potential_prefix_words)):
            # Делаем PREFIX запрос
            prefix_query = f"{word} {seed}"
            prefix_suggestions = await self.fetch_suggestions(prefix_query, country, language)
            
            # Проверяем есть ли РЕАЛЬНОЕ расширение (не просто word + seed)
            real_prefix = []
            for suggestion in prefix_suggestions:
                # Если результат начинается с нашего слова и содержит seed - это PREFIX!
                if suggestion.lower().startswith(word) and seed.lower() in suggestion.lower():
                    real_prefix.append(suggestion)
            
            if len(real_prefix) > 0:
                stage2_keywords.update(real_prefix)
                stage2_count += len(real_prefix)
                successful_prefix.append(word)
                all_keywords.update(real_prefix)
                
                print(f"[{i+1}/{len(potential_prefix_words)}] '{prefix_query}' → ✅ {len(real_prefix)} PREFIX найдено!")
                for exp in real_prefix[:3]:
                    print(f"    • {exp}")
            elif i < 5:  # Показываем первые 5 даже без результатов
                print(f"[{i+1}/{len(potential_prefix_words)}] '{prefix_query}' → ❌ нет PREFIX")
            
            delay = random.uniform(0.5, 2.0)
            await asyncio.sleep(delay)
        
        print(f"\n✅ ЭТАП 2 завершен!")
        print(f"Проверено слов: {len(potential_prefix_words)}")
        print(f"Успешных PREFIX слов: {len(successful_prefix)}")
        print(f"Найдено PREFIX запросов: {stage2_count}")
        
        # ========================================
        # ИТОГОВАЯ СТАТИСТИКА
        # ========================================
        print(f"\n{'='*60}")
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА ADAPTIVE PREFIX")
        print(f"{'='*60}")
        print(f"")
        print(f"ЭТАП 1 (SUFFIX парсинг):")
        print(f"  Запросов: {len(cyrillic_modifiers)}")
        print(f"  Результатов: {stage1_count}")
        print(f"  Извлечено слов: {len(potential_prefix_words)}")
        print(f"")
        print(f"ЭТАП 2 (PREFIX проверка):")
        print(f"  Запросов: {len(potential_prefix_words)}")
        print(f"  Успешных: {len(successful_prefix)}")
        print(f"  PREFIX запросов: {stage2_count}")
        print(f"")
        print(f"ВСЕГО:")
        print(f"  Запросов: {len(cyrillic_modifiers) + len(potential_prefix_words)}")
        print(f"  Уникальных ключей: {len(all_keywords)}")
        print(f"")
        
        if len(successful_prefix) > 0:
            print(f"🎉 ADAPTIVE PREFIX РАБОТАЕТ!")
            print(f"\nУспешные PREFIX слова:")
            for word in successful_prefix[:20]:
                print(f"  • {word}")
            if len(successful_prefix) > 20:
                print(f"  ... и ещё {len(successful_prefix) - 20}")
            
            print(f"\nПримеры PREFIX запросов:")
            for kw in sorted(stage2_keywords)[:15]:
                print(f"  • {kw}")
            if len(stage2_keywords) > 15:
                print(f"  ... и ещё {len(stage2_keywords) - 15}")
        else:
            print(f"❌ ADAPTIVE PREFIX не нашёл результатов")
        
        print(f"\n⚠️ SUFFIX Cyrillic ОТКЛЮЧЕН ДЛЯ ТЕСТА ADAPTIVE")

        cyrillic_results = 0
        
        # ========================================
        # 3. INFIX с КИРИЛЛИЦЕЙ - ЗАКОММЕНТИРОВАНО ДЛЯ ТЕСТА
        # ========================================
        # if len(cyrillic_modifiers) > 0 and len(seed_words) >= 2:
        #     print(f"\n{'='*60}")
        #     print(f"🔤 [3/4] INFIX Cyrillic (исходный seed только)")
        #     print(f"{'='*60}")
        #     print(f"Исходный seed: '{seed}'")
        #     print(f"Слов в seed: {len(seed_words)}")
        #     print(f"Шаблон: '{seed_words[0]} [модификатор] {' '.join(seed_words[1:])}'")
        #     print(f"Пример запроса: '{seed_words[0]} а {' '.join(seed_words[1:])}'")
        #     print(f"Модификаторов: {len(cyrillic_modifiers)}")
        #     
        #     infix_results = 0
        #     for i, modifier in enumerate(cyrillic_modifiers):
        #         infix_query = f"{seed_words[0]} {modifier} {' '.join(seed_words[1:])}"
        #         infix_suggestions = await self.fetch_suggestions(infix_query, country, language)
        #         all_keywords.update(infix_suggestions)
        #         infix_results += len(infix_suggestions)
        #         
        #         delay = random.uniform(0.5, 2.0)
        #         if i < 3 or len(infix_suggestions) > 0:
        #             print(f"[{i+1}/{len(cyrillic_modifiers)}] '{infix_query}' → {len(infix_suggestions)} results (wait {delay:.1f}s)")
        #         await asyncio.sleep(delay)
        #     
        #     print(f"✅ INFIX завершен: {infix_results} результатов")
        # else:
        #     print(f"\n⚠️ INFIX DISABLED (требуется: кириллические модификаторы + seed из 2+ слов)")
        
        print(f"\n⚠️ INFIX ОТКЛЮЧЕН ДЛЯ ТЕСТА WILDCARD")
        infix_results = 0
        
        # ========================================
        # 4. PREFIX - ЗАКОММЕНТИРОВАНО ДЛЯ ТЕСТА
        # ========================================
        print(f"\n⚠️ PREFIX (односимвольный) ОТКЛЮЧЕН ДЛЯ CSG ТЕСТА")
        prefix_results = 0
        
        # ========================================
        # WILDCARD PREFIX - ОТКЛЮЧЕН ДЛЯ CSG ТЕСТА
        # ========================================
        print(f"\n⚠️ WILDCARD PREFIX ОТКЛЮЧЕН ДЛЯ CSG ТЕСТА")
        wildcard_total_results = 0
        
            print(f"[{i+1}/{len(wildcard_symbols)}] '{wildcard_query}' ({description})")
            print(f"    Всего: {len(wildcard_suggestions)} | Расширений: {len(real_expansions)} | {status}")
            
            if len(real_expansions) > 0:
                print(f"    Примеры расширений:")
                for exp in real_expansions[:5]:
                    print(f"      • {exp}")
            
            print(f"    Задержка: {delay:.1f}s\n")
            await asyncio.sleep(delay)
        
        print(f"{'='*60}")
        print(f"✅ WILDCARD PREFIX тест завершен!")
        print(f"{'='*60}")
        print(f"Всего расширений найдено: {wildcard_total_results}")
        print(f"Уникальных ключевых слов: {len(wildcard_total_keywords)}")
        
        if wildcard_total_results > 0:
            print(f"\n🎉 WILDCARD РАБОТАЕТ! Найдено {wildcard_total_results} PREFIX запросов!")
            print(f"\nВсе найденные PREFIX запросы:")
            for kw in sorted(wildcard_total_keywords)[:20]:
                print(f"  • {kw}")
            if len(wildcard_total_keywords) > 20:
                print(f"  ... и ещё {len(wildcard_total_keywords) - 20}")
        else:
            print(f"\n❌ WILDCARD НЕ РАБОТАЕТ в Google Autocomplete API")
        
        print(f"\n{'='*60}")
        print(f"🎉 ПАРСИНГ ЗАВЕРШЕН")
        print(f"{'='*60}")
        print(f"Всего уникальных ключевых слов: {len(all_keywords)}")
        
        return list(all_keywords)


# ============================================
# MODELS
# ============================================

class LocationRequest(BaseModel):
    country_code: str

class LocationResponse(BaseModel):
    id: str
    name: str
    type: str

class ParseRequest(BaseModel):
    seed: str
    country: str = "IE"
    language: str = "en"
    use_numbers: bool = False
    use_morphology: bool = False

class ParseResponse(BaseModel):
    seed: str
    keywords: List[str]
    count: int
    requests_made: int
    parsing_time: float


# ============================================
# ENDPOINTS
# ============================================

@app.get("/")
async def root():
    credentials_loaded = all([
        os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
        os.getenv('GOOGLE_ADS_CLIENT_ID'),
        os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
        os.getenv('GOOGLE_ADS_CUSTOMER_ID')
    ])
    
    # Проверяем доступность морфологии
    morph_status = {
        "russian": {
            "available": PYMORPHY_AVAILABLE,
            "library": "pymorphy2",
            "features": "все падежи и числа" if PYMORPHY_AVAILABLE else "не установлено"
        },
        "english": {
            "available": INFLECT_AVAILABLE,
            "library": "inflect",
            "features": "singular/plural" if INFLECT_AVAILABLE else "не установлено"
        }
    }
    
    return {
        "service": "Semantic Agent API",
        "version": "3.0.0 (SUFFIX + INFIX + MORPHOLOGY)",
        "status": "running",
        "credentials_loaded": credentials_loaded,
        "morphology": {
            "status": "enabled" if (PYMORPHY_AVAILABLE or INFLECT_AVAILABLE) else "disabled",
            "languages": morph_status
        },
        "parsing_modes": {
            "suffix": "seed + modifier (all modifiers)",
            "infix": "word1 + modifier + word2 (cyrillic only, 1-char)",
            "morphology": "all word forms (pymorphy2 for RU, inflect for EN)"
        },
        "endpoints": {
            "health": "/health",
            "locations": "/api/locations/{country_code}",
            "countries": "/api/countries",
            "test_parser_single": "/api/test-parser/single?query={query}&country={country}&language={language}",
            "test_parser_quick": "/api/test-parser/quick?query={query}&country={country}&language={language}",
            "test_parser_full": "/api/test-parser/full?seed={seed}&country={country}&language={language}&use_numbers={bool}&use_morphology={bool}"
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "credentials": "loaded" if os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN") else "missing",
        "parser": "enabled (SUFFIX + INFIX + MORPHOLOGY)",
        "morphology": {
            "russian": "enabled ✅" if PYMORPHY_AVAILABLE else "disabled ❌",
            "english": "enabled ✅" if INFLECT_AVAILABLE else "disabled ❌"
        }
    }

@app.get("/debug")
async def debug():
    """Отладочная информация о библиотеках"""
    debug_info = {
        "pymorphy3": {
            "imported": PYMORPHY_AVAILABLE,
            "error": None
        },
        "inflect": {
            "imported": INFLECT_AVAILABLE,
            "error": None
        }
    }
    
    # Пытаемся импортировать и проверить версии
    try:
        import pymorphy3
        debug_info["pymorphy3"]["version"] = pymorphy3.__version__ if hasattr(pymorphy3, '__version__') else "unknown"
        debug_info["pymorphy3"]["module_path"] = str(pymorphy3.__file__)
    except Exception as e:
        debug_info["pymorphy3"]["error"] = str(e)
    
    try:
        import inflect
        debug_info["inflect"]["version"] = inflect.__version__ if hasattr(inflect, '__version__') else "unknown"
        debug_info["inflect"]["module_path"] = str(inflect.__file__)
    except Exception as e:
        debug_info["inflect"]["error"] = str(e)
    
    # Проверяем pkg_resources
    try:
        import pkg_resources
        debug_info["pkg_resources"] = {
            "available": True,
            "version": pkg_resources.__version__ if hasattr(pkg_resources, '__version__') else "unknown"
        }
    except Exception as e:
        debug_info["pkg_resources"] = {
            "available": False,
            "error": str(e)
        }
    
    # Проверяем setuptools
    try:
        import setuptools
        debug_info["setuptools"] = {
            "available": True,
            "version": setuptools.__version__ if hasattr(setuptools, '__version__') else "unknown"
        }
    except Exception as e:
        debug_info["setuptools"] = {
            "available": False,
            "error": str(e)
        }
    
    return debug_info

@app.get("/api/countries")
async def get_countries():
    countries = [
        {"code": "IE", "name": "Ireland", "flag": "🇮🇪"},
        {"code": "UA", "name": "Україна", "flag": "🇺🇦"},
        {"code": "US", "name": "United States", "flag": "🇺🇸"},
        {"code": "GB", "name": "United Kingdom", "flag": "🇬🇧"},
        {"code": "DE", "name": "Deutschland", "flag": "🇩🇪"},
        {"code": "FR", "name": "France", "flag": "🇫🇷"},
        {"code": "ES", "name": "España", "flag": "🇪🇸"},
        {"code": "IT", "name": "Italia", "flag": "🇮🇹"},
        {"code": "PL", "name": "Polska", "flag": "🇵🇱"},
        {"code": "RU", "name": "Россия", "flag": "🇷🇺"},
    ]
    return {"countries": countries}

@app.get("/api/locations/{country_code}")
async def get_locations(country_code: str):
    """Get locations from Google Ads API"""
    try:
        # Create config from env vars
        create_google_ads_config()
        
        # Import Google Ads service
        from google_ads_service import get_locations_for_country
        
        locations = get_locations_for_country(country_code)
        return {
            "country_code": country_code,
            "locations": locations,
            "source": "google_ads_api"
        }
    except Exception as e:
        # Fallback to mock data
        print(f"Error: {e}")
        
        mock_data = {
            "IE": {
                "regions": [
                    {"id": "1007321", "name": "Carlow", "type": "County"},
                    {"id": "1007322", "name": "Cavan", "type": "County"},
                    {"id": "1007323", "name": "Clare", "type": "County"},
                    {"id": "1007324", "name": "Cork", "type": "County"},
                    {"id": "1007325", "name": "Donegal", "type": "County"},
                    {"id": "1007326", "name": "Dublin", "type": "County"},
                ],
                "cities": [
                    {"id": "1007340", "name": "Dublin", "type": "City"},
                    {"id": "1007341", "name": "Cork", "type": "City"},
                    {"id": "1007342", "name": "Galway", "type": "City"},
                ]
            },
            "UA": {
                "regions": [
                    {"id": "21135", "name": "Дніпропетровська", "type": "Oblast"},
                    {"id": "21136", "name": "Київська", "type": "Oblast"},
                    {"id": "21137", "name": "Львівська", "type": "Oblast"},
                ],
                "cities": [
                    {"id": "1012864", "name": "Дніпро", "type": "City"},
                    {"id": "1011969", "name": "Київ", "type": "City"},
                    {"id": "1009902", "name": "Львів", "type": "City"},
                ]
            }
        }
        
        if country_code.upper() in mock_data:
            return {
                "country_code": country_code.upper(),
                "locations": mock_data[country_code.upper()],
                "source": "mock_fallback",
                "error": str(e)
            }
        else:
            return {
                "country_code": country_code.upper(),
                "locations": {"regions": [], "cities": []},
                "source": "mock_fallback",
                "error": str(e)
            }


# ============================================
# PARSER TEST ENDPOINTS
# ============================================

@app.get("/api/test-parser/single")
async def single_test(
    query: str = Query(..., description="Search query to test"),
    country: str = Query("UA", description="Country code (e.g., UA, US)"),
    language: str = Query("ru", description="Language code (e.g., ru, en)")
):
    """
    Тест одиночного запроса к Google Autocomplete
    
    Пример: 
    GET /api/test-parser/single?query=купить%20бе%20вино&country=UA&language=ru
    GET /api/test-parser/single?query=ремонт%20а%20пылесосов&country=UA&language=ru
    """
    parser = AutocompleteParser()
    
    suggestions = await parser.fetch_suggestions(
        query=query,
        country=country,
        language=language
    )
    
    return {
        "query": query,
        "country": country,
        "language": language,
        "suggestions": suggestions,
        "count": len(suggestions),
        "status": "success" if suggestions else "no_results"
    }


@app.get("/api/test-parser/quick")
async def quick_test(
    query: str = "vacuum repair",
    country: str = "IE",
    language: str = "en"
):
    """
    Быстрый тест парсера - один запрос к Google Autocomplete
    
    Пример: GET /api/test-parser/quick?query=ремонт пылесосов&country=UA&language=ru
    """
    parser = AutocompleteParser()
    
    suggestions = await parser.fetch_suggestions(
        query=query,
        country=country,
        language=language
    )
    
    return {
        "query": query,
        "country": country,
        "language": language,
        "suggestions": suggestions,
        "count": len(suggestions),
        "status": "success" if suggestions else "no_results"
    }


@app.get("/api/test-parser/full")
async def full_test(
    seed: str = "vacuum repair",
    country: str = "IE",
    language: str = "en",
    use_numbers: bool = True,
    use_morphology: bool = False
):
    """
    Полный парсинг с модификаторами (SUFFIX + INFIX + MORPH)
    
    SUFFIX: seed + модификатор (все модификаторы a-z + а-я + 0-9)
    INFIX: слово1 + модификатор + слово2 (только кириллица а-я)
    MORPH: парсинг всех морфологических форм seed фразы
    
    Пример: GET /api/test-parser/full?seed=ремонт пылесосов&country=UA&language=ru&use_numbers=true&use_morphology=true
    """
    parser = AutocompleteParser()
    
    # Получаем список модификаторов для информации
    all_modifiers = parser.get_modifiers(language)
    if not use_numbers:
        all_modifiers = [m for m in all_modifiers if not m.isdigit()]
    
    # Разделяем модификаторы
    language_specific = parser.language_modifiers.get(language.lower(), [])
    cyrillic_modifiers = [m for m in all_modifiers if m in language_specific]
    latin_digit_modifiers = [m for m in all_modifiers if m not in language_specific]
    seed_words = seed.split()
    
    # Получаем seed вариации если морфология включена
    seed_variations = 1
    morph_available = False
    # МОРФОЛОГИЯ ЗАКОММЕНТИРОВАНА ДЛЯ ТЕСТИРОВАНИЯ PREFIX  
    # if use_morphology:
    #     if language.lower() == 'ru' and PYMORPHY_AVAILABLE:
    #         morph_available = True
    #     elif language.lower() == 'en' and INFLECT_AVAILABLE:
    #         morph_available = True
    #     
    #     if morph_available:
    #         variations = parser.get_seed_variations(seed, language)
    #         seed_variations = len(variations)
    
    # ПРАВИЛЬНЫЙ РАСЧЕТ (МОРФОЛОГИЯ ВРЕМЕННО ОТКЛЮЧЕНА):
    # 1. SUFFIX латиница/цифры (БЕЗ морфологии)
    suffix_latin_requests = len(latin_digit_modifiers)
    
    # 2. SUFFIX кириллица (БЕЗ морфологии - ВРЕМЕННО!)
    suffix_cyrillic_requests = len(cyrillic_modifiers) * seed_variations
    
    # 3. INFIX кириллица (БЕЗ морфологии!)
    infix_requests = len(cyrillic_modifiers) if len(seed_words) >= 2 else 0
    
    # 4. PREFIX кириллица (БЕЗ морфологии!) - НОВОЕ!
    prefix_requests = len(cyrillic_modifiers)
    
    # ВСЕГО запросов
    total_requests = suffix_latin_requests + suffix_cyrillic_requests + infix_requests + prefix_requests

    
    start_time = time.time()
    
    keywords = await parser.parse_with_modifiers(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers,
        use_morphology=use_morphology
    )
    
    parsing_time = time.time() - start_time
    
    return {
        "seed": seed,
        "country": country,
        "language": language,
        "modifiers_info": {
            "total_modifiers": len(all_modifiers),
            "latin_digit_modifiers": len(latin_digit_modifiers),
            "cyrillic_modifiers": len(cyrillic_modifiers),
            "base": "a-z" + (" + 0-9" if use_numbers else ""),
            "language_specific": "".join(language_specific) or "none"
        },
        "morphology_info": {
            "enabled": use_morphology,
            "available": morph_available,
            "seed_variations": seed_variations if morph_available else 1,
            "library": "pymorphy2" if language.lower() == 'ru' else "inflect" if language.lower() == 'en' else "none"
        },
        "requests_info": {
            "suffix_latin_digit": suffix_latin_requests,
            "suffix_cyrillic": suffix_cyrillic_requests,
            "infix": infix_requests,
            "prefix": prefix_requests,
            "total_requests": total_requests,
            "formula": f"{suffix_latin_requests} (latin/digit) + {suffix_cyrillic_requests} (cyrillic×{seed_variations}) + {infix_requests} (infix) + {prefix_requests} (prefix) = {total_requests}"
        },
        "keywords": keywords,
        "count": len(keywords),
        "requests_made": total_requests,
        "parsing_time": round(parsing_time, 2)
    }


@app.post("/api/test-parser", response_model=ParseResponse)
async def test_parser(request: ParseRequest):
    """
    Полный парсинг с модификаторами (a-z, опционально 0-9, морфология)
    
    Пример запроса:
    POST /api/test-parser
    {
        "seed": "ремонт пылесосов",
        "country": "UA",
        "language": "ru",
        "use_numbers": false,
        "use_morphology": true
    }
    """
    parser = AutocompleteParser()
    
    start_time = time.time()
    
    keywords = await parser.parse_with_modifiers(
        seed=request.seed,
        country=request.country,
        language=request.language,
        use_numbers=request.use_numbers,
        use_morphology=request.use_morphology
    )
    
    parsing_time = time.time() - start_time
    
    # Получаем модификаторы
    all_modifiers = parser.get_modifiers(request.language)
    if not request.use_numbers:
        all_modifiers = [m for m in all_modifiers if not m.isdigit()]
    
    # Разделяем модификаторы
    language_specific = parser.language_modifiers.get(request.language.lower(), [])
    cyrillic_modifiers = [m for m in all_modifiers if m in language_specific]
    latin_digit_modifiers = [m for m in all_modifiers if m not in language_specific]
    seed_words = request.seed.split()
    
    # МОРФОЛОГИЯ ЗАКОММЕНТИРОВАНА ДЛЯ ТЕСТИРОВАНИЯ PREFIX
    # Seed вариации если морфология включена
    seed_variations = 1
    # if request.use_morphology:
    #     morph_available = (request.language.lower() == 'ru' and PYMORPHY_AVAILABLE) or \
    #                      (request.language.lower() == 'en' and INFLECT_AVAILABLE)
    #     if morph_available:
    #         variations = parser.get_seed_variations(request.seed, request.language)
    #         seed_variations = len(variations)
    
    # Расчет запросов (МОРФОЛОГИЯ ОТКЛЮЧЕНА, ДОБАВЛЕН PREFIX)
    suffix_latin = len(latin_digit_modifiers)
    suffix_cyrillic = len(cyrillic_modifiers) * seed_variations
    infix = len(cyrillic_modifiers) if len(seed_words) >= 2 else 0
    prefix = len(cyrillic_modifiers)  # НОВОЕ!
    total_requests = suffix_latin + suffix_cyrillic + infix + prefix
    
    return ParseResponse(
        seed=request.seed,
        keywords=keywords,
        count=len(keywords),
        requests_made=total_requests,
        parsing_time=round(parsing_time, 2)
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
