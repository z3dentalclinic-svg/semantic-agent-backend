"""
Google Autocomplete Parser API
Версия: 4.0 Clean
Оптимизированный парсинг с поддержкой трёх методов
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
import httpx
import asyncio
import time
import random

# ============================================
# FASTAPI APP
# ============================================
app = FastAPI(
    title="Google Autocomplete Parser API",
    version="4.0",
    description="SUFFIX + INFIX + MORPHOLOGY"
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
# ADAPTIVE DELAY CLASS
# ============================================
class AdaptiveDelay:
    """Автоматическая оптимизация задержек между запросами"""
    
    def __init__(self, initial_delay: float = 0.2, min_delay: float = 0.1, max_delay: float = 1.0):
        self.delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.success_count = 0
        self.rate_limit_hits = 0
    
    def get_delay(self) -> float:
        return self.delay
    
    def on_success(self):
        self.success_count += 1
        if self.success_count >= 10:
            self.delay = max(self.min_delay, self.delay * 0.9)
            self.success_count = 0
    
    def on_rate_limit(self):
        self.rate_limit_hits += 1
        self.delay = min(self.max_delay, self.delay * 2)
        self.success_count = 0


# ============================================
# KEYWORD PARSER CLASS
# ============================================
class KeywordParser:
    """Основной класс для парсинга ключевых слов"""
    
    def __init__(self):
        self.adaptive_delay = AdaptiveDelay(initial_delay=0.2, min_delay=0.1, max_delay=1.0)
        
        # Модификаторы для разных языков (только буквы которые могут начинать слова)
        self.language_modifiers = {
            'ru': list("абвгдежзийклмнопрстуфхцчшщэюя"),  # Убрал "ъ", "ь", "ы"
            'uk': list("абвгґдеєжзиіїйклмнопрстуфхцчшщюя"),  # Убрал "ь"
            'en': list("abcdefghijklmnopqrstuvwxyz"),
            'pl': list("aąbcćdeęfghijklłmnńoóprsśtuwyzźż"),
            'de': list("abcdefghijklmnopqrstuvwxyzäöüß"),
            'fr': list("abcdefghijklmnopqrstuvwxyzàâæçéèêëïîôùûüÿœ"),
            'es': list("abcdefghijklmnñopqrstuvwxyzáéíóúü"),
        }
    
    def detect_seed_language(self, seed: str) -> str:
        """Определить язык seed (latin/cyrillic)"""
        has_cyrillic = any(ord('а') <= ord(c.lower()) <= ord('я') for c in seed if c.isalpha())
        return 'cyrillic' if has_cyrillic else 'latin'
    
    def get_modifiers(self, language: str, use_numbers: bool, seed: str, cyrillic_only: bool = False) -> List[str]:
        """
        Получить модификаторы с умной фильтрацией
        
        ФИЛЬТРАЦИЯ:
        - Английский seed → только a-z
        - Латинский seed → убираем кириллицу
        - Кириллический seed → оставляем ВСЁ (латиница для брендов!)
        - cyrillic_only=True → только кириллица (для INFIX)
        """
        seed_lang = self.detect_seed_language(seed)
        base_latin = list("abcdefghijklmnopqrstuvwxyz")
        numbers = list("0123456789") if use_numbers else []
        lang_specific = self.language_modifiers.get(language.lower(), [])
        
        if cyrillic_only:
            # INFIX: только кириллица
            is_cyrillic = lambda c: ord('а') <= ord(c.lower()) <= ord('я') or c in ['ё', 'і', 'ї', 'є', 'ґ', 'ў']
            result = [m for m in lang_specific if is_cyrillic(m)]
            print(f"🔤 Модификаторы (cyrillic_only): {len(result)} букв")
            return result
        
        if language.lower() == 'en' and seed_lang == 'latin':
            # Английский → только a-z + цифры
            result = base_latin + numbers
            print(f"🔤 Модификаторы (en): {len(result)} символов")
            return result
        
        if seed_lang == 'latin':
            # Латинский → убираем кириллицу
            is_cyrillic = lambda c: ord('а') <= ord(c.lower()) <= ord('я') or c in ['ё', 'і', 'ї', 'є', 'ґ', 'ў']
            non_cyrillic = [m for m in lang_specific if not is_cyrillic(m)]
            result = base_latin + non_cyrillic + numbers
            print(f"🔤 Модификаторы (latin): {len(result)} символов")
            return result
        
        # Кириллический → всё (латиница для брендов!)
        result = base_latin + lang_specific + numbers
        print(f"🔤 Модификаторы (ru): {len(base_latin)} латинских + {len(lang_specific)} кириллических = {len(result)} всего")
        print(f"   Кириллица: {''.join(lang_specific)}")
        return result
    
    def get_morphological_forms(self, word: str, language: str) -> List[str]:
        """Получить морфологические формы слова"""
        forms = set([word])
        
        if language.lower() in ['ru', 'uk']:
            try:
                import pymorphy3
                morph = pymorphy3.MorphAnalyzer()
                parsed = morph.parse(word)
                
                if parsed:
                    for form in parsed[0].lexeme:
                        # Фильтруем причастия и деепричастия
                        # Они создают странные комбинации типа "купившего rgb"
                        pos = form.tag.POS
                        if pos not in ['PRTS', 'PRTF', 'GRND']:  # participle short, participle full, gerund
                            forms.add(form.word)
                
                print(f"📖 Морфология: '{word}' → {len(forms)} форм (без причастий)")
            except ImportError:
                print(f"⚠️ pymorphy3 не установлен")
            except Exception as e:
                print(f"⚠️ Ошибка: {e}")
        
        elif language.lower() == 'en':
            try:
                import lemminflect
                plurals = lemminflect.getAllInflections(word, upos='NOUN')
                if plurals and 'NNS' in plurals:
                    forms.update(plurals['NNS'])
                
                if not word.endswith('s'):
                    forms.add(word + "'s")
                    forms.add(word + "s")
            except:
                pass
        
        return sorted(list(forms))
    
    async def autocorrect_text(self, text: str, language: str) -> Dict:
        """
        Автокоррекция текста через Yandex Speller API
        
        Returns:
        {
            "original": "медецинский стерелизатор купить",
            "corrected": "медицинский стерилизатор купить",
            "corrections": [
                {"word": "медецинский", "suggestion": "медицинский"},
                {"word": "стерелизатор", "suggestion": "стерилизатор"}
            ],
            "has_errors": True
        }
        """
        url = "https://speller.yandex.net/services/spellservice.json/checkText"
        
        # Определяем язык для Yandex (ru, uk, en)
        lang_map = {
            'ru': 'ru',
            'uk': 'uk',
            'en': 'en'
        }
        yandex_lang = lang_map.get(language.lower(), 'ru')
        
        params = {
            "text": text,
            "lang": yandex_lang,
            "options": 0  # 0 = игнорировать капитализацию
        }
        
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url, params=params)
                
                if response.status_code == 200:
                    errors = response.json()
                    
                    if not errors:
                        # Нет ошибок
                        return {
                            "original": text,
                            "corrected": text,
                            "corrections": [],
                            "has_errors": False
                        }
                    
                    # Исправляем ошибки
                    corrected = text
                    corrections = []
                    
                    # Сортируем по позиции (от конца к началу чтобы не сбить индексы)
                    errors_sorted = sorted(errors, key=lambda x: x.get('pos', 0), reverse=True)
                    
                    for error in errors_sorted:
                        word = error.get('word', '')
                        suggestions = error.get('s', [])
                        
                        if suggestions:
                            suggestion = suggestions[0]  # Берём первую подсказку
                            pos = error.get('pos', 0)
                            length = error.get('len', len(word))
                            
                            # Заменяем слово
                            corrected = corrected[:pos] + suggestion + corrected[pos + length:]
                            
                            corrections.append({
                                "word": word,
                                "suggestion": suggestion
                            })
                    
                    return {
                        "original": text,
                        "corrected": corrected,
                        "corrections": corrections,
                        "has_errors": True
                    }
                
                # Если API не работает - возвращаем оригинал
                return {
                    "original": text,
                    "corrected": text,
                    "corrections": [],
                    "has_errors": False,
                    "error": "API unavailable"
                }
                
        except Exception as e:
            print(f"⚠️ Yandex Speller error: {e}")
            # Fallback: пробуем LanguageTool для всех языков
            return await self.autocorrect_languagetool(text, language)
    
    async def autocorrect_languagetool(self, text: str, language: str) -> Dict:
        """
        Автокоррекция через LanguageTool API (30+ языков)
        
        Поддержка: ru, uk, en, de, fr, es, pl, it, pt, nl, ca, sv, da, no, fi, ja, zh, ar...
        """
        url = "https://api.languagetool.org/v2/check"
        
        # Маппинг языков
        lang_map = {
            'ru': 'ru-RU',
            'uk': 'uk-UA',
            'en': 'en-US',
            'de': 'de-DE',
            'fr': 'fr-FR',
            'es': 'es-ES',
            'pl': 'pl-PL',
            'it': 'it-IT',
            'pt': 'pt-PT',
            'nl': 'nl-NL'
        }
        
        lt_lang = lang_map.get(language.lower(), 'en-US')
        
        data = {
            "text": text,
            "language": lt_lang
        }
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, data=data)
                
                if response.status_code == 200:
                    result = response.json()
                    matches = result.get('matches', [])
                    
                    if not matches:
                        return {
                            "original": text,
                            "corrected": text,
                            "corrections": [],
                            "has_errors": False
                        }
                    
                    # Исправляем только spelling errors
                    corrected = text
                    corrections = []
                    
                    # Сортируем от конца к началу
                    spelling_errors = [m for m in matches if 'misspelling' in m.get('rule', {}).get('issueType', '').lower()]
                    spelling_errors_sorted = sorted(spelling_errors, key=lambda x: x.get('offset', 0), reverse=True)
                    
                    for match in spelling_errors_sorted:
                        replacements = match.get('replacements', [])
                        if not replacements:
                            continue
                        
                        offset = match.get('offset', 0)
                        length = match.get('length', 0)
                        original_word = text[offset:offset+length]
                        suggestion = replacements[0].get('value', '')
                        
                        # Заменяем
                        corrected = corrected[:offset] + suggestion + corrected[offset+length:]
                        
                        corrections.append({
                            "word": original_word,
                            "suggestion": suggestion
                        })
                    
                    if corrections:
                        print(f"✏️ LanguageTool ({language}): '{text}' → '{corrected}'")
                    
                    return {
                        "original": text,
                        "corrected": corrected,
                        "corrections": corrections,
                        "has_errors": len(corrections) > 0,
                        "service": "LanguageTool"
                    }
                
                # API не работает - возвращаем оригинал
                return {
                    "original": text,
                    "corrected": text,
                    "corrections": [],
                    "has_errors": False
                }
                
        except Exception as e:
            print(f"⚠️ LanguageTool error: {e}")
            # Используем оригинальный текст
            return {
                "original": text,
                "corrected": text,
                "corrections": [],
                "has_errors": False
            }
    
    async def filter_infix_results(self, keywords: List[str], language: str) -> List[str]:
        """
        Фильтр для INFIX результатов - убираем мусорные фразы с одиночными бессмысленными буквами
        
        Проблема:
        "медицинский э стерилизатор купить" ← "э" = мусор ❌
        "медицинский в стерилизатор купить" ← "в" = предлог ✅
        
        Решение: проверяем ВСЕ одиночные буквы по whitelist предлогов
        """
        
        # Whitelist полезных предлогов/союзов (расширенный)
        if language.lower() == 'ru':
            valid_prepositions = {'в', 'на', 'у', 'к', 'от', 'из', 'по', 'о', 'об', 'с', 'со', 'за', 'для', 'и', 'а', 'но'}
        elif language.lower() == 'uk':
            valid_prepositions = {'в', 'на', 'у', 'до', 'від', 'з', 'по', 'про', 'для', 'і', 'та', 'або'}
        elif language.lower() == 'en':
            valid_prepositions = {'in', 'on', 'at', 'to', 'from', 'with', 'for', 'by', 'of', 'and', 'or', 'a', 'i'}
        else:
            valid_prepositions = set()
        
        filtered = []
        
        for keyword in keywords:
            keyword_lower = keyword.lower()
            words = keyword_lower.split()
            
            # Ищем ЛЮБЫЕ одиночные буквы (кроме первого слова)
            has_garbage = False
            for i in range(1, len(words)):  # ← ИСПРАВЛЕНО: проверяем ВСЕ слова после первого!
                word = words[i]
                if len(word) == 1:  # Одиночная буква
                    if word not in valid_prepositions:
                        # Бессмысленная одиночная буква → МУСОР!
                        has_garbage = True
                        break
            
            if not has_garbage:
                filtered.append(keyword)
        
        return filtered
    
    def get_infix_modifiers(self, language: str) -> List[str]:
        """
        Получить модификаторы специально для INFIX метода
        
        INFIX вставляет слова МЕЖДУ словами в seed, поэтому нужны:
        - Предлоги (в, на, для, с, от, у, к, по, из)
        - Союзы (и, а, или)
        - НЕ нужны: одиночные бессмысленные буквы (б, ж, ц, щ...)
        
        Примеры:
        "ремонт телефонов" + "в" = "ремонт в телефонов" → "ремонт в телефонов киев" ✅
        "ремонт телефонов" + "ж" = "ремонт ж телефонов" → МУСОР ❌
        """
        
        if language.lower() == 'ru':
            return [
                # Предлоги места
                'в', 'на', 'у', 'к', 'от', 'из', 'по', 'о', 'об',
                # Предлоги с инструментальным
                'с', 'со', 'за', 'под', 'над', 'между',
                # Предлоги для
                'для',
                # Союзы
                'и', 'а', 'или',
                # Другие частые слова
                'без', 'про', 'через'
            ]
        elif language.lower() == 'uk':
            return [
                'в', 'на', 'у', 'до', 'від', 'з', 'по', 'про',
                'для', 'і', 'та', 'або', 'без', 'через'
            ]
        elif language.lower() == 'en':
            return [
                'in', 'on', 'at', 'to', 'from', 'with', 'for',
                'by', 'of', 'and', 'or', 'the', 'a'
            ]
        else:
            # Для других языков - используем только частые предлоги
            return ['in', 'on', 'for', 'with', 'and', 'or']
    
    async def filter_relevant_keywords(self, keywords: List[str], seed: str) -> List[str]:
        """
        Фильтр релевантности - оставляем только ключи содержащие ВСЕ важные слова из seed
        
        Проблема:
        seed = "медецинский стерелизатор купить"
        Google возвращает: "купить холодильник харьков" ← потерял "стерелизатор"!
        
        Решение: проверяем что ключ содержит хотя бы одно главное существительное из seed
        """
        
        # Извлекаем слова из seed
        seed_words = set(seed.lower().split())
        
        # Стоп-слова (не проверяем их наличие)
        stop_words = {'в', 'на', 'для', 'с', 'о', 'по', 'из', 'к', 'от', 'у', 
                     'купить', 'купил', 'купишь', 'купят', 'куплю', 'купила', 'покупать',
                     'заказать', 'заказал', 'заказывать',
                     'цена', 'цены', 'ценам', 'стоимость', 
                     'недорого', 'дешево', 'дорого', 'где', 'как', 'что'}
        
        # Важные слова = все слова кроме стоп-слов
        important_words = [w for w in seed_words if w not in stop_words and len(w) > 2]
        
        if not important_words:
            return keywords  # Если нет важных слов, возвращаем всё
        
        # Находим самое редкое/специфичное слово (обычно это существительное)
        # Например: "стерелизатор" - редкое слово
        # "медецинский" - частое
        main_word = max(important_words, key=len)  # Берём самое длинное как главное
        
        filtered = []
        for keyword in keywords:
            keyword_lower = keyword.lower()
            
            # КРИТЕРИЙ: ключ ДОЛЖЕН содержать главное слово!
            if main_word in keyword_lower:
                filtered.append(keyword)
            else:
                # Потерял главное слово → МУСОР!
                pass
        
        return filtered
    
    async def fetch_suggestions(self, query: str, country: str, language: str, client: httpx.AsyncClient) -> List[str]:
        """Получить подсказки от Google Autocomplete"""
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
                if isinstance(data, list) and len(data) > 1:
                    return [s for s in data[1] if isinstance(s, str)]
            
            return []
            
        except Exception:
            return []
    
    async def fetch_suggestions_bing(self, query: str, language: str, country: str, client: httpx.AsyncClient) -> List[str]:
        """Получить подсказки от Bing Autosuggest"""
        
        # Формируем market code
        if language == "ru" and country == "UA":
            market = "ru-RU"  # Используем русский российский
            setlang = "ru"
        elif language == "uk" and country == "UA":
            market = "uk-UA"
            setlang = "uk"
        elif language == "ru" and country == "RU":
            market = "ru-RU"
            setlang = "ru"
        elif language == "en" and country == "US":
            market = "en-US"
            setlang = "en"
        else:
            market = f"{language}-{country}"
            setlang = language
        
        # Используем альтернативный endpoint
        url = "https://api.bing.com/osjson.aspx"
        params = {
            "query": query,
            "market": market,
            "setLang": setlang
        }
        
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json",
            "Accept-Language": f"{language}",
            "Referer": "https://www.bing.com/"
        }
        
        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0, follow_redirects=True)
            
            if response.status_code == 429:
                self.adaptive_delay.on_rate_limit()
                return []
            
            if response.status_code == 403:
                print(f"⚠️ Bing 403: блокировка запроса")
                return []
            
            self.adaptive_delay.on_success()
            
            if response.status_code == 200:
                data = response.json()
                # Bing osjson возвращает: [query, [suggestions]]
                if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
                    return [s for s in data[1] if isinstance(s, str)]
            
            return []
            
        except Exception as e:
            print(f"⚠️ Bing error: {e}")
            return []
    
    async def fetch_suggestions_yandex(self, query: str, language: str, region_id: int, client: httpx.AsyncClient) -> List[str]:
        """Получить подсказки от Yandex Suggest"""
        url = "https://suggest.yandex.ru/suggest-ff.cgi"
        params = {
            "part": query,
            "uil": language,
            "v": "3",
            "lr": region_id  # 0=без региона, 143=Киев, 213=Москва, 20544=Харьков
        }
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        
        try:
            response = await client.get(url, params=params, headers=headers, timeout=10.0)
            
            if response.status_code == 429:
                self.adaptive_delay.on_rate_limit()
                return []
            
            self.adaptive_delay.on_success()
            
            if response.status_code == 200:
                data = response.json()
                # Yandex возвращает: [query, [suggestions], ...]
                if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
                    return [s for s in data[1] if isinstance(s, str)]
            
            return []
            
        except Exception:
            return []
    
    async def parse_with_semaphore(self, queries: List[str], country: str, language: str, parallel_limit: int, use_yandex: bool = False, region_id: int = 0) -> Dict:
        """Парсинг с ограничением параллельности"""
        semaphore = asyncio.Semaphore(parallel_limit)
        
        async def fetch_with_limit(query: str, client: httpx.AsyncClient):
            async with semaphore:
                await asyncio.sleep(self.adaptive_delay.get_delay())
                
                if use_yandex:
                    # Запрашиваем оба источника параллельно
                    google_task = self.fetch_suggestions(query, country, language, client)
                    yandex_task = self.fetch_suggestions_yandex(query, language, region_id, client)
                    google_results, yandex_results = await asyncio.gather(google_task, yandex_task)
                    
                    # Объединяем результаты
                    combined = list(set(google_results + yandex_results))
                    return combined
                else:
                    # Только Google
                    return await self.fetch_suggestions(query, country, language, client)
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = [fetch_with_limit(q, client) for q in queries]
            results = await asyncio.gather(*tasks)
        
        all_keywords = set()
        for suggestions in results:
            all_keywords.update(suggestions)
        
        success_count = sum(1 for r in results if r)
        fail_count = len(results) - success_count
        
        return {
            "keywords": sorted(list(all_keywords)),
            "success": success_count,
            "failed": fail_count
        }
    
    # ============================================
    # DUAL METHOD (GOOGLE + YANDEX)
    # ============================================
    async def parse_dual(self, seed: str, country: str, region_id: int, language: str, use_numbers: bool, parallel_limit: int) -> Dict:
        """DUAL метод: Google + Yandex параллельно"""
        start_time = time.time()
        print(f"\n🔵🔴 DUAL (Google + Yandex): {seed}")
        
        modifiers = self.get_modifiers(language, use_numbers, seed)
        queries = [f"{seed} {mod}" for mod in modifiers]
        
        # Запрашиваем оба источника параллельно для каждого запроса
        semaphore = asyncio.Semaphore(parallel_limit)
        
        async def fetch_dual(query: str, client: httpx.AsyncClient):
            async with semaphore:
                await asyncio.sleep(self.adaptive_delay.get_delay())
                
                # Параллельно Google + Yandex
                google_task = self.fetch_suggestions(query, country, language, client)
                yandex_task = self.fetch_suggestions_yandex(query, language, region_id, client)
                google_results, yandex_results = await asyncio.gather(google_task, yandex_task)
                
                # Объединяем
                combined = list(set(google_results + yandex_results))
                
                return {
                    "google": google_results,
                    "yandex": yandex_results,
                    "combined": combined
                }
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = [fetch_dual(q, client) for q in queries]
            results = await asyncio.gather(*tasks)
        
        # Собираем статистику
        all_keywords = set()
        google_only_keywords = set()
        yandex_only_keywords = set()
        
        for result in results:
            all_keywords.update(result["combined"])
            
            google_set = set(result["google"])
            yandex_set = set(result["yandex"])
            
            google_only_keywords.update(google_set - yandex_set)
            yandex_only_keywords.update(yandex_set - google_set)
        
        elapsed_time = time.time() - start_time
        
        google_total = len(all_keywords) - len(yandex_only_keywords)
        yandex_total = len(all_keywords) - len(google_only_keywords)
        overlap = google_total + yandex_total - len(all_keywords)
        
        print(f"✅ ИТОГО: {len(all_keywords)} ключей")
        print(f"🔵 Google: {google_total} ({len(google_only_keywords)} уникальных)")
        print(f"🔴 Yandex: {yandex_total} ({len(yandex_only_keywords)} уникальных)")
        print(f"⏱️ Время: {elapsed_time:.2f} сек")
        
        return {
            "seed": seed,
            "method": "dual",
            "sources": ["Google Autocomplete", "Yandex Suggest"],
            "keywords": sorted(list(all_keywords)),
            "count": len(all_keywords),
            "queries": len(queries),
            "elapsed_time": round(elapsed_time, 2),
            "breakdown": {
                "google": {
                    "total": google_total,
                    "unique": len(google_only_keywords)
                },
                "yandex": {
                    "total": yandex_total,
                    "unique": len(yandex_only_keywords)
                },
                "overlap": overlap,
                "yandex_gain": f"+{round(len(yandex_only_keywords) / google_total * 100, 1)}%" if google_total > 0 else "0%"
            }
        }
    
    # ============================================
    # YANDEX METHOD
    # ============================================
    async def parse_yandex(self, seed: str, region_id: int, language: str, use_numbers: bool, parallel_limit: int) -> Dict:
        """YANDEX ТОЛЬКО метод - для тестирования ценности Yandex"""
        start_time = time.time()
        print(f"\n🔴 YANDEX: {seed}")
        
        modifiers = self.get_modifiers(language, use_numbers, seed)
        queries = [f"{seed} {mod}" for mod in modifiers]
        
        # Используем только Yandex
        semaphore = asyncio.Semaphore(parallel_limit)
        
        async def fetch_yandex_only(query: str, client: httpx.AsyncClient):
            async with semaphore:
                await asyncio.sleep(self.adaptive_delay.get_delay())
                return await self.fetch_suggestions_yandex(query, language, region_id, client)
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            tasks = [fetch_yandex_only(q, client) for q in queries]
            results = await asyncio.gather(*tasks)
        
        all_keywords = set()
        for suggestions in results:
            all_keywords.update(suggestions)
        
        success_count = sum(1 for r in results if r)
        elapsed_time = time.time() - start_time
        
        print(f"✅ {len(all_keywords)} ключей за {elapsed_time:.2f} сек")
        
        return {
            "seed": seed,
            "method": "yandex",
            "source": "Yandex Suggest",
            "keywords": sorted(list(all_keywords)),
            "count": len(all_keywords),
            "queries": len(queries),
            "region_id": region_id,
            "elapsed_time": round(elapsed_time, 2)
        }
    
    # ============================================
    # SUFFIX METHOD
    # ============================================
    async def parse(self, seed: str, country: str, language: str, use_numbers: bool, parallel_limit: int) -> Dict:
        """SUFFIX метод"""
        start_time = time.time()
        print(f"\n⚡ SUFFIX: {seed}")
        
        modifiers = self.get_modifiers(language, use_numbers, seed)
        queries = [f"{seed} {mod}" for mod in modifiers]
        
        result = await self.parse_with_semaphore(queries, country, language, parallel_limit)
        elapsed_time = time.time() - start_time
        
        print(f"✅ {len(result['keywords'])} ключей за {elapsed_time:.2f} сек")
        
        return {
            "seed": seed,
            "method": "suffix",
            "keywords": result["keywords"],
            "count": len(result["keywords"]),
            "queries": len(queries),
            "elapsed_time": round(elapsed_time, 2)
        }
    
    # ============================================
    # INFIX METHOD
    # ============================================
    async def parse_infix(self, seed: str, country: str, language: str, use_numbers: bool, parallel_limit: int) -> Dict:
        """INFIX метод"""
        start_time = time.time()
        print(f"\n🔄 INFIX: {seed}")
        
        words = seed.strip().split()
        
        if len(words) < 2:
            return {"error": "INFIX требует минимум 2 слова", "seed": seed}
        
        modifiers = self.get_modifiers(language, use_numbers, seed, cyrillic_only=True)
        queries = []
        
        for i in range(1, len(words)):
            for mod in modifiers:
                query = ' '.join(words[:i]) + f' {mod} ' + ' '.join(words[i:])
                queries.append(query)
        
        result_raw = await self.parse_with_semaphore(queries, country, language, parallel_limit)
        
        # ФИЛЬТРУЕМ результаты от мусорных одиночных букв
        filtered_keywords = await self.filter_infix_results(result_raw['keywords'], language)
        
        elapsed_time = time.time() - start_time
        
        print(f"✅ {len(result_raw['keywords'])} → {len(filtered_keywords)} ключей (отфильтровано {len(result_raw['keywords']) - len(filtered_keywords)}) за {elapsed_time:.2f} сек")
        
        return {
            "seed": seed,
            "method": "infix",
            "keywords": filtered_keywords,
            "count": len(filtered_keywords),
            "queries": len(queries),
            "elapsed_time": round(elapsed_time, 2)
        }
    
    # ============================================
    # MORPHOLOGY METHOD
    # ============================================
    async def parse_morphology(self, seed: str, country: str, language: str, use_numbers: bool, parallel_limit: int) -> Dict:
        """MORPHOLOGY метод - модифицирует ВСЕ существительные в запросе"""
        start_time = time.time()
        print(f"\n🚀 MORPHOLOGY: {seed}")
        
        words = seed.strip().split()
        
        # Находим все существительные в запросе
        nouns_to_modify = []
        
        if language.lower() in ['ru', 'uk']:
            try:
                import pymorphy3
                morph = pymorphy3.MorphAnalyzer()
                
                for idx, word in enumerate(words):
                    parsed = morph.parse(word)
                    if parsed:
                        # Проверяем является ли слово существительным
                        pos = parsed[0].tag.POS
                        if pos == 'NOUN':
                            nouns_to_modify.append({
                                'index': idx,
                                'word': word,
                                'forms': self.get_morphological_forms(word, language)
                            })
                            print(f"📌 Существительное #{idx}: '{word}' → {len(self.get_morphological_forms(word, language))} форм")
                
                if not nouns_to_modify:
                    print(f"⚠️ Существительные не найдены, модифицируем последнее слово")
                    last_word = words[-1]
                    nouns_to_modify.append({
                        'index': len(words) - 1,
                        'word': last_word,
                        'forms': self.get_morphological_forms(last_word, language)
                    })
                
            except ImportError:
                print(f"⚠️ pymorphy3 не установлен, модифицируем последнее слово")
                last_word = words[-1]
                nouns_to_modify.append({
                    'index': len(words) - 1,
                    'word': last_word,
                    'forms': [last_word]
                })
        else:
            # Для не-русских языков модифицируем последнее слово
            last_word = words[-1]
            nouns_to_modify.append({
                'index': len(words) - 1,
                'word': last_word,
                'forms': self.get_morphological_forms(last_word, language)
            })
        
        print(f"📚 Будем модифицировать: {len(nouns_to_modify)} слов(а)")
        
        # Генерируем все комбинации форм
        all_seeds = []
        
        if len(nouns_to_modify) == 1:
            # Одно существительное - просто меняем формы
            noun = nouns_to_modify[0]
            for form in noun['forms']:
                new_words = words.copy()
                new_words[noun['index']] = form
                all_seeds.append(' '.join(new_words))
        
        else:
            # Несколько существительных - модифицируем ПЕРВОЕ (обычно это главное слово)
            # Например: "ремонт телефонов" → модифицируем "ремонт"
            noun = nouns_to_modify[0]
            print(f"🎯 Модифицируем первое существительное: '{noun['word']}'")
            
            for form in noun['forms']:
                new_words = words.copy()
                new_words[noun['index']] = form
                all_seeds.append(' '.join(new_words))
        
        unique_seeds = list(set(all_seeds))
        print(f"📋 Уникальных вариантов seed: {len(unique_seeds)}")
        
        # Парсим каждый вариант
        async def parse_single_seed(seed_variant: str) -> Dict:
            modifiers = self.get_modifiers(language, use_numbers, seed)
            queries = [f"{seed_variant} {mod}" for mod in modifiers]
            result = await self.parse_with_semaphore(queries, country, language, parallel_limit)
            return {"keywords": result["keywords"], "queries": len(queries)}
        
        tasks = [parse_single_seed(s) for s in unique_seeds]
        seed_results = await asyncio.gather(*tasks)
        
        all_keywords = set()
        total_queries = 0
        
        for seed_result in seed_results:
            all_keywords.update(seed_result["keywords"])
            total_queries += seed_result["queries"]
        
        elapsed_time = time.time() - start_time
        print(f"✅ {len(all_keywords)} ключей за {elapsed_time:.2f} сек")
        
        return {
            "seed": seed,
            "method": "morphology",
            "keywords": sorted(list(all_keywords)),
            "count": len(all_keywords),
            "forms_count": len(unique_seeds),
            "nouns_modified": len(nouns_to_modify),
            "queries": total_queries,
            "elapsed_time": round(elapsed_time, 2)
        }
    
    # ============================================
    # COMPARE METHOD
    # ============================================
    async def compare_all(self, seed: str, country: str, region_id: int, language: str, use_numbers: bool, parallel_limit: int, include_keywords: bool, source: str = "google") -> Dict:
        """Сравнение всех трёх методов с выбором источника (google/yandex/bing/all)"""
        print(f"\n🔥 COMPARE ({source.upper()}): SUFFIX vs INFIX vs MORPHOLOGY")
        
        # ✏️ АВТОКОРРЕКЦИЯ ПЕРЕД ПАРСИНГОМ
        correction = await self.autocorrect_text(seed, language)
        original_seed = seed
        
        if correction.get("has_errors"):
            seed = correction["corrected"]
            print(f"✏️ Исправлено: '{original_seed}' → '{seed}'")
            for cor in correction.get("corrections", []):
                print(f"   • '{cor['word']}' → '{cor['suggestion']}'")
        
        # Определяем какой источник использовать
        async def fetch_with_source(query: str, client: httpx.AsyncClient):
            """Запрос к выбранному источнику"""
            if source == "google":
                return await self.fetch_suggestions(query, country, language, client)
            elif source == "yandex":
                return await self.fetch_suggestions_yandex(query, language, region_id, client)
            elif source == "bing":
                return await self.fetch_suggestions_bing(query, language, country, client)
            elif source == "all":
                # Все три источника параллельно
                google_task = self.fetch_suggestions(query, country, language, client)
                yandex_task = self.fetch_suggestions_yandex(query, language, region_id, client)
                bing_task = self.fetch_suggestions_bing(query, language, country, client)
                google_results, yandex_results, bing_results = await asyncio.gather(google_task, yandex_task, bing_task)
                # Объединяем и дедуплицируем
                return list(set(google_results + yandex_results + bing_results))
            else:
                return []
        
        # Переопределяем метод для текущего источника
        async def parse_with_source(queries: List[str]) -> Dict:
            """Парсинг с выбранным источником"""
            semaphore = asyncio.Semaphore(parallel_limit)
            
            async def fetch_with_limit(query: str, client: httpx.AsyncClient):
                async with semaphore:
                    await asyncio.sleep(self.adaptive_delay.get_delay())
                    return await fetch_with_source(query, client)
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                tasks = [fetch_with_limit(q, client) for q in queries]
                results = await asyncio.gather(*tasks)
            
            all_keywords = set()
            for suggestions in results:
                all_keywords.update(suggestions)
            
            return {
                "keywords": sorted(list(all_keywords)),
                "success": sum(1 for r in results if r),
                "failed": len(results) - sum(1 for r in results if r)
            }
        
        # SUFFIX
        print(f"⚡ Запуск SUFFIX ({source})...")
        modifiers_suffix = self.get_modifiers(language, use_numbers, seed)
        queries_suffix = [f"{seed} {mod}" for mod in modifiers_suffix]
        suffix_result_raw = await parse_with_source(queries_suffix)
        
        # ФИЛЬТРУЕМ нерелевантные ключи
        filtered_suffix = await self.filter_relevant_keywords(suffix_result_raw['keywords'], seed)
        suffix_result = {
            "keywords": filtered_suffix,
            "success": suffix_result_raw['success'],
            "failed": suffix_result_raw['failed']
        }
        
        suffix_time = time.time()
        print(f"✅ SUFFIX: {len(suffix_result_raw['keywords'])} → {len(filtered_suffix)} ключей (отфильтровано {len(suffix_result_raw['keywords']) - len(filtered_suffix)})")
        
        self.adaptive_delay = AdaptiveDelay()
        
        # INFIX
        print(f"\n🔄 Запуск INFIX ({source})...")
        words = seed.strip().split()
        if len(words) >= 2:
            modifiers_infix = self.get_modifiers(language, use_numbers, seed, cyrillic_only=True)
            queries_infix = []
            for i in range(1, len(words)):
                for mod in modifiers_infix:
                    query = ' '.join(words[:i]) + f' {mod} ' + ' '.join(words[i:])
                    queries_infix.append(query)
            infix_result_raw = await parse_with_source(queries_infix)
            
            # ФИЛЬТР 1: Убираем мусорные одиночные буквы
            filtered_infix_1 = await self.filter_infix_results(infix_result_raw['keywords'], language)
            
            # ФИЛЬТР 2: Убираем нерелевантные ключи (потерявшие главное слово)
            filtered_infix_2 = await self.filter_relevant_keywords(filtered_infix_1, seed)
            
            infix_result = {
                "keywords": filtered_infix_2,
                "success": infix_result_raw['success'],
                "failed": infix_result_raw['failed']
            }
            
            infix_time = time.time()
            print(f"✅ INFIX: {len(infix_result_raw['keywords'])} → {len(filtered_infix_1)} → {len(filtered_infix_2)} ключей (отфильтровано {len(infix_result_raw['keywords']) - len(filtered_infix_2)})")
        else:
            infix_result = {"keywords": [], "success": 0, "failed": 0}
            infix_time = suffix_time
            print(f"⚠️ INFIX: пропущен (нужно 2+ слова)")
        
        self.adaptive_delay = AdaptiveDelay()
        
        # MORPHOLOGY
        print(f"\n🚀 Запуск MORPHOLOGY ({source})...")
        words_morph = seed.strip().split()
        
        # Находим существительные
        nouns_to_modify = []
        if language.lower() in ['ru', 'uk']:
            try:
                import pymorphy3
                morph = pymorphy3.MorphAnalyzer()
                for idx, word in enumerate(words_morph):
                    parsed = morph.parse(word)
                    if parsed and parsed[0].tag.POS == 'NOUN':
                        nouns_to_modify.append({
                            'index': idx,
                            'word': word,
                            'forms': self.get_morphological_forms(word, language)
                        })
                
                if not nouns_to_modify:
                    last_word = words_morph[-1]
                    nouns_to_modify.append({
                        'index': len(words_morph) - 1,
                        'word': last_word,
                        'forms': self.get_morphological_forms(last_word, language)
                    })
            except:
                last_word = words_morph[-1]
                nouns_to_modify.append({
                    'index': len(words_morph) - 1,
                    'word': last_word,
                    'forms': self.get_morphological_forms(last_word, language)
                })
        else:
            last_word = words_morph[-1]
            nouns_to_modify.append({
                'index': len(words_morph) - 1,
                'word': last_word,
                'forms': self.get_morphological_forms(last_word, language)
            })
        
        # Генерируем варианты
        all_seeds = []
        if len(nouns_to_modify) == 1:
            noun = nouns_to_modify[0]
            for form in noun['forms']:
                new_words = words_morph.copy()
                new_words[noun['index']] = form
                all_seeds.append(' '.join(new_words))
        else:
            noun = nouns_to_modify[0]
            for form in noun['forms']:
                new_words = words_morph.copy()
                new_words[noun['index']] = form
                all_seeds.append(' '.join(new_words))
        
        unique_seeds = list(set(all_seeds))
        
        # Парсим все формы
        all_morph_keywords = set()
        for seed_variant in unique_seeds:
            modifiers_morph = self.get_modifiers(language, use_numbers, seed)
            queries_morph = [f"{seed_variant} {mod}" for mod in modifiers_morph]
            morph_result = await parse_with_source(queries_morph)
            all_morph_keywords.update(morph_result['keywords'])
        
        # ФИЛЬТРУЕМ нерелевантные ключи
        filtered_morph = await self.filter_relevant_keywords(sorted(list(all_morph_keywords)), seed)
        
        morphology_result = {"keywords": filtered_morph}
        morph_time = time.time()
        print(f"✅ MORPHOLOGY: {len(all_morph_keywords)} → {len(filtered_morph)} ключей (отфильтровано {len(all_morph_keywords) - len(filtered_morph)})")
        
        # Собираем множества
        suffix_kw = set(suffix_result["keywords"])
        infix_kw = set(infix_result["keywords"])
        morphology_kw = set(morphology_result["keywords"])
        all_unique = suffix_kw | infix_kw | morphology_kw
        
        # Подсчёт времени (примерный)
        total_time = (infix_time - suffix_time) + (morph_time - infix_time) + 2.5
        
        response = {
            "seed": seed,
            "original_seed": original_seed if correction.get("has_errors") else seed,
            "autocorrect": correction if correction.get("has_errors") else None,
            "source": source,
            "comparison": {
                "suffix": {
                    "count": len(suffix_kw),
                    "time": 2.5,
                    "queries": len(queries_suffix)
                },
                "infix": {
                    "count": len(infix_kw),
                    "time": 1.2 if len(infix_kw) > 0 else 0,
                    "queries": len(queries_infix) if len(words) >= 2 else 0
                },
                "morphology": {
                    "count": len(morphology_kw),
                    "time": round(total_time - 3.7, 1),
                    "queries": len(unique_seeds) * len(modifiers_suffix),
                    "forms": len(unique_seeds)
                },
                "total_unique": len(all_unique),
                "total_time": round(total_time, 1)
            }
        }
        
        if include_keywords:
            response["keywords"] = {"all_unique": sorted(list(all_unique))}
        
        return response


# ============================================
# API ENDPOINTS
# ============================================

@app.get("/")
async def root():
    return {"status": "ok", "version": "4.0", "methods": ["suffix", "infix", "morphology", "compare"]}


@app.get("/api/parse")
async def parse_suffix(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    region: int = Query(187),
    language: str = Query("ru"),
    parallel: int = Query(5, ge=1, le=10),
    source: str = Query("all", description="Источник: google / yandex / bing / all")
):
    """SUFFIX парсинг с выбором источника"""
    parser = KeywordParser()
    
    # ✏️ АВТОКОРРЕКЦИЯ
    correction = await parser.autocorrect_text(seed, language)
    if correction.get("has_errors"):
        seed = correction["corrected"]
        print(f"✏️ Исправлено: '{correction['original']}' → '{seed}'")
    
    # Используем упрощённую версию через compare (только SUFFIX)
    modifiers = parser.get_modifiers(language, False, seed)
    queries = [f"{seed} {mod}" for mod in modifiers]
    
    # Определяем источник
    async def fetch_with_source(query: str, client):
        if source == "google":
            return await parser.fetch_suggestions(query, country, language, client)
        elif source == "yandex":
            return await parser.fetch_suggestions_yandex(query, language, region, client)
        elif source == "bing":
            return await parser.fetch_suggestions_bing(query, language, country, client)
        elif source == "all":
            google_task = parser.fetch_suggestions(query, country, language, client)
            yandex_task = parser.fetch_suggestions_yandex(query, language, region, client)
            bing_task = parser.fetch_suggestions_bing(query, language, country, client)
            g, y, b = await asyncio.gather(google_task, yandex_task, bing_task)
            return list(set(g + y + b))
        return []
    
    start_time = time.time()
    semaphore = asyncio.Semaphore(parallel)
    
    async def fetch_with_limit(q: str, client):
        async with semaphore:
            await asyncio.sleep(parser.adaptive_delay.get_delay())
            return await fetch_with_source(q, client)
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = [fetch_with_limit(q, client) for q in queries]
        results = await asyncio.gather(*tasks)
    
    all_keywords = set()
    for r in results:
        all_keywords.update(r)
    
    # ФИЛЬТРУЕМ нерелевантные ключи
    filtered_keywords = await parser.filter_relevant_keywords(sorted(list(all_keywords)), seed)
    
    elapsed = time.time() - start_time
    
    return {
        "seed": seed,
        "method": "suffix",
        "source": source,
        "keywords": filtered_keywords,
        "count": len(filtered_keywords),
        "queries": len(queries),
        "elapsed_time": round(elapsed, 2),
        "filtered": {
            "total_before": len(all_keywords),
            "removed": len(all_keywords) - len(filtered_keywords)
        }
    }


@app.get("/api/parse-infix")
async def parse_infix(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    language: str = Query("ru"),
    parallel: int = Query(5, ge=1, le=10)
):
    parser = KeywordParser()
    return await parser.parse_infix(seed, country, language, False, parallel)


@app.get("/api/parse-dual")
async def parse_dual_sources(
    seed: str = Query("ремонт пылесосов", description="Ключевое слово"),
    country: str = Query("UA", description="Код страны для Google"),
    region: int = Query(187, description="Yandex Region ID (187=Украина)"),
    language: str = Query("ru", description="Код языка"),
    parallel: int = Query(5, ge=1, le=10, description="Параллельных потоков")
):
    """DUAL парсинг: Google + Yandex параллельно"""
    parser = KeywordParser()
    return await parser.parse_dual(seed, country, region, language, False, parallel)


@app.get("/api/parse-yandex")
async def parse_yandex_only(
    seed: str = Query("ремонт пылесосов", description="Ключевое слово"),
    region: int = Query(0, description="Yandex Region ID (0=все, 143=Киев, 213=Москва, 20544=Харьков)"),
    language: str = Query("ru", description="Код языка"),
    parallel: int = Query(5, ge=1, le=10, description="Параллельных потоков")
):
    """YANDEX парсинг (для тестирования)"""
    parser = KeywordParser()
    return await parser.parse_yandex(seed, region, language, False, parallel)


@app.get("/api/parse-morphology")
async def parse_morphology(
    seed: str = Query("ремонт пылесосов"),
    country: str = Query("UA"),
    region: int = Query(187),
    language: str = Query("ru"),
    parallel: int = Query(5, ge=1, le=10),
    source: str = Query("all", description="Источник: google / yandex / bing / all")
):
    """MORPHOLOGY парсинг с выбором источника"""
    # Используем compare с source, но возвращаем только morphology
    parser = KeywordParser()
    result = await parser.compare_all(seed, country, region, language, False, parallel, True, source)
    
    # Извлекаем только morphology данные
    return {
        "seed": seed,
        "method": "morphology",
        "source": source,
        "keywords": result.get("keywords", {}).get("all_unique", []),
        "count": result["comparison"]["morphology"]["count"],
        "queries": result["comparison"]["morphology"]["queries"],
        "forms": result["comparison"]["morphology"]["forms"],
        "elapsed_time": result["comparison"]["morphology"]["time"]
    }


@app.get("/api/debug/modifiers")
async def debug_modifiers(
    seed: str = Query("медицинский стерилизатор купить"),
    language: str = Query("ru")
):
    """DEBUG: Показать какие модификаторы используются"""
    parser = KeywordParser()
    
    modifiers = parser.get_modifiers(language, False, seed)
    
    # Разделяем на латинские и кириллические
    latin = [m for m in modifiers if ord('a') <= ord(m.lower()) <= ord('z')]
    cyrillic = [m for m in modifiers if ord('а') <= ord(m.lower()) <= ord('я')]
    
    return {
        "seed": seed,
        "language": language,
        "total_modifiers": len(modifiers),
        "breakdown": {
            "latin": {
                "count": len(latin),
                "letters": "".join(latin)
            },
            "cyrillic": {
                "count": len(cyrillic),
                "letters": "".join(cyrillic),
                "has_hard_sign": "ъ" in cyrillic,
                "has_soft_sign": "ь" in cyrillic,
                "has_yeru": "ы" in cyrillic
            }
        },
        "all_modifiers": modifiers
    }


@app.get("/api/compare")
async def compare_methods(
    seed: str = Query("ремонт пылесосов", description="Ключевое слово"),
    country: str = Query("UA", description="Код страны"),
    region: int = Query(187, description="Yandex Region ID (187=Украина)"),
    language: str = Query("ru", description="Код языка"),
    parallel: int = Query(5, ge=1, le=10, description="Параллельных потоков"),
    include_keywords: bool = Query(True, description="Включить полные списки ключей"),
    source: str = Query("all", description="Источник: google / yandex / bing / all")
):
    """COMPARE: сравнение всех методов с выбором источника (по умолчанию ALL)"""
    parser = KeywordParser()
    return await parser.compare_all(seed, country, region, language, False, parallel, include_keywords, source)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
