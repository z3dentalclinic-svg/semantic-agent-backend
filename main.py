"""
Semantic Agent Backend
FastAPI server with Google Ads API integration
Credentials from environment variables

ФИНАЛЬНАЯ ВЕРСИЯ:
- SUFFIX парсинг (a-z + а-я + 0-9) = 65 модификаторов
- INFIX парсинг (только кириллица а-я) = 33 модификатора
- /api/test-parser/single - тестирование одиночных запросов
- /api/test-parser/full - полный парсинг
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import os
import yaml
import httpx
import asyncio
import time
import random

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
            'ru': list("абвгдежзийклмнопрстуфхцчшщэюя"),  # Русский
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
        use_numbers: bool = False
    ) -> List[str]:
        """
        Парсинг с модификаторами (SUFFIX + INFIX для кириллицы)
        
        МЕТОД 1: SUFFIX - "seed модификатор" (все модификаторы)
        МЕТОД 2: INFIX - "слово1 модификатор слово2" (только кириллица, 1-символьный)
        """
        all_keywords = set()
        
        # Получаем модификаторы для выбранного языка
        modifiers = self.get_modifiers(language)
        
        # Если use_numbers=False, убираем цифры из базовых
        if not use_numbers:
            modifiers = [m for m in modifiers if not m.isdigit()]
        
        # Определяем кириллические модификаторы (только языковые символы)
        language_specific = self.language_modifiers.get(language.lower(), [])
        cyrillic_modifiers = [m for m in modifiers if m in language_specific]
        
        # Разбиваем seed на слова для INFIX парсинга
        seed_words = seed.split()
        
        print(f"🌍 Language: {language.upper()} | Modifiers: {len(modifiers)} ({', '.join(modifiers[:10])}...)")
        print(f"📍 INFIX mode: {'ENABLED' if len(cyrillic_modifiers) > 0 and len(seed_words) >= 2 else 'DISABLED'} (cyrillic modifiers: {len(cyrillic_modifiers)})")
        
        for i, modifier in enumerate(modifiers):
            # 1. SUFFIX (прямое) - для ВСЕХ модификаторов
            query = f"{seed} {modifier}"
            suggestions = await self.fetch_suggestions(query, country, language)
            all_keywords.update(suggestions)
            
            suffix_count = len(suggestions)
            
            # 2. INFIX (внутрь) - ТОЛЬКО для кириллицы и если seed >= 2 слов
            infix_count = 0
            if modifier in cyrillic_modifiers and len(seed_words) >= 2:
                # Вставляем модификатор после первого слова
                infix_query = f"{seed_words[0]} {modifier} {' '.join(seed_words[1:])}"
                infix_suggestions = await self.fetch_suggestions(infix_query, country, language)
                all_keywords.update(infix_suggestions)
                infix_count = len(infix_suggestions)
                
                # Дополнительная задержка после INFIX запроса
                await asyncio.sleep(random.uniform(0.3, 0.8))
            
            # Случайная задержка между 0.5 и 2 секунд
            delay = random.uniform(0.5, 2.0)
            
            # Логирование с информацией о INFIX
            if infix_count > 0:
                print(f"[{i+1}/{len(modifiers)}] '{modifier}' → SUFFIX: {suffix_count}, INFIX: {infix_count} (wait {delay:.1f}s)")
            else:
                print(f"[{i+1}/{len(modifiers)}] '{modifier}' → {suffix_count} results (wait {delay:.1f}s)")
            
            await asyncio.sleep(delay)
        
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
    
    return {
        "service": "Semantic Agent API",
        "version": "2.0.0 (INFIX + SUFFIX + SINGLE)",
        "status": "running",
        "credentials_loaded": credentials_loaded,
        "parsing_modes": {
            "suffix": "seed + modifier (all modifiers)",
            "infix": "word1 + modifier + word2 (cyrillic only, 1-char)"
        },
        "endpoints": {
            "health": "/health",
            "locations": "/api/locations/{country_code}",
            "countries": "/api/countries",
            "test_parser_single": "/api/test-parser/single?query={query}&country={country}&language={language}",
            "test_parser_quick": "/api/test-parser/quick?query={query}&country={country}&language={language}",
            "test_parser_full": "/api/test-parser/full?seed={seed}&country={country}&language={language}&use_numbers={bool}"
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "credentials": "loaded" if os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN") else "missing",
        "parser": "enabled (SUFFIX + INFIX)"
    }

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
    use_numbers: bool = True
):
    """
    Полный парсинг с модификаторами (SUFFIX + INFIX)
    
    SUFFIX: seed + модификатор (все модификаторы a-z + а-я + 0-9)
    INFIX: слово1 + модификатор + слово2 (только кириллица а-я)
    
    Пример: GET /api/test-parser/full?seed=ремонт пылесосов&country=UA&language=ru&use_numbers=true
    """
    parser = AutocompleteParser()
    
    # Получаем список модификаторов для информации
    modifiers = parser.get_modifiers(language)
    if not use_numbers:
        modifiers = [m for m in modifiers if not m.isdigit()]
    
    # Считаем INFIX модификаторы (только кириллица для seed с 2+ словами)
    language_specific = parser.language_modifiers.get(language.lower(), [])
    cyrillic_modifiers = [m for m in modifiers if m in language_specific]
    seed_words = seed.split()
    
    # SUFFIX запросов = все модификаторы
    suffix_requests = len(modifiers)
    
    # INFIX запросов = кириллические модификаторы (если seed >= 2 слов)
    infix_requests = len(cyrillic_modifiers) if len(seed_words) >= 2 else 0
    
    # ВСЕГО запросов
    total_requests = suffix_requests + infix_requests
    
    start_time = time.time()
    
    keywords = await parser.parse_with_modifiers(
        seed=seed,
        country=country,
        language=language,
        use_numbers=use_numbers
    )
    
    parsing_time = time.time() - start_time
    
    return {
        "seed": seed,
        "country": country,
        "language": language,
        "modifiers_info": {
            "total_modifiers": len(modifiers),
            "suffix_modifiers": len(modifiers),
            "infix_modifiers": len(cyrillic_modifiers) if len(seed_words) >= 2 else 0,
            "base": "a-z" + (" + 0-9" if use_numbers else ""),
            "language_specific": "".join(language_specific) or "none"
        },
        "requests_info": {
            "suffix_requests": suffix_requests,
            "infix_requests": infix_requests,
            "total_requests": total_requests
        },
        "keywords": keywords,
        "count": len(keywords),
        "requests_made": total_requests,
        "parsing_time": round(parsing_time, 2)
    }


@app.post("/api/test-parser", response_model=ParseResponse)
async def test_parser(request: ParseRequest):
    """
    Полный парсинг с модификаторами (a-z, опционально 0-9)
    
    Пример запроса:
    POST /api/test-parser
    {
        "seed": "vacuum repair",
        "country": "IE",
        "language": "en",
        "use_numbers": false
    }
    """
    parser = AutocompleteParser()
    
    start_time = time.time()
    
    keywords = await parser.parse_with_modifiers(
        seed=request.seed,
        country=request.country,
        language=request.language,
        use_numbers=request.use_numbers
    )
    
    parsing_time = time.time() - start_time
    
    # Получаем модификаторы
    modifiers = parser.get_modifiers(request.language)
    if not request.use_numbers:
        modifiers = [m for m in modifiers if not m.isdigit()]
    
    # Считаем INFIX (кириллица для seed с 2+ словами)
    language_specific = parser.language_modifiers.get(request.language.lower(), [])
    cyrillic_modifiers = [m for m in modifiers if m in language_specific]
    seed_words = request.seed.split()
    
    suffix_requests = len(modifiers)
    infix_requests = len(cyrillic_modifiers) if len(seed_words) >= 2 else 0
    total_requests = suffix_requests + infix_requests
    
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
