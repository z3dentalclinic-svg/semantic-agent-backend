import asyncio
import logging
import random
import re
import time
from typing import List, Dict, Set, Optional
import httpx
import pymorphy3
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

# --- CONFIG ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FGS_PARSER")

# База городов и брендов (Whitelist)
WHITELIST_TOKENS = {"филипс", "philips", "самсунг", "samsung", "бош", "bosch", "lg", "dyson", "желтые воды"}
GEO_BLACKLIST = {
    "ua": {"москва", "спб", "санкт-петербург", "минск", "новосибирск", "екатеринбург", "казань", "ростов"},
    "ru": {"киев", "харьков", "днепр", "одесса", "львов", "запорожье", "винница", "кривой рог"}
}

class GoogleAutocompleteParser:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]

    # --- ФИЛЬТРЫ v5.3.0 ---
    def is_query_safe_pre(self, query: str, country: str) -> bool:
        q_lower = query.lower()
        if any(w in q_lower for w in WHITELIST_TOKENS): return True
        
        words = re.findall(r'\b\w+\b', q_lower)
        blacklist = GEO_BLACKLIST.get(country.lower(), set())
        
        for w in words:
            lemma = self.morph.parse(w)[0].normal_form
            if (w in blacklist or lemma in blacklist) and lemma not in WHITELIST_TOKENS:
                logger.warning(f"🚫 [PRE] Blocked: '{query}' (Found: {lemma})")
                return False
        return True

    async def filter_results_post(self, keywords: List[str], country: str) -> List[str]:
        clean = []
        blacklist = GEO_BLACKLIST.get(country.lower(), set())
        for kw in keywords:
            kw_l = kw.lower()
            if any(w in kw_l for w in WHITELIST_TOKENS):
                clean.append(kw)
                continue
            if any(city in kw_l for city in blacklist):
                logger.info(f"⚠️ [POST] Cleaned: '{kw}'")
                continue
            clean.append(kw)
        return clean

    # --- СБОР ДАННЫХ ---
    async def fetch(self, query: str, lang: str, country: str, client: httpx.AsyncClient) -> List[str]:
        url = "https://www.google.com/complete/search"
        params = {"q": query, "client": "firefox", "hl": lang, "gl": country}
        try:
            resp = await client.get(url, params=params, timeout=5)
            return resp.json()[1] if resp.status_code == 200 else []
        except: return []

    async def parse_adaptive_prefix(self, seed: str, country: str, lang: str, use_numbers: bool, limit: int):
        start_time = time.time()
        
        # Генерация расширенного пула запросов (чтобы вернуть объем ключей)
        prefixes = ["", "купить ", "цена ", "отзывы ", "ремонт ", "в "]
        alphabet = "абвгдежзийклмнопрстуфхцчшщэюя"
        if lang == "en": alphabet = "abcdefghijklmnopqrstuvwxyz"
        
        queries = [f"{p}{seed}".strip() for p in prefixes]
        for char in alphabet:
            queries.append(f"{seed} {char}")
        if use_numbers:
            for n in range(10): queries.append(f"{seed} {n}")

        results = set()
        semaphore = asyncio.Semaphore(limit)

        async def worker(q, client):
            async with semaphore:
                if self.is_query_safe_pre(q, country):
                    data = await self.fetch(q, lang, country, client)
                    results.update(data)

        async with httpx.AsyncClient(headers={"User-Agent": random.choice(self.user_agents)}) as client:
            await asyncio.gather(*[worker(q, client) for q in queries])

        # Фильтрация
        final_list = await self.filter_results_post(list(results), country)
        
        # Возвращаем полный объект для фронтенда
        return {
            "seed": seed,
            "keywords": sorted(final_list),
            "count": len(final_list),
            "time": round(time.time() - start_time, 2),
            "method": "adaptive-prefix",
            "source": "google"
        }

# --- API ---
app = FastAPI()
parser = GoogleAutocompleteParser()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/parse/adaptive-prefix")
async def api_adaptive_prefix(
    seed: str = Query(...), 
    country: str = "ua", 
    lang: str = "ru", 
    use_numbers: bool = False,
    parallel_limit: int = 10
):
    return await parser.parse_adaptive_prefix(seed, country, lang, use_numbers, parallel_limit)

@app.get("/")
def root():
    return FileResponse('static/index.html')
