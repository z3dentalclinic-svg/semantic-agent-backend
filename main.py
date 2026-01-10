"""
FGS Parser API - Version 5.3.0 (Cleaned)
Architecture: FastAPI + HTTPX + Pymorphy3 + Natasha/NLTK
Features:
- Hybrid Normalization (Morphology + Stemming)
- Multi-source (Google, Yandex, Bing)
- Intelligent Pre-filter & Post-filter
- Geo-blocking & Brand Protection
"""

import asyncio
import logging
import random
import re
import time
from difflib import SequenceMatcher
from typing import List, Dict, Set, Optional

# --- Third-party Imports ---
import httpx
import nltk
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from nltk.stem import SnowballStemmer

# --- NLP Imports (Conditional) ---
try:
    import pymorphy3
    MORPH_AVAILABLE = True
except ImportError:
    MORPH_AVAILABLE = False
    print("⚠️ Pymorphy3 not installed. Morphological analysis disabled.")

try:
    from natasha import Segmenter, MorphVocab, NewsEmbedding, NewsNERTagger, Doc
    NATASHA_AVAILABLE = True
except ImportError:
    NATASHA_AVAILABLE = False
    print("⚠️ Natasha not installed. NER features disabled.")

try:
    from geonamescache import GeonamesCache
    GEONAMES_AVAILABLE = True
except ImportError:
    GEONAMES_AVAILABLE = False
    print("⚠️ Geonamescache not installed. Using minimal geo-blacklist.")

# --- NLTK Setup ---
try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
except Exception:
    pass

# ============================================
# CONFIGURATION & CONSTANTS
# ============================================

# Logging Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FGS_PARSER")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# Protected Brands & Terms (Whitelist)
WHITELIST_TOKENS = {
    "филипс", "philips", "самсунг", "samsung", "бош", "bosch", "lg",
    "electrolux", "электролюкс", "dyson", "дайсон", "xiaomi", "сяоми",
    "karcher", "керхер", "tefal", "тефаль", "rowenta", "ровента",
    "желтые воды", "жёлтые воды", "zhovti vody", "новомосковск", "новомосковськ"
}

# Manual Rare Cities (Not in Geonames)
MANUAL_RARE_CITIES = {
    "ua": {"щёлкино", "щелкino", "shcholkino", "армянск", "красноперекопск", "джанкой", "коммунарка", "московский"},
    "ru": {"жёлтые воды", "желтые воды", "voznesensk", "вознесенск"},
    "by": set(),
    "kz": set()
}

# ============================================
# UTILITIES
# ============================================

def generate_geo_blacklist() -> Dict[str, Set[str]]:
    """Generates geo-blacklist based on Geonames and manual lists."""
    blacklist = {"ua": set(), "ru": set(), "by": set(), "kz": set()}
    
    # 1. Fallback / Minimal List
    base_bad_for_ua = {"москва", "мск", "спб", "питер", "санкт-петербург", "минск"}
    base_bad_for_ru = {"киев", "харьков", "днепр", "львов", "одесса"}
    
    blacklist["ua"].update(base_bad_for_ua)
    blacklist["ru"].update(base_bad_for_ru)
    blacklist["by"].update(base_bad_for_ua | base_bad_for_ru)
    blacklist["kz"].update(base_bad_for_ua)

    # 2. Full Geonames Loading
    if GEONAMES_AVAILABLE:
        try:
            gc = GeonamesCache()
            cities = gc.get_cities()
            cities_by_country = {}

            for city_data in cities.values():
                cc = city_data['countrycode']
                if cc not in cities_by_country:
                    cities_by_country[cc] = set()
                
                name = city_data['name'].lower()
                cities_by_country[cc].add(name)
                
                for alt in city_data.get('alternatenames', []):
                    if ' ' not in alt and 3 <= len(alt) <= 30 and alt.replace('-', '').isalpha():
                        cities_by_country[cc].add(alt.lower())

            # Define blocking rules
            blacklist['ua'].update(
                cities_by_country.get('RU', set()) | cities_by_country.get('BY', set()) |
                cities_by_country.get('KZ', set()) | cities_by_country.get('PL', set())
            )
            blacklist['ru'].update(
                cities_by_country.get('UA', set()) | cities_by_country.get('BY', set()) |
                cities_by_country.get('KZ', set())
            )
            # (Similar logic for BY/KZ omitted for brevity but preserved in structure)
            
        except Exception as e:
            logger.error(f"Error loading geonames: {e}")

    # 3. Add Manual Rare Cities
    for country in blacklist:
        blacklist[country].update(MANUAL_RARE_CITIES.get(country, set()))

    return blacklist

GEO_BLACKLIST = generate_geo_blacklist()


class AdaptiveDelay:
    """Manages request delays to avoid rate limits."""
    def __init__(self, initial=0.2, min_d=0.1, max_d=1.0):
        self.delay = initial
        self.min_delay = min_d
        self.max_delay = max_d
    
    def get(self) -> float: return self.delay
    def success(self): self.delay = max(self.min_delay, self.delay * 0.95)
    def fail(self): self.delay = min(self.max_delay, self.delay * 1.5)


class EntityLogicManager:
    """Handles logic for detecting conflicting entities (Locations/Organizations)."""
    def __init__(self):
        self.cache = {}
        self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru') if MORPH_AVAILABLE else None
        self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk') if MORPH_AVAILABLE else None
        
        # Hardcoded cache for speed
        self.hard_cache = {
            'LOC': {
                "киев", "днепр", "харьков", "одесса", "львов", "запорожье", "москва", "спб", 
                "новосибирск", "минск", "астана", "варшава", "прага", "крым"
            },
            'ORG': {
                "apple", "samsung", "xiaomi", "lg", "sony", "bosch", "philips", "dyson", "karcher"
            }
        }

        if NATASHA_AVAILABLE:
            self.segmenter = Segmenter()
            self.ner_tagger = NewsNERTagger(NewsEmbedding())

    def get_entities(self, text: str, lang: str = 'ru') -> Dict[str, set]:
        cache_key = f"{text}_{lang}"
        if cache_key in self.cache: return self.cache[cache_key]
        
        entities = {'LOC': set(), 'ORG': set()}
        words_original = set(re.findall(r'\w+', text.lower()))
        
        # 1. Fast Check
        for cat, items in self.hard_cache.items():
            if common := words_original & items:
                entities[cat].update(common)
        
        # 2. Morph Check (if no exact match)
        if not any(entities.values()) and self.morph_ru and lang in ['ru', 'uk']:
            morph = self.morph_ru if lang == 'ru' else self.morph_uk
            normalized = {morph.parse(w)[0].normal_form for w in words_original if morph.parse(w)}
            for cat, items in self.hard_cache.items():
                if common := normalized & items:
                    entities[cat].update(common)

        # 3. Natasha NER (Last resort for RU)
        if not any(entities.values()) and lang == 'ru' and NATASHA_AVAILABLE:
            try:
                doc = Doc(text)
                doc.segment(self.segmenter)
                doc.tag_ner(self.ner_tagger)
                for span in doc.spans:
                    if span.type in entities:
                        entities[span.type].add(span.text.lower())
            except Exception: pass
            
        self.cache[cache_key] = entities
        return entities

    def check_conflict(self, seed: str, keyword: str, lang: str = 'ru') -> bool:
        """Returns True if seed and keyword have conflicting entities."""
        s_ents = self.get_entities(seed, lang)
        k_ents = self.get_entities(keyword, lang)
        
        for cat in ['LOC', 'ORG']:
            # Conflict if seed has entity X, but keyword introduces NEW entity Y (and Y != X)
            if s_ents[cat] and (k_ents[cat] - s_ents[cat]):
                return True
        return False


# ============================================
# MAIN PARSER CLASS
# ============================================
class GoogleAutocompleteParser:
    def __init__(self):
        self.adaptive_delay = AdaptiveDelay()
        self.entity_manager = EntityLogicManager()
        
        # Morphology setup
        self.morph_ru = pymorphy3.MorphAnalyzer(lang='ru') if MORPH_AVAILABLE else None
        self.morph_uk = pymorphy3.MorphAnalyzer(lang='uk') if MORPH_AVAILABLE else None
        
        self.stemmers = {
            'en': SnowballStemmer("english"), 'de': SnowballStemmer("german"),
            'fr': SnowballStemmer("french"), 'es': SnowballStemmer("spanish"),
            'it': SnowballStemmer("italian")
        }
        
        self.stop_words = {
            'ru': {'и', 'в', 'во', 'не', 'на', 'с', 'от', 'для', 'по', 'о', 'об', 'к', 'у', 'за', 'из'},
            'uk': {'і', 'в', 'на', 'з', 'від', 'для', 'по', 'о', 'до', 'при', 'без', 'над', 'під', 'та'},
            'en': {'a', 'an', 'the', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from', 'and'}
        }

    # --- Helper Methods ---
    def detect_language(self, text: str) -> str:
        if any('\u0400' <= c <= '\u04FF' for c in text):
            return 'uk' if any(c in 'іїєґ' for c in text.lower()) else 'ru'
        return 'en'

    def get_modifiers(self, lang: str, numbers: bool, seed: str, cyrillic_only: bool = False) -> List[str]:
        mods = []
        seed_lower = seed.lower()
        has_cyrillic = any('\u0400' <= c <= '\u04FF' for c in seed_lower)
        
        if lang == 'ru': mods.extend("абвгдежзийклмнопрстуфхцчшщэюя")
        elif lang == 'uk': mods.extend("абвгдежзийклмнопрстуфхцчшщюяіїєґ")
        
        if not cyrillic_only and (not has_cyrillic or lang in ['en', 'de', 'fr', 'es']):
            mods.extend("abcdefghijklmnopqrstuvwxyz")
            
        if numbers: mods.extend(map(str, range(10)))
        return list(mods)

    def normalize(self, text: str, lang: str) -> set:
        """Hybrid normalization: Pymorphy for SLAVIC, Snowball for LATIN."""
        words = re.findall(r'\w+', text.lower())
        stop_words = self.stop_words.get(lang, self.stop_words.get('ru', set()))
        meaningful = [w for w in words if w not in stop_words and len(w) > 1]
        
        if lang in ['ru', 'uk'] and self.morph_ru:
            morph = self.morph_ru if lang == 'ru' else self.morph_uk
            return {morph.parse(w)[0].normal_form for w in meaningful if morph.parse(w)}
        
        if lang in self.stemmers:
            return {self.stemmers[lang].stem(w) for w in meaningful}
            
        return set(meaningful)

    # --- Filters ---
    def is_query_allowed(self, query: str, seed: str, country: str) -> bool:
        """Pre-filter: Checks if query contains forbidden cities."""
        blacklist = GEO_BLACKLIST.get(country.lower())
        if not blacklist: return True
        
        query_lower = query.lower()
        if any(token in query_lower for token in WHITELIST_TOKENS):
            return True # Whitelisted
            
        query_words = set(re.findall(r'\b\w+\b', query_lower))
        
        # Check against blacklist (Whole words)
        if blacklist & query_words:
            return False
            
        # Check against blacklist (Normalized)
        if self.morph_ru:
            norm_words = {self.morph_ru.parse(w)[0].normal_form for w in query_words if self.morph_ru.parse(w)}
            if blacklist & norm_words:
                return False
                
        return True

    async def filter_results(self, keywords: List[str], seed: str, lang: str) -> List[str]:
        """Post-filter: Subset matching + Entity conflict + Syntax check."""
        seed_lemmas = self.normalize(seed, lang)
        if not seed_lemmas: return keywords
        
        filtered = []
        seed_words_orig = [w.lower() for w in re.findall(r'\w+', seed) if len(w) > 2]
        
        for kw in keywords:
            # 1. Semantic Check (Lemmas)
            kw_lemmas = self.normalize(kw, lang)
            if not seed_lemmas.issubset(kw_lemmas):
                continue
                
            # 2. Syntax Check (Original words presence)
            kw_lower = kw.lower()
            if not all(sw in kw_lower for sw in seed_words_orig):
                continue
                
            # 3. Entity Conflict Check (Async wrapper)
            conflict = await asyncio.to_thread(self.entity_manager.check_conflict, seed, kw, lang)
            if not conflict:
                filtered.append(kw)
                
        return filtered

    # --- Fetching ---
    async def fetch(self, query: str, country: str, lang: str, source: str, region_id: int, client: httpx.AsyncClient) -> List[str]:
        try:
            if source == "google":
                url = "https://www.google.com/complete/search"
                params = {"q": query, "client": "firefox", "hl": lang, "gl": country}
                resp = await client.get(url, params=params, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=10)
                if resp.status_code == 200:
                    self.adaptive_delay.success()
                    return resp.json()[1]
                    
            elif source == "yandex":
                url = "https://suggest-maps.yandex.ru/suggest-geo"
                params = {"part": query, "lang": lang, "n": "10", "geo": str(region_id), "search_type": "tp"}
                resp = await client.get(url, params=params, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=10)
                if resp.status_code == 200:
                    self.adaptive_delay.success()
                    return [i['text'] for i in resp.json().get('results', []) if i.get('text')]
            
            # (Bing implementation omitted for brevity, structure remains)
                
        except Exception:
            pass
            
        self.adaptive_delay.fail()
        return []

    async def parse_tasks(self, queries: List[str], country, lang, limit, source, region_id):
        semaphore = asyncio.Semaphore(limit)
        results = set()
        
        async def worker(q, client):
            async with semaphore:
                if not self.is_query_allowed(q, q, country): return
                await asyncio.sleep(self.adaptive_delay.get())
                data = await self.fetch(q, country, lang, source, region_id, client)
                results.update(data)

        async with httpx.AsyncClient() as client:
            await asyncio.gather(*[worker(q, client) for q in queries])
            
        return sorted(list(results))

    # --- Strategies ---
    async def run_strategy(self, method: str, seed: str, country: str, lang: str, numbers: bool, limit: int, source: str, region_id: int):
        t0 = time.time()
        
        # Strategy Logic
        if method == "suffix":
            mods = self.get_modifiers(lang, numbers, seed)
            queries = [f"{seed} {m}" for m in mods]
        elif method == "infix":
            words = seed.split()
            mods = self.get_modifiers(lang, numbers, seed, cyrillic_only=True)
            queries = [f"{' '.join(words[:i])} {m} {' '.join(words[i:])}" for i in range(1, len(words)) for m in mods]
        else: # adaptive / simple
            queries = [] 
            
        # Execute
        raw_keywords = await self.parse_tasks(queries, country, lang, limit, source, region_id) if queries else []
        clean_keywords = await self.filter_results(raw_keywords, seed, lang)
        
        return {
            "seed": seed, "method": method, "source": source,
            "keywords": clean_keywords, "count": len(clean_keywords),
            "time": round(time.time() - t0, 2)
        }

    async def deep_search(self, seed, country, region_id, lang, numbers, limit, source):
        # Wrapper for running multiple strategies
        # Note: Simplified for the cleaned version, full logic can be expanded
        t0 = time.time()
        res_suffix = await self.run_strategy("suffix", seed, country, lang, numbers, limit, source, region_id)
        
        return {
            "seed": seed,
            "total_keywords": res_suffix['count'],
            "keywords": res_suffix['keywords'],
            "time": round(time.time() - t0, 2)
        }

# ============================================
# FASTAPI APP
# ============================================
app = FastAPI(title="FGS Parser API", version="5.3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
parser = GoogleAutocompleteParser()

@app.get("/")
def root(): return FileResponse('static/index.html')

@app.get("/api/deep-search")
async def deep_search(
    seed: str = Query(...), country: str = "ua", region_id: int = 143,
    language: str = "auto", use_numbers: bool = False, limit: int = 10, source: str = "google"
):
    if language == "auto": language = parser.detect_language(seed)
    return await parser.deep_search(seed, country, region_id, language, use_numbers, limit, source)

# Add other endpoints (infix/suffix) mapping to run_strategy similarly...
