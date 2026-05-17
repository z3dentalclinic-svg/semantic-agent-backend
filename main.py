"""
FGS1 Parser API - Full Fixed Version
"""
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from typing import List, Dict
import httpx
import asyncio
import time
import random
import re
import logging
from difflib import SequenceMatcher

from filters import (
    BatchPostFilter, 
    DISTRICTS_EXTENDED,
    filter_infix_results,
    filter_relevant_keywords
)
from geo import generate_geo_blacklist_full
from config import USER_AGENTS, WHITELIST_TOKENS, MANUAL_RARE_CITIES, FORBIDDEN_GEO
from utils.normalizer import normalize_keywords

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

import nltk
from nltk.stem import SnowballStemmer

try:
    from natasha import Segmenter, MorphVocab, NewsEmbedding, NewsNERTagger, Doc
    NATASHA_AVAILABLE = True
except ImportError:
    NATASHA_AVAILABLE = False

try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
except Exception as e:
    pass

import pymorphy3

# --- Класс парсера (основная логика) ---
class GoogleAutocompleteParser:
    def __init__(self):
        self.post_filter = BatchPostFilter()
        self.morph = pymorphy3.MorphAnalyzer()
    
    # ... (Здесь подразумеваются твои внутренние методы parse_suffix, parse_infix и т.д.)
    # Для краткости они опущены, так как они у тебя уже есть в классе.
    # Если ты заменяешь файл, убедись, что методы класса остались на месте.

parser = GoogleAutocompleteParser()
app = FastAPI(title="FGS Parser API", version="7.9.5")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Умная нормализация для сохранения уникальности ---
def finalize_results(result: Dict, seed: str, language: str):
    if result.get("keywords") and len(result["keywords"]) > 0:
        try:
            # 1. Приводим к падежам сида
            normalized = normalize_keywords(
                keywords=result["keywords"],
                language=language,
                seed=seed
            )
            # 2. Дедупликация (сохраняем разные города)
            seen = set()
            final = []
            for kw in normalized:
                low = kw.lower().strip()
                if low not in seen:
                    final.append(kw)
                    seen.add(low)
            
            result["keywords"] = final
            # Обновляем все счетчики
            for k in ["count", "total_count", "total_unique_keywords"]:
                if k in result: result[k] = len(final)
        except Exception as e:
            logger.error(f"Finalize error: {e}")
    return result

# --- ЭНДПОИНТЫ ---

@app.get("/")
async def root():
    return FileResponse('static/index.html')

@app.get("/api/light-search")
async def light_search_endpoint(
    seed: str = Query(...), country: str = "ua", region_id: int = 143,
    language: str = "auto", use_numbers: bool = False, parallel_limit: int = 10, source: str = "google"
):
    if language == "auto": language = "ru" # упрощенный детект
    result = await parser.parse_light_search(seed, country, language, use_numbers, parallel_limit, source, region_id)
    return finalize_results(result, seed, language)

@app.get("/api/deep-search")
async def deep_search_endpoint(
    seed: str = Query(...), country: str = "ua", region_id: int = 143,
    language: str = "auto", use_numbers: bool = False, parallel_limit: int = 10, include_keywords: bool = True
):
    if language == "auto": language = "ru"
    result = await parser.parse_deep_search(seed, country, region_id, language, use_numbers, parallel_limit, include_keywords)
    seed_to_use = result.get("corrected_seed", seed)
    return finalize_results(result, seed_to_use, language)

@app.get("/api/parse/suffix")
async def parse_suffix_endpoint(seed: str = Query(...), country: str = "ua", language: str = "auto", source: str = "google"):
    result = await parser.parse_suffix(seed, country, language, False, 10, source, 0)
    return finalize_results(result, seed, language)

@app.get("/api/parse/infix")
async def parse_infix_endpoint(seed: str = Query(...), country: str = "ua", language: str = "auto", source: str = "google"):
    result = await parser.parse_infix(seed, country, language, False, 10, source, 0)
    return finalize_results(result, seed, language)

@app.get("/api/parse/morphology")
async def parse_morphology_endpoint(seed: str = Query(...), country: str = "ua", language: str = "auto", source: str = "google"):
    result = await parser.parse_morphology(seed, country, language, False, 10, source, 0)
    return finalize_results(result, seed, language)

@app.get("/api/parse/adaptive-prefix")
async def parse_adaptive_prefix_endpoint(seed: str = Query(...), country: str = "ua", language: str = "auto", source: str = "google"):
    result = await parser.parse_adaptive_prefix(seed, country, language, False, 10, source, 0)
    return finalize_results(result, seed, language)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
