from typing import List
import re
from difflib import SequenceMatcher
from functools import lru_cache
from typing import Iterable, List

import pymorphy3
from nltk.stem.snowball import SnowballStemmer

def simple_normalize_keyword(keyword: str, seed: str) -> str:
    if not keyword or not seed:
        return keyword
    
    seed_tokens = seed.lower().split()
    kw_tokens = keyword.lower().split()
    
    replacements = {}
    
    for s in seed_tokens:
        for k in kw_tokens:
            if k == s:
                replacements[k] = s
                continue
            
            prefix_len = 5
            if len(k) >= prefix_len and len(s) >= prefix_len and k[:prefix_len] == s[:prefix_len]:
                replacements[k] = s
                continue
            
            sim = SequenceMatcher(None, k, s).ratio()
            if sim >= 0.7:
                replacements[k] = s
    
    result = []
    
    for token in keyword.split():
        low = token.lower()
        if low in replacements:
            result.append(replacements[low])
        else:
            result.append(token)
    
    return " ".join(result)
WORD_RE = re.compile(r"[\w-]+", re.UNICODE)
CYRILLIC_RE = re.compile(r"[а-яА-ЯёЁіїєґІЇЄҐ]")
UK_SPECIFIC_RE = re.compile(r"[іїєґІЇЄҐ]")

# Один анализатор на процесс.
_MORPH = pymorphy3.MorphAnalyzer()

# Языковые адаптеры Snowball (без stop-слов и без хардкода словарей).
_SNOWBALL_LANGUAGE_MAP = {
    "en": "english",
    "de": "german",
    "fr": "french",
    "es": "spanish",
    "it": "italian",
    "pt": "portuguese",
    "nl": "dutch",
    "sv": "swedish",
    "no": "norwegian",
    "da": "danish",
    "fi": "finnish",
    "ro": "romanian",
    "ru": "russian",
}


@lru_cache(maxsize=32)
def _get_snowball_stemmer(language: str):
    name = _SNOWBALL_LANGUAGE_MAP.get(language.lower())
    if not name:
        return None
    return SnowballStemmer(name)


def _tokenize(text: str) -> list[str]:
    return WORD_RE.findall(text or "")


def _looks_like_brand_or_model(token: str) -> bool:
    has_digit = any(ch.isdigit() for ch in token)
    has_latin = any("a" <= ch.lower() <= "z" for ch in token)
    has_cyr = bool(CYRILLIC_RE.search(token))
    has_hyphen = "-" in token

    # Бренд/модель: цифры, смешение скриптов, модели через дефис.
    if has_digit:
        return True
    if has_latin and has_cyr:
        return True
    if has_hyphen and (has_latin or has_digit):
        return True
    return False


def _resolve_language(language: str, seed: str, keywords: List[str]) -> str:
    if language and language.lower() != "auto":
        return language.lower()

    text = " ".join([seed or ""] + (keywords or []))
    if UK_SPECIFIC_RE.search(text):
        return "uk"
    if CYRILLIC_RE.search(text):
        return "ru"
    return "en"


@lru_cache(maxsize=100_000)
def _normalize_token_ru_uk(token: str) -> str:
    """
    RU/UK: сохраняем морфологическую дивергенцию числа у существительных.
    """
    if not token:
        return token

    if _looks_like_brand_or_model(token):
        return token.lower()

    parses = _MORPH.parse(token)
    if not parses:
        return token.lower()

    best = parses[0]
    pos = best.tag.POS
    number = best.tag.number

    if pos == "NOUN":
        if number == "plur":
            inflected = best.inflect({"nomn", "plur"})
            if inflected:
                return inflected.word
        if number == "sing":
            inflected = best.inflect({"nomn", "sing"})
            if inflected:
                return inflected.word

    if pos in {"ADJF", "ADJS", "PRTF", "PRTS"} and number in {"sing", "plur"}:
        inflected = best.inflect({"nomn", number})
        if inflected:
            return inflected.word

    return best.normal_form


@lru_cache(maxsize=100_000)
def _normalize_token_stemmed(token: str, language: str) -> str:
    if not token:
        return token
    if _looks_like_brand_or_model(token):
        return token.lower()

    stemmer = _get_snowball_stemmer(language)
    lowered = token.lower()
    if not stemmer:
        return lowered
    return stemmer.stem(lowered)


def _should_align_to_seed(seed_token: str, token: str) -> bool:
    if not seed_token or not token:
        return False
    if seed_token == token:
        return True

    # Только близкие формы (опечатки), без агрессивного схлопывания.
    sim = SequenceMatcher(None, seed_token, token).ratio()
    return sim >= 0.97


def _align_with_seed(tokens: Iterable[str], seed: str) -> list[str]:
    seed_tokens = [s.lower() for s in _tokenize(seed)]
    if not seed_tokens:
        return list(tokens)

    aligned: list[str] = []
    for token in tokens:
        replacement = token
        for seed_token in seed_tokens:
            if _should_align_to_seed(seed_token, token):
                replacement = seed_token
                break
        aligned.append(replacement)
    return aligned


def _normalize_phrase(keyword: str, language: str, seed: str) -> str:
    tokens = [t.lower() for t in _tokenize(keyword)]

    if language in {"ru", "uk"}:
        normalized = [_normalize_token_ru_uk(t) for t in tokens]
    else:
        normalized = [_normalize_token_stemmed(t, language) for t in tokens]

    aligned = _align_with_seed(normalized, seed)
    return " ".join(aligned)


def normalize_keywords(keywords: List[str], language: str, seed: str) -> List[str]:
    return [simple_normalize_keyword(kw, seed) for kw in keywords]
    """
    Контракт совместим с текущим main.py.

    - RU/UK: морфология через pymorphy3 + сохранение sing/plur у NOUN.
    - Остальные языки: логическая нормализация через Snowball stemming.
    - Без stop-word списков и без обязательной зависимости от Natasha.
    """
    if not keywords:
        return []

    lang = _resolve_language(language=language, seed=seed, keywords=keywords)
    return [_normalize_phrase(kw, language=lang, seed=seed) for kw in keywords]
