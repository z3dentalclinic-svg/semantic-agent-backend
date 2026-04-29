"""
Извлечение хвостов: вычитание токенов сида (по леммам) из ключей.
Хвосты сохраняют оригинальные формы. Дедупликация payload → keywords.
"""
import re
from collections import defaultdict
from typing import Iterable

import pymorphy3

# Singleton морфоанализатора. На проде заменить на shared_morph если есть.
_morph = pymorphy3.MorphAnalyzer()

_TOKEN_RE = re.compile(r'\w+', re.UNICODE)


def _lemma(token: str) -> str:
    return _morph.parse(token)[0].normal_form


def get_seed_lemmas(seed: str) -> frozenset[str]:
    tokens = _TOKEN_RE.findall(seed.lower())
    return frozenset(_lemma(t) for t in tokens)


def extract_payload(keyword: str, seed_lemmas: frozenset[str]) -> str:
    out = []
    for t in _TOKEN_RE.findall(keyword.lower()):
        if _lemma(t) in seed_lemmas:
            continue
        out.append(t)
    return ' '.join(out)


def build_payload_mapping(
    keywords: Iterable[str],
    seed: str,
) -> tuple[list[str], dict[str, list[str]]]:
    """
    Возвращает:
      unique_payloads — список уникальных хвостов (порядок сохранён, 0-based)
      payload_to_keywords — {payload: [keyword, ...]}
    """
    seed_lemmas = get_seed_lemmas(seed)
    payload_to_keywords: dict[str, list[str]] = defaultdict(list)
    for kw in keywords:
        p = extract_payload(kw, seed_lemmas)
        payload_to_keywords[p].append(kw)
    unique_payloads = list(payload_to_keywords.keys())
    return unique_payloads, dict(payload_to_keywords)


def estimate_tokens(text: str) -> int:
    """Грубая оценка числа токенов для русского/смешанного текста."""
    cyrillic = sum(1 for c in text if 'а' <= c.lower() <= 'я' or c.lower() == 'ё')
    other = len(text) - cyrillic
    return int(cyrillic * 0.55 + other * 0.27) + 1
