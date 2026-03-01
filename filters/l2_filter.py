"""
L2 Filter — Dual-Signal Classifier (PMI + KNN + L0 signals)

Слой 2 фильтрации: обрабатывает GREY хвосты из L0.

Сигналы:
1. PMI (Pointwise Mutual Information) — частотность слов хвоста в батче.
   Слова из валидных хвостов встречаются чаще. "Купить", "гелевый", "50 кубов"
   появляются в десятках ключей. "Жиклеры", "жало" — единичные.

2. KNN similarity — top-k mean cosine к L0 VALID хвостам.
   "Похож ли хвост на что-то КОНКРЕТНОЕ из валидных?"
   "гелевый" → top-3 mean к "литиевый","электрический","тяговый" ≈ 0.7+ → VALID.
   "глушитель" → top-3 mean ≈ 0.25 → TRASH.
   
   [ЗАКОММЕНТИРОВАНО] Centroid distance — код остался, можно раскомментировать.

3. L0 negative signals — структурные red flags от L0.
   orphan_genitive, single_infinitive, incoherent_tail и т.д.
   Учитываем ТОЛЬКО если нет позитивных сигналов ("pure negative").

Решение:
- PMI ≥ порог И нет L0 pure-neg → VALID
- PMI ≥ порог НО L0 pure-neg → конфликт → GREY (→ L3)
- PMI < порог, KNN ≥ порог, нет L0 pure-neg → VALID
- L0 pure-neg + KNN < порог → TRASH
- PMI low + KNN low → TRASH
- Остальное → GREY (→ L3 LLM)

Результат:
- VALID → добавляется к keywords
- TRASH → добавляется к anchors
- GREY → остаётся для L3 (DeepSeek API, ≤5% от входа)
"""

import os
import json
import math
import re
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from collections import Counter

import numpy as np
import pymorphy3

from .shared_model import get_embedding_model

# Singleton morph analyzer (0 MB extra — pymorphy3 already in L0)
_morph_analyzer = None

def _get_morph():
    global _morph_analyzer
    if _morph_analyzer is None:
        _morph_analyzer = pymorphy3.MorphAnalyzer()
    return _morph_analyzer

logger = logging.getLogger(__name__)


@dataclass
class L2Config:
    """Конфигурация L2 классификатора."""
    
    embedding_model: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    
    # PMI порог: выше → автоматически VALID (если нет L0 pure-neg)
    pmi_valid_threshold: float = 2.5
    
    # KNN: top-k mean cosine similarity к L0 VALID хвостам
    knn_k: int = 3
    # KNN score: выше → VALID (для PMI < pmi_valid)
    knn_valid_threshold: float = 0.70
    # KNN score: ниже → TRASH
    knn_trash_threshold: float = 0.35
    
    # --- CENTROID (закомментировано, раскомментировать для сравнения) ---
    # centroid_valid_threshold: float = 0.65
    # centroid_trash_threshold: float = 0.50
    
    cache_file: str = "l2_cache.json"


class L2Classifier:
    """
    Слой 2: PMI + KNN классификатор.
    PMI + KNN Similarity + L0 Signals.
    """
    
    def __init__(self, config: Optional[L2Config] = None):
        self.config = config or L2Config()
        self._embedder = None
        self._cache: Dict[str, dict] = {}
        self._load_cache()
    
    def _load_cache(self):
        if os.path.exists(self.config.cache_file):
            try:
                with open(self.config.cache_file, 'r', encoding='utf-8') as f:
                    self._cache = json.load(f)
                logger.info(f"Loaded {len(self._cache)} cached entries")
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
    
    def _save_cache(self):
        try:
            with open(self.config.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")
    
    @property
    def embedder(self):
        """Lazy load fastembed model via shared singleton."""
        if self._embedder is None:
            self._embedder = get_embedding_model()
            if self._embedder is None:
                raise RuntimeError("L2: embedding model failed to load")
        return self._embedder
    
    @property
    def is_available(self) -> bool:
        try:
            _ = self.embedder
            return True
        except Exception:
            return False
    
    # =========================================================
    # SIGNAL 1: PMI (Batch Word Frequency)
    # =========================================================
    
    def _compute_word_df(self, all_tails: List[str]) -> Counter:
        """
        Document frequency: в скольких хвостах встречается каждое слово.
        """
        word_df = Counter()
        for tail in all_tails:
            for w in set(tail.lower().split()):
                word_df[w] += 1
        return word_df
    
    def _pmi_score(self, tail: str, word_df: Counter) -> float:
        """
        PMI score = MIN log2(df+1) контентных слов хвоста.
        
        Используем min, не mean: если хоть одно слово редкое — PMI низкий.
        "щетки купить" = min(2.0, 6.4) = 2.0 → KNN zone.
        "купить гелевый" = min(6.4, 4.4) = 4.4 → VALID.
        """
        words = tail.lower().split()
        content = [w for w in words if len(w) > 2 or w.isdigit()]
        if not content:
            content = words
        if not content:
            return 0.0
        scores = [math.log2(word_df.get(w, 0) + 1) for w in content]
        return min(scores)
    
    # =========================================================
    # SIGNAL 2: KNN Similarity (top-k mean cosine к VALID)
    # =========================================================
    
    def _normalize(self, v: np.ndarray) -> np.ndarray:
        norm = np.linalg.norm(v)
        return v / norm if norm > 0 else v
    
    def _compute_knn_scores(
        self, 
        grey_tails: List[str], 
        valid_tails: List[str], 
        k: int = 3
    ) -> Dict[str, float]:
        """
        Для каждого GREY хвоста: top-k mean cosine similarity к VALID хвостам.
        
        KNN спрашивает: "похож ли на что-то КОНКРЕТНОЕ?"
        В отличие от centroid ("похож ли на СРЕДНЕЕ?").
        
        k=3: берём 3 ближайших VALID, считаем среднее.
        Робастнее чем single max (одна случайная высокая cosine не делает VALID).
        """
        if not valid_tails or not grey_tails:
            return {tail: 0.0 for tail in grey_tails}
        
        # Embed all at once for efficiency
        valid_embs = np.array(list(self.embedder.embed(valid_tails)))
        valid_embs = np.array([self._normalize(e) for e in valid_embs])
        
        grey_embs = np.array(list(self.embedder.embed(grey_tails)))
        grey_embs = np.array([self._normalize(e) for e in grey_embs])
        
        # Cosine similarity matrix: (n_grey, n_valid)
        sim_matrix = np.dot(grey_embs, valid_embs.T)
        
        # Top-k mean for each grey tail
        actual_k = min(k, len(valid_tails))
        scores = {}
        for i, tail in enumerate(grey_tails):
            top_k = np.sort(sim_matrix[i])[-actual_k:]  # top-k highest
            scores[tail] = float(np.mean(top_k))
        
        return scores
    
    # =========================================================
    # SIGNAL 2 (LEGACY): Centroid Distance — ЗАКОММЕНТИРОВАНО
    # Раскомментировать для сравнения с KNN.
    # =========================================================
    
    # def _compute_centroid(self, valid_tails: List[str]) -> Optional[np.ndarray]:
    #     """Центроид = нормализованный средний вектор L0 VALID хвостов."""
    #     if not valid_tails:
    #         return None
    #     embs = np.array(list(self.embedder.embed(valid_tails)))
    #     embs = np.array([self._normalize(e) for e in embs])
    #     centroid = np.mean(embs, axis=0)
    #     return self._normalize(centroid)
    #
    # def _centroid_distances(self, tails: List[str], centroid: np.ndarray) -> np.ndarray:
    #     """Cosine distance каждого хвоста до центроида."""
    #     if centroid is None:
    #         return np.full(len(tails), 0.5)
    #     embs = np.array(list(self.embedder.embed(tails)))
    #     embs = np.array([self._normalize(e) for e in embs])
    #     return np.dot(embs, centroid)
    
    # =========================================================
    # SIGNAL 3: L0 Signals
    # =========================================================
    
    # =========================================================
    # SIGNAL 4: Морфологическая совместимость (seed head ↔ tail)
    # =========================================================
    #
    # Ключевая идея: "гелевый" — ADJ, согласуется с "аккумулятор" (муж.род, ед.число)
    # → это МОДИФИКАТОР seed'а → VALID.
    # "глушитель" — NOUN, не согласуется как определение → не модификатор.
    #
    # Это POSITIVE-ONLY сигнал:
    #   morph_score ≥ 0.7 → VALID (прилагательное/числительное, согласованное с seed)
    #   morph_score < 0.7 → НЕ информативно (глаголы, бренды тоже валидны)
    #
    # 0 MB extra — pymorphy2 уже в L0, singleton analyzer.
    
    def _extract_seed_head(self, seed: str) -> tuple:
        """
        Извлекаем головное существительное из seed.
        
        "аккумулятор на скутер" → ("аккумулятор", gender=masc, number=sing)
        "шины для bmw" → ("шины", gender=femn, number=plur)
        "кондиционер в квартиру" → ("кондиционер", gender=masc, number=sing)
        
        Берём ПЕРВОЕ существительное — оно обычно главное в PPC seed'ах.
        """
        morph = _get_morph()
        for word in seed.lower().split():
            parses = morph.parse(word)
            if not parses:
                continue
            p = parses[0]
            if 'NOUN' in p.tag:
                return (p.normal_form, p.tag.gender, p.tag.number)
        return (None, None, None)
    
    def _morph_compatibility_score(self, tail: str, seed_gender, seed_number, seed_head=None) -> tuple:
        """
        Морфологическая совместимость хвоста с head noun seed'а.
        
        Returns: (score: float, detail: str)
        
        score ≥ 0.7: хвост содержит согласованный модификатор → VALID signal
        score < 0.7: не информативно (не значит TRASH)
        
        Проверяем ПЕРВЫЕ 1-2 слова хвоста:
        - ADJ согласованное с seed → 1.0 ("гелевый" + аккумулятор[masc])
        - ADJ не согласованное → 0.3 ("гелевая" + аккумулятор[masc] — род не тот)
        - NUMR / цифра → 0.8 (спецификация)
        - PRTF (причастие) согласованное → 0.9 ("тяговый", "заряженный")
        - Всё остальное → 0.0 (NOUN, VERB, бренды — нейтрально)
        
        NOUN-ADJ guard отключён: pymorphy парсит бренды/модели как NOUN
        ("дио", "такт", "штиль" → NOUN), что убивает 120+ валидных хвостов.
        FP типа "жиклер холостого хода", "желтого цвета" → задача L3.
        """
        if seed_gender is None:
            return (0.0, "no_seed_head")
        
        morph = _get_morph()
        words = tail.lower().split()[:2]  # первые 1-2 слова
        
        best_score = 0.0
        best_detail = "no_adj"
        
        for word in words:
            if not word or len(word) < 2:
                continue
                
            # Цифры/спецификации
            if word[0].isdigit():
                if best_score < 0.8:
                    best_score = 0.8
                    best_detail = f"numeric:{word}"
                continue
            
            parses = morph.parse(word)
            if not parses:
                continue
            p = parses[0]
            
            # Прилагательное (полное или краткое)
            if 'ADJF' in p.tag or 'ADJS' in p.tag or 'PRTF' in p.tag:
                gender_match = (p.tag.gender == seed_gender) if p.tag.gender else False
                number_match = (p.tag.number == seed_number) if p.tag.number else False
                
                # Множественное число: род не различается
                if seed_number == 'plur' and p.tag.number == 'plur':
                    gender_match = True
                
                if gender_match and number_match:
                    score = 1.0 if ('ADJF' in p.tag or 'ADJS' in p.tag) else 0.9
                    detail = f"adj_agree:{word}({p.tag.gender},{p.tag.number})"
                    if score > best_score:
                        best_score = score
                        best_detail = detail
                elif gender_match or number_match:
                    if best_score < 0.5:
                        best_score = 0.5
                        best_detail = f"adj_partial:{word}({p.tag.gender},{p.tag.number})"
                else:
                    if best_score < 0.3:
                        best_score = 0.3
                        best_detail = f"adj_no_agree:{word}({p.tag.gender},{p.tag.number})"
        
        return (best_score, best_detail)
    
    def _parse_l0_signals(self, l0_trace: List[dict]) -> Dict[str, dict]:
        """
        Парсим L0 trace → для каждого keyword:
        positive, negative, pure_neg (neg без pos).
        """
        result = {}
        for trace in l0_trace:
            kw = trace.get("keyword", "")
            signals = trace.get("signals", [])
            neg = [s.lstrip('-') for s in signals if s.startswith('-')]
            pos = [s for s in signals if not s.startswith('-')]
            result[kw] = {
                "positive": pos,
                "negative": neg,
                "pure_neg": bool(neg) and not bool(pos),
            }
        return result
    
    # =========================================================
    # SIGNAL 6: Word Overlap с Reference (L0 VALID)
    # =========================================================
    #
    # Извлекаем все значимые слова из L0 VALID хвостов.
    # Если GREY tail содержит хотя бы одно такое слово → VALID.
    #
    # Примеры: reference содержит "хонда дио", "ямаха джог", "сузуки"
    # → "ямаха джог 36" содержит "ямаха" → VALID
    # → "глушитель" не содержит ни одного → GREY
    #
    # Cross-niche: работает на любой нише, используя данные самого батча.
    
    _STOP_WORDS = frozenset({
        'и', 'или', 'на', 'в', 'для', 'с', 'по', 'от', 'из', 'к', 'у',
        'а', 'но', 'не', 'ни', 'да', 'же', 'ли', 'бы', 'то', 'что',
        'как', 'где', 'кто', 'чем', 'при', 'до', 'без', 'под', 'над',
        'за', 'про', 'через', 'это', 'его', 'её', 'их', 'мой', 'наш',
        'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
        'and', 'or', 'is', 'are', 'was', 'be',
        'купить', 'цена', 'стоит', 'можно', 'нужно', 'лучше',
        'какой', 'сколько', 'новый', 'своими', 'руками',
    })
    
    def _extract_reference_words(
        self,
        valid_tails: List[str],
        seed_head: Optional[str] = None
    ) -> set:
        """
        Извлечь уникальные значимые слова из L0 VALID хвостов.
        """
        ref_words = set()
        for tail in valid_tails:
            tokens = tail.lower().split()
            for token in tokens:
                clean = token.strip('.,;:!?/\\()[]{}"\'-')
                if not clean or len(clean) < 2:
                    continue
                if clean in self._STOP_WORDS:
                    continue
                if clean.isdigit():
                    continue
                if seed_head and clean == seed_head.lower():
                    continue
                ref_words.add(clean)
        return ref_words
    
    def _compute_word_overlap(
        self,
        grey_tails: List[str],
        ref_words: set
    ) -> Dict[str, List[str]]:
        """
        Для каждого GREY tail: какие reference-слова в нём есть?
        Returns: {tail: [matched_words]}
        """
        if not ref_words:
            return {tail: [] for tail in grey_tails}
        
        overlap = {}
        for tail in grey_tails:
            tokens = set()
            for token in tail.lower().split():
                clean = token.strip('.,;:!?/\\()[]{}"\'-')
                if clean and len(clean) >= 2:
                    tokens.add(clean)
            matched = sorted(tokens & ref_words)
            overlap[tail] = matched
        return overlap
    
    # =========================================================
    # SIGNAL 7: Digit-Letter Specification Pattern
    # =========================================================
    #
    # Универсальный паттерн: токен смешивает цифры и буквы = спецификация.
    # Cross-niche: 12v, 7ah (аккум), 225/45r17 (шины), 16gb (телефоны),
    # ytz7s, ctx5l (модели батарей), 50кубов (скутеры).
    #
    # Проверяем ВСЕ слова хвоста (не только первые 2 как morph).
    # Positive-only: нет паттерна ≠ TRASH.
    
    _SPEC_PATTERN = re.compile(r'\d+[a-zа-яё]+|[a-zа-яё]+\d+', re.IGNORECASE)
    
    def _has_spec_token(self, tail: str) -> tuple:
        """
        Проверяет наличие digit-letter токенов в хвосте.
        
        Returns: (has_spec: bool, matched_token: str)
        
        "12v 7ah" → (True, "12v")
        "ytz7s" → (True, "ytz7s")
        "купить гелевый" → (False, "")
        """
        for token in tail.lower().split():
            clean = token.strip('.,;:!?/\\()[]{}"\'-')
            if clean and self._SPEC_PATTERN.search(clean):
                return (True, clean)
        return (False, "")
    
    # =========================================================
    # MAIN: Classify L0 result
    # =========================================================
    
    def classify_l0_result(
        self,
        l0_result: Dict[str, Any],
        seed: str
    ) -> Dict[str, Any]:
        """
        Обработать результат L0, классифицировать GREY через три сигнала.
        """
        grey_keywords = l0_result.get("keywords_grey", [])
        l0_valid_keywords = l0_result.get("keywords", [])
        
        if not grey_keywords:
            logger.info("L2: No GREY to process")
            return l0_result
        
        # === Parse L0 signals ===
        l0_trace = l0_result.get("_l0_trace", [])
        l0_signals = self._parse_l0_signals(l0_trace)
        
        # === Build keyword→tail lookup from L0 trace ===
        # L0 trace содержит правильно извлечённые хвосты.
        # keywords_grey может содержать строки (полные запросы),
        # поэтому lookup через _l0_trace критичен.
        l0_tail_lookup = {}
        for rec in l0_trace:
            kw_lower = rec.get("keyword", "").lower().strip()
            tail = rec.get("tail", "")
            if kw_lower and tail:
                l0_tail_lookup[kw_lower] = tail
        
        # === Извлекаем хвосты GREY ===
        grey_tails = []
        tail_to_kw = {}
        kw_to_tail = {}
        
        for kw in grey_keywords:
            if isinstance(kw, dict):
                keyword = kw.get("keyword", kw.get("query", ""))
                tail = kw.get("tail") or l0_tail_lookup.get(keyword.lower().strip(), keyword)
            else:
                keyword = str(kw)
                tail = l0_tail_lookup.get(keyword.lower().strip(), keyword)
            
            if tail:
                grey_tails.append(tail)
                tail_to_kw[tail] = kw
                kw_to_tail[keyword] = tail
        
        # L0 VALID хвосты для KNN + PMI
        valid_tails = []
        for kw in l0_valid_keywords:
            if isinstance(kw, dict):
                keyword = kw.get("keyword", kw.get("query", ""))
                tail = kw.get("tail") or l0_tail_lookup.get(keyword.lower().strip(), keyword)
            else:
                keyword = str(kw)
                tail = l0_tail_lookup.get(keyword.lower().strip(), keyword)
            if tail:
                valid_tails.append(tail)
        
        logger.info(
            f"L2: {len(grey_tails)} GREY, "
            f"KNN from {len(valid_tails)} L0 VALID"
        )
        
        # === SIGNAL 1: PMI ===
        all_tails_for_df = valid_tails + grey_tails
        word_df = self._compute_word_df(all_tails_for_df)
        pmi_scores = {tail: self._pmi_score(tail, word_df) for tail in grey_tails}
        
        # === SIGNAL 2: KNN Similarity — ОТКЛЮЧЕНО ===
        # MiniLM не различает функциональную совместимость.
        # KNN VALID (R3) отключён давно. KNN TRASH (R4/R5) ловит ~7 хвостов,
        # L3 обработает их без потерь. Экономим compute time на embed 270+ хвостов.
        knn_scores = {tail: 0.0 for tail in grey_tails}
        
        # --- CENTROID (закомментировано) ---
        # centroid_scores = {}
        # ...
        
        # === SIGNAL 4: Morph Compatibility ===
        seed_head, seed_gender, seed_number = self._extract_seed_head(seed)
        morph_scores = {}
        morph_details = {}
        if seed_head:
            logger.info(f"L2: Seed head noun: '{seed_head}' ({seed_gender}, {seed_number})")
            for tail in grey_tails:
                score, detail = self._morph_compatibility_score(tail, seed_gender, seed_number, seed_head)
                morph_scores[tail] = score
                morph_details[tail] = detail
        else:
            logger.warning(f"L2: No head noun in seed '{seed}', morph disabled")
            for tail in grey_tails:
                morph_scores[tail] = 0.0
                morph_details[tail] = "no_seed_head"
        
        # === SIGNAL 5: Word Overlap with Reference ===
        ref_words = self._extract_reference_words(valid_tails, seed_head)
        word_overlap = self._compute_word_overlap(grey_tails, ref_words)
        logger.info(f"L2: Reference words ({len(ref_words)}): {sorted(ref_words)[:20]}...")
        
        # === MULTI-SIGNAL DECISION ===
        cfg = self.config
        classified = {"valid": [], "grey": [], "trash": []}
        
        for tail in grey_tails:
            kw = tail_to_kw.get(tail)
            keyword = kw.get("keyword", tail) if isinstance(kw, dict) else tail
            
            pmi = pmi_scores.get(tail, 0)
            knn = knn_scores.get(tail, 0.0)
            morph = morph_scores.get(tail, 0.0)
            morph_detail = morph_details.get(tail, "")
            l0_sig = l0_signals.get(keyword, {
                "positive": [], "negative": [], "pure_neg": False
            })
            pure_neg = l0_sig["pure_neg"]
            overlap = word_overlap.get(tail, [])
            has_spec, spec_token = self._has_spec_token(tail)
            
            debug = {
                "pmi": round(pmi, 3),
                "knn_score": round(knn, 4),
                "morph_score": round(morph, 2),
                "morph_detail": morph_detail,
                "ref_overlap": overlap,
                "spec_token": spec_token,
                "l0_pos": l0_sig["positive"],
                "l0_neg": l0_sig["negative"],
                "pure_neg": pure_neg,
            }
            
            # --- Decision rules ---
            label = "GREY"
            reason = ""
            
            # R1: High PMI + no pure negative → VALID
            if pmi >= cfg.pmi_valid_threshold and not pure_neg:
                label = "VALID"
                reason = f"PMI {pmi:.2f} >= {cfg.pmi_valid_threshold}"
            
            # R2: High PMI + pure negative → conflict → GREY (→ LLM)
            elif pmi >= cfg.pmi_valid_threshold and pure_neg:
                label = "GREY"
                reason = f"Conflict: PMI {pmi:.2f} high, L0 pure-neg"
            
            # R2.5: Morph agreement (ADJ agrees with seed head) → VALID
            # "гелевый" + аккумулятор[masc] → согласуется → VALID
            # Positive-only: morph < 0.7 НЕ значит TRASH
            # KNN guard НЕ работает: "литий ионный" = morph 1.0, KNN 0.378 (валидный, но KNN низкий)
            # "женский" = morph 1.0, KNN 0.752 (FP, но KNN высокий). Диапазоны пересекаются.
            elif morph >= 0.7 and not pure_neg:
                label = "VALID"
                reason = f"Morph {morph:.1f} ({morph_detail})"
            
            # R2.8: Digit-letter spec token → VALID
            # "12v", "7ah", "ytz7s", "50кубов" — спецификация = коммерческий запрос
            # Cross-niche: любой токен смешивающий цифры и буквы
            elif has_spec and not pure_neg:
                label = "VALID"
                reason = f"SpecToken: {spec_token}"
            
            # R2.7: Word overlap с reference — ОТКЛЮЧЕНО (даёт FP через intent-слова)
            # Overlap считается и логируется в диагностику, но не влияет на решение.
            # elif overlap and not pure_neg:
            #     label = "VALID"
            #     reason = f"RefOverlap: {','.join(overlap)}"
            
            # R3: KNN VALID — ОТКЛЮЧЕНО (false positives: глушитель 0.84, жигули 0.89)
            # Template cosine пока diagnostic-only, порог установим после анализа
            # elif knn >= cfg.knn_valid_threshold and not pure_neg:
            #     label = "VALID"
            #     reason = f"KNN {knn:.3f} >= {cfg.knn_valid_threshold}"
            
            # R4: Pure neg + low KNN → TRASH — ОТКЛЮЧЕНО (KNN отключён)
            # elif pure_neg and knn < cfg.knn_trash_threshold:
            #     label = "TRASH"
            #     reason = f"Pure-neg + KNN {knn:.3f} < {cfg.knn_trash_threshold}"
            
            # R5: Low PMI + low KNN → TRASH — ОТКЛЮЧЕНО (KNN отключён)
            # elif pmi < cfg.pmi_valid_threshold and knn < cfg.knn_trash_threshold:
            #     label = "TRASH"
            #     reason = f"PMI {pmi:.2f} low + KNN {knn:.3f} low"
            
            # R6: Everything else → GREY (→ LLM)
            else:
                label = "GREY"
                reason = f"Uncertain: PMI {pmi:.2f}, KNN {knn:.3f}"
            
            debug["decision"] = reason
            classified[label.lower()].append({"tail": tail, "debug": debug})
        
        # === Assemble result ===
        result = l0_result.copy()
        
        # VALID: L0 + L2 (записываем l2 debug в каждый keyword)
        l2_valid = []
        for item in classified["valid"]:
            tail = item["tail"]
            if tail in tail_to_kw:
                kw = tail_to_kw[tail]
                if isinstance(kw, dict):
                    kw = kw.copy()
                debug = item.get("debug", {})
                # l2 info в формате для output JSON
                if isinstance(kw, dict):
                    kw["l2"] = {
                        "label": "VALID",
                        "pmi": debug.get("pmi", 0),
                        "knn_score": debug.get("knn_score", 0),
                        "morph_score": debug.get("morph_score", 0),
                        "morph_detail": debug.get("morph_detail", ""),
                        "ref_overlap": debug.get("ref_overlap", []),
                        "spec_token": debug.get("spec_token", ""),
                        "decision": debug.get("decision", ""),
                    }
                l2_valid.append(kw)
        result["keywords"] = l0_result.get("keywords", []) + l2_valid
        
        # TRASH: L0 + L2
        l2_trash = []
        for item in classified["trash"]:
            tail = item["tail"]
            if tail in tail_to_kw:
                kw = tail_to_kw[tail]
                if isinstance(kw, dict):
                    kw = kw.copy()
                    kw["anchor_reason"] = "L2_TRASH"
                    kw["l2_debug"] = item.get("debug", {})
                else:
                    kw = {
                        "keyword": kw, "tail": tail,
                        "anchor_reason": "L2_TRASH",
                        "l2_debug": item.get("debug", {})
                    }
                l2_trash.append(kw)
        result["anchors"] = l0_result.get("anchors", []) + l2_trash
        
        # GREY → L3 (с l2 debug)
        l2_grey = []
        for item in classified["grey"]:
            tail = item["tail"]
            if tail in tail_to_kw:
                kw = tail_to_kw[tail]
                if isinstance(kw, dict):
                    kw = kw.copy()
                debug = item.get("debug", {})
                if isinstance(kw, dict):
                    kw["l2"] = {
                        "label": "GREY",
                        "pmi": debug.get("pmi", 0),
                        "knn_score": debug.get("knn_score", 0),
                        "morph_score": debug.get("morph_score", 0),
                        "morph_detail": debug.get("morph_detail", ""),
                        "ref_overlap": debug.get("ref_overlap", []),
                        "spec_token": debug.get("spec_token", ""),
                        "decision": debug.get("decision", ""),
                    }
                l2_grey.append(kw)
        result["keywords_grey"] = l2_grey
        
        # Stats
        result["l2_stats"] = {
            "input_grey": len(grey_tails),
            "l0_valid_for_knn": len(valid_tails),
            "l2_valid": len(classified["valid"]),
            "l2_trash": len(classified["trash"]),
            "l2_grey": len(classified["grey"]),
            "reduction_pct": round(
                (1 - len(classified["grey"]) / len(grey_tails)) * 100, 1
            ) if grey_tails else 0
        }
        
        # Detailed trace (без underscore — чтобы не стрипилось)
        l2_trace = []
        for category, lbl in [("valid", "VALID"), ("trash", "TRASH"), ("grey", "GREY")]:
            for item in classified[category]:
                tail = item["tail"]
                debug = item.get("debug", {})
                kw = tail_to_kw.get(tail)
                keyword = kw.get("keyword", tail) if isinstance(kw, dict) else tail
                l2_trace.append({
                    "keyword": keyword, "tail": tail, "label": lbl,
                    "pmi": debug.get("pmi", 0),
                    "knn_score": debug.get("knn_score", 0),
                    "morph_score": debug.get("morph_score", 0),
                    "morph_detail": debug.get("morph_detail", ""),
                        "ref_overlap": debug.get("ref_overlap", []),
                    "spec_token": debug.get("spec_token", ""),
                    "l0_pos": debug.get("l0_pos", []),
                    "l0_neg": debug.get("l0_neg", []),
                    "decision": debug.get("decision", ""),
                })
        result["l2_trace"] = l2_trace
        
        # === Diagnostic dump ===
        try:
            diag_path = os.path.join(os.path.dirname(self.config.cache_file) or '.', 'l2_diagnostic.json')
            diag = {
                "config": {
                    "pmi_valid_threshold": cfg.pmi_valid_threshold,
                    "knn_k": cfg.knn_k,
                    "knn_valid_threshold": cfg.knn_valid_threshold,
                    "knn_trash_threshold": cfg.knn_trash_threshold,
                    "morph_valid_threshold": 0.7,
                    "seed_head": seed_head,
                    "seed_gender": str(seed_gender),
                    "seed_number": str(seed_number),
                },
                "stats": result["l2_stats"],
                "knn_score_distribution": {
                    "min": round(min(knn_scores.values()), 4) if knn_scores else 0,
                    "max": round(max(knn_scores.values()), 4) if knn_scores else 0,
                    "mean": round(sum(knn_scores.values()) / len(knn_scores), 4) if knn_scores else 0,
                },
                "morph_score_distribution": {
                    "min": round(min(morph_scores.values()), 2) if morph_scores else 0,
                    "max": round(max(morph_scores.values()), 2) if morph_scores else 0,
                    "scores_gte_07": sum(1 for v in morph_scores.values() if v >= 0.7),
                    "scores_eq_0": sum(1 for v in morph_scores.values() if v == 0.0),
                },
                "word_overlap": {
                    "reference_words": sorted(ref_words),
                    "reference_word_count": len(ref_words),
                    "tails_with_overlap": sum(1 for v in word_overlap.values() if v),
                    "tails_without_overlap": sum(1 for v in word_overlap.values() if not v),
                },
                "trace": l2_trace,
            }
            with open(diag_path, 'w', encoding='utf-8') as f:
                json.dump(diag, f, ensure_ascii=False, indent=2)
            logger.info(f"L2: Diagnostic dump → {diag_path}")
        except Exception as e:
            logger.warning(f"L2: Failed to write diagnostic: {e}")
        
        stats = result["l2_stats"]
        logger.info(
            f"L2: {stats['l2_valid']} VALID, "
            f"{stats['l2_trash']} TRASH, "
            f"{stats['l2_grey']} GREY→L3 "
            f"({stats['reduction_pct']}% reduction)"
        )
        
        return result


# === Singleton ===
_l2_instance: Optional[L2Classifier] = None


def get_l2_classifier(config: Optional[L2Config] = None) -> L2Classifier:
    global _l2_instance
    if _l2_instance is None:
        _l2_instance = L2Classifier(config)
    return _l2_instance


def apply_l2_filter(
    l0_result: Dict[str, Any],
    seed: str,
    enable_l2: bool = True,
    config: Optional[L2Config] = None
) -> Dict[str, Any]:
    """Обёртка для применения L2 фильтра к результату L0."""
    if not enable_l2:
        return l0_result
    
    grey_count = len(l0_result.get("keywords_grey", []))
    if grey_count == 0:
        return l0_result
    
    try:
        classifier = get_l2_classifier()
        if config is not None:
            classifier.config = config
        
        result = classifier.classify_l0_result(l0_result, seed)
        result["l2_config"] = {
            "pmi_valid_threshold": classifier.config.pmi_valid_threshold,
            "knn_k": classifier.config.knn_k,
            "knn_valid_threshold": classifier.config.knn_valid_threshold,
            "knn_trash_threshold": classifier.config.knn_trash_threshold,
        }
        return result
    
    except Exception as e:
        logger.error(f"L2: Failed: {e}")
        l0_result["l2_error"] = str(e)
        return l0_result
