"""
Category Mismatch Detector для L0.

Каскадная архитектура (три стадии):

Stage 0: short-circuit — если geo/brand уже в positive_signals,
         детектор не вызывается (логика в tail_function_classifier.py)

Stage 1: char n-gram gate — мгновенный sparse cosine между seed и tail.
         Без нейросети. <1ms на весь батч.
         score < chargram_low  → MISMATCH
         score > chargram_high → PASS
         иначе → Stage 2

Stage 2: MiniLM semantic fallback — только для спорных случаев.
         Embeddings берутся из кэша (заполненного pre_batch() из l0_filter).
         Один батч-вызов на весь запрос вместо N одиночных.

Интерфейс detect_category_mismatch(seed, tail) не изменился.
"""

import logging
from typing import Optional, Tuple, Dict, List
from dataclasses import dataclass
import numpy as np

from .shared_model import get_embedding_model

logger = logging.getLogger(__name__)


@dataclass
class CategoryConfig:
    # Порог semantic cosine — ниже → MISMATCH
    semantic_threshold: float = 0.35
    # Char n-gram: ниже → очевидный мусор (Stage 1 TRASH, без embed)
    chargram_low: float = 0.05
    # Char n-gram: выше → очевидно совместимо (Stage 1 PASS, без embed)
    chargram_high: float = 0.20
    # Размер char n-gram
    ngram_size: int = 3


def _char_ngrams(text: str, n: int = 3) -> set:
    """Символьные n-граммы. Пробел → '_' для учёта границ слов."""
    t = text.replace(' ', '_')
    if len(t) < n:
        return {t} if t else set()
    return {t[i:i+n] for i in range(len(t) - n + 1)}


def _chargram_similarity(text1: str, text2: str, n: int = 3) -> float:
    """
    Jaccard-подобное сходство по символьным n-граммам.
    Нормируем по min чтобы короткие слова не дискриминировались.

    "ремонт пылесосов" vs "щербет"   → ~0.0  (нет общих триграмм → TRASH)
    "ремонт пылесосов" vs "ремонтник" → ~0.4  (общие "рем", "емо", "мон" → PASS)
    """
    ng1 = _char_ngrams(text1.lower(), n)
    ng2 = _char_ngrams(text2.lower(), n)
    if not ng1 or not ng2:
        return 0.0
    return len(ng1 & ng2) / min(len(ng1), len(ng2))


class CategoryMismatchDetector:
    """Синглтон. Создаётся один раз при импорте."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.config = CategoryConfig()
        # Кэш embedding'ов seed'а (по тексту seed)
        self._seed_embedding_cache: Dict[str, np.ndarray] = {}
        # Кэш embedding'ов хвостов — заполняется через pre_batch() одним вызовом.
        # Персистентен между запросами: повторяющиеся хвосты не пересчитываются.
        self._tail_embedding_cache: Dict[str, np.ndarray] = {}
        self._initialized = True

    def _get_model(self):
        return get_embedding_model()

    def _get_seed_embedding(self, seed_clean: str) -> Optional[np.ndarray]:
        """Embedding seed'а — кэшируется навсегда (seed меняется редко)."""
        if seed_clean in self._seed_embedding_cache:
            return self._seed_embedding_cache[seed_clean]
        model = self._get_model()
        if model is None:
            return None
        try:
            embs = list(model.embed([seed_clean]))
            if not embs:
                return None
            emb = np.array(embs[0])
            self._seed_embedding_cache[seed_clean] = emb
            return emb
        except Exception as e:
            logger.warning("[CategoryMismatch] seed embed failed: %s", e)
            return None

    def pre_batch(self, tails: List[str]):
        """
        Вычисляет embeddings для всех хвостов ОДНИМ батч-вызовом модели.
        Вызывается из apply_l0_filter ДО основного цикла classify.

        После этого detect_mismatch() берёт embedding из кэша — O(1), без инференса.
        Хвосты которые уже в кэше пропускаются.
        """
        if not tails:
            return
        model = self._get_model()
        if model is None:
            return

        unknown = list({
            t.lower().strip() for t in tails
            if t and t.lower().strip() not in self._tail_embedding_cache
        })
        if not unknown:
            return

        try:
            embeddings = list(model.embed(unknown))  # ОДИН вызов на весь батч
            for tail_text, emb in zip(unknown, embeddings):
                self._tail_embedding_cache[tail_text] = np.array(emb)
            logger.debug("[CategoryMismatch] pre_batch: %d tails embedded", len(unknown))
        except Exception as e:
            logger.warning("[CategoryMismatch] pre_batch failed: %s", e)

    def _cosine(self, a: np.ndarray, b: np.ndarray) -> float:
        na, nb = np.linalg.norm(a), np.linalg.norm(b)
        if na == 0 or nb == 0:
            return 0.0
        return float(np.dot(a, b) / (na * nb))

    def detect_mismatch(self, seed: str, tail: str) -> Tuple[bool, str]:
        """
        Каскадная проверка несовместимости tail с seed.

        Stage 1 → Stage 2 (Stage 0 снаружи в tail_function_classifier.py)
        """
        if not tail or not seed:
            return False, ""

        tail_clean = tail.lower().strip()
        seed_clean = seed.lower().strip()

        # ── Stage 1: char n-gram gate (мгновенно) ───────────────────────────
        cg_score = _chargram_similarity(seed_clean, tail_clean, self.config.ngram_size)

        if cg_score <= self.config.chargram_low:
            return True, (
                f"Несовместимость категории: chargram={cg_score:.3f} "
                f"— tail лексически далёк от seed"
            )

        if cg_score >= self.config.chargram_high:
            return False, ""  # Лексически близко → совместимо

        # ── Stage 2: semantic fallback (из кэша pre_batch) ───────────────────
        model = self._get_model()
        if model is None:
            return False, ""

        seed_emb = self._get_seed_embedding(seed_clean)
        if seed_emb is None:
            return False, ""

        # Берём из кэша (pre_batch уже заполнил)
        tail_emb = self._tail_embedding_cache.get(tail_clean)
        if tail_emb is None:
            # Fallback: embed прямо сейчас если pre_batch не был вызван
            try:
                embs = list(model.embed([tail_clean]))
                if not embs:
                    return False, ""
                tail_emb = np.array(embs[0])
                self._tail_embedding_cache[tail_clean] = tail_emb
            except Exception:
                return False, ""

        cosine_score = self._cosine(seed_emb, tail_emb)

        if cosine_score < self.config.semantic_threshold:
            return True, (
                f"Семантическая несовместимость: cosine={cosine_score:.3f} "
                f"(< {self.config.semantic_threshold})"
            )

        return False, ""


_detector: Optional[CategoryMismatchDetector] = None


def get_category_detector() -> CategoryMismatchDetector:
    global _detector
    if _detector is None:
        _detector = CategoryMismatchDetector()
    return _detector


def detect_category_mismatch(seed: str, tail: str) -> Tuple[bool, str]:
    """Функция для вызова из L0. Интерфейс не изменился."""
    return get_category_detector().detect_mismatch(seed, tail)
