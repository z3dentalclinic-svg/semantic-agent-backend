"""
Category Mismatch Detector для L0.

Определяет несовместимость категории tail со seed через embeddings.
Например: seed="аккумулятор на скутер", tail="щербет" → еда ≠ запчасть → TRASH

Использует SHARED модель (shared_model.py) — та же что и L2.
"""

import logging
from typing import Optional, Tuple, Dict
from dataclasses import dataclass
import numpy as np

from .shared_model import get_embedding_model

logger = logging.getLogger(__name__)


@dataclass
class CategoryConfig:
    """Конфигурация категорий."""
    category_threshold: float = 0.35
    min_category_margin: float = 0.05


# Anchor тексты для категорий
CATEGORY_ANCHORS = {
    "auto_parts": [
        "запчасть", "деталь", "механизм", "агрегат", "узел",
        "аккумулятор", "двигатель", "тормоз", "фильтр", "ремень",
        "свеча", "масло моторное", "антифриз", "колодки", "генератор",
        "стартер", "радиатор", "амортизатор", "подшипник",
    ],
    "food": [
        "еда", "пища", "продукт", "сладость", "десерт",
        "щербет", "конфета", "торт", "пирожное", "шоколад",
        "мороженое", "варенье", "печенье", "булочка", "хлеб",
        "фрукт", "овощ", "мясо", "рыба", "молоко",
    ],
    "animals": [
        "животное", "зверь", "питомец", "собака", "кошка",
        "птица", "рыба", "хомяк", "попугай", "кролик",
        "лошадь", "корова", "свинья", "овца", "коза",
    ],
    "clothing": [
        "одежда", "обувь", "аксессуар", "куртка", "штаны",
        "платье", "шапка", "перчатки", "ботинки", "кроссовки",
    ],
    "furniture": [
        "мебель", "стол", "стул", "шкаф", "диван",
        "кровать", "тумбочка", "полка", "кресло", "комод",
    ],
    "mythology": [
        "мифическое существо", "йети", "снежный человек", "дракон",
        "единорог", "русалка", "леший", "домовой", "вампир",
        "оборотень", "фея", "эльф", "гном", "тролль",
    ],
    "sounds": [
        "звук", "шум", "грохот", "бум", "бах", "хлопок",
        "взрыв", "треск", "гул", "свист", "писк",
    ],
}

# Несовместимые категории
INCOMPATIBLE_CATEGORIES = {
    "auto_parts": ["food", "animals", "clothing", "furniture", "mythology", "sounds"],
    "electronics": ["food", "animals", "clothing", "furniture", "mythology", "sounds"],
    "tools": ["food", "animals", "clothing", "mythology", "sounds"],
}


class CategoryMismatchDetector:
    """Детектор несовместимости категорий через embeddings."""
    
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
        self._anchor_embeddings: Dict[str, np.ndarray] = {}
        self._seed_category_cache: Dict[str, str] = {}
        self._initialized = True
        self._anchors_computed = False
    
    def _get_model(self):
        """Получить shared модель."""
        return get_embedding_model()
    
    def _compute_anchor_embeddings(self):
        """Вычислить embeddings для anchor категорий."""
        if self._anchors_computed:
            return
        
        model = self._get_model()
        if model is None:
            return
        
        logger.info("CategoryMismatchDetector: Computing anchor embeddings...")
        
        for category, anchors in CATEGORY_ANCHORS.items():
            anchor_text = " ".join(anchors)
            embeddings = list(model.embed([anchor_text]))
            if embeddings:
                self._anchor_embeddings[category] = embeddings[0]
        
        self._anchors_computed = True
        logger.info(f"CategoryMismatchDetector: Initialized {len(self._anchor_embeddings)} categories")
    
    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))
    
    def get_category(self, word: str) -> Tuple[str, float]:
        """Определить категорию слова."""
        self._compute_anchor_embeddings()
        
        model = self._get_model()
        if not self._anchor_embeddings or model is None:
            return ("unknown", 0.0)
        
        word_embeddings = list(model.embed([word]))
        if not word_embeddings:
            return ("unknown", 0.0)
        
        word_emb = word_embeddings[0]
        scores = [(cat, self._cosine_similarity(word_emb, emb)) 
                  for cat, emb in self._anchor_embeddings.items()]
        scores.sort(key=lambda x: x[1], reverse=True)
        
        if not scores:
            return ("unknown", 0.0)
        
        best_category, best_score = scores[0]
        
        if len(scores) >= 2 and (best_score - scores[1][1]) < self.config.min_category_margin:
            return ("ambiguous", best_score)
        
        return (best_category, best_score)
    
    def detect_mismatch(self, seed: str, tail: str) -> Tuple[bool, str]:
        """Проверить несовместимость категории tail с seed."""
        model = self._get_model()
        if model is None:
            return (False, "")  # Модель недоступна — не блокируем
        
        self._compute_anchor_embeddings()
        if not self._anchor_embeddings:
            return (False, "")
        
        # Категория seed (кэш)
        seed_key = seed.lower().strip()
        if seed_key not in self._seed_category_cache:
            seed_word = seed.split()[0] if seed else seed
            seed_cat, _ = self.get_category(seed_word)
            self._seed_category_cache[seed_key] = seed_cat
        
        seed_category = self._seed_category_cache[seed_key]
        
        if seed_category not in INCOMPATIBLE_CATEGORIES:
            return (False, "")
        
        incompatible = INCOMPATIBLE_CATEGORIES[seed_category]
        
        for tail_word in tail.lower().strip().split():
            if len(tail_word) <= 2:
                continue
            
            tail_cat, tail_conf = self.get_category(tail_word)
            
            if tail_cat in incompatible and tail_conf >= self.config.category_threshold:
                reason = f"Категория '{tail_word}' ({tail_cat}, {tail_conf:.2f}) ≠ seed '{seed_category}'"
                return (True, reason)
        
        return (False, "")


# Глобальный instance
_detector: Optional[CategoryMismatchDetector] = None


def get_category_detector() -> CategoryMismatchDetector:
    global _detector
    if _detector is None:
        _detector = CategoryMismatchDetector()
    return _detector


def detect_category_mismatch(seed: str, tail: str) -> Tuple[bool, str]:
    """Функция для вызова из L0."""
    return get_category_detector().detect_mismatch(seed, tail)
