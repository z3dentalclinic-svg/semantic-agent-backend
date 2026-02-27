"""
Category Mismatch Detector для L0.

Определяет несовместимость категории tail со seed через embeddings.
Например: seed="аккумулятор на скутер", tail="щербет" → еда ≠ запчасть → TRASH

Использует модель L2 (fastembed) для embeddings.
Ленивая инициализация — модель загружается только при первом вызове.
"""

import logging
from typing import Optional, Tuple, Dict
from dataclasses import dataclass
import numpy as np

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
        self.model = None
        self.config = CategoryConfig()
        self._anchor_embeddings: Dict[str, np.ndarray] = {}
        self._seed_category_cache: Dict[str, str] = {}
        self._initialized = True
        self._model_loaded = False
    
    def _load_model(self):
        """Ленивая загрузка модели."""
        if self._model_loaded:
            return True
        try:
            from fastembed import TextEmbedding
            logger.info("CategoryMismatchDetector: Loading embedding model...")
            self.model = TextEmbedding("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
            self._model_loaded = True
            logger.info("CategoryMismatchDetector: Model loaded")
            return True
        except Exception as e:
            logger.warning(f"CategoryMismatchDetector: Failed to load model: {e}")
            return False
    
    def _compute_anchor_embeddings(self):
        """Вычислить embeddings для anchor категорий."""
        if self._anchor_embeddings or not self._load_model():
            return
        
        for category, anchors in CATEGORY_ANCHORS.items():
            anchor_text = " ".join(anchors)
            embeddings = list(self.model.embed([anchor_text]))
            if embeddings:
                self._anchor_embeddings[category] = embeddings[0]
    
    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))
    
    def get_category(self, word: str) -> Tuple[str, float]:
        """Определить категорию слова."""
        self._compute_anchor_embeddings()
        
        if not self._anchor_embeddings or self.model is None:
            return ("unknown", 0.0)
        
        word_embeddings = list(self.model.embed([word]))
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
        if not self._model_loaded and not self._load_model():
            return (False, "")
        
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
