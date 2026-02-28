"""
Shared Embedding Models — singletons для L2 и CategoryMismatch.

Модели:
1. MiniLM (основная) — multilingual embeddings для KNN, CategoryMismatch
2. rubert-tiny (опциональная) — русскоязычная модель для template cosine

Загружаются ОДИН раз, переиспользуются всеми модулями.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# === MiniLM (основная) ===
_model = None
_model_loading = False


def get_embedding_model():
    """
    Получить singleton модели embeddings (MiniLM).
    Ленивая загрузка — грузится при первом вызове.
    """
    global _model, _model_loading
    
    if _model is not None:
        return _model
    
    if _model_loading:
        import time
        for _ in range(30):
            time.sleep(1)
            if _model is not None:
                return _model
        return None
    
    _model_loading = True
    
    try:
        from fastembed import TextEmbedding
        logger.info("[SharedModel] Loading MiniLM embedding model...")
        _model = TextEmbedding("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
        logger.info("[SharedModel] MiniLM loaded successfully")
    except Exception as e:
        logger.error(f"[SharedModel] Failed to load MiniLM: {e}")
        _model = None
    finally:
        _model_loading = False
    
    return _model


def is_model_loaded() -> bool:
    """Проверить загружена ли MiniLM."""
    return _model is not None


# === rubert-tiny (опциональная, для template cosine) ===
_rubert_model = None
_rubert_loading = False
_rubert_attempted = False  # Не пытаться повторно если failed


def get_rubert_model():
    """
    Получить singleton rubert-tiny модели.
    Ленивая загрузка. Возвращает None если модель недоступна.
    
    Пробуем несколько вариантов ONNX моделей для русского:
    1. cointegrated/rubert-tiny2 (основной)
    2. fallback на None если не получится
    """
    global _rubert_model, _rubert_loading, _rubert_attempted
    
    if _rubert_model is not None:
        return _rubert_model
    
    if _rubert_attempted:
        return None  # Уже пробовали, не вышло
    
    if _rubert_loading:
        import time
        for _ in range(30):
            time.sleep(1)
            if _rubert_model is not None:
                return _rubert_model
        return None
    
    _rubert_loading = True
    _rubert_attempted = True
    
    # Список моделей для попытки (в порядке приоритета)
    model_candidates = [
        "cointegrated/rubert-tiny2",
        "sergeyzh/rubert-tiny-turbo",
    ]
    
    try:
        from fastembed import TextEmbedding
        
        for model_name in model_candidates:
            try:
                logger.info(f"[SharedModel] Trying rubert model: {model_name}...")
                _rubert_model = TextEmbedding(model_name)
                logger.info(f"[SharedModel] rubert loaded: {model_name}")
                break
            except Exception as e:
                logger.warning(f"[SharedModel] {model_name} failed: {e}")
                continue
        
        if _rubert_model is None:
            logger.warning("[SharedModel] No rubert model available, template cosine will use MiniLM fallback")
    except Exception as e:
        logger.error(f"[SharedModel] Failed to import fastembed for rubert: {e}")
    finally:
        _rubert_loading = False
    
    return _rubert_model


def is_rubert_loaded() -> bool:
    """Проверить загружена ли rubert модель."""
    return _rubert_model is not None
