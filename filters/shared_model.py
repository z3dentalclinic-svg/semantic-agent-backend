"""
Shared Embedding Model — singleton для L2 и CategoryMismatch.

Загружает модель ОДИН раз, переиспользуется всеми модулями.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_model = None
_model_loading = False


def get_embedding_model():
    """
    Получить singleton модели embeddings.
    Ленивая загрузка — грузится при первом вызове.
    """
    global _model, _model_loading
    
    if _model is not None:
        return _model
    
    if _model_loading:
        # Уже грузится в другом потоке — подождать
        import time
        for _ in range(30):  # max 30 sec
            time.sleep(1)
            if _model is not None:
                return _model
        return None
    
    _model_loading = True
    
    try:
        from fastembed import TextEmbedding
        logger.info("[SharedModel] Loading embedding model...")
        _model = TextEmbedding("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
        logger.info("[SharedModel] Model loaded successfully")
    except Exception as e:
        logger.error(f"[SharedModel] Failed to load model: {e}")
        _model = None
    finally:
        _model_loading = False
    
    return _model


def is_model_loaded() -> bool:
    """Проверить загружена ли модель."""
    return _model is not None
