"""
Shared Embedding Models — singletons для L2 и CategoryMismatch.

Модели:
1. MiniLM (основная) — multilingual embeddings для KNN, CategoryMismatch
"""

import logging

logger = logging.getLogger(__name__)

_model = None
_model_loading = False


def get_embedding_model():
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
    return _model is not None


# Заглушка — rubert убран, но другие файлы могут импортировать
def get_rubert_mlm():
    return None

def is_rubert_loaded() -> bool:
    return False
