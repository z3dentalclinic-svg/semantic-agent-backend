"""
Shared Embedding Models — singletons для L2 и CategoryMismatch.

Модели:
1. MiniLM (основная) — multilingual embeddings для KNN, CategoryMismatch
2. rubert-tiny2 MLM (опциональная) — fill-mask для substitution test
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# === MiniLM (основная) ===
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


# === rubert-tiny2 MLM (для substitution test) ===
_rubert_mlm = None
_rubert_loading = False
_rubert_attempted = False


def get_rubert_mlm():
    """
    Получить singleton rubert-tiny2 для fill-mask scoring.
    Возвращает dict {"model": ..., "tokenizer": ...} или None.
    """
    global _rubert_mlm, _rubert_loading, _rubert_attempted
    
    if _rubert_mlm is not None:
        return _rubert_mlm
    
    if _rubert_attempted:
        return None
    
    if _rubert_loading:
        import time
        for _ in range(30):
            time.sleep(1)
            if _rubert_mlm is not None:
                return _rubert_mlm
        return None
    
    _rubert_loading = True
    _rubert_attempted = True
    
    try:
        from transformers import AutoTokenizer, AutoModelForMaskedLM
        
        model_name = "cointegrated/rubert-tiny2"
        logger.info(f"[SharedModel] Loading rubert MLM: {model_name}...")
        
        model = AutoModelForMaskedLM.from_pretrained(model_name)
        model.eval()
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        
        _rubert_mlm = {"model": model, "tokenizer": tokenizer}
        logger.info("[SharedModel] rubert MLM loaded successfully")
        
    except ImportError as e:
        logger.warning(f"[SharedModel] rubert requires transformers + torch: {e}")
    except Exception as e:
        logger.error(f"[SharedModel] Failed to load rubert MLM: {e}")
    finally:
        _rubert_loading = False
    
    return _rubert_mlm


def is_rubert_loaded() -> bool:
    return _rubert_mlm is not None
