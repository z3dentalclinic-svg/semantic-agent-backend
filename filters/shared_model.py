"""
Shared Embedding Models — singletons для L2 и CategoryMismatch.

Модели:
1. MiniLM (основная) — multilingual embeddings для KNN, CategoryMismatch
2. rubert-tiny2 MLM (опциональная) — fill-mask для substitution test

Загружаются ОДИН раз, переиспользуются всеми модулями.
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

# Путь к предварительно экспортированному ONNX (build script)
RUBERT_LOCAL_PATH = "./models/rubert-tiny2-onnx"


def get_rubert_mlm():
    """
    Получить singleton rubert-tiny2 для fill-mask scoring.
    
    Грузит из локальной папки (ONNX, экспортирован при build).
    torch НЕ нужен в runtime — только onnxruntime.
    
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
        import os
        from optimum.onnxruntime import ORTModelForMaskedLM
        from transformers import AutoTokenizer
        
        if not os.path.exists(RUBERT_LOCAL_PATH):
            logger.warning(
                f"[SharedModel] rubert ONNX not found at {RUBERT_LOCAL_PATH}. "
                "Run build script to export. Substitution test disabled."
            )
            return None
        
        logger.info(f"[SharedModel] Loading rubert MLM from {RUBERT_LOCAL_PATH}...")
        model = ORTModelForMaskedLM.from_pretrained(RUBERT_LOCAL_PATH)
        tokenizer = AutoTokenizer.from_pretrained(RUBERT_LOCAL_PATH)
        
        _rubert_mlm = {"model": model, "tokenizer": tokenizer}
        logger.info("[SharedModel] rubert MLM loaded successfully (~40MB)")
        
    except ImportError as e:
        logger.warning(f"[SharedModel] rubert requires optimum: {e}")
    except Exception as e:
        logger.error(f"[SharedModel] Failed to load rubert MLM: {e}")
    finally:
        _rubert_loading = False
    
    return _rubert_mlm


def is_rubert_loaded() -> bool:
    return _rubert_mlm is not None
