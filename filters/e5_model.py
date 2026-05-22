"""
E5-large Model для L1.5 — отдельный singleton.

ВАЖНО: Этот модуль ДОБАВЛЯЕТСЯ, не заменяет shared_model.py.
Остальные фильтры (L0, L2, CategoryMismatch) продолжают использовать MiniLM 
через shared_model.py.

Только L1.5 использует E5-large для улучшенной семантики на русском.

Модель: intfloat/multilingual-e5-large
- 1024-dim embeddings
- SOTA для multilingual (2024+)
- Размер на диске: ~2.2GB
- RAM при загрузке: ~1.0-1.2GB
- ВАЖНО: требует префиксы 'query: ' / 'passage: '
  Для symmetric similarity (наш случай — сравнение слов) используем 'query: '
"""

import logging
import os
from typing import Optional, List, Iterable
import numpy as np

logger = logging.getLogger(__name__)

_e5_model = None
_e5_loading = False
_E5_MODEL_NAME = "intfloat/multilingual-e5-large"

# Persistent disk для модели (по умолчанию fastembed качает в /tmp, лимит 2GB
# на Render → eviction. E5-large ~2.2GB не помещается).
_E5_CACHE_DIR = "/var/data/models"
try:
    os.makedirs(_E5_CACHE_DIR, exist_ok=True)
except Exception as _e:
    logger.error(f"[E5/L1.5] Cannot create {_E5_CACHE_DIR}: {_e}. Falling back to default cache.")
    _E5_CACHE_DIR = None

# Кеш эмбеддингов слов
_word_emb_cache: dict = {}
_CACHE_MAX = 10000


def get_e5_model():
    """Singleton E5-large для L1.5."""
    global _e5_model, _e5_loading

    if _e5_model is not None:
        return _e5_model

    if _e5_loading:
        import time
        for _ in range(120):  # E5-large грузится 1-2 минуты на первый раз
            time.sleep(1)
            if _e5_model is not None:
                return _e5_model
        return None

    _e5_loading = True

    try:
        from fastembed import TextEmbedding
        logger.info(f"[E5/L1.5] Loading {_E5_MODEL_NAME}...")
        logger.info(f"[E5/L1.5] cache_dir={_E5_CACHE_DIR or 'default(/tmp)'}")
        logger.info("[E5/L1.5] First-time download ~2.2GB, may take 1-2 minutes")
        if _E5_CACHE_DIR:
            _e5_model = TextEmbedding(_E5_MODEL_NAME, cache_dir=_E5_CACHE_DIR)
        else:
            _e5_model = TextEmbedding(_E5_MODEL_NAME)
        logger.info("[E5/L1.5] Loaded successfully (1024-dim embeddings)")
    except Exception as e:
        logger.error(f"[E5/L1.5] Failed to load E5-large: {e}")
        logger.error(f"[E5/L1.5] L1.5 will fall back to no-semantic mode")
        _e5_model = None
    finally:
        _e5_loading = False

    return _e5_model


def is_e5_loaded() -> bool:
    return _e5_model is not None


def get_e5_word_embedding(word: str) -> Optional[np.ndarray]:
    """
    Получить E5-эмбеддинг для слова/фразы.
    Применяет префикс 'query: ' автоматически (требование E5 для symmetric similarity).
    Результаты нормализованы и кешируются.
    """
    if not word or not isinstance(word, str):
        return None
    
    word = word.strip().lower()
    if not word:
        return None
    
    # Кеш
    if word in _word_emb_cache:
        return _word_emb_cache[word]
    
    model = get_e5_model()
    if model is None:
        return None
    
    try:
        # E5 prefix: 'query: ' для symmetric similarity 
        # (sentence-to-sentence, classification, clustering)
        prefixed = f"query: {word}"
        embeddings = list(model.embed([prefixed]))
        if not embeddings:
            return None
        
        emb = embeddings[0]
        if isinstance(emb, list):
            emb = np.array(emb, dtype=np.float32)
        
        # E5 уже нормализован, но проверим на всякий случай
        norm = np.linalg.norm(emb)
        if norm > 0:
            emb = emb / norm
        
        # Кеш с ограничением размера
        if len(_word_emb_cache) < _CACHE_MAX:
            _word_emb_cache[word] = emb
        
        return emb
    except Exception as e:
        logger.warning(f"[E5/L1.5] Embedding failed for '{word}': {e}")
        return None


def warm_e5_word_cache(words: Iterable[str], batch_size: int = 64) -> int:
    """Предкомпьютит embeddings для слов одним батчем и сохраняет в кеш.

    Это критично для скорости L1.5: индивидуальные вызовы get_e5_word_embedding
    запускают ONNX runtime на каждое слово (overhead ~50ms на E5-large CPU).
    Батч-вызов работает в 10-30x быстрее на CPU.

    После этой функции get_e5_word_embedding(w) для всех `words` мгновенно
    возвращает значение из кеша.

    Возвращает кол-во новых embeddings которые были посчитаны (без учёта тех
    что уже были в кеше или пустых слов).
    """
    if not words:
        return 0

    # Нормализация + дедупликация + отбор тех что НЕТ в кеше
    to_embed: List[str] = []
    seen: set = set()
    for w in words:
        if not w or not isinstance(w, str):
            continue
        norm = w.strip().lower()
        if not norm or norm in seen or norm in _word_emb_cache:
            continue
        seen.add(norm)
        to_embed.append(norm)

    if not to_embed:
        return 0

    model = get_e5_model()
    if model is None:
        return 0

    # Уважаем лимит кеша
    free_slots = _CACHE_MAX - len(_word_emb_cache)
    if free_slots <= 0:
        return 0
    if len(to_embed) > free_slots:
        to_embed = to_embed[:free_slots]

    computed = 0
    # Батчим: одной model.embed([...]) call. fastembed внутри сам разобьёт
    # по сублимиту onnx если надо. Поэтому отдаём batch_size за раз — это
    # компромисс между latency и memory peak.
    try:
        for start in range(0, len(to_embed), batch_size):
            batch = to_embed[start:start + batch_size]
            prefixed = [f"query: {w}" for w in batch]
            embeddings = list(model.embed(prefixed))
            for word, emb in zip(batch, embeddings):
                if emb is None:
                    continue
                if isinstance(emb, list):
                    emb = np.array(emb, dtype=np.float32)
                # E5 уже нормализован, проверим
                norm = np.linalg.norm(emb)
                if norm > 0:
                    emb = emb / norm
                _word_emb_cache[word] = emb
                computed += 1
    except Exception as e:
        logger.warning(f"[E5/L1.5] Batch warm failed: {e}")

    return computed


def e5_cosine_sim(emb1: Optional[np.ndarray], emb2: Optional[np.ndarray]) -> float:
    """Cosine similarity между двумя эмбеддингами (предполагается что нормализованы)."""
    if emb1 is None or emb2 is None:
        return 0.0
    try:
        # Эмбеддинги нормализованы → dot product = cosine
        return float(np.dot(emb1, emb2))
    except Exception:
        return 0.0


def clear_e5_cache():
    """Очистить кеш эмбеддингов (для тестов / отладки памяти)."""
    global _word_emb_cache
    _word_emb_cache.clear()


def get_e5_cache_size() -> int:
    return len(_word_emb_cache)
