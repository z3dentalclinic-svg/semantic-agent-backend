"""
Embedding Model для L1.5 — singleton с переключателем backend.

ВАЖНО: Этот модуль ДОБАВЛЯЕТСЯ, не заменяет shared_model.py.
Остальные фильтры (L0, L2, CategoryMismatch) продолжают использовать MiniLM 
через shared_model.py.

L1.5 использует свою E5-модель для улучшенной семантики на русском.

Поддерживаемые backend'ы:
- e5_large (intfloat/multilingual-e5-large) — 1024-dim, ~2.2GB, ~85ms/embed CPU
- e5_small (intfloat/multilingual-e5-small) — 384-dim, ~470MB, ~10-15ms/embed CPU

Откат назад: меняем EMBEDDING_BACKEND + рестарт сервера. Без изменений кода.
Пороги в l1_5_filter_v2.py привязаны к backend через _THRESHOLDS_BY_BACKEND.

Префикс 'query: ' одинаков для обеих моделей (это семейство E5).
"""

import logging
import os
import sys
from typing import Optional, List, Iterable
import numpy as np

logger = logging.getLogger(__name__)


def _diag(msg: str) -> None:
    """Диагностический print который ТОЧНО виден в Render startup-логах.

    На уровне импорта модуля logger.info(...) уходит в никуда — uvicorn
    настраивает logging позже. Поэтому используем print в stderr с
    префиксом, который легко грепать.
    """
    print(f"[E5/L1.5/DIAG] {msg}", file=sys.stderr, flush=True)


# ─── Логируем версию fastembed для диагностики ───────────────────────────
_diag("=== e5_model.py module init starting ===")
try:
    import fastembed as _fe
    _fe_version = getattr(_fe, "__version__", "unknown")
    _diag(f"fastembed version: {_fe_version}")
    from fastembed import TextEmbedding as _TE_check
    _has_add_custom = hasattr(_TE_check, "add_custom_model")
    _diag(f"add_custom_model available: {_has_add_custom}")
    try:
        from fastembed.common.model_description import PoolingType as _pt_check, ModelSource as _ms_check
        _diag(f"fastembed.common.model_description: OK")
    except Exception as _md_err:
        _diag(f"fastembed.common.model_description: NOT AVAILABLE ({_md_err})")
except Exception as _diag_err:
    _diag(f"Version diagnostic failed: {_diag_err}")

# ─── Backend конфигурация ────────────────────────────────────────────────
# Переключатель backend. Меняется значение → рестарт сервера → переход
# на новую модель. Пороги в фильтре переключаются автоматически.
EMBEDDING_BACKEND = "e5_small"  # "e5_large" | "e5_small"

_BACKEND_CONFIG = {
    "e5_large": {
        "model_name": "intfloat/multilingual-e5-large",
        "dim": 1024,
        "prefix": "query: ",
        "native_in_fastembed": True,  # fastembed знает эту модель из коробки
    },
    "e5_small": {
        "model_name": "intfloat/multilingual-e5-small",
        "dim": 384,
        "prefix": "query: ",
        "native_in_fastembed": False,  # требует add_custom_model
    },
}

if EMBEDDING_BACKEND not in _BACKEND_CONFIG:
    raise ValueError(
        f"EMBEDDING_BACKEND={EMBEDDING_BACKEND!r} not in {list(_BACKEND_CONFIG)}"
    )

_CFG = _BACKEND_CONFIG[EMBEDDING_BACKEND]
_E5_MODEL_NAME = _CFG["model_name"]
_E5_PREFIX = _CFG["prefix"]

_e5_model = None
_e5_loading = False

# Persistent disk для модели (по умолчанию fastembed качает в /tmp, лимит 2GB
# на Render → eviction. E5-large ~2.2GB не помещается, e5-small ~470MB
# тоже хочется держать persistent чтобы не качать каждый рестарт).
_E5_CACHE_DIR = "/var/data/models"
try:
    os.makedirs(_E5_CACHE_DIR, exist_ok=True)
except Exception as _e:
    logger.error(f"[E5/L1.5] Cannot create {_E5_CACHE_DIR}: {_e}. Falling back to default cache.")
    _E5_CACHE_DIR = None

# Кеш эмбеддингов слов. Лимит выставлен под e5-large (4KB на запись → 40MB).
# Для e5-small запись 1.5KB → можно поднять лимит при необходимости.
_word_emb_cache: dict = {}
_CACHE_MAX = 10000


def _register_custom_models():
    """Регистрирует модели не входящие в нативный список fastembed.

    Возвращает True если всё ок (либо native, либо успешно зарегистрирована),
    False если регистрация провалилась — caller должен сделать fallback.
    """
    if _CFG["native_in_fastembed"]:
        _diag(f"backend={EMBEDDING_BACKEND}: native in fastembed, no registration needed")
        return True

    _diag(f"backend={EMBEDDING_BACKEND}: registering custom model {_E5_MODEL_NAME}")

    try:
        from fastembed import TextEmbedding
        from fastembed.common.model_description import PoolingType, ModelSource
        _diag("custom model: imports OK")
    except Exception as e:
        _diag(f"custom model: imports FAILED: {type(e).__name__}: {e}")
        return False

    # Проверяем — может модель уже в нативном списке (новая версия fastembed)
    try:
        supported = {m.get("model") for m in TextEmbedding.list_supported_models()}
        if _E5_MODEL_NAME in supported:
            _diag(f"custom model: {_E5_MODEL_NAME} already in native fastembed list, skipping registration")
            return True
    except Exception as e:
        _diag(f"custom model: list_supported_models check failed: {e} (continuing to add_custom_model)")

    try:
        TextEmbedding.add_custom_model(
            model=_E5_MODEL_NAME,
            pooling=PoolingType.MEAN,
            normalization=True,
            sources=ModelSource(hf=_E5_MODEL_NAME),
            dim=_CFG["dim"],
            model_file="onnx/model.onnx",
        )
        _diag(f"custom model: add_custom_model({_E5_MODEL_NAME!r}, dim={_CFG['dim']}) OK")
        return True
    except Exception as e:
        _diag(f"custom model: add_custom_model FAILED: {type(e).__name__}: {e}")
        return False


def get_e5_model():
    """Singleton модели для L1.5 (E5-large или E5-small в зависимости от EMBEDDING_BACKEND).

    Если загрузка/регистрация выбранного backend упала — пытается fallback
    на e5_large (если ещё не на нём). Это защищает от ситуации когда
    переключили на e5_small, а он не загрузился по любой причине.
    """
    global _e5_model, _e5_loading, _CFG, _E5_MODEL_NAME, _E5_PREFIX

    if _e5_model is not None:
        return _e5_model

    if _e5_loading:
        import time
        for _ in range(120):
            time.sleep(1)
            if _e5_model is not None:
                return _e5_model
        return None

    _e5_loading = True
    _diag(f"get_e5_model: starting load, backend={EMBEDDING_BACKEND}, model={_E5_MODEL_NAME}")

    try:
        # Регистрируем не-нативные модели в fastembed (e5-small требует add_custom_model)
        registered = _register_custom_models()
        if not registered:
            _diag(f"get_e5_model: registration failed for {_E5_MODEL_NAME}, will try anyway")

        from fastembed import TextEmbedding
        _diag(f"get_e5_model: instantiating TextEmbedding({_E5_MODEL_NAME!r}, cache_dir={_E5_CACHE_DIR!r})")
        if _E5_CACHE_DIR:
            _e5_model = TextEmbedding(_E5_MODEL_NAME, cache_dir=_E5_CACHE_DIR)
        else:
            _e5_model = TextEmbedding(_E5_MODEL_NAME)
        _diag(f"get_e5_model: TextEmbedding OK, dim={_CFG['dim']}")

        # ── ONNX warmup ──
        try:
            import time as _t
            _t0 = _t.perf_counter()
            _ = list(_e5_model.embed([f"{_E5_PREFIX}warmup"]))
            _diag(f"get_e5_model: ONNX warmup OK ({_t.perf_counter() - _t0:.2f}s)")
        except Exception as _w_err:
            _diag(f"get_e5_model: warmup failed (non-fatal): {_w_err}")
    except Exception as e:
        _diag(f"get_e5_model: FAILED to load {_E5_MODEL_NAME}: {type(e).__name__}: {e}")
        import traceback
        _diag(f"get_e5_model: traceback:\n{traceback.format_exc()}")
        _e5_model = None

        # ── Auto-fallback на e5_large если упал e5_small ──
        if EMBEDDING_BACKEND != "e5_large":
            _diag(f"get_e5_model: attempting auto-fallback to e5_large")
            try:
                from fastembed import TextEmbedding
                fallback_cfg = _BACKEND_CONFIG["e5_large"]
                fallback_name = fallback_cfg["model_name"]
                if _E5_CACHE_DIR:
                    _e5_model = TextEmbedding(fallback_name, cache_dir=_E5_CACHE_DIR)
                else:
                    _e5_model = TextEmbedding(fallback_name)
                # Обновляем активные переменные на e5_large
                _CFG = fallback_cfg
                _E5_MODEL_NAME = fallback_name
                _E5_PREFIX = fallback_cfg["prefix"]
                _diag(
                    f"get_e5_model: FALLBACK to {fallback_name} OK "
                    f"(dim={fallback_cfg['dim']}). "
                    f"NOTE: пороги фильтра остаются для {EMBEDDING_BACKEND}, могут быть несовместимы — рестарт с правильным EMBEDDING_BACKEND рекомендуется."
                )
            except Exception as fe:
                _diag(f"get_e5_model: fallback to e5_large also FAILED: {type(fe).__name__}: {fe}")
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
        prefixed = f"{_E5_PREFIX}{word}"
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
            prefixed = [f"{_E5_PREFIX}{w}" for w in batch]
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


# ─── НЕТ auto-load при импорте ────────────────────────────────────────────
# Раньше тут был `try: get_e5_model() except: ...` — грузил модель при
# импорте чтобы первый запрос не платил за ~10s загрузки. Это убрано
# потому что:
#   1. При OOM kill во время загрузки на module-level импорт фильтра
#      может остаться в неконсистентном состоянии (имена становятся None)
#      → 'NoneType' object is not callable в _prove_object.
#   2. Render Shell не может импортировать модуль т.к. модель не помещается
#      в память Shell-процесса. Делает отладку невозможной.
#
# Теперь модель грузится при ПЕРВОМ вызове get_e5_model() — это первый
# пользовательский запрос. Цена: +1-2s на первом запросе (модель уже
# скачана в /var/data/models, читается с диска, не из сети).
_diag(f"=== e5_model.py module init complete, backend={EMBEDDING_BACKEND}, model lazy-load ===")
