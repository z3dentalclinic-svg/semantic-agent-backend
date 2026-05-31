"""
Embedding Model для L1.5 — singleton с переключателем backend.

ВАЖНО: Этот модуль ДОБАВЛЯЕТСЯ, не заменяет shared_model.py.
Остальные фильтры (L0, L2, CategoryMismatch) продолжают использовать MiniLM 
через shared_model.py.

L1.5 использует свою E5-модель для улучшенной семантики на русском.

Поддерживаемые backend'ы:
- e5_large   (intfloat/multilingual-e5-large) — 1024-dim, ~2.2GB, ~85ms/embed CPU, native
- e5_large_q (квантованный int8 e5-large)     — 1024-dim, ~550MB, embed ~3-4x быстрее large
- e5_small   (intfloat/multilingual-e5-small) — 384-dim, ~470MB, ~10-15ms/embed CPU

Откат назад: меняем EMBEDDING_BACKEND + рестарт сервера. Без изменений кода.
Пороги в l1_5_filter_v2.py привязаны к backend через _THRESHOLDS_BY_BACKEND.
Если backend неизвестен фильтру — он дефолтит на пороги e5_large (с warning).

Логи загрузки модели грепаются по '[E5/L1.5/DIAG]'; сводка — 'LOAD BREAKDOWN'.
Префикс 'query: ' одинаков для всех (это семейство E5).
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
EMBEDDING_BACKEND = "e5_large_q"  # "e5_large" | "e5_large_q" | "e5_small"

_BACKEND_CONFIG = {
    "e5_large": {
        "model_name": "intfloat/multilingual-e5-large",
        "dim": 1024,
        "prefix": "query: ",
        "native_in_fastembed": True,   # fastembed знает эту модель из коробки
        "hf_source": None,             # native — не нужен
        "model_file": None,
    },
    "e5_large_q": {
        # Квантованный (int8) e5-large. Качество ≈ e5-large, embed-инференс
        # быстрее в ~3-4x (квантизация ускоряет ТОЛЬКО embed, не загрузку модели).
        #
        # ВНИМАНИЕ: имя onnx-файла "onnx/model_quantized.onnx" — это дефолтное
        # имя transformers.js quantize (которым собран репозиторий), но я НЕ
        # подтвердил его листингом репо. Если instantiate упадёт с
        # "file not found / no such file" — посмотри точное имя командой:
        #   python -c "from huggingface_hub import list_repo_files; \
        #     print([f for f in list_repo_files('morgendigital/multilingual-e5-large-quantized') if f.endswith('.onnx')])"
        # и впиши его в model_file ниже. При падении сработает auto-fallback
        # на e5_large (fp32) — сервис не ляжет, но скорость будет как у fp32.
        "model_name": "morgendigital/multilingual-e5-large-quantized",
        "dim": 1024,
        "prefix": "query: ",
        "native_in_fastembed": False,
        "hf_source": "morgendigital/multilingual-e5-large-quantized",
        "model_file": "onnx/model_quantized.onnx",
    },
    "e5_small": {
        "model_name": "intfloat/multilingual-e5-small",
        "dim": 384,
        "prefix": "query: ",
        "native_in_fastembed": False,  # требует add_custom_model
        "hf_source": "intfloat/multilingual-e5-small",
        "model_file": "onnx/model.onnx",
    },
}

if EMBEDDING_BACKEND not in _BACKEND_CONFIG:
    raise ValueError(
        f"EMBEDDING_BACKEND={EMBEDDING_BACKEND!r} not in {list(_BACKEND_CONFIG)}"
    )

_CFG = _BACKEND_CONFIG[EMBEDDING_BACKEND]
_E5_MODEL_NAME = _CFG["model_name"]
_E5_PREFIX = _CFG["prefix"]

# ─── Замер параллелизма (intra-op потоки onnxruntime) ────────────────────
# По дефолту ORT берёт потоки по числу ВИДИМЫХ ядер (на Render это 32), но
# контейнер CPU-ограничен через cgroup → 32 потока дерутся за реальные ядра
# и тормозят. Явное ограничение часто УСКОРЯЕТ embed.
#
# ЗАМЕР: меняй env E5_ONNX_THREADS (в Render dashboard, без коммита) + рестарт,
# прогоняй один и тот же сид, сравнивай 'e5_warm_total' в логах.
# Пробовать: пусто(дефолт ORT) → 16 → 8 → 4 → 2 → 1.
# Пусто или 0 = дефолт onnxruntime (по видимым ядрам).
# Это intra-op (один процесс, БЕЗ копий модели) — не путать с parallel=
# (мультипроцесс, который для large-модели = OOM, по копии на процесс).
try:
    _E5_ONNX_THREADS = int(os.getenv("E5_ONNX_THREADS", "0")) or None
except (TypeError, ValueError):
    _E5_ONNX_THREADS = None
_diag(f"E5_ONNX_THREADS = {_E5_ONNX_THREADS} (None = onnxruntime default by visible cores)")

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


def _dir_size_mb(path: Optional[str]) -> float:
    """Суммарный размер файлов в каталоге (МБ). Читает только метаданные
    (stat), не содержимое — быстро даже для многогигабайтных моделей.
    Используется для диагностики: был ли download при instantiate."""
    if not path or not os.path.isdir(path):
        return 0.0
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total / (1024.0 * 1024.0)


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

    # Источник и имя onnx-файла берём из конфига backend (с дефолтами для
    # старого формата без этих ключей).
    hf_source = _CFG.get("hf_source") or _E5_MODEL_NAME
    model_file = _CFG.get("model_file") or "onnx/model.onnx"

    try:
        TextEmbedding.add_custom_model(
            model=_E5_MODEL_NAME,
            pooling=PoolingType.MEAN,
            normalization=True,
            sources=ModelSource(hf=hf_source),
            dim=_CFG["dim"],
            model_file=model_file,
        )
        _diag(
            f"custom model: add_custom_model({_E5_MODEL_NAME!r}, dim={_CFG['dim']}, "
            f"hf={hf_source!r}, file={model_file!r}) OK"
        )
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
    import time as _t
    _t_load0 = _t.perf_counter()
    _diag(f"get_e5_model: starting load, backend={EMBEDDING_BACKEND}, model={_E5_MODEL_NAME}")

    dt_reg = dt_inst = dt_warm = 0.0
    downloaded_mb = 0.0
    try:
        # ── Фаза 1: регистрация не-нативных моделей (add_custom_model) ──
        _t_reg = _t.perf_counter()
        registered = _register_custom_models()
        dt_reg = _t.perf_counter() - _t_reg
        if not registered:
            _diag(f"get_e5_model: registration failed for {_E5_MODEL_NAME}, will try anyway")
        _diag(f"get_e5_model: [phase] register = {dt_reg:.2f}s")

        # ── Фаза 2: instantiate TextEmbedding (= download если нет на диске
        #    + инициализация ONNX session). disk-snapshot до/после разделяет
        #    "скачали с HF" (разово) от "прочитали с диска" (каждый рестарт). ──
        from fastembed import TextEmbedding
        disk_before = _dir_size_mb(_E5_CACHE_DIR)
        _diag(
            f"get_e5_model: instantiating TextEmbedding({_E5_MODEL_NAME!r}, "
            f"cache_dir={_E5_CACHE_DIR!r}); cache before = {disk_before:.0f} MB"
        )
        _t_inst = _t.perf_counter()
        _te_kwargs = {}
        if _E5_CACHE_DIR:
            _te_kwargs["cache_dir"] = _E5_CACHE_DIR
        if _E5_ONNX_THREADS is not None:
            _te_kwargs["threads"] = _E5_ONNX_THREADS
        _e5_model = TextEmbedding(_E5_MODEL_NAME, **_te_kwargs)
        dt_inst = _t.perf_counter() - _t_inst
        downloaded_mb = max(0.0, _dir_size_mb(_E5_CACHE_DIR) - disk_before)
        _diag(
            f"get_e5_model: [phase] instantiate = {dt_inst:.2f}s "
            f"(downloaded ~{downloaded_mb:.0f} MB → "
            f"{'DOWNLOAD from HF (one-time)' if downloaded_mb > 5 else 'read from disk/cache'}), "
            f"dim={_CFG['dim']}"
        )

        # ── Фаза 3: ONNX warmup (первый прогон графа) ──
        try:
            _t_warm = _t.perf_counter()
            _ = list(_e5_model.embed([f"{_E5_PREFIX}warmup"]))
            dt_warm = _t.perf_counter() - _t_warm
            _diag(f"get_e5_model: [phase] warmup = {dt_warm:.2f}s")
        except Exception as _w_err:
            _diag(f"get_e5_model: warmup failed (non-fatal): {_w_err}")

        # ── Сводка: одной строкой, грепается как 'LOAD BREAKDOWN' ──
        _diag(
            f"get_e5_model: LOAD BREAKDOWN backend={EMBEDDING_BACKEND} "
            f"threads={_E5_ONNX_THREADS} "
            f"register={dt_reg:.2f}s instantiate={dt_inst:.2f}s "
            f"(download~{downloaded_mb:.0f}MB) warmup={dt_warm:.2f}s "
            f"TOTAL={_t.perf_counter() - _t_load0:.2f}s"
        )
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
                _fb_kwargs = {}
                if _E5_CACHE_DIR:
                    _fb_kwargs["cache_dir"] = _E5_CACHE_DIR
                if _E5_ONNX_THREADS is not None:
                    _fb_kwargs["threads"] = _E5_ONNX_THREADS
                _e5_model = TextEmbedding(fallback_name, **_fb_kwargs)
                # Обновляем активные переменные на e5_large
                _CFG = fallback_cfg
                _E5_MODEL_NAME = fallback_name
                _E5_PREFIX = fallback_cfg["prefix"]
                _diag(
                    f"get_e5_model: FALLBACK to {fallback_name} (fp32) OK "
                    f"(dim={fallback_cfg['dim']}). "
                    f"WARNING: backend {EMBEDDING_BACKEND!r} НЕ загрузился — сейчас работает "
                    f"fp32 e5-large, НЕ квантованная. Скорость embed будет как у fp32. "
                    f"Проверь причину выше (вероятно неверное model_file в _BACKEND_CONFIG)."
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


def bench_embed_threads(words, thread_values=(16, 8, 4, 2)):
    """РАЗОВЫЙ замер: скорость embed одного набора слов при разных
    intra-op потоках onnxruntime.

    Для каждого значения threads создаётся ВРЕМЕННАЯ модель → прогрев →
    замер embed всего набора → выгрузка (del + gc). Пик памяти = основная
    модель сервиса + одна временная (~2.7GB на int8-large при 4GB RAM).
    НЕ трогает _word_emb_cache сервиса — считает напрямую через model.embed,
    поэтому каждое значение меряется «вхолодную» и кэш живого сервиса цел.

    Возвращает list записей: {threads, seconds, ms_per_word, n_words, init_s}
    или {threads, error}.
    """
    import time as _t
    import gc as _gc

    # Нормализация + дедуп слов, префикс E5 один раз
    norm, seen = [], set()
    for w in words:
        if not w or not isinstance(w, str):
            continue
        x = w.strip().lower()
        if not x or x in seen:
            continue
        seen.add(x)
        norm.append(x)
    prefixed = [f"{_E5_PREFIX}{w}" for w in norm]
    n = len(prefixed)
    _diag(f"[BENCH] start: {n} unique words, thread_values={list(thread_values)}, model={_E5_MODEL_NAME}")

    if n == 0:
        return [{"threads": None, "error": "no words to embed"}]

    # Гарантируем что модель зарегистрирована в fastembed (для e5_large_q/e5_small
    # это already-done если сервис прогрет, но вызов безопасен/идемпотентен).
    try:
        get_e5_model()
    except Exception as e:
        _diag(f"[BENCH] get_e5_model warmup failed (continuing): {e}")

    try:
        from fastembed import TextEmbedding
    except Exception as e:
        return [{"threads": None, "error": f"fastembed import failed: {e}"}]

    out = []
    for tv in thread_values:
        m = None
        try:
            kw = {}
            if _E5_CACHE_DIR:
                kw["cache_dir"] = _E5_CACHE_DIR
            if tv is not None:
                kw["threads"] = tv
            t_make = _t.perf_counter()
            m = TextEmbedding(_E5_MODEL_NAME, **kw)
            init_s = _t.perf_counter() - t_make
            # прогрев (не учитываем в замере)
            list(m.embed([f"{_E5_PREFIX}warmup"]))
            # сам замер: embed всего набора
            t0 = _t.perf_counter()
            _ = list(m.embed(prefixed))
            dt = _t.perf_counter() - t0
            rec = {
                "threads": tv,
                "seconds": round(dt, 3),
                "ms_per_word": round(dt * 1000.0 / n, 1),
                "n_words": n,
                "init_s": round(init_s, 2),
            }
            out.append(rec)
            _diag(f"[BENCH] threads={tv}: embed {n} words in {dt:.2f}s ({rec['ms_per_word']} ms/word)")
        except Exception as e:
            out.append({"threads": tv, "error": f"{type(e).__name__}: {e}"})
            _diag(f"[BENCH] threads={tv}: ERROR {type(e).__name__}: {e}")
        finally:
            del m
            _gc.collect()

    _diag(f"[BENCH] done: {out}")
    return out


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
