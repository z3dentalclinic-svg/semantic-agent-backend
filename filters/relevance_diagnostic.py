"""
relevance_diagnostic.py — diagnostic-модуль для GREY-зоны.

НЕ ПРИНИМАЕТ РЕШЕНИЙ. Только считает 5 cosine-сигналов и пишет в результат.
Цель — собрать статистику на реальных датасетах для подбора порогов.

Сигналы (на каждый GREY-ключ):
  A1: cos(seed, kw)                       — общая близость
  B1: cos(seed, tail)                     — близость хвоста (если tail есть)
  B2: max_i cos(object_seed, word_i_tail) — лучшее слово в tail vs объект seed
  C1: cos(kw, centroid_tail_VALID)        — центроид валидного домена
  C2: KNN top-3 mean(kw, L0_VALID_tails)  — устойчивее centroid

Где object_seed — последнее существительное seed-а (для "доставка цветов" → "цветок").

Использование из main.py:
  from filters.relevance_diagnostic import compute_relevance_diagnostic
  data = compute_relevance_diagnostic(data, seed)

Зависимости: numpy, pymorphy3, shared_model.get_embedding_model() (MiniLM).
"""

import logging
import time
from typing import Dict, Any, List, Optional

import numpy as np

logger = logging.getLogger(__name__)


def _norm(v: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(v)
    return v / n if n > 0 else v


def _cos(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.dot(_norm(a), _norm(b)))


def _get_object_seed(seed: str) -> str:
    """
    Объект seed = последняя NOUN-лемма.
    "доставка цветов" → "цветок"
    "купить аккумулятор" → "аккумулятор"
    "ремонт пылесосов" → "пылесос"
    Fallback: последнее слово seed.
    """
    try:
        from .shared_morph import morph
    except Exception:
        try:
            import pymorphy3
            morph = pymorphy3.MorphAnalyzer(lang='ru')
        except Exception as e:
            logger.warning("[relevance_diag] pymorphy3 unavailable: %s", e)
            return seed.split()[-1] if seed.split() else seed

    noun_lemmas = []
    for w in seed.split():
        try:
            p = morph.parse(w)[0]
            if 'NOUN' in p.tag:
                noun_lemmas.append(p.normal_form)
        except Exception:
            continue
    if noun_lemmas:
        return noun_lemmas[-1]
    return seed.split()[-1] if seed.split() else seed


def compute_relevance_diagnostic(
    data: Dict[str, Any],
    seed: str,
) -> Dict[str, Any]:
    """
    Считает 5 cosine-сигналов для всех GREY-ключей в data["keywords_grey"].

    Записывает результат в data["_relevance_diag"]:
        {
            "seed": "...",
            "object_seed": "...",
            "l0_valid_tails_count": N,
            "grey_processed": M,
            "elapsed_s": float,
            "results": [
                {
                    "keyword": "...",
                    "tail": "..." | None,
                    "l0_reason": "...",
                    "A1_cos_kw_seed": 0.xxxx,
                    "B1_cos_tail_seed": 0.xxxx | None,
                    "B2_max_word_obj": 0.xxxx | None,
                    "C1_cos_kw_centroid": 0.xxxx,
                    "C2_knn_top3_mean": 0.xxxx,
                },
                ...
            ]
        }

    Безопасно: при любой ошибке (нет модели, нет данных) пишет в data
    пустой diag-блок с error-полем и возвращает data без изменений.
    """
    t_start = time.perf_counter()

    diag = {
        "seed": seed,
        "object_seed": None,
        "l0_valid_tails_count": 0,
        "grey_processed": 0,
        "elapsed_s": 0.0,
        "results": [],
        "error": None,
    }
    data["_relevance_diag"] = diag

    grey = data.get("keywords_grey", [])
    l0_trace = data.get("_l0_trace", [])

    if not grey:
        diag["error"] = "no_grey_keywords"
        return data
    if not l0_trace:
        diag["error"] = "no_l0_trace"
        return data

    # Загрузка модели
    try:
        from .shared_model import get_embedding_model
    except Exception as e:
        diag["error"] = f"shared_model_import_failed: {e}"
        return data

    model = get_embedding_model()
    if model is None:
        diag["error"] = "embedder_unavailable"
        return data

    # Маппинг kw → trace
    trace_map = {t.get("keyword", "").lower().strip(): t for t in l0_trace}

    # L0 VALID хвосты (только реально валидные от L0, не L2-promoted)
    l0_valid_tails = [
        (t.get("tail") or "").lower().strip()
        for t in l0_trace
        if t.get("label") == "VALID" and t.get("tail")
    ]
    l0_valid_tails = [t for t in l0_valid_tails if t]  # не пустые

    if not l0_valid_tails:
        diag["error"] = "no_l0_valid_tails"
        return data

    diag["l0_valid_tails_count"] = len(l0_valid_tails)

    # Object seed
    object_seed = _get_object_seed(seed.lower().strip())
    diag["object_seed"] = object_seed

    # Сбор всех уникальных текстов для одного batch-вызова embedder
    seed_l = seed.lower().strip()
    texts = {seed_l, object_seed}

    grey_keys = []
    for kw in grey:
        kw_l = kw.lower().strip()
        grey_keys.append((kw, kw_l))
        texts.add(kw_l)
        t = trace_map.get(kw_l)
        if t and t.get("tail"):
            tail = t["tail"].lower().strip()
            texts.add(tail)
            for w in tail.split():
                if len(w) >= 2:
                    texts.add(w)

    for tail in l0_valid_tails:
        texts.add(tail)

    text_list = list(texts)

    # Batch embed
    try:
        emb_list = list(model.embed(text_list))
    except Exception as e:
        diag["error"] = f"embed_failed: {e}"
        return data

    emb_map = {t: np.array(e) for t, e in zip(text_list, emb_list)}

    # Centroid + KNN-пул из L0_VALID tails
    valid_tail_embs = np.array([emb_map[t] for t in l0_valid_tails if t in emb_map])
    if len(valid_tail_embs) == 0:
        diag["error"] = "no_valid_tail_embeddings"
        return data
    centroid_valid_tail = valid_tail_embs.mean(axis=0)
    valid_tail_embs_n = np.array([_norm(e) for e in valid_tail_embs])

    seed_emb = emb_map[seed_l]
    obj_emb = emb_map[object_seed]

    results = []
    for kw_orig, kw_l in grey_keys:
        kw_emb = emb_map.get(kw_l)
        if kw_emb is None:
            continue
        t = trace_map.get(kw_l, {})
        tail = (t.get("tail") or "").lower().strip()
        l0_reason = (t.get("reason") or "")[:200]

        # A1
        a1 = _cos(kw_emb, seed_emb)

        # B1, B2
        b1 = None
        b2 = None
        if tail:
            tail_emb = emb_map.get(tail)
            if tail_emb is not None:
                b1 = _cos(tail_emb, seed_emb)
            word_cos = []
            for w in tail.split():
                if len(w) < 2:
                    continue
                we = emb_map.get(w)
                if we is not None:
                    word_cos.append(_cos(we, obj_emb))
            if word_cos:
                b2 = max(word_cos)

        # C1
        c1 = _cos(kw_emb, centroid_valid_tail)

        # C2: KNN top-3 mean
        kw_n = _norm(kw_emb)
        sims = valid_tail_embs_n @ kw_n
        k = min(3, len(sims))
        top_k = np.sort(sims)[-k:]
        c2 = float(top_k.mean())

        results.append({
            "keyword": kw_orig,
            "tail": tail or None,
            "l0_reason": l0_reason,
            "A1_cos_kw_seed": round(a1, 4),
            "B1_cos_tail_seed": round(b1, 4) if b1 is not None else None,
            "B2_max_word_obj": round(b2, 4) if b2 is not None else None,
            "C1_cos_kw_centroid": round(c1, 4),
            "C2_knn_top3_mean": round(c2, 4),
        })

    diag["results"] = results
    diag["grey_processed"] = len(results)
    diag["elapsed_s"] = round(time.perf_counter() - t_start, 3)

    logger.info(
        "[relevance_diag] seed='%s' object='%s' grey=%d valid_tails=%d elapsed=%.2fs",
        seed, object_seed, len(results), len(l0_valid_tails), diag["elapsed_s"],
    )

    return data
