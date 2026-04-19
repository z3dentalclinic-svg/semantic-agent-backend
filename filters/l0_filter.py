"""
l0_filter.py — Серверная обёртка L0 классификатора.

Встраивается в пайплайн ПОСЛЕ всех существующих фильтров:
    pre_filter → geo_garbage → batch_post → deduplicate → L0

Принимает result dict с keywords → классифицирует каждый ключ →
разделяет на VALID / TRASH / GREY.

Результат:
    result["keywords"]       → VALID ключи (высокая уверенность)
    result["keywords_grey"]  → GREY ключи (для будущего perplexity)
    result["anchors"]       += TRASH ключи с пометкой [L0]
    result["_l0_trace"]      → детальный трейсинг каждого ключа
    result["_filter_timings"]["l0"] → тайминги L0 (extract_tail / classify / total)

ОПТИМИЗАЦИИ:
1. Персистентный кэш TailFunctionClassifier — не пересоздаётся на каждый батч.
   Кэш по ключу (seed, target_country). geo_db/brand_db/retailer_db загружаются
   один раз.
2. build_seed_ctx() вызывается один раз до цикла — seed одинаков для всего батча.
3. _filter_timings добавляется к result — для профилирования горячих точек.
"""

import logging
import time
from typing import Dict, List, Set, Any, Optional

from .tail_extractor import extract_tail, build_seed_ctx
from .tail_function_classifier import TailFunctionClassifier

logger = logging.getLogger(__name__)


# ============================================================
# Персистентный кэш классификатора.
# TailFunctionClassifier создаётся ОДИН РАЗ на (seed, target_country)
# и переиспользуется во всех последующих запросах с тем же seed.
#
# ВАЖНО: кэш хранит ссылку на geo_db / brand_db / retailer_db.
# Если эти базы пересоздаются при каждом запросе (новый dict/set) —
# кэш теряет смысл. Убедитесь что bases загружаются при старте сервера
# и передаются одним и тем же объектом.
# ============================================================
_CLASSIFIER_CACHE: Dict[tuple, TailFunctionClassifier] = {}


# ============================================================
# Буквы эксклюзивно украинского алфавита.
# Конечный набор из 4 символов (+ верхний регистр). Не ниша, не хардкод слов —
# базовое свойство алфавита. Используется для быстрой детекции UA-запросов
# в RU-пайплайне: если в kw есть хоть одна такая буква — это не RU, TRASH.
# ============================================================
_UA_EXCLUSIVE_LETTERS = frozenset('іїєґІЇЄҐ')


# ============================================================
# Fallback-загрузка retailer_db — модульный singleton.
# Если вызывающий код не передаёт retailer_db явно (backward-compat),
# грузим один раз при первом обращении и переиспользуем далее.
# ============================================================
_RETAILER_DB_FALLBACK: Optional[Set[str]] = None


def _get_fallback_retailer_db() -> Set[str]:
    """Ленивая загрузка retailer_db из databases.load_retailers_db().

    Вызывается только когда apply_l0_filter не получил retailer_db извне.
    Результат кэшируется на уровне модуля — один load_retailers_db() на
    весь процесс.
    """
    global _RETAILER_DB_FALLBACK
    if _RETAILER_DB_FALLBACK is None:
        try:
            from .databases import load_retailers_db
            _RETAILER_DB_FALLBACK = load_retailers_db()
            logger.info(
                "[L0] Loaded retailer_db fallback: %d retailers",
                len(_RETAILER_DB_FALLBACK),
            )
        except Exception as e:
            logger.warning("[L0] Failed to load retailer_db fallback: %s", e)
            _RETAILER_DB_FALLBACK = set()
    return _RETAILER_DB_FALLBACK


def _get_classifier(
    seed: str,
    target_country: str,
    geo_db: Dict,
    brand_db: Set,
    retailer_db: Set,
) -> TailFunctionClassifier:
    """
    Возвращает персистентный классификатор для данного (seed, target_country).
    Создаёт новый только при первом вызове или при смене seed/country.
    """
    key = (seed.lower().strip(), target_country.lower())
    if key not in _CLASSIFIER_CACHE:
        logger.info(
            "[L0] Creating new classifier for seed='%s' country='%s' "
            "(geo_db=%d, brand_db=%d, retailer_db=%d)",
            seed, target_country,
            len(geo_db) if geo_db else 0,
            len(brand_db) if brand_db else 0,
            len(retailer_db) if retailer_db else 0,
        )
        _CLASSIFIER_CACHE[key] = TailFunctionClassifier(
            geo_db=geo_db,
            brand_db=brand_db,
            seed=seed,
            target_country=target_country,
            retailer_db=retailer_db,
        )
    return _CLASSIFIER_CACHE[key]


def apply_l0_filter(
    result: Dict[str, Any],
    seed: str,
    target_country: str = "ua",
    geo_db: Dict[str, Set[str]] = None,
    brand_db: Set[str] = None,
    retailer_db: Set[str] = None,
) -> Dict[str, Any]:
    """
    Применяет L0 классификатор к списку ключевых слов.

    Args:
        result: dict с ключами "keywords", "anchors"
        seed: базовый запрос
        target_country: целевая страна
        geo_db: база городов Dict[str, Set[str]] (название → {коды_стран})
        brand_db: база брендов (set). Если None — пустой set
        retailer_db: база ритейлеров/маркетплейсов (set). Если None — грузится
                     fallback через databases.load_retailers_db() (backward-compat).

    Returns:
        result с обновлёнными keywords, keywords_grey, anchors, _l0_trace, _filter_timings
    """
    t_l0_start = time.perf_counter()
    # Детальные тайминги этапов L0 для диагностики узких мест
    _t_stage = {}

    keywords = result.get("keywords", [])
    if not keywords:
        result.setdefault("keywords_grey", [])
        result.setdefault("_l0_trace", [])
        _add_timings(result, 0.0, 0.0, 0.0)
        return result

    if geo_db is None:
        geo_db = {}
    if brand_db is None:
        brand_db = set()
    # Если вызывающий код не передал retailer_db — грузим сами.
    # Это backward-compat для старых вызовов apply_l0_filter(..., brand_db=...).
    if retailer_db is None:
        retailer_db = _get_fallback_retailer_db()

    # ── Pre-compute seed_ctx ОДИН РАЗ для всего батча ──────────────────────
    # Seed одинаков для всех ключей → лематизация seed не повторяется N раз.
    _t = time.perf_counter()
    seed_lower = seed.lower().strip()
    seed_ctx = build_seed_ctx(seed_lower)
    _t_stage['seed_ctx'] = time.perf_counter() - _t

    # ── Персистентный классификатор ─────────────────────────────────────────
    _t = time.perf_counter()
    clf = _get_classifier(seed, target_country, geo_db, brand_db, retailer_db)
    _t_stage['get_classifier'] = time.perf_counter() - _t

    # ── Глобальный словарь morph-парсов для всего батча ────────────────────
    from .shared_morph import morph as _morph

    t_extract_total = 0.0
    t_classify_total = 0.0

    # Первый проход: собрать все хвосты и уникальные слова
    _t = time.perf_counter()
    _kw_tail_pairs = []
    _all_tail_words: set = set()
    for kw_item in keywords:
        kw = kw_item.strip() if isinstance(kw_item, str) else kw_item.get("query", "").strip()
        if not kw:
            continue
        _t0 = time.perf_counter()
        tail = extract_tail(kw, seed, seed_ctx=seed_ctx)
        t_extract_total += time.perf_counter() - _t0
        _kw_tail_pairs.append((kw_item, kw, tail))
        if tail:
            _all_tail_words.update(tail.lower().split())
    _t_stage['collect_tails_loop'] = time.perf_counter() - _t

    # Один проход morph.parse для всех уникальных слов
    _t = time.perf_counter()
    tail_parses = {w: _morph.parse(w) for w in _all_tail_words}
    _t_stage['morph_parse_batch'] = time.perf_counter() - _t

    # ── Pre-batch embeddings для category_mismatch ───────────────────────────
    # category_mismatch имеет каскад Stage 1 (chargram) → Stage 2 (MiniLM).
    # Индивидуальный inference на Stage 2 — главная причина тормозов L0.
    # Оптимизация: заранее вычисляем embeddings ОДНИМ батчем для tails,
    # которые попадут в Stage 2 (chargram в зоне 0.05-0.20). Stage 1 TRASH
    # и PASS не требуют embedding'а и в pre_batch не идут.
    #
    # Экономия: 7-8 секунд (индивидуальные вызовы MiniLM) → ~0.3 секунды
    # (один батч для всех проблемных tails).
    _t = time.perf_counter()
    _stage2_count = 0
    try:
        from .category_mismatch_detector import (
            get_category_detector, _chargram_similarity, CategoryConfig
        )
        _cm_detector = get_category_detector()
        _cm_cfg = _cm_detector.config
        # Собираем tails-кандидаты для Stage 2 (между порогами chargram)
        _seed_for_cm = seed_lower
        _stage2_tails = []
        _seen = set()
        for _, _, tail in _kw_tail_pairs:
            if not tail:
                continue
            t_clean = tail.lower().strip()
            if t_clean in _seen:
                continue
            _seen.add(t_clean)
            cg = _chargram_similarity(_seed_for_cm, t_clean, _cm_cfg.ngram_size)
            if _cm_cfg.chargram_low < cg < _cm_cfg.chargram_high:
                _stage2_tails.append(t_clean)
        _stage2_count = len(_stage2_tails)
        if _stage2_tails:
            _cm_detector.pre_batch(_stage2_tails)
    except ImportError:
        # category_mismatch_detector не доступен (stub в тестах) — пропускаем
        pass
    except Exception as e:
        logger.warning("[L0] pre_batch for category_mismatch failed: %s", e)
    _t_stage['pre_batch_cm'] = time.perf_counter() - _t
    _t_stage['_stage2_count'] = _stage2_count

    valid_keywords = []
    grey_keywords = []
    trash_keywords = []
    trace_records = []

    # Замер всего основного цикла (включая classify, UA check, NO_SEED, etc.)
    _t_loop_start = time.perf_counter()
    for kw_item, kw, tail in _kw_tail_pairs:

        # ── UA-язык → TRASH (ДО любой другой классификации) ───────────────
        # Пайплайн настроен под RU-морфологию (pymorphy3, RU детекторы).
        # UA-запросы через него не проходят корректно — нужна отдельная
        # UA-копия фильтров (отложенный проект).
        #
        # Алгоритмический критерий: буквы {і, ї, є, ґ} существуют в UA
        # и отсутствуют в RU. Одна такая буква в kw = украинский запрос.
        # Проверяется на всём kw (а не на tail), чтобы ловить случаи
        # где seed не найден из-за UA-морфологии.
        #
        # Не трогает:
        #   - чистую латиницу (all on 6, straumann) — свои детекторы
        #   - смешанный алфавит (рrice) — detect_mixed_alphabet
        if _UA_EXCLUSIVE_LETTERS.intersection(kw):
            trash_keywords.append(kw_item)
            trace_records.append({
                "keyword": kw,
                "tail": tail,
                "label": "TRASH",
                "decided_by": "l0",
                "reason": "Не русский язык: UA-буквы (і/ї/є/ґ) — нужна UA-копия пайплайна",
                "signals": ["-wrong_language"],
            })
            continue

        # NO_SEED → GREY
        if tail is None:
            grey_keywords.append(kw_item)
            trace_records.append({
                "keyword": kw,
                "tail": None,
                "label": "GREY",
                "decided_by": "l0",
                "reason": "seed не найден → GREY",
                "signals": [],
            })
            continue

        # Пустой хвост = запрос совпадает с seed → VALID
        if not tail:
            valid_keywords.append(kw_item)
            trace_records.append({
                "keyword": kw,
                "tail": "",
                "label": "VALID",
                "decided_by": "l0",
                "reason": "запрос = seed",
                "signals": ["exact_seed"],
            })
            continue

        # ── classify с глобальным tail_parses ───────────────────────────────
        t0 = time.perf_counter()
        r = clf.classify(tail, tail_parses=tail_parses, kw=kw)
        t_classify_total += time.perf_counter() - t0

        label = r["label"]
        all_signals = r["positive_signals"] + [f"-{s}" for s in r["negative_signals"]]

        trace_record = {
            "keyword": kw,
            "tail": tail,
            "label": label,
            "decided_by": "l0",
            "reason": "; ".join(r["reasons"][:3]),
            "signals": all_signals,
            "confidence": r["confidence"],
        }
        trace_records.append(trace_record)

        if label == "VALID":
            valid_keywords.append(kw_item)
        elif label == "TRASH":
            trash_keywords.append(kw_item)
        else:  # GREY
            grey_keywords.append(kw_item)

    # ── Обновляем result ─────────────────────────────────────────────────────
    _t_stage['main_loop'] = time.perf_counter() - _t_loop_start

    result["keywords"] = valid_keywords
    result["count"] = len(valid_keywords)

    result["keywords_grey"] = grey_keywords
    result["keywords_grey_count"] = len(grey_keywords)

    # TRASH → якоря
    existing_anchors = result.get("anchors", [])
    for kw_item in trash_keywords:
        kw = kw_item if isinstance(kw_item, str) else kw_item.get("query", "")
        existing_anchors.append(kw)
    result["anchors"] = existing_anchors
    result["anchors_count"] = len(existing_anchors)

    result["_l0_trace"] = trace_records

    # ── Сохраняем диагностику ────────────────────────────────────────────────
    _t = time.perf_counter()
    try:
        import json as _json
        diag = {
            "seed": seed,
            "target_country": target_country,
            "stats": {
                "total": len(trace_records),
                "valid": len(valid_keywords),
                "trash": len(trash_keywords),
                "grey": len(grey_keywords),
                "no_seed": sum(1 for r in trace_records if r.get("tail") is None),
            },
            "trace": trace_records,
        }
        with open("l0_diagnostic.json", "w", encoding="utf-8") as f:
            _json.dump(diag, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("[L0] Failed to save diagnostic: %s", e)
    _t_stage['save_diagnostic'] = time.perf_counter() - _t

    # ── Тайминги ─────────────────────────────────────────────────────────────
    t_l0_total = time.perf_counter() - t_l0_start
    _add_timings(result, t_extract_total, t_classify_total, t_l0_total)

    # Детальный лог этапов — видно где 8 секунд прячется
    _stages_str = " | ".join(f"{k}={v:.3f}s" if isinstance(v, float) else f"{k}={v}"
                              for k, v in _t_stage.items())
    logger.info("[L0_STAGES] %s", _stages_str)

    # ── Лог итогов ──────────────────────────────────────────────────────────
    total = len(trace_records)
    v = len(valid_keywords)
    t = len(trash_keywords)
    g = len(grey_keywords)

    logger.info(
        "[L0] seed='%s' | total=%d | VALID=%d (%d%%) | TRASH=%d (%d%%) | GREY=%d (%d%%)",
        seed, total,
        v, v * 100 // total if total else 0,
        t, t * 100 // total if total else 0,
        g, g * 100 // total if total else 0,
    )
    logger.info(
        "[L0_PROFILE] n=%d | extract_tail=%.3fs | classify=%.3fs | total=%.3fs",
        total, t_extract_total, t_classify_total, t_l0_total,
    )

    # ── Per-detector тайминги (топ по убыванию) ─────────────────────────────
    if clf.detector_timings:
        sorted_det = sorted(clf.detector_timings.items(), key=lambda x: -x[1])
        logger.info(
            "[L0_DETECTORS] %s",
            " | ".join(f"{k}={v:.3f}s" for k, v in sorted_det)
        )
        clf.detector_timings.clear()  # сброс после батча

    return result


def _add_timings(result: dict, t_extract: float, t_classify: float, t_total: float):
    """Добавляет L0-тайминги к _filter_timings result'а."""
    if "_filter_timings" not in result:
        result["_filter_timings"] = {}
    result["_filter_timings"]["l0"] = {
        "extract_tail": round(t_extract, 4),
        "classify": round(t_classify, 4),
        "total": round(t_total, 4),
    }
