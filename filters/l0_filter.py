"""
l0_filter.py — Серверная обёртка L0 классификатора1.

Встраивается в пайплайн ПОСЛЕ всех существующих фильтров:
    pre_filter → geo_garbage → batch_post → deduplicate → L0

Принимает result dict с keywords → классифицирует каждый ключ → 
разделяет на VALID / TRASH / GREY.

Результат:
    result["keywords"]       → VALID ключи (высокая уверенность)
    result["keywords_grey"]  → GREY ключи (для будущего perplexity)
    result["anchors"]       += TRASH ключи с пометкой [L0]
    result["_l0_trace"]      → детальный трейсинг каждого ключа
"""

import logging
from typing import Dict, List, Set, Any

from .tail_extractor import extract_tail
from .tail_function_classifier import TailFunctionClassifier

logger = logging.getLogger(__name__)


def apply_l0_filter(
    result: Dict[str, Any],
    seed: str,
    target_country: str = "ua",
    geo_db: Dict[str, Set[str]] = None,
    brand_db: Set[str] = None,
) -> Dict[str, Any]:
    """
    Применяет L0 классификатор к списку ключевых слов.
    
    Args:
        result: dict с ключами "keywords", "anchors"
        seed: базовый запрос
        target_country: целевая страна
        geo_db: база городов Dict[str, Set[str]] (название → {коды_стран})
        brand_db: база брендов (set). Если None — пустой set
    
    Returns:
        result с обновлёнными keywords, keywords_grey, anchors, _l0_trace
    """
    keywords = result.get("keywords", [])
    if not keywords:
        result.setdefault("keywords_grey", [])
        result.setdefault("_l0_trace", [])
        return result
    
    if geo_db is None:
        geo_db = {}
    if brand_db is None:
        brand_db = set()
    
    clf = TailFunctionClassifier(
        geo_db=geo_db,
        brand_db=brand_db,
        seed=seed,
        target_country=target_country,
    )
    
    valid_keywords = []
    grey_keywords = []
    trash_keywords = []
    trace_records = []
    
    for kw_item in keywords:
        # Поддержка str и dict форматов
        if isinstance(kw_item, str):
            kw = kw_item.strip()
        elif isinstance(kw_item, dict):
            kw = kw_item.get("query", "").strip()
        else:
            valid_keywords.append(kw_item)
            continue
        
        if not kw:
            continue
        
        # Извлекаем хвост
        tail = extract_tail(kw, seed)
        
        # NO_SEED → GREY (не можем классифицировать, пусть perplexity решит)
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
        
        # Классификация хвоста
        r = clf.classify(tail)
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
    
    # --- Обновляем result ---
    
    # keywords = только VALID
    result["keywords"] = valid_keywords
    result["count"] = len(valid_keywords)
    
    # keywords_grey = для будущего perplexity
    result["keywords_grey"] = grey_keywords
    result["keywords_grey_count"] = len(grey_keywords)
    
    # TRASH → якоря с пометкой [L0]
    existing_anchors = result.get("anchors", [])
    for kw_item in trash_keywords:
        kw = kw_item if isinstance(kw_item, str) else kw_item.get("query", "")
        existing_anchors.append(kw)
    result["anchors"] = existing_anchors
    result["anchors_count"] = len(existing_anchors)
    
    # Трейсинг
    result["_l0_trace"] = trace_records
    
    # Сохраняем диагностику в файл (аналогично l2_diagnostic.json)
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
        logger.warning(f"[L0] Failed to save diagnostic: {e}")
    
    # Статистика для лога
    total = len(trace_records)
    v = len(valid_keywords)
    t = len(trash_keywords)
    g = len(grey_keywords)
    
    logger.info(
        f"[L0] seed='{seed}' | total={total} | "
        f"VALID={v} ({v*100//total if total else 0}%) | "
        f"TRASH={t} ({t*100//total if total else 0}%) | "
        f"GREY={g} ({g*100//total if total else 0}%)"
    )
    
    return result
