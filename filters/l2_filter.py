"""
L2 Filter — Semantic Classifier (Dual Cosine)

Слой 2 фильтрации: обрабатывает GREY хвосты из L0 через embeddings.

Два сигнала:
1. Combined: cosine(embed(seed), embed(seed + tail))
   - Высокий = tail хорошо дополняет seed
   
2. Direct: cosine(embed(seed), embed(tail))
   - Средний/высокий = tail семантически связан с seed

Модель: paraphrase-multilingual-MiniLM-L12-v2 (~470MB ONNX через fastembed)

Результат:
- VALID → добавляется к keywords
- TRASH → добавляется к anchors  
- GREY → остаётся для L3 (DeepSeek API)
"""

import os
import json
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
import numpy as np

from .shared_model import get_embedding_model

logger = logging.getLogger(__name__)


@dataclass
class L2Config:
    """Конфигурация L2 классификатора."""
    
    # Модель fastembed
    # Варианты: 
    #   "intfloat/multilingual-e5-large" (~560MB, лучше качество)
    #   "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2" (~470MB)
    embedding_model: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    
    # Пороги для Cosine Combined (seed vs seed+tail)
    # Высокий = tail хорошо дополняет seed
    combined_valid_threshold: float = 0.92   # выше → VALID
    combined_trash_threshold: float = 0.75   # ниже → TRASH
    
    # Пороги для Cosine Direct (seed vs tail)
    # Средний = tail семантически связан с seed
    direct_valid_threshold: float = 0.50    # выше → VALID
    direct_trash_threshold: float = 0.25    # ниже → TRASH
    
    # Веса для combined scoring
    combined_weight: float = 0.6
    direct_weight: float = 0.4
    
    # Итоговые пороги (weighted sum)
    final_valid_threshold: float = 0.70
    final_trash_threshold: float = 0.45
    
    # Режим: "weighted" | "conservative" | "any_trash"
    # weighted: взвешенная сумма двух сигналов
    # conservative: оба сигнала должны согласиться
    # any_trash: если хоть один сигнал = TRASH → TRASH
    combination_mode: str = "conservative"
    
    # Кэш
    cache_file: str = "l2_cache.json"


class L2Classifier:
    """
    Слой 2: Dual Cosine классификатор через fastembed.
    
    Два сигнала:
    1. Combined: cosine(embed(seed), embed(seed + tail))
    2. Direct: cosine(embed(seed), embed(tail))
    
    Принимает GREY хвосты от L0, возвращает VALID/GREY/TRASH.
    GREY остаётся для L3 (DeepSeek API).
    """
    
    def __init__(self, config: Optional[L2Config] = None):
        self.config = config or L2Config()
        self._embedder = None
        self._cache: Dict[str, dict] = {}
        self._load_cache()
    
    def _load_cache(self):
        """Загрузить кэш из файла."""
        if os.path.exists(self.config.cache_file):
            try:
                with open(self.config.cache_file, 'r', encoding='utf-8') as f:
                    self._cache = json.load(f)
                logger.info(f"Loaded {len(self._cache)} cached entries")
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
    
    def _save_cache(self):
        """Сохранить кэш в файл."""
        try:
            with open(self.config.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")
    
    @property
    def embedder(self):
        """Lazy load fastembed model via shared singleton."""
        if self._embedder is None:
            self._embedder = get_embedding_model()
            if self._embedder is None:
                raise RuntimeError("L2 classifier unavailable: embedding model failed to load")
        return self._embedder
    
    @property
    def is_available(self) -> bool:
        """Проверить доступность L2 классификатора."""
        try:
            _ = self.embedder
            return True
        except Exception:
            return False
    
    def _normalize(self, v: np.ndarray) -> np.ndarray:
        """L2 normalize vector."""
        norm = np.linalg.norm(v)
        return v / norm if norm > 0 else v
    
    def _compute_scores(self, seed: str, tails: List[str]) -> Tuple[np.ndarray, np.ndarray]:
        """
        Вычислить два cosine scores для каждого tail.
        
        Returns:
            (combined_scores, direct_scores)
        """
        # Для paraphrase-multilingual prefix не нужен
        # (E5 модели требуют "query: ", но MiniLM — нет)
        seed_text = seed
        combined_texts = [f"{seed} {tail}" for tail in tails]
        tail_texts = tails
        
        # Собираем все тексты для батч-эмбеддинга
        all_texts = [seed_text] + combined_texts + tail_texts
        
        # fastembed возвращает generator, конвертируем в list
        embeddings = list(self.embedder.embed(all_texts))
        embeddings = np.array(embeddings)
        
        # Разбираем результаты
        seed_emb = self._normalize(embeddings[0])
        combined_embs = embeddings[1:len(tails)+1]
        tail_embs = embeddings[len(tails)+1:]
        
        # Нормализуем
        combined_embs = np.array([self._normalize(e) for e in combined_embs])
        tail_embs = np.array([self._normalize(e) for e in tail_embs])
        
        # Cosine similarities (dot product нормализованных векторов)
        combined_scores = np.dot(combined_embs, seed_emb)
        direct_scores = np.dot(tail_embs, seed_emb)
        
        return combined_scores, direct_scores
    
    def _classify_single(
        self,
        combined_score: float,
        direct_score: float
    ) -> Tuple[str, dict]:
        """
        Классифицировать один хвост по двум сигналам.
        
        Returns:
            (label, debug_info)
        """
        cfg = self.config
        debug = {
            "combined": round(combined_score, 4),
            "direct": round(direct_score, 4)
        }
        
        # Определяем голоса каждого сигнала
        combined_vote = (
            "VALID" if combined_score >= cfg.combined_valid_threshold
            else "TRASH" if combined_score < cfg.combined_trash_threshold
            else "GREY"
        )
        direct_vote = (
            "VALID" if direct_score >= cfg.direct_valid_threshold
            else "TRASH" if direct_score < cfg.direct_trash_threshold
            else "GREY"
        )
        
        debug["combined_vote"] = combined_vote
        debug["direct_vote"] = direct_vote
        
        if cfg.combination_mode == "conservative":
            # Оба сигнала должны согласиться
            if combined_vote == direct_vote:
                return combined_vote, debug
            else:
                return "GREY", debug
        
        elif cfg.combination_mode == "any_trash":
            # Агрессивная фильтрация: если хоть один = TRASH → TRASH
            if combined_vote == "TRASH" or direct_vote == "TRASH":
                return "TRASH", debug
            elif combined_vote == "VALID" and direct_vote == "VALID":
                return "VALID", debug
            else:
                return "GREY", debug
        
        else:  # weighted
            # Взвешенная сумма
            weighted = (
                cfg.combined_weight * combined_score +
                cfg.direct_weight * direct_score
            )
            debug["weighted"] = round(weighted, 4)
            
            if weighted >= cfg.final_valid_threshold:
                return "VALID", debug
            elif weighted < cfg.final_trash_threshold:
                return "TRASH", debug
            return "GREY", debug
    
    def classify_tails(
        self,
        seed: str,
        tails: List[str],
        return_debug: bool = False
    ) -> Dict[str, List]:
        """
        Классифицировать список хвостов.
        
        Args:
            seed: Базовый запрос
            tails: Список хвостов для классификации
            return_debug: Включить debug info для калибровки
        
        Returns:
            {
                "valid": [{"tail": ..., "debug": ...}, ...],
                "grey": [...],
                "trash": [...]
            }
        """
        if not tails:
            return {"valid": [], "grey": [], "trash": []}
        
        # Проверяем кэш
        uncached_tails = []
        uncached_indices = []
        results = [None] * len(tails)
        
        for i, tail in enumerate(tails):
            cache_key = f"{seed}||{tail}".lower()
            if cache_key in self._cache:
                cached = self._cache[cache_key]
                results[i] = (cached["label"], cached.get("debug", {}))
            else:
                uncached_tails.append(tail)
                uncached_indices.append(i)
        
        cache_hits = len(tails) - len(uncached_tails)
        if cache_hits:
            logger.info(f"Cache: {cache_hits} hits, {len(uncached_tails)} misses")
        
        # Вычисляем для некэшированных
        if uncached_tails:
            combined_scores, direct_scores = self._compute_scores(seed, uncached_tails)
            
            # Классифицируем
            for i, (tail, comb, direct) in enumerate(zip(uncached_tails, combined_scores, direct_scores)):
                label, debug = self._classify_single(float(comb), float(direct))
                idx = uncached_indices[i]
                results[idx] = (label, debug)
                
                # Кэшируем
                cache_key = f"{seed}||{tail}".lower()
                self._cache[cache_key] = {"label": label, "debug": debug}
            
            self._save_cache()
        
        # Группируем результаты
        output = {"valid": [], "grey": [], "trash": []}
        
        for tail, (label, debug) in zip(tails, results):
            entry = {"tail": tail}
            if return_debug:
                entry["debug"] = debug
            output[label.lower()].append(entry)
        
        logger.info(
            f"L2 Results: {len(output['valid'])} VALID, "
            f"{len(output['grey'])} GREY, "
            f"{len(output['trash'])} TRASH"
        )
        
        return output
    
    def classify_l0_result(
        self,
        l0_result: Dict[str, Any],
        seed: str
    ) -> Dict[str, Any]:
        """
        Обработать результат L0, классифицировать GREY.
        
        Args:
            l0_result: Результат от L0 с keywords_grey и _l0_trace
            seed: Базовый запрос
        
        Returns:
            Обновлённый результат с L2 классификацией
        
        Note:
            Если L0 пометил GREY с негативными сигналами (orphan_genitive,
            single_infinitive и т.д.), L2 НЕ может промоутить в VALID.
            Максимум GREY → уходит в L3 для семантического решения.
        """
        grey_keywords = l0_result.get("keywords_grey", [])
        
        # === Собираем L0 негативные сигналы для каждого keyword ===
        l0_trace = l0_result.get("_l0_trace", [])
        l0_negative_signals: Dict[str, List[str]] = {}
        for trace in l0_trace:
            kw = trace.get("keyword", "")
            signals = trace.get("signals", [])
            neg = [s.lstrip('-') for s in signals if s.startswith('-')]
            if neg:
                l0_negative_signals[kw] = neg
        
        # Извлекаем хвосты
        tails = []
        tail_to_kw = {}
        
        for kw in grey_keywords:
            if isinstance(kw, dict):
                tail = kw.get("tail") or kw.get("keyword", "")
            else:
                tail = str(kw)
            
            if tail:
                tails.append(tail)
                tail_to_kw[tail] = kw
        
        if not tails:
            logger.info("No GREY tails to classify")
            return l0_result
        
        # Классифицируем
        classified = self.classify_tails(seed, tails, return_debug=True)
        
        # === Понижаем VALID → GREY если L0 имел негативные сигналы ===
        # L0 структурно нашёл проблему → cosine similarity не может переспорить
        downgraded = []
        still_valid = []
        
        for item in classified["valid"]:
            tail = item["tail"]
            kw = tail_to_kw.get(tail)
            # Ищем keyword для проверки L0 сигналов
            keyword = kw.get("keyword", tail) if isinstance(kw, dict) else tail
            
            l0_neg = l0_negative_signals.get(keyword, [])
            if l0_neg:
                # L0 нашёл структурную проблему — L2 не может промоутить
                item_debug = item.get("debug", {})
                item_debug["l2_original"] = "VALID"
                item_debug["downgraded_by"] = f"L0 signals: {', '.join(l0_neg)}"
                downgraded.append(item)
                logger.debug(f"L2 downgrade: '{tail}' VALID→GREY (L0: {l0_neg})")
            else:
                still_valid.append(item)
        
        classified["valid"] = still_valid
        classified["grey"] = classified["grey"] + downgraded
        
        # Собираем результат
        result = l0_result.copy()
        
        # VALID из L0 + VALID из L2
        l2_valid = [
            tail_to_kw[item["tail"]]
            for item in classified["valid"]
            if item["tail"] in tail_to_kw
        ]
        result["keywords"] = l0_result.get("keywords", []) + l2_valid
        
        # TRASH из L0 + TRASH из L2
        l2_trash = []
        for item in classified["trash"]:
            tail = item["tail"]
            if tail in tail_to_kw:
                kw = tail_to_kw[tail]
                if isinstance(kw, dict):
                    kw = kw.copy()
                    kw["anchor_reason"] = "L2_TRASH"
                    kw["l2_debug"] = item.get("debug", {})
                else:
                    kw = {
                        "keyword": kw,
                        "tail": tail,
                        "anchor_reason": "L2_TRASH",
                        "l2_debug": item.get("debug", {})
                    }
                l2_trash.append(kw)
        
        result["anchors"] = l0_result.get("anchors", []) + l2_trash
        
        # GREY остаётся для L3
        l2_grey = [
            tail_to_kw[item["tail"]]
            for item in classified["grey"]
            if item["tail"] in tail_to_kw
        ]
        result["keywords_grey"] = l2_grey
        
        # Статистика
        result["l2_stats"] = {
            "input_grey": len(tails),
            "l2_valid": len(classified["valid"]),
            "l2_trash": len(classified["trash"]),
            "l2_grey": len(classified["grey"]),
            "l2_downgraded": len(downgraded),
            "reduction_pct": round(
                (1 - len(classified["grey"]) / len(tails)) * 100, 1
            ) if tails else 0
        }
        
        # Детальный трейс L2 для каждого ключа
        l2_trace = []
        for category, label in [("valid", "VALID"), ("trash", "TRASH"), ("grey", "GREY")]:
            for item in classified[category]:
                tail = item["tail"]
                debug = item.get("debug", {})
                kw = tail_to_kw.get(tail)
                keyword = kw.get("keyword", tail) if isinstance(kw, dict) else tail
                l2_trace.append({
                    "keyword": keyword,
                    "tail": tail,
                    "label": label,
                    "combined_score": debug.get("combined", 0),
                    "direct_score": debug.get("direct", 0),
                    "combined_vote": debug.get("combined_vote", ""),
                    "direct_vote": debug.get("direct_vote", ""),
                })
        result["_l2_trace"] = l2_trace
        
        return result


# === Singleton instance для использования в main.py ===
_l2_instance: Optional[L2Classifier] = None

def get_l2_classifier(config: Optional[L2Config] = None) -> L2Classifier:
    """Получить singleton instance L2 классификатора."""
    global _l2_instance
    if _l2_instance is None:
        _l2_instance = L2Classifier(config)
    return _l2_instance


def apply_l2_filter(
    l0_result: Dict[str, Any],
    seed: str,
    enable_l2: bool = True,
    config: Optional[L2Config] = None
) -> Dict[str, Any]:
    """
    Обёртка для применения L2 фильтра к результату L0.
    
    Args:
        l0_result: Результат от apply_l0_filter с keywords_grey
        seed: Базовый запрос
        enable_l2: Включить L2 (False = passthrough)
        config: Кастомный L2Config (пороги из UI). None = дефолтные пороги.
    
    Returns:
        Результат с классифицированными GREY:
        - keywords: VALID из L0 + VALID из L2
        - anchors: TRASH из L0 + TRASH из L2
        - keywords_grey: оставшиеся GREY для L3
        - l2_stats: статистика L2
        - l2_config: использованные пороги
    
    Note:
        При ошибке L2 возвращает l0_result без изменений (graceful degradation).
    """
    if not enable_l2:
        return l0_result
    
    grey_count = len(l0_result.get("keywords_grey", []))
    if grey_count == 0:
        logger.info("L2: No GREY to process, skipping")
        return l0_result
    
    try:
        logger.info(f"L2: Processing {grey_count} GREY keywords")
        
        classifier = get_l2_classifier()
        
        # Применяем кастомный config если передан
        if config is not None:
            classifier.config = config
            logger.info(f"L2: Using custom config: mode={config.combination_mode}, comb_valid={config.combined_valid_threshold}, comb_trash={config.combined_trash_threshold}")
        
        result = classifier.classify_l0_result(l0_result, seed)
        
        # Добавляем использованные пороги в результат
        used_config = classifier.config
        result["l2_config"] = {
            "combined_valid_threshold": used_config.combined_valid_threshold,
            "combined_trash_threshold": used_config.combined_trash_threshold,
            "direct_valid_threshold": used_config.direct_valid_threshold,
            "direct_trash_threshold": used_config.direct_trash_threshold,
            "combination_mode": used_config.combination_mode,
        }
        
        stats = result.get("l2_stats", {})
        logger.info(
            f"L2: {stats.get('l2_valid', 0)} VALID, "
            f"{stats.get('l2_trash', 0)} TRASH, "
            f"{stats.get('l2_grey', 0)} GREY remaining "
            f"({stats.get('reduction_pct', 0)}% reduction)"
        )
        
        return result
    
    except Exception as e:
        logger.error(f"L2: Failed to classify, returning L0 result unchanged: {e}")
        l0_result["l2_error"] = str(e)
        return l0_result


# === CLI для тестирования и калибровки ===

def test_classifier():
    """Тест на примерах из контекста."""
    
    classifier = L2Classifier()
    
    seed = "аккумулятор на скутер"
    
    # Тестовые хвосты разных категорий
    test_tails = [
        # Ожидаем VALID (характеристики, модели, действия)
        "гелевый",
        "12 вольт",
        "литиевый",
        "купить",
        "цена",
        "honda dio",
        "ямаха",
        
        # Ожидаем TRASH (мусор, нерелевантное)
        "щербет",
        "жало",
        "навигатор",
        "макита",  # cross-domain brand
        
        # Спорные (GREY → L3)
        "чертеж",
        "жалобы",
        "из чего состоит",
        "нужны ли права",
    ]
    
    print(f"\nSeed: {seed}")
    print(f"Testing {len(test_tails)} tails...\n")
    
    results = classifier.classify_tails(seed, test_tails, return_debug=True)
    
    print("=== VALID ===")
    for item in results["valid"]:
        d = item["debug"]
        print(f"  {item['tail']:20} | comb={d['combined']:.3f} ({d.get('combined_vote','')}) direct={d['direct']:.3f} ({d.get('direct_vote','')})")
    
    print("\n=== GREY ===")
    for item in results["grey"]:
        d = item["debug"]
        print(f"  {item['tail']:20} | comb={d['combined']:.3f} ({d.get('combined_vote','')}) direct={d['direct']:.3f} ({d.get('direct_vote','')})")
    
    print("\n=== TRASH ===")
    for item in results["trash"]:
        d = item["debug"]
        print(f"  {item['tail']:20} | comb={d['combined']:.3f} ({d.get('combined_vote','')}) direct={d['direct']:.3f} ({d.get('direct_vote','')})")


def calibrate_thresholds(
    seed: str,
    labeled_data: List[Tuple[str, str]]  # [(tail, expected_label), ...]
):
    """
    Калибровка порогов на размеченных данных.
    
    Выводит распределение scores для каждого класса
    чтобы подобрать оптимальные пороги.
    """
    classifier = L2Classifier()
    
    tails = [t for t, _ in labeled_data]
    labels = {t: l for t, l in labeled_data}
    
    results = classifier.classify_tails(seed, tails, return_debug=True)
    
    # Собираем scores по ожидаемым классам
    scores_by_expected = {"VALID": [], "GREY": [], "TRASH": []}
    
    all_items = results["valid"] + results["grey"] + results["trash"]
    
    for item in all_items:
        tail = item["tail"]
        expected = labels.get(tail, "UNKNOWN")
        if expected in scores_by_expected:
            scores_by_expected[expected].append({
                "tail": tail,
                "combined": item["debug"]["combined"],
                "direct": item["debug"]["direct"],
                "predicted": item["debug"].get("combined_vote", "")
            })
    
    print(f"\n{'='*60}")
    print(f"Calibration for seed: {seed}")
    print(f"{'='*60}\n")
    
    for label in ["VALID", "TRASH", "GREY"]:
        items = scores_by_expected[label]
        if not items:
            continue
        
        combined = [x["combined"] for x in items]
        direct = [x["direct"] for x in items]
        
        print(f"{label} ({len(items)} samples):")
        print(f"  Combined: min={min(combined):.3f} max={max(combined):.3f} mean={np.mean(combined):.3f}")
        print(f"  Direct:   min={min(direct):.3f} max={max(direct):.3f} mean={np.mean(direct):.3f}")
        
        for item in items:
            status = "✓" if item["predicted"] == label else "✗"
            print(f"    {status} {item['tail']:20} comb={item['combined']:.3f} direct={item['direct']:.3f}")
        print()


def raw_scores(seed: str, tails: List[str]):
    """
    Вывести сырые scores без классификации.
    Полезно для начальной калибровки порогов.
    """
    classifier = L2Classifier()
    
    combined_scores, direct_scores = classifier._compute_scores(seed, tails)
    
    print(f"\n{'='*60}")
    print(f"Raw scores for seed: {seed}")
    print(f"{'='*60}\n")
    print(f"{'Tail':25} | {'Combined':>10} | {'Direct':>10}")
    print("-" * 50)
    
    for tail, comb, direct in zip(tails, combined_scores, direct_scores):
        print(f"{tail:25} | {comb:10.4f} | {direct:10.4f}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "calibrate":
        # Пример калибровки
        labeled = [
            ("гелевый", "VALID"),
            ("12 вольт", "VALID"),
            ("ямаха", "VALID"),
            ("купить", "VALID"),
            ("литиевый", "VALID"),
            ("honda dio", "VALID"),
            ("щербет", "TRASH"),
            ("макита", "TRASH"),
            ("навигатор", "TRASH"),
            ("жало", "TRASH"),
            ("чертеж", "GREY"),
            ("жалобы", "GREY"),
            ("из чего состоит", "GREY"),
        ]
        calibrate_thresholds("аккумулятор на скутер", labeled)
    
    elif len(sys.argv) > 1 and sys.argv[1] == "raw":
        # Сырые scores
        tails = [
            "гелевый", "12 вольт", "ямаха", "купить",
            "щербет", "макита", "навигатор",
            "чертеж", "жалобы", "из чего состоит"
        ]
        raw_scores("аккумулятор на скутер", tails)
    
    else:
        test_classifier()
