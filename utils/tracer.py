"""
tracer.py — Модуль трассировки фильтрации ключевых слов.
Размещается в utils/tracer.py

Отслеживает путь каждого ключа через ВСЕ фильтры:
  1. pre_filter          (дубли seed, повторы)
  2. geo_garbage_filter   (оккупированные, чужие города/страны)
  3. BatchPostFilter      (гео-конфликты внутри parse-методов)
  4. relevance_filter     (релевантность seed'у)
  5. deduplicate          (дубликаты)

Использование:
  from utils.tracer import FilterTracer
  
  tracer = FilterTracer(enabled=True)
  tracer.start_request(seed="ремонт пылесосов", country="ua")
  
  # Перед каждым фильтром:
  tracer.before_filter("pre_filter", keywords)
  # ... фильтрация ...
  tracer.after_filter("pre_filter", filtered_keywords)
  
  # В конце:
  summary = tracer.finish_request()
  # summary содержит полную трассу по каждому ключу
"""

import logging
import time
from typing import List, Dict, Optional, Any
from collections import defaultdict

logger = logging.getLogger("FilterTracer")


class FilterTracer:
    """
    Трассировщик фильтрации.
    Записывает состояние списка ключей ДО и ПОСЛЕ каждого фильтра.
    Позволяет точно видеть: какой фильтр убил какой ключ.
    """

    # Порядок фильтров в пайплайне
    FILTER_ORDER = [
        "pre_filter",
        "geo_garbage_filter",
        "batch_post_filter",
        "deduplicate",
        "l0_filter",
    ]

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._reset()

    def _reset(self):
        """Сброс состояния для нового запроса."""
        self.seed = ""
        self.country = ""
        self.method = ""
        self.start_time = 0.0
        
        # {filter_name: {"before": set(), "after": set(), "blocked": set(), "reasons": {}}}
        self.stages: Dict[str, Dict] = {}
        
        # Итоговая карта: {keyword: {"blocked_by": str, "reason": str, "stage": int}}
        self.keyword_map: Dict[str, Dict] = {}
        
        # Все ключи которые когда-либо появлялись
        self._all_seen: set = set()

    # ─────────────────────────────────────────────
    # API: жизненный цикл запроса
    # ─────────────────────────────────────────────

    def start_request(self, seed: str, country: str, method: str = ""):
        """Начало трассировки нового запроса."""
        if not self.enabled:
            return
        self._reset()
        self.seed = seed
        self.country = country
        self.method = method
        self.start_time = time.time()
        logger.info(f"[TRACER] ▶ START | seed='{seed}' | country={country} | method={method}")

    def before_filter(self, filter_name: str, keywords: List[Any]):
        """Фиксирует список ключей ПЕРЕД фильтром."""
        if not self.enabled:
            return
        
        kw_set = self._extract_keywords(keywords)
        self._all_seen.update(kw_set)
        
        if filter_name not in self.stages:
            self.stages[filter_name] = {
                "before": set(),
                "after": set(),
                "blocked": set(),
                "reasons": {},
                "time": 0.0,
                "_start": time.time(),
            }
        
        self.stages[filter_name]["before"] = kw_set
        self.stages[filter_name]["_start"] = time.time()
        
        logger.debug(f"[TRACER] → {filter_name} | input: {len(kw_set)} keywords")

    def after_filter(self, filter_name: str, keywords: List[Any], 
                     reasons: Optional[Dict[str, str]] = None):
        """
        Фиксирует список ключей ПОСЛЕ фильтра.
        
        Args:
            filter_name: название фильтра
            keywords: оставшиеся ключи
            reasons: опционально {keyword: reason} для заблокированных
        """
        if not self.enabled:
            return
        
        if filter_name not in self.stages:
            logger.warning(f"[TRACER] after_filter('{filter_name}') без before_filter!")
            return
        
        kw_set = self._extract_keywords(keywords)
        stage = self.stages[filter_name]
        
        stage["after"] = kw_set
        stage["blocked"] = stage["before"] - kw_set
        stage["time"] = time.time() - stage.get("_start", time.time())
        
        if reasons:
            stage["reasons"].update(reasons)
        
        # Записываем в keyword_map
        for kw in stage["blocked"]:
            if kw not in self.keyword_map:
                self.keyword_map[kw] = {
                    "blocked_by": filter_name,
                    "reason": stage["reasons"].get(kw, ""),
                }
        
        blocked_count = len(stage["blocked"])
        if blocked_count > 0:
            logger.info(
                f"[TRACER] ✗ {filter_name} | blocked: {blocked_count} | "
                f"passed: {len(kw_set)} | time: {stage['time']:.3f}s"
            )
            # Логируем первые 10 заблокированных
            for kw in sorted(stage["blocked"])[:10]:
                reason = stage["reasons"].get(kw, "—")
                logger.info(f"[TRACER]   ✗ '{kw}' → {reason}")
            if blocked_count > 10:
                logger.info(f"[TRACER]   ... и ещё {blocked_count - 10}")
        else:
            logger.debug(
                f"[TRACER] ✓ {filter_name} | all passed ({len(kw_set)}) | "
                f"time: {stage['time']:.3f}s"
            )

    def after_l0_filter(self, valid: List[Any], trash: List[Any], grey: List[Any],
                        l0_trace: List[Dict] = None):
        """
        Специальный метод для L0 — три исхода (VALID/TRASH/GREY), не два.
        
        Args:
            valid: ключи прошедшие как VALID
            trash: ключи заблокированные как TRASH
            grey: ключи ушедшие в GREY (для perplexity)
            l0_trace: детальный трейс из l0_filter
        """
        if not self.enabled:
            return
        
        filter_name = "l0_filter"
        if filter_name not in self.stages:
            logger.warning(f"[TRACER] after_l0_filter без before_filter!")
            return
        
        stage = self.stages[filter_name]
        valid_set = self._extract_keywords(valid)
        trash_set = self._extract_keywords(trash)
        grey_set = self._extract_keywords(grey)
        
        stage["after"] = valid_set | grey_set  # "прошедшие" = VALID + GREY
        stage["blocked"] = trash_set           # "заблокированные" = TRASH
        stage["grey"] = grey_set               # новое поле для L0
        stage["valid"] = valid_set             # явно VALID
        stage["time"] = time.time() - stage.get("_start", time.time())
        
        # Записываем reasons из l0_trace
        # ВАЖНО: сохраняем трейс для ВСЕХ ключей (valid, grey, trash)
        # чтобы при мега-тесте видеть почему каждый ключ получил свой label
        if l0_trace:
            for rec in l0_trace:
                kw = rec.get("keyword", "").lower().strip()
                reason = rec.get("reason", "")
                signals = rec.get("signals", [])
                label = rec.get("label", "")
                confidence = rec.get("confidence", 0.0)
                decided_by = rec.get("decided_by", "")
                tail = rec.get("tail", "")
                sig_str = ", ".join(signals) if signals else ""
                stage["reasons"][kw] = f"[{label}] {sig_str}: {reason}" if sig_str else f"[{label}] {reason}"
                
                # Детальный трейс для каждого ключа (включая VALID)
                stage.setdefault("l0_details", {})[kw] = {
                    "label": label,
                    "tail": tail,
                    "signals": signals,
                    "reason": reason,
                    "confidence": confidence,
                    "decided_by": decided_by,
                }
                
                if label == "TRASH" and kw not in self.keyword_map:
                    self.keyword_map[kw] = {
                        "blocked_by": "l0_filter",
                        "reason": stage["reasons"].get(kw, ""),
                    }
        
        logger.info(
            f"[TRACER] ◆ l0_filter | VALID: {len(valid_set)} | "
            f"TRASH: {len(trash_set)} | GREY: {len(grey_set)} | "
            f"time: {stage['time']:.3f}s"
        )

    def finish_request(self) -> Dict:
        """
        Завершение трассировки. Возвращает полный отчёт.
        
        Returns:
            {
                "seed": str,
                "country": str,
                "method": str,
                "total_time": float,
                "input_count": int,
                "output_count": int,
                "blocked_count": int,
                "stages": [
                    {
                        "name": str,
                        "input": int,
                        "output": int,
                        "blocked": int,
                        "blocked_keywords": [str],
                        "time": float,
                    }
                ],
                "blocked_keywords": {
                    "keyword": {"blocked_by": str, "reason": str}
                },
                "surviving_keywords": [str],
            }
        """
        if not self.enabled:
            return {}

        total_time = time.time() - self.start_time

        # Собираем input_count из первого фильтра
        first_stage_name = None
        for name in self.FILTER_ORDER:
            if name in self.stages:
                first_stage_name = name
                break
        
        input_count = len(self.stages[first_stage_name]["before"]) if first_stage_name else 0

        # Собираем output_count из последнего фильтра
        last_stage_name = None
        for name in reversed(self.FILTER_ORDER):
            if name in self.stages:
                last_stage_name = name
                break
        
        output_count = len(self.stages[last_stage_name]["after"]) if last_stage_name else 0
        surviving = sorted(self.stages[last_stage_name]["after"]) if last_stage_name else []

        # Собираем стадии
        stages_report = []
        for name in self.FILTER_ORDER:
            if name not in self.stages:
                continue
            s = self.stages[name]
            stages_report.append({
                "name": name,
                "input": len(s["before"]),
                "output": len(s["after"]),
                "blocked": len(s["blocked"]),
                "grey": len(s.get("grey", set())),
                "valid": len(s.get("valid", s["after"])),
                "blocked_keywords": sorted(s["blocked"])[:50],
                "time": round(s["time"], 4),
            })

        report = {
            "seed": self.seed,
            "country": self.country,
            "method": self.method,
            "total_time": round(total_time, 3),
            "input_count": input_count,
            "output_count": output_count,
            "blocked_count": input_count - output_count,
            "stages": stages_report,
            "blocked_keywords": dict(sorted(self.keyword_map.items())),
            "surviving_keywords": surviving,
        }
        
        # Добавляем трейс для VALID ключей — какой фильтр их пропустил и почему
        valid_trace = {}
        for kw in surviving:
            kw_trace = {
                "passed_filters": [],
            }
            # Путь через все фильтры
            for name in self.FILTER_ORDER:
                if name not in self.stages:
                    continue
                s = self.stages[name]
                if kw in s.get("before", set()):
                    kw_trace["passed_filters"].append(name)
            
            # L0 детали (если L0 был включён)
            l0_stage = self.stages.get("l0_filter", {})
            l0_details = l0_stage.get("l0_details", {}).get(kw)
            if l0_details:
                kw_trace["l0"] = l0_details
            
            valid_trace[kw] = kw_trace
        
        report["valid_keywords"] = valid_trace
        
        # Трейс для GREY ключей (L0)
        grey_trace = {}
        l0_stage = self.stages.get("l0_filter", {})
        for kw in sorted(l0_stage.get("grey", set())):
            l0_details = l0_stage.get("l0_details", {}).get(kw)
            if l0_details:
                grey_trace[kw] = l0_details
        report["grey_keywords"] = grey_trace

        # Итоговый лог
        logger.info(
            f"[TRACER] ■ FINISH | seed='{self.seed}' | "
            f"in={input_count} → out={output_count} "
            f"(blocked {input_count - output_count}) | "
            f"time={total_time:.2f}s"
        )
        for s in stages_report:
            if s["blocked"] > 0:
                logger.info(
                    f"[TRACER]   {s['name']}: -{s['blocked']} "
                    f"({s['input']}→{s['output']})"
                )

        return report

    # ─────────────────────────────────────────────
    # Хелперы
    # ─────────────────────────────────────────────

    @staticmethod
    def _extract_keywords(items: List[Any]) -> set:
        """Извлекает строки из списка (поддержка str и dict)."""
        result = set()
        if not items:
            return result
        for item in items:
            if isinstance(item, str):
                result.add(item.lower().strip())
            elif isinstance(item, dict):
                q = item.get("query", "")
                if q:
                    result.add(q.lower().strip())
        return result

    def get_keyword_trace(self, keyword: str) -> Dict:
        """
        Получить трассу конкретного ключа через все фильтры.
        
        Returns:
            {
                "keyword": str,
                "status": "passed" | "blocked",
                "blocked_by": str | None,
                "reason": str,
                "path": [
                    {"filter": str, "result": "passed" | "blocked"}
                ]
            }
        """
        if not self.enabled:
            return {}
        
        kw = keyword.lower().strip()
        path = []
        
        for name in self.FILTER_ORDER:
            if name not in self.stages:
                continue
            s = self.stages[name]
            if kw in s["before"]:
                if kw in s["blocked"]:
                    path.append({
                        "filter": name,
                        "result": "blocked",
                        "reason": s["reasons"].get(kw, ""),
                    })
                    break  # дальше не пошёл
                elif kw in s["after"]:
                    path.append({"filter": name, "result": "passed"})
        
        blocked_info = self.keyword_map.get(kw)
        
        # L0 детали (для любого ключа — valid, grey, trash)
        l0_stage = self.stages.get("l0_filter", {})
        l0_details = l0_stage.get("l0_details", {}).get(kw)
        
        # Определяем статус с учётом L0 grey
        if blocked_info:
            status = "blocked"
        elif l0_details and l0_details.get("label") == "GREY":
            status = "grey"
        else:
            status = "passed"
        
        result = {
            "keyword": kw,
            "status": status,
            "blocked_by": blocked_info["blocked_by"] if blocked_info else None,
            "reason": blocked_info.get("reason", "") if blocked_info else "",
            "path": path,
        }
        
        if l0_details:
            result["l0"] = l0_details
        
        return result

    def format_report_text(self) -> str:
        """Форматирует отчёт в читаемый текст для логов/файлов."""
        report = self.finish_request()
        if not report:
            return "[TRACER disabled]"
        
        lines = [
            f"═══ FILTER TRACE REPORT ═══",
            f"Seed: '{report['seed']}' | Country: {report['country']} | Method: {report['method']}",
            f"Total: {report['input_count']} → {report['output_count']} "
            f"(blocked {report['blocked_count']}) | Time: {report['total_time']}s",
            f"",
            f"── Stages ──",
        ]
        
        for s in report["stages"]:
            marker = "✗" if s["blocked"] > 0 else "✓"
            lines.append(
                f"  {marker} {s['name']}: {s['input']} → {s['output']} "
                f"(-{s['blocked']}) [{s['time']}s]"
            )
            if s["blocked_keywords"]:
                for kw in s["blocked_keywords"][:5]:
                    lines.append(f"      ✗ {kw}")
                if s["blocked"] > 5:
                    lines.append(f"      ... +{s['blocked'] - 5} more")
        
        if report["blocked_keywords"]:
            lines.append(f"")
            lines.append(f"── Blocked keywords ({len(report['blocked_keywords'])}) ──")
            for kw, info in list(report["blocked_keywords"].items())[:20]:
                lines.append(f"  ✗ '{kw}' → {info['blocked_by']}: {info.get('reason', '—')}")
            if len(report["blocked_keywords"]) > 20:
                lines.append(f"  ... +{len(report['blocked_keywords']) - 20} more")
        
        # GREY keywords с L0 деталями
        grey_kws = report.get("grey_keywords", {})
        if grey_kws:
            lines.append(f"")
            lines.append(f"── Grey keywords ({len(grey_kws)}) ──")
            for kw, details in list(grey_kws.items())[:20]:
                tail = details.get("tail", "")
                sigs = ", ".join(details.get("signals", [])) or "—"
                lines.append(f"  ⚠ '{kw}' | tail='{tail}' | signals: {sigs}")
            if len(grey_kws) > 20:
                lines.append(f"  ... +{len(grey_kws) - 20} more")
        
        # VALID keywords с L0 деталями
        valid_kws = report.get("valid_keywords", {})
        if valid_kws:
            lines.append(f"")
            lines.append(f"── Valid keywords ({len(valid_kws)}) ──")
            for kw, info in list(valid_kws.items())[:30]:
                l0 = info.get("l0")
                if l0:
                    tail = l0.get("tail", "")
                    sigs = ", ".join(l0.get("signals", [])) or "—"
                    decided = l0.get("decided_by", "")
                    lines.append(f"  ✓ '{kw}' | tail='{tail}' | signals: {sigs} | by: {decided}")
                else:
                    filters = ", ".join(info.get("passed_filters", []))
                    lines.append(f"  ✓ '{kw}' | filters: {filters}")
            if len(valid_kws) > 30:
                lines.append(f"  ... +{len(valid_kws) - 30} more")
        
        lines.append(f"═══════════════════════════")
        return "\n".join(lines)
