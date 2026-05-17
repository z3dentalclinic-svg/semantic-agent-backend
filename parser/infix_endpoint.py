"""
Infix Map API endpoint — drop-in for main.py

Usage in main.py:
    from parser.infix_endpoint import register_infix_endpoint
    register_infix_endpoint(app)

Architecture:
    Chrome only (Firefox убран в v2.1 — ROI < 1%).
    E-буквы: 26 букв параллельно через asyncio.gather.
    Non-E (WC/A/B/C/D): asyncio.Semaphore(BATCH_SIZE=5) параллельно с E.
"""

from fastapi import FastAPI, Query
from typing import Optional

_infix_parser = None


def get_infix_parser():
    global _infix_parser
    if _infix_parser is None:
        from parser.infix_parser import InfixParser
        _infix_parser = InfixParser(lang="ru")
    return _infix_parser


def register_infix_endpoint(app: FastAPI):
    """Register /api/infix-map endpoint on the FastAPI app."""

    @app.get("/api/infix-map")
    async def infix_map_endpoint(
        seed: str = Query(..., description="Базовый сид"),
        country: str = Query("ua", description="Код страны"),
        language: str = Query("ru", description="Язык"),
        groups: Optional[str] = Query(None, description="Группы через запятую: WC,A,B,C,D,E. None = все"),
        city: str = Query(None, description="Город для uule гео-таргетинга (по-английски). None = столица страны."),
        debug: int = Query(0, description="1 = добавить полную трассу (per-request entries, structs массив, summary_by_gap/group)"),
    ):
        """
        INFIX MAP: Full infix matrix run (Chrome only).

        Production mode (debug=0): только ключи и stage_stats/trace_log (легкие тайминги).
        Debug mode (debug=1): + per-request trace, structs массив, summary_by_gap/group.
        """
        ip = get_infix_parser()

        selected_groups = [g.strip() for g in groups.split(",")] if groups else None

        result = await ip.parse(
            seed=seed,
            country=country,
            language=language,
            groups=selected_groups,
            city=city,
        )

        keywords_for_html = []
        for kw, structs in result.all_keywords.items():
            entry = {
                "keyword": kw,
                "weight": len(structs),
                "exclusive": len(structs) == 1,
                "alt_seed": kw in result.alt_seed_keywords,
            }
            if debug:
                entry["structs"] = structs  # тяжёлое поле, только в дебаге
            keywords_for_html.append(entry)

        keywords_for_html.sort(key=lambda x: x["weight"], reverse=True)

        # Базовый ответ — лёгкий, всегда включает только stage_stats/trace_log
        response = {
            "method": "infix-map",
            "seed": seed,
            "keywords": keywords_for_html,
            "keywords_grey": [],
            "anchors": [],
            "total": len(keywords_for_html),
            "alt_seed_count": len(result.alt_seed_keywords),
            "time": f"{result.total_time_ms:.0f}ms",
            "infix_trace": {
                "total_keywords": result.total_keywords,
                "total_time_ms": result.total_time_ms,
                "total_queries": result.total_queries,
                "with_results": result.with_results,
                "empty_queries": result.empty_queries,
                "error_queries": result.error_queries,
                "exclusive_count": result.exclusive_count,
                "groups_used": result.groups_used,
                "stage_stats": result.stage_stats,
            },
            "infixStageStats": result.stage_stats,  # на верхнем уровне для удобства
            "infixTraceLog": result.trace_log,      # лёгкий trace ~12 событий
        }

        # Debug-only поля
        if debug:
            response["infix_trace"]["summary_by_gap"] = result.summary_by_gap
            response["infix_trace"]["summary_by_group"] = result.summary_by_group
            response["infix_trace"]["trace"] = result.trace  # per-request entries (тяжёлое)

        return response
