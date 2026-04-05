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
    ):
        """
        INFIX MAP: Full infix matrix run (Chrome only).

        E-группа: 3 структуры × 26 букв = 78 запросов на gap.
        Остальные (WC/A/B/C/D): ~26 запросов на gap.
        Итого: ~104 запроса на gap, ~208 на 3-токенный сид.

        Returns keywords + detailed infix tracer data.
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
            keywords_for_html.append({
                "keyword": kw,
                "structs": structs,
                "weight": len(structs),
                "exclusive": len(structs) == 1,
                "alt_seed": kw in result.alt_seed_keywords,
            })

        keywords_for_html.sort(key=lambda x: x["weight"], reverse=True)

        return {
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
                "summary_by_gap": result.summary_by_gap,
                "summary_by_group": result.summary_by_group,
                "trace": result.trace,
            },
        }
