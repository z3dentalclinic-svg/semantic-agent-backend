"""
Prefix Map API endpoint — drop-in for main.py

Usage in main.py:
    from parser.prefix_endpoint import register_prefix_endpoint
    register_prefix_endpoint(app)

Architecture:
    Dual-agent: Chrome + Firefox запускаются параллельно внутри parse().
    PA группа → Chrome only.
    G1-G9 + PC → Chrome + Firefox.
    google_client параметр не нужен — агенты определяются автоматически.
"""

from fastapi import FastAPI, Query
from typing import Optional

_prefix_parser = None


def get_prefix_parser():
    global _prefix_parser
    if _prefix_parser is None:
        from parser.prefix_parser import PrefixParser
        _prefix_parser = PrefixParser(lang="ru")
    return _prefix_parser


def register_prefix_endpoint(app: FastAPI):
    """Register /api/prefix-map endpoint on the FastAPI app."""

    @app.get("/api/prefix-map")
    async def prefix_map_endpoint(
        seed: str = Query(..., description="Базовый сид (без операторов)"),
        operator: str = Query("купить", description="Оператор для G1-G9 групп"),
        country: str = Query("ua", description="Код страны"),
        language: str = Query("ru", description="Язык"),
        groups: Optional[str] = Query(None, description="Группы через запятую: G1,G2,PA,PC. None = все"),
        parallel: int = Query(10, description="Параллельных запросов на агента"),
    ):
        """
        PREFIX MAP: Full prefix matrix run with dual-agent (Chrome + Firefox).

        Chrome:  G1-G9 + PA (9 структур × 30 букв) + PC
        Firefox: G1-G9 + PC only (PA даёт мусор на Firefox)

        Returns keywords + detailed prefix tracer data.
        """
        pp = get_prefix_parser()

        selected_groups = [g.strip() for g in groups.split(",")] if groups else None

        result = await pp.parse(
            seed=seed,
            operator=operator,
            country=country,
            language=language,
            groups=selected_groups,
        )

        # Формируем keywords для HTML — аналог suffix_endpoint keywords_for_html
        keywords_for_html = []
        for kw, structs in result.all_keywords.items():
            keywords_for_html.append({
                "keyword": kw,
                "structs": structs,
                "weight": len(structs),
                "is_prefix_expanded": True,
                "alt_seed": kw in result.alt_seed_keywords,
            })

        # Сортировка по weight desc
        keywords_for_html.sort(key=lambda x: x["weight"], reverse=True)

        return {
            "method": "prefix-map",
            "seed": seed,
            "operator": operator,
            "agents": ["chrome", "firefox"],
            "keywords": keywords_for_html,
            "keywords_grey": [],
            "anchors": [],
            "total": len(keywords_for_html),
            "alt_seed_count": len(result.alt_seed_keywords),
            "time": f"{result.total_time_ms:.0f}ms",
            "prefix_trace": {
                "total_keywords": result.total_keywords,
                "total_time_ms": result.total_time_ms,
                "total_queries": result.total_queries,
                "with_results": result.with_results,
                "empty_queries": result.empty_queries,
                "error_queries": result.error_queries,
                "exclusive_count": result.exclusive_count,
                "groups_used": result.groups_used,
                "trace": result.trace,
                "summary_by_group": result.summary_by_group,
            },
        }
