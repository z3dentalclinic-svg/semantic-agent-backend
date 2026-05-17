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
        city: str = Query(None, description="Город для uule гео-таргетинга (по-английски). None = столица страны."),
        debug: int = Query(0, description="1 = добавить полную трассу (per-request entries, structs массив, summary_by_group)"),
    ):
        """
        PREFIX MAP: Full prefix matrix run with dual-agent (Chrome + Firefox).

        Production mode (debug=0): только ключи и stage_stats/trace_log (лёгкие тайминги).
        Debug mode (debug=1): + per-request trace, structs массив, summary_by_group.
        """
        pp = get_prefix_parser()

        selected_groups = [g.strip() for g in groups.split(",")] if groups else None

        result = await pp.parse(
            seed=seed,
            operator=operator,
            country=country,
            language=language,
            groups=selected_groups,
            city=city,
        )

        # Формируем keywords для HTML
        keywords_for_html = []
        for kw, structs in result.all_keywords.items():
            entry = {
                "keyword": kw,
                "weight": len(structs),
                "is_prefix_expanded": True,
                "alt_seed": kw in result.alt_seed_keywords,
            }
            if debug:
                entry["structs"] = structs  # тяжёлое поле, только в дебаге
            keywords_for_html.append(entry)

        # Сортировка по weight desc
        keywords_for_html.sort(key=lambda x: x["weight"], reverse=True)

        # Базовый ответ
        response = {
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
                "stage_stats": result.stage_stats,
            },
            "prefixStageStats": result.stage_stats,  # на верхнем уровне
            "prefixTraceLog": result.trace_log,      # лёгкий trace ~13 событий
        }

        # Debug-only поля
        if debug:
            response["prefix_trace"]["summary_by_group"] = result.summary_by_group
            response["prefix_trace"]["trace"] = result.trace  # per-request entries (тяжёлое)

        return response
