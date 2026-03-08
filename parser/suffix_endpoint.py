"""
Suffix Map API endpoint — drop-in for main.py

Usage in main.py:
    from parser.suffix_endpoint import register_suffix_endpoint
    register_suffix_endpoint(app)
"""

from fastapi import FastAPI, Query
from parser.suffix_parser import SuffixParser
from dataclasses import asdict


_suffix_parser = None

def get_suffix_parser():
    global _suffix_parser
    if _suffix_parser is None:
        _suffix_parser = SuffixParser(lang="ru")
    return _suffix_parser


def register_suffix_endpoint(app: FastAPI):
    """Register /api/suffix-map endpoint on the FastAPI app"""

    @app.get("/api/suffix-map")
    async def suffix_map_endpoint(
        seed: str = Query(..., description="Базовый запрос"),
        country: str = Query("ua", description="Код страны"),
        language: str = Query("ru", description="Язык"),
        parallel: int = Query(5, description="Параллельных запросов"),
        source: str = Query("google", description="Источник"),
        echelon: int = Query(0, description="0=все, 1=только P1, 2=только P2"),
        include_numbers: bool = Query(False, description="Числовые суффиксы 0-9"),
        filters: str = Query("none", description="Фильтры (для совместимости)"),
        google_client: str = Query("firefox", description="Autocomplete client: firefox/chrome/chrome-omni/safari/psy-ab/gws-wiz"),
        cp: int = Query(None, description="Cursor position: None=конец, 0=начало строки"),
        include_letters: bool = Query(False, description="Letter Sweep — буквенный перебор (а е и о у б в д к р)"),
    ):
        """
        SUFFIX MAP: Smart suffix expansion with priority matrix + tracer.
        Returns keywords + detailed suffix tracer data.
        """
        sp = get_suffix_parser()
        result = await sp.parse(
            seed=seed,
            country=country,
            language=language,
            parallel_limit=parallel,
            include_numbers=include_numbers,
            echelon=echelon,
            google_client=google_client,
            cursor_position=cp,
            include_letters=include_letters,
        )

        # Format response compatible with existing HTML displayResults
        # all_keywords now: [{keyword, sources, weight, is_suffix_expanded}, ...]
        keywords_for_html = []
        for kw_data in result.all_keywords:
            keywords_for_html.append({
                "keyword": kw_data["keyword"],
                "source_type": kw_data["sources"][0]["suffix_type"] if kw_data["sources"] else "?",
                "weight": kw_data["weight"],
                "is_suffix_expanded": True,
                "sources": kw_data["sources"],
            })

        return {
            "method": "suffix-map",
            "seed": seed,
            "google_client": google_client,
            "cursor_position": cp,
            "keywords": keywords_for_html,
            "keywords_grey": [],
            "anchors": [],
            "total": len(result.all_keywords),
            "time": f"{result.total_time_ms:.0f}ms",
            "suffix_trace": {
                "analysis": result.analysis,
                "total_keywords": len(result.all_keywords),
                "total_time_ms": result.total_time_ms,
                "total_queries": result.total_queries,
                "successful_queries": result.successful_queries,
                "empty_queries": result.empty_queries,
                "blocked_queries": result.blocked_queries,
                "trace": result.trace,
                "summary_by_type": result.summary_by_type,
                "summary_by_suffix": result.summary_by_suffix,
            },
        }
