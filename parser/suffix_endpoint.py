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
        include_numbers: bool = Query(True, description="Числовые суффиксы 0-9"),
        filters: str = Query("none", description="Фильтры (для совместимости)"),
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
        )

        # Format response compatible with existing HTML displayResults
        keywords_with_source = [
            {
                "keyword": kw["keyword"],
                "source_suffix": kw["source_suffix"],
                "source_type": kw["source_type"],
                "source_priority": kw["source_priority"],
                "source_query": kw["source_query"],
            }
            for kw in result.all_keywords
        ]

        return {
            "method": "suffix-map",
            "seed": seed,
            "keywords": keywords_with_source,
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
