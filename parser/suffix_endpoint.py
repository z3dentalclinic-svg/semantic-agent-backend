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

def _get_geo_db() -> dict:
    """Ленивый импорт GEO_DB из main — вызывается при первом запросе."""
    try:
        import sys
        main_module = sys.modules.get("main") or sys.modules.get("__main__")
        if main_module and hasattr(main_module, "GEO_DB"):
            return main_module.GEO_DB
    except Exception:
        pass
    return {}

def _get_morph():
    """Ленивый импорт morph_ru из main."""
    try:
        import sys
        main_module = sys.modules.get("main") or sys.modules.get("__main__")
        if main_module:
            p = getattr(main_module, "parser", None)
            if p and hasattr(p, "morph_ru"):
                return p.morph_ru
    except Exception:
        pass
    return None

def get_suffix_parser():
    global _suffix_parser
    if _suffix_parser is None:
        _suffix_parser = SuffixParser(lang="ru", geo_db=_get_geo_db(), morph=_get_morph())
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
        cp: int = Query(None, description="Cursor position: None=конец, 0=начало строки"),
        include_letters: bool = Query(True, description="Letter Sweep — буквенный перебор"),
        city: str = Query(None, description="Город для uule гео-таргетинга (по-английски). None = столица страны."),
    ):
        """
        SUFFIX MAP: Smart suffix expansion with priority matrix + tracer.
        Dual-agent: Chrome + Firefox запускаются параллельно внутри парсера.
        uule гео-таргетинг: по умолчанию столица страны, опционально конкретный город.
        """
        sp = get_suffix_parser()
        result = await sp.parse(
            seed=seed,
            country=country,
            language=language,
            parallel_limit=parallel,
            include_numbers=include_numbers,
            echelon=echelon,
            cursor_position=cp,
            include_letters=include_letters,
            city=city,
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

    # ═══════════════════════════════════════════════════════════════════
    # RESEARCH ENDPOINT — разовый широкий прогон с SD + SDL + E_LAT + полный E_RU
    # Зеркало /api/prefix-map с research-структурами.
    #
    # Объём на 1 сид:
    #   ~17 200 запросов (базовая часть + E_LAT + SD×3 агента + SDL×3 агента)
    #   Время прогона: ~1.5-2 часа на сид (50 IP пул)
    #
    # Использование:
    #   GET /api/suffix-research?seed=доставка цветов&country=ua&language=ru
    #   → возвращает полный trace с агрегированными ключами и source-pointers.
    #   Запускать ПО ОДНОМУ СИДУ — 7 раз для 7 сидов GAP-анализа.
    #
    # is_new_research=True помечает каждый SD/SDL запрос —
    # парсер направит их в research-пул (все IP пула round-robin)
    # и прогонит на 3 агентах (chrome+firefox+safari).
    # ═══════════════════════════════════════════════════════════════════
    @app.get("/api/suffix-research")
    async def suffix_research_endpoint(
        seed: str = Query(..., description="Базовый сид для research-прогона"),
        country: str = Query("ua", description="Код страны"),
        language: str = Query("ru", description="Язык"),
        city: str = Query(None, description="Город для uule. None = столица страны."),
    ):
        """
        SUFFIX RESEARCH: разовый широкий прогон с SD/SDL/E_LAT/full E_RU.
        Зеркало /api/prefix-map с research структурами.

        Возвращает полный trace в HTTP-response (~10 МБ JSON).
        Запускать ПО ОДНОМУ СИДУ за раз.
        """
        sp = get_suffix_parser()
        result = await sp.parse(
            seed=seed,
            country=country,
            language=language,
            parallel_limit=5,
            include_numbers=True,        # A_num включён
            echelon=0,                    # все priority levels
            cursor_position=None,         # auto
            include_letters=True,         # E_RU
            city=city,
            include_research=True,        # ← SD + SDL
            include_lat_sweep=True,       # ← E_LAT
            use_full_ru_sweep=True,       # ← E_RU full 30 букв
        )

        return {
            "method": "suffix-research",
            "seed": seed,
            "country": country,
            "language": language,
            "total_queries": result.total_queries,
            "total_keywords": len(result.all_keywords),
            "successful_queries": result.successful_queries,
            "empty_queries": result.empty_queries,
            "time": f"{result.total_time_ms:.0f}ms",
            "suffix_trace": {
                "analysis": result.analysis,
                "total_time_ms": result.total_time_ms,
                "total_queries": result.total_queries,
                "successful_queries": result.successful_queries,
                "empty_queries": result.empty_queries,
                "blocked_queries": result.blocked_queries,
                "trace": result.trace,
                "summary_by_type": result.summary_by_type,
                "summary_by_suffix": result.summary_by_suffix,
                "all_keywords": result.all_keywords,
            },
        }
