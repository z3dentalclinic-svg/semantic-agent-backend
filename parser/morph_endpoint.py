"""
Morph Endpoint v2.1 — FastAPI registration for /api/morph-map.

Route: GET /api/morph-map
Params:
  seed:            str   (required)
  country:         str   (default "ua") — Google gl=
  language:        str   (default "ru") — Google hl=
  region:          str   (default "ua") — "ua"/"ru"/"all" — Type A symbol cluster
  include_letters: bool  (default True) — include/exclude Type E letter sweep
                         Set False for fast smoke-test (A/B/C/D only, ~100 queries)

Response JSON:
{
  "seed":         str,
  "method":       "morph-map",
  "keywords":     [...],          # normalized nominative, deduped across all cases
  "keywords_raw": [...],          # raw pre-normalization
  "trace": {
    "nomn_sing": {
      "_meta": {
        "case_display":       str,
        "seed_variant":       str,
        "raw_count":          int,
        "normalized_count":   int,
        "normalized_keywords": [...],
        "unique_count":       int,
        "unique_keywords":    [...],
        "blocked_count":      int,
        "blocked":            [{suffix_label, suffix_val, suffix_type, blocked_by}]
      },
      "prep_v_v1": {
        "chrome":  {query, results, count, suffix_type, suffix_val, variant, priority, unique_in_case},
        "firefox": {query, results, count, suffix_type, suffix_val, variant, priority, unique_in_case}
      },
      "а_plain": { "chrome": {...}, "firefox": {...} },
      ...
    },
    "gent_sing": { ... },
    ...
  },
  "analysis": { noun, lemma, case_variants, by_case, ... },
  "stats": {
    "cases_active":                  int,
    "total_morph_queries_generated": int,
    "fetch_requests":                int,
    "total_raw_keywords":            int,
    "total_normalized":              int,
    "by_case":  { case_label: {raw, normalized, unique} }
  },
  "elapsed_time": float
}
"""

import logging
import sys
from fastapi import FastAPI, Query
from parser.morph_parser import MorphParser

logger = logging.getLogger(__name__)


def _get_geo_db() -> dict:
    """Ленивый импорт GEO_DB из main — вызывается при первом запросе."""
    try:
        main_module = sys.modules.get("main") or sys.modules.get("__main__")
        if main_module and hasattr(main_module, "GEO_DB"):
            return main_module.GEO_DB
    except Exception:
        pass
    return {}


def register_morph_endpoint(app: FastAPI) -> None:
    """Register /api/morph-map on the given FastAPI app."""

    # MorphParser инициализируется лениво при первом запросе
    # чтобы GEO_DB успел загрузиться в main.py
    _morph_parser: MorphParser | None = None

    def get_morph_parser() -> MorphParser:
        nonlocal _morph_parser
        if _morph_parser is None:
            _morph_parser = MorphParser(lang="ru", geo_db=_get_geo_db())
            logger.info(f"[MORPH] MorphParser инициализирован, geo_db={len(_morph_parser.generator.geo_db)} записей")
        return _morph_parser

    @app.get("/api/morph-map")
    async def morph_map(
        seed: str = Query(..., description="Seed phrase"),
        country: str = Query("ua", description="Country code (ua/ru/by/kz) for Google gl="),
        language: str = Query("ru", description="Language (ru/uk) for Google hl="),
        region: str = Query(
            "ua",
            description="Symbol cluster: 'ua' → ':' symbol, 'ru' → '&' symbol, 'all' → both"
        ),
        include_letters: bool = Query(
            True,
            description=(
                "Include Type E letter sweep (26 letters × 14 structures per case). "
                "Set False for fast smoke-test: only A/B/C/D structures, ~100 queries total."
            )
        ),
    ):
        """
        Morphology Map Parser v2.1.

        Generates all unique case variants of the first noun in seed,
        then runs the FULL suffix map (A+B+C+D+E) on each case variant.
        Returns detailed 4-axis trace (case × suffix_type × suffix_label × UA)
        for post-run dataset analysis.
        """
        logger.info(
            f"[MORPH-MAP] seed='{seed}' country={country} "
            f"language={language} region={region} letters={include_letters}"
        )

        result = await get_morph_parser().parse(
            seed=seed,
            country=country,
            language=language,
            region=region,
            include_numbers=False,
        )

        # If include_letters=False — strip E-type entries from trace to save bandwidth
        trace = result.trace
        if not include_letters:
            trace = {}
            for cl, case_data in result.trace.items():
                filtered = {"_meta": case_data.get("_meta", {})}
                for suffix_label, ua_data in case_data.items():
                    if suffix_label == "_meta":
                        continue
                    any_ua = next(iter(ua_data.values()), {})
                    if any_ua.get("suffix_type") != "E":
                        filtered[suffix_label] = ua_data
                trace[cl] = filtered

        return {
            "seed": result.seed,
            "method": "morph-map",
            "keywords": result.keywords,
            "keywords_raw": result.keywords_raw,
            "trace": trace,
            "analysis": result.analysis_summary,
            "stats": result.stats,
            "elapsed_time": result.total_time_s,
        }
