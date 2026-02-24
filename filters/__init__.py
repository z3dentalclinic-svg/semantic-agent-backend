"""
Filters module for FGS Parser1
Contains all filtering logic:
- batch_post_filter: Main geo-filter with multi-layer checks
- geo_garbage_filter: Advanced geo-filter (occupied territories, multiple cities, districts)
- pre_filter: Sanitary parsing cleanup (duplicates, echoes, repeats)
- infix_filter: Infix results filtering
- relevance_filter: Relevance keyword filtering
- entity_logic: Morphology and NER filtering (DEPRECATED)
"""
import json
import os
import logging

from .batch_post_filter import BatchPostFilter
from .geo_garbage_filter import filter_geo_garbage, OCCUPIED_TERRITORIES
from .pre_filter import pre_filter, apply_pre_filter
from .infix_filter import filter_infix_results
from .relevance_filter import filter_relevant_keywords
from .l0_filter import apply_l0_filter

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# Загрузка districts.json → {name: country_code}
# Формат файла: {"салтовка": {"city": "харків", "country": "ua"}}
# Формат для BPF: {"салтовка": "ua"}
# ═══════════════════════════════════════════════════════════════

_districts_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'districts.json')

if os.path.exists(_districts_path):
    try:
        with open(_districts_path, 'r', encoding='utf-8') as f:
            _raw = json.load(f)
        DISTRICTS_EXTENDED = {k: v['country'] for k, v in _raw.items()}
        logger.info(f"[FILTERS] districts.json loaded: {len(DISTRICTS_EXTENDED):,} districts")
    except Exception as e:
        logger.error(f"[FILTERS] Error loading districts.json: {e}")
        DISTRICTS_EXTENDED = {}
else:
    logger.warning(f"[FILTERS] districts.json not found at {_districts_path}")
    DISTRICTS_EXTENDED = {}

__all__ = [
    'BatchPostFilter',
    'DISTRICTS_EXTENDED',
    'filter_geo_garbage',
    'OCCUPIED_TERRITORIES',
    'pre_filter',
    'apply_pre_filter',
    'filter_infix_results',
    'filter_relevant_keywords',
    'apply_l0_filter',
]
