"""
Filters module for FGS Parser1
Contains all filtering logic:
- batch_post_filter: Main geo-filter with multi-layer checks
- geo_garbage_filter: Advanced geo-filter (occupied territories, multiple cities, districts)
- infix_filter: Infix results filtering
- relevance_filter: Relevance keyword filtering
- entity_logic: Morphology and NER filtering (DEPRECATED)
"""
from .batch_post_filter import BatchPostFilter, DISTRICTS_EXTENDED
from .geo_garbage_filter import filter_geo_garbage, OCCUPIED_TERRITORIES
from .infix_filter import filter_infix_results
from .relevance_filter import filter_relevant_keywords

__all__ = [
    'BatchPostFilter',
    'DISTRICTS_EXTENDED',
    'filter_geo_garbage',
    'OCCUPIED_TERRITORIES',
    'filter_infix_results',
    'filter_relevant_keywords'
]
