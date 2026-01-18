"""
Geo module for FGS Parser
Handles geo blacklist generation and city database loading
"""

from .blacklist import generate_geo_blacklist_full

__all__ = ['generate_geo_blacklist_full']
