"""
Configuration module for FGS Parser
"""

from .constants import USER_AGENTS, WHITELIST_TOKENS, MANUAL_RARE_CITIES
from .forbidden_geo import FORBIDDEN_GEO

__all__ = [
    'USER_AGENTS',
    'WHITELIST_TOKENS', 
    'MANUAL_RARE_CITIES',
    'FORBIDDEN_GEO'
]

