"""
Configuration constants for FGS Parser
"""

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

WHITELIST_TOKENS = {
    "филипс", "philips",
    "самсунг", "samsung",
    "бош", "bosch",
    "lg",
    "electrolux", "электролюкс",
    "dyson", "дайсон",
    "xiaomi", "сяоми",
    "karcher", "керхер",
    "tefal", "тефаль",
    "rowenta", "ровента",

    "желтые воды", "жёлтые воды", "zhovti vody",
    "новомосковск", "новомосковськ",  # Украина, НЕ Подмосковье!
}

MANUAL_RARE_CITIES = {
    "ua": {
        "щёлкино", "щелкino", "shcholkino",
        "армянск", "армjansk",
        "красноперекопск", "krasnoperekopsk",
        "джанкой", "dzhankoi",

        "коммунарка", "kommunarka",
        "московский", "moskovskiy",
    },

    "ru": {
        "жёлтые воды", "желтые воды", "zhovti vody",
        "вознесенск", "voznesensk",
    },

    "by": set(),

    "kz": set(),
}
