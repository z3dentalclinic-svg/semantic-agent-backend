"""
UULE geo-targeting для Google Autocomplete.

Источник данных: Google Ads Targetable Locations 2026-03-31.csv
Покрытие: UA, RU, BY, KZ, US, GB, DE, FR, PL, CZ, SK, RO, HU, MD

Использование:
    from geo_uule import get_uule, get_cities

    # Столица по умолчанию
    uule = get_uule("ua")           # → Kyiv,Kyiv city,Ukraine

    # Конкретный город
    uule = get_uule("ua", "Lviv")   # → Lviv,Lviv Oblast,Ukraine

    # Список городов для UI
    cities = get_cities("ua")       # → ["Berdychiv", "Boryspil", ...]
"""

import json
import base64
import os
from typing import Optional

_data: dict = {}
_loaded = False


def _load() -> dict:
    global _data, _loaded
    if _loaded:
        return _data
    # Ищем geo_uule.json рядом с этим файлом
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, "geo_uule.json")
    if not os.path.exists(path):
        # Fallback: рядом с parser/ директорией
        path = os.path.join(base, "..", "geo_uule.json")
    try:
        with open(path, encoding="utf-8") as f:
            _data = json.load(f)
    except FileNotFoundError:
        _data = {}
    _loaded = True
    return _data


def generate_uule(canonical_name: str) -> str:
    """
    Генерирует uule параметр из Google Ads canonical location name.

    Алгоритм Google:
        1. Длина строки → символ из secret_keys (таблица 64 символа)
        2. Base64(canonical_name)
        3. Результат: "w+" + key + base64
    """
    secret_keys = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    length = len(canonical_name)
    key = secret_keys[min(length, len(secret_keys) - 1)]
    hashed = base64.b64encode(canonical_name.encode("utf-8")).decode("utf-8")
    return f"w+{key}{hashed}"


def get_uule(country_code: str, city_name: Optional[str] = None) -> Optional[str]:
    """
    Возвращает uule параметр для страны и опционального города.

    Args:
        country_code: Двухбуквенный код (ua, ru, by, kz, us, gb, ...).
                      Регистр не важен.
        city_name:    Название города (как в Google Ads, по-английски).
                      None → столица страны по умолчанию.

    Returns:
        uule строка вида "w+XYZBase64==" или None если страна/город не найдены.

    Examples:
        get_uule("ua")          → uule для Kyiv
        get_uule("ua", "Lviv") → uule для Lviv
        get_uule("ru")          → uule для Moscow
    """
    data = _load()
    cc = country_code.upper()
    if cc not in data:
        return None

    country_data = data[cc]
    cities: dict = country_data.get("cities", {})

    if city_name is None:
        # Столица по умолчанию
        default = country_data.get("default")
        if not default:
            return None
        city_data = cities.get(default)
    else:
        # Точное совпадение
        city_data = cities.get(city_name)
        if not city_data:
            # Case-insensitive поиск
            city_lower = city_name.lower().strip()
            city_data = next(
                (v for k, v in cities.items() if k.lower() == city_lower),
                None,
            )

    if not city_data:
        return None

    return city_data["uule"]


def get_default_city(country_code: str) -> Optional[str]:
    """Возвращает название столицы для страны."""
    data = _load()
    cc = country_code.upper()
    if cc not in data:
        return None
    return data[cc].get("default")


def get_cities(country_code: str) -> list:
    """
    Возвращает отсортированный список городов для UI дропдауна.

    Returns:
        List[str] — названия городов по-английски.
    """
    data = _load()
    cc = country_code.upper()
    if cc not in data:
        return []
    return sorted(data[cc]["cities"].keys())
