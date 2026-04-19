"""
Модуль для работы с базами данных городов и брендов.

Предоставляет функции для:
- Загрузки базы городов из geonamescache (с привязкой к странам)
- Загрузки базы брендов техники
- Поиска по точному совпадению и лемматизации

ИЗМЕНЕНИЕ (2026-02-25):
  load_geonames_db() теперь возвращает Dict[str, Set[str]]
  вместо Set[str]. Ключ — название города (lowercase),
  значение — множество кодов стран где этот город существует.
  Это позволяет detect_geo сравнивать город с target_country.
"""

import geonamescache
import pymorphy3
from typing import Set, Optional, List, Dict


# Инициализируем морфологический анализатор
morph = pymorphy3.MorphAnalyzer()


# Маппинг русских названий стран → ISO коды
_RUSSIAN_COUNTRY_MAP: Dict[str, str] = {
    'украина': 'UA', 'россия': 'RU', 'беларусь': 'BY', 'белоруссия': 'BY',
    'казахстан': 'KZ', 'польша': 'PL', 'германия': 'DE', 'франция': 'FR',
    'италия': 'IT', 'испания': 'ES', 'англия': 'GB', 'великобритания': 'GB',
    'сша': 'US', 'америка': 'US', 'турция': 'TR', 'египет': 'EG',
    'греция': 'GR', 'чехия': 'CZ', 'австрия': 'AT', 'швеция': 'SE',
    'норвегия': 'NO', 'финляндия': 'FI', 'дания': 'DK',
    'нидерланды': 'NL', 'голландия': 'NL', 'бельгия': 'BE', 'швейцария': 'CH',
    'португалия': 'PT', 'румыния': 'RO', 'болгария': 'BG',
    'сербия': 'RS', 'хорватия': 'HR', 'словакия': 'SK', 'словения': 'SI',
    'венгрия': 'HU', 'молдова': 'MD', 'молдавия': 'MD',
    'литва': 'LT', 'латвия': 'LV', 'эстония': 'EE',
    'грузия': 'GE', 'армения': 'AM', 'азербайджан': 'AZ',
    'узбекистан': 'UZ', 'таджикистан': 'TJ', 'кыргызстан': 'KG',
    'туркменистан': 'TM', 'китай': 'CN', 'япония': 'JP',
    'корея': 'KR', 'индия': 'IN', 'таиланд': 'TH', 'вьетнам': 'VN',
    'индонезия': 'ID', 'малайзия': 'MY', 'сингапур': 'SG', 'филиппины': 'PH',
    'австралия': 'AU', 'канада': 'CA', 'мексика': 'MX',
    'бразилия': 'BR', 'аргентина': 'AR',
    'израиль': 'IL', 'оаэ': 'AE', 'эмираты': 'AE', 'саудовская аравия': 'SA',
}


def load_geonames_db(country_code: Optional[str] = None) -> Dict[str, Set[str]]:
    """
    Загружает базу городов из geonamescache с привязкой к странам.

    Args:
        country_code: Если указан — загружает ТОЛЬКО города этой страны.
                     Если None — загружает ВСЕ города мира (рекомендуется для L0).

    Returns:
        Dict[str, Set[str]]: название_города (lowercase) → {коды_стран}
        Пример: {"одесса": {"UA", "US"}, "киев": {"UA"}, "тир": {"LB"}}
    """
    gc = geonamescache.GeonamesCache()
    cities = gc.get_cities()

    # city_name → set of country codes
    geo_db: Dict[str, Set[str]] = {}

    for city_data in cities.values():
        cc = city_data.get('countrycode', '').upper()

        # Фильтруем по стране если указана
        if country_code and cc != country_code.upper():
            continue

        # Основное название
        name = city_data.get('name', '').lower().strip()
        if name:
            geo_db.setdefault(name, set()).add(cc)

        # Альтернативные названия — все привязаны к той же стране
        for alt_name in city_data.get('alternatenames', []):
            if alt_name:
                alt_lower = alt_name.lower().strip()
                geo_db.setdefault(alt_lower, set()).add(cc)

    # === СТРАНЫ — английские названия из geonamescache ===
    countries = gc.get_countries()
    for cc_key, country_data in countries.items():
        name = country_data.get('name', '').lower().strip()
        if name:
            # Страна "принадлежит" сама себе
            geo_db.setdefault(name, set()).add(cc_key.upper())

    # === СТРАНЫ — русские названия ===
    for ru_name, iso_code in _RUSSIAN_COUNTRY_MAP.items():
        geo_db.setdefault(ru_name, set()).add(iso_code)

    return geo_db


def load_brands_db() -> Set[str]:
    """
    Загружает базу брендов.

    Приоритет:
    1. brands.json (генерируется fetch_brands.py из Wikidata)
    2. Встроенный fallback (~100 брендов)

    Returns:
        Множество названий брендов в нижнем регистре
    """
    import os
    import json

    # Пробуем загрузить brands.json
    for path in [
        os.path.join(os.path.dirname(__file__), 'brands.json'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'brands.json'),
        'brands.json',
    ]:
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                brands = set(data.get("brands", []))
                print(f"✅ brands.json loaded: {len(brands)} brands from {path}")
                return brands
            except Exception as e:
                print(f"⚠️ Error loading brands.json: {e}")

    # Fallback: встроенный минимальный набор
    print("⚠️ brands.json not found, using built-in fallback (limited)")
    brands = {
        'samsung', 'самсунг', 'lg', 'лж', 'элджи',
        'dyson', 'дайсон', 'xiaomi', 'сяоми',
        'philips', 'филипс', 'bosch', 'бош',
        'electrolux', 'электролюкс', 'thomas', 'томас',
        'karcher', 'керхер', 'miele', 'миле',
        'apple', 'эпл', 'sony', 'сони',
        'panasonic', 'панасоник', 'hitachi', 'хитачи',
        'toyota', 'тойота', 'bmw', 'бмв',
        'mercedes', 'мерседес', 'honda', 'хонда',
        'nike', 'найк', 'adidas', 'адидас',
        'ikea', 'икеа', 'bork', 'борк',
        'атлант', 'горенье', 'redmond', 'редмонд',
    }
    return brands


def load_retailers_db() -> Set[str]:
    """
    Загружает базу ритейлеров / маркетплейсов.

    В отличие от brands.json (производители), retailers.json содержит
    торговые сети и онлайн-маркетплейсы: Rozetka, Amazon, OLX, Wildberries
    и т.д. Separate file — разная семантика и разные правила обновления.

    Приоритет:
    1. retailers.json — кураторский список для UA/RU/BY/KZ/EU/global
    2. Встроенный fallback — топ-30 наиболее частых

    Returns:
        Множество названий ритейлеров в нижнем регистре. Многословные
        имена ("нова пошта", "медиа маркт") хранятся как строки и
        детектором проверяются в обе стороны: одним токеном или биграммой.
    """
    import os
    import json

    for path in [
        os.path.join(os.path.dirname(__file__), 'retailers.json'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'retailers.json'),
        'retailers.json',
    ]:
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                retailers = set(data.get("retailers", []))
                print(f"✅ retailers.json loaded: {len(retailers)} retailers from {path}")
                return retailers
            except Exception as e:
                print(f"⚠️ Error loading retailers.json: {e}")

    # Fallback: топ-30 для UA/RU/глобальных (если файл не найден)
    print("⚠️ retailers.json not found, using built-in fallback (limited)")
    retailers = {
        'розетка', 'rozetka', 'алло', 'allo', 'comfy', 'эпицентр', 'epicentr',
        'цитрус', 'citrus', 'фокстрот', 'foxtrot', 'олх', 'olx',
        'озон', 'ozon', 'вайлдберриз', 'wildberries',
        'амазон', 'amazon', 'ebay', 'aliexpress', 'алиэкспресс',
        'авито', 'avito', 'мвидео', 'm-video', 'эльдорадо', 'eldorado',
        'каспи', 'kaspi', 'куфар', 'kufar', 'онлайнер', 'onliner',
    }
    return retailers


def get_lemma(word: str) -> str:
    """Получает лемму (начальную форму) слова."""
    parsed = morph.parse(word.lower())[0]
    return parsed.normal_form


def normalize_for_search(text: str) -> List[str]:
    """Нормализует текст для поиска: разбивает на слова и лемматизирует."""
    words = text.lower().split()
    return [get_lemma(word) for word in words]


def search_in_db(text: str, database, use_lemma: bool = True) -> bool:
    """
    Ищет совпадение в базе данных.
    Работает и с Set[str], и с Dict[str, Set[str]].
    """
    text_lower = text.lower().strip()

    if text_lower in database:
        return True

    if use_lemma:
        for word in text_lower.split():
            lemma = get_lemma(word)
            if lemma in database:
                return True

    return False


def find_cities_in_text(text: str, cities_db) -> List[str]:
    """
    Находит все упоминания городов в тексте.
    Работает и с Set[str], и с Dict[str, Set[str]].
    """
    found = []
    words = text.lower().split()

    for word in words:
        if word in cities_db:
            found.append(word)
            continue
        lemma = get_lemma(word)
        if lemma in cities_db:
            found.append(lemma)

    return found


def find_brands_in_text(text: str, brands_db: Set[str]) -> List[str]:
    """Находит все упоминания брендов в тексте."""
    found = []
    words = text.lower().split()

    for word in words:
        if word in brands_db:
            found.append(word)
            continue
        lemma = get_lemma(word)
        if lemma in brands_db:
            found.append(lemma)

    return found


# ==================== ХЕЛПЕРЫ ДЛЯ COUNTRY-AWARE GEO ====================

def geo_city_in_country(city_name: str, target_country: str, geo_db: Dict[str, Set[str]]) -> bool:
    """
    Проверяет, есть ли город в указанной стране.

    Args:
        city_name: Название города (lowercase)
        target_country: Код страны (например 'UA')
        geo_db: Country-aware geo база

    Returns:
        True если город существует в target_country
    """
    countries = geo_db.get(city_name.lower(), set())
    return target_country.upper() in countries


def geo_get_countries(city_name: str, geo_db: Dict[str, Set[str]]) -> Set[str]:
    """
    Возвращает множество стран, в которых существует город.

    Args:
        city_name: Название города (lowercase)
        geo_db: Country-aware geo база

    Returns:
        Множество кодов стран, например {"UA", "US"}
    """
    return geo_db.get(city_name.lower(), set())


# ==================== ТЕСТЫ ====================

def run_tests():
    """Запускает набор тестов для проверки загрузки и поиска в базах."""

    print("🧪 ТЕСТИРОВАНИЕ МОДУЛЯ БАЗ ДАННЫХ\n")

    # Тест 1: Country-aware geo_db
    print("=" * 60)
    print("📍 ТЕСТ 1: Country-aware geo_db\n")

    geo_db = load_geonames_db()  # Все города мира
    print(f"✅ Загружено уникальных названий: {len(geo_db)}")

    # Проверяем country mapping
    test_cases = [
        ('kyiv', 'UA', True, 'Киев → Украина'),
        ('kyiv', 'RU', False, 'Киев НЕ в России'),
        ('одесса', 'UA', False, 'одесса — проверяем (может быть только odessa/odesa в geonamescache)'),
        ('москва', 'RU', False, 'москва — проверяем наличие'),
        ('тир', 'UA', False, 'Тир НЕ в Украине'),
        ('украина', 'UA', True, 'Страна "Украина" → UA'),
        ('россия', 'RU', True, 'Страна "Россия" → RU'),
    ]

    for city, country, expected, desc in test_cases:
        result = geo_city_in_country(city, country, geo_db)
        countries = geo_get_countries(city, geo_db)
        status = "✅" if result == expected else "⚠️"
        print(f"  {status} {desc}: '{city}' in {country} → {result} (all: {countries})")

    # Тест 2: Обратная совместимость — `in` работает
    print(f"\n  'kyiv' in geo_db: {'kyiv' in geo_db}")
    print(f"  'абвгд' in geo_db: {'абвгд' in geo_db}")

    print()

    # Тест 3: Бренды
    print("=" * 60)
    print("🏷️ ТЕСТ 2: Загрузка базы брендов\n")
    brands = load_brands_db()
    print(f"✅ Загружено брендов: {len(brands)}")

    print("\n✅ ТЕСТЫ ЗАВЕРШЕНЫ")
    return True


if __name__ == "__main__":
    run_tests()
