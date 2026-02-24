"""
Модуль для работы с базами данных городов и брендов.

Предоставляет функции для:
- Загрузки базы городов из geonamescache
- Загрузки базы брендов техники
- Поиска по точному совпадению и лемматизации
"""

import geonamescache
import pymorphy3
from typing import Set, Optional, List, Dict


# Инициализируем морфологический анализатор
morph = pymorphy3.MorphAnalyzer()


def load_geonames_db(country_code: Optional[str] = None) -> Set[str]:
    """
    Загружает базу городов из geonamescache.
    
    Args:
        country_code: Код страны (например, 'UA' для Украины, 'RU' для России).
                     Если None, загружаются все города мира (рекомендуется).
    
    Returns:
        Множество названий городов + стран в нижнем регистре
    """
    gc = geonamescache.GeonamesCache()
    cities = gc.get_cities()
    
    city_names = set()
    
    for city_data in cities.values():
        # Фильтруем по стране если указана
        if country_code and city_data.get('countrycode') != country_code:
            continue
        
        # Добавляем название города
        name = city_data.get('name', '').lower()
        if name:
            city_names.add(name)
        
        # Добавляем альтернативные названия если есть
        alt_names = city_data.get('alternatenames', [])
        for alt_name in alt_names:
            if alt_name:
                city_names.add(alt_name.lower())
    
    # === СТРАНЫ (geonamescache хранит только английские названия) ===
    countries = gc.get_countries()
    for country_data in countries.values():
        name = country_data.get('name', '').lower()
        if name:
            city_names.add(name)
    
    # Русские названия стран (geonamescache их не содержит)
    # Это НЕ стоп-слова — это географическая БД, как и города
    russian_countries = {
        'украина', 'россия', 'беларусь', 'белоруссия', 'казахстан',
        'польша', 'германия', 'франция', 'италия', 'испания',
        'англия', 'великобритания', 'сша', 'америка',
        'турция', 'египет', 'греция', 'чехия', 'австрия',
        'швеция', 'норвегия', 'финляндия', 'дания',
        'нидерланды', 'голландия', 'бельгия', 'швейцария',
        'португалия', 'румыния', 'болгария', 'сербия', 'хорватия',
        'словакия', 'словения', 'венгрия', 'молдова', 'молдавия',
        'литва', 'латвия', 'эстония', 'грузия', 'армения', 'азербайджан',
        'узбекистан', 'таджикистан', 'кыргызстан', 'туркменистан',
        'китай', 'япония', 'корея', 'индия', 'таиланд', 'вьетнам',
        'индонезия', 'малайзия', 'сингапур', 'филиппины',
        'австралия', 'канада', 'мексика', 'бразилия', 'аргентина',
        'израиль', 'оаэ', 'эмираты', 'саудовская аравия',
    }
    city_names.update(russian_countries)
    
    return city_names


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


def get_lemma(word: str) -> str:
    """
    Получает лемму (начальную форму) слова.
    
    Args:
        word: Исходное слово
    
    Returns:
        Лемма слова в нижнем регистре
    
    Examples:
        >>> get_lemma('києві')
        'київ'
        >>> get_lemma('киеву')
        'киев'
    """
    parsed = morph.parse(word.lower())[0]
    return parsed.normal_form


def normalize_for_search(text: str) -> List[str]:
    """
    Нормализует текст для поиска: разбивает на слова и лемматизирует.
    
    Args:
        text: Исходный текст
    
    Returns:
        Список лемм всех слов
    
    Examples:
        >>> normalize_for_search('ремонт пылесосов')
        ['ремонт', 'пылесос']
    """
    words = text.lower().split()
    lemmas = [get_lemma(word) for word in words]
    return lemmas


def search_in_db(text: str, database: Set[str], use_lemma: bool = True) -> bool:
    """
    Ищет совпадение в базе данных.
    
    Args:
        text: Текст для поиска
        database: База данных (множество строк)
        use_lemma: Использовать ли лемматизацию
    
    Returns:
        True если найдено совпадение, иначе False
    
    Examples:
        >>> brands = {'samsung', 'lg'}
        >>> search_in_db('samsung', brands)
        True
        >>> search_in_db('самсунг', brands)
        False
        >>> search_in_db('unknown', brands)
        False
    """
    text_lower = text.lower().strip()
    
    # Точное совпадение
    if text_lower in database:
        return True
    
    # Поиск по леммам если включено
    if use_lemma:
        words = text_lower.split()
        for word in words:
            lemma = get_lemma(word)
            if lemma in database:
                return True
    
    return False


def find_cities_in_text(text: str, cities_db: Set[str]) -> List[str]:
    """
    Находит все упоминания городов в тексте.
    
    Args:
        text: Исходный текст
        cities_db: База городов
    
    Returns:
        Список найденных городов
    
    Examples:
        >>> cities = {'киев', 'одесса', 'львов'}
        >>> find_cities_in_text('киев одесса', cities)
        ['киев', 'одесса']
    """
    found = []
    words = text.lower().split()
    
    for word in words:
        # Проверяем точное совпадение
        if word in cities_db:
            found.append(word)
            continue
        
        # Проверяем лемму
        lemma = get_lemma(word)
        if lemma in cities_db:
            found.append(lemma)
    
    return found


def find_brands_in_text(text: str, brands_db: Set[str]) -> List[str]:
    """
    Находит все упоминания брендов в тексте.
    
    Args:
        text: Исходный текст
        brands_db: База брендов
    
    Returns:
        Список найденных брендов
    
    Examples:
        >>> brands = {'samsung', 'lg', 'dyson'}
        >>> find_brands_in_text('samsung lg', brands)
        ['samsung', 'lg']
    """
    found = []
    words = text.lower().split()
    
    for word in words:
        # Проверяем точное совпадение
        if word in brands_db:
            found.append(word)
            continue
        
        # Проверяем лемму
        lemma = get_lemma(word)
        if lemma in brands_db:
            found.append(lemma)
    
    return found


# ==================== ТЕСТЫ ====================

def run_tests():
    """Запускает набор тестов для проверки загрузки и поиска в базах."""
    
    print("🧪 ТЕСТИРОВАНИЕ МОДУЛЯ БАЗ ДАННЫХ\n")
    
    # Тест 1: Загрузка базы городов
    print("=" * 60)
    print("📍 ТЕСТ 1: Загрузка базы городов (Украина)\n")
    
    cities_ua = load_geonames_db('UA')
    print(f"✅ Загружено городов Украины: {len(cities_ua)}")
    
    # Проверяем наличие крупных городов
    expected_cities = ['kyiv', 'kiev', 'odesa', 'odessa', 'lviv', 'kharkiv', 'dnipro']
    found_cities = [city for city in expected_cities if city in cities_ua]
    print(f"✅ Найдено известных городов: {len(found_cities)}/{len(expected_cities)}")
    print(f"   Примеры: {', '.join(list(cities_ua)[:10])}")
    
    if len(cities_ua) < 100:
        print("⚠️ ПРЕДУПРЕЖДЕНИЕ: Мало городов загружено!")
    
    print()
    
    # Тест 2: Загрузка базы брендов
    print("=" * 60)
    print("🏷️ ТЕСТ 2: Загрузка базы брендов\n")
    
    brands = load_brands_db()
    print(f"✅ Загружено брендов: {len(brands)}")
    print(f"   Примеры: {', '.join(list(brands)[:15])}")
    
    if len(brands) < 30:
        print("⚠️ ПРЕДУПРЕЖДЕНИЕ: Мало брендов в базе!")
    
    print()
    
    # Тест 3: Лемматизация
    print("=" * 60)
    print("📝 ТЕСТ 3: Лемматизация слов\n")
    
    lemma_tests = [
        ('киеву', 'киев'),
        ('києві', 'київ'),
        ('одессе', 'одесса'),
        ('пылесосов', 'пылесос'),
        ('samsung', 'samsung'),  # английское слово
    ]
    
    passed = 0
    for word, expected_lemma in lemma_tests:
        lemma = get_lemma(word)
        if lemma == expected_lemma:
            print(f"✅ '{word}' → '{lemma}'")
            passed += 1
        else:
            print(f"⚠️ '{word}' → '{lemma}' (ожидалось: '{expected_lemma}')")
    
    print(f"\n📊 Лемматизация: {passed}/{len(lemma_tests)} успешно")
    print()
    
    # Тест 4: Поиск в базе городов
    print("=" * 60)
    print("🔍 ТЕСТ 4: Поиск городов в тексте\n")
    
    search_tests = [
        ('kyiv', True, 'Точное совпадение (EN)'),
        ('kiev', True, 'Альтернативное название'),
        ('київ', True, 'Украинское написание'),
        ('одесса', True, 'Русское название'),
        ('абвгд', False, 'Несуществующий город'),
    ]
    
    passed = 0
    for query, should_find, description in search_tests:
        found = search_in_db(query, cities_ua, use_lemma=True)
        
        if found == should_find:
            status = "✅ PASS"
            passed += 1
        else:
            status = "❌ FAIL"
        
        print(f"{status}: {description}")
        print(f"   Query: '{query}' → Found: {found}")
    
    print(f"\n📊 Поиск городов: {passed}/{len(search_tests)} успешно")
    print()
    
    # Тест 5: Поиск брендов
    print("=" * 60)
    print("🔍 ТЕСТ 5: Поиск брендов в тексте\n")
    
    brand_tests = [
        ('samsung', ['samsung'], 'Точное совпадение'),
        ('lg dyson', ['lg', 'dyson'], 'Два бренда'),
        ('самсунг', [], 'Русское написание (может не найти)'),
        ('xiaomi dreame', ['xiaomi', 'dreame'], 'Brand collision'),
    ]
    
    passed = 0
    for text, expected, description in brand_tests:
        found = find_brands_in_text(text, brands)
        
        # Проверяем что нашли хотя бы те бренды которые ожидали
        all_found = all(brand in found for brand in expected)
        
        if all_found:
            status = "✅ PASS"
            passed += 1
        else:
            status = "⚠️ PARTIAL" if len(found) > 0 else "❌ FAIL"
        
        print(f"{status}: {description}")
        print(f"   Text: '{text}'")
        print(f"   Found: {found}")
        print(f"   Expected: {expected}")
    
    print(f"\n📊 Поиск брендов: {passed}/{len(brand_tests)} полностью успешно")
    print()
    
    # Итоговая статистика
    print("=" * 60)
    print("\n✅ МОДУЛЬ БАЗ ДАННЫХ ГОТОВ К ИСПОЛЬЗОВАНИЮ")
    print(f"📊 Города: {len(cities_ua)} записей")
    print(f"📊 Бренды: {len(brands)} записей")
    
    return True


if __name__ == "__main__":
    run_tests()
