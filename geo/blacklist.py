"""
Geo blacklist generation module
Loads cities from geonamescache and embedded_cities
"""

def generate_geo_blacklist_full():
    """
    Generates global city dictionary with country codes
    Returns: dict {city_name: country_code}
    """
    try:
        from geonamescache import GeonamesCache

        gc = GeonamesCache()
        cities = gc.get_cities()

        all_cities_global = {}  # {город: код_страны}

        for city_id, city_data in cities.items():
            country = city_data['countrycode'].lower()  # 'RU', 'UA', 'BY' → 'ru', 'ua', 'by'

            name = city_data['name'].lower().strip()
            all_cities_global[name] = country

            for alt in city_data.get('alternatenames', []):
                if ' ' in alt:
                    continue

                if not (3 <= len(alt) <= 30):
                    continue

                if not any(c.isalpha() for c in alt):
                    continue

                alt_clean = alt.replace('-', '').replace("'", "")
                if alt_clean.isalpha():
                    is_latin_cyrillic = all(
                        ('\u0000' <= c <= '\u007F') or  # ASCII (латиница)
                        ('\u0400' <= c <= '\u04FF') or  # Кириллица
                        c in ['-', "'"]
                        for c in alt
                    )

                    if is_latin_cyrillic:
                        alt_lower = alt.lower().strip()
                        if alt_lower not in all_cities_global:
                            all_cities_global[alt_lower] = country

        print("✅ v5.6.0 TURBO: O(1) WORD BOUNDARY LOOKUP - Гео-Фильтрация инициализирована")
        print(f"   ALL_CITIES_GLOBAL: {len(all_cities_global)} городов с привязкой к странам")
        
        from collections import Counter
        country_stats = Counter(all_cities_global.values())
        print(f"   Топ-5 стран: {dict(country_stats.most_common(5))}")

        return all_cities_global

    except ImportError:
        print("⚠️ geonamescache не установлен, используется минимальный словарь")
        
        all_cities_global = {
            'москва': 'ru', 'мск': 'ru', 'спб': 'ru', 'питер': 'ru', 
            'санкт-петербург': 'ru', 'екатеринбург': 'ru', 'казань': 'ru',
            'новосибирск': 'ru', 'челябинск': 'ru', 'омск': 'ru',
            'минск': 'by', 'гомель': 'by', 'витебск': 'by', 'могилев': 'by',
            'алматы': 'kz', 'астана': 'kz', 'караганда': 'kz',
            'киев': 'ua', 'харьков': 'ua', 'одесса': 'ua', 'днепр': 'ua',
            'львов': 'ua', 'запорожье': 'ua', 'кривой рог': 'ua',
            'николаев': 'ua', 'винница': 'ua', 'херсон': 'ua',
            'полтава': 'ua', 'чернигов': 'ua', 'черкассы': 'ua',
            'днепропетровск': 'ua', 'kyiv': 'ua', 'kiev': 'ua',
            'kharkiv': 'ua', 'odessa': 'ua', 'lviv': 'ua', 'dnipro': 'ua',
        }
        
        return all_cities_global
