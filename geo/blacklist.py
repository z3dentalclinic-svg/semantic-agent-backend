"""
Geo blacklist generation module
Loads cities from geonamescache and embedded_cities
"""

def generate_geo_blacklist_full():
    """
    Generates global city dictionary with country codes
    Returns: dict {city_name: country_code}

    Fix (апрель 2026): разрешаем multiword alt-имена (биграммы/триграммы),
    чтобы в словарь попадали русские названия составных городов:
      - 'кривой рог' (UA) — ранее пропускалось из-за 'if \" \" in alt: continue'
      - 'нижний новгород' (RU)
      - 'посёлок котовского' (UA)
      - 'санкт-петербург' (RU) — уже попадал через dash, теперь и через space
    Защита от шума: max_words=3 (покрывает 'ростов на дону'),
    плюс существующие фильтры длины/алфавита.
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
                # Max 3 слова — покрывает 'ростов на дону', 'посёлок котовского',
                # но отсекает 'City of Kryvyi Rih' и длинные административные названия
                if len(alt.split()) > 3:
                    continue

                if not (3 <= len(alt) <= 40):
                    continue

                if not any(c.isalpha() for c in alt):
                    continue

                # Multiword alt (с пробелами): принимаем для всех скриптов
                # (latin + cyrillic), поскольку парсер работает по всему миру.
                # Фильтры качества против мусорных транслитераций:
                #   - каждое слово ≥ 2 символов (отсекает 'a b', 'st 1')
                #   - не смешиваем скрипты внутри одной биграммы
                if ' ' in alt:
                    words = [w for w in alt.split() if w]
                    if any(len(w) < 2 for w in words):
                        continue
                    has_cyrillic = any('\u0400' <= c <= '\u04FF' for c in alt)
                    has_latin = any('a' <= c.lower() <= 'z' for c in alt)
                    # Смесь кириллицы и латиницы в одном имени — мусор
                    if has_cyrillic and has_latin:
                        continue
                    # Ни латиница ни кириллица (только спецсимволы/цифры) — skip
                    if not (has_cyrillic or has_latin):
                        continue

                alt_clean = alt.replace('-', '').replace("'", "").replace(' ', '')
                if alt_clean.isalpha():
                    is_latin_cyrillic = all(
                        ('\u0000' <= c <= '\u007F') or  # ASCII (латиница)
                        ('\u0400' <= c <= '\u04FF') or  # Кириллица
                        c in ['-', "'", ' ']
                        for c in alt
                    )

                    if is_latin_cyrillic:
                        alt_lower = alt.lower().strip()
                        if alt_lower not in all_cities_global:
                            all_cities_global[alt_lower] = country

        print("✅ v5.6.1 TURBO: multiword alternatenames enabled")
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
