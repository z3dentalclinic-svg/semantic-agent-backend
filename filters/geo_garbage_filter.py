"""
GEO Garbage Filter - Universal multilingual geographical filtering
Works for ALL countries and ALL languages

Removes:
1. Queries with occupied territories (Ukraine-specific)
2. Queries with 2+ cities
3. Queries with districts from OTHER countries
4. Queries mentioning other countries

UNIVERSAL SUPPORT:
- Works for ANY country via target_country parameter (ua, by, kz, uz, pl, us, de, etc.)
- Loads cities automatically via geonamescache for ANY country
- Multilingual: supports all languages in geonamescache database
- Uses filters.DISTRICTS_EXTENDED for district validation

REQUIREMENTS:
- geonamescache - loads cities for any country
- filters.DISTRICTS_EXTENDED - district-to-country mapping {район: код_страны}
"""

import re
import logging
from typing import Dict, Set, List

logger = logging.getLogger("GeoGarbageFilter")


# ═══════════════════════════════════════════════════════════════════
# БАЗА ОККУПИРОВАННЫХ ТЕРРИТОРИЙ УКРАИНЫ
# Ukraine-specific: Crimea, Donetsk, Luhansk regions (since 2014)
# Kherson, Zaporizhzhia regions (partially, since 2022)
# ═══════════════════════════════════════════════════════════════════

OCCUPIED_TERRITORIES = {
    # ═══ КРЫМ / CRIMEA ═══
    "севастополь", "sevastopol", "sebastopol",
    "симферополь", "simferopol",
    "керчь", "kerch",
    "евпатория", "yevpatoria", "eupatoria",
    "ялта", "yalta",
    "феодосия", "feodosia", "theodosia",
    "джанкой", "dzhankoy",
    "алушта", "alushta",
    "бахчисарай", "bakhchisaray",
    "красноперекопск", "krasnoperekopsk",
    "армянск", "armyansk",
    "саки", "saki",
    "судак", "sudak",
    "белогорск", "belogorsk",
    "старый крым", "stary krym",
    "алупка", "alupka",
    "гурзуф", "gurzuf",
    "ливадия", "livadia",
    "массандра", "massandra",
    "гаспра", "gaspra",
    "форос", "foros",
    "партенит", "partenit",
    "коктебель", "koktebel",
    "новый свет", "novyi svet",
    "щелкино", "shchelkino",
    "ленино", "lenino",
    "красногвардейское", "krasnogvardeyskoye",
    "нижнегорский", "nizhnegorsky",
    "советский", "sovetsky",
    "кировское", "kirovskoye",
    "черноморское", "chernomorskoye",
    "раздольное", "razdolnoye",
    "первомайское", "pervomaiske",
    "октябрьское", "oktyabrskoye",
    "молодежное", "molodezhnoye",
    "мирный", "mirny",
    "инкерман", "inkerman",
    "балаклава", "balaklava",
    "крым", "crimea", "крыма", "крыму", "крымский", "крымская", "крымское",
    "арк", "ark",
    
    # ═══ ДОНЕЦКАЯ ОБЛАСТЬ / DONETSK REGION ═══
    "донецк", "donetsk",
    "горловка", "horlivka", "gorlovka",
    "макеевка", "makiivka", "makeyevka",
    "енакиево", "yenakiieve", "enakievo",
    "дебальцево", "debaltseve", "debaltsevo",
    "харцызск", "khartsyzsk",
    "снежное", "snizhne",
    "торез", "torez",
    "шахтерск", "shakhtarsk",
    "красноармейск", "krasnoarmiysk",
    "иловайск", "ilovaisk",
    "амвросиевка", "amvrosiivka",
    "старобешево", "starobesheve",
    "тельманово", "telmanove",
    "новоазовск", "novoazovsk",
    "ясиноватая", "yasynuvata",
    "авдеевка", "avdiivka", "avdeevka",
    "докучаевск", "dokuchaievsk",
    "зугрэс", "zuhres",
    "моспино", "mospyne",
    "углегорск", "vuhledar", "uglegorsk",
    "дзержинск", "toretsk", "dzerzhinsk",
    "горняк", "hirnyak",
    "комсомольское", "komsomolske",
    "новоселидовка", "novoselidivka",
    "седово", "siedove",
    "безыменное", "bezimenne",
    "сартана", "sartana",
    "старогнатовка", "starohnativka",
    "мангуш", "manhush",
    "володарское", "volodarske",
    "новотроицкое", "novotroitske",
    "оленовка", "olenivka",
    "еленовка", "yelenivka",
    "новоселовка", "novoselivka",
    "мариуполь", "mariupol",
    "талаковка", "talakivka",
    "виноградное", "vynohradne",
    "приморское", "prymorske",
    "урзуф", "urzuf",
    "днр", "dnr", "донецкая народная республика",
    "донбасс", "donbass", "донбасса", "донбассе",
    
    # ═══ ЛУГАНСКАЯ ОБЛАСТЬ / LUHANSK REGION ═══
    "луганск", "luhansk", "lugansk",
    "алчевск", "alchevsk",
    "стаханов", "stakhanov", "kadiivka", "кадиевка",
    "краснодон", "krasnodon",
    "ровеньки", "rovenky",
    "свердловск", "sverdlovsk", "dovzhansk", "довжанськ",
    "антрацит", "antratsyt",
    "брянка", "brianka",
    "красный луч", "krasny luch", "khrustalnyi", "хрустальный",
    "первомайск", "pervomaisk",
    "молодогвардейск", "molodohvardiisk",
    "лутугино", "lutuhyne",
    "ирмино", "irmino",
    "зоринск", "zorynsk",
    "перевальск", "perevalsk",
    "кировск", "kirovsk",
    "александровск", "oleksandrivsk",
    "суходольск", "sukhodilsk",
    "хрящеватое", "khriashchuvate",
    "металлист", "metalist",
    "георгиевка", "heorgiivka",
    "успенка", "uspenka",
    "изварино", "izvaryne",
    "краснодонецкое", "krasnodonetske",
    "петровское", "petrivske",
    "дьяково", "diakove",
    "лозовое", "lozove",
    "новосветловка", "novosvitlivka",
    "городище", "horodyshche",
    "артемово", "artemove",
    "родаково", "rodakove",
    "червонопартизанск", "chervonyi partyzan",
    "лнр", "lnr", "луганская народная республика",
    "лднр", "ldnr",
    
    # ═══ ХЕРСОНСКАЯ ОБЛАСТЬ / KHERSON REGION (частично) ═══
    "каховка", "kakhovka",
    "новая каховка", "nova kakhovka",
    "геническ", "henichesk",
    "скадовск", "skadovsk",
    "таврийск", "tavriisk",
    "чаплынка", "chaplynka",
    "калачи", "kalanchak",
    
    # ═══ ЗАПОРОЖСКАЯ ОБЛАСТЬ / ZAPORIZHZHIA REGION (частично) ═══
    "мелитополь", "melitopol",
    "бердянск", "berdiansk", "berdyansk",
    "энергодар", "enerhodar",
    "токмак", "tokmak",
    "василевка", "vasylivka",
    "приморск", "prymorsk",
    "пологи", "polohy",
    "михайловка", "mykhailivka",
    "молочанск", "molochansk",
    "якимовка", "yakymivka",
}


# ═══════════════════════════════════════════════════════════════════
# УНИВЕРСАЛЬНАЯ ФУНКЦИЯ ФИЛЬТРАЦИИ
# Universal filtering for ALL countries and ALL languages
# ═══════════════════════════════════════════════════════════════════

def filter_geo_garbage(data: dict, seed: str, target_country: str = 'ua') -> dict:
    """
    Universal multilingual geo-filter for ANY country
    
    Removes:
    1. Occupied territories (if target_country='ua')
    2. Queries with 2+ cities
    3. Queries with districts from OTHER countries
    4. Queries mentioning other countries
    
    Args:
        data: dict with "keywords" key (list of strings or dicts)
        seed: original search query
        target_country: country code (ua, by, kz, uz, pl, us, de, fr, es, etc.)
    
    Returns:
        data with filtered keywords
    
    Examples:
        # Ukraine
        filter_geo_garbage(data, "ремонт пылесосов киев", "ua")
        
        # Belarus
        filter_geo_garbage(data, "рамонт пыласосаў мінск", "by")
        
        # Poland
        filter_geo_garbage(data, "naprawa odkurzaczy warszawa", "pl")
        
        # USA
        filter_geo_garbage(data, "vacuum repair new york", "us")
    """
    if not data or "keywords" not in data:
        return data
    
    # ═══════════════════════════════════════════════════════════════
    # Импорт DISTRICTS_EXTENDED из filters
    # ═══════════════════════════════════════════════════════════════
    
    try:
        from filters import DISTRICTS_EXTENDED
        logger.info(f"[GEO_GARBAGE] Loaded {len(DISTRICTS_EXTENDED)} districts from DISTRICTS_EXTENDED")
    except ImportError:
        logger.warning("⚠️ DISTRICTS_EXTENDED not found in filters, using empty dict")
        DISTRICTS_EXTENDED = {}
    
    # ═══════════════════════════════════════════════════════════════
    # Загрузка базы городов через geonamescache
    # ═══════════════════════════════════════════════════════════════
    
    try:
        import geonamescache
        gc = geonamescache.GeonamesCache()
        has_geonames = True
        logger.info(f"[GEO_GARBAGE] geonamescache loaded successfully")
    except ImportError:
        logger.warning("⚠️ geonamescache not installed, using minimal fallback database")
        has_geonames = False
    
    seed_lower = seed.lower()
    target_country_upper = target_country.upper()
    
    # ═══════════════════════════════════════════════════════════════
    # Загрузка городов целевой страны + альтернативных названий
    # ═══════════════════════════════════════════════════════════════
    
    all_cities = {}  # {название: 'city'}
    
    if has_geonames:
        try:
            cities = gc.get_cities()
            
            for city_id, city_data in cities.items():
                country_code = city_data.get('countrycode', '').upper()
                
                if country_code != target_country_upper:
                    continue
                
                # Основное название
                name = city_data['name'].lower()
                all_cities[name] = 'city'
                
                # Альтернативные названия (мультиязычные)
                alt_names = city_data.get('alternatenames', [])
                for alt in alt_names:
                    if len(alt) > 2:
                        all_cities[alt.lower()] = 'city'
            
            logger.info(f"[GEO_GARBAGE] Loaded {len(all_cities)} city names for {target_country_upper}")
            
        except Exception as e:
            logger.warning(f"Error loading geonamescache cities: {e}")
            all_cities = _get_fallback_cities(target_country)
    else:
        all_cities = _get_fallback_cities(target_country)
    
    # Добавляем название страны
    try:
        if has_geonames:
            countries = gc.get_countries()
            country_data = countries.get(target_country_upper, {})
            if country_data:
                country_name = country_data.get('name', '').lower()
                if country_name:
                    all_cities[country_name] = 'country'
    except:
        pass
    
    # ═══════════════════════════════════════════════════════════════
    # Определяем город из seed (если есть)
    # ═══════════════════════════════════════════════════════════════
    
    seed_city = None
    seed_words = re.findall(r'[а-яёіїєґa-z]+', seed_lower)
    
    for word in seed_words:
        if word in all_cities and all_cities[word] == 'city':
            seed_city = word
            logger.info(f"[GEO_GARBAGE] Detected city in seed: '{seed_city}'")
            break
    
    # ═══════════════════════════════════════════════════════════════
    # Фильтрация keywords
    # ═══════════════════════════════════════════════════════════════
    
    unique_keywords = []
    stats = {
        'total': len(data["keywords"]),
        'blocked_occupied': 0,
        'blocked_multiple_cities': 0,
        'blocked_foreign_district': 0,
        'blocked_other_country': 0,
        'allowed': 0,
    }
    
    for item in data["keywords"]:
        # Поддержка строк и dict
        if isinstance(item, str):
            query = item
        elif isinstance(item, dict):
            query = item.get("query", "")
        else:
            continue
        
        query_lower = query.lower()
        words = re.findall(r'[а-яёіїєґa-z0-9]+', query_lower)
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 1: Оккупированные территории (только для UA)
        # ═══════════════════════════════════════════════════════════
        
        if target_country.lower() == 'ua':
            has_occupied = False
            for word in words:
                if word in OCCUPIED_TERRITORIES:
                    has_occupied = True
                    logger.info(f"[GEO_GARBAGE] ❌ OCCUPIED: '{query}' contains '{word}'")
                    stats['blocked_occupied'] += 1
                    break
            
            if has_occupied:
                continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 2: Множественные города
        # ═══════════════════════════════════════════════════════════
        
        found_cities = set()
        for word in words:
            if len(word) < 3:
                continue
            
            if word in all_cities and all_cities[word] == 'city':
                # Игнорируем город из seed
                if word != seed_city:
                    found_cities.add(word)
        
        if len(found_cities) >= 1:
            logger.info(f"[GEO_GARBAGE] ❌ MULTIPLE_CITIES: '{query}' contains cities: {found_cities}")
            stats['blocked_multiple_cities'] += 1
            continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 3: Районы из других стран
        # Используем DISTRICTS_EXTENDED: {район: код_страны}
        # ═══════════════════════════════════════════════════════════
        
        has_foreign_district = False
        
        for word in words:
            if word in DISTRICTS_EXTENDED:
                district_country = DISTRICTS_EXTENDED[word]
                
                # Если район принадлежит другой стране - блокируем
                if district_country != target_country.lower():
                    has_foreign_district = True
                    logger.info(f"[GEO_GARBAGE] ❌ FOREIGN_DISTRICT: '{query}' contains district '{word}' "
                              f"from {district_country.upper()}, but target is {target_country_upper}")
                    stats['blocked_foreign_district'] += 1
                    break
        
        if has_foreign_district:
            continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 4: Упоминание других стран
        # ═══════════════════════════════════════════════════════════
        
        # Мультиязычные названия стран
        other_countries = {
            # Русский
            'россия': 'ru', 'рф': 'ru', 'российская': 'ru', 'россии': 'ru',
            'беларусь': 'by', 'белоруссия': 'by', 'беларуси': 'by',
            'украина': 'ua', 'украине': 'ua', 'украины': 'ua',
            'польша': 'pl', 'польше': 'pl',
            'казахстан': 'kz', 'казахстане': 'kz',
            'узбекистан': 'uz', 'узбекистане': 'uz',
            'молдова': 'md', 'молдавия': 'md',
            'израиль': 'il', 'израиле': 'il',
            'германия': 'de', 'германии': 'de',
            'франция': 'fr', 'францию': 'fr',
            'испания': 'es', 'италия': 'it',
            
            # English
            'russia': 'ru', 'russian': 'ru',
            'belarus': 'by', 'belarusian': 'by',
            'ukraine': 'ua', 'ukrainian': 'ua',
            'poland': 'pl', 'polish': 'pl',
            'kazakhstan': 'kz', 'kazakh': 'kz',
            'uzbekistan': 'uz', 'uzbek': 'uz',
            'moldova': 'md', 'moldovan': 'md',
            'israel': 'il', 'israeli': 'il',
            'germany': 'de', 'german': 'de',
            'france': 'fr', 'french': 'fr',
            'spain': 'es', 'spanish': 'es',
            'italy': 'it', 'italian': 'it',
            'usa': 'us', 'america': 'us', 'american': 'us',
            
            # Беларуская
            'расія': 'ru', 'расіі': 'ru',
            'беларусь': 'by', 'беларусі': 'by',
            'украіна': 'ua',
            
            # Українська
            'росія': 'ru', 'російська': 'ru',
            'білорусь': 'by',
            
            # Polski
            'rosja': 'ru', 'rosyjski': 'ru',
            'białoruś': 'by',
            'ukraina': 'ua', 'ukraiński': 'ua',
            'polska': 'pl', 'polski': 'pl',
            
            # Deutsch
            'russland': 'ru',
            'weißrussland': 'by',
            'deutschland': 'de',
        }
        
        has_other_country = False
        for word in words:
            if word in other_countries:
                country_code = other_countries[word]
                
                # Игнорируем если это наша страна
                if country_code == target_country.lower():
                    continue
                
                # Игнорируем если слово есть в seed
                if word in seed_lower:
                    continue
                
                has_other_country = True
                logger.info(f"[GEO_GARBAGE] ❌ OTHER_COUNTRY: '{query}' mentions '{word}' ({country_code.upper()})")
                stats['blocked_other_country'] += 1
                break
        
        if has_other_country:
            continue
        
        # ═══════════════════════════════════════════════════════════
        # Запрос прошел все проверки - разрешаем
        # ═══════════════════════════════════════════════════════════
        
        unique_keywords.append(item)
        stats['allowed'] += 1
    
    # ═══════════════════════════════════════════════════════════════
    # Обновляем данные
    # ═══════════════════════════════════════════════════════════════
    
    data["keywords"] = unique_keywords
    
    if "total_count" in data:
        data["total_count"] = len(unique_keywords)
    if "count" in data:
        data["count"] = len(unique_keywords)
    
    logger.info(f"[GEO_GARBAGE] STATS for {target_country_upper}: {stats}")
    
    return data


def _get_fallback_cities(country_code: str) -> Dict[str, str]:
    """
    Minimal fallback city database if geonamescache unavailable
    Supports multiple countries
    """
    fallback_db = {
        'ua': {
            'київ': 'city', 'киев': 'city', 'kyiv': 'city',
            'харків': 'city', 'харьков': 'city', 'kharkiv': 'city',
            'одеса': 'city', 'одесса': 'city', 'odesa': 'city',
            'дніпро': 'city', 'днепр': 'city', 'dnipro': 'city',
            'львів': 'city', 'львов': 'city', 'lviv': 'city',
            'україна': 'country', 'украина': 'country', 'ukraine': 'country',
        },
        'by': {
            'мінск': 'city', 'минск': 'city', 'minsk': 'city',
            'гомель': 'city', 'homel': 'city',
            'магілёў': 'city', 'могилев': 'city', 'mogilev': 'city',
            'беларусь': 'country', 'belarus': 'country',
        },
        'kz': {
            'алматы': 'city', 'almaty': 'city',
            'астана': 'city', 'нұр-сұлтан': 'city', 'nur-sultan': 'city',
            'шымкент': 'city', 'shymkent': 'city',
            'қазақстан': 'country', 'казахстан': 'country', 'kazakhstan': 'country',
        },
        'uz': {
            'ташкент': 'city', 'tashkent': 'city',
            'самарканд': 'city', 'samarkand': 'city',
            'бухара': 'city', 'bukhara': 'city',
            'ўзбекистон': 'country', 'узбекистан': 'country', 'uzbekistan': 'country',
        },
        'pl': {
            'warszawa': 'city', 'варшава': 'city',
            'kraków': 'city', 'краков': 'city',
            'wrocław': 'city',
            'polska': 'country', 'poland': 'country', 'польша': 'country',
        },
        'us': {
            'new york': 'city', 'нью-йорк': 'city',
            'los angeles': 'city', 'лос-анджелес': 'city',
            'chicago': 'city', 'чикаго': 'city',
            'usa': 'country', 'united states': 'country', 'сша': 'country',
        },
        'de': {
            'berlin': 'city', 'берлин': 'city',
            'münchen': 'city', 'мюнхен': 'city',
            'hamburg': 'city', 'гамбург': 'city',
            'deutschland': 'country', 'germany': 'country', 'германия': 'country',
        },
    }
    
    return fallback_db.get(country_code.lower(), {})


# Экспорт
__all__ = [
    'filter_geo_garbage',
    'OCCUPIED_TERRITORIES',
]
