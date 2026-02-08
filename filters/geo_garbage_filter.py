"""
GEO Garbage Filter - WHITE-LIST Approach (v3.0)
Universal multilingual geographical filtering

ПРИНЦИП WHITE-LIST (РАЗРЕШАЮЩАЯ ЛОГИКА):
✅ Разрешено ТОЛЬКО:
   - seed_city (город из seed)
   - Официальные районы seed_city
   
❌ Блокируется ВСЁ остальное:
   - Любой другой город из geonamescache
   - Любой район другого города
   - Природные объекты (горы, реки, озера)
   - Упоминания областей без seed_city
   - Оккупированные территории (для UA)

КЛЮЧЕВОЕ ОТЛИЧИЕ от v2.0:
- НЕТ черных списков городов
- НЕТ ручного добавления стоп-слов
- Динамическая проверка через geonamescache
- Если город не seed_city → автоматический БЛОК

ПРИМЕРЫ:
seed = "ремонт днепр"

БЛОКИРУЕТСЯ (автоматически):
- "ремонт днепр щецин"         → щецин = город ≠ днепр → БЛОК
- "ремонт днепр эльбрус"       → эльбрус = локация ≠ днепр → БЛОК
- "ремонт днепр киев"          → киев = город ≠ днепр → БЛОК
- "ремонт днепр голосеевский"  → голосеевский = район Киева ≠ днепр → БЛОК

РАЗРЕШАЕТСЯ:
- "ремонт днепр"               → только seed_city → ОК
- "ремонт днепр амур"          → амур = район Днепра → ОК
- "ремонт днепр центральный"   → центральный = район Днепра → ОК
"""

import re
import logging
from typing import Dict, Set, List

logger = logging.getLogger("GeoGarbageFilter")


# ═══════════════════════════════════════════════════════════════════
# БАЗА ОККУПИРОВАННЫХ ТЕРРИТОРИЙ УКРАИНЫ
# Остается для специфичной блокировки (только для target_country='ua')
# ═══════════════════════════════════════════════════════════════════

OCCUPIED_TERRITORIES = {
    # Крым
    "севастополь", "sevastopol", "sebastopol", "симферополь", "simferopol",
    "керчь", "kerch", "евпатория", "yevpatoria", "eupatoria", "ялта", "yalta",
    "феодосия", "feodosia", "theodosia", "джанкой", "dzhankoy", "алушта", "alushta",
    "бахчисарай", "bakhchisaray", "красноперекопск", "krasnoperekopsk", "армянск", "armyansk",
    "саки", "saki", "судак", "sudak", "белогорск", "belogorsk", "старый крым", "stary krym",
    "алупка", "alupka", "гурзуф", "gurzuf", "ливадия", "livadia", "массандра", "massandra",
    "гаспра", "gaspra", "форос", "foros", "партенит", "partenit", "коктебель", "koktebel",
    "новый свет", "novyi svet", "щелкино", "shchelkino", "ленино", "lenino",
    "красногвардейское", "krasnogvardeyskoye", "нижнегорский", "nizhnegorsky",
    "советский", "sovetsky", "кировское", "kirovskoye", "черноморское", "chernomorskoye",
    "раздольное", "razdolnoye", "первомайское", "pervomaiske", "октябрьское", "oktyabrskoye",
    "молодежное", "molodezhnoye", "мирный", "mirny", "инкерман", "inkerman",
    "балаклава", "balaklava", "крым", "crimea", "крыма", "крыму", "крымский", 
    "крымская", "крымское", "арк", "ark",
    
    # Донецкая область
    "донецк", "donetsk", "горловка", "horlivka", "gorlovka", "макеевка", "makiivka", 
    "makeyevka", "енакиево", "yenakiieve", "enakievo", "дебальцево", "debaltseve", 
    "debaltsevo", "харцызск", "khartsyzsk", "снежное", "snizhne", "торез", "torez",
    "шахтерск", "shakhtarsk", "красноармейск", "krasnoarmiysk", "иловайск", "ilovaisk",
    "амвросиевка", "amvrosiivka", "старобешево", "starobesheve", "тельманово", "telmanove",
    "новоазовск", "novoazovsk", "ясиноватая", "yasynuvata", "авдеевка", "avdiivka", 
    "avdeevka", "докучаевск", "dokuchaievsk", "зугрэс", "zuhres", "моспино", "mospyne",
    "углегорск", "vuhledar", "uglegorsk", "дзержинск", "toretsk", "dzerzhinsk",
    "горняк", "hirnyak", "комсомольское", "komsomolske", "новоселидовка", "novoselidivka",
    "седово", "siedove", "безыменное", "bezimenne", "сартана", "sartana",
    "старогнатовка", "starohnativka", "мангуш", "manhush", "володарское", "volodarske",
    "новотроицкое", "novotroitske", "оленовка", "olenivka", "еленовка", "yelenivka",
    "новоселовка", "novoselivka", "мариуполь", "mariupol", "талаковка", "talakivka",
    "виноградное", "vynohradne", "приморское", "prymorske", "урзуф", "urzuf",
    "днр", "dnr", "донецкая народная республика", "донбасс", "donbass", "донбасса", "донбассе",
    
    # Луганская область
    "луганск", "luhansk", "lugansk", "алчевск", "alchevsk", "стаханов", "stakhanov",
    "kadiivka", "кадиевка", "краснодон", "krasnodon", "ровеньки", "rovenky",
    "свердловск", "sverdlovsk", "dovzhansk", "довжанськ", "антрацит", "antratsyt",
    "брянка", "brianka", "красный луч", "krasny luch", "khrustalnyi", "хрустальный",
    "первомайск", "pervomaisk", "молодогвардейск", "molodohvardiisk", "лутугино", "lutuhyne",
    "ирмино", "irmino", "зоринск", "zorynsk", "перевальск", "perevalsk", "кировск", "kirovsk",
    "александровск", "oleksandrivsk", "суходольск", "sukhodilsk", "хрящеватое", "khriashchuvate",
    "металлист", "metalist", "георгиевка", "heorgiivka", "успенка", "uspenka",
    "изварино", "izvaryne", "краснодонецкое", "krasnodonetske", "петровское", "petrivske",
    "дьяково", "diakove", "лозовое", "lozove", "новосветловка", "novosvitlivka",
    "городище", "horodyshche", "артемово", "artemove", "родаково", "rodakove",
    "червонопартизанск", "chervonyi partyzan", "лнр", "lnr", "луганская народная республика",
    "лднр", "ldnr",
    
    # Херсонская область (частично)
    "каховка", "kakhovka", "новая каховка", "nova kakhovka", "геническ", "henichesk",
    "скадовск", "skadovsk", "таврийск", "tavriisk", "чаплынка", "chaplynka",
    "калачи", "kalanchak",
    
    # Запорожская область (частично)
    "мелитополь", "melitopol", "бердянск", "berdiansk", "berdyansk", "энергодар", "enerhodar",
    "токмак", "tokmak", "василевка", "vasylivka", "приморск", "prymorsk", "пологи", "polohy",
    "михайловка", "mykhailivka", "молочанск", "molochansk", "якимовка", "yakymivka",
}


# ═══════════════════════════════════════════════════════════════════
# БАЗА РАЙОНОВ ГОРОДОВ
# Для определения разрешенных районов seed_city
# ═══════════════════════════════════════════════════════════════════

CITY_DISTRICTS = {
    "киев": {
        "голосеевский", "голосіївський", "holosiivskyi", "obolon", "оболонь", 
        "оболонський", "obolonsky", "печерск", "печерський", "pechersk", "pechersky", 
        "подол", "подільський", "podil", "podilsky", "шевченковский", "шевченківський", 
        "shevchenkivskyi", "святошин", "святошинський", "sviatoshyn", "sviatoshynsky", 
        "соломенка", "соломʼянський", "solomianskyi", "дарница", "дарницький", 
        "darnytsia", "darnytsky", "днепровский", "дніпровський", "dniprovskyi", 
        "деснянский", "деснянський", "desnianskyi"
    },
    
    "днепр": {
        "амур", "amur", "чечеловский", "чечелівський", "chechelivskyi", 
        "шевченковский", "шевченківський", "shevchenkivskyi", "соборный", "соборний", 
        "soborny", "центральный", "центральний", "tsentralnyi", "central", "индустриальный", 
        "індустріальний", "industrialnyi", "industrial", "новокодацкий", "новокодацький", 
        "novokodatsky", "самарский", "самарський", "samarsky"
    },
    
    "днепропетровск": {
        "амур", "amur", "чечеловский", "чечелівський", "шевченковский", "шевченківський", 
        "соборный", "соборний", "центральный", "центральний", "индустриальный", 
        "індустріальний", "новокодацкий", "новокодацький", "самарский", "самарський"
    },
    
    "харьков": {
        "киевский", "київський", "kyivskyi", "московский", "московський", "moskovsky", 
        "дзержинский", "дзержинський", "фрунзенский", "фрунзенський", "ленинский", 
        "ленінський", "октябрьский", "жовтневий", "zhovtnevyi", "червонозаводской", 
        "червонозаводський", "коминтерновский", "комінтернівський", "орджоникидзевский", 
        "орджонікідзевський", "индустриальный", "індустріальний"
    },
    
    "одесса": {
        "киевский", "київський", "kyivskyi", "малиновский", "малиновський", "malynovskyi", 
        "приморский", "приморський", "prymorskyi", "суворовский", "суворовський", "suvorovskyi"
    },
    
    "львов": {
        "галицкий", "галицький", "halytskyi", "железнодорожный", "залізничний", "zaliznychnyi", 
        "лычаковский", "личаківський", "lychakivskyi", "сиховский", "сихівський", "sykhivskyi", 
        "франковский", "франківський", "frankivskyi", "шевченковский", "шевченківський"
    },
    
    "запорожье": {
        "александровский", "олександрівський", "oleksandrivskyi", "вознесеновский", 
        "вознесенівський", "voznesenovskyi", "днепровский", "дніпровський", "dniprovskyi", 
        "заводской", "заводський", "zavodskyi", "коммунарский", "комунарський", "komunarskyi", 
        "ленинский", "ленінський", "leninskyi", "хортицкий", "хортицький", "khortytskyi"
    },
    
    "минск": {
        "уручье", "uruchye", "шабаны", "shabany", "каменная горка", "kamennaya gorka", 
        "серебрянка", "serebryanka"
    },
    
    "ташкент": {
        "чиланзар", "chilanzar", "юнусабад", "yunusabad", "сергели", "sergeli", 
        "яккасарай", "yakkasaray"
    },
}


# ═══════════════════════════════════════════════════════════════════
# WHITE-LIST ФИЛЬТР v3.0
# Разрешаем только seed_city и его районы, всё остальное - БЛОК
# ═══════════════════════════════════════════════════════════════════

def filter_geo_garbage(data: dict, seed: str, target_country: str = 'ua') -> dict:
    """
    WHITE-LIST гео-фильтр: разрешаем ТОЛЬКО seed_city и его районы
    
    ПРИНЦИП:
    ✅ РАЗРЕШЕНО:
       - seed_city (город из seed)
       - Районы seed_city (из CITY_DISTRICTS)
       
    ❌ БЛОКИРУЕТСЯ всё остальное:
       - Любой другой город (динамически из geonamescache)
       - Любой район другого города
       - Природные объекты (горы, реки, озера)
       - Оккупированные территории (для UA)
       - Упоминания областей без seed_city
    
    Args:
        data: dict with "keywords" key (list of strings or dicts)
        seed: original search query
        target_country: country code (ua, by, kz, uz, pl, us, de, etc.)
    
    Returns:
        data with filtered keywords
    
    Examples:
        seed = "ремонт пылесосов днепр"
        
        БЛОКИРУЕТСЯ:
        - "ремонт днепр щецин"         → щецин = город ≠ днепр
        - "ремонт днепр эльбрус"       → эльбрус = локация ≠ днепр
        - "ремонт днепр киев"          → киев = город ≠ днепр
        - "ремонт днепр голосеевский"  → район Киева ≠ Днепра
        
        РАЗРЕШАЕТСЯ:
        - "ремонт днепр"               → только seed_city
        - "ремонт днепр амур"          → амур = район Днепра
        - "ремонт днепр центральный"   → центральный = район Днепра
    """
    if not data or "keywords" not in data:
        return data
    
    # ═══════════════════════════════════════════════════════════════
    # Загрузка geonamescache
    # ═══════════════════════════════════════════════════════════════
    
    try:
        import geonamescache
        gc = geonamescache.GeonamesCache()
        has_geonames = True
        logger.info(f"[GEO_WHITE_LIST] geonamescache loaded successfully")
    except ImportError:
        logger.warning("⚠️ geonamescache not installed, using minimal fallback")
        has_geonames = False
    
    seed_lower = seed.lower()
    target_country_upper = target_country.upper()
    
    # Предлоги для удаления (выявление склеек)
    prepositions = {
        'в', 'на', 'из', 'под', 'во', 'до', 'возле', 'с', 'со', 'от', 'ко', 'за', 'над',
        'у', 'біля', 'поруч', 'коло', 'від', 'про',
        'in', 'at', 'near', 'from', 'to', 'on', 'by', 'with', 'for', 'of', 'about',
        'ў', 'каля', 'ля', 'пры',
        'w', 'na', 'przy', 'od', 'do', 'z', 'o',
    }
    
    # ═══════════════════════════════════════════════════════════════
    # Шаг 1: Загрузка ВСЕХ городов из geonamescache (ALL countries!)
    # ═══════════════════════════════════════════════════════════════
    
    all_cities_global = {}  # {название: страна}
    
    if has_geonames:
        try:
            cities = gc.get_cities()
            
            for city_id, city_data in cities.items():
                country_code = city_data.get('countrycode', '').upper()
                
                # Основное название
                name = city_data['name'].lower()
                all_cities_global[name] = country_code
                
                # Альтернативные названия (мультиязычные)
                alt_names = city_data.get('alternatenames', [])
                for alt in alt_names:
                    if len(alt) > 2:
                        all_cities_global[alt.lower()] = country_code
            
            logger.info(f"[GEO_WHITE_LIST] Loaded {len(all_cities_global)} city names from ALL countries")
            
        except Exception as e:
            logger.warning(f"Error loading geonamescache: {e}")
            all_cities_global = _get_fallback_cities_all()
    else:
        all_cities_global = _get_fallback_cities_all()
    
    # Добавляем города из CITY_DISTRICTS
    for city_name in CITY_DISTRICTS.keys():
        if city_name not in all_cities_global:
            all_cities_global[city_name] = target_country_upper
    
    # ═══════════════════════════════════════════════════════════════
    # Шаг 2: Определяем seed_city
    # ═══════════════════════════════════════════════════════════════
    
    seed_city = None
    seed_words = re.findall(r'[а-яёіїєґa-z]+', seed_lower)
    
    # Приоритет: ищем самый длинный город из CITY_DISTRICTS
    potential_cities = [c for c in CITY_DISTRICTS.keys() if c in seed_lower]
    if potential_cities:
        seed_city = max(potential_cities, key=len)
        logger.info(f"[GEO_WHITE_LIST] Detected seed_city: '{seed_city}' (from CITY_DISTRICTS)")
    
    # Если не нашли, ищем в all_cities_global
    if not seed_city:
        for word in seed_words:
            if word in all_cities_global:
                seed_city = word
                logger.info(f"[GEO_WHITE_LIST] Detected seed_city: '{seed_city}' (from geonames)")
                break
    
    if not seed_city:
        logger.warning(f"[GEO_WHITE_LIST] ⚠️ No city detected in seed: '{seed}'. All queries will pass.")
    
    # Получаем разрешенные районы seed_city
    allowed_districts = CITY_DISTRICTS.get(seed_city, set()) if seed_city else set()
    
    # ═══════════════════════════════════════════════════════════════
    # Шаг 3: Фильтрация keywords (WHITE-LIST логика)
    # ═══════════════════════════════════════════════════════════════
    
    unique_keywords = []
    stats = {
        'total': len(data["keywords"]),
        'blocked_occupied': 0,
        'blocked_foreign_city': 0,      # ← НОВОЕ! Чужой город из geonames
        'blocked_wrong_district': 0,
        'blocked_wrong_oblast': 0,
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
        
        # Удаляем предлоги
        clean_words = [w for w in words if w not in prepositions and len(w) > 1]
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 1: Оккупированные территории (только для UA)
        # ═══════════════════════════════════════════════════════════
        
        if target_country.lower() == 'ua':
            has_occupied = False
            for word in clean_words:
                if word in OCCUPIED_TERRITORIES:
                    has_occupied = True
                    logger.info(f"[GEO_WHITE_LIST] ❌ OCCUPIED: '{query}' contains '{word}'")
                    stats['blocked_occupied'] += 1
                    break
            
            if has_occupied:
                continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 2: WHITE-LIST логика - ЧУЖИЕ ГОРОДА
        # Если город != seed_city → БЛОК (динамически из geonames)
        # ═══════════════════════════════════════════════════════════
        
        if seed_city:
            found_foreign_cities = []
            
            for word in clean_words:
                # Проверяем в глобальной базе городов
                if word in all_cities_global:
                    # Если это НЕ seed_city → это чужой город → БЛОК
                    if word != seed_city:
                        found_foreign_cities.append(word)
            
            if found_foreign_cities:
                logger.info(f"[GEO_WHITE_LIST] ❌ FOREIGN_CITY: '{query}' contains cities: {found_foreign_cities}, but seed_city is '{seed_city}'")
                stats['blocked_foreign_city'] += 1
                continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 3: WHITE-LIST логика - ЧУЖИЕ РАЙОНЫ
        # Если район != район seed_city → БЛОК
        # ═══════════════════════════════════════════════════════════
        
        if seed_city and allowed_districts:
            has_wrong_district = False
            
            # Проверяем все районы из CITY_DISTRICTS
            for city, districts in CITY_DISTRICTS.items():
                if city == seed_city:
                    continue  # Пропускаем свой город
                
                # Если нашли район другого города → БЛОК
                for district in districts:
                    if district in query_lower:
                        has_wrong_district = True
                        logger.info(f"[GEO_WHITE_LIST] ❌ WRONG_DISTRICT: '{query}' contains district '{district}' "
                                  f"from city '{city}', but seed_city is '{seed_city}'")
                        stats['blocked_wrong_district'] += 1
                        break
                
                if has_wrong_district:
                    break
            
            if has_wrong_district:
                continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 4: Упоминание "область/обл" без seed_city
        # ═══════════════════════════════════════════════════════════
        
        if any(oblast_word in query_lower for oblast_word in ['область', 'обл', 'област', 'области']):
            if seed_city and seed_city not in query_lower:
                logger.info(f"[GEO_WHITE_LIST] ❌ WRONG_OBLAST: '{query}' mentions oblast but seed_city '{seed_city}' not found")
                stats['blocked_wrong_oblast'] += 1
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
    
    logger.info(f"[GEO_WHITE_LIST] STATS: {stats}")
    
    return data


def _get_fallback_cities_all() -> Dict[str, str]:
    """
    Минимальная база городов ВСЕХ стран (fallback)
    Формат: {город: код_страны}
    """
    return {
        # Украина
        'київ': 'UA', 'киев': 'UA', 'kyiv': 'UA',
        'харків': 'UA', 'харьков': 'UA', 'kharkiv': 'UA',
        'одеса': 'UA', 'одесса': 'UA', 'odesa': 'UA',
        'дніпро': 'UA', 'днепр': 'UA', 'dnipro': 'UA',
        'львів': 'UA', 'львов': 'UA', 'lviv': 'UA',
        'запоріжжя': 'UA', 'запорожье': 'UA', 'zaporizhzhia': 'UA',
        
        # Беларусь
        'мінск': 'BY', 'минск': 'BY', 'minsk': 'BY',
        'гомель': 'BY', 'homel': 'BY',
        'могилев': 'BY', 'mogilev': 'BY',
        
        # Польша
        'warszawa': 'PL', 'варшава': 'PL',
        'kraków': 'PL', 'краков': 'PL',
        'wrocław': 'PL',
        'szczecin': 'PL', 'щецин': 'PL',  # ← Важно для блокировки!
        'gdańsk': 'PL',
        
        # Казахстан
        'алматы': 'KZ', 'almaty': 'KZ',
        'астана': 'KZ', 'nur-sultan': 'KZ',
        'шымкент': 'KZ', 'shymkent': 'KZ',
        
        # Узбекистан
        'ташкент': 'UZ', 'tashkent': 'UZ',
        'самарканд': 'UZ', 'samarkand': 'UZ',
        
        # Россия (для блокировки)
        'москва': 'RU', 'moscow': 'RU',
        'санкт-петербург': 'RU', 'petersburg': 'RU',
        'новосибирск': 'RU', 'novosibirsk': 'RU',
        
        # США
        'new york': 'US',
        'los angeles': 'US',
        'chicago': 'US',
        
        # Германия
        'berlin': 'DE', 'берлин': 'DE',
        'münchen': 'DE', 'мюнхен': 'DE',
        'hamburg': 'DE',
        
        # Природные объекты (для блокировки)
        'эльбрус': 'RU',  # ← Гора, блокируется автоматически
        'elbrus': 'RU',
    }


# Экспорт
__all__ = [
    'filter_geo_garbage',
    'OCCUPIED_TERRITORIES',
    'CITY_DISTRICTS',
]
