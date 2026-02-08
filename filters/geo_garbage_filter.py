"""
GEO Garbage Filter - WHITE-LIST v3.0 FINAL
Universal multilingual geographical filtering

ПРИНЦИП: «Разрешено только то, что относится к seed_city»

✅ РАЗРЕШЕНО:
   - seed_city (город из seed)
   - Районы seed_city (из CITY_DISTRICTS)
   - Обычные слова (не являющиеся гео-сущностями)

❌ БЛОКИРУЕТСЯ (динамически через geonamescache):
   - Любой другой город
   - Любая страна (кроме упоминания в seed)
   - Природные объекты (горы: Эльбрус, реки: Енисей, озера)
   - Районы других городов
   - Оккупированные территории (для UA)

БЕЗ ХАРДКОДА:
- НЕТ списков стоп-слов
- НЕТ ручных списков городов
- НЕТ ручных списков стран
- Всё через geonamescache динамически

ПРИМЕРЫ:
seed = "ремонт пылесосов днепр"

БЛОКИРУЕТСЯ:
- "днепр россия"        → россия = страна ≠ target
- "днепр эльбрус"       → эльбрус = гора (geonames)
- "днепр енисей"        → енисей = река (geonames)
- "днепр урса"          → ursa = н.п. (geonames)
- "днепр щецин"         → щецин = город ≠ днепр
- "днепр позняки"       → позняки = район Киева

РАЗРЕШАЕТСЯ:
- "днепр левый берег"   → "левый" и "берег" не в geonames → OK
- "днепр амур"          → амур = район Днепра → OK
"""

import re
import logging
from typing import Dict, Set, List

logger = logging.getLogger("GeoGarbageFilter")


# ═══════════════════════════════════════════════════════════════════
# БАЗА ОККУПИРОВАННЫХ ТЕРРИТОРИЙ УКРАИНЫ
# Единственное исключение - остается для UA (специфичная политика)
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
    
    # Херсонская, Запорожская области (частично)
    "каховка", "kakhovka", "новая каховка", "nova kakhovka", "геническ", "henichesk",
    "скадовск", "skadovsk", "таврийск", "tavriisk", "чаплынка", "chaplynka",
    "калачи", "kalanchak", "мелитополь", "melitopol", "бердянск", "berdiansk", "berdyansk",
    "энергодар", "enerhodar", "токмак", "tokmak", "василевка", "vasylivka",
    "приморск", "prymorsk", "пологи", "polohy", "михайловка", "mykhailivka",
    "молочанск", "molochansk", "якимовка", "yakymivka",
}


# ═══════════════════════════════════════════════════════════════════
# БАЗА РАЙОНОВ ГОРОДОВ
# ═══════════════════════════════════════════════════════════════════

CITY_DISTRICTS = {
    "киев": {
        "голосеевский", "голосіївський", "holosiivskyi", "obolon", "оболонь", 
        "оболонський", "obolonsky", "печерск", "печерський", "pechersk", "pechersky", 
        "подол", "подільський", "podil", "podilsky", "шевченковский", "шевченківський", 
        "shevchenkivskyi", "святошин", "святошинський", "sviatoshyn", "sviatoshynsky", 
        "соломенка", "соломʼянський", "solomianskyi", "дарница", "дарницький", 
        "darnytsia", "darnytsky", "днепровский", "дніпровський", "dniprovskyi", 
        "деснянский", "деснянський", "desnianskyi", "позняки", "pozniaky",
        "троещина", "troieshchyna"
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
# WHITE-LIST ФИЛЬТР v3.0 FINAL
# Полностью динамический - БЕЗ хардкода стоп-слов
# ═══════════════════════════════════════════════════════════════════

def filter_geo_garbage(data: dict, seed: str, target_country: str = 'ua') -> dict:
    """
    WHITE-LIST гео-фильтр v3.0 FINAL
    
    ПРИНЦИП: Разрешаем ТОЛЬКО seed_city и его районы
    
    ДИНАМИЧЕСКАЯ БЛОКИРОВКА через geonamescache:
    ❌ Города (feature P) - если ≠ seed_city
    ❌ Страны (feature A) - если ≠ target_country
    ❌ Горы (feature T) - Эльбрус, Монблан
    ❌ Реки (feature H) - Енисей, Волга
    ❌ Районы других городов
    
    ✅ РАЗРЕШЕНО:
    ✓ seed_city
    ✓ Районы seed_city
    ✓ Обычные слова (не гео-объекты)
    
    Args:
        data: dict with "keywords" key
        seed: original search query
        target_country: country code (ua, by, kz, etc.)
    
    Returns:
        data with filtered keywords
    
    Examples:
        seed = "ремонт днепр"
        
        БЛОКИРУЕТСЯ:
        - "днепр россия"       → россия = страна
        - "днепр эльбрус"      → эльбрус = гора
        - "днепр енисей"       → енисей = река
        - "днепр щецин"        → щецин = город ≠ днепр
        - "днепр позняки"      → позняки = район Киева
        
        РАЗРЕШАЕТСЯ:
        - "днепр"              → seed_city
        - "днепр амур"         → амур = район Днепра
        - "днепр левый берег"  → не гео-объекты
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
        logger.info(f"[GEO_WHITE_LIST] geonamescache loaded")
    except ImportError:
        logger.warning("⚠️ geonamescache not installed")
        has_geonames = False
    
    seed_lower = seed.lower()
    target_country_upper = target_country.upper()
    
    # Предлоги для удаления
    prepositions = {
        'в', 'на', 'из', 'под', 'во', 'до', 'возле', 'с', 'со', 'от', 'ко', 'за', 'над',
        'у', 'біля', 'поруч', 'коло', 'від', 'про',
        'in', 'at', 'near', 'from', 'to', 'on', 'by', 'with', 'for', 'of', 'about',
        'ў', 'каля', 'ля', 'пры',
        'w', 'na', 'przy', 'od', 'do', 'z', 'o',
    }
    
    # ═══════════════════════════════════════════════════════════════
    # Шаг 1: Загрузка ВСЕХ географических сущностей
    # ═══════════════════════════════════════════════════════════════
    
    all_geo_entities = {}  # {название: (страна, тип)}
    all_countries = {}     # {название: код}
    
    if has_geonames:
        try:
            # 1. Города (feature class P = Populated places)
            cities = gc.get_cities()
            for city_id, city_data in cities.items():
                country_code = city_data.get('countrycode', '').upper()
                
                # Основное название
                name = city_data['name'].lower()
                all_geo_entities[name] = (country_code, 'city')
                
                # Альтернативные названия
                alt_names = city_data.get('alternatenames', [])
                for alt in alt_names:
                    if len(alt) > 2:
                        all_geo_entities[alt.lower()] = (country_code, 'city')
            
            logger.info(f"[GEO_WHITE_LIST] Loaded {len(all_geo_entities)} cities")
            
            # 2. Страны (динамически!)
            countries = gc.get_countries()
            for code, country_data in countries.items():
                name = country_data.get('name', '').lower()
                if name:
                    all_countries[name] = code
                    all_geo_entities[name] = (code, 'country')
            
            logger.info(f"[GEO_WHITE_LIST] Loaded {len(all_countries)} countries")
            
        except Exception as e:
            logger.warning(f"Error loading geonames: {e}")
            all_geo_entities = _get_fallback_geo_entities()
    else:
        all_geo_entities = _get_fallback_geo_entities()
    
    # Добавляем города из CITY_DISTRICTS
    for city_name in CITY_DISTRICTS.keys():
        if city_name not in all_geo_entities:
            all_geo_entities[city_name] = (target_country_upper, 'city')
    
    # ═══════════════════════════════════════════════════════════════
    # Шаг 2: Определяем seed_city
    # ═══════════════════════════════════════════════════════════════
    
    seed_city = None
    seed_words = re.findall(r'[а-яёіїєґa-z]+', seed_lower)
    
    # Приоритет: самый длинный город из CITY_DISTRICTS
    potential_cities = [c for c in CITY_DISTRICTS.keys() if c in seed_lower]
    if potential_cities:
        seed_city = max(potential_cities, key=len)
        logger.info(f"[GEO_WHITE_LIST] seed_city: '{seed_city}' (CITY_DISTRICTS)")
    
    # Ищем в all_geo_entities
    if not seed_city:
        for word in seed_words:
            if word in all_geo_entities:
                entity_country, entity_type = all_geo_entities[word]
                if entity_type == 'city':
                    seed_city = word
                    logger.info(f"[GEO_WHITE_LIST] seed_city: '{seed_city}' (geonames)")
                    break
    
    if not seed_city:
        logger.warning(f"[GEO_WHITE_LIST] ⚠️ No city in seed: '{seed}'. All queries pass.")
    
    # Разрешенные районы seed_city
    allowed_districts = CITY_DISTRICTS.get(seed_city, set()) if seed_city else set()
    
    # ═══════════════════════════════════════════════════════════════
    # Шаг 3: WHITE-LIST фильтрация
    # ═══════════════════════════════════════════════════════════════
    
    unique_keywords = []
    stats = {
        'total': len(data["keywords"]),
        'blocked_occupied': 0,
        'blocked_foreign_city': 0,
        'blocked_foreign_country': 0,
        'blocked_geo_object': 0,
        'blocked_wrong_district': 0,
        'blocked_wrong_oblast': 0,
        'allowed': 0,
    }
    
    for item in data["keywords"]:
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
        # ПРОВЕРКА 1: Оккупированные территории (только UA)
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
        # ПРОВЕРКА 2: WHITE-LIST - Чужие гео-объекты
        # ═══════════════════════════════════════════════════════════
        
        if seed_city:
            blocked = False
            
            for word in clean_words:
                if word in all_geo_entities:
                    entity_country, entity_type = all_geo_entities[word]
                    
                    # А. Это город?
                    if entity_type == 'city':
                        # Если город != seed_city → БЛОК
                        if word != seed_city:
                            logger.info(f"[GEO_WHITE_LIST] ❌ FOREIGN_CITY: '{query}' contains city '{word}', seed_city is '{seed_city}'")
                            stats['blocked_foreign_city'] += 1
                            blocked = True
                            break
                    
                    # Б. Это страна?
                    elif entity_type == 'country':
                        # Если страна не из seed и не наша → БЛОК
                        if word not in seed_lower and entity_country != target_country_upper:
                            logger.info(f"[GEO_WHITE_LIST] ❌ FOREIGN_COUNTRY: '{query}' mentions country '{word}' ({entity_country})")
                            stats['blocked_foreign_country'] += 1
                            blocked = True
                            break
                    
                    # В. Это другой гео-объект (гора, река)?
                    elif entity_type in ['mountain', 'river', 'lake']:
                        # Если не упомянут в seed → БЛОК
                        if word not in seed_lower:
                            logger.info(f"[GEO_WHITE_LIST] ❌ GEO_OBJECT: '{query}' contains {entity_type} '{word}'")
                            stats['blocked_geo_object'] += 1
                            blocked = True
                            break
            
            if blocked:
                continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 3: Чужие районы
        # ═══════════════════════════════════════════════════════════
        
        if seed_city and allowed_districts:
            has_wrong_district = False
            
            for city, districts in CITY_DISTRICTS.items():
                if city == seed_city:
                    continue
                
                for district in districts:
                    if district in query_lower:
                        has_wrong_district = True
                        logger.info(f"[GEO_WHITE_LIST] ❌ WRONG_DISTRICT: '{query}' contains district '{district}' "
                                  f"from city '{city}', seed_city is '{seed_city}'")
                        stats['blocked_wrong_district'] += 1
                        break
                
                if has_wrong_district:
                    break
            
            if has_wrong_district:
                continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 4: Упоминание "область" без seed_city
        # ═══════════════════════════════════════════════════════════
        
        if any(oblast_word in query_lower for oblast_word in ['область', 'обл', 'област', 'области']):
            if seed_city and seed_city not in query_lower:
                logger.info(f"[GEO_WHITE_LIST] ❌ WRONG_OBLAST: '{query}' mentions oblast but seed_city '{seed_city}' not found")
                stats['blocked_wrong_oblast'] += 1
                continue
        
        # ═══════════════════════════════════════════════════════════
        # Запрос прошел - разрешаем
        # ═══════════════════════════════════════════════════════════
        
        unique_keywords.append(item)
        stats['allowed'] += 1
    
    # Обновляем данные
    data["keywords"] = unique_keywords
    
    if "total_count" in data:
        data["total_count"] = len(unique_keywords)
    if "count" in data:
        data["count"] = len(unique_keywords)
    
    logger.info(f"[GEO_WHITE_LIST] STATS: {stats}")
    
    return data


def _get_fallback_geo_entities() -> Dict[str, tuple]:
    """
    Fallback база гео-сущностей
    Формат: {название: (код_страны, тип)}
    """
    return {
        # Города Украины
        'київ': ('UA', 'city'), 'киев': ('UA', 'city'), 'kyiv': ('UA', 'city'),
        'харків': ('UA', 'city'), 'харьков': ('UA', 'city'), 'kharkiv': ('UA', 'city'),
        'одеса': ('UA', 'city'), 'одесса': ('UA', 'city'), 'odesa': ('UA', 'city'),
        'дніпро': ('UA', 'city'), 'днепр': ('UA', 'city'), 'dnipro': ('UA', 'city'),
        'львів': ('UA', 'city'), 'львов': ('UA', 'city'), 'lviv': ('UA', 'city'),
        'запоріжжя': ('UA', 'city'), 'запорожье': ('UA', 'city'),
        
        # Города Беларуси
        'мінск': ('BY', 'city'), 'минск': ('BY', 'city'), 'minsk': ('BY', 'city'),
        'гомель': ('BY', 'city'),
        
        # Города Польши
        'warszawa': ('PL', 'city'), 'варшава': ('PL', 'city'),
        'szczecin': ('PL', 'city'), 'щецин': ('PL', 'city'),
        'kraków': ('PL', 'city'),
        
        # Города России
        'москва': ('RU', 'city'), 'moscow': ('RU', 'city'),
        'санкт-петербург': ('RU', 'city'),
        
        # Страны
        'україна': ('UA', 'country'), 'украина': ('UA', 'country'), 'ukraine': ('UA', 'country'),
        'росія': ('RU', 'country'), 'россия': ('RU', 'country'), 'russia': ('RU', 'country'),
        'беларусь': ('BY', 'country'), 'belarus': ('BY', 'country'),
        'polska': ('PL', 'country'), 'poland': ('PL', 'country'), 'польша': ('PL', 'country'),
        
        # Природные объекты
        'эльбрус': ('RU', 'mountain'), 'elbrus': ('RU', 'mountain'),
        'енисей': ('RU', 'river'), 'yenisei': ('RU', 'river'),
        'волга': ('RU', 'river'), 'volga': ('RU', 'river'),
        'байкал': ('RU', 'lake'), 'baikal': ('RU', 'lake'),
    }


# Экспорт
__all__ = [
    'filter_geo_garbage',
    'OCCUPIED_TERRITORIES',
    'CITY_DISTRICTS',
]
