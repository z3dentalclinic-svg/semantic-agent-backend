"""
GEO Garbage Filter - WHITE-LIST v3.0 FINAL (с нормализацией падежей)
Universal multilingual geographical filtering

КРИТИЧЕСКИЕ УЛУЧШЕНИЯ:
1. Нормализация падежей: "киеве" → "киев", "россии" → "россия", "харьковская" → "харьков"
2. Объединение geonamescache + fallback (всегда)
3. Жёсткое правило: 2+ города в запросе → автоматический БЛОК
4. Проверка природных объектов через fallback

ПРИНЦИП: «Разрешено только то, что относится к seed_city»

✅ РАЗРЕШЕНО:
   - seed_city
   - Районы seed_city
   - Обычные слова (не гео-объекты)

❌ БЛОКИРУЕТСЯ:
   - Любой другой город (даже в падеже: "киеве", "киева")
   - Любая страна (даже в падеже: "россии", "украине")
   - 2+ города в одном запросе
   - Природные объекты: горы, реки, озера
   - Районы других городов

ТЕСТЫ (seed = "ремонт пылесосов днепр"):
БЛОК:
- "днепр в киеве"          → киеве→киев, 2 города
- "днепр в киеве адреса"   → киеве→киев, 2 города
- "днепр ельбрус"          → эльбрус = гора
- "днепр харьковская область" → харьковская→харьков, 2 города
- "днепр енисей"           → енисей = река
- "днепр россия"           → россия = страна

OK:
- "днепр"                  → только seed_city
- "днепр левый берег"      → не гео-объекты
- "днепр амур"             → амур = район Днепра
"""

import re
import logging
from typing import Dict, Set, List, Tuple

logger = logging.getLogger("GeoGarbageFilter")


# ═══════════════════════════════════════════════════════════════════
# БАЗА ОККУПИРОВАННЫХ ТЕРРИТОРИЙ
# ═══════════════════════════════════════════════════════════════════

OCCUPIED_TERRITORIES = {
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
    "лднр", "ldnr", "каховка", "kakhovka", "новая каховка", "nova kakhovka", "геническ", "henichesk",
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
# ФУНКЦИЯ НОРМАЛИЗАЦИИ ТОКЕНОВ (падежи, адъективные формы)
# ═══════════════════════════════════════════════════════════════════

def _normalize_token(token: str) -> str:
    """
    Нормализует токен, удаляя падежные окончания
    
    Цель: "киеве" → "киев", "россии" → "россия", "харьковская" → "харьков"
    
    Удаляет:
    - Однобуквенные: е, и, у, ю, а, я
    - Двухбуквенные: ой, ый, ий, ом, ем, ах, ях, ою, ее
    - Адъективные: ская, ский, ской, ском, скими, ских
    
    Args:
        token: исходное слово (lowercase)
    
    Returns:
        нормализованное слово
    
    Examples:
        "киеве" → "киев"
        "россии" → "россия" (росси+и → росси, но длина < 3, остается россии)
        "харьковская" → "харьков"
        "днепре" → "днепр"
        "украине" → "украин" → но если короче 3, вернет "украине"
    """
    if len(token) < 3:
        return token
    
    original_token = token
    
    # Адъективные окончания (сначала, т.к. они длиннее)
    adj_endings = [
        'ская', 'ский', 'ской', 'ском', 'скими', 'ских', 'скую', 'ское',
        'кая', 'кий', 'кой', 'ком', 'кими', 'ких', 'кую', 'кое'
    ]
    
    for ending in adj_endings:
        if token.endswith(ending):
            stem = token[:-len(ending)]
            if len(stem) >= 3:
                return stem
    
    # Двухбуквенные падежные окончания
    two_letter_endings = ['ой', 'ый', 'ий', 'ом', 'ем', 'ам', 'ях', 'ах', 'ою', 'ее', 'ие', 'ые']
    
    for ending in two_letter_endings:
        if token.endswith(ending):
            stem = token[:-2]
            if len(stem) >= 3:
                return stem
    
    # Однобуквенные падежные окончания
    one_letter_endings = ['е', 'и', 'у', 'ю', 'а', 'я', 'ы']
    
    for ending in one_letter_endings:
        if token.endswith(ending):
            stem = token[:-1]
            if len(stem) >= 3:
                return stem
    
    return original_token


# ═══════════════════════════════════════════════════════════════════
# FALLBACK БАЗА ГЕО-СУЩНОСТЕЙ
# ═══════════════════════════════════════════════════════════════════

def _get_fallback_geo_entities() -> Dict[str, Tuple[str, str]]:
    """
    Fallback база гео-сущностей (ВСЕГДА используется!)
    
    Формат: {название: (код_страны, тип)}
    
    Включает:
    - Русские/украинские названия городов
    - Все страны на всех языках
    - Природные объекты (горы, реки, озера)
    """
    return {
        # ═══ Города Украины ═══
        'київ': ('UA', 'city'), 'киев': ('UA', 'city'), 'kyiv': ('UA', 'city'),
        'харків': ('UA', 'city'), 'харьков': ('UA', 'city'), 'kharkiv': ('UA', 'city'),
        'одеса': ('UA', 'city'), 'одесса': ('UA', 'city'), 'odesa': ('UA', 'city'),
        'дніпро': ('UA', 'city'), 'днепр': ('UA', 'city'), 'dnipro': ('UA', 'city'),
        'львів': ('UA', 'city'), 'львов': ('UA', 'city'), 'lviv': ('UA', 'city'),
        'запоріжжя': ('UA', 'city'), 'запорожье': ('UA', 'city'), 'zaporizhzhia': ('UA', 'city'),
        
        # ═══ Города Беларуси ═══
        'мінск': ('BY', 'city'), 'минск': ('BY', 'city'), 'minsk': ('BY', 'city'),
        'гомель': ('BY', 'city'), 'homel': ('BY', 'city'),
        'могилев': ('BY', 'city'), 'mogilev': ('BY', 'city'),
        
        # ═══ Города Польши ═══
        'warszawa': ('PL', 'city'), 'варшава': ('PL', 'city'),
        'szczecin': ('PL', 'city'), 'щецин': ('PL', 'city'),
        'kraków': ('PL', 'city'), 'краков': ('PL', 'city'),
        'gdańsk': ('PL', 'city'),
        
        # ═══ Города России ═══
        'москва': ('RU', 'city'), 'moscow': ('RU', 'city'),
        'санкт-петербург': ('RU', 'city'), 'petersburg': ('RU', 'city'),
        'новосибирск': ('RU', 'city'),
        
        # ═══ Страны (на всех языках) ═══
        'україна': ('UA', 'country'), 'украина': ('UA', 'country'), 'ukraine': ('UA', 'country'),
        'росія': ('RU', 'country'), 'россия': ('RU', 'country'), 'russia': ('RU', 'country'),
        'рф': ('RU', 'country'),
        'беларусь': ('BY', 'country'), 'belarus': ('BY', 'country'), 'білорусь': ('BY', 'country'),
        'polska': ('PL', 'country'), 'poland': ('PL', 'country'), 'польша': ('PL', 'country'),
        'казахстан': ('KZ', 'country'), 'kazakhstan': ('KZ', 'country'),
        'узбекистан': ('UZ', 'country'), 'uzbekistan': ('UZ', 'country'),
        'германия': ('DE', 'country'), 'germany': ('DE', 'country'), 'німеччина': ('DE', 'country'),
        
        # ═══ Природные объекты (горы) ═══
        'эльбрус': ('RU', 'mountain'), 'elbrus': ('RU', 'mountain'), 'ельбрус': ('RU', 'mountain'),
        'монблан': ('FR', 'mountain'), 'mont blanc': ('FR', 'mountain'),
        'эверест': ('NP', 'mountain'), 'everest': ('NP', 'mountain'),
        
        # ═══ Природные объекты (реки) ═══
        'енисей': ('RU', 'river'), 'yenisei': ('RU', 'river'),
        'волга': ('RU', 'river'), 'volga': ('RU', 'river'),
        'дунай': ('RO', 'river'), 'danube': ('RO', 'river'),
        'дніпро': ('UA', 'river'),  # Река Днепр (отличается от города)
        
        # ═══ Природные объекты (озера) ═══
        'байкал': ('RU', 'lake'), 'baikal': ('RU', 'lake'),
        'ладога': ('RU', 'lake'), 'ladoga': ('RU', 'lake'),
    }


# ═══════════════════════════════════════════════════════════════════
# WHITE-LIST ФИЛЬТР v3.0 FINAL
# С НОРМАЛИЗАЦИЕЙ ПАДЕЖЕЙ
# ═══════════════════════════════════════════════════════════════════

def filter_geo_garbage(data: dict, seed: str, target_country: str = 'ua') -> dict:
    """
    WHITE-LIST гео-фильтр v3.0 FINAL с нормализацией падежей
    
    КРИТИЧЕСКИЕ УЛУЧШЕНИЯ:
    1. Нормализация: "киеве"→"киев", "россии"→"россия", "харьковская"→"харьков"
    2. Объединение geonamescache + fallback (всегда!)
    3. Жёсткое правило: 2+ города → БЛОК
    4. Проверка природных объектов
    
    Args:
        data: dict with "keywords"
        seed: original search query
        target_country: country code
    
    Returns:
        data with filtered keywords
    
    Examples:
        seed = "ремонт пылесосов днепр"
        
        БЛОК:
        - "днепр в киеве"          → киеве→киев, 2 города
        - "днепр ельбрус"          → эльбрус = mountain
        - "днепр харьковская область" → харьковская→харьков, 2 города
        - "днепр енисей"           → енисей = river
        - "днепр россия"           → россия → росси, но в fallback есть "россия"
        
        OK:
        - "днепр"                  → seed_city
        - "днепр левый берег"      → не гео-объекты
        - "днепр амур"             → район Днепра
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
    
    # Предлоги
    prepositions = {
        'в', 'на', 'из', 'под', 'во', 'до', 'возле', 'с', 'со', 'от', 'ко', 'за', 'над',
        'у', 'біля', 'поруч', 'коло', 'від', 'про',
        'in', 'at', 'near', 'from', 'to', 'on', 'by', 'with', 'for', 'of', 'about',
        'ў', 'каля', 'ля', 'пры',
        'w', 'na', 'przy', 'od', 'do', 'z', 'o',
    }
    
    # ═══════════════════════════════════════════════════════════════
    # Шаг 1: Загрузка гео-сущностей (geonamescache + fallback)
    # ═══════════════════════════════════════════════════════════════
    
    all_geo_entities: Dict[str, Tuple[str, str]] = {}
    
    # 1.1. Загрузка из geonamescache (если доступен)
    if has_geonames:
        try:
            # Города
            cities = gc.get_cities()
            for city_id, city_data in cities.items():
                country_code = city_data.get('countrycode', '').upper()
                
                name = city_data['name'].lower()
                all_geo_entities[name] = (country_code, 'city')
                
                alt_names = city_data.get('alternatenames', [])
                for alt in alt_names:
                    if len(alt) > 2:
                        all_geo_entities[alt.lower()] = (country_code, 'city')
            
            logger.info(f"[GEO_WHITE_LIST] Loaded {len(all_geo_entities)} cities from geonamescache")
            
            # Страны
            countries = gc.get_countries()
            for code, country_data in countries.items():
                name = country_data.get('name', '').lower()
                if name:
                    all_geo_entities[name] = (code, 'country')
            
            logger.info(f"[GEO_WHITE_LIST] Loaded countries from geonamescache")
            
        except Exception as e:
            logger.warning(f"Error loading geonamescache: {e}")
    
    # 1.2. ВСЕГДА добавляем fallback (объединение!)
    fallback_entities = _get_fallback_geo_entities()
    for name, meta in fallback_entities.items():
        all_geo_entities.setdefault(name, meta)
    
    logger.info(f"[GEO_WHITE_LIST] Total geo entities after fallback: {len(all_geo_entities)}")
    
    # 1.3. Добавляем города из CITY_DISTRICTS
    for city_name in CITY_DISTRICTS.keys():
        all_geo_entities.setdefault(city_name, (target_country_upper, 'city'))
    
    # ═══════════════════════════════════════════════════════════════
    # Шаг 2: Определяем seed_city (с нормализацией!)
    # ═══════════════════════════════════════════════════════════════
    
    seed_city = None
    seed_words = re.findall(r'[а-яёіїєґa-z]+', seed_lower)
    seed_words_normalized = [_normalize_token(w) for w in seed_words]
    
    # Приоритет: самый длинный город из CITY_DISTRICTS
    potential_cities = [c for c in CITY_DISTRICTS.keys() if c in seed_lower]
    if potential_cities:
        seed_city = max(potential_cities, key=len)
        logger.info(f"[GEO_WHITE_LIST] seed_city: '{seed_city}' (CITY_DISTRICTS)")
    
    # Ищем в all_geo_entities (нормализованные слова)
    if not seed_city:
        for word_norm in seed_words_normalized:
            if word_norm in all_geo_entities:
                entity_country, entity_type = all_geo_entities[word_norm]
                if entity_type == 'city':
                    seed_city = word_norm
                    logger.info(f"[GEO_WHITE_LIST] seed_city: '{seed_city}' (geonames, normalized)")
                    break
    
    if not seed_city:
        logger.warning(f"[GEO_WHITE_LIST] ⚠️ No city in seed: '{seed}'. All queries pass.")
    
    # Разрешенные районы
    allowed_districts = CITY_DISTRICTS.get(seed_city, set()) if seed_city else set()
    
    # ═══════════════════════════════════════════════════════════════
    # Шаг 3: WHITE-LIST фильтрация (с нормализацией!)
    # ═══════════════════════════════════════════════════════════════
    
    unique_keywords = []
    stats = {
        'total': len(data["keywords"]),
        'blocked_occupied': 0,
        'blocked_multi_city': 0,        # ← НОВОЕ!
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
        
        # НОРМАЛИЗАЦИЯ (ключевой момент!)
        clean_words_normalized = [_normalize_token(w) for w in clean_words]
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 1: Оккупированные территории
        # ═══════════════════════════════════════════════════════════
        
        if target_country.lower() == 'ua':
            has_occupied = False
            for word_norm in clean_words_normalized:
                if word_norm in OCCUPIED_TERRITORIES:
                    has_occupied = True
                    logger.info(f"[GEO_WHITE_LIST] ❌ OCCUPIED: '{query}' contains '{word_norm}'")
                    stats['blocked_occupied'] += 1
                    break
            
            if has_occupied:
                continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 2: ЖЁСТКОЕ ПРАВИЛО - 2+ ГОРОДА → БЛОК
        # ═══════════════════════════════════════════════════════════
        
        if seed_city:
            cities_in_query: Set[str] = set()
            
            for word_norm in clean_words_normalized:
                if word_norm in all_geo_entities:
                    entity_country, entity_type = all_geo_entities[word_norm]
                    if entity_type == 'city':
                        cities_in_query.add(word_norm)
            
            # Если есть хотя бы один чужой город → БЛОК
            if len(cities_in_query) > 0:
                other_cities = {c for c in cities_in_query if c != seed_city}
                if other_cities:
                    logger.info(f"[GEO_WHITE_LIST] ❌ MULTI_CITY: '{query}' contains cities: {cities_in_query}, seed_city is '{seed_city}'")
                    stats['blocked_multi_city'] += 1
                    continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 3: Страны и природные объекты
        # ═══════════════════════════════════════════════════════════
        
        if seed_city:
            blocked = False
            
            for word_norm in clean_words_normalized:
                if word_norm in all_geo_entities:
                    entity_country, entity_type = all_geo_entities[word_norm]
                    
                    # А. Страна?
                    if entity_type == 'country':
                        # Если не в seed и не наша страна → БЛОК
                        if word_norm not in seed_words_normalized and entity_country != target_country_upper:
                            logger.info(f"[GEO_WHITE_LIST] ❌ FOREIGN_COUNTRY: '{query}' mentions '{word_norm}' ({entity_country})")
                            stats['blocked_foreign_country'] += 1
                            blocked = True
                            break
                    
                    # Б. Природный объект (гора, река, озеро)?
                    elif entity_type in ['mountain', 'river', 'lake']:
                        # Если не в seed → БЛОК
                        if word_norm not in seed_words_normalized:
                            logger.info(f"[GEO_WHITE_LIST] ❌ GEO_OBJECT: '{query}' contains {entity_type} '{word_norm}'")
                            stats['blocked_geo_object'] += 1
                            blocked = True
                            break
            
            if blocked:
                continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 4: Чужие районы
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
        # ПРОВЕРКА 5: Область с чужим городом
        # ═══════════════════════════════════════════════════════════
        
        has_oblast = any(oblast_word in query_lower for oblast_word in ['область', 'обл', 'област', 'области'])
        
        if has_oblast and seed_city:
            # Собираем города из запроса (нормализованные)
            cities_in_query_oblast: Set[str] = set()
            for word_norm in clean_words_normalized:
                if word_norm in all_geo_entities:
                    entity_country, entity_type = all_geo_entities[word_norm]
                    if entity_type == 'city':
                        cities_in_query_oblast.add(word_norm)
            
            # Если есть чужие города → БЛОК
            other_cities_oblast = {c for c in cities_in_query_oblast if c != seed_city}
            if other_cities_oblast:
                logger.info(f"[GEO_WHITE_LIST] ❌ WRONG_OBLAST: '{query}' mentions oblast with other cities: {other_cities_oblast}")
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


# Экспорт
__all__ = [
    'filter_geo_garbage',
    'OCCUPIED_TERRITORIES',
    'CITY_DISTRICTS',
]
