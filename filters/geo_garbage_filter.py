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
import json
import os
from typing import Dict, Set, List, Tuple

logger = logging.getLogger("GeoGarbageFilter")


# ═══════════════════════════════════════════════════════════════════
# ДИНАМИЧЕСКАЯ ЗАГРУЗКА РАЙОНОВ (districts.json + geonamescache)
# ═══════════════════════════════════════════════════════════════════

# Маппинг: любое_имя_города → canonical (из geonamescache)
CITY_CANONICAL_MAP: Dict[str, str] = {}

# Маппинг: canonical_city → set районов (из districts.json)
CANONICAL_CITY_DISTRICTS: Dict[str, Set[str]] = {}

# Маппинг: район → canonical_city (обратный lookup)
DISTRICT_TO_CANONICAL: Dict[str, str] = {}

# Маппинг: country_name (ru/uk/en) → (country_code, 'country')
COUNTRY_NAMES_MULTILINGUAL: Dict[str, tuple] = {}

try:
    import geonamescache
    _gc = geonamescache.GeonamesCache()
    _cities = _gc.get_cities()
    
    # Строим CITY_CANONICAL_MAP: сначала мелкие, потом крупные (крупные перезаписывают)
    for _city_data in sorted(_cities.values(), key=lambda c: c.get('population', 0)):
        _canonical = _city_data['name'].lower()
        CITY_CANONICAL_MAP[_canonical] = _canonical
        for _alt in _city_data.get('alternatenames', []):
            if len(_alt) > 1:
                CITY_CANONICAL_MAP[_alt.lower()] = _canonical
    
    logger.info(f"[GEO_DISTRICTS] CITY_CANONICAL_MAP: {len(CITY_CANONICAL_MAP)} entries")
    
    # Загружаем districts.json и строим CANONICAL_CITY_DISTRICTS
    _districts_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "districts.json")
    if os.path.exists(_districts_path):
        with open(_districts_path, "r", encoding="utf-8") as _f:
            _raw_districts = json.load(_f)
        
        from collections import defaultdict
        _temp = defaultdict(set)
        
        for _district_name, _info in _raw_districts.items():
            _city_raw = _info.get('city', '').lower()
            # Маппим city из districts.json на canonical через geonamescache
            _canonical_city = CITY_CANONICAL_MAP.get(_city_raw, _city_raw)
            _temp[_canonical_city].add(_district_name)
            DISTRICT_TO_CANONICAL[_district_name] = _canonical_city
        
        CANONICAL_CITY_DISTRICTS = dict(_temp)
        logger.info(f"[GEO_DISTRICTS] CANONICAL_CITY_DISTRICTS: {len(CANONICAL_CITY_DISTRICTS)} cities, "
                    f"{len(DISTRICT_TO_CANONICAL)} districts")
    else:
        logger.warning(f"[GEO_DISTRICTS] districts.json not found at {_districts_path}")
        
except ImportError:
    logger.warning("[GEO_DISTRICTS] geonamescache not available, dynamic districts disabled")
except Exception as e:
    logger.error(f"[GEO_DISTRICTS] Error loading: {e}")

# Загружаем названия стран на ru/uk/en через Babel
try:
    from babel import Locale
    _countries = _gc.get_countries() if _gc else {}
    
    # Английские имена
    for _code, _data in _countries.items():
        COUNTRY_NAMES_MULTILINGUAL[_data['name'].lower()] = (_code, 'country')
    
    # Русские и украинские через Babel
    for _lang in ['ru', 'uk']:
        _locale = Locale(_lang)
        for _code in _countries.keys():
            _name = _locale.territories.get(_code)
            if _name and len(_name) > 2:
                COUNTRY_NAMES_MULTILINGUAL[_name.lower()] = (_code, 'country')
    
    logger.info(f"[GEO_DISTRICTS] COUNTRY_NAMES_MULTILINGUAL: {len(COUNTRY_NAMES_MULTILINGUAL)} entries")
except ImportError:
    logger.warning("[GEO_DISTRICTS] babel not available, multilingual country names disabled")
except Exception as e:
    logger.error(f"[GEO_DISTRICTS] Error loading country names: {e}")


# ═══════════════════════════════════════════════════════════════════
# БАЗА ОККУПИРОВАННЫХ ТЕРРИТОРИЙ
# ═══════════════════════════════════════════════════════════════════

OCCUPIED_TERRITORIES = {
    "севастополь", "sevastopol", "sebastopol", "симферополь", "simferopol",
    "керчь", "kerch", "евпатория", "yevpatoria", "eupatoria", "ялта", "yalta", "ялт",
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
        
        # ═══ Оккупированные города (тоже city для блокировки через MULTI_CITY) ═══
        'севастополь': ('UA', 'city'), 'sevastopol': ('UA', 'city'),
        'симферополь': ('UA', 'city'), 'simferopol': ('UA', 'city'),
        'ялта': ('UA', 'city'), 'yalta': ('UA', 'city'), 'ялт': ('UA', 'city'),  # + нормализованная
        'керчь': ('UA', 'city'), 'kerch': ('UA', 'city'),
        'донецк': ('UA', 'city'), 'donetsk': ('UA', 'city'),
        'луганск': ('UA', 'city'), 'luhansk': ('UA', 'city'),
        'мариуполь': ('UA', 'city'), 'mariupol': ('UA', 'city'),
        'мелитополь': ('UA', 'city'), 'melitopol': ('UA', 'city'),
        'бердянск': ('UA', 'city'), 'berdiansk': ('UA', 'city'),
        
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
        'новосибирск': ('RU', 'city'), 'novosibirsk': ('RU', 'city'),
        'екатеринбург': ('RU', 'city'), 'yekaterinburg': ('RU', 'city'),
        'казань': ('RU', 'city'), 'kazan': ('RU', 'city'),
        'йошкар-ола': ('RU', 'city'), 'yoshkar-ola': ('RU', 'city'),
        'йошкар': ('RU', 'city'), 'ола': ('RU', 'city'),  # компоненты составного названия
        'нижний новгород': ('RU', 'city'),
        'самара': ('RU', 'city'), 'samara': ('RU', 'city'),
        'омск': ('RU', 'city'), 'omsk': ('RU', 'city'),
        'челябинск': ('RU', 'city'), 'chelyabinsk': ('RU', 'city'),
        
        # ═══ Страны (на всех языках + нормализованные формы) ═══
        'україна': ('UA', 'country'), 'украина': ('UA', 'country'), 'ukraine': ('UA', 'country'),
        'украин': ('UA', 'country'),  # нормализованная форма от "украина", "украине"
        
        'росія': ('RU', 'country'), 'россия': ('RU', 'country'), 'russia': ('RU', 'country'),
        'росси': ('RU', 'country'),  # нормализованная форма от "россия", "россии"
        'рф': ('RU', 'country'),
        
        'беларусь': ('BY', 'country'), 'belarus': ('BY', 'country'), 'білорусь': ('BY', 'country'),
        'беларус': ('BY', 'country'),  # нормализованная форма
        
        'polska': ('PL', 'country'), 'poland': ('PL', 'country'), 'польша': ('PL', 'country'),
        'польш': ('PL', 'country'),  # нормализованная форма
        
        'казахстан': ('KZ', 'country'), 'kazakhstan': ('KZ', 'country'),
        'казахстан': ('KZ', 'country'),  # не меняется при нормализации
        
        'узбекистан': ('UZ', 'country'), 'uzbekistan': ('UZ', 'country'),
        'узбекистан': ('UZ', 'country'),  # не меняется при нормализации
        
        'германия': ('DE', 'country'), 'germany': ('DE', 'country'), 'німеччина': ('DE', 'country'),
        'германи': ('DE', 'country'),  # нормализованная форма
        
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
    
    # 1.3. Названия стран на ru/uk/en (через Babel)
    for name, meta in COUNTRY_NAMES_MULTILINGUAL.items():
        all_geo_entities.setdefault(name, meta)
    
    logger.info(f"[GEO_WHITE_LIST] Total geo entities after fallback+countries: {len(all_geo_entities)}")
    
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
    
    # Разрешенные районы — динамически из districts.json + geonamescache
    # seed_city "днепр" → canonical "dnipro" → 126 районов из districts.json
    seed_canonical = CITY_CANONICAL_MAP.get(seed_city, seed_city) if seed_city else None
    allowed_districts = set()
    if seed_canonical:
        # Динамические районы из districts.json
        allowed_districts = CANONICAL_CITY_DISTRICTS.get(seed_canonical, set())
        # Плюс хардкодные (fallback, если districts.json неполный)
        allowed_districts = allowed_districts | CITY_DISTRICTS.get(seed_city, set())
        logger.info(f"[GEO_WHITE_LIST] seed_city='{seed_city}' → canonical='{seed_canonical}' → "
                    f"{len(allowed_districts)} allowed districts")
    
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
        
        # Извлекаем также слова через дефис: "южно-сахалинск", "санкт-петербург"
        hyphenated = re.findall(r'[а-яёіїєґa-z0-9]+-[а-яёіїєґa-z0-9]+(?:-[а-яёіїєґa-z0-9]+)*', query_lower)
        
        # Удаляем предлоги
        clean_words = [w for w in words if w not in prepositions and len(w) > 1]
        # Добавляем дефисные слова
        clean_words = clean_words + [h for h in hyphenated if h not in clean_words]
        
        # НОРМАЛИЗАЦИЯ (ключевой момент!)
        clean_words_normalized = [_normalize_token(w) for w in clean_words]
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 1: Оккупированные территории (блокируем ВСЕГДА!)
        # Убрано условие target_country == 'ua'
        # ═══════════════════════════════════════════════════════════
        
        has_occupied = False
        for raw_word, word_norm in zip(clean_words, clean_words_normalized):
            if raw_word in OCCUPIED_TERRITORIES or word_norm in OCCUPIED_TERRITORIES:
                has_occupied = True
                matched = raw_word if raw_word in OCCUPIED_TERRITORIES else word_norm
                logger.info(f"[GEO_WHITE_LIST] ❌ OCCUPIED: '{query}' contains '{matched}'")
                stats['blocked_occupied'] += 1
                break
        
        if has_occupied:
            continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 2: ЖЁСТКОЕ ПРАВИЛО - 2+ ГОРОДА → БЛОК
        # Но сначала исключаем разрешенные районы seed_city
        # ═══════════════════════════════════════════════════════════
        
        if seed_city:
            cities_in_query: Set[str] = set()
            
            for raw_word, word_norm in zip(clean_words, clean_words_normalized):
                # КРИТИЧНО: Пропускаем разрешенные районы seed_city
                if raw_word in allowed_districts or word_norm in allowed_districts:
                    continue
                
                # Проверяем обе формы — raw и normalized
                for check_word in [raw_word, word_norm]:
                    if check_word in all_geo_entities:
                        entity_country, entity_type = all_geo_entities[check_word]
                        if entity_type == 'city':
                            # Защита от ложных срабатываний: короткие обычные слова
                            # "час" = город Chas (Чехия), но это обычное слово
                            if len(check_word) <= 3:
                                continue
                            cities_in_query.add(check_word)
                            break
            
            # Если есть хотя бы один чужой город → БЛОК
            if len(cities_in_query) > 0:
                other_cities = {c for c in cities_in_query if c != seed_city}
                if other_cities:
                    logger.info(f"[GEO_WHITE_LIST] ❌ MULTI_CITY: '{query}' contains cities: {cities_in_query}, seed_city is '{seed_city}'")
                    stats['blocked_multi_city'] += 1
                    continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 3: Страны и природные объекты
        # Также пропускаем разрешенные районы seed_city
        # ═══════════════════════════════════════════════════════════
        
        if seed_city:
            blocked = False
            
            for raw_word, word_norm in zip(clean_words, clean_words_normalized):
                # КРИТИЧНО: Пропускаем разрешенные районы seed_city
                if raw_word in allowed_districts or word_norm in allowed_districts:
                    continue
                
                # Проверяем обе формы
                matched_entity = None
                for check_word in [raw_word, word_norm]:
                    if check_word in all_geo_entities:
                        matched_entity = (check_word, all_geo_entities[check_word])
                        break
                
                if not matched_entity:
                    continue
                    
                check_word, (entity_country, entity_type) = matched_entity
                
                # А. Страна?
                if entity_type == 'country':
                    # Любая страна, не упомянутая в seed, считается мусором
                    if raw_word not in seed_words and word_norm not in seed_words_normalized:
                        logger.info(
                            f"[GEO_WHITE_LIST] ❌ COUNTRY_MENTION: '{query}' mentions country '{check_word}' ({entity_country})"
                        )
                        stats['blocked_foreign_country'] += 1
                        blocked = True
                        break
                
                # Б. Природный объект (гора, река, озеро)?
                elif entity_type in ['mountain', 'river', 'lake']:
                    if raw_word not in seed_words and word_norm not in seed_words_normalized:
                        logger.info(f"[GEO_WHITE_LIST] ❌ GEO_OBJECT: '{query}' contains {entity_type} '{check_word}'")
                        stats['blocked_geo_object'] += 1
                        blocked = True
                        break
            
            if blocked:
                continue
        
        # ═══════════════════════════════════════════════════════════
        # ПРОВЕРКА 4: Чужие районы (динамическая через districts.json)
        # Если слово = район другого города → БЛОК
        # Проверяем и raw и normalized формы (normalize обрезает окончания)
        # ═══════════════════════════════════════════════════════════
        
        if seed_city and seed_canonical:
            has_wrong_district = False
            
            # Проверяем пары (raw, normalized) вместе
            for raw_word, word_norm in zip(clean_words, clean_words_normalized):
                # Пропускаем разрешенные районы seed_city
                if raw_word in allowed_districts or word_norm in allowed_districts:
                    continue
                
                # Слово — район какого-то города? Проверяем обе формы
                district_canonical_city = (
                    DISTRICT_TO_CANONICAL.get(raw_word) or 
                    DISTRICT_TO_CANONICAL.get(word_norm)
                )
                if district_canonical_city and district_canonical_city != seed_canonical:
                    has_wrong_district = True
                    matched_form = raw_word if DISTRICT_TO_CANONICAL.get(raw_word) else word_norm
                    logger.info(f"[GEO_WHITE_LIST] ❌ WRONG_DISTRICT: '{query}' contains district "
                              f"'{matched_form}' from city '{district_canonical_city}', "
                              f"seed_city is '{seed_city}' (canonical='{seed_canonical}')")
                    stats['blocked_wrong_district'] += 1
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
