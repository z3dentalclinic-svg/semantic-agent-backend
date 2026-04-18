"""
function_detectors.py — 11 детекторов функции хвоста.

Каждый детектор отвечает на вопрос: "Какую ФУНКЦИЮ выполняет хвост7?"
Если функция определена → сигнал VALID.
Если обнаружен дефект формы → сигнал TRASH.

Каждый детектор возвращает: (bool, str) — (сработал?, причина)
"""

import pymorphy3  # noqa: F401
import json
import os
import logging
from typing import Tuple, Set, Dict

from .shared_morph import morph

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# DISTRICT-базы — слова которые являются районами/улицами городов.
# Загружается из districts.json один раз при импорте модуля.
#
# Две параллельные мапы:
#   _DISTRICT_TO_CANONICAL: district_name → canonical_city
#   _DISTRICT_TO_COUNTRY:   district_name → country_code (lowercased, 'ua'/'ru'/'by'/...)
#
# Используется в detect_foreign_geo для контекстного определения:
#   — район нашей страны (country == target) → не блокируем (улица в нашем городе)
#   — район чужой страны (country != target) → блокируем как foreign (чужой город/район)
#
# Каждый фильтр держит собственную копию базы (независимость модулей).
# Память: ~7 МБ — несущественно.
# ═══════════════════════════════════════════════════════════════════════════
_DISTRICT_TO_CANONICAL: Dict[str, str] = {}
_DISTRICT_TO_COUNTRY: Dict[str, str] = {}

def _load_districts():
    """Загружает districts.json один раз при импорте модуля.
    Возвращает две мапы: district→city и district→country."""
    global _DISTRICT_TO_CANONICAL, _DISTRICT_TO_COUNTRY
    try:
        _path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "districts.json"
        )
        if not os.path.exists(_path):
            logger.warning("[FOREIGN_GEO_GUARD] districts.json not found at %s", _path)
            return
        with open(_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for name, info in raw.items():
            if not isinstance(info, dict):
                continue
            name_lower = name.lower()
            _DISTRICT_TO_CANONICAL[name_lower] = info.get("city", "").lower()
            _DISTRICT_TO_COUNTRY[name_lower] = info.get("country", "").lower()
        logger.info(
            "[FOREIGN_GEO_GUARD] loaded %d district entries from districts.json",
            len(_DISTRICT_TO_CANONICAL)
        )
    except Exception as e:
        logger.error("[FOREIGN_GEO_GUARD] failed to load districts.json: %s", e)
        _DISTRICT_TO_CANONICAL = {}
        _DISTRICT_TO_COUNTRY = {}

_load_districts()


# ═══════════════════════════════════════════════════════════════════════════
# CITY NORMALIZATION — привязка разных орфографий города к канонической форме.
#
# Проблема: districts.json хранит city в разных орфографиях:
#   'голосеевский район' → city='kyiv'       (англ)
#   'позняки'            → city='київ'       (укр)
#   'приднепровский район' → city='черкаси'  (укр)
# А seed всегда на русском ("днепр", "киев", "харьков").
#
# Решение: reverse-индекс через geonamescache.alternatenames.
# Любое написание ('днепр', 'дніпро', 'dnipro') нормализуется в одну и ту же
# каноническую форму ('dnipro'). При коллизии (одинаковый alt для разных городов)
# выигрывает город с большей population — защита от мелких foreign омонимов.
#
# Fallback: если geonamescache не установлен или не импортируется — словарь
# остаётся пустым. Детекторы _wrong_district/_unknown_district в этом случае
# просто не срабатывают (возвращают False), деградируя до текущего поведения.
# ═══════════════════════════════════════════════════════════════════════════
_CITY_NORMALIZE: Dict[str, str] = {}

# Множество alt-names всех столиц мира из geonamescache.
# Используется DISTRICT-guard'ом в detect_foreign_geo для защиты от
# слишком агрессивной district-маскировки.
#
# Проблема без этого множества: _DISTRICT_TO_CANONICAL содержит варшава/
# париж как микрорайоны в мелких городах (Славянск, Павловский Посад),
# и district-guard безусловно пропускал ЛЮБОЙ match. Результат:
# tail='в варшаве' с target=UA не ловился как foreign, потому что
# 'варшава' есть в DISTRICT-базе.
#
# Изначально пробовалось population-based решение (>= 500k жителей), но
# у некоторых нарицательных слов (заря) в geonamescache аномальные
# population-значения из-за коллизий alt-names с foreign деревнями.
# Capital-flag — более надёжный и простой сигнал: это буквально список
# "важных" городов мира, курируемый Wikidata/OpenStreetMap.
#
# Содержит ~11k alt-names для ~200 столиц на всех языках (кириллица,
# латиница, локальные алфавиты).
_CAPITAL_ALT_NAMES: Set[str] = set()

try:
    import geonamescache as _gnc
    _GEONAMESCACHE_AVAILABLE = True
except ImportError:
    _GEONAMESCACHE_AVAILABLE = False


def _build_city_normalize():
    """Строит reverse-индекс alt_name → canonical_english_name.

    При коллизии выигрывает город с большей population (крупный город
    перевешивает мелкий омоним).

    Пример:
      'днепр' → 'dnipro'
      'дніпро' → 'dnipro'
      'киев' → 'kyiv'
      'київ' → 'kyiv'

    Если geonamescache недоступен — оставляем пустой словарь и логируем warning.
    Это НЕ ошибка: детекторы wrong_district/unknown_district просто не
    сработают, поведение деградирует до текущего (как будто их нет).
    """
    global _CITY_NORMALIZE
    if not _GEONAMESCACHE_AVAILABLE:
        logger.warning(
            "[CITY_NORMALIZE] geonamescache not available — district city "
            "normalization disabled, detect_wrong_district will no-op"
        )
        return

    try:
        gc = _gnc.GeonamesCache()
        cities = gc.get_cities()
        countries = gc.get_countries()
        # Сортируем по убыванию population — при коллизии выигрывает крупный.
        sorted_cities = sorted(cities.values(), key=lambda c: -c.get('population', 0))

        for city in sorted_cities:
            canonical = city['name'].lower().strip()
            if not canonical:
                continue
            # Идентити-маппинг для каноникала (первый регистрирующийся выигрывает
            # за счёт сортировки по population).
            if canonical not in _CITY_NORMALIZE:
                _CITY_NORMALIZE[canonical] = canonical
            # Все альтернативные имена
            for alt in city.get('alternatenames', []):
                alt_lower = alt.lower().strip()
                if alt_lower and alt_lower not in _CITY_NORMALIZE:
                    _CITY_NORMALIZE[alt_lower] = canonical

        # Собираем множество alt-names всех столиц мира.
        # countries[cc]['capital'] содержит английское имя столицы.
        # Находим city-запись этой столицы (по name + countrycode) и берём
        # все её alternatenames на всех языках.
        capitals_by_cc = {}
        for cc, info in countries.items():
            cap_name = info.get('capital', '').lower().strip()
            if cap_name:
                capitals_by_cc[cc.upper()] = cap_name

        for city in cities.values():
            cc = city.get('countrycode', '').upper()
            if cc not in capitals_by_cc:
                continue
            if city['name'].lower().strip() != capitals_by_cc[cc]:
                continue
            # Это столица — добавляем её canonical + все alt-names
            _CAPITAL_ALT_NAMES.add(city['name'].lower().strip())
            for alt in city.get('alternatenames', []):
                alt_lower = alt.lower().strip()
                if alt_lower:
                    _CAPITAL_ALT_NAMES.add(alt_lower)

        logger.info(
            "[CITY_NORMALIZE] loaded %d city name variants, %d capital alt-names",
            len(_CITY_NORMALIZE), len(_CAPITAL_ALT_NAMES)
        )
    except Exception as e:
        logger.error("[CITY_NORMALIZE] failed to build index: %s", e)
        _CITY_NORMALIZE = {}


_build_city_normalize()


def _normalize_city(name: str) -> str:
    """Возвращает каноническую форму города по любому его alt-name.

    Если имя не найдено в индексе — возвращает как есть (lower).
    Это defensive fallback: для неизвестных городов сравнение работает
    по строке-как-есть. Когда geonamescache недоступен, всё сравнение
    сводится к сравнению "как есть", что эквивалентно отключённому детектору
    (в абсолютном большинстве случаев district.city != seed_word по строке).
    """
    if not name:
        return ""
    key = name.lower().strip()
    return _CITY_NORMALIZE.get(key, key)


def _is_foreign_district_name(name: str, target_country: str) -> bool:
    """Возвращает True если name — известный район/улица чужого города.

    Используется в detect_geo чтобы защититься от ложного +geo на словах,
    которые одновременно являются одиночным UA-городом (например 'южное') и
    частью foreign-района ('южное бутово' = район в RU).

    Логика:
      — name есть в _DISTRICT_TO_CANONICAL
      — его country в базе != target_country
      → это чужой район, одиночное +geo выдавать нельзя

    Symmetrically для 'позняки' (country=UA): если target=UA, возвращает
    False (свой район, не foreign). Если target=RU, возвращает True.
    """
    if not name or not _DISTRICT_TO_CANONICAL:
        return False
    country = _DISTRICT_TO_COUNTRY.get(name.lower(), '')
    if not country:
        return False
    return country.lower() != target_country.lower()


def _get_parses(word: str, tp: dict = None):
    """
    Возвращает все морфологические разборы слова.
    Если передан tp (tail_parses dict) — берёт из него (O(1) dict lookup).
    Иначе вызывает morph.parse (LRU cache).

    tp = {word: _get_parses(word, tp)} — строится один раз на весь батч в l0_filter.py.
    Все детекторы читают из него вместо независимых вызовов morph.parse.
    """
    if tp is not None and word in tp:
        return tp[word]
    return morph.parse(word)

# ── Индекс для detect_truncated_geo ─────────────────────────────────────────
# Строится ОДИН РАЗ при первом вызове detect_truncated_geo с конкретным geo_db.
# Ключ: id(geo_db) — при смене базы индекс перестраивается автоматически.
# Структура: {первая_часть_города → полное_название}
# Пример: "ханты" → "ханты-мансийск", "санкт" → "санкт-петербург"
# Заменяет O(65k) перебор на O(1) lookup.
_truncated_geo_index: Dict[str, str] = {}
_truncated_geo_index_for: int = -1  # id последнего geo_db


def _build_truncated_geo_index(geo_db: dict) -> Dict[str, str]:
    """Строит индекс первых частей составных городов."""
    global _truncated_geo_index, _truncated_geo_index_for
    db_id = id(geo_db)
    if db_id == _truncated_geo_index_for:
        return _truncated_geo_index
    index = {}
    for city_name in geo_db:
        if '-' in city_name:
            first_part = city_name.split('-')[0]
            if first_part and first_part not in index:
                index[first_part] = city_name
        elif ' ' in city_name:
            first_part = city_name.split(' ')[0]
            if first_part and first_part not in index:
                index[first_part] = city_name
    _truncated_geo_index = index
    _truncated_geo_index_for = db_id
    return index


_seed_has_verb_cache: Dict[str, bool] = {}
_seed_lemmas_cache: Dict[str, frozenset] = {}


def _get_seed_lemmas(seed: str) -> frozenset:
    """
    Возвращает frozenset лемм слов seed'а.
    Кэшируется — seed одинаков для всего батча (282 ключа),
    поэтому пересчитывается только один раз.
    """
    if seed in _seed_lemmas_cache:
        return _seed_lemmas_cache[seed]
    lemmas = frozenset(morph.parse(w)[0].normal_form for w in seed.lower().split())
    _seed_lemmas_cache[seed] = lemmas
    return lemmas


def _seed_has_verb(seed: str) -> bool:
    """
    Проверяет наличие глагола в seed.
    Результат кэшируется — seed одинаков для всего батча (282 ключа),
    поэтому morph.parse для seed-слов вызывается только один раз.
    """
    if seed in _seed_has_verb_cache:
        return _seed_has_verb_cache[seed]
    if not seed:
        _seed_has_verb_cache[seed] = False
        return False
    result = False
    for sw in seed.lower().split():
        sp = morph.parse(sw)[0]
        if sp.tag.POS in ('INFN', 'VERB') and morph.word_is_known(sw):
            result = True
            break
    _seed_has_verb_cache[seed] = result
    return result


# ============================================================
# ПОЗИТИВНЫЕ ДЕТЕКТОРЫ (функция хвоста определена → VALID)
# ============================================================

def detect_geo(tail: str, geo_db: Dict[str, Set[str]], target_country: str = "ua", tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор географии: город, район, страна.
    Использует geonamescache (65k+ городов) + лемматизацию.
    
    COUNTRY-AWARE: geo_db = Dict[str, Set[str]] (название → {коды_стран}).
    Город считается VALID только если он существует в target_country.
    Страна (из _COUNTRIES) — аналогично: позитив только если совпадает с target.
    
    АЛГОРИТМ: longest-first scanning window.
    1. Триграммы (raw + head-lemma, с вариантами dash/space)
    2. Биграммы (raw + head-lemma + full-lemma, с вариантами dash/space)
    3. Одиночные токены (raw + lemma при len>=5)
    
    EARLY EXIT: если длинный n-грамм найден (в любой стране) — не идём дальше.
    Это критично: "в комсомольске на амуре" найдёт триграмму 'комсомольск-на-амуре' (RU)
    и НЕ спустится к одиночному 'комсомольск' (который в UA, переименован в 2016).
    
    NO-LEMMA-FOR-SHORT: одиночные токены < 5 символов не лемматизируются.
    Без этого 'горно' → 'горный' (артефакт pymorphy) и матчится на случайный микро-топоним.
    Для составных имён голова перехватывается биграммой 'горно-алтайск' раньше.
    
    "киев" (UA) → True (киев ∈ UA)
    "тир"  (UA) → False (тир ∈ LB, не в UA)
    "днс"  (UA) → False (днс нет в geo_db вообще)
    "или"  (UA) → False (или ∈ GB, не в UA + CONJ)
    "одессе" (UA) → True (лемма "одесса" ∈ UA)
    "украине" (UA) → True (лемма "украина" ∈ _COUNTRIES, UA == target)
    "нижний тагил" (UA) → False (биграмм 'нижний тагил' ∈ RU, foreign early exit)
    "в комсомольске на амуре" (UA) → False (триграмм 'комсомольск-на-амуре' ∈ RU)
    "горно алтайск" (UA) → False (биграмм 'горно-алтайск' ∈ RU)
    """
    target = target_country.upper()
    # POS которые НИКОГДА не являются городами в контексте поиска
    skip_pos = {'CONJ', 'PREP', 'PRCL', 'INTJ'}
    
    words = tail.lower().split()
    if not words:
        return False, ""
    
    # Лемматизируем все токены один раз (используется везде)
    lemmas = [_get_parses(w, tp)[0].normal_form for w in words]
    
    # ── 1. ТРИГРАММЫ (scanning window) ─────────────────────────────────
    # Для составных имён: "ростов-на-дону", "комсомольск-на-амуре", "каменск-на-оби".
    # head-lemma покрывает косвенные падежи головы: "в ростове на дону" → "ростов на дону".
    # Остальные токены (на/амуре/дону) raw — они часть имени, не свободные грам.формы.
    for i in range(len(words) - 2):
        raw3 = words[i:i+3]
        head3 = [lemmas[i]] + words[i+1:i+3]
        bases = {' '.join(raw3), ' '.join(head3)}
        variants = set()
        for b in bases:
            variants.add(b)
            variants.add(b.replace(' ', '-'))
            variants.add(b.replace('-', ' '))
        for v in variants:
            if v in geo_db:
                if target in geo_db[v]:
                    return True, f"Город (триграмм): '{v}' ({target})"
                # найдено, но не для target — foreign составное имя
                # EARLY EXIT: НЕ идём к биграммам/одиночным, иначе ложный positive
                # на части составного имени (комсомольск → UA одиночный).
                return False, ""
    
    # ── 2. БИГРАММЫ (scanning window) ──────────────────────────────────
    # Покрывает: "нижний тагил", "ивано франковск", "кривой рог", "горно алтайск",
    # а также косвенные падежи головы ("в нижнем тагиле" → full-lemma "нижний тагил").
    for i in range(len(words) - 1):
        raw2 = words[i:i+2]
        head2 = [lemmas[i]] + words[i+1:i+2]
        full2 = lemmas[i:i+2]
        bases = {' '.join(raw2), ' '.join(head2), ' '.join(full2)}
        variants = set()
        for b in bases:
            variants.add(b)
            variants.add(b.replace(' ', '-'))
            variants.add(b.replace('-', ' '))

        # === FOREIGN DISTRICT guard (задача "южное бутово") ===
        # Если биграмма есть в _DISTRICT_TO_CANONICAL с country != target,
        # это foreign-район. Нельзя выдавать +geo от одиночного слова
        # (например 'южное' как UA-city), потому что в контексте биграммы
        # 'южное бутово' это район Москвы. Early exit: идём сразу к концу
        # функции, не проверяя одиночные.
        district_foreign = False
        for v in variants:
            if v in _DISTRICT_TO_CANONICAL:
                country = _DISTRICT_TO_COUNTRY.get(v, '')
                if country and country.lower() != target_country.lower():
                    district_foreign = True
                    break
        if district_foreign:
            # Пропускаем проверки в ЭТОЙ биграмме — чтобы:
            # 1. Не выдать +geo от биграммы, которой нет в geo_db (южное бутово)
            # 2. Не выдать +geo от одиночного слова внутри биграммы (южное → UA)
            # Выходим из функции — это был foreign district, detect_foreign_geo
            # даст свой вердикт отдельно.
            return False, ""

        # Проверка городов
        for v in variants:
            if v in geo_db:
                if target in geo_db[v]:
                    return True, f"Город (биграмм): '{v}' ({target})"
                return False, ""
        # Проверка стран-биграмм ("саудовская аравия", "новая зеландия")
        for v in variants:
            if v in _COUNTRIES:
                if _COUNTRIES[v] == target:
                    return True, f"Страна (биграмм): '{v}' ({target})"
                return False, ""
    
    # ── 3. ОДИНОЧНЫЕ ТОКЕНЫ ────────────────────────────────────────────
    # Сюда доходим только если n-граммы ничего не нашли.
    # Короткие токены (< 5 символов) НЕ лемматизируем — pymorphy даёт артефакты
    # на частях составных имён (горно → горный, усть → устье, верх → верх).
    for word, lem in zip(words, lemmas):
        parsed = _get_parses(word, tp)[0]
        if parsed.tag.POS in skip_pos:
            continue

        # === FOREIGN DISTRICT guard для одиночных (задача "позняки") ===
        # Если слово (или его лемма) есть в _DISTRICT_TO_CANONICAL с country
        # != target — это чужой район, не независимый город. Не выдаём +geo
        # даже если это слово есть в geo_db как topoним target-страны.
        # Пример: 'позняки' country=UA → target=UA → не блокируем (свой район).
        #         'позняки' при target=RU → блокируем.
        #
        # ПРИОРИТЕТ raw-слова: если word сам по себе точно есть в geo_db как
        # target-city (например 'южное' есть в geo_db как {'UA'}), то raw-матч
        # перевешивает district-guard по лемме. Иначе 'южное' → lemma 'южный'
        # → в districts как район Всеволожска (RU) → guard ошибочно режет
        # корректный UA-город одиночным.
        # ПРОВЕРКА guard'а на raw остаётся (если word сам foreign district).
        if _is_foreign_district_name(word, target_country):
            continue
        # Guard по лемме применяем только если raw НЕ найден как target-city
        word_is_target_city = word in geo_db and target in geo_db[word]
        if (not word_is_target_city
                and len(word) >= 5 and lem != word
                and _is_foreign_district_name(lem, target_country)):
            continue

        # Точное совпадение (city)
        if word in geo_db:
            if target in geo_db[word]:
                return True, f"Город: '{word}' ({target})"
            continue  # чужой город одиночным токеном — не блокируем тут
        
        # Лемматизация (киеву → киев, одессе → одесса) — только для длинных
        if len(word) >= 5 and lem != word and lem in geo_db:
            if target in geo_db[lem]:
                return True, f"Город (лемма): '{lem}' ({target})"
            continue
        
        # Страны (точное слово)
        if word in _COUNTRIES:
            if _COUNTRIES[word] == target:
                return True, f"Страна: '{word}' ({target})"
            continue
        # Страны (лемма) — для стран порог длины не критичен, но оставим консистентно
        if len(word) >= 5 and lem != word and lem in _COUNTRIES:
            if _COUNTRIES[lem] == target:
                return True, f"Страна (лемма): '{lem}' ({target})"
            continue
    
    return False, ""


def detect_brand(tail: str, brand_db: Set[str], tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор бренда/модели.
    Использует базу брендов + лемматизацию.
    
    "samsung" → True   "самсунга" → True (лемма)
    "v15" → True (модель Dyson)
    """
    words = tail.lower().split()
    
    for word in words:
        if word in brand_db:
            return True, f"Бренд: '{word}'"
        
        lemma = _get_parses(word, tp)[0].normal_form
        if lemma in brand_db:
            return True, f"Бренд (лемма): '{lemma}'"
    
    return False, ""


def detect_commerce(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор коммерческого модификатора.
    Слова, которые сужают поиск до ценовой/транзакционной плоскости.
    
    Разделён на strong (самодостаточные) и weak (нужен контекст):
    "цена" → True (strong)    "заказ" → False (weak, одно слово)
    "заказ цена" → True (weak + ещё слово)
    """
    # Strong: самодостаточные — одного слова хватает для VALID
    # "цена", "стоимость" — всегда коммерческий интент
    commerce_lemmas_strong = {
        'цена', 'стоимость', 'прайс', 'тариф', 'расценка',
        'прейскурант', 'скидка', 'акция', 'гарантия', 'гарантийный',
        'бесплатно', 'бесплатный', 'платно', 'платный',
        'рассрочка', 'кредит', 'предоплата', 'аванс',
        # Состояние товара — cross-niche коммерческий модификатор
        # "видеокарта бу", "айфон б/у", "авто бу" — всегда покупательский
        'бу', 'б/у',
        # Украинские
        'ціна', 'вартість', 'знижка',
    }
    
    # Weak: нужен контекст — одного слова / одного контентного НЕ хватает
    # "купить" для айфона → VALID, "купить" для пластики → бред
    # "заказ" для торта → VALID, "заказ" для пластики → бред
    commerce_lemmas_weak = {
        'купить', 'заказать', 'оформить', 'приобрести', 'арендовать',
        'покупка', 'заказ', 'оплата', 'оплатить', 'доставка',
        'замовити', 'купити',
    }
    
    # Паттерны (могут быть частью фразы) — всегда strong
    commerce_patterns = [
        'сколько стоит', 'почём', 'по цене',
        'недорого', 'дёшево', 'дешево', 'дорого', 'бюджетно',
    ]
    
    tail_lower = tail.lower()
    words = tail_lower.split()

    # Один проход — парсим каждое слово один раз, получаем и POS и лемму
    skip_pos_commerce = {'PREP', 'CONJ', 'PRCL', 'INTJ', 'ADVB', 'PRED'}
    parses = [_get_parses(w, tp)[0] for w in words]
    content_count = sum(1 for p in parses if p.tag.POS not in skip_pos_commerce)
    is_single_content = content_count <= 1

    # Проверка паттернов (всегда strong)
    for pattern in commerce_patterns:
        if pattern in tail_lower:
            return True, f"Коммерция (паттерн): '{pattern}'"

    # Проверка по леммам — используем уже готовые parses
    for word, p in zip(words, parses):
        lemma = p.normal_form
        
        # Strong — работает даже одним словом
        if lemma in commerce_lemmas_strong:
            return True, f"Коммерция (лемма): '{lemma}'"
        
        # Weak — одним контентным словом НЕ VALID
        # "заказ" → False, "на заказ" → False (на=предлог), "заказ цена" → True
        if lemma in commerce_lemmas_weak:
            if is_single_content:
                return False, ""
            return True, f"Коммерция (слабая лемма): '{lemma}'"
    
    return False, ""


def detect_reputation(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор репутационного модификатора.
    Человек ищет отзывы, рейтинги, мнения о сервисе.
    
    "отзывы" → True    "форум" → True    "рейтинг" → True
    "жалобы" → False   (негативный/юридический интент, не покупательский)
    """
    # Покупательский research: человек сравнивает, выбирает → VALID
    reputation_lemmas = {
        'отзыв', 'рейтинг', 'обзор', 'форум', 'рекомендация',
        'опыт', 'мнение', 'оценка',
        'сравнение', 'рекомендовать',
    }
    
    # Негативный/юридический интент: человек ищет куда пожаловаться → НЕ покупательский
    # Cross-niche: "пылесос жалобы", "скутер претензия" — нигде не покупка
    # Эти леммы НЕ триггерят позитивный сигнал → хвост уйдёт в GREY → Слой 2
    # NB: оставлены в _check_coherence.reputation_lemmas чтобы не стали orphans
    # reputation_lemmas_negative (не используются здесь, только документация):
    # {'жалоба', 'претензия', 'обман', 'мошенничество', 'развод', 'обманывать'}
    
    reputation_patterns = [
        'топ ', 'топ-', 'лучший сервис', 'хороший сервис',
        'кто лучше', 'куда лучше', 'куда обратиться',
        'стоит ли', 'можно ли доверять',
    ]
    
    tail_lower = tail.lower()
    
    for pattern in reputation_patterns:
        if pattern in tail_lower:
            return True, f"Репутация (паттерн): '{pattern}'"
    
    for word in tail_lower.split():
        lemma = _get_parses(word, tp)[0].normal_form
        if lemma in reputation_lemmas:
            return True, f"Репутация (лемма): '{lemma}'"
    
    return False, ""


def detect_location(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор локационного модификатора (не город, а паттерн поиска).
    
    "рядом" → True    "на дому" → True    "ближайший" → True
    """
    location_patterns = [
        'рядом', 'поблизости', 'неподалёку', 'неподалеку',
        'на дому', 'с выездом', 'выезд на дом',
        'ближайший', 'ближайшая', 'ближе всего',
        'в моём районе', 'в моем районе', 'мой район',
        'возле', 'около', 'недалеко',
        'на левом берегу', 'на правом берегу',
        'центр города', 'в центре',
        'район', 'микрорайон', 'улица',
    ]
    
    # Также леммы отдельных слов
    location_lemmas = {
        'рядом', 'поблизости', 'ближайший', 'недалеко',
    }
    
    tail_lower = tail.lower()
    
    for pattern in location_patterns:
        if pattern in tail_lower:
            return True, f"Локация (паттерн): '{pattern}'"
    
    for word in tail_lower.split():
        lemma = _get_parses(word, tp)[0].normal_form
        if lemma in location_lemmas:
            return True, f"Локация (лемма): '{lemma}'"
    
    return False, ""


def detect_time(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор временного/срочного модификатора.
    Универсальный — работает для любой темы.
    
    "круглосуточно" → True    "сегодня" → True    "срочно" → True
    """
    time_lemmas = {
        'круглосуточно', 'круглосуточный',
        'срочно', 'срочный', 'экстренно', 'экстренный',
        'сегодня', 'завтра', 'сейчас',
        'быстро', 'быстрый',
        'ночью', 'ночной',
        'утром', 'утренний',
        'выходные', 'выходной', 'праздник', 'праздничный',
    }
    
    time_patterns = [
        '24/7', '24 часа', 'без выходных',
        'в праздники', 'на праздник',
        'на ночь', 'на утро',
    ]
    
    tail_lower = tail.lower()
    
    for pattern in time_patterns:
        if pattern in tail_lower:
            return True, f"Время (паттерн): '{pattern}'"
    
    for word in tail_lower.split():
        parsed = _get_parses(word, tp)[0]
        lemma = parsed.normal_form
        pos = parsed.tag.POS
        
        # Прилагательные без существительного — не time сигнал
        # "быстрые" (ADJF) → reject, "быстро" (ADVB) → OK
        # "срочный" (ADJF) → reject, "срочно" (ADVB) → OK
        if pos in ('ADJF', 'ADJS', 'PRTF', 'PRTS'):
            continue
        
        if lemma in time_lemmas or word in time_lemmas:
            return True, f"Время (лемма): '{lemma}'"
    
    return False, ""


def detect_action(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор действия/способа — хвост описывает КАК делать.
    
    "своими руками" → True    "инструкция" → True
    "разборка" → True         "видео" → True
    """
    action_patterns = [
        'своими руками', 'самостоятельно', 'самому',
        'в домашних условиях', 'дома',
        'пошагово', 'пошаговая', 'поэтапно',
        'как разобрать', 'как почистить', 'как починить',
        'как заменить', 'как собрать', 'как отремонтировать',
    ]
    
    # Самодостаточные леммы — одного слова хватает для VALID
    # "видео" → VALID, "инструкция" → VALID, "разборка" → VALID
    action_lemmas_strong = {
        'инструкция', 'руководство', 'мануал',
        'видео', 'видеоинструкция', 'фото',
        'разборка', 'сборка', 'чистка', 'замена',
        'диагностика', 'профилактика', 'обслуживание',
    }
    
    # Техническая документация — чистый инфо-интент, не покупательский → НЕ VALID
    # Cross-niche: "пылесос чертёж", "скутер схема" — нигде не покупка
    # Эти леммы НЕ триггерят позитивный сигнал → хвост уйдёт в GREY → Слой 2
    # NB: оставлены в _check_coherence.action_lemmas чтобы не стали orphans
    # action_lemmas_info (не используются здесь, только документация):
    # {'схема', 'чертёж', 'чертеж', 'диаграмма'}
    
    # Запчасти и обучение — одного слова НЕ хватает, нужен контекст (≥2 слова)
    # "щетка" → GREY, но "замена щетки" → VALID
    # "обучение" → GREY, но "обучение цена" → VALID (через commerce)
    action_lemmas_parts = {
        'запчасть', 'деталь', 'комплектующие', 'фильтр',
        'щётка', 'щетка', 'шланг', 'мешок', 'пылесборник',
        'мотор', 'двигатель', 'турбина', 'аккумулятор',
        'курс', 'обучение', 'мастер-класс',
    }
    
    # Паттерн: существительное-действие (разборка, замена фильтра)
    action_verb_lemmas = {
        'разобрать', 'собрать', 'почистить', 'починить',
        'заменить', 'отремонтировать', 'восстановить',
        'промыть', 'продуть', 'смазать', 'перемотать',
    }
    
    tail_lower = tail.lower()
    words = tail_lower.split()
    is_single_word = len(words) == 1
    
    for pattern in action_patterns:
        if pattern in tail_lower:
            return True, f"Действие (паттерн): '{pattern}'"
    
    for word in words:
        parsed = _get_parses(word, tp)[0]
        lemma = parsed.normal_form
        
        # Одиночный инфинитив без объекта — обрывок, не действие
        # "почистить" → GREY, но "почистить фильтр" → VALID
        if lemma in action_verb_lemmas:
            if is_single_word:
                return False, ""
            return True, f"Действие (глагол): '{lemma}'"
        
        # Самодостаточные леммы — работают даже одним словом
        # "видео" → VALID, "инструкция" → VALID
        if lemma in action_lemmas_strong:
            if is_single_word and parsed.tag.case == 'ablt':
                return False, ""
            return True, f"Действие (лемма): '{lemma}'"
        
        # Запчасти — одним словом НЕ VALID, нужен контекст
        # "щетка" → GREY, "замена щетки" → VALID
        if lemma in action_lemmas_parts:
            if is_single_word:
                return False, ""
            return True, f"Действие (запчасть): '{lemma}'"
    
    return False, ""


# ============================================================
# НЕГАТИВНЫЕ ДЕТЕКТОРЫ (дефект формы → TRASH)
# ============================================================

def detect_fragment(tail: str, seed: str = "", tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор обрывка: хвост заканчивается на служебное слово,
    или состоит из одной копулы/частицы.
    
    "и" → True    "для" → True    "есть" → True
    "рядом" → False (наречие, не служебное)
    
    SEED-AWARE: если seed содержит глагол, одиночное "когда"/"сколько"
    в tail = временной модификатор глагола, не обрывок.
    """
    words = tail.lower().split()
    if not words:
        return False, ""
    
    # Проверяем: есть ли глагол в seed?
    seed_has_verb = _seed_has_verb(seed)
    
    last_word = words[-1]
    last_parsed = _get_parses(last_word, tp)[0]
    
    # Вопросительные/временные слова которые могут модифицировать глагол seed
    # "когда" при seed "как принимать" → "когда принимать" → NOT обрывок
    verb_modifier_questions = {'когда', 'сколько', 'коли', 'скільки'}
    
    # Правило 1: Заканчивается на предлог, союз, частицу
    # Исключение: продуктовые суффиксы — "про"(Pro), "макс"(Max), "мини"(Mini), 
    # "плюс"(Plus), "лайт"(Lite) — pymorphy видит PREP/PRCL, но это модели товаров.
    # Универсально для любой темы.
    product_suffixes = {'про', 'макс', 'мини', 'плюс', 'лайт', 'ультра'}
    
    # === ФИКС: pymorphy ошибочно тегирует некоторые NOUN как CONJ ===
    # "минус" → pymorphy: CONJ, но в контексте "где плюс где минус" это NOUN
    # "плюс" → уже в product_suffixes
    # Не блокируем эти слова как fragment, они семантически NOUN
    misclassified_as_conj = {'минус'}  # pymorphy баг: CONJ вместо NOUN
    
    if last_parsed.tag.POS in ('PREP', 'CONJ', 'PRCL') and last_word not in product_suffixes:
        if last_word in misclassified_as_conj:
            pass  # Не блокируем — это ложное срабатывание pymorphy
        elif seed_has_verb and last_word in verb_modifier_questions:
            pass  # "когда"/"сколько" при seed с глаголом = модификатор, не обрывок
        elif last_parsed.tag.POS == 'PREP' and len(words) >= 2 and words[-2] in ('или', 'и', 'либо', 'чи'):
            pass  # Эллипсис: "до еды или после" = "до еды или после [еды]"
        elif (len(last_word) == 1 and last_word.isalpha()
              and len(words) >= 2
              and _get_parses(words[-2], tp)[0].tag.POS == 'NOUN'):
            # Одиночная буква после NOUN — классификатор/маркер, не обрывок:
            # "гепатит б", "витамин с", "класс а", "группа о", "тип в".
            # pymorphy тегирует одиночные буквы как PRCL/CONJ/INTJ, но в контексте
            # "NOUN + БУКВА" это медицинская/техническая классификация.
            # Универсальное структурное правило: если перед буквой существительное
            # — это его маркер, не служебное слово.
            pass
        else:
            return True, f"Обрывок: '{last_word}' ({last_parsed.tag.POS}) на конце"
    
    # Правило 2: Одиночная копула / бытийный глагол без объекта
    copula_forms = {'есть', 'быть', 'бывает', 'бывают', 'бывать',
                    'является', 'являться', 'имеется'}
    if len(words) == 1 and last_word in copula_forms:
        return True, f"Обрывок: копула '{last_word}' без объекта"
    
    # Правило 3: Одиночное "можно", "нужно", "надо" — модальное без действия
    modal_words = {'можно', 'нужно', 'надо', 'нельзя', 'стоит', 'следует'}
    if len(words) == 1 and last_word in modal_words:
        return True, f"Обрывок: модальное '{last_word}' без действия"
    
    # Правило 4: Заканчивается на "это" (незавершённая мысль)
    if last_word == 'это' and len(words) <= 2:
        return True, f"Обрывок: незавершённое '...это'"
    
    # Вопросительные слова: "как обманывают", "где купить" — валидный запрос.
    # pymorphy тегирует их как CONJ/ADVB, но они формируют вопросительную 
    # конструкцию → глагол после них НЕ является обрывком.
    # Включает составные: "из чего состоит", "для чего нужен"
    interrogative_words = {'как', 'где', 'куда', 'откуда', 'почему', 
                           'зачем', 'когда', 'сколько', 'чем', 'чего',
                           'кто', 'кого', 'что'}
    starts_with_question = (
        words[0] in interrogative_words or
        (len(words) >= 2 and words[1] in interrogative_words)  # "из чего", "для чего", "от чего"
    )
    
    # Правило 5: Одиночный спрягаемый глагол (не инфинитив, не императив)
    # "заикается", "зависают", "работает" — 3-е лицо без подлежащего = обрывок.
    # НО: 1-е/2-е лицо подразумевает "я/мы/ты" → "продам", "куплю" = валидно.
    # Инфинитив = POS 'INFN' (отдельная часть речи в pymorphy3).
    # Императив = mood 'impr'.
    if len(words) == 1 and last_parsed.tag.POS == 'VERB':
        is_imperative = last_parsed.tag.mood == 'impr'
        is_1st_2nd_person = last_parsed.tag.person in ('1per', '2per')
        if not is_imperative and not is_1st_2nd_person:
            return True, f"Обрывок: спрягаемый глагол '{last_word}' без подлежащего"
    
    # Правило 6: Многословный хвост, заканчивающийся спрягаемым глаголом
    if len(words) >= 2 and last_parsed.tag.POS == 'VERB' and not starts_with_question:
        is_imperative = last_parsed.tag.mood == 'impr'
        if not is_imperative:
            has_subject = False
            for w in words[:-1]:
                wp = _get_parses(w, tp)[0]
                if wp.tag.POS == 'NOUN' and wp.tag.case == 'nomn':
                    has_subject = True
                    break
            if not has_subject:
                return True, f"Обрывок: глагол '{last_word}' без подлежащего"
    
    # Правило 7: НАЧИНАЕТСЯ с союза (обрывок)
    # "и пылесосов", "или что-то" — хвост не может начинаться с союза
    # НО: "как", "куда", "когда", "чем" — pymorphy считает CONJ,
    # а это вопросительные слова → "как обманывают" = валидный запрос
    first_word = words[0]
    first_parsed = _get_parses(first_word, tp)[0]
    if first_parsed.tag.POS == 'CONJ' and len(words) >= 2 and not starts_with_question:
        return True, f"Обрывок: хвост начинается с союза '{first_word}'"
    
    # Правило 8: Модальная конструкция без действия
    # "может быть", "должен быть" — повисает в воздухе
    modal_phrases = {'может быть', 'должен быть', 'не может быть',
                     'не может', 'не должен', 'не будет'}
    tail_lower = ' '.join(words)
    if tail_lower in modal_phrases:
        return True, f"Обрывок: модальная фраза '{tail_lower}' без объекта"
    
    # Правило 9: Одиночный компаратив без объекта сравнения
    # "лучше", "хуже", "дороже" — что лучше? чего?
    if len(words) == 1 and last_parsed.tag.POS == 'COMP':
        return True, f"Обрывок: компаратив '{last_word}' без объекта сравнения"
    
    return False, ""


def detect_meta(tail: str, seed: str = "", tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор мета-вопроса: вопрос О САМОМ ПОНЯТИИ, а не уточнение поиска.
    
    "зачем" → True           "что означает" → True
    "как разобрать" → False   (это действие, не мета)
    
    SEED-AWARE: если seed содержит глагол, "можно ли" и "когда" в tail
    модифицируют этот глагол, а не задают мета-вопрос.
    "можно ли жаропонижающее" при seed "как принимать нимесил" → NOT мета
    "когда" при seed "как принимать нимесил" → NOT мета
    """
    tail_lower = tail.lower().strip()
    words = tail_lower.split()
    
    # Проверяем: есть ли глагол в seed?
    seed_has_verb = _seed_has_verb(seed)
    
    # Паттерн 1: Мета-фразы целиком
    meta_patterns = [
        'это что', 'что означает', 'что такое', 'что это такое',
        'что это', 'как называется', 'что значит',
        'это что означает', 'зачем нужен', 'зачем нужна', 'зачем нужно',
        'в чём смысл', 'в чем смысл', 'что даёт', 'что дает',
        'чем отличается', 'какая разница', 'какие бывают',
        'что входит', 'что включает',
        # Вопросы-размышления (не уточнение поиска, а рефлексия)
        'что делать', 'что нужно знать', 'что нужно',
        'что важно', 'что лучше', 'как выбрать',
        'на что обратить', 'на что смотреть',
        # Украинские мета-паттерны
        'що це', 'що таке', 'що означає', 'що це таке',
        'навіщо', 'для чого', 'як називається',
        'чим відрізняється', 'яка різниця', 'які бувають',
        'що потрібно', 'що важливо', 'як обрати',
    ]
    
    # Модальные паттерны: "можно ли", "стоит ли", "нужно ли"
    # МЕТА только если НЕ продолжены глаголом:
    #   "можно ли" → мета
    #   "можно ли заряжать" → валидный вопрос
    modal_question_patterns = ['стоит ли', 'нужно ли', 'можно ли']
    
    for pattern in modal_question_patterns:
        if pattern in tail_lower:
            # Проверяем: есть ли глагол ПОСЛЕ паттерна?
            after = tail_lower.split(pattern, 1)[1].strip()
            if after:
                after_words = after.split()
                after_parsed = _get_parses(after_words[0], tp)[0]
                if after_parsed.tag.POS in ('INFN', 'VERB'):
                    # "можно ли заряжать" — валидный вопрос, НЕ мета
                    continue
            # Глагол в seed'е: "можно ли [принимать]" — глагол из seed, не tail
            # "можно ли жаропонижающее" при seed "как принимать" → не мета
            if seed_has_verb:
                continue
            # "можно ли" без глагола нигде — мета
            return True, f"Мета-вопрос: '{pattern}'"
    
    for pattern in meta_patterns:
        if pattern in tail_lower:
            return True, f"Мета-вопрос: '{pattern}'"
    
    # Паттерн 2: Одиночное вопросительное слово (без объекта)
    bare_question_words = {'зачем', 'почему', 'что', 'как', 'когда',
                            'навіщо', 'чому', 'що', 'як', 'коли'}
    if len(words) == 1 and words[0] in bare_question_words:
        # Если seed имеет глагол, временные/модальные вопросы модифицируют его
        # "когда" при seed "как принимать нимесил" → "когда принимать" → NOT мета
        # "сколько" при seed "как принимать" → "сколько принимать" → NOT мета
        verb_modifier_questions = {'когда', 'сколько', 'коли', 'скільки'}
        if seed_has_verb and words[0] in verb_modifier_questions:
            return False, ""
        # Исключение: "как" может быть частью "как разобрать" — но тут одиночное
        return True, f"Мета-вопрос: голое '{words[0]}'"
    
    # Паттерн 3: "почему + прилагательное" без объекта
    # "почему дорого", "почему долго" — мета-рассуждение
    if len(words) == 2 and words[0] in {'почему', 'зачем'}:
        second_parsed = _get_parses(words[1], tp)[0]
        if second_parsed.tag.POS in ('ADVB', 'ADJF', 'ADJS', 'PRED'):
            return True, f"Мета-вопрос: '{words[0]} {words[1]}'"
    
    return False, ""


def detect_number_hijack(tail: str, seed: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Ловит генитив-паразит на числе из seed'а.
    
    Если seed заканчивается числом (напр. "купить айфон 17"),
    а хвост — одиночное существительное в генитиве (род. падеж),
    то оно присасывается к числу: "17 лет", "17 звёзд".
    
    Алгоритмическая проверка: числительное + существительное должны
    согласоваться по правилам русского языка:
      2-4 (кроме 12-14) → род.п. ЕДИНСТВЕННОГО числа
      5-20, и оканч. на 5-9,0 → род.п. МНОЖЕСТВЕННОГО числа
    Если не согласуется → НЕ числовая конструкция → не блокируем.
    
    "17 лет" → gen plur, 17 требует gen plur → MATCH → TRASH
    "17 цвета" → gen sing, 17 требует gen plur → MISMATCH → OK
    """
    if not tail or not seed:
        return False, ""
    
    seed_words = seed.strip().split()
    last_seed = seed_words[-1]
    if not last_seed.isdigit():
        return False, ""
    
    words = tail.strip().split()
    if len(words) != 1:
        return False, ""
    
    word = words[0].lower()
    parsed = _get_parses(word, tp)[0]
    
    # Аббревиатуры — спецификации, не трогаем
    if 'Abbr' in parsed.tag:
        return False, ""
    
    # Существительное в генитиве?
    if parsed.tag.POS != 'NOUN' or parsed.tag.case != 'gent':
        return False, ""
    
    # Слово неизвестно словарю — pymorphy угадывает падеж → не доверяем
    if not morph.word_is_known(word):
        return False, ""
    
    # === Проверка согласования числительное-существительное ===
    num = int(last_seed)
    last_two = num % 100
    last_one = num % 10
    
    if last_two in range(11, 15):
        # 11-14: требуют gen plur
        required_number = 'plur'
    elif last_one == 1:
        # оканч. на 1 (кроме 11): nom sing — не генитив вообще
        return False, ""
    elif last_one in (2, 3, 4):
        # оканч. на 2-4 (кроме 12-14): gen sing
        required_number = 'sing'
    else:
        # оканч. на 5-9, 0: gen plur
        required_number = 'plur'
    
    # Проверяем: число существительного совпадает с требуемым?
    if parsed.tag.number != required_number:
        return False, ""
    
    return True, f"Паразит на числе: '{last_seed} {word}' (генитив {parsed.tag.number})"


def detect_short_garbage(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Ловит короткие бессмысленные токены: "жт", "хр", "щ".
    
    Правило: одиночный токен ≤2 символа, POS неизвестен или INTJ (междометие).
    Исключения: числа, аббревиатуры (тб, гб), латиница, известные сокращения.
    """
    if not tail:
        return False, ""
    
    words = tail.strip().split()
    if len(words) != 1:
        return False, ""
    
    word = words[0].lower()
    
    if len(word) > 2:
        return False, ""
    
    # Числа — пропускаем
    if word.isdigit():
        return False, ""
    
    # Латиница — может быть аббревиатура (gb, tb, hp)
    if word.isascii() and word.isalpha():
        return False, ""
    
    # === ФИКС: Известные сокращения (коммерческий интент) ===
    # "бу" = "б/у" = бывший в употреблении — валидный коммерческий модификатор
    # Cross-niche: "аккумулятор бу", "телефон бу", "авто бу" — везде покупательский
    known_abbreviations = {
        'бу',   # б/у = бывший в употреблении
        'б',    # сокращение (б/у, б.у.)
        'шт',   # штуки
        'уа',   # UA = Украина
        'юа',   # UA = Украина (альтернативная транслитерация)
        'рф',   # РФ = Россия
        'сш',   # США
        'ес',   # ЕС = Европейский союз
        # Технические product suffixes (транслитерация)
        # "гтх 3060 хт" = GTX 3060 XT, "ртх 3060 ти" = RTX 3060 Ti
        # Cross-niche: GPU, CPU, телефоны ("про", "се")
        'хт',   # XT
        'гт',   # GT
        'се',   # SE (iPhone SE)
        'еу',   # EU = Европейский Союз (транслитерация)
    }
    if word in known_abbreviations:
        return False, ""
    
    parsed = _get_parses(word, tp)[0]
    
    # Аббревиатуры (тб, гб) — пропускаем
    if 'Abbr' in parsed.tag:
        return False, ""
    
    # Известные POS — NOUN, VERB и т.д. — пропускаем (может быть аббревиатура темы)
    # Ловим только UNKN, INTJ, None
    if parsed.tag.POS in (None, 'INTJ'):
        return True, f"Мусорный токен: '{word}' ({len(word)} символа, неизвестное слово)"
    
    return False, ""


def detect_dangling(tail: str, seed: str = "ремонт пылесосов", geo_db = None, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор висячего модификатора: прилагательное без существительного.
    
    КЛЮЧЕВАЯ ЛОГИКА:
    1. Если слово — город в geo_db → НЕ dangling (Волжский, Жуковский)
    2. Если согласуется с seed-существительным → НЕ dangling (промышленных)
    3. Иначе → dangling (лучшие, хорошие)
    """
    words = tail.lower().split()
    if not words:
        return False, ""

    # Один вызов morph.parse на слово — получаем сразу все парсы и первый парс
    all_parses_list = [_get_parses(w, tp) for w in words]
    parsed_first = [p[0] for p in all_parses_list]

    has_adj = False
    has_noun = False
    adj_words_info = []

    # Маркеры именованных сущностей pymorphy3: географические названия,
    # фамилии, имена, неизменяемые имена собственные, singular tantum.
    # Используются для отсечения ложного dangling на транслитерациях брендов
    # (глово, дреаме и пр.), которые pymorphy тегирует ADJS первым разбором,
    # но имеют NOUN-разбор с маркером имени собственного.
    NAMED_ENTITY_MARKERS = ('Geox', 'Sgtm', 'Name', 'Surn', 'Fixd')

    for i, (w, p) in enumerate(zip(words, parsed_first)):
        if p.tag.POS in ('ADJF', 'ADJS'):
            # === FIX FP (глово): ambiguous transliteration guard ===
            # Неизвестное pymorphy слово с первым парсом ADJF/ADJS, но имеющее
            # NOUN-разбор с маркером именованной сущности — это транслитерированный
            # бренд/название, а не настоящее прилагательное. Не считаем ADJ.
            #
            # Пример: tail='глово' → первый парс ADJS 'гловый' (score 0.489),
            # но есть NOUN-парс 'глово' с тегами Sgtm,Geox (имя собственное,
            # singularia tantum). Это бренд Glovo (сервис доставки), не "гловое".
            #
            # Защита от сломки настоящих прилагательных:
            #   — 'новое/гелевый/большое' тоже имеют NOUN-разборы, но БЕЗ маркеров
            #     именованной сущности, поэтому не попадают под это правило
            #   — word_is_known=False гарантирует что это неизвестное слово,
            #     а не обычное прилагательное русского языка
            if not morph.word_is_known(w):
                has_named_entity_noun = any(
                    alt.tag.POS == 'NOUN'
                    and any(m in str(alt.tag) for m in NAMED_ENTITY_MARKERS)
                    for alt in all_parses_list[i]
                )
                if has_named_entity_noun:
                    # Трактуем как NOUN (именованная сущность), не как ADJ
                    has_noun = True
                    continue
            has_adj = True
            adj_words_info.append((w, all_parses_list[i]))  # уже готовые полные парсы
        if p.tag.POS == 'NOUN':
            has_noun = True
    
    if has_noun or not has_adj:
        return False, ""
    
    if len(words) > 2:
        return False, ""
    
    # === ПРОВЕРКА 1: Это город? ===
    # "Волжский", "Жуковский", "Раменское" — pymorphy видит ADJF,
    # но это города. Проверяем geo_db ДО dangling.
    if geo_db:
        for w, p in zip(words, parsed_first):
            if w in geo_db:
                return False, ""
            lemma = p.normal_form  # уже готово из parsed_first
            if lemma in geo_db:
                return False, ""

    # === ПРОВЕРКА 2: Согласование с seed ===
    seed_words = seed.lower().split()
    seed_noun_parses = None

    for sw in reversed(seed_words):
        sw_all = morph.parse(sw)  # нужны все парсы для падежей
        if sw_all[0].tag.POS == 'NOUN':
            seed_noun_parses = sw_all
            break
    
    # Если в seed НЕТ существительного (напр. "купить гтх 3060") — 
    # нельзя проверить согласование, не убиваем
    if not seed_noun_parses:
        return False, ""
    
    if seed_noun_parses:
        seed_cases = set()
        for sp in seed_noun_parses:
            if sp.tag.case:
                # Проверяем только падеж, БЕЗ числа.
                # В SEO-запросах число часто не совпадает:
                # "гелевые аккумулятор" = "гелевый аккумулятор" (опечатка числа)
                seed_cases.add(sp.tag.case)
        
        for adj_word, adj_parses in adj_words_info:
            for ap in adj_parses:
                if ap.tag.case:
                    if ap.tag.case in seed_cases:
                        return False, ""
    
    adj_strs = [w for w, _ in adj_words_info]
    return True, f"Висячий модификатор: '{' '.join(adj_strs)}' не согласуется с seed"


def detect_duplicate_words(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор дублирования слов — признак парсинг-мусора.
    
    "ремонт ремонт" → True    "пылесосов пылесос" → True (лемма)
    "samsung samsung" → True
    
    ИСКЛЮЧЕНИЕ: interrogative patterns — "где плюс где минус"
    Паттерн "где X где Y" — валидный вопрос о расположении (полярность батареи и т.д.)
    """
    words = tail.lower().split()
    if len(words) < 2:
        return False, ""
    
    # === ФИКС: Interrogative patterns ===
    # "где плюс где минус" — валидный вопрос, не дубликат
    # Паттерн: вопросительное слово повторяется с разными объектами между
    interrogative_words = {'где', 'как', 'куда', 'когда', 'какой', 'какая', 'какое', 'сколько'}
    
    # Находим позиции вопросительных слов
    interrogative_positions = [i for i, w in enumerate(words) if w in interrogative_words]
    
    # Если вопросительное слово встречается 2+ раза и между ними есть другие слова
    if len(interrogative_positions) >= 2:
        # Проверяем что между повторами есть контент
        first_pos = interrogative_positions[0]
        second_pos = interrogative_positions[1]
        if second_pos - first_pos >= 2:  # Минимум 1 слово между "где ... где"
            # Это interrogative pattern — НЕ блокируем
            return False, ""
    
    # === ФИКС: Альтернативные конструкции ===
    # "до еды или после еды" — повтор "еды" через "или"/"и" = валидная альтернатива
    # "с едой или без еды" — повтор леммы через "или" = валидная альтернатива
    # Алгоритм: если между позициями повторяющегося слова есть "или"/"и" → не дубликат
    conjunctions = {'или', 'и', 'либо', 'чи'}  # чи = укр. "или"
    conjunction_positions = {i for i, w in enumerate(words) if w in conjunctions}
    
    if conjunction_positions:
        # Собираем позиции каждого повторяющегося слова
        from collections import defaultdict
        word_positions = defaultdict(list)
        for i, w in enumerate(words):
            word_positions[w].append(i)
        
        # Есть ли дубликат с союзом между его вхождениями?
        has_dup_across_conjunction = False
        for w, positions in word_positions.items():
            if len(positions) >= 2 and w not in conjunctions:
                for ci in conjunction_positions:
                    if positions[0] < ci < positions[-1]:
                        has_dup_across_conjunction = True
                        break
            if has_dup_across_conjunction:
                break
        
        # Те же проверки для лемм
        if not has_dup_across_conjunction:
            lemma_positions = defaultdict(list)
            for i, w in enumerate(words):
                lemma = _get_parses(w, tp)[0].normal_form
                lemma_positions[lemma].append(i)
            
            for lemma, positions in lemma_positions.items():
                if len(positions) >= 2 and lemma not in conjunctions:
                    for ci in conjunction_positions:
                        if positions[0] < ci < positions[-1]:
                            has_dup_across_conjunction = True
                            break
                if has_dup_across_conjunction:
                    break
        
        if has_dup_across_conjunction:
            return False, ""
    
    # Проверка точных дубликатов
    if len(words) != len(set(words)):
        dupes = [w for w in words if words.count(w) > 1]
        return True, f"Дублирование слов: '{dupes[0]}'"
    
    # Проверка дубликатов по леммам
    lemmas = [_get_parses(w, tp)[0].normal_form for w in words]
    if len(lemmas) != len(set(lemmas)):
        dupe_lemmas = [l for l in lemmas if lemmas.count(l) > 1]
        return True, f"Дублирование лемм: '{dupe_lemmas[0]}'"
    
    return False, ""


def detect_brand_collision(tail: str, brand_db: Set[str], tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор brand collision: два бренда подряд = подозрительно.
    
    "xiaomi dreame" → True (два разных бренда)
    "dyson v15" → False (бренд + модель того же бренда)
    "samsung" → False (один бренд)
    """
    words = tail.lower().split()
    if len(words) < 2:
        return False, ""
    
    # Модели, которые НЕ считаются отдельными брендами при collision
    model_patterns = {'v8', 'v10', 'v11', 'v12', 'v15',
                      's5', 's6', 's7', 's8',
                      'roomba',
                      '2000', '3000', '4000', '5000'}
    
    # Находим бренды в хвосте
    found_brands = []
    for word in words:
        if word in brand_db and word not in model_patterns:
            found_brands.append(word)
        else:
            lemma = _get_parses(word, tp)[0].normal_form
            if lemma in brand_db and lemma not in model_patterns:
                found_brands.append(lemma)
    
    # Убираем дубликаты одного бренда
    unique_brands = list(set(found_brands))
    
    if len(unique_brands) >= 2:
        return True, f"Brand collision: {', '.join(unique_brands)}"
    
    return False, ""


# ============================================================
# ДОПОЛНИТЕЛЬНЫЙ ДЕТЕКТОР: хвост = мусорный суффикс
# ============================================================

def detect_seed_echo(tail: str, seed: str = "ремонт пылесосов", tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор эхо seed'а: хвост повторяет слова из seed.
    
    seed="ремонт пылесосов", tail="ремонт" → True (дубль)
    seed="ремонт пылесосов", tail="после ремонт" → частичный дубль
    """
    tail_words = tail.lower().split()
    seed_lemmas = _get_seed_lemmas(seed)

    # Хвост целиком = одно из слов seed'а
    if len(tail_words) == 1:
        tail_lemma = _get_parses(tail_words[0], tp)[0].normal_form
        if tail_lemma in seed_lemmas:
            return True, f"Эхо seed: '{tail_words[0]}' повторяет слово из seed"
    
    return False, ""


def detect_broken_grammar(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор сломанной грамматики: предлог + слово в неправильном падеже.
    
    "после ремонт" → True (после требует род.п., а "ремонт" в им.п.)
    "после ремонта" → False (правильное управление)
    
    ОСЛАБЛЕНИЕ ДЛЯ SEARCH QUERIES:
    Поисковые запросы часто не соблюдают грамматику:
    "аккумулятор для скутер" — человек просто набирает слова, не склоняя.
    
    Не блокируем если:
    1. Хвост = только "предлог + существительное в nomn" (типичный search pattern)
    2. Существительное — конкретный объект (не абстрактное слово)
    """
    words = tail.lower().split()
    if len(words) < 2:
        return False, ""
    
    # Предлоги и их требуемые падежи
    # Включаем вариантные формы: gen2 (второй родительный), loc2 (второй предложный)
    prep_cases = {
        'после': {'gent', 'gen2'},
        'до': {'gent', 'gen2'},
        'без': {'gent', 'gen2'},
        'для': {'gent', 'gen2'},
        'от': {'gent', 'gen2'},
        'из': {'gent', 'gen2'},
        'у': {'gent', 'gen2'},
        'около': {'gent', 'gen2'},
        'вместо': {'gent', 'gen2'},
        'кроме': {'gent', 'gen2'},
        'при': {'loct', 'loc2'},
        'на': {'loct', 'loc2', 'accs'},
        'в': {'loct', 'loc2', 'accs'},
        'о': {'loct', 'loc2'},
        'по': {'datv'},
        'к': {'datv'},
    }
    
    first = words[0]
    if first in prep_cases:
        required_cases = prep_cases[first]

        # === FIX FP (из за границы): составные предлоги ===
        # Русские составные предлоги: "из-за", "из-под", "по-над", "по-под",
        # "по-за". Пишутся через дефис, но пользователи часто пишут через
        # пробел. Без этой проверки "из за границы" → TRASH (broken_grammar),
        # хотя это валидный русский оборот ("из-за границы" = причина).
        #
        # Важно: составные предлоги имеют СВОИ падежные требования, отличные
        # от простой формы предлога. "по" требует датив, но "по-над" требует
        # творительный ("по-над водой"). Таблица держит точные требования
        # для каждой составной формы.
        compound_preps = {
            ('из', 'за'):   {'gent', 'gen2'},  # "из-за границы"
            ('из', 'под'):  {'gent', 'gen2'},  # "из-под крана"
            ('по', 'над'):  {'ablt'},          # "по-над водой"
            ('по', 'под'):  {'ablt'},          # "по-под деревом"
            ('по', 'за'):   {'ablt'},          # "по-за рекой"
        }
        if len(words) >= 3:
            compound_key = (first, words[1])
            if compound_key in compound_preps:
                compound_cases = compound_preps[compound_key]
                third_word = words[2]
                # Числа пропускаем
                if third_word.isdigit():
                    return False, ""
                third_parses = _get_parses(third_word, tp)
                third_best = third_parses[0]
                # Неизвестное слово — не считаем сломанной грамматикой
                if third_best.tag.POS is None:
                    return False, ""
                # Если хотя бы один парс даёт нужный падеж — грамматика OK
                for tp_ in third_parses:
                    if tp_.tag.case in compound_cases:
                        return False, ""
                # Падеж неверный для составного предлога — реально сломано
                actual_case = third_best.tag.case
                return True, (
                    f"Грамматика: составной предлог '{first} {words[1]}' "
                    f"требует {compound_cases}, а '{third_word}' в {actual_case}"
                )

        # Проверяем падеж следующего слова
        second_word = words[1]
        
        # Числа не имеют падежа — пропускаем ("на 256", "в 2024")
        if second_word.isdigit():
            return False, ""
        
        second_parses = _get_parses(second_word, tp)
        
        # === ФИКС: Неизвестные слова (бренды, транслитерация) ===
        # pymorphy не знает "авито", "озон", "алиэкспресс" → POS=None, case=None
        # Это НЕ сломанная грамматика, это просто незнакомое слово
        second_best = second_parses[0]
        if second_best.tag.POS is None:
            return False, ""

        # === ФИКС FP #3: одиночная буква как модификатор/элемент идиомы ===
        # "от а до я", "при гепатите б" (здесь fragment ловит), "класса а", "витамин с"
        # Одиночная буква после PREP — не "сломанная грамматика", это классификатор,
        # буква алфавита в идиоме или медицинская/техническая маркировка.
        # pymorphy даёт для одиночных букв POS=CONJ/PRCL/INTJ/NOUN(Fixd) — все варианты
        # ненадёжны в контексте. Универсальное правило: len=1 → skip.
        if len(second_word) == 1 and second_word.isalpha():
            return False, ""

        # === ФИКС FP #2: эллипсис после предлога ===
        # "до или после беременности" — после PREP идёт союз, значит эллипсис:
        # "до [чего-то] или после [чего-то]". Грамматика не сломана, просто
        # первый объект опущен. Существующая логика в detect_fragment уже
        # знает про эллипсис в конце (правило 1), но для broken_grammar
        # его нужно явно пропустить.
        if second_best.tag.POS == 'CONJ':
            return False, ""

        # === ФИКС FP #1: compound term (составной термин) после предлога ===
        # "без синус лифтинга" — "синус" (nomn) + "лифтинга" (gent) = compound
        # технический термин, который функционально = одно существительное в gent.
        # Паттерн: PREP + NOUN(nomn) + NOUN(gent|gen2) в tail длиной >= 3 слов.
        # Регрессия-safe: если только 2 слова (PREP + NOUN nomn) — это отдельный
        # ослабляющий guard ниже ("для скутер"), не трогаем.
        if len(words) >= 3:
            third_parses = _get_parses(words[2], tp)
            third_best = third_parses[0]
            if (second_best.tag.POS == 'NOUN'
                and second_best.tag.case == 'nomn'
                and third_best.tag.POS == 'NOUN'
                and third_best.tag.case in ('gent', 'gen2')):
                return False, ""

        # === ФИКС: Ослабление для search queries ===
        # Паттерн "PREP + NOUN(nomn)" в 2-словном хвосте — типичный search query
        # "для скутер", "на мотоцикл", "от генератор" — человек не склоняет
        # НЕ блокируем если это выглядит как search query
        if len(words) == 2:
            second_best = second_parses[0]
            # Если слово — конкретное существительное в именительном падеже
            if second_best.tag.POS == 'NOUN' and second_best.tag.case == 'nomn':
                # Проверяем: это конкретный объект, не абстракция?
                # Абстракции ("ремонт", "смысл") скорее будут ошибкой парсинга
                # Конкретные объекты ("скутер", "мотоцикл") — search query
                # Простая эвристика: одушевлённость или 5+ символов = конкретный объект
                is_concrete = (
                    second_best.tag.animacy == 'inan' or
                    len(second_word) >= 5 or
                    'anim' in str(second_best.tag)
                )
                if is_concrete:
                    # Это скорее search query, не блокируем
                    return False, ""
        
        # Ни один парс не даёт требуемый падеж → грамматика сломана
        has_valid_case = False
        for sp in second_parses:
            if sp.tag.case in required_cases:
                has_valid_case = True
                break
        
        if not has_valid_case:
            actual_case = second_parses[0].tag.case
            return True, f"Грамматика: '{first}' требует {required_cases}, а '{second_word}' в {actual_case}"
    
    return False, ""


def detect_type_specifier(tail: str, seed: str = "ремонт пылесосов", tp: dict = None) -> Tuple[bool, str]:
    """
    Позитивный детектор: прилагательное, согласованное с seed-существительным.
    Означает спецификацию ТИПА объекта.
    
    WEAK детектор: одно прилагательное → False (нужен контекст).
    Прилагательное + ещё контентное слово → True.
    
    "промышленных пылесосов" → True (adj + noun)
    "голубой" одно → False (weak, нужен контекст)
    """
    words = tail.lower().split()
    if not words:
        return False, ""
    
    parsed_first = [_get_parses(w, tp)[0] for w in words]
    
    has_adj = any(p.tag.POS in ('ADJF', 'ADJS') for p in parsed_first)
    has_noun = any(p.tag.POS == 'NOUN' for p in parsed_first)
    
    if not has_adj or has_noun or len(words) > 2:
        return False, ""
    
    # Ищем существительное в seed'е
    seed_words = seed.lower().split()
    seed_cases = set()
    
    for sw in reversed(seed_words):
        sp_all = morph.parse(sw)
        sp_first = sp_all[0]
        if sp_first.tag.POS == 'NOUN':
            for sp in sp_all:
                # Исключаем собственные имена: города (Geox), фамилии (Surn), имена (Name)
                # "Львов" как город → masc nomn sing — ложное согласование
                tag_str = str(sp.tag)
                if 'Geox' in tag_str or 'Surn' in tag_str or 'Name' in tag_str:
                    continue
                if sp.tag.case and sp.tag.number:
                    # Сохраняем (падеж, число, род) — род может быть None для мн.ч.
                    gender = sp.tag.gender if sp.tag.number == 'sing' else None
                    seed_cases.add((sp.tag.case, sp.tag.number, gender))
            break
    
    if not seed_cases:
        return False, ""
    
    # Проверяем каждое прилагательное на согласование
    for w, pf in zip(words, parsed_first):
        if pf.tag.POS not in ('ADJF', 'ADJS'):
            continue
        
        all_parses = _get_parses(w, tp)
        for ap in all_parses:
            if ap.tag.case and ap.tag.number:
                adj_gender = ap.tag.gender if ap.tag.number == 'sing' else None
                if (ap.tag.case, ap.tag.number, adj_gender) in seed_cases:
                    # WEAK: одни прилагательные без другого контента → не хватает
                    # "голубые" → GREY (пусть решает следующий слой)
                    # "промышленных" → GREY (может быть валидным, может нет)
                    # Оба случая требуют семантики, L0 не может решить
                    return False, ""
    
    return False, ""


def detect_noise_suffix(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор мусорных суффиксов — слова, которые НИКОГДА не бывают
    осмысленным хвостом поискового запроса.
    
    Это НЕ whitelist валидных слов. Это blacklist дефектных окончаний,
    выявленных анализом ошибок парсинга.
    
    "различия" → True    "означает" → True
    """
    tail_lower = tail.lower().strip()
    words = tail_lower.split()
    
    if not words:
        return False, ""
    
    # Одиночные слова, которые ВСЕГДА мусор как хвост поискового запроса.
    # Это не "слова которые мы не любим" — это слова, которые грамматически
    # не могут быть завершением поискового запроса вида "{seed} {tail}".
    noise_single = {
        # Незавершённые конструкции
        'различия', 'отличия', 'особенности', 'преимущества', 'недостатки',
        'разница', 'разницы',
        # ↑ эти слова ВАЛИДНЫ если есть объект: "различия моделей"
        #   но как одиночный хвост = обрывок "ремонт пылесосов различия" → ???
        
        # Бытийные / абстрактные
        'означает', 'значит',
        
        # Незавершённые глаголы
        'включает', 'содержит', 'относится',
    }
    
    if len(words) == 1 and words[0] in noise_single:
        return True, f"Мусорный суффикс: '{words[0]}' (незавершённая конструкция)"
    
    return False, ""


# ============================================================
# НОВЫЕ ПОЗИТИВНЫЕ ДЕТЕКТОРЫ
# ============================================================

def detect_verb_modifier(tail: str, seed: str = "", tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор модификатора глагола: хвост = наречие/компаратив/модальное,
    а seed содержит глагол → хвост модифицирует глагол seed'а → VALID.
    
    Алгоритмический, через POS-теги pymorphy3. Ноль хардкода.
    
    Фильтрация по семантическому классу наречия:
    - PRED (можно, нужно) → всегда модификатор → OK
    - COMP (лучше, больше) → всегда модификатор → OK
    - ADVB на -о/-е (правильно, долго, часто) → способ действия → OK
    - ADVB без -о (домой, навалом, онлайн) → направление/канал → reject
    
    Лингвистический принцип: продуктивные наречия способа действия
    в русском языке образуются от прилагательных суффиксом -о/-е.
    Направительные (домой, туда), инструментальные (навалом),
    канальные (онлайн) не имеют этого суффикса.
    """
    if not tail or not seed:
        return False, ""

    _tp_orig = tp  # сохраняем dict до любых локальных присваиваний
    
    tail_words = tail.lower().split()
    
    # Проверяем: есть ли глагол в seed?
    if not _seed_has_verb(seed):
        return False, ""
    
    # Одиночное вопросительное слово-модификатор глагола
    # "когда принимать" = вопрос о времени, "сколько принимать" = вопрос о количестве
    verb_question_modifiers = {'когда', 'сколько', 'коли', 'скільки'}
    if len(tail_words) == 1 and tail_words[0] in verb_question_modifiers:
        return True, f"Вопросительный модификатор глагола: '{tail_words[0]}' при seed с глаголом"
    
    # Хвост = 1-2 слова, все — модификаторы глагола?
    if len(tail_words) > 2:
        # Расширенный паттерн: modifier + object_refinement
        # "правильно в гранулах" = ADVB + PREP + NOUN(inan)
        # Первое слово — модификатор, остальное — уточнение объекта (форма выпуска и т.д.)
        if len(tail_words) <= 5:
            # Пропускаем все ведущие модификаторы (ADVB/-о, PRED, COMP, INFN)
            modifier_end = 0
            for tw in tail_words:
                tw_p = _get_parses(tw, tp)[0]
                is_mod = (
                    tw_p.tag.POS in ('PRED', 'COMP', 'INFN') or
                    (tw_p.tag.POS == 'ADVB' and (tw.endswith('о') or tw.endswith('е'))
                     and not any(p.tag.POS == 'PRCL' for p in _get_parses(tw, tp)))
                    # "ли"/"же" — вопросительные частицы, пропускаем ТОЛЬКО после модификатора
                    or (tw in ('ли', 'же', 'ведь') and modifier_end >= 1)
                )
                if is_mod:
                    modifier_end += 1
                else:
                    break
            
            if modifier_end >= 1 and modifier_end < len(tail_words):
                # Остаток после модификаторов
                rest = tail_words[modifier_end:]
                rest_start = _get_parses(rest[0], tp)[0]
                
                # Пропускаем PREP если есть
                noun_words = rest[1:] if rest_start.tag.POS == 'PREP' and len(rest) > 1 else rest
                
                # Пропускаем ADJ перед NOUN
                noun_idx = 0
                for nw in noun_words:
                    np_ = _get_parses(nw, tp)[0]
                    if np_.tag.POS in ('ADJF', 'ADJS', 'PRTF', 'PRTS'):
                        noun_idx += 1
                    else:
                        break
                
                if noun_idx < len(noun_words):
                    target_word = noun_words[noun_idx]
                    target_parses = _get_parses(target_word, tp)
                    target_p = target_parses[0]
                    is_inan = target_p.tag.POS == 'NOUN' and 'inan' in target_p.tag
                    has_geox = any('Geox' in str(p.tag) for p in target_parses)
                    
                    if is_inan and not has_geox:
                        return True, f"Модификатор + уточнение объекта: '{tail_words[0]}' + '{target_word}' (inan, no Geox)"
                
                # Субстантивированное прилагательное: "жаропонижающее", "обезболивающее"
                # Все ADJ, последний — neut,sing → выступает как NOUN(inan)
                elif noun_idx == len(noun_words) and noun_idx >= 1:
                    last_adj = noun_words[-1]
                    last_p = _get_parses(last_adj, _tp_orig)[0]
                    if (last_p.tag.POS in ('ADJF', 'PRTF') and 
                        last_p.tag.gender == 'neut' and 
                        last_p.tag.case in ('nomn', 'accs')):
                        return True, f"Модификатор + субстантив: '{tail_words[0]}' + '{last_adj}' (ADJ как NOUN)"
        
        return False, ""
    
    all_modifiers = True
    for tw in tail_words:
        tw_p = _get_parses(tw, _tp_orig)[0]
        pos = tw_p.tag.POS
        
        # PRED (можно, нужно) и COMP (лучше) — всегда модификаторы
        # INFN (разводить, пить) — параллельный глагол при seed с глаголом
        # "правильно разводить" = ADVB + INFN → оба модифицируют действие
        if pos in ('PRED', 'COMP', 'INFN'):
            continue
        
        # ADVB — только если образовано от прилагательного (суффикс -о/-е)
        # "правильно", "долго", "часто" → OK
        # "домой", "навалом", "онлайн" → reject
        # PRCL guard: "только", "именно", "примерно" — pymorphy видит ADVB,
        # но имеют альт. парс PRCL → ограничительные частицы, не способ действия
        if pos == 'ADVB' and (tw.endswith('о') or tw.endswith('е')):
            has_prcl = any(p.tag.POS == 'PRCL' for p in _get_parses(tw, _tp_orig))
            if has_prcl:
                all_modifiers = False
                break
            continue
        
        # Всё остальное — не модификатор
        all_modifiers = False
        break
    
    if all_modifiers:
        pos_tags = [_get_parses(w, _tp_orig)[0].tag.POS for w in tail_words]
        return True, f"Модификатор глагола: '{tail}' ({', '.join(pos_tags)}) при seed с глаголом"
    
    # 2 слова: modifier + NOUN(inan, no Geox) = модификатор + уточнение объекта
    # "нужно порошок" = PRED + NOUN(inan), "правильно лекарство" = ADVB + NOUN(inan)
    if len(tail_words) == 2:
        first_p = _get_parses(tail_words[0], _tp_orig)[0]
        first_is_modifier = (
            first_p.tag.POS in ('PRED', 'COMP', 'INFN') or
            (first_p.tag.POS == 'ADVB' and (tail_words[0].endswith('о') or tail_words[0].endswith('е'))
             and not any(p.tag.POS == 'PRCL' for p in _get_parses(tail_words[0], _tp_orig)))
        )
        if first_is_modifier:
            second_parses = _get_parses(tail_words[1], tp)
            sp = second_parses[0]
            is_inan = sp.tag.POS == 'NOUN' and 'inan' in sp.tag
            has_geox = any('Geox' in str(p.tag) for p in second_parses)
            if is_inan and not has_geox:
                return True, f"Модификатор + уточнение объекта: '{tail_words[0]}' + '{tail_words[1]}' (inan, no Geox)"
    
    # Уточнение типа продукта: ADJF + NOUN(inan, no Geox) при seed с глаголом
    # "шипучие таблетки" = ADJF + NOUN(inan) → форма выпуска
    # Cross-niche: "литиевый аккумулятор", "дисковый тормоз", "напольный кондиционер"
    # Guard: ADJF + NOUN(Geox) = "московский район" → не пройдёт
    if len(tail_words) == 2:
        p0 = _get_parses(tail_words[0], tp)[0]
        p1_parses = _get_parses(tail_words[1], tp)
        p1 = p1_parses[0]
        if p0.tag.POS in ('ADJF', 'PRTF') and p1.tag.POS == 'NOUN' and 'inan' in p1.tag:
            has_geox = any('Geox' in str(p.tag) for p in p1_parses)
            if not has_geox:
                return True, f"Уточнение типа: '{tail_words[0]}' + '{tail_words[1]}' (ADJ+NOUN inan)"
    
    return False, ""


def detect_conjunctive_extension(tail: str, seed: str = "", tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор конъюнктивного расширения: союз/предлог связывает хвост с seed'ом.
    
    Два направления:
    1. Начало: "и подарков" → CONJ + содержание → расширение
    2. Конец: "терафлю и" → содержание + CONJ → связка к seed
       "омепразол с" → содержание + PREP → связка к seed
    
    Алгоритмический: POS-теги определяют структуру.
    """
    words = tail.lower().split()
    
    if len(words) < 2:
        return False, ""
    
    content_pos = {'NOUN', 'ADJF', 'ADJS', 'ADVB', 'INFN', 'VERB', 'COMP', 'PRED', 'NPRO'}
    
    first_parsed = _get_parses(words[0], tp)[0]
    last_parsed = _get_parses(words[-1], tp)[0]
    
    # Направление 1: НАЧИНАЕТСЯ с союза + содержание после
    if first_parsed.tag.POS == 'CONJ':
        rest_words = words[1:]
        for rw in rest_words:
            for rp in _get_parses(rw, tp):
                if rp.tag.POS in content_pos:
                    return True, f"Конъюнктивное расширение: '{tail}' (союз + содержание)"
    
    # Направление 2: ЗАКАНЧИВАЕТСЯ союзом/предлогом + содержание до
    # "терафлю и" → NOUN + CONJ (связка к seed)
    # "омепразол с" → NOUN + PREP (связка к seed)
    if last_parsed.tag.POS in ('CONJ', 'PREP'):
        before_words = words[:-1]
        for bw in before_words:
            for bp in _get_parses(bw, tp):
                if bp.tag.POS in content_pos:
                    return True, f"Конъюнктивное расширение: '{tail}' (содержание + связка к seed)"
    
    return False, ""


def detect_prepositional_modifier(tail: str, seed: str = "", tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор обстоятельственного модификатора: PREP + NOUN(правильный падеж) + seed с глаголом.
    
    Зеркало detect_broken_grammar: тот ловит НЕПРАВИЛЬНЫЙ падеж → TRASH,
    этот ловит ПРАВИЛЬНЫЙ падеж → VALID.
    
    "при болях" → при(PREP) + болях(NOUN,loct) → VALID (условие)
    "после еды" → после(PREP) + еды(NOUN,gent) → VALID (время)
    "для детей" → для(PREP) + детей(NOUN,gent) → VALID (цель)
    
    Требует: seed с глаголом (word_is_known guard через _seed_has_verb).
    
    Cross-niche: "принимать при болях", "использовать при перегреве",
    "менять после 50000 км", "хранить без упаковки"
    """
    if not tail or not seed:
        return False, ""
    
    tail_words = tail.lower().split()
    if not tail_words:
        return False, ""
    
    # Раньше тут был guard `if not _seed_has_verb(seed): return False`
    # который блокировал детектор для noun-seed ("имплантация зубов",
    # "ремонт пылесосов"). Это резало валидные ключи типа
    # "имплантация зубов без боли / для детей / при диабете".
    # Структурной защиты достаточно: PREP + правильный падеж для конкретного
    # предлога — это надёжный сигнал, работающий одинаково для любого seed.
    
    # Tail должен начинаться с предлога
    first_word = tail_words[0]
    first_parsed = _get_parses(first_word, tp)[0]
    
    if first_parsed.tag.POS != 'PREP':
        return False, ""
    
    # 3. Правила управления предлогов (единый dict с detect_broken_grammar)
    # ТОЛЬКО обстоятельственные предлоги — работают с ЛЮБЫМ глаголом.
    # Аргументные (к, о, у, над, из) зависят от конкретного глагола → пропускаем.
    # Многозначные (на, в) дают слишком много FP:
    #   "на лицо", "на организм", "на озоне" — мусор; "на ночь" — валид (→ detect_time)
    #   "в таблетках" — GREY, нормально для L2
    #   "в домашних условиях" — уже ловится detect_action
    prep_cases = {
        'после': {'gent', 'gen2'},
        'до': {'gent', 'gen2'},
        'без': {'gent', 'gen2'},
        'для': {'gent', 'gen2'},
        'от': {'gent', 'gen2'},
        'около': {'gent', 'gen2'},
        'вместо': {'gent', 'gen2'},
        'кроме': {'gent', 'gen2'},
        'у': {'gent', 'gen2'},     # "у кого/чего" — привязка к получателю/обладателю
                                    # "имплантация зубов у собак", "у детей", "у пенсионеров"
                                    # Раньше считался "аргументным" (зависит от глагола),
                                    # но для noun-seed это валидная структурная привязка.
        'при': {'loct', 'loc2'},
        'по': {'datv'},
        'перед': {'ablt'},
        'с': {'ablt', 'gent'},
        'за': {'ablt', 'accs'},
        'под': {'ablt', 'accs'},
        'между': {'ablt'},
        'через': {'accs'},
    }
    
    if first_word not in prep_cases:
        return False, ""
    
    required_cases = prep_cases[first_word]
    
    # 4. Найти NOUN после предлога (пропуская ADJ, NUM, PRCL, вложенные PREP)
    _tp_prep = tp  # сохраняем оригинальный tail_parses dict
    for tw in tail_words[1:]:
        tw_p = _get_parses(tw, _tp_prep)[0]

        if tw_p.tag.POS in ('ADJF', 'ADJS', 'PRTF', 'PRTS', 'NUMR', 'PRCL'):
            continue

        if tw.isdigit():
            continue

        if tw_p.tag.POS == 'NOUN':
            all_parses = _get_parses(tw, _tp_prep)
            for p in all_parses:
                if p.tag.case in required_cases:
                    return True, f"Обстоятельственный модификатор: '{first_word} ... {tw}' ({p.tag.case}) при seed с глаголом"
            return False, ""

        if tw_p.tag.POS == 'PREP':
            continue

        break
    
    return False, ""


def detect_contacts(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор контактной информации.
    "телефон" → True    "адрес" → True    "официальный сайт" → True
    """
    contacts_lemmas = {
        'телефон', 'адрес', 'контакт', 'email', 'почта',
        'сайт', 'график', 'расписание', 'карта', 'маршрут',
        # Украинские
        'телефон', 'адреса', 'контакт', 'пошта', 'графік',
    }
    
    contacts_patterns = [
        'номер телефона', 'адрес и телефон', 'официальный сайт',
        'как добраться', 'как доехать', 'где находится', 'на карте',
        'часы работы', 'время работы', 'режим работы',
        'офіційний сайт', 'як дістатися', 'де знаходиться',
        'години роботи',
    ]
    
    tail_lower = tail.lower()
    
    for pattern in contacts_patterns:
        if pattern in tail_lower:
            return True, f"Контакты (паттерн): '{pattern}'"
    
    for word in tail_lower.split():
        lemma = _get_parses(word, tp)[0].normal_form
        if lemma in contacts_lemmas:
            return True, f"Контакты (лемма): '{lemma}'"
    
    return False, ""



# ============================================================
# НОВЫЕ НЕГАТИВНЫЕ ДЕТЕКТОРЫ
# ============================================================

def detect_technical_garbage(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор технического мусора: email, URL, телефон, длинные числа.
    "info@mail.ru" → True    "http://site.com" → True    "+380991234567" → True
    """
    import re
    
    tail_stripped = tail.strip()
    
    # Email
    if re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', tail_stripped):
        return True, f"Техмусор: email в '{tail_stripped}'"
    
    # URL
    if re.search(r'https?://', tail_stripped) or re.search(r'www\.', tail_stripped):
        return True, f"Техмусор: URL в '{tail_stripped}'"
    
    # Домен (.ru, .com, .ua)
    if re.search(r'\.[a-z]{2,4}$', tail_stripped) and '.' in tail_stripped:
        return True, f"Техмусор: домен в '{tail_stripped}'"
    
    # Телефонный номер (7+ цифр подряд или с +/-)
    digits_only = re.sub(r'[\s\-\(\)\+]', '', tail_stripped)
    if digits_only.isdigit() and len(digits_only) >= 7:
        return True, f"Техмусор: телефон '{tail_stripped}'"
    
    # Длинное число (5+ цифр, не модель товара)
    words = tail_stripped.split()
    if len(words) == 1 and words[0].isdigit() and len(words[0]) >= 5:
        return True, f"Техмусор: длинное число '{words[0]}'"
    
    return False, ""


def detect_mixed_alphabet(tail: str, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор смешанных алфавитов в одном слове.
    "рrice" (р-кириллица + rice-латиница) → True
    "iPhone" → False (чистая латиница)
    "прайс" → False (чистая кириллица)
    """
    import re
    
    for word in tail.split():
        has_cyrillic = bool(re.search(r'[а-яёіїєґА-ЯЁІЇЄҐ]', word))
        has_latin = bool(re.search(r'[a-zA-Z]', word))
        
        if has_cyrillic and has_latin and len(word) > 1:
            return True, f"Смешанный алфавит: '{word}'"
    
    return False, ""


def detect_standalone_number(tail: str, seed: str = "", tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор: хвост = просто число без контекста.
    "202" → True    "15" → True
    Исключения: числа-модели если seed = товар (v15, 3060)
    """
    words = tail.strip().split()
    
    if len(words) != 1:
        return False, ""
    
    word = words[0]
    
    if not word.isdigit():
        return False, ""
    
    num = int(word)
    
    # Год — валидный (2020-2030)
    if 2000 <= num <= 2030:
        return False, ""
    
    # Маленькие числа могут быть моделями (3060, 256)
    # Но одиночное число без букв — подозрительно
    # Исключение: seed содержит число (seed="айфон 17", tail="про" ok, tail="202" trash)
    seed_has_number = any(w.isdigit() for w in seed.split())
    
    # Если число ≤ 3 цифры и seed не числовой — TRASH
    if len(word) <= 3 and not seed_has_number:
        return True, f"Голое число: '{word}' без контекста"
    
    # 4+ цифры без буквенного контекста — подозрительно
    if len(word) >= 4:
        return True, f"Голое число: '{word}' без контекста"
    
    return False, ""


# ============================================================
# НОВЫЕ НЕГАТИВНЫЕ ДЕТЕКТОРЫ (мягкие — понижают вес, не убивают)
# ============================================================

def detect_truncated_geo_fast(tail: str, geo_db: dict, geo_index: dict, tp: dict = None) -> Tuple[bool, str]:
    """
    Быстрая версия detect_truncated_geo с pre-built индексом.
    geo_index строится один раз в TailFunctionClassifier.__init__.
    Вызывается из classify() вместо оригинальной версии.
    """
    if not tail or not geo_db:
        return False, ""

    words = tail.lower().split()
    if len(words) != 1:
        return False, ""

    word = words[0]

    if word.isdigit():
        return False, ""

    if len(word) < 3:
        return False, ""

    if word in geo_db:
        return False, ""

    word_parsed = _get_parses(word, tp)[0]
    lemma = word_parsed.normal_form
    if lemma in geo_db:
        return False, ""

    # Прилагательные — модификаторы ("старых пылесосов"), не усечённые топонимы.
    # Числительные (NUMR) — "двух", "трех", "пяти" — не могут быть обрезанным
    # топонимом даже если лемма случайно совпала с префиксом города.
    # FP: "имплантация двух зубов" (tail='двух') → лемма 'два' → 'два ручья',
    # но 'двух' — числительное в родительном падеже, не город.
    if word_parsed.tag.POS in ('ADJF', 'ADJS', 'NUMR'):
        return False, ""

    # O(1) lookup из pre-built индекса (не вызывает _build_truncated_geo_index)
    city = geo_index.get(word) or geo_index.get(lemma)
    if city:
        return True, f"Обрезанный город: '{word}' → '{city}'"

    return False, ""


def detect_truncated_geo(tail: str, geo_db: dict = None, tp: dict = None) -> Tuple[bool, str]:
    """
    Детектор обрезанного составного города.

    "ханты" → первая часть "ханты-мансийск" → TRASH
    "санкт" → первая часть "санкт-петербург" → TRASH
    "южно" → первая часть "южно-сахалинск" → TRASH

    ОПТИМИЗАЦИЯ: O(65k) перебор geo_db заменён на O(1) lookup
    через _build_truncated_geo_index — индекс {первая_часть → город}.
    Строится один раз при первом вызове, переиспользуется для всего батча.
    Логика детектора не изменена.
    """
    if not tail or not geo_db:
        return False, ""

    words = tail.lower().split()
    if len(words) != 1:
        return False, ""

    word = words[0]

    if word.isdigit():
        return False, ""

    if len(word) < 3:
        return False, ""

    # Если слово само является полноценным городом — не обрезанное
    if word in geo_db:
        return False, ""

    # Лемма — тоже полноценный город?
    word_parsed = _get_parses(word, tp)[0]
    lemma = word_parsed.normal_form
    if lemma in geo_db:
        return False, ""

    # Прилагательные — модификаторы ("старых пылесосов"), не усечённые топонимы.
    # Числительные (NUMR) — "двух", "трех", "пяти" — не могут быть обрезанным
    # топонимом даже если лемма случайно совпала с префиксом города.
    # FP: "имплантация двух зубов" (tail='двух') → лемма 'два' → 'два ручья',
    # но 'двух' — числительное в родительном падеже, не город.
    if word_parsed.tag.POS in ('ADJF', 'ADJS', 'NUMR'):
        return False, ""

    # O(1) lookup через pre-built индекс вместо O(65k) перебора
    index = _build_truncated_geo_index(geo_db)

    city = index.get(word) or index.get(lemma)
    if city:
        return True, f"Обрезанный город: '{word}' → '{city}'"

    return False, ""


# Страны мира: название → ISO код (кириллица + латиница)
# Конечный, стабильный список — не hardcode ниши, а базовая география
_COUNTRIES = {
    # СНГ + ближнее зарубежье
    'россия': 'RU', 'рф': 'RU', 'беларусь': 'BY', 'белоруссия': 'BY',
    'казахстан': 'KZ', 'узбекистан': 'UZ', 'кыргызстан': 'KG',
    'таджикистан': 'TJ', 'туркменистан': 'TM', 'азербайджан': 'AZ',
    'армения': 'AM', 'грузия': 'GE', 'молдова': 'MD', 'молдавия': 'MD',
    'украина': 'UA',
    # Европа
    'польша': 'PL', 'германия': 'DE', 'франция': 'FR', 'италия': 'IT',
    'испания': 'ES', 'португалия': 'PT', 'чехия': 'CZ', 'словакия': 'SK',
    'венгрия': 'HU', 'румыния': 'RO', 'болгария': 'BG', 'хорватия': 'HR',
    'сербия': 'RS', 'словения': 'SI', 'австрия': 'AT', 'швейцария': 'CH',
    'нидерланды': 'NL', 'голландия': 'NL', 'бельгия': 'BE',
    'швеция': 'SE', 'норвегия': 'NO', 'дания': 'DK', 'финляндия': 'FI',
    'литва': 'LT', 'латвия': 'LV', 'эстония': 'EE',
    'греция': 'GR', 'турция': 'TR', 'кипр': 'CY',
    'ирландия': 'IE', 'исландия': 'IS',
    'великобритания': 'GB', 'англия': 'GB', 'шотландия': 'GB',
    # Азия
    'китай': 'CN', 'япония': 'JP', 'корея': 'KR', 'индия': 'IN',
    'таиланд': 'TH', 'вьетнам': 'VN', 'индонезия': 'ID',
    'малайзия': 'MY', 'сингапур': 'SG', 'филиппины': 'PH',
    # Америка
    'сша': 'US', 'америка': 'US', 'канада': 'CA', 'мексика': 'MX',
    'бразилия': 'BR', 'аргентина': 'AR',
    # Ближний Восток
    'израиль': 'IL', 'иран': 'IR', 'ирак': 'IQ',
    'египет': 'EG', 'марокко': 'MA',
    'оаэ': 'AE', 'эмираты': 'AE', 'саудовская аравия': 'SA',
    # Океания
    'австралия': 'AU', 'новая зеландия': 'NZ',
    # Латиница
    'russia': 'RU', 'belarus': 'BY', 'ukraine': 'UA',
    'poland': 'PL', 'germany': 'DE', 'france': 'FR', 'italy': 'IT',
    'spain': 'ES', 'czech': 'CZ', 'switzerland': 'CH',
    'usa': 'US', 'uk': 'GB', 'china': 'CN', 'japan': 'JP',
    'turkey': 'TR', 'israel': 'IL', 'canada': 'CA',
}


def detect_foreign_geo(tail: str, geo_db: dict = None, target_country: str = "ua", tp: dict = None) -> Tuple[bool, str]:
    """
    Негативный детектор: чужая география в хвосте.
    
    Ловит:
    1. Города из ДРУГОЙ страны (через geo_db)
    2. Страны, отличные от target_country (через _COUNTRIES)
    
    НЕ ловит:
    - Паттерн "из X в Y" (обе страны в хвосте) → международный сервис
      "из украины в италию" → своя + чужая → cross-border → пропускаем
    
    Cross-niche: работает для любого seed. Ноль хардкода ниши.
    """
    if not geo_db:
        return False, ""
    
    target = target_country.upper()
    skip_pos = {'CONJ', 'PREP', 'PRCL', 'INTJ'}
    
    words = tail.lower().split()
    
    # === Предпроверка: есть ли target_country в хвосте? ===
    # Если да → паттерн "из [своей] в [чужую]" → cross-border intent → не блокируем
    #
    # ВАЖНО: эта предпроверка строго ограничена географическими словами
    # (POS=NOUN с тегом Geox). Без этого ADJS типа 'южно' (лемма 'южный',
    # inflect(nomn)='южное') случайно совпадают с UA-топонимами через
    # pymorphy.inflect и отключают весь детектор. Пример регрессии:
    #   tail='южно сахалинск' → 'южно' ADJS → nomn='южное' → есть в UA geo_db
    #   → has_target_country=True → False, "" (не блокируем)
    # После фикса 'южно' (не Geox, не NOUN) пропускается, биграмма
    # 'южно-сахалинск' проверяется корректно.
    has_target_country = False
    for word in words:
        all_parses = _get_parses(word, tp)
        parsed = all_parses[0]
        if parsed.tag.POS in skip_pos:
            continue
        # Geox-guard: только географические существительные могут быть
        # target-страной/городом в этой предпроверке. Прилагательные,
        # глаголы, нарицательные существительные — не могут.
        if parsed.tag.POS != 'NOUN' or not any('Geox' in str(p.tag) for p in all_parses):
            continue
        lemma = parsed.normal_form
        nomn_form = parsed.inflect({'nomn'})
        check_forms = {word, lemma}
        if nomn_form:
            check_forms.add(nomn_form.word)
        for cf in check_forms:
            if cf in _COUNTRIES and _COUNTRIES[cf] == target:
                has_target_country = True
                break
            if cf in geo_db and target in geo_db[cf]:
                has_target_country = True
                break
        if has_target_country:
            break

    if has_target_country:
        # "из украины в италию" → обе страны → cross-border → не блокируем
        return False, ""
    
    # === Основная проверка ===
    # Для контекстного определения улицы нужен индекс слов.
    for i_word, word in enumerate(words):
        # Один вызов — получаем и первый парс и все парсы для Geox check
        all_parses = _get_parses(word, tp)
        parsed = all_parses[0]

        if parsed.tag.POS in skip_pos:
            continue

        lemma = parsed.normal_form

        # Собираем все формы для проверки: слово, лемма, номинатив
        check_forms = {word, lemma}
        nomn_form = parsed.inflect({'nomn'})
        if nomn_form:
            check_forms.add(nomn_form.word)

        has_geox = any('Geox' in str(p.tag) for p in all_parses)
        
        # ═══════════════════════════════════════════════════════════════════
        # Guard: DISTRICT_TO_CANONICAL — слово является районом/улицей
        # ═══════════════════════════════════════════════════════════════════
        # Если слово (или его лемма) есть в базе районов/улиц (districts.json),
        # пропускаем ЗА ИСКЛЮЧЕНИЕМ случая когда это столица мира.
        # База содержит:
        #   — улицы-фамилии (гагарина, ленина, шевченко) — универсальные для
        #     всех городов
        #   — микрорайоны с распространёнными нарицательными названиями (бор,
        #     сады, маяк, заря, рог, церковь) — совпадают с обычными словами
        #   — реальные foreign-микрорайоны (лошица, чиланзар, юнусабад) —
        #     редкие специфичные названия
        #   — ТОПОНИМЫ-ОМОНИМЫ КРУПНЫХ ГОРОДОВ (варшава/париж/лондон/
        #     амстердам — улицы/микрорайоны в мелких городах, называющиеся
        #     в честь столиц). Без capital-bypass 'в варшаве' пропускалось
        #     как "район Славянска" и не блокировалось как foreign.
        #
        # Capital-bypass: если слово — alt-name столицы мира (из set
        # _CAPITAL_ALT_NAMES), district-guard НЕ пропускаем, слово идёт
        # дальше на проверку foreign_geo. Мелкие микротопонимы (бор, заря)
        # не являются столицами → guard работает как раньше.
        if word in _DISTRICT_TO_CANONICAL or lemma in _DISTRICT_TO_CANONICAL:
            # Проверяем: возможно это alt-name столицы мира
            is_capital = word in _CAPITAL_ALT_NAMES or lemma in _CAPITAL_ALT_NAMES
            if not is_capital:
                continue
            # Иначе — не пропускаем, идём дальше на foreign-check

        # Проверка 1: чужой город (geo_db) — только для географических слов
        if has_geox:
            for check_word in check_forms:
                if check_word in geo_db:
                    countries = geo_db[check_word]
                    if target not in countries:
                        foreign = ', '.join(sorted(countries))
                        return True, f"Чужой город: '{check_word}' ({foreign}, не {target})"
                    # Город из target_country — не negative
                    break
        
        # Проверка 2: чужая страна (_COUNTRIES) — тоже только для Geox/Sgtm
        # Страны всегда имеют Geox в pymorphy (Италия, Чехия, etc.)
        if has_geox:
            for check_word in check_forms:
                if check_word in _COUNTRIES:
                    country_code = _COUNTRIES[check_word]
                    if country_code != target:
                        return True, f"Чужая страна: '{check_word}' ({country_code}, не {target})"
                    break
    
    # === Проверка биграмм (мультислов топонимов) ===
    # Google Autocomplete часто отдаёт мультисловные города через пробел
    # ("улан удэ", "ханты мансийск"), а geo_db хранит через дефис
    # ("улан-удэ"). Проверяем обе нормализации.
    # Не требует Geox-guard: биграммы достаточно специфичны (6+ символов),
    # ложных срабатываний на общих словах не даёт.
    for i in range(len(words) - 1):
        w1, w2 = words[i], words[i + 1]
        # пропускаем служебные
        p1 = _get_parses(w1, tp)[0]
        p2 = _get_parses(w2, tp)[0]
        if p1.tag.POS in skip_pos or p2.tag.POS in skip_pos:
            continue
        bigram = f"{w1} {w2}"
        bigram_variants = {bigram, bigram.replace(' ', '-')}
        # DISTRICT guard для биграмм: если биграмма есть в DISTRICT базе —
        # обрабатываем по country:
        #   — country == target_country → свой район → пропускаем (continue),
        #     не блокируем ("кривой рог", "белая церковь" с target=UA)
        #   — country != target_country → чужой район → БЛОКИРУЕМ как foreign
        #     ("южное бутово" с country=RU при target=UA → foreign)
        # Capital-bypass: если биграмма-городом является столицей (редкий
        # случай, обычно столицы однословные, но "сан марино" бывает) —
        # не пропускаем через district-guard, идём на обычную проверку.
        district_country = None
        district_bigram = None
        for v in bigram_variants:
            if v in _DISTRICT_TO_CANONICAL:
                if v in _CAPITAL_ALT_NAMES:
                    # Столица-биграмм — не district, идём дальше
                    break
                district_country = _DISTRICT_TO_COUNTRY.get(v, '').lower()
                district_bigram = v
                break
        if district_country is not None:
            if district_country == target_country.lower():
                # Свой район — не блокируем, continue к следующей биграмме
                continue
            # Чужой район → foreign (аналогично чужому городу)
            return True, (
                f"Чужой район (биграмм): '{district_bigram}' "
                f"принадлежит городу '{_DISTRICT_TO_CANONICAL.get(district_bigram, '?')}' "
                f"({district_country.upper()}, не {target})"
            )
        
        for variant in bigram_variants:
            if variant in geo_db:
                countries = geo_db[variant]
                if target not in countries:
                    foreign = ', '.join(sorted(countries))
                    return True, f"Чужой город (биграмм): '{variant}' ({foreign}, не {target})"
                break
            if variant in _COUNTRIES:
                country_code = _COUNTRIES[variant]
                if country_code != target:
                    return True, f"Чужая страна (биграмм): '{variant}' ({country_code}, не {target})"
                break
    
    return False, ""


def detect_orphan_genitive(tail: str, seed: str = "", tp: dict = None) -> Tuple[bool, str]:
    """
    Мягкий детектор: одиночный генитив после seed в генитиве.
    
    seed="ремонт пылесосов", tail="аппаратов"
    → "пылесосов" = NOUN gent plur, "аппаратов" = NOUN gent plur
    → Параллельные генитивы → негативный сигнал
    
    НЕ TRASH — мягкий негативный сигнал (может быть "фильтров").
    Понижает вес в арбитраже, финальное решение за L3.
    
    Cross-niche: "купить айфон телефонов", "аккумулятор скутер моторов"
    """
    if not tail or not seed:
        return False, ""
    
    words = tail.lower().split()
    if len(words) != 1:
        return False, ""
    
    word = words[0]
    parsed = _get_parses(word, tp)[0]
    
    # Хвост = существительное в генитиве?
    if parsed.tag.POS != 'NOUN' or parsed.tag.case != 'gent':
        return False, ""
    
    # Последнее существительное seed тоже в генитиве?
    seed_words = seed.lower().split()

    # Если seed содержит паттерн [NOUN_nomn → NOUN_gent] ("ремонт пылесосов"),
    # хвост в генитиве — законное расширение той же генитивной конструкции, не сирота.
    # "ремонт двигателей пылесосов" = ремонт управляет обоими генитивами.
    last_gent_idx = -1
    for i, sw in enumerate(seed_words):
        sp = _get_parses(sw, tp)[0]
        if sp.tag.POS == 'NOUN' and sp.tag.case == 'gent':
            last_gent_idx = i
    if last_gent_idx > 0:
        prev_p = _get_parses(seed_words[last_gent_idx - 1], tp)[0]
        if (prev_p.tag.POS in ('VERB', 'INFN') or
                (prev_p.tag.POS == 'NOUN' and prev_p.tag.case == 'nomn')):
            return False, ""

    for sw in reversed(seed_words):
        sp = morph.parse(sw)[0]
        if sp.tag.POS == 'NOUN':
            if sp.tag.case == 'gent':
                return True, f"Генитив-сирота: '{word}' (gent) после seed '{sw}' (gent)"
            # Нашли существительное, но оно не в gent → не ловим
            return False, ""
    
    return False, ""


# Commerce-инфинитивы — чистые транзакционные глаголы (покупка/заказ).
# На товарном seed ("аккумулятор на скутер") одиночный commerce-инфинитив —
# валидный покупательский intent, не голый "повисший" глагол.
# На сервисном seed ("ремонт пылесосов") остаётся TRASH: "ремонт пылесосов купить"
# — несовместимая конструкция.
#
# NB: "зарядить/заменить/отремонтировать" — техническое действие, не purchase,
# поэтому в whitelist НЕ входят (subproblem_2 в fp_analysis_report — DEFERRED).
#
# ТОЛЬКО РУССКИЕ ЛЕММЫ. Украинский язык обрабатывается отдельной копией
# фильтров — смешивать нельзя (разная морфология → регрессии).
COMMERCE_INFN_LEMMAS = frozenset({
    'купить', 'заказать', 'приобрести', 'арендовать',
})


def _is_service_seed(seed: str) -> bool:
    """
    Алгоритмически определяет "сервисный" seed (услуга/процесс).
    
    Сервисный seed: NOUN(nomn/accs) + ... + NOUN(gent)
    без предлога между первыми двумя словами.
    
    Примеры сервисных:
    - "ремонт пылесосов"  (NOUN nomn + NOUN gent)
    - "доставка цветов"
    - "установка кондиционера цена"
    - "пластика лица львов"
    
    Примеры товарных (НЕ сервисных):
    - "аккумулятор на скутер"  (NOUN + PREP → False)
    - "айфон 16"               (нет второго NOUN,gent → False)
    - "как принимать нимесил"  (первое — не NOUN → False)
    
    Без словарей, чистая морфология.
    """
    words = seed.lower().split()
    if len(words) < 2:
        return False
    
    p0 = morph.parse(words[0])[0]
    if p0.tag.POS != 'NOUN':
        return False
    # Первое слово должно быть в nomn или accs (именительный/винительный)
    if 'nomn' not in p0.tag and 'accs' not in p0.tag:
        return False
    
    p1 = morph.parse(words[1])[0]
    # Предлог после первого NOUN → товарный паттерн (аккумулятор НА скутер)
    if p1.tag.POS == 'PREP':
        return False
    
    # Ищем NOUN в родительном падеже среди остальных слов seed
    for w in words[1:]:
        p = morph.parse(w)[0]
        if p.tag.POS == 'NOUN' and 'gent' in p.tag:
            return True
    return False


def detect_single_infinitive(
    tail: str,
    seed: str = "",
    tp: dict = None,
    seed_is_service: bool = True,
) -> Tuple[bool, str]:
    """
    Мягкий детектор: одиночный инфинитив без объекта.
    
    "почистить" → INFN, одно слово, seed без глагола → повисает
    
    НЕ ловим если:
    - detect_verb_modifier уже поймал (seed с глаголом + наречие)
    - Хвост > 1 слова ("почистить фильтр" — это detect_action)
    - tail — commerce-инфинитив И seed НЕ сервисный (товарный)
    
    Мягкий негативный сигнал — может быть валидным интентом,
    но структурно неполный.
    
    Cross-niche: "аккумулятор скутер заменить", "окна заклеить"
    
    Параметр seed_is_service: True (default, безопасно) = старое поведение.
    Передаётся из TailFunctionClassifier через pre-computed флаг.
    """
    if not tail or not seed:
        return False, ""
    
    words = tail.lower().split()
    if len(words) != 1:
        return False, ""
    
    word = words[0]
    parsed = _get_parses(word, tp)[0]
    
    # Только инфинитив
    if parsed.tag.POS != 'INFN':
        return False, ""
    
    # Если seed содержит глагол — хвост может быть модификатором,
    # detect_verb_modifier это уже обрабатывает → не дублируем
    seed_words = seed.lower().split()
    for sw in seed_words:
        sp = morph.parse(sw)[0]
        if sp.tag.POS in ('INFN', 'VERB'):
            return False, ""
    
    # Commerce-инфинитив на товарном seed — валидный purchase intent.
    # Защита: срабатывает только если seed ЯВНО товарный (seed_is_service=False).
    # Default seed_is_service=True не пропускает этот guard — безопасно
    # для вызовов без нового параметра.
    if not seed_is_service:
        lemma = parsed.normal_form
        if lemma in COMMERCE_INFN_LEMMAS:
            return False, ""
    
    return True, f"Голый инфинитив: '{word}' без объекта (seed без глагола)"


# ============================================================
# Info-intent detector — реальные информационные запросы
# ============================================================
# Задача: распознавать информационные/research/how-to/troubleshooting запросы
# как ПОЗИТИВНЫЙ сигнал. Semantic Agent ищет максимально широкий пул ключей
# любой направленности — definition и research запросы валидны сами по себе.
#
# Триггеры — чисто структурные (без списков ниш), работают cross-niche:
#   1. Вопросительное слово в tail (что/как/почему/зачем/когда/где/чем/какой/сколько)
#   2. "это" на конце tail — definition-маркер ("X это", "X что это")
#   3. Частица "не" + глагол — troubleshooting-паттерн ("не заряжается", "не работает")

# Вопросительные слова (RU + UA). Единый набор — используется и в detect_meta,
# и в detect_info_intent. Согласованность между детекторами.
_INTERROGATIVE_WORDS = frozenset({
    # RU
    'что', 'как', 'почему', 'зачем', 'когда', 'где', 'куда', 'откуда',
    'чем', 'чего', 'сколько',
    'какой', 'какая', 'какое', 'какие',
    # UA
    'що', 'як', 'чому', 'навіщо', 'коли', 'де', 'куди', 'звідки',
    'скільки', 'який', 'яка', 'яке', 'які',
})


def detect_info_intent(tail: str, seed: str = "", tp: dict = None) -> Tuple[bool, str]:
    """
    Позитивный детектор: информационный/research/how-to/troubleshooting запрос.
    
    Срабатывает когда хвост структурно похож на вопрос/уточнение/definition,
    независимо от ниши seed. Распознаёт реальные пользовательские вопросы:
    - "что такое X", "чем отличается X", "как называется X" — definition
    - "почему X дорогая", "чем опасна X" — research/сравнение
    - "какой X лучше", "как выбрать X" — how-to
    - "X это", "X что это" — definition-маркер
    - "X не заряжается", "X не работает" — troubleshooting
    
    Работает в паре с detect_meta: meta ловит "что такое" как сигнал
    мета-запроса (структурный маркер), info_intent говорит что этот маркер
    на самом деле ВАЛИДЕН (перекрывает meta в арбитраже).
    
    Structure-based (не blacklists/whitelists). Cross-niche.
    """
    if not tail:
        return False, ""
    
    tail_lower = tail.lower().strip()
    words = tail_lower.split()
    if not words:
        return False, ""
    
    # Триггер 1: вопросительное слово где угодно в tail (по лемме, не surface).
    # "каких" → лемма "какой" ∈ _INTERROGATIVE_WORDS → info-маркер.
    # Это маркер вопроса — человек спрашивает/уточняет/сравнивает.
    for w in words:
        # Сначала surface — дёшево, для несклоняемых (что/как/почему...)
        if w in _INTERROGATIVE_WORDS:
            return True, f"Информационный запрос: вопросительное слово '{w}'"
        parsed = _get_parses(w, tp)[0]
        # Потом лемма — для склоняемых (какой/какая/какое/какие)
        if parsed.normal_form in _INTERROGATIVE_WORDS:
            return True, f"Информационный запрос: вопросительное слово '{w}' (лемма '{parsed.normal_form}')"
        # pymorphy3 тег Ques — ловит "насколько", "откуда" без расширения списка.
        # Список — для слов без Ques-тега (что/как/когда/чем/сколько/куда).
        # Комбинация покрывает оба набора без дублирования.
        if 'Ques' in str(parsed.tag):
            return True, f"Информационный запрос: вопросительное слово '{w}' (Ques-тег)"
    
    # Триггер 2: "это" на конце tail или в составе "что это".
    # "X это [описание]" или "X что это" — definition-маркер.
    # Исключение: одиночное "это" без seed-слова перед — может быть обрывком.
    # Но мы работаем с tail (seed уже отделён), так что "это" в tail
    # означает что человек задаёт definition-вопрос.
    if words[-1] == 'это':
        return True, "Информационный запрос: 'это' на конце (definition-маркер)"
    # "что это" в середине/начале (например "лазерная что это")
    if 'это' in words:
        idx = words.index('это')
        if idx > 0 and words[idx - 1] == 'что':
            return True, "Информационный запрос: паттерн 'что это'"
    
    # Триггер 3: частица "не" + глагол — troubleshooting.
    # "не заряжается", "не работает", "не включается", "почему не работает"
    # Ищем "не" НЕ в конце tail (на конце — это обрывок fragment, не troubleshooting)
    # и следующий за ним глагол (VERB или INFN).
    for i, w in enumerate(words):
        if w == 'не' and i < len(words) - 1:
            next_parsed = _get_parses(words[i + 1], tp)[0]
            if next_parsed.tag.POS in ('VERB', 'INFN'):
                return True, f"Информационный запрос: troubleshooting 'не {words[i + 1]}'"
    
    return False, ""


# ============================================================
# Позитивные детекторы модификаторов seed (premod / postmod)
# ============================================================

# Части речи, допустимые как adjective-like модификаторы seed.
# ADJF/ADJS — полные/краткие прилагательные.
# PRTF/PRTS — причастия (функционально адъективны: "щадящая", "формирующий").
_ADJ_LIKE_POS = frozenset({'ADJF', 'ADJS', 'PRTF', 'PRTS'})


def _tail_agrees_with_seed(tail_word: str, seed_words, tp: dict = None) -> bool:
    """
    Проверяет: согласуется ли прилагательно-подобное слово tail_word
    с хотя бы одним существительным из seed по морфологии.

    Правила согласования (стандартная русская грамматика):
    - Число (sing/plur) должно совпадать
    - Падеж должен совпадать
    - Род проверяется ТОЛЬКО для singular (во множественном числе
      род в русских прилагательных не различается).

    Seed-слова парсим только первым парсом (best score) — это защита
    от ложных парсов типа "зубов" как фамилии (Sgtm,Surn).
    Прилагательное перебираем по всем парсам, но с score >= 0.1.

    Универсально для любого seed. Ноль хардкода.
    """
    adj_parses = _get_parses(tail_word, tp)
    # Seed — только best parse для каждого слова
    seed_parses = [_get_parses(sw, tp)[0] for sw in seed_words]

    for ap in adj_parses:
        if ap.tag.POS not in _ADJ_LIKE_POS:
            continue
        if ap.score < 0.1:
            continue
        for sp in seed_parses:
            if sp.tag.POS != 'NOUN':
                continue
            if ap.tag.number != sp.tag.number:
                continue
            if ap.tag.case != sp.tag.case:
                continue
            # Gender check только для singular
            if ap.tag.number == 'sing' and ap.tag.gender != sp.tag.gender:
                continue
            return True
    return False


def _tail_position_in_kw(tail: str, seed: str, kw: str) -> str:
    """
    Возвращает позицию tail относительно seed в оригинальном kw:
    - 'pre'  — tail перед первым словом seed ("базальная имплантация зубов")
    - 'post' — tail после первого слова seed ("имплантация жевательных зубов",
               "имплантация зубов этапы")
    - 'none' — не удалось определить

    Принцип: ищем индекс первого слова seed в списке слов kw.
    Сравниваем с индексом первого слова tail.

    Работает даже когда seed РАЗОРВАН вставкой:
    "имплантация жевательных зубов" — seed='имплантация зубов' разорван,
    но первое слово seed ('имплантация') идёт перед tail ('жевательных').

    Универсальный, без хардкода. Единственный случай когда возвращает 'none' —
    если слово tail или первое слово seed отсутствует в kw как отдельный токен
    (например склонение, разные регистры обработаны через lower()).
    """
    if not tail or not seed or not kw:
        return 'none'

    kw_words = kw.lower().strip().split()
    seed_words = seed.lower().strip().split()
    tail_words = tail.lower().strip().split()

    if not kw_words or not seed_words or not tail_words:
        return 'none'

    seed_first = seed_words[0]
    tail_first = tail_words[0]

    if seed_first not in kw_words:
        return 'none'
    if tail_first not in kw_words:
        return 'none'

    seed_first_idx = kw_words.index(seed_first)
    tail_first_idx = kw_words.index(tail_first)

    if tail_first_idx < seed_first_idx:
        return 'pre'
    if tail_first_idx > seed_first_idx:
        return 'post'
    return 'none'


def detect_premod_adjective(tail: str, seed: str = "", kw: str = "", tp: dict = None) -> Tuple[bool, str]:
    """
    Позитивный детектор: прилагательное/причастие ПЕРЕД seed,
    согласованное с seed по морфологии.

    Примеры (валидно):
      "базальная имплантация зубов"     → tail='базальная' pre  ~ имплантация
      "быстрая имплантация зубов"       → tail='быстрая'
      "лазерная имплантация зубов"      → tail='лазерная'
      "щадящая имплантация зубов"       → tail='щадящая' (PRTF)
      "адресная доставка цветов"        → tail='адресная' ~ доставка
      "круглосуточная доставка цветов"  → tail='круглосуточная'
      "срочный ремонт пылесосов"        → tail='срочный' ~ ремонт

    Не срабатывает:
      "кривой имплантация зубов" — masc не согласован с femn/plur
      "синий доставка цветов"    — masc nomn не согласован

    Cross-niche: работает для любого seed. Ноль хардкода ниши.
    Требует kw для определения позиции pre/post.

    Если kw не передан — детектор не срабатывает (без позиции нельзя
    отличить premod от postmod, они требуют разных детекторов).
    """
    if not tail or not seed or not kw:
        return False, ""

    words = tail.split()
    if len(words) != 1:
        return False, ""

    word = words[0].lower()
    if _tail_position_in_kw(tail, seed, kw) != 'pre':
        return False, ""

    seed_words = seed.lower().split()
    if _tail_agrees_with_seed(word, seed_words, tp=tp):
        return True, f"Премодификатор seed: '{word}' (ADJF/PRTF согласовано с seed)"
    return False, ""


def detect_postmod_adjective(tail: str, seed: str = "", kw: str = "", tp: dict = None) -> Tuple[bool, str]:
    """
    Позитивный детектор: прилагательное/причастие ПОСЛЕ seed,
    согласованное с seed по морфологии.

    Примеры (валидно):
      "имплантация жевательных зубов"   → tail='жевательных' post ~ зубов
      "имплантация верхних зубов"       → tail='верхних'
      "имплантация передних зубов"      → tail='передних'
      "имплантация коренных зубов"      → tail='коренных'
      "имплантация молочных зубов"      → tail='молочных'
      "доставка редких цветов"          → tail='редких'
      "ремонт старых пылесосов"         → tail='старых'

    По ИНДИКАТОРАМ работает ровно как premod — отличается только
    позицией tail относительно seed. Разделение на два детектора
    сделано осознанно для диагностики: по имени сигнала в trace
    видно, что сработал — premod или postmod. Упрощает отладку
    при появлении ложных срабатываний.

    Cross-niche, ноль хардкода ниши.
    """
    if not tail or not seed or not kw:
        return False, ""

    words = tail.split()
    if len(words) != 1:
        return False, ""

    word = words[0].lower()
    if _tail_position_in_kw(tail, seed, kw) != 'post':
        return False, ""

    seed_words = seed.lower().split()
    if _tail_agrees_with_seed(word, seed_words, tp=tp):
        return True, f"Постмодификатор seed: '{word}' (ADJF/PRTF согласовано с seed)"
    return False, ""


# ═══════════════════════════════════════════════════════════════════════════
# DISTRICT GUARD — защита от районов ЧУЖИХ городов.
#
# Проблема: seed='ремонт пылесосов днепр' таргетит город Днепр.
# Google Autocomplete подсовывает ключи с районами ДРУГИХ городов:
#   'днепр голосеевский район' — Голосеевский район = район Киева
#   'днепр позняки'            — Позняки = район Киева
#   'днепр солонянский район'  — район Днепропетровской области (не город)
# Сейчас detect_location тупо ловит слово "район" → VALID, detect_geo
# ловит биграмму "голосеевский район" как UA city → +geo → VALID.
#
# Решение — ДВА параллельных детектора, работающих поверх существующих
# позитивных сигналов:
#
# 1. detect_wrong_district (HARD NEGATIVE)
#    Биграмма "X район/микрорайон/квартал" есть в _DISTRICT_TO_CANONICAL,
#    её city после нормализации НЕ совпадает с seed_city → TRASH.
#
# 2. detect_unknown_district (SOFT NEGATIVE)
#    Биграммы НЕТ в базе, но структура валидна (ADJF/NOUN + район) →
#    GREY (L3 разрулит по семантике).
#
# Оба работают ТОЛЬКО когда в seed есть distinct city. Без city в seed
# интент районного таргетинга невозможен — детекторы no-op.
# ═══════════════════════════════════════════════════════════════════════════

# Суффиксы, которые образуют district-биграмму вместе с префиксом-модификатором.
# Единственный "полу-хардкод" в модуле — но это лингвистические категории слов,
# а не список конкретных районов. Cross-niche неизменен.
_DISTRICT_SUFFIXES = frozenset({'район', 'микрорайон', 'квартал'})

# POS префиксов-модификаторов, которые НЕ дают district-биграмму.
# Местоименные прилагательные (моём/нашем/вашем/своём) имеют тег Apro —
# отфильтровываем по нему алгоритмически, без хардкод-списка.
_SKIP_POS_FOR_DISTRICT_PREFIX = {'NPRO', 'PREP', 'CONJ', 'PRCL', 'INTJ'}


# Кэш seed_city — извлечение делается один раз на весь батч.
# Ключ: tuple(seed_words_lower), чтобы был hashable.
_seed_city_cache: Dict[tuple, str] = {}


def _extract_seed_city(seed: str, tp: dict = None) -> str:
    """Извлекает канонический city из seed, если он есть.

    Ищет в seed первое слово (или лемма первого слова), которое нормализуется
    в известный город geonamescache. Возвращает канонический english name
    ('dnipro', 'kyiv', ...) или '' если города в seed нет.

    Результат кэшируется — seed одинаков для всего батча.
    """
    if not seed or not _CITY_NORMALIZE:
        return ""

    seed_words = tuple(seed.lower().split())
    if seed_words in _seed_city_cache:
        return _seed_city_cache[seed_words]

    result = ""
    for w in seed_words:
        # 1. Прямой lookup
        if w in _CITY_NORMALIZE:
            result = _CITY_NORMALIZE[w]
            break
        # 2. Через лемму (днепром → днепр, киеве → киев)
        parsed = _get_parses(w, tp)
        if parsed:
            lemma = parsed[0].normal_form
            if lemma in _CITY_NORMALIZE:
                result = _CITY_NORMALIZE[lemma]
                break

    _seed_city_cache[seed_words] = result
    return result


def _find_district_bigram(tail: str, tp: dict = None):
    """Ищет биграмму 'X район/микрорайон/квартал' в tail.

    X — любое слово с валидной POS (ADJF/ADJS/NOUN/PRTF/PRTS), но НЕ
    местоименное прилагательное (тег Apro) и НЕ NPRO/PREP/CONJ/PRCL/INTJ.

    Возвращает:
      (bigram_lower, prefix_word, suffix_word) — если найдена
      (None, None, None) — если не найдена

    bigram_lower возвращается в двух формах (raw и с леммой суффикса),
    чтобы проверить матч в _DISTRICT_TO_CANONICAL. Суффиксы в базе
    'район'/'микрорайон'/'квартал' хранятся в номинативе единственного;
    пользователь может написать 'районе' (loct) → берём лемму суффикса.
    """
    words = tail.lower().split()
    if len(words) < 2:
        return None, None, None

    for i in range(len(words) - 1):
        w_suffix = words[i + 1]
        p_suffix = _get_parses(w_suffix, tp)[0]
        # Суффикс — либо точное совпадение raw, либо лемма
        suffix_lemma = p_suffix.normal_form
        if w_suffix not in _DISTRICT_SUFFIXES and suffix_lemma not in _DISTRICT_SUFFIXES:
            continue

        # Проверяем префикс
        w_prefix = words[i]
        p_prefix = _get_parses(w_prefix, tp)[0]

        if p_prefix.tag.POS in _SKIP_POS_FOR_DISTRICT_PREFIX:
            continue
        # Местоименные прилагательные: моём/нашем/вашем/своём/том/каком/этом
        if 'Apro' in str(p_prefix.tag):
            continue

        # Допустимые POS — ADJF/ADJS/NOUN/PRTF/PRTS и редкие variant'ы
        if p_prefix.tag.POS not in {'ADJF', 'ADJS', 'NOUN', 'PRTF', 'PRTS'}:
            continue

        # Нормализация биграммы для поиска в _DISTRICT_TO_CANONICAL:
        # варианты "raw X + raw suffix", "raw X + lemma suffix".
        # Лемматизировать префикс НЕ надо — в districts.json они хранятся
        # как 'голосеевский район' (именительный ADJF), а в русских хвостах
        # ADJF прилагательные приходят в согласовании с номинативным 'район'
        # тоже в номинативе. Для 'районе' нужна только лемма суффикса.
        return (w_prefix, w_suffix, suffix_lemma)

    return None, None, None


def detect_wrong_district(
    tail: str, seed: str = "",
    tp: dict = None
) -> Tuple[bool, str]:
    """HARD NEGATIVE: в хвосте биграмма 'X район', этот район принадлежит
    другому городу (не тому, что в seed).

    Пример:
      seed='ремонт пылесосов днепр', tail='голосеевский район'
      → биграмма найдена в _DISTRICT_TO_CANONICAL с city='kyiv'
      → seed_city='dnipro' ≠ 'kyiv' → TRASH

    No-op когда:
      - _DISTRICT_TO_CANONICAL пуст (districts.json не загружен)
      - _CITY_NORMALIZE пуст (geonamescache недоступен)
      - в seed нет distinct city (taрgetинг не городской)
      - биграммы 'X район' в tail нет
      - биграммы нет в базе _DISTRICT_TO_CANONICAL
    """
    if not seed or not _DISTRICT_TO_CANONICAL or not _CITY_NORMALIZE:
        return False, ""

    seed_city = _extract_seed_city(seed, tp=tp)
    if not seed_city:
        return False, ""

    prefix, suffix_raw, suffix_lemma = _find_district_bigram(tail, tp=tp)
    if prefix is None:
        return False, ""

    # Проверяем все варианты биграммы: raw/lemma prefix × raw/lemma suffix.
    # В _DISTRICT_TO_CANONICAL биграммы хранятся в номинативе ('голосеевский район'),
    # но пользователи пишут и в loct ('голосеевском районе'). Покрываем оба случая.
    prefix_lemma = _get_parses(prefix, tp)[0].normal_form
    candidates = {
        f"{prefix} {suffix_raw}",
        f"{prefix} {suffix_lemma}",
        f"{prefix_lemma} {suffix_raw}",
        f"{prefix_lemma} {suffix_lemma}",
    }
    for bigram in candidates:
        district_city_raw = _DISTRICT_TO_CANONICAL.get(bigram)
        if not district_city_raw:
            continue
        district_city_norm = _normalize_city(district_city_raw)
        if district_city_norm != seed_city:
            return True, (
                f"Район другого города: '{bigram}' принадлежит "
                f"'{district_city_raw}' (норм: '{district_city_norm}'), "
                f"а seed указывает '{seed_city}'"
            )
        # Совпал — свой район своего города, не блокируем
        return False, ""

    # Биграмма не в базе — это работа detect_unknown_district
    return False, ""


def detect_unknown_district(
    tail: str, seed: str = "",
    tp: dict = None
) -> Tuple[bool, str]:
    """SOFT NEGATIVE: в хвосте структура 'X район/микрорайон/квартал',
    биграммы НЕТ в базе _DISTRICT_TO_CANONICAL.

    Используется когда в seed есть город, но конкретный район не знаем.
    Это мягкий сигнал: может быть реальный район того же города (тогда
    VALID), может быть район другого города (тогда TRASH) — решение
    оставляем за L3.

    Возвращает True только если:
      - в seed есть distinct city
      - биграмма 'X район' действительно есть (валидная структура)
      - биграммы НЕТ в _DISTRICT_TO_CANONICAL

    No-op симметрично detect_wrong_district. Без city в seed не срабатывает,
    чтобы не ловить generic-случаи типа "купить пылесос голосеевский район"
    (где seed не городской → район может быть из любого города → пусть
    location-паттерн работает как раньше).
    """
    if not seed:
        return False, ""

    # Нужен хотя бы пустой, но загруженный (глобальный initialized) индекс.
    # Если districts не загрузились, это равно "биграмма не в базе" всегда,
    # и мы будем заваливать все 'X район' в GREY. Защита: no-op.
    if not _DISTRICT_TO_CANONICAL:
        return False, ""

    # Без city в seed детектор не работает (см. docstring).
    # _CITY_NORMALIZE может быть пуст (geonamescache недоступен) — тогда
    # _extract_seed_city вернёт '' и мы выйдем. Это корректно: без
    # нормализации мы не можем ничего осмысленно противопоставить.
    if not _CITY_NORMALIZE:
        return False, ""

    seed_city = _extract_seed_city(seed, tp=tp)
    if not seed_city:
        return False, ""

    prefix, suffix_raw, suffix_lemma = _find_district_bigram(tail, tp=tp)
    if prefix is None:
        return False, ""

    # Есть ли биграмма в базе? Проверяем все варианты (raw/lemma × raw/lemma).
    prefix_lemma = _get_parses(prefix, tp)[0].normal_form
    candidates = {
        f"{prefix} {suffix_raw}",
        f"{prefix} {suffix_lemma}",
        f"{prefix_lemma} {suffix_raw}",
        f"{prefix_lemma} {suffix_lemma}",
    }
    for bigram in candidates:
        if bigram in _DISTRICT_TO_CANONICAL:
            # Биграмма известна — это работа detect_wrong_district,
            # а не unknown.
            return False, ""

    # Биграмма валидна структурно, но не в базе — мягкий сигнал.
    # Используем лемматизированные формы в reason для читаемости.
    display_bigram = f"{prefix_lemma} {suffix_lemma}"
    return True, (
        f"Неизвестный район: '{display_bigram}' отсутствует в базе "
        f"districts.json, а seed указывает '{seed_city}'"
    )


# ═══════════════════════════════════════════════════════════════════════════
# PRODUCT SPEC — техническая спецификация товара в хвосте.
#
# Проблема: Google Autocomplete для товарных seed'ов возвращает ключи с
# техническими параметрами:
#   'аккумулятор на скутер 12 в'    (12 вольт)
#   'аккумулятор на скутер 220в'    (220 вольт слитно)
#   'аккумулятор на скутер 4т'      (4-тактный)
#   'аккумулятор на скутер 150 мм'  (150 миллиметров)
#
# Это валидные коммерческие запросы — пользователь уточняет параметры
# товара. Но голое число триггерит detect_standalone_number (TRASH),
# а 'в'/'а' на конце триггерит detect_fragment (TRASH).
#
# АЛГОРИТМИЧЕСКИЙ ПАТТЕРН (без хардкода единиц):
#   — <число><1-3 буквы>      слитно:  4т, 12в, 220ом, 150мм
#   — <число> <1-3 буквы>     раздельно: '12 в', '220 ом', '150 мм'
#
# Ограничение на 1-3 буквы после числа — это все стандартные единицы
# измерения в русском и английском (в, а, т, ом, вт, гц, мм, см, кг, нм,
# мгц, кгц, ma, mm, kg, hz, w, v, a). Полные слова ('12 вольт') не
# попадают — они валидируются другими детекторами.
#
# Срабатывает только если в seed есть NOUN (товар или услуга, не голое
# action-слово). Страховка от "купить 12 в" где seed = голый глагол.
# ═══════════════════════════════════════════════════════════════════════════

import re as _re_for_spec

_PRODUCT_SPEC_COMBINED = _re_for_spec.compile(
    r'^\d+[а-яёіїєґa-z]{1,3}$', _re_for_spec.IGNORECASE
)
_PRODUCT_SPEC_DIGIT = _re_for_spec.compile(r'^\d+$')
_PRODUCT_SPEC_SHORT_LETTERS = _re_for_spec.compile(
    r'^[а-яёіїєґa-z]{1,3}$', _re_for_spec.IGNORECASE
)


def detect_product_spec(tail: str, seed: str = "", tp: dict = None) -> Tuple[bool, str]:
    """Позитивный детектор: технические спецификации товара в хвосте.

    Ловит два паттерна:
      1. Один токен '<число><1-3 буквы>' — '4т', '12в', '220ом', '150мм'
      2. Два токена '<число> <1-3 буквы>' — '12 в', '220 ом', '150 мм'

    Cross-niche: работает для любого товарного seed (аккумулятор, лампочка,
    провод, труба, двигатель). Не срабатывает для сервисных seed без NOUN
    ('купить', 'доставить' без товара).

    Защита от FP:
      — Не ловит полные слова ('12 вольт' — 5 букв, не попадёт)
      — Не ловит модели ('v1', 'iphone15' — начинаются с буквы)
      — Не ловит даты и время ('14:00', '24/7' — имеют разделители)
      — Требует NOUN в seed (исключает "купить 12 в")
    """
    if not tail:
        return False, ""

    # Страховка: seed должен содержать NOUN (это реальный товар/услуга,
    # а не голый глагол). Иначе '12 в' без товарного контекста — мусор.
    if seed:
        seed_words = seed.lower().split()
        has_noun = False
        for sw in seed_words:
            sp = _get_parses(sw, tp)[0]
            if sp.tag.POS == 'NOUN':
                has_noun = True
                break
        if not has_noun:
            return False, ""

    tokens = tail.lower().split()
    if not tokens:
        return False, ""

    # Случай 1: один токен, слитная форма '<число><1-3 буквы>'
    if len(tokens) == 1:
        if _PRODUCT_SPEC_COMBINED.match(tokens[0]):
            return True, f"Спецификация товара: '{tokens[0]}' (<число>+<единица>)"
        return False, ""

    # Случай 2: два токена '<число> <1-3 буквы>'
    if len(tokens) == 2:
        if (_PRODUCT_SPEC_DIGIT.match(tokens[0])
                and _PRODUCT_SPEC_SHORT_LETTERS.match(tokens[1])):
            return True, (
                f"Спецификация товара: '{tokens[0]} {tokens[1]}' "
                f"(<число> <единица>)"
            )
        return False, ""

    return False, ""
