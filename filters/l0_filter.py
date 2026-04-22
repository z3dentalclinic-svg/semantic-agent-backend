"""
l0_filter.py — Серверная обёртка L0 классификатора.

Встраивается в пайплайн ПОСЛЕ всех существующих фильтров:
    pre_filter → geo_garbage → batch_post → deduplicate → L0

Принимает result dict с keywords → классифицирует каждый ключ →
разделяет на VALID / TRASH / GREY.

Результат:
    result["keywords"]       → VALID ключи (высокая уверенность)
    result["keywords_grey"]  → GREY ключи (для будущего perplexity)
    result["anchors"]       += TRASH ключи с пометкой [L0]
    result["_l0_trace"]      → детальный трейсинг каждого ключа
    result["_filter_timings"]["l0"] → тайминги L0 (extract_tail / classify / total)

ОПТИМИЗАЦИИ:
1. Персистентный кэш TailFunctionClassifier — не пересоздаётся на каждый батч.
   Кэш по ключу (seed, target_country). geo_db/brand_db загружаются один раз.
2. build_seed_ctx() вызывается один раз до цикла — seed одинаков для всего батча.
3. _filter_timings добавляется к result — для профилирования горячих точек.
"""

import logging
import re
import time
from typing import Dict, List, Set, Any, Optional

from .tail_extractor import extract_tail, build_seed_ctx
from .tail_function_classifier import TailFunctionClassifier

logger = logging.getLogger(__name__)


# ============================================================
# Персистентный кэш классификатора.
# TailFunctionClassifier создаётся ОДИН РАЗ на (seed, target_country)
# и переиспользуется во всех последующих запросах с тем же seed.
#
# ВАЖНО: кэш хранит ссылку на geo_db / brand_db.
# Если эти базы пересоздаются при каждом запросе (новый dict) —
# кэш теряет смысл. Убедитесь что bases загружаются при старте сервера
# и передаются одним и тем же объектом.
# ============================================================
_CLASSIFIER_CACHE: Dict[tuple, TailFunctionClassifier] = {}


# ============================================================
# Буквы эксклюзивно украинского алфавита.
# Конечный набор из 4 символов (+ верхний регистр). Не ниша, не хардкод слов —
# базовое свойство алфавита. Используется для быстрой детекции UA-запросов
# в RU-пайплайне: если в kw есть хоть одна такая буква — это не RU, TRASH.
# ============================================================
_UA_EXCLUSIVE_LETTERS = frozenset('іїєґІЇЄҐ')


# ============================================================
# Украинские слова БЕЗ эксклюзивных UA-букв.
# Эти леммы не существуют в русском (купити, замовити, заказати) или
# существуют но НИКОГДА не встречаются в хвосте русского поискового
# запроса как осмысленная единица (пошта, ринок, ціни, вартість).
#
# Используются в fallback-проверке когда `tail is None`:
# если extractor не нашёл seed, и в kw присутствует хотя бы одно из
# этих слов — это UA-запрос для отдельного пайплайна → TRASH.
#
# НЕ используется на tail после успешного извлечения: там классификатор
# сам разбирает ключи типа "купити аккумулятор на скутер 12 в" (VALID
# через product_spec).
# ============================================================
_UA_AMBIGUOUS_WORDS = frozenset({
    'купити', 'замовити', 'заказати',
    'огляд', 'розстрочку', 'ринок',
    'ціни', 'вартість', 'пошта',
})


# Regex: числовые токены (целые числа как отдельные слова).
# Используется для обнаружения "wrong model" сценария:
# seed="купить айфон 16", kw="купить айфон 14 киев" → seed extractor
# вернул None (число 16 не найдено в kw, только 14) → TRASH.
#
# Boundary \b гарантирует что 16 НЕ матчится в 16gb, 2016, 160.
_NUMBER_TOKEN_RE = re.compile(r'\b\d+\b')


# ============================================================
# SANITY CHECK: отсев ложных L0 VALID на LATN-сидах.
#
# Проблема: tail_extractor на сидах с латинскими токенами (samsung galaxy s21,
# macbook pro, xiaomi redmi) возвращает пустой tail для ключей, в которых
# часть токенов сида отсутствует. Causes (существующие в Шагах 2/3/4):
#   - partial_match разрешает пропуск LATN-токенов seed (POS=None, не CONTENT)
#   - cross-script мост ошибочно связывает ремонт↔review, ремонт↔hotline
# Исправить эти баги системно требует большой правки в tail_extractor; это
# отдельная задача с риском регрессий на других LATN-паттернах.
#
# Текущий подход: принимаем как есть выход tail_extractor, но добавляем
# финальную проверку L0 VALID. Если kw не содержит обязательных content-слов
# seed (точное совпадение, лемма, или кросс-скрипт через brand_db / translit),
# переводим VALID → GREY с reason "sanity_mismatch". L2 затем через PMI/KNN
# может реабилитировать семантические синонимы (замена экрана, не работает,
# чистка) как близкие к reference words сида.
#
# Правило (подтверждено пользователем):
#   - len(seed) <= 2: все content-слова обязательны (строго)
#   - len(seed) >= 3: допустим 1 пропуск content-слова (soft, N-1 из N)
#   - LATN-модели (LATN-токены seed ОТСУТСТВУЮЩИЕ в brand_db) — ВСЕГДА строго.
#     Модель — уникальный идентификатор продукта: пропуск = другая модель.
#
# Cross-script поддержка (чтобы 'ремонт самсунг с21' при seed 'samsung galaxy
# s21 ремонт' оставался VALID):
#   - LATN-бренды seed считаются присутствующими, если в kw есть ЛЮБОЕ
#     cyr-слово из brand_db (т.е. "kw упоминает какой-то известный бренд").
#   - LATN-модели seed сравниваются с транслитерированным ключом (с21→s21).
#
# Безопасность для cyr-only сидов (цветы, зубы, кондиционер, скутер):
#   - latn_model пустой → model-check не срабатывает
#   - len >= 3 → soft rule допускает 1 пропуск
#   - Результат: 0 регрессий на 4 контрольных cyr-only датасетах.
# ============================================================

# Алгоритмическая транслитерация cyr→lat. Используется для проверки LATN-моделей
# сида в kw: слова вида "с21"/"м21"/"с24" в kw автоматически матчатся на
# "s21"/"m21"/"s24" в seed. Таблица не покрывает все фонетические варианты
# (х→h/kh, ц→ts/c) — это не нужно для моделей, которые обычно состоят из
# 1-2 букв + цифр.
_TRANSLIT_CYR_TO_LAT = str.maketrans({
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
    'ж': 'z', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
    'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
    'ф': 'f', 'х': 'h', 'ц': 'c', 'ч': 'c', 'ш': 's', 'щ': 's',
    'ъ': '',  'ы': 'y', 'ь': '',  'э': 'e', 'ю': 'u', 'я': 'a',
})


def _translit_cyr_to_lat(w: str) -> str:
    """Транслитерация кириллического слова в латиницу."""
    return w.translate(_TRANSLIT_CYR_TO_LAT)


def _parse_seed_for_sanity(seed_ctx: dict, brand_db: Set[str]) -> Optional[dict]:
    """
    Разбирает seed на 3 категории content-токенов:
      - cyr_content: леммы cyr-слов с POS in {NOUN,VERB,ADJF,ADJS,INFN,PRTF,PRTS}
      - latn_brand:  LATN-токены seed, ПРИСУТСТВУЮЩИЕ в brand_db (бренды)
      - latn_model:  LATN-токены seed, ОТСУТСТВУЮЩИЕ в brand_db (модели/коды)

    Возвращает None если у seed нет ни одного content-токена (пустой/мусорный seed).
    """
    words = seed_ctx['words']
    lemmas = seed_ctx['lemmas']
    pos = seed_ctx['pos']
    is_lat = seed_ctx['is_lat']
    is_number = seed_ctx['is_number']

    CONTENT_POS = {'NOUN', 'VERB', 'ADJF', 'ADJS', 'INFN', 'PRTF', 'PRTS'}

    cyr_content: Set[str] = set()
    latn_brand: Set[str] = set()
    latn_model: Set[str] = set()

    for i, w in enumerate(words):
        if is_number[i]:
            continue  # числа — отдельная логика (_seed_numbers), не трогаем
        p = pos[i]
        if p is None and is_lat[i]:
            # LATN-токен: разделяем на brand (в brand_db) или model (вне brand_db)
            if w in brand_db or lemmas[i] in brand_db:
                latn_brand.add(w)
            else:
                latn_model.add(w)
        elif p in CONTENT_POS:
            # cyr-контентное слово (или любое с известной POS-ролью): лемма как anchor
            cyr_content.add(lemmas[i])
        # PREP/CONJ/PRCL/INTJ и прочие функциональные — пропускаем

    # Длина seed в словах (как пишет пользователь), включая PREP/CONJ.
    # По ней определяется жёсткость правила: <=2 строго, >=3 soft (N-1).
    # Причина использовать words, а не content-токены: для сида "аккумулятор на
    # скутер" (3 слова, 2 content-токена) пользователь ожидает soft-поведение
    # (типа "аккумулятор для segway" остаётся VALID). А content_len=2 дал бы
    # строгое правило и ложную демотацию.
    seed_word_count = len(seed_ctx['words'])

    # Если нет ни одного content-токена — нечего проверять
    if len(cyr_content) + len(latn_brand) + len(latn_model) == 0:
        return None

    return {
        'cyr_content': frozenset(cyr_content),
        'latn_brand': frozenset(latn_brand),
        'latn_model': frozenset(latn_model),
        'seed_word_count': seed_word_count,
    }


def _sanity_check_valid(
    kw: str,
    sanity_ctx: dict,
    brand_db: Set[str],
    morph,
) -> Optional[str]:
    """
    Проверяет L0 VALID ключ на соответствие требованиям seed.

    Возвращает None если всё ок (ключ остаётся VALID).
    Возвращает строку-причину если ключ нужно демотировать в GREY.

    Причины:
      - "missing_model:<token>"   — LATN-модель seed отсутствует в kw
      - "missing_brand"           — ни один LATN-бренд seed не найден (ни прямо,
                                     ни через cyr-brand из brand_db)
      - "missing_intent:<lemma>"  — обязательное cyr-content слово отсутствует
                                     (intent-anchor в LATN-сиде)
      - "missing_content:<N>"     — пропущено больше допустимого cyr-content
                                     слов в cyr-only сиде

    Правила:
      1. LATN-модели (latn_model) — всегда все обязательны. Модель — уникальный
         идентификатор продукта, пропуск = другая модель. С translit fallback:
         "с21" в kw → засчитывается как "s21" из seed.
      2. LATN-бренды (latn_brand) — достаточно одного присутствующего (прямо
         в kw, или через любой cyr-brand из brand_db, напр. 'самсунг', 'макбук').
      3. CYR-content-слова:
         - Если в seed есть LATN-токены (бренды/модели) → сид товарный, cyr-слова
           являются intent-anchor (ремонт/купить/чехол). Все обязательны.
         - Если LATN в seed нет → сид чисто услуговый, cyr-слова квалификаторы.
           При seed_word_count <= 2 все обязательны, при >= 3 допустим 1 пропуск.
           Это сохраняет fallback поведение для сидов типа "установка
           кондиционера цена" (пропуск 'цена' → 'установка кондиционера olx' ok).

    Обоснование разделения:
      - "samsung galaxy s21 hotline" (LATN-сид без 'ремонт'): intent-mismatch,
        пользователь ищет магазин hotline, не ремонт.
      - "samsung galaxy s21 разборка", "... teardown": другой intent (teardown
        != ремонт), demote.
      - "установка кондиционера olx": cyr-сид, 'цена' опциональный квалификатор,
        OLX это ретейлер → commerce intent совместим с установкой. Keep.
      - "ремонт samsung s21" (без galaxy): все LATN-бренды seed покрыты через
        'samsung' в kw (N-1 достаточно для brand pool). Keep.
    """
    cyr_content = sanity_ctx['cyr_content']
    latn_brand = sanity_ctx['latn_brand']
    latn_model = sanity_ctx['latn_model']
    seed_word_count = sanity_ctx['seed_word_count']

    # Если в сиде нет ни content-слов — нечего проверять
    if not cyr_content and not latn_brand and not latn_model:
        return None

    is_latn_seed = bool(latn_brand or latn_model)

    # Собираем множества kw: точные слова + леммы + флаг наличия cyr-brand
    kw_words = kw.lower().split()
    kw_words_set = set(kw_words)
    kw_lemmas_set: Set[str] = set()
    kw_has_cyr_brand = False
    for w in kw_words:
        lemma = morph.parse(w)[0].normal_form
        kw_lemmas_set.add(lemma)
        if not kw_has_cyr_brand and any('\u0400' <= c <= '\u04ff' for c in w):
            if w in brand_db or lemma in brand_db:
                kw_has_cyr_brand = True
    kw_all = kw_words_set | kw_lemmas_set

    # ── Шаг 1: latn_model обязательны строго (с translit) ────────────────
    kw_translit_cache = None
    for m in latn_model:
        if m in kw_all:
            continue
        if kw_translit_cache is None:
            kw_translit_cache = set()
            for w in kw_words:
                if any('\u0400' <= c <= '\u04ff' for c in w):
                    kw_translit_cache.add(_translit_cyr_to_lat(w))
        if m in kw_translit_cache:
            continue
        return f"missing_model:{m}"

    # ── Шаг 2: latn_brand хотя бы один ────────────────────────────────────
    if latn_brand:
        if not (latn_brand & kw_all):
            if not kw_has_cyr_brand:
                return "missing_brand"

    # ── Шаг 3: cyr_content по правилу длины/типа сида ─────────────────────
    missing_cyr = [cl for cl in cyr_content if cl not in kw_all]
    if is_latn_seed:
        # Товарный сид: cyr-content = intent-anchor, все обязательны
        if missing_cyr:
            return f"missing_intent:{missing_cyr[0]}"
    else:
        # Услуговый cyr-only сид: квалификаторы, soft при len>=3
        allowed = 0 if seed_word_count <= 2 else 1
        if len(missing_cyr) > allowed:
            return f"missing_content:{','.join(missing_cyr[:3])}"

    return None


def _check_last_word_skip_eligible(seed_ctx: dict) -> bool:
    """
    Проверяет, допустим ли fallback-экстракт с укороченным seed (без последнего слова).

    Структурный критерий: разрешаем пропуск последнего слова seed'а ТОЛЬКО когда
    это слово — самостоятельная сущность-квалификатор (цена/москва/лондон), а не
    грамматически зависимый объект (нимесил, скутер, машин).

    Условия (ВСЕ обязательны):
      1. seed >= 3 слов — короткие seed (2 слова) не затрагиваются правкой.
      2. Последнее слово — NOUN в nomn или accs (самостоятельная сущность, не
         зависимый модификатор). Падежи gent/datv/instr/loct обычно означают
         зависимость от предыдущего слова → разрывать нельзя.
      3. Предпоследнее слово — NOUN. Если там ADJF ("стиральных машин") или
         INFN ("принимать нимесил") — последнее слово связано грамматически
         с предыдущим в единое NP или является прямым объектом глагола.
      4. Предпоследнее слово не PREP/CONJ. Если seed вида "аккумулятор на скутер"
         или "холодильник и плита", последнее слово зависит от предлога/союза
         и не может быть пропущено. (Покрыто условием 3 — PREP/CONJ не NOUN,
         но комментарий оставляем для ясности.)

    Примеры eligible=True:
      - "установка кондиционера цена"  → 'цена' = nomn NOUN, 'кондиционера' = NOUN
      - "доставка цветов москва"       → 'москва' = nomn NOUN, 'цветов' = NOUN
      - "услуги юриста лондон"          → 'лондон' = accs NOUN, 'юриста' = NOUN
      - "курсы английского языка киев" → 'киев' = nomn NOUN, 'языка' = NOUN

    Примеры eligible=False:
      - "аккумулятор на скутер"  → предпоследнее 'на' — PREP, не NOUN
      - "как принимать нимесил"  → предпоследнее 'принимать' — INFN, не NOUN
      - "купить айфон 16"         → последнее '16' — не NOUN (число)
      - "ремонт стиральных машин" → предпоследнее 'стиральных' — ADJF, не NOUN
      - "пластика лица львов"     → последнее 'львов' в gent, не nomn/accs
    """
    words = seed_ctx['words']
    if len(words) < 3:
        return False

    pos = seed_ctx['pos']

    # Последнее и предпоследнее — NOUN
    if pos[-1] != 'NOUN' or pos[-2] != 'NOUN':
        return False

    # Падеж последнего слова: nomn (независимая сущность) или accs (часто
    # совпадает с nomn для неодуш. м.р., pymorphy выбирает один из вариантов
    # непредсказуемо для городов: "москва"→nomn, "лондон"→accs).
    # Gent/datv/instr/loct означают грамматическую зависимость — разрывать нельзя.
    from .shared_morph import morph as _m
    last_case = _m.parse(words[-1])[0].tag.case
    if last_case not in ('nomn', 'accs'):
        return False

    return True


def _get_classifier(
    seed: str,
    target_country: str,
    geo_db: Dict,
    brand_db: Set,
    retailer_db: Set,
) -> TailFunctionClassifier:
    """
    Возвращает персистентный классификатор для данного (seed, target_country).
    Создаёт новый только при первом вызове или при смене seed/country.
    """
    key = (seed.lower().strip(), target_country.lower())
    if key not in _CLASSIFIER_CACHE:
        logger.info(
            "[L0] Creating new classifier for seed='%s' country='%s' "
            "(geo_db=%d brand_db=%d retailer_db=%d)",
            seed, target_country,
            len(geo_db) if geo_db else 0,
            len(brand_db) if brand_db else 0,
            len(retailer_db) if retailer_db else 0,
        )
        _CLASSIFIER_CACHE[key] = TailFunctionClassifier(
            geo_db=geo_db,
            brand_db=brand_db,
            seed=seed,
            target_country=target_country,
            retailer_db=retailer_db,
        )
    return _CLASSIFIER_CACHE[key]


def apply_l0_filter(
    result: Dict[str, Any],
    seed: str,
    target_country: str = "ua",
    geo_db: Dict[str, Set[str]] = None,
    brand_db: Set[str] = None,
    retailer_db: Set[str] = None,
) -> Dict[str, Any]:
    """
    Применяет L0 классификатор к списку ключевых слов.

    Args:
        result: dict с ключами "keywords", "anchors"
        seed: базовый запрос
        target_country: целевая страна
        geo_db: база городов Dict[str, Set[str]] (название → {коды_стран})
        brand_db: база брендов (set). Если None — пустой set
        retailer_db: база ритейлеров/маркетплейсов (set). Если None — пустой set.
                     Должен загружаться вызывающим кодом через databases.load_retailers_db().

    Returns:
        result с обновлёнными keywords, keywords_grey, anchors, _l0_trace, _filter_timings
    """
    t_l0_start = time.perf_counter()
    # Детальные тайминги этапов L0 для диагностики узких мест
    _t_stage = {}

    keywords = result.get("keywords", [])
    if not keywords:
        result.setdefault("keywords_grey", [])
        result.setdefault("_l0_trace", [])
        _add_timings(result, 0.0, 0.0, 0.0)
        return result

    if geo_db is None:
        geo_db = {}
    if brand_db is None:
        brand_db = set()
    if retailer_db is None:
        retailer_db = set()

    # ── Pre-compute seed_ctx ОДИН РАЗ для всего батча ──────────────────────
    # Seed одинаков для всех ключей → лематизация seed не повторяется N раз.
    _t = time.perf_counter()
    seed_lower = seed.lower().strip()
    seed_ctx = build_seed_ctx(seed_lower)
    # Pre-compute: цельные числовые токены в seed (как frozenset для O(1) lookup).
    # Используется в NO_SEED-ветке ниже для классификации "wrong model"
    # сценария: seed="купить айфон 16", kw="купить айфон 14 киев" → 14 не
    # совпадает ни с одним числом seed → TRASH.
    _seed_numbers = frozenset(_NUMBER_TOKEN_RE.findall(seed_lower))

    # Pre-compute: fallback-экстракт с укороченным seed (без последнего слова).
    # Мотивация: когда пользователь вводит seed из 3+ слов, последнее слово
    # которого — самостоятельный квалификатор ('цена', 'москва', 'отзывы'),
    # то большинство реальных autocomplete-запросов его НЕ содержат (редкое
    # слово-триггер). Пример: seed="установка кондиционера цена" — 383 из 393
    # ключей без "цена" в них, но все про установку кондиционера. Без fallback
    # все они теряются в GREY 'seed не найден'.
    #
    # Eligibility строго морфологическая (см. _check_last_word_skip_eligible).
    # Cross-niche защита: для сидов из 2 слов, сидов с PREP между словами,
    # сидов с ADJF/INFN перед последним словом — правило НЕ срабатывает.
    _skip_last_eligible = _check_last_word_skip_eligible(seed_ctx)
    if _skip_last_eligible:
        _short_seed = ' '.join(seed_ctx['words'][:-1])
        _short_seed_ctx = build_seed_ctx(_short_seed)
        # Lemma-guard: для валидации fallback требуем, чтобы ВСЕ леммы
        # укороченного seed присутствовали в kw как леммы (прямое совпадение,
        # без cross-script моста). Это отсекает случаи когда cross-script
        # из _extract_fuzzy_ordered ложно матчит контентное русское слово seed
        # на случайный latin-токен в kw (напр. 'установка vs', 'кондиционер vs
        # инструкция' — 'vs' ошибочно принимается за транслитерацию одного из
        # слов seed). Cross-script мост оставляем для полного seed (там он
        # важен для "купить iphone" при seed "купить айфон"), но в fallback —
        # консервативнее.
        _short_seed_lemmas_set = frozenset(_short_seed_ctx['lemmas'])
    else:
        _short_seed = None
        _short_seed_ctx = None
        _short_seed_lemmas_set = frozenset()

    # ── Pre-compute sanity-check контекст ──────────────────────────────────
    # Разбираем seed один раз: cyr_content / latn_brand / latn_model.
    # Использует тот же brand_db, который уже загружен для detect_brand.
    # Если brand_db пустой — latn_brand также пустой, весь LATN идёт в
    # latn_model (строгая проверка). Это безопасная деградация.
    _sanity_ctx = _parse_seed_for_sanity(seed_ctx, brand_db) if brand_db else None
    _t_stage['seed_ctx'] = time.perf_counter() - _t

    # ── Персистентный классификатор ─────────────────────────────────────────
    _t = time.perf_counter()
    clf = _get_classifier(seed, target_country, geo_db, brand_db, retailer_db)
    _t_stage['get_classifier'] = time.perf_counter() - _t

    # ── Глобальный словарь morph-парсов для всего батча ────────────────────
    from .shared_morph import morph as _morph

    t_extract_total = 0.0
    t_classify_total = 0.0

    # Первый проход: собрать все хвосты и уникальные слова
    _t = time.perf_counter()
    _kw_tail_pairs = []
    _all_tail_words: set = set()
    _fallback_used_count = 0  # счётчик для логов
    for kw_item in keywords:
        kw = kw_item.strip() if isinstance(kw_item, str) else kw_item.get("query", "").strip()
        if not kw:
            continue
        _t0 = time.perf_counter()
        tail = extract_tail(
            kw, seed, seed_ctx=seed_ctx,
            geo_db=geo_db, target_country=target_country,
        )

        # Fallback: если основной extract не нашёл seed и критерий eligibility
        # выполнен, пробуем укороченный seed (без последнего слова). Это ловит
        # случаи когда пользователь не добавлял слово-квалификатор к основному
        # seed ("установка кондиционера в квартире" при seed="...цена"). Tail
        # при этом получается нормального вида — его обрабатывают существующие
        # детекторы (premod_adj, geo, info_intent, retailer, category_mismatch).
        #
        # Lemma-guard: принимаем fallback-tail ТОЛЬКО если ВСЕ леммы укороченного
        # seed реально присутствуют в kw как леммы (прямое lemma-совпадение,
        # без cross-script моста). Это отсекает ложные срабатывания Шага 2
        # _extract_fuzzy_ordered, где cross-script мост соединяет русское слово
        # seed с случайным latin-токеном в kw (vs, pro, new):
        #   kw="установка vs"            → cross-script: кондиционер↔vs → tail=''
        #   kw="кондиционер vs инструкция" → cross-script: установка↔vs → tail='инструкция'
        # На полном seed такие кейсы отсеиваются (там ещё 'цена' нужна), но
        # fallback с коротким seed их пропускает. Guard закрывает эту дыру.
        if tail is None and _skip_last_eligible:
            _tail_fb = extract_tail(
                kw, _short_seed, seed_ctx=_short_seed_ctx,
                geo_db=geo_db, target_country=target_country,
            )
            if _tail_fb is not None:
                # Lemma-guard: все леммы укороченного seed должны быть в kw.
                _kw_lemmas = {_morph.parse(w)[0].normal_form for w in kw.lower().split()}
                if _short_seed_lemmas_set.issubset(_kw_lemmas):
                    tail = _tail_fb
                    _fallback_used_count += 1

        t_extract_total += time.perf_counter() - _t0
        _kw_tail_pairs.append((kw_item, kw, tail))
        if tail:
            _all_tail_words.update(tail.lower().split())
    _t_stage['collect_tails_loop'] = time.perf_counter() - _t
    _t_stage['fallback_used'] = _fallback_used_count

    # Один проход morph.parse для всех уникальных слов
    _t = time.perf_counter()
    tail_parses = {w: _morph.parse(w) for w in _all_tail_words}
    _t_stage['morph_parse_batch'] = time.perf_counter() - _t

    # ── Pre-batch embeddings для category_mismatch ───────────────────────────
    # category_mismatch имеет каскад Stage 1 (chargram) → Stage 2 (MiniLM).
    # Индивидуальный inference на Stage 2 — главная причина тормозов L0.
    # Оптимизация: заранее вычисляем embeddings ОДНИМ батчем для tails,
    # которые попадут в Stage 2 (chargram в зоне 0.05-0.20). Stage 1 TRASH
    # и PASS не требуют embedding'а и в pre_batch не идут.
    #
    # Экономия: 7-8 секунд (индивидуальные вызовы MiniLM) → ~0.3 секунды
    # (один батч для всех проблемных tails).
    _t = time.perf_counter()
    _stage2_count = 0
    try:
        from .category_mismatch_detector import (
            get_category_detector, _chargram_similarity, CategoryConfig
        )
        _cm_detector = get_category_detector()
        _cm_cfg = _cm_detector.config
        # Собираем tails-кандидаты для Stage 2 (между порогами chargram)
        _seed_for_cm = seed_lower
        _stage2_tails = []
        _seen = set()
        for _, _, tail in _kw_tail_pairs:
            if not tail:
                continue
            t_clean = tail.lower().strip()
            if t_clean in _seen:
                continue
            _seen.add(t_clean)
            cg = _chargram_similarity(_seed_for_cm, t_clean, _cm_cfg.ngram_size)
            if _cm_cfg.chargram_low < cg < _cm_cfg.chargram_high:
                _stage2_tails.append(t_clean)
        _stage2_count = len(_stage2_tails)
        if _stage2_tails:
            _cm_detector.pre_batch(_stage2_tails)
    except ImportError:
        # category_mismatch_detector не доступен (stub в тестах) — пропускаем
        pass
    except Exception as e:
        logger.warning("[L0] pre_batch for category_mismatch failed: %s", e)
    _t_stage['pre_batch_cm'] = time.perf_counter() - _t
    _t_stage['_stage2_count'] = _stage2_count

    valid_keywords = []
    grey_keywords = []
    trash_keywords = []
    trace_records = []

    # Замер всего основного цикла (включая classify, UA check, NO_SEED, etc.)
    _t_loop_start = time.perf_counter()
    for kw_item, kw, tail in _kw_tail_pairs:

        # ── UA-язык → TRASH (ДО любой другой классификации) ───────────────
        # Пайплайн настроен под RU-морфологию (pymorphy3, RU детекторы).
        # UA-запросы через него не проходят корректно — нужна отдельная
        # UA-копия фильтров (отложенный проект).
        #
        # Алгоритмический критерий: буквы {і, ї, є, ґ} существуют в UA
        # и отсутствуют в RU. Одна такая буква в kw = украинский запрос.
        # Проверяется на всём kw (а не на tail), чтобы ловить случаи
        # где seed не найден из-за UA-морфологии.
        #
        # Не трогает:
        #   - чистую латиницу (all on 6, straumann) — свои детекторы
        #   - смешанный алфавит (рrice) — detect_mixed_alphabet
        if _UA_EXCLUSIVE_LETTERS.intersection(kw):
            trash_keywords.append(kw_item)
            trace_records.append({
                "keyword": kw,
                "tail": tail,
                "label": "TRASH",
                "decided_by": "l0",
                "reason": "Не русский язык: UA-буквы (і/ї/є/ґ) — нужна UA-копия пайплайна",
                "signals": ["-wrong_language"],
            })
            continue

        # NO_SEED: extractor не нашёл seed в kw.
        # Решение label'а зависит от ДВУХ алгоритмических условий:
        #
        # Условие A — "wrong model" сценарий.
        # Если seed содержит целые числовые токены (например "купить айфон 16"
        # → seed_numbers = {"16"}), и в kw есть числа, но НИ ОДНО из них не
        # совпадает с seed_numbers — значит пользователь ищет ДРУГУЮ модель
        # или версию, семантически несовместимую с seed. Примеры:
        #   • "купить айфон 14 киев"      → 14 ∉ {16} → TRASH
        #   • "iphone 6 16gb"             → 6 ∉ {16} → TRASH (16gb не токен)
        #   • "купить айфон 9 плюс"       → 9 ∉ {16} → TRASH
        # Cross-niche защита: для сидов без чисел ("доставка цветов",
        # "имплантация зубов") seed_numbers пустой — условие никогда не
        # срабатывает. Для "аккумулятор на скутер" seed_numbers тоже пустой.
        #
        # Условие B — украинский язык без эксклюзивных букв.
        # UA-буквы {і, ї, є, ґ} уже ловит verify выше. Но есть украинские
        # слова на той же кириллице что и русский: "купити", "замовити",
        # "огляд", "розстрочку" и т.д. — они не отличаются от RU буквами,
        # но это всё равно украинский пайплайн.
        #
        # Если ни A ни B не сработали — оставляем GREY как раньше
        # (возможные валидные синонимы, перестановки, частичные совпадения).
        if tail is None:
            _kw_lower = kw.lower()
            _kw_words_set = set(_kw_lower.split())

            # Условие B — проверяем первым, дешевле (один set intersection)
            _ua_hit = _kw_words_set & _UA_AMBIGUOUS_WORDS
            if _ua_hit:
                trash_keywords.append(kw_item)
                trace_records.append({
                    "keyword": kw,
                    "tail": None,
                    "label": "TRASH",
                    "decided_by": "l0",
                    "reason": (
                        f"UA-слово в kw ('{sorted(_ua_hit)[0]}') — "
                        "нужна UA-копия пайплайна"
                    ),
                    "signals": ["-wrong_language"],
                })
                continue

            # Условие A — проверяем если seed содержит числа
            if _seed_numbers:
                _kw_numbers = _NUMBER_TOKEN_RE.findall(_kw_lower)
                # Есть числа в kw и ни одно не совпадает с seed-числом
                if _kw_numbers and not any(n in _seed_numbers for n in _kw_numbers):
                    trash_keywords.append(kw_item)
                    trace_records.append({
                        "keyword": kw,
                        "tail": None,
                        "label": "TRASH",
                        "decided_by": "l0",
                        "reason": (
                            f"seed не найден + другие числа "
                            f"{sorted(set(_kw_numbers))} "
                            f"≠ seed-числа {sorted(_seed_numbers)} → TRASH "
                            f"(wrong model/version)"
                        ),
                        "signals": ["-wrong_model"],
                    })
                    continue

            # Fallback: seed не найден, но ни A, ни B не сработали → GREY
            # Могут быть валидные синонимы, перестановки, частичные совпадения.
            grey_keywords.append(kw_item)
            trace_records.append({
                "keyword": kw,
                "tail": None,
                "label": "GREY",
                "decided_by": "l0",
                "reason": "seed не найден → GREY",
                "signals": [],
            })
            continue

        # Пустой хвост = запрос совпадает с seed → VALID
        if not tail:
            valid_keywords.append(kw_item)
            trace_records.append({
                "keyword": kw,
                "tail": "",
                "label": "VALID",
                "decided_by": "l0",
                "reason": "запрос = seed",
                "signals": ["exact_seed"],
            })
            continue

        # ── classify с глобальным tail_parses ───────────────────────────────
        t0 = time.perf_counter()
        r = clf.classify(tail, tail_parses=tail_parses, kw=kw)
        t_classify_total += time.perf_counter() - t0

        label = r["label"]
        all_signals = r["positive_signals"] + [f"-{s}" for s in r["negative_signals"]]

        trace_record = {
            "keyword": kw,
            "tail": tail,
            "label": label,
            "decided_by": "l0",
            "reason": "; ".join(r["reasons"][:3]),
            "signals": all_signals,
            "confidence": r["confidence"],
        }
        trace_records.append(trace_record)

        if label == "VALID":
            valid_keywords.append(kw_item)
        elif label == "TRASH":
            trash_keywords.append(kw_item)
        else:  # GREY
            grey_keywords.append(kw_item)

    # ── Обновляем result ─────────────────────────────────────────────────────
    _t_stage['main_loop'] = time.perf_counter() - _t_loop_start

    # ── SANITY CHECK: демотируем ложные VALID в GREY ────────────────────────
    # Для LATN-heavy сидов (samsung galaxy s21 ремонт, ремонт macbook pro и т.п.)
    # tail_extractor пропускает через partial_match/cross-script ключи, в которых
    # нет обязательных content-слов seed. Проверяем каждый VALID ключ и если он
    # не соответствует правилам (см. _sanity_check_valid) — переводим в GREY,
    # где L2 через PMI/KNN попробует реабилитировать семантически близкие.
    # Cross-niche: на cyr-only seeds (цветы/зубы/кондиционер) правило либо
    # пропускает всё (len>=3 soft), либо требует все леммы (len==2 строго).
    _t_sanity_start = time.perf_counter()
    _sanity_demoted_count = 0
    if _sanity_ctx is not None:
        _kept_valid = []
        for kw_item in valid_keywords:
            kw = kw_item if isinstance(kw_item, str) else kw_item.get("query", "")
            reason = _sanity_check_valid(kw, _sanity_ctx, brand_db, _morph)
            if reason is None:
                _kept_valid.append(kw_item)
                continue
            # Демотируем: добавляем в GREY, обновляем trace_record
            grey_keywords.append(kw_item)
            _sanity_demoted_count += 1
            for tr in trace_records:
                if tr.get("keyword") == kw and tr.get("label") == "VALID":
                    tr["label"] = "GREY"
                    tr["decided_by"] = "l0_sanity"
                    tr["reason"] = f"sanity_mismatch: {reason}"
                    tr["signals"] = tr.get("signals", []) + ["-sanity_mismatch"]
                    break
        valid_keywords = _kept_valid
    _t_stage['sanity_check'] = time.perf_counter() - _t_sanity_start
    _t_stage['sanity_demoted'] = _sanity_demoted_count

    result["keywords"] = valid_keywords
    result["count"] = len(valid_keywords)

    result["keywords_grey"] = grey_keywords
    result["keywords_grey_count"] = len(grey_keywords)

    # TRASH → якоря
    existing_anchors = result.get("anchors", [])
    for kw_item in trash_keywords:
        kw = kw_item if isinstance(kw_item, str) else kw_item.get("query", "")
        existing_anchors.append(kw)
    result["anchors"] = existing_anchors
    result["anchors_count"] = len(existing_anchors)

    result["_l0_trace"] = trace_records

    # ── Сохраняем диагностику ────────────────────────────────────────────────
    _t = time.perf_counter()
    try:
        import json as _json
        diag = {
            "seed": seed,
            "target_country": target_country,
            "stats": {
                "total": len(trace_records),
                "valid": len(valid_keywords),
                "trash": len(trash_keywords),
                "grey": len(grey_keywords),
                "no_seed": sum(1 for r in trace_records if r.get("tail") is None),
            },
            "trace": trace_records,
        }
        with open("l0_diagnostic.json", "w", encoding="utf-8") as f:
            _json.dump(diag, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning("[L0] Failed to save diagnostic: %s", e)
    _t_stage['save_diagnostic'] = time.perf_counter() - _t

    # ── Тайминги ─────────────────────────────────────────────────────────────
    t_l0_total = time.perf_counter() - t_l0_start
    _add_timings(result, t_extract_total, t_classify_total, t_l0_total)

    # Детальный лог этапов — видно где 8 секунд прячется
    _stages_str = " | ".join(f"{k}={v:.3f}s" if isinstance(v, float) else f"{k}={v}"
                              for k, v in _t_stage.items())
    logger.info("[L0_STAGES] %s", _stages_str)

    # ── Лог итогов ──────────────────────────────────────────────────────────
    total = len(trace_records)
    v = len(valid_keywords)
    t = len(trash_keywords)
    g = len(grey_keywords)

    logger.info(
        "[L0] seed='%s' | total=%d | VALID=%d (%d%%) | TRASH=%d (%d%%) | GREY=%d (%d%%)",
        seed, total,
        v, v * 100 // total if total else 0,
        t, t * 100 // total if total else 0,
        g, g * 100 // total if total else 0,
    )
    logger.info(
        "[L0_PROFILE] n=%d | extract_tail=%.3fs | classify=%.3fs | total=%.3fs",
        total, t_extract_total, t_classify_total, t_l0_total,
    )

    # ── Per-detector тайминги (топ по убыванию) ─────────────────────────────
    if clf.detector_timings:
        sorted_det = sorted(clf.detector_timings.items(), key=lambda x: -x[1])
        logger.info(
            "[L0_DETECTORS] %s",
            " | ".join(f"{k}={v:.3f}s" for k, v in sorted_det)
        )
        clf.detector_timings.clear()  # сброс после батча

    return result


def _add_timings(result: dict, t_extract: float, t_classify: float, t_total: float):
    """Добавляет L0-тайминги к _filter_timings result'а."""
    if "_filter_timings" not in result:
        result["_filter_timings"] = {}
    result["_filter_timings"]["l0"] = {
        "extract_tail": round(t_extract, 4),
        "classify": round(t_classify, 4),
        "total": round(t_total, 4),
    }
