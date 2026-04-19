"""
TailFunctionClassifier v2 — исправленный классификатор.

Изменения относительно v1:
1. Пустой хвост = VALID (запрос = seed), не TRASH
2. Арбитраж с ВЕСАМИ сигналов (гео/бренд > эвристик)
3. Добавлен detect_noise_suffix (12-й детектор)
4. Приоритет: позитивный сигнал из БД перевешивает эвристический негатив
"""

import time
from typing import Dict, List, Tuple, Set
from .function_detectors import (
    detect_geo, detect_brand, detect_commerce, detect_reputation,
    detect_location, detect_action, detect_time,
    detect_fragment, detect_meta,
    detect_dangling, detect_duplicate_words, detect_brand_collision,
    detect_noise_suffix, detect_type_specifier,
    detect_seed_echo, detect_broken_grammar,
    detect_number_hijack, detect_short_garbage,
    # Новые детекторы
    detect_contacts,
    detect_technical_garbage, detect_mixed_alphabet, detect_standalone_number,
    detect_verb_modifier, detect_conjunctive_extension,
    detect_prepositional_modifier,
    # Новые мягкие детекторы
    detect_truncated_geo, detect_truncated_geo_fast, detect_orphan_genitive, detect_single_infinitive,
    detect_foreign_geo,
    # Info-intent detector — информационные/research/how-to запросы как positive
    detect_info_intent,
    # Premod/Postmod adjective — позиционные детекторы согласованных модификаторов
    detect_premod_adjective, detect_postmod_adjective,
    # District guard — районы чужих городов при городском seed
    detect_wrong_district, detect_unknown_district,
    # Product spec — технические спецификации товара (12в, 4т, 220 ом)
    detect_product_spec,
    # Retailer — онлайн-магазины и маркетплейсы (Rozetka, Amazon, OLX)
    detect_retailer,
    # Model variant — короткие латинские модификаторы модели/единиц (pro, ultra, gb, hz)
    detect_model_variant,
    # Helpers
    _is_service_seed, COMMERCE_INFN_LEMMAS,
)

# Category mismatch detector (использует embeddings, ленивая загрузка)
try:
    from .category_mismatch_detector import detect_category_mismatch
    CATEGORY_MISMATCH_AVAILABLE = True
except ImportError:
    CATEGORY_MISMATCH_AVAILABLE = False
    def detect_category_mismatch(seed, tail):
        return (False, "")

from .shared_morph import morph

# Накопленные тайминги по детекторам — суммируются за весь батч.
# Читается и сбрасывается из l0_filter после каждого батча.


# Веса сигналов: чем выше, тем сильнее влияние на решение
SIGNAL_WEIGHTS = {
    # Позитивные — опираются на БАЗЫ ДАННЫХ (высокая надёжность)
    'geo':        1.0,    # город из 65k базы — почти гарантия
    'brand':      1.0,    # бренд из проверенной базы
    
    # Позитивные — опираются на ПАТТЕРНЫ (средняя надёжность)
    'commerce':   0.8,
    'reputation': 0.8,
    'location':   0.9,    # "рядом" — типичный поисковый паттерн
    'action':     0.7,
    'time':       0.8,    # "круглосуточно", "срочно" — универсальный сигнал
    'type_spec':  0.85,   # согласование с seed — надёжный лингвистический сигнал
    'contacts':   0.85,   # "телефон", "адрес" — конкретный интент
    'verb_modifier': 0.85,  # наречие при глаголе seed — лингвистически надёжный
    'conjunctive': 0.8,    # "и подарков" — расширение запроса
    'prep_modifier': 0.85,  # "при болях", "после еды" — лингвистически надёжный (PREP+case)
    'info_intent': 0.9,     # информационный/research/how-to/troubleshooting запрос — структурный сигнал (вопросительные слова, "это" на конце, "не + VERB")
    'premod_adj':  0.75,    # ADJF/PRTF перед seed, согласованное с ним — модификатор типа ("базальная имплантация")
    'postmod_adj': 0.75,    # ADJF/PRTF после seed, согласованное с ним — модификатор вида ("имплантация жевательных зубов")
    
    # Негативные — ЭВРИСТИКИ (могут ошибаться)
    'fragment':        0.8,
    'meta':            0.9,    # мета-вопросы довольно надёжно ловятся
    'dangling':        0.6,    # может ошибаться (pymorphy не идеален)
    'duplicate':       0.9,    # дубликат почти всегда мусор
    'brand_collision': 0.5,    # спорный сигнал, низкий вес
    'noise_suffix':    0.7,
    'seed_echo':       0.9,    # повтор слова из seed — почти всегда мусор
    'broken_grammar':  0.8,    # сломанное управление предлога
    'number_hijack':   0.85,   # генитив-паразит на числе из seed
    'short_garbage':   0.9,    # бессмысленные 1-2 символьные токены
    'tech_garbage':    0.95,   # email/URL/телефон — почти 100% мусор
    'mixed_alpha':     0.9,    # смешанные алфавиты
    'standalone_num':  0.7,    # голое число — может ошибиться (модели)
    'category_mismatch': 0.5,  # мягкий — embeddings ненадёжно классифицируют категории
    'truncated_geo':     0.85,  # обрезанный составной город — довольно надёжно
    'foreign_geo':       0.95,  # город/страна из чужого региона — очень надёжно (geo_db)
    'orphan_genitive':   0.5,   # мягкий — может быть валидным ("фильтров")
    'single_infinitive': 0.5,   # мягкий — может быть валидным интентом
    'intent_mismatch':   0.9,   # информационный seed + коммерческий tail — надёжный конфликт
    'wrong_district':    0.95,  # известный район ЧУЖОГО города при городском seed — hard block
    'unknown_district':  0.4,   # мягкий — район, которого нет в базе, но структура валидна
    'product_spec':      0.85,  # технические параметры товара (12 в, 4т, 220 ом) — надёжный сигнал
    'retailer':          0.85,  # упоминание ритейлера/маркетплейса — сильный коммерческий интент
    'model_variant':     0.75,  # короткий латинский модификатор (pro/ultra/gb/hz) — структурный сигнал для англ. моделей и ед. измерения
}

# Мягкие негативные сигналы — эвристики с высокой вероятностью ошибки.
# Если ВСЕ негативные сигналы из этого множества → максимум GREY, никогда TRASH.
# Два мягких сигнала вместе не должны давать TRASH (напр. category_mismatch + orphan_genitive).
_SOFT_NEGATIVES = frozenset({
    'category_mismatch',  # chargram — не знает семантику частей/типов объекта
    'orphan_genitive',    # "ремонт двигателей пылесосов" — валидная конструкция
    'brand_collision',    # спорный сигнал
    'unknown_district',   # 'X район' валидной структуры, нет в базе — L3 разрулит
    'standalone_num',     # голое число ('12', '150') — может быть товарный параметр, L3 решит
    'truncated_geo',      # обрезок города — часто ложные на товарных терминах
                          # (pro/корпус/макс — продуктовые суффиксы, не города)
    # single_infinitive намеренно НЕ здесь:
    # в сочетании с category_mismatch (chargram=0) = голый несвязанный инфинитив → TRASH
})


# ============================================================
# Константы уровня модуля.
# Вычисляются ОДИН РАЗ при импорте — не пересоздаются на каждый вызов classify().
# ============================================================

# Наборы для intent_mismatch check — тоже уровень модуля
_GEO_COMPATIBLE_INTERROGATIVES = frozenset({'где', 'куда', 'откуда'})
_GEO_INCOMPATIBLE_INTERROGATIVES = frozenset({
    'как', 'почему', 'зачем', 'когда', 'что', 'кто', 'чем',
    'чего', 'какой', 'какая', 'какое', 'какие',
})
_COMMERCE_INCOMPATIBLE = (
    _GEO_COMPATIBLE_INTERROGATIVES | _GEO_INCOMPATIBLE_INTERROGATIVES |
    frozenset({'можно', 'нужно', 'стоит', 'сколько'})
)

# Сильные позитивные сигналы — если есть хотя бы один,
# category_mismatch пропускается (Stage 0 short-circuit).
# Эти детекторы уже надёжно валидировали хвост → дорогой embed не нужен.
# Сильные позитивные сигналы — если есть хотя бы один,
# category_mismatch пропускается (Stage 0 short-circuit).
# Любой позитивный детектор уже подтвердил что хвост содержит валидный интент —
# дорогая semantic проверка не даёт дополнительной точности.
# category_mismatch — мягкий safety-net для хвостов БЕЗ позитивов.
_STRONG_POSITIVES_SKIP_MISMATCH = frozenset({
    # Database / structural
    'geo', 'brand',
    # Pattern-based strong positives
    'location', 'contacts', 'time',
    'verb_modifier', 'prep_modifier', 'conjunctive', 'info_intent',
    'premod_adj', 'postmod_adj',
    # Commerce / reputation / action / type — если сработали, интент подтверждён
    'commerce', 'reputation', 'action', 'type_spec',
    # Product spec — технические параметры на товарном seed уже валидируют интент
    'product_spec',
    # Retailer — ритейлер/маркетплейс в хвосте = конкретный коммерческий интент
    'retailer',
})


class TailFunctionClassifier:
    """Классификатор хвостов на основе детекторов функций."""
    
    def __init__(self, geo_db: Set[str], brand_db: Set[str], seed: str = "ремонт пылесосов", target_country: str = "ua", retailer_db: Set[str] = None):
        self.geo_db = geo_db
        self.brand_db = brand_db
        # retailer_db опциональный — если None, детектор не будет ничего ловить.
        # Загружать его должен вызывающий код через databases.load_retailers_db().
        self.retailer_db = retailer_db or set()
        self.seed = seed
        self.target_country = target_country

        # Pre-computed seed data — вычисляется один раз при создании классификатора,
        # а не на каждый вызов classify()
        self._seed_words_lower = seed.lower().split()
        self._seed_first_word = self._seed_words_lower[0] if self._seed_words_lower else ''
        self._seed_first_in_geo_incompatible = self._seed_first_word in _GEO_INCOMPATIBLE_INTERROGATIVES
        self._seed_first_in_commerce_incompatible = self._seed_first_word in _COMMERCE_INCOMPATIBLE
        # Pre-computed: seed сервисный (услуга/процесс) или товарный?
        # Используется в detect_single_infinitive и как флаг для commerce-infn positive.
        self._seed_is_service = _is_service_seed(seed) if seed else True
        # Тайминги детекторов — накапливаются за батч, сбрасываются из l0_filter
        self.detector_timings: Dict[str, float] = {}
        # Индекс для truncated_geo — строится один раз при создании классификатора
        from .function_detectors import _build_truncated_geo_index
        self._truncated_geo_index = _build_truncated_geo_index(geo_db) if geo_db else {}
    
    def classify(self, tail: str, tail_parses: dict = None, kw: str = "") -> Dict:
        """
        Классифицирует хвост запроса.

        tail_parses: глобальный словарь {слово → morph.parse(слово)} для всего батча.
        Строится один раз в l0_filter.py и передаётся сюда.
        Детекторы используют его вместо независимых вызовов morph.parse.

        kw: оригинальный ключевой запрос (kw=seed+tail в разных позициях).
        Нужен для позиционных детекторов (premod/postmod), которые определяют
        позицию tail относительно seed. Если kw не передан — позиционные
        детекторы просто не срабатывают, остальные работают как обычно.

        Returns:
            {
                'label': 'VALID' | 'TRASH' | 'GREY',
                'positive_signals': [...],
                'negative_signals': [...],
                'reasons': [...],
                'confidence': float,
                'positive_score': float,
                'negative_score': float,
            }
        """
        # ===== ПУСТОЙ ХВОСТ = запрос совпадает с seed → VALID =====
        if not tail or not tail.strip():
            return {
                'label': 'VALID',
                'positive_signals': ['exact_seed'],
                'negative_signals': [],
                'reasons': ['Запрос совпадает с seed — валидный поисковый запрос'],
                'confidence': 0.95,
                'positive_score': 1.0,
                'negative_score': 0.0,
            }

        # tp — ссылка на глобальный словарь батча (или None → детекторы сами парсят)
        tp = tail_parses

        positive_signals = []
        negative_signals = []
        reasons = []

        # ===== ПОЗИТИВНЫЕ ДЕТЕКТОРЫ =====
        detectors_positive = [
            ('geo',           lambda: detect_geo(tail, self.geo_db, self.target_country, tp=tp)),
            ('brand',         lambda: detect_brand(tail, self.brand_db, tp=tp)),
            ('commerce',      lambda: detect_commerce(tail, tp=tp)),
            ('reputation',    lambda: detect_reputation(tail, tp=tp)),
            ('location',      lambda: detect_location(tail, tp=tp)),
            ('action',        lambda: detect_action(tail, tp=tp)),
            ('time',          lambda: detect_time(tail, tp=tp)),
            ('type_spec',     lambda: detect_type_specifier(tail, self.seed, tp=tp)),
            ('contacts',      lambda: detect_contacts(tail, tp=tp)),
            ('verb_modifier', lambda: detect_verb_modifier(tail, self.seed, tp=tp)),
            ('conjunctive',   lambda: detect_conjunctive_extension(tail, self.seed, tp=tp)),
            ('prep_modifier', lambda: detect_prepositional_modifier(tail, self.seed, tp=tp)),
            ('info_intent',   lambda: detect_info_intent(tail, self.seed, tp=tp)),
            ('premod_adj',    lambda: detect_premod_adjective(tail, self.seed, kw, tp=tp)),
            ('postmod_adj',   lambda: detect_postmod_adjective(tail, self.seed, kw, tp=tp)),
            ('product_spec',  lambda: detect_product_spec(tail, self.seed, tp=tp)),
            ('retailer',      lambda: detect_retailer(tail, self.retailer_db, tp=tp)),
            ('model_variant', lambda: detect_model_variant(tail, self.seed, tp=tp)),
        ]

        for signal_name, detector in detectors_positive:
            _t0 = time.perf_counter()
            detected, reason = detector()
            self.detector_timings[signal_name] = self.detector_timings.get(signal_name, 0.0) + (time.perf_counter() - _t0)
            if detected:
                positive_signals.append(signal_name)
                reasons.append(f"✅ {reason}")

        # Commerce-инфинитив на ТОВАРНОМ seed = валидный purchase intent.
        # detect_commerce держит "купить/заказать" как weak (нужен контекст из 2+ слов).
        # Здесь добавляем positive для единичного commerce-инфинитива ТОЛЬКО
        # когда seed явно товарный (не сервис), иначе "ремонт пылесосов купить"
        # ошибочно станет VALID.
        #
        # ТОЛЬКО РУССКИЙ: lemma-match через pymorphy3. Украинские инфинитивы
        # pymorphy3 не распознаёт как INFN → не попадут в этот блок → отдельный
        # UA-пайплайн обрабатывает их своей морфологией.
        if 'commerce' not in positive_signals and not self._seed_is_service:
            _tail_words = tail.lower().split()
            if len(_tail_words) == 1:
                _w = _tail_words[0]
                _p = (tail_parses.get(_w, None) if tail_parses else None)
                if _p is None:
                    _p = morph.parse(_w)
                if _p and _p[0].tag.POS == 'INFN' and _p[0].normal_form in COMMERCE_INFN_LEMMAS:
                    positive_signals.append('commerce')
                    reasons.append(f"✅ Коммерческий intent (товарный seed): '{_p[0].normal_form}'")

        # ===== НЕГАТИВНЫЕ ДЕТЕКТОРЫ =====
        detectors_negative = [
            ('fragment',        lambda: detect_fragment(tail, self.seed, tp=tp, kw=kw)),
            ('meta',            lambda: detect_meta(tail, self.seed, tp=tp)),
            ('dangling',        lambda: detect_dangling(tail, self.seed, self.geo_db, tp=tp)),
            ('duplicate',       lambda: detect_duplicate_words(tail, tp=tp)),
            ('brand_collision', lambda: detect_brand_collision(tail, self.brand_db, tp=tp)),
            ('noise_suffix',    lambda: detect_noise_suffix(tail, tp=tp)),
            ('seed_echo',       lambda: detect_seed_echo(tail, self.seed, tp=tp)),
            ('broken_grammar',  lambda: detect_broken_grammar(tail, tp=tp)),
            ('number_hijack',   lambda: detect_number_hijack(tail, self.seed, tp=tp)),
            ('short_garbage',   lambda: detect_short_garbage(tail, tp=tp)),
            ('tech_garbage',    lambda: detect_technical_garbage(tail, tp=tp)),
            ('mixed_alpha',     lambda: detect_mixed_alphabet(tail, tp=tp)),
            ('standalone_num',  lambda: detect_standalone_number(tail, self.seed, tp=tp)),
            ('category_mismatch', lambda: detect_category_mismatch(self.seed, tail)),
            ('truncated_geo',   lambda: detect_truncated_geo_fast(tail, self.geo_db, self._truncated_geo_index, tp=tp)),
            ('foreign_geo',     lambda: detect_foreign_geo(tail, self.geo_db, self.target_country, tp=tp)),
            ('orphan_genitive', lambda: detect_orphan_genitive(tail, self.seed, tp=tp)),
            ('single_infinitive', lambda: detect_single_infinitive(tail, self.seed, tp=tp, seed_is_service=self._seed_is_service)),
            # District guard — районы чужих городов при городском seed.
            # wrong_district = HARD: биграмма 'X район' есть в базе, city ≠ seed_city.
            # unknown_district = SOFT: биграмма валидна структурно, но не в базе → GREY.
            ('wrong_district',   lambda: detect_wrong_district(tail, self.seed, tp=tp)),
            ('unknown_district', lambda: detect_unknown_district(tail, self.seed, tp=tp)),
        ]
        
        for signal_name, detector in detectors_negative:
            # ── Stage 0: short-circuit для category_mismatch ────────────────
            # Если сильный позитивный сигнал уже подтвердил хвост (geo, brand,
            # location и др.) — дорогой semantic детектор не запускается.
            # Эти сигналы надёжнее и специализированнее чем общий mismatch.
            if signal_name == 'category_mismatch':
                if set(positive_signals) & _STRONG_POSITIVES_SKIP_MISMATCH:
                    continue
            _t0 = time.perf_counter()
            detected, reason = detector()
            self.detector_timings[signal_name] = self.detector_timings.get(signal_name, 0.0) + (time.perf_counter() - _t0)
            if detected:
                negative_signals.append(signal_name)
                reasons.append(f"❌ {reason}")
        
        # ===== ПРОВЕРКА КОНФЛИКТА ИНТЕНТОВ =====
        # Два типа конфликтов:
        # 1. Информационный seed + коммерческий tail: "как принимать нимесил цена"
        # 2. Гео-несовместимый seed + гео tail: "как принимать нимесил киев"
        #    НО: "где починить пылесос киев" → гео валидно (вопрос о месте)
        if positive_signals:
            # Гео-совместимые вопросы: ответ — МЕСТО → гео в tail валидно
            # Гео-несовместимые: ответ — СПОСОБ/ПРИЧИНА → гео в tail мусор
            # Commerce несовместимы с ЛЮБЫМ вопросительным seed
            # Используем pre-computed флаги из __init__ (не пересчитываем split/set-lookup)

            seed_first_word = self._seed_first_word

            # Commerce в хвосте информационного seed = конфликт
            if self._seed_first_in_commerce_incompatible and 'commerce' in positive_signals:
                negative_signals.append('intent_mismatch')
                reasons.append(f"⚠️ Конфликт интентов: seed информационный ('{seed_first_word}'), tail коммерческий")

            # Гео в хвосте гео-несовместимого seed = конфликт
            if self._seed_first_in_geo_incompatible and 'geo' in positive_signals:
                negative_signals.append('intent_mismatch')
                reasons.append(f"⚠️ Конфликт интентов: seed '{seed_first_word}' (не о месте), tail содержит гео")

        # ===== DISTRICT GUARD — подавление дублирующего location =====
        # Если сработал wrong_district или unknown_district, они ловили ту же
        # биграмму 'X район', которая даёт +location через тупой паттерн-матч
        # на слово "район". Этот +location информационно дублирует сигнал от
        # district-guard и в арбитраже неоправданно защищает ключ от мягкого
        # unknown_district (location=0.9 > unknown_district=0.4).
        #
        # Подавляем location когда он пришёл именно от district-биграммы:
        # детектор district уже обработал биграмму и вынес свой вердикт.
        # Другие location-паттерны (рядом, на дому, 'в моём районе') не
        # триггерят district-guard, поэтому безопасны.
        if ('wrong_district' in negative_signals or
                'unknown_district' in negative_signals):
            if 'location' in positive_signals:
                positive_signals.remove('location')
                reasons.append(
                    "⚠ location подавлен: сигнал дублирует district-guard"
                )

        # ===== PRODUCT SPEC — подавление дублирующих негативов =====
        # Если сработал product_spec, он валидировал короткий технический
        # tail ('12 в', '4т', '220ом'). На таких паттернах некоторые
        # негативные детекторы ошибочно срабатывают:
        #   — detect_fragment видит 'в' как PREP на конце ('12 в')
        #   — detect_short_garbage видит '4т' как неизвестный короткий токен
        #   — detect_standalone_number видит '12' как голое число
        #   — detect_broken_grammar видит некорректное согласование
        # Все эти сигналы ловят ту же цифро-буквенную структуру, которую
        # product_spec уже опознал как валидную спецификацию. Их негатив
        # информационно дублирующий и неоправданно ведёт ключ в GREY.
        #
        # Подавляем только когда product_spec сработал. Другие tail'ы
        # (обычные короткие мусорные 'аб', 'кх', голые числа без спека
        # паттерна) проходят через эти детекторы нормально.
        if 'product_spec' in positive_signals:
            _spec_suppressed = {
                'fragment', 'short_garbage', 'standalone_num',
                'broken_grammar',
            }
            _removed = [s for s in negative_signals if s in _spec_suppressed]
            if _removed:
                negative_signals[:] = [
                    s for s in negative_signals if s not in _spec_suppressed
                ]
                reasons.append(
                    f"⚠ подавлены дубликаты product_spec: {_removed}"
                )

        # ===== АРБИТРАЖ С ВЕСАМИ =====
        label, confidence, pos_score, neg_score = self._arbitrate(
            positive_signals, negative_signals
        )
        
        return {
            'label': label,
            'positive_signals': positive_signals,
            'negative_signals': negative_signals,
            'reasons': reasons,
            'confidence': confidence,
            'positive_score': pos_score,
            'negative_score': neg_score,
        }
    
    def _arbitrate(
        self, positive: List[str], negative: List[str]
    ) -> Tuple[str, float, float, float]:
        """
        Арбитраж с весами.
        
        Ключевая логика:
        - Сигналы из БД (geo, brand) перевешивают эвристики (dangling)
        - При конфликте: если есть geo/brand → скорее VALID
        - Без сигналов вообще → GREY
        
        Returns:
            (label, confidence, positive_score, negative_score)
        """
        pos_score = sum(SIGNAL_WEIGHTS.get(s, 0.5) for s in positive)
        neg_score = sum(SIGNAL_WEIGHTS.get(s, 0.5) for s in negative)
        
        has_positive = len(positive) > 0
        has_negative = len(negative) > 0
        
        # --- Случай 1: Только позитивные ---
        if has_positive and not has_negative:
            confidence = min(0.85 + pos_score * 0.05, 0.99)
            return 'VALID', confidence, pos_score, neg_score
        
        # --- Случай 2: Только негативные ---
        if has_negative and not has_positive:
            # Если ВСЕ негативные — мягкие эвристики, максимум GREY.
            # Два мягких вместе (category_mismatch + orphan_genitive = 1.0)
            # не должны давать TRASH — финальное решение за L3.
            if all(s in _SOFT_NEGATIVES for s in negative):
                return 'GREY', 0.4, pos_score, neg_score
            # Жёсткий TRASH только при наличии хотя бы одного не-мягкого сигнала
            # и neg_score >= 0.6
            if neg_score < 0.6:
                return 'GREY', 0.4, pos_score, neg_score
            confidence = min(0.85 + neg_score * 0.05, 0.99)
            return 'TRASH', confidence, pos_score, neg_score
        
        # --- Случай 3: Конфликт ---
        if has_positive and has_negative:
            # Абсолютные блокаторы — жёсткие вердикты, которые перевешивают
            # любые позитивы. foreign_geo: если в kw чужое гео при target=UA,
            # ключ не может быть "нашим" клиентом никакими коммерческими/info
            # сигналами. Это вердикт о неприменимости, не об интенте.
            # wrong_district: в хвосте известный район чужого города при
            # городском seed — аналогичная неприменимость таргетинга.
            absolute_blockers = {'foreign_geo', 'wrong_district'}
            if set(negative) & absolute_blockers:
                return 'TRASH', 0.9, pos_score, neg_score
            
            # Приоритет БД-сигналов: если geo или brand подтверждён,
            # а негатив — только эвристика, доверяем БД
            db_signals = {'geo', 'brand', 'verb_modifier', 'conjunctive',
                          'prep_modifier', 'info_intent',
                          'premod_adj', 'postmod_adj'}
            has_db_positive = bool(set(positive) & db_signals)
            
            # Жёсткие негативные (почти всегда правы)
            hard_negatives = {'duplicate', 'meta', 'tech_garbage', 'mixed_alpha', 'foreign_geo', 'intent_mismatch'}
            has_hard_negative = bool(set(negative) & hard_negatives)
            
            # ─── Info-intent guard ───────────────────────────────────────────
            # Если info_intent (структурный маркер реального пользовательского
            # запроса: вопросительное слово, "это" на конце, "не + VERB") в позитивах,
            # и ВСЕ негативы принадлежат whitelist мягких definition-эвристик —
            # это валидный info/research/how-to/troubleshooting запрос.
            #
            # Semantic Agent ищет максимально широкий пул ключей любой направленности:
            # коммерция, research, definition, how-to. Такие запросы — реальные
            # вопросы реальных людей, которых Google выдаёт в autocomplete.
            #
            # Whitelist негативов — только мягкие эвристики, которые на info-запросах
            # часто ложно срабатывают:
            #   meta — помечает "что такое/чем отличается/как называется" (это ВАЛИДНЫЕ паттерны)
            #   fragment — помечает "X это" и "не VERB" как обрывок (это НЕ обрывки на info-запросах)
            #   dangling — "чем опасна" имеет висячее ADJ (нормально для info)
            #   category_mismatch — chargram не умеет семантически проверять вопросы
            #
            # Жёсткие негативы (duplicate, tech_garbage, mixed_alpha, foreign_geo,
            # intent_mismatch) в whitelist НЕ входят — если они сработали, это
            # реальный мусор несмотря на info-маркер.
            if 'info_intent' in positive:
                _info_whitelist = {'meta', 'fragment', 'dangling', 'category_mismatch'}
                if all(s in _info_whitelist for s in negative):
                    confidence = 0.7
                    return 'VALID', confidence, pos_score, neg_score
            # ─────────────────────────────────────────────────────────────────
            
            if has_db_positive and not has_hard_negative:
                # БД говорит VALID, эвристика говорит TRASH → доверяем БД
                confidence = 0.75
                return 'VALID', confidence, pos_score, neg_score
            
            if has_hard_negative:
                # Мета-вопрос или дублирование → даже бренд не спасает
                if pos_score > neg_score * 1.5:
                    return 'GREY', 0.3, pos_score, neg_score
                return 'TRASH', 0.65, pos_score, neg_score
            
            # Обычный конфликт — по весам
            if pos_score > neg_score * 1.2:
                return 'VALID', 0.6, pos_score, neg_score
            elif neg_score > pos_score * 1.2:
                return 'TRASH', 0.6, pos_score, neg_score
            else:
                return 'GREY', 0.3, pos_score, neg_score
        
        # --- Случай 4: Ничего не сработало ---
        return 'GREY', 0.5, pos_score, neg_score


# ==================== ТЕСТЫ ====================

def run_tests():
    """Тестирование классификатора."""
    
    print("🧪 ТЕСТИРОВАНИЕ TailFunctionClassifier v2\n")
    
    from databases import load_geonames_db, load_brands_db
    
    print("Загрузка баз данных...")
    geo_db = load_geonames_db()
    brand_db = load_brands_db()
    print(f"✅ Загружено: {len(geo_db)} городов, {len(brand_db)} брендов\n")
    
    classifier = TailFunctionClassifier(geo_db, brand_db, seed="ремонт пылесосов")
    
    # Тестовые кейсы: (tail, expected_label, description)
    test_cases = [
        # VALID — позитивные сигналы
        ("",              "VALID", "Пустой хвост (= seed)"),
        ("киев",          "VALID", "Город UA"),
        ("samsung",       "VALID", "Бренд"),
        ("цена",          "VALID", "Коммерция"),
        ("отзывы",        "VALID", "Репутация"),
        ("рядом",         "VALID", "Локация"),
        ("своими руками",  "VALID", "Действие"),
        ("форум",         "VALID", "Репутация (форум)"),
        ("услуги",        "GREY",  "Нет позитивного детектора → GREY"),
        ("работа",        "GREY",  "Нет позитивного детектора → GREY"),
        ("на дому",       "VALID", "Локация (на дому)"),
        ("недорого",      "VALID", "Коммерция (недорого)"),
        
        # Country-aware geo — новые тесты
        ("тир",           "GREY",  "Тир = Ливан, не UA → нет geo → GREY"),
        ("або",           "GREY",  "Або — нет в UA geo → GREY"),
        
        # TRASH — негативные сигналы
        ("есть",          "TRASH", "Копула без объекта"),
        ("зачем",         "TRASH", "Мета-вопрос"),
        ("лучшие",        "TRASH", "Висячий модификатор"),
        ("и",             "TRASH", "Союз на конце (обрывок)"),
        ("для",           "TRASH", "Предлог на конце (обрывок)"),
        ("различия",      "TRASH", "Мусорный суффикс"),
        ("это что означает", "TRASH", "Мета-вопрос"),
        ("можно",         "TRASH", "Модальное без действия"),
        
        # GREY — конфликт или неопределённость
        ("xiaomi dreame", "GREY",  "Brand collision + brand → конфликт"),
        ("купить",        "GREY",  "Ни позитивный, ни негативный"),
    ]
    
    print("=" * 70)
    passed = 0
    
    for tail, expected, description in test_cases:
        result = classifier.classify(tail)
        label = result['label']
        
        status = "✅" if label == expected else "❌"
        if label == expected:
            passed += 1
        
        print(f"{status} {description}")
        print(f"   Хвост: '{tail}'")
        print(f"   Ожидалось: {expected}, Получено: {label} "
              f"(conf: {result['confidence']:.2f}, "
              f"+{result['positive_score']:.1f} / -{result['negative_score']:.1f})")
        
        if result['positive_signals']:
            print(f"   ✅ {', '.join(result['positive_signals'])}")
        if result['negative_signals']:
            print(f"   ❌ {', '.join(result['negative_signals'])}")
        print()
    
    print("=" * 70)
    print(f"\n📊 РЕЗУЛЬТАТ: {passed}/{len(test_cases)} "
          f"({passed/len(test_cases)*100:.1f}%)")
    
    return passed, len(test_cases)


if __name__ == "__main__":
    run_tests()
