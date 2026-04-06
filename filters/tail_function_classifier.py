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
    detect_truncated_geo, detect_orphan_genitive, detect_single_infinitive,
    detect_foreign_geo,
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
    'incoherent_tail': 0.85,   # многословный хвост с "чужими" словами
    'category_mismatch': 0.5,  # мягкий — embeddings ненадёжно классифицируют категории
    'truncated_geo':     0.85,  # обрезанный составной город — довольно надёжно
    'foreign_geo':       0.95,  # город/страна из чужого региона — очень надёжно (geo_db)
    'orphan_genitive':   0.5,   # мягкий — может быть валидным ("фильтров")
    'single_infinitive': 0.5,   # мягкий — может быть валидным интентом
    'intent_mismatch':   0.9,   # информационный seed + коммерческий tail — надёжный конфликт
}


# ============================================================
# Константы уровня модуля для _check_coherence.
# Вычисляются ОДИН РАЗ при импорте — не пересоздаются на каждый вызов classify().
# ============================================================

_COHERENCE_COMMERCE = frozenset({
    'цена', 'стоимость', 'прайс', 'тариф', 'расценка',
    'купить', 'заказать', 'заказ', 'покупка', 'оплата',
    'недорого', 'дёшево', 'дешево', 'бюджетный', 'акция',
    'скидка', 'распродажа', 'бесплатно', 'стоить',
    'услуга', 'сервис', 'прейскурант', 'калькулятор',
})

_COHERENCE_REPUTATION = frozenset({
    'отзыв', 'рейтинг', 'оценка', 'обзор', 'мнение',
    'рекомендация', 'жалоба', 'форум', 'блог',
    'лучший', 'топ', 'худший', 'сравнение', 'рекомендовать',
    'хороший', 'плохой',
})

_COHERENCE_ACTION = frozenset({
    'инструкция', 'руководство', 'мануал',
    'видео', 'видеоинструкция', 'фото', 'фотография',
    'схема', 'чертёж', 'чертеж', 'диаграмма',
    'разборка', 'сборка', 'чистка', 'замена',
    'диагностика', 'профилактика', 'обслуживание',
    'запчасть', 'деталь', 'комплектующие', 'фильтр',
    'щётка', 'щетка', 'шланг', 'мешок', 'пылесборник',
    'мотор', 'двигатель', 'турбина', 'аккумулятор',
    'смотреть', 'скачать', 'найти', 'сделать', 'починить',
    'почистить', 'разобрать', 'собрать', 'подключить',
    'установить', 'настроить', 'проверить', 'заменить',
    'показать', 'объяснить',
    'пошаговый', 'подробный',
})

_COHERENCE_CONTACTS = frozenset({
    'адрес', 'телефон', 'контакт', 'карта', 'маршрут',
    'график', 'расписание', 'режим', 'часы', 'работа',
    'контактный',
})

_COHERENCE_LOCATION = frozenset({
    'рядом', 'поблизости', 'ближайший', 'недалеко',
    'район', 'улица', 'дом', 'квартира',
    'ближний', 'близкий',
})

_COHERENCE_TIME = frozenset({
    'круглосуточно', 'срочно', 'быстро', 'сегодня', 'сейчас',
    'срочный', 'круглосуточный',
})

_COHERENCE_MARKETPLACE = frozenset({
    'олх', 'olx', 'розетка', 'rozetka', 'пром', 'hotline',
    'алиэкспресс', 'aliexpress', 'амазон', 'amazon',
    'эпицентр',
})

_COHERENCE_VALID_ADJ = frozenset({
    'бюджетный', 'бесплатный', 'платный', 'гарантийный',
    'новый', 'старый', 'профессиональный', 'домашний',
    'дешёвый', 'дешевый', 'дорогой',
})

# Объединённый словарь всех известных лемм — для быстрой проверки
_COHERENCE_ALL_KNOWN = (
    _COHERENCE_COMMERCE | _COHERENCE_REPUTATION | _COHERENCE_ACTION |
    _COHERENCE_CONTACTS | _COHERENCE_LOCATION | _COHERENCE_TIME |
    _COHERENCE_MARKETPLACE | _COHERENCE_VALID_ADJ
)

# POS которые пропускаем в _check_coherence (служебные — НЕ прилагательные)
_COHERENCE_SKIP_POS = frozenset({'PREP', 'CONJ', 'PRCL', 'INTJ', 'ADVB', 'PRED', 'COMP'})

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
_STRONG_POSITIVES_SKIP_MISMATCH = frozenset({
    'geo', 'brand', 'location', 'contacts', 'time',
    'verb_modifier', 'prep_modifier', 'conjunctive',
})


class TailFunctionClassifier:
    """Классификатор хвостов на основе детекторов функций."""
    
    def __init__(self, geo_db: Set[str], brand_db: Set[str], seed: str = "ремонт пылесосов", target_country: str = "ua"):
        self.geo_db = geo_db
        self.brand_db = brand_db
        self.seed = seed
        self.target_country = target_country

        # Pre-computed seed data — вычисляется один раз при создании классификатора,
        # а не на каждый вызов classify()
        self._seed_words_lower = seed.lower().split()
        self._seed_first_word = self._seed_words_lower[0] if self._seed_words_lower else ''
        self._seed_first_in_geo_incompatible = self._seed_first_word in _GEO_INCOMPATIBLE_INTERROGATIVES
        self._seed_first_in_commerce_incompatible = self._seed_first_word in _COMMERCE_INCOMPATIBLE
        # Тайминги детекторов — накапливаются за батч, сбрасываются из l0_filter
        self.detector_timings: Dict[str, float] = {}
    
    def classify(self, tail: str) -> Dict:
        """
        Классифицирует хвост запроса.
        
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
        
        positive_signals = []
        negative_signals = []
        reasons = []
        
        # ===== ПОЗИТИВНЫЕ ДЕТЕКТОРЫ =====
        detectors_positive = [
            ('geo',        lambda: detect_geo(tail, self.geo_db, self.target_country)),
            ('brand',      lambda: detect_brand(tail, self.brand_db)),
            ('commerce',   lambda: detect_commerce(tail)),
            ('reputation', lambda: detect_reputation(tail)),
            ('location',   lambda: detect_location(tail)),
            ('action',     lambda: detect_action(tail)),
            ('time',       lambda: detect_time(tail)),
            ('type_spec',  lambda: detect_type_specifier(tail, self.seed)),
            ('contacts',   lambda: detect_contacts(tail)),
            ('verb_modifier', lambda: detect_verb_modifier(tail, self.seed)),
            ('conjunctive', lambda: detect_conjunctive_extension(tail, self.seed)),
            ('prep_modifier', lambda: detect_prepositional_modifier(tail, self.seed)),
        ]
        
        for signal_name, detector in detectors_positive:
            _t0 = time.perf_counter()
            detected, reason = detector()
            self.detector_timings[signal_name] = self.detector_timings.get(signal_name, 0.0) + (time.perf_counter() - _t0)
            if detected:
                positive_signals.append(signal_name)
                reasons.append(f"✅ {reason}")
        
        # ===== НЕГАТИВНЫЕ ДЕТЕКТОРЫ =====
        detectors_negative = [
            ('fragment',        lambda: detect_fragment(tail, self.seed)),
            ('meta',            lambda: detect_meta(tail, self.seed)),
            ('dangling',        lambda: detect_dangling(tail, self.seed, self.geo_db)),
            ('duplicate',       lambda: detect_duplicate_words(tail)),
            ('brand_collision', lambda: detect_brand_collision(tail, self.brand_db)),
            ('noise_suffix',    lambda: detect_noise_suffix(tail)),
            ('seed_echo',       lambda: detect_seed_echo(tail, self.seed)),
            ('broken_grammar',  lambda: detect_broken_grammar(tail)),
            ('number_hijack',   lambda: detect_number_hijack(tail, self.seed)),
            ('short_garbage',   lambda: detect_short_garbage(tail)),
            ('tech_garbage',    lambda: detect_technical_garbage(tail)),
            ('mixed_alpha',     lambda: detect_mixed_alphabet(tail)),
            ('standalone_num',  lambda: detect_standalone_number(tail, self.seed)),
            # Детектор несовместимых категорий (использует embeddings)
            ('category_mismatch', lambda: detect_category_mismatch(self.seed, tail)),
            # Новые мягкие детекторы
            ('truncated_geo',     lambda: detect_truncated_geo(tail, self.geo_db)),
            ('foreign_geo',       lambda: detect_foreign_geo(tail, self.geo_db, self.target_country)),
            ('orphan_genitive',   lambda: detect_orphan_genitive(tail, self.seed)),
            ('single_infinitive', lambda: detect_single_infinitive(tail, self.seed)),
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
        
        # ===== ПРОВЕРКА КОГЕРЕНТНОСТИ ХВОСТА =====
        # Если детектор поймал одно слово в многословном хвосте,
        # а остальные контентные слова — "чужие", понижаем до GREY
        #
        # ИСКЛЮЧЕНИЕ: если позитивный сигнал пришёл от ПАТТЕРНА (multi-word match)
        # и хвост короткий (≤2 слов), паттерн покрывает весь хвост →
        # coherence check не нужен, он просто разломает паттерн на слова.
        # Пример: "своими руками" → detect_action ловит паттерн,
        # но coherence видит "руками" как orphan.
        if positive_signals:
            tail_word_count = len(tail.lower().split())
            has_pattern_match = any('паттерн' in r for r in reasons)
            has_prep_modifier = 'prep_modifier' in positive_signals
            
            if tail_word_count <= 2 and has_pattern_match:
                pass  # Паттерн покрывает весь хвост — coherence не нужен
            elif has_prep_modifier:
                pass  # Предложная группа покрывает весь хвост — coherence не нужен
            else:
                is_coherent, orphans = self._check_coherence(tail)
                if not is_coherent:
                    negative_signals.append('incoherent_tail')
                    reasons.append(f"⚠️ Некогерентный хвост: слова {orphans} не относятся к поисковым паттернам")
        
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
    
    def _check_coherence(self, tail: str):
        """
        Проверяет когерентность многословного хвоста.

        Принцип: если хвост 2+ слов и детектор поймал одно,
        а остальные контентные слова не из известных категорий → incoherent.

        "тигров фото" → фото=action ✅, тигров=??? → incoherent
        "замена фильтра" → замена=action ✅, фильтр=action ✅ → coherent

        Использует модульные frozenset-константы (_COHERENCE_ALL_KNOWN, _COHERENCE_SKIP_POS)
        вместо пересборки 7 set'ов на каждый вызов.

        Returns: (is_coherent: bool, orphan_words: list)
        """
        words = tail.lower().split()
        if len(words) < 2:
            return True, []

        orphans = []
        prev_pos = None
        for w in words:
            parsed = morph.parse(w)[0]
            pos = parsed.tag.POS
            lemma = parsed.normal_form

            # Служебные и модификаторы — пропускаем
            if pos in _COHERENCE_SKIP_POS:
                prev_pos = pos
                continue
            # Слово после предлога — пропускаем только если это гео
            if prev_pos == 'PREP':
                nomn_form = parsed.inflect({'nomn'})
                check_forms = {w, lemma}
                if nomn_form:
                    check_forms.add(nomn_form.word)
                is_geo = any(cf in self.geo_db for cf in check_forms)
                if not is_geo:
                    from .function_detectors import _COUNTRIES
                    is_geo = any(cf in _COUNTRIES for cf in check_forms)
                if is_geo:
                    prev_pos = pos
                    continue
                # Не гео — проверяем как обычное слово (fall through)
            # Известная лемма — используем модульную константу
            if lemma in _COHERENCE_ALL_KNOWN or w in _COHERENCE_ALL_KNOWN:
                prev_pos = pos
                continue
            # Гео или бренд
            if w in self.geo_db or lemma in self.geo_db:
                prev_pos = pos
                continue
            if w in self.brand_db or lemma in self.brand_db:
                prev_pos = pos
                continue

            orphans.append(w)
            prev_pos = pos

        return len(orphans) == 0, orphans

    
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
            # Мягкие негативные сигналы (orphan_genitive, single_infinitive)
            # не должны давать TRASH — только понижать до GREY
            # Жёсткий TRASH только при neg_score >= 0.6
            if neg_score < 0.6:
                return 'GREY', 0.4, pos_score, neg_score
            confidence = min(0.85 + neg_score * 0.05, 0.99)
            return 'TRASH', confidence, pos_score, neg_score
        
        # --- Случай 3: Конфликт ---
        if has_positive and has_negative:
            # Приоритет БД-сигналов: если geo или brand подтверждён,
            # а негатив — только эвристика, доверяем БД
            db_signals = {'geo', 'brand', 'verb_modifier', 'conjunctive', 'prep_modifier'}
            has_db_positive = bool(set(positive) & db_signals)
            
            # Жёсткие негативные (почти всегда правы)
            hard_negatives = {'duplicate', 'meta', 'tech_garbage', 'mixed_alpha', 'foreign_geo', 'intent_mismatch'}
            has_hard_negative = bool(set(negative) & hard_negatives)
            
            # Некогерентный хвост — не жёсткий, но ограничивает максимум до GREY
            has_incoherent = 'incoherent_tail' in negative
            
            if has_db_positive and not has_hard_negative and not has_incoherent:
                # БД говорит VALID, эвристика говорит TRASH → доверяем БД
                confidence = 0.75
                return 'VALID', confidence, pos_score, neg_score
            
            # Incoherent → максимум GREY, никогда VALID
            if has_incoherent and not has_hard_negative:
                return 'GREY', 0.4, pos_score, neg_score
            
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
