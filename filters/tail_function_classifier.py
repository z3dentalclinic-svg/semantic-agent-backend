"""
TailFunctionClassifier v2 — исправленный классификатор.

Изменения относительно v1:
1. Пустой хвост = VALID (запрос = seed), не TRASH
2. Арбитраж с ВЕСАМИ сигналов (гео/бренд > эвристик)
3. Добавлен detect_noise_suffix (12-й детектор)
4. Приоритет: позитивный сигнал из БД перевешивает эвристический негатив
"""

from typing import Dict, List, Tuple, Set
import pymorphy3
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
)

# Category mismatch detector (использует embeddings, ленивая загрузка)
try:
    from .category_mismatch_detector import detect_category_mismatch
    CATEGORY_MISMATCH_AVAILABLE = True
except ImportError:
    CATEGORY_MISMATCH_AVAILABLE = False
    def detect_category_mismatch(seed, tail):
        return (False, "")

morph = pymorphy3.MorphAnalyzer()


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
    'category_mismatch': 0.9,  # категория tail несовместима с seed (еда vs запчасти)
}


class TailFunctionClassifier:
    """Классификатор хвостов на основе детекторов функций."""
    
    def __init__(self, geo_db: Set[str], brand_db: Set[str], seed: str = "ремонт пылесосов", target_country: str = "ua"):
        self.geo_db = geo_db
        self.brand_db = brand_db
        self.seed = seed
        self.target_country = target_country
    
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
        ]
        
        for signal_name, detector in detectors_positive:
            detected, reason = detector()
            if detected:
                positive_signals.append(signal_name)
                reasons.append(f"✅ {reason}")
        
        # ===== НЕГАТИВНЫЕ ДЕТЕКТОРЫ =====
        detectors_negative = [
            ('fragment',        lambda: detect_fragment(tail)),
            ('meta',            lambda: detect_meta(tail)),
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
        ]
        
        for signal_name, detector in detectors_negative:
            detected, reason = detector()
            if detected:
                negative_signals.append(signal_name)
                reasons.append(f"❌ {reason}")
        
        # ===== ПРОВЕРКА КОГЕРЕНТНОСТИ ХВОСТА =====
        # Если детектор поймал одно слово в многословном хвосте,
        # а остальные контентные слова — "чужие", понижаем до GREY
        if positive_signals:
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
        
        Returns: (is_coherent: bool, orphan_words: list)
        """
        words = tail.lower().split()
        if len(words) < 2:
            return True, []
        
        # Словари известных лемм по категориям
        commerce_lemmas = {
            'цена', 'стоимость', 'прайс', 'тариф', 'расценка',
            'купить', 'заказать', 'заказ', 'покупка', 'оплата',
            'недорого', 'дёшево', 'дешево', 'бюджетный', 'акция',
            'скидка', 'распродажа', 'бесплатно', 'стоить',
            'услуга', 'сервис', 'прейскурант', 'калькулятор',
        }
        reputation_lemmas = {
            'отзыв', 'рейтинг', 'оценка', 'обзор', 'мнение',
            'рекомендация', 'жалоба', 'форум', 'блог',
            'лучший', 'топ', 'худший', 'сравнение', 'рекомендовать',
            # Леммы прилагательных (pymorphy нормализует лучший→хороший)
            'хороший', 'плохой',
        }
        action_lemmas = {
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
            # Прилагательные-действия
            'пошаговый', 'подробный',
        }
        contacts_lemmas = {
            'адрес', 'телефон', 'контакт', 'карта', 'маршрут',
            'график', 'расписание', 'режим', 'часы', 'работа',
            # Прилагательные-контакты
            'контактный',
        }
        location_lemmas = {
            'рядом', 'поблизости', 'ближайший', 'недалеко',
            'район', 'улица', 'дом', 'квартира',
            # Прилагательные-локации
            'ближний', 'близкий',
        }
        time_lemmas = {
            'круглосуточно', 'срочно', 'быстро', 'сегодня', 'сейчас',
            # Прилагательные-время
            'срочный', 'круглосуточный',
        }
        marketplace_lemmas = {
            'олх', 'olx', 'розетка', 'rozetka', 'пром', 'hotline',
            'алиэкспресс', 'aliexpress', 'амазон', 'amazon',
            'эпицентр',
        }
        # Прилагательные, которые являются валидными модификаторами для любой ниши
        valid_adj_lemmas = {
            'бюджетный', 'бесплатный', 'платный', 'гарантийный',
            'новый', 'старый', 'профессиональный', 'домашний',
            'дешёвый', 'дешевый', 'дорогой',
        }
        
        all_known = (commerce_lemmas | reputation_lemmas | action_lemmas |
                     contacts_lemmas | location_lemmas | time_lemmas |
                     marketplace_lemmas | valid_adj_lemmas)
        
        # POS которые пропускаем (только служебные — НЕ прилагательные)
        # Прилагательные проверяем: "юридический" → сирота, "лучший" → в known
        skip_pos = {'PREP', 'CONJ', 'PRCL', 'INTJ', 'ADVB', 'PRED', 'COMP'}
        
        orphans = []
        for w in words:
            parsed = morph.parse(w)[0]
            pos = parsed.tag.POS
            lemma = parsed.normal_form
            
            # Служебные и модификаторы — пропускаем
            if pos in skip_pos:
                continue
            # Известная лемма
            if lemma in all_known or w in all_known:
                continue
            # Гео или бренд
            if w in self.geo_db or lemma in self.geo_db:
                continue
            if w in self.brand_db or lemma in self.brand_db:
                continue
            
            orphans.append(w)
        
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
            confidence = min(0.85 + neg_score * 0.05, 0.99)
            return 'TRASH', confidence, pos_score, neg_score
        
        # --- Случай 3: Конфликт ---
        if has_positive and has_negative:
            # Приоритет БД-сигналов: если geo или brand подтверждён,
            # а негатив — только эвристика, доверяем БД
            db_signals = {'geo', 'brand', 'verb_modifier', 'conjunctive'}
            has_db_positive = bool(set(positive) & db_signals)
            
            # Жёсткие негативные (почти всегда правы)
            hard_negatives = {'duplicate', 'meta', 'tech_garbage', 'mixed_alpha', 'category_mismatch'}
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
