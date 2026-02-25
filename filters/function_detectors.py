"""
function_detectors.py — 11 детекторов функции хвоста.

Каждый детектор отвечает на вопрос: "Какую ФУНКЦИЮ выполняет хвост?"
Если функция определена → сигнал VALID.
Если обнаружен дефект формы → сигнал TRASH.

Каждый детектор возвращает: (bool, str) — (сработал?, причина)
"""

import pymorphy3
from typing import Tuple, Set

morph = pymorphy3.MorphAnalyzer()


# ============================================================
# ПОЗИТИВНЫЕ ДЕТЕКТОРЫ (функция хвоста определена → VALID)
# ============================================================

def detect_geo(tail: str, geo_db: Set[str]) -> Tuple[bool, str]:
    """
    Детектор географии: город, район, страна.
    Использует geonamescache (65k+ городов) + лемматизацию.
    
    "киев" → True   "одессе" → True (лемма: одесса)
    "абвгд" → False
    """
    words = tail.lower().split()
    
    for word in words:
        # Точное совпадение
        if word in geo_db:
            return True, f"Город: '{word}'"
        
        # Лемматизация (киеву → киев, одессе → одесса)
        lemma = morph.parse(word)[0].normal_form
        if lemma in geo_db:
            return True, f"Город (лемма): '{lemma}'"
    
    # Проверяем многословные названия (нью йорк, кривой рог)
    if len(words) >= 2:
        bigram = ' '.join(words[:2])
        if bigram in geo_db:
            return True, f"Город (биграмм): '{bigram}'"
    
    return False, ""


def detect_brand(tail: str, brand_db: Set[str]) -> Tuple[bool, str]:
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
        
        lemma = morph.parse(word)[0].normal_form
        if lemma in brand_db:
            return True, f"Бренд (лемма): '{lemma}'"
    
    return False, ""


def detect_commerce(tail: str) -> Tuple[bool, str]:
    """
    Детектор коммерческого модификатора.
    Слова, которые сужают поиск до ценовой/транзакционной плоскости.
    
    Разделён на strong (самодостаточные) и weak (нужен контекст):
    "цена" → True (strong)    "заказ" → False (weak, одно слово)
    "заказ цена" → True (weak + ещё слово)
    """
    # Strong: самодостаточные — одного слова хватает для VALID
    # "цена", "стоимость", "купить" — всегда коммерческий интент
    commerce_lemmas_strong = {
        'цена', 'стоимость', 'прайс', 'тариф', 'расценка',
        'прейскурант', 'скидка', 'акция', 'гарантия', 'гарантийный',
        'бесплатно', 'бесплатный', 'платно', 'платный',
        'купить', 'приобрести', 'покупка',
        'рассрочка', 'кредит', 'предоплата', 'аванс',
        # Украинские
        'ціна', 'вартість', 'купити', 'знижка',
    }
    
    # Weak: нужен контекст — одного слова НЕ хватает
    # "заказ" для торта → VALID, "заказ" для пластики → бред
    # Одним словом → False, с другими словами → True
    commerce_lemmas_weak = {
        'заказать', 'оформить', 'арендовать',
        'заказ', 'оплата', 'оплатить', 'доставка',
        'замовити',
    }
    
    # Паттерны (могут быть частью фразы) — всегда strong
    commerce_patterns = [
        'сколько стоит', 'почём', 'по цене',
        'недорого', 'дёшево', 'дешево', 'дорого', 'бюджетно',
    ]
    
    tail_lower = tail.lower()
    words = tail_lower.split()
    
    # Считаем контентные слова (не предлоги/союзы/частицы)
    # "на заказ" → "на" предлог, "заказ" единственное → single content
    skip_pos_commerce = {'PREP', 'CONJ', 'PRCL', 'INTJ'}
    content_count = sum(1 for w in words if morph.parse(w)[0].tag.POS not in skip_pos_commerce)
    is_single_content = content_count <= 1
    
    # Проверка паттернов (всегда strong)
    for pattern in commerce_patterns:
        if pattern in tail_lower:
            return True, f"Коммерция (паттерн): '{pattern}'"
    
    # Проверка по леммам
    for word in words:
        lemma = morph.parse(word)[0].normal_form
        
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


def detect_reputation(tail: str) -> Tuple[bool, str]:
    """
    Детектор репутационного модификатора.
    Человек ищет отзывы, рейтинги, мнения о сервисе.
    
    "отзывы" → True    "форум" → True    "рейтинг" → True
    """
    reputation_lemmas = {
        'отзыв', 'рейтинг', 'обзор', 'форум', 'рекомендация',
        'жалоба', 'претензия', 'опыт', 'мнение', 'оценка',
        'сравнение', 'рекомендовать',
    }
    
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
        lemma = morph.parse(word)[0].normal_form
        if lemma in reputation_lemmas:
            return True, f"Репутация (лемма): '{lemma}'"
    
    return False, ""


def detect_location(tail: str) -> Tuple[bool, str]:
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
        lemma = morph.parse(word)[0].normal_form
        if lemma in location_lemmas:
            return True, f"Локация (лемма): '{lemma}'"
    
    return False, ""


def detect_time(tail: str) -> Tuple[bool, str]:
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
    ]
    
    tail_lower = tail.lower()
    
    for pattern in time_patterns:
        if pattern in tail_lower:
            return True, f"Время (паттерн): '{pattern}'"
    
    for word in tail_lower.split():
        lemma = morph.parse(word)[0].normal_form
        if lemma in time_lemmas or word in time_lemmas:
            return True, f"Время (лемма): '{lemma}'"
    
    return False, ""


def detect_action(tail: str) -> Tuple[bool, str]:
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
        'схема', 'чертёж', 'чертеж', 'диаграмма',
        'разборка', 'сборка', 'чистка', 'замена',
        'диагностика', 'профилактика', 'обслуживание',
    }
    
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
        parsed = morph.parse(word)[0]
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

def detect_fragment(tail: str) -> Tuple[bool, str]:
    """
    Детектор обрывка: хвост заканчивается на служебное слово,
    или состоит из одной копулы/частицы.
    
    "и" → True    "для" → True    "есть" → True
    "рядом" → False (наречие, не служебное)
    """
    words = tail.lower().split()
    if not words:
        return False, ""
    
    last_word = words[-1]
    last_parsed = morph.parse(last_word)[0]
    
    # Правило 1: Заканчивается на предлог, союз, частицу
    # Исключение: продуктовые суффиксы — "про"(Pro), "макс"(Max), "мини"(Mini), 
    # "плюс"(Plus), "лайт"(Lite) — pymorphy видит PREP/PRCL, но это модели товаров.
    # Универсально для любой темы.
    product_suffixes = {'про', 'макс', 'мини', 'плюс', 'лайт', 'ультра'}
    if last_parsed.tag.POS in ('PREP', 'CONJ', 'PRCL') and last_word not in product_suffixes:
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
                wp = morph.parse(w)[0]
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
    first_parsed = morph.parse(first_word)[0]
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


def detect_meta(tail: str) -> Tuple[bool, str]:
    """
    Детектор мета-вопроса: вопрос О САМОМ ПОНЯТИИ, а не уточнение поиска.
    
    "зачем" → True           "что означает" → True
    "как разобрать" → False   (это действие, не мета)
    """
    tail_lower = tail.lower().strip()
    words = tail_lower.split()
    
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
                after_parsed = morph.parse(after_words[0])[0]
                if after_parsed.tag.POS in ('INFN', 'VERB'):
                    # "можно ли заряжать" — валидный вопрос, НЕ мета
                    continue
            # "можно ли" без продолжения или без глагола — мета
            return True, f"Мета-вопрос: '{pattern}'"
    
    for pattern in meta_patterns:
        if pattern in tail_lower:
            return True, f"Мета-вопрос: '{pattern}'"
    
    # Паттерн 2: Одиночное вопросительное слово (без объекта)
    bare_question_words = {'зачем', 'почему', 'что', 'как', 'когда',
                            'навіщо', 'чому', 'що', 'як', 'коли'}
    if len(words) == 1 and words[0] in bare_question_words:
        # Исключение: "как" может быть частью "как разобрать" — но тут одиночное
        return True, f"Мета-вопрос: голое '{words[0]}'"
    
    # Паттерн 3: "почему + прилагательное" без объекта
    # "почему дорого", "почему долго" — мета-рассуждение
    if len(words) == 2 and words[0] in {'почему', 'зачем'}:
        second_parsed = morph.parse(words[1])[0]
        if second_parsed.tag.POS in ('ADVB', 'ADJF', 'ADJS', 'PRED'):
            return True, f"Мета-вопрос: '{words[0]} {words[1]}'"
    
    return False, ""


def detect_number_hijack(tail: str, seed: str) -> Tuple[bool, str]:
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
    parsed = morph.parse(word)[0]
    
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


def detect_short_garbage(tail: str) -> Tuple[bool, str]:
    """
    Ловит короткие бессмысленные токены: "жт", "хр", "щ".
    
    Правило: одиночный токен ≤2 символа, POS неизвестен или INTJ (междометие).
    Исключения: числа, аббревиатуры (тб, гб), латиница.
    """
    if not tail:
        return False, ""
    
    words = tail.strip().split()
    if len(words) != 1:
        return False, ""
    
    word = words[0]
    
    if len(word) > 2:
        return False, ""
    
    # Числа — пропускаем
    if word.isdigit():
        return False, ""
    
    # Латиница — может быть аббревиатура (gb, tb, hp)
    if word.isascii() and word.isalpha():
        return False, ""
    
    parsed = morph.parse(word)[0]
    
    # Аббревиатуры (тб, гб) — пропускаем
    if 'Abbr' in parsed.tag:
        return False, ""
    
    # Известные POS — NOUN, VERB и т.д. — пропускаем (может быть аббревиатура темы)
    # Ловим только UNKN, INTJ, None
    if parsed.tag.POS in (None, 'INTJ'):
        return True, f"Мусорный токен: '{word}' ({len(word)} символа, неизвестное слово)"
    
    return False, ""


def detect_dangling(tail: str, seed: str = "ремонт пылесосов", geo_db: set = None) -> Tuple[bool, str]:
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
    
    parsed_first = [morph.parse(w)[0] for w in words]
    
    has_adj = False
    has_noun = False
    adj_words_info = []
    
    for w, p in zip(words, parsed_first):
        if p.tag.POS in ('ADJF', 'ADJS'):
            has_adj = True
            adj_words_info.append((w, morph.parse(w)))
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
        for w in words:
            if w in geo_db:
                return False, ""
            lemma = morph.parse(w)[0].normal_form
            if lemma in geo_db:
                return False, ""
    
    # === ПРОВЕРКА 2: Согласование с seed ===
    seed_words = seed.lower().split()
    seed_noun_parses = None
    
    for sw in reversed(seed_words):
        sp = morph.parse(sw)[0]
        if sp.tag.POS == 'NOUN':
            seed_noun_parses = morph.parse(sw)
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


def detect_duplicate_words(tail: str) -> Tuple[bool, str]:
    """
    Детектор дублирования слов — признак парсинг-мусора.
    
    "ремонт ремонт" → True    "пылесосов пылесос" → True (лемма)
    "samsung samsung" → True
    """
    words = tail.lower().split()
    if len(words) < 2:
        return False, ""
    
    # Проверка точных дубликатов
    if len(words) != len(set(words)):
        dupes = [w for w in words if words.count(w) > 1]
        return True, f"Дублирование слов: '{dupes[0]}'"
    
    # Проверка дубликатов по леммам
    lemmas = [morph.parse(w)[0].normal_form for w in words]
    if len(lemmas) != len(set(lemmas)):
        dupe_lemmas = [l for l in lemmas if lemmas.count(l) > 1]
        return True, f"Дублирование лемм: '{dupe_lemmas[0]}'"
    
    return False, ""


def detect_brand_collision(tail: str, brand_db: Set[str]) -> Tuple[bool, str]:
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
            lemma = morph.parse(word)[0].normal_form
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

def detect_seed_echo(tail: str, seed: str = "ремонт пылесосов") -> Tuple[bool, str]:
    """
    Детектор эхо seed'а: хвост повторяет слова из seed.
    
    seed="ремонт пылесосов", tail="ремонт" → True (дубль)
    seed="ремонт пылесосов", tail="после ремонт" → частичный дубль
    """
    tail_words = tail.lower().split()
    seed_words = seed.lower().split()
    seed_lemmas = {morph.parse(w)[0].normal_form for w in seed_words}
    
    # Хвост целиком = одно из слов seed'а
    if len(tail_words) == 1:
        tail_lemma = morph.parse(tail_words[0])[0].normal_form
        if tail_lemma in seed_lemmas:
            return True, f"Эхо seed: '{tail_words[0]}' повторяет слово из seed"
    
    return False, ""


def detect_broken_grammar(tail: str) -> Tuple[bool, str]:
    """
    Детектор сломанной грамматики: предлог + слово в неправильном падеже.
    
    "после ремонт" → True (после требует род.п., а "ремонт" в им.п.)
    "после ремонта" → False (правильное управление)
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
        
        # Проверяем падеж следующего слова
        second_word = words[1]
        
        # Числа не имеют падежа — пропускаем ("на 256", "в 2024")
        if second_word.isdigit():
            return False, ""
        
        second_parses = morph.parse(second_word)
        
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


def detect_type_specifier(tail: str, seed: str = "ремонт пылесосов") -> Tuple[bool, str]:
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
    
    parsed_first = [morph.parse(w)[0] for w in words]
    
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
        
        all_parses = morph.parse(w)
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


def detect_noise_suffix(tail: str) -> Tuple[bool, str]:
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

def detect_verb_modifier(tail: str, seed: str = "") -> Tuple[bool, str]:
    """
    Детектор модификатора глагола: хвост = наречие/компаратив/модальное,
    а seed содержит глагол → хвост модифицирует глагол seed'а → VALID.
    
    Алгоритмический, через POS-теги pymorphy3. Ноль хардкода.
    
    seed="как принимать нимесил":
      "лучше" → COMP + seed имеет INFN "принимать" → модификатор → VALID
      "можно" → PRED + seed имеет INFN → модификатор → VALID  
      "правильно" → ADVB + seed имеет INFN → модификатор → VALID
    
    seed="ремонт пылесосов":
      "лучше" → COMP, но seed НЕ имеет глагола → НЕ модификатор → False
    """
    if not tail or not seed:
        return False, ""
    
    tail_words = tail.lower().split()
    
    # Проверяем: есть ли глагол в seed?
    seed_words = seed.lower().split()
    seed_has_verb = False
    for sw in seed_words:
        sp = morph.parse(sw)[0]
        if sp.tag.POS in ('INFN', 'VERB'):
            seed_has_verb = True
            break
    
    if not seed_has_verb:
        return False, ""
    
    # Хвост = 1-2 слова, все — модификаторы глагола?
    if len(tail_words) > 2:
        return False, ""
    
    # POS-теги которые модифицируют глагол
    verb_modifier_pos = {'ADVB', 'COMP', 'PRED'}
    
    all_modifiers = True
    for tw in tail_words:
        tp = morph.parse(tw)[0]
        if tp.tag.POS not in verb_modifier_pos:
            all_modifiers = False
            break
    
    if all_modifiers:
        pos_tags = [morph.parse(w)[0].tag.POS for w in tail_words]
        return True, f"Модификатор глагола: '{tail}' ({', '.join(pos_tags)}) при seed с глаголом"
    
    return False, ""


def detect_conjunctive_extension(tail: str, seed: str = "") -> Tuple[bool, str]:
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
    
    first_parsed = morph.parse(words[0])[0]
    last_parsed = morph.parse(words[-1])[0]
    
    # Направление 1: НАЧИНАЕТСЯ с союза + содержание после
    if first_parsed.tag.POS == 'CONJ':
        rest_words = words[1:]
        for rw in rest_words:
            for rp in morph.parse(rw):
                if rp.tag.POS in content_pos:
                    return True, f"Конъюнктивное расширение: '{tail}' (союз + содержание)"
    
    # Направление 2: ЗАКАНЧИВАЕТСЯ союзом/предлогом + содержание до
    # "терафлю и" → NOUN + CONJ (связка к seed)
    # "омепразол с" → NOUN + PREP (связка к seed)
    if last_parsed.tag.POS in ('CONJ', 'PREP'):
        before_words = words[:-1]
        for bw in before_words:
            for bp in morph.parse(bw):
                if bp.tag.POS in content_pos:
                    return True, f"Конъюнктивное расширение: '{tail}' (содержание + связка к seed)"
    
    return False, ""


def detect_contacts(tail: str) -> Tuple[bool, str]:
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
        lemma = morph.parse(word)[0].normal_form
        if lemma in contacts_lemmas:
            return True, f"Контакты (лемма): '{lemma}'"
    
    return False, ""


def detect_marketplace(tail: str, target_country: str = "ua") -> Tuple[bool, str]:
    """
    Детектор площадок/маркетплейсов — VALID для целевого региона.
    
    1. Если страна в конфиге → проверяет по списку VALID площадок
    2. Если площадка известная но нет в конфиге → всё равно позитивный сигнал
       (коммерческий интент, человек ищет где купить)
    """
    tail_lower = tail.lower().strip()
    
    # Сначала проверяем конфиг для известных стран (ускоритель)
    country_valid = _REGIONAL_MARKETPLACES.get(target_country.lower(), {}).get('valid', set())
    
    for check in [tail_lower] + tail_lower.split():
        if check in country_valid or check in _GLOBAL_VALID:
            return True, f"Площадка ({target_country.upper()}): '{check}'"
    
    # Если страна не в конфиге — проверяем глобальный список известных площадок
    # Это даёт позитивный сигнал "человек ищет где купить"
    if not country_valid:
        for check in [tail_lower] + tail_lower.split():
            if check in _ALL_KNOWN_MARKETPLACES:
                return True, f"Площадка (известная): '{check}'"
    
    return False, ""


def detect_trash_marketplace(tail: str, target_country: str = "ua") -> Tuple[bool, str]:
    """
    НЕГАТИВНЫЙ детектор: площадки заведомо чужих регионов → TRASH.
    
    Работает ТОЛЬКО для стран с конфигом (UA/KZ/RU/BY).
    Для остальных стран → не срабатывает, уходит в GREY → perplexity решает.
    """
    country_trash = _REGIONAL_MARKETPLACES.get(target_country.lower(), {}).get('trash', set())
    
    if not country_trash:
        return False, ""  # Нет конфига → не блокируем, пусть решает perplexity
    
    tail_lower = tail.lower().strip()
    
    for check in [tail_lower] + tail_lower.split():
        if check in country_trash:
            return True, f"Площадка (чужая для {target_country.upper()}): '{check}'"
    
    # Биграмы: "м видео", "5 элемент", "яндекс маркет"
    words = tail_lower.split()
    for i in range(len(words) - 1):
        bigram = f"{words[i]} {words[i+1]}"
        if bigram in country_trash:
            return True, f"Площадка (чужая для {target_country.upper()}): '{bigram}'"
    
    return False, ""


# ============================================================
# КОНФИГ ПЛОЩАДОК
# ============================================================

# Глобальные (работают везде)
_GLOBAL_VALID = {
    'amazon', 'ebay', 'aliexpress', 'алиэкспресс',
}

# Все известные площадки (для стран без конфига → позитивный сигнал)
_ALL_KNOWN_MARKETPLACES = _GLOBAL_VALID | {
    'олх', 'olx', 'розетка', 'rozetka', 'фокстрот', 'foxtrot',
    'комфи', 'comfy', 'хотлайн', 'hotline', 'цитрус', 'citrus',
    'эпицентр', 'epicentr', 'эльдорадо', 'eldorado', 'алло', 'allo',
    'prom.ua', 'prom', 'bigl.ua',
    'авито', 'avito', 'озон', 'ozon', 'wildberries', 'вайлдберриз',
    'днс', 'dns', 'м видео', 'mvideo', 'яндекс маркет',
    'куфар', 'kufar', 'онлайнер', 'onliner',
    'kaspi', 'каспи', 'технодом', 'technodom', 'сулпак', 'sulpak',
    'tesco', 'argos', 'currys', 'john lewis',
    'mediamarkt', 'saturn', 'fnac', 'darty',
}

# Региональные конфиги (ускоритель для основных рынков)
# Для стран НЕ в этом словаре → detect_trash_marketplace не срабатывает → GREY → perplexity
_REGIONAL_MARKETPLACES = {
    # UA намеренно НЕ включена — тестируем через perplexity
    # После подтверждения работы perplexity — можно добавить как ускоритель
    'kz': {
        'valid': {
            'олх', 'olx', 'kaspi', 'каспи', 'forte', 'форте',
            'колёса', 'kolesa', 'крыша', 'krisha',
            'flip.kz', 'technodom', 'технодом', 'sulpak', 'сулпак',
            'мечта', 'mechta', 'белый ветер', 'авито', 'avito',
        },
        'trash': {
            'розетка', 'rozetka', 'фокстрот', 'foxtrot',
            'комфи', 'comfy', 'хотлайн', 'hotline',
            'куфар', 'kufar', 'онлайнер', 'onliner',
            'шоп бай', 'shop.by',
        },
    },
    'ru': {
        'valid': {
            'авито', 'avito', 'озон', 'ozon', 'wildberries', 'вайлдберриз',
            'днс', 'dns', 'мтс', 'mts', 'яндекс', 'yandex',
            'м видео', 'mvideo', 'сбермегамаркет', 'сбер',
            'касторама', 'леруа', 'юла', 'youla',
        },
        'trash': {
            'олх', 'olx', 'розетка', 'rozetka', 'фокстрот', 'foxtrot',
            'комфи', 'comfy', 'хотлайн', 'hotline',
            'куфар', 'kufar', 'онлайнер', 'onliner',
            'шоп бай', 'shop.by',
        },
    },
    'by': {
        'valid': {
            'куфар', 'kufar', 'онлайнер', 'onliner', 'шоп бай', 'shop.by',
            '5 элемент', 'а1', 'a1', 'электросила', 'юнит',
        },
        'trash': {
            'олх', 'olx', 'розетка', 'rozetka', 'фокстрот', 'foxtrot',
            'комфи', 'comfy', 'хотлайн', 'hotline',
        },
    },
}


# ============================================================
# НОВЫЕ НЕГАТИВНЫЕ ДЕТЕКТОРЫ
# ============================================================

def detect_technical_garbage(tail: str) -> Tuple[bool, str]:
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


def detect_mixed_alphabet(tail: str) -> Tuple[bool, str]:
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


def detect_standalone_number(tail: str, seed: str = "") -> Tuple[bool, str]:
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
