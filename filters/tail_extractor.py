"""
tail_extractor.py — извлечение хвоста из поискового запроса.

Двухшаговая логика:
1. Точный split по seed-подстроке (быстро, надёжно)
2. Нечёткий поиск: слова seed'а по порядку с допуском вставок
   "как правильно принимать нимесил" → seed найден, хвост = "правильно"

Предлоги ПЕРЕД seed'ом отсекаются через POS-тег (не стоп-слова).

ОПТИМИЗАЦИЯ:
- Использует shared morph singleton (один LRU-кэш на весь пакет)
- Принимает pre-computed seed_ctx — seed лематизируется ОДИН РАЗ на весь батч
  в apply_l0_filter, а не заново на каждое ключевое слово
- query_lemmas вычисляются один раз и передаются в шаги 2-4
"""

from .shared_morph import morph


def build_seed_ctx(seed_lower: str) -> dict:
    """
    Строит контекст seed: слова, леммы, POS-теги и вспомогательные флаги.

    Вызывается ОДИН РАЗ на батч в apply_l0_filter.
    Передаётся в extract_tail для каждого ключевого слова.

    Args:
        seed_lower: seed в нижнем регистре (уже .lower().strip())

    Returns:
        dict с полями:
          'words'     : list[str]  — слова seed
          'lemmas'    : list[str]  — леммы (normal_form)
          'pos'       : list[str]  — POS-теги (tag.POS)
          'is_function': list[bool] — True если слово служебное (PREP/CONJ/PRCL/INTJ)
          'is_number' : list[bool] — True если слово — число (NUMB или isdigit)
          'is_cyr'    : list[bool] — True если слово содержит кириллицу
          'is_lat'    : list[bool] — True если слово содержит латиницу
    """
    words = seed_lower.split()
    parses = [morph.parse(w)[0] for w in words]
    return {
        'words': words,
        'lemmas': [p.normal_form for p in parses],
        'pos': [p.tag.POS for p in parses],
        'is_function': [
            p.tag.POS in ('PREP', 'CONJ', 'PRCL', 'INTJ')
            for p in parses
        ],
        'is_number': [
            'NUMB' in str(p.tag) or w.isdigit()
            for w, p in zip(words, parses)
        ],
        'is_cyr': [
            any('\u0400' <= c <= '\u04ff' for c in w)
            for w in words
        ],
        'is_lat': [
            any('a' <= c <= 'z' for c in w)
            for w in words
        ],
    }


def _strip_trailing_preps(words: list) -> list:
    """Убирает предлоги с конца списка слов (они связаны с seed'ом)."""
    while words:
        p = morph.parse(words[-1])[0]
        if p.tag.POS == 'PREP':
            words.pop()
        else:
            break
    return words


def extract_tail(query: str, seed: str, seed_ctx: dict = None):
    """
    Извлекает хвост запроса относительно seed'а.

    Шаг 1 (точный): split по подстроке seed.
    Шаг 2 (нечёткий): слова seed'а по порядку, вставки → хвост.
    Шаг 3 (неупорядоченный): слова seed'а в любом порядке + кросс-скрипт.
    Шаг 4 (частичный): допускаем пропуск одного НЕконтентного слова seed'а.

    Args:
        query: поисковый запрос
        seed: базовый запрос
        seed_ctx: pre-computed контекст seed (из build_seed_ctx).
                  Если None — вычисляется внутри (backward-compat, медленнее).

    Returns:
        str:  Хвост (может быть пустым если запрос ≈ seed)
        None: Seed не найден в запросе
    """
    q = query.lower().strip()
    s = seed.lower().strip()

    # Строим seed_ctx если не передан (backward-compat / тесты)
    if seed_ctx is None:
        seed_ctx = build_seed_ctx(s)

    # === Шаг 1: Точный split ===
    # Подстрочный match с проверкой word-boundary.
    # Без boundary: seed="купить айфон 16" найдётся в q="купить айфон 16е"
    # как подстрока, tail='е'. Но "16е" — цельный артикул/модель (iPhone 16e),
    # и не должен разрываться. Разделяем только на word-boundary:
    # после последнего символа seed должна быть НЕ буква и НЕ цифра
    # (или конец строки). Универсально для 16е, 16e, 16gb, 16s, 160, 2016.
    if s in q:
        import re as _re
        _matched = False
        for _m in _re.finditer(_re.escape(s), q):
            _end = _m.end()
            # Конец строки → boundary OK
            if _end >= len(q):
                _matched = True
                _before_idx = _m.start()
                break
            # Следующий символ не буква и не цифра → boundary OK
            _next_ch = q[_end]
            if not (_next_ch.isalpha() or _next_ch.isdigit()):
                _matched = True
                _before_idx = _m.start()
                break
        if _matched:
            before = q[:_before_idx].strip()
            after = q[_end:].strip()

            if before:
                before = ' '.join(_strip_trailing_preps(before.split()))

            return ' '.join([before, after]).strip()
        # Нет корректной boundary — идём к шагу 2 (он работает по токенам,
        # а не по подстроке, там '16е' не равно '16' и не даст ложный матч)

    # Pre-compute query data ОДИН РАЗ для шагов 2-4
    # (если точный матч не сработал — нужны леммы для fuzzy/unordered)
    query_words = q.split()
    query_lemmas = [morph.parse(w)[0].normal_form for w in query_words]

    # === Шаг 2: Нечёткий поиск (по порядку) ===
    result = _extract_fuzzy_ordered(query_words, query_lemmas, seed_ctx)
    if result is not None:
        return result

    # === Шаг 3: Неупорядоченный поиск (fallback) ===
    result = _extract_tail_unordered(query_words, query_lemmas, seed_ctx)
    if result is not None:
        return result

    # === Шаг 4: Частичный матч ===
    return _extract_partial_match(query_words, query_lemmas, seed_ctx)


def _extract_fuzzy_ordered(
    query_words: list, query_lemmas: list, seed_ctx: dict
):
    """
    Шаг 2: Нечёткий поиск (seed-слова по порядку + кросс-скрипт).

    Использует pre-computed seed_ctx (без дополнительных morph.parse для seed).
    query_words / query_lemmas уже готовы (переданы из extract_tail).

    Returns: tail str or None.
    """
    seed_words = seed_ctx['words']
    seed_lemmas = seed_ctx['lemmas']
    seed_is_function = seed_ctx['is_function']
    seed_is_cyr = seed_ctx['is_cyr']
    seed_is_lat = seed_ctx['is_lat']

    seed_positions = []
    search_from = 0

    for i_sw in range(len(seed_words)):
        sw = seed_words[i_sw]
        sl = seed_lemmas[i_sw]
        found = False

        for i in range(search_from, len(query_words)):
            if query_words[i] == sw or query_lemmas[i] == sl:
                seed_positions.append(i)
                search_from = i + 1
                found = True
                break

        if not found:
            # === Шаг 2.5: Кросс-скриптовый мост ===
            # Только для контентных слов (бренды, модели)
            if not seed_is_function[i_sw]:
                sw_is_cyr = seed_is_cyr[i_sw]
                sw_is_lat = seed_is_lat[i_sw]

                for i in range(search_from, len(query_words)):
                    qw = query_words[i]
                    qw_is_cyr = any('\u0400' <= c <= '\u04ff' for c in qw)
                    qw_is_lat = any('a' <= c <= 'z' for c in qw)

                    if (sw_is_cyr and qw_is_lat) or (sw_is_lat and qw_is_cyr):
                        seed_positions.append(i)
                        search_from = i + 1
                        found = True
                        break

            if not found:
                return None  # слово seed'а отсутствует → seed не найден

    # Собираем хвост: всё что не на позициях seed'а
    seed_idx = set(seed_positions)

    # === Browse-артефакт: одиночная буква между seed-позициями ===
    sorted_positions = sorted(seed_positions)
    browse_artifacts = set()
    for pi in range(len(sorted_positions) - 1):
        pos_left = sorted_positions[pi]
        pos_right = sorted_positions[pi + 1]
        for mid in range(pos_left + 1, pos_right):
            word = query_words[mid]
            if len(word) == 1 and word.isalpha():
                browse_artifacts.add(mid)

    tail_words = [
        query_words[i] for i in range(len(query_words))
        if i not in seed_idx and i not in browse_artifacts
    ]

    tail = ' '.join(tail_words).strip()
    return tail if tail else ''


def _extract_tail_unordered(
    query_words: list, query_lemmas: list, seed_ctx: dict
):
    """
    Шаг 3: Неупорядоченный поиск seed'а в запросе.
    Вызывается ТОЛЬКО если шаги 1+2 не нашли seed.

    "iphone 17 купить в швейцарии" + seed "купить айфон 17"
    → купить(2) + iphone↔айфон(0) + 17(1) → хвост = "в швейцарии"
    """
    positions = _unordered_match(
        query_words, query_lemmas,
        seed_ctx['words'], seed_ctx['lemmas'], seed_ctx['is_function']
    )

    if positions is None:
        return None

    tail_words = [query_words[i] for i in range(len(query_words)) if i not in positions]
    tail = ' '.join(tail_words).strip()
    return tail if tail else ''


def _is_cross_script(w1: str, w2: str) -> bool:
    """Проверяет что слова в разных скриптах (кириллица vs латиница)."""
    w1_cyr = any('\u0400' <= c <= '\u04ff' for c in w1)
    w1_lat = any('a' <= c <= 'z' for c in w1)
    w2_cyr = any('\u0400' <= c <= '\u04ff' for c in w2)
    w2_lat = any('a' <= c <= 'z' for c in w2)
    return (w1_cyr and w2_lat) or (w1_lat and w2_cyr)


def _unordered_match(
    query_words, query_lemmas,
    seed_words, seed_lemmas, seed_is_function
):
    """
    Шаг 3: Неупорядоченный матч (двухпроходный).

    Проход 1: точное совпадение + леммы (надёжно).
    Проход 2: кросс-скрипт ТОЛЬКО для оставшихся + только контентные слова.

    Returns: set позиций найденных seed-слов, или None если не все найдены.
    """
    used_positions = set()
    matched_seed_idx = set()

    # Проход 1: точное + лемма
    for si in range(len(seed_words)):
        sw = seed_words[si]
        sl = seed_lemmas[si]
        for qi in range(len(query_words)):
            if qi in used_positions:
                continue
            if query_words[qi] == sw or query_lemmas[qi] == sl:
                used_positions.add(qi)
                matched_seed_idx.add(si)
                break

    # Проход 2: кросс-скрипт для НЕнайденных seed-слов
    # Служебные слова не матчим cross-script (используем pre-computed флаг)
    for si in range(len(seed_words)):
        if si in matched_seed_idx:
            continue
        if seed_is_function[si]:
            continue  # служебное слово — не матчим cross-script
        sw = seed_words[si]
        for qi in range(len(query_words)):
            if qi in used_positions:
                continue
            if _is_cross_script(sw, query_words[qi]):
                used_positions.add(qi)
                matched_seed_idx.add(si)
                break

    if len(matched_seed_idx) == len(seed_words):
        return used_positions
    return None


def _extract_partial_match(
    query_words: list, query_lemmas: list, seed_ctx: dict
):
    """
    Шаг 4: Частичный матч — допускаем пропуск ОДНОГО НЕконтентного слова seed'а.

    Условия:
    1. Не найдено МАКСИМУМ 1 слово из seed'а
    2. Пропущенное слово — НЕ контентное (не NOUN, VERB, ADJF, ADJS, INFN, PRTF, PRTS)
    3. Минимум 2 слова seed'а найдены

    "можно принимать нимесил после алкоголя" + seed "как принимать нимесил"
    → "как" пропущено (ADVB, не контентное) → разрешаем
    → хвост = "можно после алкоголя"

    "как принимать немозол" + seed "как принимать нимесил"
    → "нимесил" пропущено (NOUN, контентное) → НЕ разрешаем → None
    """
    seed_words = seed_ctx['words']
    seed_lemmas = seed_ctx['lemmas']
    seed_pos = seed_ctx['pos']
    seed_is_number = seed_ctx['is_number']
    seed_is_function = seed_ctx['is_function']

    # Минимум 3 слова в seed (иначе частичный матч слишком рискованный)
    if len(seed_words) < 3:
        return None

    # POS контентных слов
    CONTENT_POS = {'NOUN', 'VERB', 'ADJF', 'ADJS', 'INFN', 'PRTF', 'PRTS'}

    # Пробуем unordered match для каждого подмножества seed минус 1 слово
    for skip_idx in range(len(seed_words)):
        skip_pos = seed_pos[skip_idx]
        is_number = seed_is_number[skip_idx]

        if skip_pos in CONTENT_POS or is_number:
            continue  # контентное/числовое — нельзя пропускать

        # Формируем seed без пропущенного слова
        partial_seed_words = [w for i, w in enumerate(seed_words) if i != skip_idx]
        partial_seed_lemmas = [l for i, l in enumerate(seed_lemmas) if i != skip_idx]
        partial_seed_is_function = [f for i, f in enumerate(seed_is_function) if i != skip_idx]

        positions = _unordered_match(
            query_words, query_lemmas,
            partial_seed_words, partial_seed_lemmas, partial_seed_is_function
        )

        if positions is not None and len(positions) >= 2:
            tail_words = [
                query_words[i] for i in range(len(query_words))
                if i not in positions
            ]

            # === Фикс: предлог-замена ===
            # Если пропустили PREP из seed ("на") и tail начинается с PREP ("для", "в")
            # → это замена предлога, а не хвост → strip
            if skip_pos == 'PREP' and tail_words:
                first_tail_parse = morph.parse(tail_words[0])[0]
                if first_tail_parse.tag.POS == 'PREP':
                    tail_words = tail_words[1:]

            tail = ' '.join(tail_words).strip()
            return tail if tail else ''

    return None


# ==================== ТЕСТ ====================

if __name__ == "__main__":
    from shared_morph import morph as _m  # noqa (для прямого запуска)

    tests = [
        # seed, query, expected_tail

        # Точный seed
        ("ремонт стиральных машин", "ремонт стиральных машин", ""),
        ("ремонт стиральных машин", "ремонт стиральных машин киев", "киев"),
        ("ремонт стиральных машин", "киев ремонт стиральных машин", "киев"),

        # Предлоги перед seed'ом
        ("ремонт стиральных машин", "мастер по ремонт стиральных машин", "мастер"),
        ("ремонт стиральных машин", "запчасти для ремонт стиральных машин", "запчасти"),
        ("ремонт стиральных машин", "для ремонт стиральных машин", ""),

        # Нечёткий — вставки между словами seed'а
        ("как принимать нимесил", "как правильно принимать нимесил", "правильно"),
        ("как принимать нимесил", "как долго можно принимать нимесил", "долго можно"),
        ("как принимать нимесил", "как долго принимать нимесил", "долго"),
        ("как принимать нимесил", "как правильно принимать нимесил в порошке", "правильно в порошке"),
        ("как принимать нимесил", "как нужно принимать порошок нимесил", "нужно порошок"),
        ("как принимать нимесил", "как принимать нимесил порошок", "порошок"),

        # Seed отсутствует
        ("ремонт стиральных машин", "купить холодильник", None),
        ("как принимать нимесил", "купить холодильник", None),

        # Аккумулятор на скутер (предлог "на" внутри seed'а)
        ("аккумулятор на скутер", "аккумулятор на скутер цена", "цена"),
        ("аккумулятор на скутер", "как зарядить аккумулятор на скутер", "как зарядить"),

        # Шаг 4: Частичный матч (пропуск НЕконтентного слова)
        ("как принимать нимесил", "можно принимать нимесил после алкоголя", "можно после алкоголя"),
        ("как принимать нимесил", "сколько принимать нимесил", "сколько"),
        ("как принимать нимесил", "сколько дней принимать нимесил", "сколько дней"),
        ("как принимать нимесил", "сколько раз принимать нимесил в день", "сколько раз в день"),
        # НЕ должен матчить — "нимесил" контентное, другой препарат
        ("как принимать нимесил", "как принимать немозол взрослым", None),
    ]

    print("ТЕСТ extract_tail")
    print("=" * 70)

    all_ok = True
    for seed, query, expected in tests:
        # Проверяем что seed_ctx работает
        ctx = build_seed_ctx(seed.lower().strip())
        result = extract_tail(query, seed, seed_ctx=ctx)
        ok = result == expected
        if not ok:
            all_ok = False
        status = "✅" if ok else "❌"
        print(f'{status} seed="{seed}" query="{query}"')
        if not ok:
            print(f'   GOT: "{result}" EXPECTED: "{expected}"')

    print(f'\n{"✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ" if all_ok else "❌ ЕСТЬ ОШИБКИ"}')
