"""
tail_extractor.py — извлечение хвоста из поискового запроса.

Двухшаговая логика:
1. Точный split по seed-подстроке (быстро, надёжно)
2. Нечёткий поиск: слова seed'а по порядку с допуском вставок
   "как правильно принимать нимесил" → seed найден, хвост = "правильно"

Предлоги ПЕРЕД seed'ом отсекаются через POS-тег (не стоп-слова).
"""

import pymorphy3

morph = pymorphy3.MorphAnalyzer()


def _strip_trailing_preps(words: list) -> list:
    """Убирает предлоги с конца списка слов (они связаны с seed'ом)."""
    while words:
        p = morph.parse(words[-1])[0]
        if p.tag.POS == 'PREP':
            words.pop()
        else:
            break
    return words


def extract_tail(query: str, seed: str):
    """
    Извлекает хвост запроса относительно seed'а.
    
    Шаг 1 (точный): split по подстроке seed.
    Шаг 2 (нечёткий): слова seed'а по порядку, вставки → хвост.
    Шаг 3 (неупорядоченный): слова seed'а в любом порядке + кросс-скрипт.
    
    Returns:
        str:  Хвост (может быть пустым если запрос ≈ seed)
        None: Seed не найден в запросе
    """
    q = query.lower().strip()
    s = seed.lower().strip()
    
    # === Шаг 1: Точный split ===
    if s in q:
        parts = q.split(s, 1)
        before = parts[0].strip()
        after = parts[1].strip() if len(parts) > 1 else ""
        
        if before:
            before = ' '.join(_strip_trailing_preps(before.split()))
        
        return ' '.join([before, after]).strip()
    
    # === Шаг 2: Нечёткий поиск (по порядку) ===
    result = _extract_fuzzy_ordered(q, s)
    if result is not None:
        return result
    
    # === Шаг 3: Неупорядоченный поиск (fallback) ===
    result = _extract_tail_unordered(q, s)
    if result is not None:
        return result
    
    # === Шаг 4: Частичный матч (допускаем пропуск НЕконтентного слова) ===
    return _extract_partial_match(q, s)
    
def _extract_fuzzy_ordered(q: str, s: str):
    """
    Шаг 2: Нечёткий поиск (seed-слова по порядку + кросс-скрипт).
    Returns: tail str or None.
    """
    # Ищем каждое слово seed'а по порядку в запросе.
    seed_words = s.split()
    query_words = q.split()
    
    # Лемматизируем seed и query
    seed_lemmas = [morph.parse(w)[0].normal_form for w in seed_words]
    query_lemmas = [morph.parse(w)[0].normal_form for w in query_words]
    
    seed_positions = []
    search_from = 0
    
    for i_sw, (sw, sl) in enumerate(zip(seed_words, seed_lemmas)):
        found = False
        for i in range(search_from, len(query_words)):
            # Точное совпадение ИЛИ совпадение лемм
            if query_words[i] == sw or query_lemmas[i] == sl:
                seed_positions.append(i)
                search_from = i + 1
                found = True
                break
        if not found:
            # === Шаг 2.5: Кросс-скриптовый мост ===
            # Если seed-слово кириллическое а query-слово латинское (или наоборот)
            # в позиции где seed-слово ДОЛЖНО быть → считаем матч.
            # "айфон" (кир) ↔ "iphone" (лат) — разный скрипт, та же позиция.
            #
            # ОГРАНИЧЕНИЕ: служебные слова (PREP, CONJ, PRCL) НЕ матчатся
            # через cross-script. "на" ≠ "segway", "и" ≠ "gt3".
            # Cross-script мост — только для контентных слов (бренды, модели).
            sw_parse = morph.parse(sw)[0]
            sw_is_function = sw_parse.tag.POS in ('PREP', 'CONJ', 'PRCL', 'INTJ')
            
            if not sw_is_function:
                sw_is_cyr = any('\u0400' <= c <= '\u04ff' for c in sw)
                sw_is_lat = any('a' <= c <= 'z' for c in sw)
                
                for i in range(search_from, len(query_words)):
                    qw = query_words[i]
                    qw_is_cyr = any('\u0400' <= c <= '\u04ff' for c in qw)
                    qw_is_lat = any('a' <= c <= 'z' for c in qw)
                    
                    # Скрипты РАЗНЫЕ → кросс-скриптовая замена
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
    # "купить гтх в 3060 ti" → "в" между позициями 1 и 3 → артефакт
    # "купить гтх 3060 в украине" → "в" после позиции 2 → реальный хвост
    sorted_positions = sorted(seed_positions)
    browse_artifacts = set()
    for pi in range(len(sorted_positions) - 1):
        pos_left = sorted_positions[pi]
        pos_right = sorted_positions[pi + 1]
        # Проверяем слова между двумя соседними seed-позициями
        for mid in range(pos_left + 1, pos_right):
            word = query_words[mid]
            # Одиночная буква (не цифра) → browse-артефакт
            if len(word) == 1 and word.isalpha():
                browse_artifacts.add(mid)
    
    tail_words = [query_words[i] for i in range(len(query_words))
                  if i not in seed_idx and i not in browse_artifacts]
    
    tail = ' '.join(tail_words).strip()
    return tail if tail else ''


def _extract_tail_unordered(q: str, s: str):
    """
    Шаг 3: Неупорядоченный поиск seed'а в запросе.
    Вызывается ТОЛЬКО если шаги 1+2 не нашли seed.
    
    "iphone 17 купить в швейцарии" + seed "купить айфон 17"
    → купить(2) + iphone↔айфон(0) + 17(1) → хвост = "в швейцарии"
    """
    seed_words = s.lower().split()
    query_words = q.lower().split()
    
    seed_lemmas = [morph.parse(w)[0].normal_form for w in seed_words]
    query_lemmas = [morph.parse(w)[0].normal_form for w in query_words]
    
    positions = _unordered_match(query_words, query_lemmas, seed_words, seed_lemmas)
    
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


def _unordered_match(query_words, query_lemmas, seed_words, seed_lemmas):
    """
    Шаг 3: Неупорядоченный матч (двухпроходный).
    
    Проход 1: точное совпадение + леммы (надёжно).
    Проход 2: кросс-скрипт ТОЛЬКО для оставшихся (если seed-слово кирил., а query-слово лат. → мост).
    
    Returns: set позиций найденных seed-слов, или None если не все найдены.
    """
    used_positions = set()
    matched_seed_idx = set()
    
    # Проход 1: точное + лемма
    for si, (sw, sl) in enumerate(zip(seed_words, seed_lemmas)):
        for qi in range(len(query_words)):
            if qi in used_positions:
                continue
            if query_words[qi] == sw or query_lemmas[qi] == sl:
                used_positions.add(qi)
                matched_seed_idx.add(si)
                break
    
    # Проход 2: кросс-скрипт для НЕнайденных seed-слов
    # ОГРАНИЧЕНИЕ: служебные слова (PREP, CONJ, PRCL) не матчатся cross-script
    for si, (sw, sl) in enumerate(zip(seed_words, seed_lemmas)):
        if si in matched_seed_idx:
            continue
        sw_parse = morph.parse(sw)[0]
        if sw_parse.tag.POS in ('PREP', 'CONJ', 'PRCL', 'INTJ'):
            continue  # служебное слово — не матчим cross-script
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


def _extract_partial_match(q: str, s: str):
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
    seed_words = s.lower().split()
    query_words = q.lower().split()
    
    # Минимум 3 слова в seed (иначе частичный матч слишком рискованный)
    if len(seed_words) < 3:
        return None
    
    seed_lemmas = [morph.parse(w)[0].normal_form for w in seed_words]
    query_lemmas = [morph.parse(w)[0].normal_form for w in query_words]
    
    # POS-теги контентных слов
    CONTENT_POS = {'NOUN', 'VERB', 'ADJF', 'ADJS', 'INFN', 'PRTF', 'PRTS'}
    
    # Пробуем unordered match для каждого подмножества seed минус 1 слово
    for skip_idx in range(len(seed_words)):
        # Проверяем POS пропускаемого слова — контентное нельзя пропускать
        skip_word = seed_words[skip_idx]
        skip_parse = morph.parse(skip_word)[0]
        skip_pos = skip_parse.tag.POS
        
        # Числа тоже контентные (номер модели, количество и т.д.)
        is_number = 'NUMB' in str(skip_parse.tag) or skip_word.isdigit()
        
        if skip_pos in CONTENT_POS or is_number:
            continue
        
        # Формируем seed без пропущенного слова
        partial_seed_words = [w for i, w in enumerate(seed_words) if i != skip_idx]
        partial_seed_lemmas = [l for i, l in enumerate(seed_lemmas) if i != skip_idx]
        
        # Пробуем unordered match с частичным seed
        positions = _unordered_match(query_words, query_lemmas, partial_seed_words, partial_seed_lemmas)
        
        if positions is not None and len(positions) >= 2:
            tail_words = [query_words[i] for i in range(len(query_words)) if i not in positions]
            
            # === Фикс: предлог-замена ===
            # Если пропустили PREP из seed ("на") и tail начинается с PREP ("для", "в")
            # → это замена предлога, а не хвост.
            # "аккумулятор для скутер 12 вольт" → skip "на" → tail ["для", "12", "вольт"]
            # "для" заменяет "на" → strip → tail ["12", "вольт"]
            if skip_pos == 'PREP' and tail_words:
                first_tail_parse = morph.parse(tail_words[0])[0]
                if first_tail_parse.tag.POS == 'PREP':
                    tail_words = tail_words[1:]
            
            tail = ' '.join(tail_words).strip()
            return tail if tail else ''
    
    return None


# ==================== ТЕСТ ====================

if __name__ == "__main__":
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
        result = extract_tail(query, seed)
        ok = result == expected
        if not ok:
            all_ok = False
        status = "✅" if ok else "❌"
        print(f'{status} seed="{seed}" query="{query}"')
        if not ok:
            print(f'   GOT: "{result}" EXPECTED: "{expected}"')
    
    print(f'\n{"✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ" if all_ok else "❌ ЕСТЬ ОШИБКИ"}')
