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


# Feature flag Fix 2 — geo-first tail extraction для CYR-сидов.
# Активируется только если seed без LATN и без цифр, и geo_db+target_country
# переданы в extract_tail. Откат: ENABLE_GEO_FIRST = False (без удаления кода).
ENABLE_GEO_FIRST = True


def _all_top_lemmas(word: str) -> set:
    """
    Возвращает множество лемм со score ≥ 0.9 × top_score.

    Решает "лемма-ловушку" pymorphy3: слово "цветов" имеет две равновероятные
    гипотезы ('цвет' 0.5, 'цветок' 0.5), а morph.parse(w)[0] возвращает только
    первую. Если seed содержит 'цветов' (→ 'цвет'), а kw содержит 'цветы' (→
    'цветок'), они не матчатся через parse[0].normal_form, хотя грамматически
    идентичны. Multi-lemma сравнение через пересечение множеств лемм решает это.

    Порог 0.9 × top — консервативный: берём только гипотезы почти равные лучшей.
    Для однозначных слов (score top=0.6, alt=0.4) вернётся одна лемма. Для
    неоднозначных (два score по 0.5) — обе.
    """
    parses = morph.parse(word)
    if not parses:
        return {word}
    top_score = parses[0].score
    threshold = top_score * 0.9
    return {p.normal_form for p in parses if p.score >= threshold}


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
          'lemmas'    : list[str]  — первая лемма каждого слова (normal_form)
          'lemmas_all': list[set[str]] — все леммы с score ≥ 0.9×top, для multi-lemma match
          'pos'       : list[str]  — POS-теги (tag.POS)
          'is_function': list[bool] — True если слово служебное (PREP/CONJ/PRCL/INTJ)
          'is_number' : list[bool] — True если слово — число (NUMB или isdigit)
          'is_cyr'    : list[bool] — True если слово содержит кириллицу
          'is_lat'    : list[bool] — True если слово содержит латиницу

    Backward-compat: поле 'lemmas' (list[str]) сохранено для внешнего кода
    (l0_filter._parse_seed_for_sanity, _short_seed_lemmas_set). Новое поле
    'lemmas_all' используется только внутри tail_extractor для multi-lemma match.
    """
    words = seed_lower.split()
    parses = [morph.parse(w)[0] for w in words]
    return {
        'words': words,
        'lemmas': [p.normal_form for p in parses],
        'lemmas_all': [_all_top_lemmas(w) for w in words],
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


def _extract_geo_first(
    query_words: list,
    query_lemmas_all: list,
    seed_ctx: dict,
    geo_db: dict,
    target_country: str,
):
    """
    Шаг 0 (Fix 2): Geo-first tail extraction для CYR-сидов.

    Работает только для чистых CYR-сидов (без LATN и без цифр). Если в ключе
    есть гео-токен target_country и хотя бы один content-токен seed присутствует
    в не-гео части — возвращаем tail = все не-seed токены (включая гео).

    Назначение: покрыть потери на CYR service-сидах типа "доставка цветов на дом",
    где пользователи естественно опускают часть seed ("доставка цветов киев"),
    и старая логика Шагов 1-4 помечает ключ как no-tail GREY.

    Guards:
      - гео принадлежит target_country (foreign гео → early exit return None)
      - foreign district guard (задача "южное бутово" — биграммы и одиночные)
      - ≥1 seed-content-токен в не-гео части (multi-lemma match)
      - гео должен быть в tail (защитный — если попал в seed_matched, None)

    Returns: tail str или None если Шаг 0 не должен применяться.
    """
    if not query_words or not geo_db or not target_country:
        return None

    target = target_country.upper()
    target_lower = target_country.lower()
    words_lower = [w.lower() for w in query_words]
    # Первая лемма каждого query-слова — для гео-сканирования (лемматизированные
    # биграммы/триграммы). Multi-lemma версия (query_lemmas_all) используется
    # ниже для seed-content hits и seed_matched indices.
    first_lemmas = [
        next(iter(s)) if s else w
        for s, w in zip(query_lemmas_all, words_lower)
    ]

    # Ленивый импорт districts/countries из function_detectors (избежать cycle)
    try:
        from .function_detectors import (
            _DISTRICT_TO_CANONICAL,
            _DISTRICT_TO_COUNTRY,
            _is_foreign_district_name,
            _COUNTRIES,
        )
    except ImportError:
        _DISTRICT_TO_CANONICAL = {}
        _DISTRICT_TO_COUNTRY = {}
        _COUNTRIES = {}
        _is_foreign_district_name = lambda *a, **kw: False

    geo_indices = None  # set позиций в query где гео (single/bigram/trigram)

    # ── 1. ТРИГРАММЫ (longest-first) ──────────────────────────────────────
    for i in range(len(words_lower) - 2):
        raw3 = words_lower[i:i + 3]
        head3 = [first_lemmas[i]] + words_lower[i + 1:i + 3]
        bases = {' '.join(raw3), ' '.join(head3)}
        variants = set()
        for b in bases:
            variants.add(b)
            variants.add(b.replace(' ', '-'))
            variants.add(b.replace('-', ' '))
        foreign_hit = False
        for v in variants:
            if v in geo_db:
                if target in geo_db[v]:
                    geo_indices = {i, i + 1, i + 2}
                    break
                # foreign trigram — early exit
                foreign_hit = True
                break
        if foreign_hit:
            return None
        if geo_indices is not None:
            break

    # ── 2. БИГРАММЫ (longest-first, если триграммы не нашли) ──────────────
    if geo_indices is None:
        for i in range(len(words_lower) - 1):
            raw2 = words_lower[i:i + 2]
            head2 = [first_lemmas[i]] + words_lower[i + 1:i + 2]
            full2 = first_lemmas[i:i + 2]
            bases = {' '.join(raw2), ' '.join(head2), ' '.join(full2)}
            variants = set()
            for b in bases:
                variants.add(b)
                variants.add(b.replace(' ', '-'))
                variants.add(b.replace('-', ' '))

            # FOREIGN DISTRICT guard (задача "южное бутово")
            for v in variants:
                if v in _DISTRICT_TO_CANONICAL:
                    country = _DISTRICT_TO_COUNTRY.get(v, '')
                    if country and country.lower() != target_lower:
                        return None  # foreign district — не применяем Шаг 0

            # Cities
            foreign_hit = False
            for v in variants:
                if v in geo_db:
                    if target in geo_db[v]:
                        geo_indices = {i, i + 1}
                        break
                    foreign_hit = True
                    break
            if foreign_hit:
                return None  # foreign bigram city — early exit
            if geo_indices is not None:
                break

            # Countries bigrams
            for v in variants:
                if v in _COUNTRIES:
                    if _COUNTRIES[v] == target:
                        geo_indices = {i, i + 1}
                        break
                    foreign_hit = True
                    break
            if foreign_hit:
                return None  # foreign country bigram
            if geo_indices is not None:
                break

    # ── 3. ОДИНОЧНЫЕ ТОКЕНЫ ───────────────────────────────────────────────
    if geo_indices is None:
        skip_pos = {'CONJ', 'PREP', 'PRCL', 'INTJ'}
        for i, (word, lem) in enumerate(zip(words_lower, first_lemmas)):
            parsed = morph.parse(word)[0]
            if parsed.tag.POS in skip_pos:
                continue

            # FOREIGN DISTRICT guard для одиночных
            if _is_foreign_district_name(word, target_country):
                continue
            word_is_target_city = (
                word in geo_db and target in geo_db[word]
            )
            if (not word_is_target_city
                    and len(word) >= 5 and lem != word
                    and _is_foreign_district_name(lem, target_country)):
                continue

            # Город (raw)
            if word in geo_db:
                if target in geo_db[word]:
                    geo_indices = {i}
                    break
                continue  # foreign single — не блокируем, идём дальше
            # Город (лемма)
            if len(word) >= 5 and lem != word and lem in geo_db:
                if target in geo_db[lem]:
                    geo_indices = {i}
                    break
                continue
            # Страны
            if word in _COUNTRIES:
                if _COUNTRIES[word] == target:
                    geo_indices = {i}
                    break
                continue
            if len(word) >= 5 and lem != word and lem in _COUNTRIES:
                if _COUNTRIES[lem] == target:
                    geo_indices = {i}
                    break
                continue

    if geo_indices is None:
        return None  # гео не найден → Шаг 0 не применяется, идём на Шаг 1

    # ── Guard: ≥1 seed-content-токен в не-гео части ───────────────────────
    seed_lemmas_all = seed_ctx['lemmas_all']
    seed_is_function = seed_ctx['is_function']
    seed_is_number = seed_ctx['is_number']
    seed_words_lower = [w.lower() for w in seed_ctx['words']]

    non_geo_indices_list = [
        i for i in range(len(words_lower)) if i not in geo_indices
    ]
    non_geo_lemmas_all = [query_lemmas_all[i] for i in non_geo_indices_list]
    non_geo_words = [words_lower[i] for i in non_geo_indices_list]

    seed_content_hits = 0
    for s_idx in range(len(seed_words_lower)):
        if seed_is_function[s_idx] or seed_is_number[s_idx]:
            continue
        sw = seed_words_lower[s_idx]
        sl_set = seed_lemmas_all[s_idx]
        for ngi, ngw in enumerate(non_geo_words):
            if ngw == sw or (sl_set & non_geo_lemmas_all[ngi]):
                seed_content_hits += 1
                break

    if seed_content_hits == 0:
        return None  # seed совсем не представлен

    # ── Собираем tail: все не-seed токены query (multi-lemma match) ───────
    seed_matched_indices = set()
    used_query_indices = set()
    for s_idx in range(len(seed_words_lower)):
        sw = seed_words_lower[s_idx]
        sl_set = seed_lemmas_all[s_idx]
        for qi in range(len(words_lower)):
            if qi in used_query_indices:
                continue
            if words_lower[qi] == sw or (sl_set & query_lemmas_all[qi]):
                seed_matched_indices.add(qi)
                used_query_indices.add(qi)
                break

    # Защитный guard: гео должен остаться в tail (главный сигнал detect_geo)
    if not (geo_indices - seed_matched_indices):
        return None

    tail_indices = [
        i for i in range(len(words_lower)) if i not in seed_matched_indices
    ]
    tail_words = [query_words[i] for i in tail_indices]
    tail = ' '.join(tail_words).strip()

    return tail if tail else None


def extract_tail(
    query: str,
    seed: str,
    seed_ctx: dict = None,
    geo_db: dict = None,
    target_country: str = None,
):
    """
    Извлекает хвост запроса относительно seed'а.

    Шаг 0 (Fix 2): Geo-first для CYR-сидов без LATN/NUMB.  [NEW]
    Шаг 1 (точный): split по подстроке seed.
    Шаг 2 (нечёткий): слова seed'а по порядку, вставки → хвост.
    Шаг 3 (неупорядоченный): слова seed'а в любом порядке + кросс-скрипт.
    Шаг 4 (частичный): допускаем пропуск одного НЕконтентного слова seed'а.

    Args:
        query: поисковый запрос
        seed: базовый запрос
        seed_ctx: pre-computed контекст seed (из build_seed_ctx).
                  Если None — вычисляется внутри (backward-compat, медленнее).
        geo_db: база городов {name_lower: {country_codes}} (опциональна, для Шага 0)
        target_country: код целевой страны (опциональна, для Шага 0)

    Returns:
        str:  Хвост (может быть пустым если запрос ≈ seed)
        None: Seed не найден в запросе
    """
    q = query.lower().strip()
    s = seed.lower().strip()

    # Строим seed_ctx если не передан (backward-compat / тесты)
    if seed_ctx is None:
        seed_ctx = build_seed_ctx(s)

    # === Шаг 0 (Fix 2): Geo-first для CYR-сидов без LATN/NUMB ===
    # Активируется только при всех условиях одновременно:
    #   - ENABLE_GEO_FIRST = True
    #   - geo_db и target_country переданы (не None и не пустые)
    #   - seed не содержит латиницу (LATN guard — защита samsung/galaxy/s21)
    #   - seed не содержит цифры (NUMB guard — защита "айфон 16")
    # Внутри Шага 0 дополнительно:
    #   - foreign гео → early return None → падаем на Шаг 1 как было
    #   - гео не найден → return None → Шаг 1
    #   - seed не представлен → return None → Шаг 1
    if (ENABLE_GEO_FIRST and geo_db and target_country
            and not any(seed_ctx['is_lat'])
            and not any(seed_ctx['is_number'])):
        _query_words_pre = q.split()
        _query_lemmas_all_pre = [
            _all_top_lemmas(w) for w in _query_words_pre
        ]
        _geo_first_result = _extract_geo_first(
            _query_words_pre, _query_lemmas_all_pre,
            seed_ctx, geo_db, target_country,
        )
        if _geo_first_result is not None:
            return _geo_first_result

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
    # Multi-lemma: все гипотезы с score ≥ 0.9×top для каждого query-слова.
    # Используется через пересечение с seed_ctx['lemmas_all'] — решает
    # лемма-ловушку pymorphy3 (см. _all_top_lemmas).
    query_lemmas_all = [_all_top_lemmas(w) for w in query_words]

    # === Шаг 2: Нечёткий поиск (по порядку) ===
    result = _extract_fuzzy_ordered(query_words, query_lemmas, query_lemmas_all, seed_ctx)
    if result is not None:
        return result

    # === Шаг 3: Неупорядоченный поиск (fallback) ===
    result = _extract_tail_unordered(query_words, query_lemmas, query_lemmas_all, seed_ctx)
    if result is not None:
        return result

    # === Шаг 4: Частичный матч ===
    return _extract_partial_match(query_words, query_lemmas, query_lemmas_all, seed_ctx)


def _extract_fuzzy_ordered(
    query_words: list, query_lemmas: list, query_lemmas_all: list, seed_ctx: dict
):
    """
    Шаг 2: Нечёткий поиск (seed-слова по порядку + кросс-скрипт).

    Использует pre-computed seed_ctx (без дополнительных morph.parse для seed).
    query_words / query_lemmas / query_lemmas_all уже готовы (переданы из extract_tail).

    Multi-lemma match: совпадение считается если пересекаются МНОЖЕСТВА лемм
    seed-слова и query-слова. Это решает "лемма-ловушку" pymorphy3, когда у
    слова есть несколько равновероятных гипотез и parse[0] выбирает не ту
    лемму которая совпадёт с другим флексом того же слова.

    Returns: tail str or None.
    """
    seed_words = seed_ctx['words']
    seed_lemmas = seed_ctx['lemmas']
    seed_lemmas_all = seed_ctx['lemmas_all']
    seed_is_function = seed_ctx['is_function']
    seed_is_cyr = seed_ctx['is_cyr']
    seed_is_lat = seed_ctx['is_lat']

    seed_positions = []
    search_from = 0

    for i_sw in range(len(seed_words)):
        sw = seed_words[i_sw]
        sl_set = seed_lemmas_all[i_sw]
        found = False

        for i in range(search_from, len(query_words)):
            # Multi-lemma match: exact word OR пересечение множеств лемм.
            if query_words[i] == sw or (sl_set & query_lemmas_all[i]):
                seed_positions.append(i)
                search_from = i + 1
                found = True
                break

        if not found:
            # === Шаг 2.5: Кросс-скриптовый мост ===
            # Мост предназначен для транслитераций бренда/модели между скриптами:
            # seed='купить айфон' + kw='купить iphone 15' → айфон ↔ iphone.
            #
            # Guard: если слово seed (по точному совпадению или лемме) УЖЕ
            # присутствует в kw где-либо — мост НЕ активируется. Вместо этого
            # возвращаем None, чтобы Шаг 3 (unordered) смог найти слово на
            # реальной позиции. Без этого guard'а ordered-режим Шага 2
            # "застревает" на search_from, и cross-script ошибочно матчит
            # слово seed на случайный latin/cyr токен в хвосте.
            #
            # Пример без guard'а:
            #   seed='samsung galaxy s21 ремонт', kw='ремонт samsung galaxy s21 ultra'
            #   samsung→kw[1], galaxy→kw[2], s21→kw[3], search_from=4
            #   ремонт: kw[4]='ultra' — не лемма → cross-script ложно: ремонт↔ultra
            #   → tail='ремонт' (ложное seed_echo)
            #
            # С guard'ом:
            #   ремонт есть в kw[0] (точно) → Шаг 2 возвращает None →
            #   Шаг 3 unordered находит {0,1,2,3} → tail='ultra' ✓
            if (sw in query_words) or any(sl_set & ql for ql in query_lemmas_all):
                return None  # слово есть в kw → пусть unordered отработает
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
    query_words: list, query_lemmas: list, query_lemmas_all: list, seed_ctx: dict
):
    """
    Шаг 3: Неупорядоченный поиск seed'а в запросе.
    Вызывается ТОЛЬКО если шаги 1+2 не нашли seed.

    "iphone 17 купить в швейцарии" + seed "купить айфон 17"
    → купить(2) + iphone↔айфон(0) + 17(1) → хвост = "в швейцарии"
    """
    positions = _unordered_match(
        query_words, query_lemmas, query_lemmas_all,
        seed_ctx['words'], seed_ctx['lemmas'], seed_ctx['lemmas_all'],
        seed_ctx['is_function']
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
    query_words, query_lemmas, query_lemmas_all,
    seed_words, seed_lemmas, seed_lemmas_all, seed_is_function
):
    """
    Шаг 3: Неупорядоченный матч (двухпроходный).

    Проход 1: точное совпадение + multi-lemma match (надёжно).
    Проход 2: кросс-скрипт ТОЛЬКО для оставшихся + только контентные слова.

    Multi-lemma match: пересечение множеств лемм — см. _all_top_lemmas.

    Returns: set позиций найденных seed-слов, или None если не все найдены.
    """
    used_positions = set()
    matched_seed_idx = set()

    # Проход 1: точное + multi-lemma
    for si in range(len(seed_words)):
        sw = seed_words[si]
        sl_set = seed_lemmas_all[si]
        for qi in range(len(query_words)):
            if qi in used_positions:
                continue
            if query_words[qi] == sw or (sl_set & query_lemmas_all[qi]):
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
    query_words: list, query_lemmas: list, query_lemmas_all: list, seed_ctx: dict
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
    seed_lemmas_all = seed_ctx['lemmas_all']
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
        partial_seed_lemmas_all = [la for i, la in enumerate(seed_lemmas_all) if i != skip_idx]
        partial_seed_is_function = [f for i, f in enumerate(seed_is_function) if i != skip_idx]

        positions = _unordered_match(
            query_words, query_lemmas, query_lemmas_all,
            partial_seed_words, partial_seed_lemmas, partial_seed_lemmas_all,
            partial_seed_is_function
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
