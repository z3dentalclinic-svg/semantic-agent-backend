"""
pre_filter.py — санитарная очистка парсинга 1.
Применяется ДО умного классификатора.
Убирает очевидный брак парсера: дубли seed'а, повторы слов.
Быстрый, без зависимостей.
"""

import re


def normalize(text: str) -> str:
    """Lowercase + убрать лишние пробелы."""
    return ' '.join(text.lower().split())


def pre_filter(query: str, seed: str) -> tuple:
    """
    Проверяет запрос на очевидный брак парсера.
    
    Returns:
        (is_trash: bool, reason: str or None)
        
        True  → мусор, не передавать в классификатор
        False → чистый, передать дальше
    """
    q = normalize(query)
    s = normalize(seed)
    
    # 1. Seed встречается 2+ раз: "ремонт пылесосов ремонт пылесосов"
    if q.count(s) >= 2:
        return True, "дубль seed целиком"

    # 1b. Однобуквенный шум ВНУТРИ seed-контекста.
    #
    # Google Autocomplete иногда досыпает однобуквенные токены между словами
    # запроса как артефакт генерации ("услуги а юриста лондон цена",
    # "услуги б юриста лондоне"). Эти буквы не несут смысла — это шум парсера.
    #
    # Правило размещено до извлечения tail (шаг 2), потому что вставка буквы
    # между словами seed разрывает литеральный match "seed in query", и без
    # этой проверки ключ проходит дальше как будто seed не найден.
    #
    # ВАЖНО: блокируем ТОЛЬКО буквы 'а' и 'б' (а также латинские 'a', 'b').
    # Остальные однобуквенные русские токены могут быть значимыми словами:
    #   — в, у, с, к, о — предлоги
    #   — и — союз
    #   — я — местоимение
    #   — ж — частица
    # Блокировать их опасно: "услуги в юриста лондоне" может быть разорванной
    # конструкцией "услуги В-юриста лондон" или артефактом где В ≠ шум.
    #
    # ИСКЛЮЧЕНИЕ для 'б у' / 'у б': биграмма из букв 'б' и 'у' рядом —
    # устойчивое сокращение "б/у" (бывшее в употреблении), валидный
    # коммерческий термин в e-commerce ("купить айфон 16 б у",
    # "купить айфон 16 про макс б у"). Блокировать нельзя.
    #
    # Срабатывает когда:
    #   — все слова seed присутствуют в query (с префикс-match для падежей)
    #   — в query есть одиночный токен ИЗ SAFE_NOISE_LETTERS
    #   — этот токен НЕ часть биграммы 'б у' / 'у б'
    SAFE_NOISE_LETTERS = {'а', 'б', 'a', 'b'}
    q_words = q.split()

    def _is_b_u_abbreviation(idx: int) -> bool:
        """Проверяет: токен на позиции idx — часть 'б у' (рус. б/у)."""
        token = q_words[idx]
        # только русская 'б'
        if token != 'б':
            return False
        # 'б' + следующее слово 'у'
        if idx + 1 < len(q_words) and q_words[idx + 1] == 'у':
            return True
        # 'у' + предыдущее 'б' (для паттерна 'у б', хотя редкий)
        if idx - 1 >= 0 and q_words[idx - 1] == 'у':
            return True
        return False

    noise_letter = None
    noise_idx = None
    for i, w in enumerate(q_words):
        if w in SAFE_NOISE_LETTERS:
            # Защита для 'б у'
            if _is_b_u_abbreviation(i):
                continue
            noise_letter = w
            noise_idx = i
            break
    if noise_letter:
        s_words_list = s.split()
        seed_fully_present = all(
            _seed_word_in_query(sw, q_words) for sw in s_words_list
        )
        if seed_fully_present:
            return True, f"однобуквенный шум '{noise_letter}' в seed-контексте"

    # 2. Извлекаем хвост
    if s not in q:
        return False, None

    # Word-boundary guard: seed найден как подстрока, но граница символа
    # может не совпадать с границей слова. Пример: seed="купить айфон 16",
    # query="купить айфон 16е" → split даёт tail="е" (одинокая буква),
    # что ниже блокируется правилом #6 как "одиночный символ". Но на
    # самом деле "16е" — цельный артикул/модель, а не seed+буква.
    #
    # Аналогично для любой ситуации где после seed идёт буква или цифра:
    #   — "16е", "16e", "16gb", "16s", "16x" (цифро-буквенные модели)
    #   — "160", "1600", "2016" (другие числа начинающиеся с 16)
    #
    # Алгоритм: находим все позиции seed в query и проверяем хотя бы одну
    # с корректной word-boundary справа. Если ни одна не корректна —
    # значит seed не найден как отдельный токен, возвращаем False (не TRASH).
    # Классификатор/extractor разберутся дальше.
    def _seed_has_word_boundary(q_text: str, s_text: str) -> bool:
        import re as _re
        # \b на границе seed-подстроки: после последнего символа seed
        # должна быть НЕ буква и НЕ цифра (или конец строки)
        for m in _re.finditer(_re.escape(s_text), q_text):
            end_pos = m.end()
            # Конец строки = OK
            if end_pos >= len(q_text):
                return True
            # Следующий символ НЕ буква и НЕ цифра = OK (пробел, -, /, . и т.д.)
            next_ch = q_text[end_pos]
            if not (next_ch.isalpha() or next_ch.isdigit()):
                return True
        return False

    if not _seed_has_word_boundary(q, s):
        # Seed найден как подстрока, но НЕ как отдельный токен.
        # Не TRASH на этом уровне — пусть дальше разбирается extract_tail
        # и классификатор (они имеют свою логику обработки).
        return False, None

    parts = q.split(s, 1)
    before = parts[0].strip()
    after = parts[1].strip() if len(parts) > 1 else ""
    tail = ' '.join([before, after]).strip()
    
    if not tail:
        return False, None  # запрос = seed, это валидно
    
    # 3. Хвост = одно слово из seed: "ремонт пылесосов ремонт"
    seed_words = set(s.split())
    tail_words = tail.split()
    
    if len(tail_words) == 1 and tail_words[0] in seed_words:
        return True, f"эхо seed: '{tail_words[0]}'"
    
    # 4. Весь хвост состоит из слов seed: "ремонт пылесосов пылесосов ремонт"
    if all(w in seed_words for w in tail_words):
        return True, f"хвост целиком из слов seed: '{tail}'"
    
    # 5. Подряд одинаковые слова: "цветов цветов"
    for i in range(len(tail_words) - 1):
        if tail_words[i] == tail_words[i + 1]:
            return True, f"повтор подряд: '{tail_words[i]}'"
    
    # 6. Одиночный символ: "ремонт пылесосов а"
    if len(tail) == 1 and not tail.isdigit():
        return True, f"одиночный символ: '{tail}'"

    # 7. Токен-спецсимвол в хвосте: "*1", "#2", "**", "©" и т.п.
    # Мусор = нет букв И есть хотя бы один спецсимвол (не цифра).
    # Чистые цифры ("12", "100") — валидны: номера кварталов, адреса.
    #
    # ИСКЛЮЧЕНИЯ (валидные цифро-символьные токены, НЕ мусор):
    #   — "24/7", "12/24", "9-18", "10:00", "24×7", "24х7" — режим работы / время
    #   — "+1", "+7" — номера/коды с плюсом
    #   — "№1", "№2" — номера
    # Эти паттерны = цифра + разделитель + цифра ИЛИ префикс + цифры.
    # Без исключения "24/7" и т.п. блокируются, хотя это частые валидные абревиатуры.
    #
    # Разделители между цифрами: / - : × х x (slash, dash, colon,
    # юникод-умножение, кириллическое "х", латинское "x")
    for token in tail_words:
        has_letters = re.search(r'[а-яёіїєґa-z]', token)
        has_special = re.search(r'[^а-яёіїєґa-z0-9]', token)
        if has_letters or not has_special:
            continue
        # Валидные цифро-символьные паттерны:
        if (re.match(r'^\d+[/\-:×хx]\d+$', token)  # 24/7, 10:00, 9-18
                or re.match(r'^\+\d+$', token)       # +1
                or re.match(r'^№\d+$', token)):      # №1
            continue
        return True, f"токен-спецсимвол: '{token}'"

    return False, None


def _seed_word_in_query(seed_word: str, q_words: list) -> bool:
    """Проверяет присутствие seed-слова в списке слов query.

    Прямое совпадение или префикс-match (для косвенных падежей).
    Префикс-match — только для seed-слов >= 4 символов,
    чтобы короткие слова вроде "и", "в", "на" не давали ложных matches.

    Примеры:
      seed_word='лондон', q_words=['услуги','юриста','лондоне'] → True
        (лондон ~ лондоне префиксом, разница 1 символ)
      seed_word='лондон', q_words=['услуги','юриста','лондонианец'] → False
        (разница больше 3 символов — скорее всего не косвенный падеж)
      seed_word='в', q_words=['а','в','киеве'] → True (прямое)
    """
    if seed_word in q_words:
        return True
    if len(seed_word) < 4:
        return False
    # Префикс-match: сокращает лемматизацию
    # Максимальная длина косвенного падежа — лемма + ~3 символа
    # (дом → домами, киев → киевом/киевов, лондон → лондонами/лондоне)
    for qw in q_words:
        if qw.startswith(seed_word) and len(qw) <= len(seed_word) + 3:
            return True
    return False


# ═══════════════════════════════════════════════════════════════════
# WRAPPER для API: apply_pre_filter
# ═══════════════════════════════════════════════════════════════════

def apply_pre_filter(data: dict, seed: str) -> dict:
    """
    Применяет pre_filter к каждому query в data["keywords"]
    
    Args:
        data: dict с ключом "keywords" (list)
        seed: базовый запрос
    
    Returns:
        data с отфильтрованными keywords
    """
    if not data or "keywords" not in data:
        return data
    
    filtered_keywords = []
    blocked_reasons: dict = data.setdefault("_blocked_reasons", {})
    
    for item in data["keywords"]:
        # Поддержка строк и dict
        if isinstance(item, str):
            query = item
        elif isinstance(item, dict):
            query = item.get("query", "")
        else:
            continue
        
        # Проверка через pre_filter
        is_trash, reason = pre_filter(query, seed)
        
        if not is_trash:
            filtered_keywords.append(item)
        else:
            blocked_reasons[query.lower().strip()] = reason or ""
    
    # Обновляем данные
    data["keywords"] = filtered_keywords
    
    if "total_count" in data:
        data["total_count"] = len(filtered_keywords)
    if "count" in data:
        data["count"] = len(filtered_keywords)
    
    return data


# ==================== ТЕСТ ====================

if __name__ == "__main__":
    seed = "ремонт пылесосов"
    
    tests = [
        ("ремонт пылесосов ремонт", True),
        ("ремонт пылесосов пылесосов", True),
        ("ремонт пылесосов ремонт пылесосов", True),
        ("ремонт пылесосов пылесосов ремонт", True),
        ("доставка цветов цветов цветов", None),  # другой seed
        ("ремонт пылесосов а", True),
        ("ремонт пылесосов киев", False),
        ("ремонт пылесосов", False),
        ("ремонт пылесосов цена", False),
        ("ремонт пылесосов samsung", False),
    ]
    
    print("ТЕСТ pre_filter")
    print("=" * 50)
    
    for query, expected_trash in tests:
        is_trash, reason = pre_filter(query, seed)
        
        if expected_trash is None:
            print(f"⏭️  \"{query}\" → skip (другой seed)")
            continue
        
        ok = is_trash == expected_trash
        status = "✅" if ok else "❌"
        print(f"{status} \"{query}\" → {'TRASH' if is_trash else 'OK'}  {reason or ''}")
