"""
pre_filter.py — санитарная очистка парсинга.
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
    
    # 2. Извлекаем хвост
    if s not in q:
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
    
    return False, None


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
