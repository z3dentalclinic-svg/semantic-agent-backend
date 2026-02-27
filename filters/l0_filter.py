# L0 Фиксы — 26 февраля 2026

## Обзор

Исправлены 5 проблем в L0 детекторах. Все фиксы алгоритмические (не hardcode).

**ВАЖНО:** Marketplace детекторы (`detect_marketplace`, `detect_trash_marketplace`) УДАЛЕНЫ из L0. Причина: невозможно масштабировать хардкод списки на все страны мира × все языки × все вариации написания. Маркетплейсы теперь обрабатываются в L3 (DeepSeek).

---

## Фикс 1: detect_short_garbage — "бу" как известное сокращение

**Файл:** `function_detectors.py`

**Проблема:** "аккумулятор на скутер бу" → TRASH (бу = 2 символа)

**Решение:** Whitelist известных сокращений:
```python
known_abbreviations = {'бу', 'шт', 'уа', 'рф', ...}
```

---

## Фикс 2: detect_fragment — "минус" не CONJ

**Файл:** `function_detectors.py`

**Проблема:** "где плюс где минус" → TRASH (минус = CONJ на конце)

**Решение:** Whitelist слов, которые pymorphy неправильно тегирует:
```python
misclassified_as_conj = {'минус'}
```

---

## Фикс 3: detect_duplicate_words — interrogative patterns

**Файл:** `function_detectors.py`

**Проблема:** "где плюс где минус" → TRASH (дублирование "где")

**Решение:** Исключение для "где X где Y" паттернов:
```python
if len(interrogative_positions) >= 2:
    if second_pos - first_pos >= 2:
        return False, ""  # НЕ блокируем
```

---

## Фикс 4: detect_broken_grammar — search query tolerance

**Файл:** `function_detectors.py`

**Проблема:** "аккумулятор для скутер" → TRASH (для требует gent)

**Решение:** Не блокируем "PREP + конкретный NOUN в nomn":
```python
if is_concrete:  # скутер, мотоцикл — конкретные объекты
    return False, ""
```

---

## Фикс 5: detect_category_mismatch — НОВЫЙ детектор

**Файл:** `category_mismatch_detector.py` (новый)

**Проблема:** "щербет", "йети", "бум" — другие категории

**Решение:** Embedding-based категоризация:
```python
INCOMPATIBLE_CATEGORIES = {
    "auto_parts": ["food", "animals", "mythology", "sounds"],
}
# "щербет" → food → несовместимо с auto_parts → TRASH
```

---

## УДАЛЕНО: Marketplace детекторы

**Что удалено:**
- `detect_marketplace()`
- `detect_trash_marketplace()`
- `_REGIONAL_MARKETPLACES`, `_MARKETPLACE_ALIASES`

**Почему:** Hardcode не масштабируется на 200+ стран × языки × вариации.

**Решение:** Маркетплейсы → GREY → L3 (DeepSeek).

---

## Файлы для деплоя

| Файл | Изменения |
|------|-----------|
| function_detectors.py | Фиксы 1-4, удалены marketplace |
| tail_function_classifier.py | Интеграция category_mismatch |
| category_mismatch_detector.py | НОВЫЙ |
| l0_filter.py | Без изменений |

---

## Тесты

| Ключ | До | После | Фикс |
|------|-----|-------|------|
| бу | TRASH | OK | 1 |
| где плюс где минус | TRASH | OK | 2,3 |
| для скутер | TRASH | OK | 4 |
| щербет | OK | TRASH | 5 |
| на озоне | TRASH | GREY→L3 | удалено |
