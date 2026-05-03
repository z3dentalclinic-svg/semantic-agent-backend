"""Промпт для динамической кластеризации: только буквы по порядку + легенда."""

SYSTEM_PROMPT_TEMPLATE = """Ты — эксперт по контекстной рекламе и SEO. Твоя задача: сгруппировать поисковые запросы (хвосты) по смыслу (интенту).

Условия задачи:
1. Ты сам определяешь, сколько кластеров нужно для этого списка (обычно от 5 до 10).
2. Названия кластеров должны быть короткими (1-3 слова на {language}) и отражать суть.
3. Каждому кластеру ты присваиваешь уникальный латинский код: A, B, C, D...

Формат вывода — РОВНО ДВЕ СТРОКИ, без markdown, без пояснений, без пустых строк:

Строка 1 — РАЗМЕТКА: коды кластеров СТРОГО ПО ПОРЯДКУ хвостов от 1 до N, через запятую, без пробелов.
КРИТИЧНО: количество букв в разметке должно быть РОВНО {n_payloads} — столько же, сколько хвостов. Ни одной не пропусти, ни одной лишней. Если хвост #5 относится к кластеру B, то 5-й код в строке должен быть B.

Строка 2 — ЛЕГЕНДА: краткие имена для всех использованных кодов, через точку с запятой, в формате `КОД=имя`.

Пример ответа на 6 хвостов:
A,B,A,C,B,A
A=гео_город;B=коммерция;C=инфо_отзывы"""


USER_PROMPT_TEMPLATE = """Сид: {seed}
Регион: {region}
Язык запросов: {language}

Хвосты ({n}):
{payloads_text}"""


def build_prompts(seed: str, region: str, language: str, payloads: list[str]) -> tuple[str, str]:
    # Форматируем хвосты компактно через |
    payload_entries = []
    for i, p in enumerate(payloads, 1):
        payload_entries.append(f"{i}:{p}")
    
    payloads_text = "|".join(payload_entries)
    
    n = len(payloads)
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(language=language, n_payloads=n)
    user_prompt = USER_PROMPT_TEMPLATE.format(
        seed=seed,
        region=region,
        language=language,
        n=n,
        payloads_text=payloads_text
    )
    
    return system_prompt, user_prompt
