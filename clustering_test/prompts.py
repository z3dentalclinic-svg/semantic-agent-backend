"""Промпт для двухэтапной кластеризации: разметка → легенда."""

SYSTEM_PROMPT_TEMPLATE = """Ты — эксперт по контекстной рекламе и SEO. Твоя задача: сгруппировать поисковые запросы (хвосты) по смыслу (интенту).

Условия задачи:
1. Используй СТРОГО НЕ БОЛЕЕ 8 кластеров. Коды кластеров — латинские буквы A, B, C, D, E, F, G, H.
2. Иди строго по порядку хвостов от 1 до N и присваивай каждому код кластера.
3. Когда вся разметка готова — на новой строке выдай легенду: краткое название (1-3 слова на {language}) для каждого использованного кода.

Формат вывода — РОВНО ДВЕ СТРОКИ:
- Строка 1: разметка через запятую, без пробелов: `1:A,2:B,3:A,4:C,...`
- Строка 2: легенда через точку с запятой: `A=имя_первого;B=имя_второго;...`

Без markdown, без пояснений, без пустых строк между разметкой и легендой.

Пример ответа на 6 хвостов:
1:A,2:B,3:A,4:C,5:B,6:A
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
    
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(language=language)
    user_prompt = USER_PROMPT_TEMPLATE.format(
        seed=seed,
        region=region,
        language=language,
        n=len(payloads),
        payloads_text=payloads_text
    )
    
    return system_prompt, user_prompt
