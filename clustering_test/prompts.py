"""Промпт для динамической кластеризации на лету."""

SYSTEM_PROMPT_TEMPLATE = """Ты — эксперт по контекстной рекламе и SEO. Твоя задача: сгруппировать поисковые запросы (хвосты) по смыслу (интенту).

Условия задачи:
1. Ты сам определяешь, сколько кластеров нужно для этого списка (обычно от 5 до 10).
2. Имя кластера — строго ОДНО слово, язык: {language}.
3. Каждому кластеру ты присваиваешь уникальный латинский код: A, B, C, D...
4. Одна буква жёстко закрепляется за одним кластером. Повторно переопределять или менять имя буквы ЗАПРЕЩЕНО.

Формат вывода:
- Иди строго по порядку хвостов от 1 до N.
- Ответ верни СТРОГО ОДНОЙ СТРОКОЙ через запятую, без пробелов, без markdown и без переносов.
- Когда код кластера используется ВПЕРВЫЕ, после него в скобках укажи имя кластера. При повторном использовании кода пиши ТОЛЬКО букву.

Пример ответа на 6 хвостов:
1:A(гео),2:B(цена),3:A,4:C(бренд),5:B,6:A"""


USER_PROMPT_TEMPLATE = """Сид: {seed}
Регион: {region}
Язык: {language}

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
