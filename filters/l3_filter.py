"""
l3_filter.py — Слой 3: LLM-классификатор GREY-зоны с переключателем моделей.

Версия: БИНАРНАЯ (0 или 1), 2 корзины, MULTI-MODEL (тест июль 2026), lean-промпт.

Архитектура:
- Реестр MODELS: 4 модели, 3 провайдера (OpenAI / Anthropic / Google)
    gpt-5.6-sol      $5/$30   (OpenAI, reasoning_effort)
    claude-fable-5   $10/$50  (Anthropic, adaptive thinking + effort) — референс качества
    claude-opus-5    $5/$25   (Anthropic, adaptive thinking + effort)
    gemini-3.6-flash $1.5/$7.5 (Google, thinkingLevel)
- Модель и effort приходят из UI: config.model / config.reasoning_effort
  (backend должен пробросить l3_model / l3_effort из тела запроса в L3Config)
- batch_size=20, max_parallel=7 — без изменений
- Бинарная классификация: 1 → VALID, 0 → TRASH — без изменений
- exponential backoff (2->4->8->16с) на 5 попытках — без изменений
- Промпт lean — БЕЗ ИЗМЕНЕНИЙ (правило теста: промпт не трогаем)
- Стоимость прогона: считается из usage ответа API × цены реестра,
  агрегируется в l3_stats (tokens_prompt/completion/thinking, cost_usd)

Маппинг effort по провайдерам (в лог пишется фактически отправленное):
- OpenAI:    reasoning_effort = low|medium|high (как задано)
- Anthropic: thinking adaptive + output_config.effort = low|medium|high (как задано)
- Gemini:    thinkingLevel — только low|high, поэтому medium→high (см. _GEMINI_LEVEL)

Fable 5: классификаторы Anthropic могут перекинуть запрос на Opus 4.8 —
в ответе поле model будет отличаться от запрошенного. Ловим это через
diag.served_model, несовпадения попадают в l3_stats.served_model_mismatch.

ИСТОРИЯ моделей в этом проекте:
- Gemini 2.5 Flash-Lite preview — старт, отбросили (плохо UA)
- Together GPT-OSS 20B (low) — 75% совпадение с Gemini
- GPT-5 Nano (minimal) — клеит на 60-69, 50% совпадение
- GPT-5.4 Mini (low) — 92% совпадение, $33/мес — финал на много дней
- Claude Haiku 4.5 (no thinking) — слабая интент-фильтрация
- Claude Sonnet 4.6 (no thinking) — пропускает мусор, режет review
- Gemini 3.5 Flash (thinking=medium) — пропускал мусор/доп-гео непоследовательно
- Gemini 3.1 Flash-Lite (high/minimal) — слабо
- Gemini 3.1 Pro (thinking=medium) — сыпалась так же на гео
- GPT-5.5 (reasoning=medium) + lean-промпт — предыдущая (закомментирована)
- Claude Sonnet 4.6 (effort=medium, adaptive thinking) — пропускала пограничный мусор
- MULTI-MODEL тест: Sol / Fable 5 / Opus 5 / Gemini 3.6 Flash — текущая

Ключи: env OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY (Render).
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


# --- ИСТОРИЯ (закомментировано, не удалять — точки отката) ---
# MODEL = "gpt-5.5"
# API_URL = "https://api.openai.com/v1/chat/completions"
# MODEL = "claude-sonnet-4-6"
# API_URL = "https://api.anthropic.com/v1/messages"

ANTHROPIC_VERSION = "2023-06-01"

# =============================================================================
# РЕЕСТР МОДЕЛЕЙ — тест июль 2026. Цены: $ за 1M токенов (input/output).
# Стоимость output считается по billable_output_tokens:
#   OpenAI    — completion_tokens (reasoning уже внутри)
#   Anthropic — output_tokens (thinking уже внутри)
#   Gemini    — candidatesTokenCount + thoughtsTokenCount (thinking биллится как output)
# =============================================================================
MODELS: Dict[str, Dict[str, Any]] = {
    "gpt-5.6-sol": {
        "provider": "openai",
        "api_url": "https://api.openai.com/v1/chat/completions",
        "key_env": "OPENAI_API_KEY",
        "price_in": 5.00, "price_out": 30.00,
        "label": "GPT-5.6 Sol",
    },
    "claude-fable-5": {
        "provider": "anthropic",
        "api_url": "https://api.anthropic.com/v1/messages",
        "key_env": "ANTHROPIC_API_KEY",
        "price_in": 10.00, "price_out": 50.00,
        "label": "Claude Fable 5",
    },
    "claude-opus-5": {
        "provider": "anthropic",
        "api_url": "https://api.anthropic.com/v1/messages",
        "key_env": "ANTHROPIC_API_KEY",
        "price_in": 5.00, "price_out": 25.00,
        "label": "Claude Opus 5",
    },
    "gemini-3.6-flash": {
        "provider": "gemini",
        "api_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent",
        "key_env": "GEMINI_API_KEY",
        "price_in": 1.50, "price_out": 7.50,
        "label": "Gemini 3.6 Flash",
    },
}

DEFAULT_MODEL = "claude-opus-5"

# Gemini 3.x: thinkingLevel принимает только low|high → medium маппим в high.
_GEMINI_LEVEL = {"low": "low", "medium": "high", "high": "high", "xhigh": "high", "max": "high"}


@dataclass
class L3Config:
    api_key: str = ""
    batch_size: int = 20
    timeout: int = 120
    max_retries: int = 4            # = 5 попыток с exponential backoff
    max_parallel: int = 7
    region: str = "Украина"
    language: str = "русский"
    reasoning_effort: str = "medium"  # low|medium|high (общая шкала; Gemini: medium→high)
    model: str = DEFAULT_MODEL        # ключ из MODELS; приходит из UI (l3_model)


# =============================================================================
# СИСТЕМНЫЙ ПРОМПТ — БИНАРНАЯ КЛАССИФИКАЦИЯ (1 = VALID, 0 = TRASH)
# =============================================================================

# -----------------------------------------------------------------------------
# БЭКАП: предыдущий драфт 12 (без блока МАРКЕРЫ ЧУЖОЙ ЮРИСДИКЦИИ).
# Использовался до доработки по результатам консилиума 4 моделей.
# Сохранён на случай отката если драфт 13 будет хуже на тестах.
# Чтобы откатиться: SYSTEM_PROMPT = SYSTEM_PROMPT_DRAFT_12
# -----------------------------------------------------------------------------
SYSTEM_PROMPT_DRAFT_12 = """Классифицируй каждый запрос по связи с темой сида: 1 (связано) или 0 (не связано).

Тебе передаются:
- Регион поиска
- Язык поиска
- Сид (тема)
- Список запросов

ПРИОРИТЕТ ПРАВИЛ:
1. Несоответствие региону поиска приоритетнее всех других правил → 0.
2. Несоответствие интенту сида (купить vs ремонт) → 0.

ЗАПРЕТЫ:
- Не выдумывай факты о брендах, компаниях, сайтах, сервисах, законах, локациях, людях.
- Если регион в запросе явно указан — учитывай. Если не указан — не домысливай.
- Не додумывай за пользователя. Оценивай только то что написано.
- Опечатки и грамматические ошибки не делают запрос мусором.
- Полностью нечитаемый запрос или бессмыслица → 0.

ПРАВИЛО ШИРОТЫ ТЕМЫ:
К теме относятся: характеристики, диагностика, способы, условия, сравнения, отзывы, противопоказания, аналоги, последствия, альтернативы, симптомы, обслуживание, обучение. Любой запрос, который помогает пользователю принять решение или разобраться в теме — 1.

ИНТЕНТ:
Если сид содержит конкретное действие (ремонт, купить, аренда), запросы с другим действием по тому же товару → 0.
Информационные/диагностические запросы по теме сида → 1.

ФОРМАТ ОТВЕТА:
Только цифры 1 или 0 через запятую. Без пояснений, без нумерации.
Пример: 1,0,1,1,0,1,0"""


# -----------------------------------------------------------------------------
# БЭКАП: драфт 13 — блок юрисдикции В НАЧАЛЕ с "не уверен → 0".
# Перефокусировал модель на гео, начала резать UA-кейсы (viaflor, гурт, квит,
# Киев, Одесса, "украина" в запросе и т.д.). Откатили после трагического прогона.
# -----------------------------------------------------------------------------
SYSTEM_PROMPT_DRAFT_13 = """Классифицируй каждый запрос по связи с темой сида: 1 (связано) или 0 (не связано).

Тебе передаются:
- Регион поиска
- Язык поиска
- Сид (тема)
- Список запросов

ПРИОРИТЕТ ПРАВИЛ:
1. Несоответствие региону поиска приоритетнее всех других правил → 0.
2. Несоответствие интенту сида (купить vs ремонт) → 0.

МАРКЕРЫ ЧУЖОЙ ЮРИСДИКЦИИ (→ 0):
Запрос содержит признак принадлежности к стране ВНЕ региона поиска: налоги/льготы/госорганы/юрформы/доменные зоны/локальные бренды другой страны. Глобальные бренды (Apple, Samsung, IKEA и подобные) исключение → 1.
Если не уверен в принадлежности бренда или термина к региону поиска → 0.

ЗАПРЕТЫ:
- Не выдумывай факты о брендах, компаниях, сайтах, сервисах, законах, локациях, людях.
- Если регион в запросе явно указан — учитывай. Если не указан — не домысливай.
- Не додумывай за пользователя. Оценивай только то что написано.
- Опечатки и грамматические ошибки не делают запрос мусором.
- Полностью нечитаемый запрос или бессмыслица → 0.

ПРАВИЛО ШИРОТЫ ТЕМЫ:
К теме относятся: характеристики, диагностика, способы, условия, сравнения, отзывы, противопоказания, аналоги, последствия, альтернативы, симптомы, обслуживание, обучение. Любой запрос, который помогает пользователю принять решение или разобраться в теме — 1.

ИНТЕНТ:
Если сид содержит конкретное действие (ремонт, купить, аренда), запросы с другим действием по тому же товару → 0.
Информационные/диагностические запросы по теме сида → 1.

ФОРМАТ ОТВЕТА:
Только цифры 1 или 0 через запятую. Без пояснений, без нумерации.
Пример: 1,0,1,1,0,1,0"""


# -----------------------------------------------------------------------------
# БЭКАП: драфт 14 — блок юрисдикции В КОНЦЕ с "не уверен → 1".
# Меньше резал UA чем драфт 13, но всё ещё резал явные UA-кейсы 
# ("где купить цветы в киеве", "форум цветоводов украина").
# Откатили обратно на драфт 12.
# -----------------------------------------------------------------------------
SYSTEM_PROMPT_DRAFT_14 = """Классифицируй каждый запрос по связи с темой сида: 1 (связано) или 0 (не связано).

Тебе передаются:
- Регион поиска
- Язык поиска
- Сид (тема)
- Список запросов

ПРИОРИТЕТ ПРАВИЛ:
1. Несоответствие региону поиска приоритетнее всех других правил → 0.
2. Несоответствие интенту сида (купить vs ремонт) → 0.

ЗАПРЕТЫ:
- Не выдумывай факты о брендах, компаниях, сайтах, сервисах, законах, локациях, людях.
- Если регион в запросе явно указан — учитывай. Если не указан — не домысливай.
- Не додумывай за пользователя. Оценивай только то что написано.
- Опечатки и грамматические ошибки не делают запрос мусором.
- Полностью нечитаемый запрос или бессмыслица → 0.

ПРАВИЛО ШИРОТЫ ТЕМЫ:
К теме относятся: характеристики, диагностика, способы, условия, сравнения, отзывы, противопоказания, аналоги, последствия, альтернативы, симптомы, обслуживание, обучение. Любой запрос, который помогает пользователю принять решение или разобраться в теме — 1.

ИНТЕНТ:
Если сид содержит конкретное действие (ремонт, купить, аренда), запросы с другим действием по тому же товару → 0.
Информационные/диагностические запросы по теме сида → 1.

МАРКЕРЫ ЧУЖОЙ СТРАНЫ (→ 0):
Запрос с явной привязкой к стране ВНЕ региона поиска (бренды, законы, домены, города) → 0.
Если не уверен — сначала проверь, есть ли это в регионе поиска. Если нет данных в пользу региона поиска → 1.

ФОРМАТ ОТВЕТА:
Только цифры 1 или 0 через запятую. Без пояснений, без нумерации.
Пример: 1,0,1,1,0,1,0"""


# -----------------------------------------------------------------------------
# БЭКАП: драфт 17 — расширенный список маркеров с "не уверен → 1".
# Тестовые результаты:
#   доставка цветов: 10 ошибок (хуже драфта 12 = 6)
#   имплантация:     6 ошибок (лучше драфта 12 = 9)
#   купить айфон 16: ~20+ ошибок (катастрофически — режет UA-сети 
#                                  Фокстрот, Эпицентр, Алло, Жук, Яблоки,
#                                  украинские города Житомир, Шостка)
# Вывод: на бренд-насыщенных сидах (цветы, телефоны) драфт 17 ломает UA-кейсы.
# -----------------------------------------------------------------------------
SYSTEM_PROMPT_DRAFT_17 = """Классифицируй каждый запрос по связи с темой сида: 1 (связано) или 0 (не связано).

Тебе передаются:
- Регион поиска
- Язык поиска
- Сид (тема)
- Список запросов

ПРИОРИТЕТ ПРАВИЛ:
1. Несоответствие региону поиска приоритетнее всех других правил → 0.
   Если запрос содержит географическое название, аббревиатуру, бренд (компания, клиника, магазин, сеть, сайт), юридическую/бухгалтерскую/налоговую формулировку, форму организации, госструктуру, госсервис, доменную зону, локальный платёжный/логистический сервис или иной региональный маркер — точно убедись, что это соответствует региону поиска. Если явно НЕ соответствует — 0. Если не уверен в принадлежности — 1.
2. Несоответствие интенту сида (купить vs ремонт) → 0.

ЗАПРЕТЫ:
- Не выдумывай факты о брендах, компаниях, сайтах, сервисах, законах, локациях, людях.
- Если регион в запросе явно указан — учитывай. Если не указан — не домысливай.
- Не додумывай за пользователя. Оценивай только то что написано.
- Опечатки и грамматические ошибки не делают запрос мусором.
- Полностью нечитаемый запрос или бессмыслица → 0.

ПРАВИЛО ШИРОТЫ ТЕМЫ:
К теме относятся: характеристики, диагностика, способы, условия, сравнения, отзывы, противопоказания, аналоги, последствия, альтернативы, симптомы, обслуживание, обучение. Любой запрос, который помогает пользователю принять решение или разобраться в теме — 1.

ИНТЕНТ:
Если сид содержит конкретное действие (ремонт, купить, аренда), запросы с другим действием по тому же товару → 0.
Информационные/диагностические запросы по теме сида → 1.

ФОРМАТ ОТВЕТА:
Только цифры 1 или 0 через запятую. Без пояснений, без нумерации.
Пример: 1,0,1,1,0,1,0"""


# -----------------------------------------------------------------------------
# АКТИВНЫЙ ПРОМПТ — ровно ручная чат-формулировка: одна фраза + вывод 1/0.
# Регион/язык/сид подставляются в user-prompt (_build_user_prompt).
# -----------------------------------------------------------------------------
SYSTEM_PROMPT = """Очисти список запросов от брака парсинга.

Ответь только цифрами через запятую: 1 (оставить) или 0 (удалить). Без пояснений и нумерации."""


def _build_user_prompt(region: str, language: str, seed: str, keywords: List[str]) -> str:
    lines = [
        f"Регион поиска: {region}",
        f"Язык поиска: {language}",
        f'Сид: "{seed}"',
        "",
        "Запросы:",
    ]
    for i, kw in enumerate(keywords, 1):
        lines.append(f"{i}. {kw}")
    lines.append(f"\nОтветь {len(keywords)} цифрами через запятую (только 1 или 0):")
    return "\n".join(lines)


# ЛЕГАСИ (GPT-5.5, single-model). Заменена активной универсальной _call_openai ниже.
# Не удалять — точка отката.
# def _call_openai(
#     api_key: str,
#     system_prompt: str,
#     user_prompt: str,
#     timeout: int,
#     reasoning_effort: str,
# ) -> Tuple[str, Dict[str, Any]]:
#     """Возвращает (content, diag) где diag = {usage, reasoning_tokens, finish_reason}.
# 
#     ВАЖНО про reasoning-модели OpenAI (GPT-5 family):
#     - max_completion_tokens вместо max_tokens
#     - temperature, top_p, etc. НЕ поддерживаются
#     - reasoning_effort: none | low | medium | high | xhigh
#     """
#     import requests
# 
#     payload = {
#         "model": MODEL,
#         "messages": [
#             {"role": "system", "content": system_prompt},
#             {"role": "user", "content": user_prompt},
#         ],
#         "max_completion_tokens": 8192,
#         "stream": False,
#         "reasoning_effort": reasoning_effort,
#     }
# 
#     try:
#         response = requests.post(
#             API_URL,
#             headers={
#                 "Content-Type": "application/json",
#                 "Authorization": f"Bearer {api_key}",
#             },
#             json=payload,
#             timeout=timeout,
#         )
#     except requests.exceptions.RequestException as e:
#         raise Exception(f"OpenAI network error: {type(e).__name__}: {e}")
# 
#     if response.status_code != 200:
#         raise Exception(f"OpenAI API error {response.status_code}: {response.text[:500]}")
# 
#     try:
#         data = response.json()
#     except Exception as e:
#         raise Exception(f"OpenAI JSON parse error: {e}. Raw: {response.text[:300]}")
# 
#     try:
#         choice = data['choices'][0]
#         message = choice['message']
#         content = (message.get('content') or '').strip()
# 
#         usage = data.get('usage', {}) or {}
#         completion_details = usage.get('completion_tokens_details', {}) or {}
#         diag = {
#             "prompt_tokens": usage.get('prompt_tokens'),
#             "completion_tokens": usage.get('completion_tokens'),
#             "reasoning_tokens": completion_details.get('reasoning_tokens'),
#             "finish_reason": choice.get('finish_reason'),
#         }
#         return content, diag
#     except (KeyError, IndexError, TypeError):
#         if 'choices' not in data:
#             raise Exception(f"OpenAI no choices: {str(data)[:400]}")
#         choice = data['choices'][0] if data.get('choices') else {}
#         finish = choice.get('finish_reason', 'UNKNOWN')
#         raise Exception(f"OpenAI unexpected response (finish_reason={finish}): {str(data)[:400]}")


def _call_openai(
    spec: Dict[str, Any],
    model_id: str,
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    timeout: int,
    effort: str,
) -> Tuple[str, Dict[str, Any]]:
    """OpenAI Chat Completions (GPT-5.6 family).

    - max_completion_tokens вместо max_tokens
    - temperature/top_p НЕ поддерживаются reasoning-моделями
    - reasoning_effort: low|medium|high (передаём как есть)
    - completion_tokens уже ВКЛЮЧАЕТ reasoning_tokens → биллинг по нему
    """
    import requests

    payload = {
        "model": model_id,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_completion_tokens": 8192,
        "stream": False,
        "reasoning_effort": effort,
    }

    try:
        response = requests.post(
            spec["api_url"],
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            json=payload,
            timeout=timeout,
        )
    except requests.exceptions.RequestException as e:
        raise Exception(f"OpenAI network error: {type(e).__name__}: {e}")

    if response.status_code != 200:
        raise Exception(f"OpenAI API error {response.status_code}: {response.text[:500]}")

    try:
        data = response.json()
    except Exception as e:
        raise Exception(f"OpenAI JSON parse error: {e}. Raw: {response.text[:300]}")

    try:
        choice = data['choices'][0]
        message = choice['message']
        content = (message.get('content') or '').strip()

        usage = data.get('usage', {}) or {}
        completion_details = usage.get('completion_tokens_details', {}) or {}
        diag = {
            "prompt_tokens": usage.get('prompt_tokens'),
            "completion_tokens": usage.get('completion_tokens'),
            "reasoning_tokens": completion_details.get('reasoning_tokens'),
            "billable_output_tokens": usage.get('completion_tokens'),  # reasoning внутри
            "finish_reason": choice.get('finish_reason'),
            "served_model": data.get('model'),
            "effort_sent": effort,
        }
    except (KeyError, IndexError, TypeError):
        if 'choices' not in data:
            raise Exception(f"OpenAI no choices: {str(data)[:400]}")
        choice = data['choices'][0] if data.get('choices') else {}
        finish = choice.get('finish_reason', 'UNKNOWN')
        raise Exception(f"OpenAI unexpected response (finish_reason={finish}): {str(data)[:400]}")

    if not content:
        raise Exception(f"OpenAI empty text (finish_reason={diag['finish_reason']}): {str(data)[:400]}")

    return content, diag


def _call_anthropic(
    spec: Dict[str, Any],
    model_id: str,
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    timeout: int,
    effort: str,
) -> Tuple[str, Dict[str, Any]]:
    """Anthropic Messages API (Fable 5 / Opus 5), adaptive thinking + effort.

    - endpoint /v1/messages, заголовки x-api-key + anthropic-version
    - system — отдельный top-level параметр (не сообщение)
    - max_tokens — лимит на ВЕСЬ output (thinking + текст)
    - thinking={"type":"adaptive"} + output_config={"effort": ...}
    - ответ: content[] из блоков; текст берём из type=="text", thinking пропускаем
    - output_tokens уже ВКЛЮЧАЕТ thinking → биллинг по нему
    - served_model: у Fable 5 классификаторы могут перекинуть запрос на Opus 4.8 —
      тогда data.model != model_id, ловим в _process_batch
    """
    import requests

    payload = {
        "model": model_id,
        "max_tokens": 8192,
        "system": system_prompt,
        "messages": [
            {"role": "user", "content": user_prompt},
        ],
        "thinking": {"type": "adaptive"},
        "output_config": {"effort": effort},
    }

    try:
        response = requests.post(
            spec["api_url"],
            headers={
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": ANTHROPIC_VERSION,
            },
            json=payload,
            timeout=timeout,
        )
    except requests.exceptions.RequestException as e:
        raise Exception(f"Anthropic network error: {type(e).__name__}: {e}")

    if response.status_code != 200:
        raise Exception(f"Anthropic API error {response.status_code}: {response.text[:500]}")

    try:
        data = response.json()
    except Exception as e:
        raise Exception(f"Anthropic JSON parse error: {e}. Raw: {response.text[:300]}")

    blocks = data.get("content")
    if not isinstance(blocks, list):
        raise Exception(f"Anthropic no content blocks: {str(data)[:400]}")

    # Текст-ответ — конкатенация блоков type=="text"; thinking/redacted_thinking игнорим
    text_parts = [
        b.get("text", "")
        for b in blocks
        if isinstance(b, dict) and b.get("type") == "text"
    ]
    content = "".join(text_parts).strip()

    usage = data.get("usage", {}) or {}
    diag = {
        "prompt_tokens": usage.get("input_tokens"),
        "completion_tokens": usage.get("output_tokens"),   # thinking входит сюда же
        "reasoning_tokens": None,                           # Anthropic отдельно не выдаёт
        "billable_output_tokens": usage.get("output_tokens"),
        "finish_reason": data.get("stop_reason"),
        "served_model": data.get("model"),
        "effort_sent": effort,
    }

    if not content:
        # частый кейс: stop_reason=="max_tokens" — thinking съел весь бюджет
        raise Exception(
            f"Anthropic empty text (stop_reason={data.get('stop_reason')}): {str(data)[:400]}"
        )

    return content, diag


def _call_gemini(
    spec: Dict[str, Any],
    model_id: str,
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    timeout: int,
    effort: str,
) -> Tuple[str, Dict[str, Any]]:
    """Google Gemini generateContent (Gemini 3.6 Flash).

    - endpoint .../models/{model}:generateContent, ключ в заголовке x-goog-api-key
    - systemInstruction — отдельный top-level параметр
    - thinkingConfig.thinkingLevel: только low|high → medium маппим в high (_GEMINI_LEVEL)
    - ответ: candidates[0].content.parts[]; parts с thought==true пропускаем
    - thinking биллится как output → billable = candidatesTokenCount + thoughtsTokenCount
    """
    import requests

    level = _GEMINI_LEVEL.get(effort, "high")
    payload = {
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "contents": [
            {"role": "user", "parts": [{"text": user_prompt}]},
        ],
        "generationConfig": {
            "maxOutputTokens": 8192,
            "thinkingConfig": {"thinkingLevel": level},
        },
    }

    try:
        response = requests.post(
            spec["api_url"],
            headers={
                "Content-Type": "application/json",
                "x-goog-api-key": api_key,
            },
            json=payload,
            timeout=timeout,
        )
    except requests.exceptions.RequestException as e:
        raise Exception(f"Gemini network error: {type(e).__name__}: {e}")

    if response.status_code != 200:
        raise Exception(f"Gemini API error {response.status_code}: {response.text[:500]}")

    try:
        data = response.json()
    except Exception as e:
        raise Exception(f"Gemini JSON parse error: {e}. Raw: {response.text[:300]}")

    candidates = data.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise Exception(f"Gemini no candidates: {str(data)[:400]}")

    cand = candidates[0]
    parts = ((cand.get("content") or {}).get("parts")) or []
    # Текст-ответ — конкатенация parts без флага thought (thinking пропускаем)
    text_parts = [
        p.get("text", "")
        for p in parts
        if isinstance(p, dict) and not p.get("thought")
    ]
    content = "".join(text_parts).strip()

    um = data.get("usageMetadata", {}) or {}
    cand_tokens = um.get("candidatesTokenCount") or 0
    thought_tokens = um.get("thoughtsTokenCount") or 0
    diag = {
        "prompt_tokens": um.get("promptTokenCount"),
        "completion_tokens": cand_tokens,
        "reasoning_tokens": thought_tokens,
        "billable_output_tokens": cand_tokens + thought_tokens,  # thinking биллится как output
        "finish_reason": cand.get("finishReason"),
        "served_model": data.get("modelVersion"),
        "effort_sent": level,
    }

    if not content:
        raise Exception(
            f"Gemini empty text (finishReason={cand.get('finishReason')}): {str(data)[:400]}"
        )

    return content, diag


def _call_llm(
    config: "L3Config",
    system_prompt: str,
    user_prompt: str,
) -> Tuple[str, Dict[str, Any]]:
    """Диспетчер: по config.model выбирает провайдера, зовёт его caller,
    добавляет в diag стоимость батча по ценам реестра."""
    spec = MODELS[config.model]
    provider = spec["provider"]

    if provider == "openai":
        caller = _call_openai
    elif provider == "anthropic":
        caller = _call_anthropic
    elif provider == "gemini":
        caller = _call_gemini
    else:
        raise Exception(f"Unknown provider {provider!r} for model {config.model!r}")

    content, diag = caller(
        spec, config.model, config.api_key,
        system_prompt, user_prompt,
        config.timeout, config.reasoning_effort,
    )

    pt = diag.get("prompt_tokens") or 0
    bo = diag.get("billable_output_tokens") or 0
    diag["cost_usd"] = pt / 1e6 * spec["price_in"] + bo / 1e6 * spec["price_out"]
    return content, diag


def _parse_labels(response: str, expected_count: int) -> List[Optional[int]]:
    """Парсит ответ в список 0/1 или None для невалидных."""
    # Берём только первую строку — на случай если модель добавила лишний текст
    first_line = response.split('\n', 1)[0].strip()

    # Оставляем только цифры, запятые, пробелы
    clean = ''.join(c for c in first_line if c.isdigit() or c in ', ')
    parts = [p.strip() for p in clean.split(',') if p.strip()]

    labels: List[Optional[int]] = []
    for p in parts:
        try:
            v = int(p)
            if v in (0, 1):
                labels.append(v)
            else:
                # Если модель внезапно вернула что-то другое (например, score) — None
                labels.append(None)
        except ValueError:
            labels.append(None)

    if len(labels) != expected_count:
        logger.warning(f"[L3] Expected {expected_count} labels, got {len(labels)}. Response: {response[:200]}")
        while len(labels) < expected_count:
            labels.append(None)
        labels = labels[:expected_count]

    return labels


def _label_to_bucket(label: Optional[int]) -> str:
    """Преобразует 0/1 в метку VALID/TRASH/ERROR."""
    if label is None:
        return "ERROR"
    if label == 1:
        return "VALID"
    return "TRASH"


def _extract_keyword_string(kw) -> str:
    if isinstance(kw, dict):
        return kw.get("keyword", kw.get("query", ""))
    return str(kw)


def _process_batch(
    batch_idx: int,
    batch: List[str],
    seed: str,
    config: L3Config,
    total_batches: int,
) -> Tuple[int, List[Optional[int]], float, Dict[str, Any]]:
    batch_num = batch_idx + 1
    user_prompt = _build_user_prompt(config.region, config.language, seed, batch)

    for attempt in range(config.max_retries + 1):
        try:
            t0 = time.time()
            response, diag = _call_llm(config, SYSTEM_PROMPT, user_prompt)
            elapsed = time.time() - t0

            labels = _parse_labels(response, len(batch))

            valid_count = sum(1 for v in labels if v == 1)
            trash_count = sum(1 for v in labels if v == 0)
            error_count = sum(1 for v in labels if v is None)

            logger.info(
                f"[L3] Batch {batch_num}/{total_batches} — "
                f"VALID: {valid_count}, TRASH: {trash_count}, ERROR: {error_count} "
                f"({elapsed:.1f}s, ${diag.get('cost_usd', 0):.6f})"
            )
            logger.info(
                f"[L3-DIAG] Batch {batch_num}: "
                f"prompt={diag.get('prompt_tokens')} "
                f"completion={diag.get('completion_tokens')} "
                f"reasoning_tokens={diag.get('reasoning_tokens')} "
                f"finish={diag.get('finish_reason')} "
                f"served={diag.get('served_model')} "
                f"effort_sent={diag.get('effort_sent')}"
            )
            return (batch_idx, labels, elapsed, diag)

        except Exception as e:
            if attempt < config.max_retries:
                backoff = 2 ** (attempt + 1)
                logger.warning(
                    f"[L3] Batch {batch_num} attempt {attempt+1}/{config.max_retries+1} failed: {e}. "
                    f"Retrying in {backoff}s..."
                )
                time.sleep(backoff)
            else:
                logger.error(f"[L3] Batch {batch_num} FAILED after {config.max_retries+1} attempts: {e}")
                return (batch_idx, [None] * len(batch), 0.0, {})


def apply_l3_filter(
    result: Dict[str, Any],
    seed: str,
    enable_l3: bool = True,
    config: Optional[L3Config] = None,
) -> Dict[str, Any]:
    if not enable_l3:
        return result

    grey_keywords = result.get("keywords_grey", [])
    if not grey_keywords:
        logger.info("[L3] No GREY keywords to process")
        return result

    if config is None:
        config = L3Config()

    # Валидация модели: незнакомый id → дефолт (защита от опечатки в UI/backend)
    if config.model not in MODELS:
        logger.warning(f"[L3] Unknown model {config.model!r} — falling back to {DEFAULT_MODEL}")
        config.model = DEFAULT_MODEL
    spec = MODELS[config.model]

    # Ключ провайдера из env (Render). Берём свежий на каждый вызов.
    config.api_key = os.environ.get(spec["key_env"], "").strip()

    if not config.api_key:
        logger.warning(f"[L3] {spec['key_env']} не задан в окружении — skipping")
        result["l3_stats"] = {
            "error": "no_api_key",
            "key_env": spec["key_env"],
            "model": config.model,
            "input_grey": len(grey_keywords),
        }
        return result

    logger.info(
        f"[L3] key_len={len(config.api_key)} prefix={config.api_key[:6]!r} suffix={config.api_key[-4:]!r}"
    )

    kw_strings = []
    kw_objects = []
    for kw in grey_keywords:
        kw_strings.append(_extract_keyword_string(kw))
        kw_objects.append(kw)

    batches = [kw_strings[i:i + config.batch_size] for i in range(0, len(kw_strings), config.batch_size)]
    total_batches = len(batches)
    workers = min(config.max_parallel, total_batches)

    logger.info(
        f"[L3] Processing {len(kw_strings)} GREY keywords via {config.model} "
        f"({total_batches} batches of {config.batch_size}, {workers} parallel) "
        f"region={config.region} language={config.language} reasoning={config.reasoning_effort} "
        f"binary classification: 1=VALID, 0=TRASH"
    )

    batch_results: Dict[int, List[Optional[int]]] = {}
    api_time = 0.0
    t_wall_start = time.time()

    # Агрегация usage/стоимости по батчам
    tok_prompt = 0
    tok_completion = 0
    tok_thinking = 0
    thinking_known = False
    cost_usd = 0.0
    effort_sent = None
    served_mismatch: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_batch, idx, batch, seed, config, total_batches): idx
            for idx, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            batch_idx, labels, elapsed, diag = future.result()
            batch_results[batch_idx] = labels
            api_time += elapsed

            tok_prompt += diag.get("prompt_tokens") or 0
            tok_completion += diag.get("completion_tokens") or 0
            if diag.get("reasoning_tokens") is not None:
                tok_thinking += diag["reasoning_tokens"]
                thinking_known = True
            cost_usd += diag.get("cost_usd") or 0.0
            if diag.get("effort_sent") is not None:
                effort_sent = diag["effort_sent"]
            # Fable 5: несовпадение served/requested = переброс классификатором на Opus 4.8
            served = diag.get("served_model")
            if served and config.model not in str(served):
                served_mismatch.append({"batch": batch_idx + 1, "served_model": served})

    wall_time = round(time.time() - t_wall_start, 2)

    all_labels: List[Optional[int]] = []
    for idx in range(total_batches):
        all_labels.extend(batch_results[idx])

    out = result.copy()
    l3_valid = []
    l3_trash = []
    l3_error = []
    l3_trace = []

    for kw_obj, kw_str, label_int in zip(kw_objects, kw_strings, all_labels):
        bucket = _label_to_bucket(label_int)

        trace_rec = {"keyword": kw_str, "label": bucket, "binary": label_int}
        if isinstance(kw_obj, dict):
            trace_rec["tail"] = kw_obj.get("tail", "")
            if "l2" in kw_obj:
                trace_rec["l2_info"] = kw_obj["l2"]
        l3_trace.append(trace_rec)

        l3_meta = {"label": bucket, "binary": label_int, "source": config.model}

        if bucket == "VALID":
            if isinstance(kw_obj, dict):
                kw_out = kw_obj.copy()
                kw_out["l3"] = l3_meta
            else:
                kw_out = kw_str
            l3_valid.append(kw_out)
        elif bucket == "TRASH":
            if isinstance(kw_obj, dict):
                kw_out = kw_obj.copy()
                kw_out["anchor_reason"] = "L3_TRASH"
                kw_out["l3"] = l3_meta
            else:
                kw_out = {
                    "keyword": kw_str,
                    "anchor_reason": "L3_TRASH",
                    "l3": l3_meta,
                }
            l3_trash.append(kw_out)
        else:  # ERROR — модель не вернула 0/1
            l3_error.append(kw_obj)

    out["keywords"] = result.get("keywords", []) + l3_valid
    out["anchors"] = result.get("anchors", []) + l3_trash
    # ERROR-кейсы оставляем в keywords_grey для возможной повторной обработки
    out["keywords_grey"] = l3_error

    kw_count = len(out["keywords"])
    if "count" in out:
        out["count"] = kw_count
    if "total_count" in out:
        out["total_count"] = kw_count
    if "total_unique_keywords" in out:
        out["total_unique_keywords"] = kw_count

    out["l3_stats"] = {
        "input_grey": len(kw_strings),
        "l3_valid": len(l3_valid),
        "l3_trash": len(l3_trash),
        "l3_error": len(l3_error),
        "api_time": round(api_time, 2),
        "wall_time": wall_time,
        "batches": total_batches,
        "batch_size": config.batch_size,
        "parallel": workers,
        "model": config.model,
        "model_label": spec["label"],
        "provider": spec["provider"],
        "region": config.region,
        "language": config.language,
        "reasoning_effort": config.reasoning_effort,
        "effort_sent": effort_sent or config.reasoning_effort,
        "classification": "binary_1_0",
        # Токены и стоимость прогона (для HTML-панели)
        "tokens_prompt": tok_prompt,
        "tokens_completion": tok_completion,
        "tokens_thinking": tok_thinking if thinking_known else None,
        "cost_usd": round(cost_usd, 6),
        "price_in": spec["price_in"],
        "price_out": spec["price_out"],
        "served_model_mismatch": served_mismatch,  # Fable 5 → переброс на Opus 4.8
    }
    out["_l3_trace"] = l3_trace

    logger.info(
        f"[L3] Done ({config.model}): {len(l3_valid)} VALID, {len(l3_trash)} TRASH, "
        f"{len(l3_error)} ERROR | wall: {wall_time}s | cost: ${round(cost_usd, 6)}"
    )

    return out
