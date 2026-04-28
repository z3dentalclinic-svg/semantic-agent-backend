"""
l3_filter.py — Слой 3: OpenAI GPT-5.4 Mini классификатор для GREY-зоны.

Версия: БИНАРНАЯ (0 или 1), 2 корзины, reasoning=low.

Архитектура:
- Модель: gpt-5.4-mini (OpenAI, GA, $0.75/$4.50 за 1M токенов)
- reasoning_effort="low" — минимальный уровень thinking для gpt-5.4-mini
- batch_size=20, max_parallel=7
- Бинарная классификация: 1 → VALID, 0 → TRASH
- exponential backoff (2->4->8->16с) на 5 попытках
- Параметры region/language передаются в user-prompt

ВАЖНО про API:
- Endpoint: /v1/chat/completions
- max_completion_tokens (НЕ max_tokens) — обязательно для reasoning-моделей
- temperature НЕ поддерживается у reasoning-моделей, убрана
- reasoning_effort: low | medium | high | xhigh (minimal только у nano)

Ключ: env OPENAI_API_KEY
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


MODEL = "gpt-5.4-mini"
API_URL = "https://api.openai.com/v1/chat/completions"


@dataclass
class L3Config:
    api_key: str = ""
    batch_size: int = 20
    timeout: int = 120
    max_retries: int = 4            # = 5 попыток с exponential backoff
    max_parallel: int = 7
    region: str = "Украина"
    language: str = "русский"
    reasoning_effort: str = "low"  # low | medium | high | xhigh (gpt-5.4-mini НЕ поддерживает minimal)


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
# АКТИВНЫЙ ПРОМПТ — драфт 17.
# История:
#   Драфт 12 — без блока юрисдикции, чистый. 99% чистоты VALID.
#                Минус: ~9 РФ-брендов проходят в VALID.
#   Драфт 13 — блок юрисдикции В НАЧАЛЕ с "не уверен → 0".
#                Сломал UA-кейсы (viaflor, гурт, квит, "украина").
#   Драфт 14 — блок юрисдикции В КОНЦЕ с "не уверен → 1".
#                Меньше резал, но всё ещё ломал явные UA-кейсы.
#   Драфт 16 — расширенный список маркеров встроен в правило 1, "не уверен → 0".
#                Поймал все РФ ✅, но порезал ~20 украинских брендов
#                (viaflor, sflowers, цветовик, юфл, элитный букет и т.д.).
#   Драфт 17 — то же что 16, но "не уверен → 1" вместо "не уверен → 0".
#                Цель: сохранить агрессивную защиту от явных РФ при сохранении
#                неоднозначных UA-брендов.
# -----------------------------------------------------------------------------
SYSTEM_PROMPT = """Классифицируй каждый запрос по связи с темой сида: 1 (связано) или 0 (не связано).

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


def _call_openai(
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    timeout: int,
    reasoning_effort: str,
) -> Tuple[str, Dict[str, Any]]:
    """Возвращает (content, diag) где diag = {usage, reasoning_tokens, finish_reason}.
    
    ВАЖНО про reasoning-модели OpenAI (GPT-5 family):
    - max_completion_tokens вместо max_tokens
    - temperature, top_p, etc. НЕ поддерживаются
    - reasoning_effort: low | medium | high | xhigh (для gpt-5.4-mini)
    """
    import requests

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_completion_tokens": 8192,
        "stream": False,
        "reasoning_effort": reasoning_effort,
    }

    try:
        response = requests.post(
            API_URL,
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
            "finish_reason": choice.get('finish_reason'),
        }
        return content, diag
    except (KeyError, IndexError, TypeError):
        if 'choices' not in data:
            raise Exception(f"OpenAI no choices: {str(data)[:400]}")
        choice = data['choices'][0] if data.get('choices') else {}
        finish = choice.get('finish_reason', 'UNKNOWN')
        raise Exception(f"OpenAI unexpected response (finish_reason={finish}): {str(data)[:400]}")


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
) -> Tuple[int, List[Optional[int]], float]:
    batch_num = batch_idx + 1
    user_prompt = _build_user_prompt(config.region, config.language, seed, batch)

    for attempt in range(config.max_retries + 1):
        try:
            t0 = time.time()
            response, diag = _call_openai(
                config.api_key, SYSTEM_PROMPT, user_prompt,
                config.timeout, config.reasoning_effort
            )
            elapsed = time.time() - t0

            labels = _parse_labels(response, len(batch))

            valid_count = sum(1 for v in labels if v == 1)
            trash_count = sum(1 for v in labels if v == 0)
            error_count = sum(1 for v in labels if v is None)

            logger.info(
                f"[L3] Batch {batch_num}/{total_batches} — "
                f"VALID: {valid_count}, TRASH: {trash_count}, ERROR: {error_count} "
                f"({elapsed:.1f}s)"
            )
            logger.info(
                f"[L3-DIAG] Batch {batch_num}: "
                f"prompt={diag.get('prompt_tokens')} "
                f"completion={diag.get('completion_tokens')} "
                f"reasoning_tokens={diag.get('reasoning_tokens')} "
                f"finish={diag.get('finish_reason')}"
            )
            return (batch_idx, labels, elapsed)

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
                return (batch_idx, [None] * len(batch), 0.0)


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

    # Всегда берём свежий ключ из env
    env_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if env_key:
        config.api_key = env_key

    if not config.api_key:
        logger.warning("[L3] No OPENAI_API_KEY — skipping")
        result["l3_stats"] = {"error": "no_api_key", "input_grey": len(grey_keywords)}
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
        f"[L3] Processing {len(kw_strings)} GREY keywords via {MODEL} "
        f"({total_batches} batches of {config.batch_size}, {workers} parallel) "
        f"region={config.region} language={config.language} reasoning={config.reasoning_effort} "
        f"binary classification: 1=VALID, 0=TRASH"
    )

    batch_results: Dict[int, List[Optional[int]]] = {}
    api_time = 0.0
    t_wall_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_batch, idx, batch, seed, config, total_batches): idx
            for idx, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            batch_idx, labels, elapsed = future.result()
            batch_results[batch_idx] = labels
            api_time += elapsed

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

        l3_meta = {"label": bucket, "binary": label_int, "source": MODEL}

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
        "model": MODEL,
        "region": config.region,
        "language": config.language,
        "reasoning_effort": config.reasoning_effort,
        "classification": "binary_1_0",
    }
    out["_l3_trace"] = l3_trace

    logger.info(
        f"[L3] Done: {len(l3_valid)} VALID, {len(l3_trash)} TRASH, "
        f"{len(l3_error)} ERROR | wall: {wall_time}s"
    )

    return out
