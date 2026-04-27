"""
l3_filter.py — Слой 3: Anthropic Claude Sonnet 4.6 классификатор для GREY-зоны.

Версия: score-based (0-100), 3 корзины, БЕЗ thinking.

Архитектура:
- Модель: claude-sonnet-4-6 (Anthropic, GA, $3/$15 за 1M токенов)
- thinking ВЫКЛЮЧЕН (не передаём параметр)
- batch_size=20, max_parallel=7
- score 0-100, 3 корзины: VALID (>=70), GREY (40-69), TRASH (<40)
- exponential backoff (2->4->8->16с) на 5 попытках
- Параметры region/language передаются в user-prompt

ВАЖНО про Anthropic API (отличается от OpenAI):
- URL: /v1/messages (не /chat/completions)
- Headers: x-api-key (не Authorization Bearer), anthropic-version обязателен
- system промпт — отдельным полем (не в messages)
- max_tokens (не max_completion_tokens)
- temperature 0.0 поддерживается
- Ответ в content[0].text
- Без thinking — просто не передаём этот параметр

Цена: ~$3/$15 за 1M токенов (в 3 раза дороже Haiku 4.5)
Скорость: средняя (40-60 t/s vs 80-120 у Haiku)
Прогноз: 5-8 секунд на 86 ключей, $0.05-0.07 за прогон, ~$57/мес на 2700 прогонов

Ключ: env ANTHROPIC_API_KEY (тот же что для Haiku 4.5)
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


MODEL = "claude-sonnet-4-6"
API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"


@dataclass
class L3Config:
    api_key: str = ""
    batch_size: int = 20
    timeout: int = 120
    temperature: float = 0.0
    max_retries: int = 4            # = 5 попыток с exponential backoff
    max_parallel: int = 7
    score_valid_threshold: int = 70 # score >= 70 -> VALID
    score_trash_threshold: int = 40 # score < 40 -> TRASH (между = GREY)
    region: str = "Украина"
    language: str = "русский"


# =============================================================================
# СИСТЕМНЫЙ ПРОМПТ (драфт 11 — тот же что использовали везде)
# =============================================================================
SYSTEM_PROMPT = """Отфильтруй каждый запрос по его связи с темой сида и поставь ОДНО ЧИСЛО от 0 до 100.

Тебе передаются:
- Регион поиска
- Язык поиска
- Сид (тема)
- Список запросов

ПРИОРИТЕТ ПРАВИЛ:
1. Несоответствие региону поиска приоритетнее всех других правил.
2. Неуверенность применяется только при отсутствии явных нарушений.

ЗАПРЕТЫ:
- Не выдумывай факты о брендах, компаниях, сайтах, сервисах, законах, локациях, людях.
- Если регион в запросе явно указан — учитывай. Если не указан — не домысливай.
- Не додумывай за пользователя. Оценивай только то что написано.
- Намерение покупки не влияет на оценку. Информационный, диагностический или зеркальный (продам, куплю) запрос оценивается по связи с темой.
- Опечатки и грамматические ошибки не снижают score. Полностью нечитаемый запрос — score 0-9.

ШКАЛА:
| Score   | Решение                                                       |
|---------|---------------------------------------------------------------|
| 70-100  | Связано с темой сида (включая описывающие и расширяющие)      |
| 40-69   | Сомнительно                                                   |
| 10-39   | Не связано: другая ниша или несоответствие региону            |
| 0-9     | Мусор, бессмыслица, обрывки                                   |

ПРАВИЛО ШИРОТЫ ТЕМЫ:
К теме относятся: характеристики, диагностика, способы, условия, сравнения, отзывы, противопоказания, аналоги, последствия, альтернативы, симптомы, обслуживание, обучение. Любой запрос, который помогает пользователю принять решение или разобраться в теме — score 70+.

Если связь неясна — ставь 50-65, не угадывай.

ФОРМАТ ОТВЕТА:
Числа через запятую. Без пояснений, без нумерации.
Пример: 85,15,72,50,90,10,75"""


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
    lines.append(f"\nОтветь {len(keywords)} числами через запятую (0-100):")
    return "\n".join(lines)


def _call_anthropic(
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    timeout: int,
    temperature: float,
) -> Tuple[str, Dict[str, Any]]:
    """Возвращает (content, diag).
    
    Anthropic API формат:
    - URL: /v1/messages
    - Headers: x-api-key, anthropic-version
    - system - отдельным полем, не в messages
    - max_tokens обязателен
    - thinking НЕ передаём — отключен по умолчанию
    """
    import requests

    payload = {
        "model": MODEL,
        "max_tokens": 1024,  # для нашей задачи: ~30 токенов на 20 чисел + запас
        "temperature": temperature,
        "system": system_prompt,
        "messages": [
            {"role": "user", "content": user_prompt}
        ],
    }

    try:
        response = requests.post(
            API_URL,
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

    try:
        # Anthropic возвращает content как список блоков
        content_blocks = data.get('content', [])
        if not content_blocks:
            raise Exception(f"Anthropic empty content: {str(data)[:300]}")
        
        # Берём первый text-блок
        text_content = ""
        for block in content_blocks:
            if block.get('type') == 'text':
                text_content = block.get('text', '').strip()
                break
        
        if not text_content:
            raise Exception(f"Anthropic no text in content: {str(data)[:300]}")

        usage = data.get('usage', {}) or {}
        diag = {
            "input_tokens": usage.get('input_tokens'),
            "output_tokens": usage.get('output_tokens'),
            "cache_creation": usage.get('cache_creation_input_tokens'),
            "cache_read": usage.get('cache_read_input_tokens'),
            "stop_reason": data.get('stop_reason'),
        }
        return text_content, diag
    except KeyError as e:
        raise Exception(f"Anthropic unexpected response format: {e}. Data: {str(data)[:400]}")


def _parse_scores(response: str, expected_count: int) -> List[Optional[int]]:
    """Парсит ответ в список int 0-100 или None для невалидных."""
    # Берём только первую строку — на случай если модель добавила лишний текст
    first_line = response.split('\n', 1)[0].strip()

    # Оставляем только цифры, запятые, пробелы
    clean = ''.join(c for c in first_line if c.isdigit() or c in ', ')
    parts = [p.strip() for p in clean.split(',') if p.strip()]

    scores: List[Optional[int]] = []
    for p in parts:
        try:
            score = int(p)
            if 0 <= score <= 100:
                scores.append(score)
            else:
                scores.append(None)
        except ValueError:
            scores.append(None)

    if len(scores) != expected_count:
        logger.warning(f"[L3] Expected {expected_count} scores, got {len(scores)}. Response: {response[:200]}")
        while len(scores) < expected_count:
            scores.append(None)
        scores = scores[:expected_count]

    return scores


def _score_to_label(score: Optional[int], config: L3Config) -> str:
    """Преобразует score в метку VALID/GREY/TRASH/ERROR."""
    if score is None:
        return "ERROR"
    if score >= config.score_valid_threshold:
        return "VALID"
    if score < config.score_trash_threshold:
        return "TRASH"
    return "GREY"


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
            response, diag = _call_anthropic(
                config.api_key, SYSTEM_PROMPT, user_prompt,
                config.timeout, config.temperature
            )
            elapsed = time.time() - t0

            scores = _parse_scores(response, len(batch))

            valid_count = sum(1 for s in scores if s is not None and s >= config.score_valid_threshold)
            grey_count = sum(1 for s in scores if s is not None and config.score_trash_threshold <= s < config.score_valid_threshold)
            trash_count = sum(1 for s in scores if s is not None and s < config.score_trash_threshold)
            error_count = sum(1 for s in scores if s is None)

            logger.info(
                f"[L3] Batch {batch_num}/{total_batches} — "
                f"VALID: {valid_count}, GREY: {grey_count}, TRASH: {trash_count}, ERROR: {error_count} "
                f"({elapsed:.1f}s)"
            )
            # Anthropic диагностика: input/output токены отдельно (без скрытого reasoning)
            logger.info(
                f"[L3-DIAG] Batch {batch_num}: "
                f"input={diag.get('input_tokens')} "
                f"output={diag.get('output_tokens')} "
                f"stop_reason={diag.get('stop_reason')}"
            )
            return (batch_idx, scores, elapsed)

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
    env_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if env_key:
        config.api_key = env_key

    if not config.api_key:
        logger.warning("[L3] No ANTHROPIC_API_KEY — skipping")
        result["l3_stats"] = {"error": "no_api_key", "input_grey": len(grey_keywords)}
        return result

    logger.info(
        f"[L3] key_len={len(config.api_key)} prefix={config.api_key[:10]!r} suffix={config.api_key[-4:]!r}"
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
        f"region={config.region} language={config.language} thinking=OFF "
        f"thresholds: VALID>={config.score_valid_threshold} TRASH<{config.score_trash_threshold}"
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
            batch_idx, scores, elapsed = future.result()
            batch_results[batch_idx] = scores
            api_time += elapsed

    wall_time = round(time.time() - t_wall_start, 2)

    all_scores: List[Optional[int]] = []
    for idx in range(total_batches):
        all_scores.extend(batch_results[idx])

    out = result.copy()
    l3_valid = []
    l3_grey = []
    l3_trash = []
    l3_error = []
    l3_trace = []

    for kw_obj, kw_str, score in zip(kw_objects, kw_strings, all_scores):
        label = _score_to_label(score, config)

        trace_rec = {"keyword": kw_str, "label": label, "score": score}
        if isinstance(kw_obj, dict):
            trace_rec["tail"] = kw_obj.get("tail", "")
            if "l2" in kw_obj:
                trace_rec["l2_info"] = kw_obj["l2"]
        l3_trace.append(trace_rec)

        l3_meta = {"label": label, "score": score, "source": MODEL}

        if label == "VALID":
            if isinstance(kw_obj, dict):
                kw_out = kw_obj.copy()
                kw_out["l3"] = l3_meta
            else:
                kw_out = kw_str
            l3_valid.append(kw_out)
        elif label == "TRASH":
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
        elif label == "GREY":
            if isinstance(kw_obj, dict):
                kw_out = kw_obj.copy()
                kw_out["l3"] = l3_meta
            else:
                kw_out = {"keyword": kw_str, "l3": l3_meta}
            l3_grey.append(kw_out)
        else:  # ERROR
            l3_error.append(kw_obj)

    out["keywords"] = result.get("keywords", []) + l3_valid
    out["anchors"] = result.get("anchors", []) + l3_trash
    out["keywords_grey"] = l3_grey + l3_error

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
        "l3_grey": len(l3_grey),
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
        "thinking": "off",
        "score_valid_threshold": config.score_valid_threshold,
        "score_trash_threshold": config.score_trash_threshold,
    }
    out["_l3_trace"] = l3_trace

    logger.info(
        f"[L3] Done: {len(l3_valid)} VALID, {len(l3_grey)} GREY, {len(l3_trash)} TRASH, "
        f"{len(l3_error)} ERROR | wall: {wall_time}s"
    )

    return out
