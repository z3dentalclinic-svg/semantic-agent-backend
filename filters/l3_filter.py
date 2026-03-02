"""
l3_filter.py — Слой 3: DeepSeek LLM классификатор для оставшихся GREY.

Размещается в filters/l3_filter.py

Что делает:
    1. Берёт keywords_grey из result dict (после L0 → L2)
    2. Отправляет ПАРАЛЛЕЛЬНО батчами на DeepSeek API
    3. VALID → добавляет в keywords
    4. TRASH → добавляет в anchors (с anchor_reason="L3_TRASH")
    5. ERROR → оставляет в keywords_grey (fallback)

Возвращает:
    result dict с обновлёнными keywords/anchors/keywords_grey
    + l3_stats (для UI)
    + _l3_trace (для tracer)

Параллельность:
    Все батчи отправляются одновременно через ThreadPoolExecutor.
    3 батча по 50 ключей → ~3 секунды вместо ~9 последовательно.

Стоимость: ~$0.001 за батч 50 ключей (DeepSeek-chat).

Требования:
    pip install requests
    env var DEEPSEEK_API_KEY или передать через L3Config
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════

@dataclass
class L3Config:
    """Конфигурация L3 классификатора."""
    api_key: str = ""
    model: str = "deepseek-chat"
    api_url: str = "https://api.deepseek.com/chat/completions"
    batch_size: int = 50
    timeout: int = 60
    temperature: float = 0.0
    max_retries: int = 2
    max_parallel: int = 5   # максимум параллельных запросов


# ═══════════════════════════════════════════════════════════════
# ПРОМПТ (взят из layer2_deepseek.py, адаптирован)
# ═══════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """Ты — эксперт по фильтрации поисковых запросов для украинского рынка.

Задача: определить, является ли каждый запрос осмысленным поисковым запросом реального человека, СВЯЗАННЫМ с темой seed'а и РЕЛЕВАНТНЫМ для Украины.

VALID — запрос, который реальный человек в Украине мог бы ввести в поисковик, и он связан с темой seed'а.
TRASH — бессмысленный запрос, мусор парсинга, запрос не связанный с темой seed'а, ИЛИ запрос с площадкой/городом не из Украины.

Регион: Украина.
Площадки TRASH (не работают в Украине): авито, озон, wildberries, горбушка, днс, мтс, яндекс, куфар, онлайнер, фарпост, юла, м видео, шоп бай, 5 элемент, а1, ябко, рестор, медиаэксперт, электросила, юнит.
Площадки VALID (работают в Украине): олх, розетка, фокстрот, комфи, хотлайн, цитрус, цум, шейн, цифрус, эпицентр, эпл стор, эльдорадо, сота, лайф.
Города России/Беларуси — TRASH. Города Украины и Европы — VALID.

Важно:
- Грамматика может быть нарушена (это нормально для поисковых запросов) — оценивай по СМЫСЛУ
- Если хвост запроса НИКАК не связан с темой — это TRASH
- Если связь есть (даже косвенная) — это VALID
- НЕ додумывай за пользователя. Если связь натянутая — TRASH

Ответь ТОЛЬКО списком через запятую: 1 = VALID, 0 = TRASH.
Без пояснений. Без нумерации. Только цифры через запятую.
Пример ответа: 1,0,1,1,0,1,0"""


# ═══════════════════════════════════════════════════════════════
# ВНУТРЕННИЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════

def _build_user_prompt(seed: str, keywords: List[str]) -> str:
    """Формирует пользовательский промпт с батчем ключей."""
    lines = [f'Seed: "{seed}"', "", "Запросы:"]
    for i, kw in enumerate(keywords, 1):
        lines.append(f"{i}. {kw}")
    lines.append(f"\nОтветь {len(keywords)} цифрами через запятую (1=VALID, 0=TRASH):")
    return "\n".join(lines)


def _call_deepseek(config: L3Config, system_prompt: str, user_prompt: str) -> str:
    """Отправляет запрос к DeepSeek API."""
    import requests

    response = requests.post(
        config.api_url,
        headers={
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json"
        },
        json={
            "model": config.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "max_tokens": 500,
            "temperature": config.temperature,
        },
        timeout=config.timeout,
    )

    if response.status_code != 200:
        raise Exception(f"DeepSeek API error {response.status_code}: {response.text[:200]}")

    data = response.json()
    return data['choices'][0]['message']['content'].strip()


def _parse_response(response: str, expected_count: int) -> List[str]:
    """Парсит ответ DeepSeek: '1,0,1,1,0' → ['VALID', 'TRASH', ...]"""
    clean = ''.join(c for c in response if c in '01,')
    parts = [p.strip() for p in clean.split(',') if p.strip()]

    labels = []
    for p in parts:
        if p == '1':
            labels.append('VALID')
        elif p == '0':
            labels.append('TRASH')

    if len(labels) != expected_count:
        logger.warning(
            f"[L3] Expected {expected_count} labels, got {len(labels)}. "
            f"Response: {response[:200]}"
        )
        while len(labels) < expected_count:
            labels.append('ERROR')
        labels = labels[:expected_count]

    return labels


def _extract_keyword_string(kw) -> str:
    """Извлекает строку из keyword (str или dict)."""
    if isinstance(kw, dict):
        return kw.get("keyword", kw.get("query", ""))
    return str(kw)


def _process_batch(
    batch_idx: int,
    batch: List[str],
    seed: str,
    config: L3Config,
    total_batches: int,
) -> Tuple[int, List[str], float]:
    """
    Обрабатывает один батч. Вызывается в потоке ThreadPoolExecutor.
    
    Returns: (batch_idx, labels, api_time)
    """
    batch_num = batch_idx + 1
    user_prompt = _build_user_prompt(seed, batch)

    for attempt in range(config.max_retries + 1):
        try:
            t0 = time.time()
            response = _call_deepseek(config, SYSTEM_PROMPT, user_prompt)
            elapsed = time.time() - t0

            labels = _parse_response(response, len(batch))

            valid_count = labels.count('VALID')
            trash_count = labels.count('TRASH')
            logger.info(
                f"[L3] Batch {batch_num}/{total_batches} — "
                f"VALID: {valid_count}, TRASH: {trash_count} ({elapsed:.1f}s)"
            )
            return (batch_idx, labels, elapsed)

        except Exception as e:
            if attempt < config.max_retries:
                logger.warning(
                    f"[L3] Batch {batch_num} attempt {attempt+1} failed: {e}. Retrying..."
                )
                time.sleep(2)
            else:
                logger.error(
                    f"[L3] Batch {batch_num} FAILED after {config.max_retries+1} attempts: {e}"
                )
                return (batch_idx, ['ERROR'] * len(batch), 0.0)


# ═══════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ: apply_l3_filter
# ═══════════════════════════════════════════════════════════════

def apply_l3_filter(
    result: Dict[str, Any],
    seed: str,
    enable_l3: bool = True,
    config: Optional[L3Config] = None,
) -> Dict[str, Any]:
    """
    Применяет L3 фильтр к результату после L0 → L2.

    Берёт keywords_grey, отправляет ПАРАЛЛЕЛЬНО на DeepSeek API,
    распределяет: VALID → keywords, TRASH → anchors, ERROR → keywords_grey.

    Args:
        result: dict с keywords, keywords_grey, anchors (выход L2)
        seed: базовый запрос
        enable_l3: включить L3
        config: L3Config (api_key, model, batch_size...)

    Returns:
        обновлённый result dict + l3_stats + _l3_trace
    """
    if not enable_l3:
        return result

    grey_keywords = result.get("keywords_grey", [])
    if not grey_keywords:
        logger.info("[L3] No GREY keywords to process")
        return result

    cfg = config or L3Config()

    # API key: параметр → env var → пусто
    if not cfg.api_key:
        cfg.api_key = os.environ.get("DEEPSEEK_API_KEY", "")

    if not cfg.api_key:
        logger.warning("[L3] No API key — skipping L3 filter")
        result["l3_stats"] = {"error": "no_api_key", "input_grey": len(grey_keywords)}
        return result

    # Извлекаем строки ключей и оригинальные объекты
    kw_strings = []
    kw_objects = []
    for kw in grey_keywords:
        kw_strings.append(_extract_keyword_string(kw))
        kw_objects.append(kw)

    # Нарезаем на батчи
    batches = []
    for i in range(0, len(kw_strings), cfg.batch_size):
        batches.append(kw_strings[i:i + cfg.batch_size])

    total_batches = len(batches)
    workers = min(cfg.max_parallel, total_batches)

    logger.info(
        f"[L3] Processing {len(kw_strings)} GREY keywords via {cfg.model} "
        f"({total_batches} batches, {workers} parallel)"
    )

    # ── Параллельная обработка батчей ──
    batch_results: Dict[int, List[str]] = {}
    api_time = 0.0
    t_wall_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _process_batch, idx, batch, seed, cfg, total_batches
            ): idx
            for idx, batch in enumerate(batches)
        }

        for future in as_completed(futures):
            batch_idx, labels, elapsed = future.result()
            batch_results[batch_idx] = labels
            api_time += elapsed

    wall_time = round(time.time() - t_wall_start, 2)

    # Собираем labels в правильном порядке
    all_labels = []
    for idx in range(total_batches):
        all_labels.extend(batch_results[idx])

    # ── Собираем результат ──
    out = result.copy()

    l3_valid = []
    l3_trash = []
    l3_error = []  # ERROR → остаются в grey
    l3_trace = []

    for kw_obj, kw_str, label in zip(kw_objects, kw_strings, all_labels):
        # Трейс-запись (для tracer и UI)
        trace_rec = {
            "keyword": kw_str,
            "label": label,
        }
        if isinstance(kw_obj, dict):
            trace_rec["tail"] = kw_obj.get("tail", "")
            if "l2" in kw_obj:
                trace_rec["l2_info"] = kw_obj["l2"]

        l3_trace.append(trace_rec)

        # Распределяем по корзинам
        if label == "VALID":
            if isinstance(kw_obj, dict):
                kw_out = kw_obj.copy()
                kw_out["l3"] = {"label": "VALID", "source": cfg.model}
            else:
                kw_out = kw_str
            l3_valid.append(kw_out)

        elif label == "TRASH":
            if isinstance(kw_obj, dict):
                kw_out = kw_obj.copy()
                kw_out["anchor_reason"] = "L3_TRASH"
                kw_out["l3"] = {"label": "TRASH", "source": cfg.model}
            else:
                kw_out = {
                    "keyword": kw_str,
                    "anchor_reason": "L3_TRASH",
                    "l3": {"label": "TRASH", "source": cfg.model},
                }
            l3_trash.append(kw_out)

        else:
            # ERROR → fallback в grey
            l3_error.append(kw_obj)

    # Обновляем result
    out["keywords"] = result.get("keywords", []) + l3_valid
    out["anchors"] = result.get("anchors", []) + l3_trash
    out["keywords_grey"] = l3_error  # обычно пусто если API работает

    # Обновляем счётчики
    kw_count = len(out["keywords"])
    if "count" in out:
        out["count"] = kw_count
    if "total_count" in out:
        out["total_count"] = kw_count
    if "total_unique_keywords" in out:
        out["total_unique_keywords"] = kw_count

    # Статистика для UI
    out["l3_stats"] = {
        "input_grey": len(kw_strings),
        "l3_valid": len(l3_valid),
        "l3_trash": len(l3_trash),
        "l3_error": len(l3_error),
        "api_time": round(api_time, 2),   # суммарное время всех батчей
        "wall_time": wall_time,            # реальное время (параллельно)
        "batches": total_batches,
        "parallel": workers,
        "model": cfg.model,
    }

    # Трейс для tracer
    out["_l3_trace"] = l3_trace

    logger.info(
        f"[L3] Done: {len(l3_valid)} VALID, {len(l3_trash)} TRASH, "
        f"{len(l3_error)} ERROR | wall: {wall_time}s "
        f"({total_batches} batches × {workers} parallel)"
    )

    return out
