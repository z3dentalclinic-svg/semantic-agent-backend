"""
l3_filter.py — Слой 3: Gemini 3.1 Pro классификатор для оставшихся GREY.

Ключ: env GEMINI_API_KEY
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


MODEL = "gemini-3.1-flash-lite-preview"
API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"


@dataclass
class L3Config:
    api_key: str = ""
    batch_size: int = 50
    timeout: int = 120
    temperature: float = 0.0
    max_retries: int = 2
    max_parallel: int = 5
    enable_explanations: bool = True  # диагностический режим: модель объясняет каждое решение


SYSTEM_PROMPT = """Ты — эксперт по фильтрации поисковых запросов для украинского рынка. Задача: отфильтровать поисковые запросы от мусора  парсинга. Регион украина Важно: - Грамматика может быть нарушена (это нормально для поисковых запросов) — оценивай по СМЫСЛУ - НЕ додумывай за пользователя. Если связь натянутая — TRASH

Ответь ТОЛЬКО списком через запятую: 1 = VALID, 0 = TRASH.
Без пояснений. Без нумерации. Только цифры через запятую.
Пример ответа: 1,0,1,1,0,1,0"""


SYSTEM_PROMPT_EXPLAIN = """Ты — эксперт по фильтрации поисковых запросов для украинского рынка. Задача: отфильтровать поисковые запросы от мусора  парсинга. Регион украина Важно: - Грамматика может быть нарушена (это нормально для поисковых запросов) — оценивай по СМЫСЛУ - НЕ додумывай за пользователя. Если связь натянутая — TRASH

Формат ответа СТРОГО двумя блоками:

БЛОК 1 (одна строка): только цифры через запятую (1=VALID, 0=TRASH).
Пример: 1,0,1,1,0

БЛОК 2 (после строки ---): для КАЖДОГО запроса по строке вида:
N. LABEL — причина в 5-15 словах
Пример:
1. VALID — релевантный запрос про услугу
2. TRASH — российский город не Украина
3. VALID — улица в целевом городе

Никакого другого текста до БЛОКА 1 или после БЛОКА 2."""


def _build_user_prompt(seed: str, keywords: List[str]) -> str:
    lines = [f'Seed: "{seed}"', "", "Запросы:"]
    for i, kw in enumerate(keywords, 1):
        lines.append(f"{i}. {kw}")
    lines.append(f"\nОтветь {len(keywords)} цифрами через запятую (1=VALID, 0=TRASH):")
    return "\n".join(lines)


def _build_user_prompt_explain(seed: str, keywords: List[str]) -> str:
    lines = [f'Seed: "{seed}"', "", "Запросы:"]
    for i, kw in enumerate(keywords, 1):
        lines.append(f"{i}. {kw}")
    lines.append(
        f"\nОтветь СТРОГО в формате:\n"
        f"Строка 1: {len(keywords)} цифр через запятую (1=VALID, 0=TRASH)\n"
        f"Строка с ---\n"
        f"{len(keywords)} строк объяснений вида: N. LABEL — причина"
    )
    return "\n".join(lines)


def _call_gemini(api_key: str, system_prompt: str, user_prompt: str, timeout: int, temperature: float) -> str:
    import requests

    payload = {
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": 16384,
        }
    }

    try:
        response = requests.post(
            f"{API_URL}?key={api_key}",
            headers={"Content-Type": "application/json"},
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

    try:
        return data['candidates'][0]['content']['parts'][0]['text'].strip()
    except (KeyError, IndexError, TypeError):
        if 'candidates' not in data:
            raise Exception(f"Gemini no candidates: {str(data)[:400]}")
        cand = data['candidates'][0] if data.get('candidates') else {}
        finish = cand.get('finishReason', 'UNKNOWN')
        raise Exception(f"Gemini unexpected response (finishReason={finish}): {str(data)[:400]}")


def _parse_response(response: str, expected_count: int) -> List[str]:
    # Берём только первую строку — после неё могут быть объяснения
    first_line = response.split('\n', 1)[0].strip()
    # На всякий случай отсекаем по ---
    first_line = first_line.split('---', 1)[0].strip()

    clean = ''.join(c for c in first_line if c in '01,')
    parts = [p.strip() for p in clean.split(',') if p.strip()]

    labels = []
    for p in parts:
        if p == '1':
            labels.append('VALID')
        elif p == '0':
            labels.append('TRASH')

    if len(labels) != expected_count:
        logger.warning(f"[L3] Expected {expected_count} labels, got {len(labels)}. Response: {response[:200]}")
        while len(labels) < expected_count:
            labels.append('ERROR')
        labels = labels[:expected_count]

    return labels


def _parse_explanations(response: str, expected_count: int) -> List[str]:
    """Извлекает объяснения из второго блока (после ---). Возвращает список строк по индексам ключей."""
    if '---' not in response:
        return [''] * expected_count

    explain_block = response.split('---', 1)[1].strip()
    lines = [l.strip() for l in explain_block.split('\n') if l.strip()]

    explanations = [''] * expected_count
    for line in lines:
        # Парсим формат "N. LABEL — текст"
        # N может быть 1-3 цифры, дальше точка, потом текст
        m = line.split('.', 1)
        if len(m) != 2:
            continue
        try:
            idx = int(m[0].strip()) - 1
        except ValueError:
            continue
        if 0 <= idx < expected_count:
            explanations[idx] = m[1].strip()

    return explanations


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
) -> Tuple[int, List[str], List[str], float]:
    batch_num = batch_idx + 1

    if config.enable_explanations:
        system_prompt = SYSTEM_PROMPT_EXPLAIN
        user_prompt = _build_user_prompt_explain(seed, batch)
    else:
        system_prompt = SYSTEM_PROMPT
        user_prompt = _build_user_prompt(seed, batch)

    for attempt in range(config.max_retries + 1):
        try:
            t0 = time.time()
            response = _call_gemini(
                config.api_key, system_prompt, user_prompt,
                config.timeout, config.temperature
            )
            elapsed = time.time() - t0

            labels = _parse_response(response, len(batch))

            if config.enable_explanations:
                explanations = _parse_explanations(response, len(batch))
            else:
                explanations = [''] * len(batch)

            valid_count = labels.count('VALID')
            trash_count = labels.count('TRASH')
            logger.info(
                f"[L3] Batch {batch_num}/{total_batches} — "
                f"VALID: {valid_count}, TRASH: {trash_count} ({elapsed:.1f}s)"
            )
            return (batch_idx, labels, explanations, elapsed)

        except Exception as e:
            if attempt < config.max_retries:
                logger.warning(f"[L3] Batch {batch_num} attempt {attempt+1} failed: {e}. Retrying...")
                time.sleep(2)
            else:
                logger.error(f"[L3] Batch {batch_num} FAILED after {config.max_retries+1} attempts: {e}")
                return (batch_idx, ['ERROR'] * len(batch), [''] * len(batch), 0.0)


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

    # Всегда берём свежий ключ из env (игнорируем устаревший в config)
    env_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if env_key:
        config.api_key = env_key

    # Опциональный режим объяснений через env L3_EXPLAIN=1 (для отладки промпта)
    if os.environ.get("L3_EXPLAIN", "").strip() in ("1", "true", "True"):
        config.enable_explanations = True

    if not config.api_key:
        logger.warning("[L3] No GEMINI_API_KEY — skipping")
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
        f"({total_batches} batches, {workers} parallel)"
    )

    batch_results: Dict[int, List[str]] = {}
    batch_explanations: Dict[int, List[str]] = {}
    api_time = 0.0
    t_wall_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_batch, idx, batch, seed, config, total_batches): idx
            for idx, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            batch_idx, labels, explanations, elapsed = future.result()
            batch_results[batch_idx] = labels
            batch_explanations[batch_idx] = explanations
            api_time += elapsed

    wall_time = round(time.time() - t_wall_start, 2)

    all_labels = []
    all_explanations = []
    for idx in range(total_batches):
        all_labels.extend(batch_results[idx])
        all_explanations.extend(batch_explanations[idx])

    out = result.copy()
    l3_valid = []
    l3_trash = []
    l3_error = []
    l3_trace = []

    for kw_obj, kw_str, label, reason in zip(kw_objects, kw_strings, all_labels, all_explanations):
        trace_rec = {"keyword": kw_str, "label": label}
        if reason:
            trace_rec["reason"] = reason
        if isinstance(kw_obj, dict):
            trace_rec["tail"] = kw_obj.get("tail", "")
            if "l2" in kw_obj:
                trace_rec["l2_info"] = kw_obj["l2"]
        l3_trace.append(trace_rec)

        if label == "VALID":
            if isinstance(kw_obj, dict):
                kw_out = kw_obj.copy()
                kw_out["l3"] = {"label": "VALID", "source": MODEL}
            else:
                kw_out = kw_str
            l3_valid.append(kw_out)
        elif label == "TRASH":
            if isinstance(kw_obj, dict):
                kw_out = kw_obj.copy()
                kw_out["anchor_reason"] = "L3_TRASH"
                kw_out["l3"] = {"label": "TRASH", "source": MODEL}
            else:
                kw_out = {
                    "keyword": kw_str,
                    "anchor_reason": "L3_TRASH",
                    "l3": {"label": "TRASH", "source": MODEL},
                }
            l3_trash.append(kw_out)
        else:
            l3_error.append(kw_obj)

    out["keywords"] = result.get("keywords", []) + l3_valid
    out["anchors"] = result.get("anchors", []) + l3_trash
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
        "parallel": workers,
        "model": MODEL,
        "explanations": config.enable_explanations,
    }
    out["_l3_trace"] = l3_trace

    logger.info(
        f"[L3] Done: {len(l3_valid)} VALID, {len(l3_trash)} TRASH, "
        f"{len(l3_error)} ERROR | wall: {wall_time}s"
    )

    return out
