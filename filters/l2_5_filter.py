"""
l2_5_filter.py — Слой 2.5: Gemini 3.1 Flash-Lite, чистка ВАЛИДОВ.

Позиция в пайплайне: между L2 и L3. Гоняется по VALID (result["keywords"]),
грей-зону НЕ трогает (её обрабатывает L3). Критерий — валид/невалид.
Режет (0) только то, что:
  — не может существовать как реальный запрос человека к Google (брак: битый
    порядок слов, обрывки, склейка, бессмысленный набор токенов);
  — противоречит SEED семантически или логически (сущность не может сочетаться
    с SEED в реальности — напр. модель другого класса, бренд другого происхождения).
Всё остальное — валид (1).

Срезанное (binary=0) уходит в result["anchors"] как anchor_reason="L2_5_TRASH".
Нераспознанное (None) ОСТАЁТСЯ в VALID — фильтр только понижает уверенно.

API (Google Generative Language, Gemini):
- POST https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent?key=...
- systemInstruction — отдельный top-level блок
- thinkingConfig.thinkingLevel: minimal | low | medium | high  (Gemini 3.x; НЕ thinkingBudget)
- ответ: candidates[0].content.parts[*].text ; usageMetadata.{promptTokenCount,
  candidatesTokenCount, thoughtsTokenCount, totalTokenCount}

Трейс/статы (как у L3): result["_l2_5_trace"], result["l2_5_stats"]
(stats содержит токены и время).

Ключ: env GEMINI_API_KEY.

Standalone (увидеть, что срежет): GEMINI_API_KEY=... python l2_5_filter.py file.json
"""

import os
import sys
import json
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# --- Gemini 3.1 Flash-Lite ---
MODEL = "gemini-3.1-flash-lite"   # если 404 — попробуй "gemini-3.1-flash-lite-preview"
API_BASE = "https://generativelanguage.googleapis.com/v1beta/models"

# thinkingLevel: "minimal" (дешевле/быстрее), "low" (рекоменд. для классификации),
# "medium"/"high" (точнее/дороже). Старт — "low".
THINKING_LEVEL = "low"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()


@dataclass
class L2_5Config:
    api_key: str = ""
    region: str = "ua"
    language: str = "ru"
    batch_size: int = 20
    timeout: int = 120
    max_retries: int = 4          # = 5 попыток с exponential backoff
    max_parallel: int = 4         # Gemini: 4 потока
    thinking_level: str = THINKING_LEVEL


SYSTEM_PROMPT = """Проверь список ключевых слов. Дан SEED. Для каждого запроса вынеси вердикт: 1 (валид) или 0 (невалид).

Отбрось (0) только то, что:
— не может существовать как запрос, заданный Google живым человеком (битый порядок слов, обрывки, склейка слов, бессмысленный набор токенов);
— противоречит SEED семантически или логически (названная сущность не может сочетаться с SEED в реальности).

Всё остальное — валид (1).
Верни только строку из 1 и 0 через запятую, по числу запросов, без пояснений."""


def _build_user_prompt(region: str, language: str, seed: str, keywords: List[str]) -> str:
    lines = [
        f"Регион поиска: {region}",
        f"Язык поиска: {language}",
        f'SEED: "{seed}"',
        "",
        "Запросы:",
    ]
    for i, kw in enumerate(keywords, 1):
        lines.append(f"{i}. {kw}")
    lines.append(f"\nОтветь {len(keywords)} цифрами через запятую (только 1 или 0):")
    return "\n".join(lines)


def _call_gemini(
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    timeout: int,
    thinking_level: str,
) -> Tuple[str, Dict[str, Any]]:
    """Google Generative Language API, Gemini 3.1 Flash-Lite."""
    import requests

    url = f"{API_BASE}/{MODEL}:generateContent?key={api_key}"
    payload = {
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
        "generationConfig": {
            "temperature": 0,
            # Если API вернёт 400 на thinkingConfig — замени на плоское
            # "thinkingLevel": thinking_level прямо в generationConfig.
            "thinkingConfig": {"thinkingLevel": thinking_level},
        },
    }

    try:
        response = requests.post(
            url,
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

    cands = data.get("candidates")
    if not cands:
        raise Exception(
            f"Gemini no candidates (promptFeedback={data.get('promptFeedback')}): {str(data)[:300]}"
        )

    parts = (cands[0].get("content") or {}).get("parts") or []
    text = "".join(p.get("text", "") for p in parts if isinstance(p, dict)).strip()

    um = data.get("usageMetadata", {}) or {}
    diag = {
        "finish_reason": cands[0].get("finishReason"),
        "prompt_tokens": um.get("promptTokenCount", 0),
        "output_tokens": um.get("candidatesTokenCount", 0),    # видимый ответ
        "thinking_tokens": um.get("thoughtsTokenCount", 0),    # мышление
        "total_tokens": um.get("totalTokenCount", 0),
    }

    if not text:
        raise Exception(
            f"Gemini empty text (finishReason={cands[0].get('finishReason')}): {str(data)[:300]}"
        )

    return text, diag


def _parse_labels(response: str, expected_count: int) -> List[Optional[int]]:
    """Парсит ответ в список 0/1 или None для невалидных."""
    first_line = response.split('\n', 1)[0].strip()
    clean = ''.join(c for c in first_line if c.isdigit() or c in ', ')
    parts = [p.strip() for p in clean.split(',') if p.strip()]

    labels: List[Optional[int]] = []
    for p in parts:
        try:
            v = int(p)
            labels.append(v if v in (0, 1) else None)
        except ValueError:
            labels.append(None)

    if len(labels) != expected_count:
        logger.warning(f"[L2.5] Expected {expected_count} labels, got {len(labels)}. Response: {response[:200]}")
        while len(labels) < expected_count:
            labels.append(None)
        labels = labels[:expected_count]

    return labels


def _extract_keyword_string(kw) -> str:
    if isinstance(kw, dict):
        return kw.get("keyword", kw.get("query", ""))
    return str(kw)


def _process_batch(
    batch_idx: int,
    batch: List[str],
    seed: str,
    config: L2_5Config,
    total_batches: int,
) -> Tuple[int, List[Optional[int]], float, Dict[str, int]]:
    batch_num = batch_idx + 1
    user_prompt = _build_user_prompt(config.region, config.language, seed, batch)
    empty_usage = {"prompt_tokens": 0, "output_tokens": 0, "thinking_tokens": 0, "total_tokens": 0}

    for attempt in range(config.max_retries + 1):
        try:
            t0 = time.time()
            response, diag = _call_gemini(
                config.api_key, SYSTEM_PROMPT, user_prompt,
                config.timeout, config.thinking_level
            )
            dt = time.time() - t0
            labels = _parse_labels(response, len(batch))
            usage = {
                "prompt_tokens": diag.get("prompt_tokens", 0),
                "output_tokens": diag.get("output_tokens", 0),
                "thinking_tokens": diag.get("thinking_tokens", 0),
                "total_tokens": diag.get("total_tokens", 0),
            }
            logger.info(
                f"[L2.5] batch {batch_num}/{total_batches} ok in {dt:.1f}s "
                f"(finish={diag.get('finish_reason')}, tok={usage['total_tokens']})"
            )
            return batch_idx, labels, dt, usage
        except Exception as e:
            if attempt < config.max_retries:
                backoff = 2 ** (attempt + 1)
                logger.warning(
                    f"[L2.5] batch {batch_num}/{total_batches} attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {backoff}s..."
                )
                time.sleep(backoff)
            else:
                logger.error(f"[L2.5] batch {batch_num}/{total_batches} failed after all retries: {e}")
                return batch_idx, [None] * len(batch), 0.0, dict(empty_usage)

    return batch_idx, [None] * len(batch), 0.0, dict(empty_usage)


def _run(
    keywords: List[Any],
    seed: str,
    config: L2_5Config,
) -> Tuple[List[Optional[int]], Dict[str, Any]]:
    """Ядро: гоняет валиды через Gemini батчами параллельно.
    Возвращает (метки по порядку, stats с токенами/временем)."""
    kw_strings = [_extract_keyword_string(k) for k in keywords]

    batches = [kw_strings[i:i + config.batch_size] for i in range(0, len(kw_strings), config.batch_size)]
    total_batches = len(batches)
    workers = min(config.max_parallel, total_batches) if total_batches else 1

    logger.info(
        f"[L2.5] Gemini={MODEL} thinking={config.thinking_level} | "
        f"{len(kw_strings)} valids, {total_batches} batches, {workers} workers"
    )

    t_start = time.time()
    results: List[Optional[Tuple[int, List[Optional[int]], float, Dict[str, int]]]] = [None] * total_batches
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_batch, idx, batch, seed, config, total_batches): idx
            for idx, batch in enumerate(batches)
        }
        for fut in as_completed(futures):
            idx, labels, dt, usage = fut.result()
            results[idx] = (idx, labels, dt, usage)
    wall = time.time() - t_start

    all_labels: List[Optional[int]] = []
    sum_prompt = sum_output = sum_think = sum_total = 0
    sum_api = 0.0
    for idx in range(total_batches):
        r = results[idx]
        if r:
            all_labels.extend(r[1])
            sum_api += r[2]
            u = r[3]
            sum_prompt += u.get("prompt_tokens", 0)
            sum_output += u.get("output_tokens", 0)
            sum_think += u.get("thinking_tokens", 0)
            sum_total += u.get("total_tokens", 0)
        else:
            all_labels.extend([None] * len(batches[idx]))

    stats = {
        "model": MODEL,
        "thinking_level": config.thinking_level,
        "input": len(kw_strings),
        "prompt_tokens": sum_prompt,
        "output_tokens": sum_output,
        "thinking_tokens": sum_think,
        "total_tokens": sum_total,
        "api_time_sec": round(sum_api, 1),    # суммарно по батчам (если бы последовательно)
        "wall_time_sec": round(wall, 1),      # реальное время (параллельно)
        "batches": total_batches,
    }
    return all_labels, stats


def apply_l2_5_filter(
    result: Dict[str, Any],
    seed: str,
    enable_l2_5: bool = True,
    config: Optional[L2_5Config] = None,
) -> Dict[str, Any]:
    """Чистит result["keywords"] (VALID) через Gemini Flash-Lite. Интерфейс как у apply_l3_filter.

    Срезанные → result["anchors"] (anchor_reason="L2_5_TRASH").
    Ставит result["_l2_5_trace"] и result["l2_5_stats"] (с токенами/временем).
    """
    if not enable_l2_5:
        return result

    if config is None:
        config = L2_5Config()
    config.api_key = os.environ.get("GEMINI_API_KEY", "").strip() or config.api_key or GEMINI_API_KEY

    valids = result.get("keywords", [])

    if not config.api_key:
        logger.warning("[L2.5] GEMINI_API_KEY не задан — skipping")
        result["l2_5_stats"] = {"error": "no_api_key", "input_valid": len(valids)}
        result["_l2_5_trace"] = []
        return result

    if not valids:
        result["l2_5_stats"] = {"input_valid": 0, "model": MODEL}
        result["_l2_5_trace"] = []
        return result

    labels, stats = _run(valids, seed, config)

    if "anchors" not in result:
        result["anchors"] = []

    trace: List[Dict[str, Any]] = []
    kept: List[Any] = []
    n_valid = n_trash = n_error = 0
    for orig, lab in zip(valids, labels):
        kw = _extract_keyword_string(orig)
        if lab == 1:
            bucket = "VALID"; n_valid += 1
            kept.append(orig)
        elif lab == 0:
            bucket = "TRASH"; n_trash += 1
            result["anchors"].append({
                "keyword": kw,
                "anchor_reason": "L2_5_TRASH",
                "l2_5": {"label": "TRASH", "binary": 0, "source": MODEL},
            })
        else:
            bucket = "ERROR"; n_error += 1
            kept.append(orig)  # ошибка парсинга → НЕ режем
        trace.append({"keyword": kw, "label": bucket, "binary": lab, "source": MODEL})

    result["keywords"] = kept
    result["_l2_5_trace"] = trace
    stats.update({"l2_5_valid": n_valid, "l2_5_trash": n_trash, "l2_5_error": n_error})
    result["l2_5_stats"] = stats

    logger.info(
        f"[L2.5] valid={n_valid} trash={n_trash} error={n_error} | "
        f"tokens={stats['total_tokens']} wall={stats['wall_time_sec']}s"
    )
    return result


# --------------------------------------------------------------------------
# Standalone-прогон: python l2_5_filter.py path/to.json  [keywords_key]
# --------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(sys.argv) < 2:
        print("usage: GEMINI_API_KEY=... python l2_5_filter.py file.json [keywords_key]")
        sys.exit(1)

    path = sys.argv[1]
    key = sys.argv[2] if len(sys.argv) > 2 else "keywords"
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    seed = data.get("seed", "")
    before = list(data.get(key, []))
    print(f"SEED: {seed}")
    print(f"Валидов на входе ('{key}'): {len(before)}\n")

    data = apply_l2_5_filter(data, seed, enable_l2_5=True, config=L2_5Config())
    kept = {_extract_keyword_string(k) for k in data.get("keywords", [])}
    removed = [t["keyword"] for t in data.get("_l2_5_trace", []) if t["binary"] == 0]
    errors = [t["keyword"] for t in data.get("_l2_5_trace", []) if t["binary"] is None]

    print("==== СРЕЗАНО (0) ====", len(removed))
    for kw in sorted(removed):
        print("  -", kw)
    print("\n==== ОСТАВЛЕНО (1) ====", len(kept) - len(errors))
    if errors:
        print("\n==== НЕ РАСПОЗНАНО (оставлены) ====", len(errors))
        for kw in errors:
            print("  ?", kw)
    print("\nl2_5_stats:", json.dumps(data.get("l2_5_stats", {}), ensure_ascii=False))
