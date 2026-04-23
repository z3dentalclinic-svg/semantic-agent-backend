"""
l3_filter.py — Слой 3: LLM классификатор для оставшихся GREY.
Поддерживает два провайдера: DeepSeek и Gemini.

Переключение через env var L3_PROVIDER:
    L3_PROVIDER=deepseek  → DeepSeek (дефолт)
    L3_PROVIDER=gemini    → Gemini 2.5 Flash-Lite

Ключи API:
    DEEPSEEK_API_KEY — для DeepSeek
    GEMINI_API_KEY   — для Gemini
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ ПРОВАЙДЕРОВ
# ═══════════════════════════════════════════════════════════════

PROVIDERS = {
    "deepseek": {
        "model": "deepseek-chat",
        "api_url": "https://api.deepseek.com/chat/completions",
        "env_key": "DEEPSEEK_API_KEY",
    },
    "gemini": {
        "model": "gemini-2.5-flash-lite",
        "api_url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent",
        "env_key": "GEMINI_API_KEY",
    },
}


def _default_provider() -> str:
    """Читает провайдера из env L3_PROVIDER, дефолт deepseek."""
    return os.environ.get("L3_PROVIDER", "deepseek").lower().strip()


@dataclass
class L3Config:
    """Конфигурация L3 классификатора.

    provider по умолчанию берётся из env L3_PROVIDER.
    Чтобы принудительно задать — передай provider="gemini" явно.
    """
    provider: str = field(default_factory=_default_provider)
    api_key: str = ""
    model: str = ""                # берётся из PROVIDERS[provider]["model"] если пусто
    api_url: str = ""              # берётся из PROVIDERS[provider]["api_url"] если пусто
    batch_size: int = 50
    timeout: int = 60
    temperature: float = 0.0
    max_retries: int = 2
    max_parallel: int = 5

    def __post_init__(self):
        if self.provider not in PROVIDERS:
            raise ValueError(f"Unknown provider: {self.provider}. Use 'deepseek' or 'gemini'")
        prov = PROVIDERS[self.provider]
        if not self.model:
            self.model = prov["model"]
        if not self.api_url:
            self.api_url = prov["api_url"]


# ═══════════════════════════════════════════════════════════════
# ПРОМПТ (без изменений)
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
    lines = [f'Seed: "{seed}"', "", "Запросы:"]
    for i, kw in enumerate(keywords, 1):
        lines.append(f"{i}. {kw}")
    lines.append(f"\nОтветь {len(keywords)} цифрами через запятую (1=VALID, 0=TRASH):")
    return "\n".join(lines)


def _call_deepseek(config: L3Config, system_prompt: str, user_prompt: str) -> str:
    """Вызов DeepSeek API (OpenAI-compatible формат)."""
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


def _call_gemini(config: L3Config, system_prompt: str, user_prompt: str) -> str:
    """Вызов Gemini API (Google Generative Language)."""
    import requests

    # Gemini: system идёт отдельным полем systemInstruction, user — в contents
    payload = {
        "systemInstruction": {
            "parts": [{"text": system_prompt}]
        },
        "contents": [
            {
                "role": "user",
                "parts": [{"text": user_prompt}]
            }
        ],
        "generationConfig": {
            "temperature": config.temperature,
            "maxOutputTokens": 500,
            # thinkingBudget=0 отключает reasoning → максимальная скорость
            "thinkingConfig": {
                "thinkingBudget": 0
            }
        }
    }

    response = requests.post(
        f"{config.api_url}?key={config.api_key}",
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=config.timeout,
    )

    if response.status_code != 200:
        raise Exception(f"Gemini API error {response.status_code}: {response.text[:300]}")

    data = response.json()

    # Gemini формат: candidates[0].content.parts[0].text
    try:
        return data['candidates'][0]['content']['parts'][0]['text'].strip()
    except (KeyError, IndexError) as e:
        raise Exception(f"Gemini unexpected response format: {str(data)[:300]}")


def _call_api(config: L3Config, system_prompt: str, user_prompt: str) -> str:
    """Роутер: выбирает нужную функцию по провайдеру."""
    if config.provider == "deepseek":
        return _call_deepseek(config, system_prompt, user_prompt)
    elif config.provider == "gemini":
        return _call_gemini(config, system_prompt, user_prompt)
    else:
        raise ValueError(f"Unknown provider: {config.provider}")


def _parse_response(response: str, expected_count: int) -> List[str]:
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
    batch_num = batch_idx + 1
    user_prompt = _build_user_prompt(seed, batch)

    for attempt in range(config.max_retries + 1):
        try:
            t0 = time.time()
            response = _call_api(config, SYSTEM_PROMPT, user_prompt)
            elapsed = time.time() - t0

            labels = _parse_response(response, len(batch))

            valid_count = labels.count('VALID')
            trash_count = labels.count('TRASH')
            logger.info(
                f"[L3] Batch {batch_num}/{total_batches} [{config.provider}] — "
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
# ГЛАВНАЯ ФУНКЦИЯ
# ═══════════════════════════════════════════════════════════════

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

    # Определяем провайдера из env (если config не задан)
    if config is None:
        provider = os.environ.get("L3_PROVIDER", "deepseek").lower()
        config = L3Config(provider=provider)

    # API key: параметр → env var → пусто
    if not config.api_key:
        env_key = PROVIDERS[config.provider]["env_key"]
        config.api_key = os.environ.get(env_key, "")

    if not config.api_key:
        env_key = PROVIDERS[config.provider]["env_key"]
        logger.warning(f"[L3] No API key for provider={config.provider} (set {env_key}) — skipping")
        result["l3_stats"] = {
            "error": "no_api_key",
            "provider": config.provider,
            "input_grey": len(grey_keywords)
        }
        return result

    kw_strings = []
    kw_objects = []
    for kw in grey_keywords:
        kw_strings.append(_extract_keyword_string(kw))
        kw_objects.append(kw)

    batches = []
    for i in range(0, len(kw_strings), config.batch_size):
        batches.append(kw_strings[i:i + config.batch_size])

    total_batches = len(batches)
    workers = min(config.max_parallel, total_batches)

    logger.info(
        f"[L3] Processing {len(kw_strings)} GREY keywords via {config.model} "
        f"[provider={config.provider}] ({total_batches} batches, {workers} parallel)"
    )

    batch_results: Dict[int, List[str]] = {}
    api_time = 0.0
    t_wall_start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _process_batch, idx, batch, seed, config, total_batches
            ): idx
            for idx, batch in enumerate(batches)
        }

        for future in as_completed(futures):
            batch_idx, labels, elapsed = future.result()
            batch_results[batch_idx] = labels
            api_time += elapsed

    wall_time = round(time.time() - t_wall_start, 2)

    all_labels = []
    for idx in range(total_batches):
        all_labels.extend(batch_results[idx])

    out = result.copy()

    l3_valid = []
    l3_trash = []
    l3_error = []
    l3_trace = []

    for kw_obj, kw_str, label in zip(kw_objects, kw_strings, all_labels):
        trace_rec = {
            "keyword": kw_str,
            "label": label,
        }
        if isinstance(kw_obj, dict):
            trace_rec["tail"] = kw_obj.get("tail", "")
            if "l2" in kw_obj:
                trace_rec["l2_info"] = kw_obj["l2"]

        l3_trace.append(trace_rec)

        if label == "VALID":
            if isinstance(kw_obj, dict):
                kw_out = kw_obj.copy()
                kw_out["l3"] = {"label": "VALID", "source": config.model}
            else:
                kw_out = kw_str
            l3_valid.append(kw_out)

        elif label == "TRASH":
            if isinstance(kw_obj, dict):
                kw_out = kw_obj.copy()
                kw_out["anchor_reason"] = "L3_TRASH"
                kw_out["l3"] = {"label": "TRASH", "source": config.model}
            else:
                kw_out = {
                    "keyword": kw_str,
                    "anchor_reason": "L3_TRASH",
                    "l3": {"label": "TRASH", "source": config.model},
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
        "model": config.model,
        "provider": config.provider,
    }

    out["_l3_trace"] = l3_trace

    logger.info(
        f"[L3] Done [provider={config.provider}]: {len(l3_valid)} VALID, "
        f"{len(l3_trash)} TRASH, {len(l3_error)} ERROR | wall: {wall_time}s "
        f"({total_batches} batches × {workers} parallel)"
    )

    return out
