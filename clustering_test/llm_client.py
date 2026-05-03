"""
Унифицированный LLM-клиент: OpenAI, Gemini.
Возвращает dict с raw_response, tokens_in, tokens_out, api_time_sec.
"""
import os
import time
import asyncio
import json
from dataclasses import dataclass

import httpx


@dataclass
class ModelConfig:
    name: str
    provider: str  # 'openai' или 'gemini'
    api_model: str
    input_per_1m: float
    output_per_1m: float
    # OpenAI reasoning_effort: None = не передавать (non-reasoning или модель отбивает параметр).
    # Допустимые значения зависят от семейства:
    #   gpt-5/gpt-5-mini: 'minimal' | 'low' | 'medium' | 'high'
    #   gpt-5.5:          'none' | 'low' | 'medium' | 'high' | 'xhigh'
    #   gpt-5-nano:       параметр не поддерживается (400 Bad Request) → None
    #   gpt-4.x:          параметр не существует → None
    reasoning_effort: str | None = None
    # Gemini thinking_budget: None = не передавать (использовать default модели).
    # Допустимые значения (для Gemini 2.5 series):
    #   gemini-2.5-flash-lite: thinking off by default → None или 0
    #   gemini-2.5-flash:      thinking on by default → 0 чтобы выключить, иначе медленнее
    #   gemini-2.5-pro:        thinking ВСЕГДА on, нельзя 0 → минимум 128, максимум 32768, или -1 (dynamic)
    thinking_budget: int | None = None
    # Gemini thinking_level: новый параметр для 3.x моделей. None = не передавать (default high).
    # Допустимые значения: 'minimal' | 'low' | 'medium' | 'high'.
    # Для аналога thinking_budget=0 в 2.5 → используй thinking_level='minimal'.
    # ВАЖНО: нельзя одновременно с thinking_budget — модель отбьёт запрос. Используй ровно один.
    thinking_level: str | None = None


MODELS: dict[str, ModelConfig] = {
    # ── Gemini ────────────────────────────────────────────────────────
    'gemini-2.5-flash-lite': ModelConfig(
        name='gemini-2.5-flash-lite',
        provider='gemini',
        api_model='gemini-2.5-flash-lite',
        input_per_1m=0.10,
        output_per_1m=0.40,
    ),
    'gemini-2.5-flash': ModelConfig(
        name='gemini-2.5-flash',
        provider='gemini',
        api_model='gemini-2.5-flash',
        input_per_1m=0.30,
        output_per_1m=2.50,
        thinking_budget=0,  # выключаем thinking — иначе по дефолту включён, заметно медленнее
    ),
    'gemini-2.5-pro': ModelConfig(
        name='gemini-2.5-pro',
        provider='gemini',
        api_model='gemini-2.5-pro',
        input_per_1m=1.25,
        output_per_1m=10.00,
        thinking_budget=128,  # минимум для pro (нельзя 0); 128-32768 диапазон, или -1 для dynamic
    ),
    # ── Gemini 3.x · preview, используют thinking_level вместо thinking_budget ──
    # API field: thinkingConfig.thinkingLevel = 'minimal'|'low'|'medium'|'high' (default: 'high').
    # Для скорости ставим 'minimal' — аналог thinking_budget=0 в 2.5 series.
    'gemini-3.1-flash-lite-preview': ModelConfig(
        name='gemini-3.1-flash-lite-preview',
        provider='gemini',
        api_model='gemini-3.1-flash-lite-preview',
        input_per_1m=0.25,
        output_per_1m=1.50,
        thinking_level='minimal',
    ),
    'gemini-3-flash-preview': ModelConfig(
        name='gemini-3-flash-preview',
        provider='gemini',
        api_model='gemini-3-flash-preview',
        input_per_1m=0.50,
        output_per_1m=3.00,
        thinking_level='minimal',
    ),
    'gemini-3.1-pro-preview': ModelConfig(
        name='gemini-3.1-pro-preview',
        provider='gemini',
        api_model='gemini-3.1-pro-preview',
        input_per_1m=2.00,
        output_per_1m=12.00,
        thinking_level='minimal',  # минимум для скорости, иначе reasoning будет долгим
    ),
    # ── OpenAI · non-reasoning (reasoning_effort не передаём) ─────────
    'gpt-4.1-nano': ModelConfig(
        name='gpt-4.1-nano',
        provider='openai',
        api_model='gpt-4.1-nano',
        input_per_1m=0.10,
        output_per_1m=0.40,
    ),
    'gpt-4o-mini': ModelConfig(
        name='gpt-4o-mini',
        provider='openai',
        api_model='gpt-4o-mini',
        input_per_1m=0.15,
        output_per_1m=0.60,
    ),
    'gpt-4.1-mini': ModelConfig(
        name='gpt-4.1-mini',
        provider='openai',
        api_model='gpt-4.1-mini',
        input_per_1m=0.40,
        output_per_1m=1.60,
    ),
    'gpt-4.1': ModelConfig(
        name='gpt-4.1',
        provider='openai',
        api_model='gpt-4.1',
        input_per_1m=2.00,
        output_per_1m=8.00,
    ),
    # ── OpenAI · reasoning ────────────────────────────────────────────
    # gpt-5-nano: параметр reasoning отбивается с 400 (см. OpenAI Community) → None.
    'gpt-5-nano': ModelConfig(
        name='gpt-5-nano',
        provider='openai',
        api_model='gpt-5-nano',
        input_per_1m=0.05,
        output_per_1m=0.40,
        reasoning_effort=None,
    ),
    # gpt-5-mini: 'minimal' — самый быстрый поддерживаемый режим.
    'gpt-5-mini': ModelConfig(
        name='gpt-5-mini',
        provider='openai',
        api_model='gpt-5-mini',
        input_per_1m=0.25,
        output_per_1m=2.00,
        reasoning_effort='minimal',
    ),
    # gpt-5: 'minimal' — самый быстрый поддерживаемый режим.
    'gpt-5': ModelConfig(
        name='gpt-5',
        provider='openai',
        api_model='gpt-5',
        input_per_1m=1.25,
        output_per_1m=10.00,
        reasoning_effort='minimal',
    ),
    # gpt-5.5: 'none' — отключает reasoning полностью (поддерживается только этим семейством).
    'gpt-5.5': ModelConfig(
        name='gpt-5.5',
        provider='openai',
        api_model='gpt-5.5',
        input_per_1m=1.25,
        output_per_1m=10.00,
        reasoning_effort='none',
    ),
}


async def call_openai(
    cfg: ModelConfig,
    system_prompt: str,
    user_prompt: str,
    timeout: float = 60.0,
) -> dict:
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        raise RuntimeError('OPENAI_API_KEY not set')
    
    payload = {
        'model': cfg.api_model,
        'messages': [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt},
        ],
    }
    if cfg.reasoning_effort is not None:
        payload['reasoning_effort'] = cfg.reasoning_effort
    
    t0 = time.time()
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            'https://api.openai.com/v1/chat/completions',
            headers={'Authorization': f'Bearer {api_key}'},
            json=payload,
        )
    api_time = time.time() - t0
    if resp.status_code >= 400:
        body = resp.text[:500]
        raise RuntimeError(f'OpenAI {resp.status_code}: {body}')
    data = resp.json()
    
    return {
        'raw_response': data['choices'][0]['message']['content'],
        'tokens_in': data['usage']['prompt_tokens'],
        'tokens_out': data['usage']['completion_tokens'],
        'api_time_sec': round(api_time, 3),
    }


async def call_gemini(
    cfg: ModelConfig,
    system_prompt: str,
    user_prompt: str,
    timeout: float = 60.0,
) -> dict:
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        raise RuntimeError('GEMINI_API_KEY not set')
    
    url = (
        f'https://generativelanguage.googleapis.com/v1beta/models/'
        f'{cfg.api_model}:generateContent?key={api_key}'
    )
    payload = {
        'systemInstruction': {'parts': [{'text': system_prompt}]},
        'contents': [{'role': 'user', 'parts': [{'text': user_prompt}]}],
        'generationConfig': {
            'temperature': 0,
        },
    }
    # thinking_budget (старый, 2.5 series) и thinking_level (новый, 3.x series) —
    # взаимоисключающие. Передаём ровно один. Если конфиг содержит оба —
    # это ошибка конфигурации, отбиваем явно.
    if cfg.thinking_budget is not None and cfg.thinking_level is not None:
        raise ValueError(
            f'Model {cfg.name}: cannot use thinking_budget and thinking_level '
            f'simultaneously. Use one of them.'
        )
    if cfg.thinking_level is not None:
        payload['generationConfig']['thinkingConfig'] = {
            'thinkingLevel': cfg.thinking_level,
        }
    elif cfg.thinking_budget is not None:
        payload['generationConfig']['thinkingConfig'] = {
            'thinkingBudget': cfg.thinking_budget,
        }
    
    t0 = time.time()
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=payload)
    api_time = time.time() - t0
    resp.raise_for_status()
    data = resp.json()
    
    text = data['candidates'][0]['content']['parts'][0]['text']
    usage = data.get('usageMetadata', {})
    
    return {
        'raw_response': text,
        'tokens_in': usage.get('promptTokenCount', 0),
        'tokens_out': usage.get('candidatesTokenCount', 0),
        'api_time_sec': round(api_time, 3),
    }


async def call_llm(
    model_name: str,
    system_prompt: str,
    user_prompt: str,
) -> dict:
    if model_name not in MODELS:
        raise ValueError(f'Unknown model: {model_name}. Available: {list(MODELS)}')
    cfg = MODELS[model_name]
    if cfg.provider == 'openai':
        return await call_openai(cfg, system_prompt, user_prompt)
    elif cfg.provider == 'gemini':
        return await call_gemini(cfg, system_prompt, user_prompt)
    else:
        raise ValueError(f'Unknown provider: {cfg.provider}')


def calc_cost(model_name: str, tokens_in: int, tokens_out: int) -> float:
    cfg = MODELS[model_name]
    return round(
        tokens_in * cfg.input_per_1m / 1_000_000
        + tokens_out * cfg.output_per_1m / 1_000_000,
        6,
    )
