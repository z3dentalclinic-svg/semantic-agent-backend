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


MODELS: dict[str, ModelConfig] = {
    'gemini-2.5-flash-lite': ModelConfig(
        name='gemini-2.5-flash-lite',
        provider='gemini',
        api_model='gemini-2.5-flash-lite',
        input_per_1m=0.10,
        output_per_1m=0.40,
    ),
    'gpt-5-nano': ModelConfig(
        name='gpt-5-nano',
        provider='openai',
        api_model='gpt-5-nano',
        input_per_1m=0.05,
        output_per_1m=0.40,
    ),
    'gpt-5-mini': ModelConfig(
        name='gpt-5-mini',
        provider='openai',
        api_model='gpt-5-mini',
        input_per_1m=0.25,
        output_per_1m=2.00,
    ),
    'gpt-5.5': ModelConfig(
        name='gpt-5.5',
        provider='openai',
        api_model='gpt-5.5',
        input_per_1m=1.25,
        output_per_1m=10.00,
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
        'reasoning_effort': 'none',
    }
    
    t0 = time.time()
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            'https://api.openai.com/v1/chat/completions',
            headers={'Authorization': f'Bearer {api_key}'},
            json=payload,
        )
    api_time = time.time() - t0
    resp.raise_for_status()
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
