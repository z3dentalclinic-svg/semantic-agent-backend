"""
FastAPI роуты для тестового модуля кластеризации.

Подключение в main.py:
    from clustering_test.endpoint import router as clustering_test_router
    app.include_router(clustering_test_router)
"""
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .runner import run_debug_payloads, run_clustering
from .llm_client import MODELS


router = APIRouter(prefix='/api/test-clustering', tags=['clustering-test'])


class DebugRequest(BaseModel):
    light_search_result: dict[str, Any] = Field(..., description='Полный JSON из /api/light-search')


class ClusterRequest(BaseModel):
    light_search_result: dict[str, Any] = Field(..., description='Полный JSON из /api/light-search')
    model: str = Field(..., description=f'Модель: {list(MODELS.keys())}')


@router.post('/debug-payloads')
async def debug_payloads(req: DebugRequest):
    """Извлекает хвосты и возвращает диагностику. БЕЗ вызова LLM."""
    try:
        return await run_debug_payloads(req.light_search_result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post('')
async def cluster(req: ClusterRequest):
    """Полный пайплайн: extract → LLM → expand."""
    if req.model not in MODELS:
        raise HTTPException(
            status_code=400,
            detail=f'Unknown model: {req.model}. Available: {list(MODELS.keys())}',
        )
    try:
        return await run_clustering(req.light_search_result, req.model)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get('/models')
async def list_models():
    """Список поддерживаемых моделей с ценами."""
    return {
        name: {
            'provider': cfg.provider,
            'api_model': cfg.api_model,
            'input_per_1m_usd': cfg.input_per_1m,
            'output_per_1m_usd': cfg.output_per_1m,
        }
        for name, cfg in MODELS.items()
    }
