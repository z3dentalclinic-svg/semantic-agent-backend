"""
FastAPI роуты для тестового модуля кластеризации.

Подключение в main.py:
    from clustering_test.endpoint import register_clustering_test_endpoint
    register_clustering_test_endpoint(app)
"""
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .runner import run_debug_payloads, run_clustering
from .llm_client import MODELS


class DebugRequest(BaseModel):
    light_search_result: dict[str, Any] = Field(..., description='Полный JSON из /api/light-search или объект с seed/keywords/l3_stats')


class ClusterRequest(BaseModel):
    light_search_result: dict[str, Any] = Field(..., description='Полный JSON из /api/light-search или объект с seed/keywords/l3_stats')
    model: str = Field(..., description=f'Модель: {list(MODELS.keys())}')


def register_clustering_test_endpoint(app: FastAPI) -> None:
    """Регистрирует роуты тестовой кластеризации в FastAPI-приложении."""

    @app.post('/api/test-clustering/debug-payloads', tags=['clustering-test'])
    async def debug_payloads(req: DebugRequest):
        """Извлекает хвосты из ключей и возвращает диагностику. БЕЗ вызова LLM."""
        try:
            return await run_debug_payloads(req.light_search_result)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post('/api/test-clustering', tags=['clustering-test'])
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

    @app.get('/api/test-clustering/models', tags=['clustering-test'])
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
