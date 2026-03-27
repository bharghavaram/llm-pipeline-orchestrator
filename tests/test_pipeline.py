"""Tests for LLM Pipeline Orchestrator."""
import pytest
import pandas as pd
from app.core.config import settings
from app.services.pipeline_service import PipelineStep, PipelineExecutor

def test_settings():
    assert settings.MAX_PIPELINE_STEPS == 10
    assert settings.TEMPERATURE == 0.2

def test_pipeline_step_creation():
    step = PipelineStep(1, "filter_rows", {"column": "age", "operator": ">", "value": 18}, "Filter adults")
    assert step.step == 1
    assert step.action == "filter_rows"
    assert step.status == "pending"

def test_executor_filter_rows():
    executor = PipelineExecutor()
    df = pd.DataFrame({"age": [15, 25, 10, 30], "name": ["A", "B", "C", "D"]})
    step = PipelineStep(1, "filter_rows", {"column": "age", "operator": ">", "value": 18})
    result = executor.execute_step(df, step)
    assert len(result) == 2
    assert step.status == "completed"

def test_executor_select_columns():
    executor = PipelineExecutor()
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    step = PipelineStep(1, "select_columns", {"columns": ["a", "b"]})
    result = executor.execute_step(df, step)
    assert list(result.columns) == ["a", "b"]

def test_executor_deduplicate():
    executor = PipelineExecutor()
    df = pd.DataFrame({"x": [1, 1, 2, 2], "y": ["a", "a", "b", "b"]})
    step = PipelineStep(1, "deduplicate", {})
    result = executor.execute_step(df, step)
    assert len(result) == 2

def test_executor_limit():
    executor = PipelineExecutor()
    df = pd.DataFrame({"x": range(100)})
    step = PipelineStep(1, "limit", {"n": 10})
    result = executor.execute_step(df, step)
    assert len(result) == 10

def test_executor_sort():
    executor = PipelineExecutor()
    df = pd.DataFrame({"score": [3, 1, 4, 1, 5]})
    step = PipelineStep(1, "sort", {"column": "score", "ascending": False})
    result = executor.execute_step(df, step)
    assert result["score"].iloc[0] == 5

@pytest.mark.asyncio
async def test_api_health():
    from fastapi.testclient import TestClient
    from main import app
    client = TestClient(app)
    resp = client.get("/api/v1/pipeline/health/check")
    assert resp.status_code == 200
