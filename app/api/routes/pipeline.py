"""LLM Pipeline Orchestrator – API routes."""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional
from app.services.pipeline_service import PipelineOrchestrator, get_pipeline_orchestrator

router = APIRouter(prefix="/pipeline", tags=["Pipeline Orchestrator"])

class NLRequest(BaseModel):
    request: str

class PipelineDefRequest(BaseModel):
    pipeline_def: dict

@router.post("/create")
async def create_from_nl(req: NLRequest, orch: PipelineOrchestrator = Depends(get_pipeline_orchestrator)):
    if len(req.request.strip()) < 10:
        raise HTTPException(400, "Request too short (min 10 chars)")
    return orch.nl_to_pipeline(req.request)

@router.post("/generate")
async def generate_pipeline(req: NLRequest, orch: PipelineOrchestrator = Depends(get_pipeline_orchestrator)):
    return orch.generate_pipeline(req.request)

@router.post("/execute")
async def execute_pipeline(req: PipelineDefRequest, orch: PipelineOrchestrator = Depends(get_pipeline_orchestrator)):
    return orch.execute_pipeline(req.pipeline_def)

@router.get("/list")
async def list_pipelines(orch: PipelineOrchestrator = Depends(get_pipeline_orchestrator)):
    return {"pipelines": orch.list_pipelines()}

@router.get("/{pipeline_id}")
async def get_pipeline(pipeline_id: str, orch: PipelineOrchestrator = Depends(get_pipeline_orchestrator)):
    result = orch.get_pipeline(pipeline_id)
    if not result:
        raise HTTPException(404, f"Pipeline {pipeline_id} not found")
    return result

@router.get("/health/check")
async def health():
    return {"status": "ok", "service": "LLM Data Pipeline Orchestrator – Natural Language → Data Pipelines"}
