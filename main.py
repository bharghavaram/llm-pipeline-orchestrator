"""LLM Pipeline Orchestrator – FastAPI Entry Point."""
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes.pipeline import router
from app.core.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s – %(message)s")

app = FastAPI(
    title="LLM Data Pipeline Orchestrator",
    description="Natural language → working data pipelines. Describe what you want in plain English; GPT-4o generates, executes, and self-corrects a structured ETL pipeline with automatic error recovery.",
    version="1.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(router, prefix="/api/v1")

@app.get("/")
async def root():
    return {
        "service": "LLM Data Pipeline Orchestrator",
        "version": "1.0.0",
        "supported_steps": ["load_csv", "filter_rows", "select_columns", "rename_columns", "aggregate", "sort", "add_column", "deduplicate", "fill_nulls", "limit", "describe", "export_csv", "export_json"],
        "features": ["Natural language pipeline generation", "Automatic error recovery (LLM-guided)", "Step-by-step execution tracing", "Result preview and statistics"],
        "docs": "/docs",
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=settings.APP_HOST, port=settings.APP_PORT, reload=True)
