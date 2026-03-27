# LLM Data Pipeline Orchestrator

> Natural language → working data pipelines with automatic error recovery

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green)](https://fastapi.tiangolo.com)
[![OpenAI](https://img.shields.io/badge/GPT--4o-Orchestrator-purple)](https://openai.com)

## Overview

Describe what you want to do with your data in plain English — the LLM generates a structured pipeline definition, executes it step-by-step, and automatically recovers from errors using LLM-guided debugging.

## How It Works

```
"Filter users over 25, group by city, count them, and sort descending"
                              ↓
GPT-4o Pipeline Generator → Structured JSON pipeline definition
                              ↓
Pipeline Executor → Step-by-step execution with tracing
                              ↓
Error Recovery → LLM fixes failed steps automatically
                              ↓
Result: data preview + statistics + full execution trace
```

## Supported Pipeline Steps

| Step | Description |
|------|-------------|
| `load_csv` | Load CSV from URL or inline data |
| `filter_rows` | Filter by condition (==, !=, >, <, contains) |
| `select_columns` | Keep specific columns |
| `rename_columns` | Rename column mapping |
| `aggregate` | Group by + sum/mean/count/max/min |
| `sort` | Sort ascending or descending |
| `add_column` | Computed column via expression |
| `deduplicate` | Remove duplicate rows |
| `fill_nulls` | Fill missing values (value/mean/median/mode) |
| `limit` | Limit row count |
| `describe` | Statistical summary |
| `export_csv/json` | Export result |

## Quick Start

```bash
git clone https://github.com/bharghavaram/llm-pipeline-orchestrator
cd llm-pipeline-orchestrator
pip install -r requirements.txt
cp .env.example .env
uvicorn main:app --reload
```

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/pipeline/create` | NL → generate + execute pipeline |
| POST | `/api/v1/pipeline/generate` | NL → pipeline definition only |
| POST | `/api/v1/pipeline/execute` | Execute a pipeline definition |
| GET | `/api/v1/pipeline/list` | List all pipelines |

### Example

```bash
curl -X POST "http://localhost:8000/api/v1/pipeline/create" \
  -H "Content-Type: application/json" \
  -d '{"request": "Load the CSV from https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv, select only the first_name and last_name columns, remove duplicates, and sort by last_name"}'
```
