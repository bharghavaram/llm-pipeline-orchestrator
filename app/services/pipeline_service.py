"""
LLM Data Pipeline Orchestrator – Natural language → working data pipelines.
Users describe what they want in plain English; the LLM generates and executes
a structured pipeline of data transformation steps.
"""
import logging
import json
import uuid
import io
import re
from datetime import datetime
from typing import Optional, List
import pandas as pd
from openai import OpenAI
from app.core.config import settings

logger = logging.getLogger(__name__)

PIPELINE_GENERATION_PROMPT = """You are a data engineering expert. Generate a data pipeline from this natural language description.

User Request: {request}

Available Pipeline Steps (use ONLY these):
- load_csv: Load CSV data (params: url or inline data)
- filter_rows: Filter rows by condition (params: column, operator, value)
- select_columns: Keep only specified columns (params: columns list)
- rename_columns: Rename columns (params: mapping dict)
- aggregate: Group by and aggregate (params: group_by, agg_col, agg_func: sum/mean/count/max/min)
- sort: Sort by column (params: column, ascending)
- add_column: Add computed column (params: name, expression using existing columns)
- deduplicate: Remove duplicate rows (params: subset columns or null for all)
- fill_nulls: Fill missing values (params: column, value or strategy: mean/median/mode)
- limit: Limit number of rows (params: n)
- export_csv: Export result as CSV (params: filename)
- export_json: Export result as JSON (params: filename)
- describe: Show statistical summary

Return ONLY valid JSON:
{{
  "pipeline_name": "descriptive_name",
  "description": "what this pipeline does",
  "steps": [
    {{"step": 1, "action": "action_name", "params": {{}}, "description": "what this step does"}}
  ]
}}"""

STEP_ERROR_RECOVERY_PROMPT = """A pipeline step failed. Suggest a fix.

Step: {step}
Error: {error}
Current Data Schema: {schema}

Return JSON: {{"fixed_step": {{...same structure as step...}}, "explanation": "..."}}"""


class PipelineStep:
    def __init__(self, step: int, action: str, params: dict, description: str = ""):
        self.step = step
        self.action = action
        self.params = params
        self.description = description
        self.status = "pending"
        self.output_rows = 0
        self.error = None
        self.duration_ms = 0

    def to_dict(self):
        return {
            "step": self.step,
            "action": self.action,
            "params": self.params,
            "description": self.description,
            "status": self.status,
            "output_rows": self.output_rows,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


class PipelineExecutor:
    def execute_step(self, df: Optional[pd.DataFrame], step: PipelineStep) -> pd.DataFrame:
        import time
        start = time.time()
        p = step.params

        try:
            if step.action == "load_csv":
                if "url" in p:
                    df = pd.read_csv(p["url"])
                elif "data" in p:
                    df = pd.read_csv(io.StringIO(p["data"]))
                else:
                    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})

            elif step.action == "filter_rows":
                col, op, val = p["column"], p["operator"], p["value"]
                if op == "==": df = df[df[col] == val]
                elif op == "!=": df = df[df[col] != val]
                elif op == ">": df = df[df[col] > float(val)]
                elif op == "<": df = df[df[col] < float(val)]
                elif op == ">=": df = df[df[col] >= float(val)]
                elif op == "<=": df = df[df[col] <= float(val)]
                elif op == "contains": df = df[df[col].astype(str).str.contains(str(val), na=False)]
                elif op == "not_null": df = df[df[col].notna()]

            elif step.action == "select_columns":
                cols = [c for c in p["columns"] if c in df.columns]
                df = df[cols]

            elif step.action == "rename_columns":
                df = df.rename(columns=p["mapping"])

            elif step.action == "aggregate":
                agg_func = p.get("agg_func", "sum")
                df = df.groupby(p["group_by"])[p["agg_col"]].agg(agg_func).reset_index()

            elif step.action == "sort":
                df = df.sort_values(p["column"], ascending=p.get("ascending", True)).reset_index(drop=True)

            elif step.action == "add_column":
                df[p["name"]] = df.eval(p["expression"])

            elif step.action == "deduplicate":
                subset = p.get("subset")
                df = df.drop_duplicates(subset=subset).reset_index(drop=True)

            elif step.action == "fill_nulls":
                col = p["column"]
                strategy = p.get("strategy", "value")
                if strategy == "mean": df[col] = df[col].fillna(df[col].mean())
                elif strategy == "median": df[col] = df[col].fillna(df[col].median())
                elif strategy == "mode": df[col] = df[col].fillna(df[col].mode()[0])
                else: df[col] = df[col].fillna(p.get("value", 0))

            elif step.action == "limit":
                df = df.head(int(p.get("n", 100)))

            elif step.action == "describe":
                pass  # Just pass through, stats captured in result

            elif step.action in ("export_csv", "export_json"):
                pass  # Data stays in memory in API mode

            step.status = "completed"
            step.output_rows = len(df) if df is not None else 0

        except Exception as exc:
            step.status = "failed"
            step.error = str(exc)
            logger.error("Step %d (%s) failed: %s", step.step, step.action, exc)

        step.duration_ms = round((time.time() - start) * 1000, 1)
        return df


class PipelineOrchestrator:
    def __init__(self):
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        self.executor = PipelineExecutor()
        self._pipelines: dict = {}

    def generate_pipeline(self, request: str) -> dict:
        resp = self.client.chat.completions.create(
            model=settings.LLM_MODEL,
            messages=[{"role": "user", "content": PIPELINE_GENERATION_PROMPT.format(request=request)}],
            response_format={"type": "json_object"},
            temperature=settings.TEMPERATURE,
        )
        try:
            pipeline_def = json.loads(resp.choices[0].message.content)
        except Exception as exc:
            raise ValueError(f"Failed to parse pipeline definition: {exc}")
        return pipeline_def

    def execute_pipeline(self, pipeline_def: dict) -> dict:
        pipeline_id = str(uuid.uuid4())
        steps_data = pipeline_def.get("steps", [])
        steps = [PipelineStep(s["step"], s["action"], s.get("params", {}), s.get("description", "")) for s in steps_data]

        df = None
        for step in steps[:settings.MAX_PIPELINE_STEPS]:
            df = self.executor.execute_step(df, step)
            if step.status == "failed":
                # Attempt LLM-guided recovery
                schema = str(df.dtypes.to_dict()) if df is not None else "No data"
                recovery_resp = self.client.chat.completions.create(
                    model=settings.LLM_MODEL,
                    messages=[{"role": "user", "content": STEP_ERROR_RECOVERY_PROMPT.format(
                        step=json.dumps(step.to_dict()), error=step.error, schema=schema
                    )}],
                    response_format={"type": "json_object"},
                    temperature=0.1,
                )
                try:
                    recovery = json.loads(recovery_resp.choices[0].message.content)
                    fixed = recovery.get("fixed_step", {})
                    step.action = fixed.get("action", step.action)
                    step.params = fixed.get("params", step.params)
                    step.status = "retrying"
                    df = self.executor.execute_step(df, step)
                except Exception:
                    pass

        # Build result
        result_preview = []
        stats = {}
        output_format = "dataframe"
        if df is not None and len(df) > 0:
            result_preview = df.head(50).to_dict(orient="records")
            stats = {
                "rows": len(df),
                "columns": list(df.columns),
                "dtypes": {k: str(v) for k, v in df.dtypes.items()},
                "null_counts": df.isnull().sum().to_dict(),
            }
            # Check if last step is describe
            last_step = steps[-1] if steps else None
            if last_step and last_step.action == "describe":
                stats["describe"] = df.describe().to_dict()

        pipeline_result = {
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_def.get("pipeline_name", "unnamed"),
            "description": pipeline_def.get("description", ""),
            "steps_executed": len(steps),
            "steps_completed": sum(1 for s in steps if s.status == "completed"),
            "steps_failed": sum(1 for s in steps if s.status == "failed"),
            "total_duration_ms": sum(s.duration_ms for s in steps),
            "step_details": [s.to_dict() for s in steps],
            "output_stats": stats,
            "result_preview": result_preview,
            "timestamp": datetime.utcnow().isoformat(),
        }
        self._pipelines[pipeline_id] = pipeline_result
        return pipeline_result

    def nl_to_pipeline(self, request: str) -> dict:
        pipeline_def = self.generate_pipeline(request)
        result = self.execute_pipeline(pipeline_def)
        result["original_request"] = request
        result["generated_pipeline"] = pipeline_def
        return result

    def get_pipeline(self, pipeline_id: str) -> Optional[dict]:
        return self._pipelines.get(pipeline_id)

    def list_pipelines(self) -> list:
        return [{"pipeline_id": k, "name": v["pipeline_name"], "steps": v["steps_executed"], "timestamp": v["timestamp"]} for k, v in self._pipelines.items()]


_orchestrator: Optional[PipelineOrchestrator] = None
def get_pipeline_orchestrator() -> PipelineOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = PipelineOrchestrator()
    return _orchestrator
