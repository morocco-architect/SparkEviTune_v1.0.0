from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse

from sparkevitune.models import ClusterProfile, HybridReport, WorkloadProfile
from sparkevitune.parser import InvalidSparkLogError
from sparkevitune.pipeline import SparkEviTunePipeline
from sparkevitune.utils import to_jsonable

app = FastAPI(
    title="SparkEviTune API",
    version="1.0.0",
    description=(
        "Hybrid deterministic, machine-learning and optional LLM-assisted Apache Spark tuning API. "
        "Configurations are never auto-applied."
    ),
)
pipeline = SparkEviTunePipeline()
REPORT_CACHE: dict[str, HybridReport] = {}


@app.get("/health", tags=["system"])
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "version": "1.0.0",
        "history_rows": pipeline.store.count(),
        "models": pipeline.registry.status(),
        "llm_enabled": bool(os.getenv("ANTHROPIC_API_KEY") and os.getenv("SPARKEVITUNE_LLM_MODEL")),
        "auto_apply": False,
    }


@app.post("/analyze/upload", tags=["analysis"])
async def analyze_upload(
    file: UploadFile = File(...),
    workers: int = Query(1, ge=1, le=10000),
    cores_per_worker: int = Query(4, ge=1, le=1024),
    memory_per_worker_gb: float = Query(4.0, gt=0),
    spark_version: str = Query("3.5.0"),
    workload_type: str = Query("unknown"),
    input_size_gb: float = Query(0.0, ge=0),
    num_joins: int = Query(0, ge=0),
    num_aggregations: int = Query(0, ge=0),
    include_explanation: bool = Query(True),
):
    content = await file.read()
    if not content:
        raise HTTPException(400, "Empty file.")
    if len(content) > 500 * 1024 * 1024:
        raise HTTPException(413, "Event log exceeds the 500 MB upload limit.")
    temporary_path = ""
    try:
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".jsonl", delete=False) as handle:
            handle.write(content)
            temporary_path = handle.name
        report = pipeline.analyze(
            temporary_path,
            cluster=ClusterProfile(
                workers=workers,
                cores_per_worker=cores_per_worker,
                memory_per_worker_gb=memory_per_worker_gb,
                spark_version=spark_version,
            ),
            workload=WorkloadProfile(
                workload_type=workload_type,
                input_size_gb=input_size_gb,
                num_joins=num_joins,
                num_aggregations=num_aggregations,
            ),
            include_explanation=include_explanation,
        )
    except InvalidSparkLogError as exc:
        raise HTTPException(422, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(500, f"Analysis failed: {exc}") from exc
    finally:
        if temporary_path:
            Path(temporary_path).unlink(missing_ok=True)
    REPORT_CACHE[report.run_id] = report
    return JSONResponse(to_jsonable(report))


@app.get("/reports/{run_id}", tags=["analysis"])
def get_report(run_id: str):
    report = REPORT_CACHE.get(run_id)
    if report is None:
        raise HTTPException(404, "Report not found in the in-memory cache.")
    return JSONResponse(to_jsonable(report))


@app.post("/models/train", tags=["ml"])
def train_models():
    summary = pipeline.train_models()
    return JSONResponse(to_jsonable(summary))


@app.get("/models/status", tags=["ml"])
def model_status():
    return {
        "history_rows": pipeline.store.count(),
        "models": pipeline.registry.status(),
    }


@app.post("/feedback/{run_id}", tags=["learning"])
def record_feedback(
    run_id: str,
    observed: dict[str, float] = Body(
        ...,
        examples=[{"duration_s": 38.2, "memory_spill_gb": 0.0, "cost": 0.02, "oom": 0}],
    ),
):
    report = REPORT_CACHE.get(run_id)
    if report is None:
        raise HTTPException(404, "Report not found. Analyze the baseline run in this API process first.")
    if "duration_s" not in observed:
        raise HTTPException(400, "observed.duration_s is required.")
    pipeline.record_feedback(report, observed)
    return {"stored": True, "run_id": run_id, "history_rows": pipeline.store.count()}


@app.post("/reports/{run_id}/explain", tags=["llm"])
def regenerate_explanation(run_id: str):
    report = REPORT_CACHE.get(run_id)
    if report is None:
        raise HTTPException(404, "Report not found.")
    report.explanation = pipeline.explainer.explain(report)
    return {"run_id": run_id, "explanation": report.explanation}
