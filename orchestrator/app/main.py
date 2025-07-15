"""FastAPI surface for the orchestrator."""
from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from .config import settings
from .dispatcher_client import DispatcherClient
from .models import JobSpec, Pipeline, PipelineRecord
from .pipeline import PipelineRunner, PipelineStore

logging.basicConfig(level=settings.log_level)
log = logging.getLogger("orchestrator")

app = FastAPI(title="HPC CI Orchestrator", version="0.1.0")

dispatcher = DispatcherClient()
store = PipelineStore()
runner = PipelineRunner(dispatcher, store)

PIPELINE_COUNTER = Counter("orch_pipelines_submitted_total", "submitted pipelines")
ACTIVE_PIPELINES = Gauge("orch_pipelines_active", "active pipelines")


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/v1/pipelines", response_model=PipelineRecord)
async def submit_pipeline(pipeline: Pipeline) -> PipelineRecord:
    if len(pipeline.jobs) + sum(len(m.axes) for m in pipeline.matrices) > settings.max_matrix_jobs:
        raise HTTPException(400, "pipeline exceeds max matrix size")
    PIPELINE_COUNTER.inc()
    ACTIVE_PIPELINES.inc()
    rec = await runner.submit(pipeline)
    return rec


@app.get("/v1/pipelines/{pid}", response_model=PipelineRecord)
async def get_pipeline(pid: str) -> PipelineRecord:
    rec = await store.get(pid)
    if rec is None:
        raise HTTPException(404, "not found")
    return rec


@app.post("/v1/jobs/{job_id}:cancel")
async def cancel_job(job_id: str) -> dict:
    await dispatcher.cancel(job_id)
    return {"ok": True}


@app.get("/v1/events")
async def stream_events(label: str | None = None) -> StreamingResponse:
    async def gen():
        async for evt in dispatcher.stream_events(label=label):
            yield (str(evt) + "\n").encode()
    return StreamingResponse(gen(), media_type="application/x-ndjson")


@app.post("/v1/jobs", response_model=dict)
async def submit_job(spec: JobSpec) -> dict:
    handle = await dispatcher.submit_job(spec)
    return {"job_id": handle.job_id, "queue": handle.queue}
