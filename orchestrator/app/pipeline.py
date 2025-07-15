"""Pipeline expansion, DAG validation, and submission to the dispatcher.

A *pipeline* is the unit of work submitted by a CI integration. Each
pipeline is one or more jobs (with explicit `depends_on` for sequencing)
plus optional matrix sweeps. The orchestrator:

  1. Validates the DAG (no cycles, all `depends_on` references resolve).
  2. Expands matrices into concrete jobs (cartesian product over axes).
  3. Topologically sorts the DAG into stages and submits each stage to
     the dispatcher only after its predecessors succeed.
  4. Records every transition in Postgres so pipelines survive an
     orchestrator restart.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from itertools import product
from typing import Iterable

from .dispatcher_client import DispatcherClient
from .models import JobSpec, MatrixSpec, Phase, Pipeline, PipelineRecord, JobRecord

log = logging.getLogger(__name__)


class CycleError(ValueError):
    pass


class UnknownDependencyError(ValueError):
    pass


def expand_matrices(matrices: list[MatrixSpec]) -> list[JobSpec]:
    """Cartesian product over axes; one JobSpec per combination."""
    out: list[JobSpec] = []
    for matrix in matrices:
        axis_value_lists = [[(a.name, v) for v in a.values] for a in matrix.axes]
        if not axis_value_lists:
            out.append(matrix.base.model_copy(deep=True))
            continue
        for combo in product(*axis_value_lists):
            base = matrix.base.model_copy(deep=True)
            suffix = ",".join(f"{k}={v}" for k, v in combo)
            base.name = f"{matrix.base.name}::{suffix}"
            for k, v in combo:
                base.env[f"MATRIX_{k.upper()}"] = v
                base.labels[k] = v
            out.append(base)
    return out


def topo_sort(jobs: list[JobSpec]) -> list[list[JobSpec]]:
    """Kahn's algorithm. Returns stages of jobs that can run in parallel."""
    name_to_job = {j.name: j for j in jobs}
    indeg: dict[str, int] = {j.name: 0 for j in jobs}
    fwd: dict[str, list[str]] = defaultdict(list)
    for j in jobs:
        for dep in j.depends_on:
            if dep not in name_to_job:
                raise UnknownDependencyError(f"{j.name} depends on unknown job {dep}")
            indeg[j.name] += 1
            fwd[dep].append(j.name)

    stages: list[list[JobSpec]] = []
    ready = [n for n, d in indeg.items() if d == 0]
    seen = 0
    while ready:
        stage = [name_to_job[n] for n in ready]
        stages.append(stage)
        seen += len(ready)
        next_ready: list[str] = []
        for n in ready:
            for child in fwd[n]:
                indeg[child] -= 1
                if indeg[child] == 0:
                    next_ready.append(child)
        ready = next_ready
    if seen != len(jobs):
        raise CycleError("pipeline DAG has a cycle")
    return stages


class PipelineRunner:
    """Owns the lifecycle of one running pipeline."""

    def __init__(self, dispatcher: DispatcherClient, store: "PipelineStore"):
        self.dispatcher = dispatcher
        self.store = store

    async def submit(self, pipeline: Pipeline) -> PipelineRecord:
        # 1. expand matrices
        all_jobs: list[JobSpec] = list(pipeline.jobs)
        all_jobs.extend(expand_matrices(pipeline.matrices))

        # 2. validate
        stages = topo_sort(all_jobs)

        # 3. record + persist
        rec = PipelineRecord(
            id=str(uuid.uuid4()),
            name=pipeline.name,
            git_sha=pipeline.git_sha,
            created_at=datetime.now(timezone.utc),
            phase=Phase.PENDING,
            jobs=[
                JobRecord(id=str(uuid.uuid4()), pipeline_id="", name=j.name, phase=Phase.PENDING)
                for j in all_jobs
            ],
        )
        for jr in rec.jobs:
            jr.pipeline_id = rec.id
        await self.store.create(rec)

        # 4. run stages — launched in the background.
        asyncio.create_task(self._run(rec, stages))
        return rec

    async def _run(self, rec: PipelineRecord, stages: list[list[JobSpec]]):
        rec.phase = Phase.RUNNING
        await self.store.update_phase(rec.id, Phase.RUNNING)
        try:
            for stage in stages:
                log.info("pipeline %s — stage of %d jobs", rec.name, len(stage))
                handles = await asyncio.gather(*(self.dispatcher.submit_job(j) for j in stage))
                results = await asyncio.gather(*(self._await_job(h.job_id) for h in handles))
                if any(p != Phase.SUCCEEDED for p in results):
                    rec.phase = Phase.FAILED
                    await self.store.update_phase(rec.id, Phase.FAILED)
                    return
            rec.phase = Phase.SUCCEEDED
            await self.store.update_phase(rec.id, Phase.SUCCEEDED)
        except Exception as e:
            log.exception("pipeline %s failed: %s", rec.name, e)
            rec.phase = Phase.FAILED
            await self.store.update_phase(rec.id, Phase.FAILED)

    async def _await_job(self, job_id: str) -> Phase:
        async for evt in self.dispatcher.stream_events():
            if evt.get("job_id") != job_id:
                continue
            phase = Phase(evt["phase"])
            if phase in (Phase.SUCCEEDED, Phase.FAILED, Phase.CANCELED, Phase.TIMED_OUT):
                return phase
        return Phase.FAILED


class PipelineStore:
    """In-memory store with the same shape as the Postgres-backed one.

    The production deploy swaps this for SQLAlchemy/asyncpg; the
    interface (create / update_phase / get) is identical so callers
    don't care which is wired up.
    """

    def __init__(self) -> None:
        self._records: dict[str, PipelineRecord] = {}

    async def create(self, rec: PipelineRecord) -> None:
        self._records[rec.id] = rec

    async def update_phase(self, pid: str, phase: Phase) -> None:
        if pid in self._records:
            self._records[pid].phase = phase

    async def get(self, pid: str) -> PipelineRecord | None:
        return self._records.get(pid)

    async def list(self) -> Iterable[PipelineRecord]:
        return list(self._records.values())
