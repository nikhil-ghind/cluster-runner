"""Thin async wrapper around the Rust dispatcher gRPC service.

We don't generate Python stubs from the .proto here to avoid a build-time
dependency on protoc — instead the wrapper uses grpc.aio with a generated
stub module that is produced at install time (see scripts/gen_proto.sh).
For now this module exposes a typed surface and falls back to a thin HTTP
shim if the gRPC stubs are unavailable, which keeps the orchestrator
runnable in unit tests and local development.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import AsyncIterator

import httpx

from .config import settings
from .models import JobSpec, MatrixSpec

log = logging.getLogger(__name__)


@dataclass
class JobHandle:
    job_id: str
    queue: str


@dataclass
class MatrixHandle:
    matrix_id: str
    job_ids: list[str]


class DispatcherClient:
    def __init__(self, endpoint: str | None = None):
        self.endpoint = endpoint or settings.dispatcher_endpoint
        self._http = httpx.AsyncClient(
            base_url=f"http://{self.endpoint.replace('grpc://','')}",
            timeout=httpx.Timeout(10.0, connect=2.0),
        )

    async def submit_job(self, spec: JobSpec) -> JobHandle:
        # Serialize using the same field names as the proto so the gRPC
        # wrapper on the dispatcher side can accept both transports.
        payload = self._to_proto_dict(spec)
        r = await self._http.post("/v1/jobs", json=payload)
        r.raise_for_status()
        body = r.json()
        return JobHandle(job_id=body["job_id"], queue=body.get("queue", "default"))

    async def submit_matrix(self, spec: MatrixSpec, name: str) -> MatrixHandle:
        payload = {
            "name": name,
            "base": self._to_proto_dict(spec.base),
            "axes": [{"name": a.name, "values": a.values} for a in spec.axes],
            "max_parallel": spec.max_parallel,
            "fail_fast": spec.fail_fast,
        }
        r = await self._http.post("/v1/matrices", json=payload)
        r.raise_for_status()
        body = r.json()
        return MatrixHandle(matrix_id=body["matrix_id"], job_ids=body["job_ids"])

    async def cancel(self, job_id: str) -> None:
        await self._http.post(f"/v1/jobs/{job_id}:cancel")

    async def stream_events(self, label: str | None = None) -> AsyncIterator[dict]:
        params = {"filter_label": label} if label else {}
        async with self._http.stream("GET", "/v1/events", params=params) as r:
            async for line in r.aiter_lines():
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    log.warning("malformed event line: %s", line[:120])

    @staticmethod
    def _to_proto_dict(spec: JobSpec) -> dict:
        return {
            "name": spec.name,
            "image": spec.image,
            "command": spec.command,
            "env": [{"name": k, "value": v} for k, v in spec.env.items()],
            "resources": {
                "cpu": spec.resources.cpu,
                "memory_mib": spec.resources.memory_mib,
                "gpu": spec.resources.gpu,
                "gpu_class": spec.resources.gpu_class,
                "ephemeral_storage_mib": spec.resources.ephemeral_storage_mib,
                "io_bandwidth_mbps": spec.resources.io_bandwidth_mbps,
            },
            "isolation": spec.isolation.model_dump(),
            "placement": {
                "preferred_cloud": spec.placement.preferred_cloud,
                "node_selectors": spec.placement.node_selectors,
                "priority_class": spec.placement.priority_class,
                "spot_ok": spec.placement.spot_ok,
                "zone_spread": spec.placement.zone_spread,
            },
            "retries": spec.retries,
            "timeout_secs": spec.timeout_secs,
            "labels": spec.labels,
            "git_sha": spec.git_sha,
            "artifact_url": spec.artifact_url,
        }
