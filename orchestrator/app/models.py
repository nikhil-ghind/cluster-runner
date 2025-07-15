"""Pydantic models for the orchestrator API and persisted pipeline state."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Phase(str, Enum):
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    TIMED_OUT = "TIMED_OUT"


class Resources(BaseModel):
    cpu: float = 1.0
    memory_mib: int = 1024
    gpu: int = 0
    gpu_class: str = ""
    ephemeral_storage_mib: int = 0
    io_bandwidth_mbps: int = 0


class Isolation(BaseModel):
    pid_namespace: bool = True
    mount_namespace: bool = True
    net_namespace: bool = True
    user_namespace: bool = True
    ipc_namespace: bool = True
    cgroup_namespace: bool = True
    seccomp_profile: str = "runtime/default"
    apparmor_profile: str = "runtime/default"
    readonly_rootfs: bool = True
    drop_capabilities: bool = True


class Placement(BaseModel):
    preferred_cloud: str = "onprem"  # "onprem"|"aws"|"gcp"|"azure"
    node_selectors: list[str] = []
    priority_class: str = "normal"
    spot_ok: bool = False
    zone_spread: int = 0


class JobSpec(BaseModel):
    name: str
    image: str
    command: list[str] = []
    env: dict[str, str] = {}
    resources: Resources = Resources()
    isolation: Isolation = Isolation()
    placement: Placement = Placement()
    retries: int = 0
    timeout_secs: int = 3600
    labels: dict[str, str] = {}
    git_sha: str = ""
    artifact_url: str = ""
    depends_on: list[str] = Field(default_factory=list,
        description="Names of jobs (within the same pipeline) that must succeed first.")


class MatrixAxis(BaseModel):
    name: str
    values: list[str]


class MatrixSpec(BaseModel):
    base: JobSpec
    axes: list[MatrixAxis]
    max_parallel: int = 16
    fail_fast: bool = False


class Pipeline(BaseModel):
    name: str
    git_sha: str = ""
    jobs: list[JobSpec] = []
    matrices: list[MatrixSpec] = []


class JobRecord(BaseModel):
    id: str
    pipeline_id: str
    name: str
    phase: Phase
    exit_code: Optional[int] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    node: Optional[str] = None
    pod: Optional[str] = None
    attempt: int = 0
    cpu_seconds: float = 0.0
    peak_rss_bytes: int = 0


class PipelineRecord(BaseModel):
    id: str
    name: str
    git_sha: str
    created_at: datetime
    phase: Phase
    jobs: list[JobRecord] = []
