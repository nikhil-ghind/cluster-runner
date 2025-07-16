"""DAG validation + matrix expansion tests."""
import pytest

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "orchestrator"))

from app.models import JobSpec, MatrixAxis, MatrixSpec, Resources
from app.pipeline import CycleError, UnknownDependencyError, expand_matrices, topo_sort


def J(name, deps=None):
    return JobSpec(name=name, image="x", depends_on=deps or [])


def test_topo_sort_simple_chain():
    stages = topo_sort([J("a"), J("b", ["a"]), J("c", ["b"])])
    assert [j.name for s in stages for j in s] == ["a", "b", "c"]


def test_topo_sort_fan_out():
    stages = topo_sort([J("root"), J("a", ["root"]), J("b", ["root"]), J("c", ["root"])])
    assert stages[0][0].name == "root"
    assert {j.name for j in stages[1]} == {"a", "b", "c"}


def test_topo_sort_cycle():
    with pytest.raises(CycleError):
        topo_sort([J("a", ["b"]), J("b", ["a"])])


def test_topo_sort_unknown_dep():
    with pytest.raises(UnknownDependencyError):
        topo_sort([J("a", ["ghost"])])


def test_expand_matrices_cartesian():
    base = JobSpec(name="bench", image="x", resources=Resources(cpu=2))
    spec = MatrixSpec(base=base, axes=[
        MatrixAxis(name="os", values=["linux", "macos"]),
        MatrixAxis(name="rev", values=["main", "feature"]),
    ])
    jobs = expand_matrices([spec])
    assert len(jobs) == 4
    names = {j.name for j in jobs}
    assert "bench::os=linux,rev=main"     in names
    assert "bench::os=macos,rev=feature"  in names
    # MATRIX_* env vars are injected.
    for j in jobs:
        assert any(k.startswith("MATRIX_") for k in j.env)
