"""`ci` CLI — submit a pipeline.yaml to the orchestrator from a workstation."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx
import typer
import yaml
from rich.console import Console
from rich.table import Table

app = typer.Typer(no_args_is_help=True, add_completion=False)
console = Console()


@app.command()
def submit(file: Path, endpoint: str = "http://orchestrator.ci-runner.svc:8080") -> None:
    """Submit a pipeline.yaml."""
    raw = yaml.safe_load(file.read_text())
    r = httpx.post(f"{endpoint}/v1/pipelines", json=raw, timeout=30.0)
    r.raise_for_status()
    rec = r.json()
    console.print(f"[green]submitted[/] pipeline {rec['id']} with {len(rec['jobs'])} jobs")


@app.command()
def status(pipeline_id: str, endpoint: str = "http://orchestrator.ci-runner.svc:8080") -> None:
    r = httpx.get(f"{endpoint}/v1/pipelines/{pipeline_id}", timeout=10.0)
    r.raise_for_status()
    rec = r.json()
    t = Table(title=f"pipeline {rec['name']} ({rec['phase']})")
    t.add_column("Job"); t.add_column("Phase"); t.add_column("Exit"); t.add_column("Pod")
    for j in rec["jobs"]:
        t.add_row(j["name"], j["phase"], str(j.get("exit_code") or "-"), j.get("pod") or "-")
    console.print(t)


@app.command()
def watch(label: str = "", endpoint: str = "http://orchestrator.ci-runner.svc:8080") -> None:
    with httpx.stream("GET", f"{endpoint}/v1/events", params={"label": label}, timeout=None) as r:
        for line in r.iter_lines():
            if line:
                console.print(line)


if __name__ == "__main__":
    app()
