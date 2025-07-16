#!/usr/bin/env bash
# Submit the example pipeline. Use after kubectl port-forward of orchestrator:8080.
set -euo pipefail
ENDPOINT="${ENDPOINT:-http://127.0.0.1:8080}"
curl -fsSL -X POST "$ENDPOINT/v1/pipelines" \
  -H 'Content-Type: application/json' \
  -d @"$(dirname "$0")/../examples/pipeline.json"
