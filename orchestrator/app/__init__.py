"""HPC CI orchestrator — Python control plane.

The orchestrator is the high-level surface that humans + CI integrations
talk to. It owns the *pipeline graph* (jobs, dependencies, matrix
expansion), persists pipeline state in Postgres, and forwards individual
job specs to the Rust dispatcher via gRPC for actual scheduling.
"""
