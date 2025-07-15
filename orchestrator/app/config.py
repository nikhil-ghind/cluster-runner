from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    dispatcher_endpoint: str = "dispatcher.ci-runner.svc:7070"
    postgres_url: str = "postgresql+asyncpg://ci:ci@postgres:5432/ci"
    listen_host: str = "0.0.0.0"
    listen_port: int = 8080
    metrics_port: int = 9100
    artifact_bucket: str = "s3://ci-artifacts"
    default_image: str = "ghcr.io/nikhil-ghind/ci-base:latest"
    log_level: str = "INFO"
    max_matrix_jobs: int = 4096
    enable_spot: bool = True
    default_seccomp: str = "runtime/default"

    class Config:
        env_prefix = "CI_"


settings = Settings()
