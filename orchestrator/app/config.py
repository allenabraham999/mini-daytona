from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"

    orchestrator_host: str = "0.0.0.0"
    orchestrator_port: int = 9000

    pool_min_size: int = 2
    pool_max_size: int = 10

    pool_scale_up_threshold: float = 0.3
    pool_scale_down_threshold: float = 0.7

    idle_timeout_seconds: int = 600
    health_check_interval_seconds: int = 30
    pool_scaler_interval_seconds: int = 30

    sandbox_backend: str = "mock"
    incus_project: str = "default"


settings = Settings()
