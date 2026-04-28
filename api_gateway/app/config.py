from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "INFO"

    gateway_host: str = "0.0.0.0"
    gateway_port: int = 8000

    jwt_secret: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"
    jwt_audience: str = ""

    rate_limit_per_second: float = 10.0
    rate_limit_burst: int = 10

    orchestrator_url: str = "http://orchestrator:9000"
    orchestrator_timeout_seconds: float = 10.0


settings = Settings()
