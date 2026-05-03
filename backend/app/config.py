from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "local"
    backend_cors_origins: str = "http://localhost:5173,http://localhost:8080"

    test_username: str = "demo"
    test_password: str = "demouser123"
    jwt_secret: str = "ferrum-molibden"

    openai_api_key: str | None = None
    openai_model: str = "gpt-5.4-mini"

    flyforecast_dataset_path: str = "/app/data/processed/dataset_daily_flights.csv"
    prediction_log_path: str = "/app/data/interim/prediction_logs.jsonl"

    airport_latitude: float = 43.958
    airport_longitude: float = 145.683
    airport_timezone: str = "Asia/Sakhalin"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.backend_cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()