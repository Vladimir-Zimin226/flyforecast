from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "local"
    backend_cors_origins: str = "http://localhost:5173,http://localhost:8080"

    jwt_secret: str = "ferrum-molibden"
    admin_email: str = "admin@example.com"
    admin_password: str = "change-me-admin-password"

    flyforecast_dataset_path: str = "/app/data/processed/dataset_daily_flights.csv"
    prediction_log_path: str = "/app/data/interim/prediction_logs.jsonl"
    database_url: str = "postgresql://flyforecast:flyforecast@db:5432/flyforecast"
    user_store_path: str = "/app/data/interim/users.json"
    feedback_log_path: str = "/app/data/interim/feedback_logs.jsonl"
    consent_log_path: str = "/app/data/interim/consent_logs.jsonl"
    forecast_monitor_db_path: str = "/app/data/interim/evaluation/forecast_monitor.sqlite"
    weather_forecast_cache_path: str = "/app/data/interim/weather_forecast_cache.sqlite"
    weather_cache_fresh_hours: int = 1
    weather_live_cache_fresh_minutes: int = 15
    weather_cache_stale_hours: int = 72
    open_meteo_url: str = "https://api.open-meteo.com/v1/forecast"
    open_meteo_failure_cooldown_minutes: int = 30
    weather_forecast_window_start_hour: int = 8
    weather_forecast_window_end_hour: int = 20
    weather_flight_window_min_hours: int = 3
    weather_scheduled_window_duration_hours: float = 1.5
    weather_flight_window_min_visibility: float = 5000.0
    weather_flight_window_max_cloud_low: float = 80.0
    weather_flight_window_max_wind_gusts: float = 65.0
    weather_flight_window_max_precipitation: float = 2.5
    met_no_fallback_enabled: bool = True
    met_no_user_agent: str = "flyforecast.ru/0.1(+https://flyforecast.ru;admin@example.com)"
    flight_status_dataset_path: str = "/app/data/raw/flight_status/kunashir_flight_status_hourly.csv"
    flight_status_errors_path: str = "/app/data/raw/flight_status/collection_errors.csv"
    sakhalin_airports_board_output: str = "/app/data/raw/sakhalin_airports/sakhalin_airport_board_hourly.csv"
    sakhalin_airports_weather_output: str = "/app/data/raw/sakhalin_airports/sakhalin_airport_weather_hourly.csv"
    sakhalin_airports_errors_output: str = "/app/data/raw/sakhalin_airports/collection_errors.csv"
    background_service_stale_hours: int = 26
    historical_ml_enabled: bool = True
    historical_ml_model_path: str = "/app/app/model_artifacts/historical_model_v1.joblib"
    historical_ml_metadata_path: str = "/app/app/model_artifacts/historical_model_v1.json"

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
