import re

from pydantic import BaseModel, Field, field_validator


EMAIL_PATTERN = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)


def validate_email_address(value: str) -> str:
    email = value.strip().lower()
    if not EMAIL_PATTERN.fullmatch(email):
        raise ValueError("Enter a valid email address")
    return email


class LoginRequest(BaseModel):
    email: str
    password: str

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        return validate_email_address(value)


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class RegisterRequest(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    email: str = Field(min_length=5, max_length=254)
    password: str = Field(min_length=8, max_length=128)
    personal_data_consent: bool
    analytics_consent: bool = False
    initial_prediction_count: int = Field(default=0, ge=0, le=5)

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        return validate_email_address(value)


class UserProfileResponse(BaseModel):
    name: str
    email: str
    prediction_count: int
    feedback_count: int
    registered_at: str
    personal_data_consent: bool
    analytics_consent: bool


class FeedbackRequest(BaseModel):
    message: str = Field(min_length=3, max_length=500)


class FeedbackResponse(BaseModel):
    status: str


class ConsentRequest(BaseModel):
    event: str = Field(pattern="^(necessary_cookies_ack|analytics_consent)$")
    necessary_cookies_ack: bool = False
    analytics_consent: bool = False


class AdminFeedbackResponse(BaseModel):
    id: int
    message: str
    created_at: str


class AdminUserResponse(BaseModel):
    name: str
    email: str
    prediction_count: int
    feedback_count: int
    registered_at: str
    updated_at: str
    personal_data_consent: bool
    analytics_consent: bool
    last_prediction_at: str | None = None
    last_feedback_at: str | None = None
    feedbacks: list[AdminFeedbackResponse] = []


class AdminUsersResponse(BaseModel):
    total_users: int
    total_predictions: int
    total_feedback: int
    analytics_consents: int
    users: list[AdminUserResponse]


class BackgroundServiceHealth(BaseModel):
    name: str
    status: str
    last_seen_at: str | None = None
    message: str


class BoardCollectorStatus(BaseModel):
    health: BackgroundServiceHealth
    dataset_path: str
    errors_path: str
    total_rows: int
    rows_last_observation: int
    latest_observed_at: str | None = None
    latest_observation_date: str | None = None
    latest_statuses: dict[str, int]
    recent_errors: list[dict[str, str]]


class ForecastMonitorRun(BaseModel):
    id: int | None = None
    run_date: str
    created_at: str
    status: str
    predictions_count: int
    expected_predictions: int
    error: str | None = None


class ForecastMonitorPrediction(BaseModel):
    target_date: str
    horizon_days: int
    probability_flight: float
    decision: str
    confidence: str
    created_at: str
    outcome_status: str | None = None
    evaluated: bool = False
    hit: bool | None = None


class ForecastMonitorStatus(BaseModel):
    health: BackgroundServiceHealth
    db_path: str
    total_runs: int
    total_predictions: int
    total_evaluations: int
    total_hits: int = 0
    total_misses: int = 0
    total_pending: int = 0
    accuracy: float | None = None
    recalculated_model_version: str | None = None
    recalculated_total_evaluations: int = 0
    recalculated_total_hits: int = 0
    recalculated_total_misses: int = 0
    recalculated_predicted_yes: int = 0
    recalculated_predicted_no: int = 0
    recalculated_observed_completed: int = 0
    recalculated_observed_cancelled: int = 0
    recalculated_false_yes: int = 0
    recalculated_false_no: int = 0
    recalculated_accuracy: float | None = None
    recalculated_brier_score: float | None = None
    recalculated_mean_absolute_error: float | None = None
    recalculated_metrics_available: bool = False
    recalculated_metrics_reason: str | None = None
    latest_run: ForecastMonitorRun | None = None
    recent_runs: list[ForecastMonitorRun]
    recent_predictions: list[ForecastMonitorPrediction]


class AdminServicesResponse(BaseModel):
    board_collector: BoardCollectorStatus
    forecast_monitor: ForecastMonitorStatus


class AdminUpdateUserRequest(BaseModel):
    name: str | None = Field(default=None, min_length=2, max_length=120)
    email: str | None = Field(default=None, min_length=5, max_length=254)
    password: str | None = Field(default=None, min_length=8, max_length=128)
    prediction_count: int | None = Field(default=None, ge=0)
    feedback_count: int | None = Field(default=None, ge=0)
    personal_data_consent: bool | None = None
    analytics_consent: bool | None = None

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str | None) -> str | None:
        return validate_email_address(value) if value is not None else None


class WeatherSnapshot(BaseModel):
    source: str
    available: bool
    reason: str | None = None
    temperature_2m: float | None = None
    relative_humidity_2m: float | None = None
    dew_point_2m: float | None = None
    dew_point_spread: float | None = None
    pressure_msl: float | None = None
    cloud_cover: float | None = None
    cloud_cover_low: float | None = None
    precipitation: float | None = None
    wind_speed_10m: float | None = None
    wind_gusts_10m: float | None = None
    wind_direction_10m: float | None = None
    weather_code: float | None = None
    visibility: float | None = None
    fog_low_cloud_risk_score: float | None = None
    fog_low_cloud_risk_level: str | None = None
    aggregation_window_start_hour: int | None = None
    aggregation_window_end_hour: int | None = None
    aggregation_window_hours: int | None = None
    flight_window_available: bool | None = None
    flight_window_start_hour: int | None = None
    flight_window_end_hour: int | None = None
    flight_window_hours: int | None = None
    flight_window_visibility: float | None = None
    flight_window_cloud_cover_low: float | None = None
    flight_window_fog_low_cloud_risk_score: float | None = None
    flight_window_fog_low_cloud_risk_level: str | None = None


class FlightScheduleFlight(BaseModel):
    direction: str | None = None
    flight_time: str | None = None
    flight_numbers: str | None = None
    status: str | None = None
    actual_date: str | None = None
    hour: int | None = None
    state: str | None = None


class FlightScheduleSnapshot(BaseModel):
    source: str
    available: bool
    reason: str | None = None
    observed_at: str | None = None
    flight_numbers: str | None = None
    first_departure_hour: int | None = None
    first_scheduled_hour: int | None = None
    last_scheduled_hour: int | None = None
    schedule_window_start_hour: int | None = None
    schedule_window_end_hour: int | None = None
    moved_next_day: bool = False
    completed_same_day: bool = False
    status_summary: str | None = None
    total_flights: int = 0
    completed_flights: int = 0
    unavailable_flights: int = 0
    pending_flights: int = 0
    active_flight_index: int | None = None
    active_flight_hour: int | None = None
    active_flight_time: str | None = None
    active_flight_numbers: str | None = None
    active_flight_status: str | None = None
    flights: list[FlightScheduleFlight] = Field(default_factory=list)


class HistoricalSnapshot(BaseModel):
    source: str
    similar_days_count: int
    completed_count: int
    cancelled_count: int
    historical_probability_flight: float
    month_probability_flight: float | None = None
    decade_probability_flight: float | None = None


class PredictResponse(BaseModel):
    date: str
    decision: str = Field(pattern="^(yes|no)$")
    probability_flight: float = Field(ge=0.0, le=1.0)
    confidence: str
    horizon_days: int
    forecast_mode: str = Field(pattern="^(weather_model|climate_history)$")
    forecast_mode_label: str
    explanation: str
    weather: WeatherSnapshot
    schedule: FlightScheduleSnapshot | None = None
    history: HistoricalSnapshot
    model_version: str
    data_version: str
    disclaimer: str
