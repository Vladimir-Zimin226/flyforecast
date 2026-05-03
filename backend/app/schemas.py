from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class WeatherSnapshot(BaseModel):
    source: str
    available: bool
    reason: str | None = None
    temperature_2m: float | None = None
    relative_humidity_2m: float | None = None
    dew_point_2m: float | None = None
    pressure_msl: float | None = None
    cloud_cover: float | None = None
    precipitation: float | None = None
    wind_speed_10m: float | None = None
    wind_gusts_10m: float | None = None


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
    explanation: str
    weather: WeatherSnapshot
    history: HistoricalSnapshot
    model_version: str
    data_version: str
    disclaimer: str