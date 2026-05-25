from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    email: str
    password: str


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


class UserProfileResponse(BaseModel):
    name: str
    email: str
    prediction_count: int
    feedback_count: int
    registered_at: str
    personal_data_consent: bool
    analytics_consent: bool


class FeedbackRequest(BaseModel):
    message: str = Field(min_length=3, max_length=2000)


class FeedbackResponse(BaseModel):
    status: str


class ConsentRequest(BaseModel):
    event: str = Field(pattern="^(necessary_cookies_ack|analytics_consent)$")
    necessary_cookies_ack: bool = False
    analytics_consent: bool = False


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
