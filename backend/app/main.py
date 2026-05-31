import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.auth import create_token, optional_user, require_admin, require_user
from app.config import get_settings
from app.schemas import (
    ConsentRequest,
    AdminUpdateUserRequest,
    AdminServicesResponse,
    AdminUsersResponse,
    FeedbackRequest,
    FeedbackResponse,
    LoginRequest,
    LoginResponse,
    PredictResponse,
    RegisterRequest,
    UserProfileResponse,
)
from app.services.background_services import get_admin_services_status
from app.services.history import get_historical_snapshot
from app.services.llm import generate_user_explanation
from app.services.predictor import (
    DATA_VERSION,
    DISCLAIMER,
    MODEL_VERSION,
    calculate_probability,
    get_confidence,
    get_horizon_days,
    make_decision,
)
from app.services.weather import OPEN_METEO_MAX_HORIZON_DAYS, fetch_weather_for_date
from app.services.users import (
    authenticate_user,
    delete_admin_user,
    get_user,
    increment_prediction_count,
    init_database,
    list_admin_users,
    log_consent,
    register_user,
    save_feedback,
    save_prediction_event,
    update_admin_user,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

logger = logging.getLogger("flyforecast.api")

settings = get_settings()

app = FastAPI(
    title="Flyforecast API",
    version="0.1.0",
    description="MVP API for flight probability estimation for Mendeleyevo airport, Kunashir.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup() -> None:
    init_database()


@app.get("/health")
def health() -> dict:
    logger.info("health_check status=ok")
    return {"status": "ok"}


@app.post("/auth/login", response_model=LoginResponse)
def login(payload: LoginRequest) -> LoginResponse:
    logger.info("login_attempt email=%s", payload.email)

    user = authenticate_user(payload.email, payload.password)

    logger.info("login_success email=%s", user["email"])
    return LoginResponse(access_token=create_token(user["email"]))


@app.post("/auth/register", response_model=LoginResponse)
def register(payload: RegisterRequest) -> LoginResponse:
    logger.info("register_attempt email=%s", payload.email)

    user = register_user(
        name=payload.name,
        email=payload.email,
        password=payload.password,
        personal_data_consent=payload.personal_data_consent,
        analytics_consent=payload.analytics_consent,
        initial_prediction_count=payload.initial_prediction_count,
    )

    logger.info("register_success email=%s", user["email"])
    return LoginResponse(access_token=create_token(user["email"]))


@app.get("/me", response_model=UserProfileResponse)
def me(user: Annotated[str, Depends(require_user)]) -> UserProfileResponse:
    return UserProfileResponse(**get_user(user))


@app.post("/feedback", response_model=FeedbackResponse)
def feedback(
    payload: FeedbackRequest,
    user: Annotated[str, Depends(require_user)],
) -> FeedbackResponse:
    save_feedback(user, payload.message)
    logger.info("feedback_saved email=%s message_length=%s", user, len(payload.message))
    return FeedbackResponse(status="ok")


@app.post("/consents", response_model=FeedbackResponse)
def consent(payload: ConsentRequest) -> FeedbackResponse:
    log_consent(
        email=None,
        event=payload.event,
        necessary_cookies_ack=payload.necessary_cookies_ack,
        analytics_consent=payload.analytics_consent,
    )
    logger.info("consent_logged event=%s analytics_consent=%s", payload.event, payload.analytics_consent)
    return FeedbackResponse(status="ok")


@app.get("/admin/users", response_model=AdminUsersResponse)
def admin_users(admin: Annotated[str, Depends(require_admin)]) -> AdminUsersResponse:
    logger.info("admin_users_requested admin=%s", admin)
    return AdminUsersResponse(**list_admin_users())


@app.get("/admin/services", response_model=AdminServicesResponse)
def admin_services(admin: Annotated[str, Depends(require_admin)]) -> AdminServicesResponse:
    logger.info("admin_services_requested admin=%s", admin)
    return AdminServicesResponse(**get_admin_services_status())


@app.patch("/admin/users/{email}", response_model=UserProfileResponse)
def admin_update_user(
    email: str,
    payload: AdminUpdateUserRequest,
    admin: Annotated[str, Depends(require_admin)],
) -> UserProfileResponse:
    logger.info("admin_user_update admin=%s email=%s", admin, email)
    changes = {
        key: value
        for key, value in payload.model_dump(exclude_unset=True).items()
        if value is not None
    }
    user = update_admin_user(email, changes)
    return UserProfileResponse(**user)


@app.delete("/admin/users/{email}", response_model=FeedbackResponse)
def admin_delete_user(
    email: str,
    admin: Annotated[str, Depends(require_admin)],
) -> FeedbackResponse:
    logger.info("admin_user_delete admin=%s email=%s", admin, email)
    delete_admin_user(email)
    return FeedbackResponse(status="ok")


@app.get("/predict", response_model=PredictResponse)
async def predict(
    user: Annotated[str | None, Depends(optional_user)],
    target_date: date = Query(alias="date"),
    session_prediction_number: int = Query(default=1, ge=1),
    utm_source: str | None = Query(default=None),
) -> PredictResponse:
    request_id = str(uuid4())[:8]

    logger.info(
        "predict_started request_id=%s username=%s target_date=%s session_prediction_number=%s utm_source=%s",
        request_id,
        user or "anonymous",
        target_date.isoformat(),
        session_prediction_number,
        utm_source,
    )

    today = datetime.now().date()
    max_date = today + timedelta(days=365)

    if target_date < today:
        logger.warning(
            "predict_rejected request_id=%s target_date=%s reason=past_date today=%s",
            request_id,
            target_date.isoformat(),
            today.isoformat(),
        )
        raise HTTPException(status_code=400, detail="Date must be today or in the future")

    if target_date > max_date:
        logger.warning(
            "predict_rejected request_id=%s target_date=%s reason=too_far max_date=%s",
            request_id,
            target_date.isoformat(),
            max_date.isoformat(),
        )
        raise HTTPException(status_code=400, detail="Date must be within 365 days from today")

    horizon_days = get_horizon_days(target_date)

    logger.info(
        "predict_horizon request_id=%s target_date=%s horizon_days=%s",
        request_id,
        target_date.isoformat(),
        horizon_days,
    )

    weather = await fetch_weather_for_date(target_date)

    logger.info(
        (
            "weather_snapshot request_id=%s target_date=%s source=%s available=%s reason=%s "
            "temperature_2m=%s humidity=%s dew_point_2m=%s pressure_msl=%s "
            "cloud_cover=%s cloud_cover_low=%s visibility=%s weather_code=%s "
            "dew_point_spread=%s fog_low_cloud_risk=%s precipitation=%s "
            "wind_speed_10m=%s wind_gusts_10m=%s aggregation_window=%s-%s hours=%s "
            "flight_window_available=%s flight_window=%s-%s flight_window_hours=%s "
            "flight_window_visibility=%s flight_window_fog_risk=%s"
        ),
        request_id,
        target_date.isoformat(),
        weather.source,
        weather.available,
        weather.reason,
        weather.temperature_2m,
        weather.relative_humidity_2m,
        weather.dew_point_2m,
        weather.pressure_msl,
        weather.cloud_cover,
        weather.cloud_cover_low,
        weather.visibility,
        weather.weather_code,
        weather.dew_point_spread,
        weather.fog_low_cloud_risk_level,
        weather.precipitation,
        weather.wind_speed_10m,
        weather.wind_gusts_10m,
        weather.aggregation_window_start_hour,
        weather.aggregation_window_end_hour,
        weather.aggregation_window_hours,
        weather.flight_window_available,
        weather.flight_window_start_hour,
        weather.flight_window_end_hour,
        weather.flight_window_hours,
        weather.flight_window_visibility,
        weather.flight_window_fog_low_cloud_risk_level,
    )

    if horizon_days <= OPEN_METEO_MAX_HORIZON_DAYS and not weather.available:
        logger.warning(
            "predict_blocked_weather_unavailable request_id=%s target_date=%s reason=%s",
            request_id,
            target_date.isoformat(),
            weather.reason,
        )
        raise HTTPException(
            status_code=503,
            detail="Временные проблемы с погодными сервисами, предсказания пока недоступны.",
        )

    history = get_historical_snapshot(target_date)

    logger.info(
        (
            "history_snapshot request_id=%s target_date=%s source=%s similar_days_count=%s "
            "completed_count=%s cancelled_count=%s historical_probability_flight=%s "
            "month_probability_flight=%s decade_probability_flight=%s"
        ),
        request_id,
        target_date.isoformat(),
        history.source,
        history.similar_days_count,
        history.completed_count,
        history.cancelled_count,
        history.historical_probability_flight,
        history.month_probability_flight,
        history.decade_probability_flight,
    )

    probability_flight = calculate_probability(
        horizon_days=horizon_days,
        weather=weather,
        history=history,
    )

    confidence = get_confidence(
        horizon_days=horizon_days,
        weather=weather,
        history=history,
    )

    decision = make_decision(
        probability_flight=probability_flight,
        horizon_days=horizon_days,
    )

    logger.info(
        (
            "prediction_calculated request_id=%s target_date=%s probability_flight=%s "
            "decision=%s confidence=%s model_version=%s data_version=%s"
        ),
        request_id,
        target_date.isoformat(),
        probability_flight,
        decision,
        confidence,
        MODEL_VERSION,
        DATA_VERSION,
    )

    explanation = generate_user_explanation(
        target_date=target_date.isoformat(),
        decision=decision,
        probability_flight=probability_flight,
        confidence=confidence,
        horizon_days=horizon_days,
        weather=weather,
        history=history,
    )

    forecast_mode = "weather_model" if horizon_days <= OPEN_METEO_MAX_HORIZON_DAYS and weather.available else "climate_history"
    forecast_mode_label = (
        "Прогноз с учетом погодной модели"
        if forecast_mode == "weather_model"
        else "Климатико-историческая оценка риска"
    )

    logger.info(
        "explanation_generated request_id=%s target_date=%s explanation_length=%s",
        request_id,
        target_date.isoformat(),
        len(explanation),
    )

    result = PredictResponse(
        date=target_date.isoformat(),
        decision=decision,
        probability_flight=probability_flight,
        confidence=confidence,
        horizon_days=horizon_days,
        forecast_mode=forecast_mode,
        forecast_mode_label=forecast_mode_label,
        explanation=explanation,
        weather=weather,
        history=history,
        model_version=MODEL_VERSION,
        data_version=DATA_VERSION,
        disclaimer=DISCLAIMER,
    )

    is_admin_user = bool(user) and user.strip().lower() == settings.admin_email.strip().lower()
    session_prediction_number = (
        session_prediction_number
        if not user or is_admin_user
        else increment_prediction_count(user)
    )

    log_prediction(
        result=result,
        username=user or "anonymous",
        session_prediction_number=session_prediction_number,
        utm_source=utm_source,
        request_id=request_id,
    )

    if user and not is_admin_user:
        save_prediction_event(
            email=user,
            request_id=request_id,
            target_date=result.date,
            horizon_days=result.horizon_days,
            probability_flight=result.probability_flight,
            decision=result.decision,
            confidence=result.confidence,
            model_version=result.model_version,
            data_version=result.data_version,
            session_prediction_number=session_prediction_number,
            utm_source=utm_source,
        )

    logger.info(
        "predict_finished request_id=%s target_date=%s status=success",
        request_id,
        target_date.isoformat(),
    )

    return result


def log_prediction(
    result: PredictResponse,
    username: str,
    session_prediction_number: int,
    utm_source: str | None,
    request_id: str,
) -> None:
    path = Path(settings.prediction_log_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "request_datetime": datetime.now().isoformat(),
        "request_id": request_id,
        "username": username,
        "target_date": result.date,
        "horizon_days": result.horizon_days,
        "probability_flight": result.probability_flight,
        "decision": result.decision,
        "confidence": result.confidence,
        "model_version": result.model_version,
        "data_version": result.data_version,
        "session_prediction_number": session_prediction_number,
        "cta_shown": session_prediction_number >= 5,
        "utm_source": utm_source,
        "weather_available": result.weather.available,
        "weather_source": result.weather.source,
        "weather_reason": result.weather.reason,
        "forecast_mode": result.forecast_mode,
        "forecast_mode_label": result.forecast_mode_label,
        "visibility": result.weather.visibility,
        "cloud_cover_low": result.weather.cloud_cover_low,
        "weather_code": result.weather.weather_code,
        "dew_point_spread": result.weather.dew_point_spread,
        "fog_low_cloud_risk_score": result.weather.fog_low_cloud_risk_score,
        "fog_low_cloud_risk_level": result.weather.fog_low_cloud_risk_level,
        "aggregation_window_start_hour": result.weather.aggregation_window_start_hour,
        "aggregation_window_end_hour": result.weather.aggregation_window_end_hour,
        "aggregation_window_hours": result.weather.aggregation_window_hours,
        "flight_window_available": result.weather.flight_window_available,
        "flight_window_start_hour": result.weather.flight_window_start_hour,
        "flight_window_end_hour": result.weather.flight_window_end_hour,
        "flight_window_hours": result.weather.flight_window_hours,
        "flight_window_visibility": result.weather.flight_window_visibility,
        "flight_window_cloud_cover_low": result.weather.flight_window_cloud_cover_low,
        "flight_window_fog_low_cloud_risk_score": result.weather.flight_window_fog_low_cloud_risk_score,
        "flight_window_fog_low_cloud_risk_level": result.weather.flight_window_fog_low_cloud_risk_level,
        "wind_speed_10m": result.weather.wind_speed_10m,
        "wind_gusts_10m": result.weather.wind_gusts_10m,
        "relative_humidity_2m": result.weather.relative_humidity_2m,
        "cloud_cover": result.weather.cloud_cover,
        "history_source": result.history.source,
        "similar_days_count": result.history.similar_days_count,
        "completed_count": result.history.completed_count,
        "cancelled_count": result.history.cancelled_count,
        "historical_probability_flight": result.history.historical_probability_flight,
    }

    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(row, ensure_ascii=False) + "\n")

    logger.info(
        "prediction_logged request_id=%s path=%s target_date=%s",
        request_id,
        str(path),
        result.date,
    )
