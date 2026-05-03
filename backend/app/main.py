import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.auth import create_token, require_user
from app.config import get_settings
from app.schemas import LoginRequest, LoginResponse, PredictResponse
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
from app.services.weather import fetch_weather_for_date


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


@app.get("/health")
def health() -> dict:
    logger.info("health_check status=ok")
    return {"status": "ok"}


@app.post("/auth/login", response_model=LoginResponse)
def login(payload: LoginRequest) -> LoginResponse:
    logger.info("login_attempt username=%s", payload.username)

    if payload.username != settings.test_username or payload.password != settings.test_password:
        logger.warning("login_failed username=%s reason=invalid_credentials", payload.username)
        raise HTTPException(status_code=401, detail="Invalid username or password")

    logger.info("login_success username=%s", payload.username)
    return LoginResponse(access_token=create_token(payload.username))


@app.get("/predict", response_model=PredictResponse)
async def predict(
    user: Annotated[str, Depends(require_user)],
    target_date: date = Query(alias="date"),
    session_prediction_number: int = Query(default=1, ge=1),
    utm_source: str | None = Query(default=None),
) -> PredictResponse:
    request_id = str(uuid4())[:8]

    logger.info(
        "predict_started request_id=%s username=%s target_date=%s session_prediction_number=%s utm_source=%s",
        request_id,
        user,
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
            "cloud_cover=%s precipitation=%s wind_speed_10m=%s wind_gusts_10m=%s"
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
        weather.precipitation,
        weather.wind_speed_10m,
        weather.wind_gusts_10m,
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
        explanation=explanation,
        weather=weather,
        history=history,
        model_version=MODEL_VERSION,
        data_version=DATA_VERSION,
        disclaimer=DISCLAIMER,
    )

    log_prediction(
        result=result,
        username=user,
        session_prediction_number=session_prediction_number,
        utm_source=utm_source,
        request_id=request_id,
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
        "cta_shown": session_prediction_number >= 2,
        "utm_source": utm_source,
        "weather_available": result.weather.available,
        "weather_source": result.weather.source,
        "weather_reason": result.weather.reason,
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