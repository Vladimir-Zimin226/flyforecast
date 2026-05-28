import asyncio
import logging
from datetime import date, datetime

import httpx

from app.config import get_settings
from app.schemas import WeatherSnapshot
from app.services.fog_risk import (
    calculate_dew_point_spread,
    calculate_fog_low_cloud_risk_score,
    fog_low_cloud_risk_level,
)


logger = logging.getLogger("flyforecast.weather")

HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "pressure_msl",
    "cloud_cover",
    "cloud_cover_low",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
    "visibility",
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_MAX_HORIZON_DAYS = 15
OPEN_METEO_RETRIES = 2
OPEN_METEO_RETRY_DELAY_SECONDS = 0.8


def _unavailable_weather(reason: str) -> WeatherSnapshot:
    return WeatherSnapshot(
        source="open-meteo",
        available=False,
        reason=reason,
    )


async def fetch_weather_for_date(target_date: date) -> WeatherSnapshot:
    """
    Open-Meteo forecast обычно полезен только на ближнем горизонте.
    Для дальних дат возвращаем available=false, а prediction уходит в seasonal baseline.
    """
    settings = get_settings()
    today = datetime.now().date()
    horizon_days = (target_date - today).days

    if horizon_days < 0:
        return _unavailable_weather("Past dates are not supported by /predict in MVP.")

    if horizon_days > OPEN_METEO_MAX_HORIZON_DAYS:
        return _unavailable_weather("Open-Meteo forecast is not available for this long horizon in MVP.")

    params = {
        "latitude": settings.airport_latitude,
        "longitude": settings.airport_longitude,
        "timezone": settings.airport_timezone,
        "forecast_days": min(max(horizon_days + 1, 1), 16),
        "hourly": ",".join(HOURLY_FIELDS),
    }

    payload = None

    async with httpx.AsyncClient(timeout=10) as client:
        for attempt in range(OPEN_METEO_RETRIES + 1):
            try:
                response = await client.get(OPEN_METEO_URL, params=params)
                response.raise_for_status()
                payload = response.json()
                break
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                logger.warning(
                    "open_meteo_status_error target_date=%s attempt=%s status_code=%s",
                    target_date.isoformat(),
                    attempt + 1,
                    status_code,
                )
                if status_code < 500 or attempt == OPEN_METEO_RETRIES:
                    return _unavailable_weather(f"Open-Meteo returned HTTP {status_code}.")
            except (httpx.RequestError, ValueError) as exc:
                logger.warning(
                    "open_meteo_request_error target_date=%s attempt=%s error=%s",
                    target_date.isoformat(),
                    attempt + 1,
                    exc,
                )
                if attempt == OPEN_METEO_RETRIES:
                    return _unavailable_weather("Open-Meteo is temporarily unavailable.")

            await asyncio.sleep(OPEN_METEO_RETRY_DELAY_SECONDS * (attempt + 1))

    if payload is None:
        return _unavailable_weather("Open-Meteo is temporarily unavailable.")

    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])

    indices = [
        i for i, value in enumerate(times)
        if value.startswith(target_date.isoformat())
    ]

    if not indices:
        return _unavailable_weather("No hourly weather rows returned for target date.")

    # MVP: берём дневное среднее по доступным hourly values.
    aggregated: dict[str, float | None] = {}

    for field in HOURLY_FIELDS:
        values = hourly.get(field, [])
        day_values = [
            values[i] for i in indices
            if i < len(values) and values[i] is not None
        ]
        if not day_values:
            aggregated[field] = None
        elif field == "visibility":
            aggregated[field] = round(min(day_values), 2)
        elif field == "weather_code":
            aggregated[field] = float(max(set(day_values), key=day_values.count))
        else:
            aggregated[field] = round(sum(day_values) / len(day_values), 2)

    dew_point_spread = calculate_dew_point_spread(
        aggregated["temperature_2m"],
        aggregated["dew_point_2m"],
    )
    fog_low_cloud_risk_score = calculate_fog_low_cloud_risk_score(
        visibility=aggregated["visibility"],
        cloud_cover_low=aggregated["cloud_cover_low"],
        relative_humidity_2m=aggregated["relative_humidity_2m"],
        dew_point_spread=dew_point_spread,
        wind_speed_10m=aggregated["wind_speed_10m"],
        wind_gusts_10m=aggregated["wind_gusts_10m"],
        precipitation=aggregated["precipitation"],
        weather_code=aggregated["weather_code"],
    )

    return WeatherSnapshot(
        source="open-meteo",
        available=True,
        temperature_2m=aggregated["temperature_2m"],
        relative_humidity_2m=aggregated["relative_humidity_2m"],
        dew_point_2m=aggregated["dew_point_2m"],
        dew_point_spread=dew_point_spread,
        pressure_msl=aggregated["pressure_msl"],
        cloud_cover=aggregated["cloud_cover"],
        cloud_cover_low=aggregated["cloud_cover_low"],
        precipitation=aggregated["precipitation"],
        wind_speed_10m=aggregated["wind_speed_10m"],
        wind_gusts_10m=aggregated["wind_gusts_10m"],
        weather_code=aggregated["weather_code"],
        visibility=aggregated["visibility"],
        fog_low_cloud_risk_score=fog_low_cloud_risk_score,
        fog_low_cloud_risk_level=fog_low_cloud_risk_level(fog_low_cloud_risk_score),
    )
