from datetime import date, datetime

import httpx

from app.config import get_settings
from app.schemas import WeatherSnapshot


HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "pressure_msl",
    "cloud_cover",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
]


async def fetch_weather_for_date(target_date: date) -> WeatherSnapshot:
    """
    Open-Meteo forecast обычно полезен только на ближнем горизонте.
    Для дальних дат возвращаем available=false, а prediction уходит в seasonal baseline.
    """
    settings = get_settings()
    today = datetime.now().date()
    horizon_days = (target_date - today).days

    if horizon_days < 0:
        return WeatherSnapshot(
            source="open-meteo",
            available=False,
            reason="Past dates are not supported by /predict in MVP.",
        )

    if horizon_days > 16:
        return WeatherSnapshot(
            source="open-meteo",
            available=False,
            reason="Open-Meteo forecast is not available for this long horizon in MVP.",
        )

    params = {
        "latitude": settings.airport_latitude,
        "longitude": settings.airport_longitude,
        "timezone": settings.airport_timezone,
        "forecast_days": min(max(horizon_days + 1, 1), 16),
        "hourly": ",".join(HOURLY_FIELDS),
    }

    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
        response.raise_for_status()
        payload = response.json()

    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])

    indices = [
        i for i, value in enumerate(times)
        if value.startswith(target_date.isoformat())
    ]

    if not indices:
        return WeatherSnapshot(
            source="open-meteo",
            available=False,
            reason="No hourly weather rows returned for target date.",
        )

    # MVP: берём дневное среднее по доступным hourly values.
    aggregated: dict[str, float | None] = {}

    for field in HOURLY_FIELDS:
        values = hourly.get(field, [])
        day_values = [
            values[i] for i in indices
            if i < len(values) and values[i] is not None
        ]
        aggregated[field] = round(sum(day_values) / len(day_values), 2) if day_values else None

    return WeatherSnapshot(
        source="open-meteo",
        available=True,
        temperature_2m=aggregated["temperature_2m"],
        relative_humidity_2m=aggregated["relative_humidity_2m"],
        dew_point_2m=aggregated["dew_point_2m"],
        pressure_msl=aggregated["pressure_msl"],
        cloud_cover=aggregated["cloud_cover"],
        precipitation=aggregated["precipitation"],
        wind_speed_10m=aggregated["wind_speed_10m"],
        wind_gusts_10m=aggregated["wind_gusts_10m"],
    )