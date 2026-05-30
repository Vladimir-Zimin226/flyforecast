import asyncio
import json
import logging
import sqlite3
from contextlib import closing
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from app.config import Settings, get_settings
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
OPEN_METEO_CACHE_FORECAST_DAYS = OPEN_METEO_MAX_HORIZON_DAYS + 1

MET_NO_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
MET_NO_MAX_HORIZON_DAYS = 9


def _unavailable_weather(reason: str) -> WeatherSnapshot:
    return WeatherSnapshot(
        source="weather-unavailable",
        available=False,
        reason=reason,
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _mean(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 2) if values else None


def _minimum(values: list[float]) -> float | None:
    return round(min(values), 2) if values else None


def _mode(values: list[float]) -> float | None:
    return float(max(set(values), key=values.count)) if values else None


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_weather_snapshot(
    *,
    source: str,
    reason: str | None,
    aggregated: dict[str, float | None],
    extra_fog_risk_score: float = 0.0,
) -> WeatherSnapshot:
    dew_point_spread = calculate_dew_point_spread(
        aggregated.get("temperature_2m"),
        aggregated.get("dew_point_2m"),
    )
    fog_low_cloud_risk_score = calculate_fog_low_cloud_risk_score(
        visibility=aggregated.get("visibility"),
        cloud_cover_low=aggregated.get("cloud_cover_low"),
        relative_humidity_2m=aggregated.get("relative_humidity_2m"),
        dew_point_spread=dew_point_spread,
        wind_speed_10m=aggregated.get("wind_speed_10m"),
        wind_gusts_10m=aggregated.get("wind_gusts_10m"),
        precipitation=aggregated.get("precipitation"),
        weather_code=aggregated.get("weather_code"),
    )
    fog_low_cloud_risk_score = round(min(max(fog_low_cloud_risk_score + extra_fog_risk_score, 0.0), 1.0), 3)

    return WeatherSnapshot(
        source=source,
        available=True,
        reason=reason,
        temperature_2m=aggregated.get("temperature_2m"),
        relative_humidity_2m=aggregated.get("relative_humidity_2m"),
        dew_point_2m=aggregated.get("dew_point_2m"),
        dew_point_spread=dew_point_spread,
        pressure_msl=aggregated.get("pressure_msl"),
        cloud_cover=aggregated.get("cloud_cover"),
        cloud_cover_low=aggregated.get("cloud_cover_low"),
        precipitation=aggregated.get("precipitation"),
        wind_speed_10m=aggregated.get("wind_speed_10m"),
        wind_gusts_10m=aggregated.get("wind_gusts_10m"),
        weather_code=aggregated.get("weather_code"),
        visibility=aggregated.get("visibility"),
        fog_low_cloud_risk_score=fog_low_cloud_risk_score,
        fog_low_cloud_risk_level=fog_low_cloud_risk_level(fog_low_cloud_risk_score),
    )


def _connect_weather_cache(path: str) -> sqlite3.Connection:
    cache_path = Path(path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cache_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_forecast_cache (
            target_date TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def _load_open_meteo_cache(
    settings: Settings,
    target_date: date,
) -> tuple[WeatherSnapshot, float, datetime] | None:
    try:
        with closing(_connect_weather_cache(settings.weather_forecast_cache_path)) as conn:
            row = conn.execute(
                """
                SELECT payload_json, fetched_at
                FROM weather_forecast_cache
                WHERE target_date = ? AND provider = ?
                """,
                (target_date.isoformat(), "open-meteo"),
            ).fetchone()
    except sqlite3.Error as exc:
        logger.warning("weather_cache_read_failed target_date=%s error=%s", target_date.isoformat(), exc)
        return None

    if not row:
        return None

    payload_json, fetched_at_raw = row
    try:
        payload = json.loads(payload_json)
        fetched_at = datetime.fromisoformat(fetched_at_raw)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("weather_cache_decode_failed target_date=%s error=%s", target_date.isoformat(), exc)
        return None

    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)

    age_hours = (_utc_now() - fetched_at).total_seconds() / 3600
    snapshot = WeatherSnapshot(**payload)
    return snapshot, age_hours, fetched_at


def _cache_snapshot_with_source(
    snapshot: WeatherSnapshot,
    *,
    source: str,
    reason: str | None,
) -> WeatherSnapshot:
    payload = snapshot.model_dump()
    payload["source"] = source
    payload["reason"] = reason
    return WeatherSnapshot(**payload)


def _store_open_meteo_cache(settings: Settings, snapshots: dict[date, WeatherSnapshot]) -> None:
    if not snapshots:
        return

    fetched_at = _utc_now().isoformat()
    rows = [
        (
            target_date.isoformat(),
            "open-meteo",
            json.dumps(snapshot.model_dump(), ensure_ascii=False),
            fetched_at,
        )
        for target_date, snapshot in snapshots.items()
    ]

    try:
        with closing(_connect_weather_cache(settings.weather_forecast_cache_path)) as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO weather_forecast_cache (
                    target_date,
                    provider,
                    payload_json,
                    fetched_at
                )
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
    except sqlite3.Error as exc:
        logger.warning("weather_cache_write_failed rows=%s error=%s", len(rows), exc)


def _open_meteo_payload_to_snapshots(payload: dict) -> dict[date, WeatherSnapshot]:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    date_to_indices: dict[date, list[int]] = {}

    for index, value in enumerate(times):
        if not isinstance(value, str):
            continue
        try:
            day = date.fromisoformat(value[:10])
        except ValueError:
            continue
        date_to_indices.setdefault(day, []).append(index)

    snapshots: dict[date, WeatherSnapshot] = {}
    for target_date, indices in date_to_indices.items():
        aggregated: dict[str, float | None] = {}

        for field in HOURLY_FIELDS:
            values = hourly.get(field, [])
            day_values = [
                float(values[i])
                for i in indices
                if i < len(values) and values[i] is not None
            ]
            if field == "visibility":
                aggregated[field] = _minimum(day_values)
            elif field == "weather_code":
                aggregated[field] = _mode(day_values)
            else:
                aggregated[field] = _mean(day_values)

        snapshots[target_date] = _build_weather_snapshot(
            source="open-meteo",
            reason=None,
            aggregated=aggregated,
        )

    return snapshots


async def _fetch_open_meteo_snapshots(settings: Settings) -> tuple[dict[date, WeatherSnapshot] | None, str | None]:
    params = {
        "latitude": settings.airport_latitude,
        "longitude": settings.airport_longitude,
        "timezone": settings.airport_timezone,
        "forecast_days": OPEN_METEO_CACHE_FORECAST_DAYS,
        "hourly": ",".join(HOURLY_FIELDS),
    }

    async with httpx.AsyncClient(timeout=10) as client:
        for attempt in range(OPEN_METEO_RETRIES + 1):
            try:
                response = await client.get(OPEN_METEO_URL, params=params)
                response.raise_for_status()
                snapshots = _open_meteo_payload_to_snapshots(response.json())
                if not snapshots:
                    return None, "Open-Meteo returned no hourly weather rows."
                return snapshots, None
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                logger.warning(
                    "open_meteo_status_error attempt=%s status_code=%s",
                    attempt + 1,
                    status_code,
                )
                if status_code < 500 or attempt == OPEN_METEO_RETRIES:
                    return None, f"Open-Meteo returned HTTP {status_code}."
            except (httpx.RequestError, ValueError, json.JSONDecodeError) as exc:
                logger.warning(
                    "open_meteo_request_error attempt=%s error=%s",
                    attempt + 1,
                    exc,
                )
                if attempt == OPEN_METEO_RETRIES:
                    return None, "Open-Meteo is temporarily unavailable."

            await asyncio.sleep(OPEN_METEO_RETRY_DELAY_SECONDS * (attempt + 1))

    return None, "Open-Meteo is temporarily unavailable."


def _parse_met_no_time(value: str, timezone_name: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(ZoneInfo(timezone_name))
    except (TypeError, ValueError):
        return None


def _met_no_precipitation(details: dict) -> float | None:
    for period in ("next_1_hours", "next_6_hours", "next_12_hours"):
        period_details = details.get(period, {}).get("details", {})
        value = _float_or_none(period_details.get("precipitation_amount"))
        if value is not None:
            return value
    return None


def _met_no_fog_extra_score(fog_area_fraction: float | None) -> float:
    if fog_area_fraction is None:
        return 0.0
    if fog_area_fraction >= 50:
        return 0.25
    if fog_area_fraction >= 20:
        return 0.15
    if fog_area_fraction >= 5:
        return 0.08
    return 0.0


def _met_no_payload_to_snapshot(
    payload: dict,
    target_date: date,
    timezone_name: str,
) -> WeatherSnapshot | None:
    timeseries = payload.get("properties", {}).get("timeseries", [])
    rows: list[dict[str, float | None]] = []

    for item in timeseries:
        local_datetime = _parse_met_no_time(item.get("time"), timezone_name)
        if local_datetime is None or local_datetime.date() != target_date:
            continue

        data = item.get("data", {})
        instant = data.get("instant", {}).get("details", {})
        wind_speed = _float_or_none(instant.get("wind_speed"))
        wind_gusts = _float_or_none(instant.get("wind_speed_of_gust"))

        rows.append(
            {
                "temperature_2m": _float_or_none(instant.get("air_temperature")),
                "relative_humidity_2m": _float_or_none(instant.get("relative_humidity")),
                "dew_point_2m": _float_or_none(instant.get("dew_point_temperature")),
                "pressure_msl": _float_or_none(instant.get("air_pressure_at_sea_level")),
                "cloud_cover": _float_or_none(instant.get("cloud_area_fraction")),
                "cloud_cover_low": _float_or_none(instant.get("cloud_area_fraction_low")),
                "precipitation": _met_no_precipitation(data),
                "wind_speed_10m": round(wind_speed * 3.6, 2) if wind_speed is not None else None,
                "wind_gusts_10m": round(wind_gusts * 3.6, 2) if wind_gusts is not None else None,
                "fog_area_fraction": _float_or_none(instant.get("fog_area_fraction")),
            }
        )

    if not rows:
        return None

    aggregated = {
        field: _mean([row[field] for row in rows if row.get(field) is not None])
        for field in [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "pressure_msl",
            "cloud_cover",
            "cloud_cover_low",
            "precipitation",
            "wind_speed_10m",
            "wind_gusts_10m",
        ]
    }
    aggregated["visibility"] = None
    aggregated["weather_code"] = None

    fog_area_values = [row["fog_area_fraction"] for row in rows if row.get("fog_area_fraction") is not None]
    fog_area_fraction = max(fog_area_values) if fog_area_values else None

    return _build_weather_snapshot(
        source="met-no",
        reason="Open-Meteo недоступен; используется резервный прогноз MET Norway без видимости.",
        aggregated=aggregated,
        extra_fog_risk_score=_met_no_fog_extra_score(fog_area_fraction),
    )


async def _fetch_met_no_snapshot(
    settings: Settings,
    target_date: date,
) -> tuple[WeatherSnapshot | None, str | None]:
    params = {
        "lat": round(settings.airport_latitude, 4),
        "lon": round(settings.airport_longitude, 4),
    }
    headers = {
        "User-Agent": settings.met_no_user_agent,
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=10, headers=headers) as client:
        for attempt in range(OPEN_METEO_RETRIES + 1):
            try:
                response = await client.get(MET_NO_URL, params=params)
                response.raise_for_status()
                snapshot = _met_no_payload_to_snapshot(
                    response.json(),
                    target_date=target_date,
                    timezone_name=settings.airport_timezone,
                )
                if snapshot is None:
                    return None, "MET Norway returned no rows for target date."
                return snapshot, None
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                logger.warning(
                    "met_no_status_error target_date=%s attempt=%s status_code=%s",
                    target_date.isoformat(),
                    attempt + 1,
                    status_code,
                )
                if status_code < 500 or attempt == OPEN_METEO_RETRIES:
                    return None, f"MET Norway returned HTTP {status_code}."
            except (httpx.RequestError, ValueError, json.JSONDecodeError) as exc:
                logger.warning(
                    "met_no_request_error target_date=%s attempt=%s error=%s",
                    target_date.isoformat(),
                    attempt + 1,
                    exc,
                )
                if attempt == OPEN_METEO_RETRIES:
                    return None, "MET Norway is temporarily unavailable."

            await asyncio.sleep(OPEN_METEO_RETRY_DELAY_SECONDS * (attempt + 1))

    return None, "MET Norway is temporarily unavailable."


async def fetch_weather_for_date(target_date: date) -> WeatherSnapshot:
    """
    Forecast weather is used only for the near horizon. For resilience, we keep
    a persistent Open-Meteo cache and fall back to MET Norway when needed.
    """
    settings = get_settings()
    today = datetime.now().date()
    horizon_days = (target_date - today).days

    if horizon_days < 0:
        return _unavailable_weather("Past dates are not supported by /predict in MVP.")

    if horizon_days > OPEN_METEO_MAX_HORIZON_DAYS:
        return _unavailable_weather("Forecast weather is not available for this long horizon in MVP.")

    cached = _load_open_meteo_cache(settings, target_date)
    if cached is not None:
        cached_snapshot, age_hours, _ = cached
        if age_hours <= settings.weather_cache_fresh_hours:
            logger.info(
                "weather_cache_fresh_hit target_date=%s age_hours=%.2f",
                target_date.isoformat(),
                age_hours,
            )
            return _cache_snapshot_with_source(
                cached_snapshot,
                source="open-meteo-cache",
                reason=None,
            )

    snapshots, open_meteo_error = await _fetch_open_meteo_snapshots(settings)
    if snapshots is not None:
        _store_open_meteo_cache(settings, snapshots)
        snapshot = snapshots.get(target_date)
        if snapshot is not None:
            return snapshot
        open_meteo_error = "Open-Meteo returned no hourly weather rows for target date."

    if cached is not None:
        cached_snapshot, age_hours, fetched_at = cached
        if age_hours <= settings.weather_cache_stale_hours:
            logger.warning(
                "weather_cache_stale_hit target_date=%s age_hours=%.2f open_meteo_error=%s",
                target_date.isoformat(),
                age_hours,
                open_meteo_error,
            )
            return _cache_snapshot_with_source(
                cached_snapshot,
                source="open-meteo-cache-stale",
                reason=(
                    "Open-Meteo недоступен; используется сохранённый прогноз "
                    f"от {fetched_at.isoformat()}."
                ),
            )

    if settings.met_no_fallback_enabled and horizon_days <= MET_NO_MAX_HORIZON_DAYS:
        met_no_snapshot, met_no_error = await _fetch_met_no_snapshot(settings, target_date)
        if met_no_snapshot is not None:
            return met_no_snapshot
        return _unavailable_weather(
            f"{open_meteo_error or 'Open-Meteo is unavailable'}; {met_no_error or 'MET Norway is unavailable'}"
        )

    return _unavailable_weather(open_meteo_error or "Forecast weather is temporarily unavailable.")
