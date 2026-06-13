import asyncio
import json
import logging
import math
import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from app.config import Settings, get_settings
from app.schemas import WeatherSnapshot
from app.services.fog_risk import (
    FOG_WEATHER_CODES,
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
    "wind_direction_10m",
    "weather_code",
    "visibility",
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_MAX_HORIZON_DAYS = 15
OPEN_METEO_RETRIES = 2
OPEN_METEO_RETRY_DELAY_SECONDS = 0.8
OPEN_METEO_CACHE_FORECAST_DAYS = OPEN_METEO_MAX_HORIZON_DAYS + 1
OPEN_METEO_CACHE_PROVIDER = "open-meteo-flight-window-v2"
OPEN_METEO_STATE_PROVIDER = f"{OPEN_METEO_CACHE_PROVIDER}:live-refresh"

MET_NO_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
MET_NO_MAX_HORIZON_DAYS = 9


def _fresh_cache_max_age_hours(settings: Settings, horizon_days: int) -> float:
    base_hours = max(float(settings.weather_cache_fresh_hours), 0.0)
    if horizon_days <= 1:
        live_hours = max(float(settings.weather_live_cache_fresh_minutes), 1.0) / 60.0
        if base_hours <= 0:
            return live_hours
        return min(base_hours, live_hours)
    return base_hours


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


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None

    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return round(sorted_values[0], 2)

    position = (len(sorted_values) - 1) * q
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    weight = position - lower_index
    value = sorted_values[lower_index] * (1 - weight) + sorted_values[upper_index] * weight
    return round(value, 2)


def _mode(values: list[float]) -> float | None:
    return float(max(set(values), key=values.count)) if values else None


def _mean_wind_direction(values: list[float]) -> float | None:
    if not values:
        return None

    sin_sum = sum(math.sin(math.radians(value)) for value in values)
    cos_sum = sum(math.cos(math.radians(value)) for value in values)
    if sin_sum == 0 and cos_sum == 0:
        return None

    return round((math.degrees(math.atan2(sin_sum, cos_sum)) + 360) % 360, 2)


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _aggregate_weather_rows(rows: list[dict[str, float | int | None]]) -> dict[str, float | None]:
    aggregated: dict[str, float | None] = {}

    for field in HOURLY_FIELDS:
        values = [
            float(row[field])
            for row in rows
            if row.get(field) is not None
        ]
        if field == "visibility":
            aggregated[field] = _quantile(values, 0.25)
        elif field == "wind_direction_10m":
            aggregated[field] = _mean_wind_direction(values)
        elif field == "weather_code":
            aggregated[field] = _mode(values)
        else:
            aggregated[field] = _mean(values)

    return aggregated


def _fog_score_for_aggregated(aggregated: dict[str, float | None], extra_fog_risk_score: float = 0.0) -> float:
    dew_point_spread = calculate_dew_point_spread(
        aggregated.get("temperature_2m"),
        aggregated.get("dew_point_2m"),
    )
    score = calculate_fog_low_cloud_risk_score(
        visibility=aggregated.get("visibility"),
        cloud_cover_low=aggregated.get("cloud_cover_low"),
        relative_humidity_2m=aggregated.get("relative_humidity_2m"),
        dew_point_spread=dew_point_spread,
        wind_speed_10m=aggregated.get("wind_speed_10m"),
        wind_gusts_10m=aggregated.get("wind_gusts_10m"),
        precipitation=aggregated.get("precipitation"),
        weather_code=aggregated.get("weather_code"),
    )
    return round(min(max(score + extra_fog_risk_score, 0.0), 1.0), 3)


def _is_flight_opportunity_hour(row: dict[str, float | int | None], settings: Settings) -> bool:
    visibility = row.get("visibility")
    weather_code = row.get("weather_code")
    has_fog_code = weather_code is not None and int(weather_code) in FOG_WEATHER_CODES
    if visibility is not None and visibility < settings.weather_flight_window_min_visibility:
        return False

    wind_gusts = row.get("wind_gusts_10m")
    if wind_gusts is not None and wind_gusts > settings.weather_flight_window_max_wind_gusts:
        return False

    precipitation = row.get("precipitation")
    if precipitation is not None and precipitation > settings.weather_flight_window_max_precipitation:
        return False

    if has_fog_code:
        return False

    return True


def _flight_window_sort_key(rows: list[dict[str, float | int | None]]) -> tuple:
    aggregated = _aggregate_weather_rows(rows)
    visibility = aggregated.get("visibility")
    cloud_cover_low = aggregated.get("cloud_cover_low")
    wind_gusts = aggregated.get("wind_gusts_10m")
    precipitation = aggregated.get("precipitation")
    fog_score = _fog_score_for_aggregated(aggregated)
    return (
        len(rows),
        visibility if visibility is not None else 0.0,
        -(fog_score if fog_score is not None else 1.0),
        -(cloud_cover_low if cloud_cover_low is not None else 100.0),
        -(wind_gusts if wind_gusts is not None else 100.0),
        -(precipitation if precipitation is not None else 10.0),
    )


def _find_flight_opportunity_window(
    rows: list[dict[str, float | int | None]],
    settings: Settings,
) -> list[dict[str, float | int | None]] | None:
    min_hours = max(1, int(settings.weather_flight_window_min_hours))
    candidates: list[list[dict[str, float | int | None]]] = []
    current: list[dict[str, float | int | None]] = []

    for row in rows:
        if current and row.get("hour") is not None and current[-1].get("hour") is not None:
            if int(row["hour"]) != int(current[-1]["hour"]) + 1:
                if len(current) >= min_hours:
                    candidates.append(current)
                current = []

        if _is_flight_opportunity_hour(row, settings):
            current.append(row)
        else:
            if len(current) >= min_hours:
                candidates.append(current)
            current = []

    if len(current) >= min_hours:
        candidates.append(current)

    if not candidates:
        return None

    return max(candidates, key=_flight_window_sort_key)


def _flight_window_payload(
    rows: list[dict[str, float | int | None]] | None,
) -> dict[str, float | int | bool | str | None]:
    if not rows:
        return {
            "flight_window_available": False,
            "flight_window_start_hour": None,
            "flight_window_end_hour": None,
            "flight_window_hours": None,
            "flight_window_visibility": None,
            "flight_window_cloud_cover_low": None,
            "flight_window_fog_low_cloud_risk_score": None,
            "flight_window_fog_low_cloud_risk_level": None,
        }

    aggregated = _aggregate_weather_rows(rows)
    fog_score = _fog_score_for_aggregated(aggregated)
    return {
        "flight_window_available": True,
        "flight_window_start_hour": int(rows[0]["hour"]) if rows[0].get("hour") is not None else None,
        "flight_window_end_hour": int(rows[-1]["hour"]) if rows[-1].get("hour") is not None else None,
        "flight_window_hours": len(rows),
        "flight_window_visibility": aggregated.get("visibility"),
        "flight_window_cloud_cover_low": aggregated.get("cloud_cover_low"),
        "flight_window_fog_low_cloud_risk_score": fog_score,
        "flight_window_fog_low_cloud_risk_level": fog_low_cloud_risk_level(fog_score),
    }


def _build_weather_snapshot(
    *,
    source: str,
    reason: str | None,
    aggregated: dict[str, float | None],
    extra_fog_risk_score: float = 0.0,
    aggregation_window_start_hour: int | None = None,
    aggregation_window_end_hour: int | None = None,
    aggregation_window_hours: int | None = None,
    flight_window: dict[str, float | int | bool | str | None] | None = None,
) -> WeatherSnapshot:
    dew_point_spread = calculate_dew_point_spread(
        aggregated.get("temperature_2m"),
        aggregated.get("dew_point_2m"),
    )
    fog_low_cloud_risk_score = _fog_score_for_aggregated(aggregated, extra_fog_risk_score)
    flight_window = flight_window or _flight_window_payload(None)

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
        wind_direction_10m=aggregated.get("wind_direction_10m"),
        weather_code=aggregated.get("weather_code"),
        visibility=aggregated.get("visibility"),
        fog_low_cloud_risk_score=fog_low_cloud_risk_score,
        fog_low_cloud_risk_level=fog_low_cloud_risk_level(fog_low_cloud_risk_score),
        aggregation_window_start_hour=aggregation_window_start_hour,
        aggregation_window_end_hour=aggregation_window_end_hour,
        aggregation_window_hours=aggregation_window_hours,
        flight_window_available=bool(flight_window.get("flight_window_available")),
        flight_window_start_hour=flight_window.get("flight_window_start_hour"),
        flight_window_end_hour=flight_window.get("flight_window_end_hour"),
        flight_window_hours=flight_window.get("flight_window_hours"),
        flight_window_visibility=flight_window.get("flight_window_visibility"),
        flight_window_cloud_cover_low=flight_window.get("flight_window_cloud_cover_low"),
        flight_window_fog_low_cloud_risk_score=flight_window.get("flight_window_fog_low_cloud_risk_score"),
        flight_window_fog_low_cloud_risk_level=flight_window.get("flight_window_fog_low_cloud_risk_level"),
    )


def _weather_window(settings: Settings) -> tuple[int, int]:
    start_hour = min(max(int(settings.weather_forecast_window_start_hour), 0), 23)
    end_hour = min(max(int(settings.weather_forecast_window_end_hour), 0), 23)
    return start_hour, end_hour


def _hour_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
    if start_hour <= end_hour:
        return start_hour <= hour <= end_hour
    return hour >= start_hour or hour <= end_hour


def _hour_from_open_meteo_time(value: str) -> int | None:
    try:
        return datetime.fromisoformat(value).hour
    except ValueError:
        return None


def _filter_indices_to_window(
    times: list,
    indices: list[int],
    *,
    start_hour: int,
    end_hour: int,
) -> list[int]:
    filtered: list[int] = []
    for index in indices:
        if index >= len(times) or not isinstance(times[index], str):
            continue

        hour = _hour_from_open_meteo_time(times[index])
        if hour is not None and _hour_in_window(hour, start_hour, end_hour):
            filtered.append(index)

    return filtered or indices


def _open_meteo_rows_for_indices(hourly: dict, times: list, indices: list[int]) -> list[dict[str, float | int | None]]:
    rows: list[dict[str, float | int | None]] = []

    for index in indices:
        if index >= len(times) or not isinstance(times[index], str):
            continue

        hour = _hour_from_open_meteo_time(times[index])
        if hour is None:
            continue

        row: dict[str, float | int | None] = {"hour": hour}
        for field in HOURLY_FIELDS:
            values = hourly.get(field, [])
            row[field] = _float_or_none(values[index]) if index < len(values) else None
        rows.append(row)

    return rows


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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_provider_state (
            provider TEXT PRIMARY KEY,
            failure_count INTEGER NOT NULL DEFAULT 0,
            last_failure_at TEXT,
            last_failure_reason TEXT,
            cooldown_until TEXT
        )
        """
    )
    conn.commit()
    return conn


def _parse_utc_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _open_meteo_circuit_state(settings: Settings) -> tuple[datetime | None, str | None]:
    try:
        with closing(_connect_weather_cache(settings.weather_forecast_cache_path)) as conn:
            row = conn.execute(
                """
                SELECT cooldown_until, last_failure_reason
                FROM weather_provider_state
                WHERE provider = ?
                """,
                (OPEN_METEO_STATE_PROVIDER,),
            ).fetchone()
    except sqlite3.Error as exc:
        logger.warning("open_meteo_circuit_read_failed error=%s", exc)
        return None, None

    if not row:
        return None, None

    cooldown_until_raw, reason = row
    return _parse_utc_datetime(cooldown_until_raw), reason


def _is_open_meteo_circuit_open(settings: Settings) -> tuple[bool, datetime | None, str | None]:
    cooldown_until, reason = _open_meteo_circuit_state(settings)
    if cooldown_until is None:
        return False, None, reason

    if cooldown_until > _utc_now():
        return True, cooldown_until, reason

    return False, cooldown_until, reason


def _record_open_meteo_success(settings: Settings) -> None:
    try:
        with closing(_connect_weather_cache(settings.weather_forecast_cache_path)) as conn:
            conn.execute(
                """
                DELETE FROM weather_provider_state
                WHERE provider = ?
                """,
                (OPEN_METEO_STATE_PROVIDER,),
            )
            conn.commit()
    except sqlite3.Error as exc:
        logger.warning("open_meteo_circuit_reset_failed error=%s", exc)


def _record_open_meteo_failure(settings: Settings, reason: str | None) -> None:
    cooldown_minutes = max(1, int(settings.open_meteo_failure_cooldown_minutes))
    now = _utc_now()
    cooldown_until = now + timedelta(minutes=cooldown_minutes)
    failure_reason = reason or "Open-Meteo is temporarily unavailable."

    try:
        with closing(_connect_weather_cache(settings.weather_forecast_cache_path)) as conn:
            conn.execute(
                """
                INSERT INTO weather_provider_state (
                    provider,
                    failure_count,
                    last_failure_at,
                    last_failure_reason,
                    cooldown_until
                )
                VALUES (?, 1, ?, ?, ?)
                ON CONFLICT(provider) DO UPDATE SET
                    failure_count = weather_provider_state.failure_count + 1,
                    last_failure_at = excluded.last_failure_at,
                    last_failure_reason = excluded.last_failure_reason,
                    cooldown_until = excluded.cooldown_until
                """,
                (
                    OPEN_METEO_STATE_PROVIDER,
                    now.isoformat(),
                    failure_reason,
                    cooldown_until.isoformat(),
                ),
            )
            conn.commit()
    except sqlite3.Error as exc:
        logger.warning("open_meteo_circuit_write_failed error=%s", exc)
        return

    logger.warning(
        "open_meteo_circuit_recorded_failure cooldown_until=%s reason=%s",
        cooldown_until.isoformat(),
        failure_reason,
    )


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
                (target_date.isoformat(), OPEN_METEO_CACHE_PROVIDER),
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
            OPEN_METEO_CACHE_PROVIDER,
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


def _open_meteo_payload_to_snapshots(payload: dict, settings: Settings) -> dict[date, WeatherSnapshot]:
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    date_to_indices: dict[date, list[int]] = {}
    start_hour, end_hour = _weather_window(settings)

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
        window_indices = _filter_indices_to_window(
            times,
            indices,
            start_hour=start_hour,
            end_hour=end_hour,
        )
        window_rows = _open_meteo_rows_for_indices(hourly, times, window_indices)
        flight_window_rows = _find_flight_opportunity_window(window_rows, settings)
        aggregation_rows = flight_window_rows or window_rows
        aggregated = _aggregate_weather_rows(aggregation_rows)

        snapshots[target_date] = _build_weather_snapshot(
            source="open-meteo",
            reason=None,
            aggregated=aggregated,
            aggregation_window_start_hour=start_hour,
            aggregation_window_end_hour=end_hour,
            aggregation_window_hours=len(window_indices),
            flight_window=_flight_window_payload(flight_window_rows),
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
                snapshots = _open_meteo_payload_to_snapshots(response.json(), settings)
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
    settings: Settings,
) -> WeatherSnapshot | None:
    timeseries = payload.get("properties", {}).get("timeseries", [])
    start_hour, end_hour = _weather_window(settings)
    rows: list[dict[str, float | None]] = []
    fallback_rows: list[dict[str, float | None]] = []

    for item in timeseries:
        local_datetime = _parse_met_no_time(item.get("time"), settings.airport_timezone)
        if local_datetime is None or local_datetime.date() != target_date:
            continue

        data = item.get("data", {})
        instant = data.get("instant", {}).get("details", {})
        wind_speed = _float_or_none(instant.get("wind_speed"))
        wind_gusts = _float_or_none(instant.get("wind_speed_of_gust"))

        row = {
            "hour": local_datetime.hour,
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
        fallback_rows.append(row)
        if _hour_in_window(local_datetime.hour, start_hour, end_hour):
            rows.append(row)

    rows = rows or fallback_rows
    if not rows:
        return None

    flight_window_rows = _find_flight_opportunity_window(rows, settings)
    aggregation_rows = flight_window_rows or rows
    aggregated = _aggregate_weather_rows(aggregation_rows)
    aggregated["visibility"] = None
    aggregated["weather_code"] = None

    fog_area_values = [row["fog_area_fraction"] for row in aggregation_rows if row.get("fog_area_fraction") is not None]
    fog_area_fraction = max(fog_area_values) if fog_area_values else None

    return _build_weather_snapshot(
        source="met-no",
        reason="Open-Meteo недоступен; используется резервный прогноз MET Norway без видимости.",
        aggregated=aggregated,
        extra_fog_risk_score=_met_no_fog_extra_score(fog_area_fraction),
        aggregation_window_start_hour=start_hour,
        aggregation_window_end_hour=end_hour,
        aggregation_window_hours=len(rows),
        flight_window=_flight_window_payload(flight_window_rows),
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
                    settings=settings,
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
        fresh_max_age_hours = _fresh_cache_max_age_hours(settings, horizon_days)
        if age_hours <= fresh_max_age_hours:
            logger.info(
                "weather_cache_fresh_hit target_date=%s horizon_days=%s age_hours=%.2f max_age_hours=%.2f",
                target_date.isoformat(),
                horizon_days,
                age_hours,
                fresh_max_age_hours,
            )
            return _cache_snapshot_with_source(
                cached_snapshot,
                source="open-meteo-cache",
                reason=None,
            )

    circuit_open, cooldown_until, circuit_reason = _is_open_meteo_circuit_open(settings)
    open_meteo_error = circuit_reason
    if circuit_open:
        logger.warning(
            "open_meteo_circuit_open target_date=%s cooldown_until=%s reason=%s",
            target_date.isoformat(),
            cooldown_until.isoformat() if cooldown_until else None,
            circuit_reason,
        )
        open_meteo_error = "Open-Meteo refresh is paused after recent failures."
    else:
        snapshots, open_meteo_error = await _fetch_open_meteo_snapshots(settings)
        if snapshots is not None:
            _record_open_meteo_success(settings)
            _store_open_meteo_cache(settings, snapshots)
            snapshot = snapshots.get(target_date)
            if snapshot is not None:
                return snapshot
            open_meteo_error = "Open-Meteo returned no hourly weather rows for target date."
        else:
            _record_open_meteo_failure(settings, open_meteo_error)

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
                    "Open-Meteo временно недоступен; используется сохранённый прогноз "
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
