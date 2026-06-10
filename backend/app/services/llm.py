import hashlib
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.schemas import FlightScheduleSnapshot, HistoricalSnapshot, WeatherSnapshot


logger = logging.getLogger("flyforecast.llm")
PROMPT_VERSION = "explanation-v10-deterministic-weather"
CACHE_SCHEMA_VERSION = 5
FORBIDDEN_EXPLANATION_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bнет\s+мест\b",
        r"\bмест[ао]?\b",
        r"\bбилет",
        r"\bпассажир",
        r"\bброн",
        r"\bсалон",
    )
]
DECISION_CONTRADICTION_PATTERNS = {
    "yes": [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"риск\s+(отмены|невыполнения|задержки)(\s+или\s+(отмены|невыполнения|задержки))?\s+(рейса\s+)?(выше|повышен|больше)",
            r"вероятн\w*\s+(не\s+полетит|не\s+выполнится|будет\s+отмен)",
            r"низк\w+\s+вероятност\w+\s+выполн",
            r"маловероятн\w+",
            r"скорее\s+не",
        )
    ],
    "no": [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"скорее\s+(да|полетит|выполнится)",
            r"высок\w+\s+шанс\w+\s+(вылета|выполнения)",
            r"благоприятн\w+\s+окн",
            r"риск\s+отмены\s+(ниже|невысок)",
        )
    ],
}


def _snapshot_dict(snapshot: WeatherSnapshot | HistoricalSnapshot | FlightScheduleSnapshot) -> dict[str, Any]:
    return snapshot.model_dump(mode="json")


def _cache_key(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _connect_cache(path: str) -> sqlite3.Connection:
    cache_path = Path(path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cache_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS explanation_cache (
            cache_key TEXT PRIMARY KEY,
            explanation TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    return conn


def _get_cached_explanation(cache_path: str, key: str) -> str | None:
    try:
        with _connect_cache(cache_path) as conn:
            row = conn.execute(
                "SELECT explanation FROM explanation_cache WHERE cache_key = ?",
                (key,),
            ).fetchone()
    except sqlite3.Error as exc:
        logger.warning("explanation_cache_read_failed error=%s", exc)
        return None

    return str(row[0]) if row else None


def _store_cached_explanation(cache_path: str, key: str, explanation: str) -> None:
    try:
        with _connect_cache(cache_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO explanation_cache (cache_key, explanation, created_at)
                VALUES (?, ?, ?)
                """,
                (key, explanation, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
    except sqlite3.Error as exc:
        logger.warning("explanation_cache_write_failed error=%s", exc)


def _is_safe_explanation(text: str, decision: str) -> bool:
    normalized = " ".join(text.split()).lower()
    if any(pattern.search(normalized) for pattern in FORBIDDEN_EXPLANATION_PATTERNS):
        return False

    contradiction_patterns = DECISION_CONTRADICTION_PATTERNS.get(decision, [])
    return not any(pattern.search(normalized) for pattern in contradiction_patterns)


def _format_percent(value: float | None) -> str | None:
    if value is None:
        return None
    return f"{round(value)}%"


def _format_number(value: float | None, unit: str = "") -> str | None:
    if value is None:
        return None
    rounded = round(value, 1)
    if rounded == round(rounded):
        rounded = int(rounded)
    return f"{rounded}{unit}"


def _format_wind_ms(value_kmh: float | None) -> str | None:
    if value_kmh is None:
        return None
    return _format_number(value_kmh / 3.6, " м/с")


def _wind_direction_label(degrees: float | None) -> str | None:
    if degrees is None:
        return None
    sectors = (
        ("северный", "N"),
        ("северо-восточный", "NE"),
        ("восточный", "E"),
        ("юго-восточный", "SE"),
        ("южный", "S"),
        ("юго-западный", "SW"),
        ("западный", "W"),
        ("северо-западный", "NW"),
    )
    label, code = sectors[int(((degrees + 22.5) % 360) // 45)]
    return f"{label} ({code})"


def _ru_day_phrase(count: int, singular: str, few: str, many: str) -> str:
    last_two = count % 100
    last = count % 10
    if 11 <= last_two <= 14:
        form = many
    elif last == 1:
        form = singular
    elif 2 <= last <= 4:
        form = few
    else:
        form = many
    return f"{count} {form}"


def _visibility_label(visibility_m: float) -> str:
    if visibility_m >= 10000:
        return "отличная"
    if visibility_m >= 5000:
        return "хорошая"
    if visibility_m >= 3000:
        return "достаточная"
    if visibility_m >= 1000:
        return "умеренная"
    return "низкая"


def _visibility_accusative_label(visibility_m: float) -> str:
    labels = {
        "отличная": "отличную",
        "хорошая": "хорошую",
        "достаточная": "достаточную",
        "умеренная": "умеренную",
        "низкая": "низкую",
    }
    return labels[_visibility_label(visibility_m)]


def _cloud_low_text(cloud_low: float) -> str:
    cloud_value = _format_percent(cloud_low)
    if cloud_low <= 10:
        return f"низкой облачности практически нет, всего {cloud_value}"
    if cloud_low <= 30:
        return f"низкая облачность небольшая, {cloud_value}"
    if cloud_low <= 70:
        return f"низкая облачность умеренная, {cloud_value}"
    if cloud_low <= 90:
        return f"низкая облачность заметная, {cloud_value}"
    return f"сильная низкая облачность, {cloud_value}"


def _cloud_low_risk_text(cloud_low: float) -> str:
    cloud_value = _format_percent(cloud_low)
    if cloud_low >= 95:
        return f"наблюдается сильная низкая облачность, {cloud_value}"
    if cloud_low >= 80:
        return f"наблюдается заметная низкая облачность, {cloud_value}"
    return _cloud_low_text(cloud_low)


def _wind_text(wind_gusts_kmh: float) -> str:
    wind_ms = wind_gusts_kmh / 3.6
    formatted = _format_wind_ms(wind_gusts_kmh)
    if wind_ms < 6:
        return f"ветер слабый: порывы до {formatted}"
    if wind_ms < 11:
        return f"ветер умеренный: порывы до {formatted}"
    if wind_ms < 17:
        return f"ветер заметный: порывы до {formatted}"
    return f"ветер сильный: порывы до {formatted}"


def _wind_risk_text(wind_gusts_kmh: float) -> str:
    wind_ms = wind_gusts_kmh / 3.6
    formatted = _format_wind_ms(wind_gusts_kmh)
    if wind_ms >= 18:
        return f"сильный ветер: порывы до {formatted}"
    if wind_ms >= 15:
        return f"заметный ветер: порывы до {formatted}"
    return _wind_text(wind_gusts_kmh)


def _dew_point_spread_text(dew_point_spread: float) -> str:
    formatted = _format_number(dew_point_spread, " °C")
    if dew_point_spread <= 1:
        return f"туман вероятнее, потому что разница температуры и точки росы всего {formatted}"
    if dew_point_spread <= 2:
        return f"туман возможен: разница температуры и точки росы {formatted}"
    return f"туман маловероятен по температуре и точке росы: разница {formatted}"


def _explicit_fog_code(weather: WeatherSnapshot) -> bool:
    return weather.weather_code is not None and int(weather.weather_code) in {45, 48}


def _weather_code(weather: WeatherSnapshot) -> int | None:
    if weather.weather_code is None:
        return None
    return int(weather.weather_code)


def _fog_status(weather: WeatherSnapshot) -> str | None:
    if _explicit_fog_code(weather) or weather.fog_low_cloud_risk_level == "high":
        return "да"
    if weather.fog_low_cloud_risk_level == "medium" or (
        weather.dew_point_spread is not None and weather.dew_point_spread <= 2
    ):
        return "возможно"
    return None


def _rain_status(weather: WeatherSnapshot) -> str | None:
    code = _weather_code(weather)
    precipitation = weather.precipitation
    if code is not None and (51 <= code <= 67 or 80 <= code <= 82 or code in {95, 96, 99}):
        return "да"
    if precipitation is not None and precipitation >= 0.5:
        return "да"
    if precipitation is not None and precipitation > 0:
        return "возможно"
    return None


def _snow_status(weather: WeatherSnapshot) -> str | None:
    code = _weather_code(weather)
    if code is not None and (71 <= code <= 77 or 85 <= code <= 86):
        return "да"
    return None


def _flight_window_hours(weather: WeatherSnapshot) -> int | None:
    if weather.flight_window_hours is not None:
        return weather.flight_window_hours
    if weather.flight_window_start_hour is None or weather.flight_window_end_hour is None:
        return None
    return max(weather.flight_window_end_hour - weather.flight_window_start_hour + 1, 1)


def _weather_window_text(weather: WeatherSnapshot, decision: str) -> str:
    if (
        weather.flight_window_available
        and weather.flight_window_start_hour is not None
        and weather.flight_window_end_hour is not None
    ):
        if decision == "no":
            hours = _flight_window_hours(weather)
            if hours is not None and hours <= 3:
                return "летное окно есть, но оно минимальное"
            return "летное окно есть, но погодные условия внутри него остаются рискованными"

        has_aggregation_window = (
            weather.aggregation_window_start_hour is not None
            and weather.aggregation_window_end_hour is not None
        )
        flight_window_matches_full_window = (
            has_aggregation_window
            and weather.flight_window_start_hour == weather.aggregation_window_start_hour
            and weather.flight_window_end_hour == weather.aggregation_window_end_hour
        )
        if flight_window_matches_full_window:
            return "есть летное окно"

        return (
            "есть летное окно примерно "
            f"с {weather.flight_window_start_hour:02d}:00 до {weather.flight_window_end_hour:02d}:00"
        )

    if weather.flight_window_available is False:
        return "устойчивое полетное окно не наблюдается"

    if weather.aggregation_window_start_hour is not None and weather.aggregation_window_end_hour is not None:
        return f"оценено рабочее окно {weather.aggregation_window_start_hour:02d}:00-{weather.aggregation_window_end_hour:02d}:00"

    return "оценены доступные погодные признаки"


def _fog_text(weather: WeatherSnapshot) -> str:
    risk_labels = {"low": "низкий", "medium": "средний", "high": "высокий"}
    risk = risk_labels.get(weather.fog_low_cloud_risk_level or "", weather.fog_low_cloud_risk_level)

    if _explicit_fog_code(weather):
        return "в погодном коде есть туман"

    if weather.fog_low_cloud_risk_level == "high":
        return "риск тумана высокий"

    if weather.fog_low_cloud_risk_level == "medium":
        return "риск тумана средний"

    if risk:
        return f"риск тумана {risk}"

    return "явных признаков тумана в прогнозе нет"


def _weather_details_text(weather: WeatherSnapshot) -> str:
    details: list[str] = []

    visibility = weather.flight_window_visibility if weather.flight_window_visibility is not None else weather.visibility
    if visibility is not None:
        details.append(f"видимость {_visibility_label(visibility)}, около {_format_number(visibility, ' м')}")

    cloud_low = (
        weather.flight_window_cloud_cover_low
        if weather.flight_window_cloud_cover_low is not None
        else weather.cloud_cover_low
    )
    if cloud_low is not None:
        details.append(_cloud_low_text(cloud_low))

    if weather.wind_gusts_10m is not None:
        details.append(_wind_text(weather.wind_gusts_10m))

    if weather.relative_humidity_2m is not None:
        details.append(f"влажность {_format_percent(weather.relative_humidity_2m)}")

    if weather.dew_point_spread is not None:
        details.append(_dew_point_spread_text(weather.dew_point_spread))

    if not details:
        return "конкретные погодные показатели в ответе источника неполные"

    return ", ".join(details)


def _no_flight_window_weather_text(weather: WeatherSnapshot) -> str:
    blockers: list[str] = []
    context: list[str] = []

    visibility = weather.visibility
    if visibility is not None:
        if visibility >= 5000:
            context.append(
                "несмотря на "
                f"{_visibility_accusative_label(visibility)} видимость около {_format_number(visibility, ' м')}"
            )
        else:
            blockers.append(f"видимость {_visibility_label(visibility)}, около {_format_number(visibility, ' м')}")

    if weather.cloud_cover_low is not None and weather.cloud_cover_low >= 80:
        blockers.append(_cloud_low_risk_text(weather.cloud_cover_low))

    if weather.wind_gusts_10m is not None and weather.wind_gusts_10m >= 55:
        blockers.append(_wind_risk_text(weather.wind_gusts_10m))

    if weather.precipitation is not None and weather.precipitation >= 2.5:
        blockers.append(f"ожидаются осадки около {_format_number(weather.precipitation, ' мм')}")

    if _explicit_fog_code(weather):
        blockers.append("в погодном коде есть туман")
    elif weather.dew_point_spread is not None and weather.dew_point_spread <= 2:
        blockers.append(_dew_point_spread_text(weather.dew_point_spread))

    if not blockers:
        blockers.append(_weather_details_text(weather))

    detail_parts = context + blockers
    return "устойчивое полетное окно не наблюдается: " + ", ".join(detail_parts)


def _history_details_text(history: HistoricalSnapshot) -> str:
    historical_percent = round(history.historical_probability_flight * 100)

    if history.similar_days_count > 0:
        completed_phrase = _ru_day_phrase(
            history.completed_count,
            "день с выполненным рейсом",
            "дня с выполненным рейсом",
            "дней с выполненным рейсом",
        )
        cancelled_phrase = _ru_day_phrase(
            history.cancelled_count,
            "день с отменой",
            "дня с отменой",
            "дней с отменой",
        )
        return (
            f"Исторически в календарном окне ±14 дней вокруг выбранной даты было {completed_phrase} "
            f"и {cancelled_phrase} из {history.similar_days_count}, "
            f"то есть около {historical_percent}% выполнений"
        )

    return f"Историческая оценка по календарному окну ±14 дней около {historical_percent}% выполнений"


def _has_specific_weather_details(text: str, weather: WeatherSnapshot) -> bool:
    if not weather.available:
        return True

    normalized = text.lower()
    has_number = bool(re.search(r"\d", normalized))
    has_weather_term = any(
        term in normalized
        for term in ("видим", "облач", "туман", "ветер", "порыв", "влажн", "рос")
    )
    return has_number and has_weather_term


def is_board_cancelled_for_target_date(schedule: FlightScheduleSnapshot | None) -> bool:
    return bool(schedule is not None and schedule.available and schedule.moved_next_day)


def is_board_completed_for_target_date(schedule: FlightScheduleSnapshot | None) -> bool:
    return bool(schedule is not None and schedule.available and schedule.completed_same_day)


def _board_cancelled_explanation(schedule: FlightScheduleSnapshot | None) -> str:
    return "По табло рейс отменен для этой даты."


def _board_completed_explanation(schedule: FlightScheduleSnapshot | None) -> str:
    return "По табло рейс уже выполнен для этой даты."


def _weather_detail_lines(weather: WeatherSnapshot) -> list[str]:
    lines: list[str] = []

    visibility = weather.flight_window_visibility if weather.flight_window_visibility is not None else weather.visibility
    if visibility is not None:
        lines.append(f"видимость - около {_format_number(visibility, ' м')}")

    cloud_low = (
        weather.flight_window_cloud_cover_low
        if weather.flight_window_cloud_cover_low is not None
        else weather.cloud_cover_low
    )
    if cloud_low is not None:
        lines.append(f"низкая облачность - {_format_percent(cloud_low)}")

    wind_parts: list[str] = []
    if weather.wind_gusts_10m is not None:
        wind_parts.append(f"порывы до {_format_wind_ms(weather.wind_gusts_10m)}")
    elif weather.wind_speed_10m is not None:
        wind_parts.append(f"скорость около {_format_wind_ms(weather.wind_speed_10m)}")
    wind_direction = _wind_direction_label(weather.wind_direction_10m)
    if wind_direction:
        wind_parts.append(f"направление {wind_direction}")
    if wind_parts:
        lines.append(f"ветер - {', '.join(wind_parts)}")

    if weather.relative_humidity_2m is not None:
        lines.append(f"влажность - {_format_percent(weather.relative_humidity_2m)}")

    if weather.pressure_msl is not None:
        lines.append(f"давление - {_format_number(weather.pressure_msl, ' гПа')}")

    fog_status = _fog_status(weather)
    if fog_status is not None:
        lines.append(f"туман - {fog_status}")

    rain_status = _rain_status(weather)
    if rain_status is not None:
        lines.append(f"дождь - {rain_status}")

    snow_status = _snow_status(weather)
    if snow_status is not None:
        lines.append(f"снег - {snow_status}")

    return lines


def _weather_model_explanation(
    decision: str,
    probability_flight: float,
    weather: WeatherSnapshot,
    schedule: FlightScheduleSnapshot | None = None,
) -> str:
    probability_percent = round(probability_flight * 100)

    decision_text = "Да" if decision == "yes" else "Нет"
    weather_lines = _weather_detail_lines(weather)
    if not weather_lines:
        weather_lines = ["погодные показатели - данные источника неполные"]

    return "\n".join(
        [
            f"{decision_text}. Данные погоды:",
            *weather_lines,
            f"Исходя из этих факторов, вероятность вылета — {probability_percent}%.",
        ]
    )


def fallback_explanation(
    target_date: str,
    decision: str,
    probability_flight: float,
    confidence: str,
    horizon_days: int,
    weather: WeatherSnapshot,
    history: HistoricalSnapshot,
    schedule: FlightScheduleSnapshot | None = None,
) -> str:
    probability_percent = round(probability_flight * 100)
    if is_board_cancelled_for_target_date(schedule):
        return _board_cancelled_explanation(schedule)
    if is_board_completed_for_target_date(schedule):
        return _board_completed_explanation(schedule)

    if weather.available:
        return _weather_model_explanation(
            decision=decision,
            probability_flight=probability_flight,
            weather=weather,
            schedule=schedule,
        )

    history_text = _history_details_text(history)
    schedule_text = ""
    if schedule is not None and schedule.available:
        if schedule.completed_same_day:
            schedule_text = "По последним строкам табло рейс на эту дату уже выполнялся."

    if not weather.available:
        horizon_text = (
            "Точного погодного прогноза на эту дату пока нет, поэтому оценка опирается на историю и сезонность."
        )
    elif weather.available:
        if weather.flight_window_available is False:
            weather_parts = [_no_flight_window_weather_text(weather)]
        else:
            weather_parts = [_weather_window_text(weather, decision), _weather_details_text(weather)]
        should_add_fog_summary = weather.dew_point_spread is None or _explicit_fog_code(weather)
        if should_add_fog_summary:
            weather_parts.append(_fog_text(weather))
        horizon_text = f"По погоде: {'; '.join(weather_parts)}."
    else:
        horizon_text = "Погодный прогноз для даты недоступен, поэтому оценка опирается на историю календарно близких дат."

    schedule_prefix = f"{schedule_text} " if schedule_text else ""

    if decision == "yes":
        return (
            f"Да: вероятность выполнения рейса {probability_percent}%, поэтому дата выглядит скорее подходящей для вылета. "
            f"{schedule_prefix}{horizon_text} {history_text}."
        )

    return (
        f"Нет: вероятность выполнения рейса {probability_percent}%, поэтому риск отмены или невыполнения выглядит повышенным. "
        f"{schedule_prefix}{horizon_text} {history_text}."
    )


def generate_user_explanation(
    target_date: str,
    decision: str,
    probability_flight: float,
    confidence: str,
    horizon_days: int,
    weather: WeatherSnapshot,
    history: HistoricalSnapshot,
    schedule: FlightScheduleSnapshot | None = None,
) -> str:
    return fallback_explanation(
        target_date=target_date,
        decision=decision,
        probability_flight=probability_flight,
        confidence=confidence,
        horizon_days=horizon_days,
        weather=weather,
        history=history,
        schedule=schedule,
    )
