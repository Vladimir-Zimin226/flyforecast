from app.schemas import FlightScheduleSnapshot, HistoricalSnapshot, WeatherSnapshot


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


def is_board_cancelled_for_target_date(schedule: FlightScheduleSnapshot | None) -> bool:
    return bool(schedule is not None and schedule.available and schedule.moved_next_day)


def is_board_completed_for_target_date(schedule: FlightScheduleSnapshot | None) -> bool:
    return bool(schedule is not None and schedule.available and schedule.completed_same_day)


def _schedule_uses_arrival_facts(schedule: FlightScheduleSnapshot | None) -> bool:
    return bool(
        schedule is not None
        and schedule.flights
        and all((flight.direction or "").lower() == "arrival" for flight in schedule.flights)
    )


def _completed_progress_text(schedule: FlightScheduleSnapshot) -> str:
    if _schedule_uses_arrival_facts(schedule):
        verb = "прибыл" if schedule.completed_flights == 1 else "прибыло"
        return f"По табло {verb} {schedule.completed_flights} из {schedule.total_flights} рейсов"
    return f"По табло выполнено {schedule.completed_flights} из {schedule.total_flights} рейсов"


def _board_cancelled_explanation(schedule: FlightScheduleSnapshot | None) -> str:
    if schedule is not None and schedule.total_flights > 1:
        if schedule.completed_flights > 0:
            return (
                f"{_completed_progress_text(schedule)}. "
                "Следующий рейс отменен или перенесен для этой даты."
            )
        if schedule.unavailable_flights >= schedule.total_flights:
            return "По табло все рейсы отменены или перенесены для этой даты."
        if schedule.active_flight_index == 1:
            return "По табло первый рейс отменен или перенесен для этой даты."
        return "По табло следующий рейс отменен или перенесен для этой даты."
    return "По табло рейс отменен для этой даты."


def _board_completed_explanation(schedule: FlightScheduleSnapshot | None) -> str:
    if schedule is not None and schedule.total_flights > 1:
        if _schedule_uses_arrival_facts(schedule):
            return "По табло сегодняшние рейсы успешно прибыли."
        return "По табло сегодняшние рейсы успешно выполнены."
    if _schedule_uses_arrival_facts(schedule):
        return "По табло рейс уже прибыл для этой даты."
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

    lines = []
    if (
        schedule is not None
        and schedule.total_flights > 1
        and schedule.completed_flights > 0
        and not schedule.completed_same_day
        and not schedule.moved_next_day
    ):
        lines.append(
            f"{_completed_progress_text(schedule)}. "
            "Прогноз относится к следующему рейсу."
        )

    lines.extend(
        [
            f"{decision_text}. Данные погоды:",
            *weather_lines,
            f"Исходя из этих факторов, вероятность вылета — {probability_percent}%.",
        ]
    )
    return "\n".join(lines)


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
    horizon_text = "Точного погодного прогноза на эту дату пока нет, поэтому оценка опирается на историю и сезонность."

    if decision == "yes":
        return (
            f"Да: вероятность выполнения рейса {probability_percent}%, поэтому дата выглядит скорее подходящей для вылета. "
            f"{horizon_text} {history_text}."
        )

    return (
        f"Нет: вероятность выполнения рейса {probability_percent}%, поэтому риск отмены или невыполнения выглядит повышенным. "
        f"{horizon_text} {history_text}."
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
