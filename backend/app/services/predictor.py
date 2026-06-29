from datetime import date, datetime
from zoneinfo import ZoneInfo

from app.config import get_settings
from app.schemas import FlightScheduleSnapshot, HistoricalSnapshot, WeatherSnapshot


MODEL_VERSION = "mvp-baseline-009"
DATA_VERSION = "telegram-v2-plus-historical-board-manual-v3-2026-05-20"

DISCLAIMER = (
    "Это вероятностная оценка, а не гарантия выполнения рейса. "
    "Сервис не является официальным источником статуса рейса. "
    "Проверяйте официальные источники перед поездкой."
)


def get_horizon_days(target_date: date) -> int:
    settings = get_settings()
    today = datetime.now(ZoneInfo(settings.airport_timezone)).date()
    return (target_date - today).days


def get_confidence(
    horizon_days: int,
    weather: WeatherSnapshot,
    history: HistoricalSnapshot,
    schedule: FlightScheduleSnapshot | None = None,
) -> str:
    if schedule is not None and schedule.available and (schedule.moved_next_day or schedule.completed_same_day):
        return "high"
    if horizon_days <= 10 and weather.available and history.similar_days_count >= 20:
        return "medium"
    if horizon_days <= 46 and history.similar_days_count >= 20:
        return "medium"
    return "low"


def is_weather_window_too_late_for_schedule(
    weather: WeatherSnapshot,
    schedule: FlightScheduleSnapshot | None,
) -> bool:
    if (
        schedule is None
        or not schedule.available
        or schedule.first_departure_hour is None
        or weather.flight_window_start_hour is None
    ):
        return False

    latest_useful_start_hour = min(schedule.first_departure_hour + 1, 23)
    return weather.flight_window_start_hour > latest_useful_start_hour


def is_weather_window_before_active_flight(
    weather: WeatherSnapshot,
    schedule: FlightScheduleSnapshot | None,
) -> bool:
    if (
        schedule is None
        or not schedule.available
        or schedule.active_flight_hour is None
        or not weather.flight_window_available
        or weather.flight_window_end_hour is None
    ):
        return False

    return weather.flight_window_end_hour < schedule.active_flight_hour


def wind_sector(degrees: float | None) -> str | None:
    if degrees is None:
        return None
    sectors = ("N", "NE", "E", "SE", "S", "SW", "W", "NW")
    return sectors[int(((degrees + 22.5) % 360) // 45)]


def has_compound_humidity_risk(weather: WeatherSnapshot) -> bool:
    if weather.relative_humidity_2m is None or weather.relative_humidity_2m < 92:
        return False

    visibility = weather.flight_window_visibility if weather.flight_window_visibility is not None else weather.visibility
    low_visibility = visibility is not None and visibility <= 3000
    meaningful_precipitation = weather.precipitation is not None and weather.precipitation >= 0.5
    low_pressure = weather.pressure_msl is not None and weather.pressure_msl <= 1005
    closed_dew_point = weather.dew_point_spread is not None and weather.dew_point_spread <= 1
    windy_low_cloud = (
        weather.cloud_cover_low is not None
        and weather.cloud_cover_low >= 95
        and weather.wind_speed_10m is not None
        and weather.wind_speed_10m >= 25
    )

    return (
        low_visibility
        or meaningful_precipitation
        or low_pressure
        or windy_low_cloud
        or (closed_dew_point and weather.fog_low_cloud_risk_level == "high")
    )


def has_good_late_clearing_support(
    weather: WeatherSnapshot,
    late_schedule_window: bool,
    early_active_flight_window: bool = False,
) -> bool:
    if not weather.flight_window_available or late_schedule_window or early_active_flight_window:
        return False

    visibility = weather.flight_window_visibility if weather.flight_window_visibility is not None else weather.visibility
    return (
        visibility is not None
        and visibility >= 8000
        and (weather.pressure_msl is None or weather.pressure_msl > 1010)
        and (weather.precipitation is None or weather.precipitation <= 0.2)
        and (weather.wind_gusts_10m is None or weather.wind_gusts_10m < 60)
        and not has_compound_humidity_risk(weather)
    )


def _weather_row_value(row: dict[str, float | int | None], field: str) -> float | int | None:
    return row.get(field)


def has_bad_early_weather(weather: WeatherSnapshot) -> bool:
    if not weather.available:
        return False

    rows = [
        row
        for row in (weather.hourly_rows or [])
        if row.get("hour") is not None and 8 <= int(row["hour"]) <= 11
    ]
    if not rows:
        visibility = weather.visibility
        return bool(
            visibility is not None
            and visibility <= 3000
            and (
                (weather.cloud_cover_low is not None and weather.cloud_cover_low >= 90)
                or (weather.cloud_cover is not None and weather.cloud_cover >= 95)
                or weather.fog_low_cloud_risk_level in {"medium", "high"}
            )
        )

    for row in rows:
        visibility = _weather_row_value(row, "visibility")
        cloud_cover = _weather_row_value(row, "cloud_cover")
        cloud_cover_low = _weather_row_value(row, "cloud_cover_low")
        weather_code = _weather_row_value(row, "weather_code")
        has_fog_code = weather_code is not None and int(weather_code) in {45, 48}

        if visibility is not None and visibility <= 1000:
            return True
        if (
            visibility is not None
            and visibility <= 3000
            and (
                (cloud_cover_low is not None and cloud_cover_low >= 90)
                or (cloud_cover is not None and cloud_cover >= 95)
                or has_fog_code
            )
        ):
            return True

    return False


def schedule_has_operational_disruption(schedule: FlightScheduleSnapshot | None) -> bool:
    if schedule is None or not schedule.available:
        return False

    statuses = {
        status.strip().lower()
        for status in (schedule.status_summary or "").split(";")
        if status.strip()
    }
    statuses.update(
        (flight.status or "").strip().lower()
        for flight in schedule.flights
        if (flight.status or "").strip()
    )
    return bool(statuses & {"delayed", "combined", "cancelled"})


def has_compressed_operational_window(
    weather: WeatherSnapshot,
    schedule: FlightScheduleSnapshot | None,
) -> bool:
    if schedule is None or not schedule.available:
        return False

    late_schedule = (
        (schedule.first_departure_hour is not None and schedule.first_departure_hour >= 13)
        or (schedule.last_scheduled_hour is not None and schedule.last_scheduled_hour >= 17)
    )
    late_weather_window = (
        weather.flight_window_available
        and weather.flight_window_start_hour is not None
        and weather.flight_window_start_hour >= 12
    )
    many_pending = schedule.pending_flights >= 2
    return bool(late_schedule or late_weather_window or many_pending)


def has_marginal_active_flight_weather(weather: WeatherSnapshot) -> bool:
    visibility = weather.flight_window_visibility if weather.flight_window_visibility is not None else weather.visibility
    return bool(
        weather.fog_low_cloud_risk_level in {"medium", "high"}
        or (visibility is not None and visibility <= 8000)
        or (weather.cloud_cover_low is not None and weather.cloud_cover_low >= 95)
        or (weather.dew_point_spread is not None and weather.dew_point_spread <= 1)
        or (weather.precipitation is not None and weather.precipitation >= 0.5)
    )


def calculate_operational_stress_adjustment(
    weather: WeatherSnapshot,
    schedule: FlightScheduleSnapshot | None = None,
) -> float:
    if (
        schedule is None
        or not schedule.available
        or schedule.moved_next_day
        or schedule.completed_same_day
        or not schedule_has_operational_disruption(schedule)
    ):
        return 0.0

    penalty = 0.04

    if has_bad_early_weather(weather):
        penalty += 0.08

    if has_compressed_operational_window(weather, schedule):
        penalty += 0.04

    if is_weather_window_before_active_flight(weather, schedule) and has_marginal_active_flight_weather(weather):
        penalty += 0.10

    if (
        weather.available
        and weather.cloud_cover is not None
        and weather.cloud_cover >= 95
        and weather.wind_gusts_10m is not None
        and weather.wind_gusts_10m >= 35
    ):
        penalty += 0.02

    return -min(penalty, 0.18)


def calculate_weather_adjustment(
    weather: WeatherSnapshot,
    schedule: FlightScheduleSnapshot | None = None,
) -> float:
    """
    MVP-эвристика.
    Она не заменяет ML-модель, а даёт слабую поправку на ближнем горизонте.
    Потом заменим на обученную модель.
    """
    if not weather.available:
        return 0.0

    adjustment = 0.0
    explicit_fog_code = weather.weather_code is not None and int(weather.weather_code) in {45, 48}
    low_visibility = weather.visibility is not None and weather.visibility <= 1000
    extreme_fog_proxy = (
        weather.visibility is not None
        and weather.visibility <= 300
        and weather.wind_gusts_10m is not None
        and weather.wind_gusts_10m >= 35
        and weather.cloud_cover_low is not None
        and weather.cloud_cover_low >= 80
        and weather.fog_low_cloud_risk_level == "high"
    )
    severe_visibility_risk = explicit_fog_code or extreme_fog_proxy or (
        low_visibility and weather.fog_low_cloud_risk_level in {"medium", "high"}
    )
    late_schedule_window = is_weather_window_too_late_for_schedule(weather, schedule)
    compound_humidity_risk = has_compound_humidity_risk(weather)
    early_active_flight_window = (
        is_weather_window_before_active_flight(weather, schedule)
        and schedule_has_operational_disruption(schedule)
    )
    good_late_clearing_support = has_good_late_clearing_support(
        weather,
        late_schedule_window,
        early_active_flight_window,
    )
    direction = wind_sector(weather.wind_direction_10m)

    if weather.flight_window_available and not late_schedule_window and not early_active_flight_window:
        adjustment += 0.08
    elif weather.flight_window_available and (late_schedule_window or early_active_flight_window):
        adjustment -= 0.08
    else:
        adjustment -= 0.06

    if weather.wind_speed_10m is not None and weather.wind_speed_10m >= 30:
        adjustment -= 0.03

    if weather.wind_gusts_10m is not None:
        if weather.wind_gusts_10m >= 65:
            adjustment -= 0.05
        elif weather.wind_gusts_10m >= 55:
            adjustment -= 0.03

    if weather.relative_humidity_2m is not None and compound_humidity_risk:
        if weather.relative_humidity_2m >= 97:
            adjustment -= 0.06
        elif weather.relative_humidity_2m >= 92:
            adjustment -= 0.035

    if weather.cloud_cover is not None and weather.cloud_cover >= 95:
        adjustment -= 0.01

    if weather.cloud_cover_low is not None:
        if weather.cloud_cover_low >= 95:
            adjustment -= 0.02
        elif weather.cloud_cover_low >= 80:
            adjustment -= 0.01

    if weather.visibility is not None:
        if weather.visibility <= 300 and extreme_fog_proxy:
            adjustment -= 0.16
        elif weather.visibility <= 1000 and explicit_fog_code:
            adjustment -= 0.12
        elif weather.visibility <= 1000:
            adjustment -= 0.10
        elif weather.visibility <= 3000 and explicit_fog_code:
            adjustment -= 0.08
        elif weather.visibility <= 3000:
            adjustment -= 0.06
        elif weather.visibility <= 6000:
            adjustment -= 0.03

    if weather.dew_point_spread is not None:
        if weather.dew_point_spread <= 1:
            adjustment -= 0.04 if compound_humidity_risk else 0.015
        elif weather.dew_point_spread <= 2:
            adjustment -= 0.015

    if weather.fog_low_cloud_risk_level == "medium":
        adjustment -= 0.015
    elif weather.fog_low_cloud_risk_level == "high":
        adjustment -= 0.10 if severe_visibility_risk else 0.03

    if weather.precipitation is not None and weather.precipitation >= 3:
        adjustment -= 0.03

    if weather.pressure_msl is not None:
        if weather.pressure_msl <= 995:
            adjustment -= 0.07
        elif weather.pressure_msl <= 1000:
            adjustment -= 0.05
        elif weather.pressure_msl <= 1005:
            adjustment -= 0.035 if (compound_humidity_risk or severe_visibility_risk) else 0.01
        elif weather.pressure_msl > 1015 and not severe_visibility_risk:
            adjustment += 0.03
        elif weather.pressure_msl > 1010 and good_late_clearing_support:
            adjustment += 0.015

    if direction in {"E", "SE"}:
        if (weather.wind_speed_10m is not None and weather.wind_speed_10m >= 25) or (
            weather.wind_gusts_10m is not None and weather.wind_gusts_10m >= 65
        ):
            adjustment -= 0.05
        elif compound_humidity_risk or severe_visibility_risk:
            adjustment -= 0.02
    elif direction in {"SW", "W"} and not severe_visibility_risk:
        if (
            (weather.wind_gusts_10m is None or weather.wind_gusts_10m < 60)
            and (weather.precipitation is None or weather.precipitation <= 0.2)
        ):
            adjustment += 0.02

    if weather.flight_window_available and not severe_visibility_risk and not late_schedule_window and adjustment < 0.03:
        adjustment = 0.03
    if good_late_clearing_support and adjustment < 0.08:
        adjustment = 0.08

    return adjustment


def apply_schedule_guardrails(
    probability: float,
    schedule: FlightScheduleSnapshot | None,
) -> float:
    if schedule is None or not schedule.available:
        return probability

    if schedule.moved_next_day:
        return 0.0

    if schedule.completed_same_day:
        return max(probability, 0.85)

    return probability


def calculate_probability(
    horizon_days: int,
    weather: WeatherSnapshot,
    history: HistoricalSnapshot,
    schedule: FlightScheduleSnapshot | None = None,
) -> float:
    base = history.historical_probability_flight

    if history.decade_probability_flight is not None:
        base = 0.65 * history.historical_probability_flight + 0.35 * history.decade_probability_flight

    # Погодную поправку используем только там, где forecast реально есть.
    if horizon_days <= 15:
        base += calculate_weather_adjustment(weather, schedule=schedule)
    if horizon_days <= 1:
        base += calculate_operational_stress_adjustment(weather, schedule=schedule)

    base = apply_schedule_guardrails(base, schedule)
    lower_bound = 0.0 if schedule is not None and schedule.available and schedule.moved_next_day else 0.05

    return round(min(max(base, lower_bound), 0.95), 4)


def decision_threshold(horizon_days: int) -> float:
    """
    Продуктовый порог:
    - близкая дата требует более уверенного "Да";
    - дальняя дата может быть "Да", если она лучше исторического окна.
    """
    if horizon_days <= 2:
        return 0.58
    if horizon_days <= 10:
        return 0.55
    if horizon_days <= 46:
        return 0.45
    return 0.30


def make_decision(probability_flight: float, horizon_days: int, threshold: float | None = None) -> str:
    cutoff = decision_threshold(horizon_days) if threshold is None else threshold
    return "yes" if probability_flight >= cutoff else "no"


def _format_wind_ms(value_kmh: float) -> str:
    value_ms = round(value_kmh / 3.6, 1)
    if value_ms == round(value_ms):
        value_ms = int(value_ms)
    return f"{value_ms} м/с"


def _schedule_uses_arrival_facts(schedule: FlightScheduleSnapshot | None) -> bool:
    return bool(
        schedule is not None
        and schedule.flights
        and all((flight.direction or "").lower() == "arrival" for flight in schedule.flights)
    )


def _completed_progress_text(schedule: FlightScheduleSnapshot) -> str:
    if _schedule_uses_arrival_facts(schedule):
        verb = "прибыл" if schedule.completed_flights == 1 else "прибыло"
        return f"по табло {verb} {schedule.completed_flights} из {schedule.total_flights} рейсов"
    return f"по табло выполнено {schedule.completed_flights} из {schedule.total_flights} рейсов"


def get_factor_summary(
    weather: WeatherSnapshot,
    history: HistoricalSnapshot,
    horizon_days: int,
    schedule: FlightScheduleSnapshot | None = None,
) -> list[str]:
    factors: list[str] = []

    factors.append(
        f"историческая вероятность календарно близких дат: {round(history.historical_probability_flight * 100)}%"
    )

    if history.similar_days_count > 0:
        factors.append(
            f"в истории найдено календарно близких дней: {history.similar_days_count}"
        )

    if schedule is not None and schedule.available:
        if schedule.moved_next_day:
            if schedule.total_flights > 1 and schedule.completed_flights > 0:
                factors.append(
                    f"{_completed_progress_text(schedule)}, следующий рейс отменен или перенесен"
                )
            elif schedule.total_flights > 1 and schedule.unavailable_flights >= schedule.total_flights:
                factors.append("по последним строкам табло все рейсы на эту дату отменены или перенесены")
            elif schedule.total_flights > 1:
                factors.append("по последним строкам табло активный рейс на эту дату отменен или перенесен")
            else:
                factors.append("по последним строкам табло рейс на эту дату перенесен на следующую дату")
        elif schedule.completed_same_day:
            if schedule.total_flights > 1:
                if _schedule_uses_arrival_facts(schedule):
                    factors.append("по последним строкам табло сегодняшние рейсы уже прибыли")
                else:
                    factors.append("по последним строкам табло сегодняшние рейсы уже выполнены")
            else:
                if _schedule_uses_arrival_facts(schedule):
                    factors.append("по последним строкам табло рейс на эту дату уже прибыл")
                else:
                    factors.append("по последним строкам табло рейс на эту дату уже выполнялся")
        elif schedule.total_flights > 1 and schedule.completed_flights > 0:
            factors.append(
                f"{_completed_progress_text(schedule)}, прогноз относится к следующему рейсу"
            )
        elif schedule.first_departure_hour is not None and schedule.last_scheduled_hour is not None:
            schedule_label = "рейс" if _schedule_uses_arrival_facts(schedule) else "вылет"
            factors.append(
                "расписание табло на дату: "
                f"первый {schedule_label} около {schedule.first_departure_hour:02d}:00, "
                f"последний рейс около {schedule.last_scheduled_hour:02d}:00"
            )

    if horizon_days > 46:
        factors.append("дальний горизонт: точного прогноза погоды нет, уверенность ниже")
    elif weather.available:
        if weather.fog_low_cloud_risk_level:
            risk_labels = {"low": "низкий", "medium": "средний", "high": "высокий"}
            factors.append(
                f"риск тумана и низкой облачности: {risk_labels.get(weather.fog_low_cloud_risk_level, weather.fog_low_cloud_risk_level)}"
            )
        has_flight_window = (
            weather.flight_window_available
            and weather.flight_window_start_hour is not None
            and weather.flight_window_end_hour is not None
        )
        if has_flight_window:
            if is_weather_window_too_late_for_schedule(weather, schedule):
                factors.append("найденное погодное окно начинается позже основного времени вылета по табло")
            elif is_weather_window_before_active_flight(weather, schedule) and schedule_has_operational_disruption(schedule):
                factors.append("найденное погодное окно заканчивается раньше активного рейса по табло")
            has_aggregation_window = (
                weather.aggregation_window_start_hour is not None
                and weather.aggregation_window_end_hour is not None
            )
            if (
                has_aggregation_window
                and weather.flight_window_start_hour == weather.aggregation_window_start_hour
                and weather.flight_window_end_hour == weather.aggregation_window_end_hour
            ):
                factors.append("есть летное окно")
            else:
                factors.append(
                    "есть летное окно примерно "
                    f"с {weather.flight_window_start_hour:02d}:00 до {weather.flight_window_end_hour:02d}:00"
                )
        if weather.visibility is not None:
            if has_flight_window:
                visibility = weather.flight_window_visibility if weather.flight_window_visibility is not None else weather.visibility
                factors.append(
                    "видимость в найденном погодном окне "
                    f"{weather.flight_window_start_hour:02d}:00-{weather.flight_window_end_hour:02d}:00: "
                    f"{round(visibility)} м"
                )
            elif weather.aggregation_window_start_hour is not None and weather.aggregation_window_end_hour is not None:
                factors.append(
                    "видимость в рабочем окне "
                    f"{weather.aggregation_window_start_hour:02d}:00-{weather.aggregation_window_end_hour:02d}:00: "
                    f"{round(weather.visibility)} м"
                )
            else:
                factors.append(f"видимость по прогнозу: {round(weather.visibility)} м")
        if weather.cloud_cover_low is not None:
            cloud_cover_low = (
                weather.flight_window_cloud_cover_low
                if has_flight_window and weather.flight_window_cloud_cover_low is not None
                else weather.cloud_cover_low
            )
            factors.append(f"низкая облачность по прогнозу: {cloud_cover_low}%")
        if weather.dew_point_spread is not None:
            factors.append(f"разница температуры и точки росы: {weather.dew_point_spread} °C")
        if weather.wind_gusts_10m is not None:
            factors.append(f"порывы ветра по прогнозу: до {_format_wind_ms(weather.wind_gusts_10m)}")
        if weather.wind_direction_10m is not None:
            sector = wind_sector(weather.wind_direction_10m)
            factors.append(f"направление ветра по прогнозу: {sector or round(weather.wind_direction_10m)}")
        if weather.pressure_msl is not None:
            factors.append(f"давление по прогнозу: {round(weather.pressure_msl)} гПа")
        if weather.relative_humidity_2m is not None:
            factors.append(f"средняя влажность по прогнозу: {weather.relative_humidity_2m}%")
            if has_compound_humidity_risk(weather):
                factors.append("высокая влажность совпадает с другими погодными рисками")
        if weather.cloud_cover is not None:
            factors.append(f"средняя облачность по прогнозу: {weather.cloud_cover}%")
    else:
        factors.append("погодный прогноз для даты недоступен")

    return factors
