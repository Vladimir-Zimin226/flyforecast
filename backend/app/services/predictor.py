from datetime import date, datetime

from app.schemas import FlightScheduleSnapshot, HistoricalSnapshot, WeatherSnapshot


MODEL_VERSION = "mvp-baseline-009"
DATA_VERSION = "telegram-v2-plus-historical-board-manual-v3-2026-05-20"

DISCLAIMER = (
    "Это вероятностная оценка, а не гарантия выполнения рейса. "
    "Сервис не является официальным источником статуса рейса. "
    "Проверяйте официальные источники перед поездкой."
)


def get_horizon_days(target_date: date) -> int:
    return (target_date - datetime.now().date()).days


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


def has_good_late_clearing_support(weather: WeatherSnapshot, late_schedule_window: bool) -> bool:
    if not weather.flight_window_available or late_schedule_window:
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
    good_late_clearing_support = has_good_late_clearing_support(weather, late_schedule_window)
    direction = wind_sector(weather.wind_direction_10m)

    if weather.flight_window_available and not late_schedule_window:
        adjustment += 0.08
    elif weather.flight_window_available and late_schedule_window:
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


def make_decision(probability_flight: float, horizon_days: int) -> str:
    return "yes" if probability_flight >= decision_threshold(horizon_days) else "no"


def _format_wind_ms(value_kmh: float) -> str:
    value_ms = round(value_kmh / 3.6, 1)
    if value_ms == round(value_ms):
        value_ms = int(value_ms)
    return f"{value_ms} м/с"


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
                    f"по табло выполнено {schedule.completed_flights} из {schedule.total_flights} рейсов, "
                    "следующий рейс отменен или перенесен"
                )
            elif schedule.total_flights > 1 and schedule.unavailable_flights >= schedule.total_flights:
                factors.append("по последним строкам табло все рейсы на эту дату отменены или перенесены")
            elif schedule.total_flights > 1:
                factors.append("по последним строкам табло активный рейс на эту дату отменен или перенесен")
            else:
                factors.append("по последним строкам табло рейс на эту дату перенесен на следующую дату")
        elif schedule.completed_same_day:
            if schedule.total_flights > 1:
                factors.append("по последним строкам табло сегодняшние рейсы уже выполнены")
            else:
                factors.append("по последним строкам табло рейс на эту дату уже выполнялся")
        elif schedule.total_flights > 1 and schedule.completed_flights > 0:
            factors.append(
                f"по табло выполнено {schedule.completed_flights} из {schedule.total_flights} рейсов, "
                "прогноз относится к следующему рейсу"
            )
        elif schedule.first_departure_hour is not None and schedule.last_scheduled_hour is not None:
            factors.append(
                "расписание табло на дату: "
                f"первый вылет около {schedule.first_departure_hour:02d}:00, "
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
