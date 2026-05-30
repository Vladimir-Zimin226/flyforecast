from datetime import date, datetime

from app.schemas import HistoricalSnapshot, WeatherSnapshot


MODEL_VERSION = "mvp-baseline-003"
DATA_VERSION = "telegram-v2-plus-historical-board-manual-v3-2026-05-20"

DISCLAIMER = (
    "Это вероятностная оценка, а не гарантия выполнения рейса. "
    "Сервис не является официальным источником статуса рейса. "
    "Проверяйте официальные источники перед поездкой."
)


def get_horizon_days(target_date: date) -> int:
    return (target_date - datetime.now().date()).days


def get_confidence(horizon_days: int, weather: WeatherSnapshot, history: HistoricalSnapshot) -> str:
    if horizon_days <= 10 and weather.available and history.similar_days_count >= 20:
        return "medium"
    if horizon_days <= 46 and history.similar_days_count >= 20:
        return "medium"
    return "low"


def calculate_weather_adjustment(weather: WeatherSnapshot) -> float:
    """
    MVP-эвристика.
    Она не заменяет ML-модель, а даёт слабую поправку на ближнем горизонте.
    Потом заменим на обученную модель.
    """
    if not weather.available:
        return 0.0

    adjustment = 0.0

    if weather.wind_speed_10m is not None and weather.wind_speed_10m >= 12:
        adjustment -= 0.05

    if weather.wind_gusts_10m is not None and weather.wind_gusts_10m >= 18:
        adjustment -= 0.07

    if weather.relative_humidity_2m is not None and weather.relative_humidity_2m >= 92:
        adjustment -= 0.04

    if weather.cloud_cover is not None and weather.cloud_cover >= 85:
        adjustment -= 0.03

    if weather.cloud_cover_low is not None and weather.cloud_cover_low >= 80:
        adjustment -= 0.05

    if weather.visibility is not None and weather.visibility <= 3000:
        adjustment -= 0.06

    if weather.dew_point_spread is not None and weather.dew_point_spread <= 2:
        adjustment -= 0.04

    if weather.fog_low_cloud_risk_level == "medium":
        adjustment -= 0.04
    elif weather.fog_low_cloud_risk_level == "high":
        adjustment -= 0.09

    if weather.precipitation is not None and weather.precipitation >= 3:
        adjustment -= 0.03

    return adjustment


def calculate_probability(
    horizon_days: int,
    weather: WeatherSnapshot,
    history: HistoricalSnapshot,
) -> float:
    base = history.historical_probability_flight

    if history.decade_probability_flight is not None:
        base = 0.65 * history.historical_probability_flight + 0.35 * history.decade_probability_flight

    # Погодную поправку используем только там, где forecast реально есть.
    if horizon_days <= 15:
        base += calculate_weather_adjustment(weather)

    return round(min(max(base, 0.05), 0.95), 4)


def decision_threshold(horizon_days: int) -> float:
    """
    Продуктовый порог:
    - близкая дата требует более уверенного "Да";
    - дальняя дата может быть "Да", если она лучше исторического окна.
    """
    if horizon_days <= 10:
        return 0.55
    if horizon_days <= 46:
        return 0.45
    return 0.30


def make_decision(probability_flight: float, horizon_days: int) -> str:
    return "yes" if probability_flight >= decision_threshold(horizon_days) else "no"


def get_factor_summary(weather: WeatherSnapshot, history: HistoricalSnapshot, horizon_days: int) -> list[str]:
    factors: list[str] = []

    factors.append(
        f"историческая вероятность похожих дат: {round(history.historical_probability_flight * 100)}%"
    )

    if history.similar_days_count > 0:
        factors.append(
            f"в истории найдено похожих дней: {history.similar_days_count}"
        )

    if horizon_days > 46:
        factors.append("дальний горизонт: точного прогноза погоды нет, уверенность ниже")
    elif weather.available:
        if weather.fog_low_cloud_risk_level:
            risk_labels = {"low": "низкий", "medium": "средний", "high": "высокий"}
            factors.append(
                f"риск тумана и низкой облачности: {risk_labels.get(weather.fog_low_cloud_risk_level, weather.fog_low_cloud_risk_level)}"
            )
        if weather.visibility is not None:
            if weather.aggregation_window_start_hour is not None and weather.aggregation_window_end_hour is not None:
                factors.append(
                    "видимость в рабочем окне "
                    f"{weather.aggregation_window_start_hour:02d}:00-{weather.aggregation_window_end_hour:02d}:00: "
                    f"{round(weather.visibility)} м"
                )
            else:
                factors.append(f"видимость по прогнозу: {round(weather.visibility)} м")
        if weather.cloud_cover_low is not None:
            factors.append(f"низкая облачность по прогнозу: {weather.cloud_cover_low}%")
        if weather.dew_point_spread is not None:
            factors.append(f"разница температуры и точки росы: {weather.dew_point_spread} °C")
        if weather.wind_gusts_10m is not None:
            factors.append(f"средние порывы ветра по прогнозу: {weather.wind_gusts_10m} км/ч")
        if weather.relative_humidity_2m is not None:
            factors.append(f"средняя влажность по прогнозу: {weather.relative_humidity_2m}%")
        if weather.cloud_cover is not None:
            factors.append(f"средняя облачность по прогнозу: {weather.cloud_cover}%")
    else:
        factors.append("погодный прогноз для даты недоступен")

    return factors
