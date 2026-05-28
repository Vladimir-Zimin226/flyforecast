from __future__ import annotations


FOG_WEATHER_CODES = {45, 48}
LOW_VISIBILITY_METERS = 1000
REDUCED_VISIBILITY_METERS = 3000


def calculate_dew_point_spread(temperature_2m: float | None, dew_point_2m: float | None) -> float | None:
    if temperature_2m is None or dew_point_2m is None:
        return None
    return round(temperature_2m - dew_point_2m, 2)


def calculate_fog_low_cloud_risk_score(
    *,
    visibility: float | None,
    cloud_cover_low: float | None,
    relative_humidity_2m: float | None,
    dew_point_spread: float | None,
    wind_speed_10m: float | None,
    wind_gusts_10m: float | None,
    precipitation: float | None,
    weather_code: float | None,
) -> float:
    score = 0.0

    if visibility is not None:
        if visibility <= LOW_VISIBILITY_METERS:
            score += 0.45
        elif visibility <= REDUCED_VISIBILITY_METERS:
            score += 0.28
        elif visibility <= 6000:
            score += 0.12

    if cloud_cover_low is not None:
        if cloud_cover_low >= 90:
            score += 0.22
        elif cloud_cover_low >= 70:
            score += 0.14
        elif cloud_cover_low >= 50:
            score += 0.08

    if relative_humidity_2m is not None:
        if relative_humidity_2m >= 97:
            score += 0.18
        elif relative_humidity_2m >= 92:
            score += 0.12
        elif relative_humidity_2m >= 88:
            score += 0.06

    if dew_point_spread is not None:
        if dew_point_spread <= 1.0:
            score += 0.18
        elif dew_point_spread <= 2.0:
            score += 0.12
        elif dew_point_spread <= 3.0:
            score += 0.06

    if wind_speed_10m is not None and wind_speed_10m <= 6:
        score += 0.05

    if wind_gusts_10m is not None and wind_gusts_10m >= 50:
        score += 0.08

    if precipitation is not None and precipitation >= 3:
        score += 0.06

    if weather_code is not None and int(weather_code) in FOG_WEATHER_CODES:
        score += 0.25

    return round(min(max(score, 0.0), 1.0), 3)


def fog_low_cloud_risk_level(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 0.65:
        return "high"
    if score >= 0.35:
        return "medium"
    return "low"
