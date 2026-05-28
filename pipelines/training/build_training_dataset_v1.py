import argparse
import math
import sys
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
for candidate in (PROJECT_ROOT, PROJECT_ROOT / "backend"):
    if (candidate / "app").exists():
        sys.path.insert(0, str(candidate))
        break

from app.services.fog_risk import (
    calculate_dew_point_spread,
    calculate_fog_low_cloud_risk_score,
    fog_low_cloud_risk_level,
)


OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DATA_VERSION = "training-v2-openmeteo-fog-risk-2026-05-28"

LOCATIONS = {
    "mendeleyevo": {
        "latitude": 43.958,
        "longitude": 145.683,
        "timezone": "Asia/Sakhalin",
    },
    "khomutovo": {
        "latitude": 46.888,
        "longitude": 142.718,
        "timezone": "Asia/Sakhalin",
    },
}

HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "pressure_msl",
    "cloud_cover",
    "cloud_cover_low",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "visibility",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build training_dataset_v1 from flight targets, Open-Meteo archive weather, and leakage-safe rolling features."
    )
    parser.add_argument(
        "--target",
        default="data/processed/dataset_daily_flights_v3.csv",
    )
    parser.add_argument(
        "--weather-cache",
        default="data/interim/weather/open_meteo_daily_weather_v1.csv",
    )
    parser.add_argument(
        "--output",
        default="data/processed/training_dataset_v1.csv",
    )
    parser.add_argument(
        "--summary",
        default="data/interim/training/training_dataset_v1_summary.txt",
    )
    parser.add_argument("--refresh-weather", action="store_true")
    return parser.parse_args()


def read_target(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = [str(column).lstrip("\ufeff;").strip().strip(";") for column in df.columns]
    df = df.loc[:, [column for column in df.columns if column and not column.startswith("Unnamed")]]
    if "date" not in df.columns:
        raise ValueError(f"Target dataset has no date column after CSV cleanup: {list(df.columns)}")

    for column in df.select_dtypes(include="object").columns:
        df[column] = df[column].str.strip().str.strip(";")

    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df = df.sort_values("date").reset_index(drop=True)
    return df


def load_weather_cache(path: Path, refresh: bool) -> pd.DataFrame:
    if refresh or not path.exists():
        return pd.DataFrame()
    cache = pd.read_csv(path, encoding="utf-8-sig")
    if "date" in cache.columns:
        cache["date"] = pd.to_datetime(cache["date"]).dt.date.astype(str)
    return cache


def fetch_hourly_archive(
    location_name: str,
    location: dict,
    start_date: str,
    end_date: str,
    hourly_fields: list[str],
) -> pd.DataFrame:
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "timezone": location["timezone"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(hourly_fields),
    }

    with httpx.Client(timeout=60) as client:
        response = client.get(OPEN_METEO_ARCHIVE_URL, params=params)

    if response.status_code >= 400 and "visibility" in hourly_fields:
        fallback_fields = [field for field in hourly_fields if field != "visibility"]
        return fetch_hourly_archive(location_name, location, start_date, end_date, fallback_fields)

    response.raise_for_status()
    payload = response.json()
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return pd.DataFrame()

    df = pd.DataFrame({"time": times})
    for field in hourly_fields:
        values = hourly.get(field)
        if values is not None:
            df[field] = values

    df["date"] = pd.to_datetime(df["time"]).dt.date.astype(str)
    df["location"] = location_name
    return df


def circular_wind_direction(values: pd.Series, weights: pd.Series) -> float | None:
    clean = pd.DataFrame({"direction": values, "weight": weights}).dropna()
    if clean.empty:
        return None

    radians = clean["direction"].astype(float).map(math.radians)
    wind_weights = clean["weight"].fillna(1).astype(float).clip(lower=0)
    if wind_weights.sum() == 0:
        wind_weights = pd.Series([1.0] * len(clean), index=clean.index)

    sin_sum = (radians.map(math.sin) * wind_weights).sum()
    cos_sum = (radians.map(math.cos) * wind_weights).sum()
    if sin_sum == 0 and cos_sum == 0:
        return None
    return round((math.degrees(math.atan2(sin_sum, cos_sum)) + 360) % 360, 2)


def mode_or_none(values: pd.Series) -> float | None:
    clean = values.dropna()
    if clean.empty:
        return None
    return float(clean.mode().iloc[0])


def aggregate_daily_weather(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly.empty:
        return pd.DataFrame()

    rows: list[dict] = []
    for (location, day), group in hourly.groupby(["location", "date"], sort=True):
        row = {"date": day, "location": location}

        def mean(field: str) -> float | None:
            return round(float(group[field].mean()), 4) if field in group and group[field].notna().any() else None

        def minimum(field: str) -> float | None:
            return round(float(group[field].min()), 4) if field in group and group[field].notna().any() else None

        def maximum(field: str) -> float | None:
            return round(float(group[field].max()), 4) if field in group and group[field].notna().any() else None

        def total(field: str) -> float | None:
            return round(float(group[field].sum()), 4) if field in group and group[field].notna().any() else None

        row.update(
            {
                "temperature_2m_mean": mean("temperature_2m"),
                "temperature_2m_min": minimum("temperature_2m"),
                "temperature_2m_max": maximum("temperature_2m"),
                "relative_humidity_2m_mean": mean("relative_humidity_2m"),
                "relative_humidity_2m_max": maximum("relative_humidity_2m"),
                "dew_point_2m_mean": mean("dew_point_2m"),
                "pressure_msl_mean": mean("pressure_msl"),
                "pressure_msl_min": minimum("pressure_msl"),
                "cloud_cover_mean": mean("cloud_cover"),
                "cloud_cover_max": maximum("cloud_cover"),
                "cloud_cover_low_mean": mean("cloud_cover_low"),
                "cloud_cover_low_max": maximum("cloud_cover_low"),
                "precipitation_sum": total("precipitation"),
                "rain_sum": total("rain"),
                "snowfall_sum": total("snowfall"),
                "weather_code_mode": mode_or_none(group["weather_code"]) if "weather_code" in group else None,
                "wind_speed_10m_mean": mean("wind_speed_10m"),
                "wind_speed_10m_max": maximum("wind_speed_10m"),
                "wind_gusts_10m_max": maximum("wind_gusts_10m"),
                "wind_direction_10m_dominant": circular_wind_direction(
                    group["wind_direction_10m"],
                    group.get("wind_speed_10m", pd.Series([1] * len(group))),
                )
                if "wind_direction_10m" in group
                else None,
                "visibility_mean": mean("visibility"),
                "visibility_min": minimum("visibility"),
            }
        )

        row["humidity_ge_92_flag"] = int((row["relative_humidity_2m_mean"] or 0) >= 92)
        row["cloud_ge_85_flag"] = int((row["cloud_cover_mean"] or 0) >= 85)
        row["cloud_low_ge_70_flag"] = int((row["cloud_cover_low_mean"] or 0) >= 70)
        row["cloud_low_ge_90_flag"] = int((row["cloud_cover_low_mean"] or 0) >= 90)
        row["wind_speed_ge_12_flag"] = int((row["wind_speed_10m_max"] or 0) >= 12)
        row["wind_gust_ge_18_flag"] = int((row["wind_gusts_10m_max"] or 0) >= 18)
        row["wind_gust_ge_50_flag"] = int((row["wind_gusts_10m_max"] or 0) >= 50)
        row["precipitation_flag"] = int((row["precipitation_sum"] or 0) > 0)
        row["snow_flag"] = int((row["snowfall_sum"] or 0) > 0)
        row["low_pressure_flag"] = int((row["pressure_msl_mean"] or 9999) <= 1000)
        row["dew_point_spread_mean"] = calculate_dew_point_spread(
            row["temperature_2m_mean"],
            row["dew_point_2m_mean"],
        )
        row["dew_point_spread_le_2_flag"] = int(
            row["dew_point_spread_mean"] is not None and row["dew_point_spread_mean"] <= 2
        )

        low_visibility = row["visibility_min"] is not None and row["visibility_min"] <= 1000
        proxy_fog_conditions = (
            (row["relative_humidity_2m_mean"] or 0) >= 92
            and ((row["cloud_cover_low_mean"] or 0) >= 70 or (row["cloud_cover_mean"] or 0) >= 85)
            and (row["wind_gusts_10m_max"] or 0) <= 35
        )
        row["fog_risk_proxy_flag"] = int(low_visibility or proxy_fog_conditions)
        row["fog_low_cloud_risk_score"] = calculate_fog_low_cloud_risk_score(
            visibility=row["visibility_min"],
            cloud_cover_low=row["cloud_cover_low_mean"],
            relative_humidity_2m=row["relative_humidity_2m_mean"],
            dew_point_spread=row["dew_point_spread_mean"],
            wind_speed_10m=row["wind_speed_10m_mean"],
            wind_gusts_10m=row["wind_gusts_10m_max"],
            precipitation=row["precipitation_sum"],
            weather_code=row["weather_code_mode"],
        )
        row["fog_low_cloud_risk_level"] = fog_low_cloud_risk_level(row["fog_low_cloud_risk_score"])

        rows.append(row)

    return pd.DataFrame(rows)


def ensure_weather_cache(target: pd.DataFrame, cache_path: Path, refresh: bool) -> pd.DataFrame:
    cache = load_weather_cache(cache_path, refresh=refresh)
    target_dates = set(target["date"].astype(str))

    if not cache.empty:
        cache_dates = set(cache["date"].astype(str))
        cache_locations = set(cache["location"].astype(str))
    else:
        cache_dates = set()
        cache_locations = set()

    frames = [cache] if not cache.empty else []
    for location_name, location in LOCATIONS.items():
        missing_dates = sorted(target_dates - cache_dates) if location_name in cache_locations else sorted(target_dates)
        if not missing_dates:
            continue

        hourly = fetch_hourly_archive(
            location_name=location_name,
            location=location,
            start_date=missing_dates[0],
            end_date=missing_dates[-1],
            hourly_fields=HOURLY_FIELDS,
        )
        daily = aggregate_daily_weather(hourly)
        if not daily.empty:
            frames.append(daily[daily["date"].isin(missing_dates)].copy())

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "location"], keep="last")
    combined = combined.sort_values(["location", "date"]).reset_index(drop=True)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return combined


def pivot_weather(weather: pd.DataFrame) -> pd.DataFrame:
    if weather.empty:
        return pd.DataFrame({"date": []})

    frames = []
    for location_name in sorted(weather["location"].unique()):
        subset = weather[weather["location"] == location_name].drop(columns=["location"]).copy()
        rename = {column: f"{location_name}_{column}" for column in subset.columns if column != "date"}
        frames.append(subset.rename(columns=rename))

    result = frames[0]
    for frame in frames[1:]:
        result = result.merge(frame, on="date", how="outer")

    return result.sort_values("date").reset_index(drop=True)


def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    ts = pd.to_datetime(result["date"])
    result["day_of_week"] = ts.dt.dayofweek
    result["is_weekend"] = result["day_of_week"].isin([5, 6]).astype(int)
    result["day_of_year_sin"] = (2 * math.pi * result["day_of_year"] / 366).map(math.sin).round(6)
    result["day_of_year_cos"] = (2 * math.pi * result["day_of_year"] / 366).map(math.cos).round(6)
    result["month_sin"] = (2 * math.pi * result["month"] / 12).map(math.sin).round(6)
    result["month_cos"] = (2 * math.pi * result["month"] / 12).map(math.cos).round(6)
    return result


def streak_before(values: list[int], target: int) -> int:
    count = 0
    for value in reversed(values):
        if value == target:
            count += 1
        else:
            break
    return count


def add_rolling_features(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy().sort_values("date").reset_index(drop=True)
    completed = result["is_flight_completed"].astype(int)
    dates = pd.to_datetime(result["date"])

    result["prev_1_completed"] = completed.shift(1)
    result["prev_3_cancelled_count"] = (1 - completed).shift(1).rolling(3, min_periods=1).sum()
    result["prev_7_cancelled_count"] = (1 - completed).shift(1).rolling(7, min_periods=1).sum()
    result["prev_14_completed_rate"] = completed.shift(1).rolling(14, min_periods=3).mean()
    result["prev_30_completed_rate"] = completed.shift(1).rolling(30, min_periods=5).mean()

    same_month_rates: list[float | None] = []
    same_decade_rates: list[float | None] = []
    days_since_cancelled: list[int | None] = []
    cancelled_streaks: list[int] = []
    completed_streaks: list[int] = []
    past_values: list[int] = []
    past_dates: list[pd.Timestamp] = []

    for index, row in result.iterrows():
        current_date = dates.iloc[index]
        month = int(row["month"])
        month_decade = int(row["month_decade"])

        month_values = [
            value for value, past_date in zip(past_values, past_dates)
            if past_date.month == month
        ]
        decade_values = [
            value for value, past_date in zip(past_values, past_dates)
            if past_date.month == month and ((past_date.day - 1) // 10 + 1) == month_decade
        ]

        same_month_rates.append(round(sum(month_values) / len(month_values), 6) if month_values else None)
        same_decade_rates.append(round(sum(decade_values) / len(decade_values), 6) if decade_values else None)

        cancelled_dates = [
            past_date for value, past_date in zip(past_values, past_dates)
            if value == 0
        ]
        days_since_cancelled.append(
            int((current_date - cancelled_dates[-1]).days) if cancelled_dates else None
        )

        cancelled_streaks.append(streak_before(past_values, target=0))
        completed_streaks.append(streak_before(past_values, target=1))

        past_values.append(int(row["is_flight_completed"]))
        past_dates.append(current_date)

    result["same_month_past_completed_rate"] = same_month_rates
    result["same_decade_past_completed_rate"] = same_decade_rates
    result["days_since_last_cancelled"] = days_since_cancelled
    result["cancelled_streak_before"] = cancelled_streaks
    result["completed_streak_before"] = completed_streaks
    return result


def build_training_dataset(target: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
    weather_wide = pivot_weather(weather)
    result = target.merge(weather_wide, on="date", how="left")
    result = add_calendar_features(result)
    result = add_rolling_features(result)
    result["training_data_version"] = DATA_VERSION
    return result


def write_summary(path: Path, target: pd.DataFrame, weather: pd.DataFrame, training: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    weather_counts = (
        weather.groupby("location")["date"].nunique().to_dict()
        if not weather.empty
        else {}
    )
    missing_by_location = {}
    for location_name in LOCATIONS:
        expected = set(target["date"].astype(str))
        available = set(weather[weather["location"] == location_name]["date"].astype(str)) if not weather.empty else set()
        missing_by_location[location_name] = len(expected - available)

    weather_columns = [
        column for column in training.columns
        if column.startswith("mendeleyevo_") or column.startswith("khomutovo_")
    ]
    lines = [
        f"generated_at={datetime.now().isoformat(timespec='seconds')}",
        f"data_version={DATA_VERSION}",
        "",
        f"target_rows={len(target)}",
        f"target_date_min={target['date'].min()}",
        f"target_date_max={target['date'].max()}",
        f"target_status_counts={target['status'].value_counts(dropna=False).to_dict()}",
        "",
        f"weather_cache_rows={len(weather)}",
        f"weather_coverage_by_location={weather_counts}",
        f"weather_missing_dates_by_location={missing_by_location}",
        "",
        f"training_rows={len(training)}",
        f"training_columns={len(training.columns)}",
        f"weather_feature_columns={len(weather_columns)}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    target_path = Path(args.target)
    weather_cache_path = Path(args.weather_cache)
    output_path = Path(args.output)
    summary_path = Path(args.summary)

    target = read_target(target_path)
    weather = ensure_weather_cache(target, weather_cache_path, refresh=args.refresh_weather)
    training = build_training_dataset(target, weather)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    training.to_csv(output_path, index=False, encoding="utf-8-sig")
    write_summary(summary_path, target, weather, training)

    print(f"Target rows:        {len(target)}")
    print(f"Weather cache:      {weather_cache_path} ({len(weather)} rows)")
    print(f"Training dataset:   {output_path} ({len(training)} rows, {len(training.columns)} columns)")
    print(f"Summary:            {summary_path}")


if __name__ == "__main__":
    main()
