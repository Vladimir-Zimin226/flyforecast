import argparse
import sys
from datetime import date, datetime, timedelta
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
OPEN_METEO_HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
MENDELEYEVO = {
    "latitude": 43.958,
    "longitude": 145.683,
    "timezone": "Asia/Sakhalin",
}
HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "cloud_cover",
    "cloud_cover_low",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
]
HISTORICAL_FORECAST_FIELDS = [
    "visibility",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Mendeleyevo fog/low-cloud risk dataset from Open-Meteo archive and known flight outcomes."
    )
    parser.add_argument("--target", default="data/processed/dataset_daily_flights_v3.csv")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--cache", default="data/interim/weather/mendeleyevo_open_meteo_archive_fog_risk.csv")
    parser.add_argument("--output", default="data/processed/mendeleyevo_fog_risk_dataset.csv")
    parser.add_argument("--summary", default="data/interim/weather/mendeleyevo_fog_risk_summary.md")
    parser.add_argument("--refresh-cache", action="store_true")
    return parser.parse_args()


def read_target(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame({"date": []})

    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = [str(column).lstrip("\ufeff;").strip().strip(";") for column in df.columns]
    df = df.loc[:, [column for column in df.columns if column and not column.startswith("Unnamed")]]
    if "date" not in df.columns:
        return pd.DataFrame({"date": []})

    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    keep_columns = [
        column
        for column in (
            "date",
            "status",
            "is_flight_completed",
            "label_confidence",
            "reason_class",
            "source",
        )
        if column in df.columns
    ]
    result = df[keep_columns].drop_duplicates(subset=["date"], keep="last")
    if "is_flight_completed" in result.columns:
        result["is_flight_completed"] = pd.to_numeric(result["is_flight_completed"], errors="coerce")
    return result


def default_start_date(target: pd.DataFrame) -> str:
    if not target.empty and target["date"].notna().any():
        return str(target["date"].min())
    return (date.today() - timedelta(days=365 * 5)).isoformat()


def default_end_date() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def month_starts(start_date: str, end_date: str) -> list[tuple[str, str]]:
    start = pd.Timestamp(start_date).date().replace(day=1)
    end = pd.Timestamp(end_date).date()
    ranges: list[tuple[str, str]] = []
    current = start
    while current <= end:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        chunk_start = max(current, pd.Timestamp(start_date).date())
        chunk_end = min(next_month - timedelta(days=1), end)
        ranges.append((chunk_start.isoformat(), chunk_end.isoformat()))
        current = next_month
    return ranges


def fetch_hourly_chunk(start_date: str, end_date: str) -> pd.DataFrame:
    params = {
        "latitude": MENDELEYEVO["latitude"],
        "longitude": MENDELEYEVO["longitude"],
        "timezone": MENDELEYEVO["timezone"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_FIELDS),
    }
    with httpx.Client(timeout=60) as client:
        response = client.get(OPEN_METEO_ARCHIVE_URL, params=params)
    response.raise_for_status()
    payload = response.json()
    archive = hourly_payload_to_frame(payload, HOURLY_FIELDS)

    forecast_params = {
        "latitude": MENDELEYEVO["latitude"],
        "longitude": MENDELEYEVO["longitude"],
        "timezone": MENDELEYEVO["timezone"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HISTORICAL_FORECAST_FIELDS),
    }
    with httpx.Client(timeout=60) as client:
        response = client.get(OPEN_METEO_HISTORICAL_FORECAST_URL, params=forecast_params)
    response.raise_for_status()
    historical_forecast = hourly_payload_to_frame(response.json(), HISTORICAL_FORECAST_FIELDS)

    return merge_hourly_sources(archive, historical_forecast)


def hourly_payload_to_frame(payload: dict, hourly_fields: list[str]) -> pd.DataFrame:
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
    return df


def merge_hourly_sources(archive: pd.DataFrame, historical_forecast: pd.DataFrame) -> pd.DataFrame:
    if archive.empty:
        return historical_forecast
    if historical_forecast.empty:
        return archive

    merge_columns = ["time", "date"]
    forecast_columns = [column for column in historical_forecast.columns if column not in merge_columns]
    merged = archive.merge(
        historical_forecast[merge_columns + forecast_columns],
        on=merge_columns,
        how="left",
        suffixes=("", "_historical_forecast"),
    )

    for column in forecast_columns:
        forecast_column = f"{column}_historical_forecast"
        if forecast_column in merged.columns:
            if column in merged.columns:
                merged[column] = merged[column].combine_first(merged[forecast_column])
                merged = merged.drop(columns=[forecast_column])
            else:
                merged[column] = merged[forecast_column]
                merged = merged.drop(columns=[forecast_column])

    return merged


def load_or_fetch_weather(cache_path: Path, start_date: str, end_date: str, refresh: bool) -> pd.DataFrame:
    if cache_path.exists() and not refresh:
        cache = pd.read_csv(cache_path, encoding="utf-8-sig")
        if "date" in cache.columns:
            cache["date"] = pd.to_datetime(cache["date"]).dt.date.astype(str)
        dates = {value.date().isoformat() for value in pd.date_range(start_date, end_date)}
        cached_dates = set(cache["date"].astype(str)) if "date" in cache.columns else set()
        if dates.issubset(cached_dates):
            return cache[cache["date"].isin(dates)].copy()

    frames = [
        frame.dropna(axis=1, how="all")
        for frame in (fetch_hourly_chunk(start, end) for start, end in month_starts(start_date, end_date))
        if not frame.empty
    ]
    hourly = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    daily = aggregate_daily_weather(hourly)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return daily


def mode_or_none(values: pd.Series) -> float | None:
    clean = values.dropna()
    if clean.empty:
        return None
    return float(clean.mode().iloc[0])


def aggregate_daily_weather(hourly: pd.DataFrame) -> pd.DataFrame:
    if hourly.empty:
        return pd.DataFrame({"date": []})

    rows: list[dict] = []
    for day, group in hourly.groupby("date", sort=True):
        def mean(field: str) -> float | None:
            return round(float(group[field].mean()), 4) if field in group and group[field].notna().any() else None

        def minimum(field: str) -> float | None:
            return round(float(group[field].min()), 4) if field in group and group[field].notna().any() else None

        def maximum(field: str) -> float | None:
            return round(float(group[field].max()), 4) if field in group and group[field].notna().any() else None

        def total(field: str) -> float | None:
            return round(float(group[field].sum()), 4) if field in group and group[field].notna().any() else None

        row = {
            "date": day,
            "temperature_2m_mean": mean("temperature_2m"),
            "temperature_2m_min": minimum("temperature_2m"),
            "temperature_2m_max": maximum("temperature_2m"),
            "relative_humidity_2m_mean": mean("relative_humidity_2m"),
            "relative_humidity_2m_max": maximum("relative_humidity_2m"),
            "dew_point_2m_mean": mean("dew_point_2m"),
            "cloud_cover_mean": mean("cloud_cover"),
            "cloud_cover_max": maximum("cloud_cover"),
            "cloud_cover_low_mean": mean("cloud_cover_low"),
            "cloud_cover_low_max": maximum("cloud_cover_low"),
            "precipitation_sum": total("precipitation"),
            "weather_code_mode": mode_or_none(group["weather_code"]) if "weather_code" in group else None,
            "wind_speed_10m_mean": mean("wind_speed_10m"),
            "wind_speed_10m_max": maximum("wind_speed_10m"),
            "wind_gusts_10m_max": maximum("wind_gusts_10m"),
            "visibility_mean": mean("visibility"),
            "visibility_min": minimum("visibility"),
        }
        row["dew_point_spread_mean"] = calculate_dew_point_spread(
            row["temperature_2m_mean"],
            row["dew_point_2m_mean"],
        )
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
        row["fog_risk_proxy_flag"] = int(
            (row["visibility_min"] is not None and row["visibility_min"] <= 1000)
            or (
                (row["relative_humidity_2m_mean"] or 0) >= 92
                and (row["cloud_cover_low_mean"] or 0) >= 70
                and (row["dew_point_spread_mean"] is not None and row["dew_point_spread_mean"] <= 2)
            )
        )
        rows.append(row)

    return pd.DataFrame(rows)


def write_summary(path: Path, dataset: pd.DataFrame, start_date: str, end_date: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Mendeleyevo fog-risk dataset",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Date range: {start_date}..{end_date}",
        f"- Rows: {len(dataset)}",
        f"- Visibility non-empty rows: {int(dataset['visibility_min'].notna().sum()) if 'visibility_min' in dataset else 0}",
        "",
        "## Risk levels",
        "",
        dataset["fog_low_cloud_risk_level"].value_counts(dropna=False).to_string(),
    ]

    if "is_flight_completed" in dataset.columns:
        labelled = dataset[dataset["is_flight_completed"].notna()].copy()
        if not labelled.empty:
            by_risk = (
                labelled.groupby("fog_low_cloud_risk_level", dropna=False)["is_flight_completed"]
                .agg(["count", "mean"])
                .rename(columns={"count": "days", "mean": "completion_rate"})
                .reset_index()
            )
            by_risk["completion_rate"] = by_risk["completion_rate"].round(4)
            lines.extend(["", "## Flight completion by fog-risk level", "", by_risk.to_string(index=False)])

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    target = read_target(Path(args.target))
    start_date = args.start_date or default_start_date(target)
    end_date = args.end_date or default_end_date()
    if pd.Timestamp(start_date) > pd.Timestamp(end_date):
        raise ValueError(f"start-date must be before end-date: {start_date} > {end_date}")

    weather = load_or_fetch_weather(Path(args.cache), start_date, end_date, args.refresh_cache)
    dataset = weather.merge(target, on="date", how="left") if not target.empty else weather
    dataset["data_version"] = "mendeleyevo-fog-risk-openmeteo-historical-forecast-visibility-2026-05-28"
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(output_path, index=False, encoding="utf-8-sig")
    write_summary(Path(args.summary), dataset, start_date, end_date)

    print(f"Fog-risk dataset: {output_path} ({len(dataset)} rows)")
    print(f"Summary:          {args.summary}")


if __name__ == "__main__":
    main()
