import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx


PROJECT_ROOT = Path(__file__).resolve().parents[2]
for candidate in (PROJECT_ROOT, PROJECT_ROOT / "backend"):
    if (candidate / "app").exists():
        sys.path.insert(0, str(candidate))
        break

from app.config import get_settings
from app.schemas import HistoricalSnapshot, WeatherSnapshot
from app.services.predictor import MODEL_VERSION, calculate_probability, make_decision
from app.services.weather import (
    HOURLY_FIELDS,
    OPEN_METEO_MAX_HORIZON_DAYS,
    _open_meteo_payload_to_snapshots,
    _unavailable_weather,
)


DEFAULT_DB_PATH = "data/interim/evaluation/forecast_monitor.sqlite"
DEFAULT_OUTPUT_DIR = "data/interim/evaluation/backtests"
HISTORICAL_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backtest the current baseline on evaluated forecast-monitor rows using rebuilt "
            "Open-Meteo historical weather snapshots and stored leakage-safe history snapshots."
        )
    )
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--label", default=None)
    parser.add_argument(
        "--weather-provider",
        choices=("historical-forecast", "archive"),
        default="historical-forecast",
        help="Weather endpoint used to rebuild target-date weather snapshots.",
    )
    parser.add_argument(
        "--target-accuracy",
        type=float,
        default=0.90,
        help="Product target shown in the markdown report; does not change calculations.",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=31,
        help="Maximum date range per Open-Meteo request.",
    )
    return parser.parse_args()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_evaluated_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            p.*,
            e.outcome_binary,
            e.outcome_status,
            e.horizon_bucket,
            e.hit AS original_hit,
            e.probability_flight AS original_probability_flight,
            e.brier_score AS original_brier_score,
            e.absolute_error AS original_absolute_error
        FROM predictions p
        JOIN prediction_evaluations e ON e.prediction_id = p.id
        ORDER BY p.run_date, p.target_date, p.horizon_days, p.id
        """
    ).fetchall()


def snapshot_history(row: sqlite3.Row) -> HistoricalSnapshot:
    return HistoricalSnapshot(
        source=row["history_source"],
        similar_days_count=row["similar_days_count"],
        completed_count=row["completed_count"],
        cancelled_count=row["cancelled_count"],
        historical_probability_flight=row["historical_probability_flight"],
        month_probability_flight=row["month_probability_flight"],
        decade_probability_flight=row["decade_probability_flight"],
    )


def metric_row(rows: list[dict[str, Any]], *, prefix: str = "backtest") -> dict[str, Any]:
    count = len(rows)
    if count == 0:
        return {
            "evaluated_count": 0,
            "predicted_yes": 0,
            "predicted_no": 0,
            "observed_completed": 0,
            "observed_cancelled": 0,
            "false_yes": 0,
            "false_no": 0,
            "accuracy": None,
            "brier_score": None,
            "mean_absolute_error": None,
            "mean_predicted_probability": None,
            "observed_completion_rate": None,
        }

    decision_key = f"{prefix}_decision"
    probability_key = f"{prefix}_probability_flight"
    hit_key = f"{prefix}_hit"
    brier_key = f"{prefix}_brier_score"
    absolute_error_key = f"{prefix}_absolute_error"

    return {
        "evaluated_count": count,
        "predicted_yes": sum(1 for row in rows if row[decision_key] == "yes"),
        "predicted_no": sum(1 for row in rows if row[decision_key] == "no"),
        "observed_completed": sum(row["outcome_binary"] for row in rows),
        "observed_cancelled": sum(1 - row["outcome_binary"] for row in rows),
        "false_yes": sum(1 for row in rows if row[decision_key] == "yes" and row["outcome_binary"] == 0),
        "false_no": sum(1 for row in rows if row[decision_key] == "no" and row["outcome_binary"] == 1),
        "accuracy": round(sum(row[hit_key] for row in rows) / count, 4),
        "brier_score": round(sum(row[brier_key] for row in rows) / count, 6),
        "mean_absolute_error": round(sum(row[absolute_error_key] for row in rows) / count, 6),
        "mean_predicted_probability": round(sum(row[probability_key] for row in rows) / count, 4),
        "observed_completion_rate": round(sum(row["outcome_binary"] for row in rows) / count, 4),
    }


def original_metric_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    if count == 0:
        return {
            "evaluated_count": 0,
            "accuracy": None,
            "brier_score": None,
            "mean_absolute_error": None,
            "mean_predicted_probability": None,
            "observed_completion_rate": None,
        }

    return {
        "evaluated_count": count,
        "accuracy": round(sum(row["original_hit"] for row in rows) / count, 4),
        "brier_score": round(sum(row["original_brier_score"] for row in rows) / count, 6),
        "mean_absolute_error": round(sum(row["original_absolute_error"] for row in rows) / count, 6),
        "mean_predicted_probability": round(sum(row["original_probability_flight"] for row in rows) / count, 4),
        "observed_completion_rate": round(sum(row["outcome_binary"] for row in rows) / count, 4),
    }


def date_chunks(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    current = start
    step = max(1, chunk_days)
    while current <= end:
        chunk_end = min(current + timedelta(days=step - 1), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def endpoint_for_provider(provider: str) -> str:
    if provider == "archive":
        return ARCHIVE_URL
    return HISTORICAL_FORECAST_URL


def fetch_weather_snapshots(
    *,
    target_dates: set[date],
    provider: str,
    chunk_days: int,
) -> dict[date, WeatherSnapshot]:
    if not target_dates:
        return {}

    settings = get_settings()
    snapshots: dict[date, WeatherSnapshot] = {}
    endpoint = endpoint_for_provider(provider)
    sorted_dates = sorted(target_dates)

    with httpx.Client(timeout=30) as client:
        for start, end in date_chunks(sorted_dates[0], sorted_dates[-1], chunk_days):
            params = {
                "latitude": settings.airport_latitude,
                "longitude": settings.airport_longitude,
                "timezone": settings.airport_timezone,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "hourly": ",".join(HOURLY_FIELDS),
            }
            response = client.get(endpoint, params=params)
            response.raise_for_status()
            for target_date, snapshot in _open_meteo_payload_to_snapshots(response.json(), settings).items():
                if target_date in target_dates:
                    payload = snapshot.model_dump()
                    payload["source"] = f"open-meteo-{provider}-backtest"
                    snapshots[target_date] = WeatherSnapshot(**payload)

    return snapshots


def unavailable_backtest_weather(target_date: date) -> WeatherSnapshot:
    return _unavailable_weather(
        f"Backtest weather is unavailable for {target_date.isoformat()} because the original horizon exceeded "
        f"{OPEN_METEO_MAX_HORIZON_DAYS} days."
    )


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def format_metric_lines(title: str, metrics: dict[str, Any]) -> list[str]:
    lines = [
        f"## {title}",
        "",
        f"- evaluated_count: {metrics['evaluated_count']}",
    ]
    for key in (
        "predicted_yes",
        "predicted_no",
        "observed_completed",
        "observed_cancelled",
        "false_yes",
        "false_no",
    ):
        if key in metrics:
            lines.append(f"- {key}: {metrics[key]}")
    lines.extend(
        [
            f"- accuracy: {metrics['accuracy']}",
            f"- brier_score: {metrics['brier_score']}",
            f"- mean_absolute_error: {metrics['mean_absolute_error']}",
            f"- mean_predicted_probability: {metrics['mean_predicted_probability']}",
            f"- observed_completion_rate: {metrics['observed_completion_rate']}",
            "",
        ]
    )
    return lines


def write_markdown(
    path: Path,
    *,
    generated_at: str,
    label: str,
    weather_provider: str,
    target_accuracy: float,
    original_total: dict[str, Any],
    backtest_total: dict[str, Any],
    by_bucket: list[dict[str, Any]],
    by_weather_source: list[dict[str, Any]],
    csv_path: Path,
) -> None:
    target_met = (
        backtest_total["accuracy"] is not None
        and backtest_total["accuracy"] >= target_accuracy
    )
    lines = [
        "# Current Baseline Backtest",
        "",
        f"generated_at: {generated_at}",
        f"label: {label}",
        f"current_model_version: {MODEL_VERSION}",
        f"weather_provider: {weather_provider}",
        "history_source: stored forecast_monitor history snapshots",
        "ledger_mutated: false",
        f"target_accuracy: {target_accuracy}",
        f"target_met: {str(target_met).lower()}",
        "",
        "This backtest rebuilds near-horizon weather snapshots with current weather-window logic. "
        "It keeps the historical snapshot saved at prediction time to avoid future-label leakage.",
        "",
    ]
    lines.extend(format_metric_lines("Backtest Totals", backtest_total))
    lines.extend(format_metric_lines("Original Ledger Totals", original_total))
    lines.extend(
        [
            "## Backtest By Horizon Bucket",
            "",
            "| horizon_bucket | evaluated_count | predicted_yes | predicted_no | observed_completed | observed_cancelled | false_yes | false_no | accuracy | brier_score | mean_absolute_error | mean_predicted_probability | observed_completion_rate |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in by_bucket:
        lines.append(
            f"| {row['horizon_bucket']} | {row['evaluated_count']} | {row['predicted_yes']} | "
            f"{row['predicted_no']} | {row['observed_completed']} | {row['observed_cancelled']} | "
            f"{row['false_yes']} | {row['false_no']} | {row['accuracy']} | {row['brier_score']} | "
            f"{row['mean_absolute_error']} | {row['mean_predicted_probability']} | "
            f"{row['observed_completion_rate']} |"
        )
    lines.extend(
        [
            "",
            "## Backtest By Weather Source",
            "",
            "| weather_source | evaluated_count | predicted_yes | predicted_no | observed_completed | observed_cancelled | false_yes | false_no | accuracy | brier_score | mean_absolute_error | mean_predicted_probability | observed_completion_rate |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in by_weather_source:
        lines.append(
            f"| {row['backtest_weather_source']} | {row['evaluated_count']} | {row['predicted_yes']} | "
            f"{row['predicted_no']} | {row['observed_completed']} | {row['observed_cancelled']} | "
            f"{row['false_yes']} | {row['false_no']} | {row['accuracy']} | {row['brier_score']} | "
            f"{row['mean_absolute_error']} | {row['mean_predicted_probability']} | "
            f"{row['observed_completion_rate']} |"
        )
    lines.extend(["", f"details_csv: {csv_path.as_posix()}", ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    generated_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    label = args.label or f"current_baseline_backtest_{MODEL_VERSION}_{generated_stamp}"
    safe_label = "".join(char if char.isalnum() or char in "-_" else "_" for char in label)
    markdown_path = output_dir / f"{safe_label}.md"
    csv_path = output_dir / f"{safe_label}_details.csv"

    with connect(Path(args.db_path)) as conn:
        rows = fetch_evaluated_rows(conn)

    near_horizon_dates = {
        date.fromisoformat(row["target_date"])
        for row in rows
        if int(row["horizon_days"]) <= OPEN_METEO_MAX_HORIZON_DAYS
    }
    weather_by_date = fetch_weather_snapshots(
        target_dates=near_horizon_dates,
        provider=args.weather_provider,
        chunk_days=args.chunk_days,
    )

    details: list[dict[str, Any]] = []
    for row in rows:
        horizon_days = int(row["horizon_days"])
        target_date = date.fromisoformat(row["target_date"])
        weather = (
            weather_by_date.get(target_date)
            if horizon_days <= OPEN_METEO_MAX_HORIZON_DAYS
            else unavailable_backtest_weather(target_date)
        )
        if weather is None:
            weather = WeatherSnapshot(
                source=f"open-meteo-{args.weather_provider}-missing",
                available=False,
                reason="No rebuilt weather snapshot returned for target date.",
            )

        history = snapshot_history(row)
        probability = calculate_probability(
            horizon_days=horizon_days,
            weather=weather,
            history=history,
        )
        decision = make_decision(
            probability_flight=probability,
            horizon_days=horizon_days,
        )
        decision_binary = 1 if decision == "yes" else 0
        outcome_binary = int(row["outcome_binary"])
        absolute_error = abs(probability - outcome_binary)

        details.append(
            {
                "prediction_id": row["id"],
                "run_date": row["run_date"],
                "target_date": row["target_date"],
                "horizon_days": horizon_days,
                "horizon_bucket": row["horizon_bucket"],
                "original_model_version": row["model_version"],
                "outcome_status": row["outcome_status"],
                "outcome_binary": outcome_binary,
                "original_probability_flight": row["original_probability_flight"],
                "original_decision": row["decision"],
                "original_hit": row["original_hit"],
                "original_brier_score": row["original_brier_score"],
                "original_absolute_error": row["original_absolute_error"],
                "backtest_model_version": MODEL_VERSION,
                "backtest_probability_flight": probability,
                "backtest_decision": decision,
                "backtest_hit": int(decision_binary == outcome_binary),
                "backtest_brier_score": round((probability - outcome_binary) ** 2, 6),
                "backtest_absolute_error": round(absolute_error, 6),
                "backtest_weather_source": weather.source,
                "backtest_weather_available": int(weather.available),
                "flight_window_available": weather.flight_window_available,
                "flight_window_start_hour": weather.flight_window_start_hour,
                "flight_window_end_hour": weather.flight_window_end_hour,
                "flight_window_hours": weather.flight_window_hours,
                "visibility": weather.visibility,
                "weather_code": weather.weather_code,
                "wind_gusts_10m": weather.wind_gusts_10m,
                "cloud_cover_low": weather.cloud_cover_low,
                "fog_low_cloud_risk_level": weather.fog_low_cloud_risk_level,
                "historical_probability_flight": history.historical_probability_flight,
                "decade_probability_flight": history.decade_probability_flight,
            }
        )

    bucket_order = {"0": 0, "1-3": 1, "4-7": 2, "8-14": 3, "15-30": 4, "31-45": 5}
    grouped_by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    grouped_by_weather_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in details:
        grouped_by_bucket[row["horizon_bucket"]].append(row)
        grouped_by_weather_source[row["backtest_weather_source"]].append(row)

    by_bucket = []
    for bucket, bucket_rows in grouped_by_bucket.items():
        metrics = metric_row(bucket_rows)
        metrics["horizon_bucket"] = bucket
        by_bucket.append(metrics)
    by_bucket.sort(key=lambda row: bucket_order.get(row["horizon_bucket"], 99))

    by_weather_source = []
    for weather_source, source_rows in grouped_by_weather_source.items():
        metrics = metric_row(source_rows)
        metrics["backtest_weather_source"] = weather_source
        by_weather_source.append(metrics)
    by_weather_source.sort(key=lambda row: row["backtest_weather_source"])

    fieldnames = [
        "prediction_id",
        "run_date",
        "target_date",
        "horizon_days",
        "horizon_bucket",
        "original_model_version",
        "outcome_status",
        "outcome_binary",
        "original_probability_flight",
        "original_decision",
        "original_hit",
        "original_brier_score",
        "original_absolute_error",
        "backtest_model_version",
        "backtest_probability_flight",
        "backtest_decision",
        "backtest_hit",
        "backtest_brier_score",
        "backtest_absolute_error",
        "backtest_weather_source",
        "backtest_weather_available",
        "flight_window_available",
        "flight_window_start_hour",
        "flight_window_end_hour",
        "flight_window_hours",
        "visibility",
        "weather_code",
        "wind_gusts_10m",
        "cloud_cover_low",
        "fog_low_cloud_risk_level",
        "historical_probability_flight",
        "decade_probability_flight",
    ]
    write_csv(csv_path, details, fieldnames)

    original_total = original_metric_row(details)
    backtest_total = metric_row(details)
    write_markdown(
        markdown_path,
        generated_at=datetime.now().isoformat(timespec="seconds"),
        label=label,
        weather_provider=args.weather_provider,
        target_accuracy=args.target_accuracy,
        original_total=original_total,
        backtest_total=backtest_total,
        by_bucket=by_bucket,
        by_weather_source=by_weather_source,
        csv_path=csv_path,
    )

    print(f"wrote_markdown={markdown_path}")
    print(f"wrote_csv={csv_path}")
    print(f"evaluated_count={backtest_total['evaluated_count']}")
    print(f"original_accuracy={original_total['accuracy']}")
    print(f"backtest_accuracy={backtest_total['accuracy']}")
    print(f"target_accuracy={args.target_accuracy}")
    print(f"target_met={backtest_total['accuracy'] is not None and backtest_total['accuracy'] >= args.target_accuracy}")
    print(f"predicted_yes={backtest_total['predicted_yes']}")
    print(f"predicted_no={backtest_total['predicted_no']}")
    print(f"false_yes={backtest_total['false_yes']}")
    print(f"false_no={backtest_total['false_no']}")


if __name__ == "__main__":
    main()
