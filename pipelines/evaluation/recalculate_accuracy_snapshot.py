import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
for candidate in (PROJECT_ROOT, PROJECT_ROOT / "backend"):
    if (candidate / "app").exists():
        sys.path.insert(0, str(candidate))
        break

from app.schemas import HistoricalSnapshot, WeatherSnapshot
from app.services.predictor import MODEL_VERSION, calculate_probability, make_decision


DEFAULT_DB_PATH = "data/interim/evaluation/forecast_monitor.sqlite"
DEFAULT_OUTPUT_DIR = "data/interim/evaluation/snapshots"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Recalculate evaluated forecast-monitor rows with the current predictor logic "
            "using stored weather/history snapshots. This does not mutate the production ledger."
        )
    )
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--label", default=None)
    return parser.parse_args()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def snapshot_weather(row: sqlite3.Row) -> WeatherSnapshot:
    return WeatherSnapshot(
        source=row["weather_source"],
        available=bool(row["weather_available"]),
        reason=row["weather_reason"],
        temperature_2m=row["temperature_2m"],
        relative_humidity_2m=row["relative_humidity_2m"],
        dew_point_2m=row["dew_point_2m"],
        dew_point_spread=row["dew_point_spread"],
        pressure_msl=row["pressure_msl"],
        cloud_cover=row["cloud_cover"],
        cloud_cover_low=row["cloud_cover_low"],
        precipitation=row["precipitation"],
        wind_speed_10m=row["wind_speed_10m"],
        wind_gusts_10m=row["wind_gusts_10m"],
        wind_direction_10m=row["wind_direction_10m"] if "wind_direction_10m" in row.keys() else None,
        weather_code=row["weather_code"],
        visibility=row["visibility"],
        fog_low_cloud_risk_score=row["fog_low_cloud_risk_score"],
        fog_low_cloud_risk_level=row["fog_low_cloud_risk_level"],
        aggregation_window_start_hour=row["aggregation_window_start_hour"],
        aggregation_window_end_hour=row["aggregation_window_end_hour"],
        aggregation_window_hours=row["aggregation_window_hours"],
        flight_window_available=(
            bool(row["flight_window_available"])
            if row["flight_window_available"] is not None
            else None
        ),
        flight_window_start_hour=row["flight_window_start_hour"],
        flight_window_end_hour=row["flight_window_end_hour"],
        flight_window_hours=row["flight_window_hours"],
        flight_window_visibility=row["flight_window_visibility"],
        flight_window_cloud_cover_low=row["flight_window_cloud_cover_low"],
        flight_window_fog_low_cloud_risk_score=row["flight_window_fog_low_cloud_risk_score"],
        flight_window_fog_low_cloud_risk_level=row["flight_window_fog_low_cloud_risk_level"],
    )


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


def metric_row(rows: list[dict]) -> dict:
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

    return {
        "evaluated_count": count,
        "predicted_yes": sum(1 for row in rows if row["recalculated_decision"] == "yes"),
        "predicted_no": sum(1 for row in rows if row["recalculated_decision"] == "no"),
        "observed_completed": sum(row["outcome_binary"] for row in rows),
        "observed_cancelled": sum(1 - row["outcome_binary"] for row in rows),
        "false_yes": sum(
            1
            for row in rows
            if row["recalculated_decision"] == "yes" and row["outcome_binary"] == 0
        ),
        "false_no": sum(
            1
            for row in rows
            if row["recalculated_decision"] == "no" and row["outcome_binary"] == 1
        ),
        "accuracy": round(sum(row["recalculated_hit"] for row in rows) / count, 4),
        "brier_score": round(sum(row["recalculated_brier_score"] for row in rows) / count, 6),
        "mean_absolute_error": round(sum(row["recalculated_absolute_error"] for row in rows) / count, 6),
        "mean_predicted_probability": round(sum(row["recalculated_probability_flight"] for row in rows) / count, 4),
        "observed_completion_rate": round(sum(row["outcome_binary"] for row in rows) / count, 4),
    }


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(
    path: Path,
    *,
    generated_at: str,
    label: str,
    total: dict,
    original_total: dict,
    by_bucket: list[dict],
    by_original_model: list[dict],
    csv_path: Path,
) -> None:
    lines = [
        "# Recalculated Forecast Accuracy Snapshot",
        "",
        f"generated_at: {generated_at}",
        f"label: {label}",
        f"current_model_version: {MODEL_VERSION}",
        "weather_history_source: stored forecast_monitor snapshots",
        "ledger_mutated: false",
        "",
        "Important: this audit uses the weather/history snapshots already saved in SQLite. "
        "It does not recover forecast fields that were not stored by older model versions.",
        "",
        "## Recalculated Totals",
        "",
        f"- evaluated_count: {total['evaluated_count']}",
        f"- predicted_yes: {total['predicted_yes']}",
        f"- predicted_no: {total['predicted_no']}",
        f"- observed_completed: {total['observed_completed']}",
        f"- observed_cancelled: {total['observed_cancelled']}",
        f"- false_yes: {total['false_yes']}",
        f"- false_no: {total['false_no']}",
        f"- accuracy: {total['accuracy']}",
        f"- brier_score: {total['brier_score']}",
        f"- mean_absolute_error: {total['mean_absolute_error']}",
        f"- mean_predicted_probability: {total['mean_predicted_probability']}",
        f"- observed_completion_rate: {total['observed_completion_rate']}",
        "",
        "## Original Ledger Totals",
        "",
        f"- evaluated_count: {original_total['evaluated_count']}",
        f"- accuracy: {original_total['accuracy']}",
        f"- brier_score: {original_total['brier_score']}",
        f"- mean_absolute_error: {original_total['mean_absolute_error']}",
        f"- mean_predicted_probability: {original_total['mean_predicted_probability']}",
        f"- observed_completion_rate: {original_total['observed_completion_rate']}",
        "",
        "## Recalculated By Horizon Bucket",
        "",
        "| horizon_bucket | evaluated_count | accuracy | brier_score | mean_absolute_error | mean_predicted_probability | observed_completion_rate |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for row in by_bucket:
        lines.append(
            f"| {row['horizon_bucket']} | {row['evaluated_count']} | {row['accuracy']} | "
            f"{row['brier_score']} | {row['mean_absolute_error']} | "
            f"{row['mean_predicted_probability']} | {row['observed_completion_rate']} |"
        )

    lines.extend(
        [
            "",
            "## Recalculated By Original Model Version",
            "",
            "| original_model_version | evaluated_count | accuracy | brier_score | mean_absolute_error | mean_predicted_probability | observed_completion_rate |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for row in by_original_model:
        lines.append(
            f"| {row['original_model_version']} | {row['evaluated_count']} | {row['accuracy']} | "
            f"{row['brier_score']} | {row['mean_absolute_error']} | "
            f"{row['mean_predicted_probability']} | {row['observed_completion_rate']} |"
        )

    lines.extend(["", f"details_csv: {csv_path.as_posix()}", ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    output_dir = Path(args.output_dir)
    generated_at = datetime.now().strftime("%Y%m%d_%H%M%S")
    label = args.label or f"recalculated_{MODEL_VERSION}_{generated_at}"
    safe_label = "".join(char if char.isalnum() or char in "-_" else "_" for char in label)
    markdown_path = output_dir / f"{safe_label}.md"
    csv_path = output_dir / f"{safe_label}_details.csv"

    with connect(db_path) as conn:
        rows = fetch_evaluated_rows(conn)

    recalculated: list[dict] = []
    for row in rows:
        probability = calculate_probability(
            horizon_days=row["horizon_days"],
            weather=snapshot_weather(row),
            history=snapshot_history(row),
        )
        decision = make_decision(
            probability_flight=probability,
            horizon_days=row["horizon_days"],
        )
        decision_binary = 1 if decision == "yes" else 0
        outcome_binary = int(row["outcome_binary"])
        absolute_error = abs(probability - outcome_binary)

        recalculated.append(
            {
                "prediction_id": row["id"],
                "run_date": row["run_date"],
                "target_date": row["target_date"],
                "horizon_days": row["horizon_days"],
                "horizon_bucket": row["horizon_bucket"],
                "original_model_version": row["model_version"],
                "outcome_status": row["outcome_status"],
                "outcome_binary": outcome_binary,
                "original_probability_flight": row["original_probability_flight"],
                "original_decision": row["decision"],
                "original_hit": row["original_hit"],
                "recalculated_model_version": MODEL_VERSION,
                "recalculated_probability_flight": probability,
                "recalculated_decision": decision,
                "recalculated_hit": int(decision_binary == outcome_binary),
                "recalculated_brier_score": round((probability - outcome_binary) ** 2, 6),
                "recalculated_absolute_error": round(absolute_error, 6),
                "weather_source": row["weather_source"],
                "weather_available": row["weather_available"],
                "flight_window_available": row["flight_window_available"],
                "visibility": row["visibility"],
                "weather_code": row["weather_code"],
                "wind_gusts_10m": row["wind_gusts_10m"],
                "cloud_cover_low": row["cloud_cover_low"],
                "fog_low_cloud_risk_level": row["fog_low_cloud_risk_level"],
            }
        )

    original_total = {
        "evaluated_count": len(recalculated),
        "accuracy": round(sum(row["original_hit"] for row in recalculated) / len(recalculated), 4)
        if recalculated
        else None,
        "brier_score": round(sum(row["original_brier_score"] for row in rows) / len(rows), 6)
        if rows
        else None,
        "mean_absolute_error": round(sum(row["original_absolute_error"] for row in rows) / len(rows), 6)
        if rows
        else None,
        "mean_predicted_probability": round(
            sum(row["original_probability_flight"] for row in recalculated) / len(recalculated), 4
        )
        if recalculated
        else None,
        "observed_completion_rate": round(
            sum(row["outcome_binary"] for row in recalculated) / len(recalculated), 4
        )
        if recalculated
        else None,
    }

    grouped_by_bucket: dict[str, list[dict]] = defaultdict(list)
    grouped_by_model: dict[str, list[dict]] = defaultdict(list)
    for row in recalculated:
        grouped_by_bucket[row["horizon_bucket"]].append(row)
        grouped_by_model[row["original_model_version"]].append(row)

    bucket_order = {"0": 0, "1-3": 1, "4-7": 2, "8-14": 3, "15-30": 4, "31-45": 5}
    by_bucket = []
    for bucket, bucket_rows in grouped_by_bucket.items():
        metrics = metric_row(bucket_rows)
        metrics["horizon_bucket"] = bucket
        by_bucket.append(metrics)
    by_bucket.sort(key=lambda row: bucket_order.get(row["horizon_bucket"], 99))

    by_original_model = []
    for model_version, model_rows in grouped_by_model.items():
        metrics = metric_row(model_rows)
        metrics["original_model_version"] = model_version
        by_original_model.append(metrics)
    by_original_model.sort(key=lambda row: row["original_model_version"])

    detail_fields = [
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
        "recalculated_model_version",
        "recalculated_probability_flight",
        "recalculated_decision",
        "recalculated_hit",
        "recalculated_brier_score",
        "recalculated_absolute_error",
        "weather_source",
        "weather_available",
        "flight_window_available",
        "visibility",
        "weather_code",
        "wind_gusts_10m",
        "cloud_cover_low",
        "fog_low_cloud_risk_level",
    ]
    write_csv(csv_path, recalculated, detail_fields)

    total = metric_row(recalculated)
    write_markdown(
        markdown_path,
        generated_at=datetime.now().isoformat(timespec="seconds"),
        label=label,
        total=total,
        original_total=original_total,
        by_bucket=by_bucket,
        by_original_model=by_original_model,
        csv_path=csv_path,
    )

    print(f"wrote_markdown={markdown_path}")
    print(f"wrote_csv={csv_path}")
    print(f"evaluated_count={total['evaluated_count']}")
    print(f"original_accuracy={original_total['accuracy']}")
    print(f"recalculated_accuracy={total['accuracy']}")


if __name__ == "__main__":
    main()
