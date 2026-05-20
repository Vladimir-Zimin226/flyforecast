import argparse
import asyncio
import csv
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from app.config import get_settings
from app.services.history import get_historical_snapshot
from app.services.predictor import (
    DATA_VERSION,
    MODEL_VERSION,
    calculate_probability,
    get_confidence,
    make_decision,
)
from app.services.weather import fetch_weather_for_date


DEFAULT_DB_PATH = "data/interim/evaluation/forecast_monitor.sqlite"
DEFAULT_EXPORT_DIR = "data/interim/evaluation/exports"
DEFAULT_BOARD_STATUS_PATH = "data/raw/flight_status/kunashir_flight_status_hourly.csv"
DEFAULT_HORIZONS = ",".join(str(value) for value in range(0, 46))
DEFAULT_EXTRA_HORIZONS = "60,90"

COMPLETED_BOARD_STATUSES = {"departed", "arrived", "in_flight"}
UNKNOWN_OUTCOME_STATUSES = {"unknown", "planned_only", "needs_review"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create forecast ledger rows, resolve outcomes from board data, and evaluate prediction quality."
    )
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--export-dir", default=DEFAULT_EXPORT_DIR)
    parser.add_argument("--board-status-path", default=DEFAULT_BOARD_STATUS_PATH)
    parser.add_argument("--horizons", default=DEFAULT_HORIZONS)
    parser.add_argument("--extra-horizons", default=DEFAULT_EXTRA_HORIZONS)
    parser.add_argument("--finalize-lag-days", type=int, default=2)
    parser.add_argument("--prediction-start-hour", type=int, default=6)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=3600)
    parser.add_argument("--timezone", default=None)
    return parser.parse_args()


def parse_horizons(*values: str) -> list[int]:
    result: set[int] = set()
    for value in values:
        for part in value.split(","):
            clean = part.strip()
            if clean:
                result.add(int(clean))
    return sorted(result)


def now_in_timezone(timezone_name: str) -> datetime:
    return datetime.now(ZoneInfo(timezone_name)).replace(microsecond=0)


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS prediction_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TEXT NOT NULL,
            created_at TEXT NOT NULL,
            timezone TEXT NOT NULL,
            model_version TEXT NOT NULL,
            data_version TEXT NOT NULL,
            dataset_path TEXT NOT NULL,
            horizons TEXT NOT NULL,
            UNIQUE(run_date, model_version, data_version, dataset_path, horizons)
        );

        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL REFERENCES prediction_runs(id) ON DELETE CASCADE,
            created_at TEXT NOT NULL,
            run_date TEXT NOT NULL,
            target_date TEXT NOT NULL,
            horizon_days INTEGER NOT NULL,
            probability_flight REAL NOT NULL,
            decision TEXT NOT NULL CHECK(decision IN ('yes', 'no')),
            confidence TEXT NOT NULL,
            model_version TEXT NOT NULL,
            data_version TEXT NOT NULL,
            weather_source TEXT NOT NULL,
            weather_available INTEGER NOT NULL,
            weather_reason TEXT,
            temperature_2m REAL,
            relative_humidity_2m REAL,
            dew_point_2m REAL,
            pressure_msl REAL,
            cloud_cover REAL,
            precipitation REAL,
            wind_speed_10m REAL,
            wind_gusts_10m REAL,
            history_source TEXT NOT NULL,
            similar_days_count INTEGER NOT NULL,
            completed_count INTEGER NOT NULL,
            cancelled_count INTEGER NOT NULL,
            historical_probability_flight REAL NOT NULL,
            month_probability_flight REAL,
            decade_probability_flight REAL,
            UNIQUE(run_date, target_date, model_version, data_version)
        );

        CREATE TABLE IF NOT EXISTS board_outcomes (
            target_date TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            is_final INTEGER NOT NULL,
            finalized_at TEXT,
            first_observed_at TEXT,
            last_observed_at TEXT,
            evidence_count INTEGER NOT NULL,
            evidence_statuses TEXT NOT NULL,
            source_types TEXT NOT NULL,
            flight_numbers TEXT NOT NULL,
            reason_class TEXT NOT NULL,
            raw_evidence_sample TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS prediction_evaluations (
            prediction_id INTEGER PRIMARY KEY REFERENCES predictions(id) ON DELETE CASCADE,
            target_date TEXT NOT NULL,
            outcome_status TEXT NOT NULL,
            outcome_binary INTEGER NOT NULL,
            decision_binary INTEGER NOT NULL,
            hit INTEGER NOT NULL,
            probability_flight REAL NOT NULL,
            brier_score REAL NOT NULL,
            absolute_error REAL NOT NULL,
            horizon_days INTEGER NOT NULL,
            horizon_bucket TEXT NOT NULL,
            evaluated_at TEXT NOT NULL
        );
        """
    )
    conn.commit()


def get_or_create_run(
    conn: sqlite3.Connection,
    run_date: date,
    created_at: datetime,
    timezone_name: str,
    dataset_path: str,
    horizons: list[int],
) -> int:
    horizons_text = ",".join(str(value) for value in horizons)
    conn.execute(
        """
        INSERT OR IGNORE INTO prediction_runs (
            run_date, created_at, timezone, model_version, data_version, dataset_path, horizons
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_date.isoformat(),
            created_at.isoformat(),
            timezone_name,
            MODEL_VERSION,
            DATA_VERSION,
            dataset_path,
            horizons_text,
        ),
    )
    row = conn.execute(
        """
        SELECT id FROM prediction_runs
        WHERE run_date = ?
          AND model_version = ?
          AND data_version = ?
          AND dataset_path = ?
          AND horizons = ?
        """,
        (run_date.isoformat(), MODEL_VERSION, DATA_VERSION, dataset_path, horizons_text),
    ).fetchone()
    if row is None:
        raise RuntimeError("Could not create or find prediction run.")
    conn.commit()
    return int(row["id"])


def row_count_changed(cursor: sqlite3.Cursor) -> bool:
    return cursor.rowcount > 0


async def make_prediction_rows(conn: sqlite3.Connection, horizons: list[int], timezone_name: str) -> int:
    settings = get_settings()
    created_at = now_in_timezone(timezone_name)
    run_date = created_at.date()
    run_id = get_or_create_run(
        conn=conn,
        run_date=run_date,
        created_at=created_at,
        timezone_name=timezone_name,
        dataset_path=settings.flyforecast_dataset_path,
        horizons=horizons,
    )

    inserted = 0
    for horizon in horizons:
        target_date = run_date + timedelta(days=horizon)
        weather = await fetch_weather_for_date(target_date)
        history = get_historical_snapshot(target_date)
        probability = calculate_probability(horizon_days=horizon, weather=weather, history=history)
        confidence = get_confidence(horizon_days=horizon, weather=weather, history=history)
        decision = make_decision(probability_flight=probability, horizon_days=horizon)

        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO predictions (
                run_id, created_at, run_date, target_date, horizon_days,
                probability_flight, decision, confidence, model_version, data_version,
                weather_source, weather_available, weather_reason,
                temperature_2m, relative_humidity_2m, dew_point_2m, pressure_msl,
                cloud_cover, precipitation, wind_speed_10m, wind_gusts_10m,
                history_source, similar_days_count, completed_count, cancelled_count,
                historical_probability_flight, month_probability_flight, decade_probability_flight
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                created_at.isoformat(),
                run_date.isoformat(),
                target_date.isoformat(),
                horizon,
                probability,
                decision,
                confidence,
                MODEL_VERSION,
                DATA_VERSION,
                weather.source,
                int(weather.available),
                weather.reason,
                weather.temperature_2m,
                weather.relative_humidity_2m,
                weather.dew_point_2m,
                weather.pressure_msl,
                weather.cloud_cover,
                weather.precipitation,
                weather.wind_speed_10m,
                weather.wind_gusts_10m,
                history.source,
                history.similar_days_count,
                history.completed_count,
                history.cancelled_count,
                history.historical_probability_flight,
                history.month_probability_flight,
                history.decade_probability_flight,
            ),
        )
        inserted += int(row_count_changed(cursor))

    conn.commit()
    return inserted


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def join_unique(values: Iterable[object]) -> str:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = clean_text(value)
        if clean and clean not in seen:
            seen.add(clean)
            result.append(clean)
    return ";".join(result)


def choose_outcome_status(statuses: set[str]) -> str:
    if statuses & COMPLETED_BOARD_STATUSES:
        return "completed"
    if "cancelled" in statuses or "combined" in statuses:
        return "cancelled"
    if "delayed" in statuses:
        return "needs_review"
    if "scheduled" in statuses or "check_in" in statuses:
        return "planned_only"
    return "unknown"


def build_outcomes_from_board(board_path: Path, today: date, finalize_lag_days: int) -> list[dict]:
    if not board_path.exists():
        return []

    with board_path.open("r", encoding="utf-8-sig", newline="") as file:
        rows = list(csv.DictReader(file))

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        flight_date = clean_text(row.get("flight_date"))
        if not flight_date:
            continue
        grouped.setdefault(flight_date, []).append(row)

    outcomes: list[dict] = []
    for flight_date, day_rows in sorted(grouped.items()):
        try:
            target_date = date.fromisoformat(flight_date)
        except ValueError:
            continue

        statuses = {clean_text(row.get("status_normalized")) for row in day_rows}
        statuses.discard("")
        status = choose_outcome_status(statuses)
        is_final = target_date <= today - timedelta(days=finalize_lag_days) and status not in UNKNOWN_OUTCOME_STATUSES

        if status == "cancelled" and "combined" in statuses:
            reason_class = "schedule_combined"
        else:
            reason_class = join_unique(row.get("reason_class") for row in day_rows) or "unknown"

        outcomes.append(
            {
                "target_date": target_date.isoformat(),
                "status": status,
                "is_final": int(is_final),
                "finalized_at": datetime.now().isoformat(timespec="seconds") if is_final else "",
                "first_observed_at": min(clean_text(row.get("observed_at")) for row in day_rows),
                "last_observed_at": max(clean_text(row.get("observed_at")) for row in day_rows),
                "evidence_count": len(day_rows),
                "evidence_statuses": ";".join(sorted(statuses)),
                "source_types": join_unique(row.get("source") for row in day_rows),
                "flight_numbers": join_unique(row.get("flight_numbers") for row in day_rows),
                "reason_class": reason_class,
                "raw_evidence_sample": " | ".join(
                    clean_text(row.get("raw_row_text"))[:300]
                    for row in day_rows[:8]
                    if clean_text(row.get("raw_row_text"))
                ),
            }
        )

    return outcomes


def upsert_outcomes(conn: sqlite3.Connection, outcomes: list[dict]) -> int:
    changed = 0
    for outcome in outcomes:
        cursor = conn.execute(
            """
            INSERT INTO board_outcomes (
                target_date, status, is_final, finalized_at, first_observed_at, last_observed_at,
                evidence_count, evidence_statuses, source_types, flight_numbers, reason_class,
                raw_evidence_sample
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(target_date) DO UPDATE SET
                status = excluded.status,
                is_final = CASE
                    WHEN board_outcomes.is_final = 1 THEN 1
                    ELSE excluded.is_final
                END,
                finalized_at = CASE
                    WHEN board_outcomes.is_final = 1 AND board_outcomes.finalized_at != '' THEN board_outcomes.finalized_at
                    ELSE excluded.finalized_at
                END,
                first_observed_at = excluded.first_observed_at,
                last_observed_at = excluded.last_observed_at,
                evidence_count = excluded.evidence_count,
                evidence_statuses = excluded.evidence_statuses,
                source_types = excluded.source_types,
                flight_numbers = excluded.flight_numbers,
                reason_class = excluded.reason_class,
                raw_evidence_sample = excluded.raw_evidence_sample
            """,
            (
                outcome["target_date"],
                outcome["status"],
                outcome["is_final"],
                outcome["finalized_at"],
                outcome["first_observed_at"],
                outcome["last_observed_at"],
                outcome["evidence_count"],
                outcome["evidence_statuses"],
                outcome["source_types"],
                outcome["flight_numbers"],
                outcome["reason_class"],
                outcome["raw_evidence_sample"],
            ),
        )
        changed += int(row_count_changed(cursor))

    conn.commit()
    return changed


def horizon_bucket(horizon_days: int) -> str:
    if horizon_days == 0:
        return "0"
    if horizon_days <= 3:
        return "1-3"
    if horizon_days <= 7:
        return "4-7"
    if horizon_days <= 14:
        return "8-14"
    if horizon_days <= 30:
        return "15-30"
    if horizon_days <= 45:
        return "31-45"
    return "46+"


def evaluate_predictions(conn: sqlite3.Connection, evaluated_at: datetime) -> int:
    rows = conn.execute(
        """
        SELECT
            p.id AS prediction_id,
            p.target_date,
            p.horizon_days,
            p.probability_flight,
            p.decision,
            o.status AS outcome_status
        FROM predictions p
        JOIN board_outcomes o ON o.target_date = p.target_date
        WHERE o.is_final = 1
          AND o.status IN ('completed', 'cancelled')
        """
    ).fetchall()

    changed = 0
    for row in rows:
        outcome_binary = 1 if row["outcome_status"] == "completed" else 0
        decision_binary = 1 if row["decision"] == "yes" else 0
        hit = int(outcome_binary == decision_binary)
        probability = float(row["probability_flight"])
        error = probability - outcome_binary
        cursor = conn.execute(
            """
            INSERT INTO prediction_evaluations (
                prediction_id, target_date, outcome_status, outcome_binary, decision_binary,
                hit, probability_flight, brier_score, absolute_error, horizon_days,
                horizon_bucket, evaluated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(prediction_id) DO UPDATE SET
                outcome_status = excluded.outcome_status,
                outcome_binary = excluded.outcome_binary,
                decision_binary = excluded.decision_binary,
                hit = excluded.hit,
                probability_flight = excluded.probability_flight,
                brier_score = excluded.brier_score,
                absolute_error = excluded.absolute_error,
                horizon_days = excluded.horizon_days,
                horizon_bucket = excluded.horizon_bucket,
                evaluated_at = excluded.evaluated_at
            """,
            (
                row["prediction_id"],
                row["target_date"],
                row["outcome_status"],
                outcome_binary,
                decision_binary,
                hit,
                probability,
                round(error * error, 6),
                round(abs(error), 6),
                row["horizon_days"],
                horizon_bucket(int(row["horizon_days"])),
                evaluated_at.isoformat(),
            ),
        )
        changed += int(row_count_changed(cursor))

    conn.commit()
    return changed


def export_table(conn: sqlite3.Connection, table_name: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    if rows:
        fieldnames = rows[0].keys()
    else:
        fieldnames = [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")]

    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def export_metrics(conn: sqlite3.Connection, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        """
        SELECT
            horizon_bucket,
            COUNT(*) AS evaluated_count,
            ROUND(AVG(hit), 4) AS accuracy,
            ROUND(AVG(brier_score), 6) AS brier_score,
            ROUND(AVG(absolute_error), 6) AS mean_absolute_error,
            ROUND(AVG(probability_flight), 4) AS mean_predicted_probability,
            ROUND(AVG(outcome_binary), 4) AS observed_completion_rate
        FROM prediction_evaluations
        GROUP BY horizon_bucket
        ORDER BY
            CASE horizon_bucket
                WHEN '0' THEN 0
                WHEN '1-3' THEN 1
                WHEN '4-7' THEN 2
                WHEN '8-14' THEN 3
                WHEN '15-30' THEN 4
                WHEN '31-45' THEN 5
                ELSE 6
            END
        """
    ).fetchall()

    fieldnames = [
        "horizon_bucket",
        "evaluated_count",
        "accuracy",
        "brier_score",
        "mean_absolute_error",
        "mean_predicted_probability",
        "observed_completion_rate",
    ]
    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def export_all(conn: sqlite3.Connection, export_dir: Path) -> None:
    export_table(conn, "prediction_runs", export_dir / "forecast_prediction_runs.csv")
    export_table(conn, "predictions", export_dir / "forecast_predictions.csv")
    export_table(conn, "board_outcomes", export_dir / "forecast_outcomes.csv")
    export_table(conn, "prediction_evaluations", export_dir / "forecast_evaluations.csv")
    export_metrics(conn, export_dir / "forecast_metrics_summary.csv")


async def run_once(args: argparse.Namespace) -> None:
    settings = get_settings()
    timezone_name = args.timezone or settings.airport_timezone
    horizons = parse_horizons(args.horizons, args.extra_horizons)
    now = now_in_timezone(timezone_name)

    with connect(Path(args.db_path)) as conn:
        init_db(conn)
        if now.hour >= args.prediction_start_hour:
            predictions_inserted = await make_prediction_rows(conn, horizons, timezone_name)
        else:
            predictions_inserted = 0
        outcomes = build_outcomes_from_board(
            board_path=Path(args.board_status_path),
            today=now.date(),
            finalize_lag_days=args.finalize_lag_days,
        )
        outcomes_changed = upsert_outcomes(conn, outcomes)
        evaluations_changed = evaluate_predictions(conn, now)
        export_all(conn, Path(args.export_dir))

    print(
        f"{now.isoformat()} forecast_monitor "
        f"predictions_inserted={predictions_inserted} "
        f"outcomes_seen={len(outcomes)} outcomes_changed={outcomes_changed} "
        f"evaluations_changed={evaluations_changed} "
        f"db={args.db_path} export_dir={args.export_dir}"
    )


async def run_loop(args: argparse.Namespace) -> None:
    while True:
        try:
            await run_once(args)
        except Exception as exc:
            print(f"{datetime.now().isoformat(timespec='seconds')} forecast_monitor_error={type(exc).__name__}: {exc}")
        await asyncio.sleep(args.interval_seconds)


def main() -> None:
    args = parse_args()
    if args.loop:
        asyncio.run(run_loop(args))
    else:
        asyncio.run(run_once(args))


if __name__ == "__main__":
    main()
