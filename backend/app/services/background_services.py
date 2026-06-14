import csv
import sqlite3
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app.config import get_settings
from app.schemas import FlightScheduleSnapshot, HistoricalSnapshot, WeatherSnapshot
from app.services.predictor import MODEL_VERSION, calculate_probability, make_decision


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = value.strip().replace("Z", "+00:00")
    if not normalized:
        return None

    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _is_stale(value: str | None, stale_hours: int) -> bool:
    parsed = _parse_datetime(value)
    if parsed is None:
        return True

    now = datetime.now(parsed.tzinfo) if parsed.tzinfo else datetime.now()
    return now - parsed > timedelta(hours=stale_hours)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def _file_message(path: Path, title: str) -> str | None:
    if not path.exists():
        return f"{title} не найден: {path}"
    return None


def _service_health(name: str, status: str, last_seen_at: str | None, message: str) -> dict:
    return {
        "name": name,
        "status": status,
        "last_seen_at": last_seen_at,
        "message": message,
    }


def _counter_dict(values: list[str]) -> dict[str, int]:
    return dict(Counter(value or "unknown" for value in values))


def get_board_collector_status() -> dict:
    settings = get_settings()
    dataset_path = Path(settings.flight_status_dataset_path)
    errors_path = Path(settings.flight_status_errors_path)
    rows = _read_csv_rows(dataset_path)
    errors = _read_csv_rows(errors_path)

    latest_observed_at = max((row.get("observed_at") or "" for row in rows), default="") or None
    latest_rows = [row for row in rows if row.get("observed_at") == latest_observed_at] if latest_observed_at else []
    recent_errors = errors[-5:]

    missing_message = _file_message(dataset_path, "CSV табло")
    if missing_message:
        status = "error"
        message = missing_message
    elif _is_stale(latest_observed_at, settings.background_service_stale_hours):
        status = "warning"
        message = f"Последняя проверка табло старше {settings.background_service_stale_hours} часов."
    elif recent_errors and any((error.get("observed_at") or "") >= (latest_observed_at or "") for error in recent_errors):
        status = "warning"
        message = "Последняя проверка выполнена, но есть ошибки по источникам."
    else:
        status = "ok"
        message = "Collector получает строки табло."

    return {
        "health": _service_health("Проверка табло", status, latest_observed_at, message),
        "dataset_path": str(dataset_path),
        "errors_path": str(errors_path),
        "total_rows": len(rows),
        "rows_last_observation": len(latest_rows),
        "latest_observed_at": latest_observed_at,
        "latest_observation_date": latest_rows[0].get("observation_date") if latest_rows else None,
        "latest_statuses": _counter_dict([row.get("status_normalized") or "" for row in latest_rows]),
        "recent_errors": [
            {
                "observed_at": error.get("observed_at") or "",
                "source": error.get("source") or "",
                "source_url": error.get("source_url") or "",
                "error": error.get("error") or "",
            }
            for error in reversed(recent_errors)
        ],
    }


def _connect_sqlite(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _scalar(conn: sqlite3.Connection, query: str, default: int = 0) -> int:
    row = conn.execute(query).fetchone()
    if row is None:
        return default
    return int(row[0] or default)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not _table_exists(conn, table_name):
        return set()
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _expected_predictions(horizons: str | None) -> int:
    if not horizons:
        return 0
    return len([part for part in horizons.split(",") if part.strip()])


def _run_from_row(row: sqlite3.Row, status: str = "success", error: str | None = None) -> dict:
    expected = _expected_predictions(row["horizons"] if "horizons" in row.keys() else None)
    predictions_count = int(row["predictions_count"] or 0)
    run_status = status

    if status == "success" and expected and predictions_count < expected:
        run_status = "partial"

    return {
        "id": int(row["id"]) if "id" in row.keys() and row["id"] is not None else None,
        "run_date": row["run_date"] or "",
        "created_at": row["created_at"] or "",
        "status": run_status,
        "predictions_count": predictions_count,
        "expected_predictions": expected,
        "error": error,
    }


def _weather_snapshot_from_prediction(row: sqlite3.Row) -> WeatherSnapshot:
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


def _history_snapshot_from_prediction(row: sqlite3.Row) -> HistoricalSnapshot:
    return HistoricalSnapshot(
        source=row["history_source"],
        similar_days_count=int(row["similar_days_count"]),
        completed_count=int(row["completed_count"]),
        cancelled_count=int(row["cancelled_count"]),
        historical_probability_flight=float(row["historical_probability_flight"]),
        month_probability_flight=row["month_probability_flight"],
        decade_probability_flight=row["decade_probability_flight"],
    )


def _schedule_snapshot_from_prediction(row: sqlite3.Row) -> FlightScheduleSnapshot | None:
    keys = set(row.keys())
    if "schedule_available" not in keys:
        return None

    return FlightScheduleSnapshot(
        source=row["schedule_source"] or "forecast-monitor",
        available=bool(row["schedule_available"]),
        reason=row["schedule_reason"],
        observed_at=row["schedule_observed_at"],
        flight_numbers=row["schedule_flight_numbers"],
        first_departure_hour=row["schedule_first_departure_hour"],
        first_scheduled_hour=row["schedule_first_scheduled_hour"],
        last_scheduled_hour=row["schedule_last_scheduled_hour"],
        moved_next_day=bool(row["schedule_moved_next_day"]),
        completed_same_day=bool(row["schedule_completed_same_day"]),
        status_summary=row["schedule_status_summary"],
        total_flights=row["schedule_total_flights"] if "schedule_total_flights" in keys else 0,
        completed_flights=row["schedule_completed_flights"] if "schedule_completed_flights" in keys else 0,
        unavailable_flights=row["schedule_unavailable_flights"] if "schedule_unavailable_flights" in keys else 0,
        pending_flights=row["schedule_pending_flights"] if "schedule_pending_flights" in keys else 0,
        active_flight_index=row["schedule_active_flight_index"] if "schedule_active_flight_index" in keys else None,
        active_flight_hour=row["schedule_active_flight_hour"] if "schedule_active_flight_hour" in keys else None,
        active_flight_time=row["schedule_active_flight_time"] if "schedule_active_flight_time" in keys else None,
        active_flight_numbers=row["schedule_active_flight_numbers"] if "schedule_active_flight_numbers" in keys else None,
        active_flight_status=row["schedule_active_flight_status"] if "schedule_active_flight_status" in keys else None,
    )


def _recalculate_forecast_metrics(conn: sqlite3.Connection) -> dict[str, Any]:
    required_prediction_columns = {
        "weather_source",
        "weather_available",
        "weather_reason",
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "dew_point_spread",
        "pressure_msl",
        "cloud_cover",
        "cloud_cover_low",
        "precipitation",
        "wind_speed_10m",
        "wind_gusts_10m",
        "weather_code",
        "visibility",
        "fog_low_cloud_risk_score",
        "fog_low_cloud_risk_level",
        "aggregation_window_start_hour",
        "aggregation_window_end_hour",
        "aggregation_window_hours",
        "flight_window_available",
        "flight_window_start_hour",
        "flight_window_end_hour",
        "flight_window_hours",
        "flight_window_visibility",
        "flight_window_cloud_cover_low",
        "flight_window_fog_low_cloud_risk_score",
        "flight_window_fog_low_cloud_risk_level",
        "history_source",
        "similar_days_count",
        "completed_count",
        "cancelled_count",
        "historical_probability_flight",
        "month_probability_flight",
        "decade_probability_flight",
    }

    if not _table_exists(conn, "predictions") or not _table_exists(conn, "prediction_evaluations"):
        return {
            "model_version": MODEL_VERSION,
            "total_evaluations": 0,
            "total_hits": 0,
            "total_misses": 0,
            "predicted_yes": 0,
            "predicted_no": 0,
            "observed_completed": 0,
            "observed_cancelled": 0,
            "false_yes": 0,
            "false_no": 0,
            "accuracy": None,
            "brier_score": None,
            "mean_absolute_error": None,
            "available": False,
            "reason": "Нет таблиц predictions/prediction_evaluations.",
        }

    missing_columns = required_prediction_columns - _table_columns(conn, "predictions")
    if missing_columns:
        return {
            "model_version": MODEL_VERSION,
            "total_evaluations": 0,
            "total_hits": 0,
            "total_misses": 0,
            "predicted_yes": 0,
            "predicted_no": 0,
            "observed_completed": 0,
            "observed_cancelled": 0,
            "false_yes": 0,
            "false_no": 0,
            "accuracy": None,
            "brier_score": None,
            "mean_absolute_error": None,
            "available": False,
            "reason": f"В SQLite нет колонок для пересчёта: {', '.join(sorted(missing_columns))}.",
        }

    rows = conn.execute(
        """
        SELECT
            p.*,
            e.outcome_binary
        FROM predictions p
        JOIN prediction_evaluations e ON e.prediction_id = p.id
        """
    ).fetchall()

    evaluated = len(rows)
    if not evaluated:
        return {
            "model_version": MODEL_VERSION,
            "total_evaluations": 0,
            "total_hits": 0,
            "total_misses": 0,
            "predicted_yes": 0,
            "predicted_no": 0,
            "observed_completed": 0,
            "observed_cancelled": 0,
            "false_yes": 0,
            "false_no": 0,
            "accuracy": None,
            "brier_score": None,
            "mean_absolute_error": None,
            "available": True,
            "reason": None,
        }

    hits = 0
    predicted_yes = 0
    predicted_no = 0
    observed_completed = 0
    observed_cancelled = 0
    false_yes = 0
    false_no = 0
    brier_sum = 0.0
    absolute_error_sum = 0.0

    for row in rows:
        row_model_version = row["model_version"] if "model_version" in row.keys() else ""
        if str(row_model_version).startswith("historical-ml"):
            probability = float(row["probability_flight"])
            decision = row["decision"]
        else:
            probability = calculate_probability(
                horizon_days=int(row["horizon_days"]),
                weather=_weather_snapshot_from_prediction(row),
                history=_history_snapshot_from_prediction(row),
                schedule=_schedule_snapshot_from_prediction(row),
            )
            decision = make_decision(
                probability_flight=probability,
                horizon_days=int(row["horizon_days"]),
            )
        decision_binary = 1 if decision == "yes" else 0
        outcome_binary = int(row["outcome_binary"])
        predicted_yes += int(decision_binary == 1)
        predicted_no += int(decision_binary == 0)
        observed_completed += int(outcome_binary == 1)
        observed_cancelled += int(outcome_binary == 0)
        hit = int(decision_binary == outcome_binary)
        hits += hit
        false_yes += int(decision_binary == 1 and outcome_binary == 0)
        false_no += int(decision_binary == 0 and outcome_binary == 1)
        brier_sum += (probability - outcome_binary) ** 2
        absolute_error_sum += abs(probability - outcome_binary)

    return {
        "model_version": MODEL_VERSION,
        "total_evaluations": evaluated,
        "total_hits": hits,
        "total_misses": evaluated - hits,
        "predicted_yes": predicted_yes,
        "predicted_no": predicted_no,
        "observed_completed": observed_completed,
        "observed_cancelled": observed_cancelled,
        "false_yes": false_yes,
        "false_no": false_no,
        "accuracy": round(hits / evaluated, 4),
        "brier_score": round(brier_sum / evaluated, 6),
        "mean_absolute_error": round(absolute_error_sum / evaluated, 6),
        "available": True,
        "reason": None,
    }


def _latest_service_run(conn: sqlite3.Connection) -> sqlite3.Row | None:
    if not _table_exists(conn, "service_runs"):
        return None

    return conn.execute(
        """
        SELECT *
        FROM service_runs
        WHERE service_name = 'forecast_monitor'
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()


def get_forecast_monitor_status() -> dict:
    settings = get_settings()
    db_path = Path(settings.forecast_monitor_db_path)

    empty_health = _service_health(
        "Ежедневные прогнозы",
        "error",
        None,
        f"SQLite forecast monitor не найден: {db_path}",
    )

    if not db_path.exists():
        return {
            "health": empty_health,
            "db_path": str(db_path),
            "total_runs": 0,
            "total_predictions": 0,
            "total_evaluations": 0,
            "total_hits": 0,
            "total_misses": 0,
            "total_pending": 0,
            "accuracy": None,
            "recalculated_model_version": MODEL_VERSION,
            "recalculated_total_evaluations": 0,
            "recalculated_total_hits": 0,
            "recalculated_total_misses": 0,
            "recalculated_predicted_yes": 0,
            "recalculated_predicted_no": 0,
            "recalculated_observed_completed": 0,
            "recalculated_observed_cancelled": 0,
            "recalculated_false_yes": 0,
            "recalculated_false_no": 0,
            "recalculated_accuracy": None,
            "recalculated_brier_score": None,
            "recalculated_mean_absolute_error": None,
            "recalculated_metrics_available": False,
            "recalculated_metrics_reason": "SQLite forecast monitor не найден.",
            "latest_run": None,
            "recent_runs": [],
            "recent_predictions": [],
        }

    with _connect_sqlite(db_path) as conn:
        total_runs = _scalar(conn, "SELECT count(*) FROM prediction_runs") if _table_exists(conn, "prediction_runs") else 0
        total_predictions = _scalar(conn, "SELECT count(*) FROM predictions") if _table_exists(conn, "predictions") else 0
        total_evaluations = (
            _scalar(conn, "SELECT count(*) FROM prediction_evaluations")
            if _table_exists(conn, "prediction_evaluations")
            else 0
        )
        total_hits = 0
        total_misses = 0

        if _table_exists(conn, "prediction_evaluations"):
            evaluation_totals = conn.execute(
                """
                SELECT
                    coalesce(sum(hit), 0) AS total_hits,
                    count(*) - coalesce(sum(hit), 0) AS total_misses
                FROM prediction_evaluations
                """
            ).fetchone()
            total_hits = int(evaluation_totals["total_hits"] or 0) if evaluation_totals else 0
            total_misses = int(evaluation_totals["total_misses"] or 0) if evaluation_totals else 0

        total_pending = max(total_predictions - total_evaluations, 0)
        accuracy = round(total_hits / total_evaluations, 4) if total_evaluations else None
        recalculated_metrics = _recalculate_forecast_metrics(conn)

        latest_run_row = None
        recent_runs: list[dict[str, Any]] = []
        recent_predictions: list[dict[str, Any]] = []

        if _table_exists(conn, "prediction_runs"):
            latest_run_row = conn.execute(
                """
                SELECT
                    r.*,
                    count(p.id) AS predictions_count
                FROM prediction_runs r
                LEFT JOIN predictions p ON p.run_id = r.id
                GROUP BY r.id
                ORDER BY r.created_at DESC
                LIMIT 1
                """
            ).fetchone()

            recent_run_rows = conn.execute(
                """
                SELECT
                    r.*,
                    count(p.id) AS predictions_count
                FROM prediction_runs r
                LEFT JOIN predictions p ON p.run_id = r.id
                GROUP BY r.id
                ORDER BY r.created_at DESC
                LIMIT 5
                """
            ).fetchall()
            recent_runs = [_run_from_row(row) for row in recent_run_rows]

        service_run = _latest_service_run(conn)

        if _table_exists(conn, "predictions"):
            has_outcomes = _table_exists(conn, "board_outcomes")
            has_evaluations = _table_exists(conn, "prediction_evaluations")
            outcome_select = "o.status AS outcome_status" if has_outcomes else "NULL AS outcome_status"
            evaluated_select = (
                "CASE WHEN e.prediction_id IS NULL THEN 0 ELSE 1 END AS evaluated"
                if has_evaluations
                else "0 AS evaluated"
            )
            hit_select = "e.hit AS hit" if has_evaluations else "NULL AS hit"
            outcome_join = "LEFT JOIN board_outcomes o ON o.target_date = p.target_date" if has_outcomes else ""
            evaluation_join = (
                "LEFT JOIN prediction_evaluations e ON e.prediction_id = p.id"
                if has_evaluations
                else ""
            )
            prediction_rows = conn.execute(
                f"""
                SELECT
                    p.target_date,
                    p.horizon_days,
                    p.probability_flight,
                    p.decision,
                    p.confidence,
                    p.created_at,
                    {outcome_select},
                    {evaluated_select},
                    {hit_select}
                FROM predictions p
                {outcome_join}
                {evaluation_join}
                ORDER BY p.created_at DESC, p.horizon_days ASC
                """
            ).fetchall()
            recent_predictions = [
                {
                    "target_date": row["target_date"],
                    "horizon_days": int(row["horizon_days"]),
                    "probability_flight": float(row["probability_flight"]),
                    "decision": row["decision"],
                    "confidence": row["confidence"],
                    "created_at": row["created_at"],
                    "outcome_status": row["outcome_status"],
                    "evaluated": bool(row["evaluated"]),
                    "hit": bool(row["hit"]) if row["hit"] is not None else None,
                }
                for row in prediction_rows
            ]

    latest_run = _run_from_row(latest_run_row) if latest_run_row is not None else None

    if service_run is not None:
        last_seen_at = service_run["finished_at"] or service_run["started_at"]
        if service_run["status"] == "error":
            health_status = "error"
            message = service_run["error"] or "Последний запуск forecast monitor завершился ошибкой."
        elif _is_stale(last_seen_at, settings.background_service_stale_hours):
            health_status = "warning"
            message = f"Последний запуск forecast monitor старше {settings.background_service_stale_hours} часов."
        else:
            health_status = "ok"
            message = "Forecast monitor выполняется и записывает статус запусков."
    elif latest_run is not None:
        last_seen_at = latest_run["created_at"]
        if _is_stale(last_seen_at, settings.background_service_stale_hours):
            health_status = "warning"
            message = f"Последний набор прогнозов старше {settings.background_service_stale_hours} часов."
        elif latest_run["status"] == "partial":
            health_status = "warning"
            message = "Последний набор прогнозов создан не полностью."
        else:
            health_status = "ok"
            message = "Есть актуальные фоновые прогнозы."
    else:
        last_seen_at = None
        health_status = "error"
        message = "В SQLite forecast monitor пока нет запусков и прогнозов."

    return {
        "health": _service_health("Ежедневные прогнозы", health_status, last_seen_at, message),
        "db_path": str(db_path),
        "total_runs": total_runs,
        "total_predictions": total_predictions,
        "total_evaluations": total_evaluations,
        "total_hits": total_hits,
        "total_misses": total_misses,
        "total_pending": total_pending,
        "accuracy": accuracy,
        "recalculated_model_version": recalculated_metrics["model_version"],
        "recalculated_total_evaluations": recalculated_metrics["total_evaluations"],
        "recalculated_total_hits": recalculated_metrics["total_hits"],
        "recalculated_total_misses": recalculated_metrics["total_misses"],
        "recalculated_predicted_yes": recalculated_metrics["predicted_yes"],
        "recalculated_predicted_no": recalculated_metrics["predicted_no"],
        "recalculated_observed_completed": recalculated_metrics["observed_completed"],
        "recalculated_observed_cancelled": recalculated_metrics["observed_cancelled"],
        "recalculated_false_yes": recalculated_metrics["false_yes"],
        "recalculated_false_no": recalculated_metrics["false_no"],
        "recalculated_accuracy": recalculated_metrics["accuracy"],
        "recalculated_brier_score": recalculated_metrics["brier_score"],
        "recalculated_mean_absolute_error": recalculated_metrics["mean_absolute_error"],
        "recalculated_metrics_available": recalculated_metrics["available"],
        "recalculated_metrics_reason": recalculated_metrics["reason"],
        "latest_run": latest_run,
        "recent_runs": recent_runs,
        "recent_predictions": recent_predictions,
    }


def get_admin_services_status() -> dict:
    return {
        "board_collector": get_board_collector_status(),
        "forecast_monitor": get_forecast_monitor_status(),
    }
