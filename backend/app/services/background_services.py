import csv
import sqlite3
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app.config import get_settings


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
                    {evaluated_select}
                FROM predictions p
                {outcome_join}
                {evaluation_join}
                ORDER BY p.created_at DESC, p.horizon_days ASC
                LIMIT 12
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
        "latest_run": latest_run,
        "recent_runs": recent_runs,
        "recent_predictions": recent_predictions,
    }


def get_admin_services_status() -> dict:
    return {
        "board_collector": get_board_collector_status(),
        "forecast_monitor": get_forecast_monitor_status(),
    }
