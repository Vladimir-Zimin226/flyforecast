import csv
import io
import json
import logging
import sqlite3
import zipfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.config import Settings, get_settings


logger = logging.getLogger("flyforecast.admin_backup")

POSTGRES_EXPORTS = {
    "postgres/users.csv": """
        SELECT
            email,
            name,
            registered_at,
            updated_at,
            prediction_count,
            feedback_count,
            personal_data_consent,
            analytics_consent
        FROM users
        ORDER BY registered_at, email
    """,
    "postgres/feedback.csv": """
        SELECT id, email, message, created_at
        FROM feedback
        ORDER BY created_at, id
    """,
    "postgres/consents.csv": """
        SELECT
            id,
            email,
            event,
            personal_data_consent,
            necessary_cookies_ack,
            analytics_consent,
            created_at
        FROM consents
        ORDER BY created_at, id
    """,
    "postgres/prediction_events.csv": """
        SELECT
            id,
            email,
            request_id,
            target_date,
            horizon_days,
            probability_flight,
            decision,
            confidence,
            model_version,
            data_version,
            session_prediction_number,
            utm_source,
            created_at
        FROM predictions
        ORDER BY created_at, id
    """,
}

FORECAST_MONITOR_TABLES = {
    "service_runs": "forecast_monitor/service_runs.csv",
    "prediction_runs": "forecast_monitor/prediction_runs.csv",
    "predictions": "forecast_monitor/predictions.csv",
    "board_outcomes": "forecast_monitor/board_outcomes.csv",
    "prediction_evaluations": "forecast_monitor/prediction_evaluations.csv",
}

WEATHER_CACHE_TABLES = {
    "weather_forecast_cache": "weather/weather_forecast_cache.csv",
    "weather_provider_state": "weather/weather_provider_state.csv",
}


def _connect_postgres(settings: Settings):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(settings.database_url, row_factory=dict_row)


def _csv_bytes(fieldnames: Iterable[str], rows: Iterable[dict[str, Any]]) -> bytes:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(fieldnames), extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _csv_value(value) for key, value in row.items()})
    return ("\ufeff" + output.getvalue()).encode("utf-8")


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _add_bytes(
    archive: zipfile.ZipFile,
    manifest: list[dict[str, Any]],
    name: str,
    content: bytes,
    *,
    source: str,
) -> None:
    archive.writestr(name, content)
    manifest.append(
        {
            "path": name,
            "source": source,
            "bytes": len(content),
            "status": "ok",
        }
    )


def _add_missing(manifest: list[dict[str, Any]], name: str, source: str, reason: str) -> None:
    manifest.append(
        {
            "path": name,
            "source": source,
            "bytes": 0,
            "status": "missing",
            "reason": reason,
        }
    )


def _add_file_if_exists(
    archive: zipfile.ZipFile,
    manifest: list[dict[str, Any]],
    source_path: Path,
    archive_name: str,
) -> None:
    if not source_path.exists():
        _add_missing(manifest, archive_name, str(source_path), "file does not exist")
        return
    if not source_path.is_file():
        _add_missing(manifest, archive_name, str(source_path), "path is not a file")
        return

    content = source_path.read_bytes()
    _add_bytes(archive, manifest, archive_name, content, source=str(source_path))


def _sqlite_rows(conn: sqlite3.Connection, table_name: str) -> tuple[list[str], list[dict[str, Any]]]:
    rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    if rows:
        return list(rows[0].keys()), [dict(row) for row in rows]
    fieldnames = [row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")]
    return fieldnames, []


def _add_sqlite_table(
    archive: zipfile.ZipFile,
    manifest: list[dict[str, Any]],
    db_path: Path,
    table_name: str,
    archive_name: str,
) -> None:
    if not db_path.exists():
        _add_missing(manifest, archive_name, str(db_path), "sqlite database does not exist")
        return

    try:
        with closing(sqlite3.connect(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            fieldnames, rows = _sqlite_rows(conn, table_name)
    except sqlite3.Error as exc:
        logger.warning("backup_sqlite_export_failed db=%s table=%s error=%s", db_path, table_name, exc)
        _add_missing(manifest, archive_name, str(db_path), f"sqlite export failed: {exc}")
        return

    _add_bytes(
        archive,
        manifest,
        archive_name,
        _csv_bytes(fieldnames, rows),
        source=f"{db_path}:{table_name}",
    )


def _add_forecast_metrics_summary(
    archive: zipfile.ZipFile,
    manifest: list[dict[str, Any]],
    db_path: Path,
) -> None:
    archive_name = "forecast_monitor/metrics_summary.csv"
    if not db_path.exists():
        _add_missing(manifest, archive_name, str(db_path), "sqlite database does not exist")
        return

    try:
        with closing(sqlite3.connect(db_path)) as conn:
            conn.row_factory = sqlite3.Row
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
    except sqlite3.Error as exc:
        logger.warning("backup_metrics_export_failed db=%s error=%s", db_path, exc)
        _add_missing(manifest, archive_name, str(db_path), f"sqlite export failed: {exc}")
        return

    fieldnames = [
        "horizon_bucket",
        "evaluated_count",
        "accuracy",
        "brier_score",
        "mean_absolute_error",
        "mean_predicted_probability",
        "observed_completion_rate",
    ]
    _add_bytes(
        archive,
        manifest,
        archive_name,
        _csv_bytes(fieldnames, [dict(row) for row in rows]),
        source=f"{db_path}:prediction_evaluations",
    )


def _add_postgres_exports(
    archive: zipfile.ZipFile,
    manifest: list[dict[str, Any]],
    settings: Settings,
) -> None:
    try:
        with _connect_postgres(settings) as conn:
            with conn.cursor() as cur:
                for archive_name, query in POSTGRES_EXPORTS.items():
                    cur.execute(query)
                    rows = cur.fetchall()
                    fieldnames = [column.name for column in cur.description or []]
                    _add_bytes(
                        archive,
                        manifest,
                        archive_name,
                        _csv_bytes(fieldnames, rows),
                        source=f"postgres:{archive_name}",
                    )
    except Exception as exc:
        logger.warning("backup_postgres_export_failed error=%s", exc)
        for archive_name in POSTGRES_EXPORTS:
            _add_missing(manifest, archive_name, "postgres", f"postgres export failed: {exc}")


def _jsonl_rows(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    fieldnames: set[str] = set()

    if not path.exists():
        return [], []

    with path.open("r", encoding="utf-8") as file:
        for line in file:
            clean = line.strip()
            if not clean:
                continue
            try:
                payload = json.loads(clean)
            except json.JSONDecodeError:
                payload = {"raw_line": clean}
            if not isinstance(payload, dict):
                payload = {"value": payload}
            rows.append(payload)
            fieldnames.update(payload.keys())

    return sorted(fieldnames), rows


def _add_jsonl_as_csv(
    archive: zipfile.ZipFile,
    manifest: list[dict[str, Any]],
    source_path: Path,
    archive_name: str,
) -> None:
    if not source_path.exists():
        _add_missing(manifest, archive_name, str(source_path), "jsonl file does not exist")
        return

    fieldnames, rows = _jsonl_rows(source_path)
    if not fieldnames:
        fieldnames = ["raw_line"]
    _add_bytes(
        archive,
        manifest,
        archive_name,
        _csv_bytes(fieldnames, rows),
        source=str(source_path),
    )


def build_admin_backup_archive(settings: Settings | None = None) -> tuple[str, bytes]:
    settings = settings or get_settings()
    created_at = datetime.now(timezone.utc)
    timestamp = created_at.strftime("%Y%m%d_%H%M%S")
    filename = f"flyforecast_service_backup_{timestamp}.zip"
    manifest: list[dict[str, Any]] = []
    buffer = io.BytesIO()

    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        _add_postgres_exports(archive, manifest, settings)

        forecast_db_path = Path(settings.forecast_monitor_db_path)
        for table_name, archive_name in FORECAST_MONITOR_TABLES.items():
            _add_sqlite_table(archive, manifest, forecast_db_path, table_name, archive_name)
        _add_forecast_metrics_summary(archive, manifest, forecast_db_path)

        weather_db_path = Path(settings.weather_forecast_cache_path)
        for table_name, archive_name in WEATHER_CACHE_TABLES.items():
            _add_sqlite_table(archive, manifest, weather_db_path, table_name, archive_name)

        _add_file_if_exists(
            archive,
            manifest,
            Path(settings.flight_status_dataset_path),
            "raw/flight_status/kunashir_flight_status_hourly.csv",
        )
        _add_file_if_exists(
            archive,
            manifest,
            Path(settings.flight_status_errors_path),
            "raw/flight_status/collection_errors.csv",
        )
        _add_file_if_exists(
            archive,
            manifest,
            Path(settings.flyforecast_dataset_path),
            "processed/dataset_daily_flights.csv",
        )

        _add_jsonl_as_csv(
            archive,
            manifest,
            Path(settings.prediction_log_path),
            "legacy_jsonl/prediction_logs.csv",
        )
        _add_jsonl_as_csv(
            archive,
            manifest,
            Path(settings.feedback_log_path),
            "legacy_jsonl/feedback_logs.csv",
        )
        _add_jsonl_as_csv(
            archive,
            manifest,
            Path(settings.consent_log_path),
            "legacy_jsonl/consent_logs.csv",
        )

        manifest_payload = {
            "created_at": created_at.isoformat(),
            "archive": filename,
            "files": manifest,
        }
        archive.writestr(
            "manifest.json",
            json.dumps(manifest_payload, ensure_ascii=False, indent=2).encode("utf-8"),
        )

    return filename, buffer.getvalue()
