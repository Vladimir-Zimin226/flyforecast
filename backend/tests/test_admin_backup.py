import io
import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.config import Settings
from app.services import admin_backup


class FakeCursor:
    def __init__(self) -> None:
        self.description = []
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def execute(self, query: str) -> None:
        if "FROM users" in query:
            self.description = [
                SimpleNamespace(name="email"),
                SimpleNamespace(name="name"),
                SimpleNamespace(name="prediction_count"),
            ]
            self._rows = [
                {
                    "email": "user@example.com",
                    "name": "Test User",
                    "prediction_count": 3,
                }
            ]
        else:
            self.description = [SimpleNamespace(name="id")]
            self._rows = []

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return FakeCursor()


class AdminBackupTests(unittest.TestCase):
    def test_backup_archive_contains_service_exports_without_password_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            forecast_db = root / "forecast_monitor.sqlite"
            weather_db = root / "weather_cache.sqlite"
            board_csv = root / "board.csv"
            errors_csv = root / "errors.csv"
            sakhalin_board_csv = root / "sakhalin_board.csv"
            sakhalin_weather_csv = root / "sakhalin_weather.csv"
            sakhalin_errors_csv = root / "sakhalin_errors.csv"
            dataset_csv = root / "dataset.csv"
            prediction_log = root / "prediction_logs.jsonl"
            feedback_log = root / "feedback_logs.jsonl"
            consent_log = root / "consent_logs.jsonl"

            with sqlite3.connect(forecast_db) as conn:
                conn.execute("CREATE TABLE service_runs (id INTEGER, status TEXT)")
                conn.execute("INSERT INTO service_runs VALUES (1, 'success')")
                conn.execute("CREATE TABLE prediction_runs (id INTEGER)")
                conn.execute("CREATE TABLE predictions (id INTEGER, decision TEXT)")
                conn.execute("CREATE TABLE board_outcomes (target_date TEXT, status TEXT)")
                conn.execute(
                    """
                    CREATE TABLE prediction_evaluations (
                        horizon_bucket TEXT,
                        hit INTEGER,
                        brier_score REAL,
                        absolute_error REAL,
                        probability_flight REAL,
                        outcome_binary INTEGER
                    )
                    """
                )
                conn.execute("INSERT INTO prediction_evaluations VALUES ('0', 1, 0.04, 0.2, 0.8, 1)")

            with sqlite3.connect(weather_db) as conn:
                conn.execute(
                    """
                    CREATE TABLE weather_forecast_cache (
                        target_date TEXT,
                        provider TEXT,
                        payload_json TEXT,
                        fetched_at TEXT
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO weather_forecast_cache VALUES ('2026-06-14', 'test', '{}', '2026-06-13T00:00:00Z')"
                )
                conn.execute(
                    """
                    CREATE TABLE weather_provider_state (
                        provider TEXT,
                        failure_count INTEGER
                    )
                    """
                )

            board_csv.write_text("flight_date,status_normalized\n2026-06-14,departed\n", encoding="utf-8")
            errors_csv.write_text("observed_at,error\n2026-06-14T00:00:00,none\n", encoding="utf-8")
            sakhalin_board_csv.write_text(
                "observed_at,airport_code,route,status_normalized\n"
                "2026-06-20T23:37:07+11:00,OHH,Оха,scheduled\n",
                encoding="utf-8",
            )
            sakhalin_weather_csv.write_text(
                "observed_at,airport_code,forecast_time,visibility\n"
                "2026-06-20T23:37:07+11:00,OHH,2026-06-21T09:00,10000\n",
                encoding="utf-8",
            )
            sakhalin_errors_csv.write_text("observed_at,source,error\n", encoding="utf-8")
            dataset_csv.write_text("date,outcome\n2026-06-14,completed\n", encoding="utf-8")
            prediction_log.write_text(json.dumps({"request_id": "abc", "decision": "yes"}) + "\n", encoding="utf-8")
            feedback_log.write_text("", encoding="utf-8")
            consent_log.write_text("", encoding="utf-8")

            settings = Settings(
                database_url="postgresql://test",
                forecast_monitor_db_path=str(forecast_db),
                weather_forecast_cache_path=str(weather_db),
                flight_status_dataset_path=str(board_csv),
                flight_status_errors_path=str(errors_csv),
                sakhalin_airports_board_output=str(sakhalin_board_csv),
                sakhalin_airports_weather_output=str(sakhalin_weather_csv),
                sakhalin_airports_errors_output=str(sakhalin_errors_csv),
                flyforecast_dataset_path=str(dataset_csv),
                prediction_log_path=str(prediction_log),
                feedback_log_path=str(feedback_log),
                consent_log_path=str(consent_log),
            )

            with patch.object(admin_backup, "_connect_postgres", return_value=FakeConnection()):
                filename, content = admin_backup.build_admin_backup_archive(settings)

        self.assertTrue(filename.startswith("flyforecast_service_backup_"))
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            names = set(archive.namelist())
            self.assertIn("manifest.json", names)
            self.assertIn("postgres/users.csv", names)
            self.assertIn("forecast_monitor/predictions.csv", names)
            self.assertIn("forecast_monitor/metrics_summary.csv", names)
            self.assertIn("weather/weather_forecast_cache.csv", names)
            self.assertIn("raw/flight_status/kunashir_flight_status_hourly.csv", names)
            self.assertIn("raw/sakhalin_airports/sakhalin_airport_board_hourly.csv", names)
            self.assertIn("raw/sakhalin_airports/sakhalin_airport_weather_hourly.csv", names)
            self.assertIn("raw/sakhalin_airports/collection_errors.csv", names)
            self.assertIn("legacy_jsonl/prediction_logs.csv", names)

            users_csv = archive.read("postgres/users.csv").decode("utf-8-sig")
            self.assertIn("user@example.com", users_csv)
            self.assertNotIn("password_hash", users_csv)

            manifest = json.loads(archive.read("manifest.json"))
            self.assertEqual(manifest["archive"], filename)


if __name__ == "__main__":
    unittest.main()
