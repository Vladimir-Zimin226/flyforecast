import csv
import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path

from pipelines.evaluation.forecast_monitor import (
    build_outcomes_from_board,
    init_db,
    upsert_outcomes,
)


FIELDNAMES = [
    "observed_at",
    "flight_date",
    "status_normalized",
    "actual_date",
    "source",
    "flight_numbers",
    "reason_class",
    "raw_row_text",
]


def write_board_rows(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


class ForecastMonitorOutcomeTests(unittest.TestCase):
    def test_same_day_departure_is_completed_and_final_without_lag_wait(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            board_path = Path(tmp) / "board.csv"
            write_board_rows(
                board_path,
                [
                    {
                        "observed_at": "2026-06-03T23:04:35+11:00",
                        "flight_date": "2026-06-03",
                        "status_normalized": "departed",
                        "actual_date": "2026-06-03",
                        "source": "airportus",
                        "flight_numbers": "HZ-3032",
                        "reason_class": "unknown",
                        "raw_row_text": "HZ-3032 | Вылетел",
                    }
                ],
            )

            outcomes = build_outcomes_from_board(board_path, today=date(2026, 6, 3), finalize_lag_days=2)

        self.assertEqual(outcomes[0]["status"], "completed")
        self.assertEqual(outcomes[0]["is_final"], 1)

    def test_next_day_departure_is_cancelled_for_original_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            board_path = Path(tmp) / "board.csv"
            write_board_rows(
                board_path,
                [
                    {
                        "observed_at": "2026-06-07T11:06:38+11:00",
                        "flight_date": "2026-06-06",
                        "status_normalized": "departed",
                        "actual_date": "2026-06-07",
                        "source": "airportus",
                        "flight_numbers": "HZ-3034",
                        "reason_class": "unknown",
                        "raw_row_text": "HZ-3034 | Задержан с 06.06 | Вылетел 07.06",
                    }
                ],
            )

            outcomes = build_outcomes_from_board(board_path, today=date(2026, 6, 7), finalize_lag_days=2)

        self.assertEqual(outcomes[0]["status"], "cancelled")
        self.assertEqual(outcomes[0]["is_final"], 1)
        self.assertIn("completed_next_day", outcomes[0]["evidence_statuses"])
        self.assertEqual(outcomes[0]["reason_class"], "schedule_moved_next_day")

    def test_next_day_delay_is_cancelled_and_final_without_lag_wait(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            board_path = Path(tmp) / "board.csv"
            write_board_rows(
                board_path,
                [
                    {
                        "observed_at": "2026-06-09T10:07:30+11:00",
                        "flight_date": "2026-06-09",
                        "status_normalized": "delayed",
                        "actual_date": "2026-06-10",
                        "source": "airportus",
                        "flight_numbers": "HZ-3032",
                        "reason_class": "unknown",
                        "raw_row_text": "HZ-3032 | Задержан до 10 Июн 09:50",
                    }
                ],
            )

            outcomes = build_outcomes_from_board(board_path, today=date(2026, 6, 9), finalize_lag_days=2)

        self.assertEqual(outcomes[0]["status"], "cancelled")
        self.assertEqual(outcomes[0]["is_final"], 1)
        self.assertIn("delayed_next_day", outcomes[0]["evidence_statuses"])

    def test_new_final_outcome_can_correct_old_final_status(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_db(conn)

        upsert_outcomes(
            conn,
            [
                {
                    "target_date": "2026-06-06",
                    "status": "completed",
                    "is_final": 1,
                    "finalized_at": "2026-06-08T00:00:00",
                    "first_observed_at": "2026-06-06T00:00:00",
                    "last_observed_at": "2026-06-08T00:00:00",
                    "evidence_count": 1,
                    "evidence_statuses": "departed",
                    "source_types": "airportus",
                    "flight_numbers": "HZ-3034",
                    "reason_class": "unknown",
                    "raw_evidence_sample": "old",
                }
            ],
        )
        upsert_outcomes(
            conn,
            [
                {
                    "target_date": "2026-06-06",
                    "status": "cancelled",
                    "is_final": 1,
                    "finalized_at": "2026-06-09T00:00:00",
                    "first_observed_at": "2026-06-06T00:00:00",
                    "last_observed_at": "2026-06-09T00:00:00",
                    "evidence_count": 2,
                    "evidence_statuses": "completed_next_day;departed",
                    "source_types": "airportus",
                    "flight_numbers": "HZ-3034",
                    "reason_class": "schedule_moved_next_day",
                    "raw_evidence_sample": "new",
                }
            ],
        )

        row = conn.execute("SELECT status, is_final, reason_class FROM board_outcomes WHERE target_date = ?", ("2026-06-06",)).fetchone()

        self.assertEqual(row["status"], "cancelled")
        self.assertEqual(row["is_final"], 1)
        self.assertEqual(row["reason_class"], "schedule_moved_next_day")


if __name__ == "__main__":
    unittest.main()
