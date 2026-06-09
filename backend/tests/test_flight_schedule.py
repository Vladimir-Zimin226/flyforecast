import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from app.schemas import FlightScheduleSnapshot
from app.services.flight_schedule import get_flight_schedule_for_date


FIELDNAMES = [
    "observed_at",
    "direction",
    "flight_date",
    "flight_time",
    "flight_numbers",
    "status_normalized",
    "actual_date",
]


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


class FlightScheduleTests(unittest.TestCase):
    def test_extracts_schedule_window_from_latest_board_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "board.csv"
            write_rows(
                path,
                [
                    {
                        "observed_at": "2026-06-09T09:00:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-09",
                        "flight_time": "10:20",
                        "flight_numbers": "HZ-3032",
                        "status_normalized": "scheduled",
                        "actual_date": "2026-06-09",
                    },
                    {
                        "observed_at": "2026-06-09T09:00:00+11:00",
                        "direction": "arrival",
                        "flight_date": "2026-06-09",
                        "flight_time": "13:45",
                        "flight_numbers": "HZ-3033",
                        "status_normalized": "scheduled",
                        "actual_date": "2026-06-09",
                    },
                ],
            )

            schedule = get_flight_schedule_for_date(date(2026, 6, 9), board_path=path)

        self.assertIsInstance(schedule, FlightScheduleSnapshot)
        self.assertTrue(schedule.available)
        self.assertEqual(schedule.first_departure_hour, 10)
        self.assertEqual(schedule.first_scheduled_hour, 10)
        self.assertEqual(schedule.last_scheduled_hour, 13)
        self.assertEqual(schedule.schedule_window_start_hour, 9)
        self.assertEqual(schedule.schedule_window_end_hour, 14)
        self.assertFalse(schedule.moved_next_day)

    def test_marks_next_day_delay_from_board_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "board.csv"
            write_rows(
                path,
                [
                    {
                        "observed_at": "2026-06-09T10:07:30+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-09",
                        "flight_time": "10:20",
                        "flight_numbers": "HZ-3032",
                        "status_normalized": "delayed",
                        "actual_date": "2026-06-10",
                    }
                ],
            )

            schedule = get_flight_schedule_for_date(date(2026, 6, 9), board_path=path)

        self.assertTrue(schedule.available)
        self.assertTrue(schedule.moved_next_day)
        self.assertFalse(schedule.completed_same_day)
        self.assertEqual(schedule.status_summary, "delayed")

    @patch("app.services.flight_schedule.get_settings")
    def test_missing_board_file_returns_unavailable(self, get_settings_mock) -> None:
        get_settings_mock.return_value.flight_status_dataset_path = "missing.csv"

        schedule = get_flight_schedule_for_date(date(2026, 6, 9), board_path=Path("missing.csv"))

        self.assertFalse(schedule.available)
        self.assertIn("No board schedule rows", schedule.reason or "")


if __name__ == "__main__":
    unittest.main()
