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
    "actual_time",
]


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


class FlightScheduleTests(unittest.TestCase):
    def test_prefers_arrival_rows_for_flight_fact_and_window(self) -> None:
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
        self.assertEqual(schedule.first_departure_hour, 13)
        self.assertEqual(schedule.first_scheduled_hour, 13)
        self.assertEqual(schedule.last_scheduled_hour, 13)
        self.assertEqual(schedule.schedule_window_start_hour, 12)
        self.assertEqual(schedule.schedule_window_end_hour, 14)
        self.assertFalse(schedule.moved_next_day)
        self.assertFalse(schedule.completed_same_day)
        self.assertEqual(schedule.total_flights, 1)
        self.assertEqual(schedule.completed_flights, 0)
        self.assertEqual(schedule.pending_flights, 1)
        self.assertEqual(schedule.active_flight_index, 1)
        self.assertEqual(schedule.active_flight_hour, 13)
        self.assertEqual(schedule.active_flight_numbers, "HZ-3033")

    def test_arrival_rows_define_same_day_flight_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "board.csv"
            write_rows(
                path,
                [
                    {
                        "observed_at": "2026-06-14T10:30:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "10:20",
                        "flight_numbers": "HZ-3032",
                        "status_normalized": "delayed",
                        "actual_date": "2026-06-14",
                    },
                    {
                        "observed_at": "2026-06-14T10:30:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "11:30",
                        "flight_numbers": "HZ-3036",
                        "status_normalized": "scheduled",
                        "actual_date": "2026-06-14",
                    },
                    {
                        "observed_at": "2026-06-14T10:30:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-15",
                        "flight_time": "16:15",
                        "flight_numbers": "HZ-3036",
                        "status_normalized": "scheduled",
                        "actual_date": "2026-06-15",
                    },
                    {
                        "observed_at": "2026-06-14T10:30:00+11:00",
                        "direction": "arrival",
                        "flight_date": "2026-06-14",
                        "flight_time": "13:45",
                        "flight_numbers": "HZ-3033",
                        "status_normalized": "delayed",
                        "actual_date": "2026-06-14",
                    },
                    {
                        "observed_at": "2026-06-14T10:30:00+11:00",
                        "direction": "arrival",
                        "flight_date": "2026-06-14",
                        "flight_time": "14:55",
                        "flight_numbers": "HZ-3037",
                        "status_normalized": "scheduled",
                        "actual_date": "2026-06-14",
                    },
                ],
            )

            schedule = get_flight_schedule_for_date(date(2026, 6, 14), board_path=path)

        self.assertTrue(schedule.available)
        self.assertFalse(schedule.moved_next_day)
        self.assertFalse(schedule.completed_same_day)
        self.assertEqual(schedule.total_flights, 2)
        self.assertEqual(schedule.completed_flights, 0)
        self.assertEqual(schedule.pending_flights, 2)
        self.assertEqual(schedule.active_flight_index, 1)
        self.assertEqual(schedule.active_flight_hour, 13)
        self.assertEqual(schedule.active_flight_numbers, "HZ-3033")
        self.assertEqual([flight.direction for flight in schedule.flights], ["arrival", "arrival"])

    def test_current_arrival_snapshot_replaces_stale_same_day_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "board.csv"
            write_rows(
                path,
                [
                    {
                        "observed_at": "2026-06-14T14:10:00+11:00",
                        "direction": "arrival",
                        "flight_date": "2026-06-14",
                        "flight_time": "13:45",
                        "flight_numbers": "HZ-3033",
                        "status_normalized": "arrived",
                        "actual_date": "2026-06-14",
                        "actual_time": "14:05",
                    },
                    {
                        "observed_at": "2026-06-14T14:10:00+11:00",
                        "direction": "arrival",
                        "flight_date": "2026-06-14",
                        "flight_time": "18:05",
                        "flight_numbers": "HZ-3035",
                        "status_normalized": "scheduled",
                        "actual_date": "2026-06-14",
                        "actual_time": "18:05",
                    },
                    {
                        "observed_at": "2026-06-14T15:00:00+11:00",
                        "direction": "arrival",
                        "flight_date": "2026-06-14",
                        "flight_time": "13:45",
                        "flight_numbers": "HZ-3033",
                        "status_normalized": "delayed",
                        "actual_date": "2026-06-14",
                        "actual_time": "15:45",
                    },
                    {
                        "observed_at": "2026-06-14T15:00:00+11:00",
                        "direction": "arrival",
                        "flight_date": "2026-06-14",
                        "flight_time": "14:55",
                        "flight_numbers": "HZ-3037",
                        "status_normalized": "delayed",
                        "actual_date": "2026-06-14",
                        "actual_time": "15:55",
                    },
                    {
                        "observed_at": "2026-06-14T15:00:00+11:00",
                        "direction": "arrival",
                        "flight_date": "2026-06-15",
                        "flight_time": "19:40",
                        "flight_numbers": "HZ-3037",
                        "status_normalized": "scheduled",
                        "actual_date": "2026-06-15",
                        "actual_time": "19:40",
                    },
                ],
            )

            schedule = get_flight_schedule_for_date(date(2026, 6, 14), board_path=path)

        self.assertTrue(schedule.available)
        self.assertFalse(schedule.moved_next_day)
        self.assertFalse(schedule.completed_same_day)
        self.assertEqual(schedule.total_flights, 2)
        self.assertEqual(schedule.completed_flights, 0)
        self.assertEqual(schedule.pending_flights, 2)
        self.assertEqual(schedule.active_flight_index, 1)
        self.assertEqual(schedule.active_flight_numbers, "HZ-3033")
        self.assertEqual(schedule.active_flight_time, "15:45")
        self.assertEqual(schedule.active_flight_hour, 15)
        self.assertEqual(schedule.first_departure_hour, 15)
        self.assertEqual(schedule.schedule_window_start_hour, 14)
        self.assertEqual(schedule.schedule_window_end_hour, 16)
        self.assertEqual([flight.flight_numbers for flight in schedule.flights], ["HZ-3033", "HZ-3037"])

    def test_arrived_first_arrival_moves_forecast_to_next_arrival(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "board.csv"
            write_rows(
                path,
                [
                    {
                        "observed_at": "2026-06-14T14:10:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "10:20",
                        "flight_numbers": "HZ-3032",
                        "status_normalized": "departed",
                        "actual_date": "2026-06-14",
                    },
                    {
                        "observed_at": "2026-06-14T14:10:00+11:00",
                        "direction": "arrival",
                        "flight_date": "2026-06-14",
                        "flight_time": "13:45",
                        "flight_numbers": "HZ-3033",
                        "status_normalized": "arrived",
                        "actual_date": "2026-06-14",
                    },
                    {
                        "observed_at": "2026-06-14T14:10:00+11:00",
                        "direction": "arrival",
                        "flight_date": "2026-06-14",
                        "flight_time": "14:55",
                        "flight_numbers": "HZ-3037",
                        "status_normalized": "scheduled",
                        "actual_date": "2026-06-14",
                    },
                ],
            )

            schedule = get_flight_schedule_for_date(date(2026, 6, 14), board_path=path)

        self.assertFalse(schedule.moved_next_day)
        self.assertFalse(schedule.completed_same_day)
        self.assertEqual(schedule.total_flights, 2)
        self.assertEqual(schedule.completed_flights, 1)
        self.assertEqual(schedule.pending_flights, 1)
        self.assertEqual(schedule.active_flight_index, 2)
        self.assertEqual(schedule.first_departure_hour, 14)
        self.assertEqual(schedule.active_flight_numbers, "HZ-3037")

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
        self.assertEqual(schedule.total_flights, 1)
        self.assertEqual(schedule.unavailable_flights, 1)

    def test_completed_first_flight_moves_forecast_to_next_flight(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "board.csv"
            write_rows(
                path,
                [
                    {
                        "observed_at": "2026-06-14T12:00:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "10:20",
                        "flight_numbers": "HZ-3032",
                        "status_normalized": "departed",
                        "actual_date": "2026-06-14",
                    },
                    {
                        "observed_at": "2026-06-14T12:00:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "15:10",
                        "flight_numbers": "HZ-3034",
                        "status_normalized": "scheduled",
                        "actual_date": "2026-06-14",
                    },
                ],
            )

            schedule = get_flight_schedule_for_date(date(2026, 6, 14), board_path=path)

        self.assertTrue(schedule.available)
        self.assertFalse(schedule.moved_next_day)
        self.assertFalse(schedule.completed_same_day)
        self.assertEqual(schedule.total_flights, 2)
        self.assertEqual(schedule.completed_flights, 1)
        self.assertEqual(schedule.pending_flights, 1)
        self.assertEqual(schedule.active_flight_index, 2)
        self.assertEqual(schedule.first_departure_hour, 15)
        self.assertEqual(schedule.active_flight_numbers, "HZ-3034")

    def test_all_completed_flights_mark_day_completed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "board.csv"
            write_rows(
                path,
                [
                    {
                        "observed_at": "2026-06-14T18:00:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "10:20",
                        "flight_numbers": "HZ-3032",
                        "status_normalized": "departed",
                        "actual_date": "2026-06-14",
                    },
                    {
                        "observed_at": "2026-06-14T18:00:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "15:10",
                        "flight_numbers": "HZ-3034",
                        "status_normalized": "departed",
                        "actual_date": "2026-06-14",
                    },
                ],
            )

            schedule = get_flight_schedule_for_date(date(2026, 6, 14), board_path=path)

        self.assertTrue(schedule.completed_same_day)
        self.assertFalse(schedule.moved_next_day)
        self.assertEqual(schedule.total_flights, 2)
        self.assertEqual(schedule.completed_flights, 2)
        self.assertIsNone(schedule.active_flight_index)

    def test_completed_then_next_flight_delay_marks_active_flight_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "board.csv"
            write_rows(
                path,
                [
                    {
                        "observed_at": "2026-06-14T12:00:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "10:20",
                        "flight_numbers": "HZ-3032",
                        "status_normalized": "departed",
                        "actual_date": "2026-06-14",
                    },
                    {
                        "observed_at": "2026-06-14T12:00:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "15:10",
                        "flight_numbers": "HZ-3034",
                        "status_normalized": "delayed",
                        "actual_date": "2026-06-15",
                    },
                ],
            )

            schedule = get_flight_schedule_for_date(date(2026, 6, 14), board_path=path)

        self.assertTrue(schedule.moved_next_day)
        self.assertFalse(schedule.completed_same_day)
        self.assertEqual(schedule.completed_flights, 1)
        self.assertEqual(schedule.unavailable_flights, 1)
        self.assertEqual(schedule.active_flight_index, 2)
        self.assertEqual(schedule.active_flight_status, "delayed")

    def test_first_cancelled_keeps_first_flight_as_failed_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "board.csv"
            write_rows(
                path,
                [
                    {
                        "observed_at": "2026-06-14T09:00:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "10:20",
                        "flight_numbers": "HZ-3032",
                        "status_normalized": "cancelled",
                        "actual_date": "2026-06-14",
                    },
                    {
                        "observed_at": "2026-06-14T09:00:00+11:00",
                        "direction": "departure",
                        "flight_date": "2026-06-14",
                        "flight_time": "15:10",
                        "flight_numbers": "HZ-3034",
                        "status_normalized": "scheduled",
                        "actual_date": "2026-06-14",
                    },
                ],
            )

            schedule = get_flight_schedule_for_date(date(2026, 6, 14), board_path=path)

        self.assertTrue(schedule.moved_next_day)
        self.assertFalse(schedule.completed_same_day)
        self.assertEqual(schedule.completed_flights, 0)
        self.assertEqual(schedule.unavailable_flights, 1)
        self.assertEqual(schedule.pending_flights, 1)
        self.assertEqual(schedule.active_flight_index, 1)
        self.assertEqual(schedule.active_flight_status, "cancelled")

    @patch("app.services.flight_schedule.get_settings")
    def test_missing_board_file_returns_unavailable(self, get_settings_mock) -> None:
        get_settings_mock.return_value.flight_status_dataset_path = "missing.csv"

        schedule = get_flight_schedule_for_date(date(2026, 6, 9), board_path=Path("missing.csv"))

        self.assertFalse(schedule.available)
        self.assertIn("No board schedule rows", schedule.reason or "")


if __name__ == "__main__":
    unittest.main()
