import unittest
import sys
import types
from datetime import date
from unittest.mock import AsyncMock, Mock, patch

psycopg_stub = types.ModuleType("psycopg")
psycopg_stub.Connection = object
psycopg_stub.connect = Mock()
psycopg_rows_stub = types.ModuleType("psycopg.rows")
psycopg_rows_stub.dict_row = object()
sys.modules.setdefault("psycopg", psycopg_stub)
sys.modules.setdefault("psycopg.rows", psycopg_rows_stub)

from app import main
from app.schemas import FlightScheduleSnapshot, HistoricalSnapshot, WeatherSnapshot


class PredictEndpointExplanationTests(unittest.IsolatedAsyncioTestCase):
    async def test_predict_passes_schedule_to_explanation_generator(self) -> None:
        target_date = date.today()
        schedule = FlightScheduleSnapshot(
            source="test-board",
            available=True,
            moved_next_day=True,
            observed_at="2026-06-10T10:00:00+11:00",
        )
        weather = WeatherSnapshot(
            source="test-weather",
            available=True,
            flight_window_available=True,
        )
        history = HistoricalSnapshot(
            source="test-history",
            similar_days_count=9,
            completed_count=5,
            cancelled_count=4,
            historical_probability_flight=0.5,
        )

        explanation_mock = Mock(return_value="По табло рейс отменен для этой даты.")
        with patch.object(main, "get_flight_schedule_for_date", return_value=schedule):
            with patch.object(main, "fetch_weather_for_date", AsyncMock(return_value=weather)):
                with patch.object(main, "get_historical_snapshot", return_value=history):
                    with patch.object(main, "generate_user_explanation", explanation_mock):
                        with patch.object(main, "log_prediction"):
                            result = await main.predict(
                                user=None,
                                target_date=target_date,
                                session_prediction_number=1,
                                utm_source=None,
                            )

        self.assertEqual(result.explanation, "По табло рейс отменен для этой даты.")
        self.assertIs(explanation_mock.call_args.kwargs["schedule"], schedule)

    async def test_completed_board_status_uses_board_forecast_label(self) -> None:
        target_date = date.today()
        schedule = FlightScheduleSnapshot(
            source="test-board",
            available=True,
            completed_same_day=True,
            observed_at="2026-06-10T14:00:00+11:00",
        )
        weather = WeatherSnapshot(
            source="test-weather",
            available=True,
            flight_window_available=True,
        )
        history = HistoricalSnapshot(
            source="test-history",
            similar_days_count=9,
            completed_count=5,
            cancelled_count=4,
            historical_probability_flight=0.5,
        )

        explanation_mock = Mock(return_value="По табло рейс уже выполнен для этой даты.")
        with patch.object(main, "get_flight_schedule_for_date", return_value=schedule):
            with patch.object(main, "fetch_weather_for_date", AsyncMock(return_value=weather)):
                with patch.object(main, "get_historical_snapshot", return_value=history):
                    with patch.object(main, "generate_user_explanation", explanation_mock):
                        with patch.object(main, "log_prediction"):
                            result = await main.predict(
                                user=None,
                                target_date=target_date,
                                session_prediction_number=1,
                                utm_source=None,
                            )

        self.assertEqual(result.explanation, "По табло рейс уже выполнен для этой даты.")
        self.assertEqual(result.forecast_mode_label, "Статус по табло аэропорта")
        self.assertIs(explanation_mock.call_args.kwargs["schedule"], schedule)

    async def test_moved_next_day_board_status_does_not_require_weather(self) -> None:
        target_date = date.today()
        schedule = FlightScheduleSnapshot(
            source="test-board",
            available=True,
            moved_next_day=True,
            observed_at="2026-06-20T10:00:00+11:00",
            total_flights=1,
            unavailable_flights=1,
        )
        weather = WeatherSnapshot(
            source="test-weather",
            available=False,
            reason="weather service unavailable",
        )
        history = HistoricalSnapshot(
            source="test-history",
            similar_days_count=9,
            completed_count=5,
            cancelled_count=4,
            historical_probability_flight=0.5,
        )

        explanation_mock = Mock(return_value="По табло рейс отменен для этой даты.")
        with patch.object(main, "get_flight_schedule_for_date", return_value=schedule):
            with patch.object(main, "fetch_weather_for_date", AsyncMock(return_value=weather)):
                with patch.object(main, "get_historical_snapshot", return_value=history):
                    with patch.object(main, "generate_user_explanation", explanation_mock):
                        with patch.object(main, "log_prediction"):
                            result = await main.predict(
                                user=None,
                                target_date=target_date,
                                session_prediction_number=1,
                                utm_source=None,
                            )

        self.assertEqual(result.decision, "no")
        self.assertEqual(result.probability_flight, 0.0)
        self.assertEqual(result.forecast_mode_label, "Статус по табло аэропорта")
        self.assertEqual(result.explanation, "По табло рейс отменен для этой даты.")


if __name__ == "__main__":
    unittest.main()
