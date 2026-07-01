import unittest

from app.config import Settings
from app.schemas import FlightScheduleFlight, FlightScheduleSnapshot, HistoricalSnapshot, WeatherSnapshot
from app.services.predictor import calculate_probability, has_compound_humidity_risk, make_decision
from app.services.weather import _is_flight_opportunity_hour


def seasonal_late_may_history() -> HistoricalSnapshot:
    return HistoricalSnapshot(
        source="test",
        similar_days_count=140,
        completed_count=85,
        cancelled_count=55,
        historical_probability_flight=0.6143,
        decade_probability_flight=0.6667,
    )


class PredictorBaselineTests(unittest.TestCase):
    def test_workable_window_offsets_soft_low_cloud_signals(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            relative_humidity_2m=97,
            cloud_cover=100,
            cloud_cover_low=100,
            wind_speed_10m=18,
            wind_gusts_10m=42,
            dew_point_spread=0.8,
            precipitation=0,
            weather_code=3,
            visibility=None,
            fog_low_cloud_risk_level="high",
        )

        probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=seasonal_late_may_history(),
        )

        self.assertGreaterEqual(probability, 0.55)
        self.assertEqual(make_decision(probability, horizon_days=0), "yes")

    def test_explicit_low_visibility_still_blocks_close_date_yes(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=False,
            relative_humidity_2m=98,
            cloud_cover=100,
            cloud_cover_low=100,
            wind_speed_10m=18,
            wind_gusts_10m=42,
            dew_point_spread=0.8,
            precipitation=0,
            weather_code=45,
            visibility=1200,
            fog_low_cloud_risk_level="high",
        )

        probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=seasonal_late_may_history(),
        )

        self.assertLess(probability, 0.55)
        self.assertEqual(make_decision(probability, horizon_days=0), "no")

    def test_extreme_fog_proxy_blocks_close_date_yes_without_fog_code(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            relative_humidity_2m=97,
            cloud_cover=100,
            cloud_cover_low=100,
            wind_speed_10m=18,
            wind_gusts_10m=57,
            dew_point_spread=0.8,
            precipitation=0,
            weather_code=3,
            visibility=240,
            fog_low_cloud_risk_level="high",
        )

        probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=seasonal_late_may_history(),
        )

        self.assertLess(probability, 0.55)
        self.assertEqual(make_decision(probability, horizon_days=0), "no")

    def test_extreme_visibility_without_gust_signal_blocks_close_date_yes(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            relative_humidity_2m=97,
            cloud_cover=100,
            cloud_cover_low=100,
            wind_speed_10m=18,
            wind_gusts_10m=31,
            dew_point_spread=0.8,
            precipitation=0,
            weather_code=3,
            visibility=240,
            fog_low_cloud_risk_level="high",
        )

        probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=seasonal_late_may_history(),
        )

        self.assertLess(probability, 0.58)
        self.assertEqual(make_decision(probability, horizon_days=0), "no")

    def test_low_cloud_alone_does_not_remove_flight_window(self) -> None:
        settings = Settings()
        row = {
            "visibility": None,
            "cloud_cover_low": 100,
            "wind_gusts_10m": 42,
            "precipitation": 0,
            "weather_code": 3,
        }

        self.assertTrue(_is_flight_opportunity_hour(row, settings))

    def test_low_visibility_without_fog_code_removes_flight_window(self) -> None:
        settings = Settings()
        row = {
            "visibility": 300,
            "cloud_cover_low": 100,
            "wind_gusts_10m": 42,
            "precipitation": 0,
            "weather_code": 51,
        }

        self.assertFalse(_is_flight_opportunity_hour(row, settings))

    def test_next_day_board_move_caps_probability(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            flight_window_start_hour=8,
            flight_window_end_hour=20,
            visibility=12000,
            cloud_cover_low=20,
            wind_gusts_10m=28,
            weather_code=3,
            fog_low_cloud_risk_level="low",
        )
        schedule = FlightScheduleSnapshot(
            source="test",
            available=True,
            first_departure_hour=10,
            first_scheduled_hour=10,
            last_scheduled_hour=14,
            moved_next_day=True,
            status_summary="delayed",
        )

        probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=seasonal_late_may_history(),
            schedule=schedule,
        )

        self.assertEqual(probability, 0.0)
        self.assertEqual(make_decision(probability, horizon_days=0), "no")

    def test_late_weather_window_does_not_raise_close_date_to_yes(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            flight_window_start_hour=14,
            flight_window_end_hour=20,
            flight_window_hours=7,
            visibility=390,
            cloud_cover_low=47,
            wind_gusts_10m=37,
            weather_code=2,
            fog_low_cloud_risk_level="medium",
        )
        schedule = FlightScheduleSnapshot(
            source="test",
            available=True,
            first_departure_hour=10,
            first_scheduled_hour=10,
            last_scheduled_hour=13,
            moved_next_day=False,
            status_summary="scheduled",
        )

        probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=HistoricalSnapshot(
                source="test",
                similar_days_count=84,
                completed_count=51,
                cancelled_count=33,
                historical_probability_flight=0.6071,
                decade_probability_flight=0.5294,
            ),
            schedule=schedule,
        )

        self.assertLess(probability, 0.58)
        self.assertEqual(make_decision(probability, horizon_days=0), "no")

    def test_same_day_operational_stress_reduces_probability_without_board_final_status(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            flight_window_start_hour=12,
            flight_window_end_hour=20,
            flight_window_hours=9,
            visibility=9000,
            flight_window_visibility=9000,
            cloud_cover=100,
            cloud_cover_low=100,
            wind_gusts_10m=38,
            weather_code=3,
            fog_low_cloud_risk_level="low",
            hourly_rows=[
                {"hour": 8, "visibility": 180, "cloud_cover": 100, "cloud_cover_low": 100, "weather_code": 3},
                {"hour": 9, "visibility": 700, "cloud_cover": 100, "cloud_cover_low": 100, "weather_code": 3},
                {"hour": 12, "visibility": 9000, "cloud_cover": 100, "cloud_cover_low": 100, "weather_code": 3},
                {"hour": 13, "visibility": 12000, "cloud_cover": 100, "cloud_cover_low": 100, "weather_code": 3},
            ],
        )
        schedule = FlightScheduleSnapshot(
            source="test-board",
            available=True,
            first_departure_hour=14,
            first_scheduled_hour=14,
            last_scheduled_hour=18,
            moved_next_day=False,
            completed_same_day=False,
            status_summary="delayed;scheduled",
            total_flights=2,
            pending_flights=2,
            flights=[
                FlightScheduleFlight(status="delayed", state="pending", hour=14),
                FlightScheduleFlight(status="scheduled", state="pending", hour=18),
            ],
        )

        stressed_probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=seasonal_late_may_history(),
            schedule=schedule,
        )
        unstressed_probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=seasonal_late_may_history(),
            schedule=FlightScheduleSnapshot(
                source="test-board",
                available=True,
                first_departure_hour=14,
                first_scheduled_hour=14,
                last_scheduled_hour=18,
                status_summary="scheduled",
                total_flights=2,
                pending_flights=2,
                flights=[
                    FlightScheduleFlight(status="scheduled", state="pending", hour=14),
                    FlightScheduleFlight(status="scheduled", state="pending", hour=18),
                ],
            ),
        )

        self.assertLess(stressed_probability, unstressed_probability)
        self.assertLess(stressed_probability, 0.58)
        self.assertEqual(make_decision(stressed_probability, horizon_days=0), "no")

    def test_delayed_active_flight_after_weather_window_blocks_close_date_yes(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            flight_window_start_hour=17,
            flight_window_end_hour=19,
            flight_window_hours=3,
            visibility=7450,
            flight_window_visibility=7450,
            cloud_cover=100,
            cloud_cover_low=100,
            flight_window_cloud_cover_low=100,
            relative_humidity_2m=95,
            dew_point_spread=0.8,
            precipitation=0.1,
            wind_speed_10m=5,
            wind_gusts_10m=20,
            weather_code=51,
            fog_low_cloud_risk_level="medium",
            flight_window_fog_low_cloud_risk_level="medium",
        )
        history = HistoricalSnapshot(
            source="test",
            similar_days_count=75,
            completed_count=47,
            cancelled_count=28,
            historical_probability_flight=0.6234,
            decade_probability_flight=0.75,
        )
        schedule = FlightScheduleSnapshot(
            source="test-board",
            available=True,
            first_departure_hour=17,
            first_scheduled_hour=20,
            last_scheduled_hour=20,
            moved_next_day=False,
            completed_same_day=False,
            status_summary="delayed",
            total_flights=1,
            pending_flights=1,
            active_flight_hour=20,
            active_flight_time="20:55",
            active_flight_status="delayed",
            flights=[
                FlightScheduleFlight(status="delayed", state="pending", hour=20),
            ],
        )

        probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=history,
            schedule=schedule,
        )

        self.assertLess(probability, 0.58)
        self.assertEqual(make_decision(probability, horizon_days=0), "no")

    def test_weather_window_before_active_flight_does_not_penalize_normal_schedule(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            flight_window_start_hour=17,
            flight_window_end_hour=19,
            flight_window_hours=3,
            visibility=7450,
            flight_window_visibility=7450,
            cloud_cover=100,
            cloud_cover_low=100,
            flight_window_cloud_cover_low=100,
            relative_humidity_2m=95,
            dew_point_spread=0.8,
            precipitation=0.1,
            wind_speed_10m=5,
            wind_gusts_10m=20,
            weather_code=51,
            fog_low_cloud_risk_level="medium",
            flight_window_fog_low_cloud_risk_level="medium",
        )
        history = HistoricalSnapshot(
            source="test",
            similar_days_count=75,
            completed_count=47,
            cancelled_count=28,
            historical_probability_flight=0.6234,
            decade_probability_flight=0.75,
        )
        schedule = FlightScheduleSnapshot(
            source="test-board",
            available=True,
            first_departure_hour=17,
            first_scheduled_hour=20,
            last_scheduled_hour=20,
            moved_next_day=False,
            completed_same_day=False,
            status_summary="scheduled",
            total_flights=1,
            pending_flights=1,
            active_flight_hour=20,
            active_flight_time="20:55",
            active_flight_status="scheduled",
            flights=[
                FlightScheduleFlight(status="scheduled", state="pending", hour=20),
            ],
        )

        probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=history,
            schedule=schedule,
        )

        self.assertGreaterEqual(probability, 0.58)
        self.assertEqual(make_decision(probability, horizon_days=0), "yes")

    def test_extreme_gust_removes_flight_window(self) -> None:
        settings = Settings()
        row = {
            "visibility": None,
            "cloud_cover_low": 0,
            "wind_gusts_10m": 72,
            "precipitation": 0,
            "weather_code": 3,
        }

        self.assertFalse(_is_flight_opportunity_hour(row, settings))

    def test_humidity_alone_does_not_create_compound_risk(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            flight_window_visibility=12000,
            relative_humidity_2m=94,
            dew_point_spread=1.4,
            pressure_msl=1017,
            precipitation=0,
            cloud_cover_low=100,
            wind_speed_10m=18,
            wind_gusts_10m=42,
            weather_code=51,
            fog_low_cloud_risk_level="high",
        )

        self.assertFalse(has_compound_humidity_risk(weather))
        probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=seasonal_late_may_history(),
        )

        self.assertEqual(make_decision(probability, horizon_days=0), "yes")

    def test_humidity_with_low_visibility_creates_compound_risk(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=False,
            visibility=2500,
            relative_humidity_2m=94,
            dew_point_spread=0.8,
            pressure_msl=1002,
            precipitation=0.7,
            cloud_cover_low=100,
            wind_speed_10m=24,
            wind_gusts_10m=50,
            weather_code=51,
            fog_low_cloud_risk_level="high",
        )

        self.assertTrue(has_compound_humidity_risk(weather))
        probability = calculate_probability(
            horizon_days=0,
            weather=weather,
            history=seasonal_late_may_history(),
        )

        self.assertEqual(make_decision(probability, horizon_days=0), "no")

    def test_long_horizon_does_not_return_yes_below_fifty_percent(self) -> None:
        self.assertEqual(make_decision(0.49, horizon_days=120), "no")
        self.assertEqual(make_decision(0.50, horizon_days=120), "yes")

    def test_long_horizon_defaults_positive_for_neutral_history(self) -> None:
        probability = calculate_probability(
            horizon_days=120,
            weather=WeatherSnapshot(source="test", available=False),
            history=HistoricalSnapshot(
                source="test",
                similar_days_count=48,
                completed_count=25,
                cancelled_count=23,
                historical_probability_flight=0.50,
                month_probability_flight=0.52,
                decade_probability_flight=0.48,
            ),
        )

        self.assertGreaterEqual(probability, 0.50)
        self.assertEqual(make_decision(probability, horizon_days=120), "yes")

    def test_long_horizon_marks_clear_historical_risk_as_no(self) -> None:
        probability = calculate_probability(
            horizon_days=120,
            weather=WeatherSnapshot(source="test", available=False),
            history=HistoricalSnapshot(
                source="test",
                similar_days_count=42,
                completed_count=12,
                cancelled_count=30,
                historical_probability_flight=0.32,
                month_probability_flight=0.42,
                decade_probability_flight=0.38,
            ),
        )

        self.assertLess(probability, 0.50)
        self.assertEqual(make_decision(probability, horizon_days=120), "no")


if __name__ == "__main__":
    unittest.main()
