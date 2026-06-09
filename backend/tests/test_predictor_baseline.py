import unittest

from app.config import Settings
from app.schemas import FlightScheduleSnapshot, HistoricalSnapshot, WeatherSnapshot
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

        self.assertEqual(probability, 0.1)
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


if __name__ == "__main__":
    unittest.main()
