import unittest

from app.config import Settings
from app.schemas import HistoricalSnapshot, WeatherSnapshot
from app.services.predictor import calculate_probability, make_decision
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

    def test_low_visibility_without_fog_code_does_not_remove_flight_window(self) -> None:
        settings = Settings()
        row = {
            "visibility": 300,
            "cloud_cover_low": 100,
            "wind_gusts_10m": 42,
            "precipitation": 0,
            "weather_code": 51,
        }

        self.assertTrue(_is_flight_opportunity_hour(row, settings))

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


if __name__ == "__main__":
    unittest.main()
