import unittest

from app.schemas import HistoricalSnapshot, WeatherSnapshot
from app.services.llm import fallback_explanation


def history_snapshot() -> HistoricalSnapshot:
    return HistoricalSnapshot(
        source="test",
        similar_days_count=140,
        completed_count=85,
        cancelled_count=55,
        historical_probability_flight=0.6071,
    )


class ExplanationTests(unittest.TestCase):
    def test_yes_explanation_includes_specific_weather_and_history(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            flight_window_start_hour=10,
            flight_window_end_hour=16,
            flight_window_visibility=9200,
            flight_window_cloud_cover_low=25,
            wind_gusts_10m=31,
            relative_humidity_2m=74,
            dew_point_spread=4.2,
            weather_code=3,
            fog_low_cloud_risk_level="low",
        )

        explanation = fallback_explanation(
            target_date="2026-06-02",
            decision="yes",
            probability_flight=0.72,
            confidence="medium",
            horizon_days=1,
            weather=weather,
            history=history_snapshot(),
        )

        self.assertIn("Да", explanation)
        self.assertIn("видимость около 9200 м", explanation)
        self.assertIn("низкая облачность 25%", explanation)
        self.assertIn("порывы ветра до 31 км/ч", explanation)
        self.assertIn("туман", explanation)
        self.assertIn("85 выполненных", explanation)
        self.assertIn("55 отмененных", explanation)

    def test_no_explanation_includes_specific_fog_risk(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=False,
            visibility=240,
            cloud_cover_low=100,
            wind_gusts_10m=57,
            relative_humidity_2m=98,
            dew_point_spread=0.6,
            weather_code=3,
            fog_low_cloud_risk_level="high",
        )

        explanation = fallback_explanation(
            target_date="2026-05-26",
            decision="no",
            probability_flight=0.41,
            confidence="medium",
            horizon_days=0,
            weather=weather,
            history=history_snapshot(),
        )

        self.assertIn("Нет", explanation)
        self.assertIn("видимость около 240 м", explanation)
        self.assertIn("низкая облачность 100%", explanation)
        self.assertIn("высокий риск тумана", explanation)
        self.assertIn("исторически", explanation)


if __name__ == "__main__":
    unittest.main()
