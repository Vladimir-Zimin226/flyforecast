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
            aggregation_window_start_hour=8,
            aggregation_window_end_hour=20,
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
        self.assertIn("есть летное окно примерно с 10:00 до 16:00", explanation)
        self.assertIn("видимость хорошая, около 9200 м", explanation)
        self.assertIn("низкая облачность небольшая, 25%", explanation)
        self.assertIn("ветер умеренный: порывы до 8.6 м/с", explanation)
        self.assertIn("туман маловероятен", explanation)
        self.assertIn("Исторически", explanation)
        self.assertIn("85 выполненных", explanation)
        self.assertIn("55 отмененных", explanation)
        self.assertNotIn("Это ориентир для планирования", explanation)

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
        self.assertIn("видимость низкая, около 240 м", explanation)
        self.assertIn("наблюдается сильная низкая облачность, 100%", explanation)
        self.assertIn("заметный ветер: порывы до 15.8 м/с", explanation)
        self.assertIn("туман вероятнее", explanation)
        self.assertIn("Исторически", explanation)

    def test_full_working_window_does_not_print_fake_precise_time(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            flight_window_start_hour=8,
            flight_window_end_hour=20,
            aggregation_window_start_hour=8,
            aggregation_window_end_hour=20,
            flight_window_visibility=1480,
            flight_window_cloud_cover_low=7,
            wind_gusts_10m=24,
            relative_humidity_2m=72,
            dew_point_spread=5.1,
            weather_code=3,
            fog_low_cloud_risk_level="low",
        )

        explanation = fallback_explanation(
            target_date="2026-06-02",
            decision="yes",
            probability_flight=0.66,
            confidence="medium",
            horizon_days=0,
            weather=weather,
            history=HistoricalSnapshot(
                source="test",
                similar_days_count=76,
                completed_count=44,
                cancelled_count=32,
                historical_probability_flight=0.58,
            ),
        )

        self.assertIn("По погоде: есть летное окно", explanation)
        self.assertNotIn("08:00-20:00", explanation)
        self.assertIn("видимость умеренная, около 1480 м", explanation)
        self.assertIn("низкой облачности практически нет, всего 7%", explanation)
        self.assertIn("ветер умеренный: порывы до 6.7 м/с", explanation)
        self.assertIn("туман маловероятен по температуре и точке росы: разница 5.1 °C", explanation)
        self.assertIn("Исторически", explanation)
        self.assertNotIn("Это ориентир для планирования", explanation)

    def test_no_window_contrasts_good_visibility_with_low_clouds(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=False,
            visibility=15480,
            cloud_cover_low=99,
            wind_gusts_10m=69.5,
            relative_humidity_2m=82,
            dew_point_spread=2.9,
            weather_code=3,
            fog_low_cloud_risk_level="medium",
        )

        explanation = fallback_explanation(
            target_date="2026-06-05",
            decision="no",
            probability_flight=0.43,
            confidence="medium",
            horizon_days=3,
            weather=weather,
            history=HistoricalSnapshot(
                source="test",
                similar_days_count=80,
                completed_count=47,
                cancelled_count=33,
                historical_probability_flight=0.59,
            ),
        )

        self.assertIn("устойчивого погодного окна в рабочем интервале не найдено", explanation)
        self.assertIn("несмотря на отличную видимость около 15480 м", explanation)
        self.assertIn("наблюдается сильная низкая облачность, 99%", explanation)
        self.assertIn("сильный ветер: порывы до 19.3 м/с", explanation)
        self.assertNotIn("видимость отличная, около 15480 м, низкая облачность", explanation)


if __name__ == "__main__":
    unittest.main()
