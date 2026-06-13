import unittest

from app.schemas import FlightScheduleSnapshot, HistoricalSnapshot, WeatherSnapshot
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
    def test_yes_weather_explanation_includes_specific_weather_without_history(self) -> None:
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
            wind_direction_10m=250,
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
        self.assertIn("Данные погоды:", explanation)
        self.assertIn("видимость - около 9200 м", explanation)
        self.assertIn("низкая облачность - 25%", explanation)
        self.assertIn("ветер - порывы до 8.6 м/с, направление западный (W)", explanation)
        self.assertIn("вероятность вылета — 72%", explanation)
        self.assertNotIn("туман -", explanation)
        self.assertNotIn("Исторически", explanation)
        self.assertNotIn("85 выполненных", explanation)
        self.assertNotIn("55 отмененных", explanation)
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
        self.assertIn("видимость - около 240 м", explanation)
        self.assertIn("низкая облачность - 100%", explanation)
        self.assertIn("ветер - порывы до 15.8 м/с", explanation)
        self.assertIn("туман - да", explanation)
        self.assertIn("вероятность вылета — 41%", explanation)
        self.assertNotIn("Исторически", explanation)

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

        self.assertIn("Данные погоды:", explanation)
        self.assertNotIn("08:00-20:00", explanation)
        self.assertIn("видимость - около 1480 м", explanation)
        self.assertIn("низкая облачность - 7%", explanation)
        self.assertIn("ветер - порывы до 6.7 м/с", explanation)
        self.assertNotIn("туман -", explanation)
        self.assertNotIn("Исторически", explanation)
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

        self.assertIn("видимость - около 15480 м", explanation)
        self.assertIn("низкая облачность - 99%", explanation)
        self.assertIn("ветер - порывы до 19.3 м/с", explanation)
        self.assertIn("туман - возможно", explanation)

    def test_no_decision_with_formal_window_explains_remaining_risks(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            flight_window_start_hour=8,
            flight_window_end_hour=20,
            flight_window_hours=13,
            aggregation_window_start_hour=8,
            aggregation_window_end_hour=20,
            flight_window_visibility=280,
            flight_window_cloud_cover_low=100,
            wind_gusts_10m=46.8,
            relative_humidity_2m=90,
            dew_point_spread=1.6,
            weather_code=3,
            fog_low_cloud_risk_level="high",
        )

        explanation = fallback_explanation(
            target_date="2026-06-03",
            decision="no",
            probability_flight=0.38,
            confidence="medium",
            horizon_days=1,
            weather=weather,
            history=HistoricalSnapshot(
                source="test",
                similar_days_count=75,
                completed_count=44,
                cancelled_count=31,
                historical_probability_flight=0.58,
            ),
        )

        self.assertIn("видимость - около 280 м", explanation)
        self.assertIn("низкая облачность - 100%", explanation)
        self.assertIn("ветер - порывы до 13 м/с", explanation)
        self.assertIn("туман - да", explanation)
        self.assertNotIn("По погоде", explanation)

    def test_board_cancelled_explanation_is_short_and_zero_percent(self) -> None:
        weather = WeatherSnapshot(source="test", available=True, flight_window_available=True)
        schedule = FlightScheduleSnapshot(
            source="test",
            available=True,
            moved_next_day=True,
            observed_at="2026-06-09T10:07:30+11:00",
        )

        explanation = fallback_explanation(
            target_date="2026-06-09",
            decision="no",
            probability_flight=0.0,
            confidence="high",
            horizon_days=0,
            weather=weather,
            history=history_snapshot(),
            schedule=schedule,
        )

        self.assertEqual(explanation, "По табло рейс отменен для этой даты.")
        self.assertNotIn("Данные погоды", explanation)
        self.assertNotIn("Исторически", explanation)

    def test_first_cancelled_among_multiple_flights_is_not_reported_as_all_cancelled(self) -> None:
        weather = WeatherSnapshot(source="test", available=True, flight_window_available=True)
        schedule = FlightScheduleSnapshot(
            source="test",
            available=True,
            moved_next_day=True,
            total_flights=2,
            unavailable_flights=1,
            pending_flights=1,
            active_flight_index=1,
        )

        explanation = fallback_explanation(
            target_date="2026-06-14",
            decision="no",
            probability_flight=0.0,
            confidence="high",
            horizon_days=0,
            weather=weather,
            history=history_snapshot(),
            schedule=schedule,
        )

        self.assertEqual(explanation, "По табло первый рейс отменен или перенесен для этой даты.")
        self.assertNotIn("все рейсы", explanation)
        self.assertNotIn("Данные погоды", explanation)

    def test_next_flight_cancelled_after_completed_flight_is_short(self) -> None:
        weather = WeatherSnapshot(source="test", available=True, flight_window_available=True)
        schedule = FlightScheduleSnapshot(
            source="test",
            available=True,
            moved_next_day=True,
            total_flights=2,
            completed_flights=1,
            unavailable_flights=1,
            active_flight_index=2,
        )

        explanation = fallback_explanation(
            target_date="2026-06-14",
            decision="no",
            probability_flight=0.0,
            confidence="high",
            horizon_days=0,
            weather=weather,
            history=history_snapshot(),
            schedule=schedule,
        )

        self.assertEqual(
            explanation,
            "По табло выполнено 1 из 2 рейсов. Следующий рейс отменен или перенесен для этой даты.",
        )
        self.assertNotIn("Данные погоды", explanation)

    def test_partial_completed_day_weather_explanation_targets_next_flight(self) -> None:
        weather = WeatherSnapshot(
            source="test",
            available=True,
            flight_window_available=True,
            flight_window_visibility=6400,
            flight_window_cloud_cover_low=38,
            wind_speed_10m=22,
            wind_direction_10m=90,
            relative_humidity_2m=81,
        )
        schedule = FlightScheduleSnapshot(
            source="test",
            available=True,
            total_flights=2,
            completed_flights=1,
            pending_flights=1,
            active_flight_index=2,
        )

        explanation = fallback_explanation(
            target_date="2026-06-14",
            decision="yes",
            probability_flight=0.64,
            confidence="medium",
            horizon_days=0,
            weather=weather,
            history=history_snapshot(),
            schedule=schedule,
        )

        self.assertIn("По табло выполнено 1 из 2 рейсов. Прогноз относится к следующему рейсу.", explanation)
        self.assertIn("Да. Данные погоды:", explanation)
        self.assertIn("видимость - около 6400 м", explanation)
        self.assertIn("ветер - скорость около 6.1 м/с, направление восточный (E)", explanation)
        self.assertIn("вероятность вылета — 64%", explanation)

    def test_board_completed_explanation_is_short(self) -> None:
        weather = WeatherSnapshot(source="test", available=True, flight_window_available=True)
        schedule = FlightScheduleSnapshot(
            source="test",
            available=True,
            completed_same_day=True,
            observed_at="2026-06-09T14:07:30+11:00",
        )

        explanation = fallback_explanation(
            target_date="2026-06-09",
            decision="yes",
            probability_flight=0.85,
            confidence="high",
            horizon_days=0,
            weather=weather,
            history=history_snapshot(),
            schedule=schedule,
        )

        self.assertEqual(explanation, "По табло рейс уже выполнен для этой даты.")
        self.assertNotIn("Данные погоды", explanation)
        self.assertNotIn("Исторически", explanation)

    def test_multiple_completed_flights_explanation_is_short(self) -> None:
        weather = WeatherSnapshot(source="test", available=True, flight_window_available=True)
        schedule = FlightScheduleSnapshot(
            source="test",
            available=True,
            completed_same_day=True,
            total_flights=2,
            completed_flights=2,
            observed_at="2026-06-14T18:07:30+11:00",
        )

        explanation = fallback_explanation(
            target_date="2026-06-14",
            decision="yes",
            probability_flight=0.85,
            confidence="high",
            horizon_days=0,
            weather=weather,
            history=history_snapshot(),
            schedule=schedule,
        )

        self.assertEqual(explanation, "По табло сегодняшние рейсы успешно выполнены.")
        self.assertNotIn("Данные погоды", explanation)
        self.assertNotIn("Исторически", explanation)

    def test_climate_history_explanation_keeps_history(self) -> None:
        weather = WeatherSnapshot(source="test", available=False)

        explanation = fallback_explanation(
            target_date="2026-07-20",
            decision="yes",
            probability_flight=0.61,
            confidence="low",
            horizon_days=41,
            weather=weather,
            history=history_snapshot(),
        )

        self.assertIn("Точного погодного прогноза", explanation)
        self.assertIn("Исторически в календарном окне ±14 дней вокруг выбранной даты", explanation)
        self.assertIn("85 дней с выполненным рейсом", explanation)
        self.assertIn("55 дней с отменой", explanation)
        self.assertNotIn("Исторически в похожие даты", explanation)


if __name__ == "__main__":
    unittest.main()
