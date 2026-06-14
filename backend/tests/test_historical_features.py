import unittest
from datetime import date

from app.services.historical_features import (
    HistoricalFlightDay,
    build_historical_ml_features,
)
from app.services.historical_probability import user_probability_from_model_score


class HistoricalFeaturesTests(unittest.TestCase):
    def test_historical_ml_yes_scores_with_low_threshold_are_lifted(self) -> None:
        self.assertEqual(user_probability_from_model_score(0.31, 0.31), 0.51)
        self.assertEqual(user_probability_from_model_score(0.3107, 0.31), 0.5107)
        self.assertEqual(user_probability_from_model_score(0.5, 0.31), 0.7)

    def test_historical_ml_no_scores_do_not_get_lifted_to_fifty_percent(self) -> None:
        self.assertEqual(user_probability_from_model_score(0.2704, 0.31), 0.2704)
        self.assertLess(user_probability_from_model_score(0.56, 0.57), 0.5)

    def test_features_use_only_rows_known_by_as_of_date(self) -> None:
        rows = [
            HistoricalFlightDay(date=date(2026, 5, 1), status="completed"),
            HistoricalFlightDay(date=date(2026, 5, 8), status="cancelled"),
            HistoricalFlightDay(date=date(2026, 5, 15), status="completed"),
            HistoricalFlightDay(date=date(2026, 6, 1), status="cancelled"),
        ]

        features = build_historical_ml_features(
            target_date=date(2026, 6, 20),
            as_of_date=date(2026, 5, 20),
            rows=rows,
        )

        self.assertEqual(features["prev_1_completed"], 1)
        self.assertEqual(features["prev_3_cancelled_count"], 1)
        self.assertEqual(features["days_since_last_cancelled"], 12)
        self.assertEqual(features["same_month_past_count"], 0)
        self.assertEqual(features["history_similar_days_count"], 0)

    def test_future_rows_change_features_only_after_they_are_known(self) -> None:
        rows = [
            HistoricalFlightDay(date=date(2026, 5, 1), status="completed"),
            HistoricalFlightDay(date=date(2026, 5, 8), status="cancelled"),
            HistoricalFlightDay(date=date(2026, 5, 15), status="completed"),
            HistoricalFlightDay(date=date(2026, 6, 1), status="cancelled"),
        ]

        before = build_historical_ml_features(
            target_date=date(2026, 6, 20),
            as_of_date=date(2026, 5, 20),
            rows=rows,
        )
        after = build_historical_ml_features(
            target_date=date(2026, 6, 20),
            as_of_date=date(2026, 6, 2),
            rows=rows,
        )

        self.assertEqual(before["same_month_past_count"], 0)
        self.assertEqual(after["same_month_past_count"], 1)
        self.assertEqual(before["prev_1_completed"], 1)
        self.assertEqual(after["prev_1_completed"], 0)


if __name__ == "__main__":
    unittest.main()
