import csv
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path


HISTORICAL_ML_FEATURE_COLUMNS = [
    "horizon_days",
    "month",
    "day",
    "day_of_year",
    "month_decade",
    "season_index",
    "day_of_week",
    "is_weekend",
    "day_of_year_sin",
    "day_of_year_cos",
    "month_sin",
    "month_cos",
    "prev_1_completed",
    "prev_3_cancelled_count",
    "prev_7_cancelled_count",
    "prev_14_completed_rate",
    "prev_30_completed_rate",
    "prev_60_completed_rate",
    "prev_90_completed_rate",
    "same_month_past_count",
    "same_month_past_completed_rate",
    "same_decade_past_count",
    "same_decade_past_completed_rate",
    "same_doy_window_14_past_count",
    "same_doy_window_14_completed_rate",
    "days_since_last_cancelled",
    "cancelled_streak_before",
    "completed_streak_before",
    "history_similar_days_count",
    "history_completed_count",
    "history_cancelled_count",
    "history_probability_flight",
    "history_month_probability_flight",
    "history_decade_probability_flight",
    "history_combined_probability",
]

SEASON_TO_INDEX = {
    "winter": 0,
    "spring": 1,
    "summer": 2,
    "autumn": 3,
}


@dataclass(frozen=True)
class HistoricalFlightDay:
    date: date
    status: str

    @property
    def is_completed(self) -> int | None:
        if self.status == "completed":
            return 1
        if self.status == "cancelled":
            return 0
        return None


def load_historical_flight_days(path: str | Path) -> list[HistoricalFlightDay]:
    dataset_path = Path(path)
    if not dataset_path.exists():
        return []

    rows: list[HistoricalFlightDay] = []
    with dataset_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            raw_date = row.get("date")
            status = (row.get("status") or "").strip().lower()
            if not raw_date or status not in {"completed", "cancelled"}:
                continue
            try:
                flight_date = date.fromisoformat(raw_date[:10])
            except ValueError:
                continue
            rows.append(HistoricalFlightDay(date=flight_date, status=status))

    return sorted(rows, key=lambda item: item.date)


def season_for_month(month: int) -> str:
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "autumn"


def circular_distance(day_a: int, day_b: int) -> int:
    raw = abs(day_a - day_b)
    return min(raw, 366 - raw)


def laplace_probability(completed: int, total: int, fallback: float = 0.5) -> float:
    if total <= 0:
        return fallback
    return (completed + 1) / (total + 2)


def _rate(values: list[int], fallback: float = 0.5, min_count: int = 1) -> float:
    if len(values) < min_count:
        return fallback
    return sum(values) / len(values)


def _streak(values: list[int], target: int) -> int:
    count = 0
    for value in reversed(values):
        if value != target:
            break
        count += 1
    return count


def _feature_row(
    target_date: date,
    as_of_date: date,
    past_rows: list[HistoricalFlightDay],
) -> dict[str, float | int]:
    day_of_year = target_date.timetuple().tm_yday
    month = target_date.month
    decade = (target_date.day - 1) // 10 + 1
    past_values = [row.is_completed for row in past_rows if row.is_completed is not None]
    typed_values = [int(value) for value in past_values]

    month_values = [
        int(row.is_completed)
        for row in past_rows
        if row.is_completed is not None and row.date.month == month
    ]
    decade_values = [
        int(row.is_completed)
        for row in past_rows
        if (
            row.is_completed is not None
            and row.date.month == month
            and ((row.date.day - 1) // 10 + 1) == decade
        )
    ]
    doy_window_values = [
        int(row.is_completed)
        for row in past_rows
        if (
            row.is_completed is not None
            and circular_distance(row.date.timetuple().tm_yday, day_of_year) <= 14
        )
    ]
    similar_values = doy_window_values if len(doy_window_values) >= 10 else month_values

    completed_count = sum(similar_values)
    cancelled_count = len(similar_values) - completed_count
    history_probability = laplace_probability(completed_count, len(similar_values))
    month_probability = laplace_probability(sum(month_values), len(month_values)) if month_values else 0.5
    decade_probability = laplace_probability(sum(decade_values), len(decade_values)) if decade_values else 0.5
    combined_probability = 0.65 * history_probability + 0.35 * decade_probability

    cancelled_dates = [
        row.date
        for row in past_rows
        if row.is_completed == 0
    ]

    season = season_for_month(month)
    days_since_last_cancelled = (
        (as_of_date - cancelled_dates[-1]).days
        if cancelled_dates
        else 9999
    )

    return {
        "horizon_days": max((target_date - as_of_date).days, 0),
        "month": month,
        "day": target_date.day,
        "day_of_year": day_of_year,
        "month_decade": decade,
        "season_index": SEASON_TO_INDEX[season],
        "day_of_week": target_date.weekday(),
        "is_weekend": int(target_date.weekday() in {5, 6}),
        "day_of_year_sin": math.sin(2 * math.pi * day_of_year / 366),
        "day_of_year_cos": math.cos(2 * math.pi * day_of_year / 366),
        "month_sin": math.sin(2 * math.pi * month / 12),
        "month_cos": math.cos(2 * math.pi * month / 12),
        "prev_1_completed": typed_values[-1] if typed_values else 0.5,
        "prev_3_cancelled_count": sum(1 - item for item in typed_values[-3:]) if typed_values else 0,
        "prev_7_cancelled_count": sum(1 - item for item in typed_values[-7:]) if typed_values else 0,
        "prev_14_completed_rate": _rate(typed_values[-14:], min_count=3),
        "prev_30_completed_rate": _rate(typed_values[-30:], min_count=5),
        "prev_60_completed_rate": _rate(typed_values[-60:], min_count=10),
        "prev_90_completed_rate": _rate(typed_values[-90:], min_count=10),
        "same_month_past_count": len(month_values),
        "same_month_past_completed_rate": _rate(month_values),
        "same_decade_past_count": len(decade_values),
        "same_decade_past_completed_rate": _rate(decade_values),
        "same_doy_window_14_past_count": len(doy_window_values),
        "same_doy_window_14_completed_rate": _rate(doy_window_values),
        "days_since_last_cancelled": days_since_last_cancelled,
        "cancelled_streak_before": _streak(typed_values, target=0),
        "completed_streak_before": _streak(typed_values, target=1),
        "history_similar_days_count": len(similar_values),
        "history_completed_count": completed_count,
        "history_cancelled_count": cancelled_count,
        "history_probability_flight": history_probability,
        "history_month_probability_flight": month_probability,
        "history_decade_probability_flight": decade_probability,
        "history_combined_probability": combined_probability,
    }


def build_historical_ml_features(
    target_date: date,
    as_of_date: date,
    rows: list[HistoricalFlightDay],
) -> dict[str, float | int]:
    past_rows = [
        row
        for row in sorted(rows, key=lambda item: item.date)
        if row.date <= as_of_date and row.is_completed is not None
    ]
    features = _feature_row(target_date=target_date, as_of_date=as_of_date, past_rows=past_rows)
    return {column: features[column] for column in HISTORICAL_ML_FEATURE_COLUMNS}
