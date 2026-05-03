import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from app.config import get_settings
from app.schemas import HistoricalSnapshot


@dataclass(frozen=True)
class FlightDay:
    date: date
    status: str

    @property
    def is_completed(self) -> int | None:
        if self.status == "completed":
            return 1
        if self.status == "cancelled":
            return 0
        return None


def _load_rows() -> list[FlightDay]:
    settings = get_settings()
    path = Path(settings.flyforecast_dataset_path)

    if not path.exists():
        return []

    rows: list[FlightDay] = []

    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            raw_date = row.get("date")
            status = row.get("status")

            if not raw_date or not status:
                continue

            if status not in {"completed", "cancelled"}:
                continue

            rows.append(
                FlightDay(
                    date=date.fromisoformat(raw_date),
                    status=status,
                )
            )

    return rows


def _safe_probability(completed: int, total: int, fallback: float = 0.5) -> float:
    if total <= 0:
        return fallback
    # Сглаживание Лапласа, чтобы не получать 0%/100% на малых выборках.
    return round((completed + 1) / (total + 2), 4)


def get_historical_snapshot(target_date: date) -> HistoricalSnapshot:
    rows = _load_rows()

    if not rows:
        return HistoricalSnapshot(
            source="processed-dataset",
            similar_days_count=0,
            completed_count=0,
            cancelled_count=0,
            historical_probability_flight=0.5,
            month_probability_flight=None,
            decade_probability_flight=None,
        )

    month_rows = [row for row in rows if row.date.month == target_date.month]
    decade = (target_date.day - 1) // 10 + 1
    decade_rows = [
        row for row in rows
        if row.date.month == target_date.month
        and ((row.date.day - 1) // 10 + 1) == decade
    ]

    # Окно похожих дат по day_of_year ±14 дней, с учётом перехода через Новый год.
    target_doy = target_date.timetuple().tm_yday

    def circular_distance(day_a: int, day_b: int) -> int:
        raw = abs(day_a - day_b)
        return min(raw, 366 - raw)

    similar_rows = [
        row for row in rows
        if circular_distance(row.date.timetuple().tm_yday, target_doy) <= 14
    ]

    if len(similar_rows) < 10:
        similar_rows = month_rows

    completed_count = sum(1 for row in similar_rows if row.is_completed == 1)
    cancelled_count = sum(1 for row in similar_rows if row.is_completed == 0)

    month_completed = sum(1 for row in month_rows if row.is_completed == 1)
    decade_completed = sum(1 for row in decade_rows if row.is_completed == 1)

    return HistoricalSnapshot(
        source="processed-dataset",
        similar_days_count=len(similar_rows),
        completed_count=completed_count,
        cancelled_count=cancelled_count,
        historical_probability_flight=_safe_probability(completed_count, len(similar_rows)),
        month_probability_flight=_safe_probability(month_completed, len(month_rows)) if month_rows else None,
        decade_probability_flight=_safe_probability(decade_completed, len(decade_rows)) if decade_rows else None,
    )