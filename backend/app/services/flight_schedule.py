import csv
from datetime import date, datetime
from pathlib import Path

from app.config import get_settings
from app.schemas import FlightScheduleSnapshot


COMPLETED_BOARD_STATUSES = {"departed", "arrived", "in_flight"}
NEXT_DAY_BOARD_STATUSES = COMPLETED_BOARD_STATUSES | {"delayed", "combined"}


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _parse_date(value: object) -> date | None:
    clean = _clean_text(value)
    if not clean:
        return None
    try:
        return date.fromisoformat(clean)
    except ValueError:
        return None


def _parse_hour(value: object) -> int | None:
    clean = _clean_text(value)
    if not clean:
        return None
    try:
        return int(clean.split(":", 1)[0])
    except (TypeError, ValueError):
        return None


def _parse_observed_at(value: object) -> datetime | None:
    clean = _clean_text(value)
    if not clean:
        return None
    try:
        return datetime.fromisoformat(clean)
    except ValueError:
        return None


def _read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def _latest_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    if not rows:
        return []

    latest = max((_clean_text(row.get("observed_at")) for row in rows), default="")
    if not latest:
        return rows
    return [row for row in rows if _clean_text(row.get("observed_at")) == latest]


def _latest_row_per_flight(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    latest_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        key = (_clean_text(row.get("direction")), _clean_text(row.get("flight_numbers")))
        if not key[1]:
            continue

        current = latest_by_key.get(key)
        if current is None:
            latest_by_key[key] = row
            continue

        current_observed = _parse_observed_at(current.get("observed_at"))
        row_observed = _parse_observed_at(row.get("observed_at"))
        if current_observed is None or (row_observed is not None and row_observed > current_observed):
            latest_by_key[key] = row

    return list(latest_by_key.values())


def _join_unique(values: list[str]) -> str | None:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = _clean_text(value)
        if clean and clean not in seen:
            seen.add(clean)
            result.append(clean)
    return ";".join(result) if result else None


def _unavailable(reason: str, source: str) -> FlightScheduleSnapshot:
    return FlightScheduleSnapshot(source=source, available=False, reason=reason)


def get_flight_schedule_for_date(
    target_date: date,
    *,
    board_path: Path | None = None,
    source: str = "airportus-board",
) -> FlightScheduleSnapshot:
    settings = get_settings()
    path = board_path or Path(settings.flight_status_dataset_path)
    rows = [
        row
        for row in _read_rows(path)
        if _clean_text(row.get("flight_date")) == target_date.isoformat()
    ]
    if not rows:
        return _unavailable(f"No board schedule rows for {target_date.isoformat()}.", source)

    latest_observation_rows = _latest_rows(rows)
    latest_flight_rows = _latest_row_per_flight(rows)
    schedule_rows = latest_flight_rows or latest_observation_rows or rows

    scheduled_hours = [
        hour
        for hour in (_parse_hour(row.get("flight_time")) for row in schedule_rows)
        if hour is not None
    ]
    if not scheduled_hours:
        return _unavailable(f"Board rows for {target_date.isoformat()} do not include parseable flight times.", source)

    departure_hours = [
        hour
        for hour in (
            _parse_hour(row.get("flight_time"))
            for row in schedule_rows
            if _clean_text(row.get("direction")) == "departure"
        )
        if hour is not None
    ]

    moved_next_day = False
    completed_same_day = False
    statuses: list[str] = []
    for row in schedule_rows:
        status = _clean_text(row.get("status_normalized"))
        if status:
            statuses.append(status)

        actual_date = _parse_date(row.get("actual_date"))
        if status in NEXT_DAY_BOARD_STATUSES and actual_date is not None and actual_date > target_date:
            moved_next_day = True
        if status in COMPLETED_BOARD_STATUSES and (actual_date is None or actual_date == target_date):
            completed_same_day = True

    first_scheduled_hour = min(scheduled_hours)
    last_scheduled_hour = max(scheduled_hours)
    first_departure_hour = min(departure_hours) if departure_hours else first_scheduled_hour

    return FlightScheduleSnapshot(
        source=source,
        available=True,
        observed_at=max((_clean_text(row.get("observed_at")) for row in rows), default="") or None,
        flight_numbers=_join_unique([_clean_text(row.get("flight_numbers")) for row in schedule_rows]),
        first_departure_hour=first_departure_hour,
        first_scheduled_hour=first_scheduled_hour,
        last_scheduled_hour=last_scheduled_hour,
        schedule_window_start_hour=max(0, first_departure_hour - 1),
        schedule_window_end_hour=min(23, last_scheduled_hour + 1),
        moved_next_day=moved_next_day,
        completed_same_day=completed_same_day,
        status_summary=_join_unique(statuses),
    )
