import csv
from datetime import date, datetime
from pathlib import Path

from app.config import get_settings
from app.schemas import FlightScheduleFlight, FlightScheduleSnapshot


COMPLETED_BOARD_STATUSES = {"departed", "arrived", "in_flight"}
NEXT_DAY_BOARD_STATUSES = COMPLETED_BOARD_STATUSES | {"delayed", "combined"}
UNAVAILABLE_BOARD_STATUSES = {"cancelled", "combined"}


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


def _parse_time(value: object) -> str | None:
    clean = _clean_text(value)
    if not clean:
        return None
    parts = clean.split(":", 1)
    if len(parts) != 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        return None
    return f"{hour:02d}:{minute:02d}"


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


def _flight_key(row: dict[str, str]) -> tuple[str, str, str]:
    direction = _clean_text(row.get("direction"))
    flight_numbers = _clean_text(row.get("flight_numbers"))
    flight_time = _clean_text(row.get("flight_time"))
    return direction, flight_numbers or flight_time, flight_time


def _direction(row: dict[str, str]) -> str:
    return _clean_text(row.get("direction")).lower()


def _latest_row_per_flight(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    latest_by_key: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = _flight_key(row)
        if not key[1] and not key[2]:
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


def _flight_state(row: dict[str, str], target_date: date) -> str:
    status = _clean_text(row.get("status_normalized"))
    actual_date = _parse_date(row.get("actual_date"))
    completed_statuses = {"arrived"} if _direction(row) == "arrival" else COMPLETED_BOARD_STATUSES

    if status in completed_statuses:
        if actual_date is None or actual_date == target_date:
            return "completed"
        if actual_date > target_date:
            return "unavailable"

    if status in UNAVAILABLE_BOARD_STATUSES:
        return "unavailable"

    if status in NEXT_DAY_BOARD_STATUSES and actual_date is not None and actual_date > target_date:
        return "unavailable"

    return "pending"


def _effective_flight_time(row: dict[str, str], target_date: date) -> str | None:
    status = _clean_text(row.get("status_normalized"))
    actual_date = _parse_date(row.get("actual_date"))
    actual_time = _parse_time(row.get("actual_time"))

    if status in {"delayed", "in_flight", "arrived", "departed"} and actual_date == target_date and actual_time:
        return actual_time
    return _parse_time(row.get("flight_time"))


def _schedule_rows_for_flight_state(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    arrival_rows = [row for row in rows if _direction(row) == "arrival"]
    if arrival_rows:
        return arrival_rows

    departure_rows = [row for row in rows if _direction(row) == "departure"]
    return departure_rows or rows


def _flight_snapshot(row: dict[str, str], target_date: date) -> FlightScheduleFlight:
    effective_time = _effective_flight_time(row, target_date)
    return FlightScheduleFlight(
        direction=_clean_text(row.get("direction")) or None,
        flight_time=effective_time or _clean_text(row.get("flight_time")) or None,
        flight_numbers=_clean_text(row.get("flight_numbers")) or None,
        status=_clean_text(row.get("status_normalized")) or None,
        actual_date=_clean_text(row.get("actual_date")) or None,
        hour=_parse_hour(effective_time),
        state=_flight_state(row, target_date),
    )


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
    latest_flight_rows = _latest_row_per_flight(latest_observation_rows or rows)
    deduplicated_rows = latest_flight_rows or latest_observation_rows or rows
    schedule_rows = _schedule_rows_for_flight_state(deduplicated_rows)

    scheduled_hours = [
        hour
        for hour in (_parse_hour(_effective_flight_time(row, target_date)) for row in schedule_rows)
        if hour is not None
    ]
    if not scheduled_hours:
        return _unavailable(f"Board rows for {target_date.isoformat()} do not include parseable flight times.", source)

    departure_hours = [
        hour
        for hour in (
            _parse_hour(_effective_flight_time(row, target_date))
            for row in schedule_rows
            if _clean_text(row.get("direction")) == "departure"
        )
        if hour is not None
    ]

    statuses: list[str] = []
    flights = sorted(
        (_flight_snapshot(row, target_date) for row in schedule_rows),
        key=lambda flight: (
            flight.hour if flight.hour is not None else 99,
            flight.direction or "",
            flight.flight_numbers or "",
        ),
    )
    total_flights = len(flights)
    completed_flights = sum(1 for flight in flights if flight.state == "completed")
    unavailable_flights = sum(1 for flight in flights if flight.state == "unavailable")
    pending_flights = sum(1 for flight in flights if flight.state == "pending")

    active_flight_index: int | None = None
    active_flight: FlightScheduleFlight | None = None
    for index, flight in enumerate(flights, start=1):
        if flight.state != "completed":
            active_flight_index = index
            active_flight = flight
            break

    for row in schedule_rows:
        status = _clean_text(row.get("status_normalized"))
        if status:
            statuses.append(status)

    first_scheduled_hour = min(scheduled_hours)
    last_scheduled_hour = max(scheduled_hours)
    first_departure_hour = (
        active_flight.hour
        if active_flight is not None and active_flight.hour is not None
        else min(departure_hours) if departure_hours else first_scheduled_hour
    )
    completed_same_day = total_flights > 0 and completed_flights == total_flights
    moved_next_day = active_flight is not None and active_flight.state == "unavailable"

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
        total_flights=total_flights,
        completed_flights=completed_flights,
        unavailable_flights=unavailable_flights,
        pending_flights=pending_flights,
        active_flight_index=active_flight_index,
        active_flight_hour=active_flight.hour if active_flight else None,
        active_flight_time=active_flight.flight_time if active_flight else None,
        active_flight_numbers=active_flight.flight_numbers if active_flight else None,
        active_flight_status=active_flight.status if active_flight else None,
        flights=flights,
    )
