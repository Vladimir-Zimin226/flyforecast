import argparse
import asyncio
import csv
import os
import re
from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx


AIRPORT_BOARD_URL = "https://airportus.ru/board/"
AURORA_STATUS_URL = os.getenv("AURORA_STATUS_URL", "https://www.flyaurora.ru/")
DEFAULT_OUTPUT = "data/raw/flight_status/kunashir_flight_status_hourly.csv"
DEFAULT_ERRORS_OUTPUT = "data/raw/flight_status/collection_errors.csv"
DEFAULT_TIMEZONE = os.getenv("AIRPORT_TIMEZONE", "Asia/Sakhalin")
KUNASHIR_CITY = "Южно-Курильск"
DEFAULT_AIRPORT_LATITUDE = float(os.getenv("AIRPORT_LATITUDE", "43.958"))
DEFAULT_AIRPORT_LONGITUDE = float(os.getenv("AIRPORT_LONGITUDE", "145.683"))

WEATHER_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "pressure_msl",
    "cloud_cover",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "visibility",
]


@dataclass
class BoardFlight:
    source: str
    source_url: str
    direction: str
    flight_numbers: str
    route: str
    scheduled_raw: str
    actual_raw: str
    status_raw: str
    radar_flight_number: str
    raw_row_text: str


@dataclass
class WeatherSnapshot:
    weather_source: str = "open-meteo"
    weather_available: bool = False
    weather_error: str = ""
    weather_observed_at: str = ""
    temperature_2m: float | None = None
    relative_humidity_2m: float | None = None
    dew_point_2m: float | None = None
    pressure_msl: float | None = None
    cloud_cover: float | None = None
    precipitation: float | None = None
    rain: float | None = None
    snowfall: float | None = None
    weather_code: float | None = None
    wind_speed_10m: float | None = None
    wind_gusts_10m: float | None = None
    wind_direction_10m: float | None = None
    visibility: float | None = None


@dataclass
class DatasetRow:
    observed_at: str
    observation_date: str
    observation_time: str
    source: str
    source_url: str
    direction: str
    flight_date: str
    flight_time: str
    flight_numbers: str
    route: str
    status_raw: str
    status_normalized: str
    reason: str
    reason_class: str
    scheduled_time_raw: str
    actual_time_raw: str
    actual_date: str
    actual_time: str
    radar_flight_number: str
    raw_row_text: str
    weather_source: str
    weather_available: bool
    weather_error: str
    weather_observed_at: str
    temperature_2m: float | None
    relative_humidity_2m: float | None
    dew_point_2m: float | None
    pressure_msl: float | None
    cloud_cover: float | None
    precipitation: float | None
    rain: float | None
    snowfall: float | None
    weather_code: float | None
    wind_speed_10m: float | None
    wind_gusts_10m: float | None
    wind_direction_10m: float | None
    visibility: float | None


@dataclass
class CollectionError:
    observed_at: str
    source: str
    source_url: str
    error: str


def clean_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


class AirportBoardParser(HTMLParser):
    def __init__(self, source: str, source_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.source = source
        self.source_url = source_url
        self.current_tab: str | None = None
        self.tab_stack: list[str | None] = []
        self.current_row: dict[str, Any] | None = None
        self.row_depth = 0
        self.cell_stack: list[int | None] = []
        self.rows: list[BoardFlight] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        class_name = attrs_dict.get("class", "")

        is_div = tag == "div"

        if is_div:
            tab_page = attrs_dict.get("tab-page")
            class_values = set(class_name.split())
            next_tab = self.current_tab

            if tab_page in {"departure", "arrival"}:
                next_tab = tab_page
            elif "departure-table" in class_values:
                next_tab = "departure"
            elif "arrival-table" in class_values:
                next_tab = "arrival"

            self.tab_stack.append(self.current_tab)
            self.current_tab = next_tab

        if self.current_row is not None:
            if is_div:
                self.row_depth += 1
                cell_match = re.search(r"board-table__item--([1-6])\b", class_name)
                if cell_match:
                    self.cell_stack.append(int(cell_match.group(1)))
                else:
                    self.cell_stack.append(self.cell_stack[-1] if self.cell_stack else None)

            radar_number = attrs_dict.get("data-airplane")
            if radar_number:
                self.current_row["radar_flight_number"] = radar_number
            return

        if is_div and self.current_tab and "board-table__row" in class_name and "board-table__row--head" not in class_name:
            self.current_row = {
                "direction": self.current_tab,
                "cells": {i: [] for i in range(1, 7)},
                "radar_flight_number": "",
            }
            self.row_depth = 1
            self.cell_stack = [None]

    def handle_endtag(self, tag: str) -> None:
        if tag != "div":
            return

        if self.current_row is not None:
            if self.cell_stack:
                self.cell_stack.pop()

            self.row_depth -= 1
            if self.row_depth == 0:
                self._finish_row()
                self.current_row = None

        if self.tab_stack:
            self.current_tab = self.tab_stack.pop()

    def handle_data(self, data: str) -> None:
        if self.current_row is None or not self.cell_stack or self.cell_stack[-1] is None:
            return

        text = clean_text(data)
        if not text:
            return

        self.current_row["cells"][self.cell_stack[-1]].append(text)

    def _finish_row(self) -> None:
        if not self.current_row:
            return

        cells = {
            index: clean_text(" ".join(values))
            for index, values in self.current_row["cells"].items()
        }

        flight_numbers = cells[1]
        route = cells[3]

        if not flight_numbers or not route:
            return

        row = BoardFlight(
            source=self.source,
            source_url=self.source_url,
            direction=self.current_row["direction"],
            flight_numbers=flight_numbers,
            route=route,
            scheduled_raw=cells[4],
            actual_raw=cells[5],
            status_raw=cells[6],
            radar_flight_number=self.current_row.get("radar_flight_number", ""),
            raw_row_text=clean_text(" | ".join(cells[index] for index in range(1, 7) if cells[index])),
        )
        self.rows.append(row)


async def fetch_text(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(
        url,
        headers={
            "User-Agent": "flyforecast-dataset-collector/0.1 (+https://flyforecast.ru)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        follow_redirects=True,
    )
    response.raise_for_status()
    return response.text


def parse_board_html(html: str, source: str, source_url: str) -> list[BoardFlight]:
    parser = AirportBoardParser(source=source, source_url=source_url)
    parser.feed(html)
    return [
        row for row in parser.rows
        if KUNASHIR_CITY.lower() in row.route.lower()
    ]


def parse_board_datetime(raw: str, observed_at: datetime) -> tuple[str, str]:
    value = clean_text(raw)
    value = re.sub(r"^по\s+расписанию\s*", "", value, flags=re.IGNORECASE).strip()

    match = re.search(r"(?:(\d{1,2})\.(\d{1,2})\s+)?(\d{1,2}):(\d{2})", value)
    if not match:
        return "", ""

    day_raw, month_raw, hour_raw, minute_raw = match.groups()

    if day_raw and month_raw:
        year = observed_at.year
        parsed_date = date(year, int(month_raw), int(day_raw))

        if (parsed_date - observed_at.date()).days > 180:
            parsed_date = date(year - 1, int(month_raw), int(day_raw))
        elif (observed_at.date() - parsed_date).days > 180:
            parsed_date = date(year + 1, int(month_raw), int(day_raw))
    else:
        parsed_date = observed_at.date()

    return parsed_date.isoformat(), f"{int(hour_raw):02d}:{int(minute_raw):02d}"


def normalize_status(status_raw: str, scheduled_date: str, scheduled_time: str, observed_at: datetime) -> str:
    normalized = status_raw.lower()

    if "отмен" in normalized:
        return "cancelled"
    if "задерж" in normalized or "перенес" in normalized or "перенос" in normalized or "отлож" in normalized:
        return "delayed"
    if "совмещ" in normalized or "объедин" in normalized:
        return "combined"
    if "вылетел" in normalized:
        return "departed"
    if "прибыл" in normalized:
        return "arrived"
    if "в пол" in normalized:
        return "in_flight"
    if "регистрац" in normalized:
        return "check_in"

    if scheduled_date and scheduled_time:
        scheduled_dt = datetime.fromisoformat(f"{scheduled_date}T{scheduled_time}").replace(tzinfo=observed_at.tzinfo)
        if scheduled_dt >= observed_at - timedelta(hours=2):
            return "scheduled"

    return "unknown"


def extract_reason(status_raw: str) -> tuple[str, str]:
    value = clean_text(status_raw)
    lower = value.lower()

    reason_match = re.search(r"(?:из-за|по причине)\s+(.+)$", value, flags=re.IGNORECASE)
    if reason_match:
        return reason_match.group(1).strip(), "source_text"

    if "отмен" in lower:
        return "", "unknown_cancel_reason"
    if "задерж" in lower or "перенес" in lower:
        return "", "unknown_delay_reason"

    return "", "unknown"


async def fetch_weather(client: httpx.AsyncClient, observed_at: datetime, latitude: float, longitude: float, timezone_name: str) -> WeatherSnapshot:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": timezone_name,
        "forecast_days": 1,
        "current": ",".join(WEATHER_FIELDS),
        "hourly": ",".join(WEATHER_FIELDS),
    }

    try:
        response = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return WeatherSnapshot(weather_available=False, weather_error=str(exc))

    current = payload.get("current") or {}
    if current:
        snapshot = WeatherSnapshot(
            weather_available=True,
            weather_observed_at=str(current.get("time") or ""),
        )
        for field in WEATHER_FIELDS:
            setattr(snapshot, field, current.get(field))
        return snapshot

    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return WeatherSnapshot(weather_available=False, weather_error="Open-Meteo returned no current or hourly rows.")

    observed_hour = observed_at.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M")
    try:
        index = times.index(observed_hour)
    except ValueError:
        index = min(range(len(times)), key=lambda i: abs(datetime.fromisoformat(times[i]).replace(tzinfo=observed_at.tzinfo) - observed_at))

    snapshot = WeatherSnapshot(
        weather_available=True,
        weather_observed_at=str(times[index]),
    )
    for field in WEATHER_FIELDS:
        values = hourly.get(field) or []
        if index < len(values):
            setattr(snapshot, field, values[index])
    return snapshot


async def collect_board_source(
    client: httpx.AsyncClient,
    source: str,
    source_url: str,
) -> tuple[list[BoardFlight], CollectionError | None]:
    try:
        html = await fetch_text(client, source_url)
        rows = parse_board_html(html, source=source, source_url=source_url)
    except Exception as exc:
        return [], CollectionError(
            observed_at="",
            source=source,
            source_url=source_url,
            error=str(exc),
        )

    if not rows:
        return [], CollectionError(
            observed_at="",
            source=source,
            source_url=source_url,
            error="No Kunashir rows found in parsed board HTML.",
        )

    return rows, None


def build_dataset_rows(
    flights: list[BoardFlight],
    weather: WeatherSnapshot,
    observed_at: datetime,
) -> list[DatasetRow]:
    rows: list[DatasetRow] = []
    weather_values = weather.__dict__

    for flight in flights:
        flight_date, flight_time = parse_board_datetime(flight.scheduled_raw, observed_at)
        actual_date, actual_time = parse_board_datetime(flight.actual_raw, observed_at)
        status = normalize_status(flight.status_raw, flight_date, flight_time, observed_at)
        reason, reason_class = extract_reason(flight.status_raw)

        rows.append(
            DatasetRow(
                observed_at=observed_at.isoformat(),
                observation_date=observed_at.date().isoformat(),
                observation_time=observed_at.strftime("%H:%M:%S"),
                source=flight.source,
                source_url=flight.source_url,
                direction=flight.direction,
                flight_date=flight_date,
                flight_time=flight_time,
                flight_numbers=flight.flight_numbers,
                route=flight.route,
                status_raw=flight.status_raw,
                status_normalized=status,
                reason=reason,
                reason_class=reason_class,
                scheduled_time_raw=flight.scheduled_raw,
                actual_time_raw=flight.actual_raw,
                actual_date=actual_date,
                actual_time=actual_time,
                radar_flight_number=flight.radar_flight_number,
                raw_row_text=flight.raw_row_text,
                **weather_values,
            )
        )

    return rows


def append_csv(path: Path, rows: list[Any], row_type: type) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = [field.name for field in fields(row_type)]
    write_header = not path.exists() or path.stat().st_size == 0

    with path.open("a", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=names, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({name: getattr(row, name) for name in names})


async def collect_once(args: argparse.Namespace) -> tuple[int, int]:
    timezone = ZoneInfo(args.timezone)
    observed_at = datetime.now(timezone).replace(microsecond=0)

    timeout = httpx.Timeout(args.timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        sources = [(args.airport_source_name, args.airport_board_url)]
        if not args.skip_aurora:
            sources.append((args.aurora_source_name, args.aurora_status_url))

        source_results = await asyncio.gather(
            *(collect_board_source(client, source, url) for source, url in sources)
        )
        weather = await fetch_weather(
            client=client,
            observed_at=observed_at,
            latitude=args.latitude,
            longitude=args.longitude,
            timezone_name=args.timezone,
        )

    flights: list[BoardFlight] = []
    errors: list[CollectionError] = []
    for rows, error in source_results:
        flights.extend(rows)
        if error:
            errors.append(
                CollectionError(
                    observed_at=observed_at.isoformat(),
                    source=error.source,
                    source_url=error.source_url,
                    error=error.error,
                )
            )

    dataset_rows = build_dataset_rows(flights=flights, weather=weather, observed_at=observed_at)
    append_csv(Path(args.output), dataset_rows, DatasetRow)

    if errors:
        append_csv(Path(args.errors_output), errors, CollectionError)

    print(
        f"{observed_at.isoformat()} collected_rows={len(dataset_rows)} "
        f"errors={len(errors)} output={args.output}"
    )
    return len(dataset_rows), len(errors)


async def run_loop(args: argparse.Namespace) -> None:
    while True:
        await collect_once(args)
        await asyncio.sleep(args.interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect hourly Kunashir flight board status and Open-Meteo weather into CSV."
    )
    parser.add_argument("--output", default=os.getenv("FLIGHT_STATUS_DATASET_PATH", DEFAULT_OUTPUT))
    parser.add_argument("--errors-output", default=os.getenv("FLIGHT_STATUS_ERRORS_PATH", DEFAULT_ERRORS_OUTPUT))
    parser.add_argument("--airport-board-url", default=os.getenv("AIRPORT_BOARD_URL", AIRPORT_BOARD_URL))
    parser.add_argument("--aurora-status-url", default=AURORA_STATUS_URL)
    parser.add_argument("--airport-source-name", default="airportus")
    parser.add_argument("--aurora-source-name", default="aurora")
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--latitude", type=float, default=DEFAULT_AIRPORT_LATITUDE)
    parser.add_argument("--longitude", type=float, default=DEFAULT_AIRPORT_LONGITUDE)
    parser.add_argument("--timeout-seconds", type=float, default=20)
    parser.add_argument("--loop", action="store_true", help="Keep collecting forever.")
    parser.add_argument("--interval-seconds", type=int, default=3600)
    parser.add_argument("--skip-aurora", action="store_true", help="Do not attempt the Aurora website source.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.loop:
        asyncio.run(run_loop(args))
    else:
        asyncio.run(collect_once(args))


if __name__ == "__main__":
    main()
