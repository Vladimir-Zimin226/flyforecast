import argparse
import asyncio
import csv
import os
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from collect_kunashir_status import (
    AIRPORT_BOARD_URL,
    OPEN_METEO_URL,
    AirportBoardParser,
    BoardFlight,
    CollectionError,
    clean_text,
    extract_reason,
    fetch_text,
    normalize_status,
    parse_board_datetime,
    parse_board_text_fallback,
)


DEFAULT_OUTPUT_DIR = "data/raw/sakhalin_airports"
DEFAULT_BOARD_OUTPUT = f"{DEFAULT_OUTPUT_DIR}/sakhalin_airport_board_hourly.csv"
DEFAULT_WEATHER_OUTPUT = f"{DEFAULT_OUTPUT_DIR}/sakhalin_airport_weather_hourly.csv"
DEFAULT_ERRORS_OUTPUT = f"{DEFAULT_OUTPUT_DIR}/collection_errors.csv"
DEFAULT_TIMEZONE = os.getenv("AIRPORT_TIMEZONE", "Asia/Sakhalin")

WEATHER_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "pressure_msl",
    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "visibility",
]


@dataclass(frozen=True)
class TargetAirport:
    airport_code: str
    iata_code: str
    icao_code: str
    name: str
    city: str
    latitude: float
    longitude: float
    route_aliases: tuple[str, ...]
    route_exclude_aliases: tuple[str, ...] = ()


TARGET_AIRPORTS = [
    TargetAirport(
        airport_code="OHH",
        iata_code="OHH",
        icao_code="UHSH",
        name="Оха (Новостройка)",
        city="Оха",
        latitude=53.51778,
        longitude=142.8800917,
        route_aliases=("Оха", "Новостройка"),
    ),
    TargetAirport(
        airport_code="NGL",
        iata_code="NGK",
        icao_code="UHSN",
        name="Ноглики",
        city="Ноглики",
        latitude=51.78389,
        longitude=143.14167,
        route_aliases=("Ноглики",),
    ),
    TargetAirport(
        airport_code="EKS",
        iata_code="EKS",
        icao_code="UHSK",
        name="Шахтёрск",
        city="Шахтёрск",
        latitude=49.19194,
        longitude=142.08167,
        route_aliases=("Шахтёрск", "Шахтерск"),
    ),
    TargetAirport(
        airport_code="ZZO",
        iata_code="ZZO",
        icao_code="UHSO",
        name="Зональное",
        city="Тымовское",
        latitude=50.66833,
        longitude=142.76,
        route_aliases=("Зональное", "Тымовское", "Кировское"),
    ),
    TargetAirport(
        airport_code="ALS_SAKH",
        iata_code="",
        icao_code="",
        name="Александровск-Сахалинский",
        city="Александровск-Сахалинский",
        latitude=50.9,
        longitude=142.15,
        route_aliases=("Александровск-Сахалинский", "Александровск"),
    ),
    TargetAirport(
        airport_code="ITU",
        iata_code="ITU",
        icao_code="UHSI",
        name="Ясный (Курильск, Итуруп)",
        city="Курильск",
        latitude=45.25639,
        longitude=147.95583,
        route_aliases=("Курильск", "Итуруп", "Ясный"),
        route_exclude_aliases=("Южно-Курильск",),
    ),
]


@dataclass
class BoardObservationRow:
    observed_at: str
    observation_date: str
    observation_time: str
    airport_code: str
    iata_code: str
    icao_code: str
    airport_name: str
    airport_city: str
    route_aliases: str
    board_match_found: bool
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
    raw_row_text: str


@dataclass
class WeatherHourlyRow:
    observed_at: str
    observation_date: str
    observation_time: str
    airport_code: str
    iata_code: str
    icao_code: str
    airport_name: str
    airport_city: str
    latitude: float
    longitude: float
    weather_source: str
    weather_error: str
    forecast_time: str
    forecast_date: str
    forecast_hour: int | None
    temperature_2m: float | None
    relative_humidity_2m: float | None
    dew_point_2m: float | None
    pressure_msl: float | None
    cloud_cover: float | None
    cloud_cover_low: float | None
    cloud_cover_mid: float | None
    cloud_cover_high: float | None
    precipitation: float | None
    rain: float | None
    snowfall: float | None
    weather_code: float | None
    wind_speed_10m: float | None
    wind_gusts_10m: float | None
    wind_direction_10m: float | None
    visibility: float | None


def parse_board_html_all(html: str, source: str, source_url: str) -> list[BoardFlight]:
    parser = AirportBoardParser(source=source, source_url=source_url)
    parser.feed(html)
    if parser.rows:
        return parser.rows
    return parse_board_text_fallback(html, source=source, source_url=source_url)


def route_matches_airport(route: str, airport: TargetAirport) -> bool:
    route_lower = clean_text(route).lower()
    if any(alias.lower() in route_lower for alias in airport.route_exclude_aliases):
        return False
    return any(alias.lower() in route_lower for alias in airport.route_aliases)


def build_board_rows(
    flights: list[BoardFlight],
    observed_at: datetime,
    *,
    include_arrivals: bool,
) -> list[BoardObservationRow]:
    rows: list[BoardObservationRow] = []
    for airport in TARGET_AIRPORTS:
        airport_flights = [
            flight
            for flight in flights
            if route_matches_airport(flight.route, airport)
            and (include_arrivals or flight.direction == "departure")
        ]

        if not airport_flights:
            rows.append(
                BoardObservationRow(
                    observed_at=observed_at.isoformat(),
                    observation_date=observed_at.date().isoformat(),
                    observation_time=observed_at.strftime("%H:%M:%S"),
                    airport_code=airport.airport_code,
                    iata_code=airport.iata_code,
                    icao_code=airport.icao_code,
                    airport_name=airport.name,
                    airport_city=airport.city,
                    route_aliases=";".join(airport.route_aliases),
                    board_match_found=False,
                    source="airportus",
                    source_url=AIRPORT_BOARD_URL,
                    direction="",
                    flight_date="",
                    flight_time="",
                    flight_numbers="",
                    route="",
                    status_raw="",
                    status_normalized="no_board_rows",
                    reason="",
                    reason_class="no_board_rows",
                    scheduled_time_raw="",
                    actual_time_raw="",
                    actual_date="",
                    actual_time="",
                    raw_row_text="",
                )
            )
            continue

        for flight in airport_flights:
            flight_date, flight_time = parse_board_datetime(flight.scheduled_raw, observed_at)
            actual_date, actual_time = parse_board_datetime(flight.actual_raw, observed_at)
            status = normalize_status(flight.status_raw, flight_date, flight_time, observed_at)
            reason, reason_class = extract_reason(flight.status_raw)

            rows.append(
                BoardObservationRow(
                    observed_at=observed_at.isoformat(),
                    observation_date=observed_at.date().isoformat(),
                    observation_time=observed_at.strftime("%H:%M:%S"),
                    airport_code=airport.airport_code,
                    iata_code=airport.iata_code,
                    icao_code=airport.icao_code,
                    airport_name=airport.name,
                    airport_city=airport.city,
                    route_aliases=";".join(airport.route_aliases),
                    board_match_found=True,
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
                    raw_row_text=flight.raw_row_text,
                )
            )

    return rows


async def collect_board_rows(
    client: httpx.AsyncClient,
    observed_at: datetime,
    *,
    board_url: str,
    include_arrivals: bool,
) -> tuple[list[BoardObservationRow], CollectionError | None]:
    try:
        html = await fetch_text(client, board_url)
        flights = parse_board_html_all(html, source="airportus", source_url=board_url)
    except Exception as exc:
        return [], CollectionError(
            observed_at=observed_at.isoformat(),
            source="airportus",
            source_url=board_url,
            error=f"board_fetch_or_parse_failed: {exc}",
        )

    return (
        build_board_rows(
            flights=flights,
            observed_at=observed_at,
            include_arrivals=include_arrivals,
        ),
        None,
    )


def parse_forecast_hour(value: str) -> int | None:
    try:
        return datetime.fromisoformat(value).hour
    except ValueError:
        return None


async def fetch_airport_weather(
    client: httpx.AsyncClient,
    airport: TargetAirport,
    observed_at: datetime,
    *,
    timezone_name: str,
    forecast_days: int,
) -> tuple[list[WeatherHourlyRow], CollectionError | None]:
    params = {
        "latitude": airport.latitude,
        "longitude": airport.longitude,
        "timezone": timezone_name,
        "forecast_days": forecast_days,
        "hourly": ",".join(WEATHER_FIELDS),
    }

    try:
        response = await client.get(OPEN_METEO_URL, params=params)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return [], CollectionError(
            observed_at=observed_at.isoformat(),
            source="open-meteo",
            source_url=OPEN_METEO_URL,
            error=f"{airport.airport_code}: weather_fetch_failed: {exc}",
        )

    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    rows: list[WeatherHourlyRow] = []
    for index, forecast_time in enumerate(times):
        values: dict[str, Any] = {}
        for field in WEATHER_FIELDS:
            field_values = hourly.get(field) or []
            values[field] = field_values[index] if index < len(field_values) else None

        rows.append(
            WeatherHourlyRow(
                observed_at=observed_at.isoformat(),
                observation_date=observed_at.date().isoformat(),
                observation_time=observed_at.strftime("%H:%M:%S"),
                airport_code=airport.airport_code,
                iata_code=airport.iata_code,
                icao_code=airport.icao_code,
                airport_name=airport.name,
                airport_city=airport.city,
                latitude=airport.latitude,
                longitude=airport.longitude,
                weather_source="open-meteo",
                weather_error="",
                forecast_time=forecast_time,
                forecast_date=forecast_time[:10],
                forecast_hour=parse_forecast_hour(forecast_time),
                **values,
            )
        )

    if not rows:
        return [], CollectionError(
            observed_at=observed_at.isoformat(),
            source="open-meteo",
            source_url=OPEN_METEO_URL,
            error=f"{airport.airport_code}: Open-Meteo returned no hourly rows.",
        )

    return rows, None


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


async def collect_once(args: argparse.Namespace) -> tuple[int, int, int]:
    timezone = ZoneInfo(args.timezone)
    observed_at = datetime.now(timezone).replace(microsecond=0)
    timeout = httpx.Timeout(args.timeout_seconds)

    async with httpx.AsyncClient(timeout=timeout) as client:
        board_task = collect_board_rows(
            client,
            observed_at,
            board_url=args.airport_board_url,
            include_arrivals=args.include_arrivals,
        )
        weather_tasks = [
            fetch_airport_weather(
                client,
                airport,
                observed_at,
                timezone_name=args.timezone,
                forecast_days=args.forecast_days,
            )
            for airport in TARGET_AIRPORTS
        ]
        board_result, *weather_results = await asyncio.gather(board_task, *weather_tasks)

    board_rows, board_error = board_result
    weather_rows: list[WeatherHourlyRow] = []
    errors: list[CollectionError] = []
    if board_error:
        errors.append(board_error)

    for rows, error in weather_results:
        weather_rows.extend(rows)
        if error:
            errors.append(error)

    if board_rows:
        append_csv(Path(args.board_output), board_rows, BoardObservationRow)
    if weather_rows:
        append_csv(Path(args.weather_output), weather_rows, WeatherHourlyRow)
    if errors:
        append_csv(Path(args.errors_output), errors, CollectionError)

    print(
        f"{observed_at.isoformat()} "
        f"board_rows={len(board_rows)} weather_rows={len(weather_rows)} errors={len(errors)} "
        f"board_output={args.board_output} weather_output={args.weather_output}"
    )
    return len(board_rows), len(weather_rows), len(errors)


async def run_loop(args: argparse.Namespace) -> None:
    while True:
        await collect_once(args)
        await asyncio.sleep(args.interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect Yuzhno-Sakhalinsk board departures and detailed Open-Meteo hourly weather "
            "for selected Sakhalin/Kuril airports."
        )
    )
    parser.add_argument("--board-output", default=os.getenv("SAKHALIN_AIRPORTS_BOARD_OUTPUT", DEFAULT_BOARD_OUTPUT))
    parser.add_argument("--weather-output", default=os.getenv("SAKHALIN_AIRPORTS_WEATHER_OUTPUT", DEFAULT_WEATHER_OUTPUT))
    parser.add_argument("--errors-output", default=os.getenv("SAKHALIN_AIRPORTS_ERRORS_OUTPUT", DEFAULT_ERRORS_OUTPUT))
    parser.add_argument("--airport-board-url", default=os.getenv("AIRPORT_BOARD_URL", AIRPORT_BOARD_URL))
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--forecast-days", type=int, default=int(os.getenv("SAKHALIN_AIRPORTS_FORECAST_DAYS", "2")))
    parser.add_argument("--timeout-seconds", type=float, default=float(os.getenv("SAKHALIN_AIRPORTS_TIMEOUT_SECONDS", "20")))
    parser.add_argument("--include-arrivals", action="store_true", help="Also collect arrival rows for target airports.")
    parser.add_argument("--loop", action="store_true", help="Keep collecting forever.")
    parser.add_argument("--interval-seconds", type=int, default=int(os.getenv("SAKHALIN_AIRPORTS_INTERVAL_SECONDS", "900")))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.loop:
        asyncio.run(run_loop(args))
    else:
        asyncio.run(collect_once(args))


if __name__ == "__main__":
    main()
