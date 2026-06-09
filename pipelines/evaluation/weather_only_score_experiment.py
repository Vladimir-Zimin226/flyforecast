from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


DEFAULT_EXPORT_DIR = Path("data/interim/analysis/export_20260609_121758")
DEFAULT_HOURLY_PATH = DEFAULT_EXPORT_DIR / "open_meteo_hourly_2026-05-23_2026-06-09.csv"
DEFAULT_BOARD_PATH = DEFAULT_EXPORT_DIR / "flight_status/kunashir_flight_status_hourly.csv"


@dataclass(frozen=True)
class DayScore:
    date: str
    outcome: str
    window_start: int
    window_end: int
    scheduled_departure_hours: tuple[int, ...]
    score: float
    best_hours: tuple[int, ...]
    best_hour_scores: tuple[float, ...]
    prediction: str


def float_or_none(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_hour(value: str | None) -> int | None:
    if not value or ":" not in value:
        return None
    try:
        return int(value.split(":", 1)[0])
    except ValueError:
        return None


def wind_sector(degrees: float | None) -> str | None:
    if degrees is None:
        return None
    sectors = ("N", "NE", "E", "SE", "S", "SW", "W", "NW")
    return sectors[int(((degrees + 22.5) % 360) // 45)]


def corrected_same_day_outcome(rows: list[dict[str, str]], flight_date: str) -> str | None:
    completed_statuses = {"departed", "arrived", "in_flight"}
    has_completed_same_day = False
    has_next_day_move = False
    has_cancel_like = False
    has_scheduled = False

    for row in rows:
        status = row.get("status_normalized", "").strip()
        actual_date = row.get("actual_date", "").strip()

        if status in completed_statuses:
            if not actual_date or actual_date == flight_date:
                has_completed_same_day = True
            elif actual_date > flight_date:
                has_next_day_move = True

        if status in {"combined", "delayed"}:
            if actual_date and actual_date > flight_date:
                has_next_day_move = True
            elif not actual_date:
                has_cancel_like = True

        if status == "cancelled":
            has_cancel_like = True
        elif status == "scheduled":
            has_scheduled = True

    if has_completed_same_day:
        return "completed"
    if has_next_day_move or has_cancel_like:
        return "cancelled"
    if has_scheduled:
        return "planned_only"
    return None


def scheduled_departure_window(rows: list[dict[str, str]]) -> tuple[int, int, tuple[int, ...]]:
    departure_hours: list[int] = []
    for row in rows:
        if row.get("direction") != "departure":
            continue
        hour = parse_hour(row.get("flight_time"))
        if hour is not None:
            departure_hours.append(hour)

    if not departure_hours:
        return 8, 20, ()

    unique_hours = tuple(sorted(set(departure_hours)))
    return max(6, min(unique_hours) - 2), min(22, max(unique_hours) + 5), unique_hours


def hourly_weather_score(row: dict[str, str]) -> tuple[float, dict[str, float]]:
    """Experimental 0-100 weather operability score, without flight history."""
    visibility = float_or_none(row.get("visibility"))
    low_cloud = float_or_none(row.get("cloud_cover_low"))
    humidity = float_or_none(row.get("relative_humidity_2m"))
    temperature = float_or_none(row.get("temperature_2m"))
    dew_point = float_or_none(row.get("dew_point_2m"))
    wind_speed = float_or_none(row.get("wind_speed_10m"))
    wind_gust = float_or_none(row.get("wind_gusts_10m"))
    direction = wind_sector(float_or_none(row.get("wind_direction_10m")))
    precipitation = float_or_none(row.get("precipitation"))
    pressure = float_or_none(row.get("pressure_msl"))
    weather_code_raw = float_or_none(row.get("weather_code"))
    weather_code = int(weather_code_raw) if weather_code_raw is not None else None
    dew_point_spread = None
    if temperature is not None and dew_point is not None:
        dew_point_spread = temperature - dew_point

    parts: dict[str, float] = {}

    if visibility is None:
        parts["visibility"] = 12
    elif visibility >= 10000:
        parts["visibility"] = 25
    elif visibility >= 6000:
        parts["visibility"] = 22
    elif visibility >= 3000:
        parts["visibility"] = 16
    elif visibility >= 1000:
        parts["visibility"] = 9
    elif visibility >= 500:
        parts["visibility"] = 4
    else:
        parts["visibility"] = 0

    if low_cloud is None:
        parts["low_cloud"] = 7
    elif low_cloud <= 25:
        parts["low_cloud"] = 15
    elif low_cloud <= 50:
        parts["low_cloud"] = 12
    elif low_cloud <= 75:
        parts["low_cloud"] = 7
    elif low_cloud <= 90:
        parts["low_cloud"] = 3
    else:
        parts["low_cloud"] = 0

    if humidity is None or dew_point_spread is None:
        parts["humidity_dew"] = 5
    elif humidity < 85 and dew_point_spread > 2:
        parts["humidity_dew"] = 10
    elif humidity < 92 and dew_point_spread > 1.5:
        parts["humidity_dew"] = 7
    elif humidity < 96 and dew_point_spread > 1:
        parts["humidity_dew"] = 4
    else:
        parts["humidity_dew"] = 0

    wind = wind_speed or 0
    gust = wind_gust or 0
    if wind_gust is None and wind_speed is None:
        parts["wind"] = 8
    elif gust < 60 and wind < 25:
        parts["wind"] = 15
    elif gust < 72 and wind < 30:
        parts["wind"] = 10
    elif gust < 90 and wind < 35:
        parts["wind"] = 5
    else:
        parts["wind"] = 0

    if direction in {"SW", "W"}:
        direction_points = 10
    elif direction in {"NW", "N"}:
        direction_points = 7
    elif direction == "S":
        direction_points = 5
    elif direction in {"NE", "E", "SE"}:
        direction_points = 4
    else:
        direction_points = 5
    if direction in {"E", "SE"} and (wind >= 25 or gust >= 72):
        direction_points = 0
    parts["direction"] = direction_points

    if precipitation is None:
        parts["precipitation"] = 5
    elif precipitation == 0:
        parts["precipitation"] = 10
    elif precipitation <= 0.2:
        parts["precipitation"] = 8
    elif precipitation <= 1:
        parts["precipitation"] = 5
    elif precipitation <= 3:
        parts["precipitation"] = 2
    else:
        parts["precipitation"] = 0

    if pressure is None:
        parts["pressure"] = 5
    elif pressure > 1015:
        parts["pressure"] = 10
    elif pressure > 1010:
        parts["pressure"] = 8
    elif pressure > 1005:
        parts["pressure"] = 6
    elif pressure > 1000:
        parts["pressure"] = 4
    elif pressure > 995:
        parts["pressure"] = 2
    else:
        parts["pressure"] = 0

    if weather_code is None:
        parts["weather_code"] = 3
    elif weather_code in {45, 48} or weather_code >= 61:
        parts["weather_code"] = 0
    elif weather_code >= 51:
        parts["weather_code"] = 2
    elif weather_code <= 3:
        parts["weather_code"] = 5
    else:
        parts["weather_code"] = 3

    return sum(parts.values()), parts


def load_csv_by_date(path: Path, date_key: str) -> dict[str, list[dict[str, str]]]:
    rows_by_date: dict[str, list[dict[str, str]]] = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            date = row.get(date_key)
            if date:
                rows_by_date[date].append(row)
    return rows_by_date


def score_day(
    flight_date: str,
    hourly_rows: list[dict[str, str]],
    board_rows: list[dict[str, str]],
    threshold: float,
) -> DayScore:
    window_start, window_end, scheduled_hours = scheduled_departure_window(board_rows)

    candidates: list[tuple[int, float]] = []
    for row in hourly_rows:
        time = row.get("time", "")
        if "T" not in time:
            continue
        hour = int(time.split("T", 1)[1].split(":", 1)[0])
        if window_start <= hour <= window_end:
            score, _ = hourly_weather_score(row)
            candidates.append((hour, score))

    best_score = 0.0
    best_window: tuple[tuple[int, float], ...] = ()
    for index, current in enumerate(candidates):
        window = [current]
        if index + 1 < len(candidates) and candidates[index + 1][0] == current[0] + 1:
            window.append(candidates[index + 1])
        average_score = sum(item[1] for item in window) / len(window)
        if average_score > best_score:
            best_score = average_score
            best_window = tuple(window)

    outcome = corrected_same_day_outcome(board_rows, flight_date) or "unknown"
    prediction = "completed" if best_score >= threshold else "cancelled"
    return DayScore(
        date=flight_date,
        outcome=outcome,
        window_start=window_start,
        window_end=window_end,
        scheduled_departure_hours=scheduled_hours,
        score=round(best_score, 1),
        best_hours=tuple(item[0] for item in best_window),
        best_hour_scores=tuple(round(item[1], 1) for item in best_window),
        prediction=prediction,
    )


def print_summary(scores: list[DayScore], threshold: float) -> None:
    evaluated = [score for score in scores if score.outcome in {"completed", "cancelled"}]
    correct = sum(score.prediction == score.outcome for score in evaluated)
    false_yes = sum(score.prediction == "completed" and score.outcome == "cancelled" for score in evaluated)
    false_no = sum(score.prediction == "cancelled" and score.outcome == "completed" for score in evaluated)
    yes_count = sum(score.prediction == "completed" for score in evaluated)

    print(f"threshold={threshold:g}")
    print(f"evaluated_days={len(evaluated)}")
    print(f"accuracy={correct / len(evaluated):.3f}" if evaluated else "accuracy=n/a")
    print(f"yes={yes_count}")
    print(f"no={len(evaluated) - yes_count}")
    print(f"false_yes={false_yes}")
    print(f"false_no={false_no}")


def print_threshold_sweep(scores: list[DayScore]) -> None:
    evaluated = [score for score in scores if score.outcome in {"completed", "cancelled"}]
    print("\nthreshold_sweep")
    print("threshold,accuracy,yes,false_yes,false_no")
    for threshold in range(40, 91, 5):
        correct = 0
        yes_count = 0
        false_yes = 0
        false_no = 0
        for score in evaluated:
            prediction = "completed" if score.score >= threshold else "cancelled"
            correct += prediction == score.outcome
            yes_count += prediction == "completed"
            false_yes += prediction == "completed" and score.outcome == "cancelled"
            false_no += prediction == "cancelled" and score.outcome == "completed"
        accuracy = correct / len(evaluated) if evaluated else 0
        print(f"{threshold},{accuracy:.3f},{yes_count},{false_yes},{false_no}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Experimental weather-only 0-100 flight operability score."
    )
    parser.add_argument("--hourly-path", type=Path, default=DEFAULT_HOURLY_PATH)
    parser.add_argument("--board-path", type=Path, default=DEFAULT_BOARD_PATH)
    parser.add_argument("--start-date", default="2026-05-23")
    parser.add_argument("--end-date", default="2026-06-09")
    parser.add_argument("--threshold", type=float, default=50)
    args = parser.parse_args()

    hourly_by_date = load_csv_by_date(args.hourly_path, "date")
    board_by_date = load_csv_by_date(args.board_path, "flight_date")

    scores: list[DayScore] = []
    for flight_date in sorted(hourly_by_date):
        if flight_date < args.start_date or flight_date > args.end_date:
            continue
        scores.append(
            score_day(
                flight_date=flight_date,
                hourly_rows=hourly_by_date[flight_date],
                board_rows=board_by_date.get(flight_date, []),
                threshold=args.threshold,
            )
        )

    print_summary(scores, args.threshold)
    print("\ndaily_scores")
    print("date,outcome,score,prediction,window,scheduled_departures,best_hours,best_hour_scores")
    for score in scores:
        if score.outcome not in {"completed", "cancelled"}:
            continue
        print(
            ",".join(
                [
                    score.date,
                    score.outcome,
                    f"{score.score:.1f}",
                    score.prediction,
                    f"{score.window_start:02d}-{score.window_end:02d}",
                    "|".join(f"{hour:02d}" for hour in score.scheduled_departure_hours) or "-",
                    "|".join(f"{hour:02d}" for hour in score.best_hours) or "-",
                    "|".join(f"{value:.1f}" for value in score.best_hour_scores) or "-",
                ]
            )
        )

    print_threshold_sweep(scores)


if __name__ == "__main__":
    main()
