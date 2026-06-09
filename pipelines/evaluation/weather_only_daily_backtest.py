from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


DEFAULT_TRAINING_DATASET = Path("data/processed/training_dataset_v1.csv")


def float_or_none(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def row_date(row: dict[str, str]) -> str:
    return row.get("date") or row.get("\ufeffdate") or ""


def row_year(row: dict[str, str]) -> str:
    return row_date(row)[:4]


def wind_sector(degrees: float | None) -> str | None:
    if degrees is None:
        return None
    sectors = ("N", "NE", "E", "SE", "S", "SW", "W", "NW")
    return sectors[int(((degrees + 22.5) % 360) // 45)]


def mendeleyevo(row: dict[str, str], field: str) -> float | None:
    return float_or_none(row.get(f"mendeleyevo_{field}"))


def daily_weather_score(row: dict[str, str]) -> float:
    """Daily approximation of the experimental 0-100 weather-only operability score."""
    visibility_mean = mendeleyevo(row, "visibility_mean")
    visibility_min = mendeleyevo(row, "visibility_min")
    low_cloud = mendeleyevo(row, "cloud_cover_low_mean")
    humidity = mendeleyevo(row, "relative_humidity_2m_mean")
    dew_point_spread = mendeleyevo(row, "dew_point_spread_mean")
    wind_speed = mendeleyevo(row, "wind_speed_10m_max")
    wind_gust = mendeleyevo(row, "wind_gusts_10m_max")
    direction = wind_sector(mendeleyevo(row, "wind_direction_10m_dominant"))
    precipitation = mendeleyevo(row, "precipitation_sum")
    pressure = mendeleyevo(row, "pressure_msl_min")
    weather_code = mendeleyevo(row, "weather_code_mode")

    score = 0.0

    if visibility_mean is None:
        visibility_points = 12
    elif visibility_mean >= 15000:
        visibility_points = 25
    elif visibility_mean >= 8000:
        visibility_points = 21
    elif visibility_mean >= 3000:
        visibility_points = 15
    elif visibility_mean >= 1000:
        visibility_points = 8
    else:
        visibility_points = 2
    if visibility_min is not None:
        if visibility_min <= 300:
            visibility_points -= 8
        elif visibility_min <= 1000:
            visibility_points -= 5
        elif visibility_min <= 3000:
            visibility_points -= 2
    score += max(0, visibility_points)

    if low_cloud is None:
        score += 7
    elif low_cloud < 25:
        score += 15
    elif low_cloud < 50:
        score += 12
    elif low_cloud < 75:
        score += 7
    elif low_cloud < 90:
        score += 3

    if humidity is None or dew_point_spread is None:
        score += 5
    elif humidity < 85 and dew_point_spread > 2:
        score += 10
    elif humidity < 92 and dew_point_spread > 1.5:
        score += 7
    elif humidity < 96 and dew_point_spread > 1:
        score += 4

    wind = wind_speed or 0
    gust = wind_gust or 0
    if wind_speed is None and wind_gust is None:
        score += 8
    elif gust < 60 and wind < 25:
        score += 15
    elif gust < 72 and wind < 30:
        score += 10
    elif gust < 90 and wind < 35:
        score += 5

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
    score += direction_points

    if precipitation is None:
        score += 5
    elif precipitation == 0:
        score += 10
    elif precipitation <= 2:
        score += 7
    elif precipitation <= 5:
        score += 4
    elif precipitation <= 10:
        score += 2

    if pressure is None:
        score += 5
    elif pressure > 1015:
        score += 10
    elif pressure > 1010:
        score += 8
    elif pressure > 1005:
        score += 6
    elif pressure > 1000:
        score += 4
    elif pressure > 995:
        score += 2

    if weather_code is None:
        score += 3
    elif int(weather_code) in {45, 48} or weather_code >= 61:
        score += 0
    elif weather_code >= 51:
        score += 2
    elif weather_code <= 3:
        score += 5
    else:
        score += 3

    return score


def daily_veto(row: dict[str, str], score: float) -> bool:
    humidity = mendeleyevo(row, "relative_humidity_2m_mean") or 0
    visibility_min = mendeleyevo(row, "visibility_min")
    precipitation = mendeleyevo(row, "precipitation_sum") or 0
    low_cloud = mendeleyevo(row, "cloud_cover_low_mean") or 0
    pressure = mendeleyevo(row, "pressure_msl_min")
    wind = mendeleyevo(row, "wind_speed_10m_max") or 0
    direction = wind_sector(mendeleyevo(row, "wind_direction_10m_dominant"))

    if score < 50 and humidity > 90:
        return True
    if visibility_min is not None and visibility_min <= 300 and humidity >= 92 and low_cloud >= 80:
        return True
    if pressure is not None and pressure <= 1000 and (precipitation > 2 or low_cloud >= 75 or wind >= 25):
        return True
    return direction in {"E", "SE"} and wind >= 35


def predict(row: dict[str, str], threshold: int, use_veto: bool) -> str:
    score = daily_weather_score(row)
    prediction = "completed" if score >= threshold else "cancelled"
    if use_veto and prediction == "completed" and daily_veto(row, score):
        return "cancelled"
    return prediction


def metrics(rows: list[dict[str, str]], threshold: int, use_veto: bool) -> dict[str, float | int]:
    correct = 0
    yes = 0
    false_yes = 0
    false_no = 0
    score_sum = 0.0

    for row in rows:
        prediction = predict(row, threshold=threshold, use_veto=use_veto)
        actual = row["status"]
        score_sum += daily_weather_score(row)
        correct += prediction == actual
        yes += prediction == "completed"
        false_yes += prediction == "completed" and actual == "cancelled"
        false_no += prediction == "cancelled" and actual == "completed"

    total = len(rows)
    return {
        "n": total,
        "accuracy": correct / total if total else 0.0,
        "yes": yes,
        "no": total - yes,
        "false_yes": false_yes,
        "false_no": false_no,
        "avg_score": score_sum / total if total else 0.0,
    }


def choose_best_threshold(rows: list[dict[str, str]]) -> tuple[int, bool, dict[str, float | int]]:
    candidates: list[tuple[float, int, int, int, bool, dict[str, float | int]]] = []
    for use_veto in (False, True):
        for threshold in range(20, 91):
            result = metrics(rows, threshold=threshold, use_veto=use_veto)
            candidates.append(
                (
                    float(result["accuracy"]),
                    -int(result["false_yes"]),
                    -int(result["false_no"]),
                    threshold,
                    use_veto,
                    result,
                )
            )
    _, _, _, threshold, use_veto, result = max(candidates)
    return threshold, use_veto, result


def print_metric_line(prefix: str, result: dict[str, float | int]) -> None:
    printable = {
        key: round(value, 3) if isinstance(value, float) else value
        for key, value in result.items()
    }
    print(prefix, printable)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest daily weather-only score on training data.")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_TRAINING_DATASET)
    parser.add_argument("--threshold", type=int, default=49)
    parser.add_argument("--use-veto", action="store_true")
    args = parser.parse_args()

    with args.dataset.open(newline="", encoding="utf-8-sig") as handle:
        rows = [row for row in csv.DictReader(handle) if row.get("status") in {"completed", "cancelled"}]

    completed = sum(row["status"] == "completed" for row in rows)
    cancelled = len(rows) - completed
    print(f"rows={len(rows)} completed={completed} cancelled={cancelled}")

    threshold, use_veto, best_result = choose_best_threshold(rows)
    print_metric_line(f"best_full threshold={threshold} use_veto={use_veto}", best_result)
    print_metric_line(
        f"configured threshold={args.threshold} use_veto={args.use_veto}",
        metrics(rows, threshold=args.threshold, use_veto=args.use_veto),
    )

    print("\nby_year for best_full")
    for year in sorted({row_year(row) for row in rows}):
        year_rows = [row for row in rows if row_year(row) == year]
        if len(year_rows) < 5:
            continue
        print_metric_line(year, metrics(year_rows, threshold=threshold, use_veto=use_veto))

    print("\nleave_one_year_out_threshold_tuning")
    heldout_results = []
    for year in sorted({row_year(row) for row in rows}):
        test_rows = [row for row in rows if row_year(row) == year]
        if len(test_rows) < 20:
            continue
        train_rows = [row for row in rows if row_year(row) != year]
        train_threshold, train_use_veto, _ = choose_best_threshold(train_rows)
        test_result = metrics(test_rows, threshold=train_threshold, use_veto=train_use_veto)
        heldout_results.append(test_result)
        print_metric_line(
            f"{year} train_threshold={train_threshold} train_use_veto={train_use_veto}",
            test_result,
        )

    heldout_total = sum(int(result["n"]) for result in heldout_results)
    if heldout_total:
        weighted_accuracy = sum(float(result["accuracy"]) * int(result["n"]) for result in heldout_results) / heldout_total
        false_yes = sum(int(result["false_yes"]) for result in heldout_results)
        false_no = sum(int(result["false_no"]) for result in heldout_results)
        print(
            "\nleave_one_year_out_weighted",
            {
                "n": heldout_total,
                "accuracy": round(weighted_accuracy, 3),
                "false_yes": false_yes,
                "false_no": false_no,
            },
        )

    print("\nscore_bands")
    for lower, upper in ((0, 30), (30, 40), (40, 50), (50, 60), (60, 70), (70, 80), (80, 90), (90, 101)):
        band = [row for row in rows if lower <= daily_weather_score(row) < upper]
        if not band:
            continue
        completed_rate = sum(row["status"] == "completed" for row in band) / len(band)
        print(
            {
                "score_range": f"{lower}-{upper}",
                "n": len(band),
                "completed_rate": round(completed_rate, 3),
                "avg_score": round(sum(daily_weather_score(row) for row in band) / len(band), 1),
            }
        )


if __name__ == "__main__":
    main()
