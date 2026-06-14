import argparse
import csv
import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


DATA_VERSION = "historical-only-2026-06-14"
TARGET_DATA_VERSION = "telegram-v2-plus-historical-board-manual-v4-historical-only-2026-06-14"
SAKHALIN_TZ = ZoneInfo("Asia/Sakhalin")
BINARY_STATUSES = {"completed", "cancelled"}

TARGET_COLUMNS = [
    "date",
    "status",
    "is_flight_completed",
    "label_confidence",
    "reason_class",
    "message_count",
    "transport_types",
    "event_date_sources",
    "year",
    "month",
    "day",
    "day_of_year",
    "month_decade",
    "season",
    "data_version",
]

MODEL_COLUMNS = [
    "date",
    "is_flight_completed",
    "dataset_split",
    "month",
    "day",
    "day_of_year",
    "month_decade",
    "season",
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
    "current_history_decision_45",
    "data_version",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a weather-free historical dataset for improving the long-horizon flight risk model."
    )
    parser.add_argument("--backup-dir", default="backup")
    parser.add_argument("--base-target", default="data/processed/dataset_daily_flights_v3.csv")
    parser.add_argument(
        "--target-output",
        default="data/processed/dataset_daily_flights_historical_only.csv",
    )
    parser.add_argument(
        "--dataset-output",
        default="data/processed/historical_only_dataset.csv",
    )
    parser.add_argument(
        "--summary-output",
        default="data/interim/training/historical_only_dataset_summary.txt",
    )
    parser.add_argument(
        "--features-output",
        default="data/interim/training/historical_only_feature_sets.json",
    )
    parser.add_argument(
        "--max-target-date",
        default="",
        help="Inclusive max target date for backup board outcomes. Defaults to backup local date minus one day.",
    )
    return parser.parse_args()


def read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        return [dict(row) for row in reader], list(reader.fieldnames or [])


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({name: "" if row.get(name) is None else row.get(name, "") for name in fieldnames})


def clean(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def parse_date(value: object) -> date | None:
    text = clean(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def parse_datetime(value: object) -> datetime | None:
    text = clean(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def to_float(value: object) -> float | None:
    text = clean(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def to_int(value: object, default: int = 0) -> int:
    number = to_float(value)
    return default if number is None else int(number)


def truthy(value: object) -> bool:
    return clean(value).lower() in {"1", "true", "yes"}


def season_for_month(month: int) -> str:
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "autumn"


def infer_backup_max_target_date(backup_dir: Path) -> str:
    manifest_path = backup_dir / "manifest.json"
    if not manifest_path.exists():
        return ""

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    created_at = parse_datetime(manifest.get("created_at"))
    if created_at is None:
        return ""
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    local_date = created_at.astimezone(SAKHALIN_TZ).date()
    return (local_date - timedelta(days=1)).isoformat()


def make_target_row(outcome: dict[str, str]) -> dict[str, object]:
    target_date = parse_date(outcome.get("target_date"))
    if target_date is None:
        raise ValueError(f"Bad target_date in board outcome: {outcome}")
    status = clean(outcome.get("status")).lower()

    return {
        "date": target_date.isoformat(),
        "status": status,
        "is_flight_completed": 1 if status == "completed" else 0,
        "label_confidence": "high",
        "reason_class": clean(outcome.get("reason_class")) or "unknown",
        "message_count": to_int(outcome.get("evidence_count"), default=1),
        "transport_types": "airplane",
        "event_date_sources": "forecast_monitor_board_outcomes",
        "year": target_date.year,
        "month": target_date.month,
        "day": target_date.day,
        "day_of_year": target_date.timetuple().tm_yday,
        "month_decade": (target_date.day - 1) // 10 + 1,
        "season": season_for_month(target_date.month),
        "data_version": TARGET_DATA_VERSION,
    }


def build_augmented_target(
    base_target: list[dict[str, str]],
    board_outcomes: list[dict[str, str]],
    max_target_date: str,
) -> list[dict[str, object]]:
    base_rows: list[dict[str, object]] = []
    for row in base_target:
        target_date = parse_date(row.get("date"))
        if target_date is None:
            continue
        new_row = {column: row.get(column, "") for column in TARGET_COLUMNS}
        new_row["date"] = target_date.isoformat()
        new_row["data_version"] = clean(row.get("data_version")) or TARGET_DATA_VERSION
        base_rows.append(new_row)

    latest_base_date = max(clean(row.get("date")) for row in base_rows)
    max_date = parse_date(max_target_date)
    additions_by_date: dict[str, dict[str, object]] = {}
    for outcome in board_outcomes:
        target_date = parse_date(outcome.get("target_date"))
        status = clean(outcome.get("status")).lower()
        if target_date is None or status not in BINARY_STATUSES or not truthy(outcome.get("is_final")):
            continue
        if target_date.isoformat() <= latest_base_date:
            continue
        if max_date is not None and target_date > max_date:
            continue
        additions_by_date[target_date.isoformat()] = make_target_row(outcome)

    existing_dates = {clean(row.get("date")) for row in base_rows}
    result = [row for row in base_rows if clean(row.get("date")) not in additions_by_date]
    for date_value in sorted(additions_by_date):
        if date_value not in existing_dates:
            result.append(additions_by_date[date_value])
    return sorted(result, key=lambda row: clean(row.get("date")))


def mean(values: list[int]) -> float | None:
    return None if not values else round(sum(values) / len(values), 6)


def laplace_probability(completed: int, total: int, fallback: float = 0.5) -> float:
    if total <= 0:
        return fallback
    return round((completed + 1) / (total + 2), 6)


def circular_distance(day_a: int, day_b: int) -> int:
    raw = abs(day_a - day_b)
    return min(raw, 366 - raw)


def streak_before(values: list[int], target: int) -> int:
    count = 0
    for value in reversed(values):
        if value == target:
            count += 1
        else:
            break
    return count


def safe_rate(values: list[int], min_count: int = 1) -> float | str:
    if len(values) < min_count:
        return ""
    value = mean(values)
    return "" if value is None else value


def split_for_index(index: int, total: int) -> str:
    train_end = math.floor(total * 0.70)
    valid_end = math.floor(total * 0.85)
    if index < train_end:
        return "train"
    if index < valid_end:
        return "valid"
    return "test"


def make_historical_dataset(target_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    rows = sorted(target_rows, key=lambda row: clean(row.get("date")))
    result: list[dict[str, object]] = []
    past_dates: list[date] = []
    past_values: list[int] = []

    for index, row in enumerate(rows):
        target_date = parse_date(row.get("date"))
        if target_date is None:
            continue

        value = to_int(row.get("is_flight_completed"))
        day_of_year = target_date.timetuple().tm_yday
        month = target_date.month
        decade = (target_date.day - 1) // 10 + 1

        month_values = [
            past_value
            for past_date, past_value in zip(past_dates, past_values)
            if past_date.month == month
        ]
        decade_values = [
            past_value
            for past_date, past_value in zip(past_dates, past_values)
            if past_date.month == month and ((past_date.day - 1) // 10 + 1) == decade
        ]
        doy_window_values = [
            past_value
            for past_date, past_value in zip(past_dates, past_values)
            if circular_distance(past_date.timetuple().tm_yday, day_of_year) <= 14
        ]
        similar_values = doy_window_values if len(doy_window_values) >= 10 else month_values
        completed_count = sum(similar_values)
        cancelled_count = len(similar_values) - completed_count

        month_probability = laplace_probability(sum(month_values), len(month_values)) if month_values else ""
        decade_probability = laplace_probability(sum(decade_values), len(decade_values)) if decade_values else ""
        historical_probability = laplace_probability(completed_count, len(similar_values))
        combined_probability = historical_probability
        if decade_probability != "":
            combined_probability = round(0.65 * historical_probability + 0.35 * float(decade_probability), 6)

        cancelled_dates = [
            past_date
            for past_date, past_value in zip(past_dates, past_values)
            if past_value == 0
        ]

        model_row = {
            "date": target_date.isoformat(),
            "is_flight_completed": value,
            "dataset_split": split_for_index(index, len(rows)),
            "month": month,
            "day": target_date.day,
            "day_of_year": day_of_year,
            "month_decade": decade,
            "season": season_for_month(month),
            "day_of_week": target_date.weekday(),
            "is_weekend": int(target_date.weekday() in {5, 6}),
            "day_of_year_sin": round(math.sin(2 * math.pi * day_of_year / 366), 6),
            "day_of_year_cos": round(math.cos(2 * math.pi * day_of_year / 366), 6),
            "month_sin": round(math.sin(2 * math.pi * month / 12), 6),
            "month_cos": round(math.cos(2 * math.pi * month / 12), 6),
            "prev_1_completed": "" if not past_values else past_values[-1],
            "prev_3_cancelled_count": sum(1 - item for item in past_values[-3:]) if past_values else "",
            "prev_7_cancelled_count": sum(1 - item for item in past_values[-7:]) if past_values else "",
            "prev_14_completed_rate": safe_rate(past_values[-14:], min_count=3),
            "prev_30_completed_rate": safe_rate(past_values[-30:], min_count=5),
            "prev_60_completed_rate": safe_rate(past_values[-60:], min_count=10),
            "prev_90_completed_rate": safe_rate(past_values[-90:], min_count=10),
            "same_month_past_count": len(month_values),
            "same_month_past_completed_rate": safe_rate(month_values),
            "same_decade_past_count": len(decade_values),
            "same_decade_past_completed_rate": safe_rate(decade_values),
            "same_doy_window_14_past_count": len(doy_window_values),
            "same_doy_window_14_completed_rate": safe_rate(doy_window_values),
            "days_since_last_cancelled": (target_date - cancelled_dates[-1]).days if cancelled_dates else "",
            "cancelled_streak_before": streak_before(past_values, target=0),
            "completed_streak_before": streak_before(past_values, target=1),
            "history_similar_days_count": len(similar_values),
            "history_completed_count": completed_count,
            "history_cancelled_count": cancelled_count,
            "history_probability_flight": historical_probability,
            "history_month_probability_flight": month_probability,
            "history_decade_probability_flight": decade_probability,
            "history_combined_probability": combined_probability,
            "current_history_decision_45": int(combined_probability >= 0.45),
            "data_version": DATA_VERSION,
        }
        result.append(model_row)

        past_dates.append(target_date)
        past_values.append(value)

    return result


def write_feature_sets(path: Path) -> None:
    target_and_split = {"date", "is_flight_completed", "dataset_split", "data_version"}
    feature_columns = [column for column in MODEL_COLUMNS if column not in target_and_split]
    model_feature_columns = [
        column for column in feature_columns
        if column not in {"current_history_decision_45"}
    ]
    feature_sets = {
        "historical_model_features": model_feature_columns,
        "current_history_probability_features": [
            "history_probability_flight",
            "history_month_probability_flight",
            "history_decade_probability_flight",
            "history_combined_probability",
        ],
        "baseline_decision_column": "current_history_decision_45",
        "target": "is_flight_completed",
        "split": "dataset_split",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(feature_sets, ensure_ascii=False, indent=2), encoding="utf-8")


def write_summary(
    path: Path,
    base_target: list[dict[str, str]],
    augmented_target: list[dict[str, object]],
    historical_dataset: list[dict[str, object]],
    max_target_date: str,
) -> None:
    base_dates = {clean(row.get("date"))[:10] for row in base_target}
    new_rows = [row for row in augmented_target if clean(row.get("date"))[:10] not in base_dates]
    status_counts: dict[str, int] = {}
    split_counts: dict[str, int] = {}
    split_completed: dict[str, int] = {}

    for row in new_rows:
        status = clean(row.get("status"))
        status_counts[status] = status_counts.get(status, 0) + 1

    for row in historical_dataset:
        split = clean(row.get("dataset_split"))
        split_counts[split] = split_counts.get(split, 0) + 1
        split_completed[split] = split_completed.get(split, 0) + to_int(row.get("is_flight_completed"))

    lines = [
        f"data_version={DATA_VERSION}",
        f"target_data_version={TARGET_DATA_VERSION}",
        f"max_backup_target_date={max_target_date or 'not_set'}",
        "",
        f"base_target_rows={len(base_target)}",
        f"augmented_target_rows={len(augmented_target)}",
        f"new_target_rows={len(new_rows)}",
        f"new_target_date_min={min((clean(row.get('date')) for row in new_rows), default='')}",
        f"new_target_date_max={max((clean(row.get('date')) for row in new_rows), default='')}",
        f"new_target_status_counts={status_counts}",
        "",
        f"historical_dataset_rows={len(historical_dataset)}",
        f"historical_dataset_date_min={min(clean(row.get('date')) for row in historical_dataset)}",
        f"historical_dataset_date_max={max(clean(row.get('date')) for row in historical_dataset)}",
    ]
    for split in ("train", "valid", "test"):
        rows = split_counts.get(split, 0)
        completed = split_completed.get(split, 0)
        rate = round(completed / rows, 4) if rows else 0
        lines.append(f"{split}_rows={rows}")
        lines.append(f"{split}_completed_rate={rate}")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    backup_dir = Path(args.backup_dir)
    max_target_date = args.max_target_date or infer_backup_max_target_date(backup_dir)

    base_target, _ = read_csv(Path(args.base_target))
    board_outcomes, _ = read_csv(backup_dir / "forecast_monitor" / "board_outcomes.csv")

    augmented_target = build_augmented_target(base_target, board_outcomes, max_target_date)
    historical_dataset = make_historical_dataset(augmented_target)

    target_output = Path(args.target_output)
    dataset_output = Path(args.dataset_output)
    summary_output = Path(args.summary_output)
    features_output = Path(args.features_output)

    write_csv(target_output, augmented_target, TARGET_COLUMNS)
    write_csv(dataset_output, historical_dataset, MODEL_COLUMNS)
    write_feature_sets(features_output)
    write_summary(summary_output, base_target, augmented_target, historical_dataset, max_target_date)

    print(f"Max backup target date: {max_target_date}")
    print(f"Target dataset: {target_output} ({len(augmented_target)} rows)")
    print(f"Historical-only dataset: {dataset_output} ({len(historical_dataset)} rows)")
    print(f"Summary: {summary_output}")
    print(f"Feature sets: {features_output}")


if __name__ == "__main__":
    main()
