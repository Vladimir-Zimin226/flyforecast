import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


DATA_VERSION = "telegram-v2-plus-historical-manual-v3-2026-05-20"

HISTORICAL_TO_DAILY_STATUS = {
    "departed": "completed",
    "arrived": "completed",
    "in_flight": "completed",
    "cancelled": "cancelled",
    "delayed": "delayed",
    "combined": "disrupted",
    "scheduled": "planned_only",
    "check_in": "planned_only",
    "unknown": "needs_review",
}

BINARY_STATUSES = {"completed", "cancelled"}
NON_BINARY_STATUSES = {"delayed", "disrupted", "planned_only", "needs_review"}
MANUAL_REVIEW_STATUSES = BINARY_STATUSES | NON_BINARY_STATUSES | {"exclude", "unknown", "unsure"}
MANUAL_EXCLUDE_STATUSES = NON_BINARY_STATUSES | {"exclude", "unknown", "unsure"}


@dataclass(frozen=True)
class Paths:
    historical_sources: Path
    telegram_daily_labels: Path
    current_processed_dataset: Path
    historical_daily_labels: Path
    needs_manual_review: Path
    manual_review: Path
    manual_overrides: Path
    manual_review_applied: Path
    manual_review_excluded: Path
    dataset_v3: Path
    summary: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate historical flight status evidence and build dataset_daily_flights_v3 candidate."
    )
    parser.add_argument(
        "--historical-sources",
        default="data/raw/flight_status/kunashir_historical_sources_v3.csv",
    )
    parser.add_argument(
        "--telegram-daily-labels",
        default="data/interim/labels/daily_flight_labels.csv",
    )
    parser.add_argument(
        "--current-processed-dataset",
        default="data/processed/dataset_daily_flights.csv",
    )
    parser.add_argument(
        "--historical-daily-labels",
        default="data/interim/flight_status/historical_daily_labels_v3.csv",
    )
    parser.add_argument(
        "--needs-manual-review",
        default="data/interim/flight_status/needs_manual_review_v3.csv",
    )
    parser.add_argument(
        "--manual-review",
        default="data/interim/flight_status/manual_review_v3.xlsx",
    )
    parser.add_argument(
        "--manual-overrides",
        default="data/interim/flight_status/manual_overrides_v3.csv",
    )
    parser.add_argument(
        "--manual-review-applied",
        default="data/interim/flight_status/manual_review_applied_v3.csv",
    )
    parser.add_argument(
        "--manual-review-excluded",
        default="data/interim/flight_status/manual_review_excluded_v3.csv",
    )
    parser.add_argument(
        "--dataset-v3",
        default="data/processed/dataset_daily_flights_v3.csv",
    )
    parser.add_argument(
        "--summary",
        default="data/interim/flight_status/dataset_v3_summary.txt",
    )
    return parser.parse_args()


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).split())


def season_for_month(month: int) -> str:
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    return "autumn"


def join_unique(values: pd.Series, limit: int | None = None) -> str:
    result: list[str] = []
    seen: set[str] = set()

    for value in values.dropna().astype(str):
        for part in value.split(";"):
            clean = normalize_text(part)
            if not clean or clean in seen:
                continue
            seen.add(clean)
            result.append(clean)
            if limit is not None and len(result) >= limit:
                return ";".join(result)

    return ";".join(result)


def choose_historical_daily_status(statuses: set[str]) -> tuple[str, bool]:
    has_completed = "completed" in statuses
    has_cancelled = "cancelled" in statuses
    has_delayed = "delayed" in statuses
    has_disrupted = "disrupted" in statuses
    has_planned = "planned_only" in statuses

    if has_completed and has_cancelled:
        return "completed", True
    if has_completed:
        return "completed", False
    if has_cancelled:
        return "cancelled", False
    if has_disrupted:
        return "disrupted", False
    if has_delayed:
        return "delayed", False
    if has_planned:
        return "planned_only", False
    return "needs_review", False


def historical_confidence(group: pd.DataFrame, daily_status: str, mixed_binary: bool) -> str:
    if daily_status == "needs_review" or mixed_binary:
        return "needs_review"

    source_types = set(group["source_type"].dropna().astype(str))
    source_confidences = set(group["confidence"].dropna().astype(str))

    if daily_status in BINARY_STATUSES and ("airportus_news" in source_types or "wayback_board" in source_types):
        return "high"
    if "high" in source_confidences:
        return "high"
    if "medium" in source_confidences:
        return "medium"
    return "low"


def build_historical_daily_labels(historical: pd.DataFrame) -> pd.DataFrame:
    historical = historical.copy()
    historical["daily_evidence_status"] = (
        historical["status_normalized"]
        .fillna("unknown")
        .map(HISTORICAL_TO_DAILY_STATUS)
        .fillna("needs_review")
    )

    historical = historical[historical["flight_date"].fillna("").astype(str).str.len() > 0].copy()
    rows: list[dict] = []

    for flight_date, group in historical.groupby("flight_date", sort=True):
        statuses = set(group["daily_evidence_status"].dropna().astype(str))
        daily_status, mixed_binary = choose_historical_daily_status(statuses)
        confidence = historical_confidence(group, daily_status, mixed_binary)
        binary_status = daily_status if daily_status in BINARY_STATUSES else ""

        reason_classes = join_unique(group["reason_class"])
        if not reason_classes:
            reason_classes = "unknown"

        rows.append(
            {
                "date": flight_date,
                "historical_status": daily_status,
                "binary_status": binary_status,
                "label_confidence": confidence,
                "mixed_binary_evidence": int(mixed_binary),
                "evidence_count": len(group),
                "source_types": join_unique(group["source_type"]),
                "source_urls": join_unique(group["source_url"]),
                "flight_numbers": join_unique(group["flight_numbers"]),
                "directions": join_unique(group["direction"]),
                "reason_class": reason_classes,
                "evidence_statuses": ";".join(sorted(statuses)),
                "raw_evidence_sample": " | ".join(
                    group["raw_text"]
                    .dropna()
                    .astype(str)
                    .map(lambda value: normalize_text(value)[:300])
                    .head(5)
                    .tolist()
                ),
            }
        )

    return pd.DataFrame(rows)


def review_reason(
    telegram_status: str,
    historical_status: str,
    mixed_binary: int,
    binary_status: str,
) -> str:
    if mixed_binary:
        return "historical_mixed_completed_cancelled"
    if telegram_status in BINARY_STATUSES and binary_status in BINARY_STATUSES and telegram_status != binary_status:
        return "telegram_historical_binary_conflict"
    if historical_status in {"delayed", "disrupted"} and telegram_status in BINARY_STATUSES:
        return "historical_disruption_with_telegram_binary_outcome"
    if historical_status in {"delayed", "disrupted"} and not telegram_status:
        return "historical_disruption_without_final_outcome"
    if historical_status == "needs_review":
        return "historical_unknown"
    return ""


def build_needs_manual_review(
    historical_daily: pd.DataFrame,
    telegram_daily: pd.DataFrame,
) -> pd.DataFrame:
    telegram = telegram_daily.rename(
        columns={
            "flight_status": "telegram_status",
            "label_confidence": "telegram_confidence",
            "reason_class": "telegram_reason_class",
            "raw_text_sample": "telegram_raw_text_sample",
            "source_message_urls": "telegram_source_urls",
        }
    )
    keep_columns = [
        "date",
        "telegram_status",
        "telegram_confidence",
        "telegram_reason_class",
        "telegram_source_urls",
        "telegram_raw_text_sample",
    ]
    telegram = telegram[[column for column in keep_columns if column in telegram.columns]]

    merged = historical_daily.merge(telegram, on="date", how="left")
    for column in ["telegram_status", "telegram_confidence", "telegram_reason_class"]:
        if column not in merged.columns:
            merged[column] = ""
        merged[column] = merged[column].fillna("")

    merged["review_reason"] = merged.apply(
        lambda row: review_reason(
            telegram_status=row["telegram_status"],
            historical_status=row["historical_status"],
            mixed_binary=int(row["mixed_binary_evidence"]),
            binary_status=row["binary_status"],
        ),
        axis=1,
    )

    review = merged[merged["review_reason"] != ""].copy()
    review = review.sort_values(["review_reason", "date"]).reset_index(drop=True)

    preferred = [
        "date",
        "review_reason",
        "telegram_status",
        "historical_status",
        "binary_status",
        "mixed_binary_evidence",
        "label_confidence",
        "evidence_count",
        "source_types",
        "flight_numbers",
        "directions",
        "reason_class",
        "evidence_statuses",
        "source_urls",
        "raw_evidence_sample",
        "telegram_source_urls",
        "telegram_raw_text_sample",
    ]
    return review[[column for column in preferred if column in review.columns]]


def make_processed_row(date_value: str, status: str, historical_row: pd.Series) -> dict:
    ts = pd.Timestamp(date_value)
    month_decade = (ts.day - 1) // 10 + 1
    return {
        "date": ts.date().isoformat(),
        "status": status,
        "is_flight_completed": 1 if status == "completed" else 0,
        "label_confidence": historical_row["label_confidence"],
        "reason_class": historical_row["reason_class"] or "unknown",
        "message_count": int(historical_row["evidence_count"]),
        "transport_types": "airplane",
        "event_date_sources": "historical_sources_v2",
        "year": ts.year,
        "month": ts.month,
        "day": ts.day,
        "day_of_year": ts.dayofyear,
        "month_decade": month_decade,
        "season": season_for_month(ts.month),
        "data_version": DATA_VERSION,
    }


def make_manual_processed_row(manual_row: pd.Series) -> dict:
    ts = pd.Timestamp(manual_row["date"])
    status = normalize_text(manual_row["manual_status"]).lower()
    confidence = normalize_text(manual_row.get("manual_confidence", "")) or "medium"
    reason_class = normalize_text(manual_row.get("manual_reason_class", "")) or "unknown"
    source = normalize_text(manual_row.get("manual_source", "")) or "manual_review_v3"
    raw_evidence_count = manual_row.get("manual_evidence_count", 1)
    evidence_count = 1 if pd.isna(raw_evidence_count) or raw_evidence_count == "" else int(raw_evidence_count)
    month_decade = (ts.day - 1) // 10 + 1

    return {
        "date": ts.date().isoformat(),
        "status": status,
        "is_flight_completed": 1 if status == "completed" else 0,
        "label_confidence": confidence,
        "reason_class": reason_class,
        "message_count": evidence_count,
        "transport_types": "airplane",
        "event_date_sources": source,
        "year": ts.year,
        "month": ts.month,
        "day": ts.day,
        "day_of_year": ts.dayofyear,
        "month_decade": month_decade,
        "season": season_for_month(ts.month),
        "data_version": DATA_VERSION,
    }


def read_manual_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
        return pd.read_excel(path)
    return pd.read_csv(path)


def load_manual_review(path: Path, source_name: str) -> pd.DataFrame:
    df = read_manual_table(path)
    if df.empty:
        return pd.DataFrame()

    required_columns = {"date", "manual_status"}
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(f"{path} is missing required columns: {', '.join(missing)}")

    df = df.copy().fillna("")
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    df["manual_status"] = df["manual_status"].astype(str).str.strip().str.lower()
    invalid_statuses = sorted(set(df["manual_status"]) - MANUAL_REVIEW_STATUSES - {""})
    if invalid_statuses:
        raise ValueError(f"{path} has unsupported manual_status values: {invalid_statuses}")

    if "manual_source" not in df.columns:
        df["manual_source"] = source_name
    else:
        df["manual_source"] = df["manual_source"].replace("", source_name)

    return df


def load_manual_reviews(manual_review_path: Path, overrides_path: Path) -> pd.DataFrame:
    frames = [
        load_manual_review(manual_review_path, "manual_review_v3"),
        load_manual_review(overrides_path, "manual_override_v3"),
    ]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()

    manual = pd.concat(frames, ignore_index=True)
    manual = manual[manual["manual_status"] != ""].copy()
    manual = manual.drop_duplicates(subset=["date"], keep="last")
    return manual.sort_values("date").reset_index(drop=True)


def apply_manual_review(dataset: pd.DataFrame, manual_review: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if manual_review.empty:
        return dataset, pd.DataFrame(), pd.DataFrame()

    manual = manual_review.copy()
    applied = manual[manual["manual_status"].isin(BINARY_STATUSES)].copy()
    excluded = manual[manual["manual_status"].isin(MANUAL_EXCLUDE_STATUSES)].copy()

    if applied.empty:
        return dataset, applied, excluded

    result = dataset.copy()
    result["date"] = result["date"].astype(str)
    applied_dates = set(applied["date"].astype(str))
    result = result[~result["date"].isin(applied_dates)].copy()
    manual_rows = pd.DataFrame([make_manual_processed_row(row) for _, row in applied.iterrows()])
    result = pd.concat([result, manual_rows], ignore_index=True)
    result = result.sort_values("date").reset_index(drop=True)
    return result, applied.sort_values("date").reset_index(drop=True), excluded.sort_values("date").reset_index(drop=True)


def build_dataset_v3(
    current_processed: pd.DataFrame,
    historical_daily: pd.DataFrame,
    needs_review: pd.DataFrame,
) -> pd.DataFrame:
    current = current_processed.copy()
    current["date"] = current["date"].astype(str)

    if "data_version" in current.columns:
        current["data_version"] = DATA_VERSION

    review_dates = set(needs_review["date"].astype(str)) if not needs_review.empty else set()
    current_dates = set(current["date"])

    append_rows: list[dict] = []
    safe_historical = historical_daily[
        historical_daily["binary_status"].isin(["completed", "cancelled"])
        & (historical_daily["mixed_binary_evidence"] == 0)
        & (~historical_daily["date"].astype(str).isin(review_dates))
        & (~historical_daily["date"].astype(str).isin(current_dates))
    ].copy()

    for _, row in safe_historical.iterrows():
        append_rows.append(make_processed_row(row["date"], row["binary_status"], row))

    if append_rows:
        current = pd.concat([current, pd.DataFrame(append_rows)], ignore_index=True)

    current = current.sort_values("date").reset_index(drop=True)
    return current


def write_summary(
    path: Path,
    historical_daily: pd.DataFrame,
    needs_review: pd.DataFrame,
    manual_applied: pd.DataFrame,
    manual_excluded: pd.DataFrame,
    dataset_v3: pd.DataFrame,
    current_processed: pd.DataFrame,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    appended_count = len(dataset_v3) - len(current_processed)
    status_counts = dataset_v3["status"].value_counts(dropna=False).to_dict()
    historical_counts = historical_daily["historical_status"].value_counts(dropna=False).to_dict()
    review_counts = (
        needs_review["review_reason"].value_counts(dropna=False).to_dict()
        if not needs_review.empty
        else {}
    )
    manual_counts = (
        manual_applied["manual_status"].value_counts(dropna=False).to_dict()
        if not manual_applied.empty
        else {}
    )
    manual_excluded_counts = (
        manual_excluded["manual_status"].value_counts(dropna=False).to_dict()
        if not manual_excluded.empty
        else {}
    )

    lines = [
        f"generated_at={datetime.now().isoformat(timespec='seconds')}",
        f"data_version={DATA_VERSION}",
        "",
        f"historical_daily_rows={len(historical_daily)}",
        f"historical_daily_status_counts={historical_counts}",
        "",
        f"needs_manual_review_rows={len(needs_review)}",
        f"needs_manual_review_reason_counts={review_counts}",
        "",
        f"manual_review_applied_rows={len(manual_applied)}",
        f"manual_review_applied_status_counts={manual_counts}",
        f"manual_review_excluded_rows={len(manual_excluded)}",
        f"manual_review_excluded_status_counts={manual_excluded_counts}",
        "",
        f"current_processed_rows={len(current_processed)}",
        f"dataset_v3_rows={len(dataset_v3)}",
        f"dataset_v3_appended_rows={appended_count}",
        f"dataset_v3_status_counts={status_counts}",
        "",
        "backend_switch_status=not_applied",
        "note=dataset_daily_flights_v3.csv includes manual binary review rows and is still a switch candidate.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    paths = Paths(
        historical_sources=Path(args.historical_sources),
        telegram_daily_labels=Path(args.telegram_daily_labels),
        current_processed_dataset=Path(args.current_processed_dataset),
        historical_daily_labels=Path(args.historical_daily_labels),
        needs_manual_review=Path(args.needs_manual_review),
        manual_review=Path(args.manual_review),
        manual_overrides=Path(args.manual_overrides),
        manual_review_applied=Path(args.manual_review_applied),
        manual_review_excluded=Path(args.manual_review_excluded),
        dataset_v3=Path(args.dataset_v3),
        summary=Path(args.summary),
    )

    historical = pd.read_csv(paths.historical_sources)
    telegram_daily = pd.read_csv(paths.telegram_daily_labels)
    current_processed = pd.read_csv(paths.current_processed_dataset)

    historical_daily = build_historical_daily_labels(historical)
    needs_review = build_needs_manual_review(historical_daily, telegram_daily)
    manual_review = load_manual_reviews(paths.manual_review, paths.manual_overrides)
    dataset_v3 = build_dataset_v3(current_processed, historical_daily, needs_review)
    dataset_v3, manual_applied, manual_excluded = apply_manual_review(dataset_v3, manual_review)

    paths.historical_daily_labels.parent.mkdir(parents=True, exist_ok=True)
    historical_daily.to_csv(
        paths.historical_daily_labels,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
    )
    needs_review.to_csv(
        paths.needs_manual_review,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
    )
    paths.manual_review_applied.parent.mkdir(parents=True, exist_ok=True)
    manual_applied.to_csv(
        paths.manual_review_applied,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
    )
    manual_excluded.to_csv(
        paths.manual_review_excluded,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
    )
    paths.dataset_v3.parent.mkdir(parents=True, exist_ok=True)
    dataset_v3.to_csv(
        paths.dataset_v3,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
    )
    write_summary(paths.summary, historical_daily, needs_review, manual_applied, manual_excluded, dataset_v3, current_processed)

    print(f"Historical daily labels: {paths.historical_daily_labels} ({len(historical_daily)} rows)")
    print(f"Needs manual review:     {paths.needs_manual_review} ({len(needs_review)} rows)")
    print(f"Manual review applied:   {paths.manual_review_applied} ({len(manual_applied)} rows)")
    print(f"Manual review excluded:  {paths.manual_review_excluded} ({len(manual_excluded)} rows)")
    print(f"Dataset v3 candidate:    {paths.dataset_v3} ({len(dataset_v3)} rows)")
    print(f"Summary:                 {paths.summary}")
    print()
    print("Historical daily status distribution:")
    print(historical_daily["historical_status"].value_counts(dropna=False))
    print()
    print("Manual review reasons:")
    print(needs_review["review_reason"].value_counts(dropna=False) if not needs_review.empty else "none")
    print()
    print("Applied manual review status distribution:")
    print(manual_applied["manual_status"].value_counts(dropna=False) if not manual_applied.empty else "none")
    print()
    print("Excluded manual review status distribution:")
    print(manual_excluded["manual_status"].value_counts(dropna=False) if not manual_excluded.empty else "none")
    print()
    print("Dataset v3 status distribution:")
    print(dataset_v3["status"].value_counts(dropna=False))


if __name__ == "__main__":
    main()
