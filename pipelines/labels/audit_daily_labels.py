from pathlib import Path

import pandas as pd


INPUT_PATH = Path("data/interim/labels/daily_flight_labels.csv")
OUTPUT_DIR = Path("data/interim/labels/audit")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_PATH)

    checks = {
        "cancelled_sample.csv": df[df["flight_status"] == "cancelled"].sample(
            min(100, (df["flight_status"] == "cancelled").sum()),
            random_state=42,
        ),
        "completed_sample.csv": df[df["flight_status"] == "completed"].sample(
            min(100, (df["flight_status"] == "completed").sum()),
            random_state=42,
        ),
        "planned_sample.csv": df[df["flight_status"] == "planned"].sample(
            min(100, (df["flight_status"] == "planned").sum()),
            random_state=42,
        ),
        "delayed_sample.csv": df[df["flight_status"] == "delayed"].sample(
            min(100, (df["flight_status"] == "delayed").sum()),
            random_state=42,
        ),
        "unknown_sample.csv": df[df["flight_status"] == "unknown"].sample(
            min(150, (df["flight_status"] == "unknown").sum()),
            random_state=42,
        ),
        "text_date_sample.csv": df[
            df["event_date_sources"].fillna("").str.contains("text_", regex=False)
        ].sample(
            min(
                150,
                df["event_date_sources"].fillna("").str.contains("text_", regex=False).sum(),
            ),
            random_state=42,
        ),
        "multi_message_days.csv": df[df["message_count"] >= 5].sort_values(
            ["message_count", "date"],
            ascending=[False, True],
        ),
    }

    for filename, sample in checks.items():
        path = OUTPUT_DIR / filename
        sample.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"Saved {len(sample):4d} rows: {path}")

    print()
    print("Status distribution:")
    print(df["flight_status"].value_counts(dropna=False))

    print()
    print("Reason distribution:")
    print(df["reason_class"].value_counts(dropna=False))


if __name__ == "__main__":
    main()