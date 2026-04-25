import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from datetime import timedelta

import pandas as pd


CANCEL_PATTERNS = [
    r"\bотмен\w*",
    r"\bрейс\w*\s+не\s+состо\w*",
    r"\bвылет\w*\s+не\s+состо\w*",
    r"\bне\s+полет\w*",
    r"\bне\s+выполня\w*",
]

COMPLETED_PATTERNS = [
    r"\bвылетел\w*",
    r"\bвылетели\b",
    r"\bприбыл\w*",
    r"\bприлетел\w*",
    r"\bсел\b",
    r"\bсели\b",
    r"\bпосадк\w+",
    r"\bрейс\w*\s+выполн\w*",
    r"\bвыполн\w*",
    r"\bуш[её]л\b",
    r"\bотправил\w*",
]

DELAY_PATTERNS = [
    r"\bзадерж\w*",
    r"\bперенос\w*",
    r"\bперенес[её]н\w*",
    r"\bожидается\b",
]

PLANNED_PATTERNS = [
    r"\bпланирует\b",
    r"\bпланируется\b",
    r"\bпо расписанию\b",
    r"\bготовится\b",
    r"\bготовят\b",
    r"\bначали регистрацию\b",
    r"\bначинаем регистрацию\b",
]

FOG_PATTERNS = [
    r"\bтуман\b",
    r"\bвидимость\b",
    r"\bнизкая облачность\b",
    r"\bоблачность\b",
]

WIND_STORM_PATTERNS = [
    r"\bветер\b",
    r"\bпорыв",
    r"\bшторм",
    r"\bциклон",
    r"\bметель\b",
    r"\bснег\b",
    r"\bпурга\b",
]

TECHNICAL_PATTERNS = [
    r"\bтехничес",
    r"\bнеисправ",
    r"\bполомк",
]

SCHEDULE_PATTERNS = [
    r"\bрасписан",
    r"\bпо расписанию\b",
    r"\bизменени[ея] в расписании\b",
]


@dataclass
class MessageLabel:
    message_id: int
    message_date: str
    event_date: str
    event_date_source: str
    flight_status: str
    reason_text: str
    reason_class: str
    label_confidence: str
    source_url: str
    text: str
    transport_type: str

MONTHS_RU = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


def infer_year_for_event_date(day: int, month: int, message_date: pd.Timestamp) -> int:
    """
    Обычно дата события рядом с датой публикации.
    Если месяц события сильно меньше месяца публикации в декабре/январе,
    можно аккуратно перекидывать год, но для MVP достаточно года сообщения.
    """
    return message_date.year


def extract_event_date_from_text(text: str, message_date: str) -> tuple[str, str]:
    """
    Returns:
        event_date, event_date_source
    """
    msg_dt = pd.to_datetime(message_date)

    normalized = " ".join(str(text).lower().split())

    if re.search(r"\bсегодня\b", normalized):
        return msg_dt.date().isoformat(), "text_today"

    if re.search(r"\bзавтра\b", normalized):
        return (msg_dt + timedelta(days=1)).date().isoformat(), "text_tomorrow"

    # Например: "за 30.07", "за 30.07.2024", "на 13.12.20"
    match = re.search(
        r"\b(?:за|на|от|по состоянию на)\s+(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?",
        normalized,
    )
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year_raw = match.group(3)

        if year_raw:
            year = int(year_raw)
            if year < 100:
                year += 2000
        else:
            year = infer_year_for_event_date(day, month, msg_dt)

        try:
            return pd.Timestamp(year=year, month=month, day=day).date().isoformat(), "text_numeric_date"
        except ValueError:
            pass

    # Например: "15 апреля", "рейс на 12 декабря"
    match = re.search(
        r"\b(\d{1,2})\s+("
        + "|".join(MONTHS_RU.keys())
        + r")\b",
        normalized,
    )
    if match:
        day = int(match.group(1))
        month = MONTHS_RU[match.group(2)]
        year = infer_year_for_event_date(day, month, msg_dt)

        try:
            return pd.Timestamp(year=year, month=month, day=day).date().isoformat(), "text_ru_month_date"
        except ValueError:
            pass

    return msg_dt.date().isoformat(), "message_date"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build daily flight labels from raw Telegram messages."
    )

    parser.add_argument(
        "--input",
        default="data/raw/telegram/aeroportuk_messages.csv",
        help="Raw Telegram messages CSV.",
    )
    parser.add_argument(
        "--output",
        default="data/interim/labels/daily_flight_labels.csv",
        help="Output daily labels CSV.",
    )
    parser.add_argument(
        "--message-labels-output",
        default="data/interim/labels/message_flight_labels.csv",
        help="Output message-level labels CSV for inspection.",
    )

    return parser.parse_args()

def classify_transport_type(text: str) -> str:
    normalized = text.lower()

    if re.search(r"\bвертол[её]т\w*|\bавиашельф\b", normalized):
        return "helicopter"

    if re.search(r"\bсамол[её]т\w*|\bаврора\b|\b4621\b|\b4622\b|\b4623\b|\b4624\b|\b4625\b|\b4626\b|\bрейс\w*", normalized):
        return "airplane"

    return "unknown"

def contains_any(text: str, patterns: list[str]) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def classify_status(text: str) -> tuple[str, str]:
    """
    Returns:
        flight_status, confidence
    """
    normalized = text.lower()

    is_cancelled = contains_any(normalized, CANCEL_PATTERNS)
    is_completed = contains_any(normalized, COMPLETED_PATTERNS)
    is_delayed = contains_any(normalized, DELAY_PATTERNS)
    is_planned = contains_any(normalized, PLANNED_PATTERNS)

    if is_cancelled and not is_completed:
        return "cancelled", "high"

    if is_completed and not is_cancelled:
        return "completed", "high"

    if is_delayed and not is_cancelled and not is_completed:
        return "delayed", "medium"

    if is_planned and not is_cancelled and not is_completed and not is_delayed:
        return "planned", "medium"

    if is_cancelled and is_completed:
        return "unknown", "low"

    return "unknown", "low"


def classify_reason(text: str, event_date: str) -> tuple[str, str]:
    normalized = text.lower()

    if contains_any(normalized, FOG_PATTERNS):
        return "fog", extract_reason_text(text)

    if contains_any(normalized, WIND_STORM_PATTERNS):
        return "wind_storm", extract_reason_text(text)

    if contains_any(normalized, TECHNICAL_PATTERNS):
        return "technical", extract_reason_text(text)

    if contains_any(normalized, SCHEDULE_PATTERNS):
        return "schedule", extract_reason_text(text)

    # MVP-эвристика для неизвестной погодной причины.
    # Используем только для отмен, позже можно будет применять аккуратнее.
    month = pd.to_datetime(event_date).month

    if 4 <= month <= 10:
        return "fog_likely", ""

    return "wind_storm_likely", ""


def extract_reason_text(text: str, max_len: int = 300) -> str:
    cleaned = " ".join(str(text).split())
    return cleaned[:max_len]


def get_message_date(date_utc: str) -> str:
    return pd.to_datetime(date_utc).date().isoformat()


def infer_event_date(row: pd.Series) -> tuple[str, str]:
    return extract_event_date_from_text(
        text=str(row.get("text", "")),
        message_date=row["date_utc"],
    )


def label_message(row: pd.Series) -> MessageLabel:
    text = str(row.get("text", ""))

    event_date, event_date_source = infer_event_date(row)
    flight_status, confidence = classify_status(text)
    transport_type = classify_transport_type(text)

    reason_class = "unknown"
    reason_text = ""

    if flight_status == "cancelled":
        reason_class, reason_text = classify_reason(text, event_date)

    return MessageLabel(
        message_id=int(row["message_id"]),
        message_date=get_message_date(row["date_utc"]),
        event_date=event_date,
        event_date_source=event_date_source,
        flight_status=flight_status,
        reason_text=reason_text,
        reason_class=reason_class,
        label_confidence=confidence,
        source_url=str(row.get("url", "")),
        text=text,
        transport_type=transport_type,
    )


def choose_daily_status(statuses: list[str]) -> str:
    """
    Правило агрегации на день.

    Для MVP:
    - completed важнее всего, потому что это факт выполнения;
    - cancelled — факт отмены, если выполнения нет;
    - delayed — промежуточный статус;
    - planned — план/регистрация/расписание, не target для ML;
    - unknown — нет полезного статуса.
    """
    unique = set(statuses)

    if "completed" in unique:
        return "completed"
    if "cancelled" in unique:
        return "cancelled"
    if "delayed" in unique:
        return "delayed"
    if "planned" in unique:
        return "planned"

    return "unknown"


def choose_daily_confidence(confidences: list[str], status: str) -> str:
    if status == "unknown":
        return "low"

    if "high" in confidences:
        return "high"

    if "medium" in confidences:
        return "medium"

    return "low"


def aggregate_daily_labels(message_labels: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for event_date, group in message_labels.groupby("event_date"):
        statuses = group["flight_status"].tolist()
        confidences = group["label_confidence"].tolist()

        daily_status = choose_daily_status(statuses)
        daily_confidence = choose_daily_confidence(confidences, daily_status)

        non_unknown_reasons = group[
            group["reason_class"].notna()
            & (group["reason_class"] != "unknown")
            & (group["reason_class"] != "")
        ]

        if not non_unknown_reasons.empty:
            reason_class = non_unknown_reasons["reason_class"].iloc[0]
            reason_text = non_unknown_reasons["reason_text"].iloc[0]
        else:
            reason_class = "unknown"
            reason_text = ""

        source_message_ids = ";".join(group["message_id"].astype(str).tolist())
        source_message_urls = ";".join(group["source_url"].dropna().astype(str).tolist())

        raw_text_sample = " | ".join(
            group["text"]
            .dropna()
            .astype(str)
            .map(lambda x: " ".join(x.split())[:300])
            .head(3)
            .tolist()
        )

        rows.append(
            {
                "date": event_date,
                "flight_status": daily_status,
                "reason_text": reason_text,
                "reason_class": reason_class,
                "label_confidence": daily_confidence,
                "message_count": len(group),
                "event_date_sources": ";".join(
                    sorted(group["event_date_source"].dropna().astype(str).unique())
                ),
                "transport_types": ";".join(
                    sorted(group["transport_type"].dropna().astype(str).unique())
                ),
                "source_message_ids": source_message_ids,
                "source_message_urls": source_message_urls,
                "raw_text_sample": raw_text_sample,
            }
        )

    result = pd.DataFrame(rows)

    if not result.empty:
        result = result.sort_values("date").reset_index(drop=True)

    return result


def main() -> None:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    message_labels_output_path = Path(args.message_labels_output)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    message_labels_output_path.parent.mkdir(parents=True, exist_ok=True)

    raw = pd.read_csv(input_path)

    required_columns = {"message_id", "date_utc", "text"}
    missing_columns = required_columns - set(raw.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    labels = [label_message(row) for _, row in raw.iterrows()]
    message_labels = pd.DataFrame([label.__dict__ for label in labels])

    message_labels_for_daily = message_labels[
        message_labels["transport_type"].isin(["airplane", "unknown"])
    ].copy()

    daily_labels = aggregate_daily_labels(message_labels_for_daily)

    message_labels.to_csv(
        message_labels_output_path,
        index=False,
        encoding="utf-8-sig",
    )
    daily_labels.to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig",
    )

    print(f"Message-level labels saved: {message_labels_output_path}")
    print(f"Daily labels saved: {output_path}")
    print()
    print("Daily status distribution:")
    print(daily_labels["flight_status"].value_counts(dropna=False))
    print()
    print("Reason class distribution:")
    print(daily_labels["reason_class"].value_counts(dropna=False))


if __name__ == "__main__":
    main()