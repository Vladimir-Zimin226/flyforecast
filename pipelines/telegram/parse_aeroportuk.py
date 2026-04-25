import argparse
import asyncio
import csv
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError


CHANNEL_USERNAME = "aeroportuk"


@dataclass
class TelegramMessageRow:
    source: str
    channel_username: str
    message_id: int
    date_utc: str
    text: str
    views: Optional[int]
    forwards: Optional[int]
    reply_to_msg_id: Optional[int]
    url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse messages from Telegram channel aeroportuk for flyforecast."
    )

    parser.add_argument(
        "--out-csv",
        default="data/raw/telegram/aeroportuk_messages.csv",
        help="Output CSV path.",
    )
    parser.add_argument(
        "--out-jsonl",
        default="data/raw/telegram/aeroportuk_messages.jsonl",
        help="Output JSONL path.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of messages to fetch. Default: all available messages.",
    )
    parser.add_argument(
        "--min-id",
        type=int,
        default=0,
        help="Fetch messages with message_id greater than this value.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing files and deduplicate by message_id.",
    )

    return parser.parse_args()


def get_required_env() -> tuple[int, str, str]:
    load_dotenv()

    api_id_raw = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_name = os.getenv("TELEGRAM_SESSION_NAME", "flyforecast_telegram")

    if not api_id_raw:
        raise RuntimeError("TELEGRAM_API_ID is missing in .env")
    if not api_hash:
        raise RuntimeError("TELEGRAM_API_HASH is missing in .env")

    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise RuntimeError("TELEGRAM_API_ID must be an integer") from exc

    return api_id, api_hash, session_name


def normalize_datetime_to_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def message_to_row(message) -> Optional[TelegramMessageRow]:
    text = message.message or ""

    # Для нашего проекта пустые сервисные сообщения обычно не нужны.
    if not text.strip():
        return None

    reply_to_msg_id = None
    if message.reply_to is not None:
        reply_to_msg_id = getattr(message.reply_to, "reply_to_msg_id", None)

    return TelegramMessageRow(
        source="telegram",
        channel_username=CHANNEL_USERNAME,
        message_id=message.id,
        date_utc=normalize_datetime_to_utc(message.date),
        text=text,
        views=getattr(message, "views", None),
        forwards=getattr(message, "forwards", None),
        reply_to_msg_id=reply_to_msg_id,
        url=f"https://t.me/{CHANNEL_USERNAME}/{message.id}",
    )


def read_existing_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    return pd.read_csv(path)


def write_outputs(
    rows: list[TelegramMessageRow],
    out_csv: Path,
    out_jsonl: Path,
    append: bool,
) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)

    new_df = pd.DataFrame([asdict(row) for row in rows])

    if append and out_csv.exists():
        old_df = read_existing_csv(out_csv)
        df = pd.concat([old_df, new_df], ignore_index=True)
        df = df.drop_duplicates(subset=["message_id"], keep="last")
    else:
        df = new_df

    if not df.empty:
        df = df.sort_values("message_id").reset_index(drop=True)

    df.to_csv(
        out_csv,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL,
    )

    with out_jsonl.open("w", encoding="utf-8") as f:
        for record in df.to_dict(orient="records"):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


async def fetch_messages(limit: Optional[int], min_id: int) -> list[TelegramMessageRow]:
    api_id, api_hash, session_name = get_required_env()

    rows: list[TelegramMessageRow] = []

    async with TelegramClient(session_name, api_id, api_hash) as client:
        entity = await client.get_entity(CHANNEL_USERNAME)

        try:
            async for message in client.iter_messages(
                entity,
                limit=limit,
                min_id=min_id,
                reverse=True,
                wait_time=1,
            ):
                row = message_to_row(message)
                if row is not None:
                    rows.append(row)

                if len(rows) % 500 == 0 and rows:
                    print(f"Fetched {len(rows)} non-empty messages...")

        except FloodWaitError as exc:
            print(f"Telegram asked to wait {exc.seconds} seconds.")
            raise

    return rows


async def main() -> None:
    args = parse_args()

    out_csv = Path(args.out_csv)
    out_jsonl = Path(args.out_jsonl)

    rows = await fetch_messages(limit=args.limit, min_id=args.min_id)

    write_outputs(
        rows=rows,
        out_csv=out_csv,
        out_jsonl=out_jsonl,
        append=args.append,
    )

    print(f"Done. Saved {len(rows)} fetched messages.")
    print(f"CSV: {out_csv}")
    print(f"JSONL: {out_jsonl}")


if __name__ == "__main__":
    asyncio.run(main())