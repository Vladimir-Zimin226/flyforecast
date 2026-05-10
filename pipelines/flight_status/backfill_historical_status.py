import argparse
import asyncio
import csv
import html
import re
import sys
from dataclasses import dataclass, fields
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx


if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from pipelines.flight_status.collect_kunashir_status import (  # noqa: E402
    extract_reason,
    normalize_status,
    parse_board_datetime,
    parse_board_html,
)


DEFAULT_OUTPUT = "data/raw/flight_status/kunashir_historical_sources.csv"
DEFAULT_ERRORS_OUTPUT = "data/raw/flight_status/historical_backfill_errors.csv"
DEFAULT_TIMEZONE = "Asia/Sakhalin"
WAYBACK_CDX_URL = "https://web.archive.org/cdx"
WAYBACK_WEB_URL = "https://web.archive.org/web"

KUNASHIR_CITY_PATTERN = re.compile(r"южно[-‐‑\s]?курильск", re.IGNORECASE)
STATUS_WORD_PATTERN = re.compile(
    r"отмен|задерж|перен[её]с|вылет|вылетел|прибыл|прилет|в\s+пол[её]те|совмещ",
    re.IGNORECASE,
)
FLIGHT_PATTERN = re.compile(r"\bHZ\s*[- ]?\s*(30(?:32|33|34|35)|46(?:21|22|23|24|25|26))\b", re.IGNORECASE)
COMPACT_FLIGHT_PATTERN = re.compile(r"\bHZ\s*[- ]?\s*(46(?:21|23|25)|30(?:32|34))\s*/\s*(46(?:22|24|26)|30(?:33|35))\b", re.IGNORECASE)

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

SEED_MEDIA_URLS = [
    "https://astv.ru/news/society/2023-08-21-shest-rejsov-zaderzhany-v-aeroportu-yuzhno-sahalinska",
    "https://astv.ru/news/society/2023-08-29-rejsy-na-treh-napravleniyah-zaderzhany-v-aeroportu-yuzhno-sahalinska",
    "https://astv.ru/news/society/2023-09-03-vosem-aviarejsov-zaderzhany-v-yuzhno-sahalinske",
    "https://astv.ru/news/society/2024-02-28-dva-rejsa-zaderzhali-na-sahaline-iz-za-sil-noj-meteli-na-kurilah",
    "https://astv.ru/news/society/2024-04-25-samolet-do-yuzhno-kuril-ska-zaderzhali-pochti-na-sutki",
    "https://astv.ru/news/society/2024-04-26-vosem-aviarejsov-po-vnutrennim-marshrutam-zaderzhali-v-sahalinskoj-oblasti",
    "https://astv.ru/news/society/2024-08-14-samolety-v-yuzhno-kuril-sk-i-ohu-zaderzhivayutsya",
    "https://astv.ru/news/society/2024-11-18-shest-aviarejsov-zaderzhivayutsya-v-aeroportu-yuzhno-sahalinska",
    "https://astv.ru/news/society/2025-04-15-samolety-v-yuzhno-kuril-sk-i-obratno-vnov-zaderzhali",
    "https://astv.ru/news/society/2025-06-16-samolety-v-yuzhno-kuril-sk-i-novosibirsk-zaderzhivayutsya",
    "https://astv.ru/news/society/2025-10-06-zaderzhannyj-samolet-v-yuzhno-kuril-sk-planiruyut-otpravit-segodnya",
    "https://astv.ru/news/society/2025-11-12-samolety-v-yuzhno-kuril-sk-i-obratno-zaderzhivayutsya",
    "https://sakh.online/news/18/2023-04-18/reys-v-yuzhno-kurilsk-otmenili-v-aeroportu-yuzhno-sahalinska-utrom-18-aprelya-365242",
    "https://sakh.online/news/18/2023-07-17/dva-reysa-do-yuzhno-kurilska-vyleteli-iz-yuzhno-sahalinska-17-iyulya-374243",
    "https://sakh.online/news/24/2024-03-29/tsiklon-narushil-aviasoobschenie-mezhdu-kurilami-i-sahalinom-411276",
    "https://sakh.online/news/18/2024-05-12/v-aeroportu-yuzhno-sahalinska-zaderzhali-neskolko-aviareysov-418338",
    "https://sakh.online/news/18/2024-08-09/na-sahaline-zaderzhali-8-reysov-aviakompanii-avrora-433252",
    "https://sakh.online/news/18/2024-08-15/v-aeroportu-yuzhno-sahalinska-otlozhili-chetyre-reysa-aviakompanii-avrora-434124",
    "https://sakh.online/news/18/2025-03-18/aviakompaniya-avrora-zaderzhala-dva-reysa-v-yuzhno-kurilsk-465051",
    "https://sakh.online/news/18/2025-03-24/aviakompaniya-avrora-zaderzhala-chetyre-reysa-mezhdu-sahalinom-i-kurilami-465996",
    "https://sakh.online/news/18/2025-03-27/pyat-reysov-mezhdu-sahalinom-i-kurilami-zaderzhali-utrom-27-marta-466550",
    "https://sakh.online/news/18/2025-12-17/v-aeroportu-yuzhno-sahalinska-zaderzhali-reys-do-moskvy-17-dekabrya-498932",
    "https://tass.ru/obschestvo/19705105",
    "https://www.aviaport.ru/news/745897/",
]


@dataclass
class HistoricalSourceRow:
    collected_at: str
    source_type: str
    source: str
    source_url: str
    source_published_date: str
    snapshot_timestamp: str
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
    raw_text: str
    confidence: str


@dataclass
class BackfillError:
    collected_at: str
    source_type: str
    source_url: str
    error: str


class ArticleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.title_parts: list[str] = []
        self.in_title = False
        self.skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "title":
            self.in_title = True
        if tag in {"script", "style", "noscript"}:
            self.skip_depth += 1
        if tag in {"br", "p", "li", "h1", "h2", "h3", "tr"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self.in_title = False
        if tag in {"script", "style", "noscript"} and self.skip_depth:
            self.skip_depth -= 1
        if tag in {"p", "li", "h1", "h2", "h3", "tr", "div"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self.skip_depth:
            return
        if self.in_title:
            self.title_parts.append(data)
        self.parts.append(data)

    @property
    def text(self) -> str:
        text = html.unescape("".join(self.parts))
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t\r\f\v]+", " ", text)
        text = re.sub(r"\n\s+", "\n", text)
        return text.strip()

    @property
    def title(self) -> str:
        return clean_inline(" ".join(self.title_parts))


def clean_inline(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def append_csv(path: Path, rows: list[Any], row_type: type) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    names = [field.name for field in fields(row_type)]
    write_header = not path.exists() or path.stat().st_size == 0

    with path.open("a", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=names, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({name: getattr(row, name) for name in names})


def parse_wayback_timestamp(value: str, timezone: ZoneInfo) -> datetime:
    captured_utc = datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=ZoneInfo("UTC"))
    return captured_utc.astimezone(timezone)


def build_board_rows(
    html_text: str,
    source_url: str,
    source_type: str,
    source: str,
    collected_at: str,
    observed_at: datetime,
    snapshot_timestamp: str,
) -> list[HistoricalSourceRow]:
    rows: list[HistoricalSourceRow] = []
    flights = parse_board_html(html_text, source=source, source_url=source_url)

    for flight in flights:
        flight_date, flight_time = parse_board_datetime(flight.scheduled_raw, observed_at)
        actual_date, actual_time = parse_board_datetime(flight.actual_raw, observed_at)
        status = normalize_status(flight.status_raw, flight_date, flight_time, observed_at)
        reason, reason_class = extract_reason(flight.status_raw)

        rows.append(
            HistoricalSourceRow(
                collected_at=collected_at,
                source_type=source_type,
                source=source,
                source_url=source_url,
                source_published_date="",
                snapshot_timestamp=snapshot_timestamp,
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
                raw_text=flight.raw_row_text,
                confidence="high" if status not in {"unknown", "scheduled"} else "medium",
            )
        )

    return rows


async def fetch_text(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(
        url,
        follow_redirects=True,
        headers={
            "User-Agent": "flyforecast-historical-backfill/0.1 (+https://flyforecast.ru)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    response.raise_for_status()
    return response.text


async def fetch_wayback_cdx(
    client: httpx.AsyncClient,
    target_url: str,
    year: int,
    limit: int,
) -> list[dict[str, str]]:
    params = {
        "url": target_url,
        "from": f"{year}0101",
        "to": f"{year}1231",
        "output": "json",
        "fl": "timestamp,original,statuscode,mimetype,digest",
        "filter": "statuscode:200",
        "collapse": "digest",
        "limit": str(limit),
    }
    response = await client.get(WAYBACK_CDX_URL, params=params)
    response.raise_for_status()
    payload = response.json()
    if not payload or len(payload) <= 1:
        return []

    headers = payload[0]
    return [dict(zip(headers, row)) for row in payload[1:]]


async def fetch_wayback_snapshots_for_url(
    client: httpx.AsyncClient,
    target_url: str,
    limit: int = 5,
) -> list[dict[str, str]]:
    params = {
        "url": target_url,
        "output": "json",
        "fl": "timestamp,original,statuscode,mimetype,digest",
        "filter": "statuscode:200",
        "collapse": "digest",
        "limit": str(limit),
    }
    response = await client.get(WAYBACK_CDX_URL, params=params)
    response.raise_for_status()
    payload = response.json()
    if not payload or len(payload) <= 1:
        return []

    headers = payload[0]
    return [dict(zip(headers, row)) for row in payload[1:]]


async def fetch_wayback_snapshots_for_url_variants(
    client: httpx.AsyncClient,
    target_url: str,
    limit: int = 5,
) -> list[dict[str, str]]:
    variants = [target_url]

    if target_url.startswith("https://"):
        variants.append("http://" + target_url.removeprefix("https://"))
    if target_url.endswith("/"):
        variants.append(target_url.rstrip("/"))
    else:
        variants.append(target_url + "/")

    seen: set[str] = set()
    for variant in variants:
        if variant in seen:
            continue
        seen.add(variant)
        try:
            snapshots = await fetch_wayback_snapshots_for_url(client, variant, limit=limit)
        except Exception:
            continue
        if snapshots:
            return snapshots

    return []


def looks_like_airportus_antibot(html_text: str) -> bool:
    markers = ["__jhash_", "ajaxload.info", "get_jhash", "noindex, noarchive"]
    return any(marker in html_text for marker in markers)


async def collect_wayback(
    client: httpx.AsyncClient,
    args: argparse.Namespace,
    collected_at: str,
    timezone: ZoneInfo,
) -> tuple[list[HistoricalSourceRow], list[BackfillError]]:
    rows: list[HistoricalSourceRow] = []
    errors: list[BackfillError] = []

    targets = ["airportus.ru/board/"]
    if args.include_mobile_wayback:
        targets.append("m.airportus.ru/board/")

    for target in targets:
        for year in range(args.from_year, args.to_year + 1):
            try:
                snapshots = await fetch_wayback_cdx(client, target, year, args.max_snapshots_per_year)
            except Exception as exc:
                errors.append(BackfillError(collected_at, "wayback_cdx", f"{target}:{year}", str(exc)))
                continue

            if args.max_wayback_snapshots and len(snapshots) > args.max_wayback_snapshots:
                snapshots = snapshots[: args.max_wayback_snapshots]

            print(f"wayback target={target} year={year} snapshots={len(snapshots)}")

            for snapshot in snapshots:
                timestamp = snapshot["timestamp"]
                original = snapshot["original"]
                snapshot_url = f"{WAYBACK_WEB_URL}/{timestamp}id_/{original}"
                observed_at = parse_wayback_timestamp(timestamp, timezone)

                try:
                    snapshot_html = await fetch_text(client, snapshot_url)
                    parsed = build_board_rows(
                        html_text=snapshot_html,
                        source_url=snapshot_url,
                        source_type="wayback_board",
                        source="airportus_board",
                        collected_at=collected_at,
                        observed_at=observed_at,
                        snapshot_timestamp=timestamp,
                    )
                    rows.extend(parsed)
                except Exception as exc:
                    errors.append(BackfillError(collected_at, "wayback_snapshot", snapshot_url, str(exc)))

                await asyncio.sleep(args.request_sleep_seconds)

    return rows, errors


def parse_article_date(text: str) -> str:
    match = re.search(r"\b(\d{1,2})\s+(" + "|".join(MONTHS_RU) + r")\s+(\d{4})\s*г", text, re.IGNORECASE)
    if match:
        day = int(match.group(1))
        month = MONTHS_RU[match.group(2).lower()]
        year = int(match.group(3))
        return date(year, month, day).isoformat()

    match = re.search(r"\b(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\b", text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3))
        if year < 100:
            year += 2000
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return ""

    return ""


def expand_flight_numbers(snippet: str) -> list[str]:
    values: set[str] = set()

    for match in COMPACT_FLIGHT_PATTERN.finditer(snippet):
        values.add(f"HZ-{match.group(1)}")
        values.add(f"HZ-{match.group(2)}")

    for match in FLIGHT_PATTERN.finditer(snippet):
        values.add(f"HZ-{match.group(1)}")

    return sorted(values)


def infer_direction(snippet: str, flight_number: str) -> str:
    lower = snippet.lower()

    if "прилет" in lower or "прибыт" in lower or "из южно" in lower:
        return "arrival"
    if "вылет" in lower or "отправ" in lower or "в южно" in lower:
        return "departure"

    if not flight_number:
        return "unknown"

    last_digit = flight_number[-1]
    if last_digit in {"2", "4", "6", "3", "5"} and flight_number.startswith("HZ-303"):
        return "arrival" if last_digit in {"3", "5"} else "departure"
    if last_digit in {"2", "4", "6"}:
        return "arrival"
    if last_digit in {"1", "3", "5"}:
        return "departure"

    return "unknown"


def infer_route(direction: str) -> str:
    if direction == "arrival":
        return "Южно-Курильск - Южно-Сахалинск"
    if direction == "departure":
        return "Южно-Сахалинск - Южно-Курильск"
    return "Южно-Курильск"


def infer_event_date(snippet: str, article_date: str) -> str:
    base_year = int(article_date[:4]) if article_date else None

    numeric = re.search(r"\b(?:за|от|с|на)?\s*(\d{1,2})[.](\d{1,2})(?:[.](\d{2,4}))?\b", snippet)
    if numeric:
        day = int(numeric.group(1))
        month = int(numeric.group(2))
        year_raw = numeric.group(3)
        year = int(year_raw) if year_raw else base_year
        if year and year < 100:
            year += 2000
        if year:
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                pass

    ru = re.search(r"\b(\d{1,2})\s+(" + "|".join(MONTHS_RU) + r")\b", snippet, re.IGNORECASE)
    if ru and base_year:
        try:
            return date(base_year, MONTHS_RU[ru.group(2).lower()], int(ru.group(1))).isoformat()
        except ValueError:
            pass

    return article_date


def infer_reason_from_article(text: str) -> tuple[str, str]:
    lower = text.lower()
    if "covid" in lower or "коронавирус" in lower:
        return "COVID-19 restrictions", "covid_restrictions"
    if "туман" in lower or "низкая видимость" in lower or "низкая облачность" in lower:
        return "Низкая видимость/туман по тексту источника", "fog_visibility"
    if "боковой ветер" in lower or "сильный ветер" in lower or "порыв" in lower:
        return "Сильный ветер по тексту источника", "wind"
    if "циклон" in lower or "метель" in lower or "непогод" in lower or "метеоуслов" in lower:
        return "Неблагоприятные метеоусловия по тексту источника", "weather"
    return "", "unknown"


def make_snippets(text: str) -> list[str]:
    snippets: list[str] = []
    blocks = [clean_inline(block) for block in text.splitlines()]

    for block in blocks:
        if not block:
            continue
        if (KUNASHIR_CITY_PATTERN.search(block) or FLIGHT_PATTERN.search(block)) and STATUS_WORD_PATTERN.search(block):
            snippets.append(block[:1200])

    if not snippets:
        compact_text = clean_inline(text)
        for match in re.finditer(r".{0,180}(?:Южно[-‐‑\s]?Курильск|HZ\s*[- ]?\s*(?:30(?:32|33|34|35)|46(?:21|22|23|24|25|26))).{0,420}", compact_text, re.IGNORECASE):
            snippet = clean_inline(match.group(0))
            if STATUS_WORD_PATTERN.search(snippet):
                snippets.append(snippet[:1200])

    deduped: list[str] = []
    seen: set[str] = set()
    for snippet in snippets:
        key = snippet.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(snippet)
    return deduped


def parse_news_article(
    html_text: str,
    source_url: str,
    source: str,
    source_type: str,
    collected_at: str,
    timezone: ZoneInfo,
) -> list[HistoricalSourceRow]:
    parser = ArticleTextParser()
    parser.feed(html_text)
    text = parser.text
    article_date = parse_article_date(text)
    reason, reason_class = infer_reason_from_article(text)
    observed_at = datetime.now(timezone)
    rows: list[HistoricalSourceRow] = []

    for snippet in make_snippets(text):
        flight_numbers = expand_flight_numbers(snippet)
        if not flight_numbers and KUNASHIR_CITY_PATTERN.search(snippet):
            flight_numbers = [""]

        for flight_number in flight_numbers:
            if flight_number and not FLIGHT_PATTERN.search(flight_number):
                continue
            direction = infer_direction(snippet, flight_number)
            event_date = infer_event_date(snippet, article_date)
            status = normalize_status(snippet, event_date, "", observed_at)
            row_reason, row_reason_class = extract_reason(snippet)
            if row_reason_class == "unknown":
                row_reason = reason
                row_reason_class = reason_class

            rows.append(
                HistoricalSourceRow(
                    collected_at=collected_at,
                    source_type=source_type,
                    source=source,
                    source_url=source_url,
                    source_published_date=article_date,
                    snapshot_timestamp="",
                    direction=direction,
                    flight_date=event_date,
                    flight_time="",
                    flight_numbers=flight_number,
                    route=infer_route(direction),
                    status_raw=snippet,
                    status_normalized=status,
                    reason=row_reason,
                    reason_class=row_reason_class,
                    scheduled_time_raw="",
                    actual_time_raw="",
                    actual_date="",
                    actual_time="",
                    raw_text=snippet,
                    confidence="medium",
                )
            )

    return rows


def airportus_post_urls(from_post_id: int, to_post_id: int) -> list[str]:
    return [f"https://airportus.ru/news/post/{post_id}/" for post_id in range(from_post_id, to_post_id + 1)]


async def collect_article_urls(
    client: httpx.AsyncClient,
    urls: list[str],
    source_type: str,
    source: str,
    args: argparse.Namespace,
    collected_at: str,
    timezone: ZoneInfo,
) -> tuple[list[HistoricalSourceRow], list[BackfillError]]:
    rows: list[HistoricalSourceRow] = []
    errors: list[BackfillError] = []
    sem = asyncio.Semaphore(args.concurrency)

    async def fetch_and_parse(url: str) -> None:
        async with sem:
            try:
                html_text = await fetch_text(client, url)
                parsed_source_url = url
                parsed_source_type = source_type

                if (
                    source_type == "airportus_news"
                    and args.airportus_news_wayback_fallback
                    and looks_like_airportus_antibot(html_text)
                ):
                    snapshots = await fetch_wayback_snapshots_for_url_variants(client, url, limit=args.news_wayback_limit)
                    if not snapshots:
                        return
                    snapshot = snapshots[0]
                    parsed_source_url = f"{WAYBACK_WEB_URL}/{snapshot['timestamp']}id_/{snapshot['original']}"
                    parsed_source_type = "airportus_news_wayback"
                    html_text = await fetch_text(client, parsed_source_url)

                parsed = parse_news_article(
                    html_text=html_text,
                    source_url=parsed_source_url,
                    source=source,
                    source_type=parsed_source_type,
                    collected_at=collected_at,
                    timezone=timezone,
                )
                rows.extend(parsed)
                if parsed:
                    print(f"{parsed_source_type} hit rows={len(parsed)} url={parsed_source_url}")
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in {404, 410}:
                    errors.append(BackfillError(collected_at, source_type, url, str(exc)))
            except Exception as exc:
                errors.append(BackfillError(collected_at, source_type, url, str(exc)))
            await asyncio.sleep(args.request_sleep_seconds)

    await asyncio.gather(*(fetch_and_parse(url) for url in urls))
    return rows, errors


def dedupe_rows(rows: list[HistoricalSourceRow]) -> list[HistoricalSourceRow]:
    deduped: list[HistoricalSourceRow] = []
    seen: set[tuple[str, str, str, str, str, str, str]] = set()

    for row in rows:
        key = (
            row.source_url,
            row.snapshot_timestamp,
            row.flight_numbers,
            row.flight_date,
            row.direction,
            row.status_normalized,
            row.raw_text[:300],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return deduped


async def run(args: argparse.Namespace) -> None:
    timezone = ZoneInfo(args.timezone)
    collected_at = datetime.now(timezone).replace(microsecond=0).isoformat()
    timeout = httpx.Timeout(args.timeout_seconds)
    limits = httpx.Limits(max_connections=max(args.concurrency + 4, 8))

    all_rows: list[HistoricalSourceRow] = []
    all_errors: list[BackfillError] = []

    async with httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True) as client:
        if args.wayback:
            rows, errors = await collect_wayback(client, args, collected_at, timezone)
            all_rows.extend(rows)
            all_errors.extend(errors)

        if args.airportus_news:
            urls = airportus_post_urls(args.from_post_id, args.to_post_id)
            if args.max_news_posts:
                urls = urls[: args.max_news_posts]
            rows, errors = await collect_article_urls(
                client=client,
                urls=urls,
                source_type="airportus_news",
                source="airportus",
                args=args,
                collected_at=collected_at,
                timezone=timezone,
            )
            all_rows.extend(rows)
            all_errors.extend(errors)

        if args.seed_media:
            rows, errors = await collect_article_urls(
                client=client,
                urls=SEED_MEDIA_URLS,
                source_type="media_seed",
                source="media",
                args=args,
                collected_at=collected_at,
                timezone=timezone,
            )
            all_rows.extend(rows)
            all_errors.extend(errors)

    all_rows = dedupe_rows(all_rows)
    append_csv(Path(args.output), all_rows, HistoricalSourceRow)
    append_csv(Path(args.errors_output), all_errors, BackfillError)

    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for row in all_rows:
        status_counts[row.status_normalized] = status_counts.get(row.status_normalized, 0) + 1
        source_counts[row.source_type] = source_counts.get(row.source_type, 0) + 1

    print(f"done rows={len(all_rows)} errors={len(all_errors)} output={args.output}")
    print(f"source_counts={source_counts}")
    print(f"status_counts={status_counts}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill historical Kunashir flight status evidence from airport boards, Wayback snapshots, and seed media articles."
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--errors-output", default=DEFAULT_ERRORS_OUTPUT)
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--request-sleep-seconds", type=float, default=0.2)
    parser.add_argument("--concurrency", type=int, default=6)

    parser.add_argument("--wayback", action="store_true", help="Collect Wayback snapshots of airport boards.")
    parser.add_argument("--include-mobile-wayback", action="store_true", help="Also collect m.airportus.ru/board/ snapshots.")
    parser.add_argument("--from-year", type=int, default=2016)
    parser.add_argument("--to-year", type=int, default=datetime.now().year)
    parser.add_argument("--max-snapshots-per-year", type=int, default=250)
    parser.add_argument("--max-wayback-snapshots", type=int, default=0, help="Optional per-year cap after CDX lookup. 0 means no cap.")

    parser.add_argument("--airportus-news", action="store_true", help="Scan official airportus.ru/news/post/{id}/ pages.")
    parser.add_argument("--from-post-id", type=int, default=2500)
    parser.add_argument("--to-post-id", type=int, default=4300)
    parser.add_argument("--max-news-posts", type=int, default=0, help="Optional cap for quick tests. 0 means no cap.")
    parser.add_argument("--airportus-news-wayback-fallback", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--news-wayback-limit", type=int, default=5)

    parser.add_argument("--seed-media", action="store_true", help="Parse known ASTV/Sakh.online/TASS/Aviaport seed URLs.")

    parser.add_argument("--all", action="store_true", help="Enable wayback, airportus news, and seed media.")

    args = parser.parse_args()
    if args.all:
        args.wayback = True
        args.airportus_news = True
        args.seed_media = True
        args.include_mobile_wayback = True

    if not args.wayback and not args.airportus_news and not args.seed_media:
        args.wayback = True
        args.airportus_news = True
        args.seed_media = True

    return args


def main() -> None:
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
