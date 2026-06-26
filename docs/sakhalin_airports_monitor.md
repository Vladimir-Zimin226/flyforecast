# Sakhalin airports raw monitor

Last checked from backup: 2026-06-26 18:33:49 +11.

## Purpose

This monitor is a research data collector for deciding how to add new Sakhalin/Kuril airports to FlyForecast. It is not yet a production prediction feature.

The main question is not just "can we scrape another airport", but:

- which airports have enough regular board facts to support a useful service;
- how often flights actually appear for each airport;
- whether weather snapshots cover the same periods as board facts;
- which rules are needed before an airport can be exposed in the product.

Current focus: flight frequency and weather coverage with factual board evidence, especially arrival facts now that `--include-arrivals` is enabled.

## Service

Docker service: `sakhalin_airports_collector`.

Command:

```bash
python /app/pipelines/flight_status/collect_sakhalin_airports_status.py --loop --include-arrivals --interval-seconds ${SAKHALIN_AIRPORTS_INTERVAL_SECONDS:-900}
```

Code: `pipelines/flight_status/collect_sakhalin_airports_status.py`.

Outputs:

- `data/raw/sakhalin_airports/sakhalin_airport_board_hourly.csv`
- `data/raw/sakhalin_airports/sakhalin_airport_weather_hourly.csv`
- `data/raw/sakhalin_airports/collection_errors.csv`

The admin backup includes these files under `raw/sakhalin_airports/`.

## Prediction Rules Draft

A working draft for how new airports should become prediction-ready lives in:

`data/interim/analysis/new_airports_prediction_rules_draft.md`

That file is intentionally under `data/interim/analysis/`, so it is local research context and is not tracked by git.

Core idea from the draft:

- do not expose a new airport just because it appears in the board;
- first require enough observed flight dates, enough arrival-based final facts, reliable weather coverage, clean alias rules, and a known flight-frequency pattern;
- use arrival `arrived` as the strongest completion fact;
- use departure `departed` as useful but weaker evidence until arrival confirmation exists;
- treat `ITU` as the first likely candidate and `OHH` as the second, pending arrival-history accumulation after 2026-06-26.

## What It Collects

Target airports in code:

| Code | Airport / city | Board route aliases |
| --- | --- | --- |
| `OHH` | Оха, Новостройка | `Оха`, `Новостройка` |
| `NGL` | Ноглики | `Ноглики` |
| `EKS` | Шахтерск | `Шахтерск`, `Шахтёрск` |
| `ZZO` | Зональное / Тымовское | `Зональное`, `Тымовское`, `Кировское` |
| `ALS_SAKH` | Александровск-Сахалинский | `Александровск-Сахалинский`, `Александровск` |
| `ITU` | Ясный / Курильск / Итуруп | `Курильск`, `Итуруп`, `Ясный`; excludes `Южно-Курильск` in current code |

Each loop:

1. Fetches `https://airportus.ru/board/`.
2. Parses all board rows, then matches route text against airport aliases.
3. Writes both departure and arrival rows in the Docker service because compose passes `--include-arrivals`.
4. Fetches Open-Meteo hourly forecast for each airport coordinate, default `forecast_days=2`.
5. Appends rows to CSV; files are append-only and contain repeated snapshots.

Weather fields include temperature, humidity, dew point, pressure, cloud cover, low/mid/high cloud cover, precipitation, rain, snowfall, weather code, wind speed, gusts, wind direction, and visibility.

## Backup Snapshot

Backup files checked:

- `backup/raw/sakhalin_airports/sakhalin_airport_board_hourly.csv`
- `backup/raw/sakhalin_airports/sakhalin_airport_weather_hourly.csv`
- `backup/raw/sakhalin_airports/collection_errors.csv`

Observed period:

- first snapshot: 2026-06-20 23:36:30 +11;
- last snapshot: 2026-06-26 18:19:24 +11;
- coverage: 7 calendar dates, about 5 days 19 hours of wall-clock collection;
- snapshots: 556 board/weather collection timestamps;
- usual interval: about 15 minutes.

Raw volume:

- board rows: 6,922;
- weather rows: 157,728;
- collection errors: 50.

All recorded errors in this backup are Open-Meteo errors. Several are `429 Too Many Requests` through the proxy endpoint. No board fetch/parse errors were present in `raw/sakhalin_airports/collection_errors.csv`.

## First Frequency Read

The board file is snapshot data, so raw row counts overstate flight counts. After removing the old `ITU` false positives for `Южно-Курильск` and roughly deduplicating by airport, flight date, flight number, route, and scheduled text, the backup contains about 56 useful board items.

| Airport | Flight dates with active items | Dates with departed evidence | Rough pattern in this backup |
| --- | ---: | ---: | --- |
| `ALS_SAKH` | 6 | 6 | Daily in the checked period, one outbound pattern per day. |
| `EKS` | 3 | 3 | About every other day: 2026-06-22, 2026-06-24, 2026-06-26. |
| `ITU` | 8 | 7 | Very frequent; yes, Iturup appears to have flights every day in this sample. |
| `NGL` | 2 | 2 | Sparse: 2026-06-22 and 2026-06-25. |
| `OHH` | 7 | 6 | Very frequent, nearly daily; next-day scheduled row visible for 2026-06-27. |
| `ZZO` | 1 | 1 | Sparse in this sample: only 2026-06-23. |

Important interpretation notes:

- `scheduled` rows often represent a future or pre-departure board state, and later snapshots can show the same flight as `departed`.
- For service readiness, `departed` evidence is stronger than `scheduled`.
- The backup checked here was collected before arrivals were enabled in compose, so it mostly reflects departures. New snapshots should include arrivals too.

## Iturup Insight

The user's intuition is supported by this backup: Iturup (`ITU`) has the strongest-looking frequency signal together with Oha.

Cleaned `ITU` board items in this backup:

- 2026-06-20: 1 departed flight;
- 2026-06-21: 1 scheduled and 1 departed item;
- 2026-06-22: 2 scheduled and 2 departed items;
- 2026-06-23: 1 scheduled and 1 departed item;
- 2026-06-24: 1 scheduled and 1 departed item;
- 2026-06-25: 1 scheduled and 1 departed item;
- 2026-06-26: 2 scheduled and 2 departed items;
- 2026-06-27: 1 scheduled future item.

This is enough to treat Iturup as a prime candidate for the next airport research path.

## Weather Coverage With Board Facts

Weather snapshots exist for all target airports on all 7 calendar dates in the backup.

Same-snapshot coverage for board observations with matched rows:

| Airport | Board snapshots with same-timestamp weather |
| --- | ---: |
| `ALS_SAKH` | 540 / 549, 98.4% |
| `EKS` | 536 / 546, 98.2% |
| `ITU` | 540 / 549, 98.4% |
| `NGL` | 376 / 377, 99.7% |
| `OHH` | 544 / 549, 99.1% |
| `ZZO` | 187 / 188, 99.5% |

This is a strong coverage signal: when the board has a relevant route row, the monitor almost always has a weather snapshot for that airport at the same collection timestamp.

Rough weather extremes seen in the collected forecasts:

| Airport | Min visibility | Max low cloud | Max gust | Max precipitation |
| --- | ---: | ---: | ---: | ---: |
| `ALS_SAKH` | 80 m | 100% | 53.6 | 8.3 |
| `EKS` | 100 m | 100% | 43.9 | 6.3 |
| `ITU` | 80 m | 100% | 63.4 | 4.7 |
| `NGL` | 180 m | 100% | 40.7 | 1.8 |
| `OHH` | 80 m | 100% | 38.9 | 11.3 |
| `ZZO` | 120 m | 100% | 33.8 | 5.3 |

These are forecast rows, not observed METAR facts. They are still useful for building airport-specific weather-risk features if paired with board outcomes.

## Known Caveats

- The CSV files are append-only snapshots. Always deduplicate before analyzing flight frequency.
- Compose now runs the collector with `--include-arrivals`; older backup rows still mostly reflect departures and should be interpreted accordingly.
- The first backup day contains old `ITU` rows that matched `Южно-Курильск`. Current code excludes this route, but historical backup rows should still be filtered before analysis.
- Open-Meteo proxy rate limiting appears in errors; if the service is expanded, request pacing or caching should be revisited.
- A week is not enough to decide seasonal reliability, but it is enough to prioritize candidates for deeper collection.

## First Product Implications

Likely next candidates:

1. `ITU` / Iturup: frequent flights and strong board/weather overlap.
2. `OHH` / Oha: frequent flights and strong board/weather overlap.
3. `ALS_SAKH`: daily signal in this sample, but route/operator semantics should be checked.

Lower-priority until more data accumulates:

- `EKS`: useful but less frequent.
- `NGL`: sparse in this sample.
- `ZZO`: very sparse in this sample.

Before exposing a new airport in the product, define:

- minimum number of observed flight dates;
- minimum weather coverage near board facts;
- route alias and exclusion rules;
- whether arrival rows are required for final outcome;
- airport-specific flight window rules for weather aggregation.
