# Data Experiments

Этот документ фиксирует историю работы с данными проекта `flyforecast`: от первого Telegram-датасета до текущего v3, hourly board collector, forecast monitor и weather-enriched training dataset.

Цель документа — отделить исследовательскую историю данных от README. README должен оставаться входной точкой в проект, а здесь можно подробно хранить решения, промежуточные результаты, ограничения и будущие идеи.

---

## Текущая карта данных

Основные слои данных:

- `data/processed/dataset_daily_flights.csv` — первый backend-safe Telegram dataset v2.
- `data/processed/dataset_daily_flights_v3.csv` — текущий рабочий кандидат/продовый датасет с ручной проверкой, historical board evidence и hourly board layer.
- `data/raw/flight_status/kunashir_flight_status_hourly.csv` — hourly-наблюдения онлайн-табло аэропорта.
- `data/interim/evaluation/forecast_monitor.sqlite` — ledger прогнозов, фактов и оценок качества.
- `data/processed/training_dataset_v1.csv` — обучающий датасет v1 с погодой Open-Meteo Archive, календарными и rolling-признаками.

Текущий production backend должен читать путь из `FLYFORECAST_DATASET_PATH`.

---

## 1. Первый Telegram Dataset

Первый рабочий исторический датасет был собран из Telegram-канала «Аэропорт на Кунашире(НЕофициально)» / `t.me/aeroportuk`.

Исходная идея:

- взять длинную историю сообщений;
- разметить дни как `completed` или `cancelled`;
- получить backend-safe binary target для MVP baseline;
- не отдавать сырые сообщения пользователю и не использовать их в LLM.

Первый рабочий backend dataset:

```text
data/processed/dataset_daily_flights.csv
```

Состояние Telegram v2 на момент MVP:

- период: `2018-03-22` — `2026-04-07`;
- всего confirmed binary flight-days: `699`;
- `completed`: `381`;
- `cancelled`: `318`;
- доля отмен среди confirmed-наблюдений: около `45.5%`;
- data version: `telegram-rule-labels-v2-2026-05-03`.

Ограничения:

- источник неофициальный;
- автоматическая разметка нуждается в ручной проверке;
- конфликтные случаи должны исключаться или подтверждаться отдельным evidence layer;
- `scheduled` не равен `completed`.

---

## 2. Источники Evidence

На этапе Data Understanding проверялись такие источники:

- Telegram `t.me/aeroportuk`: основной исторический источник v2.
- Онлайн-табло аэропорта Южно-Сахалинска `https://airportus.ru/board/`: официальный текущий источник статусов.
- Wayback Machine для `airportus.ru/board/` и `m.airportus.ru/board/`: архивные снимки табло.
- Официальные новости аэропорта `airportus.ru/news/post/{id}/`: отдельные подтверждения отмен, задержек и изменений расписания.
- ASTV и Sakh.online: вторичный media evidence layer.
- TASS и Aviaport: точечные вторичные подтверждения.
- Сайт Авроры `flyaurora.ru`: проверялся как источник, но не дал стабильного публичного табло для серверного парсинга.

Текущая позиция:

- Telegram остаётся полезным историческим baseline-источником.
- Airportus/Wayback/news/media нужны как слой подтверждений.
- Для конфликтов приоритет выше у официального табло/новостей и ручной проверки.

---

## 3. Historical Flight Status Backfill

Для ретроспективного поиска подтверждений добавлен скрипт:

```text
pipelines/flight_status/backfill_historical_status.py
```

Он собирает не финальные labels, а сырые свидетельства из источников:

- Wayback snapshots онлайн-табло;
- официальный архив новостей airportus;
- fallback на Wayback для новостей;
- seed-список статей ASTV, Sakh.online, TASS, Aviaport.

Выходные файлы:

```text
data/raw/flight_status/kunashir_historical_sources.csv
data/raw/flight_status/historical_backfill_errors.csv
```

Последний успешный полный прогон `v2`:

```bash
python pipelines/flight_status/backfill_historical_status.py \
  --all \
  --from-year 2016 \
  --to-year 2026 \
  --from-post-id 2500 \
  --to-post-id 4300 \
  --concurrency 4 \
  --request-sleep-seconds 0.3 \
  --output data/raw/flight_status/kunashir_historical_sources_v2.csv \
  --errors-output data/raw/flight_status/historical_backfill_errors_v2.csv
```

Результат `v2`:

- всего сырых свидетельств: `289`;
- `wayback_board`: `128`;
- `airportus_news`: `69`;
- `media_seed`: `92`;
- уникальных `source_url`: `91`;
- уникальных `flight_date`: `129`;
- уникальных event keys (`date`, `flight_numbers`, `direction`, `status`): `259`;
- ошибок сбора: `12`.

Распределение статусов:

- `delayed`: `104`;
- `scheduled`: `90`;
- `cancelled`: `39`;
- `unknown`: `24`;
- `departed`: `14`;
- `combined`: `10`;
- `arrived`: `6`;
- `check_in`: `1`;
- `in_flight`: `1`.

Интерпретация:

- `departed`, `arrived`, `in_flight` -> сильное evidence для `completed`;
- `cancelled` -> сильное evidence для `cancelled`;
- `delayed`, `combined` -> disruption evidence, но нужен финальный исход;
- `scheduled`, `check_in` -> только planned-only evidence;
- `unknown` -> exclude или ручной аудит.

---

## 4. Hourly Flight Status Collector

Hourly collector находится здесь:

```text
pipelines/flight_status/collect_kunashir_status.py
```

Каждый запуск:

- читает онлайн-табло `https://airportus.ru/board/`;
- выбирает строки по городу `Южно-Курильск` на вылет и прилёт;
- опционально пробует источник Авроры;
- запрашивает текущую погоду Open-Meteo для Менделеево;
- дописывает наблюдения в CSV.

Основной файл:

```text
data/raw/flight_status/kunashir_flight_status_hourly.csv
```

Файл ошибок:

```text
data/raw/flight_status/collection_errors.csv
```

Ключевые поля:

- `observed_at`, `observation_date`, `observation_time`;
- `source`, `source_url`, `direction`;
- `flight_date`, `flight_time`, `flight_numbers`, `route`;
- `status_raw`, `status_normalized`, `reason`, `reason_class`;
- `scheduled_time_raw`, `actual_time_raw`;
- погодные признаки Open-Meteo.

Запуск:

```bash
python pipelines/flight_status/collect_kunashir_status.py
```

Hourly-режим:

```bash
python pipelines/flight_status/collect_kunashir_status.py --loop --interval-seconds 3600
```

В Docker Compose добавлен сервис `collector`. В production он запускается с `--skip-aurora`, чтобы не шуметь SSL-ошибками Авроры. Основной источник `airportus` остаётся включённым.

Серверный snapshot на 2026-05-20:

- rows: `1030`;
- observed_at: `2026-05-10T13:06:50+11:00` -> `2026-05-20T15:34:40+11:00`;
- source `airportus`: `1030`;
- Аврора давала SSL errors и была отключена из prod collector.

---

## 5. Dataset v3

Для агрегации historical evidence и сборки v3 используется:

```text
pipelines/flight_status/build_dataset_v3.py
```

Входные слои:

- `data/raw/flight_status/kunashir_historical_sources_v3.csv`;
- `data/raw/flight_status/kunashir_flight_status_hourly.csv`;
- `data/interim/flight_status/manual_review_v3.xlsx`;
- `data/interim/flight_status/manual_overrides_v3.csv`;
- текущий Telegram/backend dataset.

Правила агрегации:

- `departed`, `arrived`, `in_flight` -> `completed`;
- `cancelled` -> `cancelled`;
- `delayed` -> `delayed`;
- `combined` -> `disrupted`;
- `scheduled`, `check_in` -> `planned_only`;
- `unknown` -> `needs_review` или exclude.

Hourly airport board применяется отдельным слоем:

- `departed`, `arrived`, `in_flight` -> `completed`;
- `combined` без фактического вылета/прилёта за дату -> `cancelled`;
- `scheduled`-only сохраняется в excluded audit и не попадает в target.

Ручной review применяется поверх автоматических historical labels:

- `completed`/`cancelled` попадают в backend-safe target;
- `unknown`, `unsure`, `exclude`, `delayed`, `disrupted`, `planned_only` сохраняются в excluded audit;
- точечные факты добавляются через `manual_overrides_v3.csv`.

Особый ручной факт:

- `2026-05-19` -> `cancelled`;
- причина: `fog`;
- источник: ручная проверка Telegram от 2026-05-20;
- контекст: рейс был отменён/перенесён из-за тумана.

Результат последней сборки:

- `historical_daily_labels_v3.csv`: `124` daily rows;
- `needs_manual_review_v3.csv`: `39` rows;
- `board_daily_labels_v3.csv`: `12` rows;
- `board_daily_excluded_v3.csv`: `1` row;
- `manual_review_applied_v3.csv`: `36` rows;
- `manual_review_excluded_v3.csv`: `4` rows;
- `dataset_daily_flights_v3.csv`: `759` binary rows;
- v3 status distribution: `completed=411`, `cancelled=348`;
- data version: `telegram-v2-plus-historical-board-manual-v3-2026-05-20`.

Запуск:

```bash
python pipelines/flight_status/build_dataset_v3.py
```

Production switch:

```env
FLYFORECAST_DATASET_PATH=/app/data/processed/dataset_daily_flights_v3.csv
```

На сервере v3 был проверен:

```text
dataset_path: /app/data/processed/dataset_daily_flights_v3.csv
exists: True
rows: 759
last_date: 2026-05-20
data_version: telegram-v2-plus-historical-board-manual-v3-2026-05-20
```

---

## 6. Forecast Monitor

Монитор качества прогнозов:

```text
pipelines/evaluation/forecast_monitor.py
```

Назначение:

- каждый день создавать прогнозы на набор горизонтов;
- сохранять прогнозы в ledger;
- позже сопоставлять их с фактом из табло;
- считать hit, Brier score и absolute error;
- экспортировать CSV для анализа.

Монитор не вызывает GigaChat и не ходит в публичный `/predict`. Он напрямую использует backend-логику:

- `history`;
- `weather`;
- `predictor`.

По умолчанию:

- с 06:00 по времени аэропорта создаёт прогнозы на горизонты `0..45`, `60`, `90` дней;
- не дублирует прогнозы за тот же день, `model_version`, `data_version` и dataset path;
- читает факты из `data/raw/flight_status/kunashir_flight_status_hourly.csv`;
- финализирует outcome после лага `D+2`;
- сохраняет weather snapshot прогноза в таблице `predictions`.

База:

```text
data/interim/evaluation/forecast_monitor.sqlite
```

Exports:

```text
data/interim/evaluation/exports/forecast_prediction_runs.csv
data/interim/evaluation/exports/forecast_predictions.csv
data/interim/evaluation/exports/forecast_outcomes.csv
data/interim/evaluation/exports/forecast_evaluations.csv
data/interim/evaluation/exports/forecast_metrics_summary.csv
```

Запуск:

```bash
python pipelines/evaluation/forecast_monitor.py
```

Loop:

```bash
python pipelines/evaluation/forecast_monitor.py --loop --interval-seconds 3600
```

Первый production run после фикса импорта:

```text
forecast_monitor predictions_inserted=48 outcomes_seen=13 outcomes_changed=13 evaluations_changed=0
```

`evaluations_changed=0` на первом запуске нормально, потому что факты ещё не финализированы по правилу `D+2`.

### Production accuracy snapshot перед `mvp-baseline-005`

Состояние, зафиксированное в админке перед разбором ошибок 27-31 мая и деплоем `mvp-baseline-005`:

```text
date_observed: 2026-06-01
model_before_fix: mvp-baseline-004
total_predictions: 767
total_evaluations: 70
total_hits: 24
total_misses: 46
total_pending: 697
accuracy: 0.34
```

Интерпретация:

- это метрики ledger, а не offline backtest;
- старые промахи не пересчитываются автоматически после смены `model_version`;
- `mvp-baseline-005` начнёт влиять только на новые строки `predictions`;
- для честного сравнения следующих версий нужно сохранять snapshot SQLite/CSV перед каждым изменением baseline.

Команда для production-сервера, чтобы записать текущий markdown snapshot метрик:

```bash
mkdir -p data/interim/evaluation/snapshots
SNAPSHOT="data/interim/evaluation/snapshots/forecast_accuracy_snapshot_$(date +%Y%m%d_%H%M%S).md"
docker exec flyforecast-monitor python -c "import sqlite3; from datetime import datetime; c=sqlite3.connect('/app/data/interim/evaluation/forecast_monitor.sqlite'); c.row_factory=sqlite3.Row; total=c.execute(\"select count(*) total_predictions from predictions\").fetchone(); evals=c.execute(\"select count(*) total_evaluations, coalesce(sum(hit),0) total_hits, count(*)-coalesce(sum(hit),0) total_misses, round(avg(hit),4) accuracy, round(avg(brier_score),6) brier_score, round(avg(absolute_error),6) mean_absolute_error from prediction_evaluations\").fetchone(); pending=c.execute(\"select count(*) total_pending from predictions p left join prediction_evaluations e on e.prediction_id=p.id where e.prediction_id is null\").fetchone(); versions=c.execute(\"select model_version, count(*) predictions from predictions group by model_version order by model_version\").fetchall(); buckets=c.execute(\"select horizon_bucket, count(*) evaluated_count, round(avg(hit),4) accuracy, round(avg(brier_score),6) brier_score, round(avg(absolute_error),6) mean_absolute_error from prediction_evaluations group by horizon_bucket order by horizon_bucket\").fetchall(); latest=c.execute(\"select run_date, target_date, horizon_days, model_version, probability_flight, decision, weather_source from predictions order by id desc limit 10\").fetchall(); print('# Forecast Accuracy Snapshot'); print(); print('generated_at:', datetime.now().isoformat(timespec='seconds')); print(); print('## Totals'); print(dict(total)); print(dict(evals)); print(dict(pending)); print(); print('## Predictions by model_version'); [print(dict(r)) for r in versions]; print(); print('## Metrics by horizon_bucket'); [print(dict(r)) for r in buckets]; print(); print('## Latest predictions'); [print(dict(r)) for r in latest]" > "$SNAPSHOT"
echo "$SNAPSHOT"
```

Команда для пересчёта уже оцененных прогнозов текущей backend-логикой без изменения production ledger:

```bash
docker exec flyforecast-monitor python /app/pipelines/evaluation/recalculate_accuracy_snapshot.py --db-path /app/data/interim/evaluation/forecast_monitor.sqlite --output-dir /app/data/interim/evaluation/snapshots --label recalculated_mvp_baseline_005
```

Скрипт:

```text
pipelines/evaluation/recalculate_accuracy_snapshot.py
```

Что делает:

- берёт все строки `predictions`, у которых уже есть `prediction_evaluations`;
- собирает из сохранённых колонок `WeatherSnapshot` и `HistoricalSnapshot`;
- заново считает `probability_flight`, `decision`, hit, Brier score и absolute error текущим `app.services.predictor`;
- пишет markdown summary и CSV details в `data/interim/evaluation/snapshots`;
- не меняет SQLite ledger и не перезаписывает исторические прогнозы.

Ограничение:

- это не восстановление полного исторического прогноза погоды на момент старого run. Старые версии monitor не всегда сохраняли все поля, например `flight_window_available`; audit честно использует только то, что уже лежит в SQLite.

Команда для полноценного backtest текущего baseline с восстановлением погодного snapshot через Open-Meteo Historical Forecast:

```bash
docker exec flyforecast-monitor python /app/pipelines/evaluation/backtest_current_baseline.py --db-path /app/data/interim/evaluation/forecast_monitor.sqlite --output-dir /app/data/interim/evaluation/backtests --label current_baseline_historical_forecast --weather-provider historical-forecast --target-accuracy 0.90
```

Скрипт:

```text
pipelines/evaluation/backtest_current_baseline.py
```

Что делает:

- берёт уже оцененные строки forecast monitor;
- для горизонтов `0..15` заново строит погодные признаки из Open-Meteo Historical Forecast;
- использует текущую weather-window логику из `backend/app/services/weather.py`;
- сохраняет `HistoricalSnapshot` из ledger, чтобы не подмешивать будущие факты в историческую вероятность;
- считает текущую `MODEL_VERSION`, hit, Brier score, absolute error и confusion counts;
- пишет markdown summary и CSV details в `data/interim/evaluation/backtests`;
- не меняет SQLite ledger.

Ограничение:

- Open-Meteo Historical Forecast не гарантирует точное восстановление именно того forecast run, который был доступен пользователю в день прогноза. Это лучше, чем пустые старые snapshot, но всё ещё offline-аудит, а не подмена production ledger.

Команда для безопасного snapshot текущих CSV exports без нового запуска monitor:

```bash
EXPORT_SNAPSHOT_DIR="data/interim/evaluation/exports_snapshot_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$EXPORT_SNAPSHOT_DIR"
docker cp flyforecast-monitor:/app/data/interim/evaluation/exports/. "$EXPORT_SNAPSHOT_DIR"/
echo "$EXPORT_SNAPSHOT_DIR"
```

---

## 7. Training Dataset v1

После v3 стало видно, что для настоящего ML не хватает погодного слоя в обучающем датасете. Поэтому добавлен отдельный training dataset, который не заменяет fact dataset, а строится поверх него.

Скрипт:

```text
pipelines/training/build_training_dataset_v1.py
```

Выходные файлы:

```text
data/interim/weather/open_meteo_daily_weather_v1.csv
data/processed/training_dataset_v1.csv
data/interim/training/training_dataset_v1_summary.txt
```

Источники:

- target: `data/processed/dataset_daily_flights_v3.csv`;
- погода: Open-Meteo Archive API;
- точки погоды:
  - `mendeleyevo`: аэропорт Менделеево, Кунашир;
  - `khomutovo`: Южно-Сахалинск / Хомутово.

Группы признаков:

- target columns из v3;
- daily weather aggregates по двум точкам;
- weather flags: высокая влажность, облачность, ветер, порывы, осадки, снег, низкое давление, proxy fog risk;
- calendar features: day of week, weekend, cyclical day/month;
- leakage-safe rolling history:
  - `prev_1_completed`;
  - `prev_3_cancelled_count`;
  - `prev_7_cancelled_count`;
  - `prev_14_completed_rate`;
  - `prev_30_completed_rate`;
  - `same_month_past_completed_rate`;
  - `same_decade_past_completed_rate`;
  - `days_since_last_cancelled`;
  - streak-признаки.

Запуск:

```bash
python pipelines/training/build_training_dataset_v1.py --refresh-weather
```

Последняя локальная сборка:

```text
target_rows=759
target_date_min=2017-12-13
target_date_max=2026-05-20
target_status_counts={'completed': 411, 'cancelled': 348}
weather_cache_rows=1518
weather_coverage_by_location={'khomutovo': 759, 'mendeleyevo': 759}
weather_missing_dates_by_location={'mendeleyevo': 0, 'khomutovo': 0}
training_rows=759
training_columns=90
weather_feature_columns=58
```

Важное разделение:

- `dataset_daily_flights_v3.csv` — слой фактов и labels;
- `training_dataset_v1.csv` — слой признаков для ML;
- forecast monitor weather snapshot — слой оценки качества прогнозов в момент прогноза, не полноценный historical archive.

---

## 8. Следующие Data Experiments

Ближайшие полезные шаги:

1. Проверить корреляции погодных признаков с `is_flight_completed`.
2. Разделить train/validation/test по времени, без случайного shuffle.
3. Сравнить текущий `mvp-baseline-004` из `docs/baseline_model.md`, seasonal baseline и Logistic Regression.
4. Считать Brier score, ROC-AUC, PR-AUC и calibration curve.
5. Проверить, какие признаки реально помогают: погода Менделеево, погода Хомутово, rolling history, сезонность.
6. Отдельно посмотреть fog-паттерны и дни с ручным `reason_class=fog`.
7. Добавить сохранение версий training dataset и model card.
8. Не смешивать сомнительные `planned_only`, `unknown`, `delayed_without_final` в binary target.

Главный принцип:

> Fact dataset должен быть максимально честным и осторожным, а training dataset может быть богатым признаками, но без утечки будущего.
