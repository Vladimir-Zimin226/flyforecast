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
python pipelines/flight_status/collect_kunashir_status.py --loop --interval-seconds 900
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
python pipelines/evaluation/forecast_monitor.py --loop --interval-seconds 900
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

---

## 9. Weather Pattern Search After June 2026 Errors

Дата разбора: `2026-06-09`.

Контекст:

- сервис начал заметно ошибаться на свежих днях июня;
- особенно важны кейсы `2026-06-03` / `2026-06-04` / `2026-06-08` / `2026-06-09`;
- цель разбора — не "подогнать" прогноз под известные факты, а найти погодные режимы, которые можно честно использовать в predictor.

Использованные данные из server export:

```text
data/interim/analysis/export_20260609_121758/processed/training_dataset_v1.csv
data/interim/analysis/export_20260609_121758/processed/mendeleyevo_fog_risk_dataset.csv
data/interim/analysis/export_20260609_121758/open_meteo_daily_2026-05-23_2026-06-09.csv
data/interim/analysis/export_20260609_121758/open_meteo_hourly_2026-05-23_2026-06-09.csv
data/interim/analysis/export_20260609_121758/forecast_vs_fact_joined_2026-05-23_2026-06-09.csv
```

База для primary weather-pattern анализа:

- rows: `761`;
- period: `2017-12-13` -> `2026-05-26`;
- `completed`: `411`;
- `cancelled`: `350`;
- always-completed baseline: около `54%`.

Важное ограничение:

- проблемные дни `2026-06-03` -> `2026-06-09` не входят в `training_dataset_v1.csv`;
- для них отдельно смотрелись monitor snapshots и фактическая/почасовая погода из export;
- это снижает риск прямого подгона правил под июньские ошибки.

### 9.1. Fog / visibility signal exists, but is not deterministic

Погодная закономерность по туману и видимости есть, но она вероятностная.

По Менделеево:

- `fog_low_cloud_risk_level=high`: отмена `60.8%`;
- `fog_low_cloud_risk_level=medium`: отмена `49.7%`;
- `fog_low_cloud_risk_level=low`: отмена `29.6%`;
- `visibility_mean <= 3000 м`: отмена `72.1%`;
- `visibility_min <= 300 м`: отмена `61.8%`, но при этом `92` выполненных рейса против `149` отмен.

Интерпретация:

- низкая видимость и высокий fog risk — сильные отрицательные признаки;
- но одно правило вида "видимость <= N -> отмена" будет давать много ложных `no`;
- нужен режимный прогноз, где туман комбинируется с ветром, давлением, осадками и облачностью.

### 9.2. Human-suggested wind direction hypothesis

Гипотезу про направление и силу ветра предложил человек, Владимир:

> Возможно, если есть туман, но ветер восточный и достаточно сильный, аэропорт на горе раскрывается; а при другом направлении наоборот.

Проверка показала, что сама идея смотреть направление ветра очень полезная, но конкретная гипотеза про "сильный восточный ветер спасает" в текущих данных не подтвердилась.

Менделеево, направление ветра overall:

- `SW`: отмена `21%`, самый "летный" сектор;
- `N`: отмена `41%`;
- `W` / `NW`: около `45%`;
- `E` / `SE` / `S` / `NE`: около `49-55%`.

Комбинации:

- `E/SE + wind >= 35 км/ч`: отмена `81.5%` (`22` отмены / `5` выполнений);
- `E/SE + pressure_min <= 1005 + wind >= 25 км/ч`: отмена `81.8%`;
- `E/SE + visibility_min <= 1000 + pressure_min <= 1005`: отмена `82.1%`;
- `SW/W + gust < 60 + precip <= 2`: выполнение `84.6%`, но выборка маленькая (`13` дней).

Вывод:

- направление ветра стоит добавить в `WeatherSnapshot` и predictor;
- `SW`/`W` могут быть слабым положительным фактором при отсутствии других рисков;
- `E/SE` при сильном ветре, низком давлении или низкой видимости — сильный отрицательный режим;
- важна именно комбинация, а не направление само по себе.

### 9.3. Human-suggested pressure hypothesis

Гипотезу про давление также предложил человек, Владимир.

На момент разбора pressure есть в погодном snapshot, но почти не используется в решении:

- `WeatherSnapshot.pressure_msl` есть;
- `backend/app/services/weather.py` агрегирует pressure;
- `backend/app/services/predictor.py` не дает самостоятельного штрафа/бонуса по давлению.

Найденные связи по Менделеево:

- `pressure_min <= 995`: отмена `69.2%`;
- `995 < pressure_min <= 1000`: отмена `47.5%`;
- `1000 < pressure_min <= 1005`: отмена `53.5%`;
- `1005 < pressure_min <= 1010`: отмена `44.1%`;
- `1010 < pressure_min <= 1015`: отмена `41.5%`;
- `pressure_min > 1015`: отмена `33.3%`, выполнение `66.7%`.

Полезные комбинации:

- `pressure <= 1000 + wind >= 25`: отмена `63.9%`;
- `pressure <= 1000 + precip > 2`: отмена `66.3%`;
- `pressure <= 1000 + low_cloud >= 75`: отмена `70.4%`;
- `pressure <= 1005 + fog_high + wind >= 25`: отмена `72.2%`;
- `pressure <= 1005 + fog_high + precip > 2`: отмена `71.8%`;
- `pressure > 1015 + fog_low + gust < 60`: выполнение `84.3%`.

Вывод:

- давление не является одиночной "кнопкой отмены";
- низкое давление усиливает плохие режимы с туманом, низкой облачностью, ветром и осадками;
- высокое давление в сочетании с low fog и слабыми порывами дает хороший режим для `yes`.

### 9.4. Humidity + wind / dew point spread

Влажность сама по себе слабее, чем хотелось бы. Но сочетание влажности и ветра заметно сильнее.

Менделеево:

- `humidity < 85 + wind < 20`: выполнение `78.9%`;
- `humidity >= 92 + wind < 20`: отмена `40.6%`;
- `humidity >= 92 + wind >= 25`: отмена `69.0%`;
- `humidity >= 95 + wind >= 25`: отмена `70.2%`;
- `humidity >= 92 + gust >= 72`: отмена `68.0%`.

Интерпретация:

- высокая влажность без сильного ветра не всегда плохой признак;
- высокая влажность + сильный ветер/порывы похожи на режим, где аэропорт чаще не принимает рейс;
- текущий predictor уже учитывает humidity и dew point spread, но слишком независимо от ветра.

### 9.5. "Good weather" regimes

Найдены режимы, где можно увереннее говорить `yes`.

Примеры:

- `fog_low + gust < 60`: выполнение `80.7%`, выборка `109`;
- `fog_low + low_cloud < 50 + gust < 60`: выполнение `83.1%`;
- `pressure > 1015 + fog_low + gust < 60`: выполнение `84.3%`;
- `humidity < 85 + wind < 20`: выполнение `78.9%`;
- `SW overall`: выполнение `78.9%`.

Эти признаки стоит использовать как положительные поправки, но аккуратно:

- не превращать любой `SW` в автоматическое `yes`;
- не давать "хорошему" режиму перекрывать уже известный статус табло;
- учитывать, что часть выборок маленькая.

### 9.6. Weather-only ceiling check

Чтобы не обмануть себя точечными правилами, был сделан простой sanity-check:

- только погодные признаки;
- простое decision-tree-like разбиение;
- проверка по отложенным годам.

Результат:

- отдельные weather regimes дают `80%+` precision на подвыборках;
- но weather-only модель на всех днях не дает `80%` качества;
- leave-one-year-out weighted accuracy около `58%`;
- chronological split `train<=2023`, `test>=2024` ломается из-за сильного сдвига базовой частоты отмен.

Интерпретация:

- 80% на всех днях только погодой из текущих daily aggregates пока не подтверждается;
- 80% достижимы для "уверенных погодных режимов";
- оставшаяся серая зона требует board status, расписания, свежих forecast snapshots и, возможно, более точной погодной точки/источника.

### 9.7. Practical predictor implications

Кандидаты на следующие изменения в predictor:

1. Добавить `wind_direction_10m` в `WeatherSnapshot`, forecast monitor и prediction logs.
2. Добавить давление как явный фактор:
   - отрицательный режим при `pressure_msl` низком, особенно вместе с fog/high low cloud/precip/wind;
   - положительный режим при высоком pressure + low fog + слабые порывы.
3. Перейти от независимых штрафов к interaction rules:
   - humidity + wind/gust;
   - pressure + fog + wind;
   - pressure + fog + precipitation;
   - E/SE + wind;
   - SW/W + weak gust + low precipitation.
4. В UI/объяснениях разделять прогноз на:
   - weather-confident yes;
   - weather-confident no;
   - gray zone, где нужен статус табло и оперативный мониторинг.
5. Не обещать "точность 80%" глобально, пока она не подтверждена leakage-safe backtest.

Принцип:

> Хорошие идеи с ветром, направлением ветра и давлением пришли от человека; задача модели и анализа — не заменить эту интуицию, а честно проверить ее на данных и превратить устойчивые находки в аккуратные признаки.

### 9.8. Counterfactual: weather-only baseline without flight history

Гипотеза:

- проверить, что будет, если на периоде запуска сервиса убрать из прогноза историческую базу вылетов/отмен;
- оставить погодную поправку из сохраненных forecast snapshots;
- вместо history base поставить нейтральную точку `0.50`.

Период проверки:

- run dates: `2026-05-23` -> `2026-06-09`;
- target dates: `2026-05-23` -> `2026-06-09`;
- horizons: `0` -> `15`;
- evaluated rows: `228`;
- facts by corrected same-day board logic: `151 completed`, `77 cancelled`.

Метод:

```text
logged_probability = history_base + weather_adjustment
history_base = 0.65 * historical_probability + 0.35 * decade_probability
weather_adjustment = logged_probability - history_base
weather_only_probability = 0.50 + weather_adjustment
```

Итог:

```text
with history:
  accuracy: 35.1%
  yes: 57
  no: 171
  false_yes: 27
  false_no: 121
  brier: 0.303
  avg_probability: 0.446

weather-only neutral 0.50:
  accuracy: 33.3%
  yes: 25
  no: 203
  false_yes: 13
  false_no: 139
  brier: 0.3395
  avg_probability: 0.369
```

Вывод:

- история не просто "портит" прогноз и тянет к `yes`;
- история сейчас является стартовой точкой вероятности и часто спасает от чрезмерно пессимистичного weather-only прогноза;
- без истории модель почти перестает говорить `yes`, снижает false yes, но сильно увеличивает false no;
- значит, историю полностью убирать нельзя;
- правильнее ослаблять history base только в сильных погодных bad regimes и при strong board/schedule evidence.

Same-day июньские примеры:

```text
2026-06-03 completed:
  with_history=0.380 no
  weather_only=0.315 no

2026-06-04 completed:
  with_history=0.599 yes
  weather_only=0.540 no

2026-06-05 completed:
  with_history=0.611 yes
  weather_only=0.545 no

2026-06-06 cancelled:
  with_history=0.614 yes
  weather_only=0.545 no

2026-06-08 cancelled:
  with_history=0.663 yes
  weather_only=0.595 yes

2026-06-09 cancelled:
  with_history=0.655 yes
  weather_only=0.575 no
```

Интерпретация:

- на `2026-06-06` и `2026-06-09` weather-only был бы лучше;
- на `2026-06-04` и `2026-06-05` weather-only сломал бы правильный `yes`;
- проблема не в самом факте истории, а в том, что history base и weather adjustment сейчас складываются слишком линейно;
- следующая версия predictor должна использовать history как prior, но позволять сильным погодным режимам и табло сильнее менять posterior.

### 9.9. Experimental 0-100 weather-only operability score

Следующая проверка была сделана после уточнения идеи: нужна не поправка к вероятности, а самостоятельный скоринг `0-99/100`, где `score >= threshold` означает `completed`, а `score < threshold` означает `cancelled`.

Для этого добавлен отдельный экспериментальный скрипт:

```text
pipelines/evaluation/weather_only_score_experiment.py
```

Важно:

- скрипт не трогает production predictor;
- это исследовательский weather-only score;
- он использует почасовую погоду Open-Meteo из export за сам день, поэтому это не честный forecast-backtest на момент прогноза, а проверка способности правил описать фактический погодный режим;
- история вылетов/отмен не используется;
- если в board есть расписание вылетов на дату, окно берется вокруг него; иначе fallback window `08:00-20:00`;
- для даты выбирается лучшее двухчасовое погодное окно внутри flight window.

Компоненты score:

- видимость;
- низкая облачность;
- влажность + разница температуры и точки росы;
- скорость ветра и порывы;
- направление ветра;
- осадки;
- давление;
- weather code.

Команда:

```bash
python pipelines/evaluation/weather_only_score_experiment.py
```

Период:

- `2026-05-23` -> `2026-06-09`;
- evaluated days: `18`;
- факты: `12 completed`, `6 cancelled`.

Результат при threshold `50`:

```text
accuracy: 66.7%
yes: 16
no: 2
false_yes: 5
false_no: 1
```

Daily scores:

```text
2026-05-23 completed score=95.0 yes
2026-05-24 completed score=100.0 yes
2026-05-25 cancelled score=70.5 yes
2026-05-26 cancelled score=75.5 yes
2026-05-27 cancelled score=78.0 yes
2026-05-28 completed score=40.0 no
2026-05-29 completed score=67.0 yes
2026-05-30 completed score=83.0 yes
2026-05-31 completed score=96.0 yes
2026-06-01 completed score=92.5 yes
2026-06-02 completed score=92.0 yes
2026-06-03 completed score=66.5 yes
2026-06-04 completed score=76.0 yes
2026-06-05 completed score=68.0 yes
2026-06-06 cancelled score=76.5 yes
2026-06-07 completed score=79.0 yes
2026-06-08 cancelled score=84.5 yes
2026-06-09 cancelled score=39.0 no
```

Threshold sweep:

```text
threshold  accuracy  yes  false_yes  false_no
40         72.2%     17   5          0
50         66.7%     16   5          1
60         66.7%     16   5          1
70         50.0%     13   5          4
80         61.1%     7    1          6
85         61.1%     5    0          7
90         61.1%     5    0          7
```

Интерпретация:

- самостоятельный погодный score имеет смысл как исследовательская форма;
- он лучше отражает идею "есть ли летное окно по погоде", чем текущая `weather_adjustment`;
- при низком пороге score ловит почти все реальные вылеты, включая `2026-06-03`;
- но он дает много ложных `yes` на отменах `2026-05-25`, `2026-05-26`, `2026-05-27`, `2026-06-06`, `2026-06-08`;
- это значит, что часть отмен выглядит не объяснимой текущей фактической погодой Open-Meteo в найденном окне;
- для production такой score нельзя использовать один, но его можно использовать как новый weather module, который затем корректируется расписанием, табло и strong bad-regime rules.

### 9.10. Historical daily backtest of weather-only score

После локального успеха на коротком периоде `2026-05-23` -> `2026-06-09` был сделан исторический sanity-check.

Ограничение:

- полного исторического hourly-файла за `2017-2026` в repo нет;
- доступен `training_dataset_v1.csv` с daily weather aggregates;
- поэтому это не полноценный hourly-window backtest, а daily approximation of weather-only score.

Добавлен скрипт:

```text
pipelines/evaluation/weather_only_daily_backtest.py
```

Команда:

```bash
python pipelines/evaluation/weather_only_daily_backtest.py --use-veto
```

Данные:

- rows: `761`;
- `completed`: `411`;
- `cancelled`: `350`;
- score использует только daily-погоду Менделеево;
- история вылетов/отмен не используется.

Лучший full-sample результат:

```text
threshold=49
use_veto=True
accuracy=64.0%
yes=447
no=314
false_yes=155
false_no=119
avg_score=56.6
```

По годам для best full-sample:

```text
2018 accuracy=67.0%
2019 accuracy=61.1%
2020 accuracy=56.3%
2021 accuracy=71.6%
2022 accuracy=56.2%
2023 accuracy=61.0%
2024 accuracy=68.5%
2025 accuracy=58.5%
2026 accuracy=70.0%
```

Leave-one-year-out threshold tuning:

```text
weighted_accuracy=57.0%
false_yes=170
false_no=149
```

Score bands show useful ranking:

```text
score 0-30:    completed_rate=26.8%
score 30-40:   completed_rate=30.5%
score 40-50:   completed_rate=45.2%
score 50-60:   completed_rate=54.2%
score 60-70:   completed_rate=61.0%
score 70-80:   completed_rate=68.7%
score 80-90:   completed_rate=72.5%
score 90-101:  completed_rate=87.5%
```

Вывод:

- weather-only score действительно ранжирует дни: чем выше score, тем выше completion rate;
- но как бинарный прогноз на всей истории он не дает `80%`;
- июньский результат `83.3%` был локальным и не должен считаться доказанной точностью;
- score полезен как weather operability module / calibration feature;
- для качества сервиса нужна комбинация: weather score + history prior + schedule/board evidence + отдельные strong bad-regime rules.

### 9.11. Production rule update after short-window forensic review

После разбора ошибок на периоде запуска сервиса принято рабочее правило качества:

- главным quality-window для ближайшего релиза считать период, где у нас есть полная связка `forecast snapshot + hourly board + факт рейса`, сейчас это `2026-05-23` -> `2026-06-09`;
- historical daily backtest без честной почасовой погоды не считать доказательством качества;
- если делать исторический backtest, он должен быть сопоставимым: например `23 мая -> 9 июня` для прошлых лет, с почасовым прогнозом/погодой и точно известным фактом вылета;
- нельзя без проверки применять одни и те же правила к июню, октябрю и другим сезонным режимам;
- возможно, в будущем нужны месячные или недельные weather-score profiles, если мониторинг накопит достаточно фактов.

Разбор трех спорных completed-days:

```text
2026-05-28 completed:
  Open-Meteo давал очень плохой режим: visibility около 120 м, низкая облачность 100%, высокая влажность.
  Честное weather-only правило должно было сказать no.
  Если рейс реально улетел, текущая погодная точка не описала условия аэродрома или решение экипажа/аэропорта.

2026-05-29 completed:
  Утро плохое: visibility 520 м в районе планового вылета, pressure около 986 гПа.
  Позже есть резкое улучшение видимости.
  Это кандидат на late-clearing rule, но правило рискованное и требует дальнейшей проверки.

2026-06-03 completed:
  Табло показывает departure около 10:28.
  Open-Meteo в 10-11 видел visibility около 560/460 м.
  Это также выглядит как mismatch погодной точки с реальностью; честно исправить только weather rule нельзя.
```

Разбор отмен:

```text
2026-06-06 cancelled:
  Open-Meteo выглядит летным: visibility 11-23 км, pressure >1024 гПа, осадки 0.
  Weather-only честно предсказывает completed.
  Этот день должен ловиться board/schedule monitor, а не погодным правилом.

2026-06-08 cancelled / moved next day:
  Днем есть хорошее окно, но после планового времени видимость падает, влажность растет до 97-99%, осадки усиливаются.
  Тут честное правило humidity + companion risk должно давать no/strong caution.
```

Внедренные изменения в `mvp-baseline-009`:

- `wind_direction_10m` добавлен в live `WeatherSnapshot`, prediction logs и forecast monitor ledger;
- direction агрегируется круговым средним, чтобы направления около `360/0` не ломали среднее;
- humidity больше не является самостоятельным veto-признаком;
- high humidity штрафует сильнее только в связке с companion risk:
  - низкая видимость;
  - осадки;
  - низкое давление;
  - закрытая точка росы + высокий fog/low-cloud risk;
  - низкая облачность с заметным ветром;
- pressure стал явным фактором:
  - низкое давление усиливает отрицательное решение;
  - высокое давление поддерживает найденное летное окно;
- wind direction стал явным factor:
  - `E/SE` при заметном ветре/порывах усиливает риск;
  - `SW/W` при слабых порывах и без осадков дает небольшой плюс;
- добавлен `good_late_clearing_support`: хорошая видимость в найденном окне, нормальное/высокое давление, слабые осадки и умеренные порывы могут удерживать прогноз от чрезмерного pessimistic no.

Короткий эксперимент после relaxed-veto:

```bash
python pipelines/evaluation/weather_only_score_experiment.py --threshold 66 --use-relaxed-veto
```

Результат:

```text
evaluated_days=18
accuracy=77.8%
yes=10
no=8
false_yes=1
false_no=3
```

Ошибки:

- false yes: `2026-06-06` — погода выглядит летной, нужен board/schedule guardrail;
- false no: `2026-05-28`, `2026-05-29`, `2026-06-03` — погода в Open-Meteo выглядит хуже факта, особенно в районе реального вылета.

Вывод:

- честное weather-only правило на текущей погодной точке не дает стабильные `80%+` на short-window;
- связка production predictor + hourly board monitor может быть лучше weather-only, потому что board ловит переносы/совмещения, которые погода не объясняет;
- дальнейшее улучшение нужно строить на постоянном мониторинге forecast snapshots, фактических статусов и сезонных/месячных профилей, а не на daily historical aggregates.

### 9.12. Accuracy improvement plan and airport-grade weather search

Цель:

- не обещать невозможные `100%` прогнозы заранее;
- повысить perceived quality за счет разделения прогнозов на `уверенное yes`, `уверенное no` и `серую зону`;
- приблизиться к `90%` в оперативном качестве сервиса за счет табло, свежих погодных snapshot и более точного источника погоды аэропорта.

План улучшений:

1. Для текущего/следующего дня считать главным источником сильного факта airport board:
   - перенос на следующую дату;
   - совмещение;
   - отмена;
   - фактический вылет/прилет.
2. Чаще опрашивать табло в день рейса, особенно утром и за несколько часов до планового вылета.
3. Ввести продуктовую серую зону вместо принудительного бинарного угадывания:
   - `уверенное yes`;
   - `уверенное no`;
   - `риск высокий, следите за табло`.
4. Считать качество отдельно:
   - оперативные прогнозы `0-1` день;
   - ближние погодные прогнозы `2-15` дней;
   - дальние historical/climate прогнозы.
5. Считать false yes отдельно как самый болезненный тип ошибки: сервис сказал `yes`, а пользователь мог зря поехать в аэропорт.
6. Постоянно сохранять forecast snapshots и board snapshots, чтобы будущий backtest был честным и не использовал данные, которых на момент прогноза еще не было.
7. Проверять сезонные/месячные правила отдельно, потому что июньский туман, осенний ветер и зимние условия могут быть разными режимами.

Airport-grade weather hypothesis:

- лучший потенциальный источник — авиационная погода самого аэропорта Менделеево;
- правильный код аэропорта: `UHSM`;
- нужные форматы:
  - `METAR` / `SPECI` — фактическая аэродромная погода;
  - `TAF` — аэродромный прогноз;
  - потенциально `ATIS` / локальная диспетчерская сводка, если она где-то публикуется.

Проверка публичных источников на `2026-06-09`:

- публичные агрегаторы знают аэропорт `UHSM`, но не показывают собственный METAR/TAF Менделеево;
- часть сервисов подставляет ближайший METAR/TAF из `RJCK` Kushiro, примерно `85 nm`, это не подходит как точная погода Менделеево;
- FlightAware и Bigorre явно показывали отсутствие METAR/TAF для `UHSM`;
- NOAA AviationWeather API стоит проверить программно для `UHSM`, но по публичным агрегаторам похоже, что станция не попадает в глобальный открытый METAR/TAF feed.

Практические следующие шаги:

1. Добавить исследовательский скрипт проверки авиационных источников:
   - `aviationweather.gov/api/data/metar?ids=UHSM`;
   - `aviationweather.gov/api/data/taf?ids=UHSM`;
   - CheckWX / metar.cloud / FlightAware только как diagnostic mirrors, не как основной источник.
2. Если `UHSM` пустой, искать не агрегаторы, а первичный источник:
   - Росгидромет / Авиаметтелеком;
   - аэропорт Южно-Курильск / Менделеево;
   - авиакомпания/оператор рейса;
   - локальный ATIS/диспетчерская публикация, если есть официальный или полуофициальный доступ.
3. Если официального публичного METAR/TAF нет:
   - оставить Open-Meteo как baseline;
   - добавить ближайшие реальные станции как дополнительные признаки, но не выдавать их за погоду аэропорта;
   - продолжить накапливать фактические board outcomes, чтобы скорректировать Open-Meteo bias.

Реализация:

- добавлен скрипт `pipelines/weather/check_airport_weather_sources.py`;
- команда:

```bash
python3 pipelines/weather/check_airport_weather_sources.py --icao UHSM
```

Локальная проверка в Codex/WSL среде:

- без внешней сети: `Temporary failure in name resolution`;
- с escalated network access через `curl --max-time 12`: `Resolving timed out`;
- значит из текущей среды нельзя подтвердить содержимое AviationWeather API, но скрипт готов для запуска на сервере или локально в обычной сети.
