# Летит на Курилы? / flyforecast.ru

`flyforecast` — ML-backed сервис для оценки вероятности выполнения авиарейса из/в аэропорт Менделеево на о. Кунашир.

Домен проекта: [`flyforecast.ru`](https://flyforecast.ru)  
Репозиторий: `Vladimir-Zimin226/flyforecast`

Сервис не является официальным источником статуса рейсов, не гарантирует выполнение рейса и не утверждает, что точно знает будущую погоду. Он даёт вероятностную оценку на основе исторических данных, сезонности, погодных факторов, baseline/ML-логики и объяснения для пользователя.

---

## Цель проекта

Главная задача проекта — помочь жителям Кунашира и другим пассажирам планировать выезд с острова с меньшей неопределённостью.

Сервис отвечает не на вопрос «точно ли полетит самолёт?», а на более честный вопрос:

> Насколько выбранная дата похожа на более или менее благоприятное окно для выполнения рейса?

Для MVP пользователь:

1. заходит на `flyforecast.ru`;
2. проходит временную тестовую аутентификацию;
3. выбирает дату от сегодня до +365 дней;
4. нажимает «Узнать вероятность вылета»;
5. получает:
   - ответ «Да» или «Нет»;
   - вероятность выполнения рейса;
   - уровень уверенности;
   - короткое объяснение;
   - дисклеймер;
6. после двух прогнозов видит CTA на Telegram-канал проекта.

---

## Текущий статус

На текущем этапе сделан первый работающий вертикальный MVP-срез:

- frontend на React/Vite;
- backend на FastAPI;
- запуск через Docker Compose;
- production-deploy на сервере;
- доступ через домен `https://flyforecast.ru`;
- HTTPS через Let's Encrypt / Certbot;
- reverse proxy через nginx;
- временная тестовая аутентификация;
- endpoint прогноза `/predict?date=YYYY-MM-DD`;
- подключение Open-Meteo forecast API;
- подключение OpenAI API для пользовательского объяснения;
- baseline-расчёт вероятности;
- логирование прогнозов в JSONL;
- карточка результата на frontend;
- CTA на Telegram после второго прогноза;
- тёмная UI-палитра;
- кастомный календарь выбора даты;
- backend-safe dataset на основе Telegram-разметки v2.
- hourly collector статуса рейсов Южно-Сахалинск/Южно-Курильск из табло аэропорта и погоды Open-Meteo.

Текущая модель/логика прогноза — не финальная ML-модель, а MVP baseline `mvp-baseline-001`. LLM не принимает решение о вероятности, а только формулирует объяснение уже рассчитанного результата.

---

## Данные

Данные проекта собираются слоями. Важно различать текущий backend-safe датасет,
сырые свидетельства из новых источников и будущий объединённый датасет.

### Текущий рабочий датасет

Первый рабочий исторический датасет был собран из Telegram-канала
«Аэропорт на Кунашире(НЕофициально)» / `t.me/aeroportuk`.

Текущий источник для backend:

```text
/data/processed/dataset_daily_flights.csv
```

Текущий backend-safe датасет:

- период: `2018-03-22` — `2026-04-07`;
- всего confirmed binary flight-days: `699`;
- `completed`: `381`;
- `cancelled`: `318`;
- доля отмен среди confirmed-наблюдений: около `45.5%`;
- data version: `telegram-rule-labels-v2-2026-05-03`.

Важно: это не официальная статистика аэропорта. Это предварительная автоматическая разметка Telegram-истории v2. После ручного аудита и labeler v3 цифры могут измениться.

### История источников

На момент разработки MVP использовались и проверялись такие источники:

- Telegram `t.me/aeroportuk`: основной исторический источник для backend-safe dataset v2. Даёт длинную историю, но является неофициальным источником и требует аудита правил разметки.
- Онлайн-табло аэропорта Южно-Сахалинска `https://airportus.ru/board/`: официальный текущий источник статуса рейсов. Используется hourly collector для сбора данных вперёд во времени.
- Архив Wayback Machine для `airportus.ru/board/` и `m.airportus.ru/board/`: архивные снимки официального табло, полезные для ретроспективных подтверждений статуса рейса.
- Официальные новости аэропорта `airportus.ru/news/post/{id}/`: источник отдельных подтверждённых отмен, задержек и изменений расписания. Если прямой запрос получает антибот-страницу, backfill-скрипт пробует Wayback-копию новости.
- Местные СМИ ASTV и Sakh.online: вторичный источник, часто ссылающийся на онлайн-табло аэропорта. Используется как дополнительный evidence layer, а не как единственный источник истины.
- TASS и Aviaport: точечные вторичные источники по отдельным сбоям/задержкам.
- Сайт авиакомпании Аврора `flyaurora.ru`: проверялся как потенциальный источник, но на практике не дал стабильного публичного табло статусов для серверного парсинга. Поэтому подключён только как best-effort источник в hourly collector через `AURORA_STATUS_URL`.

Текущая позиция по качеству данных: Telegram остаётся рабочим baseline-источником для backend, а официальные airportus/Wayback/news и media seed используются как слой подтверждений, который должен помочь собрать `dataset_daily_flights_v3.csv`.

### Hourly flight status collector

Новый сборщик данных находится в:

```text
pipelines/flight_status/collect_kunashir_status.py
```

Он каждый запуск:

- читает онлайн-табло аэропорта Южно-Сахалинска `https://airportus.ru/board/`;
- выбирает строки по городу `Южно-Курильск` на вылет и прилёт;
- пробует прочитать источник Авроры из `AURORA_STATUS_URL` (по умолчанию `https://www.flyaurora.ru/`; если сайт отдаёт антибот/не содержит табло, ошибка пишется отдельно);
- запрашивает текущую погоду Open-Meteo для координат аэропорта Менделеево;
- дописывает строки наблюдений в CSV.

Основной файл датасета:

```text
data/raw/flight_status/kunashir_flight_status_hourly.csv
```

Файл ошибок источников:

```text
data/raw/flight_status/collection_errors.csv
```

Ключевые колонки датасета:

- `observed_at`, `observation_date`, `observation_time`;
- `source`, `source_url`, `direction`;
- `flight_date`, `flight_time`, `flight_numbers`, `route`;
- `status_raw`, `status_normalized`, `reason`, `reason_class`;
- `scheduled_time_raw`, `actual_time_raw`;
- погодные признаки Open-Meteo: температура, влажность, давление, облачность, осадки, ветер, видимость.

Разовый локальный запуск:

```bash
python pipelines/flight_status/collect_kunashir_status.py
```

Непрерывный hourly-режим:

```bash
python pipelines/flight_status/collect_kunashir_status.py --loop --interval-seconds 3600
```

В Docker Compose добавлен сервис `collector`, который запускает этот hourly-режим вместе с проектом.

### Historical flight status backfill

Для ретроспективного поиска подтверждений по старым рейсам добавлен отдельный скрипт:

```text
pipelines/flight_status/backfill_historical_status.py
```

Он собирает не финальные labels, а сырые свидетельства из источников:

- Wayback snapshots онлайн-табло `airportus.ru/board/` и `m.airportus.ru/board/`;
- официальный архив новостей `airportus.ru/news/post/{id}/`;
- fallback на Wayback для новостей airportus, если прямой запрос получает антибот-страницу;
- seed-список статей ASTV, Sakh.online, TASS, Aviaport.

Основной выходной файл:

```text
data/raw/flight_status/kunashir_historical_sources.csv
```

Ошибки:

```text
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

Результат прогона `v2`:

- всего сырых свидетельств: `289`;
- `wayback_board`: `128`;
- `airportus_news`: `69`;
- `media_seed`: `92`;
- уникальных `source_url`: `91`;
- уникальных `flight_date`: `129`;
- уникальных event keys (`date`, `flight_numbers`, `direction`, `status`): `259`;
- ошибок сбора: `12`.

Распределение статусов в `kunashir_historical_sources_v2.csv`:

- `delayed`: `104`;
- `scheduled`: `90`;
- `cancelled`: `39`;
- `unknown`: `24`;
- `departed`: `14`;
- `combined`: `10`;
- `arrived`: `6`;
- `check_in`: `1`;
- `in_flight`: `1`.

Годы в `v2` по `flight_date`/`source_published_date`:

- `2017`: `2`;
- `2018`: `117`;
- `2019`: `16`;
- `2020`: `12`;
- `2021`: `4`;
- `2022`: `2`;
- `2023`: `2`;
- `2024`: `61`;
- `2025`: `63`;
- `2026`: `8`;
- `missing`: `2`.

Интерпретация:

- `cancelled`, `departed`, `arrived`, `in_flight`, `delayed`, `combined` — сильные или средние свидетельства о фактическом состоянии рейса;
- `scheduled` — не означает, что рейс был выполнен, это только факт присутствия в расписании/табло на момент снимка;
- `unknown` требует исключения или ручного аудита;
- `media_seed` полезен как дополнительный слой, но для конфликтных дней приоритет должен быть у `airportus_news` и `wayback_board`.

Быстрый smoke test:

```bash
python pipelines/flight_status/backfill_historical_status.py \
  --wayback \
  --include-mobile-wayback \
  --from-year 2024 \
  --to-year 2024 \
  --max-wayback-snapshots 1 \
  --max-snapshots-per-year 5
```

Полный запуск лучше делать без VPN:

```bash
python pipelines/flight_status/backfill_historical_status.py \
  --all \
  --from-year 2016 \
  --to-year 2026 \
  --from-post-id 2500 \
  --to-post-id 4300 \
  --concurrency 4 \
  --request-sleep-seconds 0.3
```

Если нужно сначала прогнать только официальный архив аэропорта:

```bash
python pipelines/flight_status/backfill_historical_status.py \
  --airportus-news \
  --from-post-id 2500 \
  --to-post-id 4300
```

Если нужно только Wayback-табло:

```bash
python pipelines/flight_status/backfill_historical_status.py \
  --wayback \
  --include-mobile-wayback \
  --from-year 2016 \
  --to-year 2026
```

### План объединения датасетов

Новые historical sources пока не заменяют рабочий `dataset_daily_flights.csv`.
Следующий корректный шаг — построить промежуточную агрегацию:

```text
data/raw/flight_status/kunashir_historical_sources_v2.csv
        ↓
data/interim/flight_status/historical_daily_labels.csv
```

Предлагаемые правила агрегации:

- `departed`, `arrived`, `in_flight` -> `completed`;
- `cancelled` -> `cancelled`;
- `delayed` -> `delayed`;
- `combined` -> `disrupted`;
- `scheduled`, `check_in` -> `planned_only`;
- `unknown` -> `needs_review` или exclude.

После этого нужно сравнить `historical_daily_labels.csv` с Telegram daily labels и вынести конфликтные дни в ручной аудит:

- Telegram говорит `completed`, а официальный/архивный источник говорит `cancelled`;
- Telegram говорит `cancelled`, а Wayback/табло говорит `departed` или `arrived`;
- есть `delayed`/`combined`, но нет финального исхода;
- несколько источников дают разные статусы на один день.

Только после этого стоит собирать новый backend-safe датасет:

```text
telegram labels + confirmed historical labels + manual review fixes
        ↓
data/processed/dataset_daily_flights_v3.csv
```

Backend нужно переключать на v3 только после проверки конфликтов и фиксации новой `data_version`.

---

## Структура проекта

```text
flyforecast/
├── README.md
├── .gitignore
├── .env.example
├── docker-compose.yml
├── docker-compose.prod.yml
├── backend/
├── frontend/
├── data/
└── docs/
```

## Backend

Backend находится в `backend/`.

```text
backend/
├── Dockerfile
├── requirements.txt
└── app/
    ├── __init__.py
    ├── main.py
    ├── config.py
    ├── auth.py
    ├── schemas.py
    └── services/
        ├── weather.py
        ├── history.py
        ├── predictor.py
        └── llm.py
```

### Основные файлы backend

- `backend/Dockerfile`  
  Docker image для FastAPI backend. Устанавливает зависимости и запускает `uvicorn app.main:app`.

- `backend/requirements.txt`  
  Python-зависимости backend: FastAPI, Uvicorn, httpx, Pydantic, OpenAI SDK и др.

- `backend/app/main.py`  
  Точка входа FastAPI. Содержит endpoints:
  - `GET /health`;
  - `POST /auth/login`;
  - `GET /predict?date=YYYY-MM-DD`.

  Также валидирует дату, вызывает погодный сервис, historical snapshot, predictor, LLM explanation и пишет prediction log.

- `backend/app/config.py`  
  Читает настройки из env:
  - `APP_ENV`;
  - `BACKEND_CORS_ORIGINS`;
  - `TEST_USERNAME`;
  - `TEST_PASSWORD`;
  - `JWT_SECRET`;
  - `OPENAI_API_KEY`;
  - `OPENAI_MODEL`;
  - `FLYFORECAST_DATASET_PATH`;
  - `PREDICTION_LOG_PATH`;
  - координаты аэропорта;
  - timezone.

- `backend/app/auth.py`  
  Временная тестовая аутентификация для закрытого MVP. Не является production-grade auth.

- `backend/app/schemas.py`  
  Pydantic-схемы API: login, weather snapshot, historical snapshot, predict response.

- `backend/app/services/weather.py`  
  Интеграция с Open-Meteo forecast API. Для дальних дат возвращает `available=false`.

- `backend/app/services/history.py`  
  Читает `dataset_daily_flights.csv`, использует `completed` и `cancelled`, считает исторические вероятности по похожим датам/периодам.

- `backend/app/services/predictor.py`  
  MVP baseline-логика: horizon, confidence, weather adjustment, probability, threshold, decision, factors.

- `backend/app/services/llm.py`  
  Генерирует короткое объяснение через OpenAI API. Не получает сырые Telegram-сообщения и не принимает решение о вероятности.

---

## Frontend

Frontend находится в `frontend/`.

```text
frontend/
├── Dockerfile
├── Dockerfile.prod
├── package.json
├── index.html
├── nginx.conf
└── src/
    ├── main.jsx
    ├── App.jsx
    └── styles.css
```

### Основные файлы frontend

- `frontend/Dockerfile`  
  Dev image для Vite dev server.

- `frontend/Dockerfile.prod`  
  Production image: собирает React/Vite build и отдаёт static-файлы через nginx.

- `frontend/nginx.conf`  
  Конфиг nginx внутри frontend-контейнера для SPA fallback.

- `frontend/src/main.jsx`  
  React entrypoint.

- `frontend/src/App.jsx`  
  Основная логика интерфейса:
  - экран логина;
  - хранение token в `localStorage`;
  - календарь;
  - запрос `/predict`;
  - карточка результата;
  - CTA на Telegram после второго прогноза.

- `frontend/src/styles.css`  
  Стили приложения: тёмная палитра, layout, карточки, календарь, кнопки, mobile adaptation.

---



## Docs

```text
docs/
├── business_analysis.md
├── prototype.md
├── adr/
└── experiments/
```

- `docs/business_analysis.md`  
  Бизнес-анализ MVP: пользователи, проблема, value, конкурирующие решения, CRISP-DM, ML-метрики.

- `docs/prototype.md`  
  Описание продуктового прототипа, сценария пользователя, формата результата, CTA и критериев тестирования.

---

## Локальный запуск в dev-режиме

### 1. Склонировать репозиторий

```bash
git clone https://github.com/Vladimir-Zimin226/flyforecast.git
cd flyforecast
```

### 2. Создать `.env`

```bash
cp .env.example .env
```

Заполнить значения:

```env
APP_ENV=development
BACKEND_CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173

TEST_USERNAME=demo
TEST_PASSWORD=demo
JWT_SECRET=change-me

OPENAI_API_KEY=your_openai_api_key
OPENAI_MODEL=gpt-4.1-mini

FLYFORECAST_DATASET_PATH=/app/data/processed/dataset_daily_flights.csv
PREDICTION_LOG_PATH=/app/data/interim/prediction_logs.jsonl

AIRPORT_LATITUDE=43.958
AIRPORT_LONGITUDE=145.683
AIRPORT_TIMEZONE=Asia/Sakhalin
```

### 3. Подготовить dataset

Положить файл:

```text
data/processed/dataset_daily_flights.csv
```

Минимальный формат:

```csv
date,status
2024-01-01,completed
2024-01-02,cancelled
```

### 4. Запустить

```bash
docker compose up -d --build
```

Dev-адреса:

```text
Frontend: http://localhost:5173
Backend:  http://localhost:8000
Health:   http://localhost:8000/health
```

---

## API

### Health

```http
GET /health
```

Ответ:

```json
{"status":"ok"}
```

### Login

```http
POST /auth/login
```

Через production nginx:

```http
POST /api/auth/login
```

Пример:

```bash
curl -X POST https://flyforecast.ru/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"demo","password":"your-password"}'
```

### Predict

```http
GET /predict?date=YYYY-MM-DD
```

Через production nginx:

```http
GET /api/predict?date=YYYY-MM-DD&session_prediction_number=1
```

Пример:

```bash
TOKEN="your-access-token"

curl "https://flyforecast.ru/api/predict?date=2026-06-01&session_prediction_number=1" \
  -H "Authorization: Bearer $TOKEN"
```

Ожидаемый ответ содержит:

- `date`;
- `decision`;
- `probability_flight`;
- `confidence`;
- `horizon_days`;
- `explanation`;
- `model_version`;
- `data_version`;
- `disclaimer`.

---

## Ближайшие задачи

1. Завершить Data Understanding:
   - ручной audit labels;
   - labeler v3;
   - фиксация правил разметки.

2. Улучшить dataset:
   - собрать финальный `dataset_daily_flights.csv`;
   - исключить сомнительные статусы из target;
   - добавить `data_version`;
   - подготовить training dataset с weather + calendar features.

3. Добавить ML:
   - seasonal baseline;
   - time-based train/validation/test;
   - Logistic Regression;
   - Brier Score;
   - calibration curve;
   - сравнение с seasonal baseline.

4. Улучшить backend:
   - аккуратная обработка малых выборок;
   - tests для `/health`, `/auth/login`, `/predict`;
   - более прозрачные confidence/threshold rules.

5. Улучшить frontend:
   - страница «Как это работает»;
   - объяснение ограничений;
   - сравнение соседних дат;
   - аккуратная аналитика CTA.

---

## Главная мысль

Мы не пытаемся идеально предсказать погоду или решение авиакомпании.

Мы делаем честный вероятностный сервис, который помогает людям получить более полезный ориентир, чем «ничего неизвестно»:

- какие даты выглядят лучше;
- какие окна исторически хуже;
- когда риск выше;
- когда риск ниже;
- насколько можно доверять оценке на выбранном горизонте.
