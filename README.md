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
5. получает ответ, вероятность, уверенность, объяснение и дисклеймер;
6. после двух прогнозов видит CTA на Telegram-канал проекта.

---

## Текущий статус

Сделан первый работающий vertical slice:

- frontend на React/Vite;
- backend на FastAPI;
- Docker Compose для dev/prod;
- production deploy на сервере;
- доступ через `https://flyforecast.ru`;
- HTTPS через Let's Encrypt / Certbot;
- reverse proxy через nginx;
- временная тестовая аутентификация;
- endpoint прогноза `/predict?date=YYYY-MM-DD`;
- Open-Meteo forecast API;
- GigaChat API для пользовательского объяснения;
- baseline-расчёт вероятности `mvp-baseline-001`;
- логирование прогнозов в JSONL;
- hourly collector статусов рейсов по табло аэропорта;
- forecast monitor для ledger прогнозов, фактов и метрик качества;
- рабочий v3 dataset с ручной проверкой и board evidence.

LLM не принимает решение о вероятности, а только формулирует объяснение уже рассчитанного результата.

---

## Данные

Подробная история работы с данными вынесена в отдельный документ:

```text
docs/data_experiments.md
```

Там зафиксированы:

- первый Telegram dataset;
- historical backfill из Wayback, airportus news и media seed;
- hourly collector онлайн-табло;
- сборка `dataset_daily_flights_v3.csv`;
- ручной review спорных вылетов/невылетов;
- forecast monitor;
- `training_dataset_v1.csv` с погодой Open-Meteo Archive, календарём и rolling-признаками.

Текущий production dataset задаётся через:

```env
FLYFORECAST_DATASET_PATH=/app/data/processed/dataset_daily_flights_v3.csv
```

Важно: данные в `data/` считаются рабочими/ценными и не обязаны коммититься в публичный репозиторий.

Описание текущей прогнозной логики закреплено отдельно:

```text
docs/baseline_model.md
```

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
├── pipelines/
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

Основные части:

- `backend/app/main.py` — FastAPI endpoints: `/health`, `/auth/login`, `/predict`.
- `backend/app/config.py` — env-настройки проекта.
- `backend/app/services/weather.py` — Open-Meteo forecast API.
- `backend/app/services/history.py` — historical snapshot из dataset.
- `backend/app/services/predictor.py` — MVP baseline probability/decision/confidence.
- `backend/app/services/llm.py` — объяснение результата через GigaChat.

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

Основные части:

- `frontend/src/App.jsx` — логин, календарь, запрос прогноза, карточка результата, CTA.
- `frontend/src/styles.css` — тёмная палитра, layout, календарь, mobile adaptation.
- `frontend/nginx.conf` — SPA fallback для production frontend.

## Pipelines

Основные data/evaluation scripts:

- `pipelines/flight_status/collect_kunashir_status.py` — hourly collector табло.
- `pipelines/flight_status/backfill_historical_status.py` — ретроспективный сбор evidence.
- `pipelines/flight_status/build_dataset_v3.py` — сборка v3 fact dataset.
- `pipelines/evaluation/forecast_monitor.py` — ledger прогнозов и оценка качества.
- `pipelines/training/build_training_dataset_v1.py` — weather-enriched training dataset.

Подробнее см. `docs/data_experiments.md`.

## Docs

```text
docs/
├── baseline_model.md
├── business_analysis.md
├── data_experiments.md
├── product_benchmarking.md
└── prototype.md
```

- `docs/baseline_model.md` — как работает текущий `mvp-baseline-001`.
- `docs/business_analysis.md` — бизнес-анализ MVP.
- `docs/data_experiments.md` — история источников, датасетов, сборщиков и ML-data экспериментов.
- `docs/product_benchmarking.md` — продуктовый benchmarking.
- `docs/prototype.md` — описание продуктового прототипа.

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

Минимальные значения:

```env
APP_ENV=development
BACKEND_CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173

TEST_USERNAME=demo
TEST_PASSWORD=demo
JWT_SECRET=change-me

GIGA_API_KEY=your_gigachat_authorization_key
GIGA_MODEL=GigaChat-2
GIGA_SCOPE=GIGACHAT_API_PERS
GIGA_VERIFY_SSL_CERTS=true
GIGA_TIMEOUT=30

FLYFORECAST_DATASET_PATH=/app/data/processed/dataset_daily_flights_v3.csv
PREDICTION_LOG_PATH=/app/data/interim/prediction_logs.jsonl

AIRPORT_LATITUDE=43.958
AIRPORT_LONGITUDE=145.683
AIRPORT_TIMEZONE=Asia/Sakhalin
```

### 3. Подготовить dataset

Положить рабочий dataset:

```text
data/processed/dataset_daily_flights_v3.csv
```

Минимальный формат:

```csv
date,status,is_flight_completed
2024-01-01,completed,1
2024-01-02,cancelled,0
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

1. Проверить `training_dataset_v1.csv` на корреляции, leakage и качество признаков.
2. Добавить time-based train/validation/test split.
3. Сравнить seasonal baseline и Logistic Regression.
4. Считать Brier Score и calibration curve.
5. Улучшить backend confidence/threshold rules после первых метрик forecast monitor.
6. Добавить tests для `/health`, `/auth/login`, `/predict`.
7. Улучшить frontend: «Как это работает», сравнение соседних дат, прозрачные ограничения.

---

## Главная мысль

Мы не пытаемся идеально предсказать погоду или решение авиакомпании.

Мы делаем честный вероятностный сервис, который помогает людям получить более полезный ориентир, чем «ничего неизвестно»:

- какие даты выглядят лучше;
- какие окна исторически хуже;
- когда риск выше;
- когда риск ниже;
- насколько можно доверять оценке на выбранном горизонте.
