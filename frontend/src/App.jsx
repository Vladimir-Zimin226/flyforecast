import React, { useMemo, useState } from "react";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

function toIsoDate(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");

  return `${year}-${month}-${day}`;
}

function todayIso() {
  return toIsoDate(new Date());
}

function addDaysIso(days) {
  const date = new Date();
  date.setDate(date.getDate() + days);
  return toIsoDate(date);
}

function parseIsoDate(value) {
  const [year, month, day] = value.split("-").map(Number);
  return new Date(year, month - 1, day);
}

function formatDisplayDate(value) {
  return parseIsoDate(value).toLocaleDateString("ru-RU", {
    day: "numeric",
    month: "long",
    year: "numeric"
  });
}

function formatCalendarTitle(date) {
  return date.toLocaleDateString("ru-RU", {
    month: "long",
    year: "numeric"
  });
}

function isSameDay(a, b) {
  return toIsoDate(a) === toIsoDate(b);
}

function isDateAllowed(date, minIso, maxIso) {
  const iso = toIsoDate(date);
  return iso >= minIso && iso <= maxIso;
}

function getCalendarDays(viewDate) {
  const year = viewDate.getFullYear();
  const month = viewDate.getMonth();

  const firstDay = new Date(year, month, 1);
  const firstWeekday = firstDay.getDay() === 0 ? 7 : firstDay.getDay();

  const start = new Date(year, month, 1);
  start.setDate(start.getDate() - (firstWeekday - 1));

  return Array.from({ length: 42 }, (_, index) => {
    const day = new Date(start);
    day.setDate(start.getDate() + index);

    return {
      date: day,
      iso: toIsoDate(day),
      isCurrentMonth: day.getMonth() === month
    };
  });
}

function probabilityPercent(value) {
  return Math.round(value * 100);
}

function decisionLabel(decision) {
  return decision === "yes" ? "Да" : "Нет";
}

function confidenceLabel(confidence) {
  const labels = {
    low: "низкая",
    medium: "средняя",
    high: "высокая"
  };

  return labels[confidence] || confidence;
}

function TelegramIcon() {
  return (
    <svg
      className="telegram-icon"
      viewBox="0 0 24 24"
      aria-hidden="true"
      focusable="false"
    >
      <path
        d="M21.7 4.3 18.6 19c-.2 1.1-.9 1.4-1.8.9l-5-3.7-2.4 2.3c-.3.3-.5.5-1 .5l.4-5.1 9.3-8.4c.4-.4-.1-.6-.6-.2L6 12.5 1.1 11c-1.1-.3-1.1-1.1.2-1.6L20.2 2c.9-.3 1.7.2 1.5 2.3Z"
        fill="currentColor"
      />
    </svg>
  );
}

export default function App() {
  const [token, setToken] = useState(localStorage.getItem("flyforecast_token") || "");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [date, setDate] = useState(todayIso());
  const [calendarOpen, setCalendarOpen] = useState(false);
  const [calendarViewDate, setCalendarViewDate] = useState(parseIsoDate(todayIso()));
  const [predictionCount, setPredictionCount] = useState(
    Number(localStorage.getItem("flyforecast_prediction_count") || "0")
  );
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  const minDate = useMemo(() => todayIso(), []);
  const maxDate = useMemo(() => addDaysIso(365), []);
  const calendarDays = useMemo(() => getCalendarDays(calendarViewDate), [calendarViewDate]);
  const selectedDateObject = useMemo(() => parseIsoDate(date), [date]);

  function goToPreviousMonth() {
    setCalendarViewDate((current) => {
      const next = new Date(current);
      next.setMonth(next.getMonth() - 1);
      return next;
    });
  }

  function goToNextMonth() {
    setCalendarViewDate((current) => {
      const next = new Date(current);
      next.setMonth(next.getMonth() + 1);
      return next;
    });
  }

  function selectDate(nextDate) {
    if (!isDateAllowed(nextDate, minDate, maxDate)) {
      return;
    }

    setDate(toIsoDate(nextDate));
    setCalendarOpen(false);
  }

  async function handleLogin(event) {
    event.preventDefault();
    setError("");
    setIsLoading(true);

    try {
      const response = await fetch(`${API_BASE_URL}/auth/login`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ username, password })
      });

      if (!response.ok) {
        throw new Error("Не удалось войти. Проверьте логин и пароль.");
      }

      const data = await response.json();

      localStorage.setItem("flyforecast_token", data.access_token);
      setToken(data.access_token);
    } catch (err) {
      setError(err.message || "Ошибка входа");
    } finally {
      setIsLoading(false);
    }
  }

  function handleLogout() {
    localStorage.removeItem("flyforecast_token");
    setToken("");
    setResult(null);
  }

  async function handlePredict(event) {
    event.preventDefault();
    setError("");
    setIsLoading(true);

    const nextPredictionNumber = predictionCount + 1;

    try {
      const params = new URLSearchParams({
        date,
        session_prediction_number: String(nextPredictionNumber)
      });

      const response = await fetch(`${API_BASE_URL}/predict?${params.toString()}`, {
        headers: {
          Authorization: `Bearer ${token}`
        }
      });

      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.detail || "Не удалось получить прогноз");
      }

      const data = await response.json();

      setResult(data);
      setPredictionCount(nextPredictionNumber);
      localStorage.setItem("flyforecast_prediction_count", String(nextPredictionNumber));
    } catch (err) {
      setError(err.message || "Ошибка прогноза");
    } finally {
      setIsLoading(false);
    }
  }

  if (!token) {
    return (
      <main className="page">
        <section className="card auth-card">
          <div className="eyebrow">flyforecast.ru</div>
          <h1>Летит на Курилы?</h1>
          <p className="lead">
            Тестовый вход в прототип сервиса оценки вероятности вылета через аэропорт Менделеево.
          </p>

          <form onSubmit={handleLogin} className="form">
            <label>
              Логин
              <input
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                autoComplete="username"
              />
            </label>

            <label>
              Пароль
              <input
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                autoComplete="current-password"
              />
            </label>

            <button disabled={isLoading}>
              {isLoading ? "Входим..." : "Войти"}
            </button>
          </form>

          {error && <div className="error">{error}</div>}

          <p className="small">
            Пока сервис не готов к публичному использованию, доступ ограничен тестовым пользователем.
          </p>
        </section>
      </main>
    );
  }

  return (
    <main className="page">
      <section className="hero">
        <div>
          <div className="eyebrow">flyforecast.ru</div>
          <h1>Летит на Курилы?</h1>
          <p className="lead">
            Оценим вероятность вылета через аэропорт Менделеево на выбранную дату.
          </p>
        </div>

        <button className="secondary" onClick={handleLogout}>
          Выйти
        </button>
      </section>

      <section className="card">
        <h2>Когда хотите вылететь с Кунашира?</h2>

        <form onSubmit={handlePredict} className="predict-form">
          <div className="date-field">
            <span className="field-label">Дата вылета</span>

            <button
              type="button"
              className="date-trigger"
              onClick={() => setCalendarOpen((current) => !current)}
              aria-expanded={calendarOpen}
            >
              <span>{formatDisplayDate(date)}</span>
              <span className="date-trigger-icon">▾</span>
            </button>

            {calendarOpen && (
              <div className="calendar">
                <div className="calendar-header">
                  <button
                    type="button"
                    className="calendar-nav"
                    onClick={goToPreviousMonth}
                    aria-label="Предыдущий месяц"
                  >
                    ←
                  </button>

                  <strong>{formatCalendarTitle(calendarViewDate)}</strong>

                  <button
                    type="button"
                    className="calendar-nav"
                    onClick={goToNextMonth}
                    aria-label="Следующий месяц"
                  >
                    →
                  </button>
                </div>

                <div className="calendar-weekdays">
                  <span>Пн</span>
                  <span>Вт</span>
                  <span>Ср</span>
                  <span>Чт</span>
                  <span>Пт</span>
                  <span>Сб</span>
                  <span>Вс</span>
                </div>

                <div className="calendar-grid">
                  {calendarDays.map((day) => {
                    const disabled = !isDateAllowed(day.date, minDate, maxDate);
                    const selected = isSameDay(day.date, selectedDateObject);

                    return (
                      <button
                        key={day.iso}
                        type="button"
                        className={[
                          "calendar-day",
                          day.isCurrentMonth ? "" : "calendar-day-muted",
                          selected ? "calendar-day-selected" : ""
                        ].join(" ")}
                        disabled={disabled}
                        onClick={() => selectDate(day.date)}
                      >
                        {day.date.getDate()}
                      </button>
                    );
                  })}
                </div>
              </div>
            )}
          </div>

          <button disabled={isLoading}>
            {isLoading ? "Считаем..." : "Узнать вероятность вылета"}
          </button>
        </form>

        <p className="small">
          Это вероятностная оценка, а не официальный статус рейса. Перед поездкой проверяйте данные у перевозчика и аэропорта.
        </p>

        {error && <div className="error">{error}</div>}
      </section>

      {result && (
        <section className={`card result result-${result.decision}`}>
          <div className="result-header">
            <div>
              <div className="eyebrow">Дата: {result.date}</div>
              <h2>{decisionLabel(result.decision)}</h2>
            </div>

            <div className="probability">
              {probabilityPercent(result.probability_flight)}%
            </div>
          </div>

          <p className="lead">
            Вероятность выполнения рейса — {probabilityPercent(result.probability_flight)}%.
          </p>

          <div className="meta-grid">
            <div>
              <span>Уверенность</span>
              <strong>{confidenceLabel(result.confidence)}</strong>
            </div>
            <div>
              <span>Горизонт</span>
              <strong>{result.horizon_days} дн.</strong>
            </div>
            <div>
              <span>Модель</span>
              <strong>{result.model_version}</strong>
            </div>
          </div>

          <p>{result.explanation}</p>

          {result.confidence === "low" && (
            <p className="hint">
              Совет: проверьте соседние даты — для дальнего горизонта полезнее выбрать благоприятное окно, а не одну точную дату.
            </p>
          )}

          <p className="small">{result.disclaimer}</p>
        </section>
      )}

      {predictionCount >= 2 && (
        <section className="card telegram-card">
          <h2>Хотите поддержать проект и следить за новостями?</h2>
          <p>
            Подпишитесь на Telegram-канал проекта «Летит на Курилы?» — там будем публиковать обновления и рассказывать, как развивается прогноз.
          </p>
          <a href="https://t.me/flyforecast" target="_blank" rel="noreferrer">
            <TelegramIcon />
            Подписаться
          </a>
        </section>
      )}

      <section className="card">
        <h2>Как это работает</h2>
        <p>
          Сервис использует историю выполненных и отменённых дней, сезонность, календарные признаки и доступный погодный прогноз.
          На дальние даты точного прогноза погоды нет, поэтому оценка становится менее уверенной.
        </p>
      </section>
    </main>
  );
}