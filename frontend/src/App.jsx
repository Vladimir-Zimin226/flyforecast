import React, { useEffect, useMemo, useState } from "react";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
const TOKEN_KEY = "flyforecast_token";
const ANON_PREDICTION_COUNT_KEY = "flyforecast_prediction_count";
const COOKIE_NOTICE_KEY = "flyforecast_cookie_notice_ack";
const ANALYTICS_CONSENT_KEY = "flyforecast_analytics_consent";
const FREE_ANON_PREDICTION_LIMIT = 5;

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
    <svg className="telegram-icon" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path
        d="M21.7 4.3 18.6 19c-.2 1.1-.9 1.4-1.8.9l-5-3.7-2.4 2.3c-.3.3-.5.5-1 .5l.4-5.1 9.3-8.4c.4-.4-.1-.6-.6-.2L6 12.5 1.1 11c-1.1-.3-1.1-1.1.2-1.6L20.2 2c.9-.3 1.7.2 1.5 2.3Z"
        fill="currentColor"
      />
    </svg>
  );
}

function PrivacyPolicyView({ onBack }) {
  return (
    <main className="page policy-page">
      <button className="secondary compact-button" onClick={onBack}>
        Назад к сервису
      </button>

      <section className="card legal-card">
        <div className="eyebrow">flyforecast.ru</div>
        <h1>Политика обработки персональных данных</h1>
        <p className="lead">
          Краткая публичная версия для сервиса «Летит на Курилы?». Документ описывает, какие данные нужны для
          регистрации, личного кабинета, статистики прогнозов, обратной связи, cookies и Яндекс Метрики.
        </p>

        <h2>Оператор и сервис</h2>
        <p>
          Оператор обрабатывает персональные данные пользователей сайта flyforecast.ru для предоставления доступа к
          сервису прогноза, ведения личного кабинета, учета статистики использования и обработки обращений.
        </p>

        <h2>Какие данные обрабатываются</h2>
        <ul>
          <li>Имя, email и пароль в хешированном виде при регистрации.</li>
          <li>Дата регистрации, количество сделанных прогнозов и сообщения обратной связи.</li>
          <li>Технические данные работы сайта, включая необходимые cookies.</li>
          <li>Данные веб-аналитики Яндекс Метрики только при согласии пользователя.</li>
        </ul>

        <h2>Cookies и аналитика</h2>
        <p>
          Необходимые cookies используются для работы сайта, хранения токена входа, счетчика бесплатных прогнозов и
          выбранных пользователем настроек согласий. Аналитические cookies и Яндекс Метрика используются только после
          отдельного одобрения.
        </p>

        <h2>Согласие при регистрации</h2>
        <p>
          При регистрации пользователь подтверждает согласие на обработку персональных данных в целях создания аккаунта,
          предоставления доступа к сервису, ведения статистики прогнозов и обработки обратной связи.
        </p>

        <h2>Обратная связь и права пользователя</h2>
        <p>
          Пользователь может направить обращение через форму в личном кабинете. По запросу можно уточнить, удалить или
          ограничить обработку персональных данных в порядке, предусмотренном законодательством РФ.
        </p>
      </section>
    </main>
  );
}

export default function App() {
  const [token, setToken] = useState(localStorage.getItem(TOKEN_KEY) || "");
  const [profile, setProfile] = useState(null);
  const [authMode, setAuthMode] = useState("register");
  const [authPanelOpen, setAuthPanelOpen] = useState(false);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [personalDataConsent, setPersonalDataConsent] = useState(false);
  const [date, setDate] = useState(todayIso());
  const [calendarOpen, setCalendarOpen] = useState(false);
  const [calendarViewDate, setCalendarViewDate] = useState(parseIsoDate(todayIso()));
  const [predictionCount, setPredictionCount] = useState(
    Number(localStorage.getItem(ANON_PREDICTION_COUNT_KEY) || "0")
  );
  const [result, setResult] = useState(null);
  const [feedbackMessage, setFeedbackMessage] = useState("");
  const [feedbackStatus, setFeedbackStatus] = useState("");
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [policyOpen, setPolicyOpen] = useState(false);
  const [cookieNoticeAck, setCookieNoticeAck] = useState(
    localStorage.getItem(COOKIE_NOTICE_KEY) === "true"
  );
  const [analyticsConsent, setAnalyticsConsent] = useState(
    localStorage.getItem(ANALYTICS_CONSENT_KEY) === "true"
  );

  const minDate = useMemo(() => todayIso(), []);
  const maxDate = useMemo(() => addDaysIso(365), []);
  const calendarDays = useMemo(() => getCalendarDays(calendarViewDate), [calendarViewDate]);
  const selectedDateObject = useMemo(() => parseIsoDate(date), [date]);
  const effectivePredictionCount = profile?.prediction_count ?? predictionCount;
  const mustRegister = !token && predictionCount >= FREE_ANON_PREDICTION_LIMIT;

  useEffect(() => {
    if (!token) {
      setProfile(null);
      return;
    }

    loadProfile(token);
  }, [token]);

  async function loadProfile(authToken = token) {
    const response = await fetch(`${API_BASE_URL}/me`, {
      headers: {
        Authorization: `Bearer ${authToken}`
      }
    });

    if (!response.ok) {
      localStorage.removeItem(TOKEN_KEY);
      setToken("");
      setProfile(null);
      return;
    }

    setProfile(await response.json());
  }

  function sendConsentEvent(event, payload) {
    fetch(`${API_BASE_URL}/consents`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ event, ...payload })
    }).catch(() => {});
  }

  function acknowledgeCookies() {
    localStorage.setItem(COOKIE_NOTICE_KEY, "true");
    setCookieNoticeAck(true);
    sendConsentEvent("necessary_cookies_ack", {
      necessary_cookies_ack: true,
      analytics_consent: false
    });
  }

  function approveAnalytics() {
    localStorage.setItem(COOKIE_NOTICE_KEY, "true");
    localStorage.setItem(ANALYTICS_CONSENT_KEY, "true");
    setCookieNoticeAck(true);
    setAnalyticsConsent(true);
    sendConsentEvent("analytics_consent", {
      necessary_cookies_ack: true,
      analytics_consent: true
    });
  }

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

  function clearAuthForm() {
    setName("");
    setEmail("");
    setPassword("");
    setPersonalDataConsent(false);
  }

  async function handleLogin(event) {
    event.preventDefault();
    setError("");
    setFeedbackStatus("");
    setIsLoading(true);

    try {
      const response = await fetch(`${API_BASE_URL}/auth/login`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ email, password })
      });

      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.detail || "Не удалось войти. Проверьте email и пароль.");
      }

      const data = await response.json();
      localStorage.setItem(TOKEN_KEY, data.access_token);
      setToken(data.access_token);
      setAuthPanelOpen(false);
      clearAuthForm();
    } catch (err) {
      setError(err.message || "Ошибка входа");
    } finally {
      setIsLoading(false);
    }
  }

  async function handleRegister(event) {
    event.preventDefault();
    setError("");
    setFeedbackStatus("");

    if (!personalDataConsent) {
      setError("Для регистрации нужно дать согласие на обработку персональных данных.");
      return;
    }

    setIsLoading(true);

    try {
      const response = await fetch(`${API_BASE_URL}/auth/register`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          name,
          email,
          password,
          personal_data_consent: personalDataConsent,
          analytics_consent: analyticsConsent,
          initial_prediction_count: Math.min(predictionCount, FREE_ANON_PREDICTION_LIMIT)
        })
      });

      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.detail || "Не удалось зарегистрироваться.");
      }

      const data = await response.json();
      localStorage.setItem(TOKEN_KEY, data.access_token);
      localStorage.removeItem(ANON_PREDICTION_COUNT_KEY);
      setToken(data.access_token);
      setAuthPanelOpen(false);
      clearAuthForm();
    } catch (err) {
      setError(err.message || "Ошибка регистрации");
    } finally {
      setIsLoading(false);
    }
  }

  function handleLogout() {
    localStorage.removeItem(TOKEN_KEY);
    setToken("");
    setProfile(null);
    setResult(null);
    setFeedbackMessage("");
    setFeedbackStatus("");
  }

  async function handlePredict(event) {
    event.preventDefault();
    setError("");
    setFeedbackStatus("");

    if (mustRegister) {
      setAuthMode("register");
      setAuthPanelOpen(true);
      setError("Вы уже сделали 5 бесплатных прогнозов. Зарегистрируйтесь, чтобы продолжить бесплатно.");
      return;
    }

    setIsLoading(true);

    const nextPredictionNumber = token ? effectivePredictionCount + 1 : predictionCount + 1;

    try {
      const params = new URLSearchParams({
        date,
        session_prediction_number: String(nextPredictionNumber)
      });

      const headers = token ? { Authorization: `Bearer ${token}` } : {};
      const response = await fetch(`${API_BASE_URL}/predict?${params.toString()}`, { headers });

      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.detail || "Не удалось получить прогноз");
      }

      const data = await response.json();
      setResult(data);

      if (token) {
        await loadProfile(token);
      } else {
        setPredictionCount(nextPredictionNumber);
        localStorage.setItem(ANON_PREDICTION_COUNT_KEY, String(nextPredictionNumber));
        if (nextPredictionNumber >= FREE_ANON_PREDICTION_LIMIT) {
          setAuthMode("register");
          setAuthPanelOpen(true);
        }
      }
    } catch (err) {
      setError(err.message || "Ошибка прогноза");
    } finally {
      setIsLoading(false);
    }
  }

  async function handleFeedback(event) {
    event.preventDefault();
    setError("");
    setFeedbackStatus("");
    setIsLoading(true);

    try {
      const response = await fetch(`${API_BASE_URL}/feedback`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`
        },
        body: JSON.stringify({ message: feedbackMessage })
      });

      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.detail || "Не удалось отправить обратную связь.");
      }

      setFeedbackMessage("");
      setFeedbackStatus("Спасибо, обратная связь сохранена.");
      await loadProfile(token);
    } catch (err) {
      setError(err.message || "Ошибка отправки обратной связи");
    } finally {
      setIsLoading(false);
    }
  }

  if (policyOpen) {
    return <PrivacyPolicyView onBack={() => setPolicyOpen(false)} />;
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

        <div className="hero-actions">
          {token ? (
            <button className="secondary" onClick={handleLogout}>
              Выйти
            </button>
          ) : (
            <>
              <button
                className="secondary"
                onClick={() => {
                  setAuthMode("login");
                  setAuthPanelOpen(true);
                }}
              >
                Войти
              </button>
              <button
                onClick={() => {
                  setAuthMode("register");
                  setAuthPanelOpen(true);
                }}
              >
                Регистрация
              </button>
            </>
          )}
        </div>
      </section>

      {!cookieNoticeAck && (
        <section className="cookie-banner" aria-label="Уведомление о cookies и аналитике">
          <div>
            <strong>Cookies и аналитика</strong>
            <p>
              Используем необходимые cookies для работы сервиса и счетчика бесплатных прогнозов. Яндекс Метрику
              подключим только после вашего согласия.
            </p>
          </div>
          <div className="cookie-actions">
            <button className="secondary" onClick={acknowledgeCookies}>
              Понял
            </button>
            <button onClick={approveAnalytics}>Разрешаю аналитику</button>
          </div>
        </section>
      )}

      {token && profile && (
        <section className="card account-card">
          <div>
            <div className="eyebrow">Личный кабинет</div>
            <h2>{profile.name}</h2>
            <p className="small">{profile.email}</p>
          </div>

          <div className="meta-grid">
            <div>
              <span>Прогнозов сделано</span>
              <strong>{profile.prediction_count}</strong>
            </div>
            <div>
              <span>Отзывов отправлено</span>
              <strong>{profile.feedback_count}</strong>
            </div>
            <div>
              <span>Аналитика</span>
              <strong>{profile.analytics_consent ? "разрешена" : "не разрешена"}</strong>
            </div>
          </div>

          <form onSubmit={handleFeedback} className="form feedback-form">
            <label>
              Обратная связь по сервису
              <textarea
                value={feedbackMessage}
                onChange={(event) => setFeedbackMessage(event.target.value)}
                placeholder="Что улучшить, что работает странно, чего не хватает?"
                maxLength={2000}
              />
            </label>
            <button disabled={isLoading || feedbackMessage.trim().length < 3}>
              Отправить отзыв
            </button>
          </form>

          {feedbackStatus && <div className="success">{feedbackStatus}</div>}
        </section>
      )}

      {(authPanelOpen || mustRegister) && !token && (
        <section className="card auth-card">
          <div className="auth-tabs">
            <button
              className={authMode === "register" ? "" : "secondary"}
              onClick={() => setAuthMode("register")}
              type="button"
            >
              Регистрация
            </button>
            <button
              className={authMode === "login" ? "" : "secondary"}
              onClick={() => setAuthMode("login")}
              type="button"
            >
              Вход
            </button>
          </div>

          {authMode === "register" ? (
            <>
              <h2>Продолжить бесплатно</h2>
              <p className="small">
                После 5 бесплатных прогнозов нужен личный кабинет: так мы считаем реальную статистику использования и
                можем собирать обратную связь по продукту.
              </p>

              <form onSubmit={handleRegister} className="form">
                <label>
                  Имя
                  <input
                    value={name}
                    onChange={(event) => setName(event.target.value)}
                    autoComplete="name"
                    required
                  />
                </label>

                <label>
                  Email
                  <input
                    type="email"
                    value={email}
                    onChange={(event) => setEmail(event.target.value)}
                    autoComplete="email"
                    required
                  />
                </label>

                <label>
                  Пароль
                  <input
                    type="password"
                    value={password}
                    onChange={(event) => setPassword(event.target.value)}
                    autoComplete="new-password"
                    minLength={8}
                    required
                  />
                </label>

                <label className="checkbox-row">
                  <input
                    type="checkbox"
                    checked={personalDataConsent}
                    onChange={(event) => setPersonalDataConsent(event.target.checked)}
                  />
                  <span>
                    Я даю согласие на обработку персональных данных и ознакомлен(а) с{" "}
                    <button type="button" className="text-button" onClick={() => setPolicyOpen(true)}>
                      политикой обработки персональных данных
                    </button>
                    .
                  </span>
                </label>

                <p className="notice">
                  Зарегистрироваться можно только после согласия на обработку персональных данных.
                </p>

                <button disabled={isLoading || !personalDataConsent}>
                  {isLoading ? "Регистрируем..." : "Зарегистрироваться"}
                </button>
              </form>
            </>
          ) : (
            <>
              <h2>Вход в личный кабинет</h2>
              <form onSubmit={handleLogin} className="form">
                <label>
                  Email
                  <input
                    type="email"
                    value={email}
                    onChange={(event) => setEmail(event.target.value)}
                    autoComplete="email"
                    required
                  />
                </label>

                <label>
                  Пароль
                  <input
                    type="password"
                    value={password}
                    onChange={(event) => setPassword(event.target.value)}
                    autoComplete="current-password"
                    required
                  />
                </label>

                <button disabled={isLoading}>
                  {isLoading ? "Входим..." : "Войти"}
                </button>
              </form>
            </>
          )}
        </section>
      )}

      <section className="card">
        <div className="section-heading">
          <div>
            <h2>Когда хотите вылететь с Кунашира?</h2>
            <p className="small">
              {token
                ? `Прогнозов в личном кабинете: ${effectivePredictionCount}.`
                : `Бесплатных прогнозов без регистрации: ${Math.min(predictionCount, FREE_ANON_PREDICTION_LIMIT)} из ${FREE_ANON_PREDICTION_LIMIT}.`}
            </p>
          </div>
        </div>

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

          <button disabled={isLoading || mustRegister}>
            {isLoading ? "Считаем..." : "Узнать вероятность вылета"}
          </button>
        </form>

        <p className="small">
          Это вероятностная оценка, а не официальный статус рейса. Перед поездкой проверяйте данные у перевозчика и
          аэропорта.
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

            <div className="probability">{probabilityPercent(result.probability_flight)}%</div>
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
              Совет: проверьте соседние даты — для дальнего горизонта полезнее выбрать благоприятное окно, а не одну
              точную дату.
            </p>
          )}

          <p className="small">{result.disclaimer}</p>
        </section>
      )}

      {predictionCount >= 2 && !token && (
        <section className="card telegram-card">
          <h2>Хотите поддержать проект и следить за новостями?</h2>
          <p>
            Подпишитесь на Telegram-канал проекта «Летит на Курилы?» — там будем публиковать обновления и рассказывать,
            как развивается прогноз.
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
          Сервис использует историю выполненных и отменённых дней, сезонность, календарные признаки и доступный погодный
          прогноз. На дальние даты точного прогноза погоды нет, поэтому оценка становится менее уверенной.
        </p>
        <button type="button" className="text-button policy-link" onClick={() => setPolicyOpen(true)}>
          Политика обработки персональных данных
        </button>
      </section>
    </main>
  );
}
