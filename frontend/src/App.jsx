import React, { useEffect, useMemo, useState } from "react";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
const TOKEN_KEY = "flyforecast_token";
const ADMIN_TOKEN_KEY = "flyforecast_admin_token";
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

function nullableProbabilityPercent(value) {
  return value === null || value === undefined ? "нет данных" : `${probabilityPercent(value)}%`;
}

function decisionLabel(decision) {
  return decision === "yes" ? "Да" : "Нет";
}

function decisionToneLabel(result) {
  const probability = probabilityPercent(result.probability_flight);

  if (result.confidence === "low") {
    return result.decision === "yes" ? "Скорее благоприятное окно" : "Ориентировочный риск отмены";
  }

  if (result.decision === "yes") {
    return probability >= 60 ? "Высокий шанс вылета" : "Скорее да";
  }

  return probability <= 45 ? "Риск отмены выше" : "Пограничный прогноз";
}

function lowConfidenceHint(result) {
  if (result.forecast_mode === "climate_history" || !result.weather?.available) {
    return "Дата далеко в будущем: точного прогноза погоды для нее пока нет, поэтому оценка опирается в основном на историю и сезонность. Сравните соседние даты.";
  }

  return "Для этой даты мало надежных похожих случаев в истории, поэтому полезно сравнить соседние даты.";
}

function PredictionDecisionIcon({ decision }) {
  const iconName = decision === "yes" ? "yes" : "no";

  return (
    <div className={`decision-icon decision-icon-${decision}`} aria-hidden="true">
      <img src={`/icons/forecast-${iconName}.svg`} alt="" width="132" height="132" />
    </div>
  );
}

function formatDateTime(value) {
  if (!value) {
    return "нет данных";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleString("ru-RU");
}

function formatPredictionDateTitle(value) {
  const parsed = parseIsoDate(value);

  if (Number.isNaN(parsed.getTime())) {
    return `Предсказание вылета на ${value}`;
  }

  return `Предсказание вылета на ${parsed.toLocaleDateString("ru-RU", {
    day: "numeric",
    month: "long",
    year: "numeric"
  })}`;
}

function confidenceLabel(confidence) {
  const labels = {
    low: "низкая",
    medium: "средняя",
    high: "высокая"
  };

  return labels[confidence] || confidence;
}

function serviceStatusLabel(status) {
  const labels = {
    ok: "работает",
    warning: "требует внимания",
    error: "ошибка",
    success: "успешно",
    partial: "частично"
  };

  return labels[status] || status;
}

function predictionEvaluationLabel(prediction) {
  if (prediction.evaluated) {
    return prediction.hit ? "угадал" : "ошибся";
  }

  return prediction.outcome_status || "ожидает факт";
}

function renderFormattedExplanation(text) {
  const source = String(text || "");
  const parts = [];
  const boldPattern = /\*\*(.+?)\*\*/g;
  let lastIndex = 0;
  let match;

  function appendText(segment) {
    const cleanedSegment = segment.replaceAll("**", "");
    cleanedSegment.split("\n").forEach((line, index, lines) => {
      if (line) {
        parts.push(line);
      }
      if (index < lines.length - 1) {
        parts.push(<br key={`br-${parts.length}`} />);
      }
    });
  }

  while ((match = boldPattern.exec(source)) !== null) {
    appendText(source.slice(lastIndex, match.index));
    parts.push(<strong key={`strong-${parts.length}`}>{match[1]}</strong>);
    lastIndex = match.index + match[0].length;
  }

  appendText(source.slice(lastIndex));

  return parts;
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

const operatorDetails = [
  ["Оператор", "Индивидуальный предприниматель Зимин Владимир Андреевич"],
  ["ИНН", "751902969999"],
  ["ОГРНИП", "325650000029609"],
  ["Сайт сервиса", "https://flyforecast.ru"],
  ["Email по вопросам персональных данных", "aisakh@zmncraft.ru"],
  ["Уведомление в Роскомнадзор", "подано 22.05.2026, номер 100293352"]
];

const policySections = [
  {
    title: "1. Общие положения",
    items: [
      "Настоящая Политика определяет порядок обработки и защиты персональных данных пользователей сайта и сервиса FLYFORECAST.RU.",
      "Политика опубликована во исполнение требований Федерального закона от 27.07.2006 N 152-ФЗ «О персональных данных» и иных применимых нормативных актов Российской Федерации.",
      "Оператор обрабатывает персональные данные на законной и справедливой основе, только для заранее определенных целей и в объеме, необходимом для работы сервиса.",
      "Политика применяется к посетителям сайта, зарегистрированным пользователям, пользователям личного кабинета, лицам, направляющим обратную связь, а также иным лицам, взаимодействующим с Оператором через сервис.",
      "Сервис не является официальным источником статуса рейсов и предоставляет вероятностную оценку на основе исторических данных, сезонности, погодных факторов и программной логики."
    ]
  },
  {
    title: "2. Основные понятия",
    items: [
      "Персональные данные - любая информация, относящаяся к прямо или косвенно определенному либо определяемому физическому лицу.",
      "Оператор - Индивидуальный предприниматель Зимин Владимир Андреевич, самостоятельно организующий и осуществляющий обработку персональных данных в рамках сайта и сервиса FLYFORECAST.RU.",
      "Пользователь - лицо, посещающее сайт, использующее прогноз, регистрирующее личный кабинет или направляющее обратную связь.",
      "Сервис - веб-приложение FLYFORECAST.RU, предназначенное для оценки вероятности выполнения рейса через аэропорт Менделеево на выбранную дату.",
      "Cookies - небольшие фрагменты данных и аналогичные технологии браузера, включая localStorage, которые используются для работы сайта, сохранения пользовательских настроек, авторизации, учета бесплатных прогнозов и аналитики при наличии согласия."
    ]
  },
  {
    title: "3. Категории субъектов персональных данных",
    items: [
      "Посетители сайта FLYFORECAST.RU.",
      "Пользователи, выполняющие прогнозы без регистрации.",
      "Зарегистрированные пользователи личного кабинета.",
      "Пользователи, направляющие обратную связь по работе сервиса.",
      "Пользователи, давшие согласие на использование аналитических cookies и Яндекс Метрики.",
      "Иные лица, персональные данные которых могут быть переданы Оператору в рамках обращения пользователя, если такая передача имеет законное основание."
    ]
  },
  {
    title: "4. Категории обрабатываемых данных",
    items: [
      "Для регистрации и личного кабинета: имя, email, пароль в хешированном виде, дата регистрации, идентификатор учетной записи, токен авторизации, настройки аккаунта и статус согласий.",
      "Для статистики использования сервиса: количество сделанных прогнозов, дата и время запроса, выбранная пользователем дата вылета, номер прогноза в пользовательской сессии или аккаунте, версия модели и технические сведения о результате.",
      "Для обратной связи: email аккаунта, имя пользователя, текст сообщения, дата и время отправки, статус обработки обращения.",
      "Для необходимых cookies и localStorage: факт принятия уведомления, выбранные настройки приватности, токен авторизации, счетчик бесплатных прогнозов без регистрации.",
      "Для аналитики при согласии пользователя: IP-адрес, cookie-идентификаторы, сведения о браузере и устройстве, источник перехода, дата и время посещения, просмотренные страницы и события интерфейса.",
      "Оператор не собирает через обычную регистрацию паспортные данные, платежные реквизиты, специальные категории персональных данных и биометрические персональные данные."
    ]
  },
  {
    title: "5. Цели обработки",
    items: [
      "Предоставление пользователю доступа к прогнозу вероятности выполнения рейса.",
      "Ограничение количества бесплатных прогнозов без регистрации и предоставление дальнейшего бесплатного доступа через личный кабинет.",
      "Регистрация пользователя, идентификация, аутентификация и поддержание сессии в личном кабинете.",
      "Ведение статистики количества прогнозов, оценки нагрузки, продуктовой аналитики и бенчмарков по зарегистрированным пользователям.",
      "Получение и обработка обратной связи по работе сервиса.",
      "Диагностика ошибок, обеспечение безопасности, предотвращение злоупотреблений и поддержание работоспособности сайта.",
      "Анализ посещаемости и поведения пользователей с помощью Яндекс Метрики только после отдельного согласия пользователя.",
      "Исполнение требований законодательства Российской Федерации, учет согласий, отказов и обращений субъектов персональных данных."
    ]
  },
  {
    title: "6. Правовые основания обработки",
    items: [
      "Согласие пользователя на обработку персональных данных при регистрации в личном кабинете.",
      "Согласие пользователя на использование аналитических cookies и Яндекс Метрики, если пользователь выбирает соответствующую настройку в cookie-баннере.",
      "Необходимость обработки данных для предоставления функционала сервиса и исполнения пользовательского соглашения, когда пользователь регистрируется и использует личный кабинет.",
      "Законный интерес Оператора в обеспечении безопасности сайта, предотвращении злоупотреблений, диагностике ошибок и защите своих прав.",
      "Исполнение обязанностей, предусмотренных законодательством Российской Федерации."
    ]
  },
  {
    title: "7. Cookies и Яндекс Метрика",
    items: [
      "Сайт использует необходимые cookies и localStorage для корректной работы интерфейса, сохранения входа в аккаунт, учета бесплатных прогнозов без регистрации и хранения выбранных пользователем настроек приватности.",
      "Необходимые cookies не используются для рекламного профилирования и нужны для предоставления выбранных пользователем функций.",
      "Яндекс Метрика и аналитические cookies используются только после отдельного согласия пользователя. До получения такого согласия счетчик Метрики не должен загружаться в интерфейсе сервиса.",
      "С помощью Метрики Оператор может получать обобщенную статистику посещаемости, источников переходов, действий на сайте, технических ошибок и удобства интерфейса.",
      "В Метрику не должны передаваться пароли, токены, паспортные данные, платежные реквизиты, закрытые документы, пользовательские файлы, специальные категории данных и иная избыточная конфиденциальная информация.",
      "Пользователь может оставить только необходимые cookies, разрешить аналитику через баннер, очистить cookies в настройках браузера или ограничить их использование средствами браузера."
    ]
  },
  {
    title: "8. Личный кабинет и статистика прогнозов",
    items: [
      "После выполнения установленного количества бесплатных прогнозов без регистрации сервис предлагает создать личный кабинет для дальнейшей бесплатной работы.",
      "При регистрации пользователь указывает имя, email и пароль. Пароль хранится только в хешированном виде.",
      "В личном кабинете пользователь может видеть количество сделанных прогнозов и отправлять обратную связь по работе сервиса.",
      "Статистика прогнозов используется для отображения пользователю, внутренней аналитики сервиса, оценки качества продукта, нагрузки и бенчмарков по реально зарегистрированным пользователям.",
      "Оператор не использует статистику прогнозов для принятия решений, порождающих юридические последствия для пользователя или существенно затрагивающих его права."
    ]
  },
  {
    title: "9. Обратная связь",
    items: [
      "Пользователь может направить сообщение через форму обратной связи в личном кабинете.",
      "Оператор использует сообщение обратной связи для анализа качества сервиса, исправления ошибок, развития продукта и ответа пользователю при необходимости.",
      "Оператор может использовать отзывы пользователей в целях продвижения сервиса, включая рекламные и информационные материалы. Публикация отзыва с именем, email или иными сведениями, позволяющими прямо идентифицировать пользователя, допускается только при наличии отдельного согласия или иного законного основания.",
      "Пользователь не должен направлять через форму обратной связи специальные категории персональных данных, паспортные данные, платежные реквизиты, медицинские сведения, пароли, токены и персональные данные третьих лиц без законного основания."
    ]
  },
  {
    title: "10. Передача третьим лицам",
    items: [
      "Оператор может передавать или предоставлять доступ к персональным данным третьим лицам, если это необходимо для работы сайта, хостинга, аналитики, технической поддержки, безопасности, исполнения требований закона или защиты прав Оператора.",
      "К таким лицам могут относиться хостинг-провайдеры и поставщики инфраструктуры, сервисы веб-аналитики, включая Яндекс Метрику, email-провайдеры, подрядчики технической поддержки и государственные органы в случаях, предусмотренных законом.",
      "Если третье лицо обрабатывает данные по поручению Оператора, условия обработки определяются договором или иным соглашением.",
      "При использовании Яндекс Метрики данные обрабатываются в порядке, установленном условиями использования сервиса Яндекс Метрика и применимыми документами Яндекса."
    ]
  },
  {
    title: "11. Локализация и трансграничная передача",
    items: [
      "При сборе персональных данных граждан Российской Федерации Оператор обеспечивает запись, систематизацию, накопление, хранение, уточнение и извлечение таких данных с использованием баз данных, находящихся на территории Российской Федерации, если иное не допускается законом.",
      "Для сервиса FLYFORECAST.RU используются ресурсы АО «Селектел», расположенные на территории Российской Федерации.",
      "Трансграничная передача персональных данных в рамках сервиса FLYFORECAST.RU не осуществляется и не планируется осуществляться.",
      "Если в будущем архитектура сервиса изменится таким образом, что потребуется трансграничная передача персональных данных, Оператор до начала такой обработки обновит документы и выполнит необходимые действия, предусмотренные законодательством Российской Федерации."
    ]
  },
  {
    title: "12. Сроки обработки и хранения",
    items: [
      "Персональные данные обрабатываются не дольше, чем требуется для достижения целей обработки, исполнения пользовательского соглашения, выполнения требований закона, защиты прав Оператора или рассмотрения обращений.",
      "Данные аккаунта хранятся в течение срока существования аккаунта и удаляются или обезличиваются после удаления аккаунта, если отсутствует обязанность дальнейшего хранения.",
      "Данные обратной связи хранятся до завершения обработки обращения и в течение разумного срока, необходимого для подтверждения коммуникации и улучшения сервиса.",
      "Данные согласий, отказов и настроек приватности хранятся в течение срока, необходимого для подтверждения факта предоставления или отзыва согласия.",
      "Логи прогнозов и обезличенная или техническая статистика могут храниться для продуктовой аналитики, контроля качества модели, безопасности и подтверждения корректной работы сервиса."
    ]
  },
  {
    title: "13. Удаление и отзыв согласия",
    items: [
      "Пользователь вправе отозвать согласие на обработку персональных данных, направив обращение Оператору на email, указанный в настоящей Политике.",
      "После получения отзыва Оператор прекращает обработку данных, обрабатываемых на основании согласия, если отсутствуют иные законные основания для продолжения обработки.",
      "Отзыв согласия на обработку данных аккаунта может повлечь невозможность дальнейшего использования личного кабинета.",
      "Отказ от аналитических cookies не влияет на возможность пользоваться основными функциями сервиса.",
      "Удаление выполняется путем удаления записи из базы данных, удаления файла, обезличивания записи, удаления из журнала обработки или иным технически доступным способом с учетом резервных копий и законных сроков хранения."
    ]
  },
  {
    title: "14. Меры защиты",
    items: [
      "Оператор принимает необходимые правовые, организационные и технические меры для защиты персональных данных от неправомерного или случайного доступа, уничтожения, изменения, блокирования, копирования, предоставления, распространения и иных неправомерных действий.",
      "К мерам защиты относятся разграничение доступа, использование паролей и токенов, хранение паролей в хешированном виде, HTTPS, резервное копирование, учет согласий, контроль состава обрабатываемых данных и минимизация избыточной обработки.",
      "Оператор ограничивает доступ к персональным данным лицами и сервисами, которым такой доступ необходим для работы сайта, поддержки, безопасности или исполнения требований закона.",
      "Оператор организует порядок реагирования на инциденты безопасности персональных данных и порядок рассмотрения запросов субъектов персональных данных."
    ]
  },
  {
    title: "15. Права субъекта персональных данных",
    items: [
      "Пользователь вправе получать информацию, касающуюся обработки его персональных данных.",
      "Пользователь вправе требовать уточнения, блокирования или уничтожения персональных данных, если они являются неполными, устаревшими, неточными, незаконно полученными или не являются необходимыми для заявленной цели обработки.",
      "Пользователь вправе отозвать согласие на обработку персональных данных.",
      "Пользователь вправе требовать прекращения обработки персональных данных в случаях, предусмотренных законом.",
      "Пользователь вправе обжаловать действия или бездействие Оператора в уполномоченный орган по защите прав субъектов персональных данных или в суд."
    ]
  },
  {
    title: "16. Порядок обращения",
    items: [
      "Для реализации своих прав пользователь может направить обращение на email aisakh@zmncraft.ru.",
      "В обращении рекомендуется указать имя, email аккаунта или иной контакт для ответа, суть требования и сведения, позволяющие подтвердить факт взаимодействия с сервисом.",
      "Оператор может запросить дополнительные сведения для подтверждения личности пользователя и защиты данных от неправомерного доступа.",
      "Обращения рассматриваются в сроки, предусмотренные законодательством Российской Федерации."
    ]
  },
  {
    title: "17. Изменение Политики",
    items: [
      "Оператор вправе изменять настоящую Политику при изменении законодательства, состава сервисов, целей обработки, используемых подрядчиков, инфраструктуры или функций сайта.",
      "Новая редакция Политики вступает в силу с момента публикации на сайте, если в ней не указано иное.",
      "Пользователю рекомендуется периодически проверять актуальную редакцию Политики."
    ]
  }
];

function PrivacyPolicyView({ onBack }) {
  return (
    <main className="page policy-page">
      <button className="secondary compact-button" onClick={onBack}>
        Назад к сервису
      </button>

      <section className="card legal-card">
        <div className="eyebrow">FLYFORECAST.RU</div>
        <h1>Политика обработки персональных данных</h1>
        <p className="lead">
          Документ описывает, как сервис FLYFORECAST.RU обрабатывает персональные данные при использовании прогноза,
          регистрации, личного кабинета, обратной связи, cookies и Яндекс Метрики.
        </p>

        <dl className="operator-grid">
          {operatorDetails.map(([label, value]) => (
            <div key={label}>
              <dt>{label}</dt>
              <dd>{value}</dd>
            </div>
          ))}
        </dl>

        <p className="notice">
          Версия: 1.0. Дата публикации: 25 мая 2026 года. Политика действует с момента публикации на сайте, если в новой
          редакции не указано иное.
        </p>
      </section>

      {policySections.map((section) => (
        <section className="card legal-card" key={section.title}>
          <h2>{section.title}</h2>
          <ul>
            {section.items.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </section>
      ))}

      <section className="card legal-card">
        <h2>18. Контакты Оператора</h2>
        <p>
          По вопросам обработки персональных данных, отзыва согласия, уточнения, удаления или ограничения обработки
          персональных данных направьте обращение на{" "}
          <a href="mailto:aisakh@zmncraft.ru">aisakh@zmncraft.ru</a>.
        </p>
      </section>
    </main>
  );
}

function AboutView({ onBack, onPolicy }) {
  return (
    <main className="page about-page">
      <section className="card about-hero-card">
        <div className="about-hero-copy">
          <div className="eyebrow">О сервисе</div>
          <h1>FLYFORECAST.RU</h1>
          <img className="about-photo about-photo-mobile" src="/my-photo.jpg" alt="Владимир Зимин" />
          <p className="lead">
            Здравствуйте! Меня зовут Владимир Зимин. Магистрант AI Talent Hub, предприниматель, занимаюсь развитием ИИ
            на Сахалине. Этот проект появился в рамках обучения в магистратуре и призван помочь жителям Кунашира
            планировать свои перелеты с острова в условиях нестабильной погоды.
          </p>
          <button className="secondary compact-button" onClick={onBack}>
            Назад к прогнозу
          </button>
        </div>
        <img className="about-photo about-photo-desktop" src="/my-photo.jpg" alt="Владимир Зимин" />
      </section>

      <section className="card about-card">
        <h2>Как работает сервис</h2>
        <p>
          Я взял доступную публичную историю вылетов через аэропорт Менделеево с конца 2017 года, сопоставил эти даты с
          исторической погодой и выделил признаки, которые чаще всего влияют на вылет или отмену рейса. Эта база
          используется для оценки вероятности вылета на выбранную дату.
        </p>
        <p>
          Для ближайших дат прогноз учитывает погоду: видимость, низкую облачность, ветер, влажность и риск тумана. Пока
          точного прогноза погоды еще нет, сервис опирается на историю похожих дат и сезонные закономерности. Поэтому
          сделать прогноз можно на 365 дней вперед, а затем перепроверить прогноз, когда до поездки остается около двух
          недель.
        </p>
      </section>

      <section className="card about-card">
        <h2>Точность и ограничения</h2>
        <p>
          С момента запуска сервиса текущая логика предоставления прогнозов показывает точность около 90%, с учетом
          наличия погодных данных. На мой взгляд, это уже хороший ориентир для планирования. Однако точность прогнозов по
          будущим рейсам, которые предсказываются без актуальной погоды, только предстоит узнать. Сервис автоматически
          накапливает статистику, что позволит уточнить правила по мере выявления новых закономерностей.
        </p>
        <p>
          Очень прошу помнить: сервис пытается предсказать событие, которое зависит от погоды, расписания и решений,
          принимаемых людьми. Мне очень близка проблема отмененных рейсов, потому что я жил на этом прекрасном острове 3
          года и не один раз разочарованно уезжал из аэропорта с семьей и горой багажа или ночевал в Панораме. Ошибка
          прогноза может стоить времени или денег, поэтому окончательное решение о поездке всегда остается за вами.
        </p>
      </section>

      <section className="card about-card">
        <h2>Как правильно пользоваться</h2>
        <ol className="about-steps">
          <li>Выберите месяц, в котором планируете поездку, и посмотрите прогноз на нужное число.</li>
          <li>Сравните соседние даты: часто полезнее найти не один день, а несколько более подходящих окон для вылета.</li>
          <li>
            Когда до поездки останется около двух недель, проверьте прогноз еще раз: в нем появится актуальная погодная
            модель.
          </li>
          <li>
            Если прогноз стал менее благоприятным, посмотрите соседние даты и выберите запасной вариант, если это
            возможно.
          </li>
        </ol>
        <p>
          Если сервис помог лучше спланировать поездку, пожалуйста, напишите об этом в отзывах. Негативные случаи тоже
          важны: они помогают быстрее находить слабые места прогноза.
        </p>
      </section>

      <section className="card about-card">
        <h2>Поддержать сервис</h2>
        <p>
          Если вам нравится работа сервиса и вы хотите его поддержать, лучшее, что вы можете сделать, это подписаться на
          мой канал в{" "}
          <a href="https://t.me/AI_na_Sakhaline" target="_blank" rel="noreferrer">
            Telegram
          </a>{" "}
          или в{" "}
          <a href="https://max.ru/id751902969999_biz" target="_blank" rel="noreferrer">
            Max
          </a>{" "}
          и поделиться ссылками на них с друзьями и знакомыми.
        </p>
        <p>
          Кстати, идея сервиса была найдена в моем Telegram-канале:{" "}
          <a href="https://t.me/AI_na_Sakhaline/26" target="_blank" rel="noreferrer">
            здесь написано подробно
          </a>
          .
        </p>
      </section>

      <section className="card about-card">
        <h2>Доступ</h2>
        <p>
          На старте без регистрации доступно 5 бесплатных прогнозов. После регистрации можно продолжить пользоваться
          сервисом бесплатно, сохранять статистику в личном кабинете и оставлять обратную связь.
        </p>
        <button type="button" className="text-button policy-link" onClick={onPolicy}>
          Политика обработки персональных данных
        </button>
      </section>
    </main>
  );
}

export default function App() {
  const [token, setToken] = useState(localStorage.getItem(TOKEN_KEY) || "");
  const [adminToken, setAdminToken] = useState(localStorage.getItem(ADMIN_TOKEN_KEY) || "");
  const [profile, setProfile] = useState(null);
  const [adminData, setAdminData] = useState(null);
  const [adminServices, setAdminServices] = useState(null);
  const [adminEditEmail, setAdminEditEmail] = useState("");
  const [adminEditForm, setAdminEditForm] = useState({});
  const [authMode, setAuthMode] = useState("register");
  const [authPanelOpen, setAuthPanelOpen] = useState(false);
  const [authPromptMessage, setAuthPromptMessage] = useState("");
  const [currentPath, setCurrentPath] = useState(() => window.location.pathname);
  const [accountOpen, setAccountOpen] = useState(false);
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
  const activeToken = token || adminToken;
  const effectivePredictionCount = profile?.prediction_count ?? predictionCount;
  const mustRegister = !activeToken && predictionCount >= FREE_ANON_PREDICTION_LIMIT;

  function navigateTo(path) {
    window.history.pushState({}, "", path);
    setCurrentPath(path);
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function openAbout(event) {
    event.preventDefault();
    navigateTo("/about");
  }

  function goHome() {
    navigateTo("/");
  }

  useEffect(() => {
    function handlePopState() {
      setCurrentPath(window.location.pathname);
    }

    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

  useEffect(() => {
    if (!token) {
      setProfile(null);
      return;
    }

    loadProfile(token);
  }, [token]);

  useEffect(() => {
    if (!adminToken) {
      setAdminData(null);
      setAdminServices(null);
      return;
    }

    loadAdminUsers(adminToken);
    loadAdminServices(adminToken);
  }, [adminToken]);

  async function loadProfile(authToken = token) {
    const response = await fetch(`${API_BASE_URL}/me`, {
      headers: {
        Authorization: `Bearer ${authToken}`
      }
    });

    if (!response.ok) {
      const adminResponse = await fetch(`${API_BASE_URL}/admin/users`, {
        headers: {
          Authorization: `Bearer ${authToken}`
        }
      });

      if (adminResponse.ok) {
        localStorage.setItem(ADMIN_TOKEN_KEY, authToken);
        setAdminToken(authToken);
        setAdminData(await adminResponse.json());
        await loadAdminServices(authToken);
      } else {
        localStorage.removeItem(TOKEN_KEY);
        localStorage.removeItem(ADMIN_TOKEN_KEY);
        setToken("");
        setAdminToken("");
        setAdminData(null);
        setAdminServices(null);
      }

      setProfile(null);
      return;
    }

    setProfile(await response.json());
  }

  async function loadAdminUsers(authToken = adminToken) {
    const response = await fetch(`${API_BASE_URL}/admin/users`, {
      headers: {
        Authorization: `Bearer ${authToken}`
      }
    });

    if (!response.ok) {
      localStorage.removeItem(ADMIN_TOKEN_KEY);
      setAdminToken("");
      setAdminData(null);
      setAdminServices(null);
      return;
    }

    setAdminData(await response.json());
  }

  async function loadAdminServices(authToken = adminToken) {
    const response = await fetch(`${API_BASE_URL}/admin/services`, {
      headers: {
        Authorization: `Bearer ${authToken}`
      }
    });

    if (response.ok) {
      setAdminServices(await response.json());
    }
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

  function openAuthPanel(mode, message = "") {
    setAuthMode(mode);
    setAuthPromptMessage(message);
    setAuthPanelOpen(true);
  }

  function closeAuthPanel() {
    setAuthPanelOpen(false);
    setAuthPromptMessage("");
    setError("");
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

      const adminResponse = await fetch(`${API_BASE_URL}/admin/users`, {
        headers: {
          Authorization: `Bearer ${data.access_token}`
        }
      });

      if (adminResponse.ok) {
        localStorage.setItem(ADMIN_TOKEN_KEY, data.access_token);
        setAdminToken(data.access_token);
        setAdminData(await adminResponse.json());
        await loadAdminServices(data.access_token);
      } else {
        localStorage.removeItem(ADMIN_TOKEN_KEY);
        setAdminToken("");
        setAdminData(null);
        setAdminServices(null);
      }

      setAuthPanelOpen(false);
      setAuthPromptMessage("");
      setAccountOpen(false);
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
      setAuthPromptMessage("");
      setAccountOpen(false);
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
    setAccountOpen(false);
  }

  function handleAdminLogout() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(ADMIN_TOKEN_KEY);
    setToken("");
    setAdminToken("");
    setAdminData(null);
    setAdminServices(null);
    setAdminEditEmail("");
    setAdminEditForm({});
  }

  function startEditUser(user) {
    setAdminEditEmail(user.email);
    setAdminEditForm({
      name: user.name,
      email: user.email,
      prediction_count: user.prediction_count,
      feedback_count: user.feedback_count,
      personal_data_consent: user.personal_data_consent,
      analytics_consent: user.analytics_consent,
      password: ""
    });
  }

  function updateAdminEditField(field, value) {
    setAdminEditForm((current) => ({
      ...current,
      [field]: value
    }));
  }

  async function handleAdminSaveUser(event) {
    event.preventDefault();
    setError("");
    setFeedbackStatus("");
    setIsLoading(true);

    const payload = {
      name: adminEditForm.name,
      email: adminEditForm.email,
      prediction_count: Number(adminEditForm.prediction_count),
      feedback_count: Number(adminEditForm.feedback_count),
      personal_data_consent: Boolean(adminEditForm.personal_data_consent),
      analytics_consent: Boolean(adminEditForm.analytics_consent)
    };

    if (adminEditForm.password) {
      payload.password = adminEditForm.password;
    }

    try {
      const response = await fetch(`${API_BASE_URL}/admin/users/${encodeURIComponent(adminEditEmail)}`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${adminToken}`
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        throw new Error(data.detail || "Не удалось сохранить пользователя.");
      }

      setAdminEditEmail("");
      setAdminEditForm({});
      await loadAdminUsers(adminToken);
    } catch (err) {
      setError(err.message || "Ошибка сохранения пользователя");
    } finally {
      setIsLoading(false);
    }
  }

  async function handleAdminDeleteUser(emailToDelete) {
    if (!window.confirm(`Удалить пользователя ${emailToDelete}?`)) {
      return;
    }

    setError("");
    setFeedbackStatus("");
    setIsLoading(true);

    try {
      const response = await fetch(`${API_BASE_URL}/admin/users/${encodeURIComponent(emailToDelete)}`, {
        method: "DELETE",
        headers: {
          Authorization: `Bearer ${adminToken}`
        }
      });

      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        throw new Error(data.detail || "Не удалось удалить пользователя.");
      }

      await loadAdminUsers(adminToken);
    } catch (err) {
      setError(err.message || "Ошибка удаления пользователя");
    } finally {
      setIsLoading(false);
    }
  }

  async function handlePredict(event) {
    event.preventDefault();
    setError("");
    setFeedbackStatus("");

    if (mustRegister) {
      openAuthPanel(
        "register",
        "Вы уже сделали 5 бесплатных прогнозов без регистрации. Создайте личный кабинет, чтобы продолжить бесплатно."
      );
      return;
    }

    setIsLoading(true);

    const nextPredictionNumber = activeToken ? effectivePredictionCount + 1 : predictionCount + 1;

    try {
      const params = new URLSearchParams({
        date,
        session_prediction_number: String(nextPredictionNumber)
      });

      const headers = activeToken ? { Authorization: `Bearer ${activeToken}` } : {};
      const response = await fetch(`${API_BASE_URL}/predict?${params.toString()}`, { headers });

      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.detail || "Не удалось получить прогноз");
      }

      const data = await response.json();
      setResult(data);

      if (adminToken) {
        await loadAdminUsers(adminToken);
        await loadAdminServices(adminToken);
      } else if (token) {
        await loadProfile(token);
      } else {
        setPredictionCount(nextPredictionNumber);
        localStorage.setItem(ANON_PREDICTION_COUNT_KEY, String(nextPredictionNumber));
        if (nextPredictionNumber >= FREE_ANON_PREDICTION_LIMIT) {
          openAuthPanel(
            "register",
            "Вы сделали 5 бесплатных прогнозов. Зарегистрируйтесь, чтобы продолжить пользоваться сервисом бесплатно."
          );
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

  const isAboutPage = (currentPath.replace(/\/+$/, "") || "/") === "/about";

  if (policyOpen) {
    return <PrivacyPolicyView onBack={() => setPolicyOpen(false)} />;
  }

  if (isAboutPage) {
    return <AboutView onBack={goHome} onPolicy={() => setPolicyOpen(true)} />;
  }

  return (
    <main className="page">
      <section className="card predict-card">
        <div className="predict-card-header">
          <div>
            <div className="eyebrow">FLYFORECAST.RU</div>
            <h1>Когда хотите вылететь с Кунашира?</h1>
            <p className="lead">
              Оценим вероятность вылета через аэропорт Менделеево на выбранную дату.
            </p>
          </div>

          <div className="hero-actions">
            {adminToken ? (
              <>
                <button
                  className="secondary"
                  onClick={() => {
                    loadAdminUsers(adminToken);
                    loadAdminServices(adminToken);
                  }}
                >
                  Обновить админку
                </button>
                <button className="secondary" onClick={handleAdminLogout}>
                  Выйти из админки
                </button>
              </>
            ) : token ? (
              null
            ) : (
              <>
                <button
                  className="secondary"
                  onClick={() => {
                    openAuthPanel("login");
                  }}
                >
                  Войти
                </button>
                <button
                  onClick={() => {
                    openAuthPanel("register");
                  }}
                >
                  Регистрация
                </button>
              </>
            )}
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

          <button disabled={isLoading}>
            {isLoading ? "Считаем..." : "Узнать вероятность вылета"}
          </button>
        </form>

        <p className="small">
          Это вероятностная оценка, а не официальный статус рейса. Перед поездкой проверяйте данные у перевозчика и
          аэропорта.
        </p>

        {error && <div className="error">{error}</div>}
      </section>

      {!cookieNoticeAck && (
        <section className="cookie-banner" aria-label="Уведомление о cookies и аналитике">
          <div>
            <strong>Мы используем cookies</strong>
            <p>
              Необходимые cookies помогают сайту работать, сохраняют вход, настройки и счетчик бесплатных прогнозов.
              С вашего согласия мы также используем Яндекс Метрику, чтобы понимать, как пользуются сервисом, и улучшать
              его.
            </p>
          </div>
          <div className="cookie-actions">
            <button className="secondary" onClick={acknowledgeCookies}>
              Только необходимые
            </button>
            <button onClick={approveAnalytics}>Разрешаю аналитику</button>
          </div>
        </section>
      )}

      {result && (
        <section className={`card result result-${result.decision}`}>
          <div className="result-header">
            <div className="result-decision">
              <PredictionDecisionIcon decision={result.decision} />
              <div>
                <div className="result-date-label">{formatPredictionDateTitle(result.date)}</div>
                <div className={`forecast-mode-badge forecast-mode-${result.forecast_mode || "weather_model"}`}>
                  {result.forecast_mode_label || "Прогноз с учетом погодной модели"}
                </div>
                <h2>{decisionLabel(result.decision)}</h2>
                <span>{decisionToneLabel(result)}</span>
              </div>
            </div>

            <div className="result-probability-box">
              <span>Вероятность вылета</span>
              <div className="probability">{probabilityPercent(result.probability_flight)}%</div>
            </div>
          </div>

          <p className="lead">
            Вероятность выполнения рейса — {probabilityPercent(result.probability_flight)}%.
          </p>

          <p className="result-explanation">{renderFormattedExplanation(result.explanation)}</p>

          {result.confidence === "low" && (
            <p className="hint">
              {lowConfidenceHint(result)}
            </p>
          )}

          <p className="small">{result.disclaimer}</p>
        </section>
      )}

      {adminToken && adminData && (
        <section className="card admin-card">
          <div className="section-heading">
            <div>
              <div className="eyebrow">Админ-панель</div>
              <h2>Пользователи и аналитика</h2>
            </div>
          </div>

          <div className="meta-grid">
            <div>
              <span>Пользователей</span>
              <strong>{adminData.total_users}</strong>
            </div>
            <div>
              <span>Прогнозов</span>
              <strong>{adminData.total_predictions}</strong>
            </div>
            <div>
              <span>Отзывов</span>
              <strong>{adminData.total_feedback}</strong>
            </div>
            <div>
              <span>Согласий на аналитику</span>
              <strong>{adminData.analytics_consents}</strong>
            </div>
          </div>

          {adminServices && (
            <div className="service-panel">
              <div className="section-heading">
                <div>
                  <div className="eyebrow">Фоновые сервисы</div>
                  <h3>Табло и ежедневные прогнозы</h3>
                </div>
              </div>

              <div className="service-grid">
                <article className={`service-card service-${adminServices.board_collector.health.status}`}>
                  <div className="service-card-header">
                    <div>
                      <strong>{adminServices.board_collector.health.name}</strong>
                      <span>{serviceStatusLabel(adminServices.board_collector.health.status)}</span>
                    </div>
                    <span className="service-pill">{adminServices.board_collector.rows_last_observation} строк</span>
                  </div>
                  <p>{adminServices.board_collector.health.message}</p>
                  <div className="admin-user-stats">
                    <span>Последняя проверка: {formatDateTime(adminServices.board_collector.latest_observed_at)}</span>
                    <span>Всего строк: {adminServices.board_collector.total_rows}</span>
                    <span>Дата наблюдения: {adminServices.board_collector.latest_observation_date || "нет данных"}</span>
                  </div>
                  <div className="status-chips">
                    {Object.entries(adminServices.board_collector.latest_statuses || {}).map(([status, count]) => (
                      <span key={status}>
                        {status}: {count}
                      </span>
                    ))}
                  </div>
                  {adminServices.board_collector.recent_errors.length > 0 && (
                    <div className="service-errors">
                      <strong>Последние ошибки источников</strong>
                      {adminServices.board_collector.recent_errors.map((item, index) => (
                        <p key={`${item.observed_at}-${item.source}-${index}`}>
                          {formatDateTime(item.observed_at)} · {item.source}: {item.error}
                        </p>
                      ))}
                    </div>
                  )}
                </article>

                <article className={`service-card service-${adminServices.forecast_monitor.health.status}`}>
                  <div className="service-card-header">
                    <div>
                      <strong>{adminServices.forecast_monitor.health.name}</strong>
                      <span>{serviceStatusLabel(adminServices.forecast_monitor.health.status)}</span>
                    </div>
                    <span className="service-pill">{adminServices.forecast_monitor.total_predictions} прогнозов</span>
                  </div>
                  <p>{adminServices.forecast_monitor.health.message}</p>
                  <div className="admin-user-stats">
                    <span>Последний запуск: {formatDateTime(adminServices.forecast_monitor.health.last_seen_at)}</span>
                    <span>Запусков: {adminServices.forecast_monitor.total_runs}</span>
                    <span>Оценено: {adminServices.forecast_monitor.total_evaluations}</span>
                    <span>Текущая логика: {adminServices.forecast_monitor.recalculated_model_version || "нет данных"}</span>
                    <span>Ledger accuracy: {nullableProbabilityPercent(adminServices.forecast_monitor.accuracy)}</span>
                    <span>
                      Да/Нет: {adminServices.forecast_monitor.recalculated_predicted_yes}/
                      {adminServices.forecast_monitor.recalculated_predicted_no}
                    </span>
                    <span>
                      Факт выполнен/отменен: {adminServices.forecast_monitor.recalculated_observed_completed}/
                      {adminServices.forecast_monitor.recalculated_observed_cancelled}
                    </span>
                    <span>
                      Ложное Да/Нет: {adminServices.forecast_monitor.recalculated_false_yes}/
                      {adminServices.forecast_monitor.recalculated_false_no}
                    </span>
                    <span>Ждет факт: {adminServices.forecast_monitor.total_pending}</span>
                  </div>
                  <div className="service-summary-grid" aria-label="Сводка фоновых прогнозов">
                    <div>
                      <span>Точность текущей логики</span>
                      <strong>{nullableProbabilityPercent(adminServices.forecast_monitor.recalculated_accuracy)}</strong>
                    </div>
                    <div>
                      <span>Угадал</span>
                      <strong>{adminServices.forecast_monitor.recalculated_total_hits}</strong>
                    </div>
                    <div>
                      <span>Ошибся</span>
                      <strong>{adminServices.forecast_monitor.recalculated_total_misses}</strong>
                    </div>
                    <div>
                      <span>Ожидает факт</span>
                      <strong>{adminServices.forecast_monitor.total_pending}</strong>
                    </div>
                  </div>
                  {!adminServices.forecast_monitor.recalculated_metrics_available && (
                    <div className="service-run-summary">
                      <strong>Пересчёт текущей логики недоступен</strong>
                      <p>{adminServices.forecast_monitor.recalculated_metrics_reason}</p>
                    </div>
                  )}
                  {adminServices.forecast_monitor.latest_run && (
                    <div className="service-run-summary">
                      <strong>Последний набор прогнозов</strong>
                      <span>
                        {formatDateTime(adminServices.forecast_monitor.latest_run.created_at)} ·{" "}
                        {serviceStatusLabel(adminServices.forecast_monitor.latest_run.status)} ·{" "}
                        {adminServices.forecast_monitor.latest_run.predictions_count}/
                        {adminServices.forecast_monitor.latest_run.expected_predictions} создано
                      </span>
                      {adminServices.forecast_monitor.latest_run.error && (
                        <p>{adminServices.forecast_monitor.latest_run.error}</p>
                      )}
                    </div>
                  )}
                </article>
              </div>

              <div className="service-tables">
                <div>
                  <strong>Последние запуски forecast monitor</strong>
                  <div className="service-list">
                    {adminServices.forecast_monitor.recent_runs.map((run) => (
                      <div className="service-list-row" key={`${run.id}-${run.created_at}`}>
                        <span>{formatDateTime(run.created_at)}</span>
                        <span>{serviceStatusLabel(run.status)}</span>
                        <span>
                          {run.predictions_count}/{run.expected_predictions} прогнозов
                        </span>
                      </div>
                    ))}
                  </div>
                </div>

                <div>
                  <strong>Все фоновые прогнозы</strong>
                  <div className="service-list service-scroll-list">
                    {adminServices.forecast_monitor.recent_predictions.map((prediction) => (
                      <div
                        className={`service-list-row service-list-row-prediction ${
                          prediction.evaluated ? (prediction.hit ? "prediction-hit" : "prediction-miss") : ""
                        }`}
                        key={`${prediction.created_at}-${prediction.target_date}-${prediction.horizon_days}`}
                      >
                        <span>{prediction.target_date}</span>
                        <span>{formatDateTime(prediction.created_at)}</span>
                        <span>{prediction.horizon_days} дн.</span>
                        <span>{probabilityPercent(prediction.probability_flight)}%</span>
                        <span>{decisionLabel(prediction.decision)}</span>
                        <span>{predictionEvaluationLabel(prediction)}</span>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          )}

          <div className="admin-users">
            {adminData.users.map((user) => (
              <div className="admin-user" key={user.email}>
                {adminEditEmail === user.email ? (
                  <form onSubmit={handleAdminSaveUser} className="admin-edit-form">
                    <label>
                      Имя
                      <input
                        value={adminEditForm.name || ""}
                        onChange={(event) => updateAdminEditField("name", event.target.value)}
                        required
                      />
                    </label>
                    <label>
                      Email
                      <input
                        type="email"
                        value={adminEditForm.email || ""}
                        onChange={(event) => updateAdminEditField("email", event.target.value)}
                        required
                      />
                    </label>
                    <label>
                      Новый пароль
                      <input
                        type="password"
                        value={adminEditForm.password || ""}
                        onChange={(event) => updateAdminEditField("password", event.target.value)}
                        placeholder="Оставьте пустым, если не меняется"
                      />
                    </label>
                    <label>
                      Прогнозов
                      <input
                        type="number"
                        min="0"
                        value={adminEditForm.prediction_count ?? 0}
                        onChange={(event) => updateAdminEditField("prediction_count", event.target.value)}
                      />
                    </label>
                    <label>
                      Отзывов
                      <input
                        type="number"
                        min="0"
                        value={adminEditForm.feedback_count ?? 0}
                        onChange={(event) => updateAdminEditField("feedback_count", event.target.value)}
                      />
                    </label>
                    <label className="checkbox-row">
                      <input
                        type="checkbox"
                        checked={Boolean(adminEditForm.personal_data_consent)}
                        onChange={(event) => updateAdminEditField("personal_data_consent", event.target.checked)}
                      />
                      <span>Согласие на обработку ПД</span>
                    </label>
                    <label className="checkbox-row">
                      <input
                        type="checkbox"
                        checked={Boolean(adminEditForm.analytics_consent)}
                        onChange={(event) => updateAdminEditField("analytics_consent", event.target.checked)}
                      />
                      <span>Согласие на аналитику</span>
                    </label>
                    <div className="admin-row-actions">
                      <button disabled={isLoading}>Сохранить</button>
                      <button
                        type="button"
                        className="secondary"
                        onClick={() => {
                          setAdminEditEmail("");
                          setAdminEditForm({});
                        }}
                      >
                        Отмена
                      </button>
                    </div>
                  </form>
                ) : (
                  <>
                    <div className="admin-user-main">
                      <div>
                        <strong>{user.name}</strong>
                        <span>{user.email}</span>
                      </div>
                      <div className="admin-row-actions">
                        <button type="button" className="secondary" onClick={() => startEditUser(user)}>
                          Редактировать
                        </button>
                        <button type="button" className="danger-button" onClick={() => handleAdminDeleteUser(user.email)}>
                          Удалить
                        </button>
                      </div>
                    </div>
                    <div className="admin-user-stats">
                      <span>Прогнозов: {user.prediction_count}</span>
                      <span>Отзывов: {user.feedback_count}</span>
                      <span>Метрика: {user.analytics_consent ? "да" : "нет"}</span>
                      <span>Регистрация: {new Date(user.registered_at).toLocaleDateString("ru-RU")}</span>
                      <span>
                        Последний прогноз:{" "}
                        {user.last_prediction_at
                          ? new Date(user.last_prediction_at).toLocaleString("ru-RU")
                          : "нет"}
                      </span>
                    </div>
                    <div className="admin-feedbacks">
                      <strong>Отзывы пользователя</strong>
                      {user.feedbacks.length > 0 ? (
                        user.feedbacks.map((feedback) => (
                          <article className="admin-feedback" key={feedback.id}>
                            <time>{new Date(feedback.created_at).toLocaleString("ru-RU")}</time>
                            <p>{feedback.message}</p>
                          </article>
                        ))
                      ) : (
                        <p className="small">Пока отзывов нет.</p>
                      )}
                    </div>
                  </>
                )}
              </div>
            ))}
          </div>
        </section>
      )}

      {token && profile && (
        <section className={`card account-card ${accountOpen ? "account-card-open" : "account-card-collapsed"}`}>
          <div className="account-header account-header-collapsible">
            <button
              type="button"
              className="account-summary-button"
              onClick={() => setAccountOpen((isOpen) => !isOpen)}
              aria-expanded={accountOpen}
              aria-label={accountOpen ? "Свернуть личный кабинет" : "Развернуть личный кабинет"}
            >
              <span>
                <span className="eyebrow">Личный кабинет</span>
                <span className="account-name">{profile.name}</span>
                <span className="small">{profile.email}</span>
              </span>
            </button>
            <button className="secondary account-action" onClick={handleLogout}>
              Выйти
            </button>
          </div>

          {accountOpen && (
            <div className="account-panel">
              <div className="meta-grid account-meta">
                <div>
                  <span>Прогнозов сделано</span>
                  <strong>{profile.prediction_count}</strong>
                </div>
                <div>
                  <span>Отзывов отправлено</span>
                  <strong>{profile.feedback_count}</strong>
                </div>
              </div>

              <form onSubmit={handleFeedback} className="form feedback-form">
                <label>
                  Обратная связь по сервису
                  <textarea
                    value={feedbackMessage}
                    onChange={(event) => setFeedbackMessage(event.target.value)}
                    placeholder="Что улучшить, что работает странно, чего не хватает?"
                    maxLength={500}
                  />
                </label>
                <p className="small">{feedbackMessage.length} из 500 символов</p>
                <button disabled={isLoading || feedbackMessage.trim().length < 3}>
                  Отправить отзыв
                </button>
              </form>

              {feedbackStatus && <div className="success">{feedbackStatus}</div>}
            </div>
          )}
        </section>
      )}

      {authPanelOpen && !token && (
        <div className="modal-backdrop" role="presentation">
          <section
            className="card auth-card auth-modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="auth-modal-title"
          >
            <button
              type="button"
              className="modal-close"
              onClick={closeAuthPanel}
              aria-label="Закрыть окно регистрации"
            >
              ×
            </button>

            <div className="auth-tabs">
              <button
                className={authMode === "register" ? "" : "secondary"}
                onClick={() => {
                  setAuthMode("register");
                  setAuthPromptMessage("");
                  setError("");
                }}
                type="button"
              >
                Регистрация
              </button>
              <button
                className={authMode === "login" ? "" : "secondary"}
                onClick={() => {
                  setAuthMode("login");
                  setAuthPromptMessage("");
                  setError("");
                }}
                type="button"
              >
                Вход
              </button>
            </div>

            {authPromptMessage && <p className="notice">{authPromptMessage}</p>}
            {error && <div className="error">{error}</div>}

            {authMode === "register" ? (
              <>
                <h2 id="auth-modal-title">Продолжить бесплатно</h2>
                <p className="small">
                  Личный кабинет нужен после 5 бесплатных прогнозов: в нем сохраняется ваша статистика и можно оставить
                  обратную связь по сервису.
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
                <h2 id="auth-modal-title">Вход в личный кабинет</h2>
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
        </div>
      )}

      {predictionCount >= 2 && !token && (
        <section className="card telegram-card">
          <h2>Хотите поддержать проект и следить за новостями?</h2>
          <p>
            Подпишитесь на Telegram-канал проекта FLYFORECAST.RU — там будем публиковать обновления и рассказывать,
            как развивается прогноз.
          </p>
          <a href="https://t.me/flyforecast" target="_blank" rel="noreferrer">
            <TelegramIcon />
            Подписаться
          </a>
        </section>
      )}

      <section className="card service-about-card">
        <h2>Как работает сервис</h2>
        <p>
          Сервис знает историю вылетов через аэропорт Менделеево с конца 2017 года, погодные условия, в которых они
          выполнялись или отменялись, и прогноз погоды примерно на ближайшие две недели. На ближайшие 14 дней
          предсказание строится с учетом погоды, на более дальний период - по истории вылетов и сезонным
          закономерностям. Прогноз можно сделать на год вперед. Более подробно о работе сервиса{" "}
          <a href="/about" className="inline-link" onClick={openAbout}>
            читайте здесь
          </a>
          .
        </p>
        <button type="button" className="text-button policy-link" onClick={() => setPolicyOpen(true)}>
          Политика обработки персональных данных
        </button>
      </section>
    </main>
  );
}
