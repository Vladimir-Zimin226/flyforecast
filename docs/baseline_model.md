# MVP Baseline Model

Этот документ фиксирует текущую логику прогноза `mvp-baseline-002`.

Baseline — это не обученная ML-модель. Это воспроизводимая эвристика в backend, которая даёт честную вероятностную оценку до появления и проверки ML-модели.

Код baseline находится в:

```text
backend/app/services/history.py
backend/app/services/weather.py
backend/app/services/predictor.py
```

Текущие версии:

```text
model_version: mvp-baseline-002
data_version: telegram-v2-plus-historical-board-manual-v3-2026-05-20
```

---

## Роль Baseline

Baseline нужен для трёх задач:

1. Давать пользователю рабочий прогноз уже сейчас.
2. Быть понятной точкой сравнения для будущей ML-модели.
3. Не притворяться, что сервис знает погоду или решение авиакомпании на год вперёд.

Если будущая ML-модель не превосходит baseline по Brier Score и калибровке на time-based test, её нельзя делать основной только потому, что она “ML”.

---

## Входные данные

Baseline использует:

- выбранную пользователем дату;
- горизонт прогноза в днях;
- historical snapshot из `FLYFORECAST_DATASET_PATH`;
- forecast weather snapshot Open-Meteo, если прогноз погоды доступен;
- только binary labels `completed` и `cancelled`.

Baseline не использует:

- GigaChat для принятия решения;
- сырые Telegram-сообщения;
- post-fact metadata вроде `reason_class`, `message_count`, `label_confidence`;
- будущие факты вылета/невылета.

GigaChat получает уже рассчитанные `decision`, `probability_flight`, `confidence` и факторы, после чего формулирует объяснение пользователю.

---

## Historical Snapshot

Файл с фактами читается из:

```env
FLYFORECAST_DATASET_PATH=/app/data/processed/dataset_daily_flights_v3.csv
```

Backend берёт только строки со статусами:

```text
completed
cancelled
```

Для выбранной даты считаются:

- `month_rows` — все исторические дни того же месяца;
- `decade_rows` — дни той же декады месяца;
- `similar_rows` — дни с day-of-year в окне `±14` дней.

Окно `similar_rows` учитывает переход через Новый год. Если похожих дней меньше 10, baseline откатывается к `month_rows`.

Вероятности считаются со сглаживанием Лапласа:

```text
probability = (completed + 1) / (total + 2)
```

Это защищает от грубых `0%` и `100%` на малых выборках.

Historical snapshot возвращает:

- `similar_days_count`;
- `completed_count`;
- `cancelled_count`;
- `historical_probability_flight`;
- `month_probability_flight`;
- `decade_probability_flight`.

---

## Base Probability

Стартовая вероятность:

```text
base = historical_probability_flight
```

Если есть вероятность по декаде месяца, она смешивается с вероятностью похожих дней:

```text
base = 0.65 * historical_probability_flight
     + 0.35 * decade_probability_flight
```

Смысл:

- `historical_probability_flight` даёт локальное окно по дню года;
- `decade_probability_flight` добавляет сезонный контекст внутри месяца;
- веса пока эвристические и должны быть проверены будущими ML/metrics experiments.

---

## Weather Adjustment

Погодная поправка применяется только для ближнего горизонта:

```text
horizon_days <= 15
```

Если Open-Meteo forecast недоступен, погодная поправка равна `0`.

Текущие правила:

| Условие | Поправка |
| --- | ---: |
| `wind_speed_10m >= 12` | `-0.05` |
| `wind_gusts_10m >= 18` | `-0.07` |
| `relative_humidity_2m >= 92` | `-0.04` |
| `cloud_cover >= 85` | `-0.03` |
| `cloud_cover_low >= 80` | `-0.05` |
| `visibility <= 3000` | `-0.06` |
| `dew_point_spread <= 2` | `-0.04` |
| `fog_low_cloud_risk_level == medium` | `-0.04` |
| `fog_low_cloud_risk_level == high` | `-0.09` |
| `precipitation >= 3` | `-0.03` |

Итоговая вероятность ограничивается диапазоном:

```text
0.05 <= probability_flight <= 0.95
```

Смысл ограничения: baseline не должен выдавать абсолютную уверенность.

---

## Decision Threshold

Вероятность превращается в `decision` через порог, зависящий от горизонта:

| Горизонт | Порог для `yes` |
| --- | ---: |
| `0..10` дней | `0.55` |
| `11..46` дней | `0.45` |
| `47+` дней | `0.30` |

Логика:

- для близкой даты “Да” требует более уверенного сигнала;
- для средней даты порог ниже, потому что прогноз менее точный;
- для дальнего горизонта ответ становится скорее seasonal risk estimate, а не прогнозом конкретной погоды.

Формула:

```text
decision = "yes" if probability_flight >= threshold else "no"
```

---

## Confidence

Текущая уверенность:

| Условие | Confidence |
| --- | --- |
| `horizon_days <= 10`, weather available, `similar_days_count >= 20` | `medium` |
| `horizon_days <= 46`, `similar_days_count >= 20` | `medium` |
| иначе | `low` |

У baseline пока нет `high`, потому что:

- dataset ещё развивается;
- правила не обучены и не откалиброваны как ML-модель;
- долгий горизонт не имеет фактического weather forecast.

---

## Горизонты Прогноза

Текущая baseline-логика поддерживает ответ до года вперёд, но смысл ответа меняется по горизонту.

### Ближний горизонт

Примерно `0..16` дней.

Используется:

- история;
- сезонность;
- forecast weather;
- погодная поправка.

### Средний горизонт

Примерно `17..46` дней.

Используется:

- история;
- сезонность;
- без погодной поправки, если forecast недоступен.

### Дальний горизонт

`47+` дней.

Используется:

- историческая вероятность похожего окна;
- декада/сезонность;
- низкая уверенность.

Это не прогноз конкретной погоды. Это оценка исторического риска выбранного календарного окна.

---

## Почему Это Baseline, А Не ML

Baseline:

- не обучает параметры на train/test;
- использует ручные веса и пороги;
- не оптимизирует Brier Score;
- не проверяет калибровку вероятностей;
- объясним и воспроизводим.

Будущая ML-модель должна быть проверена против baseline:

- на time-based split;
- по Brier Score;
- по calibration curve;
- по ROC-AUC/PR-AUC как дополнительным метрикам;
- отдельно для ближнего и дальнего горизонта.

---

## Будущая Архитектура Прогноза

Вероятная схема после появления ML:

| Горизонт | Основная логика |
| --- | --- |
| `0..15` дней | forecast weather + fog-risk + history + calendar |
| `16..365` дней | seasonal/historical model без фактической погоды |

Причина: прогноз погоды на год вперёд недоступен, но пользователю всё равно нужен ориентир по дате. Для дальних дат корректнее давать не погодный прогноз, а историко-сезонную оценку.

Baseline может остаться:

- fallback-логикой;
- сравнительной моделью для метрик;
- частью ensemble;
- основой дальнего seasonal estimate, если ML там не даёт качества.

---

## Ограничения Baseline

Известные ограничения:

- погодные веса ручные и пока не откалиброваны;
- пороги `yes/no` продуктовые, не оптимизированные;
- дневная погода грубая для рейса с конкретным временем;
- нет отдельной модели для направления рейса;
- нет route/aircraft/operational factors;
- для дальних дат нет настоящей погодной информации;
- historical dataset зависит от качества labels.

Baseline можно менять только с обновлением `model_version` и фиксацией причины изменения.

---

## Что Проверять Перед Заменой Baseline

Перед переключением backend на ML-модель нужно иметь:

- frozen training dataset version;
- список safe features без утечек;
- time-based train/validation/test split;
- сравнение с `mvp-baseline-002`;
- Brier Score;
- calibration curve;
- отчет по ближнему и дальнему горизонту;
- понятный fallback, если weather API недоступен.
