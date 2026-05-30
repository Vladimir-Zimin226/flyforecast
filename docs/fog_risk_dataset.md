# Fog-risk dataset for Mendeleyevo

Документ фиксирует решение по погодным источникам для аэропорта Менделеево и pipeline для признаков тумана/низкой облачности.

## Что решили

- FlightRadar24 не используем в ближайшей итерации: история стоит слишком дорого для текущего MVP.
- Himawari оставляем как исследовательский эксперимент на будущее, не блокируем текущую модель.
- Главный практический источник сейчас - Open-Meteo по координатам Менделеево.
- Для ближнего горизонта используем forecast weather.
- Для истории используем Open-Meteo Archive как приближение фактической погоды дня.
- Для historical visibility используем Open-Meteo Historical Forecast API, потому что обычный Archive возвращает `visibility` как пустой/undefined.

## Текущий прогноз

Backend запрашивает дополнительные Open-Meteo поля:

- `visibility`;
- `cloud_cover_low`;
- `weather_code`;
- `temperature_2m`;
- `dew_point_2m`;
- `relative_humidity_2m`;
- `precipitation`;
- `wind_speed_10m`;
- `wind_gusts_10m`.

На их основе рассчитываются:

- `dew_point_spread`;
- `fog_low_cloud_risk_score`;
- `fog_low_cloud_risk_level`.

Эти признаки используются в baseline-поправке `mvp-baseline-003`.

## Исторический датасет

Сборка:

```bash
python pipelines/training/build_mendeleyevo_fog_risk_dataset.py
```

Основные выходы:

```text
data/processed/mendeleyevo_fog_risk_dataset.csv
data/interim/weather/mendeleyevo_fog_risk_summary.md
data/interim/weather/mendeleyevo_open_meteo_archive_fog_risk.csv
```

Датасет сопоставляет погодные признаки Менделеево с известными исходами рейсов из `dataset_daily_flights_v3.csv`.

## Важное ограничение

Open-Meteo Archive не отдаёт историческую `visibility` для точки Менделеево, но Historical Forecast API отдаёт модельную visibility для части периода, начиная с `2021-03-23`. Поэтому historical fog-risk строится на сочетании visibility и proxy-признаков:

- высокая влажность;
- маленькая разница температуры и точки росы;
- высокая низкая облачность;
- weather code;
- слабый/опасный ветер;
- осадки.

Это не заменяет официальный METAR/TAF Менделеево, но дает числовой признак именно по координатам Кунашира, что полезнее для сервиса, чем авиационная погода Южно-Сахалинска.

После пересборки 28.05.2026:

- `mendeleyevo_fog_risk_dataset.csv`: `3088` строк;
- `visibility_min` заполнена в `1892` строках;
- на размеченных днях completion rate: `high=0.3925`, `medium=0.5030`, `low=0.7043`.

## Интерфейс

В интерфейсе разделяем режимы:

- `0-15` дней: прогноз с учетом погодной модели;
- `16+` дней: климатико-историческая оценка риска.

Так сервис не притворяется, что знает точную погоду на август или более дальнюю дату.
