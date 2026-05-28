# Fog-risk dataset for Mendeleyevo

Документ фиксирует решение по погодным источникам для аэропорта Менделеево и pipeline для признаков тумана/низкой облачности.

## Что решили

- FlightRadar24 не используем в ближайшей итерации: история стоит слишком дорого для текущего MVP.
- Himawari оставляем как исследовательский эксперимент на будущее, не блокируем текущую модель.
- Главный практический источник сейчас - Open-Meteo по координатам Менделеево.
- Для ближнего горизонта используем forecast weather.
- Для истории используем Open-Meteo Archive как приближение фактической погоды дня.

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

Эти признаки используются в baseline-поправке `mvp-baseline-002`.

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

Open-Meteo Archive может не отдавать историческую `visibility` для точки Менделеево. Поэтому historical fog-risk строится не только на видимости, но и на proxy-признаках:

- высокая влажность;
- маленькая разница температуры и точки росы;
- высокая низкая облачность;
- weather code;
- слабый/опасный ветер;
- осадки.

Это не заменяет официальный METAR/TAF Менделеево, но дает числовой признак именно по координатам Кунашира, что полезнее для сервиса, чем авиационная погода Южно-Сахалинска.

## Интерфейс

В интерфейсе разделяем режимы:

- `0-15` дней: прогноз с учетом погодной модели;
- `16+` дней: климатико-историческая оценка риска.

Так сервис не притворяется, что знает точную погоду на август или более дальнюю дату.
