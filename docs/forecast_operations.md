# Forecast operations notes

Документ фиксирует эксплуатационные правила для пользовательских прогнозов и ежедневного forecast monitor.

## Погодный guardrail

- На ближнем горизонте `0-15` дней прогноз зависит от forecast weather snapshot.
- Основной источник погоды — Open-Meteo forecast API.
- Backend хранит persistent SQLite-кэш Open-Meteo в `WEATHER_FORECAST_CACHE_PATH`.
- При успешном live-запросе Open-Meteo backend сохраняет weather snapshots на весь ближний горизонт `0-15` дней.
- Open-Meteo hourly-прогноз анализируется не за все сутки, а за рабочее окно `WEATHER_FORECAST_WINDOW_START_HOUR`-`WEATHER_FORECAST_WINDOW_END_HOUR` по локальному времени аэропорта. По умолчанию это `08:00-20:00`.
- Внутри рабочего окна backend ищет непрерывное погодное окно для рейса: по умолчанию минимум `WEATHER_FLIGHT_WINDOW_MIN_HOURS=3` часа подряд с видимостью от `5000 м`, низкой облачностью не выше `80%`, порывами не выше `45 км/ч`, без сильных осадков и без fog weather code. Если такое окно найдено, погодные признаки для решения агрегируются по нему.
- Если пригодное окно не найдено, признаки агрегируются по всему рабочему окну; для `visibility` используется нижний квартиль, а не абсолютный суточный минимум.
- Если свежий кэш младше `WEATHER_CACHE_FRESH_HOURS`, `/predict` использует его без live-запроса к Open-Meteo.
- Для production цель — чаще уточнять текущий/завтрашний прогноз: табло и forecast monitor запускаются каждые `900` секунд, а fresh weather cache живет `1` час.
- Forecast monitor обновляет сохраненную строку прогноза для горизонтов `0-1` день, пока по табло еще нет финального outcome. Для дальних горизонтов строка остается дневным snapshot.
- Если live-обновление Open-Meteo падает, backend открывает circuit breaker на `OPEN_METEO_FAILURE_COOLDOWN_MINUTES`: в это время повторные запросы к Open-Meteo пропускаются, а сервис сразу использует stale-кэш или fallback.
- Если Open-Meteo недоступен, backend может использовать stale-кэш до `WEATHER_CACHE_STALE_HOURS`.
- Если Open-Meteo и кэш недоступны, backend пробует резервный источник MET Norway / Yr Locationforecast для горизонта `0-9` дней.
- MET Norway fallback использует тот же поиск погодного окна, но не содержит прямой `visibility`, поэтому fog-risk в этом режиме считается по доступным proxy-признакам: низкая облачность, влажность, dew point spread, ветер, осадки и `fog_area_fraction`, если он есть.
- Если ни live Open-Meteo, ни кэш, ни MET Norway fallback не дали weather snapshot на горизонте `0-15` дней, публичный `/predict` возвращает пользователю сообщение: "Временные проблемы с погодными сервисами, предсказания пока недоступны."
- Forecast monitor на горизонте `0-15` дней тоже пропускает запись прогноза, если weather snapshot недоступен после всех fallback-слоёв.
- На дальнем горизонте `16+` дней прогнозы строятся без погодного API, по историческим данным и сезонности. Это важно для оценки качества дальних прогнозов на горизонтах `0-45`, `60` и `90` дней.
- В интерфейсе ближний режим отображается как "Прогноз с учетом погодной модели", дальний режим - как "Климатико-историческая оценка риска".

## Weather fallback order

Порядок выбора weather snapshot:

1. Fresh Open-Meteo cache.
2. Live Open-Meteo refresh с сохранением всего горизонта `0-15` дней.
3. Stale Open-Meteo cache.
4. MET Norway fallback для `0-9` дней.
5. Weather unavailable.

Источник snapshot сохраняется в `weather.source`:

| source | meaning |
| --- | --- |
| `open-meteo` | live Open-Meteo response |
| `open-meteo-cache` | fresh cached Open-Meteo snapshot |
| `open-meteo-cache-stale` | stale cached Open-Meteo snapshot after live Open-Meteo failure |
| `met-no` | MET Norway fallback |
| `weather-unavailable` | no weather snapshot available |

## Fog/low-cloud признаки

- Open-Meteo snapshot по Менделеево включает `visibility`, `cloud_cover_low`, `weather_code`, `temperature_2m`, `dew_point_2m`, `relative_humidity_2m`, осадки и ветер, агрегированные в рабочем окне прогноза.
- Backend рассчитывает `dew_point_spread`, `fog_low_cloud_risk_score` и `fog_low_cloud_risk_level`.
- Baseline `mvp-baseline-004` использует эти признаки как дополнительную отрицательную поправку на риск тумана и низкой облачности.
- Для исторического анализа используется `pipelines/training/build_mendeleyevo_fog_risk_dataset.py`.
- Historical visibility берётся из Open-Meteo Historical Forecast API, потому что обычный Open-Meteo Archive возвращает `visibility` пустой/undefined.
- Для дат без historical visibility fog-risk продолжает учитывать proxy-признаки: влажность, точку росы, низкую облачность, weather code, осадки и ветер.

## Кэш объяснений

- GigaChat используется только для пользовательского текста объяснения, а не для расчета решения, вероятности или confidence.
- Объяснение кэшируется в SQLite-файле `EXPLANATION_CACHE_PATH`.
- Ключ кэша считается по версии prompt, версии baseline-модели, версии данных, дате, решению, вероятности, confidence, горизонту, weather snapshot и historical snapshot.
- Если погода или входные признаки изменились, ключ меняется и объяснение генерируется заново.
- Если несколько пользователей в течение дня запрашивают одинаковый прогноз с теми же входными данными, повторный вызов GigaChat не нужен: backend возвращает кэшированный текст.
- Если GigaChat вернул текст с запрещенными доменными галлюцинациями, например про билеты или места, текст отклоняется, а в кэш сохраняется безопасный fallback.
