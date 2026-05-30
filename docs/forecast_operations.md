# Forecast operations notes

Документ фиксирует эксплуатационные правила для пользовательских прогнозов и ежедневного forecast monitor.

## Погодный guardrail

- На ближнем горизонте `0-15` дней прогноз зависит от forecast weather snapshot.
- Основной источник погоды — Open-Meteo forecast API.
- Backend хранит persistent SQLite-кэш Open-Meteo в `WEATHER_FORECAST_CACHE_PATH`.
- При успешном live-запросе Open-Meteo backend сохраняет дневные weather snapshots на весь ближний горизонт `0-15` дней.
- Если свежий кэш младше `WEATHER_CACHE_FRESH_HOURS`, `/predict` использует его без live-запроса к Open-Meteo.
- Если Open-Meteo недоступен, backend может использовать stale-кэш до `WEATHER_CACHE_STALE_HOURS`.
- Если Open-Meteo и кэш недоступны, backend пробует резервный источник MET Norway / Yr Locationforecast для горизонта `0-9` дней.
- MET Norway fallback не содержит прямой `visibility`, поэтому fog-risk в этом режиме считается по доступным proxy-признакам: низкая облачность, влажность, dew point spread, ветер, осадки и `fog_area_fraction`, если он есть.
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

- Open-Meteo snapshot по Менделеево включает `visibility`, `cloud_cover_low`, `weather_code`, `temperature_2m`, `dew_point_2m`, `relative_humidity_2m`, осадки и ветер.
- Backend рассчитывает `dew_point_spread`, `fog_low_cloud_risk_score` и `fog_low_cloud_risk_level`.
- Baseline `mvp-baseline-002` использует эти признаки как дополнительную отрицательную поправку на риск тумана и низкой облачности.
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
