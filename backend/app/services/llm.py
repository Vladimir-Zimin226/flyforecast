import hashlib
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from gigachat import GigaChat

from app.config import get_settings
from app.schemas import HistoricalSnapshot, WeatherSnapshot
from app.services.predictor import DATA_VERSION, DISCLAIMER, MODEL_VERSION, get_factor_summary


logger = logging.getLogger("flyforecast.llm")
PROMPT_VERSION = "explanation-v4-specific-weather-history"
CACHE_SCHEMA_VERSION = 2
FORBIDDEN_EXPLANATION_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bнет\s+мест\b",
        r"\bмест[ао]?\b",
        r"\bбилет",
        r"\bпассажир",
        r"\bброн",
        r"\bсалон",
    )
]
DECISION_CONTRADICTION_PATTERNS = {
    "yes": [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"риск\s+(отмены|невыполнения|задержки)(\s+или\s+(отмены|невыполнения|задержки))?\s+(рейса\s+)?(выше|повышен|больше)",
            r"вероятн\w*\s+(не\s+полетит|не\s+выполнится|будет\s+отмен)",
            r"низк\w+\s+вероятност\w+\s+выполн",
            r"маловероятн\w+",
            r"скорее\s+не",
        )
    ],
    "no": [
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"скорее\s+(да|полетит|выполнится)",
            r"высок\w+\s+шанс\w+\s+(вылета|выполнения)",
            r"благоприятн\w+\s+окн",
            r"риск\s+отмены\s+(ниже|невысок)",
        )
    ],
}


def _snapshot_dict(snapshot: WeatherSnapshot | HistoricalSnapshot) -> dict[str, Any]:
    return snapshot.model_dump(mode="json")


def _cache_key(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _connect_cache(path: str) -> sqlite3.Connection:
    cache_path = Path(path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cache_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS explanation_cache (
            cache_key TEXT PRIMARY KEY,
            explanation TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    return conn


def _get_cached_explanation(cache_path: str, key: str) -> str | None:
    try:
        with _connect_cache(cache_path) as conn:
            row = conn.execute(
                "SELECT explanation FROM explanation_cache WHERE cache_key = ?",
                (key,),
            ).fetchone()
    except sqlite3.Error as exc:
        logger.warning("explanation_cache_read_failed error=%s", exc)
        return None

    return str(row[0]) if row else None


def _store_cached_explanation(cache_path: str, key: str, explanation: str) -> None:
    try:
        with _connect_cache(cache_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO explanation_cache (cache_key, explanation, created_at)
                VALUES (?, ?, ?)
                """,
                (key, explanation, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
    except sqlite3.Error as exc:
        logger.warning("explanation_cache_write_failed error=%s", exc)


def _is_safe_explanation(text: str, decision: str) -> bool:
    normalized = " ".join(text.split()).lower()
    if any(pattern.search(normalized) for pattern in FORBIDDEN_EXPLANATION_PATTERNS):
        return False

    contradiction_patterns = DECISION_CONTRADICTION_PATTERNS.get(decision, [])
    return not any(pattern.search(normalized) for pattern in contradiction_patterns)


def _format_percent(value: float | None) -> str | None:
    if value is None:
        return None
    return f"{round(value)}%"


def _format_number(value: float | None, unit: str = "") -> str | None:
    if value is None:
        return None
    rounded = round(value, 1)
    if rounded == round(rounded):
        rounded = int(rounded)
    return f"{rounded}{unit}"


def _weather_window_text(weather: WeatherSnapshot) -> str:
    if (
        weather.flight_window_available
        and weather.flight_window_start_hour is not None
        and weather.flight_window_end_hour is not None
    ):
        return f"найдено погодное окно {weather.flight_window_start_hour:02d}:00-{weather.flight_window_end_hour:02d}:00"

    if weather.flight_window_available is False:
        return "устойчивого погодного окна в рабочем интервале не найдено"

    if weather.aggregation_window_start_hour is not None and weather.aggregation_window_end_hour is not None:
        return f"оценено рабочее окно {weather.aggregation_window_start_hour:02d}:00-{weather.aggregation_window_end_hour:02d}:00"

    return "оценены доступные погодные признаки"


def _fog_text(weather: WeatherSnapshot) -> str:
    risk_labels = {"low": "низкий", "medium": "средний", "high": "высокий"}
    risk = risk_labels.get(weather.fog_low_cloud_risk_level or "", weather.fog_low_cloud_risk_level)
    explicit_fog = weather.weather_code is not None and int(weather.weather_code) in {45, 48}

    if explicit_fog:
        return "в погодном коде есть туман"

    if weather.fog_low_cloud_risk_level == "high":
        return "есть высокий риск тумана или низкой облачности"

    if weather.fog_low_cloud_risk_level == "medium":
        return "есть средний риск тумана или низкой облачности"

    if risk:
        return f"туман не выглядит главным риском: риск {risk}"

    return "явных признаков тумана в прогнозе нет"


def _weather_details_text(weather: WeatherSnapshot) -> str:
    details: list[str] = []

    visibility = weather.flight_window_visibility if weather.flight_window_visibility is not None else weather.visibility
    if visibility is not None:
        details.append(f"видимость около {_format_number(visibility, ' м')}")

    cloud_low = (
        weather.flight_window_cloud_cover_low
        if weather.flight_window_cloud_cover_low is not None
        else weather.cloud_cover_low
    )
    if cloud_low is not None:
        details.append(f"низкая облачность {_format_percent(cloud_low)}")

    if weather.wind_gusts_10m is not None:
        details.append(f"порывы ветра до {_format_number(weather.wind_gusts_10m, ' км/ч')}")

    if weather.relative_humidity_2m is not None:
        details.append(f"влажность {_format_percent(weather.relative_humidity_2m)}")

    if weather.dew_point_spread is not None:
        details.append(f"разница температуры и точки росы {_format_number(weather.dew_point_spread, ' °C')}")

    if not details:
        return "конкретные погодные показатели в ответе источника неполные"

    return ", ".join(details)


def _history_details_text(history: HistoricalSnapshot) -> str:
    historical_percent = round(history.historical_probability_flight * 100)

    if history.similar_days_count > 0:
        return (
            f"исторически в похожие даты было {history.completed_count} выполненных "
            f"и {history.cancelled_count} отмененных дней из {history.similar_days_count}, "
            f"то есть около {historical_percent}% выполнений"
        )

    return f"историческая оценка для похожих дат около {historical_percent}% выполнений"


def _has_specific_weather_details(text: str, weather: WeatherSnapshot) -> bool:
    if not weather.available:
        return True

    normalized = text.lower()
    has_number = bool(re.search(r"\d", normalized))
    has_weather_term = any(
        term in normalized
        for term in ("видим", "облач", "туман", "ветер", "порыв", "влажн", "рос")
    )
    return has_number and has_weather_term


def fallback_explanation(
    target_date: str,
    decision: str,
    probability_flight: float,
    confidence: str,
    horizon_days: int,
    weather: WeatherSnapshot,
    history: HistoricalSnapshot,
) -> str:
    probability_percent = round(probability_flight * 100)
    history_text = _history_details_text(history)

    if not weather.available:
        horizon_text = (
            "Точного погодного прогноза на эту дату пока нет, поэтому оценка опирается на историю и сезонность."
        )
    elif weather.available:
        horizon_text = (
            f"По погоде: {_weather_window_text(weather)}, {_weather_details_text(weather)}; {_fog_text(weather)}."
        )
    else:
        horizon_text = "Погодный прогноз для даты недоступен, поэтому оценка опирается на историю похожих дат."

    if decision == "yes":
        return (
            f"Да: вероятность выполнения рейса {probability_percent}%, поэтому дата выглядит скорее подходящей для вылета. "
            f"{horizon_text} {history_text}. Это ориентир для планирования, а не гарантия."
        )

    return (
        f"Нет: вероятность выполнения рейса {probability_percent}%, поэтому риск отмены или невыполнения выглядит повышенным. "
        f"{horizon_text} {history_text}. Это ориентир для планирования, а не гарантия."
    )


def generate_user_explanation(
    target_date: str,
    decision: str,
    probability_flight: float,
    confidence: str,
    horizon_days: int,
    weather: WeatherSnapshot,
    history: HistoricalSnapshot,
) -> str:
    settings = get_settings()
    cache_payload = {
        "schema": CACHE_SCHEMA_VERSION,
        "prompt_version": PROMPT_VERSION,
        "giga_model": settings.giga_model,
        "model_version": MODEL_VERSION,
        "data_version": DATA_VERSION,
        "target_date": target_date,
        "decision": decision,
        "probability_flight": probability_flight,
        "confidence": confidence,
        "horizon_days": horizon_days,
        "weather": _snapshot_dict(weather),
        "history": _snapshot_dict(history),
    }
    key = _cache_key(cache_payload)
    cached = _get_cached_explanation(settings.explanation_cache_path, key)

    if cached:
        logger.info("explanation_cache_hit target_date=%s key=%s", target_date, key[:12])
        return cached

    if not settings.giga_api_key:
        explanation = fallback_explanation(
            target_date=target_date,
            decision=decision,
            probability_flight=probability_flight,
            confidence=confidence,
            horizon_days=horizon_days,
            weather=weather,
            history=history,
        )
        _store_cached_explanation(settings.explanation_cache_path, key, explanation)
        return explanation

    factors = get_factor_summary(weather, history, horizon_days)

    prompt = {
        "date": target_date,
        "decision": decision,
        "probability_flight": probability_flight,
        "confidence": confidence,
        "horizon_days": horizon_days,
        "forecast_mode": "weather_model" if weather.available else "climate_history",
        "factors": factors,
        "disclaimer": DISCLAIMER,
    }

    system_message = (
        "Ты пишешь короткое объяснение для сервиса «Летит на Курилы?». "
        "Объясняй только вероятность выполнения или невыполнения рейса по погоде, истории и сезонности. "
        "Не обещай выполнение рейса. Не называй сервис официальным источником. "
        "Не придумывай факты. Не меняй вероятность и решение. "
        "Обязательно назови 2-4 конкретных погодных показателя из factors, если погодный прогноз доступен: видимость, низкая облачность, порывы ветра, влажность, точка росы или погодное окно. "
        "Обязательно укажи, есть ли риск тумана/низкой облачности или что явных признаков тумана нет. "
        "Обязательно объясни историческую часть через похожие даты и долю выполненных рейсов. "
        "Если forecast_mode=climate_history, честно укажи, что точного прогноза погоды на дату еще нет и оценка основана на истории/сезонности. "
        "Строго запрещено упоминать билеты, наличие мест, пассажиров, бронирование, салон или продажи. "
        "Если decision=yes, объясняй, почему дата выглядит скорее подходящей для вылета; не начинай с повышенного риска отмены. "
        "Если decision=no, пиши, что риск отмены или невыполнения выше, а не что нет мест. "
        "Тон объяснения должен соответствовать decision: yes поддерживает вывод 'Да', no поддерживает вывод 'Нет'. "
        "Не используй Markdown-разметку. "
        "Пиши по-русски, спокойно и понятно. "
        "Максимум 3 предложения."
    )

    try:
        user_message = (
            f"{system_message}\n\n"
            "Сформулируй объяснение результата для пользователя на основе этих данных. "
            f"Данные: {prompt}"
        )

        with GigaChat(
            credentials=settings.giga_api_key,
            scope=settings.giga_scope,
            model=settings.giga_model,
            verify_ssl_certs=settings.giga_verify_ssl_certs,
            timeout=settings.giga_timeout,
        ) as client:
            response = client.chat(user_message)

        text = response.choices[0].message.content if response.choices else None

        if text:
            explanation = text.strip()
            if _is_safe_explanation(explanation, decision) and _has_specific_weather_details(explanation, weather):
                _store_cached_explanation(settings.explanation_cache_path, key, explanation)
                return explanation

            logger.warning(
                "explanation_rejected_forbidden_content target_date=%s key=%s text=%s",
                target_date,
                key[:12],
                explanation,
            )

    except Exception as exc:
        logger.warning("explanation_generation_failed target_date=%s error=%s", target_date, exc)

    explanation = fallback_explanation(
        target_date=target_date,
        decision=decision,
        probability_flight=probability_flight,
        confidence=confidence,
        horizon_days=horizon_days,
        weather=weather,
        history=history,
    )
    _store_cached_explanation(settings.explanation_cache_path, key, explanation)
    return explanation
