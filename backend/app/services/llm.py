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
PROMPT_VERSION = "explanation-v2-no-seat-availability"
CACHE_SCHEMA_VERSION = 1
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


def _is_safe_explanation(text: str) -> bool:
    normalized = " ".join(text.split()).lower()
    return not any(pattern.search(normalized) for pattern in FORBIDDEN_EXPLANATION_PATTERNS)


def fallback_explanation(
    target_date: str,
    decision: str,
    probability_flight: float,
    confidence: str,
    horizon_days: int,
    weather: WeatherSnapshot,
    history: HistoricalSnapshot,
) -> str:
    decision_ru = "Да" if decision == "yes" else "Нет"
    probability_percent = round(probability_flight * 100)

    if horizon_days > 46:
        horizon_text = "Дата далеко в будущем, поэтому оценка основана в основном на исторической полётности и сезонности."
    elif weather.available:
        horizon_text = "Для даты доступен погодный прогноз, поэтому в оценке учтены погодные признаки."
    else:
        horizon_text = "Погодный прогноз для даты недоступен, поэтому оценка опирается на историю похожих дат."

    return (
        f"{decision_ru}, вероятность выполнения рейса — {probability_percent}%. "
        f"Уверенность: {confidence}. {horizon_text} "
        f"Это ориентир для планирования, а не гарантия."
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
        "factors": factors,
        "disclaimer": DISCLAIMER,
    }

    system_message = (
        "Ты пишешь короткое объяснение для сервиса «Летит на Курилы?». "
        "Объясняй только вероятность выполнения или невыполнения рейса по погоде, истории и сезонности. "
        "Не обещай выполнение рейса. Не называй сервис официальным источником. "
        "Не придумывай факты. Не меняй вероятность и решение. "
        "Строго запрещено упоминать билеты, наличие мест, пассажиров, бронирование, салон или продажи. "
        "Если decision=no, пиши, что риск отмены или невыполнения выше, а не что нет мест. "
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
            if _is_safe_explanation(explanation):
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
