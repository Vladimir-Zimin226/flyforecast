from gigachat import GigaChat

from app.config import get_settings
from app.schemas import HistoricalSnapshot, WeatherSnapshot
from app.services.predictor import DISCLAIMER, get_factor_summary


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

    if not settings.giga_api_key:
        return fallback_explanation(
            target_date=target_date,
            decision=decision,
            probability_flight=probability_flight,
            confidence=confidence,
            horizon_days=horizon_days,
            weather=weather,
            history=history,
        )

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
        "Не обещай выполнение рейса. Не называй сервис официальным источником. "
        "Не придумывай факты. Не меняй вероятность и решение. "
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
            return text.strip()

    except Exception:
        pass

    return fallback_explanation(
        target_date=target_date,
        decision=decision,
        probability_flight=probability_flight,
        confidence=confidence,
        horizon_days=horizon_days,
        weather=weather,
        history=history,
    )
