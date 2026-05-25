import hashlib
import hmac
import json
import secrets
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException

from app.config import get_settings


def _now() -> str:
    return datetime.now().isoformat()


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _load_users() -> dict:
    path = Path(get_settings().user_store_path)
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _save_users(users: dict) -> None:
    path = Path(get_settings().user_store_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_suffix(".tmp")
    with temp_path.open("w", encoding="utf-8") as file:
        json.dump(users, file, ensure_ascii=False, indent=2)

    temp_path.replace(path)


def _hash_password(password: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        120_000,
    ).hex()

    return f"{salt}${digest}"


def _verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt, expected_digest = stored_hash.split("$", 1)
    except ValueError:
        return False

    actual_hash = _hash_password(password, salt)
    _, actual_digest = actual_hash.split("$", 1)
    return hmac.compare_digest(actual_digest, expected_digest)


def _log_event(path_value: str, row: dict) -> None:
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(row, ensure_ascii=False) + "\n")


def public_user(user: dict) -> dict:
    return {
        "name": user["name"],
        "email": user["email"],
        "prediction_count": int(user.get("prediction_count", 0)),
        "feedback_count": int(user.get("feedback_count", 0)),
        "registered_at": user["registered_at"],
        "personal_data_consent": bool(user.get("personal_data_consent", False)),
        "analytics_consent": bool(user.get("analytics_consent", False)),
    }


def register_user(
    *,
    name: str,
    email: str,
    password: str,
    personal_data_consent: bool,
    analytics_consent: bool,
    initial_prediction_count: int = 0,
) -> dict:
    if not personal_data_consent:
        raise HTTPException(
            status_code=400,
            detail="Personal data processing consent is required",
        )

    normalized_email = _normalize_email(email)
    if "@" not in normalized_email or "." not in normalized_email.rsplit("@", 1)[-1]:
        raise HTTPException(status_code=400, detail="Invalid email")

    users = _load_users()
    if normalized_email in users:
        raise HTTPException(status_code=409, detail="User with this email already exists")

    user = {
        "name": name.strip(),
        "email": normalized_email,
        "password_hash": _hash_password(password),
        "registered_at": _now(),
        "prediction_count": initial_prediction_count,
        "feedback_count": 0,
        "personal_data_consent": personal_data_consent,
        "analytics_consent": analytics_consent,
    }

    users[normalized_email] = user
    _save_users(users)
    log_consent(
        email=normalized_email,
        event="registration",
        personal_data_consent=personal_data_consent,
        analytics_consent=analytics_consent,
    )

    return public_user(user)


def authenticate_user(email: str, password: str) -> dict:
    normalized_email = _normalize_email(email)
    users = _load_users()
    user = users.get(normalized_email)

    if user is None or not _verify_password(password, user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    return public_user(user)


def get_user(email: str) -> dict:
    normalized_email = _normalize_email(email)
    users = _load_users()
    user = users.get(normalized_email)

    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    return public_user(user)


def increment_prediction_count(email: str) -> int:
    normalized_email = _normalize_email(email)
    users = _load_users()
    user = users.get(normalized_email)

    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    user["prediction_count"] = int(user.get("prediction_count", 0)) + 1
    _save_users(users)
    return int(user["prediction_count"])


def save_feedback(email: str, message: str) -> None:
    normalized_email = _normalize_email(email)
    users = _load_users()
    user = users.get(normalized_email)

    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    user["feedback_count"] = int(user.get("feedback_count", 0)) + 1
    _save_users(users)

    _log_event(
        get_settings().feedback_log_path,
        {
            "created_at": _now(),
            "email": normalized_email,
            "name": user["name"],
            "message": message.strip(),
        },
    )


def log_consent(
    *,
    email: str | None,
    event: str,
    personal_data_consent: bool | None = None,
    necessary_cookies_ack: bool | None = None,
    analytics_consent: bool | None = None,
) -> None:
    _log_event(
        get_settings().consent_log_path,
        {
            "created_at": _now(),
            "email": _normalize_email(email) if email else None,
            "event": event,
            "personal_data_consent": personal_data_consent,
            "necessary_cookies_ack": necessary_cookies_ack,
            "analytics_consent": analytics_consent,
        },
    )
