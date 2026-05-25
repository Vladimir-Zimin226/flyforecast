import hashlib
import hmac
import json
import secrets
import time
from datetime import datetime
from pathlib import Path

import psycopg
from fastapi import HTTPException
from psycopg.rows import dict_row

from app.config import get_settings
from app.schemas import validate_email_address


def _now() -> str:
    return datetime.now().isoformat()


def _normalize_email(email: str) -> str:
    try:
        return validate_email_address(email)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid email") from exc


def _connect() -> psycopg.Connection:
    return psycopg.connect(get_settings().database_url, row_factory=dict_row)


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


def _iso(value) -> str | None:
    if isinstance(value, str):
        return value
    return value.isoformat() if value is not None else None


def _public_user(row: dict) -> dict:
    return {
        "name": row["name"],
        "email": row["email"],
        "prediction_count": int(row.get("prediction_count", 0)),
        "feedback_count": int(row.get("feedback_count", 0)),
        "registered_at": _iso(row["registered_at"]) or "",
        "personal_data_consent": bool(row.get("personal_data_consent", False)),
        "analytics_consent": bool(row.get("analytics_consent", False)),
    }


def _admin_user(row: dict) -> dict:
    feedbacks = []
    for feedback in row.get("feedbacks", []):
        feedbacks.append(
            {
                "id": int(feedback["id"]),
                "message": feedback["message"],
                "created_at": _iso(feedback["created_at"]) or "",
            }
        )

    public = _public_user(row)
    public.update(
        {
            "updated_at": _iso(row["updated_at"]) or "",
            "last_prediction_at": _iso(row.get("last_prediction_at")),
            "last_feedback_at": _iso(row.get("last_feedback_at")),
            "feedbacks": feedbacks,
        }
    )
    return public


def init_database() -> None:
    last_error: Exception | None = None

    for _ in range(30):
        try:
            with _connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS users (
                            email TEXT PRIMARY KEY,
                            name TEXT NOT NULL,
                            password_hash TEXT NOT NULL,
                            registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                            prediction_count INTEGER NOT NULL DEFAULT 0,
                            feedback_count INTEGER NOT NULL DEFAULT 0,
                            personal_data_consent BOOLEAN NOT NULL DEFAULT false,
                            analytics_consent BOOLEAN NOT NULL DEFAULT false
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS feedback (
                            id BIGSERIAL PRIMARY KEY,
                            email TEXT NOT NULL REFERENCES users(email)
                                ON UPDATE CASCADE ON DELETE CASCADE,
                            message TEXT NOT NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS consents (
                            id BIGSERIAL PRIMARY KEY,
                            email TEXT NULL,
                            event TEXT NOT NULL,
                            personal_data_consent BOOLEAN NULL,
                            necessary_cookies_ack BOOLEAN NULL,
                            analytics_consent BOOLEAN NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS predictions (
                            id BIGSERIAL PRIMARY KEY,
                            email TEXT NULL REFERENCES users(email)
                                ON UPDATE CASCADE ON DELETE SET NULL,
                            request_id TEXT NOT NULL,
                            target_date DATE NOT NULL,
                            horizon_days INTEGER NOT NULL,
                            probability_flight DOUBLE PRECISION NOT NULL,
                            decision TEXT NOT NULL,
                            confidence TEXT NOT NULL,
                            model_version TEXT NOT NULL,
                            data_version TEXT NOT NULL,
                            session_prediction_number INTEGER NOT NULL,
                            utm_source TEXT NULL,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        )
                        """
                    )
                conn.commit()

            migrate_json_users()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(1)

    raise RuntimeError("Could not initialize Postgres user database") from last_error


def migrate_json_users() -> None:
    path = Path(get_settings().user_store_path)
    migrated_marker = path.with_suffix(".migrated")

    if not path.exists() or migrated_marker.exists():
        return

    with path.open("r", encoding="utf-8") as file:
        users = json.load(file)

    with _connect() as conn:
        with conn.cursor() as cur:
            for user in users.values():
                email = _normalize_email(user["email"])
                cur.execute(
                    """
                    INSERT INTO users (
                        email, name, password_hash, registered_at, updated_at,
                        prediction_count, feedback_count,
                        personal_data_consent, analytics_consent
                    )
                    VALUES (%s, %s, %s, %s, now(), %s, %s, %s, %s)
                    ON CONFLICT (email) DO NOTHING
                    """,
                    (
                        email,
                        user.get("name", email),
                        user["password_hash"],
                        user.get("registered_at") or _now(),
                        int(user.get("prediction_count", 0)),
                        int(user.get("feedback_count", 0)),
                        bool(user.get("personal_data_consent", False)),
                        bool(user.get("analytics_consent", False)),
                    ),
                )
        conn.commit()

    migrated_marker.write_text(_now(), encoding="utf-8")


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

    with _connect() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (
                        email, name, password_hash, prediction_count,
                        personal_data_consent, analytics_consent
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (
                        normalized_email,
                        name.strip(),
                        _hash_password(password),
                        initial_prediction_count,
                        personal_data_consent,
                        analytics_consent,
                    ),
                )
                user = cur.fetchone()
            conn.commit()
        except psycopg.errors.UniqueViolation as exc:
            raise HTTPException(status_code=409, detail="User with this email already exists") from exc

    log_consent(
        email=normalized_email,
        event="registration",
        personal_data_consent=personal_data_consent,
        analytics_consent=analytics_consent,
    )

    return _public_user(user)


def authenticate_user(email: str, password: str) -> dict:
    normalized_email = _normalize_email(email)
    settings = get_settings()

    if (
        normalized_email == _normalize_email(settings.admin_email)
        and hmac.compare_digest(password, settings.admin_password)
    ):
        return {
            "name": "Администратор",
            "email": normalized_email,
            "prediction_count": 0,
            "feedback_count": 0,
            "registered_at": _now(),
            "personal_data_consent": True,
            "analytics_consent": False,
        }

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (normalized_email,))
            user = cur.fetchone()

    if user is None or not _verify_password(password, user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    return _public_user(user)


def get_user(email: str) -> dict:
    normalized_email = _normalize_email(email)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (normalized_email,))
            user = cur.fetchone()

    if user is None:
        raise HTTPException(status_code=404, detail="User not found")

    return _public_user(user)


def increment_prediction_count(email: str) -> int:
    normalized_email = _normalize_email(email)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users
                SET prediction_count = prediction_count + 1, updated_at = now()
                WHERE email = %s
                RETURNING prediction_count
                """,
                (normalized_email,),
            )
            row = cur.fetchone()
        conn.commit()

    if row is None:
        raise HTTPException(status_code=404, detail="User not found")

    return int(row["prediction_count"])


def save_feedback(email: str, message: str) -> None:
    normalized_email = _normalize_email(email)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO feedback (email, message) VALUES (%s, %s)",
                (normalized_email, message.strip()),
            )
            cur.execute(
                """
                UPDATE users
                SET feedback_count = feedback_count + 1, updated_at = now()
                WHERE email = %s
                """,
                (normalized_email,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="User not found")
        conn.commit()


def log_consent(
    *,
    email: str | None,
    event: str,
    personal_data_consent: bool | None = None,
    necessary_cookies_ack: bool | None = None,
    analytics_consent: bool | None = None,
) -> None:
    normalized_email = _normalize_email(email) if email else None

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO consents (
                    email, event, personal_data_consent,
                    necessary_cookies_ack, analytics_consent
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    normalized_email,
                    event,
                    personal_data_consent,
                    necessary_cookies_ack,
                    analytics_consent,
                ),
            )
        conn.commit()


def save_prediction_event(
    *,
    email: str,
    request_id: str,
    target_date: str,
    horizon_days: int,
    probability_flight: float,
    decision: str,
    confidence: str,
    model_version: str,
    data_version: str,
    session_prediction_number: int,
    utm_source: str | None,
) -> None:
    normalized_email = _normalize_email(email)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO predictions (
                    email, request_id, target_date, horizon_days,
                    probability_flight, decision, confidence,
                    model_version, data_version, session_prediction_number, utm_source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    normalized_email,
                    request_id,
                    target_date,
                    horizon_days,
                    probability_flight,
                    decision,
                    confidence,
                    model_version,
                    data_version,
                    session_prediction_number,
                    utm_source,
                ),
            )
        conn.commit()


def list_admin_users() -> dict:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    u.*,
                    (
                        SELECT max(p.created_at)
                        FROM predictions p
                        WHERE p.email = u.email
                    ) AS last_prediction_at,
                    (
                        SELECT max(f.created_at)
                        FROM feedback f
                        WHERE f.email = u.email
                    ) AS last_feedback_at,
                    coalesce(
                        (
                            SELECT json_agg(
                                json_build_object(
                                    'id', f.id,
                                    'message', f.message,
                                    'created_at', f.created_at
                                )
                                ORDER BY f.created_at DESC
                            )
                            FROM feedback f
                            WHERE f.email = u.email
                        ),
                        '[]'::json
                    ) AS feedbacks
                FROM users u
                ORDER BY u.registered_at DESC
                """
            )
            users = [_admin_user(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT
                    count(*) AS total_users,
                    coalesce(sum(prediction_count), 0) AS total_predictions,
                    coalesce(sum(feedback_count), 0) AS total_feedback,
                    count(*) FILTER (WHERE analytics_consent) AS analytics_consents
                FROM users
                """
            )
            summary = cur.fetchone()

    return {
        "total_users": int(summary["total_users"]),
        "total_predictions": int(summary["total_predictions"]),
        "total_feedback": int(summary["total_feedback"]),
        "analytics_consents": int(summary["analytics_consents"]),
        "users": users,
    }


def update_admin_user(email: str, changes: dict) -> dict:
    normalized_email = _normalize_email(email)
    next_email = _normalize_email(changes["email"]) if changes.get("email") else normalized_email

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (normalized_email,))
            current = cur.fetchone()
            if current is None:
                raise HTTPException(status_code=404, detail="User not found")

            fields = {
                "email": next_email,
                "name": changes.get("name", current["name"]),
                "password_hash": _hash_password(changes["password"])
                if changes.get("password")
                else current["password_hash"],
                "prediction_count": changes.get("prediction_count", current["prediction_count"]),
                "feedback_count": changes.get("feedback_count", current["feedback_count"]),
                "personal_data_consent": changes.get(
                    "personal_data_consent",
                    current["personal_data_consent"],
                ),
                "analytics_consent": changes.get("analytics_consent", current["analytics_consent"]),
            }

            try:
                cur.execute(
                    """
                    UPDATE users
                    SET email = %s, name = %s, password_hash = %s,
                        prediction_count = %s, feedback_count = %s,
                        personal_data_consent = %s, analytics_consent = %s,
                        updated_at = now()
                    WHERE email = %s
                    RETURNING *
                    """,
                    (
                        fields["email"],
                        fields["name"].strip(),
                        fields["password_hash"],
                        fields["prediction_count"],
                        fields["feedback_count"],
                        fields["personal_data_consent"],
                        fields["analytics_consent"],
                        normalized_email,
                    ),
                )
                user = cur.fetchone()
                conn.commit()
            except psycopg.errors.UniqueViolation as exc:
                raise HTTPException(status_code=409, detail="User with this email already exists") from exc

    return _admin_user({**user, "last_prediction_at": None, "last_feedback_at": None})


def delete_admin_user(email: str) -> None:
    normalized_email = _normalize_email(email)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM users WHERE email = %s", (normalized_email,))
            deleted = cur.rowcount
        conn.commit()

    if deleted == 0:
        raise HTTPException(status_code=404, detail="User not found")
