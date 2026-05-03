import base64
import hashlib
import hmac
import json
import time
from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import get_settings

security = HTTPBearer()


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def create_token(username: str, ttl_seconds: int = 60 * 60 * 24) -> str:
    settings = get_settings()
    payload = {
        "sub": username,
        "exp": int(time.time()) + ttl_seconds,
    }

    payload_raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    payload_b64 = _b64encode(payload_raw)

    signature = hmac.new(
        settings.jwt_secret.encode("utf-8"),
        payload_b64.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    return f"{payload_b64}.{_b64encode(signature)}"


def verify_token(token: str) -> str:
    settings = get_settings()

    try:
        payload_b64, signature_b64 = token.split(".", 1)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc

    expected_signature = hmac.new(
        settings.jwt_secret.encode("utf-8"),
        payload_b64.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    try:
        actual_signature = _b64decode(signature_b64)
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc

    if not hmac.compare_digest(expected_signature, actual_signature):
        raise HTTPException(status_code=401, detail="Invalid token signature")

    try:
        payload = json.loads(_b64decode(payload_b64))
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid token payload") from exc

    if int(payload.get("exp", 0)) < int(time.time()):
        raise HTTPException(status_code=401, detail="Token expired")

    return str(payload["sub"])


def require_user(credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)]) -> str:
    return verify_token(credentials.credentials)