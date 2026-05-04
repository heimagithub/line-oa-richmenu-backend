from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import uuid


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64urldecode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _create_token(
    user_id: str,
    role: str,
    token_type: str,
    ttl_seconds: int,
    token_version: int = 0,
) -> str:
    secret = os.environ["JWT_SECRET"].encode("utf-8")
    header = {"alg": "HS256", "typ": "JWT"}
    # jti 為每張 token 獨立識別，未來可用於黑名單；tokver 對應 user 紀錄上的 tokenVersion，
    # 一旦使用者 logout 或主動撤銷，就會增加版本號使舊 token 失效。
    payload = {
        "sub": user_id,
        "role": role,
        "typ": token_type,
        "tokver": int(token_version),
        "jti": uuid.uuid4().hex,
        "exp": int(time.time()) + ttl_seconds,
    }
    h = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    p = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    message = f"{h}.{p}".encode("utf-8")
    sig = _b64url(hmac.new(secret, message, hashlib.sha256).digest())
    return f"{h}.{p}.{sig}"


def create_access_token(
    user_id: str, role: str, ttl_seconds: int = 7200, token_version: int = 0
) -> str:
    return _create_token(
        user_id=user_id,
        role=role,
        token_type="access",
        ttl_seconds=ttl_seconds,
        token_version=token_version,
    )


def create_refresh_token(
    user_id: str, role: str, ttl_seconds: int = 7 * 24 * 3600, token_version: int = 0
) -> str:
    return _create_token(
        user_id=user_id,
        role=role,
        token_type="refresh",
        ttl_seconds=ttl_seconds,
        token_version=token_version,
    )


def decode_token(token: str, expected_type: str | None = None) -> dict:
    secret = os.environ["JWT_SECRET"].encode("utf-8")
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("invalid token")
    h, p, s = parts
    expected = _b64url(hmac.new(secret, f"{h}.{p}".encode("utf-8"), hashlib.sha256).digest())
    if not hmac.compare_digest(s, expected):
        raise ValueError("invalid signature")
    payload = json.loads(_b64urldecode(p))
    if payload.get("exp", 0) < int(time.time()):
        raise ValueError("token expired")
    if expected_type and payload.get("typ") != expected_type:
        raise ValueError("invalid token type")
    return payload


def decode_access_token(token: str) -> dict:
    return decode_token(token, expected_type="access")


def decode_refresh_token(token: str) -> dict:
    return decode_token(token, expected_type="refresh")
