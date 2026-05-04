from __future__ import annotations

import base64
import json
import os
from functools import lru_cache

import boto3
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


_VERSION = "v1"
_VERSION_PREFIX = f"{_VERSION}:"


@lru_cache(maxsize=1)
def _kms_client():
    return boto3.client("kms")


def _key_id() -> str:
    key_id = os.environ.get("OA_SECRETS_KMS_KEY_ID")
    if not key_id:
        raise RuntimeError("OA_SECRETS_KMS_KEY_ID is not configured")
    return key_id


def _encryption_context(oa_id: str, field: str) -> dict[str, str]:
    if not oa_id or not field:
        raise ValueError("oa_id and field are required for encryption context")
    return {"oaId": oa_id, "field": field}


def encrypt_secret(plaintext: str, oa_id: str, field: str) -> str:
    """以 KMS envelope 加密 secret。

    回傳格式：v1:<base64(json{ek, iv, ct})>
    EncryptionContext 綁定 oaId + field，避免 cipher swap。
    """
    if plaintext is None:
        raise ValueError("plaintext must not be None")
    context = _encryption_context(oa_id, field)
    resp = _kms_client().generate_data_key(
        KeyId=_key_id(), KeySpec="AES_256", EncryptionContext=context
    )
    dek_plain = resp["Plaintext"]
    dek_enc = resp["CiphertextBlob"]
    try:
        iv = os.urandom(12)
        ct = AESGCM(dek_plain).encrypt(iv, plaintext.encode("utf-8"), None)
    finally:
        del dek_plain
    blob = {
        "ek": base64.b64encode(dek_enc).decode("ascii"),
        "iv": base64.b64encode(iv).decode("ascii"),
        "ct": base64.b64encode(ct).decode("ascii"),
    }
    return _VERSION_PREFIX + base64.b64encode(
        json.dumps(blob, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")


def decrypt_secret(token: str, oa_id: str, field: str) -> str:
    """解密 envelope 密文。

    遇到非 v1: 開頭的字串視為舊明文（migration 期相容），直接回傳。
    完成 migration 後可把這個分支拿掉並改丟例外。
    """
    if not token:
        return ""
    if not token.startswith(_VERSION_PREFIX):
        return token
    payload = base64.b64decode(token[len(_VERSION_PREFIX):])
    blob = json.loads(payload.decode("utf-8"))
    context = _encryption_context(oa_id, field)
    dek_plain = _kms_client().decrypt(
        CiphertextBlob=base64.b64decode(blob["ek"]),
        EncryptionContext=context,
    )["Plaintext"]
    try:
        pt = AESGCM(dek_plain).decrypt(
            base64.b64decode(blob["iv"]),
            base64.b64decode(blob["ct"]),
            None,
        )
    finally:
        del dek_plain
    return pt.decode("utf-8")


def is_encrypted(token: str) -> bool:
    return bool(token) and token.startswith(_VERSION_PREFIX)
