import hashlib
import hmac
import json
import os

import requests


def _linepay_log(msg: str) -> None:
    print(f"[linepay] {msg}", flush=True)


def _mask_key(value: str) -> str:
    if not value:
        return "(empty)"
    if len(value) <= 6:
        return "***"
    return f"{value[:4]}...{value[-2:]}"


def payment_signature_hex(hash_key: str, message: str) -> str:
    return hmac.new(hash_key.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_payment_callback(company_key: str, hash_key: str, order_id: str, ts: str, sig: str) -> bool:
    msg = f"company_key={company_key}&order_id={order_id}&ts={ts}"
    expected = payment_signature_hex(hash_key, msg)
    return hmac.compare_digest(expected, sig)


def post_linepay_order() -> dict:
    """
    呼叫金流建立訂單。環境變數：
    - LINEPAY_ORDERS_URL（預設 https://line-payment-service.vibelinai.com/orders/）
    - LINEPAY_COMPANY_KEY（header key）
    - LINEPAY_WRITE_KEY（header write_key）
    成功時回傳 JSON，通常含 status, order_id, payment_url。
    """
    _linepay_log("--- post_linepay_order 開始 ---")
    orders_url = (os.environ.get("LINEPAY_ORDERS_URL") or "").strip() or "https://line-payment-service.vibelinai.com/orders/"
    company_key = (os.environ.get("LINEPAY_COMPANY_KEY") or "").strip()
    write_key = (os.environ.get("LINEPAY_WRITE_KEY") or "").strip()
    if not company_key or not write_key:
        raise ValueError("LINEPAY_COMPANY_KEY and LINEPAY_WRITE_KEY must be set in the environment")

    payload = {
        "product_name": "圖文選單費用",
        "amount": 1,
        "currency": "TWD",
        "product_image_url": "",
    }
    headers = {
        "key": company_key,
        "write_key": write_key,
        "Content-Type": "application/json",
    }
    _linepay_log(f"URL: {orders_url!r}")
    _linepay_log(f"Header key (遮罩): {_mask_key(company_key)}, write_key (遮罩): {_mask_key(write_key)}")
    _linepay_log(f"payload = {json.dumps(payload, ensure_ascii=False)}")

    try:
        response = requests.post(
            orders_url,
            headers=headers,
            data=json.dumps(payload),
            timeout=20,
        )
    except requests.RequestException as exc:
        _linepay_log(f"requests 例外: {exc!r}")
        raise ValueError(f"Unable to reach LINE Pay service: {exc}") from exc

    _linepay_log(f"HTTP status_code={response.status_code}, body 前 800 字: {response.text[:800]!r}")

    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"LINE Pay service returned non-JSON (HTTP {response.status_code}): {response.text[:300]!r}"
        ) from exc

    if not response.ok:
        detail = data if isinstance(data, dict) else response.text
        raise ValueError(f"LINE Pay service HTTP {response.status_code}: {detail}")

    if isinstance(data, dict) and data.get("status") not in (None, 200):
        raise ValueError(f"LINE Pay business status not success: {json.dumps(data, ensure_ascii=False)[:500]}")

    _linepay_log(f"JSON keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
    _linepay_log("--- post_linepay_order 成功結束 ---")
    return data if isinstance(data, dict) else {}
