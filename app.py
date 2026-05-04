from __future__ import annotations

import io
import os
import time
import uuid
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from PIL import Image

from botocore.exceptions import ClientError
from chalice import Chalice, CORSConfig, Response
from boto3.dynamodb.conditions import Key

from chalicelib.auth import (
    create_access_token,
    create_refresh_token,
    decode_access_token,
    decode_refresh_token,
)
from chalicelib.db import (
    get_user_by_id,
    get_user_by_line_sub,
    list_oa,
    list_richmenus,
    now_iso,
    oa_table,
    payment_order_table,
    publish_job_table,
    richmenu_table,
    users_table,
)
from chalicelib.crypto import decrypt_secret, encrypt_secret
from chalicelib.http import error, success
from chalicelib.linepay import post_linepay_order, verify_payment_callback, verify_simple_payment_callback
from chalicelib.storage import (
    InvalidImageError,
    get_richmenu_image_url,
    upload_oa_avatar_bytes,
    upload_richmenu_image_base64,
)

app = Chalice(app_name="line-oa-richmenu-api")


def _payment_log(msg: str) -> None:
    print(f"[payments/orders] {msg}", flush=True)


def _is_prod_stage() -> bool:
    return (os.environ.get("CHALICE_STAGE") or "").strip().lower() == "prod"


app.debug = not _is_prod_stage()
app.api.cors = CORSConfig(
    allow_origin=os.environ.get("CORS_ALLOW_ORIGIN", "http://localhost:3001"),
    allow_credentials=True,
    allow_headers=["Content-Type", "Authorization", "X-Admin-Token"],
)

ACCESS_COOKIE_NAME = "access_token"
REFRESH_COOKIE_NAME = "refresh_token"
# 僅此帳號可在登入／refresh 的 JSON 回應中取得 access_token（其餘帳號僅能透過 HttpOnly cookie）
# 從環境變數讀取以避免將個人 email 寫進原始碼；未設定時視為「無 debug 帳號」
DEBUG_ACCESS_TOKEN_EMAIL = (os.environ.get("DEBUG_ACCESS_TOKEN_EMAIL") or "").strip().lower()
ACCESS_TOKEN_TTL_SECONDS = 2 * 60 * 60
REFRESH_TOKEN_TTL_SECONDS = 7 * 24 * 60 * 60


def _cookie_secure() -> bool:
    return os.environ.get("COOKIE_SECURE", "false").lower() == "true"


def _cookie_samesite() -> str:
    return os.environ.get("COOKIE_SAMESITE", "Lax")


def _cookie_header(name: str, value: str, max_age: int) -> str:
    attrs = [
        f"{name}={value}",
        "Path=/",
        "HttpOnly",
        f"SameSite={_cookie_samesite()}",
        f"Max-Age={max_age}",
    ]
    if _cookie_secure():
        attrs.append("Secure")
    return "; ".join(attrs)


def _expired_cookie_header(name: str) -> str:
    attrs = [
        f"{name}=",
        "Path=/",
        "HttpOnly",
        f"SameSite={_cookie_samesite()}",
        "Max-Age=0",
    ]
    if _cookie_secure():
        attrs.append("Secure")
    return "; ".join(attrs)


def _response_with_cookies(body: dict, status_code: int = 200, set_cookie_headers=None):
    headers = {}
    if set_cookie_headers:
        # Fallback for gateways/runtimes that drop multi-value headers.
        cookie_header_keys = ["Set-Cookie", "set-cookie", "SET-COOKIE"]
        for idx, cookie in enumerate(set_cookie_headers):
            if idx >= len(cookie_header_keys):
                break
            headers[cookie_header_keys[idx]] = cookie
    res = Response(status_code=status_code, body=body, headers=headers)
    if set_cookie_headers:
        # Keep canonical multi-value headers for environments that support it.
        res.multi_value_headers = {"Set-Cookie": set_cookie_headers}
    return res


def _build_auth_payload(user):
    return {
        "user": {
            "userId": user["userId"],
            "name": user.get("name"),
            "email": user.get("email"),
            "role": user.get("role", "editor"),
        },
        "expiresIn": ACCESS_TOKEN_TTL_SECONDS,
        "tokenType": "Bearer",
    }


def _issue_auth_response(user):
    token_version = int(user.get("tokenVersion") or 0)
    access_token = create_access_token(
        user["userId"],
        user.get("role", "editor"),
        ttl_seconds=ACCESS_TOKEN_TTL_SECONDS,
        token_version=token_version,
    )
    refresh_token = create_refresh_token(
        user["userId"],
        user.get("role", "editor"),
        ttl_seconds=REFRESH_TOKEN_TTL_SECONDS,
        token_version=token_version,
    )
    auth_payload = _build_auth_payload(user)
    body = {"data": auth_payload}
    email_normalized = (user.get("email") or "").strip().lower()
    # production stage 一律不在 JSON body 回傳 access_token，避免後門帳號變成永久繞過
    if (
        not _is_prod_stage()
        and DEBUG_ACCESS_TOKEN_EMAIL
        and email_normalized == DEBUG_ACCESS_TOKEN_EMAIL
    ):
        body["data"]["access_token"] = access_token
    set_cookies = [
        _cookie_header(ACCESS_COOKIE_NAME, access_token, ACCESS_TOKEN_TTL_SECONDS),
        _cookie_header(REFRESH_COOKIE_NAME, refresh_token, REFRESH_TOKEN_TTL_SECONDS),
    ]
    return _response_with_cookies(body=body, set_cookie_headers=set_cookies)


def _json():
    return app.current_request.json_body or {}


def _get_all_cookies():
    headers = app.current_request.headers or {}
    cookie_str = headers.get("cookie") or headers.get("Cookie") or ""
    if not cookie_str:
        return {}
    cookies = {}
    for pair in cookie_str.split(";"):
        if "=" not in pair:
            continue
        key, value = pair.strip().split("=", 1)
        cookies[key] = value
    return cookies


def _decode_access_payload():
    cookies = _get_all_cookies()
    token = cookies.get(ACCESS_COOKIE_NAME)
    if token:
        try:
            return decode_access_token(token)
        except Exception as exc:
            print(f"[auth] cookie access_token decode failed: {exc}", flush=True)
    auth = (app.current_request.headers or {}).get("authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth.replace("Bearer ", "", 1).strip()
    try:
        return decode_access_token(token)
    except Exception as exc:
        print(f"[auth] header access_token decode failed: {exc}", flush=True)
        return None


def _payload_matches_user_token_version(payload: dict) -> bool:
    # 與 user 紀錄上的 tokenVersion 比對；不一致代表 token 已被撤銷（例如 logout 後簽出的新版本）。
    user = get_user_by_id(payload.get("sub"))
    if not user:
        return False
    return int(user.get("tokenVersion") or 0) == int(payload.get("tokver") or 0)


def _auth():
    payload = _decode_access_payload()
    if not payload:
        return None
    if not _payload_matches_user_token_version(payload):
        print(f"[auth] tokver mismatch sub={payload.get('sub')!r}", flush=True)
        return None
    return payload


def _require_auth():
    payload = _auth()
    if not payload:
        raise PermissionError()
    return payload


def _get_admin_token_from_header() -> str:
    headers = app.current_request.headers or {}
    return (headers.get("x-admin-token") or headers.get("X-Admin-Token") or "").strip()


def _require_admin_token():
    expected = (os.environ.get("ADMIN_CRON_TOKEN") or "").strip()
    if not expected:
        raise RuntimeError("ADMIN_CRON_TOKEN is not configured")
    request_token = _get_admin_token_from_header()
    if not request_token or request_token != expected:
        raise PermissionError("Invalid admin token")


def _normalize_dynamo_numbers(item):
    """
    遞迴將 DynamoDB 回傳的 Decimal 轉成 JSON 可序列化型別：
      - 整數 Decimal → int
      - 帶小數的 Decimal → str（例如金額 1.99，避免 float 浮點誤差導致對帳對不上）
    呼叫端若需要做數值運算，應自己用 Decimal/int 解析字串。
    """
    if isinstance(item, Decimal):
        try:
            if item == item.to_integral_value():
                return int(item)
        except (InvalidOperation, ValueError):
            pass
        return str(item)
    if isinstance(item, list):
        return [_normalize_dynamo_numbers(v) for v in item]
    if isinstance(item, tuple):
        return tuple(_normalize_dynamo_numbers(v) for v in item)
    if isinstance(item, dict):
        return {k: _normalize_dynamo_numbers(v) for k, v in item.items()}
    return item


def _extract_linepay_order_response(body: dict) -> tuple[str | None, str | None]:
    if not body:
        return None, None
    nested = body.get("data") if isinstance(body.get("data"), dict) else None
    src = nested or body
    oid = (
        src.get("order_id")
        or src.get("orderId")
        or src.get("id")
        or body.get("order_id")
        or body.get("orderId")
        or body.get("id")
    )
    purl = src.get("payment_url") or src.get("paymentUrl") or body.get("payment_url") or body.get("paymentUrl")
    return (str(oid).strip() if oid else None, str(purl).strip() if purl else None)


def _enrich_richmenu_image(item):
    if not item:
        return item
    image_url = get_richmenu_image_url(item.get("imageS3Key"), item.get("imageUrl"))
    enriched = dict(item)
    enriched["imageUrl"] = image_url
    enriched["image_url"] = image_url
    enriched["preview_url"] = image_url
    return enriched


def _enrich_oa_image(item):
    if not item:
        return item
    picture_url = get_richmenu_image_url(item.get("pictureS3Key"), item.get("pictureUrl"))
    enriched = dict(item)
    enriched["pictureUrl"] = picture_url
    return enriched


def _is_owner(item, user_id: str) -> bool:
    if not item:
        return False
    return item.get("ownerUserId") == user_id or item.get("createdBy") == user_id


def _fetch_line_bot_info(channel_access_token: str) -> dict:
    req = Request(
        url="https://api.line.me/v2/bot/info",
        method="GET",
        headers={"Authorization": f"Bearer {channel_access_token.strip()}"},
    )
    try:
        with urlopen(req, timeout=20) as resp:
            payload = resp.read().decode("utf-8")
            if not payload:
                return {}
            import json

            return json.loads(payload)
    except HTTPError as exc:
        message = f"LINE API error: {exc.code}"
        raise ValueError(message) from exc
    except URLError as exc:
        raise ValueError("Unable to connect LINE API") from exc
    except Exception as exc:
        raise ValueError("Invalid LINE API response") from exc


def _exchange_line_login_code(code: str) -> dict:
    channel_id = (os.environ.get("LINE_LOGIN_CHANNEL_ID") or "").strip()
    channel_secret = (os.environ.get("LINE_LOGIN_CHANNEL_SECRET") or "").strip()
    redirect_uri = (os.environ.get("LINE_LOGIN_REDIRECT_URI") or "").strip()
    if not channel_id or not channel_secret or not redirect_uri:
        raise ValueError("LINE Login environment variables are not configured")

    form = urlencode(
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": channel_id,
            "client_secret": channel_secret,
        }
    ).encode("utf-8")
    req = Request(
        url="https://api.line.me/oauth2/v2.1/token",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=form,
    )
    try:
        with urlopen(req, timeout=20) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload) if payload else {}
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            body = ""
        raise ValueError(f"LINE token exchange failed: {exc.code} {body}".strip()) from exc
    except Exception as exc:
        raise ValueError("LINE token exchange failed") from exc


def _verify_line_id_token(id_token: str) -> dict:
    channel_id = (os.environ.get("LINE_LOGIN_CHANNEL_ID") or "").strip()
    if not channel_id:
        raise ValueError("LINE_LOGIN_CHANNEL_ID is not configured")
    form = urlencode({"id_token": id_token, "client_id": channel_id}).encode("utf-8")
    req = Request(
        url="https://api.line.me/oauth2/v2.1/verify",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=form,
    )
    try:
        with urlopen(req, timeout=20) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload) if payload else {}
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            body = ""
        raise ValueError(f"LINE id_token verify failed: {exc.code} {body}".strip()) from exc
    except Exception as exc:
        raise ValueError("LINE id_token verify failed") from exc


def _download_image(url: str) -> tuple[bytes, str | None]:
    req = Request(url=url, method="GET")
    with urlopen(req, timeout=20) as resp:
        content_type = resp.headers.get("Content-Type")
        body = resp.read()
    return body, content_type


# LINE 圖文選單合法尺寸：全高 2500x1686、半高 2500x843
_LINE_RICHMENU_SIZES = {(2500, 1686), (2500, 843)}

def _scale_image_for_line(image_bytes: bytes, content_type: str | None, target: dict) -> tuple[bytes, str]:
    target_w = int(target.get("width") or 2500)
    target_h = int(target.get("height") or 1686)
    # 若 target 不是 LINE 合法尺寸，強制對應到最近的合法尺寸
    if (target_w, target_h) not in _LINE_RICHMENU_SIZES:
        target_h = 843 if target_h <= 843 else 1686
        target_w = 2500

    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    if img.size != (target_w, target_h):
        img = img.resize((target_w, target_h), Image.LANCZOS)

    buf = io.BytesIO()
    # PNG 轉 JPEG 避免超過 LINE 的 1MB 限制
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue(), "image/jpeg"


def _line_headers(channel_access_token: str, content_type: str | None = "application/json") -> dict:
    headers = {"Authorization": f"Bearer {channel_access_token.strip()}"}
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def _line_request(
    method: str,
    url: str,
    channel_access_token: str,
    payload: dict | None = None,
    binary_body: bytes | None = None,
    content_type: str | None = "application/json",
) -> tuple[dict, str | None]:
    def _normalize_json_value(value):
        if isinstance(value, Decimal):
            return int(value) if value % 1 == 0 else float(value)
        if isinstance(value, dict):
            return {k: _normalize_json_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_normalize_json_value(v) for v in value]
        return value

    body = None
    if payload is not None:
        normalized_payload = _normalize_json_value(payload)
        body = json.dumps(normalized_payload).encode("utf-8")
    elif binary_body is not None:
        body = binary_body
    req = Request(
        url=url,
        method=method,
        headers=_line_headers(channel_access_token, content_type=content_type),
        data=body,
    )
    try:
        with urlopen(req, timeout=30) as resp:
            resp_body = resp.read().decode("utf-8") if resp.readable() else ""
            request_id = resp.headers.get("x-line-request-id")
            if not resp_body:
                return {}, request_id
            return json.loads(resp_body), request_id
    except HTTPError as exc:
        response_body = ""
        try:
            response_body = exc.read().decode("utf-8")
        except Exception:
            response_body = ""
        message = f"LINE API error: {exc.code}"
        if response_body:
            message = f"{message} {response_body}"
        raise ValueError(message) from exc
    except URLError as exc:
        raise ValueError("Unable to connect LINE API") from exc
    except Exception as exc:
        raise ValueError("Invalid LINE API response") from exc


def _normalize_line_richmenu_areas(areas):
    normalized_areas = []
    for area in areas or []:
        if not isinstance(area, dict):
            normalized_areas.append(area)
            continue

        normalized_area = dict(area)
        action = normalized_area.get("action")
        if isinstance(action, dict):
            normalized_action = dict(action)
            action_type = str(normalized_action.get("type") or "").strip().lower()
            if action_type == "richmenuswitch":
                alias_id = str(normalized_action.get("richMenuAliasId") or "").strip()
                data = str(normalized_action.get("data") or "").strip()
                if not data:
                    # LINE richmenuswitch action requires non-empty data.
                    data = alias_id
                normalized_action["data"] = data
            normalized_area["action"] = normalized_action

        normalized_areas.append(normalized_area)

    return normalized_areas


def _upsert_line_richmenu_alias(
    channel_access_token: str, richmenu_alias_id: str, line_richmenu_id: str
) -> list[dict]:
    alias_steps = []
    alias_payload = {
        "richMenuAliasId": richmenu_alias_id,
        "richMenuId": line_richmenu_id,
    }
    try:
        create_resp, create_req_id = _line_request(
            method="POST",
            url="https://api.line.me/v2/bot/richmenu/alias",
            channel_access_token=channel_access_token,
            payload=alias_payload,
        )
        alias_steps.append(
            {
                "step": "upsert_alias_create",
                "statusCode": 200,
                "requestBody": alias_payload,
                "responseBody": create_resp or {"ok": True},
                "requestId": create_req_id,
                "executedAt": now_iso(),
            }
        )
    except ValueError as exc:
        msg = str(exc).lower()
        alias_exists = (
            ("already exists" in msg)
            or ("conflict richmenu alias id" in msg)
            or ("error code: 409" in msg)
            or ("line api error: 409" in msg)
        )
        if not alias_exists:
            raise

        update_resp, update_req_id = _line_request(
            method="POST",
            url=f"https://api.line.me/v2/bot/richmenu/alias/{richmenu_alias_id}",
            channel_access_token=channel_access_token,
            payload={"richMenuId": line_richmenu_id},
        )
        alias_steps.append(
            {
                "step": "upsert_alias_update",
                "statusCode": 200,
                "requestBody": {"richMenuId": line_richmenu_id},
                "responseBody": update_resp or {"ok": True},
                "requestId": update_req_id,
                "executedAt": now_iso(),
            }
        )
    return alias_steps


def _delete_line_richmenu_if_needed(item: dict, channel_access_token: str):
    line_richmenu_id = (item or {}).get("lineRichMenuId")
    if not line_richmenu_id:
        return
    _line_request(
        method="DELETE",
        url=f"https://api.line.me/v2/bot/richmenu/{line_richmenu_id}",
        channel_access_token=channel_access_token,
        content_type=None,
    )


def _oa_channel_access_token(oa_item: dict) -> str:
    oa_id = (oa_item.get("oaId") or "").strip()
    return decrypt_secret(
        oa_item.get("channelAccessTokenEnc") or "",
        oa_id=oa_id,
        field="channelAccessToken",
    ).strip()


def _oa_channel_secret(oa_item: dict) -> str:
    oa_id = (oa_item.get("oaId") or "").strip()
    return decrypt_secret(
        oa_item.get("channelSecretEnc") or "",
        oa_id=oa_id,
        field="channelSecret",
    ).strip()


def _ensure_oa_access(oa_id: str, user_id: str):
    oa = oa_table.get_item(Key={"oaId": oa_id}).get("Item")
    if not oa or not _is_owner(oa, user_id):
        return None
    return oa


def _query_richmenus_by_oa(oa_id: str):
    # DynamoDB query 單次回傳上限 1MB，超過會切頁；用 LastEvaluatedKey 迴圈讀完避免漏資料。
    kwargs = {"KeyConditionExpression": Key("oaId").eq(oa_id)}
    while True:
        resp = richmenu_table.query(**kwargs)
        for item in resp.get("Items", []):
            yield item
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key


def _ensure_richmenu_access(oa_id: str, richmenu_id: str, user_id: str):
    item = richmenu_table.get_item(Key={"oaId": oa_id, "richMenuId": richmenu_id}).get("Item")
    if not item or not _is_owner(item, user_id):
        return None
    return item


@app.route("/v1/auth/line-login", methods=["POST"])
def line_login():
    body = _json()
    code = (body.get("code") or "").strip()
    if not code:
        return error("VALIDATION_ERROR", "LINE authorization code is required", 400)
    try:
        token_payload = _exchange_line_login_code(code)
        id_token = (token_payload.get("id_token") or "").strip()
        if not id_token:
            return error("AUTH_INVALID_LINE_TOKEN", "LINE id_token not found", 401)
        profile = _verify_line_id_token(id_token)
    except ValueError as exc:
        return error("AUTH_LINE_LOGIN_FAILED", str(exc), 401)

    line_sub = (profile.get("sub") or "").strip()
    if not line_sub:
        return error("AUTH_LINE_LOGIN_FAILED", "LINE user sub is missing", 401)

    now = now_iso()
    name = (profile.get("name") or "").strip() or "LINE User"
    email = (profile.get("email") or "").strip()
    picture = (profile.get("picture") or "").strip()
    user = get_user_by_line_sub(line_sub)
    if not user:
        user = {
            "userId": f"u_{uuid.uuid4().hex[:8]}",
            "lineSub": line_sub,
            "name": name,
            "email": email,
            "emailNormalized": email.lower() if email else "",
            "avatarUrl": picture,
            "role": "editor",
            "status": "active",
            "tokenVersion": 1,
            "createdAt": now,
            "updatedAt": now,
        }
    else:
        user = dict(user)
        user["lineSub"] = line_sub
        user["name"] = name
        user["avatarUrl"] = picture
        if email:
            user["email"] = email
            user["emailNormalized"] = email.lower()
        user["updatedAt"] = now
    users_table.put_item(Item=user)
    return _issue_auth_response(user)


@app.route("/v1/auth/refresh", methods=["POST"])
def refresh():
    refresh_token = _get_all_cookies().get(REFRESH_COOKIE_NAME)
    if not refresh_token:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    try:
        payload = decode_refresh_token(refresh_token)
    except Exception as exc:
        print(f"[auth] refresh_token decode failed: {exc}", flush=True)
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    user = get_user_by_id(payload.get("sub"))
    if not user:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    # 即使 refresh token 還在 7 天有效期內，只要 tokenVersion 已被遞增（例如使用者已 logout），就視同已撤銷
    if int(user.get("tokenVersion") or 0) != int(payload.get("tokver") or 0):
        print(f"[auth] refresh tokver mismatch sub={payload.get('sub')!r}", flush=True)
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    return _issue_auth_response(user)


@app.route("/v1/auth/me", methods=["GET"])
def me():
    payload = _auth()
    if not payload:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    user = get_user_by_id(payload.get("sub"))
    if not user:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    return {"data": _build_auth_payload(user)}


@app.route("/v1/auth/logout", methods=["POST"])
def logout():
    sub: str | None = None
    payload = _auth()
    if payload:
        sub = payload.get("sub")
    if not sub:
        # access token 失效時改用 refresh token；但仍須通過 tokver 驗證以避免攻擊者用已撤銷的 token 觸發進一步 revoke
        refresh_token_value = _get_all_cookies().get(REFRESH_COOKIE_NAME)
        if refresh_token_value:
            try:
                rp = decode_refresh_token(refresh_token_value)
                rp_user = get_user_by_id(rp.get("sub"))
                if rp_user and int(rp_user.get("tokenVersion") or 0) == int(rp.get("tokver") or 0):
                    sub = rp.get("sub")
            except Exception as exc:
                print(f"[auth] logout refresh_token decode failed: {exc}", flush=True)
    if not sub:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    # 遞增 tokenVersion 後，所有先前簽出的 access/refresh token 立刻失效（包含可能被竊的 7 天 refresh token）
    try:
        users_table.update_item(
            Key={"userId": sub},
            UpdateExpression="SET tokenVersion = if_not_exists(tokenVersion, :zero) + :one, updatedAt = :now",
            ExpressionAttributeValues={":zero": 0, ":one": 1, ":now": now_iso()},
        )
    except ClientError as exc:
        print(f"[auth] logout token revoke failed: {exc}", flush=True)
    return _response_with_cookies(
        body=success(),
        set_cookie_headers=[
            _expired_cookie_header(ACCESS_COOKIE_NAME),
            _expired_cookie_header(REFRESH_COOKIE_NAME),
        ],
    )


@app.route("/v1/oa", methods=["GET"])
def get_oa():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    return {"data": [_enrich_oa_image(item) for item in list_oa(user["sub"])]}


@app.route("/v1/oa", methods=["POST"])
def create_oa():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    body = _json()
    channel_secret = body.get("channelSecret")
    channel_access_token = body.get("channelAccessToken")
    if not channel_secret or not channel_access_token:
        return error("VALIDATION_ERROR", "channelSecret and channelAccessToken are required", 400)
    try:
        line_info = _fetch_line_bot_info(channel_access_token)
    except ValueError as exc:
        return error("LINE_API_ERROR", str(exc), 400)
    display_name = line_info.get("displayName")
    basic_id = line_info.get("basicId")
    if not display_name or not basic_id:
        return error("LINE_API_ERROR", "LINE OA info is incomplete", 400)

    now = now_iso()
    oa_id = f"oa-{uuid.uuid4().hex[:6]}"
    picture_filename = None
    picture_s3_key = None
    picture_url = line_info.get("pictureUrl", "")
    if picture_url:
        try:
            image_bytes, image_content_type = _download_image(picture_url)
            upload_result = upload_oa_avatar_bytes(
                oa_id=oa_id,
                image_bytes=image_bytes,
                source_url=picture_url,
                content_type=image_content_type,
            )
            picture_filename = upload_result["fileName"]
            picture_s3_key = upload_result["s3Key"]
            picture_url = upload_result["imageUrl"]
        except Exception as exc:
            # Keep OA binding flow available even if avatar upload fails.
            print(
                f"[oa/avatar] download or upload failed (oa_id={oa_id}, source={picture_url}): {exc}",
                flush=True,
            )
            picture_filename = None
            picture_s3_key = None
    item = {
        "oaId": oa_id,
        "name": display_name,
        "accountId": basic_id,
        "channelSecretEnc": encrypt_secret(channel_secret, oa_id=oa_id, field="channelSecret"),
        "channelAccessTokenEnc": encrypt_secret(channel_access_token, oa_id=oa_id, field="channelAccessToken"),
        "pictureUrl": picture_url,
        "pictureFileName": picture_filename,
        "pictureS3Key": picture_s3_key,
        "tokenVersion": 1,
        "status": "active",
        "boundAt": now,
        "ownerUserId": user["sub"],
        "createdBy": user["sub"],
        "updatedBy": user["sub"],
        "createdAt": now,
        "updatedAt": now,
    }
    oa_table.put_item(Item=item)
    enriched_item = _enrich_oa_image(item)
    return {
        "data": {
            "oaId": oa_id,
            "name": item["name"],
            "accountId": item["accountId"],
            "pictureUrl": enriched_item.get("pictureUrl", ""),
            "pictureFileName": picture_filename,
            "boundAt": now,
            "status": "active",
        }
    }


@app.route("/v1/oa/{oa_id}/token", methods=["PUT"])
def update_oa_token(oa_id):
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    body = _json()
    current = _ensure_oa_access(oa_id, user["sub"])
    if not current:
        return error("NOT_FOUND", "oa not found", 404)
    new_channel_secret = body.get("channelSecret")
    new_channel_access_token = body.get("channelAccessToken")
    if new_channel_secret:
        current["channelSecretEnc"] = encrypt_secret(
            new_channel_secret, oa_id=oa_id, field="channelSecret"
        )
    if new_channel_access_token:
        current["channelAccessTokenEnc"] = encrypt_secret(
            new_channel_access_token, oa_id=oa_id, field="channelAccessToken"
        )
    current["tokenVersion"] = int(current.get("tokenVersion", 0)) + 1
    current["updatedBy"] = user["sub"]
    current["updatedAt"] = now_iso()
    oa_table.put_item(Item=current)
    return {"data": {"oaId": oa_id, "updatedAt": current["updatedAt"]}}


@app.route("/v1/oa/{oa_id}", methods=["DELETE"])
def delete_oa(oa_id):
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    current = _ensure_oa_access(oa_id, user["sub"])
    if not current:
        return error("NOT_FOUND", "oa not found", 404)
    oa_table.delete_item(Key={"oaId": oa_id})
    return success()


@app.route("/v1/richmenus", methods=["GET"])
def get_richmenus():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    qp = app.current_request.query_params or {}
    oa_id = qp.get("oaId")
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    search = qp.get("search")
    if not _ensure_oa_access(oa_id, user["sub"]):
        return error("NOT_FOUND", "oa not found", 404)
    items = [_enrich_richmenu_image(i) for i in list_richmenus(oa_id, user["sub"], search)]
    return {"data": items}


@app.route("/v1/richmenus", methods=["POST"])
def create_richmenu():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    body = _json()
    oa_id = body.get("oaId")
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    if not _ensure_oa_access(oa_id, user["sub"]):
        return error("NOT_FOUND", "oa not found", 404)
    richmenu_id = f"rm_{uuid.uuid4().hex[:12]}"
    now = now_iso()
    image_url = body.get("imageUrl", "")
    image_file_id = None
    image_s3_key = None
    image_mime_type = None
    image_size = None
    image_base64 = body.get("imageBase64")
    if image_base64:
        try:
            upload_result = upload_richmenu_image_base64(
                oa_id=oa_id,
                image_base64=image_base64,
                mime_type=body.get("imageMimeType"),
            )
        except InvalidImageError as exc:
            return error("VALIDATION_ERROR", str(exc), 400)
        image_url = upload_result["imageUrl"]
        image_file_id = upload_result["fileId"]
        image_s3_key = upload_result["s3Key"]
        image_mime_type = upload_result["mimeType"]
        image_size = upload_result["size"]
    item = {
        "oaId": oa_id,
        "richMenuId": richmenu_id,
        "id": richmenu_id,
        "name": body.get("name", ""),
        "nameNormalized": (body.get("name", "")).lower(),
        "description": body.get("description", ""),
        "chatBarText": body.get("chatBarText", ""),
        "imageUrl": image_url,
        "imageFileId": image_file_id,
        "imageS3Key": image_s3_key,
        "imageMimeType": image_mime_type,
        "imageSize": image_size,
        "size": body.get("size", {"width": 2500, "height": 1686}),
        "areas": body.get("areas", []),
        "status": "draft",
        "isDefault": False,
        "selected": bool(body.get("selected", False)),
        "ownerUserId": user["sub"],
        "createdBy": user["sub"],
        "updatedBy": user["sub"],
        "createdAt": now,
        "updatedAt": now,
        "statusUpdatedAt": f"draft#{now}",
    }
    richmenu_table.put_item(Item=item)
    return {"data": {"id": richmenu_id, "oaId": oa_id, "name": item["name"], "status": "draft", "isDefault": False, "createdAt": now, "updatedAt": now}}


@app.route("/v1/richmenus/{richmenu_id}", methods=["GET"])
def get_richmenu(richmenu_id):
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    oa_id = (app.current_request.query_params or {}).get("oaId")
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    if not _ensure_oa_access(oa_id, user["sub"]):
        return error("NOT_FOUND", "oa not found", 404)
    item = _ensure_richmenu_access(oa_id, richmenu_id, user["sub"])
    if not item:
        return error("NOT_FOUND", "richmenu not found", 404)
    return {"data": _enrich_richmenu_image(item)}


@app.route("/v1/richmenus/{richmenu_id}", methods=["PUT"])
def update_richmenu(richmenu_id):
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    body = _json()
    oa_id = body.get("oaId")
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    if not _ensure_oa_access(oa_id, user["sub"]):
        return error("NOT_FOUND", "oa not found", 404)
    item = _ensure_richmenu_access(oa_id, richmenu_id, user["sub"])
    if not item:
        return error("NOT_FOUND", "richmenu not found", 404)
    for key in ["name", "description", "chatBarText", "imageUrl", "size", "areas", "selected"]:
        if key in body:
            item[key] = body[key]
    if body.get("imageBase64"):
        try:
            upload_result = upload_richmenu_image_base64(
                oa_id=oa_id,
                image_base64=body["imageBase64"],
                mime_type=body.get("imageMimeType"),
            )
        except InvalidImageError as exc:
            return error("VALIDATION_ERROR", str(exc), 400)
        item["imageUrl"] = upload_result["imageUrl"]
        item["imageFileId"] = upload_result["fileId"]
        item["imageS3Key"] = upload_result["s3Key"]
        item["imageMimeType"] = upload_result["mimeType"]
        item["imageSize"] = upload_result["size"]
    item["nameNormalized"] = item.get("name", "").lower()
    item["updatedBy"] = user["sub"]
    item["updatedAt"] = now_iso()
    item["statusUpdatedAt"] = f"{item.get('status', 'draft')}#{item['updatedAt']}"
    richmenu_table.put_item(Item=item)
    return {"data": {"id": item["id"], "oaId": oa_id, "name": item.get("name"), "status": item.get("status"), "isDefault": item.get("isDefault", False), "updatedAt": item["updatedAt"]}}


@app.route("/v1/richmenus/{richmenu_id}", methods=["DELETE"])
def delete_richmenu(richmenu_id):
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    oa_id = (app.current_request.query_params or {}).get("oaId")
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    oa_item = _ensure_oa_access(oa_id, user["sub"])
    if not oa_item:
        return error("NOT_FOUND", "oa not found", 404)
    item = _ensure_richmenu_access(oa_id, richmenu_id, user["sub"])
    if not item:
        return error("NOT_FOUND", "richmenu not found", 404)
    channel_access_token = _oa_channel_access_token(oa_item)
    if item.get("lineRichMenuId") and not channel_access_token:
        return error("VALIDATION_ERROR", "channelAccessToken is required", 400)
    if item.get("lineRichMenuId"):
        try:
            _delete_line_richmenu_if_needed(item, channel_access_token)
        except ValueError as exc:
            return error("LINE_API_ERROR", str(exc), 400)
    richmenu_table.delete_item(Key={"oaId": oa_id, "richMenuId": richmenu_id})
    return success()


@app.route("/v1/richmenus/{richmenu_id}/publish", methods=["POST"])
def publish_richmenu(richmenu_id):
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    body = _json()
    oa_id = body.get("oaId")
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    oa_item = _ensure_oa_access(oa_id, user["sub"])
    if not oa_item:
        return error("NOT_FOUND", "oa not found", 404)
    item = _ensure_richmenu_access(oa_id, richmenu_id, user["sub"])
    if not item:
        return error("NOT_FOUND", "richmenu not found", 404)
    channel_access_token = _oa_channel_access_token(oa_item)
    if not channel_access_token:
        return error("VALIDATION_ERROR", "channelAccessToken is required", 400)
    image_url = get_richmenu_image_url(item.get("imageS3Key"), item.get("imageUrl"))
    if not image_url:
        return error("VALIDATION_ERROR", "richmenu image is required", 400)

    raw_test_publish_without_payment = body.get("testPublishWithoutPayment") or body.get("bypassPaymentCheck")
    if isinstance(raw_test_publish_without_payment, bool):
        test_publish_without_payment = raw_test_publish_without_payment
    elif isinstance(raw_test_publish_without_payment, str):
        normalized = raw_test_publish_without_payment.strip().lower()
        test_publish_without_payment = normalized in {"true", "1", "yes", "y"}
    else:
        test_publish_without_payment = bool(raw_test_publish_without_payment)

    if test_publish_without_payment:
        # production 一律禁止任何免付費發佈旗標，避免帳號被盜後變成永久繞過付款的後門
        if _is_prod_stage():
            return error(
                "PERMISSION_DENIED",
                "testPublishWithoutPayment is not allowed in production",
                403,
            )
        # 非 prod stage 仍須限縮在 DEBUG_ACCESS_TOKEN_EMAIL 帳號
        requester = get_user_by_id(user["sub"]) or {}
        requester_email = (requester.get("email") or "").strip().lower()
        if not DEBUG_ACCESS_TOKEN_EMAIL or requester_email != DEBUG_ACCESS_TOKEN_EMAIL:
            return error(
                "PERMISSION_DENIED",
                "testPublishWithoutPayment is restricted to the debug account",
                403,
            )

    if not test_publish_without_payment:
        try:
            validity = _get_payment_validity_for_oa(oa_id)
        except ClientError as exc:
            err = exc.response.get("Error") or {}
            return error(
                "DATABASE_ERROR",
                f"{err.get('Code', 'ClientError')}: {err.get('Message', str(exc))}",
                503,
            )
        if not validity.get("isPaid"):
            return error(
                "PAYMENT_REQUIRED",
                "請付費完即可發佈圖文選單",
                402,
                details=validity,
            )

    now = now_iso()
    job_id = f"pub_{uuid.uuid4().hex[:12]}"
    raw_set_as_default = body.get("setAsDefault")
    publish_mode = str(body.get("publishMode") or body.get("mode") or "").strip().lower()
    if isinstance(raw_set_as_default, bool):
        set_as_default = raw_set_as_default
    elif isinstance(raw_set_as_default, str):
        normalized = raw_set_as_default.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            set_as_default = True
        elif normalized in {"false", "0", "no", "n", ""}:
            set_as_default = False
        else:
            set_as_default = publish_mode == "set_default"
    elif raw_set_as_default is None:
        set_as_default = publish_mode == "set_default"
    else:
        set_as_default = bool(raw_set_as_default)
    line_api_steps = []
    line_request_ids = []

    base_job = {
        "oaId": oa_id,
        "jobId": job_id,
        "richMenuId": richmenu_id,
        "jobType": "publish",
        "publishMode": "set_default" if set_as_default else "simple_publish",
        "setAsDefault": set_as_default,
        "createdBy": user["sub"],
        "createdAt": now,
    }

    try:
        normalized_areas = _normalize_line_richmenu_areas(item.get("areas") or [])
        create_payload = {
            "size": item.get("size") or {"width": 2500, "height": 1686},
            "selected": bool(item.get("selected", False)),
            "name": item.get("name") or "Rich menu",
            "chatBarText": item.get("chatBarText") or "Tap to open",
            "areas": normalized_areas,
        }
        create_result, create_req_id = _line_request(
            method="POST",
            url="https://api.line.me/v2/bot/richmenu",
            channel_access_token=channel_access_token,
            payload=create_payload,
        )
        line_richmenu_id = create_result.get("richMenuId")
        if not line_richmenu_id:
            raise ValueError("LINE API error: missing richMenuId")
        if create_req_id:
            line_request_ids.append(create_req_id)
        line_api_steps.append(
            {
                "step": "create_richmenu",
                "statusCode": 200,
                "requestBody": create_payload,
                "responseBody": create_result,
                "executedAt": now_iso(),
            }
        )

        image_bytes, image_content_type = _download_image(image_url)
        richmenu_size = item.get("size") or {"width": 2500, "height": 1686}
        image_bytes, upload_content_type = _scale_image_for_line(image_bytes, image_content_type, richmenu_size)
        _, upload_req_id = _line_request(
            method="POST",
            url=f"https://api-data.line.me/v2/bot/richmenu/{line_richmenu_id}/content",
            channel_access_token=channel_access_token,
            binary_body=image_bytes,
            content_type=upload_content_type,
        )
        if upload_req_id:
            line_request_ids.append(upload_req_id)
        line_api_steps.append(
            {
                "step": "upload_image",
                "statusCode": 200,
                "requestBody": {"contentType": upload_content_type, "imageUrl": image_url},
                "responseBody": {"ok": True},
                "executedAt": now_iso(),
            }
        )

        # Register alias so richmenuswitch can resolve target rich menu.
        alias_steps = _upsert_line_richmenu_alias(
            channel_access_token=channel_access_token,
            richmenu_alias_id=richmenu_id,
            line_richmenu_id=line_richmenu_id,
        )
        line_api_steps.extend(alias_steps)
        for step in alias_steps:
            if step.get("requestId"):
                line_request_ids.append(step["requestId"])

        if set_as_default:
            _, set_default_req_id = _line_request(
                method="POST",
                url=f"https://api.line.me/v2/bot/user/all/richmenu/{line_richmenu_id}",
                channel_access_token=channel_access_token,
                content_type=None,
            )
            if set_default_req_id:
                line_request_ids.append(set_default_req_id)
            line_api_steps.append(
                {
                    "step": "set_default",
                    "statusCode": 200,
                    "requestBody": {"lineRichMenuId": line_richmenu_id},
                    "responseBody": {"ok": True},
                    "executedAt": now_iso(),
                }
            )

        item["status"] = "published"
        item["isDefault"] = set_as_default
        item["lineRichMenuId"] = line_richmenu_id
        item["publishedAt"] = now_iso()
        item["updatedAt"] = now_iso()
        item["updatedBy"] = user["sub"]
        item["statusUpdatedAt"] = f"published#{item['updatedAt']}"
        richmenu_table.put_item(Item=item)

        publish_job_table.put_item(
            Item={
                **base_job,
                "jobStatus": "success",
                "updatedAt": now_iso(),
                "lineApiSteps": line_api_steps,
                "lineRequestIds": line_request_ids,
                "lineRichMenuIdResult": line_richmenu_id,
                "finishedAt": now_iso(),
            }
        )
        return {
            "data": {
                "id": richmenu_id,
                "oaId": oa_id,
                "status": "published",
                "isDefault": item["isDefault"],
                "lineRichMenuId": item["lineRichMenuId"],
                "updatedAt": item["updatedAt"],
            }
        }
    except ValueError as exc:
        fail_now = now_iso()
        publish_job_table.put_item(
            Item={
                **base_job,
                "jobStatus": "failed",
                "errorCode": "LINE_API_ERROR",
                "errorMessage": str(exc),
                "updatedAt": fail_now,
                "lineApiSteps": line_api_steps,
                "lineRequestIds": line_request_ids,
                "finishedAt": fail_now,
            }
        )
        return error("LINE_API_ERROR", str(exc), 400)


@app.route("/v1/richmenus/{richmenu_id}/status", methods=["GET"])
def richmenu_status(richmenu_id):
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    oa_id = (app.current_request.query_params or {}).get("oaId")
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    if not _ensure_oa_access(oa_id, user["sub"]):
        return error("NOT_FOUND", "oa not found", 404)
    item = _ensure_richmenu_access(oa_id, richmenu_id, user["sub"])
    if not item:
        return error("NOT_FOUND", "richmenu not found", 404)
    return {"data": {"id": richmenu_id, "oaId": oa_id, "status": item.get("status", "draft"), "isDefault": item.get("isDefault", False), "publishedAt": item.get("publishedAt")}}


@app.route("/v1/richmenus/unlink-default", methods=["POST"])
def unlink_default():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    oa_id = _json().get("oaId")
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    oa_item = _ensure_oa_access(oa_id, user["sub"])
    if not oa_item:
        return error("NOT_FOUND", "oa not found", 404)
    channel_access_token = _oa_channel_access_token(oa_item)
    if not channel_access_token:
        return error("VALIDATION_ERROR", "channelAccessToken is required", 400)

    try:
        _line_request(
            method="DELETE",
            url="https://api.line.me/v2/bot/user/all/richmenu",
            channel_access_token=channel_access_token,
            content_type=None,
        )
    except ValueError as exc:
        return error("LINE_API_ERROR", str(exc), 400)

    items_to_update = []
    for item in _query_richmenus_by_oa(oa_id):
        if not _is_owner(item, user["sub"]):
            continue
        if item.get("isDefault"):
            item["isDefault"] = False
            item["updatedAt"] = now_iso()
            items_to_update.append(item)
    with richmenu_table.batch_writer() as batch:
        for item in items_to_update:
            batch.put_item(Item=item)
    return success({"oaId": oa_id, "defaultRichMenuId": None})


@app.route("/v1/richmenus/close-all", methods=["POST"])
def close_all():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    oa_id = _json().get("oaId")
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    if not _ensure_oa_access(oa_id, user["sub"]):
        return error("NOT_FOUND", "oa not found", 404)
    items_to_update = []
    for item in _query_richmenus_by_oa(oa_id):
        if not _is_owner(item, user["sub"]):
            continue
        if item.get("status") != "draft":
            item["status"] = "draft"
            item["isDefault"] = False
            item["updatedAt"] = now_iso()
            item["statusUpdatedAt"] = f"draft#{item['updatedAt']}"
            items_to_update.append(item)
    with richmenu_table.batch_writer() as batch:
        for item in items_to_update:
            batch.put_item(Item=item)
    return success({"oaId": oa_id, "closedCount": len(items_to_update)})


@app.route("/v1/richmenus/bulk-delete", methods=["POST"])
def bulk_delete_richmenus():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    body = _json()
    oa_id = body.get("oaId")
    richmenu_ids = body.get("richMenuIds") or []
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    if not isinstance(richmenu_ids, list) or not richmenu_ids:
        return error("VALIDATION_ERROR", "richMenuIds is required", 400)
    oa_item = _ensure_oa_access(oa_id, user["sub"])
    if not oa_item:
        return error("NOT_FOUND", "oa not found", 404)
    channel_access_token = _oa_channel_access_token(oa_item)

    removed_ids = []
    failed_items = []
    items_to_update = []

    # Remove duplicated ids to avoid repeated delete calls.
    unique_ids = list(dict.fromkeys([str(x).strip() for x in richmenu_ids if str(x).strip()]))
    for richmenu_id in unique_ids:
        item = _ensure_richmenu_access(oa_id, richmenu_id, user["sub"])
        if not item:
            failed_items.append({"id": richmenu_id, "reason": "NOT_FOUND"})
            continue
        if item.get("lineRichMenuId") and not channel_access_token:
            failed_items.append({"id": richmenu_id, "reason": "MISSING_CHANNEL_ACCESS_TOKEN"})
            continue
        if item.get("lineRichMenuId"):
            try:
                _delete_line_richmenu_if_needed(item, channel_access_token)
            except ValueError as exc:
                failed_items.append({"id": richmenu_id, "reason": "LINE_API_ERROR", "message": str(exc)})
                continue
            item["lineRichMenuId"] = None
            item["status"] = "draft"
            item["isDefault"] = False
            item["updatedAt"] = now_iso()
            item["updatedBy"] = user["sub"]
            item["statusUpdatedAt"] = f"draft#{item['updatedAt']}"
            items_to_update.append(item)
            removed_ids.append(richmenu_id)
            continue
        # Not published to LINE before; keep DB item unchanged and return as skipped.
        failed_items.append({"id": richmenu_id, "reason": "NOT_PUBLISHED_TO_LINE"})

    with richmenu_table.batch_writer() as batch:
        for item in items_to_update:
            batch.put_item(Item=item)

    return success(
        {
            "oaId": oa_id,
            "removedCount": len(items_to_update),
            "removedIds": removed_ids,
            "failedCount": len(failed_items),
            "failedItems": failed_items,
        }
    )


@app.route("/v1/richmenus/remove-all-line", methods=["POST"])
def remove_all_line_richmenus():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)

    body = _json()
    oa_id = (body.get("oaId") or "").strip()
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)

    oa_item = _ensure_oa_access(oa_id, user["sub"])
    if not oa_item:
        return error("NOT_FOUND", "oa not found", 404)

    channel_access_token = _oa_channel_access_token(oa_item)
    if not channel_access_token:
        return error("VALIDATION_ERROR", "channel access token is required", 400)

    try:
        list_resp, _ = _line_request(
            method="GET",
            url="https://api.line.me/v2/bot/richmenu/list",
            channel_access_token=channel_access_token,
            content_type=None,
        )
    except ValueError as exc:
        return error("LINE_API_ERROR", str(exc), 502)

    richmenus = list_resp.get("richmenus") or []
    removed_ids = []
    failed_items = []
    for item in richmenus:
        line_richmenu_id = str((item or {}).get("richMenuId") or "").strip()
        if not line_richmenu_id:
            continue
        try:
            _line_request(
                method="DELETE",
                url=f"https://api.line.me/v2/bot/richmenu/{line_richmenu_id}",
                channel_access_token=channel_access_token,
                content_type=None,
            )
            removed_ids.append(line_richmenu_id)
        except ValueError as exc:
            failed_items.append(
                {
                    "lineRichMenuId": line_richmenu_id,
                    "reason": "LINE_API_ERROR",
                    "message": str(exc),
                }
            )

    return success(
        {
            "oaId": oa_id,
            "listedCount": len(richmenus),
            "removedCount": len(removed_ids),
            "removedLineRichMenuIds": removed_ids,
            "failedCount": len(failed_items),
            "failedItems": failed_items,
        }
    )


@app.route("/v1/richmenus", methods=["DELETE"], content_types=["application/json"])
def delete_all_richmenus():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    oa_id = (app.current_request.query_params or {}).get("oaId")
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    oa_item = _ensure_oa_access(oa_id, user["sub"])
    if not oa_item:
        return error("NOT_FOUND", "oa not found", 404)
    items = [item for item in _query_richmenus_by_oa(oa_id) if _is_owner(item, user["sub"])]

    # 與 bulk_delete 一致：任何已綁 LINE Rich Menu 的項目都必須先打 LINE API 刪除，
    # 不能只刪 DB 而留下殘存資源在 LINE 端。
    channel_access_token = _oa_channel_access_token(oa_item)
    requires_token = any(i.get("lineRichMenuId") for i in items)
    if requires_token and not channel_access_token:
        return error("VALIDATION_ERROR", "channelAccessToken is required", 400)

    deleted_ids: list[str] = []
    failed_items: list[dict] = []
    keys_to_delete: list[dict] = []
    for item in items:
        rm_id = item["richMenuId"]
        if item.get("lineRichMenuId"):
            try:
                _delete_line_richmenu_if_needed(item, channel_access_token)
            except ValueError as exc:
                failed_items.append(
                    {"id": rm_id, "reason": "LINE_API_ERROR", "message": str(exc)}
                )
                continue
        keys_to_delete.append({"oaId": oa_id, "richMenuId": rm_id})
        deleted_ids.append(rm_id)

    with richmenu_table.batch_writer() as batch:
        for key in keys_to_delete:
            batch.delete_item(Key=key)

    return success(
        {
            "oaId": oa_id,
            "deletedCount": len(keys_to_delete),
            "deletedIds": deleted_ids,
            "failedCount": len(failed_items),
            "failedItems": failed_items,
        }
    )


@app.route("/v1/files/richmenu-image", methods=["POST"])
def upload_image():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    body = _json()
    oa_id = body.get("oaId") or (app.current_request.query_params or {}).get("oaId")
    image_base64 = body.get("imageBase64")
    if not oa_id or not image_base64:
        return error("VALIDATION_ERROR", "oaId and imageBase64 are required", 400)
    if not _ensure_oa_access(oa_id, user["sub"]):
        return error("NOT_FOUND", "oa not found", 404)
    try:
        upload_result = upload_richmenu_image_base64(
            oa_id=oa_id,
            image_base64=image_base64,
            mime_type=body.get("imageMimeType"),
        )
    except InvalidImageError as exc:
        return error("VALIDATION_ERROR", str(exc), 400)
    return {"data": upload_result}


def _parse_iso_utc(value: str | None) -> datetime | None:
    """
    Parse ISO datetime into timezone-aware UTC datetime.
    Accepts both "...Z" and "...+00:00" formats.
    """
    if not value:
        return None
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_iso_utc(dt: datetime) -> str:
    dt_utc = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt_utc.isoformat().replace("+00:00", "Z")


def _get_plan_cleanup_due_at(plan_end_at_iso: str | None, grace_days: int = 14) -> datetime | None:
    """
    Convert plan end time to cleanup due time (plan end + grace days).
    """
    plan_end_dt = _parse_iso_utc(plan_end_at_iso)
    if not plan_end_dt:
        return None
    return plan_end_dt + timedelta(days=grace_days)


def _is_plan_cleanup_due(plan_end_at_iso: str | None, grace_days: int = 14) -> bool:
    due_at = _get_plan_cleanup_due_at(plan_end_at_iso, grace_days=grace_days)
    if not due_at:
        return False
    return datetime.now(timezone.utc) > due_at


def _compute_plan_end_at(paid_at_iso: str, billing_cycle: str | None) -> str | None:
    """
    Billing rules:
    - Start time: exact paidAt timestamp
    - Monthly: add 31 days, then set "next day 00:00"
    - Yearly: add 365 days, then set "next day 00:00"
    Example: paidAt=2026-03-31 15:00
      - Monthly endAt => 2026-05-02 00:00
    """
    paid_dt = _parse_iso_utc(paid_at_iso)
    if not paid_dt:
        return None
    cycle = (billing_cycle or "monthly").strip().lower()
    if cycle in ("yearly", "12months"):
        add_days = 365
    elif cycle == "6months":
        add_days = 183
    elif cycle == "3months":
        add_days = 92
    else:
        add_days = 31
    end_date = (paid_dt + timedelta(days=add_days)).date() + timedelta(days=1)
    end_dt = datetime(
        year=end_date.year,
        month=end_date.month,
        day=end_date.day,
        hour=0,
        minute=0,
        second=0,
        tzinfo=timezone.utc,
    )
    return _format_iso_utc(end_dt)


def _get_latest_paid_payment_order(oa_id: str) -> dict | None:
    try:
        resp = payment_order_table.query(
            IndexName="gsi_oa_created",
            KeyConditionExpression=Key("oaId").eq(oa_id),
            ScanIndexForward=False,
            Limit=10,
        )
    except ClientError:
        # Caller decides whether to handle DB errors.
        raise
    items = resp.get("Items", []) or []
    for item in items:
        if (item.get("status") or "").strip() == "paid":
            return item
    return None


def _get_payment_validity_for_oa(oa_id: str) -> dict:
    now_dt = datetime.now(timezone.utc)
    order = _get_latest_paid_payment_order(oa_id)
    if not order:
        return {"isPaid": False}

    paid_at_iso = order.get("paidAt") or order.get("paid_at") or ""
    billing_cycle = order.get("billingCycle") or order.get("billing_cycle") or "monthly"

    plan_start_at = order.get("planStartAt") or order.get("plan_start_at") or paid_at_iso
    plan_end_at = order.get("planEndAt") or order.get("plan_end_at") or ""
    if not plan_end_at:
        plan_end_at = _compute_plan_end_at(paid_at_iso, billing_cycle) or ""

    is_active = False
    plan_end_dt = _parse_iso_utc(plan_end_at)
    if plan_end_dt:
        is_active = now_dt < plan_end_dt

    # Best-effort writeback for older records missing plan fields.
    if (order.get("status") or "").strip() == "paid" and (plan_start_at and plan_end_at):
        changed = False
        updated = dict(order)
        if not updated.get("planStartAt") and plan_start_at:
            updated["planStartAt"] = plan_start_at
            changed = True
        if not updated.get("planEndAt") and plan_end_at:
            updated["planEndAt"] = plan_end_at
            changed = True
        if changed:
            updated["updatedAt"] = now_iso()
            payment_order_table.put_item(Item=updated)

    return {
        "isPaid": is_active,
        "planName": order.get("planName"),
        "billingCycle": billing_cycle,
        "paidAt": paid_at_iso or None,
        "planStartAt": plan_start_at or None,
        "planEndAt": plan_end_at or None,
    }

@app.route("/v1/admin/cron/cleanup-expired-richmenus", methods=["POST"])
def admin_cleanup_expired_richmenus():
    """
    排程專用 API：定期掃描並清理過期超過指定寬限期（14天）的 OA 圖文選單。
    """
    
    # ==========================================
    # 1. 權限驗證 (Authentication)
    # ==========================================
    try:
        _require_admin_token()
    except RuntimeError as exc:
        return error("CONFIG_ERROR", str(exc), 500)
    except PermissionError as exc:
        return error("AUTH_UNAUTHORIZED", str(exc), 401)

    body = _json()
    oa_id_filters_raw = body.get("oaIds") if isinstance(body, dict) else None
    oa_id_filters = set()
    if isinstance(oa_id_filters_raw, list):
        oa_id_filters = {str(item).strip() for item in oa_id_filters_raw if str(item).strip()}
    raw_grace_days = body.get("graceDays") if isinstance(body, dict) else None
    if raw_grace_days is None or str(raw_grace_days).strip() == "":
        grace_days = 14
    else:
        try:
            grace_days = int(raw_grace_days)
        except (TypeError, ValueError):
            return error("VALIDATION_ERROR", "graceDays must be an integer", 400)

    now = now_iso()
    scanned_oas = []
    scan_kwargs = {}
    processed_details = []
    failed_details = []

    # ==========================================
    # 2. 獲取全域 OA 列表 (DynamoDB Table Scan)
    # ==========================================
    try:
        while True:
            scan_resp = oa_table.scan(**scan_kwargs)
            scanned_oas.extend(scan_resp.get("Items", []))
            
            last_evaluated_key = scan_resp.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break
            scan_kwargs["ExclusiveStartKey"] = last_evaluated_key
    except ClientError as exc:
        err = exc.response.get("Error") or {}
        return error(
            "DATABASE_ERROR",
            f"{err.get('Code', 'ClientError')}: {err.get('Message', str(exc))}",
            503,
        )

    # ==========================================
    # 3. 逐一檢查 OA 的付費狀態與過期判定
    # ==========================================
    for oa in scanned_oas:
        oa_id = (oa.get("oaId") or "").strip()
        if not oa_id:
            continue
        if oa_id_filters and oa_id not in oa_id_filters:
            continue
            
        try:
            validity = _get_payment_validity_for_oa(oa_id)
        except Exception as exc:
            failed_details.append({"oaId": oa_id, "reason": f"Failed to read payment validity: {str(exc)}"})
            continue

        if validity.get("isPaid"):
            continue
            
        plan_end_at = validity.get("planEndAt")
        
        # 檢查是否超過 14 天寬限期
        if not _is_plan_cleanup_due(plan_end_at, grace_days=grace_days):
            continue

        channel_access_token = _oa_channel_access_token(oa)
        if not channel_access_token:
            failed_details.append({"oaId": oa_id, "reason": "Missing channelAccessToken"})
            continue

        removed_richmenu_ids = []
        removed_alias_ids = []
        failed_richmenus = []

        # ==========================================
        # 4. 執行 LINE API 精準清理作業與資料庫狀態重置
        # ==========================================
        try:
            # [步驟 A] 撈取該 OA 擁有的所有圖文選單 (處理 DynamoDB Query 分頁)
            oa_richmenus = list(_query_richmenus_by_oa(oa_id))

            # [步驟 B] 針對資料庫內有紀錄的選單，逐一刪除 LINE 端實體與 Alias
            rms_to_update = []
            for rm in oa_richmenus:
                line_richmenu_id = (rm.get("lineRichMenuId") or "").strip()
                richmenu_id = (rm.get("richMenuId") or "").strip()
                
                # 若無 lineRichMenuId，代表未曾發佈，直接略過
                if not line_richmenu_id:
                    continue

                # 1. 刪除 LINE 端綁定的 Alias
                if richmenu_id:
                    try:
                        _line_request(
                            method="DELETE",
                            url=f"https://api.line.me/v2/bot/richmenu/alias/{richmenu_id}",
                            channel_access_token=channel_access_token,
                            content_type=None,
                        )
                        removed_alias_ids.append(richmenu_id)
                    except Exception as exc:
                        # 容錯：Alias 可能已不存在；保留流程但記錄訊息以便追查
                        print(
                            f"[cleanup/alias] delete failed (oa_id={oa_id}, richmenu_id={richmenu_id}): {exc}",
                            flush=True,
                        )

                # 2. 刪除 LINE 端的圖文選單實體
                is_line_deleted = False
                try:
                    _line_request(
                        method="DELETE",
                        url=f"https://api.line.me/v2/bot/richmenu/{line_richmenu_id}",
                        channel_access_token=channel_access_token,
                        content_type=None,
                    )
                    removed_richmenu_ids.append(line_richmenu_id)
                    is_line_deleted = True
                except Exception as exc:
                    err_msg = str(exc)
                    # LINE 回 404 視同該選單已不存在，允許同步 DB 狀態
                    if "404" in err_msg:
                        removed_richmenu_ids.append(line_richmenu_id)
                        is_line_deleted = True
                    else:
                        failed_richmenus.append(
                            {
                                "richMenuId": richmenu_id,
                                "lineRichMenuId": line_richmenu_id,
                                "reason": err_msg,
                            }
                        )

                # [步驟 C] 僅當 LINE 端已確認刪除（或已不存在）才更新 DB 為草稿
                if is_line_deleted:
                    rm["status"] = "draft"
                    rm["isDefault"] = False
                    rm["lineRichMenuId"] = None
                    rm["updatedAt"] = now
                    rm["statusUpdatedAt"] = f"draft#{now}"
                    rms_to_update.append(rm)

            # [步驟 D] 累積所有「LINE 端確認刪除」的選單後一次批次寫回，降低 DynamoDB 呼叫次數
            with richmenu_table.batch_writer() as batch:
                for rm in rms_to_update:
                    batch.put_item(Item=rm)

            cleanup_due_at = _get_plan_cleanup_due_at(plan_end_at, grace_days=grace_days)
            processed_details.append(
                {
                    "oaId": oa_id,
                    "planEndAt": plan_end_at,
                    "cleanupDueAt": _format_iso_utc(cleanup_due_at) if cleanup_due_at else None,
                    "removedLineRichMenuCount": len(removed_richmenu_ids),
                    "removedLineRichMenuIds": removed_richmenu_ids,
                    "removedAliasCount": len(removed_alias_ids),
                    "removedAliasIds": removed_alias_ids,
                    "failedRichmenus": failed_richmenus,
                }
            )
        except Exception as exc:
            failed_details.append({"oaId": oa_id, "reason": f"Cleanup failed: {str(exc)}"})

    # ==========================================
    # 5. 回傳排程執行結果總結
    # ==========================================
    return success(
        {
            "summary": {
                "totalOasScanned": len(scanned_oas),
                "oaFilterEnabled": bool(oa_id_filters),
                "oaFilterCount": len(oa_id_filters),
                "graceDays": grace_days,
                "expiredOasProcessed": len(processed_details),
                "failedOas": len(failed_details),
                "executedAt": now,
            },
            "processedDetails": processed_details,
            "failedDetails": failed_details,
        }
    )


def _get_idempotency_key() -> str:
    headers = app.current_request.headers or {}
    raw = (
        headers.get("idempotency-key")
        or headers.get("Idempotency-Key")
        or headers.get("IDEMPOTENCY-KEY")
        or ""
    )
    return str(raw).strip()


def _find_payment_order_by_idempotency(user_id: str, key: str) -> dict | None:
    if not key:
        return None
    try:
        # 取最近 50 筆已建立訂單做比對；同一個用戶在這視窗內重複用同一把 key 視為同一筆
        resp = payment_order_table.query(
            IndexName="gsi_user_created",
            KeyConditionExpression=Key("userId").eq(user_id),
            ScanIndexForward=False,
            Limit=50,
        )
    except ClientError as exc:
        _payment_log(f"idempotency lookup failed: {exc!r}")
        return None
    for raw in resp.get("Items", []) or []:
        if (raw.get("idempotencyKey") or "") == key:
            return raw
    return None


@app.route("/v1/payments/orders", methods=["POST"])
def create_payment_order():
    _payment_log("=== POST /v1/payments/orders 開始 ===")
    try:
        user = _require_auth()
    except PermissionError:
        _payment_log("步驟: 認證失敗 (未登入或 token 無效)")
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)

    body = _json()
    oa_id = (body.get("oaId") or "").strip()
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    oa_item = _ensure_oa_access(oa_id, user["sub"])
    if not oa_item:
        return error("NOT_FOUND", "oa not found", 404)

    # Idempotency-Key 由前端在按下「付款」時隨機產生並重複送同一把；命中即回上次結果，避免重複建單。
    idempotency_key = _get_idempotency_key()
    if idempotency_key:
        existing = _find_payment_order_by_idempotency(user["sub"], idempotency_key)
        if existing and (existing.get("oaId") or "") == oa_id:
            _payment_log(
                f"idempotency hit: key={idempotency_key!r} -> orderId={existing.get('orderId')!r}"
            )
            return {
                "data": {
                    "orderId": existing.get("orderId"),
                    "paymentUrl": existing.get("paymentUrl"),
                    "idempotent": True,
                }
            }

    billing_cycle = str(body.get("billingCycle") or body.get("cycle") or "monthly").strip() or "monthly"
    _payment_log(
        f"步驟 1: 認證通過 userId={user['sub']!r}, oaId={oa_id!r}, billingCycle={billing_cycle!r}"
    )
    # 各方案標準金額（單位：TWD）；env 可覆寫以便測試
    cycle_lower = billing_cycle.lower()
    if cycle_lower in ("yearly", "12months"):
        amount_env = os.environ.get("LINEPAY_AMOUNT_YEARLY") or os.environ.get("LINEPAY_AMOUNT_12MONTHS")
        default_amount = 1790
    elif cycle_lower == "6months":
        amount_env = os.environ.get("LINEPAY_AMOUNT_6MONTHS")
        default_amount = 999
    elif cycle_lower == "3months":
        amount_env = os.environ.get("LINEPAY_AMOUNT_3MONTHS")
        default_amount = 549
    else:
        amount_env = os.environ.get("LINEPAY_AMOUNT_MONTHLY")
        default_amount = 199
    try:
        order_amount = int((amount_env or str(default_amount)).strip())
    except ValueError:
        return error(
            "CONFIG_ERROR",
            "LINEPAY_AMOUNT_* must be a positive integer",
            500,
        )
    if order_amount <= 0:
        return error(
            "CONFIG_ERROR",
            "LINEPAY_AMOUNT_* must be a positive integer",
            500,
        )
    _payment_log(f"步驟 2: 呼叫 chalicelib.linepay.post_linepay_order(amount={order_amount})")

    try:
        resp_body = post_linepay_order(amount=order_amount)
    except ValueError as exc:
        msg = str(exc)
        _payment_log(f"步驟 3: 金流呼叫失敗 ValueError: {msg}")
        if "LINE Pay service HTTP 401" in msg or "401" in msg:
            return error("PAYMENT_GATEWAY_UNAUTHORIZED", msg, 401)
        if "LINE Pay service HTTP 403" in msg or " HTTP 403" in msg:
            hint = " 請確認金流 key 與後台設定。"
            return error("PAYMENT_GATEWAY_FORBIDDEN", msg + hint, 403)
        return error("PAYMENT_GATEWAY_ERROR", msg, 502)

    _payment_log(f"步驟 3: 金流回應 keys={list(resp_body.keys()) if isinstance(resp_body, dict) else 'n/a'}")
    _payment_log(f"步驟 3: 金流回應 (截斷 800 字) = {json.dumps(resp_body, ensure_ascii=False)[:800]}")

    order_id, payment_url = _extract_linepay_order_response(resp_body)
    _payment_log(f"步驟 4: 解析 order_id={order_id!r}, payment_url 有值={bool(payment_url)}")
    if not order_id or not payment_url:
        _payment_log("中止: 無法從金流回應取得 order_id 或 payment_url")
        return error(
            "PAYMENT_GATEWAY_ERROR",
            f"Invalid response from payment service: {json.dumps(resp_body, ensure_ascii=False)[:500]}",
            502,
        )

    linepay_status = resp_body.get("status") if isinstance(resp_body, dict) else None
    product_name_display = "圖文選單費用"

    now = now_iso()
    item = {
        "orderId": order_id,
        "userId": user["sub"],
        "oaId": oa_id,
        "productName": product_name_display,
        "planName": "pro",
        "billingCycle": billing_cycle,
        "amount": order_amount,
        "currency": "TWD",
        "status": "pending",
        "paymentUrl": payment_url,
        "linepayResponseStatus": linepay_status,
        "createdAt": now,
        "updatedAt": now,
    }
    if idempotency_key:
        item["idempotencyKey"] = idempotency_key
    table_name = os.environ.get("PAYMENT_ORDER_TABLE", "line_payment_order")
    _payment_log(
        f"步驟 13: 寫入 DynamoDB table={table_name!r}, orderId={order_id!r}, userId={user['sub']!r}, oaId={oa_id!r}"
    )
    try:
        payment_order_table.put_item(Item=item)
    except ClientError as exc:
        err = exc.response.get("Error") or {}
        code = err.get("Code", "ClientError")
        msg = err.get("Message", str(exc))
        _payment_log(f"中止: DynamoDB put_item 失敗 code={code}, message={msg}")
        return error(
            "DATABASE_ERROR",
            f"{code}: {msg}. If the table is missing, run deploy_dynamodb.sh to create line_payment_order.",
            503,
        )

    _payment_log("步驟 14: 成功，回傳 orderId 與 paymentUrl 給前端")
    _payment_log("=== POST /v1/payments/orders 結束 ===")
    return {"data": {"orderId": order_id, "paymentUrl": payment_url}}


@app.route("/v1/payments/orders", methods=["GET"])
def list_payment_orders():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)

    try:
        resp = payment_order_table.query(
            IndexName="gsi_user_created",
            KeyConditionExpression=Key("userId").eq(user["sub"]),
            ScanIndexForward=False,
        )
    except ClientError as exc:
        err = exc.response.get("Error") or {}
        return error(
            "DATABASE_ERROR",
            f"{err.get('Code', 'ClientError')}: {err.get('Message', str(exc))}",
            503,
        )
    items = [_normalize_dynamo_numbers(i) for i in resp.get("Items", [])]
    return {"data": items}


@app.route("/v1/payments/check", methods=["GET"])
def check_payment():
    try:
        user = _require_auth()
    except PermissionError:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)

    qp = app.current_request.query_params or {}
    oa_id = (qp.get("oaId") or "").strip()
    if not oa_id:
        return error("VALIDATION_ERROR", "oaId is required", 400)
    oa_item = _ensure_oa_access(oa_id, user["sub"])
    if not oa_item:
        return error("NOT_FOUND", "oa not found", 404)

    try:
        validity = _get_payment_validity_for_oa(oa_id)
    except ClientError as exc:
        err = exc.response.get("Error") or {}
        return error(
            "DATABASE_ERROR",
            f"{err.get('Code', 'ClientError')}: {err.get('Message', str(exc))}",
            503,
        )

    return {"data": validity}


PAYMENT_CALLBACK_TS_SKEW_SECONDS = 5 * 60


@app.route("/v1/payments/callback", methods=["GET", "POST"])
def payment_callback():
    # GET: payment service 的簡易 callback（query params: order_id, ts, sig）
    if app.current_request.method == "GET":
        params = app.current_request.query_params or {}
        order_id = (params.get("order_id") or "").strip()
        ts = (params.get("ts") or "").strip()
        sig = (params.get("sig") or "").strip()
        if not order_id or not ts or not sig:
            return error("INVALID_CALLBACK", "order_id, ts, sig are required", 400)
        try:
            ts_int = int(ts)
        except ValueError:
            return error("INVALID_CALLBACK", "ts must be an integer epoch second", 400)
        now_epoch = int(time.time())
        if abs(now_epoch - ts_int) > PAYMENT_CALLBACK_TS_SKEW_SECONDS:
            return error("INVALID_CALLBACK", "timestamp is outside the allowed window", 400)
        company_key = (os.environ.get("LINEPAY_COMPANY_KEY") or "").strip()
        hash_key = (os.environ.get("LINEPAY_HASH_KEY") or "").strip()
        if not company_key or not hash_key:
            return error("CONFIG_ERROR", "LINEPAY_COMPANY_KEY and LINEPAY_HASH_KEY must be configured", 500)
        if not verify_simple_payment_callback(company_key, hash_key, order_id=order_id, ts=ts, sig=sig):
            return error("INVALID_SIGNATURE", "Signature verification failed", 401)
        resp = payment_order_table.get_item(Key={"orderId": order_id})
        order = resp.get("Item")
        if not order:
            return error("ORDER_NOT_FOUND", "Order does not exist", 404)
        billing_cycle = order.get("billingCycle") or order.get("billing_cycle") or "monthly"
        paid_at_iso = now_iso()
        plan_end_at = _compute_plan_end_at(paid_at_iso, billing_cycle)
        if not plan_end_at:
            return error("INVALID_BILLING_CYCLE", "Cannot compute plan end", 400)
        try:
            payment_order_table.update_item(
                Key={"orderId": order_id},
                UpdateExpression=(
                    "SET #status = :paid, paidAt = :paid_at, planStartAt = :plan_start, "
                    "planEndAt = :plan_end, updatedAt = :updated_at"
                ),
                ConditionExpression="#status = :pending",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":paid": "paid",
                    ":pending": "pending",
                    ":paid_at": paid_at_iso,
                    ":plan_start": paid_at_iso,
                    ":plan_end": plan_end_at,
                    ":updated_at": now_iso(),
                },
            )
        except ClientError as exc:
            code = (exc.response.get("Error") or {}).get("Code")
            if code == "ConditionalCheckFailedException":
                return error("ORDER_ALREADY_PROCESSED", "Order is not in pending state", 409)
            err = exc.response.get("Error") or {}
            return error("DATABASE_ERROR", f"{err.get('Code', 'ClientError')}: {err.get('Message', str(exc))}", 503)
        return Response(status_code=200, body="OK", headers={"Content-Type": "text/plain; charset=utf-8"})

    # POST: 改為 POST + body 簽章；簽章包含 amount/status/ts，並驗證 ts 漂移與訂單目前狀態，避免 GET URL 被重放。
    body = _json()
    order_id = (str(body.get("order_id") or "")).strip()
    ts = (str(body.get("ts") or "")).strip()
    sig = (str(body.get("sig") or "")).strip()
    callback_status = (str(body.get("status") or "")).strip().lower()
    raw_amount = body.get("amount")

    if not order_id or not ts or not sig or not callback_status or raw_amount is None:
        return error(
            "INVALID_CALLBACK",
            "order_id, ts, sig, status, amount are required",
            400,
        )

    try:
        callback_amount = int(raw_amount)
    except (TypeError, ValueError):
        return error("INVALID_CALLBACK", "amount must be an integer", 400)
    if callback_amount <= 0:
        return error("INVALID_CALLBACK", "amount must be positive", 400)

    if callback_status not in {"paid", "success"}:
        # 僅接受成功狀態；其他（如 cancelled）走 callback 不應翻訂單為 paid
        return error("INVALID_CALLBACK", f"unsupported status: {callback_status}", 400)

    try:
        ts_int = int(ts)
    except ValueError:
        return error("INVALID_CALLBACK", "ts must be an integer epoch second", 400)
    now_epoch = int(time.time())
    if abs(now_epoch - ts_int) > PAYMENT_CALLBACK_TS_SKEW_SECONDS:
        return error(
            "INVALID_CALLBACK",
            "timestamp is outside the allowed window",
            400,
        )

    company_key = (os.environ.get("LINEPAY_COMPANY_KEY") or "").strip()
    hash_key = (os.environ.get("LINEPAY_HASH_KEY") or "").strip()
    if not company_key or not hash_key:
        return error("CONFIG_ERROR", "LINEPAY_COMPANY_KEY and LINEPAY_HASH_KEY must be configured", 500)

    if not verify_payment_callback(
        company_key,
        hash_key,
        order_id=order_id,
        ts=ts,
        amount=str(callback_amount),
        status=callback_status,
        sig=sig,
    ):
        return error("INVALID_SIGNATURE", "Signature verification failed", 401)

    resp = payment_order_table.get_item(Key={"orderId": order_id})
    order = resp.get("Item")
    if not order:
        return error("ORDER_NOT_FOUND", "Order does not exist", 404)

    # 比對 callback 帶來的金額與訂單實際金額是否一致
    try:
        stored_amount = int(order.get("amount") or 0)
    except (TypeError, ValueError):
        return error("INVALID_AMOUNT", "Invalid stored amount on order", 500)
    if stored_amount != callback_amount:
        _payment_log(
            f"callback amount mismatch order_id={order_id!r} stored={stored_amount} got={callback_amount}"
        )
        return error("INVALID_AMOUNT", "Amount does not match order", 400)

    billing_cycle = order.get("billingCycle") or order.get("billing_cycle") or "monthly"
    paid_at_iso = now_iso()
    plan_start_at = paid_at_iso
    plan_end_at = _compute_plan_end_at(paid_at_iso, billing_cycle)
    if not plan_end_at:
        return error("INVALID_BILLING_CYCLE", "Cannot compute plan end", 400)

    # 條件式更新：只允許 status=pending 才能翻成 paid，避免任何 callback 被重放讓已 paid 訂單再次延長期限
    try:
        payment_order_table.update_item(
            Key={"orderId": order_id},
            UpdateExpression=(
                "SET #status = :paid, paidAt = :paid_at, planStartAt = :plan_start, "
                "planEndAt = :plan_end, updatedAt = :updated_at"
            ),
            ConditionExpression="#status = :pending",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":paid": "paid",
                ":pending": "pending",
                ":paid_at": paid_at_iso,
                ":plan_start": plan_start_at,
                ":plan_end": plan_end_at,
                ":updated_at": now_iso(),
            },
        )
    except ClientError as exc:
        code = (exc.response.get("Error") or {}).get("Code")
        if code == "ConditionalCheckFailedException":
            # 訂單已不在 pending 狀態（已付過或已取消），拒絕重放
            return error(
                "ORDER_ALREADY_PROCESSED",
                "Order is not in pending state",
                409,
            )
        err = exc.response.get("Error") or {}
        return error(
            "DATABASE_ERROR",
            f"{err.get('Code', 'ClientError')}: {err.get('Message', str(exc))}",
            503,
        )

    return Response(status_code=200, body="OK", headers={"Content-Type": "text/plain; charset=utf-8"})


@app.route("/", methods=["GET"])
def health():
    return {"service": "line-oa-richmenu-api", "stage": os.environ.get("CHALICE_STAGE", "dev")}
