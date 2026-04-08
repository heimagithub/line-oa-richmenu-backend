import os
import uuid
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from botocore.exceptions import ClientError
from chalice import Chalice, CORSConfig, Response
from boto3.dynamodb.conditions import Key

from chalicelib.auth import (
    create_access_token,
    create_refresh_token,
    decode_access_token,
    decode_refresh_token,
    hash_password,
    verify_password,
)
from chalicelib.db import (
    get_user_by_id,
    get_user_by_email,
    list_oa,
    list_richmenus,
    now_iso,
    oa_table,
    payment_order_table,
    publish_job_table,
    richmenu_table,
    users_table,
)
from chalicelib.http import error, success
from chalicelib.linepay import post_linepay_order, verify_payment_callback
from chalicelib.storage import (
    get_richmenu_image_url,
    upload_oa_avatar_bytes,
    upload_richmenu_image_base64,
)

app = Chalice(app_name="line-oa-richmenu-api")


def _payment_log(msg: str) -> None:
    print(f"[payments/orders] {msg}", flush=True)


app.debug = True
app.api.cors = CORSConfig(
    allow_origin=os.environ.get("CORS_ALLOW_ORIGIN", "http://localhost:3001"),
    allow_credentials=True,
    allow_headers=["Content-Type", "Authorization"],
)

ACCESS_COOKIE_NAME = "access_token"
REFRESH_COOKIE_NAME = "refresh_token"
# 僅此帳號可在登入／refresh 的 JSON 回應中取得 access_token（其餘帳號僅能透過 HttpOnly cookie）
DEBUG_ACCESS_TOKEN_EMAIL = "heima@gmail.com"
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
    access_token = create_access_token(
        user["userId"], user.get("role", "editor"), ttl_seconds=ACCESS_TOKEN_TTL_SECONDS
    )
    refresh_token = create_refresh_token(
        user["userId"], user.get("role", "editor"), ttl_seconds=REFRESH_TOKEN_TTL_SECONDS
    )
    auth_payload = _build_auth_payload(user)
    body = {"data": auth_payload}
    email_normalized = (user.get("email") or "").strip().lower()
    if email_normalized == DEBUG_ACCESS_TOKEN_EMAIL:
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


def _auth():
    cookies = _get_all_cookies()
    token = cookies.get(ACCESS_COOKIE_NAME)
    if token:
        try:
            return decode_access_token(token)
        except Exception:
            pass
    auth = (app.current_request.headers or {}).get("authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth.replace("Bearer ", "", 1).strip()
    try:
        return decode_access_token(token)
    except Exception:
        return None


def _require_auth():
    payload = _auth()
    if not payload:
        raise PermissionError()
    return payload


def _normalize_dynamo_numbers(item):
    if not item:
        return item
    out = {}
    for k, v in item.items():
        if isinstance(v, Decimal):
            out[k] = int(v) if v % 1 == 0 else float(v)
        else:
            out[k] = v
    return out


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


def _download_image(url: str) -> tuple[bytes, str | None]:
    req = Request(url=url, method="GET")
    with urlopen(req, timeout=20) as resp:
        content_type = resp.headers.get("Content-Type")
        body = resp.read()
    return body, content_type


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


def _ensure_oa_access(oa_id: str, user_id: str):
    oa = oa_table.get_item(Key={"oaId": oa_id}).get("Item")
    if not oa or not _is_owner(oa, user_id):
        return None
    return oa


def _ensure_richmenu_access(oa_id: str, richmenu_id: str, user_id: str):
    item = richmenu_table.get_item(Key={"oaId": oa_id, "richMenuId": richmenu_id}).get("Item")
    if not item or not _is_owner(item, user_id):
        return None
    return item


@app.route("/v1/auth/register", methods=["POST"])
def register():
    body = _json()
    name = (body.get("name") or "").strip()
    email = (body.get("email") or "").strip()
    password = body.get("password") or ""
    confirm = body.get("confirmPassword") or ""
    if not name or not email or not password or password != confirm:
        return error("VALIDATION_ERROR", "invalid register payload", 400)

    exists = get_user_by_email(email)
    if exists:
        return error("AUTH_EMAIL_ALREADY_EXISTS", "Email already registered", 409)

    user_id = f"u_{uuid.uuid4().hex[:8]}"
    now = now_iso()
    users_table.put_item(
        Item={
            "userId": user_id,
            "name": name,
            "email": email,
            "emailNormalized": email.lower(),
            "passwordHash": hash_password(password),
            "role": "editor",
            "status": "active",
            "createdAt": now,
            "updatedAt": now,
        }
    )
    return {"data": {"user": {"userId": user_id, "name": name, "email": email, "role": "editor", "createdAt": now}}}


@app.route("/v1/auth/login", methods=["POST"])
def login():
    body = _json()
    email = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""
    user = get_user_by_email(email)
    if not user or not verify_password(password, user.get("passwordHash", "")):
        return error("AUTH_INVALID_CREDENTIALS", "Email or password is incorrect", 401)
    return _issue_auth_response(user)


@app.route("/v1/auth/refresh", methods=["POST"])
def refresh():
    refresh_token = _get_all_cookies().get(REFRESH_COOKIE_NAME)
    if not refresh_token:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    try:
        payload = decode_refresh_token(refresh_token)
    except Exception:
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)
    user = get_user_by_id(payload.get("sub"))
    if not user:
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
        except Exception:
            # Keep OA binding flow available even if avatar upload fails.
            picture_filename = None
            picture_s3_key = None
    item = {
        "oaId": oa_id,
        "name": display_name,
        "accountId": basic_id,
        "channelSecretEnc": channel_secret,
        "channelAccessTokenEnc": channel_access_token,
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
    current["channelSecretEnc"] = body.get("channelSecret", current.get("channelSecretEnc"))
    current["channelAccessTokenEnc"] = body.get("channelAccessToken", current.get("channelAccessTokenEnc"))
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
    return {"data": items, "paging": {"nextCursor": None}}


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
        upload_result = upload_richmenu_image_base64(
            oa_id=oa_id,
            image_base64=image_base64,
            mime_type=body.get("imageMimeType"),
        )
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
    for key in ["name", "description", "chatBarText", "imageUrl", "size", "areas"]:
        if key in body:
            item[key] = body[key]
    if body.get("imageBase64"):
        upload_result = upload_richmenu_image_base64(
            oa_id=oa_id,
            image_base64=body["imageBase64"],
            mime_type=body.get("imageMimeType"),
        )
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
    if not _ensure_oa_access(oa_id, user["sub"]):
        return error("NOT_FOUND", "oa not found", 404)
    oa_item = _ensure_oa_access(oa_id, user["sub"])
    if not oa_item:
        return error("NOT_FOUND", "oa not found", 404)
    item = _ensure_richmenu_access(oa_id, richmenu_id, user["sub"])
    if not item:
        return error("NOT_FOUND", "richmenu not found", 404)
    channel_access_token = (oa_item.get("channelAccessTokenEnc") or "").strip()
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
    channel_access_token = (oa_item.get("channelAccessTokenEnc") or "").strip()
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

    if not test_publish_without_payment:
        try:
            validity = _get_payment_validity_for_user(user["sub"])
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
        upload_content_type = image_content_type or item.get("imageMimeType") or "image/png"
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
    channel_access_token = (oa_item.get("channelAccessTokenEnc") or "").strip()
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

    resp = richmenu_table.query(KeyConditionExpression=Key("oaId").eq(oa_id))
    for item in resp.get("Items", []):
        if not _is_owner(item, user["sub"]):
            continue
        if item.get("isDefault"):
            item["isDefault"] = False
            item["updatedAt"] = now_iso()
            richmenu_table.put_item(Item=item)
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
    resp = richmenu_table.query(KeyConditionExpression=Key("oaId").eq(oa_id))
    count = 0
    for item in resp.get("Items", []):
        if not _is_owner(item, user["sub"]):
            continue
        if item.get("status") != "draft":
            item["status"] = "draft"
            item["isDefault"] = False
            item["updatedAt"] = now_iso()
            item["statusUpdatedAt"] = f"draft#{item['updatedAt']}"
            richmenu_table.put_item(Item=item)
            count += 1
    return success({"oaId": oa_id, "closedCount": count})


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
    channel_access_token = (oa_item.get("channelAccessTokenEnc") or "").strip()

    removed_count = 0
    removed_ids = []
    failed_items = []

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
            richmenu_table.put_item(Item=item)
            removed_count += 1
            removed_ids.append(richmenu_id)
            continue
        # Not published to LINE before; keep DB item unchanged and return as skipped.
        failed_items.append({"id": richmenu_id, "reason": "NOT_PUBLISHED_TO_LINE"})

    return success(
        {
            "oaId": oa_id,
            "removedCount": removed_count,
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

    channel_access_token = (oa_item.get("channelAccessTokenEnc") or "").strip()
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
    if not _ensure_oa_access(oa_id, user["sub"]):
        return error("NOT_FOUND", "oa not found", 404)
    resp = richmenu_table.query(KeyConditionExpression=Key("oaId").eq(oa_id))
    items = [item for item in resp.get("Items", []) if _is_owner(item, user["sub"])]
    for item in items:
        richmenu_table.delete_item(Key={"oaId": oa_id, "richMenuId": item["richMenuId"]})
    return success({"oaId": oa_id, "deletedCount": len(items)})


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
    upload_result = upload_richmenu_image_base64(
        oa_id=oa_id,
        image_base64=image_base64,
        mime_type=body.get("imageMimeType"),
    )
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
    add_days = 365 if cycle == "yearly" else 31
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


def _get_latest_paid_payment_order(user_id: str) -> dict | None:
    try:
        resp = payment_order_table.query(
            IndexName="gsi_user_created",
            KeyConditionExpression=Key("userId").eq(user_id),
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


def _get_payment_validity_for_user(user_id: str) -> dict:
    now_dt = datetime.now(timezone.utc)
    order = _get_latest_paid_payment_order(user_id)
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


@app.route("/v1/payments/orders", methods=["POST"])
def create_payment_order():
    _payment_log("=== POST /v1/payments/orders 開始 ===")
    try:
        user = _require_auth()
    except PermissionError:
        _payment_log("步驟: 認證失敗 (未登入或 token 無效)")
        return error("AUTH_UNAUTHORIZED", "unauthorized", 401)

    body = _json()
    billing_cycle = str(body.get("billingCycle") or body.get("cycle") or "monthly").strip() or "monthly"
    _payment_log(f"步驟 1: 認證通過 userId={user['sub']!r}, billingCycle={billing_cycle!r}")
    _payment_log("步驟 2: 呼叫 chalicelib.linepay.post_linepay_order()（requests 寫死 payload / key）")

    try:
        resp_body = post_linepay_order()
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
    amount = 1

    now = now_iso()
    item = {
        "orderId": order_id,
        "userId": user["sub"],
        "productName": product_name_display,
        "planName": "pro",
        "billingCycle": billing_cycle,
        "amount": amount,
        "currency": "TWD",
        "status": "pending",
        "paymentUrl": payment_url,
        "linepayResponseStatus": linepay_status,
        "createdAt": now,
        "updatedAt": now,
    }
    table_name = os.environ.get("PAYMENT_ORDER_TABLE", "line_payment_order")
    _payment_log(f"步驟 13: 寫入 DynamoDB table={table_name!r}, orderId={order_id!r}, userId={user['sub']!r}")
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

    try:
        validity = _get_payment_validity_for_user(user["sub"])
    except ClientError as exc:
        err = exc.response.get("Error") or {}
        return error(
            "DATABASE_ERROR",
            f"{err.get('Code', 'ClientError')}: {err.get('Message', str(exc))}",
            503,
        )

    return {"data": validity}


@app.route("/v1/payments/callback", methods=["GET"])
def payment_callback():
    qp = app.current_request.query_params or {}
    order_id = (qp.get("order_id") or "").strip()
    ts = (qp.get("ts") or "").strip()
    sig = (qp.get("sig") or "").strip()

    if not order_id or not ts or not sig:
        return error("INVALID_CALLBACK", "Missing required parameters", 400)

    company_key = (os.environ.get("LINEPAY_COMPANY_KEY") or "").strip()
    hash_key = (os.environ.get("LINEPAY_HASH_KEY") or "").strip()
    if not company_key or not hash_key:
        return error("CONFIG_ERROR", "LINEPAY_COMPANY_KEY and LINEPAY_HASH_KEY must be configured", 500)

    if not verify_payment_callback(company_key, hash_key, order_id, ts, sig):
        return error("INVALID_SIGNATURE", "Signature verification failed", 401)

    resp = payment_order_table.get_item(Key={"orderId": order_id})
    order = resp.get("Item")
    if not order:
        return error("ORDER_NOT_FOUND", "Order does not exist", 404)

    billing_cycle = order.get("billingCycle") or order.get("billing_cycle") or "monthly"
    paid_at_iso = order.get("paidAt") or order.get("paid_at") or ""

    if order.get("status") != "paid":
        paid_at_iso = now_iso()

    plan_start_at = paid_at_iso
    plan_end_at = _compute_plan_end_at(paid_at_iso, billing_cycle)

    should_update = order.get("status") != "paid" or not order.get("planStartAt") or not order.get("planEndAt")
    if should_update and paid_at_iso and plan_end_at:
        updated = dict(order)
        updated["status"] = "paid"
        updated["paidAt"] = paid_at_iso
        updated["planStartAt"] = plan_start_at
        updated["planEndAt"] = plan_end_at
        updated["updatedAt"] = now_iso()
        payment_order_table.put_item(Item=updated)

    return Response(status_code=200, body="OK", headers={"Content-Type": "text/plain; charset=utf-8"})


@app.route("/", methods=["GET"])
def health():
    return {"service": "line-oa-richmenu-api", "stage": os.environ.get("CHALICE_STAGE", "dev")}
