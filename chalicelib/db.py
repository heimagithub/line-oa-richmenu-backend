import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr, Key


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


_dynamodb = boto3.resource("dynamodb")
users_table = _dynamodb.Table(os.environ.get("USERS_TABLE", "line_user"))
oa_table = _dynamodb.Table(os.environ.get("OA_TABLE", "line_oa"))
richmenu_table = _dynamodb.Table(os.environ.get("RICHMENU_TABLE", "line_richmenu"))
publish_job_table = _dynamodb.Table(os.environ.get("PUBLISH_JOB_TABLE", "line_richmenu_publish_job"))
payment_order_table = _dynamodb.Table(os.environ.get("PAYMENT_ORDER_TABLE", "line_payment_order"))


def get_user_by_email(email: str):
    resp = users_table.query(
        IndexName="gsi_email",
        KeyConditionExpression=Key("emailNormalized").eq(email.lower()),
        Limit=1,
    )
    items = resp.get("Items", [])
    return items[0] if items else None


def get_user_by_line_sub(line_sub: str):
    normalized = (line_sub or "").strip()
    if not normalized:
        return None
    # Use scan for compatibility with existing tables that may not have GSI on lineSub yet.
    resp = users_table.scan(FilterExpression=Attr("lineSub").eq(normalized), Limit=1)
    items = resp.get("Items", [])
    return items[0] if items else None


def get_user_by_id(user_id: str):
    return users_table.get_item(Key={"userId": user_id}).get("Item")


def list_oa(user_id: str):
    resp = oa_table.scan(
        FilterExpression=Attr("status").eq("active")
        & (Attr("ownerUserId").eq(user_id) | Attr("createdBy").eq(user_id))
    )
    return resp.get("Items", [])


def list_richmenus(oa_id: str, user_id: str, search: str | None = None):
    resp = richmenu_table.query(KeyConditionExpression=Key("oaId").eq(oa_id))
    items = [
        i
        for i in resp.get("Items", [])
        if i.get("ownerUserId") == user_id or i.get("createdBy") == user_id
    ]
    if search:
        needle = search.lower()
        items = [i for i in items if needle in i.get("nameNormalized", "")]
    return sorted(items, key=lambda x: x.get("updatedAt", ""), reverse=True)
