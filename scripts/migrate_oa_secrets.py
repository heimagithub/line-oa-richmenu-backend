"""一次性 migration：把 line_oa 表中明文的 channelSecretEnc / channelAccessTokenEnc
重新以 KMS envelope 加密寫回。

執行前必須設定環境變數：
  - OA_SECRETS_KMS_KEY_ID (例如 alias/oa-secrets)
  - OA_TABLE (預設 line_oa)
  - AWS 認證（AWS_PROFILE 或 AWS_ACCESS_KEY_ID/SECRET）

支援 --dry-run 預覽不寫入。
"""
from __future__ import annotations

import argparse
import os
import sys

# 將 backend/ 加入 sys.path 以便載入 chalicelib
_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.dirname(_HERE)
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

import boto3  # noqa: E402

from chalicelib.crypto import encrypt_secret, is_encrypted  # noqa: E402


SECRET_FIELDS = (
    ("channelSecretEnc", "channelSecret"),
    ("channelAccessTokenEnc", "channelAccessToken"),
)


def migrate(dry_run: bool = False) -> None:
    table_name = os.environ.get("OA_TABLE", "line_oa")
    table = boto3.resource("dynamodb").Table(table_name)

    scanned = 0
    encrypted = 0
    skipped_already_encrypted = 0
    skipped_empty = 0
    failed: list[tuple[str, str]] = []

    last_evaluated_key = None
    while True:
        kwargs = {}
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            scanned += 1
            oa_id = (item.get("oaId") or "").strip()
            if not oa_id:
                failed.append(("<no oaId>", "missing oaId"))
                continue

            updates: dict[str, str] = {}
            for stored_field, logical_field in SECRET_FIELDS:
                value = item.get(stored_field)
                if not value:
                    skipped_empty += 1
                    continue
                if is_encrypted(value):
                    skipped_already_encrypted += 1
                    continue
                try:
                    updates[stored_field] = encrypt_secret(
                        value, oa_id=oa_id, field=logical_field
                    )
                except Exception as exc:  # 不要因單筆錯誤中斷整批
                    failed.append((oa_id, f"{stored_field}: {exc}"))

            if not updates:
                continue

            if dry_run:
                print(f"[DRY-RUN] {oa_id} -> would update {list(updates.keys())}")
                encrypted += 1
                continue

            try:
                update_expr_parts = []
                expr_values = {}
                expr_names = {}
                for idx, (k, v) in enumerate(updates.items()):
                    name_placeholder = f"#f{idx}"
                    value_placeholder = f":v{idx}"
                    update_expr_parts.append(f"{name_placeholder} = {value_placeholder}")
                    expr_names[name_placeholder] = k
                    expr_values[value_placeholder] = v
                table.update_item(
                    Key={"oaId": oa_id},
                    UpdateExpression="SET " + ", ".join(update_expr_parts),
                    ExpressionAttributeNames=expr_names,
                    ExpressionAttributeValues=expr_values,
                )
                encrypted += 1
                print(f"[OK] {oa_id} -> updated {list(updates.keys())}")
            except Exception as exc:
                failed.append((oa_id, f"update_item failed: {exc}"))

        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    print("---- migration summary ----")
    print(f"scanned items           : {scanned}")
    print(f"encrypted (or would)    : {encrypted}")
    print(f"already encrypted skips : {skipped_already_encrypted}")
    print(f"empty value skips       : {skipped_empty}")
    print(f"failed                  : {len(failed)}")
    for oa_id, reason in failed:
        print(f"  - {oa_id}: {reason}")
    if failed:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="預覽不寫入")
    args = parser.parse_args()
    if not os.environ.get("OA_SECRETS_KMS_KEY_ID"):
        print("ERROR: OA_SECRETS_KMS_KEY_ID is not set", file=sys.stderr)
        sys.exit(2)
    migrate(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
