#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-ap-northeast-1}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"

create_table_if_missing() {
  local table_name="$1"
  local key_schema_json="$2"
  local attr_json="$3"
  local gsi_json="${4:-[]}"

  if aws dynamodb describe-table --table-name "$table_name" --region "$AWS_REGION" >/dev/null 2>&1; then
    echo "[SKIP] table exists: $table_name"
    return
  fi

  echo "[CREATE] $table_name"
  aws dynamodb create-table \
    --region "$AWS_REGION" \
    --table-name "$table_name" \
    --attribute-definitions "$attr_json" \
    --key-schema "$key_schema_json" \
    --billing-mode PAY_PER_REQUEST \
    --global-secondary-indexes "$gsi_json"

  aws dynamodb wait table-exists --table-name "$table_name" --region "$AWS_REGION"
  echo "[READY] $table_name"
}

create_table_if_missing \
  "line_user" \
  '[{"AttributeName":"userId","KeyType":"HASH"}]' \
  '[{"AttributeName":"userId","AttributeType":"S"},{"AttributeName":"emailNormalized","AttributeType":"S"},{"AttributeName":"status","AttributeType":"S"},{"AttributeName":"createdAt","AttributeType":"S"}]' \
  '[{"IndexName":"gsi_email","KeySchema":[{"AttributeName":"emailNormalized","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}},{"IndexName":"gsi_status_created","KeySchema":[{"AttributeName":"status","KeyType":"HASH"},{"AttributeName":"createdAt","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]'

create_table_if_missing \
  "line_oa" \
  '[{"AttributeName":"oaId","KeyType":"HASH"}]' \
  '[{"AttributeName":"oaId","AttributeType":"S"},{"AttributeName":"accountId","AttributeType":"S"},{"AttributeName":"status","AttributeType":"S"},{"AttributeName":"boundAt","AttributeType":"S"}]' \
  '[{"IndexName":"gsi_account_id","KeySchema":[{"AttributeName":"accountId","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}},{"IndexName":"gsi_status_bound","KeySchema":[{"AttributeName":"status","KeyType":"HASH"},{"AttributeName":"boundAt","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]'

create_table_if_missing \
  "line_richmenu" \
  '[{"AttributeName":"oaId","KeyType":"HASH"},{"AttributeName":"richMenuId","KeyType":"RANGE"}]' \
  '[{"AttributeName":"oaId","AttributeType":"S"},{"AttributeName":"richMenuId","AttributeType":"S"},{"AttributeName":"updatedAt","AttributeType":"S"},{"AttributeName":"nameNormalized","AttributeType":"S"},{"AttributeName":"statusUpdatedAt","AttributeType":"S"}]' \
  '[{"IndexName":"gsi_oa_updated","KeySchema":[{"AttributeName":"oaId","KeyType":"HASH"},{"AttributeName":"updatedAt","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}},{"IndexName":"gsi_oa_name","KeySchema":[{"AttributeName":"oaId","KeyType":"HASH"},{"AttributeName":"nameNormalized","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}},{"IndexName":"gsi_oa_status_updated","KeySchema":[{"AttributeName":"oaId","KeyType":"HASH"},{"AttributeName":"statusUpdatedAt","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]'

create_table_if_missing \
  "line_richmenu_publish_job" \
  '[{"AttributeName":"oaId","KeyType":"HASH"},{"AttributeName":"jobId","KeyType":"RANGE"}]' \
  '[{"AttributeName":"oaId","AttributeType":"S"},{"AttributeName":"jobId","AttributeType":"S"},{"AttributeName":"jobStatus","AttributeType":"S"},{"AttributeName":"createdAt","AttributeType":"S"},{"AttributeName":"richMenuIdCreatedAt","AttributeType":"S"}]' \
  '[{"IndexName":"gsi_job_status_created","KeySchema":[{"AttributeName":"jobStatus","KeyType":"HASH"},{"AttributeName":"createdAt","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}},{"IndexName":"gsi_oa_richmenu_created","KeySchema":[{"AttributeName":"oaId","KeyType":"HASH"},{"AttributeName":"richMenuIdCreatedAt","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]'

echo "All DynamoDB tables are ready."
