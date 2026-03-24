#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-ap-northeast-1}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"
CHALICE_STAGE="${CHALICE_STAGE:-dev}"

if ! command -v chalice >/dev/null 2>&1; then
  echo "chalice not found, installing from requirements..."
  python3 -m pip install -r requirements.txt
fi

echo "Deploying Chalice API (stage=$CHALICE_STAGE, region=$AWS_REGION)"
AWS_REGION="$AWS_REGION" AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION" chalice deploy --stage "$CHALICE_STAGE"
echo "Deploy done."
