# LINE OA RichMenu Backend (Chalice)

此資料夾提供：
- Python Chalice API（部署到 AWS Lambda + API Gateway）
- DynamoDB table 建立腳本（使用 `aws-cli`）

## 1. 安裝需求

```bash
cd backend
python3 -m pip install -r requirements.txt
```

## 2. 設定 AWS 認證

請先完成：

```bash
aws configure
```

## 3. 建立 DynamoDB Tables（aws-cli）

```bash
cd backend
chmod +x scripts/deploy_dynamodb.sh
AWS_REGION=ap-northeast-1 ./scripts/deploy_dynamodb.sh
```

## 4. 部署 API（Chalice -> Lambda）

先修改 `backend/.chalice/config.json` 中以下變數：
- `JWT_SECRET`
- `RICHMENU_IMAGE_BUCKET`（圖片要存入的 S3 bucket）
- `RICHMENU_IMAGE_CDN_BASE_URL`（可空，若有 CloudFront/CDN 可填）
- `OA_SECRETS_KMS_KEY_ID`（KMS CMK alias 或 ARN，詳見第 6 章）

```bash
cd backend
chmod +x scripts/deploy_api.sh
AWS_REGION=ap-northeast-1 CHALICE_STAGE=dev ./scripts/deploy_api.sh
```

部署成功後，`chalice` 會輸出 API Gateway URL。

## 5. 目前已實作 API

- Auth: `register`, `login`, `logout`
- OA: `GET/POST /api/v1/oa`, `PUT /api/v1/oa/{oaId}/token`, `DELETE /api/v1/oa/{oaId}`
- RichMenu: `list/get/create/update/delete`
- Publish: `POST /api/v1/richmenus/{id}/publish`, `GET /api/v1/richmenus/{id}/status`
- Utility: `unlink-default`, `close-all`, `delete all by oaId`

## 6. OA Channel Secret/Token 加密（KMS Envelope）

LINE OA 的 `channelSecret` 與 `channelAccessToken` 是高敏感資料，一旦外洩等同 OA 被接管。本專案以 **KMS envelope encryption** 將其儲存為密文寫入 DynamoDB `line_oa` 表的 `channelSecretEnc` / `channelAccessTokenEnc` 欄位。

### 6.1 加密設計

- **演算法**：AES-256-GCM（AEAD，內建認證）
- **Key 管理**：每筆 secret 由 KMS 產生獨立 DEK，以 CMK 加密後與密文一同存放（envelope）
- **EncryptionContext**：`{"oaId": <oa_id>, "field": "channelSecret"|"channelAccessToken"}`，避免密文被搬到別筆 record 解密（cipher swap 防護）
- **儲存格式**：`v1:<base64(json{ek, iv, ct})>`，前綴 `v1:` 預留未來金鑰/演算法輪替能力
- **相關檔案**：
  - [chalicelib/crypto.py](chalicelib/crypto.py) — 加解密模組
  - [app.py](app.py) `_oa_channel_access_token` / `_oa_channel_secret` — 統一讀取入口
  - [scripts/migrate_oa_secrets.py](scripts/migrate_oa_secrets.py) — 既有明文 → 密文 migration

### 6.2 部署到新環境（dev / prod 都適用）

> 以下範例以 prod 為主軸。dev 已部署完成，可作參照。

#### 步驟 1：建立 KMS CMK 與 alias

**prod 與 dev 必須使用「不同」的 CMK**，避免 dev 環境誤碰生產資料。

```bash
# 由具有 KMS 管理權限的 admin user 執行（一般部署 user 不會有 kms:CreateKey 權限）

# 1) 建 key
aws kms create-key \
  --region ap-northeast-1 \
  --description "LINE OA channel secret/token envelope key (PROD)" \
  --key-usage ENCRYPT_DECRYPT \
  --key-spec SYMMETRIC_DEFAULT \
  --tags TagKey=Project,TagValue=line-oa-richmenu TagKey=Stage,TagValue=prod
# 記下回傳的 KeyId

# 2) 建 alias（程式碼透過 alias 引用，未來換 key 不必改 config）
aws kms create-alias \
  --region ap-northeast-1 \
  --alias-name alias/oa-secrets-prod \
  --target-key-id <上一步的 KeyId>

# 3) 啟用自動輪替（每年由 AWS 自動產生新的 backing key）
aws kms enable-key-rotation \
  --region ap-northeast-1 \
  --key-id alias/oa-secrets-prod
```

> dev 對應的 alias 為 `alias/oa-secrets`。命名請保持一致：dev 用 `alias/oa-secrets`、prod 用 `alias/oa-secrets-prod`。

#### 步驟 2：設定 Chalice config

在 `backend/.chalice/config.json` 新增 `prod` stage 並加入環境變數：

```json
"prod": {
  "api_gateway_stage": "api",
  "autogen_policy": false,
  "iam_policy_file": "policy-prod.json",
  "environment_variables": {
    "JWT_SECRET": "<請以強隨機值替換，且不要與 dev 共用>",
    "OA_SECRETS_KMS_KEY_ID": "alias/oa-secrets-prod",
    "...": "其餘環境變數比照 dev 但用 prod 對應值"
  }
}
```

#### 步驟 3：建立 prod IAM policy

複製一份 `backend/.chalice/policy-dev.json` 為 `policy-prod.json`，把 `AllowOaSecretsKms` 區塊的 `kms:ResourceAliases` 條件改成 prod alias：

```json
{
  "Sid": "AllowOaSecretsKms",
  "Effect": "Allow",
  "Action": ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey"],
  "Resource": "arn:aws:kms:*:*:key/*",
  "Condition": {
    "StringEquals": {
      "kms:ResourceAliases": "alias/oa-secrets-prod"
    }
  }
}
```

> DynamoDB / S3 的 Resource 也建議改為 prod 專用的 table / bucket 名稱。

#### 步驟 4：部署 Lambda

```bash
cd backend
AWS_REGION=ap-northeast-1 CHALICE_STAGE=prod ./scripts/deploy_api.sh
```

> **`cryptography` 套件含 native lib**：若部署時遇到 Lambda runtime 找不到 `_rust.so` 之類錯誤，請在 `.chalice/config.json` 的 prod stage 加上 `"automatic_layer": true`，或改用 Chalice 的 Docker-based packaging。

#### 步驟 5：既有資料 migration（只在「prod 已有歷史明文資料」時需要）

若 prod 是全新環境，跳過此步即可——所有新寫入的資料都會直接是密文。

若 prod 已存在明文 OA 資料，需執行一次性 migration：

```bash
# 由具備 prod KMS GenerateDataKey 權限的 IAM user 執行
export OA_SECRETS_KMS_KEY_ID=alias/oa-secrets-prod
export OA_TABLE=<prod 的 line_oa table 名稱>
export AWS_REGION=ap-northeast-1
export AWS_PROFILE=<prod profile>

# 強烈建議先備份
aws dynamodb update-continuous-backups \
  --table-name $OA_TABLE \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
aws dynamodb scan --table-name $OA_TABLE --output json > line_oa_prod_backup_$(date +%Y%m%d_%H%M%S).json

# 預覽
python scripts/migrate_oa_secrets.py --dry-run

# 實際執行
python scripts/migrate_oa_secrets.py

# 冪等驗證（第二次執行應該全部 skip）
python scripts/migrate_oa_secrets.py --dry-run
# 預期：encrypted: 0, already encrypted skips: 2 × <OA 數>
```

#### 步驟 6：驗證

從前端做一次會用到 `channelAccessToken` 的操作（例如發布 richmenu 或 `unlink-default`），確認 Lambda 能正確解密並呼叫 LINE API 成功。

#### 步驟 7：撤回個人 KMS 權限（migration 跑完）

執行 migration 時暫時開給 IAM user 的 KMS 權限，跑完後立刻撤回，避免本機長期保有解密生產資料的能力：

```bash
aws iam delete-user-policy --user-name <your-user> --policy-name OaSecretsKmsUse
```

之後若需再跑 migration，臨時 `put-user-policy` → 跑完 → `delete-user-policy`。日常加解密由 Lambda execution role 處理。

### 6.3 Prod 強化建議（強烈建議）

| 項目 | 說明 |
|---|---|
| **CMK 隔離** | dev / prod 各自獨立 CMK，IAM policy 用 `kms:ResourceAliases` 鎖定，避免跨環境誤用 |
| **Key Rotation** | 一定要 `enable-key-rotation`（每年 AWS 自動換 backing key，舊密文仍可解） |
| **Key Policy** | 預設 key policy 允許「IAM policy 授權」即可使用；若公司 SCP 要求 key policy 明列 principal，需把 Lambda execution role ARN 加進 key policy |
| **CloudTrail** | 啟用 CloudTrail 記錄所有 `kms:Decrypt` / `kms:GenerateDataKey` 呼叫，便於 audit |
| **移除明文相容分支** | prod migration 全數完成且觀察 1～2 週後，把 [chalicelib/crypto.py](chalicelib/crypto.py) 中「非 `v1:` 前綴回傳原值」的兼容邏輯改為丟例外，徹底防止退化為明文 |
| **Logging 審查** | 確認任何 log 都不會 dump 整包 `oa_item`（避免 access token 落入 CloudWatch） |

### 6.4 故障排除

| 錯誤訊息 | 原因 | 解法 |
|---|---|---|
| `OA_SECRETS_KMS_KEY_ID is not configured` | 環境變數未設定 | 檢查對應 stage 的 `config.json` |
| `AccessDeniedException ... kms:GenerateDataKey` | IAM policy 沒授權 | 檢查 `policy-<stage>.json` 的 `AllowOaSecretsKms` 區塊；個人 user 跑 migration 則檢查該 user 的 IAM policy |
| `InvalidCiphertextException` | EncryptionContext 不一致（oaId 對不上）或 key 不對 | 確認 `oaId` 寫入時與讀取時一致；確認用的是同一把 CMK |
| Lambda cold start 時 `_rust.so not found` | `cryptography` native lib 未正確打包 | 在 `.chalice/config.json` 加 `"automatic_layer": true` 或改用 Docker-based packaging |

## 7. 注意事項

- `files/richmenu-image` 與 `create/update richmenu` 已支援 `imageBase64` 上傳 S3，檔名使用 UUID。
- `channelSecretEnc`/`channelAccessTokenEnc` 已採用 KMS envelope encryption（詳見第 6 章），不再以明文儲存。
