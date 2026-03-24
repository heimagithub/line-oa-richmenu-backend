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

## 6. 注意事項

- `files/richmenu-image` 與 `create/update richmenu` 已支援 `imageBase64` 上傳 S3，檔名使用 UUID。
- `channelSecretEnc`/`channelAccessTokenEnc` 目前先用明文儲存，正式環境請改成 KMS 或 Secrets Manager 加密流程。
