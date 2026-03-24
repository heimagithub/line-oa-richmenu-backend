import base64
import os
import uuid
from urllib.parse import urlparse

import boto3


def _content_type_from_mime(mime_type: str | None) -> str:
    if mime_type in ("image/jpeg", "image/jpg"):
        return "image/jpeg"
    return "image/png"


def _ext_from_content_type(content_type: str) -> str:
    return "jpg" if content_type == "image/jpeg" else "png"


def _ext_from_url(path: str) -> str:
    if path.lower().endswith(".jpg") or path.lower().endswith(".jpeg"):
        return "jpg"
    if path.lower().endswith(".png"):
        return "png"
    return "jpg"


def upload_richmenu_image_base64(oa_id: str, image_base64: str, mime_type: str | None = None) -> dict:
    bucket = os.environ["RICHMENU_IMAGE_BUCKET"]
    cdn_base_url = os.environ.get("RICHMENU_IMAGE_CDN_BASE_URL")
    content_type = _content_type_from_mime(mime_type)
    ext = _ext_from_content_type(content_type)
    file_id = f"file_{uuid.uuid4().hex}"
    key = f"richmenu/{oa_id}/{file_id}.{ext}"

    raw = image_base64
    if image_base64.startswith("data:") and "," in image_base64:
        raw = image_base64.split(",", 1)[1]
    binary = base64.b64decode(raw)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=binary,
        ContentType=content_type,
    )

    if cdn_base_url:
        image_url = f"{cdn_base_url.rstrip('/')}/{key}"
    else:
        region = os.environ.get("AWS_REGION", "ap-northeast-1")
        image_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

    return {
        "fileId": file_id,
        "s3Key": key,
        "imageUrl": image_url,
        "mimeType": content_type,
        "size": len(binary),
    }


def upload_oa_avatar_bytes(oa_id: str, image_bytes: bytes, source_url: str, content_type: str | None = None) -> dict:
    bucket = os.environ["RICHMENU_IMAGE_BUCKET"]
    cdn_base_url = os.environ.get("RICHMENU_IMAGE_CDN_BASE_URL")
    normalized_content_type = content_type or "image/jpeg"
    if normalized_content_type not in ("image/jpeg", "image/png"):
        normalized_content_type = "image/jpeg"
    parsed = urlparse(source_url or "")
    ext = _ext_from_content_type(normalized_content_type)
    if not content_type:
        ext = _ext_from_url(parsed.path)
    filename = f"{uuid.uuid4().hex}.{ext}"
    key = f"oa/{oa_id}/{filename}"

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=image_bytes,
        ContentType=normalized_content_type,
    )

    if cdn_base_url:
        image_url = f"{cdn_base_url.rstrip('/')}/{key}"
    else:
        region = os.environ.get("AWS_REGION", "ap-northeast-1")
        image_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

    return {
        "fileName": filename,
        "s3Key": key,
        "imageUrl": image_url,
        "mimeType": normalized_content_type,
        "size": len(image_bytes),
    }


def get_richmenu_image_url(image_s3_key: str | None, fallback_url: str | None = None, expires_in: int = 3600) -> str:
    if image_s3_key:
        bucket = os.environ["RICHMENU_IMAGE_BUCKET"]
        s3 = boto3.client("s3")
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": image_s3_key},
            ExpiresIn=expires_in,
        )
    return fallback_url or ""
