from __future__ import annotations

import base64
import binascii
import os
import uuid
from urllib.parse import urlparse

import boto3


# LINE Rich Menu 圖片官方上限 1MB；保守設 5MB 給 OA 頭像/暫存等其他用途。
DEFAULT_IMAGE_MAX_BYTES = 5 * 1024 * 1024

_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_JPEG_MAGIC = b"\xff\xd8\xff"


class InvalidImageError(ValueError):
    """前端傳入的 imageBase64 解碼失敗、超過大小上限或檔案格式不被允許。"""


def _content_type_from_mime(mime_type: str | None) -> str:
    if mime_type in ("image/jpeg", "image/jpg"):
        return "image/jpeg"
    return "image/png"


def _detect_image_format(binary: bytes) -> str | None:
    """
    依 magic bytes 判斷實際格式，避免相信前端送來的 imageMimeType。
    Rich Menu 僅允許 PNG / JPEG；SVG/HTML/任意檔案會回傳 None 並被呼叫端拒絕。
    """
    if binary.startswith(_PNG_MAGIC):
        return "image/png"
    if binary.startswith(_JPEG_MAGIC):
        return "image/jpeg"
    return None


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
    file_id = f"file_{uuid.uuid4().hex}"

    raw = image_base64 or ""
    if raw.startswith("data:") and "," in raw:
        raw = raw.split(",", 1)[1]
    try:
        binary = base64.b64decode(raw, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise InvalidImageError(f"invalid base64 image: {exc}") from exc

    max_bytes = int(os.environ.get("RICHMENU_IMAGE_MAX_BYTES", str(DEFAULT_IMAGE_MAX_BYTES)))
    if len(binary) == 0:
        raise InvalidImageError("image payload is empty")
    if len(binary) > max_bytes:
        raise InvalidImageError(f"image too large: {len(binary)} bytes (max {max_bytes})")

    # 以 magic bytes 判定實際格式；前端 imageMimeType 僅作為交叉檢查，不可信任
    detected = _detect_image_format(binary)
    if detected is None:
        raise InvalidImageError(
            "unsupported image format; only PNG and JPEG are allowed"
        )
    if mime_type:
        claimed = _content_type_from_mime(mime_type)
        if claimed != detected:
            raise InvalidImageError(
                f"image format mismatch: claimed {claimed!r}, actual {detected!r}"
            )
    content_type = detected
    ext = _ext_from_content_type(content_type)
    key = f"richmenu/{oa_id}/{file_id}.{ext}"

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
    if not image_bytes:
        raise InvalidImageError("avatar payload is empty")
    # OA 頭像來源是 LINE API 抓回來的，理論上是 PNG/JPEG；仍以 magic bytes 確認，避免 LINE 回傳異常或被中間人替換
    detected = _detect_image_format(image_bytes)
    if detected is None:
        raise InvalidImageError(
            "unsupported avatar format; only PNG and JPEG are allowed"
        )
    normalized_content_type = detected
    parsed = urlparse(source_url or "")
    ext = _ext_from_content_type(normalized_content_type)
    if not content_type:
        # 若呼叫端沒提供 content_type，仍允許用副檔名作為輔助提示，但實際 ContentType 一律用 detected
        url_ext = _ext_from_url(parsed.path)
        if url_ext in ("jpg", "png"):
            ext = url_ext if (url_ext == "jpg" and normalized_content_type == "image/jpeg") or (
                url_ext == "png" and normalized_content_type == "image/png"
            ) else ext
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
