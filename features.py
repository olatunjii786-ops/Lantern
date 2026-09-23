"""
features.py — Phase A backend additions for Lantern.

This module attaches itself to the app defined in main.py. It imports
shared pieces (SessionLocal, models, manager, helpers) from main.py,
and exposes a single `register_features(app)` function that main.py
calls at the very bottom of the file.

Contents (Phase A, step 1):
- Backblaze B2 client via the S3-compatible API (boto3)
- /media/upload endpoint
- image compression / thumbnail generation (optional, via Pillow)
"""

import io
import os
import uuid
from typing import Optional

from fastapi import APIRouter, File, UploadFile, HTTPException, Form, Depends
from pydantic import BaseModel

# These imports reach into main.py. They only work because
# register_features() is called at the very bottom of main.py,
# after everything in that file is defined.
from main import (
    SessionLocal,
    User,
    Message,
    manager,
    get_current_user,
    get_db,
    display_name_of,
    reply_preview_of,
    resolve_client_timestamp,
)

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

B2_KEY_ID = os.environ.get("B2_KEY_ID")
B2_APP_KEY = os.environ.get("B2_APP_KEY")
B2_BUCKET_NAME = os.environ.get("B2_BUCKET_NAME")
B2_BUCKET_ID = os.environ.get("B2_BUCKET_ID")
B2_REGION = os.environ.get("B2_REGION", "us-west-004")

# B2_ENDPOINT should be the S3 hostname, e.g. "s3.us-west-004.backblazeb2.com"
_b2_endpoint_raw = os.environ.get("B2_ENDPOINT", "")
if _b2_endpoint_raw and not _b2_endpoint_raw.startswith("http"):
    B2_ENDPOINT_URL = "https://" + _b2_endpoint_raw
elif _b2_endpoint_raw:
    B2_ENDPOINT_URL = _b2_endpoint_raw
else:
    B2_ENDPOINT_URL = ""

# Signed URL lifetime (7 days)
SIGNED_URL_TTL_SECONDS = 7 * 24 * 60 * 60

MAX_UPLOAD_BYTES = 15 * 1024 * 1024       # 15 MB hard cap
IMAGE_TARGET_MAX_DIM = 1600               # compress images to fit this
IMAGE_JPEG_QUALITY = 82
THUMB_TARGET_MAX_DIM = 240


# ---------------------------------------------------------------------
# B2 client via S3-compatible API (boto3)
# ---------------------------------------------------------------------

import boto3
from botocore.config import Config as BotoConfig

_s3_client = None


def _get_s3():
    """Lazy-init the S3 client pointed at B2. Reused across requests."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    if not (B2_KEY_ID and B2_APP_KEY and B2_ENDPOINT_URL and B2_BUCKET_NAME):
        raise RuntimeError(
            "B2 S3 config missing. Need B2_KEY_ID, B2_APP_KEY, B2_ENDPOINT, B2_BUCKET_NAME."
        )

    _s3_client = boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT_URL,
        aws_access_key_id=B2_KEY_ID,
        aws_secret_access_key=B2_APP_KEY,
        config=BotoConfig(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
        region_name=B2_REGION,
    )
    return _s3_client


def upload_bytes(data: bytes, key: str, content_type: str) -> str:
    """Upload raw bytes to B2. Returns the key."""
    s3 = _get_s3()
    s3.put_object(
        Bucket=B2_BUCKET_NAME,
        Key=key,
        Body=data,
        ContentType=content_type,
    )
    return key


def signed_url_for_key(key: str) -> Optional[str]:
    """
    Return a temporary download URL for a private bucket key.
    Uses S3 presigned URLs (boto3), which works reliably across versions.
    """
    if not key:
        return None
    try:
        s3 = _get_s3()
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": B2_BUCKET_NAME, "Key": key},
            ExpiresIn=SIGNED_URL_TTL_SECONDS,
        )
        return url
    except Exception as e:
        print(f"[features] signed_url_for_key failed for {key}: {e!r}")
        return None


def delete_key(key: str) -> bool:
    """Delete a single object. Used later for message deletion cleanup."""
    if not key:
        return False
    try:
        s3 = _get_s3()
        s3.delete_object(Bucket=B2_BUCKET_NAME, Key=key)
        return True
    except Exception as e:
        print(f"[features] delete_key failed for {key}: {e!r}")
        return False


# ---------------------------------------------------------------------
# Image helpers (optional, only if Pillow is installed)
# ---------------------------------------------------------------------

def _try_compress_image(raw: bytes, content_type: str):
    """
    If the upload is an image and Pillow is available, resize and re-encode
    to a JPEG. Also produce a small thumbnail.

    Returns (main_bytes, main_ct, thumb_bytes, thumb_ct, width, height)
    or None if compression couldn't be attempted.
    """
    try:
        from PIL import Image
    except Exception:
        return None

    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except Exception:
        return None

    # Main image
    main_img = img.copy()
    main_img.thumbnail((IMAGE_TARGET_MAX_DIM, IMAGE_TARGET_MAX_DIM))
    main_w, main_h = main_img.size
    if main_img.mode in ("RGBA", "P", "LA"):
        main_img = main_img.convert("RGB")

    main_buf = io.BytesIO()
    main_img.save(main_buf, format="JPEG", quality=IMAGE_JPEG_QUALITY, optimize=True)
    main_bytes = main_buf.getvalue()

    # Thumbnail
    thumb = img.copy()
    thumb.thumbnail((THUMB_TARGET_MAX_DIM, THUMB_TARGET_MAX_DIM))
    if thumb.mode in ("RGBA", "P", "LA"):
        thumb = thumb.convert("RGB")
    thumb_buf = io.BytesIO()
    thumb.save(thumb_buf, format="JPEG", quality=75, optimize=True)
    thumb_bytes = thumb_buf.getvalue()

    return main_bytes, "image/jpeg", thumb_bytes, "image/jpeg", main_w, main_h


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------

router = APIRouter(prefix="/media", tags=["media"])


class UploadResult(BaseModel):
    media_key: str
    media_url: str
    thumb_key: Optional[str] = None
    thumb_url: Optional[str] = None
    media_name: str
    media_size: int
    media_type: str          # "image" | "file" | "voice"
    content_type: str
    width: Optional[int] = None
    height: Optional[int] = None
    duration: Optional[float] = None


@router.post("/upload", response_model=UploadResult)
async def upload_media(
    file: UploadFile = File(...),
    kind: str = Form("auto"),     # "image" | "file" | "voice" | "auto"
    user: User = Depends(get_current_user),
):
    """
    Accept a multipart file upload, push it to B2, return a media key
    and a signed URL. Does NOT create a message — the client then POSTs
    a message with the returned media_key.
    """
    if not (B2_KEY_ID and B2_APP_KEY and B2_ENDPOINT_URL and B2_BUCKET_NAME):
        raise HTTPException(status_code=503, detail="Media storage not configured")

    raw = await file.read()
    size = len(raw)
    if size == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    if size > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large (max 15 MB)")

    declared_ct = (file.content_type or "application/octet-stream").lower()
    original_name = file.filename or "file"

    is_image = declared_ct.startswith("image/")
    is_audio = declared_ct.startswith("audio/")

    if kind == "auto":
        if is_image:
            kind = "image"
        elif is_audio:
            kind = "voice"
        else:
            kind = "file"

    # ---- Images: try to compress + thumbnail ----
    main_bytes = raw
    main_ct = declared_ct or "application/octet-stream"
    thumb_bytes = None
    thumb_ct = None
    width = None
    height = None

    if kind == "image":
        compressed = _try_compress_image(raw, declared_ct)
        if compressed:
            main_bytes, main_ct, thumb_bytes, thumb_ct, width, height = compressed

    # ---- Build unique keys ----
    ext = _ext_for_ct(main_ct, original_name)
    key = f"media/{user.id}/{uuid.uuid4().hex}{ext}"
    thumb_key = None

    # ---- Upload main file ----
    try:
        upload_bytes(main_bytes, key, main_ct)
    except Exception as e:
        print(f"[features] upload failed: {e!r}")
        raise HTTPException(status_code=502, detail=f"Upload failed: {e}")

    # ---- Upload thumbnail (best effort) ----
    if thumb_bytes is not None:
        thumb_key = f"media/{user.id}/thumbs/{uuid.uuid4().hex}.jpg"
        try:
            upload_bytes(thumb_bytes, thumb_key, thumb_ct or "image/jpeg")
        except Exception as e:
            print(f"[features] thumb upload failed: {e!r}")
            thumb_key = None

    # ---- Generate signed URLs ----
    media_url = signed_url_for_key(key)
    thumb_url = signed_url_for_key(thumb_key) if thumb_key else None

    if media_url is None:
        raise HTTPException(status_code=502, detail="Could not sign upload URL")

    return UploadResult(
        media_key=key,
        media_url=media_url,
        thumb_key=thumb_key,
        thumb_url=thumb_url,
        media_name=original_name,
        media_size=len(main_bytes),
        media_type=kind,
        content_type=main_ct,
        width=width,
        height=height,
        duration=None,
    )


def _ext_for_ct(ct: str, original_name: str) -> str:
    ct = (ct or "").lower()
    if ct == "image/jpeg": return ".jpg"
    if ct == "image/png":  return ".png"
    if ct == "image/webp": return ".webp"
    if ct == "image/gif":  return ".gif"
    if ct == "audio/mpeg": return ".mp3"
    if ct == "audio/mp4":  return ".m4a"
    if ct == "audio/aac":  return ".aac"
    if ct == "audio/ogg":  return ".ogg"
    if ct == "audio/webm": return ".webm"
    if ct == "video/mp4":  return ".mp4"
    if ct == "application/pdf": return ".pdf"
    if "." in original_name:
        ext = "." + original_name.rsplit(".", 1)[-1].lower()
        if 2 <= len(ext) <= 6 and ext.isprintable():
            return ext
    return ".bin"


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------

def register_features(app):
    """Called at the very bottom of main.py."""
    app.include_router(router)

    @app.on_event("startup")
    def _features_startup():
        try:
            _get_s3()
            print("[features] B2 S3 client warmed up")
        except Exception as e:
            print(f"[features] B2 S3 warmup skipped: {e!r}")