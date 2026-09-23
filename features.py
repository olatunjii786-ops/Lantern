"""
features.py — Phase A backend additions for Lantern.

This module attaches itself to the app defined in main.py. It imports
shared pieces (SessionLocal, models, manager, helpers) from main.py,
and exposes a single `register_features(app)` function that main.py
calls at the very bottom of the file.

Contents:
- Backblaze B2 client (upload, signed URL, delete)
- /media/upload endpoint
- image compression / thumbnail generation
"""

import io
import os
import uuid
import base64
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, File, UploadFile, HTTPException, Form
from fastapi.responses import JSONResponse
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

from sqlalchemy.orm import Session
from fastapi import Depends as _Depends

# B2 SDK
from b2sdk.v2 import (
    B2Api,
    InMemoryAccountInfo,
    SqliteAccountInfo,
)
from b2sdk.v2.exception import B2Error


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

B2_KEY_ID = os.environ.get("B2_KEY_ID")
B2_APP_KEY = os.environ.get("B2_APP_KEY")
B2_BUCKET_NAME = os.environ.get("B2_BUCKET_NAME")
B2_BUCKET_ID = os.environ.get("B2_BUCKET_ID")
B2_ENDPOINT = os.environ.get("B2_ENDPOINT")  # e.g. s3.us-west-004.backblazeb2.com

# Signed URL lifetime — 7 days is B2's max for b2_get_download_authorization.
SIGNED_URL_TTL_SECONDS = 7 * 24 * 60 * 60

MAX_UPLOAD_BYTES = 15 * 1024 * 1024  # 15 MB hard cap for a single file
IMAGE_TARGET_MAX_DIM = 1600
IMAGE_JPEG_QUALITY = 82
THUMB_TARGET_MAX_DIM = 240


# ---------------------------------------------------------------------
# B2 client (lazy init, reused across requests)
# ---------------------------------------------------------------------

_b2_api: Optional[B2Api] = None
_b2_bucket = None


def _get_b2():
    """Lazy-init the B2 API client. Uses in-memory account info so we
    don't have to worry about writing SQLite on Render."""
    global _b2_api, _b2_bucket

    if _b2_bucket is not None:
        return _b2_api, _b2_bucket

    if not (B2_KEY_ID and B2_APP_KEY and B2_BUCKET_NAME):
        raise RuntimeError(
            "B2 not configured. Set B2_KEY_ID, B2_APP_KEY, B2_BUCKET_NAME "
            "(and B2_BUCKET_ID) as environment variables."
        )

    info = InMemoryAccountInfo()
    _b2_api = B2Api(info)
    _b2_api.authorize_account("production", B2_KEY_ID, B2_APP_KEY)

    try:
        _b2_bucket = _b2_api.get_bucket_by_name(B2_BUCKET_NAME)
    except Exception as e:
        raise RuntimeError(f"Could not open B2 bucket '{B2_BUCKET_NAME}': {e}")

    return _b2_api, _b2_bucket


def upload_bytes(data: bytes, key: str, content_type: str) -> str:
    """
    Upload raw bytes to B2 under `key`, with a content type.
    Returns the key (so the message row can store it for later deletion).
    """
    _, bucket = _get_b2()
    bucket.upload_bytes(
        data_bytes=data,
        file_name=key,
        content_type=content_type,
    )
    return key


def signed_url_for_key(key: str) -> Optional[str]:
    """
    Return a temporary download URL for a private bucket key.
    Uses b2sdk's built-in presigned URL generator, which works with
    both v2 and v3 of the SDK.
    """
    if not key:
        return None
    try:
        api, bucket = _get_b2()
        # b2sdk's get_download_url_for_file_name with an auth token is the
        # supported way to generate a temporary URL for a private file.
        # In most v2 versions the signature is (file_name, valid_duration).
        try:
            url = bucket.get_download_url_for_file_name(
                key, valid_duration_in_seconds=SIGNED_URL_TTL_SECONDS
            )
        except TypeError:
            # Older v2 signature: (file_name, valid_duration)
            url = bucket.get_download_url_for_file_name(
                key, SIGNED_URL_TTL_SECONDS
            )
        return url
    except Exception as e:
        # Print so we can see it in Render logs if it ever fails again.
        print(f"[features] signed_url_for_key failed for {key}: {e!r}")
        return None

# ---------------------------------------------------------------------
# Image helpers
# ---------------------------------------------------------------------

def _try_compress_image(raw: bytes, content_type: str):
    """
    If the upload is a JPEG/PNG/WebP and Pillow is available, resize and
    re-encode to a JPEG. Also produce a small thumbnail.

    Returns (main_bytes, main_ct, thumb_bytes, thumb_ct, width, height) or
    None if compression couldn't be attempted.
    """
    try:
        from PIL import Image  # Pillow
    except Exception:
        return None

    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except Exception:
        return None

    orig_w, orig_h = img.size

    # Main image: fit inside IMAGE_TARGET_MAX_DIM on the long edge.
    main_img = img.copy()
    main_img.thumbnail((IMAGE_TARGET_MAX_DIM, IMAGE_TARGET_MAX_DIM))
    main_w, main_h = main_img.size

    if main_img.mode in ("RGBA", "P"):
        main_img = main_img.convert("RGB")

    main_buf = io.BytesIO()
    main_img.save(main_buf, format="JPEG", quality=IMAGE_JPEG_QUALITY, optimize=True)
    main_bytes = main_buf.getvalue()

    # Thumbnail
    thumb = img.copy()
    thumb.thumbnail((THUMB_TARGET_MAX_DIM, THUMB_TARGET_MAX_DIM))
    if thumb.mode in ("RGBA", "P"):
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
    user: User = _Depends(get_current_user),
):
    """
    Accept a multipart file upload, push it to B2, return a media key
    and a signed URL. Does NOT create a message — the client then POSTs
    a message with the returned media_key.
    """
    if not (B2_KEY_ID and B2_APP_KEY and B2_BUCKET_NAME):
        raise HTTPException(status_code=503, detail="Media storage not configured")

    # Read the body
    raw = await file.read()
    size = len(raw)
    if size == 0:
        raise HTTPException(status_code=400, detail="Empty file")
    if size > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large (max 15 MB)")

    declared_ct = file.content_type or "application/octet-stream"
    original_name = file.filename or "file"

    # Decide what kind of media this is.
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
    main_ct = declared_ct
    thumb_bytes = None
    thumb_ct = None
    width = None
    height = None

    if kind == "image":
        compressed = _try_compress_image(raw, declared_ct)
        if compressed:
            main_bytes, main_ct, thumb_bytes, thumb_ct, width, height = compressed

    # ---- Build a unique key ----
    ext = _ext_for_ct(main_ct, original_name)
    key = f"media/{user.id}/{uuid.uuid4().hex}{ext}"
    thumb_key = None

    # ---- Upload to B2 ----
    try:
        upload_bytes(main_bytes, key, main_ct)
    except B2Error as e:
        raise HTTPException(status_code=502, detail=f"Upload failed: {e}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upload failed: {e}")

    if thumb_bytes is not None:
        thumb_key = f"media/{user.id}/thumbs/{uuid.uuid4().hex}.jpg"
        try:
            upload_bytes(thumb_bytes, thumb_key, thumb_ct or "image/jpeg")
        except Exception:
            thumb_key = None  # not fatal

    # ---- Signed URLs ----
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
    # Fall back to the original extension if it looks sane.
    if "." in original_name:
        ext = "." + original_name.rsplit(".", 1)[-1].lower()
        if 2 <= len(ext) <= 6 and ext.isprintable():
            return ext
    return ".bin"


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------

def register_features(app):
    """
    Called at the very bottom of main.py. Wires up all Phase A routes
    and startup hooks.
    """
    app.include_router(router)

    @app.on_event("startup")
    def _features_startup():
        # Warm the B2 client so the first upload isn't slow.
        try:
            _get_b2()
        except Exception as e:
            print(f"[features] B2 warmup skipped: {e}")