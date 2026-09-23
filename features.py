"""
features.py — Phase A backend additions for Lantern.

Attaches itself to the app defined in main.py. Exposes:
- register_features(app)          called once at the bottom of main.py
- handle_feature_ws(...)          called from the WS loop in main.py
- reactions_for_message(...)      called by main.py's serializers
- decorate_message_dict(...)      (media) called by main.py

Contents (Phase A):
- Backblaze B2 client via the S3-compatible API (boto3)
- /media/upload + /media/sign endpoints
- Media messages (image / file / voice)
- Message reactions (add / remove / list)
"""

import io
import os
import uuid
from typing import Optional, List

from fastapi import APIRouter, File, UploadFile, HTTPException, Form, Depends
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint, func, and_
from sqlalchemy.orm import Session

# These imports reach into main.py. They only work because
# register_features() is called at the very bottom of main.py.
from main import (
    SessionLocal,
    Base,
    engine,
    User,
    Message,
    manager,
    get_current_user,
    get_db,
)

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

B2_KEY_ID = os.environ.get("B2_KEY_ID")
B2_APP_KEY = os.environ.get("B2_APP_KEY")
B2_BUCKET_NAME = os.environ.get("B2_BUCKET_NAME")
B2_BUCKET_ID = os.environ.get("B2_BUCKET_ID")
B2_REGION = os.environ.get("B2_REGION", "us-west-004")

_b2_endpoint_raw = os.environ.get("B2_ENDPOINT", "")
if _b2_endpoint_raw and not _b2_endpoint_raw.startswith("http"):
    B2_ENDPOINT_URL = "https://" + _b2_endpoint_raw
elif _b2_endpoint_raw:
    B2_ENDPOINT_URL = _b2_endpoint_raw
else:
    B2_ENDPOINT_URL = ""

SIGNED_URL_TTL_SECONDS = 7 * 24 * 60 * 60

MAX_UPLOAD_BYTES = 15 * 1024 * 1024
IMAGE_TARGET_MAX_DIM = 1600
IMAGE_JPEG_QUALITY = 82
THUMB_TARGET_MAX_DIM = 240

# Reactions whitelist. Must match what the client offers.
ALLOWED_REACTIONS = ["❤️", "😂", "😮", "😢", "👍", "🔥", "🎉"]


# ---------------------------------------------------------------------
# B2 client via S3-compatible API (boto3)
# ---------------------------------------------------------------------

import boto3
from botocore.config import Config as BotoConfig

_s3_client = None


def _get_s3():
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
    s3 = _get_s3()
    s3.put_object(Bucket=B2_BUCKET_NAME, Key=key, Body=data, ContentType=content_type)
    return key


def signed_url_for_key(key: Optional[str]) -> Optional[str]:
    if not key:
        return None
    try:
        s3 = _get_s3()
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": B2_BUCKET_NAME, "Key": key},
            ExpiresIn=SIGNED_URL_TTL_SECONDS,
        )
    except Exception as e:
        print(f"[features] signed_url_for_key failed for {key}: {e!r}")
        return None


def delete_key(key: Optional[str]) -> bool:
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
# Image helpers
# ---------------------------------------------------------------------

def _try_compress_image(raw: bytes):
    try:
        from PIL import Image
    except Exception:
        return None
    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except Exception:
        return None

    main_img = img.copy()
    main_img.thumbnail((IMAGE_TARGET_MAX_DIM, IMAGE_TARGET_MAX_DIM))
    main_w, main_h = main_img.size
    if main_img.mode in ("RGBA", "P", "LA"):
        main_img = main_img.convert("RGB")

    main_buf = io.BytesIO()
    main_img.save(main_buf, format="JPEG", quality=IMAGE_JPEG_QUALITY, optimize=True)
    main_bytes = main_buf.getvalue()

    thumb = img.copy()
    thumb.thumbnail((THUMB_TARGET_MAX_DIM, THUMB_TARGET_MAX_DIM))
    if thumb.mode in ("RGBA", "P", "LA"):
        thumb = thumb.convert("RGB")
    thumb_buf = io.BytesIO()
    thumb.save(thumb_buf, format="JPEG", quality=75, optimize=True)
    thumb_bytes = thumb_buf.getvalue()

    return main_bytes, "image/jpeg", thumb_bytes, "image/jpeg", main_w, main_h


# ---------------------------------------------------------------------
# Reactions model
# ---------------------------------------------------------------------

class MessageReaction(Base):
    __tablename__ = "message_reactions"

    id = Column(Integer, primary_key=True, index=True)
    message_id = Column(Integer, ForeignKey("messages.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    emoji = Column(String(16), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("message_id", "user_id", "emoji", name="uq_reaction_once"),
    )


# Create the table immediately (safe if it already exists).
try:
    MessageReaction.__table__.create(bind=engine, checkfirst=True)
except Exception as e:
    print(f"[features] could not create message_reactions table: {e!r}")


def reactions_for_message(db: Session, message_id: int, me_id: int) -> list:
    """
    Group reactions for a message into:
      [{"emoji": "❤️", "count": 3, "mine": true}, ...]
    `mine` is True if the current user has reacted with this emoji.
    """
    try:
        rows = (db.query(MessageReaction)
                .filter(MessageReaction.message_id == message_id)
                .all())
    except Exception:
        return []

    if not rows:
        return []

    grouped = {}
    for r in rows:
        g = grouped.setdefault(r.emoji, {"emoji": r.emoji, "count": 0, "mine": False})
        g["count"] += 1
        if r.user_id == me_id:
            g["mine"] = True

    # Order by the whitelist order so all clients render consistently.
    order = {e: i for i, e in enumerate(ALLOWED_REACTIONS)}
    return sorted(grouped.values(), key=lambda x: order.get(x["emoji"], 99))


# ---------------------------------------------------------------------
# Media routes
# ---------------------------------------------------------------------

media_router = APIRouter(prefix="/media", tags=["media"])


class UploadResult(BaseModel):
    media_key: str
    media_url: str
    thumb_key: Optional[str] = None
    thumb_url: Optional[str] = None
    media_name: str
    media_size: int
    media_type: str
    content_type: str
    width: Optional[int] = None
    height: Optional[int] = None
    duration: Optional[int] = None


@media_router.post("/upload", response_model=UploadResult)
async def upload_media(
    file: UploadFile = File(...),
    kind: str = Form("auto"),
    user: User = Depends(get_current_user),
):
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

    main_bytes = raw
    main_ct = declared_ct or "application/octet-stream"
    thumb_bytes = None
    thumb_ct = None
    width = None
    height = None

    if kind == "image":
        compressed = _try_compress_image(raw)
        if compressed:
            main_bytes, main_ct, thumb_bytes, thumb_ct, width, height = compressed

    ext = _ext_for_ct(main_ct, original_name)
    key = f"media/{user.id}/{uuid.uuid4().hex}{ext}"
    thumb_key = None

    try:
        upload_bytes(main_bytes, key, main_ct)
    except Exception as e:
        print(f"[features] upload failed: {e!r}")
        raise HTTPException(status_code=502, detail=f"Upload failed: {e}")

    if thumb_bytes is not None:
        thumb_key = f"media/{user.id}/thumbs/{uuid.uuid4().hex}.jpg"
        try:
            upload_bytes(thumb_bytes, thumb_key, thumb_ct or "image/jpeg")
        except Exception as e:
            print(f"[features] thumb upload failed: {e!r}")
            thumb_key = None

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


class SignRequest(BaseModel):
    keys: list[str]


class SignResponse(BaseModel):
    urls: dict


@media_router.post("/sign", response_model=SignResponse)
def sign_keys(req: SignRequest, user: User = Depends(get_current_user)):
    out = {}
    for k in req.keys[:200]:
        url = signed_url_for_key(k)
        if url:
            out[k] = url
    return SignResponse(urls=out)


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
# Reactions routes
# ---------------------------------------------------------------------

reactions_router = APIRouter(tags=["reactions"])


class ReactIn(BaseModel):
    emoji: str


@reactions_router.post("/messages/{message_id}/react")
def react_to_message(message_id: int,
                     data: ReactIn,
                     user: User = Depends(get_current_user),
                     db: Session = Depends(get_db)):
    emoji = (data.emoji or "").strip()
    if emoji not in ALLOWED_REACTIONS:
        raise HTTPException(status_code=400, detail="Emoji not allowed")

    m = db.query(Message).filter(Message.id == message_id).first()
    if not m:
        raise HTTPException(status_code=404, detail="Message not found")

    # Can't react to a message that belongs to a DM you're not part of.
    if m.room_id is None and m.receiver_id is not None:
        if user.id not in (m.sender_id, m.receiver_id):
            raise HTTPException(status_code=403, detail="Not your conversation")

    existing = (db.query(MessageReaction)
                .filter(MessageReaction.message_id == message_id)
                .filter(MessageReaction.user_id == user.id)
                .filter(MessageReaction.emoji == emoji)
                .first())

    action = "add"
    if existing:
        db.delete(existing)
        action = "remove"
    else:
        db.add(MessageReaction(message_id=message_id, user_id=user.id, emoji=emoji))
    db.commit()

    # Group after the change so we can broadcast the fresh count.
    grouped = reactions_for_message(db, message_id, user.id)
    this = next((g for g in grouped if g["emoji"] == emoji), {"emoji": emoji, "count": 0, "mine": False})

    payload = {
        "type": "reaction_changed",
        "event": "reaction_changed",
        "message_id": message_id,
        "emoji": emoji,
        "user_id": user.id,
        "action": action,
        "count": this["count"],
        "room_id": m.room_id,
    }

    try:
        import asyncio
        if m.room_id is not None:
            asyncio.create_task(manager.broadcast(payload))
        else:
            asyncio.create_task(manager.send_to(m.sender_id, payload))
            asyncio.create_task(manager.send_to(m.receiver_id, payload))
    except Exception:
        pass

    return {
        "message_id": message_id,
        "emoji": emoji,
        "action": action,
        "count": this["count"],
        "mine": this["mine"],
        "reactions": grouped,
    }


@reactions_router.get("/messages/{message_id}/reactions")
def get_reactions(message_id: int,
                  user: User = Depends(get_current_user),
                  db: Session = Depends(get_db)):
    m = db.query(Message).filter(Message.id == message_id).first()
    if not m:
        raise HTTPException(status_code=404, detail="Message not found")
    if m.room_id is None and m.receiver_id is not None:
        if user.id not in (m.sender_id, m.receiver_id):
            raise HTTPException(status_code=403, detail="Not your conversation")
    return {"reactions": reactions_for_message(db, message_id, user.id)}


# ---------------------------------------------------------------------
# Message serialization helpers (called from main.py)
# ---------------------------------------------------------------------

def decorate_message_dict(msg: Message, d: dict) -> dict:
    """
    Attach media fields (kind, signed URLs, metadata) to a message dict.
    Text messages get nulls.
    """
    mtype = (getattr(msg, "type", None) or "text")
    d["type"] = mtype

    if mtype == "text" or not getattr(msg, "media_key", None):
        d["media_url"] = None
        d["thumb_url"] = None
        d["media_name"] = getattr(msg, "media_name", None)
        d["media_size"] = getattr(msg, "media_size", None)
        d["media_width"] = getattr(msg, "media_width", None)
        d["media_height"] = getattr(msg, "media_height", None)
        d["media_duration"] = getattr(msg, "media_duration", None)
        return d

    d["media_url"] = signed_url_for_key(msg.media_key)
    d["thumb_url"] = signed_url_for_key(msg.media_thumb_key)
    d["media_name"] = msg.media_name
    d["media_size"] = msg.media_size
    d["media_width"] = msg.media_width
    d["media_height"] = msg.media_height
    d["media_duration"] = msg.media_duration
    return d


# ---------------------------------------------------------------------
# WebSocket dispatch — called from main.py
# ---------------------------------------------------------------------

def handle_feature_ws(user_id: int, username: str, db: Session,
                      msg_type: str, data: dict) -> bool:
    """
    Called from the WS loop in main.py for every incoming WS message.
    Return True if we handled it.

    Reactions travel over REST (POST /messages/{id}/react), not WS —
    so there's nothing to handle on the WS receive side here yet.
    Future: presence, group_message, etc.
    """
    return False


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------

def register_features(app):
    """Called at the very bottom of main.py."""
    app.include_router(media_router)
    app.include_router(reactions_router)

    @app.on_event("startup")
    def _features_startup():
        try:
            _get_s3()
            print("[features] B2 S3 client warmed up")
        except Exception as e:
            print(f"[features] B2 S3 warmup skipped: {e!r}")
        try:
            MessageReaction.__table__.create(bind=engine, checkfirst=True)
            print("[features] message_reactions table ready")
        except Exception as e:
            print(f"[features] message_reactions table create failed: {e!r}")