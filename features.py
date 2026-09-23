"""
features.py — Phase A backend additions for Lantern.

Attaches itself to the app defined in main.py. Exposes:
- register_features(app)          called once at the bottom of main.py
- handle_feature_ws(...)          called from the WS loop in main.py
- reactions_for_message(...)      called by main.py's serializers

Contents (Phase A):
- Backblaze B2 client via the S3-compatible API (boto3)
- /media/upload + /media/sign endpoints
- Media messages (image / file / voice)
- Message reactions (add / remove / list)
- Presence in the Global Room (WS broadcast + REST list)
- Daily quote bot post (ZenQuotes + Lantern-themed drawn card)
- Welcome-in-room announcement on register
"""

import io
import os
import uuid
import asyncio
import random
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from typing import Optional, List

from fastapi import APIRouter, File, UploadFile, HTTPException, Form, Depends
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint, func
from sqlalchemy.orm import Session

# These imports reach into main.py. They only work because
# register_features() is called at the very bottom of main.py.
from main import (
    SessionLocal,
    Base,
    engine,
    User,
    Message,
    Room,
    manager,
    get_current_user,
    get_db,
    get_global_room,
    get_bot,
    display_name_of,
    signed_url_for_key as _unused,  # not used; keep import shape stable
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

ALLOWED_REACTIONS = ["❤️", "😂", "😮", "😢", "👍", "🔥", "🎉"]

DAILY_QUOTE_HOUR_UTC = int(os.environ.get("DAILY_QUOTE_HOUR_UTC", "6"))
DAILY_QUOTE_MINUTE_UTC = int(os.environ.get("DAILY_QUOTE_MINUTE_UTC", "0"))


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


try:
    MessageReaction.__table__.create(bind=engine, checkfirst=True)
except Exception as e:
    print(f"[features] could not create message_reactions table: {e!r}")


def reactions_for_message(db: Session, message_id: int, me_id: int) -> list:
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

    grouped = reactions_for_message(db, message_id, user.id)
    this = next((g for g in grouped if g["emoji"] == emoji),
                {"emoji": emoji, "count": 0, "mine": False})

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
# Presence — in-memory, driven by WS connections
# ---------------------------------------------------------------------

def online_user_ids() -> list:
    """
    Return the list of user ids that currently have at least one live WS
    socket. This is the true "online right now" set.
    """
    try:
        return [uid for uid, socks in manager.active.items() if socks]
    except Exception:
        return []


presence_router = APIRouter(tags=["presence"])


@presence_router.get("/rooms/global/presence")
def room_presence(user: User = Depends(get_current_user),
                  db: Session = Depends(get_db)):
    """
    Return the currently-online users in the room, with avatars.
    The client renders these as a small horizontal row of bubbles.
    """
    ids = online_user_ids()
    if not ids:
        return {"online": [], "count": 0}

    users = (db.query(User)
             .filter(User.id.in_(ids))
             .filter(User.is_bot == False)
             .filter(User.deleted_at == None)
             .all())

    # Exclude the caller from the roster — they know they're online.
    out = []
    for u in users:
        out.append({
            "id": u.id,
            "username": u.username,
            "avatar": u.avatar,
        })

    return {"online": out, "count": len(out)}


async def _broadcast_presence(db: Session):
    """Push the current online roster to every connected socket."""
    ids = online_user_ids()
    users = []
    if ids:
        try:
            rows = (db.query(User)
                    .filter(User.id.in_(ids))
                    .filter(User.is_bot == False)
                    .filter(User.deleted_at == None)
                    .all())
            users = [{"id": u.id, "username": u.username, "avatar": u.avatar}
                     for u in rows]
        except Exception:
            users = []
    payload = {
        "type": "presence_changed",
        "event": "presence_changed",
        "online": users,
        "count": len(users),
    }
    try:
        await manager.broadcast(payload)
    except Exception:
        pass


def presence_on_connect(user_id: int):
    """Called from main.py after a WS connects. Fire-and-forget broadcast."""
    db = SessionLocal()
    try:
        asyncio.create_task(_broadcast_presence(db))
    except Exception as e:
        print(f"[features] presence_on_connect failed: {e!r}")
    finally:
        try:
            db.close()
        except Exception:
            pass


def presence_on_disconnect(user_id: int):
    """Called from main.py after a WS disconnects."""
    db = SessionLocal()
    try:
        asyncio.create_task(_broadcast_presence(db))
    except Exception as e:
        print(f"[features] presence_on_disconnect failed: {e!r}")
    finally:
        try:
            db.close()
        except Exception:
            pass


# ---------------------------------------------------------------------
# Daily quote — fetch, draw, upload, post
# ---------------------------------------------------------------------

ZENQUOTES_URL = "https://zenquotes.io/api/today"
QUOTE_USER_AGENT = "Lantern/1.0 (+https://lantern-dhhb.onrender.com)"


def _fetch_today_quote() -> Optional[dict]:
    """
    Fetch today's quote from ZenQuotes. Returns {"q": "...", "a": "..."}
    or None on failure.
    """
    try:
        req = urllib.request.Request(ZENQUOTES_URL)
        req.add_header("User-Agent", QUOTE_USER_AGENT)
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=15) as r:
            data = r.read().decode("utf-8")
        import json as _json
        arr = _json.loads(data)
        if not isinstance(arr, list) or not arr:
            return None
        first = arr[0]
        q = (first.get("q") or "").strip()
        a = (first.get("a") or "").strip() or "Unknown"
        if not q:
            return None
        return {"q": q, "a": a}
    except Exception as e:
        print(f"[features] quote fetch failed: {e!r}")
        return None


def _palette_for_weekday(weekday: int):
    """
    weekday: 0 = Monday, 6 = Sunday.
    Returns (top_rgb, bottom_rgb, glow_rgb).
    """
    palettes = {
        0: ((20, 30, 70), (10, 15, 40), (90, 130, 220)),   # Mon - cool blue
        1: ((20, 60, 50), (10, 35, 30), (90, 200, 160)),   # Tue - soft green
        2: ((45, 35, 75), (25, 20, 45), (160, 130, 220)),  # Wed - lavender
        3: ((80, 55, 25), (45, 30, 15), (220, 160, 80)),   # Thu - ochre
        4: ((110, 45, 25), (60, 25, 15), (255, 120, 70)),  # Fri - orange
        5: ((95, 60, 80), (55, 35, 50), (240, 160, 200)),  # Sat - pastel pink
        6: ((90, 40, 60), (50, 25, 35), (230, 130, 160)),  # Sun - rose
    }
    return palettes.get(weekday, palettes[4])


def _draw_quote_card(quote_text: str, author: str, weekday: int) -> bytes:
    """
    Render a Lantern-themed quote card and return JPEG bytes.
    Falls back to None on error (caller should handle).
    """
    from PIL import Image, ImageDraw, ImageFont

    W, H = 1080, 1350
    top_rgb, bot_rgb, glow_rgb = _palette_for_weekday(weekday)

    img = Image.new("RGB", (W, H), top_rgb)
    draw = ImageDraw.Draw(img)

    # Vertical gradient
    for y in range(H):
        t = y / float(H - 1)
        r = int(top_rgb[0] * (1 - t) + bot_rgb[0] * t)
        g = int(top_rgb[1] * (1 - t) + bot_rgb[1] * t)
        b = int(top_rgb[2] * (1 - t) + bot_rgb[2] * t)
        draw.line([(0, y), (W, y)], fill=(r, g, b))

    # Soft radial glow near top
    glow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gdraw = ImageDraw.Draw(glow)
    cx, cy, radius = int(W * 0.5), int(H * 0.22), int(W * 0.55)
    steps = 60
    for i in range(steps, 0, -1):
        r = int(radius * i / steps)
        alpha = int(70 * (1 - i / steps))
        gdraw.ellipse(
            [cx - r, cy - r, cx + r, cy + r],
            fill=(glow_rgb[0], glow_rgb[1], glow_rgb[2], alpha),
        )
    img = Image.alpha_composite(img.convert("RGBA"), glow).convert("RGB")
    draw = ImageDraw.Draw(img)

    # Fonts — try a few common ones, fall back to default.
    def load_font(size: int, bold: bool = False):
        candidates = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSerif-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf",
        ]
        for path in candidates:
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
        return ImageFont.load_default()

    def wrap_text(text: str, font, max_width: int):
        words = text.split()
        lines = []
        current = ""
        tmp_draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
        for word in words:
            test = (current + " " + word).strip()
            try:
                w = tmp_draw.textlength(test, font=font)
            except Exception:
                w = len(test) * (font.size if hasattr(font, "size") else 20)
            if w <= max_width:
                current = test
            else:
                if current:
                    lines.append(current)
                current = word
        if current:
            lines.append(current)
        return lines

    # Quote body
    quote_font = load_font(72, bold=False)
    author_font = load_font(40, bold=False)
    header_font = load_font(34, bold=True)

    header_text = "🌅  Good morning, Lantern"
    try:
        hw = draw.textlength(header_text, font=header_font)
    except Exception:
        hw = len(header_text) * 20
    draw.text(((W - hw) / 2, int(H * 0.08)), header_text,
              font=header_font, fill=(255, 255, 255))

    max_text_w = int(W * 0.78)
    lines = wrap_text(quote_text, quote_font, max_text_w)

    line_h = 96
    total_h = len(lines) * line_h
    y = (H - total_h) // 2
    for line in lines:
        try:
            lw = draw.textlength(line, font=quote_font)
        except Exception:
            lw = len(line) * 36
        # subtle drop shadow
        draw.text(((W - lw) / 2 + 2, y + 2), line,
                  font=quote_font, fill=(0, 0, 0))
        draw.text(((W - lw) / 2, y), line,
                  font=quote_font, fill=(255, 255, 255))
        y += line_h

    # Author
    author_line = f"— {author}"
    try:
        aw = draw.textlength(author_line, font=author_font)
    except Exception:
        aw = len(author_line) * 20
    draw.text(((W - aw) / 2, y + 40), author_line,
              font=author_font, fill=(230, 230, 240))

    # Lantern watermark, bottom right — a small glowing circle
    wx, wy, wr = int(W * 0.88), int(H * 0.92), 42
    wm = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    wmd = ImageDraw.Draw(wm)
    for i in range(20, 0, -1):
        rr = int(wr * i / 20)
        alpha = int(120 * (1 - i / 20))
        wmd.ellipse([wx - rr, wy - rr, wx + rr, wy + rr],
                    fill=(255, 210, 150, alpha))
    wmd.ellipse([wx - wr // 2, wy - wr // 2, wx + wr // 2, wy + wr // 2],
                fill=(255, 160, 90, 220))
    img = Image.alpha_composite(img.convert("RGBA"), wm).convert("RGB")
    draw = ImageDraw.Draw(img)

    # Attribution
    attr = "Powered by ZenQuotes"
    attr_font = load_font(22, bold=False)
    try:
        atw = draw.textlength(attr, font=attr_font)
    except Exception:
        atw = len(attr) * 12
    draw.text(((W - atw) / 2, H - 60), attr,
              font=attr_font, fill=(200, 200, 215))

    out = io.BytesIO()
    img.save(out, format="JPEG", quality=88, optimize=True)
    return out.getvalue()


def post_daily_quote_once() -> bool:
    """
    Fetch today's quote, draw the card, upload to B2, post as a media
    message in the Global Room from the bot. Returns True on success.
    """
    db = SessionLocal()
    try:
        bot = get_bot(db)
        if not bot:
            print("[features] daily quote: bot missing")
            return False

        quote = _fetch_today_quote()
        if not quote:
            print("[features] daily quote: fetch failed")
            return False

        text = quote["q"]
        author = quote["a"]

        weekday = datetime.utcnow().weekday()
        try:
            card_bytes = _draw_quote_card(text, author, weekday)
        except Exception as e:
            print(f"[features] daily quote: draw failed: {e!r}")
            return False

        key = f"daily/{datetime.utcnow().strftime('%Y-%m-%d')}-{uuid.uuid4().hex[:8]}.jpg"
        try:
            upload_bytes(card_bytes, key, "image/jpeg")
        except Exception as e:
            print(f"[features] daily quote: upload failed: {e!r}")
            return False

        thumb_key = f"daily/thumbs/{uuid.uuid4().hex[:8]}.jpg"
        try:
            upload_bytes(card_bytes[:200_000], thumb_key, "image/jpeg")
        except Exception:
            thumb_key = None

        room = get_global_room(db)
        caption = f"🌅 Good morning, Lantern.\n\n\"{text}\"\n— {author}"

        msg = Message(
            sender_id=bot.id,
            room_id=room.id,
            content=caption,
            type="image",
            media_key=key,
            media_thumb_key=thumb_key,
            media_name="daily-quote.jpg",
            media_size=len(card_bytes),
            media_width=1080,
            media_height=1350,
        )
        db.add(msg)
        db.commit()
        db.refresh(msg)

        payload = {
            "type": "room_message",
            "event": "room_message",
            "media_kind": "image",
            "room_id": room.id,
            "id": msg.id,
            "sender_id": bot.id,
            "sender_username": bot.username,
            "sender_display_name": "Lantern Good Boy",
            "sender_avatar": bot.avatar,
            "content": caption,
            "created_at": msg.created_at.isoformat(),
            "reply_to": None,
            "edited_at": None,
            "reactions": [],
            "media_url": signed_url_for_key(key),
            "thumb_url": signed_url_for_key(thumb_key),
            "media_name": "daily-quote.jpg",
            "media_size": len(card_bytes),
            "media_width": 1080,
            "media_height": 1350,
            "media_duration": None,
        }

        try:
            asyncio.create_task(manager.broadcast(payload))
        except Exception:
            pass

        print(f"[features] daily quote posted: {msg.id}")
        return True
    except Exception as e:
        print(f"[features] post_daily_quote_once failed: {e!r}")
        return False
    finally:
        try:
            db.close()
        except Exception:
            pass


async def _daily_quote_loop():
    """Wait until the next scheduled time, post the quote, repeat."""
    while True:
        now = datetime.now(timezone.utc)
        target = now.replace(
            hour=DAILY_QUOTE_HOUR_UTC,
            minute=DAILY_QUOTE_MINUTE_UTC,
            second=0,
            microsecond=0,
        )
        if target <= now:
            target = target + timedelta(days=1)
        wait_seconds = (target - now).total_seconds()
        print(f"[features] daily quote scheduled in {int(wait_seconds)}s "
              f"(at {target.isoformat()})")
        try:
            await asyncio.sleep(wait_seconds)
        except asyncio.CancelledError:
            return
        try:
            post_daily_quote_once()
        except Exception as e:
            print(f"[features] daily quote loop error: {e!r}")
        # brief cooldown so we don't double-post if the clock jumps
        await asyncio.sleep(60)


def post_welcome_in_room(new_user_id: int, new_username: str):
    """
    Called from main.py after a successful register, so the bot posts
    a welcome message in the Global Room.
    """
    db = SessionLocal()
    try:
        bot = get_bot(db)
        if not bot:
            return
        room = get_global_room(db)
        text = f"👋 **{new_username}** just joined Lantern. Say hi!"
        msg = Message(
            sender_id=bot.id,
            room_id=room.id,
            content=text,
            type="text",
        )
        db.add(msg)
        db.commit()
        db.refresh(msg)

        payload = {
            "type": "room_message",
            "event": "room_message",
            "media_kind": "text",
            "room_id": room.id,
            "id": msg.id,
            "sender_id": bot.id,
            "sender_username": bot.username,
            "sender_display_name": "Lantern Good Boy",
            "sender_avatar": bot.avatar,
            "content": text,
            "created_at": msg.created_at.isoformat(),
            "reply_to": None,
            "edited_at": None,
            "reactions": [],
            "media_url": None,
            "thumb_url": None,
            "media_name": None,
            "media_size": None,
            "media_width": None,
            "media_height": None,
            "media_duration": None,
        }
        try:
            asyncio.create_task(manager.broadcast(payload))
        except Exception:
            pass
    except Exception as e:
        print(f"[features] post_welcome_in_room failed: {e!r}")
    finally:
        try:
            db.close()
        except Exception:
            pass


# ---------------------------------------------------------------------
# Debug/admin: manual trigger of the daily quote
# ---------------------------------------------------------------------

debug_router = APIRouter(tags=["debug"])


@debug_router.post("/debug/run-daily-quote")
def debug_run_daily_quote(user: User = Depends(get_current_user)):
    """Manually trigger the daily quote post. Useful for testing."""
    if user.is_bot:
        raise HTTPException(status_code=403, detail="Bots can't trigger this")
    success = post_daily_quote_once()
    return {"ok": success}


# ---------------------------------------------------------------------
# Message serialization helpers (called from main.py)
# ---------------------------------------------------------------------

def decorate_message_dict(msg: Message, d: dict) -> dict:
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
    Return True if we handled it. Currently no new WS inbound events.
    """
    return False


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------

def register_features(app):
    """Called at the very bottom of main.py."""
    app.include_router(media_router)
    app.include_router(reactions_router)
    app.include_router(presence_router)
    app.include_router(debug_router)

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

        # Start the daily-quote scheduler. We capture the running loop here
        # because main.py runs inside uvicorn's event loop.
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_daily_quote_loop())
                print("[features] daily quote scheduler started")
            else:
                asyncio.create_task(_daily_quote_loop())
                print("[features] daily quote scheduler created (loop not yet running)")
        except Exception as e:
            print(f"[features] could not start daily quote scheduler: {e!r}")