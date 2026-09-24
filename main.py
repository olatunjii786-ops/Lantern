"""
Lantern Backend — FastAPI + Neon Postgres + JWT + WebSockets
Single-file backend for the Lantern chat app.

Includes:
- Auth, profile, discover, search, conversations, messages
- Read tracking, soft-delete, typing events
- Swipe-to-reply, edit within 20 minutes
- Media messages (image / file / voice / video)
- Message reactions
- Presence in the Global Room
- Daily quote bot post
- Global Room, releases / admin panel, broadcast
- Bot account ("thegoodboy")

Phase A+ features live in features.py and are registered at the bottom.
"""

import os
import secrets
import base64
from datetime import datetime, timedelta
from typing import Optional, List

import bcrypt
from jose import jwt, JWTError
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query, Header
from fastapi.responses import HTMLResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, Boolean, create_engine, or_, and_, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------

DATABASE_URL = os.environ.get("DATABASE_URL")
SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(32))
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7
ONLINE_WINDOW_SECONDS = 300
MAX_AVATAR_BYTES = 200_000
ROOM_RATE_LIMIT_SECONDS = 2
EDIT_WINDOW_SECONDS = 20 * 60

BOT_USERNAME = "thegoodboy"
BOT_BIO = "Your guide to Lantern. I welcome new users, post announcements, and answer app questions. I'm a bot — not a person. Type 'help' any time."
BOT_PASSWORD = secrets.token_hex(32)

DEFAULT_ROOM_NAME = "Global Room"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=300,
    pool_timeout=30,
    connect_args={"sslmode": "require"},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=True)
    phone = Column(String, unique=True, index=True, nullable=True)
    hashed_password = Column(String, nullable=False)
    bio = Column(String, nullable=True)
    interests = Column(String, nullable=True)
    avatar = Column(Text, nullable=True)
    is_bot = Column(Boolean, default=False, nullable=False)

    show_bio = Column(Boolean, default=True, nullable=False)
    show_interests = Column(Boolean, default=True, nullable=False)
    show_online = Column(Boolean, default=True, nullable=False)

    deleted_at = Column(DateTime(timezone=True), nullable=True)
    room_last_read_at = Column(DateTime(timezone=True), nullable=True)

    last_seen = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Room(Base):
    __tablename__ = "rooms"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)
    sender_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    receiver_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    room_id = Column(Integer, ForeignKey("rooms.id"), nullable=True)
    content = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    read_at = Column(DateTime(timezone=True), nullable=True)
    reply_to_id = Column(Integer, ForeignKey("messages.id"), nullable=True)
    edited_at = Column(DateTime(timezone=True), nullable=True)

    type = Column(String, nullable=False, server_default="text")
    media_key = Column(String, nullable=True)
    media_thumb_key = Column(String, nullable=True)
    media_name = Column(String, nullable=True)
    media_size = Column(Integer, nullable=True)
    media_width = Column(Integer, nullable=True)
    media_height = Column(Integer, nullable=True)
    media_duration = Column(Integer, nullable=True)

    sender = relationship("User", foreign_keys=[sender_id])
    receiver = relationship("User", foreign_keys=[receiver_id])


class Release(Base):
    __tablename__ = "releases"

    id = Column(Integer, primary_key=True, index=True)
    version_code = Column(Integer, nullable=False, index=True)
    version_name = Column(String, nullable=False)
    download_url = Column(String, nullable=False)
    notes = Column(Text, nullable=True)
    blocking = Column(Boolean, default=False, nullable=False)
    published_at = Column(DateTime(timezone=True), server_default=func.now())


Base.metadata.create_all(bind=engine)


def _ensure_column(table: str, column: str, coltype: str, default: Optional[str] = None):
    with engine.begin() as conn:
        try:
            ddl = f"ALTER TABLE {table} ADD COLUMN {column} {coltype}"
            if default is not None:
                ddl += f" DEFAULT {default}"
            conn.execute(text(ddl))
        except Exception:
            pass

_ensure_column("users", "bio", "VARCHAR")
_ensure_column("users", "interests", "VARCHAR")
_ensure_column("users", "avatar", "TEXT")
_ensure_column("users", "last_seen", "TIMESTAMP WITH TIME ZONE")
_ensure_column("users", "is_bot", "BOOLEAN", "FALSE")
_ensure_column("users", "show_bio", "BOOLEAN", "TRUE")
_ensure_column("users", "show_interests", "BOOLEAN", "TRUE")
_ensure_column("users", "show_online", "BOOLEAN", "TRUE")
_ensure_column("users", "deleted_at", "TIMESTAMP WITH TIME ZONE")
_ensure_column("users", "room_last_read_at", "TIMESTAMP WITH TIME ZONE")
_ensure_column("messages", "read_at", "TIMESTAMP WITH TIME ZONE")
_ensure_column("messages", "room_id", "INTEGER")
_ensure_column("messages", "reply_to_id", "INTEGER")
_ensure_column("messages", "edited_at", "TIMESTAMP WITH TIME ZONE")
_ensure_column("messages", "type", "VARCHAR", "'text'")
_ensure_column("messages", "media_key", "VARCHAR")
_ensure_column("messages", "media_thumb_key", "VARCHAR")
_ensure_column("messages", "media_name", "VARCHAR")
_ensure_column("messages", "media_size", "INTEGER")
_ensure_column("messages", "media_width", "INTEGER")
_ensure_column("messages", "media_height", "INTEGER")
_ensure_column("messages", "media_duration", "INTEGER")

with engine.begin() as conn:
    try:
        conn.execute(text("ALTER TABLE messages ALTER COLUMN receiver_id DROP NOT NULL"))
    except Exception:
        pass


# ---------------------------------------------------------------------
# Global Room helpers
# ---------------------------------------------------------------------

def get_global_room(db: Session) -> Room:
    room = db.query(Room).order_by(Room.id.asc()).first()
    if room:
        return room
    room = Room(name=DEFAULT_ROOM_NAME)
    db.add(room)
    db.commit()
    db.refresh(room)
    return room


# ---------------------------------------------------------------------
# Reply helpers
# ---------------------------------------------------------------------

def reply_preview_of(db: Session, msg: Message) -> Optional[dict]:
    if not msg.reply_to_id:
        return None
    parent = db.query(Message).filter(Message.id == msg.reply_to_id).first()
    if not parent:
        return None
    parent_sender = db.query(User).filter(User.id == parent.sender_id).first()
    text = parent.content or ""
    if len(text) > 100:
        text = text[:97] + "..."
    return {
        "id": parent.id,
        "sender_id": parent.sender_id,
        "sender_username": parent_sender.username if parent_sender else "unknown",
        "content": text,
    }


# ---------------------------------------------------------------------
# Bot helpers
# ---------------------------------------------------------------------

def get_bot(db: Session) -> Optional[User]:
    return db.query(User).filter(User.username == BOT_USERNAME).first()


def ensure_bot_exists():
    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.username == BOT_USERNAME).first()
        if existing:
            if not existing.is_bot:
                existing.is_bot = True
                db.commit()
            return
        bot = User(
            username=BOT_USERNAME,
            hashed_password=bcrypt.hashpw(BOT_PASSWORD.encode(), bcrypt.gensalt()).decode(),
            bio=BOT_BIO,
            interests="",
            is_bot=True,
            last_seen=datetime.utcnow(),
            show_bio=True,
            show_interests=True,
            show_online=True,
        )
        db.add(bot)
        db.commit()
    finally:
        db.close()


def send_bot_message(db: Session, receiver_id: int, content: str) -> Optional[Message]:
    bot = get_bot(db)
    if not bot:
        return None
    msg = Message(sender_id=bot.id, receiver_id=receiver_id, content=content)
    db.add(msg)
    db.commit()
    db.refresh(msg)
    return msg


def bot_welcome_text(username: str) -> str:
    return (
        f"Hey {username} 👋 I'm Lantern Good Boy — the app's bot.\n\n"
        "I'm not a person, but I'm here to help.\n\n"
        "• Tap Discover to find real people to chat with.\n"
        "• Tap 'Global Room' at the top to talk with everyone.\n"
        "• Type 'help' any time to see what I can do.\n\n"
        "Good luck out there."
    )


def bot_help_text() -> str:
    return (
        "Here's what I can help with. Just type one of these words:\n\n"
        "• profile — how to change your name, photo, or bio\n"
        "• delete — how to delete a message or a whole chat\n"
        "• notifications — how to control alerts\n"
        "• update — how to get the latest version\n"
        "• privacy — what this app stores about you\n"
        "• discover — how to find people to talk to\n"
        "• room — about the Global Room\n"
        "• reply — how to reply to a message\n"
        "• edit — how to edit your own messages\n"
        "• media — how to send photos and files\n\n"
        "I can't chat freely — I'm just here to help and share announcements."
    )


def bot_reply(db: Session, user: User, text: str) -> str:
    t = (text or "").strip().lower()

    if t in ("help", "hi", "hello", "hey", "start"):
        return bot_help_text()

    if "media" in t or "photo" in t or "image" in t or "picture" in t or "file" in t:
        return (
            "To send a photo or file:\n\n"
            "• Open any chat and tap the attachment icon\n"
            "• Pick an image or file from your phone\n"
            "• Add an optional caption, then send\n\n"
            "You can send photos, videos, files, and voice notes."
        )

    if "room" in t or "global" in t:
        return (
            "The Global Room is one big chat where everyone on Lantern can talk.\n\n"
            "• It's at the top of your Chats tab\n"
            "• Everyone who joins Lantern is automatically in it\n"
            "• Be kind — everyone can see your messages"
        )

    if "profile" in t or "avatar" in t or "bio" in t:
        return (
            "To edit your profile:\n\n"
            "1. Go to the Chats tab\n"
            "2. Tap 'Profile' at the top right\n"
            "3. Change your photo, username, bio, or interests\n"
            "4. Tap 'Save changes'\n\n"
            "Your avatar is what other users see next to your name."
        )

    if "delete" in t:
        return (
            "To delete things:\n\n"
            "• One message: long-press the bubble in a chat, tap Delete\n"
            "• A whole chat: long-press the person's row on the Chats tab, tap Delete\n\n"
            "Deleted messages are removed from both sides."
        )

    if "edit" in t:
        return (
            "To edit a message:\n\n"
            "• Long-press your own message, tap Edit\n"
            "• Change the text and tap the ✓ button\n"
            "• You can only edit within 20 minutes of sending\n\n"
            "An '(edited)' tag appears next to the time."
        )

    if "reply" in t or "quote" in t or "swipe" in t:
        return (
            "To reply to a message:\n\n"
            "• Swipe the message bubble to the right\n"
            "• A reply bar appears above the input — type your reply\n"
            "• Tap the quoted block on any reply to jump back to the original"
        )

    if "notification" in t or "alert" in t or "sound" in t:
        return (
            "Notifications:\n\n"
            "• Lantern uses a background service to keep you connected\n"
            "• If you don't want notifications, open your phone's Settings, find Lantern, and turn off its notifications\n"
            "• You'll still receive messages, you just won't be alerted"
        )

    if "update" in t or "version" in t:
        return (
            "To update Lantern:\n\n"
            "• When a new version is live, the app shows a dialog with an Update button\n"
            "• Tap Update and install the new version\n"
            "• Your account, chats, and profile all carry over — nothing is lost\n\n"
            "Some updates are required; you'll be told when they are."
        )

    if "privacy" in t or "data" in t or "safe" in t:
        return (
            "What Lantern stores:\n\n"
            "• Your username, and email or phone (whichever you gave)\n"
            "• Your avatar, bio, and interests\n"
            "• Your messages — only visible to you and the person you sent them to\n\n"
            "The bot only sees messages you send directly to me. "
            "I never read your other chats."
        )

    if "discover" in t or "find" in t or "people" in t or "meet" in t:
        return (
            "To find people:\n\n"
            "• Tap Discover — you'll see users ranked by shared interests\n"
            "• Tap the search bar to filter by username, bio, or interest\n"
            "• Or tap the ＋ button on the Chats tab to search by exact username\n\n"
            "A good opener: mention something from their bio or a shared interest."
        )

    return (
        "I can't chat freely — I'm a bot for help and announcements only.\n\n"
        "Type 'help' to see what I can do."
    )


# ---------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------

class UserCreate(BaseModel):
    username: str
    email: Optional[EmailStr] = None
    phone: Optional[str] = None
    password: str
    interests: Optional[List[str]] = None
    bio: Optional[str] = None


class UserOut(BaseModel):
    id: int
    username: str
    email: Optional[str]
    phone: Optional[str]
    bio: Optional[str]
    interests: Optional[str]
    avatar: Optional[str]
    is_bot: bool
    is_deleted: bool
    show_bio: bool
    show_interests: bool
    show_online: bool
    online: bool

    class Config:
        from_attributes = True


class PublicUserOut(BaseModel):
    id: int
    username: str
    display_name: str
    avatar: Optional[str]
    is_bot: bool
    is_deleted: bool
    bio: Optional[str]
    interests: List[str]
    online: Optional[bool]
    last_seen_text: Optional[str]


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class ProfileUpdate(BaseModel):
    username: Optional[str] = None
    bio: Optional[str] = None
    phone: Optional[str] = None
    interests: Optional[List[str]] = None
    avatar: Optional[str] = None
    show_bio: Optional[bool] = None
    show_interests: Optional[bool] = None
    show_online: Optional[bool] = None


class DiscoverUser(BaseModel):
    id: int
    username: str
    bio: Optional[str]
    interests: List[str]
    avatar: Optional[str]
    is_bot: bool
    online: Optional[bool]
    last_seen_text: Optional[str]
    shared_interests: List[str]


class ConversationOut(BaseModel):
    user_id: int
    username: str
    display_name: str
    avatar: Optional[str]
    is_bot: bool
    is_deleted: bool
    last_message: str
    last_timestamp: str
    online: Optional[bool]
    last_seen_text: Optional[str]
    unread_count: int


class RoomOut(BaseModel):
    id: int
    name: str
    member_count: int
    unread_count: int
    last_message_preview: Optional[str]


class RoomMessageOut(BaseModel):
    id: int
    sender_id: int
    sender_username: str
    sender_display_name: str
    sender_avatar: Optional[str]
    content: str
    created_at: str
    reply_to: Optional[dict] = None
    edited_at: Optional[str] = None
    type: str = "text"
    media_url: Optional[str] = None
    thumb_url: Optional[str] = None
    media_name: Optional[str] = None
    media_size: Optional[int] = None
    media_width: Optional[int] = None
    media_height: Optional[int] = None
    media_duration: Optional[int] = None
    reactions: List[dict] = []


class RoomMemberOut(BaseModel):
    id: int
    username: str
    avatar: Optional[str]
    online: Optional[bool]


class SendMessageIn(BaseModel):
    to: int
    content: str = ""
    created_at: Optional[str] = None
    reply_to_id: Optional[int] = None
    type: Optional[str] = "text"
    media_key: Optional[str] = None
    media_thumb_key: Optional[str] = None
    media_name: Optional[str] = None
    media_size: Optional[int] = None
    media_width: Optional[int] = None
    media_height: Optional[int] = None
    media_duration: Optional[int] = None


class SendRoomMessageIn(BaseModel):
    content: str = ""
    created_at: Optional[str] = None
    reply_to_id: Optional[int] = None
    type: Optional[str] = "text"
    media_key: Optional[str] = None
    media_thumb_key: Optional[str] = None
    media_name: Optional[str] = None
    media_size: Optional[int] = None
    media_width: Optional[int] = None
    media_height: Optional[int] = None
    media_duration: Optional[int] = None


class MessageOut(BaseModel):
    id: int
    sender_id: int
    receiver_id: int
    content: str
    created_at: str
    read_at: Optional[str]
    reply_to: Optional[dict] = None
    edited_at: Optional[str] = None
    type: str = "text"
    media_url: Optional[str] = None
    thumb_url: Optional[str] = None
    media_name: Optional[str] = None
    media_size: Optional[int] = None
    media_width: Optional[int] = None
    media_height: Optional[int] = None
    media_duration: Optional[int] = None
    reactions: List[dict] = []


class EditMessageIn(BaseModel):
    content: str


class ReleaseCreate(BaseModel):
    version_code: int
    version_name: str
    download_url: str
    notes: Optional[str] = ""
    blocking: Optional[bool] = False


class ReleaseOut(BaseModel):
    version_code: int
    version_name: str
    download_url: str
    notes: str
    blocking: bool
    published_at: str


class BroadcastIn(BaseModel):
    content: str


class DeleteAccountIn(BaseModel):
    confirm_username: str


class RenameRoomIn(BaseModel):
    name: str


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def create_access_token(user_id: int, username: str) -> str:
    payload = {
        "sub": str(user_id),
        "username": username,
        "exp": datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid token")
    user = db.query(User).filter(User.id == int(payload["sub"])).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    if user.deleted_at is not None:
        raise HTTPException(status_code=401, detail="Account deleted")
    user.last_seen = datetime.utcnow()
    db.commit()
    return user


def require_admin(key: Optional[str]):
    if not ADMIN_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_KEY not configured on server")
    if key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Bad admin key")


def parse_interests(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [t.strip().lower() for t in raw.split(",") if t.strip()]


def serialize_interests(tags: Optional[List[str]]) -> Optional[str]:
    if not tags:
        return None
    return ",".join(t.strip().lower() for t in tags if t.strip())


def is_online(user: User) -> bool:
    if user.is_bot:
        return True
    if user.deleted_at is not None:
        return False
    if not user.last_seen:
        return False
    delta = datetime.utcnow() - user.last_seen.replace(tzinfo=None)
    return delta.total_seconds() <= ONLINE_WINDOW_SECONDS


def humanize_last_seen(user: User) -> str:
    if user.is_bot:
        return "online"
    if not user.last_seen:
        return "a long time ago"
    delta = datetime.utcnow() - user.last_seen.replace(tzinfo=None)
    secs = int(delta.total_seconds())

    if secs < 60:
        return "just now"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    if secs < 172800:
        return "yesterday"
    return f"{secs // 86400}d ago"


def validate_avatar(avatar: Optional[str]) -> Optional[str]:
    if avatar is None:
        return None
    if avatar == "":
        return None
    if len(avatar) > MAX_AVATAR_BYTES:
        raise HTTPException(status_code=400, detail="Avatar too large")
    if avatar.startswith("data:image/"):
        return avatar
    try:
        base64.b64decode(avatar, validate=True)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 avatar")
    return "data:image/jpeg;base64," + avatar


def display_name_of(user: User) -> str:
    if user.deleted_at is not None:
        return "Deleted User"
    return user.username


def resolve_client_timestamp(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        parsed_naive = parsed.replace(tzinfo=None)
        delta = abs((datetime.utcnow() - parsed_naive).total_seconds())
        if delta < 60:
            return parsed_naive
    except Exception:
        pass
    return None


def public_view_of(user: User) -> PublicUserOut:
    if user.is_bot:
        return PublicUserOut(
            id=user.id, username=user.username, display_name="Lantern Good Boy",
            avatar=user.avatar, is_bot=True, is_deleted=False,
            bio=user.bio or "", interests=[],
            online=True, last_seen_text="online",
        )
    if user.deleted_at is not None:
        return PublicUserOut(
            id=user.id, username=user.username, display_name="Deleted User",
            avatar=None, is_bot=False, is_deleted=True,
            bio=None, interests=[],
            online=None, last_seen_text=None,
        )
    return PublicUserOut(
        id=user.id,
        username=user.username,
        display_name=user.username,
        avatar=user.avatar,
        is_bot=False,
        is_deleted=False,
        bio=(user.bio or "") if user.show_bio else None,
        interests=parse_interests(user.interests) if user.show_interests else [],
        online=is_online(user) if user.show_online else None,
        last_seen_text=humanize_last_seen(user) if user.show_online else None,
    )


def _media_fields_for_message(msg: Message) -> dict:
    try:
        from features import signed_url_for_key
    except Exception:
        signed_url_for_key = None

    kind = (getattr(msg, "type", None) or "text")
    if kind == "text" or not getattr(msg, "media_key", None):
        return {
            "type": "text",
            "media_url": None,
            "thumb_url": None,
            "media_name": getattr(msg, "media_name", None),
            "media_size": getattr(msg, "media_size", None),
            "media_width": getattr(msg, "media_width", None),
            "media_height": getattr(msg, "media_height", None),
            "media_duration": getattr(msg, "media_duration", None),
        }

    media_url = None
    thumb_url = None
    if signed_url_for_key is not None:
        try:
            media_url = signed_url_for_key(msg.media_key)
            thumb_url = signed_url_for_key(msg.media_thumb_key)
        except Exception:
            pass

    return {
        "type": kind,
        "media_url": media_url,
        "thumb_url": thumb_url,
        "media_name": msg.media_name,
        "media_size": msg.media_size,
        "media_width": msg.media_width,
        "media_height": msg.media_height,
        "media_duration": msg.media_duration,
    }


def _reaction_fields_for_message(msg: Message, db: Session, me_id: int) -> dict:
    try:
        from features import reactions_for_message
        return {"reactions": reactions_for_message(db, msg.id, me_id)}
    except Exception:
        return {"reactions": []}


# ---------------------------------------------------------------------
# App
# ---------------------------------------------------------------------

app = FastAPI(title="Lantern Backend")


@app.on_event("startup")
def on_startup():
    ensure_bot_exists()
    db = SessionLocal()
    try:
        get_global_room(db)
    finally:
        db.close()


@app.get("/")
def root():
    return {"app": "Lantern", "status": "running"}


# ---------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------

@app.post("/auth/register", response_model=Token)
def register(data: UserCreate, db: Session = Depends(get_db)):
    if not data.email and not data.phone:
        raise HTTPException(status_code=400, detail="Provide either email or phone")

    if data.username.lower() == BOT_USERNAME:
        raise HTTPException(status_code=400, detail="That username is reserved")

    if db.query(User).filter(User.username == data.username).first():
        raise HTTPException(status_code=400, detail="Username already taken")
    if data.email and db.query(User).filter(User.email == data.email).first():
        raise HTTPException(status_code=400, detail="Email already registered")
    if data.phone and db.query(User).filter(User.phone == data.phone).first():
        raise HTTPException(status_code=400, detail="Phone already registered")

    user = User(
        username=data.username,
        email=data.email,
        phone=data.phone,
        hashed_password=hash_password(data.password),
        bio=data.bio,
        interests=serialize_interests(data.interests),
        last_seen=datetime.utcnow(),
        is_bot=False,
        show_bio=True,
        show_interests=True,
        show_online=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    try:
        send_bot_message(db, user.id, bot_welcome_text(user.username))
    except Exception:
        pass

    try:
        from features import post_welcome_in_room
        post_welcome_in_room(user.id, user.username)
    except Exception as e:
        print(f"[register] post_welcome_in_room error: {e!r}")

    return Token(access_token=create_access_token(user.id, user.username))


@app.post("/auth/login", response_model=Token)
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(
        or_(User.username == form.username, User.email == form.username)
    ).first()
    if not user or not verify_password(form.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if user.is_bot:
        raise HTTPException(status_code=403, detail="Not a user account")
    if user.deleted_at is not None:
        raise HTTPException(status_code=403, detail="This account has been deleted")
    user.last_seen = datetime.utcnow()
    db.commit()
    return Token(access_token=create_access_token(user.id, user.username))


@app.get("/auth/me", response_model=UserOut)
def me(user: User = Depends(get_current_user)):
    return UserOut(
        id=user.id, username=user.username, email=user.email, phone=user.phone,
        bio=user.bio, interests=user.interests, avatar=user.avatar,
        is_bot=user.is_bot, is_deleted=False,
        show_bio=user.show_bio, show_interests=user.show_interests, show_online=user.show_online,
        online=True,
    )


@app.delete("/users/me")
def delete_account(data: DeleteAccountIn,
                   user: User = Depends(get_current_user),
                   db: Session = Depends(get_db)):
    if user.is_bot:
        raise HTTPException(status_code=403, detail="Cannot delete bot account")
    if data.confirm_username != user.username:
        raise HTTPException(status_code=400, detail="Confirmation username does not match")

    old_username = user.username
    user.username = f"deleted_{user.id}_{old_username}"
    user.email = None
    user.phone = None
    user.bio = None
    user.interests = None
    user.avatar = None
    user.hashed_password = ""
    user.deleted_at = datetime.utcnow()
    db.commit()
    return {"deleted": True}


@app.put("/users/me", response_model=UserOut)
def update_profile(data: ProfileUpdate,
                   user: User = Depends(get_current_user),
                   db: Session = Depends(get_db)):
    if user.is_bot:
        raise HTTPException(status_code=403, detail="Cannot modify bot account")

    if data.username is not None and data.username != user.username:
        new_name = data.username.strip()
        if not new_name:
            raise HTTPException(status_code=400, detail="Username can't be empty")
        if new_name.lower() == BOT_USERNAME:
            raise HTTPException(status_code=400, detail="That username is reserved")
        if db.query(User).filter(User.username == new_name).first():
            raise HTTPException(status_code=400, detail="Username already taken")
        user.username = new_name

    if data.bio is not None:
        user.bio = data.bio

    if data.phone is not None:
        p = data.phone.strip()
        if p == "":
            user.phone = None
        else:
            existing = db.query(User).filter(User.phone == p, User.id != user.id).first()
            if existing:
                raise HTTPException(status_code=400, detail="Phone already registered")
            user.phone = p

    if data.interests is not None:
        user.interests = serialize_interests(data.interests)

    if data.avatar is not None:
        user.avatar = validate_avatar(data.avatar)

    if data.show_bio is not None:
        user.show_bio = bool(data.show_bio)
    if data.show_interests is not None:
        user.show_interests = bool(data.show_interests)
    if data.show_online is not None:
        user.show_online = bool(data.show_online)

    db.commit()
    db.refresh(user)
    return UserOut(
        id=user.id, username=user.username, email=user.email, phone=user.phone,
        bio=user.bio, interests=user.interests, avatar=user.avatar,
        is_bot=user.is_bot, is_deleted=False,
        show_bio=user.show_bio, show_interests=user.show_interests, show_online=user.show_online,
        online=True,
    )


# ---------------------------------------------------------------------
# Discover / Users
# ---------------------------------------------------------------------

@app.get("/users/discover", response_model=List[DiscoverUser])
def discover(limit: int = 30,
             user: User = Depends(get_current_user),
             db: Session = Depends(get_db)):
    my_interests = set(parse_interests(user.interests))
    pool = (db.query(User)
            .filter(User.id != user.id)
            .filter(User.is_bot == False)
            .filter(User.deleted_at == None)
            .order_by(User.last_seen.desc().nullslast())
            .limit(200)
            .all())

    def score(u: User):
        theirs = set(parse_interests(u.interests)) if u.show_interests else set()
        s = len(my_interests & theirs) * 10
        if is_online(u):
            s += 5
        return s

    pool.sort(key=score, reverse=True)

    out = []
    for u in pool[:limit]:
        theirs = parse_interests(u.interests) if u.show_interests else []
        out.append(DiscoverUser(
            id=u.id, username=u.username,
            bio=(u.bio if u.show_bio else None),
            interests=theirs,
            avatar=u.avatar,
            is_bot=u.is_bot,
            online=(is_online(u) if u.show_online else None),
            last_seen_text=(humanize_last_seen(u) if u.show_online else None),
            shared_interests=list(my_interests & set(theirs)),
        ))
    return out


@app.get("/users/random", response_model=Optional[DiscoverUser])
def random_user(user: User = Depends(get_current_user),
                db: Session = Depends(get_db)):
    u = (db.query(User)
         .filter(User.id != user.id)
         .filter(User.is_bot == False)
         .filter(User.deleted_at == None)
         .order_by(func.random())
         .first())
    if not u:
        return None
    my_interests = set(parse_interests(user.interests))
    theirs = parse_interests(u.interests) if u.show_interests else []
    return DiscoverUser(
        id=u.id, username=u.username,
        bio=(u.bio if u.show_bio else None),
        interests=theirs,
        avatar=u.avatar,
        is_bot=u.is_bot,
        online=(is_online(u) if u.show_online else None),
        last_seen_text=(humanize_last_seen(u) if u.show_online else None),
        shared_interests=list(my_interests & set(theirs)),
    )


@app.get("/users/search")
def search_users(q: str,
                 user: User = Depends(get_current_user),
                 db: Session = Depends(get_db)):
    results = (db.query(User)
               .filter(User.username.ilike(f"%{q}%"))
               .filter(User.id != user.id)
               .filter(User.is_bot == False)
               .filter(User.deleted_at == None)
               .limit(20).all())
    out = []
    for u in results:
        out.append({
            "id": u.id, "username": u.username,
            "bio": (u.bio if u.show_bio else None),
            "interests": (parse_interests(u.interests) if u.show_interests else []),
            "avatar": u.avatar,
            "is_bot": u.is_bot,
            "online": (is_online(u) if u.show_online else None),
            "last_seen_text": (humanize_last_seen(u) if u.show_online else None),
        })
    return out


@app.get("/users/bot")
def get_bot_info(db: Session = Depends(get_db)):
    bot = get_bot(db)
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    return {
        "id": bot.id,
        "username": bot.username,
        "bio": bot.bio,
        "avatar": bot.avatar,
        "is_bot": True,
    }


@app.get("/users/{user_id}", response_model=PublicUserOut)
def get_public_user(user_id: int,
                    user: User = Depends(get_current_user),
                    db: Session = Depends(get_db)):
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    if target.id == user.id:
        return PublicUserOut(
            id=target.id, username=target.username, display_name=target.username,
            avatar=target.avatar, is_bot=target.is_bot, is_deleted=False,
            bio=target.bio or "",
            interests=parse_interests(target.interests),
            online=True, last_seen_text="just now",
        )
    return public_view_of(target)


# ---------------------------------------------------------------------
# Global Room
# ---------------------------------------------------------------------

@app.get("/rooms/global", response_model=RoomOut)
def get_global_room_info(user: User = Depends(get_current_user),
                         db: Session = Depends(get_db)):
    room = get_global_room(db)
    member_count = (db.query(User)
                    .filter(User.is_bot == False)
                    .filter(User.deleted_at == None)
                    .count())

    cutoff = user.room_last_read_at
    unread_q = db.query(Message).filter(Message.room_id == room.id)
    if cutoff is not None:
        unread_q = unread_q.filter(Message.created_at > cutoff)
    unread_q = unread_q.filter(Message.sender_id != user.id)
    unread_count = unread_q.count()

    last = (db.query(Message)
            .filter(Message.room_id == room.id)
            .order_by(Message.created_at.desc())
            .first())
    preview = None
    if last is not None:
        sender = db.query(User).filter(User.id == last.sender_id).first()
        if sender is not None:
            sender_label = "You" if sender.id == user.id else display_name_of(sender)
            text = last.content or ""
            if not text:
                kind = (last.type or "text").lower()
                if kind == "image":
                    text = "📷 Photo"
                elif kind == "video":
                    text = "🎬 Video"
                elif kind == "voice":
                    text = "🎤 Voice message"
                elif kind == "file":
                    if last.media_name:
                        text = f"📎 {last.media_name}"
                    else:
                        text = "📎 File"
            if len(text) > 80:
                text = text[:77] + "..."
            preview = f"{sender_label}: {text}"

    return RoomOut(
        id=room.id,
        name=room.name,
        member_count=member_count,
        unread_count=unread_count,
        last_message_preview=preview,
    )


@app.get("/rooms/global/messages")
def get_global_room_messages(limit: int = 40,
                             user: User = Depends(get_current_user),
                             db: Session = Depends(get_db)):
    room = get_global_room(db)
    msgs = (db.query(Message)
            .filter(Message.room_id == room.id)
            .order_by(Message.created_at.desc())
            .limit(limit).all())
    msgs.reverse()

    sender_ids = {m.sender_id for m in msgs if m.sender_id is not None}
    senders_by_id = {}
    if sender_ids:
        senders = db.query(User).filter(User.id.in_(sender_ids)).all()
        senders_by_id = {u.id: u for u in senders}

    parent_ids = {m.reply_to_id for m in msgs if m.reply_to_id is not None}
    parents_by_id = {}
    parent_senders_by_id = {}
    if parent_ids:
        parents = db.query(Message).filter(Message.id.in_(parent_ids)).all()
        parents_by_id = {p.id: p for p in parents}
        parent_sender_ids = {p.sender_id for p in parents if p.sender_id is not None}
        if parent_sender_ids:
            psenders = db.query(User).filter(User.id.in_(parent_sender_ids)).all()
            parent_senders_by_id = {u.id: u for u in psenders}

    def build_reply_preview(m: Message):
        if not m.reply_to_id:
            return None
        parent = parents_by_id.get(m.reply_to_id)
        if not parent:
            return None
        psender = parent_senders_by_id.get(parent.sender_id)
        t = parent.content or ""
        if len(t) > 100:
            t = t[:97] + "..."
        return {
            "id": parent.id,
            "sender_id": parent.sender_id,
            "sender_username": psender.username if psender else "unknown",
            "content": t,
        }

    out = []
    for m in msgs:
        sender = senders_by_id.get(m.sender_id)
        d = {
            "id": m.id,
            "sender_id": m.sender_id,
            "sender_username": sender.username if sender else "unknown",
            "sender_display_name": display_name_of(sender) if sender else "Unknown",
            "sender_avatar": (sender.avatar if sender and sender.deleted_at is None else None),
            "content": m.content,
            "created_at": m.created_at.isoformat(),
            "reply_to": build_reply_preview(m),
            "edited_at": m.edited_at.isoformat() if m.edited_at else None,
        }
        d.update(_media_fields_for_message(m))
        d.update(_reaction_fields_for_message(m, db, user.id))
        out.append(d)
    return out


@app.post("/rooms/global/messages", response_model=RoomMessageOut)
def post_global_room_message(data: SendRoomMessageIn,
                             user: User = Depends(get_current_user),
                             db: Session = Depends(get_db)):
    msg_type = (data.type or "text").strip().lower()
    if msg_type not in ("text", "image", "file", "voice", "video"):
        raise HTTPException(status_code=400, detail="Invalid message type")

    content = (data.content or "").strip()

    if msg_type == "text":
        if not content:
            raise HTTPException(status_code=400, detail="Empty message")
    else:
        if not data.media_key:
            raise HTTPException(status_code=400, detail="Media key required")
        if not content:
            content = ""

    recent_cutoff = datetime.utcnow() - timedelta(seconds=ROOM_RATE_LIMIT_SECONDS)
    recent = (db.query(Message)
              .filter(Message.sender_id == user.id)
              .filter(Message.room_id != None)
              .filter(Message.created_at >= recent_cutoff)
              .first())
    if recent is not None:
        raise HTTPException(status_code=429, detail="Slow down — one message every few seconds")

    room = get_global_room(db)
    stored_ts = resolve_client_timestamp(data.created_at)

    msg = Message(
        sender_id=user.id,
        room_id=room.id,
        content=content,
        reply_to_id=data.reply_to_id,
        type=msg_type,
        media_key=data.media_key,
        media_thumb_key=data.media_thumb_key,
        media_name=data.media_name,
        media_size=data.media_size,
        media_width=data.media_width,
        media_height=data.media_height,
        media_duration=data.media_duration,
    )
    if stored_ts is not None:
        msg.created_at = stored_ts

    db.add(msg)
    db.commit()
    db.refresh(msg)

    media_fields = _media_fields_for_message(msg)

    payload_out = {
        "type": "room_message",
        "event": "room_message",
        "media_kind": media_fields["type"],
        "room_id": room.id,
        "id": msg.id,
        "sender_id": user.id,
        "sender_username": user.username,
        "sender_display_name": display_name_of(user),
        "sender_avatar": user.avatar,
        "content": msg.content,
        "created_at": msg.created_at.isoformat(),
        "reply_to": reply_preview_of(db, msg),
        "edited_at": None,
        "reactions": [],
    }
    payload_out.update(media_fields)

    try:
        import asyncio
        asyncio.create_task(manager.broadcast_except(user.id, payload_out))
    except Exception:
        pass

    resp = {
        "id": msg.id,
        "sender_id": msg.sender_id,
        "sender_username": user.username,
        "sender_display_name": display_name_of(user),
        "sender_avatar": user.avatar,
        "content": msg.content,
        "created_at": msg.created_at.isoformat(),
        "reply_to": reply_preview_of(db, msg),
        "edited_at": None,
        "reactions": [],
    }
    resp.update(media_fields)
    return resp


@app.post("/rooms/global/read")
def mark_room_read(user: User = Depends(get_current_user),
                   db: Session = Depends(get_db)):
    user.room_last_read_at = datetime.utcnow()
    db.commit()
    return {"ok": True}


@app.get("/rooms/global/members", response_model=List[RoomMemberOut])
def get_room_members(user: User = Depends(get_current_user),
                     db: Session = Depends(get_db)):
    users = (db.query(User)
             .filter(User.is_bot == False)
             .filter(User.deleted_at == None)
             .order_by(User.username.asc())
             .limit(500)
             .all())
    out = []
    for u in users:
        out.append(RoomMemberOut(
            id=u.id,
            username=u.username,
            avatar=u.avatar,
            online=(is_online(u) if u.show_online else None),
        ))
    return out


# ---------------------------------------------------------------------
# Conversations
# ---------------------------------------------------------------------

@app.get("/conversations", response_model=List[ConversationOut])
def conversations(user: User = Depends(get_current_user),
                  db: Session = Depends(get_db)):
    sql = text("""
        SELECT
            CASE WHEN sender_id = :me THEN receiver_id ELSE sender_id END AS other_id,
            content,
            created_at,
            type,
            media_name
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY CASE WHEN sender_id = :me THEN receiver_id ELSE sender_id END
                       ORDER BY created_at DESC
                   ) AS rn
            FROM messages
            WHERE (sender_id = :me OR receiver_id = :me)
              AND room_id IS NULL
        ) t
        WHERE rn = 1
        ORDER BY created_at DESC
    """)
    rows = db.execute(sql, {"me": user.id}).fetchall()

    unread_sql = text("""
        SELECT sender_id, COUNT(*) AS c
        FROM messages
        WHERE receiver_id = :me AND read_at IS NULL AND room_id IS NULL
        GROUP BY sender_id
    """)
    unread_rows = db.execute(unread_sql, {"me": user.id}).fetchall()
    unread_by_user = {row[0]: row[1] for row in unread_rows}

    other_ids = {row[0] for row in rows if row[0] is not None}
    users_by_id = {}
    if other_ids:
        us = db.query(User).filter(User.id.in_(other_ids)).all()
        users_by_id = {u.id: u for u in us}

    out = []
    for other_id, content, created_at, mtype, media_name in rows:
        u = users_by_id.get(other_id)
        if not u:
            continue
        unread = unread_by_user.get(other_id, 0)

        text_preview = content or ""
        if not text_preview:
            kind = (mtype or "text").lower()
            if kind == "image":
                text_preview = "📷 Photo"
            elif kind == "video":
                text_preview = "🎬 Video"
            elif kind == "voice":
                text_preview = "🎤 Voice message"
            elif kind == "file":
                if media_name:
                    text_preview = f"📎 {media_name}"
                else:
                    text_preview = "📎 File"
            else:
                text_preview = ""

        out.append(ConversationOut(
            user_id=u.id,
            username=u.username,
            display_name=display_name_of(u),
            avatar=(u.avatar if u.deleted_at is None else None),
            is_bot=u.is_bot,
            is_deleted=(u.deleted_at is not None),
            last_message=text_preview,
            last_timestamp=created_at.isoformat(),
            online=(is_online(u) if (u.is_bot or u.show_online) else None),
            last_seen_text=(humanize_last_seen(u) if (u.is_bot or u.show_online) else None),
            unread_count=unread,
        ))
    return out


@app.post("/conversations/{other_user_id}/read")
def mark_conversation_read(other_user_id: int,
                           user: User = Depends(get_current_user),
                           db: Session = Depends(get_db)):
    now = datetime.utcnow()
    updated = (db.query(Message)
               .filter(Message.sender_id == other_user_id)
               .filter(Message.receiver_id == user.id)
               .filter(Message.room_id == None)
               .filter(Message.read_at == None)
               .update({"read_at": now}, synchronize_session=False))
    db.commit()

    try:
        import asyncio
        asyncio.create_task(manager.send_to(other_user_id, {
            "type": "read",
            "event": "read",
            "by": user.id,
        }))
    except Exception:
        pass

    return {"marked_read": updated}


@app.delete("/conversations/{other_user_id}")
def delete_conversation(other_user_id: int,
                        user: User = Depends(get_current_user),
                        db: Session = Depends(get_db)):
    other = db.query(User).filter(User.id == other_user_id).first()
    if other and other.is_bot:
        raise HTTPException(status_code=403, detail="You can't delete the bot chat")

    deleted = (db.query(Message)
               .filter(Message.room_id == None)
               .filter(or_(
                   and_(Message.sender_id == user.id, Message.receiver_id == other_user_id),
                   and_(Message.sender_id == other_user_id, Message.receiver_id == user.id),
               ))
               .delete(synchronize_session=False))
    db.commit()
    return {"deleted": deleted}


# ---------------------------------------------------------------------
# Messages (DMs)
# ---------------------------------------------------------------------

@app.get("/messages/{other_user_id}")
def get_messages(other_user_id: int,
                 limit: int = 50,
                 user: User = Depends(get_current_user),
                 db: Session = Depends(get_db)):
    msgs = (db.query(Message)
            .filter(Message.room_id == None)
            .filter(or_(
                and_(Message.sender_id == user.id, Message.receiver_id == other_user_id),
                and_(Message.sender_id == other_user_id, Message.receiver_id == user.id),
            ))
            .order_by(Message.created_at.desc())
            .limit(limit).all())
    msgs.reverse()

    parent_ids = {m.reply_to_id for m in msgs if m.reply_to_id is not None}
    parents_by_id = {}
    parent_senders_by_id = {}
    if parent_ids:
        parents = db.query(Message).filter(Message.id.in_(parent_ids)).all()
        parents_by_id = {p.id: p for p in parents}
        parent_sender_ids = {p.sender_id for p in parents if p.sender_id is not None}
        if parent_sender_ids:
            psenders = db.query(User).filter(User.id.in_(parent_sender_ids)).all()
            parent_senders_by_id = {u.id: u for u in psenders}

    def build_reply_preview(m: Message):
        if not m.reply_to_id:
            return None
        parent = parents_by_id.get(m.reply_to_id)
        if not parent:
            return None
        psender = parent_senders_by_id.get(parent.sender_id)
        t = parent.content or ""
        if len(t) > 100:
            t = t[:97] + "..."
        return {
            "id": parent.id,
            "sender_id": parent.sender_id,
            "sender_username": psender.username if psender else "unknown",
            "content": t,
        }

    out = []
    for m in msgs:
        d = {
            "id": m.id,
            "sender_id": m.sender_id,
            "receiver_id": m.receiver_id,
            "content": m.content,
            "created_at": m.created_at.isoformat(),
            "read_at": m.read_at.isoformat() if m.read_at else None,
            "reply_to": build_reply_preview(m),
            "edited_at": m.edited_at.isoformat() if m.edited_at else None,
        }
        d.update(_media_fields_for_message(m))
        d.update(_reaction_fields_for_message(m, db, user.id))
        out.append(d)
    return out


@app.post("/messages", response_model=MessageOut)
def post_message(data: SendMessageIn,
                 user: User = Depends(get_current_user),
                 db: Session = Depends(get_db)):
    msg_type = (data.type or "text").strip().lower()
    if msg_type not in ("text", "image", "file", "voice", "video"):
        raise HTTPException(status_code=400, detail="Invalid message type")

    content = (data.content or "").strip()

    if msg_type == "text":
        if not content:
            raise HTTPException(status_code=400, detail="Empty message")
    else:
        if not data.media_key:
            raise HTTPException(status_code=400, detail="Media key required")
        if not content:
            content = ""

    receiver = db.query(User).filter(User.id == data.to).first()
    if not receiver:
        raise HTTPException(status_code=404, detail="Recipient not found")
    if receiver.deleted_at is not None:
        raise HTTPException(status_code=403, detail="This user has deleted their account")

    stored_ts = resolve_client_timestamp(data.created_at)

    msg = Message(
        sender_id=user.id,
        receiver_id=data.to,
        content=content,
        reply_to_id=data.reply_to_id,
        type=msg_type,
        media_key=data.media_key,
        media_thumb_key=data.media_thumb_key,
        media_name=data.media_name,
        media_size=data.media_size,
        media_width=data.media_width,
        media_height=data.media_height,
        media_duration=data.media_duration,
    )
    if stored_ts is not None:
        msg.created_at = stored_ts

    db.add(msg)
    db.commit()
    db.refresh(msg)

    media_fields = _media_fields_for_message(msg)

    payload_out = {
        "id": msg.id,
        "from": user.id,
        "from_username": user.username,
        "to": data.to,
        "content": msg.content,
        "created_at": msg.created_at.isoformat(),
        "read_at": None,
        "reply_to": reply_preview_of(db, msg),
        "edited_at": None,
        "media_kind": media_fields["type"],
        "reactions": [],
    }
    payload_out.update(media_fields)

    try:
        import asyncio
        asyncio.create_task(manager.send_to(data.to, payload_out))
    except Exception:
        pass

    if receiver.is_bot:
        try:
            reply_text = bot_reply(db, user, content)
            reply_msg = send_bot_message(db, user.id, reply_text)
            if reply_msg:
                reply_payload = {
                    "id": reply_msg.id,
                    "from": reply_msg.sender_id,
                    "from_username": BOT_USERNAME,
                    "to": user.id,
                    "content": reply_msg.content,
                    "created_at": reply_msg.created_at.isoformat(),
                    "read_at": None,
                    "reply_to": None,
                    "edited_at": None,
                    "media_kind": "text",
                    "type": "text",
                    "media_url": None,
                    "thumb_url": None,
                    "media_name": None,
                    "media_size": None,
                    "media_width": None,
                    "media_height": None,
                    "media_duration": None,
                    "reactions": [],
                }
                try:
                    import asyncio
                    asyncio.create_task(manager.send_to(user.id, reply_payload))
                except Exception:
                    pass
        except Exception:
            pass

    resp = {
        "id": msg.id,
        "sender_id": msg.sender_id,
        "receiver_id": msg.receiver_id,
        "content": msg.content,
        "created_at": msg.created_at.isoformat(),
        "read_at": None,
        "reply_to": reply_preview_of(db, msg),
        "edited_at": None,
        "reactions": [],
    }
    resp.update(media_fields)
    return resp


@app.put("/messages/{message_id}")
def edit_message(message_id: int,
                 data: EditMessageIn,
                 user: User = Depends(get_current_user),
                 db: Session = Depends(get_db)):
    m = db.query(Message).filter(Message.id == message_id).first()
    if not m:
        raise HTTPException(status_code=404, detail="Message not found")
    if m.sender_id != user.id:
        raise HTTPException(status_code=403, detail="You can only edit your own messages")

    created = m.created_at.replace(tzinfo=None) if m.created_at else datetime.utcnow()
    if (datetime.utcnow() - created).total_seconds() > EDIT_WINDOW_SECONDS:
        raise HTTPException(status_code=403, detail="You can only edit within 20 minutes")

    new_content = (data.content or "").strip()
    if not new_content:
        raise HTTPException(status_code=400, detail="Empty message")
    if new_content != m.content:
        m.content = new_content
        m.edited_at = datetime.utcnow()
        db.commit()
        db.refresh(m)

    payload_out = {
        "type": "message_edited",
        "event": "message_edited",
        "id": m.id,
        "content": m.content,
        "edited_at": m.edited_at.isoformat() if m.edited_at else None,
        "room_id": m.room_id,
        "from": m.sender_id,
        "to": m.receiver_id,
    }

    try:
        import asyncio
        if m.room_id is not None:
            asyncio.create_task(manager.broadcast(payload_out))
        else:
            asyncio.create_task(manager.send_to(m.receiver_id, payload_out))
            asyncio.create_task(manager.send_to(m.sender_id, payload_out))
    except Exception:
        pass

    return {
        "id": m.id,
        "content": m.content,
        "edited_at": m.edited_at.isoformat() if m.edited_at else None,
    }


@app.delete("/messages/{message_id}")
def delete_message(message_id: int,
                   user: User = Depends(get_current_user),
                   db: Session = Depends(get_db)):
    m = db.query(Message).filter(Message.id == message_id).first()
    if not m:
        raise HTTPException(status_code=404, detail="Message not found")
    if m.sender_id != user.id and m.receiver_id != user.id:
        raise HTTPException(status_code=403, detail="Not your message")
    db.delete(m)
    db.commit()
    return {"deleted": True}


# ---------------------------------------------------------------------
# Release / update endpoints
# ---------------------------------------------------------------------

@app.get("/app/version", response_model=Optional[ReleaseOut])
def app_version(db: Session = Depends(get_db)):
    latest = (db.query(Release)
              .order_by(Release.version_code.desc())
              .first())
    if not latest:
        raise HTTPException(status_code=404, detail="No releases published yet")
    return ReleaseOut(
        version_code=latest.version_code,
        version_name=latest.version_name,
        download_url=latest.download_url,
        notes=latest.notes or "",
        blocking=latest.blocking,
        published_at=latest.published_at.isoformat(),
    )


@app.get("/admin/releases", response_model=List[ReleaseOut])
def admin_list_releases(x_admin_key: Optional[str] = Header(None),
                        db: Session = Depends(get_db)):
    require_admin(x_admin_key)
    rows = db.query(Release).order_by(Release.version_code.desc()).all()
    return [
        ReleaseOut(
            version_code=r.version_code,
            version_name=r.version_name,
            download_url=r.download_url,
            notes=r.notes or "",
            blocking=r.blocking,
            published_at=r.published_at.isoformat(),
        ) for r in rows
    ]


@app.post("/admin/releases", response_model=ReleaseOut)
def admin_publish_release(data: ReleaseCreate,
                          x_admin_key: Optional[str] = Header(None),
                          db: Session = Depends(get_db)):
    require_admin(x_admin_key)

    existing = db.query(Release).filter(Release.version_code == data.version_code).first()
    if existing:
        existing.version_name = data.version_name
        existing.download_url = data.download_url
        existing.notes = data.notes or ""
        existing.blocking = bool(data.blocking)
    else:
        r = Release(
            version_code=data.version_code,
            version_name=data.version_name,
            download_url=data.download_url,
            notes=data.notes or "",
            blocking=bool(data.blocking),
        )
        db.add(r)
    db.commit()

    latest = db.query(Release).filter(Release.version_code == data.version_code).first()
    return ReleaseOut(
        version_code=latest.version_code,
        version_name=latest.version_name,
        download_url=latest.download_url,
        notes=latest.notes or "",
        blocking=latest.blocking,
        published_at=latest.published_at.isoformat(),
    )


@app.delete("/admin/releases/{version_code}")
def admin_delete_release(version_code: int,
                         x_admin_key: Optional[str] = Header(None),
                         db: Session = Depends(get_db)):
    require_admin(x_admin_key)
    deleted = db.query(Release).filter(Release.version_code == version_code).delete()
    db.commit()
    return {"deleted": deleted}


# ---------------------------------------------------------------------
# Admin — Room
# ---------------------------------------------------------------------

@app.get("/admin/room")
def admin_get_room(x_admin_key: Optional[str] = Header(None),
                   db: Session = Depends(get_db)):
    require_admin(x_admin_key)
    room = get_global_room(db)
    return {"id": room.id, "name": room.name}


@app.put("/admin/room")
def admin_rename_room(data: RenameRoomIn,
                      x_admin_key: Optional[str] = Header(None),
                      db: Session = Depends(get_db)):
    require_admin(x_admin_key)
    new_name = (data.name or "").strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Name can't be empty")
    if len(new_name) > 60:
        raise HTTPException(status_code=400, detail="Name too long (max 60)")

    room = get_global_room(db)
    room.name = new_name
    db.commit()

    try:
        import asyncio
        asyncio.create_task(manager.broadcast({
            "type": "room_renamed",
            "event": "room_renamed",
            "room_id": room.id,
            "name": room.name,
        }))
    except Exception:
        pass

    return {"id": room.id, "name": room.name}


# ---------------------------------------------------------------------
# Broadcast
# ---------------------------------------------------------------------

@app.post("/admin/broadcast")
def admin_broadcast(data: BroadcastIn,
                    x_admin_key: Optional[str] = Header(None),
                    db: Session = Depends(get_db)):
    require_admin(x_admin_key)

    text_content = data.content.strip()
    if not text_content:
        raise HTTPException(status_code=400, detail="Empty broadcast")

    bot = get_bot(db)
    if not bot:
        raise HTTPException(status_code=500, detail="Bot account missing")

    users = db.query(User).filter(User.is_bot == False).filter(User.deleted_at == None).all()
    sent = 0
    for u in users:
        try:
            msg = send_bot_message(db, u.id, text_content)
            if msg:
                try:
                    import asyncio
                    payload = {
                        "id": msg.id,
                        "from": msg.sender_id,
                        "from_username": BOT_USERNAME,
                        "to": u.id,
                        "content": msg.content,
                        "created_at": msg.created_at.isoformat(),
                        "read_at": None,
                        "reply_to": None,
                        "edited_at": None,
                        "media_kind": "text",
                        "type": "text",
                        "media_url": None,
                        "thumb_url": None,
                        "media_name": None,
                        "media_size": None,
                        "media_width": None,
                        "media_height": None,
                        "media_duration": None,
                        "reactions": [],
                    }
                    asyncio.create_task(manager.send_to(u.id, payload))
                except Exception:
                    pass
                sent += 1
        except Exception:
            continue

    return {"sent_to": sent, "total_users": len(users)}


# ---------------------------------------------------------------------
# Admin panel
# ---------------------------------------------------------------------

ADMIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lantern — Admin</title>
<style>
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 24px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #0a0a14; color: #f2f2f7;
  }
  .wrap { max-width: 640px; margin: 0 auto; }
  h1 { font-size: 22px; font-weight: 400; margin: 0 0 4px; }
  .sub { color: #8e8ea8; font-size: 13px; margin-bottom: 24px; }
  .card {
    background: #1a1a2e; border-radius: 14px;
    padding: 18px; margin-bottom: 16px;
  }
  .tabs { display: flex; gap: 8px; margin-bottom: 16px; }
  .tab {
    padding: 10px 16px; border-radius: 10px; background: #22223a;
    color: #8e8ea8; font-size: 13px; cursor: pointer; border: 0;
    font-family: inherit;
  }
  .tab.active { background: #ff5c39; color: white; }
  .pane { display: none; }
  .pane.active { display: block; }
  label { display: block; font-size: 12px; color: #8e8ea8; margin-bottom: 6px; }
  input, textarea {
    width: 100%; padding: 12px; border: 0; border-radius: 10px;
    background: #22223a; color: #f2f2f7; font-size: 14px;
    font-family: inherit; outline: none;
  }
  textarea { min-height: 100px; resize: vertical; }
  input:focus, textarea:focus { background: #2a2a44; }
  .row { display: flex; gap: 10px; }
  .row > div { flex: 1; }
  .field { margin-bottom: 12px; }
  .checkbox-row { display: flex; align-items: center; gap: 8px; margin: 12px 0; }
  .checkbox-row input { width: auto; }
  .checkbox-row label { margin: 0; color: #f2f2f7; font-size: 14px; }
  button {
    padding: 12px 20px; border: 0; border-radius: 12px;
    font-size: 14px; font-weight: 500; cursor: pointer;
    font-family: inherit;
  }
  .primary { background: #ff5c39; color: #fff; width: 100%; }
  .primary:disabled { opacity: 0.5; cursor: not-allowed; }
  .release {
    display: flex; align-items: flex-start; gap: 12px;
    padding: 12px; border-radius: 10px; background: #22223a;
    margin-bottom: 8px;
  }
  .release-info { flex: 1; font-size: 13px; }
  .release-title { font-weight: 500; margin-bottom: 4px; }
  .release-meta { color: #8e8ea8; font-size: 12px; }
  .badge {
    display: inline-block; padding: 2px 8px; border-radius: 8px;
    font-size: 11px; margin-left: 6px;
  }
  .badge-block { background: #8a2e1a; color: #fff; }
  .badge-ok { background: #2a2a44; color: #8e8ea8; }
  .delete-btn {
    background: transparent; color: #8e8ea8; font-size: 12px;
    padding: 4px 8px;
  }
  .delete-btn:hover { color: #ff5c39; }
  .toast {
    position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%);
    background: #22223a; color: #fff; padding: 12px 20px;
    border-radius: 10px; font-size: 13px; opacity: 0;
    transition: opacity 0.3s; pointer-events: none;
  }
  .toast.show { opacity: 1; }
  .error { color: #ff5c39; font-size: 13px; margin-top: 8px; }
  .success { color: #3FD86A; font-size: 13px; margin-top: 8px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>Lantern — Admin</h1>
  <div class="sub">Publish updates, send broadcasts, and manage the Global Room</div>

  <div class="card">
    <div class="field">
      <label>Admin key</label>
      <input id="key" type="password" placeholder="Paste ADMIN_KEY" oninput="onKeyInput()">
    </div>
  </div>

  <div class="tabs">
    <button class="tab active" onclick="switchTab('releases', event)">Releases</button>
    <button class="tab" onclick="switchTab('broadcast', event)">Broadcast</button>
    <button class="tab" onclick="switchTab('room', event)">Global Room</button>
  </div>

  <div class="pane active" id="pane-releases">
    <div class="card">
      <h3 style="margin-top:0;font-size:15px;font-weight:500">Publish a release</h3>
      <div class="row">
        <div class="field">
          <label>Version code (integer)</label>
          <input id="vcode" type="number" placeholder="5">
        </div>
        <div class="field">
          <label>Version name</label>
          <input id="vname" type="text" placeholder="1.4">
        </div>
      </div>
      <div class="field">
        <label>Download URL</label>
        <input id="url" type="text" placeholder="https://github.com/.../lantern.apk">
      </div>
      <div class="field">
        <label>Release notes</label>
        <textarea id="notes" placeholder="What changed?"></textarea>
      </div>
      <div class="checkbox-row">
        <input id="blocking" type="checkbox">
        <label for="blocking">Block users below this version (use carefully)</label>
      </div>
      <button class="primary" id="publishBtn" onclick="publish()">Publish</button>
      <div class="error" id="error"></div>
    </div>

    <div class="card">
      <h3 style="margin-top:0;font-size:15px;font-weight:500">Published releases</h3>
      <div id="list" style="margin-top:12px"></div>
    </div>
  </div>

  <div class="pane" id="pane-broadcast">
    <div class="card">
      <h3 style="margin-top:0;font-size:15px;font-weight:500">Send a broadcast</h3>
      <div class="sub" style="margin-bottom:16px">
        This sends a message from the Lantern Good Boy bot to every user.
      </div>
      <div class="field">
        <label>Message</label>
        <textarea id="bcontent" placeholder="What do you want to announce?"></textarea>
      </div>
      <button class="primary" id="broadcastBtn" onclick="sendBroadcast()">Send to all users</button>
      <div class="error" id="berror"></div>
      <div id="bresult" style="margin-top:12px;color:#8e8ea8;font-size:13px"></div>
    </div>
  </div>

  <div class="pane" id="pane-room">
    <div class="card">
      <h3 style="margin-top:0;font-size:15px;font-weight:500">Global Room</h3>
      <div class="sub" style="margin-bottom:16px">
        Rename the room everyone is in. Changes take effect immediately for all users.
      </div>
      <div class="field">
        <label>Room name</label>
        <input id="roomName" type="text" placeholder="Global Room">
      </div>
      <button class="primary" id="roomBtn" onclick="renameRoom()">Save room name</button>
      <div class="error" id="rerror"></div>
      <div class="success" id="rresult"></div>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
let allReleases = [];

function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2500);
}

function switchTab(name, ev) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.pane').forEach(p => p.classList.remove('active'));
  document.getElementById('pane-' + name).classList.add('active');
  if (ev && ev.target) ev.target.classList.add('active');
  if (name === 'room') loadRoom();
}

function onKeyInput() {
  const key = document.getElementById('key').value;
  if (key.length > 0) {
    loadReleases();
    loadRoom();
  }
}

async function loadRoom() {
  const key = document.getElementById('key').value;
  if (!key) return;
  try {
    const r = await fetch('/admin/room', { headers: { 'X-Admin-Key': key } });
    if (r.ok) {
      const data = await r.json();
      document.getElementById('roomName').value = data.name;
    }
  } catch (e) {}
}

async function renameRoom() {
  const key = document.getElementById('key').value;
  if (!key) { document.getElementById('rerror').textContent = 'Enter admin key'; return; }

  const name = document.getElementById('roomName').value.trim();
  if (!name) { document.getElementById('rerror').textContent = 'Name is required'; return; }

  document.getElementById('rerror').textContent = '';
  document.getElementById('rresult').textContent = '';
  document.getElementById('roomBtn').disabled = true;

  try {
    const r = await fetch('/admin/room', {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        'X-Admin-Key': key
      },
      body: JSON.stringify({ name: name })
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      document.getElementById('rerror').textContent = err.detail || 'Rename failed';
    } else {
      document.getElementById('rresult').textContent = 'Room renamed to "' + name + '"';
      toast('Room renamed');
    }
  } catch (e) {
    document.getElementById('rerror').textContent = 'Network error';
  } finally {
    document.getElementById('roomBtn').disabled = false;
  }
}

async function loadReleases() {
  const key = document.getElementById('key').value;
  if (!key) return;
  try {
    const r = await fetch('/admin/releases', {
      headers: { 'X-Admin-Key': key }
    });
    if (!r.ok) {
      document.getElementById('list').innerHTML = '<div class="release-meta">Invalid admin key.</div>';
      return;
    }
    allReleases = await r.json();
    renderList();
  } catch (e) {
    document.getElementById('list').innerHTML = '<div class="release-meta">Network error.</div>';
  }
}

function renderList() {
  const list = document.getElementById('list');
  if (allReleases.length === 0) {
    list.innerHTML = '<div class="release-meta">No releases yet.</div>';
    return;
  }
  list.innerHTML = '';
  for (const r of allReleases) {
    const el = document.createElement('div');
    el.className = 'release';
    const blocking = r.blocking
      ? '<span class="badge badge-block">blocking</span>'
      : '<span class="badge badge-ok">optional</span>';
    el.innerHTML = `
      <div class="release-info">
        <div class="release-title">v${r.version_name} (code ${r.version_code}) ${blocking}</div>
        <div class="release-meta">${r.download_url}</div>
        <div class="release-meta">${r.notes || '(no notes)'}</div>
      </div>
      <button class="delete-btn" onclick="deleteRelease(${r.version_code})">Delete</button>
    `;
    list.appendChild(el);
  }
}

async function publish() {
  const key = document.getElementById('key').value;
  if (!key) { document.getElementById('error').textContent = 'Enter admin key'; return; }

  const vcode = parseInt(document.getElementById('vcode').value, 10);
  const vname = document.getElementById('vname').value.trim();
  const url = document.getElementById('url').value.trim();
  const notes = document.getElementById('notes').value;
  const blocking = document.getElementById('blocking').checked;

  if (!vcode || !vname || !url) {
    document.getElementById('error').textContent = 'All fields except notes are required';
    return;
  }

  document.getElementById('error').textContent = '';
  document.getElementById('publishBtn').disabled = true;

  try {
    const r = await fetch('/admin/releases', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Admin-Key': key
      },
      body: JSON.stringify({
        version_code: vcode,
        version_name: vname,
        download_url: url,
        notes: notes,
        blocking: blocking
      })
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      document.getElementById('error').textContent = err.detail || 'Publish failed';
    } else {
      toast('Published v' + vname);
      document.getElementById('vcode').value = '';
      document.getElementById('vname').value = '';
      document.getElementById('url').value = '';
      document.getElementById('notes').value = '';
      document.getElementById('blocking').checked = false;
      loadReleases();
    }
  } catch (e) {
    document.getElementById('error').textContent = 'Network error';
  } finally {
    document.getElementById('publishBtn').disabled = false;
  }
}

async function deleteRelease(vcode) {
  if (!confirm('Delete release code ' + vcode + '?')) return;
  const key = document.getElementById('key').value;
  try {
    const r = await fetch('/admin/releases/' + vcode, {
      method: 'DELETE',
      headers: { 'X-Admin-Key': key }
    });
    if (r.ok) {
      toast('Deleted');
      loadReleases();
    } else {
      toast('Delete failed');
    }
  } catch (e) {
    toast('Network error');
  }
}

async function sendBroadcast() {
  const key = document.getElementById('key').value;
  if (!key) { document.getElementById('berror').textContent = 'Enter admin key'; return; }

  const content = document.getElementById('bcontent').value.trim();
  if (!content) { document.getElementById('berror').textContent = 'Write something first'; return; }

  if (!confirm('Send this to every user?')) return;

  document.getElementById('berror').textContent = '';
  document.getElementById('bresult').textContent = '';
  document.getElementById('broadcastBtn').disabled = true;

  try {
    const r = await fetch('/admin/broadcast', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Admin-Key': key
      },
      body: JSON.stringify({ content: content })
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      document.getElementById('berror').textContent = err.detail || 'Broadcast failed';
    } else {
      const res = await r.json();
      document.getElementById('bresult').textContent =
        'Sent to ' + res.sent_to + ' of ' + res.total_users + ' users.';
      document.getElementById('bcontent').value = '';
      toast('Broadcast sent');
    }
  } catch (e) {
    document.getElementById('berror').textContent = 'Network error';
  } finally {
    document.getElementById('broadcastBtn').disabled = false;
  }
}

(function() {
  const params = new URLSearchParams(window.location.search);
  const k = params.get('key');
  if (k) {
    document.getElementById('key').value = k;
    loadReleases();
    loadRoom();
  }
})();
</script>
</body>
</html>
"""


@app.get("/admin", response_class=HTMLResponse)
def admin_panel():
    return HTMLResponse(content=ADMIN_HTML)


# ---------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------

class ConnectionManager:
    def __init__(self):
        self.active: dict[int, set[WebSocket]] = {}

    async def connect(self, user_id: int, websocket: WebSocket):
        await websocket.accept()
        if user_id not in self.active:
            self.active[user_id] = set()
        self.active[user_id].add(websocket)

    def disconnect(self, user_id: int, websocket: WebSocket):
        if user_id in self.active:
            self.active[user_id].discard(websocket)
            if not self.active[user_id]:
                del self.active[user_id]

    async def send_to(self, user_id: int, message: dict):
        sockets = self.active.get(user_id)
        if not sockets:
            return
        dead = []
        for ws in list(sockets):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(user_id, ws)

    async def broadcast(self, message: dict):
        for uid in list(self.active.keys()):
            await self.send_to(uid, message)

    async def broadcast_except(self, exclude_user_id: int, message: dict):
        for uid in list(self.active.keys()):
            if uid == exclude_user_id:
                continue
            await self.send_to(uid, message)


manager = ConnectionManager()


def _ws_handle_message(db: Session, user_id: int, username: str, data: dict) -> None:
    msg_type = (data.get("type") or "message").strip()

    try:
        from features import handle_feature_ws
        if handle_feature_ws(user_id, username, db, msg_type, data):
            return
    except Exception as e:
        print(f"[ws] feature handler error: {e!r}")

    if msg_type in ("typing", "stop_typing"):
        target = data.get("to")
        if not target:
            return
        try:
            import asyncio
            asyncio.create_task(manager.send_to(int(target), {
                "type": msg_type,
                "event": msg_type,
                "from": user_id,
                "from_username": username,
            }))
        except Exception:
            pass
        return

    if msg_type == "room_message":
        content = (data.get("content") or "").strip()
        media_kind = (data.get("media_kind") or "text").strip().lower()
        media_key = data.get("media_key")
        media_thumb_key = data.get("media_thumb_key")
        media_name = data.get("media_name")
        media_size = data.get("media_size")
        media_width = data.get("media_width")
        media_height = data.get("media_height")
        media_duration = data.get("media_duration")

        if media_kind == "text":
            if not content:
                return
        else:
            if not media_key:
                return
            if not content:
                content = ""

        recent_cutoff = datetime.utcnow() - timedelta(seconds=ROOM_RATE_LIMIT_SECONDS)
        recent = (db.query(Message)
                  .filter(Message.sender_id == user_id)
                  .filter(Message.room_id != None)
                  .filter(Message.created_at >= recent_cutoff)
                  .first())
        if recent is not None:
            try:
                import asyncio
                asyncio.create_task(manager.send_to(user_id, {
                    "type": "error",
                    "event": "error",
                    "message": "Slow down — one message every few seconds",
                }))
            except Exception:
                pass
            return

        room = get_global_room(db)
        stored_ts = resolve_client_timestamp(data.get("created_at"))
        reply_to = data.get("reply_to_id")

        msg = Message(
            sender_id=user_id,
            room_id=room.id,
            content=content,
            reply_to_id=reply_to,
            type=media_kind,
            media_key=media_key,
            media_thumb_key=media_thumb_key,
            media_name=media_name,
            media_size=media_size,
            media_width=media_width,
            media_height=media_height,
            media_duration=media_duration,
        )
        if stored_ts is not None:
            msg.created_at = stored_ts

        db.add(msg)
        db.commit()
        db.refresh(msg)

        u = db.query(User).filter(User.id == user_id).first()
        if u and not u.is_bot:
            u.last_seen = datetime.utcnow()
            db.commit()

        media_fields = _media_fields_for_message(msg)

        payload_out = {
            "type": "room_message",
            "event": "room_message",
            "media_kind": media_fields["type"],
            "room_id": room.id,
            "id": msg.id,
            "sender_id": user_id,
            "sender_username": username,
            "sender_display_name": display_name_of(u) if u else username,
            "sender_avatar": u.avatar if u else None,
            "content": content,
            "created_at": msg.created_at.isoformat(),
            "reply_to": reply_preview_of(db, msg),
            "edited_at": None,
            "reactions": [],
        }
        payload_out.update(media_fields)
        try:
            import asyncio
            asyncio.create_task(manager.broadcast_except(user_id, payload_out))
        except Exception:
            pass
        return

    receiver_id = data.get("to")
    content = (data.get("content") or "").strip()

    if not receiver_id or not content:
        return

    receiver = db.query(User).filter(User.id == receiver_id).first()
    if not receiver:
        return
    if receiver.deleted_at is not None:
        return

    stored_ts = resolve_client_timestamp(data.get("created_at"))
    reply_to = data.get("reply_to_id")

    if stored_ts is not None:
        msg = Message(sender_id=user_id, receiver_id=receiver_id,
                      content=content, created_at=stored_ts,
                      reply_to_id=reply_to)
    else:
        msg = Message(sender_id=user_id, receiver_id=receiver_id,
                      content=content, reply_to_id=reply_to)
    db.add(msg)
    db.commit()
    db.refresh(msg)

    u = db.query(User).filter(User.id == user_id).first()
    if u and not u.is_bot:
        u.last_seen = datetime.utcnow()
        db.commit()

    media_fields = _media_fields_for_message(msg)

    payload_out = {
        "id": msg.id,
        "from": user_id,
        "from_username": username,
        "to": receiver_id,
        "content": content,
        "created_at": msg.created_at.isoformat(),
        "read_at": None,
        "reply_to": reply_preview_of(db, msg),
        "edited_at": None,
        "media_kind": media_fields["type"],
        "reactions": [],
    }
    payload_out.update(media_fields)

    try:
        import asyncio
        asyncio.create_task(manager.send_to(user_id, payload_out))
        asyncio.create_task(manager.send_to(receiver_id, payload_out))
    except Exception:
        pass

    if receiver.is_bot:
        try:
            reply_text = bot_reply(db, u, content)
            reply_msg = send_bot_message(db, user_id, reply_text)
            if reply_msg:
                reply_payload = {
                    "id": reply_msg.id,
                    "from": reply_msg.sender_id,
                    "from_username": BOT_USERNAME,
                    "to": user_id,
                    "content": reply_msg.content,
                    "created_at": reply_msg.created_at.isoformat(),
                    "read_at": None,
                    "reply_to": None,
                    "edited_at": None,
                    "media_kind": "text",
                    "type": "text",
                    "media_url": None,
                    "thumb_url": None,
                    "media_name": None,
                    "media_size": None,
                    "media_width": None,
                    "media_height": None,
                    "media_duration": None,
                    "reactions": [],
                }
                try:
                    import asyncio
                    asyncio.create_task(manager.send_to(user_id, reply_payload))
                except Exception:
                    pass
        except Exception:
            pass


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = Query(...)):
    payload = decode_token(token)
    if not payload:
        await websocket.close(code=1008)
        return

    user_id = int(payload["sub"])
    username = payload["username"]

    db0 = SessionLocal()
    try:
        u0 = db0.query(User).filter(User.id == user_id).first()
        if not u0 or u0.deleted_at is not None:
            await websocket.close(code=1008)
            return
    finally:
        db0.close()

    await manager.connect(user_id, websocket)

    try:
        from features import presence_on_connect
        presence_on_connect(user_id)
    except Exception as e:
        print(f"[ws] presence_on_connect error: {e!r}")

    try:
        while True:
            data = await websocket.receive_json()
            db = SessionLocal()
            try:
                u = db.query(User).filter(User.id == user_id).first()
                if u and not u.is_bot:
                    u.last_seen = datetime.utcnow()
                    db.commit()
                _ws_handle_message(db, user_id, username, data)
            except Exception as e:
                print(f"[ws] handler error for user {user_id}: {e!r}")
            finally:
                db.close()

    except WebSocketDisconnect:
        manager.disconnect(user_id, websocket)
        try:
            from features import presence_on_disconnect
            presence_on_disconnect(user_id)
        except Exception as e:
            print(f"[ws] presence_on_disconnect error: {e!r}")
    except Exception:
        manager.disconnect(user_id, websocket)
        try:
            from features import presence_on_disconnect
            presence_on_disconnect(user_id)
        except Exception as e:
            print(f"[ws] presence_on_disconnect error: {e!r}")


# ---------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------
# Phase A+ features
# ---------------------------------------------------------------------

from features import register_features
register_features(app)