"""
Lantern Backend — FastAPI + Neon Postgres + JWT + WebSockets
Single-file backend for the Lantern chat app.
"""

import os
import secrets
import base64
from datetime import datetime, timedelta
from typing import Optional, List

import bcrypt
from jose import jwt, JWTError
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query, Header, Request
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

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, connect_args={"sslmode": "require"})
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
    last_seen = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)
    sender_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    receiver_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    content = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

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


def _ensure_column(table: str, column: str, coltype: str):
    with engine.begin() as conn:
        try:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}"))
        except Exception:
            pass

_ensure_column("users", "bio", "VARCHAR")
_ensure_column("users", "interests", "VARCHAR")
_ensure_column("users", "avatar", "TEXT")
_ensure_column("users", "last_seen", "TIMESTAMP WITH TIME ZONE")


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
    online: bool

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class ProfileUpdate(BaseModel):
    username: Optional[str] = None
    bio: Optional[str] = None
    phone: Optional[str] = None
    interests: Optional[List[str]] = None
    avatar: Optional[str] = None


class DiscoverUser(BaseModel):
    id: int
    username: str
    bio: Optional[str]
    interests: List[str]
    avatar: Optional[str]
    online: bool
    shared_interests: List[str]


class ConversationOut(BaseModel):
    user_id: int
    username: str
    avatar: Optional[str]
    last_message: str
    last_timestamp: str
    online: bool


class SendMessageIn(BaseModel):
    to: int
    content: str


class MessageOut(BaseModel):
    id: int
    sender_id: int
    receiver_id: int
    content: str
    created_at: str


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
    if not user.last_seen:
        return False
    delta = datetime.utcnow() - user.last_seen.replace(tzinfo=None)
    return delta.total_seconds() <= ONLINE_WINDOW_SECONDS


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


# ---------------------------------------------------------------------
# App
# ---------------------------------------------------------------------

app = FastAPI(title="Lantern Backend")


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
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return Token(access_token=create_access_token(user.id, user.username))


@app.post("/auth/login", response_model=Token)
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(
        or_(User.username == form.username, User.email == form.username)
    ).first()
    if not user or not verify_password(form.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    user.last_seen = datetime.utcnow()
    db.commit()
    return Token(access_token=create_access_token(user.id, user.username))


@app.get("/auth/me", response_model=UserOut)
def me(user: User = Depends(get_current_user)):
    return UserOut(
        id=user.id, username=user.username, email=user.email, phone=user.phone,
        bio=user.bio, interests=user.interests, avatar=user.avatar, online=True,
    )


@app.put("/users/me", response_model=UserOut)
def update_profile(data: ProfileUpdate,
                   user: User = Depends(get_current_user),
                   db: Session = Depends(get_db)):

    if data.username is not None and data.username != user.username:
        new_name = data.username.strip()
        if not new_name:
            raise HTTPException(status_code=400, detail="Username can't be empty")
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

    db.commit()
    db.refresh(user)
    return UserOut(
        id=user.id, username=user.username, email=user.email, phone=user.phone,
        bio=user.bio, interests=user.interests, avatar=user.avatar, online=True,
    )


# ---------------------------------------------------------------------
# Discover
# ---------------------------------------------------------------------

@app.get("/users/discover", response_model=List[DiscoverUser])
def discover(limit: int = 30,
             user: User = Depends(get_current_user),
             db: Session = Depends(get_db)):
    my_interests = set(parse_interests(user.interests))
    pool = (db.query(User)
            .filter(User.id != user.id)
            .order_by(User.last_seen.desc().nullslast())
            .limit(200)
            .all())

    def score(u: User):
        theirs = set(parse_interests(u.interests))
        s = len(my_interests & theirs) * 10
        if is_online(u):
            s += 5
        return s

    pool.sort(key=score, reverse=True)

    out = []
    for u in pool[:limit]:
        theirs = parse_interests(u.interests)
        out.append(DiscoverUser(
            id=u.id, username=u.username, bio=u.bio,
            interests=theirs, avatar=u.avatar,
            online=is_online(u),
            shared_interests=list(my_interests & set(theirs)),
        ))
    return out


@app.get("/users/random", response_model=Optional[DiscoverUser])
def random_user(user: User = Depends(get_current_user),
                db: Session = Depends(get_db)):
    u = (db.query(User)
         .filter(User.id != user.id)
         .order_by(func.random())
         .first())
    if not u:
        return None
    my_interests = set(parse_interests(user.interests))
    theirs = parse_interests(u.interests)
    return DiscoverUser(
        id=u.id, username=u.username, bio=u.bio,
        interests=theirs, avatar=u.avatar,
        online=is_online(u),
        shared_interests=list(my_interests & set(theirs)),
    )


@app.get("/users/search")
def search_users(q: str,
                 user: User = Depends(get_current_user),
                 db: Session = Depends(get_db)):
    results = (db.query(User)
               .filter(User.username.ilike(f"%{q}%"))
               .filter(User.id != user.id)
               .limit(20).all())
    return [
        {
            "id": u.id, "username": u.username, "bio": u.bio,
            "interests": parse_interests(u.interests),
            "avatar": u.avatar,
            "online": is_online(u),
        }
        for u in results
    ]


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
            created_at
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY CASE WHEN sender_id = :me THEN receiver_id ELSE sender_id END
                       ORDER BY created_at DESC
                   ) AS rn
            FROM messages
            WHERE sender_id = :me OR receiver_id = :me
        ) t
        WHERE rn = 1
        ORDER BY created_at DESC
    """)
    rows = db.execute(sql, {"me": user.id}).fetchall()

    out = []
    for other_id, content, created_at in rows:
        u = db.query(User).filter(User.id == other_id).first()
        if not u:
            continue
        out.append(ConversationOut(
            user_id=u.id,
            username=u.username,
            avatar=u.avatar,
            last_message=content,
            last_timestamp=created_at.isoformat(),
            online=is_online(u),
        ))
    return out


@app.delete("/conversations/{other_user_id}")
def delete_conversation(other_user_id: int,
                        user: User = Depends(get_current_user),
                        db: Session = Depends(get_db)):
    deleted = (db.query(Message)
               .filter(or_(
                   and_(Message.sender_id == user.id, Message.receiver_id == other_user_id),
                   and_(Message.sender_id == other_user_id, Message.receiver_id == user.id),
               ))
               .delete(synchronize_session=False))
    db.commit()
    return {"deleted": deleted}


# ---------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------

@app.get("/messages/{other_user_id}")
def get_messages(other_user_id: int,
                 limit: int = 50,
                 user: User = Depends(get_current_user),
                 db: Session = Depends(get_db)):
    msgs = (db.query(Message)
            .filter(or_(
                and_(Message.sender_id == user.id, Message.receiver_id == other_user_id),
                and_(Message.sender_id == other_user_id, Message.receiver_id == user.id),
            ))
            .order_by(Message.created_at.desc())
            .limit(limit).all())
    msgs.reverse()
    return [
        {
            "id": m.id,
            "sender_id": m.sender_id,
            "receiver_id": m.receiver_id,
            "content": m.content,
            "created_at": m.created_at.isoformat(),
        }
        for m in msgs
    ]


@app.post("/messages", response_model=MessageOut)
def post_message(data: SendMessageIn,
                 user: User = Depends(get_current_user),
                 db: Session = Depends(get_db)):
    if not data.content.strip():
        raise HTTPException(status_code=400, detail="Empty message")
    receiver = db.query(User).filter(User.id == data.to).first()
    if not receiver:
        raise HTTPException(status_code=404, detail="Recipient not found")

    msg = Message(sender_id=user.id, receiver_id=data.to, content=data.content.strip())
    db.add(msg)
    db.commit()
    db.refresh(msg)

    payload_out = {
        "id": msg.id,
        "from": user.id,
        "from_username": user.username,
        "to": data.to,
        "content": msg.content,
        "created_at": msg.created_at.isoformat(),
    }
    try:
        import asyncio
        asyncio.create_task(manager.send_to(data.to, payload_out))
    except Exception:
        pass

    return MessageOut(
        id=msg.id,
        sender_id=msg.sender_id,
        receiver_id=msg.receiver_id,
        content=msg.content,
        created_at=msg.created_at.isoformat(),
    )


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
    """Public endpoint. Returns the newest release, or 404 if none published."""
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
        # Update existing
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
# Admin panel (single HTML page served from FastAPI)
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
  label { display: block; font-size: 12px; color: #8e8ea8; margin-bottom: 6px; }
  input, textarea {
    width: 100%; padding: 12px; border: 0; border-radius: 10px;
    background: #22223a; color: #f2f2f7; font-size: 14px;
    font-family: inherit; outline: none;
  }
  textarea { min-height: 80px; resize: vertical; }
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
</style>
</head>
<body>
<div class="wrap">
  <h1>Lantern — Admin</h1>
  <div class="sub">Publish updates to all users</div>

  <div class="card">
    <div class="field">
      <label>Admin key</label>
      <input id="key" type="password" placeholder="Paste ADMIN_KEY" oninput="onKeyInput()">
    </div>
  </div>

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

<div class="toast" id="toast"></div>

<script>
const API = "";
let allReleases = [];

function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2000);
}

function onKeyInput() {
  // Auto-fill from URL hash if provided
  const key = document.getElementById('key').value;
  if (key.length > 0) loadReleases();
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

// Auto-fill key from ?key= in URL
(function() {
  const params = new URLSearchParams(window.location.search);
  const k = params.get('key');
  if (k) {
    document.getElementById('key').value = k;
    loadReleases();
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
        self.active: dict[int, WebSocket] = {}

    async def connect(self, user_id: int, websocket: WebSocket):
        await websocket.accept()
        self.active[user_id] = websocket

    def disconnect(self, user_id: int):
        self.active.pop(user_id, None)

    async def send_to(self, user_id: int, message: dict):
        ws = self.active.get(user_id)
        if ws:
            try:
                await ws.send_json(message)
            except Exception:
                self.disconnect(user_id)


manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: str = Query(...)):
    payload = decode_token(token)
    if not payload:
        await websocket.close(code=1008)
        return

    user_id = int(payload["sub"])
    username = payload["username"]

    await manager.connect(user_id, websocket)

    db = SessionLocal()
    try:
        u = db.query(User).filter(User.id == user_id).first()
        if u:
            u.last_seen = datetime.utcnow()
            db.commit()

        while True:
            data = await websocket.receive_json()
            receiver_id = data.get("to")
            content = (data.get("content") or "").strip()

            if not receiver_id or not content:
                continue

            msg = Message(sender_id=user_id, receiver_id=receiver_id, content=content)
            db.add(msg)
            db.commit()
            db.refresh(msg)

            u = db.query(User).filter(User.id == user_id).first()
            if u:
                u.last_seen = datetime.utcnow()
                db.commit()

            payload_out = {
                "id": msg.id,
                "from": user_id,
                "from_username": username,
                "to": receiver_id,
                "content": content,
                "created_at": msg.created_at.isoformat(),
            }

            await manager.send_to(user_id, payload_out)
            await manager.send_to(receiver_id, payload_out)

    except WebSocketDisconnect:
        manager.disconnect(user_id)
    except Exception:
        manager.disconnect(user_id)
    finally:
        db.close()


# ---------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}