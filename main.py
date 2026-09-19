"""
Lantern Backend — FastAPI + Neon Postgres + JWT + WebSockets
Single-file backend for the Lantern chat app.

Endpoints:
- POST   /auth/register
- POST   /auth/login
- GET    /auth/me
- PUT    /users/me
- GET    /users/discover
- GET    /users/random
- GET    /users/search
- GET    /conversations
- GET    /messages/{other_user_id}
- POST   /messages
- DELETE /messages/{message_id}
- DELETE /conversations/{other_user_id}
- WS     /ws?token=...
- GET    /health
"""

import os
import secrets
from datetime import datetime, timedelta
from typing import Optional, List

import bcrypt
from jose import jwt, JWTError
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, create_engine, or_, and_, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DATABASE_URL = os.environ.get("DATABASE_URL")
SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(32))
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7
ONLINE_WINDOW_SECONDS = 300

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


Base.metadata.create_all(bind=engine)


def _ensure_column(table: str, column: str, coltype: str):
    with engine.begin() as conn:
        try:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}"))
        except Exception:
            pass

_ensure_column("users", "bio", "VARCHAR")
_ensure_column("users", "interests", "VARCHAR")
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
    online: bool

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class ProfileUpdate(BaseModel):
    bio: Optional[str] = None
    interests: Optional[List[str]] = None


class DiscoverUser(BaseModel):
    id: int
    username: str
    bio: Optional[str]
    interests: List[str]
    online: bool
    shared_interests: List[str]


class ConversationOut(BaseModel):
    user_id: int
    username: str
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
        bio=user.bio, interests=user.interests, online=True,
    )


@app.put("/users/me", response_model=UserOut)
def update_profile(data: ProfileUpdate,
                   user: User = Depends(get_current_user),
                   db: Session = Depends(get_db)):
    if data.bio is not None:
        user.bio = data.bio
    if data.interests is not None:
        user.interests = serialize_interests(data.interests)
    db.commit()
    db.refresh(user)
    return UserOut(
        id=user.id, username=user.username, email=user.email, phone=user.phone,
        bio=user.bio, interests=user.interests, online=True,
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
            id=u.id, username=u.username, bio=u.bio, interests=theirs,
            online=is_online(u), shared_interests=list(my_interests & set(theirs)),
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
        id=u.id, username=u.username, bio=u.bio, interests=theirs,
        online=is_online(u), shared_interests=list(my_interests & set(theirs)),
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
            "interests": parse_interests(u.interests), "online": is_online(u),
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
    """REST fallback for sending a message. WebSocket is preferred,
    but this lets you test from /docs and gives the Android client
    a reliable path if the WS ever drops."""
    if not data.content.strip():
        raise HTTPException(status_code=400, detail="Empty message")
    receiver = db.query(User).filter(User.id == data.to).first()
    if not receiver:
        raise HTTPException(status_code=404, detail="Recipient not found")

    msg = Message(sender_id=user.id, receiver_id=data.to, content=data.content.strip())
    db.add(msg)
    db.commit()
    db.refresh(msg)

    # best-effort push over WebSocket if recipient is online
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
