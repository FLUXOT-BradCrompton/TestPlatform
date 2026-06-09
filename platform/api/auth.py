"""
JWT authentication, user model, and FastAPI dependency helpers.
"""
from __future__ import annotations

import enum
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, Field

from platform.api.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class UserRole(str, enum.Enum):
    ADMIN = "ADMIN"
    OPERATOR = "OPERATOR"
    VIEWER = "VIEWER"


# ---------------------------------------------------------------------------
# In-memory user store (replace with DB-backed store in production)
# ---------------------------------------------------------------------------
# Passwords are bcrypt-hashed.  The plaintext values for seeding are:
#   admin    -> "FluxOT-Admin-2024!"
#   operator -> "FluxOT-Operator-2024!"
#   viewer   -> "FluxOT-Viewer-2024!"

_HASHED_PASSWORDS: dict[str, str] = {}


class User(BaseModel):
    username: str
    hashed_password: str
    role: UserRole
    disabled: bool = False


# Populated at module load time to avoid bcrypt cost on every import
_USER_STORE: dict[str, User] = {}


def _bootstrap_users() -> None:
    """Create default users with securely hashed passwords."""
    pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
    defaults = [
        ("admin", "FluxOT-Admin-2024!", UserRole.ADMIN),
        ("operator", "FluxOT-Operator-2024!", UserRole.OPERATOR),
        ("viewer", "FluxOT-Viewer-2024!", UserRole.VIEWER),
    ]
    for username, plaintext, role in defaults:
        _USER_STORE[username] = User(
            username=username,
            hashed_password=pwd_ctx.hash(plaintext),
            role=role,
        )


_bootstrap_users()

# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    return _pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return _pwd_context.verify(plain, hashed)


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Return a signed JWT containing *data* with an expiry claim."""
    payload = data.copy()
    expire = datetime.now(tz=timezone.utc) + (
        expires_delta or timedelta(minutes=settings.JWT_EXPIRE_MINUTES)
    )
    payload["exp"] = expire
    payload["iat"] = datetime.now(tz=timezone.utc)
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def verify_token(token: str) -> dict:
    """Decode and verify a JWT.  Raises HTTPException 401 on failure."""
    try:
        payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.JWT_ALGORITHM]
        )
        return payload
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token validation failed",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


# ---------------------------------------------------------------------------
# FastAPI OAuth2 / Bearer dependency
# ---------------------------------------------------------------------------

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")


async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    """FastAPI dependency — resolves the current authenticated user."""
    credentials_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    payload = verify_token(token)
    username: Optional[str] = payload.get("sub")
    if username is None:
        raise credentials_exc

    user = _USER_STORE.get(username)
    if user is None or user.disabled:
        raise credentials_exc

    return user


async def require_admin(current_user: User = Depends(get_current_user)) -> User:
    """Dependency that additionally asserts the user has ADMIN role."""
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Administrator role required",
        )
    return current_user


async def require_operator(current_user: User = Depends(get_current_user)) -> User:
    """Dependency that asserts ADMIN or OPERATOR role."""
    if current_user.role not in (UserRole.ADMIN, UserRole.OPERATOR):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operator role or higher required",
        )
    return current_user


# ---------------------------------------------------------------------------
# User authentication
# ---------------------------------------------------------------------------


def authenticate_user(username: str, password: str) -> Optional[User]:
    """Return the user if credentials are valid, otherwise None."""
    user = _USER_STORE.get(username)
    if user is None or user.disabled:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


# ---------------------------------------------------------------------------
# Pydantic response schemas
# ---------------------------------------------------------------------------


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = Field(description="Token lifetime in seconds")
    role: UserRole


class TokenData(BaseModel):
    username: Optional[str] = None
    role: Optional[UserRole] = None


# ---------------------------------------------------------------------------
# Auth router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])


@router.post("/token", response_model=TokenResponse, summary="Obtain a JWT access token")
async def login(form: OAuth2PasswordRequestForm = Depends()) -> TokenResponse:
    user = authenticate_user(form.username, form.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(data={"sub": user.username, "role": user.role.value})
    return TokenResponse(
        access_token=token,
        expires_in=settings.JWT_EXPIRE_MINUTES * 60,
        role=user.role,
    )


@router.get("/me", response_model=dict, summary="Return current user info")
async def current_user_info(user: User = Depends(get_current_user)) -> dict:
    return {"username": user.username, "role": user.role}
