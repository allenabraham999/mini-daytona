from __future__ import annotations

from dataclasses import dataclass

import jwt
from fastapi import HTTPException, Request, status

from .config import settings


@dataclass
class Principal:
    user_id: str
    raw_claims: dict


def _decode(token: str) -> dict:
    options = {"require": ["sub", "exp"]}
    kwargs = {}
    if settings.jwt_audience:
        kwargs["audience"] = settings.jwt_audience
    return jwt.decode(
        token,
        settings.jwt_secret,
        algorithms=[settings.jwt_algorithm],
        options=options,
        **kwargs,
    )


async def authenticate(request: Request) -> Principal:
    """FastAPI dependency. Reads `Authorization: Bearer <jwt>` and resolves the
    caller. Sets `request.state.principal` so middleware (rate limiter) can read
    it without re-parsing the token."""
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = header.split(" ", 1)[1].strip()
    try:
        claims = _decode(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="token expired")
    except jwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"invalid token: {e}")

    principal = Principal(user_id=str(claims["sub"]), raw_claims=claims)
    request.state.principal = principal
    return principal
