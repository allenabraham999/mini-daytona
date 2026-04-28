from __future__ import annotations

import asyncio
import time

import jwt
from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from .config import settings


class _TokenBucket:
    __slots__ = ("tokens", "last_refill")

    def __init__(self, initial: float) -> None:
        self.tokens = initial
        self.last_refill = time.monotonic()


class RateLimiterMiddleware(BaseHTTPMiddleware):
    """Per-user token bucket. Refills at `rate_limit_per_second`, capped at
    `rate_limit_burst`. Identifies users via the JWT `sub` claim — we decode
    here without verifying because the auth dependency runs after middleware
    and will reject forged tokens. Falling back to the client IP keeps health
    checks and unauthenticated probes from sharing a bucket."""

    def __init__(self, app, *, exempt_paths: tuple[str, ...] = ()) -> None:
        super().__init__(app)
        self._buckets: dict[str, _TokenBucket] = {}
        self._lock = asyncio.Lock()
        self._exempt = exempt_paths

    async def dispatch(self, request: Request, call_next):
        if request.url.path in self._exempt:
            return await call_next(request)

        key = self._key_for(request)
        allowed = await self._consume(key)
        if not allowed:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"error": "rate_limited", "detail": "too many requests"},
                headers={"Retry-After": "1"},
            )
        return await call_next(request)

    def _key_for(self, request: Request) -> str:
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
            try:
                claims = jwt.decode(token, options={"verify_signature": False})
                sub = claims.get("sub")
                if sub:
                    return f"user:{sub}"
            except jwt.InvalidTokenError:
                pass
        client = request.client.host if request.client else "unknown"
        return f"ip:{client}"

    async def _consume(self, key: str) -> bool:
        now = time.monotonic()
        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _TokenBucket(initial=float(settings.rate_limit_burst))
                self._buckets[key] = bucket

            elapsed = now - bucket.last_refill
            bucket.tokens = min(
                float(settings.rate_limit_burst),
                bucket.tokens + elapsed * settings.rate_limit_per_second,
            )
            bucket.last_refill = now

            if bucket.tokens < 1.0:
                return False
            bucket.tokens -= 1.0
            return True
