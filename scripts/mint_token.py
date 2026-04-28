#!/usr/bin/env python3
"""Mint a dev JWT for hitting the gateway locally.

Usage:
    python scripts/mint_token.py <user_id> [--ttl 3600]

Reads JWT_SECRET / JWT_ALGORITHM / JWT_AUDIENCE from the env, falling back to
the same defaults as the gateway.
"""
from __future__ import annotations

import argparse
import os
import time

import jwt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("user_id")
    parser.add_argument("--ttl", type=int, default=3600)
    args = parser.parse_args()

    secret = os.environ.get("JWT_SECRET", "change-me-in-production")
    algo = os.environ.get("JWT_ALGORITHM", "HS256")
    aud = os.environ.get("JWT_AUDIENCE", "")

    now = int(time.time())
    claims = {"sub": args.user_id, "iat": now, "exp": now + args.ttl}
    if aud:
        claims["aud"] = aud

    print(jwt.encode(claims, secret, algorithm=algo))


if __name__ == "__main__":
    main()
