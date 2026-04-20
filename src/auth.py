"""Spotify OAuth2 — PKCE on 127.0.0.1 loopback per 2025-11-27 policy."""
from __future__ import annotations

import os
from pathlib import Path

import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth

SCOPES = "playlist-modify-private playlist-modify-public user-read-private"


def build_client(cache_dir: Path) -> spotipy.Spotify:
    load_dotenv()
    cid = os.environ.get("CLIENT_ID")
    secret = os.environ.get("CLIENT_SECRET")
    redirect = os.environ.get("REDIRECT_URI")
    missing = [n for n, v in [("CLIENT_ID", cid), ("CLIENT_SECRET", secret), ("REDIRECT_URI", redirect)] if not v]
    if missing:
        raise SystemExit(f"Missing env vars in .env: {', '.join(missing)}")
    if "localhost" in (redirect or ""):
        raise SystemExit(
            "REDIRECT_URI uses 'localhost' which Spotify rejected as of 2025-11-27. "
            "Use http://127.0.0.1:<port>/callback instead (must match Dashboard exactly)."
        )
    cache_dir.mkdir(parents=True, exist_ok=True)
    auth = SpotifyOAuth(
        client_id=cid,
        client_secret=secret,
        redirect_uri=redirect,
        scope=SCOPES,
        cache_path=str(cache_dir / ".spotify_cache"),
        open_browser=True,
    )
    # retries=0: we handle 429 / long retry-after ourselves in match.py —
    # spotipy's internal retries would otherwise silently multiply requests
    # and blow through Dev Mode quotas.
    return spotipy.Spotify(auth_manager=auth, requests_timeout=20, retries=0)
