"""Search each local track on Spotify and score the best match.

Efficiency rules:
- 1 strict query per track; fall back to 1 loose query only if strict returned 0 items
- Bail fast on 429 with long retry-after (Dev Mode lockout — no point sleeping hours)
- Save cache every N tracks so interrupts never lose progress
- Gentle throttle between requests
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import spotipy
from rapidfuzz import fuzz
from tqdm import tqdm

AUTO_ACCEPT = 80
REVIEW = 60

# Throttle + retry behavior
THROTTLE_SEC = 0.15        # ~6 req/sec — well under typical Spotify limits
MAX_RETRY_AFTER_SEC = 60   # if Spotify says wait longer than this, bail
CACHE_FLUSH_EVERY = 25     # flush cache to disk every N tracks


class RateLimitLockout(RuntimeError):
    """Raised when Spotify asks us to wait longer than MAX_RETRY_AFTER_SEC."""


def _normalize(s: str) -> str:
    s = s.lower()
    s = re.sub(r"\(feat[^)]*\)", "", s)
    s = re.sub(r"feat\.?[^,)]*", "", s)
    s = re.sub(r"\s+ost.*", "", s)
    s = re.sub(r"[^\w\s가-힣]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _score(local_artist: str, local_title: str, sp_artists: list[str], sp_title: str) -> int:
    la = _normalize(local_artist)
    lt = _normalize(local_title)
    sa = _normalize(" ".join(sp_artists))
    st = _normalize(sp_title)
    artist_score = max(fuzz.token_set_ratio(la, sa), fuzz.partial_ratio(la, sa))
    title_score = max(fuzz.token_set_ratio(lt, st), fuzz.partial_ratio(lt, st))
    return int(round(title_score * 0.6 + artist_score * 0.4))


def _search(sp: spotipy.Spotify, query: str, market: str | None):
    try:
        return sp.search(q=query, type="track", limit=5, market=market)
    except spotipy.SpotifyException as e:
        if e.http_status == 429:
            wait = 2
            if getattr(e, "headers", None):
                try:
                    wait = int(e.headers.get("Retry-After", "2"))
                except (TypeError, ValueError):
                    wait = 2
            if wait > MAX_RETRY_AFTER_SEC:
                raise RateLimitLockout(
                    f"Spotify asked to wait {wait}s ({wait / 3600:.1f}h). "
                    "Likely Dev Mode 24h quota lockout — aborting."
                ) from e
            time.sleep(wait + 1)
            return sp.search(q=query, type="track", limit=5, market=market)
        raise


def _best_from_items(local_artist: str, local_title: str, items: list[dict]):
    best = None
    best_score = -1
    for it in items or []:
        if not it or not it.get("id"):
            continue
        sp_artists = [a["name"] for a in (it.get("artists") or [])]
        score = _score(local_artist, local_title, sp_artists, it.get("name", ""))
        if score > best_score:
            best_score = score
            best = (it, score)
    return best


def _lookup(sp: spotipy.Spotify, artist: str, title: str, market: str | None):
    """At most 2 queries: strict, then loose fallback only if strict was empty."""
    if artist and title:
        res = _search(sp, f'track:"{title}" artist:"{artist}"', market)
        items = (res.get("tracks") or {}).get("items") or []
        if items:
            return _best_from_items(artist, title, items)
        # strict returned nothing — try one loose query
        time.sleep(THROTTLE_SEC)
        res = _search(sp, f"{title} {artist}", market)
        items = (res.get("tracks") or {}).get("items") or []
        return _best_from_items(artist, title, items)
    if title:
        res = _search(sp, title, market)
        items = (res.get("tracks") or {}).get("items") or []
        return _best_from_items(artist, title, items)
    return None


def _blank_record(path: str, artist: str, title: str) -> dict:
    return {
        "path": path,
        "local_artist": artist,
        "local_title": title,
        "status": "unmatched",
        "score": 0,
        "spotify_id": "",
        "spotify_uri": "",
        "spotify_artist": "",
        "spotify_title": "",
        "spotify_url": "",
    }


def _classify(score: int) -> str:
    if score >= AUTO_ACCEPT:
        return "accepted"
    if score >= REVIEW:
        return "review"
    return "low_confidence"


def _flush(cache: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def match_all(
    sp: spotipy.Spotify,
    tracks: list[dict],
    cache_path: Path,
    market: str | None = "KR",
) -> list[dict]:
    cache: dict[str, dict] = {}
    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as f:
            cache = json.load(f)

    results: list[dict] = []
    processed = 0
    try:
        for t in tqdm(tracks, desc="matching", unit="track"):
            key = t["path"]
            if key in cache:
                results.append(cache[key])
                continue

            artist = (t.get("artist") or "").strip()
            title = (t.get("title") or "").strip()
            rec = _blank_record(key, artist, title)

            if title:
                best = _lookup(sp, artist, title, market)
                if best:
                    item, score = best
                    rec.update(
                        {
                            "score": score,
                            "spotify_id": item.get("id", ""),
                            "spotify_uri": item.get("uri", ""),
                            "spotify_artist": ", ".join(a["name"] for a in item.get("artists") or []),
                            "spotify_title": item.get("name", ""),
                            "spotify_url": (item.get("external_urls") or {}).get("spotify", ""),
                            "status": _classify(score),
                        }
                    )

            cache[key] = rec
            results.append(rec)
            processed += 1

            if processed % CACHE_FLUSH_EVERY == 0:
                _flush(cache, cache_path)

            time.sleep(THROTTLE_SEC)
    finally:
        _flush(cache, cache_path)

    return results
