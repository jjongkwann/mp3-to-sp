"""Local MP3 → Spotify playlist sync.

Typical flow:
    python main.py scan    --music-dir D:/music
    python main.py match
    python main.py report  # (optional) inspect CSV
    python main.py playlist --name "My Local Library" --include review
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from src import scan as scan_mod
from src import match as match_mod
from src import playlist as playlist_mod
from src.auth import build_client

ROOT = Path(__file__).parent
DATA = ROOT / "data"
LIBRARY_JSON = DATA / "library.json"
MATCHES_JSON = DATA / "matches.json"
CACHE_JSON = DATA / "match_cache.json"
REPORT_CSV = DATA / "report.csv"


def cmd_scan(args: argparse.Namespace) -> None:
    music_dir = Path(args.music_dir).resolve()
    if not music_dir.exists():
        raise SystemExit(f"Music dir not found: {music_dir}")
    print(f"[scan] reading tags from {music_dir}")
    tracks = scan_mod.scan_dir(music_dir)
    scan_mod.save(tracks, LIBRARY_JSON)
    print(f"[scan] {len(tracks)} tracks saved -> {LIBRARY_JSON}")


def cmd_match(args: argparse.Namespace) -> None:
    if not LIBRARY_JSON.exists():
        raise SystemExit(f"Run `scan` first — {LIBRARY_JSON} missing")
    with LIBRARY_JSON.open("r", encoding="utf-8") as f:
        tracks = json.load(f)
    sp = build_client(DATA)
    try:
        results = match_mod.match_all(sp, tracks, CACHE_JSON, market=args.market)
    except match_mod.RateLimitLockout as e:
        print(f"\n[match] aborted: {e}")
        print("[match] progress cached — rerun `match` after cooldown to resume.")
        raise SystemExit(2)
    DATA.mkdir(parents=True, exist_ok=True)
    with MATCHES_JSON.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    _write_report(results)
    _summary(results)


def cmd_report(args: argparse.Namespace) -> None:
    if not MATCHES_JSON.exists():
        raise SystemExit(f"Run `match` first — {MATCHES_JSON} missing")
    with MATCHES_JSON.open("r", encoding="utf-8") as f:
        results = json.load(f)
    _write_report(results)
    _summary(results)


def cmd_playlist(args: argparse.Namespace) -> None:
    if not MATCHES_JSON.exists():
        raise SystemExit(f"Run `match` first — {MATCHES_JSON} missing")
    with MATCHES_JSON.open("r", encoding="utf-8") as f:
        results = json.load(f)

    include = {"accepted"}
    if args.include in ("review", "all"):
        include.add("review")
    if args.include == "all":
        include.add("low_confidence")

    uris = [r["spotify_uri"] for r in results if r["status"] in include and r["spotify_uri"]]
    if not uris:
        raise SystemExit("No tracks to add with current --include filter.")

    sp = build_client(DATA)
    description = f"Synced from local library ({len(uris)} tracks). Built with local-mp3-to-spotify."
    url = playlist_mod.create_and_fill(sp, args.name, description, uris, public=args.public)
    print(f"[playlist] created: {url}")
    print(f"[playlist] added {len(uris)} tracks (filter={args.include})")


def _write_report(results: list[dict]) -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    with REPORT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "status",
                "score",
                "local_artist",
                "local_title",
                "spotify_artist",
                "spotify_title",
                "spotify_url",
                "path",
            ]
        )
        for r in sorted(results, key=lambda r: (r["status"], -r.get("score", 0))):
            w.writerow(
                [
                    r["status"],
                    r["score"],
                    r["local_artist"],
                    r["local_title"],
                    r["spotify_artist"],
                    r["spotify_title"],
                    r["spotify_url"],
                    r["path"],
                ]
            )
    print(f"[report] wrote {REPORT_CSV}")


def _summary(results: list[dict]) -> None:
    buckets: dict[str, int] = {}
    for r in results:
        buckets[r["status"]] = buckets.get(r["status"], 0) + 1
    total = len(results)
    print(f"[summary] total={total}")
    for k in ("accepted", "review", "low_confidence", "unmatched"):
        v = buckets.get(k, 0)
        pct = (v / total * 100) if total else 0
        print(f"  {k:<16} {v:>5}  ({pct:.1f}%)")


def main() -> None:
    p = argparse.ArgumentParser(description="Sync local MP3 library to a Spotify playlist.")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("scan", help="Scan local music directory into library.json")
    s.add_argument("--music-dir", default=r"D:\music", help="Path to music folder")
    s.set_defaults(func=cmd_scan)

    m = sub.add_parser("match", help="Search Spotify for each local track")
    m.add_argument("--market", default="KR", help="Spotify market code (default KR)")
    m.set_defaults(func=cmd_match)

    r = sub.add_parser("report", help="Regenerate CSV summary from matches.json")
    r.set_defaults(func=cmd_report)

    pl = sub.add_parser("playlist", help="Create a Spotify playlist from matches")
    pl.add_argument("--name", required=True, help="Playlist name")
    pl.add_argument(
        "--include",
        choices=["accepted", "review", "all"],
        default="accepted",
        help="accepted=only high-confidence; review=+medium; all=+low",
    )
    pl.add_argument("--public", action="store_true", help="Make playlist public (default private)")
    pl.set_defaults(func=cmd_playlist)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
