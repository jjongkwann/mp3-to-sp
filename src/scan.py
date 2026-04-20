"""Scan a music directory, extract track metadata from tags + filename."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path

from mutagen import File as MutagenFile
from tqdm import tqdm

AUDIO_EXTS = {".mp3", ".m4a", ".flac", ".wav"}


@dataclass
class Track:
    path: str
    artist: str
    title: str
    album: str
    duration_sec: float
    source: str  # "tag" or "filename"


def _fix_mojibake(s: str) -> str:
    """Older Korean MP3s store CP949 bytes inside Latin-1 ID3 frames.

    mutagen decodes the frame per the spec (Latin-1), leaving garbled text.
    Round-trip through Latin-1 → CP949 recovers it when Hangul is present.
    """
    if not s:
        return s
    try:
        raw = s.encode("latin-1")
    except UnicodeEncodeError:
        return s  # already contains non-latin codepoints — trust it
    for enc in ("cp949", "euc-kr", "utf-8"):
        try:
            decoded = raw.decode(enc)
        except UnicodeDecodeError:
            continue
        if any("\uac00" <= c <= "\ud7af" for c in decoded):
            return decoded
    return s


def _tag_first(tags, *keys) -> str:
    if not tags:
        return ""
    for k in keys:
        v = tags.get(k)
        if v:
            if isinstance(v, list):
                v = v[0]
            s = _fix_mojibake(str(v).strip())
            if s:
                return s
    return ""


def _from_tags(path: Path):
    try:
        audio = MutagenFile(str(path), easy=True)
    except Exception:
        return None
    if audio is None:
        return None
    tags = getattr(audio, "tags", None) or {}
    artist = _tag_first(tags, "artist", "albumartist", "performer")
    title = _tag_first(tags, "title")
    album = _tag_first(tags, "album")
    duration = float(getattr(audio.info, "length", 0.0) or 0.0)
    if artist and title:
        return Track(
            path=str(path),
            artist=artist,
            title=title,
            album=album,
            duration_sec=round(duration, 2),
            source="tag",
        )
    return Track(
        path=str(path),
        artist=artist,
        title=title,
        album=album,
        duration_sec=round(duration, 2),
        source="partial",
    )


# filename patterns seen in D:/music:
#   "Artist - Album - TrackNum - Title.ext"      (most common)
#   "Artist_Title.ext"
#   "Artist - Title.ext"
_SPLIT = re.compile(r"\s+-\s+")


def _from_filename(path: Path) -> Track:
    stem = path.stem
    # "Artist_Title" fallback
    if " - " not in stem and "_" in stem:
        left, _, right = stem.partition("_")
        return Track(
            path=str(path),
            artist=left.strip(),
            title=right.strip(),
            album="",
            duration_sec=0.0,
            source="filename",
        )
    parts = [p.strip() for p in _SPLIT.split(stem)]
    artist = title = album = ""
    if len(parts) >= 4:
        artist, album, _track_no, *rest = parts
        title = " - ".join(rest)
    elif len(parts) == 3:
        artist, album, title = parts
    elif len(parts) == 2:
        artist, title = parts
    else:
        title = stem
    return Track(
        path=str(path),
        artist=artist,
        title=title,
        album=album,
        duration_sec=0.0,
        source="filename",
    )


def _clean(s: str) -> str:
    # trim things Spotify search rarely matches on
    s = re.sub(r"\[[^\]]*\]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract(path: Path) -> Track:
    tagged = _from_tags(path)
    if tagged and tagged.source == "tag":
        tagged.artist = _clean(tagged.artist)
        tagged.title = _clean(tagged.title)
        return tagged
    fname = _from_filename(path)
    # if tags had partial info, prefer it over parsed filename
    if tagged:
        fname.artist = tagged.artist or fname.artist
        fname.title = tagged.title or fname.title
        fname.album = tagged.album or fname.album
        fname.duration_sec = tagged.duration_sec or fname.duration_sec
    fname.artist = _clean(fname.artist)
    fname.title = _clean(fname.title)
    return fname


def scan_dir(music_dir: Path) -> list[Track]:
    files = [p for p in music_dir.rglob("*") if p.suffix.lower() in AUDIO_EXTS]
    files.sort()
    tracks: list[Track] = []
    for p in tqdm(files, desc="scanning", unit="file"):
        tracks.append(extract(p))
    return tracks


def save(tracks: list[Track], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(t) for t in tracks], f, ensure_ascii=False, indent=2)
