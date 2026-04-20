"""Create a Spotify playlist from matched tracks."""
from __future__ import annotations

import spotipy


def create_and_fill(
    sp: spotipy.Spotify,
    name: str,
    description: str,
    track_uris: list[str],
    public: bool = False,
) -> str:
    user_id = sp.me()["id"]
    playlist = sp.user_playlist_create(
        user=user_id, name=name, public=public, description=description
    )
    playlist_id = playlist["id"]
    # Spotify add_items limit: 100 per call
    uniq = list(dict.fromkeys(u for u in track_uris if u))
    for i in range(0, len(uniq), 100):
        sp.playlist_add_items(playlist_id, uniq[i : i + 100])
    return playlist["external_urls"]["spotify"]
