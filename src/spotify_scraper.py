"""
Spotify Scraper — sammelt Album-Metadaten je Artist-ID.

Nutzt den Client Credentials Flow (keine User-Authentifizierung noetig).
Ergebnisse werden in data/spotify_cache.json gepersistet, damit Re-Runs
keine zusaetzlichen API-Calls verursachen.
"""

import json
import os
import time
from pathlib import Path
from typing import Iterable, Optional
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


_CACHE_PATH = Path(__file__).resolve().parent.parent / "data" / "spotify_cache.json"
_cache: Optional[dict] = None

# Pacing zwischen API-Calls — schuetzt vor Spotify's 30s-Rolling-Window-Limit.
# 0.2s = max 5 Calls/Sekunde.
_REQUEST_DELAY_SEC = 0.2


def _load_cache() -> dict:
    global _cache
    if _cache is not None:
        return _cache
    if _CACHE_PATH.exists():
        raw = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
        _cache = raw.get("albums", raw) if isinstance(raw, dict) else {}
    else:
        _cache = {}
    return _cache


def _save_cache() -> None:
    if _cache is None:
        return
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CACHE_PATH.write_text(
        json.dumps(_cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


class SpotifyRateLimitError(RuntimeError):
    def __init__(self, retry_after_seconds: Optional[int], message: str):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


def _raise_if_rate_limit(exc: spotipy.SpotifyException) -> None:
    if exc.http_status != 429:
        return
    retry_after: Optional[int] = None
    headers = getattr(exc, "headers", None) or {}
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is not None:
        try:
            retry_after = int(raw)
        except (TypeError, ValueError):
            retry_after = None
    hours = f" (~{retry_after / 3600:.1f}h)" if retry_after else ""
    raise SpotifyRateLimitError(
        retry_after,
        f"Spotify API Rate-Limit erreicht. Retry-After: {retry_after}s{hours}. "
        f"Gecachte Ergebnisse bis hier sind in data/spotify_cache.json gesichert.",
    )


def get_spotify_client() -> spotipy.Spotify:
    # status_retries=0: bei 429 NICHT intern warten (spotipy-default = 3 Retries
    # mit 'Retry-After'-Sleep; bei Daily-Cap waeren das bis zu ~63h Haenger).
    load_dotenv()
    auth_manager = SpotifyClientCredentials(
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
    )
    return spotipy.Spotify(auth_manager=auth_manager, status_retries=0)


def get_artist_albums(sp: spotipy.Spotify, artist_id: str, max_albums: int = 15) -> list[dict]:
    """Holt die neuesten max_albums Alben eines Artists (Disk-Cache).

    Spotify liefert Alben default newest-first → wir kriegen die juengsten max_albums.
    Cap reduziert API-Calls und balanciert spaeter die Trainingsdaten pro Artist.

    Filter: album_type='album' — keine Singles, EPs, Compilations.
    """
    cache = _load_cache()
    if artist_id in cache:
        return [dict(a) for a in cache[artist_id][:max_albums]]

    all_albums = []
    offset = 0
    limit = 10  # Spotify-Max fuer artist_albums

    while len(all_albums) < max_albums:
        try:
            response = sp.artist_albums(
                artist_id,
                album_type='album',
                limit=limit,
                offset=offset,
            )
        except spotipy.SpotifyException as e:
            _raise_if_rate_limit(e)
            raise
        time.sleep(_REQUEST_DELAY_SEC)
        items = response.get('items', [])
        if not items:
            break

        for album in items:
            images = album.get('images', [])
            all_albums.append({
                'album_id': album['id'],
                'album_name': album['name'],
                'release_date': album.get('release_date'),
                'total_tracks': album.get('total_tracks'),
                'cover_url_640': images[0]['url'] if len(images) > 0 else None,
                'cover_url_300': images[1]['url'] if len(images) > 1 else None,
                'artist_id': artist_id,
            })
            if len(all_albums) >= max_albums:
                break

        if len(items) < limit:
            break
        offset += limit

    cache[artist_id] = all_albums
    _save_cache()
    return [dict(a) for a in all_albums]


def prune_cache(valid_artist_ids: Iterable[str]) -> list[str]:
    """Entfernt Cache-Eintraege, deren artist_id nicht in valid_artist_ids steht.

    Returns: entfernte artist_ids.
    """
    cache = _load_cache()
    valid = set(valid_artist_ids)
    stale = [aid for aid in list(cache.keys()) if aid not in valid]
    for aid in stale:
        del cache[aid]
    if stale:
        _save_cache()
    return stale
