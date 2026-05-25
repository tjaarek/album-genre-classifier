"""
Spotify Scraper — sammelt Album-Metadaten je Artist-ID.

Nutzt den Client Credentials Flow (keine User-Authentifizierung noetig).
Ergebnisse werden in data/spotify_cache.json gepersistet, damit Re-Runs
keine zusaetzlichen API-Calls verursachen.
"""

import json
import os
import time
from collections.abc import Iterable
from pathlib import Path

import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parent.parent
CACHE_PATH: Path = _ROOT / "data" / "spotify_cache.json"

REQUEST_DELAY_SEC: float = 0.2        # max 5 calls/s, within Spotify's 30s window
SPOTIFY_ARTIST_ALBUMS_LIMIT: int = 10  # conservative; documented max is 20
RATE_LIMIT_HTTP_STATUS: int = 429
MAX_ALBUMS_PER_ARTIST: int = 15

_cache: dict | None = None


def _load_cache() -> dict:
    """Load the in-memory album cache, reading from disk on first call."""
    global _cache
    if _cache is not None:
        return _cache
    if CACHE_PATH.exists():
        raw = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        _cache = raw.get("albums", raw) if isinstance(raw, dict) else {}
    else:
        _cache = {}
    return _cache


def _save_cache() -> None:
    """Persist the in-memory cache to CACHE_PATH as JSON."""
    if _cache is None:
        return
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(
        json.dumps(_cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


class SpotifyRateLimitError(RuntimeError):
    def __init__(self, retry_after_seconds: int | None, message: str) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


def _raise_if_rate_limit(exc: spotipy.SpotifyException) -> None:
    """Raise SpotifyRateLimitError if *exc* is a 429 response; otherwise no-op."""
    if exc.http_status != RATE_LIMIT_HTTP_STATUS:
        return
    retry_after: int | None = None
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
    """Construct an authenticated Spotify client using Client Credentials flow.

    Sets ``status_retries=0`` so spotipy does not internally sleep on 429
    responses — the daily-cap retry-after can be >24 h, which would hang
    the process silently.
    """
    load_dotenv()
    auth_manager = SpotifyClientCredentials(
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
    )
    return spotipy.Spotify(auth_manager=auth_manager, status_retries=0)


def _paginate_artist_albums(
    sp: spotipy.Spotify,
    artist_id: str,
    max_albums: int,
) -> list[dict]:
    """Fetch up to *max_albums* album records from Spotify, handling pagination.

    Stops early when Spotify returns an empty page or when *max_albums* is
    reached. Each page sleeps REQUEST_DELAY_SEC to respect the rate limit.

    Returns a list of normalized album dicts (keys: album_id, album_name,
    release_date, total_tracks, cover_url_640, cover_url_300, artist_id).
    """
    all_albums: list[dict] = []
    offset = 0
    limit = SPOTIFY_ARTIST_ALBUMS_LIMIT

    while len(all_albums) < max_albums:
        try:
            response = sp.artist_albums(
                artist_id,
                album_type="album",
                limit=limit,
                offset=offset,
            )
        except spotipy.SpotifyException as e:
            _raise_if_rate_limit(e)
            raise
        time.sleep(REQUEST_DELAY_SEC)
        items = response.get("items", [])
        if not items:
            break

        for album in items:
            images = album.get("images", [])
            all_albums.append({
                "album_id": album["id"],
                "album_name": album["name"],
                "release_date": album.get("release_date"),
                "total_tracks": album.get("total_tracks"),
                "cover_url_640": images[0]["url"] if len(images) > 0 else None,
                "cover_url_300": images[1]["url"] if len(images) > 1 else None,
                "artist_id": artist_id,
            })
            if len(all_albums) >= max_albums:
                break

        if len(items) < limit:
            break
        offset += limit

    return all_albums


def get_artist_albums(
    sp: spotipy.Spotify,
    artist_id: str,
    max_albums: int = MAX_ALBUMS_PER_ARTIST,
) -> list[dict]:
    """Holt die neuesten max_albums Alben eines Artists (Disk-Cache).

    Spotify liefert Alben default newest-first → wir kriegen die juengsten max_albums.
    Cap reduziert API-Calls und balanciert spaeter die Trainingsdaten pro Artist.

    Filter: album_type='album' — keine Singles, EPs, Compilations.
    """
    cache = _load_cache()
    if artist_id in cache:
        return [dict(a) for a in cache[artist_id][:max_albums]]

    all_albums = _paginate_artist_albums(sp, artist_id, max_albums)
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
