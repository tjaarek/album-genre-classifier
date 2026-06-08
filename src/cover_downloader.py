"""
Album-Cover-Download.

Liest data/spotify_cache.json (Album-Metadaten inkl. Cover-URLs) und laedt
Cover nach {covers_dir}/{genre}/{album_id}.jpg.

Auflösung wählbar über url_key ('cover_url_640' oder 'cover_url_300'); der
Zielordner ist über covers_dir parametrisierbar, sodass 640er- und 300er-
Variante parallel koexistieren können.

Globale Deduplizierung nach album_id: bei Collabs ueber Genre-Grenzen gewinnt
das erste Genre in der Iterationsreihenfolge von src/artists.json.

Resume-fähig: bestehende, nicht-leere Dateien werden uebersprungen.
Cover-URLs liegen auf i.scdn.co und sind ohne Auth abrufbar — kein Spotify-Rate-Limit.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parent.parent
CACHE_PATH: Path = _ROOT / "data" / "spotify_cache.json"
ARTISTS_PATH: Path = _ROOT / "src" / "artists.json"
COVERS_DIR: Path = _ROOT / "data" / "covers"

MAX_ALBUMS_PER_ARTIST: int = 15
MAX_DOWNLOAD_WORKERS: int = 10
REQUEST_TIMEOUT_SEC: int = 30
DEFAULT_URL_KEY: str = "cover_url_640"


def build_download_plan(
    max_albums: int = MAX_ALBUMS_PER_ARTIST,
    url_key: str = DEFAULT_URL_KEY,
) -> list[tuple[str, str, str]]:
    """Liefert [(genre, album_id, cover_url), ...], global dedupliziert nach album_id.

    Cap pro Artist: max_albums (newest-first, wie im Cache abgelegt).
    url_key wählt die Auflösung ('cover_url_640' oder 'cover_url_300').
    """
    cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    artists = json.loads(ARTISTS_PATH.read_text(encoding="utf-8"))

    seen: set[str] = set()
    plan: list[tuple[str, str, str]] = []

    for genre, entries in artists.items():
        for entry in entries:
            artist_id = entry.get("id")
            if not artist_id:
                continue
            for album in cache.get(artist_id, [])[:max_albums]:
                album_id = album["album_id"]
                if album_id in seen:
                    continue
                cover_url = album.get(url_key)
                if not cover_url:
                    continue
                seen.add(album_id)
                plan.append((genre, album_id, cover_url))
    return plan


def _download_one(target: Path, url: str, session: requests.Session) -> tuple[bool, str | None]:
    """Download a single cover image to *target*, creating parent dirs as needed.

    Returns ``(True, None)`` on success or ``(False, error_message)`` on failure.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()
    except requests.RequestException as e:
        return False, str(e)
    target.write_bytes(resp.content)
    return True, None


def download_all_covers(
    max_workers: int = MAX_DOWNLOAD_WORKERS,
    max_albums: int = MAX_ALBUMS_PER_ARTIST,
    covers_dir: Path = COVERS_DIR,
    url_key: str = DEFAULT_URL_KEY,
) -> dict:
    """Download all album covers from the Spotify cache to covers_dir.

    Builds a globally-deduplicated download plan (one file per album_id),
    skips already-present non-empty files, and runs parallel downloads via
    a ThreadPoolExecutor.

    Returns a summary dict with keys:
    geplant, uebersprungen, neu_geladen, fehlgeschlagen.
    """
    plan = build_download_plan(max_albums=max_albums, url_key=url_key)

    todo: list[tuple[Path, str, str]] = []
    skipped = 0
    for genre, album_id, url in plan:
        target = covers_dir / genre / f"{album_id}.jpg"
        if target.exists() and target.stat().st_size > 0:
            skipped += 1
            continue
        todo.append((target, url, album_id))

    print(f"Geplant: {len(plan)}  |  schon vorhanden: {skipped}  |  zu laden: {len(todo)}")

    downloaded = 0
    failed: list[tuple[str, str]] = []

    if todo:
        session = requests.Session()
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {
                ex.submit(_download_one, target, url, session): album_id
                for target, url, album_id in todo
            }
            for fut in tqdm(as_completed(futures), total=len(futures), desc="Cover laden"):
                album_id = futures[fut]
                ok, err = fut.result()
                if ok:
                    downloaded += 1
                else:
                    failed.append((album_id, err or "unknown"))

    summary = {
        "geplant": len(plan),
        "uebersprungen": skipped,
        "neu_geladen": downloaded,
        "fehlgeschlagen": len(failed),
    }
    print(summary)
    if failed:
        print(f"Fehler-Beispiele: {failed[:5]}")
    return summary


if __name__ == "__main__":
    download_all_covers()
