"""
One-shot helper: resolve null-ID entries in src/artists.json via Spotify search.

For each entry where ``id`` is null:
  1. Search Spotify (top-3 candidates).
  2. For the top hit, fetch the count of ``album_type='album'`` releases.
  3. Auto-fill the ID if the top hit looks safe (high name similarity AND
     >= MIN_ALBUMS releases). Otherwise leave the ID null and flag for review.
  4. Write the updated JSON back.

Run:  uv run python src/expand_artists.py
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import spotipy

from spotify_scraper import get_spotify_client

_ROOT = Path(__file__).resolve().parent.parent
_ARTISTS_PATH = _ROOT / "src" / "artists.json"
_REQUEST_DELAY_SEC = 0.2
MIN_ALBUMS = 3  # below this, flag for manual review even on a name match


def _normalize(name: str) -> str:
    return re.sub(r"[^\w\s]", "", name).strip().lower()


def _album_count(sp: spotipy.Spotify, artist_id: str) -> int:
    """Total ``album_type='album'`` releases (Spotify reports it as 'total')."""
    resp = sp.artist_albums(artist_id, album_type="album", limit=1)
    time.sleep(_REQUEST_DELAY_SEC)
    return resp.get("total", 0)


def _search_top3(sp: spotipy.Spotify, name: str) -> list[dict]:
    results = sp.search(q=name, type="artist", limit=3)
    time.sleep(_REQUEST_DELAY_SEC)
    return results.get("artists", {}).get("items", [])


def expand() -> None:
    data = json.loads(_ARTISTS_PATH.read_text(encoding="utf-8"))
    sp = get_spotify_client()

    auto_filled: list[tuple[str, str, str, int]] = []   # (genre, name, found, n_alb)
    needs_review: list[tuple[str, str, list[dict]]] = []  # (genre, name, candidates)

    for genre, entries in data.items():
        for entry in entries:
            if entry.get("id"):
                continue
            name = entry["name"]
            candidates = _search_top3(sp, name)
            if not candidates:
                needs_review.append((genre, name, []))
                continue

            top = candidates[0]
            n_albums = _album_count(sp, top["id"])
            name_match = _normalize(top["name"]) == _normalize(name)

            if name_match and n_albums >= MIN_ALBUMS:
                entry["id"] = top["id"]
                auto_filled.append((genre, name, top["name"], n_albums))
            else:
                # Keep id=null; surface for manual fix
                enriched = []
                for c in candidates:
                    enriched.append({
                        "name": c["name"],
                        "id": c["id"],
                        "albums": _album_count(sp, c["id"]),
                    })
                needs_review.append((genre, name, enriched))

    _ARTISTS_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"\nAuto-filled {len(auto_filled)} entries:")
    for genre, name, found, n in auto_filled:
        print(f"  [{genre:<16s}] {name:<25s} -> {found:<25s} ({n} albums)")

    if needs_review:
        print(f"\nNeeds manual review ({len(needs_review)}):")
        for genre, name, cands in needs_review:
            print(f"\n  [{genre}] {name}")
            if not cands:
                print("    (no search results)")
                continue
            for i, c in enumerate(cands):
                print(f"    [{i}] {c['name']:<35s} id={c['id']}  albums={c['albums']}")
        print(
            "\nTo fix: pick the right ID for each entry above and paste it into "
            "src/artists.json (replace the null)."
        )
    else:
        print("\nAll candidates auto-filled — no manual review needed.")


if __name__ == "__main__":
    expand()
