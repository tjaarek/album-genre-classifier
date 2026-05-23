"""
Canonical filesystem paths for the project.

All src/ modules and notebooks should import paths from here instead of
constructing them inline. This guarantees consistency when the directory
structure changes.

Usage::

    from paths import DATA_DIR, CACHE_PATH, COVERS_DIR
"""

from pathlib import Path

#: Absolute path to the repository root (parent of src/).
ROOT: Path = Path(__file__).resolve().parent.parent

#: data/ directory — all generated artifacts live here.
DATA_DIR: Path = ROOT / "data"

#: data/covers/ — one sub-folder per genre, JPEG files named by album_id.
COVERS_DIR: Path = DATA_DIR / "covers"

#: data/splits/ — train.csv, val.csv, test.csv after notebook 02.
SPLITS_DIR: Path = DATA_DIR / "splits"

#: Persistent Spotify API response cache (JSON).
CACHE_PATH: Path = DATA_DIR / "spotify_cache.json"

#: Hand-curated artist roster with Spotify IDs; source of truth for genre labels.
ARTISTS_PATH: Path = ROOT / "src" / "artists.json"
