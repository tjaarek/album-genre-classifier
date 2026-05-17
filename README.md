# album-genre-classifier

Academic project for a Deep Learning course at Hochschule Flensburg.
A CNN that predicts the music genre of an album from its cover image.

## Approach

- 10 genres: `metal`, `classical`, `hiphop`, `jazz`, `country`, `reggae`, `indie_rock`, `alternative_rock`, `techno`, `house`
- Dataset is curated: a hand-picked list of 200 artists (20 per genre, including German acts) drives the scrape. Each artist's albums are pulled from the Spotify Web API; the 640px cover image is the input.
- Ground-truth label = the genre bucket the artist was placed in. We do not rely on Spotify's genre tags (they are unreliable / often empty under the Client-Credentials Flow).

## Project structure

```
album-genre-classifier/
├── pyproject.toml             # uv project, Python >=3.12
├── uv.lock
├── src/
│   ├── artists.json           # curated genre → [{name, id}] mapping (the ground truth)
│   ├── spotify_scraper.py     # Spotify Web API client w/ disk cache + rate-limit handling
│   └── cover_downloader.py    # downloads 640px covers, resume-safe, threaded
├── notebooks/
│   └── 01_data_collection.ipynb   # runs the full pipeline end-to-end
└── data/                      # gitignored (reproducible from the scraper)
    ├── covers/{genre}/{album_id}.jpg
    ├── spotify_cache.json     # per-artist album metadata cache
    ├── albums_raw.csv         # all scraped albums (snapshot, tracked in git)
    └── artists_raw.csv        # resolved artists (snapshot, tracked in git)
```

## Setup

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
uv sync
```

Create a `.env` in the project root with your Spotify Developer credentials
(register an app at <https://developer.spotify.com>):

```
CLIENT_ID=your_spotify_client_id
CLIENT_SECRET=your_spotify_client_secret
```

## Reproducing the dataset

Open Jupyter and run `notebooks/01_data_collection.ipynb` top to bottom:

```bash
uv run jupyter lab
```

The notebook will:
1. Load `src/artists.json`
2. Resolve each artist's recent albums via the Spotify API (cached to
   `data/spotify_cache.json` — re-runs cost zero API calls for already-seen artists)
3. Download every cover to `data/covers/{genre}/{album_id}.jpg` (resume-safe;
   skips existing files)
4. Print per-genre counts and a sample grid

Notes on the Spotify API (as of late 2024):
- Editorial / algorithmic playlists return 404 for client-credentials apps.
- Artist endpoints no longer return `genres` or `followers`.
- `artist_albums` is limited to `limit=20` per request.
The scraper works around these limits.

## Status

- [x] Phase 0: hygiene, repo setup
- [x] Phase 1: data collection (2.125 covers across 10 genres)
- [ ] Phase 2: data exploration + train/val/test split (group-split by artist)
- [ ] Phase 3: PyTorch dataset + baseline model
- [ ] Phase 4: hyperparameter study
- [ ] Phase 5: evaluation (confusion matrix, per-class metrics)
- [ ] Phase 6: slides + video (deadline 2026-06-16)
