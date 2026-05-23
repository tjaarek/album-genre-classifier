"""
Project-wide constants — single source of truth for all magic numbers.

Import from here; never hard-code these values in src/ modules or notebooks.
"""

# ---------------------------------------------------------------------------
# Spotify API pacing
# ---------------------------------------------------------------------------

#: Seconds to sleep between consecutive Spotify API calls.
#: 0.2 s = max 5 calls/s, well within Spotify's 30 s rolling-window limit.
REQUEST_DELAY_SEC: float = 0.2

#: Page size used for artist_albums pagination.
#: Spotify's documented maximum for this endpoint is 20; we use 10 to stay
#: conservative and reduce per-call payload size.
SPOTIFY_ARTIST_ALBUMS_LIMIT: int = 10

#: HTTP status code Spotify returns when we are rate-limited.
RATE_LIMIT_HTTP_STATUS: int = 429

# ---------------------------------------------------------------------------
# Dataset collection caps
# ---------------------------------------------------------------------------

#: Maximum albums to collect per artist.
#: Limits API calls and keeps per-class sample counts balanced.
MAX_ALBUMS_PER_ARTIST: int = 15

#: Minimum album count a Spotify search hit must have before expand_artists
#: auto-fills the ID. Below this threshold the entry is flagged for review.
MIN_ALBUMS_FOR_AUTO_FILL: int = 3

# ---------------------------------------------------------------------------
# Cover download
# ---------------------------------------------------------------------------

#: Maximum parallel download workers (ThreadPoolExecutor).
MAX_DOWNLOAD_WORKERS: int = 10

#: HTTP request timeout in seconds for cover image downloads.
REQUEST_TIMEOUT_SEC: int = 30

