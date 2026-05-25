# Projektdokumentation — Album Genre Classifier

> Hochschule Flensburg · Deep Learning · Deadline: 2026-06-16  
> Ziel: CNN, das das Genre eines Albums anhand des Cover-Bildes vorhersagt.

---

## Überblick

Das Modell soll 10 Genres unterscheiden:

| Genre | Charakteristik (Coverstil) |
|---|---|
| `metal` | Düstere Farben, aggressive Typografie, Fantasy/Horror-Motive |
| `classical` | Porträts, Konzertfotos, ornamentale Designs |
| `hiphop` | Straßenkultur, Portraits, starke Typografie |
| `jazz` | Schwarz-weiß Fotos, Porträts, abstrakte Grafik |
| `country` | Landschaften, Americana, warme Farbtöne |
| `reggae` | Rastafari-Farben (Rot/Gelb/Grün), Portraits |
| `indie_rock` | Abstrakt, minimalistisch, fotografisch |
| `alternative_rock` | Divers – von Grunge bis Art-Rock |
| `techno` | Monochrom, abstrakt, futuristisch |
| `house` | Farbig, abstrakt, Club-Ästhetik |

**Wichtige Designentscheidung:** Wir vertrauen *nicht* den Spotify-Genre-Tags — sie sind für Client-Credentials-Apps oft leer oder unzuverlässig. Das Label kommt ausschließlich aus dem Bucket, in dem ein Artist in `src/artists.json` liegt.

---

## Abgeschlossene Phasen

### Phase 1 — Datenbeschaffung

#### 1.1 Artistliste kuratieren (`src/artists.json`)

Die Datei ist die **einzige Ground-Truth-Quelle** für Genre-Labels. Sie enthält je Genre 19–46 handverlesene Artists mit Spotify-IDs.

**Iterationen:**
- Start: ~200 Artists (20 pro Genre)
- Erweiterung auf **260 Artists** (mehr Techno, House, Classical, Indie-Rock)
- Korrektur: 8 Artists hinzugefügt (WizTheMc, HUGEL, Keinemusik, AVAION, Jonas Blue, MoBlack, Maz, Conrad Taylor)
- Bereinigung: 10 Artists entfernt (Garth Brooks, The Mighty Diamonds, Len Faki, Conrad Taylor, Fisher, MK, Frankey & Sandrino, HUGEL, MoBlack, Maz) — Duplikate oder schlechte Repräsentanz
- **Endstand: 258 Artists**

**Null-ID-Auflösung:** `src/expand_artists.py` löst fehlende Spotify-IDs automatisch per Name-Suche auf (fuzzy match + Album-Count-Schwelle ≥ 3). Manuell nachzupflegende Einträge werden ausgegeben.

#### 1.2 Album-Metadaten scrapen (`src/spotify_scraper.py`)

- Max. 15 Alben pro Artist (konfigurierbar via `config.MAX_ALBUMS_PER_ARTIST`)
- Nur `album_type='album'` — keine Singles, EPs, Compilations
- Spotify gibt Alben neueste-zuerst zurück → wir kriegen die 15 aktuellsten
- Ergebnis: `data/albums_raw.csv` (2.917 Zeilen)
- Ergebnis: `data/artists_raw.csv` (258 Zeilen, identisch zu `src/artists.json`)

**Cache-System (`data/spotify_cache.json`):**  
Jede Artist-Antwort wird lokal gecacht. Re-Runs kosten null API-Calls für bereits gesehene Artists. Das ist kritisch weil Spotify ein undokumentiertes tägliches Limit hat, das ohne `Retry-After`-Header trifft.

#### 1.3 Cover herunterladen (`src/cover_downloader.py`)

- Lädt 640px-Versionen von Spotifys CDN (`i.scdn.co`) — kein Auth nötig
- Parallele Downloads via `ThreadPoolExecutor` (10 Worker)
- Resume-sicher: vorhandene, nicht-leere Dateien werden übersprungen
- Globale Deduplizierung nach `album_id` — bei Kollaborationen über Genre-Grenzen gewinnt das erste Genre in `artists.json`

**Dateinamenschema:** Ursprünglich `{album_id}.jpg` → umbennant auf `{artist_name}_{album_id}.jpg` für bessere menschliche Lesbarkeit. Die Umbenennung nutzte Unicode-Sanitisierung (NFD-Normalisierung + manuelle Map für nicht-zerlegbare Zeichen: ø→o, æ→ae, ð→d, þ→th, ß→ss, &→and).

#### 1.4 pHash-Deduplication (`notebooks/02_phash_dedup.ipynb`)

Album-Cover-Duplikate entstehen wenn ein Artist dasselbe Artwork für mehrere Releases verwendet (z.B. „Album", „Deluxe Edition", „Remaster"). Diese würden im Trainings-Set als exakt gleiche Bilder auftauchen → Informationsleck zwischen Train und Val/Test wenn zufällig gesplittet wird.

**Algorithmus:**
1. pHash (Perceptual Hash, 64-bit DCT-basiert) für alle Cover berechnen
2. Innerhalb einer Artist-Gruppe: Paare mit Hamming-Distanz ≤ 5 als visuell identisch markieren
3. **Connected Components (BFS):** Transitive Ketten auflösen — wenn A↔B und B↔C, landen A, B, C im selben Cluster
4. Pro Cluster: ältestes Release behalten, alle anderen löschen

**Warum Connected Components?** Der ursprüngliche paarweise Ansatz hatte ein Transitivitätsproblem: B war gleichzeitig als „behalten" (wegen A↔B) und als „löschen" (wegen B↔C) markiert → 10 Konflikte. BFS löst das sauber.

**Ergebnis:** 10 Duplikate gelöscht.

---

### Phase 2 — Code-Qualität & Infrastruktur

#### Refactoring (abgeschlossen)

| Problem | Lösung |
|---|---|
| `_ROOT = Path(...)` in 3 Modulen dupliziert | `src/paths.py` — zentrale Pfad-Definitionen |
| 14+ Magic Numbers in 3 Dateien | `src/config.py` — alle Konstanten an einem Ort |
| Fehlende Docstrings | Nachgepflegt in allen `src/`-Modulen |
| Kein Linting-Setup | `ruff` + `mypy` in `pyproject.toml` |

**`src/config.py` Konstanten:**
```python
REQUEST_DELAY_SEC = 0.2        # max 5 Calls/s, unter Spotifys 30s-Window
SPOTIFY_ARTIST_ALBUMS_LIMIT = 10  # konservativ (Doku-Max wäre 20)
RATE_LIMIT_HTTP_STATUS = 429
MAX_ALBUMS_PER_ARTIST = 15
MIN_ALBUMS_FOR_AUTO_FILL = 3   # expand_artists.py: Schwelle für Auto-Fill
MAX_DOWNLOAD_WORKERS = 10
REQUEST_TIMEOUT_SEC = 30
```

**`src/paths.py`:**
```python
ROOT → REPO_ROOT
DATA_DIR → data/
COVERS_DIR → data/covers/
SPLITS_DIR → data/splits/
CACHE_PATH → data/spotify_cache.json
ARTISTS_PATH → src/artists.json
```

#### Linting-Ergebnisse

Ruff + mypy liefen auf `src/` durch. Gefundene und behobene Issues:
- `UP035` — `from typing import Iterable` → `from collections.abc import Iterable`
- `UP045` — `Optional[X]` → `X | None`
- `ANN204` — fehlendes `-> None` bei `__init__`
- `I001` — unsortierte Imports (automatisch gefixed via `ruff --fix`)
- mypy: `int()` Cast für Any-Return aus `resp.get("total", 0)`

---

## Aktueller Dataset-Stand

### Cover pro Genre

| Genre | Covers |
|---|---|
| techno | 299 |
| metal | 275 |
| house | 274 |
| jazz | 271 |
| indie_rock | 263 |
| alternative_rock | 253 |
| reggae | 249 |
| hiphop | 235 |
| country | 222 |
| classical | 208 |
| **Gesamt** | **2.549** |

**Imbalance-Ratio:** 1,44× (Min 208, Max 299) — gilt als mild. Empfohlene Behandlung beim Training: Class Weights (`nn.CrossEntropyLoss(weight=...)`) + Data Augmentation für unterrepräsentierte Klassen.

### Dateien

| Datei | Status | Beschreibung |
|---|---|---|
| `src/artists.json` | ✅ tracked | 258 Artists, 10 Genres — Ground Truth |
| `data/artists_raw.csv` | ✅ tracked | Identisch zu artists.json (258 Zeilen) |
| `data/albums_raw.csv` | ✅ tracked | 2.917 Album-Metadaten-Zeilen |
| `data/covers/` | 🚫 gitignored | 2.549 JPEGs, reproduzierbar |
| `data/spotify_cache.json` | 🚫 gitignored | API-Cache, reproduzierbar |

---

## Bekannte Schwierigkeiten & Lösungen

### Spotify API-Einschränkungen (seit Ende 2024)

Spotify hat die Web API für nicht-extended-quota-Apps drastisch eingeschränkt:

| Problem | Impact | Lösung |
|---|---|---|
| Editorial-Playlists → 404 | Keine Playlist-basierten Datasets | Artists-basierter Ansatz gewählt |
| `genres` + `followers` nicht mehr im Artist-Endpoint | Kein Vertrauen in Spotify-Tags | Labels kommen aus `artists.json` |
| `artist_albums` Limit max 20 (war 50) | Langsamer | `SPOTIFY_ARTIST_ALBUMS_LIMIT = 10` (konservativ) |
| Tägliches Limit ohne `Retry-After` | Stundenlange Hänger möglich | `status_retries=0` in spotipy, Fehler-Handling per Artist |
| Kein `Retry-After`-Header beim Daily Cap | Keine automatische Wiederholung | Cache-System: unterbrochene Scrapes werden beim nächsten Run fortgesetzt |

### Dateiname-Unicode-Probleme

Künstlernamen wie „Trentemøller", „Motörhead", „Frédéric Chopin" enthielten Sonderzeichen, die für Dateisysteme problematisch sind. 

**Lösung:** Zweistufige Sanitisierung:
1. Manuelle Map für nicht-zerlegbare Zeichen (ø, æ, ð, þ, ß, &)
2. NFD-Normalisierung + Strip aller Non-ASCII-Zeichen

### pHash-Transitivitätskonflikte

Beim ersten Durchlauf des Dedup-Notebooks (paarweiser Vergleich):
```
WARNUNG: 10 Pfade sind sowohl keep als auch dupe — übersprungen
```

**Ursache:** A ist ähnlich zu B, und B ist ähnlich zu C — aber A und C sind es nicht direkt. Paarweiser Vergleich kann B nicht konsistent behandeln.

**Lösung:** Umstieg auf BFS-basierte Connected Components: alle transitiv zusammenhängenden Cover landen in einem Cluster, nur das älteste Release wird behalten. Keine Konflikte mehr möglich.

---

## Architektur-Konventionen

### Notebooks orchestrieren, `src/` macht die Arbeit

Schwere Logik (API-Calls, Downloads, Hashing) liegt in `src/`-Modulen. Notebooks sind dünne Orchestrierungsschichten, die ein Dozent von oben nach unten lesen kann.

### Disk ist Ground Truth für Cover

`data/covers/` ist maßgeblich für was existiert. CSVs können nach einem abgebrochenen Scrape hinter dem Disk-Stand liegen. Downstream-Code (Dedup-Notebook) liest `data/covers/` direkt und joined Metadaten aus der CSV.

### Bare Imports (keine relativen Imports)

```python
from config import MAX_ALBUMS_PER_ARTIST  # ✅
from .config import MAX_ALBUMS_PER_ARTIST  # ❌
```

Module laufen sowohl als `uv run python src/foo.py` als auch via `sys.path.insert` aus Notebooks — relative Imports würden im Script-Modus brechen.

---

## Offene Punkte / Nächste Schritte

- [ ] **Notebook 03 — Cleaning & Split:** `albums_raw.csv` filtern (Regex: deluxe/remaster/live/anniversary/edition), nur Alben mit Disk-Cover behalten, Group-stratifizierten Split 70/15/15 nach `artist_id` durchführen → `albums_clean.csv` + `splits/{train,val,test}.csv`
- [ ] **Notebook 04 — PyTorch Dataset:** `AlbumCoverDataset`, DataLoader, Transforms inkl. Augmentation
- [ ] **Notebook 05 — Baseline-Modell:** Transfer Learning (z.B. ResNet-18 pretrained), Training, erste Metriken
- [ ] **Notebook 06 — Hyperparameter-Studie:** Learning Rate, Batch Size, Augmentation-Stärke
- [ ] **Notebook 07 — Evaluation:** Confusion Matrix, per-class Precision/Recall/F1, Fehleranalyse
- [ ] **Präsentation:** Video-Deadline 2026-06-16

---

## Reproduzierbarkeit

```bash
# 1. Dependencies installieren
uv sync

# 2. .env mit Spotify-Credentials anlegen
echo "CLIENT_ID=..." > .env
echo "CLIENT_SECRET=..." >> .env

# 3. Cover scrapen
uv run jupyter nbconvert --to notebook --execute --inplace notebooks/01_data_collection.ipynb

# 4. Duplikate entfernen
uv run jupyter nbconvert --to notebook --execute --inplace notebooks/02_phash_dedup.ipynb

# 5. Linting
uv run ruff check src/
uv run mypy src/
```
