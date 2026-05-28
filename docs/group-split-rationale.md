# Group-Stratified Split — Rationale & Erkenntnisse

## Was ist ein Group Split?

Der Split in `notebooks/03_split.ipynb` teilt **nicht nach Alben**, sondern nach **Artists**.
Das bedeutet: Alle Alben eines Artists landen immer im gleichen Split (Train, Val oder Test).

### Schritt für Schritt (am Beispiel Reggae)

Gegeben: 19 Artists mit je unterschiedlich vielen Alben.

1. Alle Artists des Genres Reggae werden gesammelt: `["Bob Marley", "Damian Marley", ...]`
2. Diese Artists-Liste wird mit `SEED=42` zufällig gemischt (`rng.shuffle`)
3. 70/15/15 wird auf die **Artists** angewandt — nicht auf die Alben:
   - `n_train = int(0.70 * 19) = 13` Artists → Train
   - `n_val   = int(0.15 * 19) = 2` Artists → Val
   - `n_test  = 4` Artists (Rest) → Test
4. Alle Alben dieser Artists erben den Split ihres Artists

**Invariante:** Kein `artist_id` darf in mehr als einem Split erscheinen.
→ `assert violations == 0` am Ende von Notebook 03.

---

## Warum Group Split? (Der entscheidende Grund)

Album-Cover eines Artists haben oft einen **wiedererkennbaren visuellen Stil**:
- Gleiche Fotografen, Farbpaletten, Schriftarten, Motiv-Sprache
- Das Modell würde sonst nicht "Genre erkennen" lernen, sondern "Artist-Stil erkennen"

**Konkretes Beispiel:**
Wenn Bob Marley-Alben in Train und Test sind, könnte das Modell auf dem Testbild *nicht* sagen "das ist Reggae wegen Farbe/Layout", sondern "das sieht aus wie ein Bob Marley Cover — der ist Reggae". Das wäre ein **Datenleck** (Data Leakage).

→ Beim Group Split muss das Modell generalisieren: Es sieht im Test Artists, die es **nie zuvor gesehen hat**.

---

## Der Trade-off: Nicht exakt 70/15/15

Der Group Split garantiert **keine** exakte Verteilung von 70/15/15 auf Albumebene, weil:
- Artists haben unterschiedlich viele Alben (z.B. 5 vs. 30 Alben)
- 70% der **Artists** ≠ 70% der **Alben**

### Tatsächliche Verteilung nach Split (gesamt)

| Split | Alben | Anteil |
|-------|-------|--------|
| Train | 1.777 | 69,7 % |
| Val   | 359   | 14,1 % |
| Test  | 413   | 16,2 % |

Gesamt: **2.549 Alben** (alle Alben mit vorhandenem Cover auf Disk).

### Per-Genre Aufschlüsselung

| Genre            | Test  | Train | Val |
|------------------|-------|-------|-----|
| alternative_rock |    37 |   174 |  42 |
| classical        |    22 |   150 |  36 |
| country          |    45 |   148 |  29 |
| hiphop           |    35 |   167 |  33 |
| house            |    34 |   205 |  35 |
| indie_rock       |    49 |   185 |  29 |
| jazz             |    44 |   183 |  44 |
| metal            |    42 |   193 |  40 |
| reggae           |    59 |   161 |  29 |
| techno           |    46 |   211 |  42 |

**Ausreißer:**
- Reggae: Test = 59 / 249 = **23,7 %** (überdurchschnittlich)
- Classical: Test = 22 / 208 = **10,6 %** (unterdurchschnittlich)

Ursache: ein prolifer Reggae-Artist mit vielen Alben landete zufällig in Test, ein
Classical-Artist mit vielen Alben in Train.

---

## Vergleich mit naivem Random Split

Ein einfacher Random Split (wie z.B. `train_test_split(df, test_size=0.3)` ohne Gruppen)
würde:
- exakt 70/15/15 auf Albumebene erreichen
- aber Alben desselben Artists auf alle drei Splits verteilen
- → Das Modell "lernt" Artist-Stil statt Genre-Stil
- → Metriken im Test wären **aufgeblasen** und würden bei neuen Artists versagen

Der Group Split ist methodisch korrekter, auch wenn die Albumverteilung nicht exakt 70/15/15 ist.

**Methodische Priorität: Korrektheit > Exaktheit der Verteilung**

---

## Wie das in der Präsentation argumentieren?

1. **"Warum nicht einfach random split?"**
   → Datenleck: Modell würde Artist-Stil lernen, nicht Genre. Das ist nicht das Ziel.
   Bei der Prüfung auf echten neuen Daten (andere Artists) würde das Modell schlechter abschneiden.

2. **"Die Verteilung ist nicht exakt 70/15/15 — ist das ein Problem?"**
   → Nein. Die Abweichung (~±4 Prozentpunkte) ist die Konsequenz der Methode.
   Methodische Korrektheit hat Vorrang vor sauber runden Zahlen.
   Aussage: "Wir akzeptieren leichte Verteilungsasymmetrie, um Artist-Leakage zu verhindern."

3. **"Was bedeutet die Stratifizierung pro Genre?"**
   → Ohne Stratifizierung könnten durch Zufall bestimmte Genres überrepräsentiert im Test sein.
   Mit Genre-Stratifizierung werden Artists **innerhalb jedes Genres** 70/15/15 aufgeteilt.
   Damit ist jedes Genre in allen drei Splits vertreten.

4. **"Wurde die Invariante geprüft?"**
   → Ja: `assert violations == 0` in Notebook 03, Zeile `1f3bb68a`. Ergebnis: 0 Verletzungen.

---

## Code-Referenz

**Notebook:** `notebooks/03_split.ipynb`
**Kernlogik (Zelle `5c866d34`):**

```python
rng = np.random.default_rng(SEED)   # SEED = 42

for genre, group in df.groupby("genre"):
    artists = group["artist_id"].unique().tolist()  # list() wegen rng.shuffle
    rng.shuffle(artists)
    n       = len(artists)
    n_train = int(0.70 * n)
    n_val   = int(0.15 * n)

    train_artists = set(artists[:n_train])
    val_artists   = set(artists[n_train : n_train + n_val])
    test_artists  = set(artists[n_train + n_val :])

    mask = df["genre"] == genre
    df.loc[mask & df["artist_id"].isin(train_artists), "split"] = "train"
    df.loc[mask & df["artist_id"].isin(val_artists),   "split"] = "val"
    df.loc[mask & df["artist_id"].isin(test_artists),  "split"] = "test"
```

**Output-Dateien:**
- `data/splits/train.csv` — 1.777 Alben
- `data/splits/val.csv` — 359 Alben
- `data/splits/test.csv` — 413 Alben

**Spalten:** `album_id | genre | artist_id | cover_path`
`cover_path` ist relativ zum Repo-Root (z.B. `data/covers/reggae/xyz_123.jpg`).

---

## Bekannte Stolpersteine bei der Implementierung

- **`rng.shuffle()` auf pandas StringArray:** Gibt DeprecationWarning.
  Fix: `.unique().tolist()` statt `.unique()` direkt shuffeln.
- **Absolute vs. relative `cover_path`:** Frühere Versionen speicherten absolute Pfade.
  Das ist fragil — bei Umzug des Repos brechen alle Pfade.
  Fix: `str(jpg.relative_to(ROOT))` beim Disk-Scan; `AlbumCoverDataset` unterstützt beide Formate.
- **`AlbumCoverDataset` Pfadauflösung:**
  ```python
  if not cover.is_absolute():
      cover = self.root / cover
  ```
  `self.root` ist der Repo-Root, nicht das Notebook-Verzeichnis.
