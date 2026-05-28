# ResNet-18 Transfer Learning — Rationale & Erkenntnisse

## Was ist ResNet-18?

ResNet-18 ist ein Convolutional Neural Network mit 18 Schichten, das von Microsoft Research
entwickelt wurde. Der Name "ResNet" steht für **Residual Network** — eine Architektur mit
Shortcut-Verbindungen, die das Training tiefer Netze stabil macht.

Entscheidend für dieses Projekt: Es gibt eine öffentliche Version, die auf **ImageNet**
vortrainiert wurde — einem Datensatz mit 1,2 Millionen Fotos aus 1.000 Kategorien
(Tiere, Fahrzeuge, Alltagsgegenstände, etc.).

```python
model = torchvision.models.resnet18(weights="IMAGENET1K_V1")
```

Diese Zeile lädt nicht nur die Architektur, sondern auch ~11 Millionen bereits gelernte
Gewichte. Das Netz "weiß" bereits, wie man visuelle Merkmale erkennt.

---

## Architektur: Backbone vs. Kopf

ResNet-18 besteht aus zwei konzeptionellen Teilen:

```
┌─────────────────────────────────────────────────────────┐
│  BACKBONE (~11M Parameter)                              │
│                                                         │
│  Input (3 × 224 × 224)                                  │
│    → Conv(7×7) + BN + ReLU + MaxPool                    │
│    → Layer 1: 2× Residual Block (64 Filter)             │
│    → Layer 2: 2× Residual Block (128 Filter)            │
│    → Layer 3: 2× Residual Block (256 Filter)            │
│    → Layer 4: 2× Residual Block (512 Filter)            │
│    → Global Average Pooling                             │
│    → 512-dimensionaler Feature-Vektor                   │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  KOPF / FC-LAYER (~5.000 Parameter)                     │
│                                                         │
│  Linear(512 → 1000)  ← original ImageNet               │
│  Linear(512 → 10)    ← ersetzt für dieses Projekt       │
└─────────────────────────────────────────────────────────┘
```

**Für dieses Projekt wird nur der Kopf ersetzt:**
```python
model.fc = nn.Linear(model.fc.in_features, 10)  # 1000 → 10 Genres
```

Der Backbone bleibt zunächst unverändert mit allen vortrainierten Gewichten.

---

## Was steckt in den vortrainierten Gewichten?

Die Gewichte des Backbones codieren eine **Hierarchie visueller Merkmale**:

- **Frühe Schichten (Layer 1–2):** Kanten, Ecken, Farbverläufe, einfache Texturen
- **Mittlere Schichten (Layer 3):** Muster, Gitter, Kurven, Oberflächenstrukturen
- **Tiefe Schichten (Layer 4):** Abstrakte Formen, Teilobjekte, komplexe Texturen

Diese Merkmale sind **generalisierbar** — ein Netz, das gelernt hat, Fellstrukturen von
Hunden zu erkennen, hat dabei auch gelernt, Texturen generell zu erkennen. Das ist für
Album-Cover direkt nützlich: Schrift, Farbflächen, grafische Muster, Fotografien.

---

## Warum Transfer Learning bei ~1.800 Trainingsbildern?

Ein Scratch CNN muss alle visuellen Merkmale **von Null** lernen. Bei 1.784
Trainingsbildern reicht das kaum:

| Modell | Val-Accuracy (Group Split) | Parameter |
|---|---|---|
| Scratch CNN | 0.226 | 391.946 |
| ResNet-18 | 0.326 | 11.181.642 |

ResNet-18 hat **28× mehr Parameter**, aber trotzdem bessere Generalisierung — weil der
Backbone nicht neu gelernt werden muss. Nur der Kopf (~5.000 Parameter) wird in
Phase 1 von Grund auf trainiert.

**Faustregel:** Transfer Learning lohnt sich immer, wenn der eigene Datensatz kleiner
als ~10.000 Bilder ist und die Quelldomäne (ImageNet: natürliche Fotos) nicht zu weit
von der Zieldomäne (Album-Cover: Fotografien + Grafik) entfernt ist.

---

## Two-Phase Training — Warum zwei Phasen?

### Das Problem: Catastrophic Forgetting

Der neue FC-Layer ist zufällig initialisiert. Seine Gradienten sind anfangs groß und
chaotisch. Wenn der Backbone in dieser Phase mittrainieren würde, würden diese Gradienten
die sorgfältig gelernten ImageNet-Features **überschreiben**. Dieses Phänomen heißt
**Catastrophic Forgetting**.

### Phase 1 — Backbone eingefroren (5 Epochs, lr=1e-3)

```python
for name, param in model.named_parameters():
    param.requires_grad = name.startswith("fc.")
```

Nur der FC-Layer kann sich verändern. Der Backbone ist eingefroren.

Was passiert:
1. Albumcover laufen durch den unveränderlichen Backbone → 512-Vektor
2. Der FC lernt: "Wie ordne ich diesen 512-Vektor einem Genre zu?"
3. Nach 5 Epochs hat der FC eine stabile Startposition

Ergebnis nach Phase 1 (Group Split): **val_acc = 0.262**

### Phase 2 — Fine-tuning aller Layer (bis 35 Epochs, lr=1e-4)

```python
for param in model.parameters():
    param.requires_grad = True
optimizer_p2 = torch.optim.Adam(model.parameters(), lr=1e-4)
```

Jetzt dürfen sich alle ~11M Parameter bewegen — aber mit **sehr kleiner Lernrate**.

Was passiert:
1. Der Backbone, der "Hunde und Löffel" gelernt hat, wird sanft in Richtung
   "Metal-Cover vs. Jazz-Cover" gebogen
2. Die ImageNet-Features bleiben weitgehend erhalten, werden aber leicht spezialisiert
3. Early Stopping beendet das Training, sobald Val-Accuracy nicht mehr steigt

**Warum lr=1e-4 (10× kleiner als Phase 1)?**
- Große LR würde die Backbone-Gewichte zu stark verändern → vortrainiertes Wissen geht verloren
- Kleine LR = sanfte Anpassung, nur genre-relevante Merkmale werden stärker gewichtet

---

## Overfitting in Phase 2 — erwartetes Verhalten

Beim Group Split (1.777 Trainingsbilder) war Phase 2 von starkem Overfitting geprägt:

```
Epoch  6: train_acc=0.436  val_acc=0.326  ✓ (bester Checkpoint)
Epoch  7: train_acc=0.696  val_acc=0.318
Epoch  8: train_acc=0.833  val_acc=0.306
...
Epoch 13: train_acc=0.994  val_acc=0.298  → Early Stopping
```

Train-Accuracy springt auf 99,4 % während Val bei ~30 % stagniert. Das ist kein Bug,
sondern das erwartete Verhalten bei einem 11M-Parameter-Netz auf 1.784 Bildern.

**Für die Präsentation:** Dieser Train/Val-Gap ist selbst methodisch interessantes Material.
Er zeigt, dass der Datensatz zu klein für volles Fine-tuning ist und begründet, warum
Early Stopping (Patience=7) unerlässlich ist.

**Mögliche Gegenmaßnahmen (nicht implementiert, aber argumentierbar):**
- Stärkere Augmentation (CutOut, MixUp)
- Layer-weise LR-Staffelung (tiefe Layer noch kleinere LR)
- Nur letzte 1-2 Residual Blocks auftauen statt alle
- Mehr Daten sammeln

---

## Vergleich: Random Split vs. Group Split

Beim Random Split erscheinen die Metriken besser — das ist das Datenleck:

| Split | Scratch CNN Val-Acc | ResNet-18 Val-Acc |
|---|---|---|
| Group Split | 0.226 | 0.326 |
| Random Split | 0.277 | (noch ausstehend) |

Der Random Split lässt Alben desselben Artists in Train und Val — das Modell lernt
Artist-Stil mit. Die höhere Val-Accuracy reflektiert daher nicht bessere Generalisierung,
sondern bessere Memorisierung des Trainingssets.

---

## Zusammenfassung: Warum dieser Ansatz für die Präsentation stark ist

1. **Methodisch begründbar:** Transfer Learning ist State-of-the-Art für kleine Datensätze.
   Die Entscheidung ist nicht willkürlich, sondern folgt der Literatur.

2. **Two-Phase Training ist erklärbar:** Phase 1 verhindert Catastrophic Forgetting,
   Phase 2 erlaubt domänenspezifische Anpassung. Beides ist mit konkreten Zahlen belegbar.

3. **Direkter Vergleich mit Scratch CNN** quantifiziert den Mehrwert von Pretraining:
   +10 Prozentpunkte Val-Accuracy trotz 28× mehr Parameter.

4. **Overfitting in Phase 2** ist Diskussionsmaterial, kein Versagen:
   Es illustriert die Grenzen von Fine-tuning bei kleinen Datensätzen und begründet
   Early Stopping als notwendige Regularisierung.

---

## Code-Referenz

**Notebook:** `notebooks/05_resnet.ipynb`

| Zelle | Inhalt |
|---|---|
| `cell-3` | Konfiguration (LR, Epochs, Patience) |
| `cell-7` | Modell laden + FC ersetzen |
| `cell-11` | Phase 1 Training |
| `cell-13` | Phase 2 Fine-tuning mit Early Stopping |
| `cell-15` | Loss/Accuracy-Kurven (beide Phasen) |
