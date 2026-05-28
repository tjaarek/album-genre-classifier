# Evaluation — Ergebnisse & Analyse

## Überblick

Zwei Modelle wurden auf demselben Datensatz (2.549 Album-Cover, 10 Genres) trainiert
und auf dem Test-Set evaluiert. Alle Ergebnisse beziehen sich auf den **Random Split**
(`data/splits/random/`) — zum Vergleich sind die Group-Split-Werte angegeben.

---

## Vergleichstabelle

| Modell | Test-Accuracy | Val-Accuracy | Parameter | Best Epoch |
|---|---|---|---|---|
| Scratch CNN | 0.261 | 0.277 | 391.946 | 19 |
| ResNet-18 | **0.428** | **0.448** | 11.181.642 | 20 |

**Transfer Learning bringt +16,7 Prozentpunkte Test-Accuracy** gegenüber dem Scratch CNN.

---

## Per-Klasse: ResNet-18

| Genre | Precision | Recall | F1 | Support |
|---|---|---|---|---|
| classical | 0.692 | 0.581 | **0.632** | 31 |
| country | 0.564 | 0.667 | **0.611** | 33 |
| reggae | 0.647 | 0.595 | **0.620** | 37 |
| metal | 0.585 | 0.571 | **0.578** | 42 |
| jazz | 0.513 | 0.488 | 0.500 | 41 |
| techno | 0.357 | 0.333 | 0.345 | 45 |
| hiphop | 0.306 | 0.314 | 0.310 | 35 |
| house | 0.250 | 0.317 | 0.280 | 41 |
| indie_rock | 0.310 | 0.225 | **0.261** | 40 |
| alternative_rock | 0.222 | 0.263 | **0.241** | 38 |

Makro-Avg F1: **0.438**

---

## Per-Klasse: Scratch CNN

| Genre | Precision | Recall | F1 | Support |
|---|---|---|---|---|
| metal | 0.369 | 0.571 | **0.449** | 42 |
| reggae | 0.292 | 0.568 | **0.385** | 37 |
| classical | 0.318 | 0.452 | 0.373 | 31 |
| hiphop | 0.295 | 0.371 | 0.329 | 35 |
| jazz | 0.189 | 0.171 | 0.179 | 41 |
| country | 0.190 | 0.242 | 0.213 | 33 |
| techno | 0.233 | 0.156 | 0.187 | 45 |
| indie_rock | 0.182 | 0.050 | 0.078 | 40 |
| alternative_rock | 0.125 | 0.053 | 0.074 | 38 |
| house | 0.091 | 0.049 | **0.063** | 41 |

Makro-Avg F1: **0.233**

---

## Muster: Was wird gut erkannt — und warum?

### Gut klassifizierbare Genres (ResNet F1 > 0.5)

**Classical (0.632), Country (0.611), Reggae (0.620), Metal (0.578)**

Diese Genres haben eine **starke, konsistente visuelle Identität**:
- Classical: schwarz/weiß Portraits, ornamentale Typografie, formale Ästhetik
- Country: Americana-Motive, Landschaften, erdige Farbtöne
- Reggae: kräftige Farben (Rot/Gelb/Grün), Rasta-Symbolik
- Metal: düstere, kontrastreiche Cover, symbolbeladene Grafiken

Das Modell lernt genre-typische visuelle Muster — nicht nur einzelne Artists.

### Schlecht klassifizierbare Genres (ResNet F1 < 0.3)

**Alternative Rock (0.241), House (0.280), Indie Rock (0.261)**

Diese Genres definieren sich primär durch **Klang, nicht durch Optik**:
- Alternative Rock und Indie Rock: extrem diverse Cover-Stile, keine einheitliche Ästhetik.
  Beide Genres überschneiden sich visuell stark → häufige gegenseitige Verwechslung.
- House: ähnliche Minimalismus-/Club-Ästhetik wie Techno, Grenze fließend

**Kernaussage:** Die Klassifizierbarkeit eines Genres korreliert direkt mit
der Stärke seiner visuellen Identität — nicht mit seiner musikalischen Eigenheit.

---

## Trainingsverhalten ResNet-18

### Phase 1 (Epochen 1–5, Backbone eingefroren)
Train- und Val-Kurven laufen parallel — kein Overfitting, da nur der kleine
FC-Layer (~5.000 Parameter) trainiert wird.
- Val-Accuracy nach Phase 1: **0.314**

### Phase 2 (Epochen 6–27, alle Layer, lr=1e-4)
Sofort starkes Overfitting-Signal:
```
Epoch  6: train_acc=0.443  val_acc=0.387  ✓
Epoch  7: train_acc=0.675  val_acc=0.406  ✓
Epoch 10: train_acc=0.966  val_acc=0.411
Epoch 13: train_acc=0.995  val_acc=0.437  ✓
Epoch 20: train_acc=0.998  val_acc=0.448  ✓ (bester Checkpoint)
Epoch 27: Early Stopping
```

**Paradoxon:** Val-Loss steigt ab Epoch 7 kontinuierlich, Val-Accuracy verbessert
sich trotzdem noch bis Epoch 20. Das Modell wird schlechter kalibriert (unsichere
Klassen werden extremer), aber die Rangordnung der Vorhersagen verbessert sich noch.

Ab Epoch 20 stagniert auch die Val-Accuracy → Early Stopping (Patience=7) greift.

### Fazit Trainingsverhalten
Das 11M-Parameter-Netz ist für 1.784 Trainingsbilder zu groß für stabiles Fine-tuning.
Early Stopping ist keine optionale Verbesserung, sondern **notwendige Regularisierung**.
Ohne Early Stopping würde der Checkpoint bei Epoch 6 oder 7 am besten abschneiden.

---

## Group Split vs. Random Split — Der Datenleck-Effekt in Zahlen

| | Group Split | Random Split | Δ |
|---|---|---|---|
| Scratch CNN Val-Acc | 0.226 | 0.277 | +0.051 |
| Scratch CNN Test-Acc | 0.240 | 0.261 | +0.021 |
| ResNet-18 Val-Acc | 0.326 | 0.448 | **+0.122** |
| ResNet-18 Test-Acc | 0.363 | 0.428 | **+0.065** |

**ResNet profitiert stärker vom Datenleck als das Scratch CNN.** Erklärung:
Der vortrainierte Backbone extrahiert reichhaltigere Features — er kann Artist-Stil
(Farbpalette, Fotograf, Label-Design) präziser codieren als das schwächere Scratch CNN.
Wenn dieser Artist-Stil im Val-Set bekannt ist (Random Split), steigt die Accuracy
entsprechend stärker.

Das belegt die Notwendigkeit des Group Splits für valide Generalisierungsaussagen.

---

## Drei zentrale Aussagen mit Zahlenbelegung

1. **Transfer Learning lohnt sich bei kleinen Datensätzen:**
   ResNet-18 erreicht 42,8 % Test-Accuracy vs. 26,1 % beim Scratch CNN — trotz
   28× mehr Parametern zeigt es bessere Generalisierung dank vortrainierter Features.

2. **Die Split-Methode beeinflusst Metriken substanziell:**
   Random Split gibt ResNet künstlich +6,5 Prozentpunkte. Der Group Split liefert
   die methodisch korrekte, konservativere Schätzung der echten Generalisierung.

3. **Visuelle Genre-Identität bestimmt die Klassifizierbarkeit:**
   Classical/Country/Reggae (F1 > 0.6) vs. Alternative Rock/Indie Rock (F1 < 0.25).
   Das Modell lernt Genre-Ästhetik — Genres ohne konsistente Ästhetik sind schwer
   trennbar, unabhängig vom Modell.

---

## Fehleranalyse

ResNet-18 macht **219 von 383 Test-Fehlern**.

Erwartete Verwechslungen (aus Confusion Matrix):
- **Alternative Rock ↔ Indie Rock:** Visuell kaum unterscheidbar, auch für Menschen
- **House ↔ Techno:** Beide nutzen abstrakte, minimalistische Club-Ästhetik
- **Jazz ↔ Classical:** Beide verwenden häufig schwarz/weiß Portraits, formale Typografie

Diese Verwechslungen sind **genre-inhärent** — kein Modell kann ohne zusätzliche
Metadaten (Label, Künstler, Erscheinungsjahr) zuverlässig unterscheiden, was
visuell nicht unterscheidbar ist.

---

## Code-Referenz

| Notebook | Inhalt |
|---|---|
| `notebooks/04_scratch_cnn.ipynb` | Scratch CNN Training (Random Split) |
| `notebooks/05_resnet.ipynb` | ResNet-18 Fine-tuning (Random Split) |
| `notebooks/06_evaluation.ipynb` | Test-Evaluation, Confusion Matrices, Fehleranalyse |
| `data/splits/random/` | Train/Val/Test CSVs (Random Split) |
| `data/splits/` | Train/Val/Test CSVs (Group Split) |
| `data/checkpoints/scratch_best.pt` | Bester Scratch CNN Checkpoint |
| `data/checkpoints/resnet18_best.pt` | Bester ResNet-18 Checkpoint |
