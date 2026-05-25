"""
AlbumCoverDataset — PyTorch Dataset für Album-Cover-Klassifikation.

Importierbar aus allen Notebooks via:
    import sys; sys.path.insert(0, str(ROOT / "src"))
    from dataset import AlbumCoverDataset, build_transforms, compute_class_weights
"""

from pathlib import Path

import pandas as pd
import torch
from PIL import Image
from torch import Tensor
from torch.utils.data import Dataset
from torchvision import transforms

# ---------------------------------------------------------------------------
# Genre-Mapping — alphabetisch sortiert, damit GENRE_TO_IDX stabil bleibt.
# ---------------------------------------------------------------------------

GENRES: list[str] = [
    "alternative_rock",
    "classical",
    "country",
    "hiphop",
    "house",
    "indie_rock",
    "jazz",
    "metal",
    "reggae",
    "techno",
]
GENRE_TO_IDX: dict[str, int] = {g: i for i, g in enumerate(GENRES)}
IDX_TO_GENRE: dict[int, str] = {i: g for g, i in GENRE_TO_IDX.items()}

# ImageNet normalisation constants (used for both scratch and ResNet).
_IMAGENET_MEAN = [0.485, 0.456, 0.406]
_IMAGENET_STD  = [0.229, 0.224, 0.225]


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------

def build_transforms(split: str) -> transforms.Compose:
    """Return the transform pipeline for *split* ('train', 'val', or 'test').

    Train applies data augmentation; val/test use deterministic centre-crop.
    """
    if split == "train":
        return transforms.Compose([
            transforms.Resize(256),
            transforms.RandomCrop(224),
            transforms.RandomHorizontalFlip(),
            transforms.ColorJitter(brightness=0.2, contrast=0.2, saturation=0.2),
            transforms.ToTensor(),
            transforms.Normalize(_IMAGENET_MEAN, _IMAGENET_STD),
        ])
    return transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(_IMAGENET_MEAN, _IMAGENET_STD),
    ])


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

_REPO_ROOT: Path = Path(__file__).resolve().parent.parent


class AlbumCoverDataset(Dataset):
    """Dataset backed by one of the split CSVs in data/splits/.

    Args:
        csv_path: Path to train.csv / val.csv / test.csv.
        split:    One of 'train', 'val', 'test' — controls augmentation.
        root:     Repo root used to resolve relative cover_path entries.
                  Defaults to the repo root derived from this file's location.
    """

    def __init__(self, csv_path: Path, split: str, root: Path = _REPO_ROOT) -> None:
        self.df = pd.read_csv(csv_path)
        self.root = root
        self.transform = build_transforms(split)

    def __len__(self) -> int:
        return len(self.df)

    def __getitem__(self, idx: int) -> tuple[Tensor, int]:
        row = self.df.iloc[idx]
        cover = Path(row["cover_path"])
        # Support both absolute paths (legacy) and relative paths (relative to root).
        if not cover.is_absolute():
            cover = self.root / cover
        img = Image.open(cover).convert("RGB")
        label: int = GENRE_TO_IDX[row["genre"]]
        return self.transform(img), label


# ---------------------------------------------------------------------------
# Class weights helper
# ---------------------------------------------------------------------------

def compute_class_weights(train_csv: Path) -> Tensor:
    """Compute inverse-frequency class weights from the training CSV.

    Returns a float32 Tensor of shape (10,) suitable for
    ``nn.CrossEntropyLoss(weight=...)``.
    """
    df = pd.read_csv(train_csv)
    counts = (
        df["genre"]
        .map(GENRE_TO_IDX)
        .value_counts()
        .sort_index()
    )
    weights = 1.0 / counts.values.astype(float)
    weights_norm = weights / weights.sum()
    return torch.tensor(weights_norm, dtype=torch.float32)
