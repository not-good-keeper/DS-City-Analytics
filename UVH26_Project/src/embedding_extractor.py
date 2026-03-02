from typing import Dict, List

import cv2
import numpy as np
import torch
from torch.utils.data import DataLoader, Dataset
from torchvision import transforms
from torchvision.models import ResNet50_Weights, resnet50
from tqdm import tqdm


MASK_PADDING = -40


class ImageRecordDataset(Dataset):
    def __init__(self, records: List[Dict]):
        self.records = records
        self.transform = transforms.Compose(
            [
                transforms.ToPILImage(),
                transforms.Resize((224, 224)),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406],
                    std=[0.229, 0.224, 0.225],
                ),
            ]
        )

    def __len__(self) -> int:
        return len(self.records)

    def _mask_bboxes(self, image_rgb: np.ndarray, bboxes: List[List[float]]) -> np.ndarray:
        masked = image_rgb.copy()
        height, width = masked.shape[:2]

        for bbox in bboxes:
            if len(bbox) != 4:
                raise ValueError("Each bbox must have exactly 4 values.")

            x, y, w, h = float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])
            if w <= 0 or h <= 0:
                continue

            x1 = int(round(x - MASK_PADDING))
            y1 = int(round(y - MASK_PADDING))
            x2 = int(round(x + w + MASK_PADDING))
            y2 = int(round(y + h + MASK_PADDING))

            x1 = max(0, min(width, x1))
            y1 = max(0, min(height, y1))
            x2 = max(0, min(width, x2))
            y2 = max(0, min(height, y2))

            if x2 <= x1 or y2 <= y1:
                continue

            masked[y1:y2, x1:x2] = 0

        return masked

    def __getitem__(self, index: int) -> torch.Tensor:
        record = self.records[index]
        image_path = record["image_path"]
        bboxes = record.get("bboxes", [])
        if not isinstance(bboxes, list):
            raise ValueError("Record bboxes must be a list.")

        image_bgr = cv2.imread(image_path, cv2.IMREAD_COLOR)
        if image_bgr is None:
            raise ValueError(f"Failed to read image at path: {image_path}")
        image_rgb = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2RGB)
        masked_rgb = self._mask_bboxes(image_rgb, bboxes)
        return self.transform(masked_rgb)


def extract_embeddings(
    records: List[Dict],
    model: torch.nn.Module,
    device: torch.device,
    batch_size: int = 32,
) -> np.ndarray:
    dataset = ImageRecordDataset(records)
    dataloader = DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=0,
    )

    all_embeddings = []
    with torch.no_grad():
        for batch in tqdm(dataloader, desc="Extracting embeddings"):
            batch = batch.to(device)
            embeddings = model(batch)
            all_embeddings.append(embeddings.detach().cpu().numpy())

    if len(all_embeddings) == 0:
        raise ValueError("No embeddings generated.")

    embeddings_matrix = np.concatenate(all_embeddings, axis=0)

    if embeddings_matrix.shape[0] != len(records):
        raise ValueError("Embeddings count does not match dataset size.")
    if embeddings_matrix.shape[1] != 2048:
        raise ValueError("Embedding dimension must be 2048.")

    norms = np.linalg.norm(embeddings_matrix, axis=1, keepdims=True)
    if (norms <= 0).any():
        raise ValueError("Invalid embedding norm encountered.")
    embeddings_matrix = embeddings_matrix / norms

    return embeddings_matrix


def build_resnet50_embedder() -> tuple:
    torch.manual_seed(42)
    np.random.seed(42)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(42)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = resnet50(weights=ResNet50_Weights.IMAGENET1K_V1)
    model.fc = torch.nn.Identity()
    model = model.to(device)
    model.eval()

    return model, device
