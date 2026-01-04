from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from pii_risk.ml.combine import combined_score
from pii_risk.ml.predict import predict_risk
from pii_risk.ml.train import train_model


def _write_dataset(path: Path) -> None:
    records = []
    for idx in range(10):
        created_at = f"2024-01-{idx + 1:02d}T00:00:00"
        text = "shared project update about privacy"
        if idx in {6, 8}:
            text = f"{text} email me at person{idx}@example.com"
        records.append(
            {
                "record_id": f"r{idx}",
                "created_at": created_at,
                "text": text,
                "platform": "reddit",
                "record_type": "post",
            }
        )
    table = pa.table(records)
    ds.write_dataset(table, base_dir=path, format="parquet")


def test_train_predict_smoke(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    _write_dataset(dataset_dir)

    models_dir = tmp_path / "models"
    artifacts = train_model(str(dataset_dir), models_dir=models_dir)

    assert Path(artifacts["model_path"]).exists()
    assert Path(artifacts["vectorizer_path"]).exists()
    assert Path(artifacts["metadata_path"]).exists()

    prediction = predict_risk(
        "shared project update email me at person@example.com", models_dir=models_dir
    )
    assert 0.0 <= prediction["p_risk"] <= 1.0
    assert isinstance(prediction["top_terms"], list)

    explicit = combined_score(rule_score=80, p_risk=0.1)
    assert explicit["interpretation"] == "explicit_pii_dominant"

    contextual = combined_score(rule_score=10, p_risk=0.9)
    assert contextual["interpretation"] == "contextually_concerning"
