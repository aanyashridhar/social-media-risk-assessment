from __future__ import annotations

from pathlib import Path

import pyarrow.dataset as ds

from pii_risk.ingest.reddit import ingest_reddit


def test_ingest_reddit_jsonl(tmp_path: Path) -> None:
    fixture_path = Path(__file__).parent / "fixtures" / "reddit_sample.jsonl"
    output_dir = tmp_path / "output"

    ingest_reddit(str(fixture_path), str(output_dir))

    dataset = ds.dataset(output_dir, format="parquet", partitioning="hive")
    table = dataset.to_table()
    records = table.to_pylist()

    assert len(records) == 4
    post_files = list((output_dir / "platform=reddit" / "record_type=post").rglob("*.parquet"))
    comment_files = list((output_dir / "platform=reddit" / "record_type=comment").rglob("*.parquet"))
    assert len(post_files) > 0
    assert len(comment_files) > 0

    post = next(r for r in records if r["record_id"] == "p1")
    assert post["text"] == "First post\n\nHello world"

    comment = next(r for r in records if r["record_id"] == "c1")
    assert comment["text"] == "Nice post"

    post_partition = output_dir / "platform=reddit" / "record_type=post" / "year=2025" / "month=01"
    comment_partition = output_dir / "platform=reddit" / "record_type=comment" / "year=2025" / "month=01"
    assert post_partition.exists()
    assert comment_partition.exists()
