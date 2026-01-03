from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds


def _write_sample_dataset(output_dir: Path) -> None:
    table = pa.table(
        {
            "platform": ["reddit", "reddit"],
            "record_type": ["post", "comment"],
            "record_id": ["post-1", "comment-1"],
            "author_id_hash": ["author-1", "author-2"],
            "created_at": ["2024-01-05T00:00:00Z", "2024-02-06T00:00:00Z"],
            "text": ["Post title\n\nPost body", "Comment body"],
            "year": [2024, 2024],
            "month": [1, 2],
        }
    )

    ds.write_dataset(
        table,
        output_dir,
        format="parquet",
        partitioning=["platform", "record_type", "year", "month"],
        existing_data_behavior="overwrite_or_ignore",
    )


def test_ingest_reddit_partitioned_output(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    _write_sample_dataset(output_dir)

    dataset = ds.dataset(output_dir, format="parquet", partitioning="hive")
    table = dataset.to_table()

    assert table.num_rows == 2

    platform_dir = output_dir / "platform=reddit"
    post_partition = platform_dir / "record_type=post"
    comment_partition = platform_dir / "record_type=comment"

    assert post_partition.is_dir()
    assert comment_partition.is_dir()
    assert list(post_partition.rglob("*.parquet"))
    assert list(comment_partition.rglob("*.parquet"))

    assert (post_partition / "year=2024" / "month=1").is_dir()
    assert (comment_partition / "year=2024" / "month=2").is_dir()

    post_table = ds.dataset(post_partition, format="parquet").to_table()
    comment_table = ds.dataset(comment_partition, format="parquet").to_table()

    assert post_table.column("text")[0].as_py() == "Post title\n\nPost body"
    assert comment_table.column("text")[0].as_py() == "Comment body"
