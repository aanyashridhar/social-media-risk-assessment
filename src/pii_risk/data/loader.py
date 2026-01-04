from __future__ import annotations

from collections.abc import Iterator

import pyarrow.dataset as ds


IN_FILE_COLUMNS = ["record_id", "created_at", "text"]


def iter_parquet_records(input_dir: str, max_rows: int | None = None) -> Iterator[dict]:
    dataset = ds.dataset(input_dir, format="parquet", partitioning="hive")

    columns = list(IN_FILE_COLUMNS)
    if "community" in dataset.schema.names:
        columns.append("community")

    scanner = dataset.scanner(columns=columns)
    yielded = 0

    for batch in scanner.to_batches():
        for row in batch.to_pylist():
            text = row.get("text")
            if not text:
                continue

            yield row
            yielded += 1

            if max_rows is not None and yielded >= max_rows:
                return
