from __future__ import annotations

from collections.abc import Iterator

import pyarrow.dataset as ds


REQUIRED_COLUMNS = ["record_id", "created_at", "text", "platform", "record_type"]


def iter_parquet_records(
    input_dir: str, max_rows: int | None = None
) -> Iterator[dict]:
    dataset = ds.dataset(input_dir, format="parquet")
    columns = list(REQUIRED_COLUMNS)
    if "community" in dataset.schema.names:
        columns.append("community")

    scanner = dataset.scan(columns=columns)
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
