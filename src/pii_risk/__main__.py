from __future__ import annotations

import typer

from pii_risk.ingest.reddit import ingest_reddit

app = typer.Typer(help="PII risk assessment tools.")


@app.command("ingest-reddit")
def ingest_reddit_command(
    input: str = typer.Option(..., "--input", help="Path to JSONL or CSV file."),
    output: str = typer.Option(..., "--output", help="Output directory."),
    max_rows: int | None = typer.Option(None, "--max-rows", help="Max rows to ingest."),
) -> None:
    ingest_reddit(input, output, max_rows)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
