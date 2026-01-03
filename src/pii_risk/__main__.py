from __future__ import annotations

import typer

from pii_risk.pii.detector import detect_pii_spans, redact_text
from pii_risk.pii.scoring import score_record

from pii_risk.ingest.reddit import ingest_reddit

app = typer.Typer(help="PII risk assessment tools.")


@app.command("ingest-reddit")
def ingest_reddit_command(
    input: str = typer.Option(..., "--input", help="Path to JSONL or CSV file."),
    output: str = typer.Option(..., "--output", help="Output directory."),
    max_rows: int | None = typer.Option(None, "--max-rows", help="Max rows to ingest."),
) -> None:
    ingest_reddit(input, output, max_rows)


@app.command("analyze-text")
def analyze_text_command(
    text: str = typer.Option(..., "--text", help="Text to analyze."),
) -> None:
    result = score_record(text)
    spans = detect_pii_spans(text)
    redacted = redact_text(text, spans)

    typer.echo(f"score: {result['score']}")
    typer.echo(f"counts_by_type: {result['counts_by_type']}")
    typer.echo(f"redacted_text: {redacted}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
