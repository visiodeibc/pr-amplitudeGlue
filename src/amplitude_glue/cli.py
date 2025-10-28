"""Command-line interface for the amplitude_glue proof of concept."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .config import load_env
from .exporters import save_report
from .openai_client import OpenAISchemaAssistant
from .schema_inference import analyze_payload
from .warehouse_queries import generate_queries

logger = logging.getLogger(__name__)


DEFAULT_OUTPUT = Path("artifacts/examples/analysis_report.txt")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Schema inference for Amplitude imports")
    subparsers = parser.add_subparsers(dest="command")

    analyze_parser = subparsers.add_parser("analyze", help="Analyze a JSON dataset and emit guidance")
    analyze_parser.add_argument("json_path", type=Path, help="Path to the JSON payload to inspect")
    analyze_parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Where to write the text summary (defaults to artifacts/examples/analysis_report.txt)",
    )
    analyze_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging (DEBUG level)",
    )

    args = parser.parse_args(argv)
    
    # Configure logging after parsing args to check for verbose flag
    log_level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    if args.command != "analyze":
        parser.print_help()
        return

    json_path: Path = args.json_path
    output_path: Path = args.output

    logger.info("Starting Amplitude Glue schema analysis")
    logger.info(f"Input file: {json_path}")
    logger.info(f"Output destination: {output_path}")
    load_env()
    suggestions = analyze_payload(json_path)
    queries = generate_queries(suggestions)
    assistant = OpenAISchemaAssistant()
    summary = assistant.summarize(suggestions)

    save_report(output_path, suggestions, queries, summary)
    logger.info(f"✓ Analysis complete! Report saved to {output_path}")
    print(f"Report saved to {output_path}")


if __name__ == "__main__":  # pragma: no cover
    main()
