"""Persist schema suggestions and generated SQL examples."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

from .schema_inference import EventSchema, PropertySuggestion, SchemaSuggestions

logger = logging.getLogger(__name__)


def save_report(
    path: Path,
    suggestions: SchemaSuggestions,
    queries: Dict[str, str],
    summary: str,
) -> None:
    logger.info("Building final report...")
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Amplitude Import Blueprint", ""]
    lines.append("## Summary")
    lines.append(summary)
    lines.append("")

    lines.append("## Event Schemas")
    for schema in suggestions.event_schemas:
        lines.append(f"### {schema.event_type}")
        if not schema.properties:
            lines.append("- No event properties detected")
            continue
        for prop in schema.properties:
            lines.append(_format_property(prop))
        lines.append("")

    lines.append("## User Properties")
    if suggestions.user_properties:
        for prop in suggestions.user_properties:
            lines.append(_format_property(prop))
    else:
        lines.append("- None detected")
    lines.append("")

    lines.append("## Group Properties")
    if suggestions.group_properties:
        for prop in suggestions.group_properties:
            lines.append(_format_property(prop))
    else:
        lines.append("- None detected")
    lines.append("")

    settings = suggestions.import_settings
    lines.append("## Import Settings")
    lines.append(f"- Deduplication key: `{settings.deduplication_key}`")
    lines.append(f"- Timestamp key: `{settings.timestamp_key}`")
    lines.append(f"- Delivery strategy: {settings.delivery_strategy}")
    lines.append(f"- Notes: {settings.notes}")
    lines.append("")

    lines.append("## Warehouse Queries")
    for name, query in queries.items():
        lines.append(f"### {name.title()}")
        lines.append("```sql")
        lines.append(query.strip())
        lines.append("```")
        lines.append("")

    logger.info(f"Writing report to {path}")
    path.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"✓ Report saved ({len(lines)} lines)")


def _format_property(prop: PropertySuggestion) -> str:
    snippet = f"- `{prop.name}` ({prop.datatype})"
    if prop.example:
        snippet += f" e.g. `{prop.example}`"
    if prop.description:
        snippet += f" — {prop.description}"
    return snippet


__all__ = ["save_report"]
