"""Generate example SQL statements for multiple warehouse targets."""
from __future__ import annotations

import logging
from typing import Dict, List

from .schema_inference import EventSchema, PropertySuggestion, SchemaSuggestions

logger = logging.getLogger(__name__)


def generate_queries(suggestions: SchemaSuggestions) -> Dict[str, str]:
    logger.info("Generating warehouse SQL queries...")
    warehouses = ["snowflake", "databricks", "bigquery", "redshift"]
    
    queries = {
        "snowflake": _snowflake_query(suggestions.event_schemas),
        "databricks": _databricks_query(suggestions.event_schemas),
        "bigquery": _bigquery_query(suggestions.event_schemas),
        "redshift": _redshift_query(suggestions.event_schemas),
    }
    
    for warehouse in warehouses:
        logger.info(f"✓ Generated {warehouse.capitalize()} query")
    
    return queries


def _snowflake_query(event_schemas: List[EventSchema]) -> str:
    selects = [
        _select_block(schema, warehouse="snowflake")
        for schema in event_schemas
    ]
    body = "\nUNION ALL\n".join(selects)
    return (
        "-- Snowflake example: staged JSON ingested into VARIANT column named payload\n"
        "WITH staged AS (\n    SELECT payload, metadata:source_file::string AS source_file\n    FROM @amplitude.stage/events\n)\n"
        "SELECT * FROM (" + body + ")"
    )


def _databricks_query(event_schemas: List[EventSchema]) -> str:
    selects = [
        _select_block(schema, warehouse="databricks")
        for schema in event_schemas
    ]
    body = "\nUNION ALL\n".join(selects)
    return (
        "-- Databricks example leveraging Auto Loader\n"
        "WITH bronze AS (\n    SELECT * FROM delta.`/mnt/amplitude/raw`\n)\n"
        "SELECT * FROM (" + body + ")"
    )


def _bigquery_query(event_schemas: List[EventSchema]) -> str:
    selects = [
        _select_block(schema, warehouse="bigquery")
        for schema in event_schemas
    ]
    body = "\nUNION ALL\n".join(selects)
    return (
        "-- BigQuery example reading from JSON ingestion table\n"
        "WITH source AS (\n    SELECT payload, _FILE_NAME AS source_file\n    FROM `amplitude.events_raw`\n)\n"
        "SELECT * FROM (" + body + ")"
    )


def _redshift_query(event_schemas: List[EventSchema]) -> str:
    selects = [
        _select_block(schema, warehouse="redshift")
        for schema in event_schemas
    ]
    body = "\nUNION ALL\n".join(selects)
    return (
        "-- Redshift example using SUPER column projection\n"
        "WITH staged AS (\n    SELECT payload\n    FROM amplitude_raw\n)\n"
        "SELECT * FROM (" + body + ")"
    )


def _select_block(schema: EventSchema, *, warehouse: str) -> str:
    event_literal = schema.event_type.replace("'", "''")
    lines = ["SELECT"]
    lines.append(f"    '{event_literal}' AS event_type,")
    for prop in schema.properties:
        projection = _projection(prop, warehouse)
        lines.append(f"    {projection},")
    lines.append("    CURRENT_TIMESTAMP AS loaded_at")
    return "\n".join(lines)


def _projection(prop: PropertySuggestion, warehouse: str) -> str:
    alias = prop.name.replace(".", "_").replace("[", "_").replace("]", "")
    path_parts = _path_parts(prop.name)

    if warehouse == "snowflake":
        pointer = "payload"
        for part in path_parts:
            pointer += f':"{part}"'
        return f"{pointer}::string AS {alias}"

    if warehouse == "databricks":
        pointer = "payload"
        for part in path_parts:
            pointer += f".{part}"
        return f"{pointer} AS {alias}"

    if warehouse == "bigquery":
        pointer = "payload"
        for part in path_parts:
            pointer += f".{part}"
        return f"{pointer} AS {alias}"

    if warehouse == "redshift":
        pointer = "payload"
        for part in path_parts:
            pointer += f"['{part}']"
        return f"{pointer} AS {alias}"

    raise ValueError(f"Unsupported warehouse {warehouse}")


def _path_parts(name: str) -> List[str]:
    parts: List[str] = []
    for part in name.replace("[", ".").replace("]", "").split("."):
        cleaned = part.strip().strip("'")
        if cleaned:
            parts.append(cleaned)
    return parts


__all__ = ["generate_queries"]
