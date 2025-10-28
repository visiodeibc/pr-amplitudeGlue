"""Infer event schemas and import hints from semi-structured payloads."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

IGNORED_KEYS = {"timestamp", "time", "ts", "received_at", "sent_at"}
USER_HINT_KEYS = {"user_id", "customer_id", "account_id", "profile", "user"}
GROUP_HINT_KEYS = {"group_id", "organization", "team", "company", "group"}
EVENT_HINT_KEYS = ("event_type", "event", "action", "type", "name")


@dataclass
class PropertySuggestion:
    name: str
    datatype: str
    example: Optional[str] = None
    description: Optional[str] = None


@dataclass
class EventSchema:
    event_type: str
    properties: List[PropertySuggestion] = field(default_factory=list)


@dataclass
class ImportSettings:
    deduplication_key: str
    timestamp_key: str
    delivery_strategy: str
    notes: str


@dataclass
class SchemaSuggestions:
    event_schemas: List[EventSchema]
    user_properties: List[PropertySuggestion]
    group_properties: List[PropertySuggestion]
    import_settings: ImportSettings


def analyze_payload(path: Path) -> SchemaSuggestions:
    logger.info("Loading JSON payload...")
    records = _load_records(path)
    if not records:
        raise ValueError(f"No JSON records found in {path}")
    
    logger.info(f"✓ Loaded {len(records)} record(s)")
    logger.info("Analyzing event schemas and properties...")

    event_map: Dict[str, Dict[str, PropertySuggestion]] = {}
    user_props: Dict[str, PropertySuggestion] = {}
    group_props: Dict[str, PropertySuggestion] = {}

    for record in records:
        event_type = _detect_event_type(record)
        if event_type not in event_map:
            event_map[event_type] = {}

        flattened = _flatten(record)
        for key, value in flattened.items():
            normalized_key = key.replace(" ", "_")
            datatype = _infer_type(value)
            example = _example_value(value)

            if _is_user_property(normalized_key):
                user_props.setdefault(
                    normalized_key,
                    PropertySuggestion(
                        name=normalized_key,
                        datatype=datatype,
                        example=example,
                        description="User-level attribute inferred from payload",
                    ),
                )
            elif _is_group_property(normalized_key):
                group_props.setdefault(
                    normalized_key,
                    PropertySuggestion(
                        name=normalized_key,
                        datatype=datatype,
                        example=example,
                        description="Group/organization attribute inferred from payload",
                    ),
                )
            elif normalized_key not in IGNORED_KEYS:
                event_map[event_type].setdefault(
                    normalized_key,
                    PropertySuggestion(
                        name=normalized_key,
                        datatype=datatype,
                        example=example,
                        description=f"Captured from field `{normalized_key}`",
                    ),
                )

    event_schemas = [
        EventSchema(
            event_type=etype,
            properties=sorted(props.values(), key=lambda p: p.name),
        )
        for etype, props in sorted(event_map.items())
    ]
    
    logger.info(f"✓ Detected {len(event_schemas)} event type(s)")
    for schema in event_schemas:
        logger.info(f"  - {schema.event_type} ({len(schema.properties)} properties)")
    
    logger.info(f"✓ Identified {len(user_props)} user property(ies)")
    logger.info(f"✓ Identified {len(group_props)} group property(ies)")

    timestamp_key = _guess_timestamp_key(records)
    dedup_key = _guess_dedup_key(records)
    logger.info(f"✓ Guessed timestamp key: {timestamp_key}")
    logger.info(f"✓ Guessed deduplication key: {dedup_key}")
    
    import_settings = ImportSettings(
        deduplication_key=dedup_key,
        timestamp_key=timestamp_key,
        delivery_strategy="timely",  # POC default
        notes="Review mappings before production import; adjust warehouse queries for column names.",
    )

    return SchemaSuggestions(
        event_schemas=event_schemas,
        user_properties=sorted(user_props.values(), key=lambda p: p.name),
        group_properties=sorted(group_props.values(), key=lambda p: p.name),
        import_settings=import_settings,
    )


def _load_records(path: Path) -> List[Dict[str, Any]]:
    raw_text = path.read_text(encoding="utf-8").strip()
    if not raw_text:
        logger.warning("File is empty")
        return []

    try:
        parsed = json.loads(raw_text)
        if isinstance(parsed, list):
            logger.debug("Detected JSON array format")
            return [record for record in parsed if isinstance(record, dict)]
        if isinstance(parsed, dict):
            logger.debug("Detected single JSON object format")
            return [parsed]
    except json.JSONDecodeError:
        logger.debug("Not a valid JSON array/object, trying line-delimited JSON")

    records: List[Dict[str, Any]] = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed_line = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed_line, dict):
            records.append(parsed_line)
    
    if records:
        logger.debug("Successfully parsed as line-delimited JSON")
    return records


def _detect_event_type(record: Dict[str, Any]) -> str:
    for key in EVENT_HINT_KEYS:
        value = record.get(key)
        if isinstance(value, str) and value:
            return value.strip()
    return "unknown_event"


def _flatten(data: Dict[str, Any], parent_key: str = "", separator: str = ".") -> Dict[str, Any]:
    items: Dict[str, Any] = {}
    for key, value in data.items():
        new_key = f"{parent_key}{separator}{key}" if parent_key else key
        if isinstance(value, dict):
            items.update(_flatten(value, new_key, separator=separator))
        else:
            items[new_key] = value
    return items


def _infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "double"
    if isinstance(value, list):
        if not value:
            return "array"
        inner = {_infer_type(item) for item in value}
        if len(inner) == 1:
            return f"array<{inner.pop()}>"
        joined = ",".join(sorted(inner))
        return f"array<mixed:{joined}>"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, str):
        if _looks_like_timestamp(value):
            return "timestamp"
        return "string"
    return "unknown"


def _example_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        preview = json.dumps(value, ensure_ascii=False)[:80]
        return preview + ("…" if len(preview) == 80 else "")
    return str(value)


def _looks_like_timestamp(value: str) -> bool:
    if value.endswith("Z") and "T" in value:
        return True
    if value.count("-") == 2 and ("T" in value or " " in value):
        return True
    return False


def _is_user_property(key: str) -> bool:
    lowered = key.lower()
    return any(hint in lowered for hint in USER_HINT_KEYS)


def _is_group_property(key: str) -> bool:
    lowered = key.lower()
    return any(hint in lowered for hint in GROUP_HINT_KEYS)


def _guess_timestamp_key(records: Iterable[Dict[str, Any]]) -> str:
    for record in records:
        for key in record.keys():
            lowered = key.lower()
            if "time" in lowered or "timestamp" in lowered or lowered.endswith("_at"):
                return key
    return "timestamp"


def _guess_dedup_key(records: Iterable[Dict[str, Any]]) -> str:
    candidate_counts: Dict[str, int] = {}
    for record in records:
        for key in record.keys():
            if key.lower() in {"event_id", "id", "uuid"}:
                candidate_counts[key] = candidate_counts.get(key, 0) + 1
    if candidate_counts:
        return max(candidate_counts, key=candidate_counts.get)
    return "event_id"


__all__ = [
    "PropertySuggestion",
    "EventSchema",
    "ImportSettings",
    "SchemaSuggestions",
    "analyze_payload",
]
