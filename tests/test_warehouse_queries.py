from __future__ import annotations

from amplitude_glue.schema_inference import analyze_payload
from amplitude_glue.warehouse_queries import generate_queries


def test_generates_all_warehouse_queries(samples_dir):
    suggestions = analyze_payload(samples_dir / "streaming.json")
    queries = generate_queries(suggestions)

    assert set(queries.keys()) == {"snowflake", "databricks", "bigquery", "redshift"}
    assert "playback_started" in queries["snowflake"]
    assert "UNION ALL" in queries["bigquery"]
