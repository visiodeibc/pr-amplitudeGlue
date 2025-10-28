from __future__ import annotations

from amplitude_glue.schema_inference import analyze_payload


def test_detects_event_types_and_properties(samples_dir):
    suggestions = analyze_payload(samples_dir / "ecommerce.json")

    event_types = {schema.event_type for schema in suggestions.event_schemas}
    assert "purchase_completed" in event_types
    assert "cart_abandoned" in event_types

    purchase_schema = next(schema for schema in suggestions.event_schemas if schema.event_type == "purchase_completed")
    property_names = {prop.name for prop in purchase_schema.properties}
    assert "cart.currency" in property_names
    assert suggestions.import_settings.deduplication_key in {"event_id", "uuid"}


def test_user_and_group_properties_extracted(samples_dir):
    suggestions = analyze_payload(samples_dir / "finance.json")

    user_props = {prop.name for prop in suggestions.user_properties}
    group_props = {prop.name for prop in suggestions.group_properties}

    assert "customer.user_id" in user_props
    assert any(key.startswith("account.ownership.group_id") for key in group_props)
