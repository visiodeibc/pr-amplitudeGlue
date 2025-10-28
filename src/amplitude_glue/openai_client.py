"""Wrapper around the OpenAI SDK with safe fallbacks for offline development."""
from __future__ import annotations

import json
import logging
from typing import Optional

from .config import load_env
from .schema_inference import SchemaSuggestions

logger = logging.getLogger(__name__)

try:
    from openai import OpenAI
    from openai import OpenAIError
except Exception:  # pragma: no cover - only executed when SDK missing
    OpenAI = None  # type: ignore
    OpenAIError = Exception  # type: ignore


class OpenAISchemaAssistant:
    """Thin helper that coordinates OpenAI function-calling prompts."""

    def __init__(self, model: str = "gpt-4o-mini") -> None:
        self.model = model
        self._client: Optional[OpenAI] = None
        load_env()
        if OpenAI is None:
            logger.warning("OpenAI SDK not available - will use offline fallback")
            return
        try:
            self._client = OpenAI()
            logger.info("✓ OpenAI client initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize OpenAI client: {e}")
            logger.warning("Will use offline fallback for summary generation")
            self._client = None

    def summarize(self, suggestions: SchemaSuggestions) -> str:
        if not self._client:
            logger.info("Generating offline summary (OpenAI not available)")
            return _offline_summary(suggestions)

        logger.info("Requesting AI-powered summary from OpenAI...")
        payload = json.dumps(_structured_payload(suggestions), indent=2)
        logger.debug(f"Sending schema payload:\n{payload}")
        try:
            response = self._client.responses.create(
                model=self.model,
                input=[
                    {
                        "role": "system",
                        "content": "You help data engineers design Amplitude imports.",
                    },
                    {
                        "role": "user",
                        "content": (
                            "Summarize the inferred schema below and list the next steps for validation. "
                            "Return 2-3 bullet points.\n\nSchema JSON:\n"
                            f"```json\n{payload}\n```"
                        ),
                    },
                ],
            )
            
            # Log raw response structure for debugging
            logger.debug(f"OpenAI response type: {type(response)}")
            logger.debug(f"OpenAI response attributes: {dir(response)}")
            
            # Extract message from response
            message = getattr(response, "output_text", None)
            if not message:
                message = response.output[0].content[0].text  # type: ignore[attr-defined]
            
            logger.info("✓ Received AI-generated summary from OpenAI")
            
            return message.strip()
        except (OpenAIError, AttributeError, KeyError, IndexError, TypeError) as e:
            logger.warning(f"OpenAI API call failed: {e}")
            logger.info("Falling back to offline summary generation")
            return _offline_summary(suggestions)


def _structured_payload(suggestions: SchemaSuggestions) -> dict:
    return {
        "event_types": [schema.event_type for schema in suggestions.event_schemas],
        "user_properties": [prop.name for prop in suggestions.user_properties],
        "group_properties": [prop.name for prop in suggestions.group_properties],
    }


def _offline_summary(suggestions: SchemaSuggestions) -> str:
    events = ", ".join(schema.event_type for schema in suggestions.event_schemas)
    user_props = len(suggestions.user_properties)
    group_props = len(suggestions.group_properties)
    return (
        "- Review inferred events: "
        + (events or "no events found")
        + f"\n- Map {user_props} user properties and {group_props} group properties in Amplitude."
        + "\n- Validate import settings before scheduling warehouse sync."
    )


__all__ = ["OpenAISchemaAssistant"]
