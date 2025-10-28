from __future__ import annotations

import os

from amplitude_glue.config import load_env


def test_load_env_sets_variables(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("OPENAI_API_KEY=test-key\nEXTRA_VALUE=42\n", encoding="utf-8")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("EXTRA_VALUE", raising=False)

    load_env(env_file)

    assert os.environ["OPENAI_API_KEY"] == "test-key"
    assert os.environ["EXTRA_VALUE"] == "42"


def test_load_env_preserves_existing_values(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("OPENAI_API_KEY=updated-key\n", encoding="utf-8")
    monkeypatch.setenv("OPENAI_API_KEY", "original-key")

    load_env(env_file)

    assert os.environ["OPENAI_API_KEY"] == "original-key"
