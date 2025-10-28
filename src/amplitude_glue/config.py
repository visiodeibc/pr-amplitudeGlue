"""Environment helpers for the amplitude_glue package."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Tuple


def load_env(path: Path | None = None) -> None:
    """Populate os.environ with variables defined in a .env file.

    The loader first checks an explicit path. If none is provided, it looks in the
    current working directory and then falls back to the project root (two levels
    up from this module).
    """

    candidates = _candidate_paths(path)
    for candidate in candidates:
        if candidate and candidate.is_file():
            _apply_env(candidate)
            return


def _candidate_paths(path: Path | None) -> Iterable[Path | None]:
    if path is not None:
        yield path
    else:
        cwd_candidate = Path.cwd() / ".env"
        yield cwd_candidate
        project_root = Path(__file__).resolve().parents[2]
        yield project_root / ".env"


def _apply_env(path: Path) -> None:
    for key, value in _iter_env_pairs(path):
        os.environ.setdefault(key, value)


def _iter_env_pairs(path: Path) -> Iterable[Tuple[str, str]]:
    text = path.read_text(encoding="utf-8")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = raw_value.strip().strip('"').strip("'")
        yield key, value


__all__ = ["load_env"]
