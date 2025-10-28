from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure src/ package is importable when running pytest without installing the project.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(scope="session")
def samples_dir() -> Path:
    return ROOT / "data" / "samples"
