# Repository Guidelines

## Project Structure & Module Organization
Keep Python sources inside `src/amplitude_glue/`. Separate responsibilities across `schema_inference.py` (heuristics), `warehouse_queries.py` (SQL emitters), `openai_client.py` (LLM integration), and `cli.py` (entrypoint). Store messy payloads under `data/samples/` alongside `data/samples/README.md` that explains quirks. Generated artifacts belong in `artifacts/examples/`; treat the directory as disposable. Place pytest suites in `tests/` and reuse shared fixtures from `tests/fixtures/`.

## Build, Test, and Development Commands
Provision a virtual environment with `uv venv` and install dependencies via `uv pip install -r requirements.txt`. Copy `.env.example` to `.env` and add `OPENAI_API_KEY` if you want AI summaries; the CLI loads the file before initializing the SDK. Run the CLI with `uv run python -m amplitude_glue.cli analyze data/samples/ecommerce.json --output artifacts/examples/ecommerce_report.txt`. Formatting uses `uv run ruff format .`, linting uses `uv run ruff check .`, and the test suite runs with `uv run pytest`. Generate coverage reports as needed using `uv run pytest --cov=amplitude_glue`.

## Coding Style & Naming Conventions
Follow PEP 8, 4-space indentation, and keep functions focused. Use snake_case for files (`schema_inference.py`) and variables, UpperCamelCase for classes (`OpenAISchemaAssistant`). Event/property identifiers should remain snake_case (`purchase_completed`), while OpenAI tool names stay kebab-case. Prefer pathlib over os.path, include type hints on public interfaces, and guard network calls to keep the CLI offline-friendly.

## Testing Guidelines
Tests mirror the package layout (e.g., `tests/test_schema_inference.py`). Use descriptive names such as `test_detects_user_properties_from_nested_payload`. Capture messy edge cases from sample JSON (arrays, nulls, nested objects). When introducing new samples, add fixtures and adjust SQL snapshot expectations. Aim for ≥85% coverage on schema inference logic before broad changes.

## Commit & Pull Request Guidelines
Adopt Conventional Commit prefixes (for example, `feat: add redshift sql generator` or `chore: refresh sample payloads`). PRs must include a summary, testing notes, and relevant screenshots or SQL excerpts. Link to tracked work items, note any manual steps to regenerate artifacts, and keep diffs under ~300 lines where feasible.
