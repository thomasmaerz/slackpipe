# Contributing

1. Create a virtual environment and install development dependencies with
   `uv sync --all-extras`.
2. Keep credentials and corpus output outside Git.
3. Run `uv run pytest -q` before submitting changes.
4. Preserve the ownership boundary: Slackpipe writes the canonical archive;
   downstream consumers receive it read-only.
5. Include tests for changes to extraction plans, source validation, canonical
   schema, orchestration, or deployment contracts.

Use focused commits and explain operational or migration impact in the pull
request.
