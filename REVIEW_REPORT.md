# Project Review Report

## Review findings

1. **High — REPL capacity limit races under concurrency.**  
   `server/manager.py` checks capacity while holding the condition lock, but creates
   and registers the REPL after releasing it. Concurrent calls can all observe free
   capacity. A probe with `max_repls=1` produced two busy REPLs. Reserve capacity
   under the lock, including in-progress creations.

2. **High — subprocess stderr is never consumed.**  
   `server/repl.py` opens `stderr=PIPE`, but no task reads it. The later
   `error_file` read is disconnected from the process. Enough stderr output can
   fill the pipe and deadlock Lean; shorter failures lose their actual error text.
   Drain stderr continuously or redirect it to the temporary file.

3. **High — database credentials are written to logs.**  
   `server/main.py` logs the complete database URL, commonly including username
   and password. Remove this log or redact credentials.

4. **High — the published Python compatibility claim is false.**  
   `pyproject.toml` declares Python 3.9, while public modules such as
   `client/kimina_client/base.py` evaluate `str | None` annotations without
   postponed evaluation. This raises `TypeError` on Python 3.9.6. Require Python
   3.10+, or make all modules genuinely 3.9-compatible and test a version matrix.

5. **Medium — synchronous batching changes result order.**  
   `client/kimina_client/sync_client.py` appends futures through `as_completed`,
   so results follow completion order rather than input order. A probe returned
   `["second", "first"]`. Track batch indexes and sort before merging.

6. **Medium — a base client installation can fail on import.**  
   `client/kimina_client/models.py` imports Pygments unconditionally, but the
   built wheel's metadata does not declare `pygments`. It currently arrives only
   through the optional server dependency `rich`. Declare every direct dependency
   explicitly or make terminal formatting optional.

7. **Medium — Docker's Lean upgrade is internally inconsistent.**  
   `Dockerfile` selects Lean/mathlib 4.26 but still builds the REPL from
   `FrederickPu/repl@lean415compat`; `setup.sh` instead defaults to the official
   matching-version branch. Align these paths and add a container smoke test.

8. **Medium — CI can silently use stale Lean assets or skip relevant changes.**  
   `.github/workflows/ci.yaml` ignores changes under `tests/`, `uv.lock`,
   `setup.sh`, Docker files, and workflows. Its Lean cache key references an unset
   environment variable, so version upgrades do not invalidate the cache.

9. **Medium — `create_app(settings)` does not configure authentication.**  
   `server/auth.py` reads the module-global settings object instead of the settings
   passed to `create_app`. An app created with `api_key="custom-secret"` still
   used the global `None`, disabling authentication. Bind authentication to app
   state or construct the dependency from the supplied settings.

10. **Medium — shutdown does not wait for child processes.**  
    `server/manager.py` schedules close tasks and immediately reports cleanup
    complete. Event-loop shutdown can cancel them, leaving Lean processes or
    database updates unfinished. Snapshot the REPLs and await them outside the
    lock.

## Simplification and modernization

- Move `datasets` into a benchmark extra. It pulls roughly 180 MB of NumPy,
  pandas, and PyArrow into every client installation despite benchmark imports
  already being lazy.
- Remove unused direct dependencies such as `pip`, `requests`, and
  `python-dotenv`; depend directly on `pydantic`.
- Move deprecated `tool.uv.dev-dependencies` to `dependency-groups.dev`.
- Remove or relocate the malformed, shipped
  `client/kimina_client/import json.py`. It contains a syntax error and
  project-specific imports.
- Stop excluding the entire client package in `.pre-commit-config.yaml`. This
  exclusion is why the malformed shipped file and compatibility issues pass CI.
- Consolidate duplicated request, retry, benchmark, and dataset-selection logic
  in the sync and async clients.
- Reuse HTTP connections in the synchronous client instead of constructing a new
  `httpx.Client` for every attempt.
- Restrict retries to appropriate transient failures. Both clients currently
  retry authentication and validation failures, while retrying an expensive POST
  after an ambiguous transport failure can duplicate proof execution.

## Validation baseline

- `prek -a --quiet` passed.
- The default test suite passed with 29 tests passed and 103 marker-deselected,
  at 77% server coverage.

