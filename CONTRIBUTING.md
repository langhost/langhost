# Contributing to langhost

Thanks for helping keep this **100% open source**.

This monorepo has two packages:

- **`langhost`** — CLI (`langhost serve`)
- **`langgraph-runtime-pg`** — Postgres + Redis runtime (`edition=pg`) that powers the official Agent Server

We replace the *runtime*, not the Agent Server. Prefer stock `langgraph-api` / `langgraph` / SDK behavior.

## Principles

- **Clean-room.** Do not reverse-engineer closed LangChain Docker bytecode. Use public docs, Agent Protocol behavior, and the open `langgraph-runtime-inmem` package as a surface reference.
- **Reuse upstream.** Extend the runtime edition; do not fork the HTTP API.
- **Small PRs.** Focused diffs are easier to review and ship.

## Setup

Requirements: Python 3.11+, [uv](https://docs.astral.sh/uv/), Docker, git.

```bash
git clone https://github.com/langhost/langhost.git
cd langhost
uv sync --group dev
cp .env.example .env
docker compose up -d
uv run pre-commit install   # optional
```

## Checks and tests

```bash
uv run pre-commit run --all-files

# Full e2e (compose + package tests + live API + upstream SDK suite)
./scripts/test.sh

# Faster loop when Postgres/Redis are already up
uv run pytest -q libs/langgraph-runtime-pg/tests
```

## Making changes

1. Open an issue for larger design work when practical.
2. Match existing style (`ruff` / `mypy` in the root `pyproject.toml`).
3. Add or update tests under `libs/langgraph-runtime-pg/tests/` for behavior changes.
4. Schema changes belong in Alembic revisions under
   `libs/langgraph-runtime-pg/src/langgraph_runtime_pg/migrations/versions/`.

## Pull requests

- Explain **why** the change is needed and how you verified it.
- Do not commit secrets, `.env`, or local `.tests/` checkouts.
- Keep the PR focused — one concern per PR when possible.
- CI must be green (`Lint`, `Build`, `Test`).

## Releasing

Both packages ship in **lockstep** (`langhost` and `langgraph-runtime-pg` share the same version).

1. Bump both package versions in their `pyproject.toml` files (and the `langhost` → `langgraph-runtime-pg==…` pin).
2. Run `uv lock`, update the tested versions and coverage in `docs/compatibility.md`, and run `python3 scripts/check_versions.py`.
3. Merge to `main`.
4. Tag and push (tag must match the version, with a `v` prefix):

   ```bash
   git tag v0.11.1.post4
   git push origin v0.11.1.post4
   ```

5. The **Release** workflow builds, publishes both packages to **PyPI** (Trusted Publishing / OIDC), and creates a GitHub Release with the tag's compatibility matrix in its notes and assets.

### Upgrading upstream dependencies

Follow the [Agent Server upgrade policy](docs/compatibility.md#upstream-upgrade-policy).
Keep the API pin exact and evaluate new API versions in a dedicated PR branch.
Record runtime interface changes, migration requirements, and any remaining
incompatibilities in the PR. Update the compatibility matrix with the frozen
dependency set and actual test coverage. An upgrade must pass the first-party
Postgres + Redis tests and the upstream SDK integration suite, including the SDK
v2 and nested cursor-reconnect regressions, before merging or releasing.

### One-time PyPI + GitHub setup

- Create GitHub Environments **`pypi-langhost`** and **`pypi-runtime`** (Settings → Environments) with **required reviewers** so a human must approve before each upload.
- Pending / trusted publishers (same repo + workflow `release.yml`, different environments):
  - `langhost` → environment `pypi-langhost`
  - `langgraph-runtime-pg` → environment `pypi-runtime`
- Protect `main`: require the **Lint**, **Build**, and **Test** status checks before merge.
- The Release workflow runs the full CI suite first; each package publishes only after CI is green and its environment is approved.
