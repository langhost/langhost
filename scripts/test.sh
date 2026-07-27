#!/usr/bin/env bash
# End-to-end local test runner (macOS/Linux):
#   compose up → sparse-clone upstream SDK → unit/queue → API server →
#   e2e → SDK integration → teardown
#
# Usage:
#   ./scripts/test.sh              # tag sdk==<langgraph-sdk pin>
#   ./scripts/test.sh main         # upstream main
#   UPSTREAM_LANGGRAPH_REF=<ref> ./scripts/test.sh
#
# Default ref is derived from the pinned langgraph-sdk version in the env
# (after uv sync): langchain-ai/langgraph tag "sdk==X.Y.Z".
#
# Unit/queue must run before the API server: those tests truncate PG and
# deadlock if the server already holds connections to the same DB.
#
# Test phases are hard-capped at E2E_DEADLINE_SECS (default 300).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

UPSTREAM_DIR="${ROOT}/.tests/langgraph_api"
UPSTREAM_REPO="${LANGGRAPH_UPSTREAM_REPO:-https://github.com/langchain-ai/langgraph.git}"

PORT="${LANGGRAPH_INTEGRATION_PORT:-2024}"
BASE_URL="http://127.0.0.1:${PORT}"
SERVER_LOG="$(mktemp -t lg-api-server.XXXXXX.log)"
SERVER_PID=""
SUITE_PID=""
WATCHDOG_PID=""
# sdk-py integration + slim first-party live-server E2E
E2E_DEADLINE_SECS="${E2E_DEADLINE_SECS:-300}"

export LANGGRAPH_RUNTIME_EDITION=pg
export DATABASE_URI="${DATABASE_URI:-postgresql+asyncpg://postgres:postgres@localhost:5432/langgraph}"
export REDIS_URI="${REDIS_URI:-redis://localhost:6379/0}"
export LG_RUNTIME_PG_TEST="${LG_RUNTIME_PG_TEST:-1}"

cleanup() {
  local code=$?
  set +e
  if [[ -n "${WATCHDOG_PID}" ]] && kill -0 "${WATCHDOG_PID}" 2>/dev/null; then
    kill "${WATCHDOG_PID}" 2>/dev/null || true
    wait "${WATCHDOG_PID}" 2>/dev/null || true
  fi
  if [[ -n "${SUITE_PID}" ]] && kill -0 "${SUITE_PID}" 2>/dev/null; then
    kill "${SUITE_PID}" 2>/dev/null || true
    # Kill the suite process group if we started one.
    kill -- "-${SUITE_PID}" 2>/dev/null || true
    wait "${SUITE_PID}" 2>/dev/null || true
  fi
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "==> stopping langgraph-api (pid ${SERVER_PID})"
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  echo "==> docker compose down"
  docker compose down --remove-orphans >/dev/null 2>&1 || true
  if [[ "${code}" -ne 0 && -f "${SERVER_LOG}" ]]; then
    echo "==> langgraph-api log (tail):" >&2
    tail -n 80 "${SERVER_LOG}" >&2 || true
  fi
  rm -f "${SERVER_LOG}"
  exit "${code}"
}
trap cleanup EXIT INT TERM

echo "==> uv sync"
uv sync --group dev # NOSONAR - editable workspace packages require build

SDK_VERSION="$(
  uv run python -c 'import importlib.metadata as m; print(m.version("langgraph-sdk"))' # NOSONAR
)"
DEFAULT_UPSTREAM_REF="sdk==${SDK_VERSION}"
UPSTREAM_REF="${1:-${UPSTREAM_LANGGRAPH_REF:-$DEFAULT_UPSTREAM_REF}}"
echo "    langgraph-sdk=${SDK_VERSION} → upstream ref=${UPSTREAM_REF}"

echo "==> docker compose up (postgres + redis)"
docker compose up -d postgres redis
for i in $(seq 1 60); do
  if docker compose exec -T postgres pg_isready -U postgres -d langgraph >/dev/null 2>&1 \
    && docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "    infra ready (${i}s)"
    break
  fi
  if [[ "${i}" -eq 60 ]]; then
    echo "error: postgres/redis not healthy" >&2
    docker compose ps >&2 || true
    exit 1
  fi
  sleep 1
done

echo "==> upstream sparse checkout → ${UPSTREAM_DIR} (ref=${UPSTREAM_REF})"
mkdir -p "$(dirname "${UPSTREAM_DIR}")"
if [[ ! -d "${UPSTREAM_DIR}/.git" ]]; then
  git clone --filter=blob:none --no-checkout "${UPSTREAM_REPO}" "${UPSTREAM_DIR}"
  git -C "${UPSTREAM_DIR}" sparse-checkout init --cone
  git -C "${UPSTREAM_DIR}" sparse-checkout set libs/sdk-py
fi
git -C "${UPSTREAM_DIR}" fetch --depth 1 origin "refs/tags/${UPSTREAM_REF}:refs/tags/${UPSTREAM_REF}" 2>/dev/null \
  || git -C "${UPSTREAM_DIR}" fetch --depth 1 origin "${UPSTREAM_REF}"
git -C "${UPSTREAM_DIR}" checkout --detach "FETCH_HEAD"
echo "    at $(git -C "${UPSTREAM_DIR}" rev-parse --short HEAD)"

echo "==> install upstream sdk-py (editable)"
# Editable local path may require a build; pinned deps use --no-build.
UV_NO_BUILD=0 uv pip install --quiet -e "${UPSTREAM_DIR}/libs/sdk-py" # NOSONAR
uv pip install --quiet --no-build "deepagents==0.6.12" "langchain==1.3.14"

INTEGRATION_DIR="${UPSTREAM_DIR}/libs/sdk-py/integration"
export N_JOBS_PER_WORKER="${N_JOBS_PER_WORKER:-2}"
export FF_OPTIMIZED_STREAMING=true
export LANGGRAPH_ALLOW_BLOCKING=true
export LANGGRAPH_AUTH_TYPE=noop
export LG_BG_JOB_HEARTBEAT=5
# Relative graph paths in langgraph.json resolve when cwd is integration/.
export LANGSERVE_GRAPHS="$(uv run python -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["graphs"]))' "${INTEGRATION_DIR}/langgraph.json")" # NOSONAR
echo "    graphs: $(uv run python -c 'import json,os; print(", ".join(json.loads(os.environ["LANGSERVE_GRAPHS"])))')"

echo "==> tests (wall-clock budget ${E2E_DEADLINE_SECS}s for pytest+server+SDK)"
(
  set -euo pipefail
  # Runtime tests truncate PG tables — must not run while the API server holds
  # connections to the same DB (that deadlocks truncate / claim).
  echo "==> first-party unit/queue tests"
  uv run pytest -q \
    libs/langgraph-runtime-pg/tests/test_interface.py \
    libs/langgraph-runtime-pg/tests/test_queue.py \
    libs/langhost/tests/test_cli.py \
    --tb=short

  echo "==> start langgraph-api (edition=pg) on ${BASE_URL}"
  (
    cd "${INTEGRATION_DIR}"
    # --project keeps repo venv; cwd stays integration/ so ./graph/... paths work.
    exec uv run --project "${ROOT}" uvicorn langgraph_api.server:app \
      --host 127.0.0.1 --port "${PORT}" --log-level info
  ) >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
  # Export SERVER_PID to parent via a file so cleanup can kill it.
  echo "${SERVER_PID}" >"${SERVER_LOG}.pid"
  for i in $(seq 1 90); do
    if curl -sf "${BASE_URL}/ok" >/dev/null; then
      echo "    server ready (${i}s)"
      break
    fi
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      echo "error: langgraph-api exited early; log:" >&2
      tail -n 200 "${SERVER_LOG}" >&2 || true
      exit 1
    fi
    if [[ "${i}" -eq 90 ]]; then
      echo "error: server not ready; log:" >&2
      tail -n 200 "${SERVER_LOG}" >&2 || true
      exit 1
    fi
    sleep 1
  done

  export LANGGRAPH_INTEGRATION_URL="${BASE_URL}"

  echo "==> first-party live-server E2E"
  uv run pytest -q libs/langgraph-runtime-pg/tests/test_e2e.py --tb=short # NOSONAR

  echo "==> SDK integration tests"
  # NOSONAR - integration test invocation intentionally uses project environment
  uv run pytest -q "${UPSTREAM_DIR}/libs/sdk-py/tests/integration" \
    -o "addopts=" -m integration --tb=short

  echo "==> all suites passed"
) &
SUITE_PID=$!

# Watchdog: kill suite (+ server) if wall clock exceeded.
(
  sleep "${E2E_DEADLINE_SECS}"
  echo "error: e2e exceeded ${E2E_DEADLINE_SECS}s — killing suite (likely hung)" >&2
  if [[ -f "${SERVER_LOG}.pid" ]]; then
    spid="$(cat "${SERVER_LOG}.pid" 2>/dev/null || true)"
    if [[ -n "${spid}" ]]; then
      kill "${spid}" 2>/dev/null || true
    fi
  fi
  if [[ -n "${SUITE_PID}" ]]; then
    kill "${SUITE_PID}" 2>/dev/null || true
    kill -- "-${SUITE_PID}" 2>/dev/null || true
  fi
) &
WATCHDOG_PID=$!

set +e
wait "${SUITE_PID}"
SUITE_CODE=$?
set -e

# Suite finished (or was killed) — stop watchdog.
if [[ -n "${WATCHDOG_PID}" ]] && kill -0 "${WATCHDOG_PID}" 2>/dev/null; then
  kill "${WATCHDOG_PID}" 2>/dev/null || true
  wait "${WATCHDOG_PID}" 2>/dev/null || true
  WATCHDOG_PID=""
fi

# Promote nested server pid for outer cleanup.
if [[ -f "${SERVER_LOG}.pid" ]]; then
  SERVER_PID="$(cat "${SERVER_LOG}.pid" 2>/dev/null || true)"
  rm -f "${SERVER_LOG}.pid"
fi
SUITE_PID=""

if [[ "${SUITE_CODE}" -ne 0 ]]; then
  if [[ "${SUITE_CODE}" -eq 143 || "${SUITE_CODE}" -eq 137 || "${SUITE_CODE}" -eq 130 ]]; then
    echo "error: e2e killed after ${E2E_DEADLINE_SECS}s budget (exit ${SUITE_CODE})" >&2
    exit 124
  fi
  exit "${SUITE_CODE}"
fi

echo "==> all suites passed"
# trap cleanup → stop server + docker compose down
