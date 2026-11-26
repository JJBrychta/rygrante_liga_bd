#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PY_SCRIPT="${REPO_ROOT}/scripts/download_data.py"

needs_boto3=true
has_dest=false
has_env_file=false
for arg in "$@"; do
  case "$arg" in
    --verify-only|--url|--url=*|-h|--help)
      needs_boto3=false
      ;;
    --dest|--dest=*)
      has_dest=true
      ;;
    --env-file|--env-file=*)
      has_env_file=true
      ;;
  esac
done

local_args=()
if [[ "$has_env_file" == false && -f "${REPO_ROOT}/.env" ]]; then
  local_args+=(--env-file "${REPO_ROOT}/.env")
fi

if command -v uv >/dev/null 2>&1; then
  if [[ "$needs_boto3" == true ]]; then
    exec uv run --no-project --with boto3 python "$PY_SCRIPT" "${local_args[@]}" "$@"
  else
    exec uv run --no-project python "$PY_SCRIPT" "${local_args[@]}" "$@"
  fi
fi

if command -v python3 >/dev/null 2>&1; then
  if [[ "$needs_boto3" == false ]] || python3 -c "import boto3" >/dev/null 2>&1; then
    exec python3 "$PY_SCRIPT" "${local_args[@]}" "$@"
  fi
fi

if command -v docker >/dev/null 2>&1; then
  cd "$REPO_ROOT"
  extra_args=()
  if [[ "$has_dest" == false ]]; then
    extra_args+=(--dest /opt/workspace/data/landing)
  fi
  exec docker compose run --rm spark-master python3 /opt/workspace/scripts/download_data.py "${extra_args[@]}" "$@"
fi

echo "ERROR: Could not find 'uv', 'python3' (with boto3), or 'docker' to run download_data.py." >&2
echo "Install uv (https://docs.astral.sh/uv/) or start Docker Desktop and re-run." >&2
exit 1
