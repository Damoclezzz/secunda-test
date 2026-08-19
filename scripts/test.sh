#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

if [[ ! -f .env ]]; then
    echo "Create .env from .env.example before running tests" >&2
    exit 1
fi

set -a
source .env
set +a

test_project_name="secunda-test-tests"
compose_command=(
    docker compose
    --project-name "$test_project_name"
    --profile test
)

cleanup_test_infrastructure() {
    "${compose_command[@]}" down --volumes --remove-orphans
}

trap cleanup_test_infrastructure EXIT

uv sync
"${compose_command[@]}" up --detach --wait postgres-test rabbitmq-test
uv run ruff format --check .
uv run ruff check .
uv run mypy src tests migrations
uv run pytest -q
