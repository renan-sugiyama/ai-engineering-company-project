#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository_root"

if [ ! -x .venv/bin/python ]; then
  echo "Missing .venv/bin/python. Create the repository virtual environment and install data/pipelines/requirements.txt." >&2
  exit 1
fi

if ! source data/pipelines/local_database_urls.sh; then
  echo "Could not configure local pipeline database URLs." >&2
  exit 1
fi

exec .venv/bin/python data/pipelines/pipeline.py "$@"
