#!/usr/bin/env bash
# Source this file to configure the local non-production pipeline database URLs.

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  echo "Source data/pipelines/local_database_urls.sh from a Bash shell." >&2
  exit 2
fi

local_pipeline_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [ ! -f "$local_pipeline_root/.env.telemetry" ]; then
  echo "Missing .env.telemetry; local telemetry database passwords are required." >&2
  return 1
fi

if ! local_compose_config="$(docker compose \
  --env-file "$local_pipeline_root/.env.telemetry" \
  -f "$local_pipeline_root/docker-compose.yml" config --format json)"; then
  echo "Could not read the local PostgreSQL configuration." >&2
  return 1
fi

local_database_urls="$(printf '%s' "$local_compose_config" | python -c '
import json
import sys
from urllib.parse import quote

environment = json.load(sys.stdin)["services"]["telemetry-postgres"]["environment"]
host = "127.0.0.1:5433/telemetry"
def url(username, password):
  quoted_username = quote(username, safe="")
  quoted_password = quote(password, safe="")
  return f"postgresql://{quoted_username}:{quoted_password}@{host}"
print("\t".join((
    url("telemetry_ingest", environment["TELEMETRY_INGEST_PASSWORD"]),
    url("telemetry_ingest", environment["TELEMETRY_INGEST_PASSWORD"]),
    url("telemetry_owner", environment["TELEMETRY_OWNER_PASSWORD"]),
)))
')" || {
  echo "Could not construct local pipeline database URLs." >&2
  return 1
}

IFS=$'\t' read -r local_telemetry_url local_telemetry_read_url local_reporting_url <<< "$local_database_urls"
: "${TELEMETRY_DATABASE_URL:=$local_telemetry_url}"
: "${TELEMETRY_READ_DATABASE_URL:=$local_telemetry_read_url}"
: "${REPORTING_DATABASE_URL:=$local_reporting_url}"
export TELEMETRY_DATABASE_URL TELEMETRY_READ_DATABASE_URL REPORTING_DATABASE_URL
unset local_pipeline_root local_compose_config local_database_urls
unset local_telemetry_url local_telemetry_read_url local_reporting_url
