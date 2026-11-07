#!/usr/bin/env bash
# Starts every backend service and every UI. Stop everything with Ctrl+C.
#
# Stop a single service by port (the others keep running):
#   kill $(lsof -ti :8001)    # inventory-api
#   kill $(lsof -ti :5173)    # inventory-manager
# Ports: telemetry 8000, inventory 8001, dashboard-aggregator 8002, auth 8003,
#        incident 8004, reporting 8005, website 3000, dashboard 3001,
#        inventory UI 5173, incident UI 5174
#
# Free all ports after a crash or closed terminal:
#   kill $(lsof -ti :8000,:8001,:8002,:8003,:8004,:8005,:3000,:3001,:5173,:5174)

# To stop the databse (docker-compose) run the following command:
# docker compose --env-file .env.telemetry stop telemetry-postgres
set -u
cd "$(dirname "$0")"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required. Install and start Docker Desktop first." >&2
  exit 1
fi

if [ ! -f .env.telemetry ]; then
  echo "Missing .env.telemetry. Configure the local database passwords first." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not available. Start Docker Desktop and try again." >&2
  exit 1
fi

echo "Starting telemetry PostgreSQL (it will keep running after Ctrl+C)..."
if ! docker compose --env-file .env.telemetry up -d --wait telemetry-postgres; then
  echo "PostgreSQL startup failed; backend and UI processes were not started." >&2
  exit 1
fi

if ! telemetry_events_exists="$(docker compose --env-file .env.telemetry exec -T telemetry-postgres \
  psql -U postgres -d telemetry -tAc "SELECT to_regclass('public.telemetry_events') IS NOT NULL")"; then
  echo "Could not inspect the telemetry schema." >&2
  exit 1
fi

if [ "$telemetry_events_exists" = "f" ]; then
  echo "Initializing telemetry schema..."
  for migration in \
    001_create_telemetry_events.sql \
    002_allow_null_telemetry_service.sql \
    003_add_telemetry_value_message.sql \
    004_grant_telemetry_ingest_report_select.sql; do
    if ! docker compose --env-file .env.telemetry exec -T telemetry-postgres \
      psql -U postgres -d telemetry -v ON_ERROR_STOP=1 -f "/migrations/$migration"; then
      echo "Telemetry migration $migration failed." >&2
      exit 1
    fi
  done
else
  if ! docker compose --env-file .env.telemetry exec -T telemetry-postgres \
    psql -U postgres -d telemetry -v ON_ERROR_STOP=1 <<'SQL'
\getenv telemetry_owner_password TELEMETRY_OWNER_PASSWORD
\getenv telemetry_ingest_password TELEMETRY_INGEST_PASSWORD
\o /dev/null
SELECT format(
  'CREATE ROLE telemetry_owner LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
  :'telemetry_owner_password'
)
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'telemetry_owner')
\gexec
SELECT format(
  'CREATE ROLE telemetry_ingest LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
  :'telemetry_ingest_password'
)
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'telemetry_ingest')
\gexec
\o
SQL
  then
    echo "Telemetry database roles could not be verified or created." >&2
    exit 1
  fi

  if ! docker compose --env-file .env.telemetry exec -T telemetry-postgres \
    psql -U postgres -d telemetry -v ON_ERROR_STOP=1 \
    -c "ALTER SCHEMA public OWNER TO telemetry_owner" \
    -c "ALTER TABLE public.telemetry_events OWNER TO telemetry_owner"; then
    echo "Telemetry schema ownership could not be restored." >&2
    exit 1
  fi

  if ! docker compose --env-file .env.telemetry exec -T telemetry-postgres \
    psql -U postgres -d telemetry \
    -f /migrations/004_grant_telemetry_ingest_report_select.sql; then
    echo "Telemetry report read permissions could not be applied." >&2
    exit 1
  fi
fi

echo "Ensuring local reporting schema..."
if ! docker compose --env-file .env.telemetry exec -T telemetry-postgres \
  psql -U postgres -d telemetry -v ON_ERROR_STOP=1 \
  < data/pipelines/reporting_schema.sql; then
  echo "Reporting schema setup failed; backend and UI processes were not started." >&2
  exit 1
fi

if ! docker compose --env-file .env.telemetry exec -T telemetry-postgres \
  psql -U postgres -d telemetry -v ON_ERROR_STOP=1 \
  -c "GRANT reporting_pipeline_writer, reporting_api_reader TO telemetry_owner"; then
  echo "Local telemetry owner could not be granted reporting access." >&2
  exit 1
fi

if [ -f .venv/bin/activate ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

if ! source data/pipelines/local_database_urls.sh; then
  echo "Local pipeline database URLs could not be configured." >&2
  exit 1
fi

export AUTH_API_URL="${AUTH_API_URL:-http://127.0.0.1:8003}"
export REPORTING_API_URL="${REPORTING_API_URL:-http://127.0.0.1:8005}"

if [ -z "${REPORTING_DATABASE_URL:-}" ]; then
  echo "REPORTING_DATABASE_URL is unset; Reporting API will start, but KPI routes will be unavailable." >&2
fi

pids=()
cleanup() {
  trap - INT TERM EXIT
  echo "Stopping all processes..."
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null; done
  wait 2>/dev/null
}
trap cleanup INT TERM EXIT

run() {
  local name="$1"; shift
  echo "Starting $name"
  "$@" 2>&1 | sed -u "s/^/[$name] /" &
  pids+=($!)
}

# Backends
run telemetry-api  python -m uvicorn app.main:app --app-dir services/telemetry-api --reload --reload-dir services/telemetry-api --port 8000
run inventory-api  python -m uvicorn app.main:app --app-dir services/inventory-api --reload --reload-dir services/inventory-api --port 8001
run dashboard-api  python -m uvicorn main:app --app-dir services/dashboard-aggregator-api --reload --reload-dir services/dashboard-aggregator-api --port 8002
run auth-api       python -m uvicorn app.main:app --app-dir services/auth-api --reload --reload-dir services/auth-api --port 8003
run incident-api   python -m uvicorn app.main:app --app-dir services/incident-api --reload --reload-dir services/incident-api --port 8004
run reporting-api  python -m uvicorn services.reporting.app.asgi:app --reload --reload-dir services/reporting --reload-dir data/pipelines --port 8005

# Frontends (run `npm install` in each project once beforehand)
run website            npm --prefix uis/website run dev # --port 3000
run dashboard          npm --prefix uis/backoffice/dashboard run dev # --port 3001
run inventory-manager  npm --prefix uis/backoffice/inventory-manager run dev -- --port 5173
run incident-manager   npm --prefix uis/backoffice/incident-manager run dev -- --port 5174

wait
