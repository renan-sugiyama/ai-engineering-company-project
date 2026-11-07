# Reporting API

This service factory exposes the business reporting routes separately from the telemetry API. Its host must supply two FastAPI dependencies to `create_reporting_app`: an authenticated internal-user dependency for status/KPI reads and an operator-authorization dependency for manual flow triggers. There is intentionally no default or fail-open authentication implementation in this package.

The included ASGI entry point uses the existing Auth API to validate the dashboard's Bearer session on read routes. `AUTH_API_URL` defaults to `http://127.0.0.1:8003`; configure it for the host network. Manual run triggers additionally require a comma-separated `REPORTING_OPERATOR_USER_IDS` allowlist of Auth API user IDs. If the allowlist is empty, the trigger route fails closed with `503`; authenticated users outside it receive `403`.

```python
from services.reporting.app.main import create_reporting_app

app = create_reporting_app(
    authenticate_user=host_authenticate_user,
    authorize_reporting_run=host_authorize_reporting_operator,
)
```

Routes are `GET /reporting/status`, `POST /reporting/runs`, and `GET /reporting/kpis`. Pipeline reads and deployment submission are delegated to `data/pipelines/support_performance.py`.

For local development, start the API from the repository root with `uvicorn services.reporting.app.asgi:app --host 127.0.0.1 --port 8005`. Set `REPORTING_API_URL=http://127.0.0.1:8005` for the Next.js dashboard. `start-all.sh` launches this service on port `8005` and exports the URL for the dashboard. KPI reads require an active session created by the existing Auth API; the dashboard forwards the HttpOnly session token only from its server component.

Set `REPORTING_DATABASE_URL` and `PREFECT_API_URL` through the host's secret/environment configuration. `PREFECT_SUPPORT_PERFORMANCE_DEPLOYMENT` must name the deployed Prefect flow. The trigger returns `503` until Prefect deployment settings are configured; it never runs the ETL inline in the API process.

For the local non-production Compose database, `start-all.sh` provisions the reporting schema and configures the reporting service connection. Run `./data/pipelines/run-local.sh` from the repository root to materialize the latest completed week and four-week lookback. The Compose database has no support-event producer data by default; successful KPI reads may therefore return `no_data` until such events are ingested.
