# Reporting API

This service factory exposes the business reporting routes separately from the telemetry API. Its host must supply two FastAPI dependencies to `create_reporting_app`: an authenticated internal-user dependency for status/KPI reads and an operator-authorization dependency for manual flow triggers. There is intentionally no default or fail-open authentication implementation in this package.

```python
from services.reporting.app.main import create_reporting_app

app = create_reporting_app(
    authenticate_user=host_authenticate_user,
    authorize_reporting_run=host_authorize_reporting_operator,
)
```

Routes are `GET /reporting/status`, `POST /reporting/runs`, and `GET /reporting/kpis`. Pipeline reads and deployment submission are delegated to `data/pipelines/support_performance.py`.

Set `REPORTING_DATABASE_URL` and `PREFECT_API_URL` through the host's secret/environment configuration. `PREFECT_SUPPORT_PERFORMANCE_DEPLOYMENT` must name the deployed Prefect flow. The trigger returns `503` until Prefect deployment settings are configured; it never runs the ETL inline in the API process.
