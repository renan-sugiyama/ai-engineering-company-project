# `data/pipelines` folder

This folder groups **all data pipelines in the monorepo** related to the company: ingestion, ETL/ELT, cleaning, transformation, and loading into analytical or production systems.

Each subfolder or file under `data/pipelines/` should represent **one pipeline or job set** (for example `sales-etl`, `telemetry-stream`, `customer-segmentation`) and include the required configuration (scripts, orchestration, connectors, schemas, etc.).

- **Main purpose**: consolidate in one place the data movement and transformation logic that powers the company’s applications and analytics.
- **Recommendation**: document pipelines as you add them—their goal, data sources and sinks, dependencies, and how to run them in development, testing, and production.

## Support performance pipeline

Install its Python dependencies from the repository root with:

```bash
python -m pip install -r data/pipelines/requirements.txt
```

The runtime receives `TELEMETRY_READ_DATABASE_URL` and `REPORTING_DATABASE_URL` from the deployment's secret/environment configuration. The telemetry connection must be read-only for `event_type`, `timestamp`, and `tags` on `public.telemetry_events`; the reporting connection writes only to the provisioned `reporting` tables. Do not put credentials in source files or logs. Reporting-table provisioning is managed separately from the pipeline dependency setup.

An authorized database administrator applies the reporting schema to the approved reporting database:

```bash
psql -v ON_ERROR_STOP=1 -f data/pipelines/reporting_schema.sql '<approved reporting database URL>'
```

The SQL creates non-login group roles (`reporting_pipeline_writer`, `reporting_api_reader`, and `reporting_audit_reader`) with scoped grants. After confirming the deployment's login roles, an administrator must grant the matching group membership; the repository does not assume or create login credentials. The pipeline reports source health as `unknown` until an approved external capture-health signal is configured.

Run the focused unit tests from the repository root with:

```bash
python -m unittest discover -s data/pipelines/tests -v
```

> _Spanish version: [README.es.md](./README.es.md)._
