BEGIN;

CREATE SCHEMA IF NOT EXISTS reporting;
REVOKE ALL ON SCHEMA reporting FROM PUBLIC;

DO $$
BEGIN
    CREATE ROLE reporting_pipeline_writer NOLOGIN;
EXCEPTION WHEN duplicate_object THEN
    NULL;
END;
$$;

DO $$
BEGIN
    CREATE ROLE reporting_api_reader NOLOGIN;
EXCEPTION WHEN duplicate_object THEN
    NULL;
END;
$$;

DO $$
BEGIN
    CREATE ROLE reporting_audit_reader NOLOGIN;
EXCEPTION WHEN duplicate_object THEN
    NULL;
END;
$$;

CREATE TABLE IF NOT EXISTS reporting.pipeline_runs (
    run_id UUID PRIMARY KEY,
    flow_name TEXT NOT NULL,
    flow_version TEXT NOT NULL,
    trigger_source TEXT NOT NULL CHECK (trigger_source IN ('schedule', 'manual', 'backfill')),
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL CHECK (period_end > period_start),
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('Running', 'Completed', 'Failed', 'Skipped')),
    phase_checkpoint TEXT NOT NULL,
    heartbeat_at TIMESTAMPTZ,
    records_read BIGINT NOT NULL DEFAULT 0 CHECK (records_read >= 0),
    records_validated BIGINT NOT NULL DEFAULT 0 CHECK (records_validated >= 0),
    records_processed BIGINT NOT NULL DEFAULT 0 CHECK (records_processed >= 0),
    records_rejected BIGINT NOT NULL DEFAULT 0 CHECK (records_rejected >= 0),
    rows_written BIGINT NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
    source_event_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_source_event_at_by_type JSONB NOT NULL DEFAULT '{}'::jsonb,
    data_quality_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_heartbeat_at TIMESTAMPTZ,
    source_health_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (source_health_status IN ('healthy', 'silent', 'unknown')),
    supersedes_run_id UUID REFERENCES reporting.pipeline_runs(run_id),
    error_code TEXT,
    error_summary TEXT
);

ALTER TABLE reporting.pipeline_runs
    ADD COLUMN IF NOT EXISTS data_quality_summary JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS pipeline_runs_started_at_idx
    ON reporting.pipeline_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS pipeline_runs_period_status_idx
    ON reporting.pipeline_runs (period_start, status, ended_at DESC);

CREATE TABLE IF NOT EXISTS reporting.support_performance_weekly (
    period_start TIMESTAMPTZ PRIMARY KEY,
    period_end TIMESTAMPTZ NOT NULL CHECK (period_end > period_start),
    sla_met_ticket_count BIGINT NOT NULL CHECK (sla_met_ticket_count >= 0),
    resolved_ticket_count BIGINT NOT NULL CHECK (resolved_ticket_count >= 0),
    sla_attainment_rate DOUBLE PRECISION
        CHECK (sla_attainment_rate IS NULL OR sla_attainment_rate BETWEEN 0 AND 100),
    sla_attainment_status TEXT NOT NULL CHECK (sla_attainment_status IN ('ok', 'no_data')),
    resolution_time_sum_seconds BIGINT NOT NULL CHECK (resolution_time_sum_seconds >= 0),
    resolution_sample_count BIGINT NOT NULL CHECK (resolution_sample_count >= 0),
    average_resolution_seconds DOUBLE PRECISION
        CHECK (average_resolution_seconds IS NULL OR average_resolution_seconds >= 0),
    average_resolution_hours DOUBLE PRECISION
        CHECK (average_resolution_hours IS NULL OR average_resolution_hours >= 0),
    resolution_time_status TEXT NOT NULL CHECK (resolution_time_status IN ('ok', 'no_data')),
    bot_resolved_eligible_query_count BIGINT NOT NULL
        CHECK (bot_resolved_eligible_query_count >= 0),
    eligible_query_count BIGINT NOT NULL CHECK (eligible_query_count >= 0),
    automation_resolution_rate DOUBLE PRECISION
        CHECK (automation_resolution_rate IS NULL OR automation_resolution_rate BETWEEN 0 AND 100),
    automation_resolution_status TEXT NOT NULL
        CHECK (automation_resolution_status IN ('ok', 'no_data')),
    run_id UUID NOT NULL REFERENCES reporting.pipeline_runs(run_id),
    generated_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE reporting.support_performance_weekly
    ADD COLUMN IF NOT EXISTS sla_attainment_status TEXT NOT NULL DEFAULT 'no_data'
        CHECK (sla_attainment_status IN ('ok', 'no_data'));
ALTER TABLE reporting.support_performance_weekly
    ADD COLUMN IF NOT EXISTS resolution_time_status TEXT NOT NULL DEFAULT 'no_data'
        CHECK (resolution_time_status IN ('ok', 'no_data'));
ALTER TABLE reporting.support_performance_weekly
    ADD COLUMN IF NOT EXISTS automation_resolution_status TEXT NOT NULL DEFAULT 'no_data'
        CHECK (automation_resolution_status IN ('ok', 'no_data'));

CREATE TABLE IF NOT EXISTS reporting.support_queue_weekly (
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL CHECK (period_end > period_start),
    queue_id TEXT NOT NULL,
    open_ticket_count BIGINT CHECK (open_ticket_count IS NULL OR open_ticket_count >= 0),
    overdue_ticket_count BIGINT CHECK (overdue_ticket_count IS NULL OR overdue_ticket_count >= 0),
    unassigned_ticket_count BIGINT CHECK (unassigned_ticket_count IS NULL OR unassigned_ticket_count >= 0),
    active_agent_count BIGINT CHECK (active_agent_count IS NULL OR active_agent_count >= 0),
    oldest_open_age_seconds BIGINT
        CHECK (oldest_open_age_seconds IS NULL OR oldest_open_age_seconds >= 0),
    snapshot_at TIMESTAMPTZ,
    snapshot_age_seconds BIGINT CHECK (snapshot_age_seconds IS NULL OR snapshot_age_seconds >= 0),
    freshness_status TEXT NOT NULL CHECK (
        freshness_status IN ('fresh', 'stale', 'missing', 'ambiguous')
    ),
    run_id UUID NOT NULL REFERENCES reporting.pipeline_runs(run_id),
    generated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (period_start, queue_id),
    CHECK (freshness_status <> 'missing' OR snapshot_at IS NULL)
);

CREATE TABLE IF NOT EXISTS reporting.pipeline_run_events (
    run_id UUID NOT NULL REFERENCES reporting.pipeline_runs(run_id),
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    output_table TEXT NOT NULL,
    output_key TEXT NOT NULL,
    PRIMARY KEY (run_id, event_id)
);

GRANT USAGE ON SCHEMA reporting TO
    reporting_pipeline_writer,
    reporting_api_reader,
    reporting_audit_reader;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON reporting.support_performance_weekly, reporting.support_queue_weekly
    TO reporting_pipeline_writer;
GRANT SELECT, INSERT, UPDATE
    ON reporting.pipeline_runs TO reporting_pipeline_writer;
GRANT SELECT, INSERT
    ON reporting.pipeline_run_events TO reporting_pipeline_writer;
GRANT SELECT
    ON reporting.support_performance_weekly,
       reporting.support_queue_weekly,
       reporting.pipeline_runs
    TO reporting_api_reader;
GRANT SELECT
    ON reporting.pipeline_runs, reporting.pipeline_run_events
    TO reporting_audit_reader;

COMMIT;