from __future__ import annotations

import unittest
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import json
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from psycopg import OperationalError
from prefect.states import Completed, Failed
from data.pipelines.pipeline import main as pipeline_main

from data.pipelines.support_performance import (
    EventValidationError,
    ReportingPeriodLocked,
    ReportingPeriodNotPublished,
    ReportingDeploymentNotConfigured,
    _changed_period_starts,
    _hold_reporting_period_locks,
    extract_support_events,
    identify_changed_reporting_periods,
    load_reporting_partitions,
    _event_in_requested_periods,
    _emit_fallback_run_log,
    _require_completed_task_state,
    _retry_transient_database_error,
    record_pipeline_run_progress,
    record_pipeline_run_start,
    record_pipeline_run_terminal_state,
    record_skipped_pipeline_run,
    _transformation_cache_key,
    _validate_source_record,
    _warn_if_optional_task_failed,
    get_reporting_periods,
    get_latest_run_status,
    query_support_performance_kpis,
    support_performance_flow,
    trigger_support_performance_flow,
    transform_support_metrics,
)

UTC = timezone.utc


class _QueryResult:
    def __init__(self, value=None) -> None:
        self.value = value

    def fetchone(self):
        return (self.value,)


class _MappingResult:
    def __init__(self, row=None, rows=None) -> None:
        self.row = row
        self.rows = rows or []

    def fetchone(self):
        return self.row

    def fetchall(self):
        return self.rows


class _RecordingTransaction:
    def __init__(self, connection) -> None:
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exception_type, _exception, _traceback):
        self.connection.transaction_outcomes.append(
            "rollback" if exception_type is not None else "commit"
        )
        return False


class _RecordingCursor:
    def __init__(self, connection) -> None:
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception, _traceback):
        return False

    def executemany(self, statement, parameters):
        self.connection.statements.append((statement, parameters))
        if self.connection.fail_on and self.connection.fail_on in statement:
            raise RuntimeError("synthetic database failure")


class _RecordingConnection:
    def __init__(self, lock_results=(), fail_on=None) -> None:
        self.lock_results = iter(lock_results)
        self.fail_on = fail_on
        self.statements = []
        self.transaction_outcomes = []

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception, _traceback):
        return False

    def execute(self, statement, parameters=None):
        self.statements.append((statement, parameters))
        if self.fail_on and self.fail_on in statement:
            raise RuntimeError("synthetic database failure")
        if "pg_try_advisory_lock" in statement:
            return _QueryResult(next(self.lock_results))
        return _QueryResult()

    def cursor(self):
        return _RecordingCursor(self)

    def transaction(self):
        return _RecordingTransaction(self)


def _event(event_id: str, event_type: str, timestamp: datetime, properties: dict) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": timestamp.isoformat(),
        "properties": properties,
    }


def _ticket(ticket_id: str, resolved_at: datetime, seconds: int, sla_met: bool) -> dict:
    return {
        "ticketId": ticket_id,
        "clientId": "client-a",
        "channel": "email",
        "openedAt": (resolved_at - timedelta(seconds=seconds)).isoformat(),
        "resolvedAt": resolved_at.isoformat(),
        "resolutionSeconds": seconds,
        "slaTargetSeconds": 86400,
        "slaMet": sla_met,
    }


def _query(query_id: str, eligible: bool, resolved: bool, method: str) -> dict:
    return {
        "queryId": query_id,
        "eligibleForAutomation": eligible,
        "resolutionMethod": method,
        "humanHandoff": method == "agent",
        "resolved": resolved,
    }


def _queue(queue_id: str, open_count: int) -> dict:
    return {
        "queueId": queue_id,
        "openTicketCount": open_count,
        "overdueTicketCount": 1,
        "unassignedTicketCount": 2,
        "activeAgentCount": 3,
        "oldestOpenAgeSeconds": 120,
    }


class ReportingPeriodsTests(unittest.TestCase):
    def test_default_periods_are_five_completed_madrid_weeks(self) -> None:
        now = datetime(2026, 11, 4, 12, tzinfo=UTC)
        periods = get_reporting_periods(now=now)

        self.assertEqual(len(periods), 5)
        madrid_end = periods[-1]["period_end"].astimezone(ZoneInfo("Europe/Madrid"))
        self.assertEqual(madrid_end.weekday(), 0)
        self.assertEqual(
            madrid_end.hour,
            0,
        )

    def test_explicit_period_requires_complete_local_week(self) -> None:
        with self.assertRaises(ValueError):
            get_reporting_periods(
                datetime(2026, 11, 2, tzinfo=UTC),
                datetime(2026, 11, 9, tzinfo=UTC),
            )


class PipelineCliTests(unittest.TestCase):
    def test_default_invocation_runs_latest_completed_periods(self) -> None:
        with (
            patch(
                "data.pipelines.pipeline.support_performance_flow",
                return_value={"status": "Completed", "run_id": "run-1"},
            ) as flow,
            redirect_stdout(StringIO()),
        ):
            exit_code = pipeline_main([])

        self.assertEqual(exit_code, 0)
        flow.assert_called_once_with(None, None, "manual")

    def test_explicit_bounds_are_parsed_and_forwarded(self) -> None:
        start = "2026-10-25T23:00:00Z"
        end = "2026-11-01T23:00:00Z"
        with (
            patch(
                "data.pipelines.pipeline.support_performance_flow",
                return_value={"status": "Completed"},
            ) as flow,
            redirect_stdout(StringIO()),
        ):
            exit_code = pipeline_main(
                [
                    "--period-start",
                    start,
                    "--period-end",
                    end,
                    "--trigger-source",
                    "backfill",
                ]
            )

        self.assertEqual(exit_code, 0)
        flow.assert_called_once_with(
            datetime(2026, 10, 25, 23, tzinfo=UTC),
            datetime(2026, 11, 1, 23, tzinfo=UTC),
            "backfill",
        )

    def test_partial_or_invalid_week_bounds_are_rejected(self) -> None:
        with self.assertRaises(SystemExit) as missing_bound:
            pipeline_main(["--period-start", "2026-10-25T23:00:00Z"])
        self.assertEqual(missing_bound.exception.code, 2)

        with self.assertRaises(SystemExit) as invalid_week:
            pipeline_main(
                [
                    "--period-start",
                    "2026-10-26T00:00:00Z",
                    "--period-end",
                    "2026-11-02T00:00:00Z",
                ]
            )
        self.assertEqual(invalid_week.exception.code, 2)

    def test_skipped_and_failed_runs_return_nonzero_without_raw_exception(self) -> None:
        with (
            patch(
                "data.pipelines.pipeline.support_performance_flow",
                return_value={"status": "Skipped"},
            ),
            redirect_stdout(StringIO()),
        ):
            self.assertEqual(pipeline_main([]), 2)

        error_output = StringIO()
        with (
            patch(
                "data.pipelines.pipeline.support_performance_flow",
                side_effect=RuntimeError("private database detail"),
            ),
            redirect_stderr(error_output),
        ):
            self.assertEqual(pipeline_main([]), 1)
        self.assertNotIn("private database detail", error_output.getvalue())


class ExtractSupportEventsTests(unittest.TestCase):
    def test_transport_retry_is_deduplicated_without_source_writes(self) -> None:
        period = {
            "period_start": datetime(2026, 11, 1, 23, tzinfo=UTC),
            "period_end": datetime(2026, 11, 8, 23, tzinfo=UTC),
        }
        resolved_at = period["period_start"] + timedelta(days=1)
        row = {
            "event_type": "support_ticket_resolved",
            "timestamp": resolved_at,
            "tags": {
                "event_id": "same-event",
                **_ticket("ticket-1", resolved_at, 60, True),
            },
        }
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.execute.return_value = _MappingResult(rows=[row, row.copy()])

        with (
            patch.dict("os.environ", {"TELEMETRY_READ_DATABASE_URL": "postgresql://test"}),
            patch(
                "data.pipelines.support_performance.psycopg.connect",
                return_value=connection,
            ),
        ):
            result = extract_support_events.fn([period])

        self.assertEqual(len(result["events"]), 1)
        self.assertEqual(result["events"][0]["event_id"], "same-event")
        self.assertEqual(result["records_processed"], 1)
        statement = connection.execute.call_args.args[0]
        self.assertIn("SELECT event_type, timestamp, tags", statement)
        self.assertNotRegex(statement, r"(?i)\b(insert|update|delete|merge)\b")


class TransformSupportMetricsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.period = {
            "period_start": datetime(2026, 11, 1, 23, tzinfo=UTC),
            "period_end": datetime(2026, 11, 8, 23, tzinfo=UTC),
        }

    def test_calculates_ticket_query_and_queue_metrics(self) -> None:
        end = self.period["period_end"]
        events = [
            _event(
                "ticket-old",
                "support_ticket_resolved",
                end - timedelta(days=3),
                _ticket("ticket-1", end - timedelta(days=3), 3600, False),
            ),
            _event(
                "ticket-new",
                "support_ticket_resolved",
                end - timedelta(days=2),
                _ticket("ticket-1", end - timedelta(days=2), 1800, True),
            ),
            _event(
                "ticket-2",
                "support_ticket_resolved",
                end - timedelta(days=1),
                _ticket("ticket-2", end - timedelta(days=1), 5400, False),
            ),
            _event("query-1", "support_query_closed", end - timedelta(days=1), _query("q1", True, True, "bot")),
            _event("query-2", "support_query_closed", end - timedelta(hours=12), _query("q2", True, False, "unresolved")),
            _event("query-3", "support_query_closed", end - timedelta(hours=6), _query("q3", False, True, "agent")),
            _event("queue-fresh", "support_queue_snapshot_recorded", end - timedelta(minutes=2), _queue("billing", 7)),
            _event("queue-stale", "support_queue_snapshot_recorded", end - timedelta(minutes=7), _queue("technical", 4)),
        ]

        result = transform_support_metrics.fn(events, [self.period])
        performance = result["performance_rows"][0]
        queues = {row["queue_id"]: row for row in result["queue_rows"]}

        self.assertEqual(performance["resolved_ticket_count"], 2)
        self.assertEqual(performance["sla_met_ticket_count"], 1)
        self.assertEqual(performance["sla_attainment_rate"], 50.0)
        self.assertEqual(performance["resolution_time_sum_seconds"], 7200)
        self.assertEqual(performance["average_resolution_hours"], 1.0)
        self.assertEqual(performance["eligible_query_count"], 2)
        self.assertEqual(performance["bot_resolved_eligible_query_count"], 1)
        self.assertEqual(performance["automation_resolution_rate"], 50.0)
        self.assertEqual(queues["billing"]["freshness_status"], "fresh")
        self.assertEqual(queues["billing"]["open_ticket_count"], 7)
        self.assertEqual(queues["technical"]["freshness_status"], "stale")
        self.assertIsNone(queues["technical"]["open_ticket_count"])
        self.assertNotIn(
            "queue-stale",
            {lineage_row["event_id"] for lineage_row in result["lineage"]},
        )
        self.assertTrue(all("event_timestamp" in row for row in result["lineage"]))

    def test_empty_period_returns_null_rates_and_missing_queue_snapshot(self) -> None:
        events = [
            _event(
                "queue-after-period",
                "support_queue_snapshot_recorded",
                self.period["period_end"] + timedelta(seconds=1),
                _queue("billing", 5),
            )
        ]

        result = transform_support_metrics.fn(events, [self.period])
        performance = result["performance_rows"][0]

        self.assertIsNone(performance["sla_attainment_rate"])
        self.assertEqual(performance["sla_attainment_status"], "no_data")
        self.assertIsNone(performance["average_resolution_hours"])
        self.assertIsNone(performance["automation_resolution_rate"])
        self.assertEqual(result["queue_rows"][0]["freshness_status"], "missing")
        self.assertIsNone(result["queue_rows"][0]["open_ticket_count"])

    def test_latest_query_outcome_and_queue_snapshot_are_selected(self) -> None:
        end = self.period["period_end"]
        events = [
            _event(
                "query-old",
                "support_query_closed",
                end - timedelta(days=2),
                _query("query-1", True, True, "bot"),
            ),
            _event(
                "query-latest",
                "support_query_closed",
                end - timedelta(days=1),
                _query("query-1", True, False, "agent"),
            ),
            _event(
                "queue-old",
                "support_queue_snapshot_recorded",
                end - timedelta(minutes=4),
                _queue("billing", 9),
            ),
            _event(
                "queue-latest",
                "support_queue_snapshot_recorded",
                end - timedelta(minutes=1),
                _queue("billing", 3),
            ),
        ]

        result = transform_support_metrics.fn(events, [self.period])

        performance = result["performance_rows"][0]
        self.assertEqual(performance["eligible_query_count"], 1)
        self.assertEqual(performance["bot_resolved_eligible_query_count"], 0)
        self.assertEqual(performance["automation_resolution_rate"], 0.0)
        self.assertEqual(result["queue_rows"][0]["open_ticket_count"], 3)
        self.assertEqual(
            {row["event_id"] for row in result["lineage"]},
            {"query-latest", "queue-latest"},
        )

    def test_equal_ticket_resolution_times_are_excluded_as_ambiguous(self) -> None:
        resolved_at = self.period["period_end"] - timedelta(days=1)
        events = [
            _event("ticket-a", "support_ticket_resolved", resolved_at, _ticket("t1", resolved_at, 5, True)),
            _event("ticket-b", "support_ticket_resolved", resolved_at, _ticket("t1", resolved_at, 10, False)),
        ]

        result = transform_support_metrics.fn(events, [self.period])

        self.assertEqual(result["performance_rows"][0]["resolved_ticket_count"], 0)
        self.assertEqual(result["quality_issues"]["ambiguous_ticket_resolution"], 1)

    def test_late_event_membership_changes_cache_key(self) -> None:
        parameters = {"events": [], "periods": [self.period]}
        original_key = _transformation_cache_key(None, parameters)
        changed_key = _transformation_cache_key(
            None,
            {
                "events": [
                    _event(
                        "late-event",
                        "support_query_closed",
                        self.period["period_end"] - timedelta(minutes=1),
                        _query("q1", True, True, "bot"),
                    )
                ],
                "periods": [self.period],
            },
        )
        self.assertNotEqual(original_key, changed_key)

    def test_invalid_payload_shapes_are_rejected_as_data_quality_errors(self) -> None:
        resolved_at = self.period["period_end"] - timedelta(days=1)
        properties = _ticket("ticket-1", resolved_at, 30, True)
        properties["channel"] = ["email"]
        row = {
            "event_type": "support_ticket_resolved",
            "timestamp": resolved_at,
            "tags": {"event_id": "event-1", **properties},
        }

        with self.assertRaises(EventValidationError):
            _validate_source_record(row)

        properties = _ticket("ticket-1", resolved_at, 30, True)
        properties["unexpected"] = "value"
        row["tags"] = {"event_id": "event-2", **properties}
        with self.assertRaises(EventValidationError):
            _validate_source_record(row)

    def test_event_window_uses_resolved_at_and_keeps_queue_history(self) -> None:
        resolved_at = self.period["period_start"] + timedelta(days=1)
        ticket = _validate_source_record(
            {
                "event_type": "support_ticket_resolved",
                "timestamp": datetime(2026, 1, 1, tzinfo=UTC),
                "tags": {
                    "event_id": "ticket-event",
                    **_ticket("ticket-1", resolved_at, 30, True),
                },
            }
        )
        old_queue_snapshot = _validate_source_record(
            {
                "event_type": "support_queue_snapshot_recorded",
                "timestamp": self.period["period_start"] - timedelta(days=30),
                "tags": {"event_id": "queue-event", **_queue("billing", 5)},
            }
        )

        self.assertTrue(_event_in_requested_periods(ticket, [self.period]))
        self.assertTrue(_event_in_requested_periods(old_queue_snapshot, [self.period]))

    def test_ticket_and_query_period_bounds_are_half_open(self) -> None:
        start = self.period["period_start"]
        end = self.period["period_end"]
        events = [
            _event("ticket-at-start", "support_ticket_resolved", start, _ticket("at-start", start, 1, True)),
            _event("ticket-at-end", "support_ticket_resolved", end, _ticket("at-end", end, 1, True)),
            _event("query-at-start", "support_query_closed", start, _query("at-start", True, True, "bot")),
            _event("query-at-end", "support_query_closed", end, _query("at-end", True, True, "bot")),
        ]

        result = transform_support_metrics.fn(events, [self.period])

        performance = result["performance_rows"][0]
        self.assertEqual(performance["resolved_ticket_count"], 1)
        self.assertEqual(performance["eligible_query_count"], 1)
        self.assertEqual(
            {row["event_id"] for row in result["lineage"]},
            {"ticket-at-start", "query-at-start"},
        )


class TaskConfigurationTests(unittest.TestCase):
    def test_database_tasks_have_bounded_retries(self) -> None:
        self.assertEqual(extract_support_events.retries, 3)
        self.assertEqual(extract_support_events.retry_delay_seconds, 10)
        self.assertEqual(load_reporting_partitions.retries, 3)
        self.assertEqual(load_reporting_partitions.retry_delay_seconds, 10)
        self.assertEqual(identify_changed_reporting_periods.retries, 3)
        self.assertEqual(identify_changed_reporting_periods.retry_delay_seconds, 10)
        self.assertEqual(record_pipeline_run_start.retries, 3)
        self.assertEqual(record_pipeline_run_progress.retries, 3)
        self.assertEqual(record_pipeline_run_terminal_state.retries, 3)
        self.assertEqual(record_skipped_pipeline_run.retries, 3)

    def test_only_transient_database_failures_are_retryable(self) -> None:
        self.assertTrue(
            _retry_transient_database_error(
                None,
                None,
                Failed(data=OperationalError("connection interrupted")),
            )
        )
        self.assertFalse(
            _retry_transient_database_error(
                None,
                None,
                Failed(data=ValueError("invalid event contract")),
            )
        )

    def test_transform_task_uses_expiring_persisted_cache(self) -> None:
        self.assertEqual(transform_support_metrics.cache_expiration, timedelta(hours=1))
        self.assertTrue(transform_support_metrics.persist_result)
        self.assertIsNotNone(transform_support_metrics.cache_key_fn)

    def test_latest_run_status_returns_not_run_without_metadata(self) -> None:
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.execute.side_effect = [
            _MappingResult(row=None),
            _MappingResult(row=None),
        ]
        with (
            patch.dict("os.environ", {"REPORTING_DATABASE_URL": "postgresql://test"}),
            patch("data.pipelines.support_performance.psycopg.connect", return_value=connection),
        ):
            result = get_latest_run_status()

        self.assertEqual(result["status"], "not_run")
        self.assertEqual(result["source_health_status"], "unknown")
        self.assertIsNone(result["last_successful_publication"])

    def test_kpi_query_returns_only_aggregate_fields(self) -> None:
        start = datetime(2026, 10, 25, 23, tzinfo=UTC)
        end = datetime(2026, 11, 1, 23, tzinfo=UTC)
        performance = {
            "period_start": start,
            "period_end": end,
            "generated_at": end,
            "run_id": "run-1",
            "sla_met_ticket_count": 8,
            "resolved_ticket_count": 10,
            "sla_attainment_rate": 80.0,
            "sla_attainment_status": "ok",
            "resolution_time_sum_seconds": 3600,
            "resolution_sample_count": 1,
            "average_resolution_seconds": 3600.0,
            "average_resolution_hours": 1.0,
            "resolution_time_status": "ok",
            "bot_resolved_eligible_query_count": 2,
            "eligible_query_count": 4,
            "automation_resolution_rate": 50.0,
            "automation_resolution_status": "ok",
        }
        queues = [
            {
                "queue_id": "billing",
                "open_ticket_count": 3,
                "overdue_ticket_count": 0,
                "unassigned_ticket_count": 1,
                "active_agent_count": 2,
                "oldest_open_age_seconds": 90,
                "snapshot_at": end - timedelta(seconds=30),
                "snapshot_age_seconds": 30,
                "freshness_status": "fresh",
            }
        ]
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.execute.side_effect = [
            _MappingResult(row=performance),
            _MappingResult(rows=queues),
        ]
        with (
            patch.dict("os.environ", {"REPORTING_DATABASE_URL": "postgresql://test"}),
            patch("data.pipelines.support_performance.psycopg.connect", return_value=connection),
        ):
            result = query_support_performance_kpis(start, end, "billing")

        self.assertEqual(result["sla_attainment"]["denominator"], 10)
        self.assertEqual(result["eligible_query_automation_resolution"]["rate_percent"], 50.0)
        self.assertEqual(result["queues"][0]["queue_id"], "billing")
        self.assertNotIn("event_id", repr(result))
        self.assertNotIn("ticket_id", repr(result))

    def test_kpi_query_marks_unpublished_period(self) -> None:
        start = datetime(2026, 10, 25, 23, tzinfo=UTC)
        end = datetime(2026, 11, 1, 23, tzinfo=UTC)
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.execute.return_value = _MappingResult(row=None)
        with (
            patch.dict("os.environ", {"REPORTING_DATABASE_URL": "postgresql://test"}),
            patch("data.pipelines.support_performance.psycopg.connect", return_value=connection),
            self.assertRaises(ReportingPeriodNotPublished),
        ):
            query_support_performance_kpis(start, end)

    def test_manual_trigger_uses_configured_deployment_and_canonical_run_id(self) -> None:
        start = datetime(2026, 10, 25, 23, tzinfo=UTC)
        end = datetime(2026, 11, 1, 23, tzinfo=UTC)
        prefect_run = SimpleNamespace(id="prefect-run-1")
        with (
            patch.dict(
                "os.environ",
                {
                    "PREFECT_API_URL": "http://prefect.internal/api",
                    "PREFECT_SUPPORT_PERFORMANCE_DEPLOYMENT": "support-performance/prod",
                },
            ),
            patch("data.pipelines.support_performance._find_active_reporting_run", return_value=None),
            patch("data.pipelines.support_performance.run_deployment", return_value=prefect_run) as submit,
        ):
            result = trigger_support_performance_flow(start, end)

        self.assertEqual(result["status"], "Scheduled")
        self.assertEqual(result["prefect_flow_run_id"], "prefect-run-1")
        self.assertEqual(submit.call_args.args[0], "support-performance/prod")
        self.assertEqual(submit.call_args.kwargs["parameters"]["run_id"], result["run_id"])

    def test_manual_trigger_fails_closed_without_deployment_settings(self) -> None:
        with (
            patch.dict(
                "os.environ",
                {
                    "PREFECT_API_URL": "",
                    "PREFECT_SUPPORT_PERFORMANCE_DEPLOYMENT": "",
                },
            ),
            self.assertRaises(ReportingDeploymentNotConfigured),
        ):
            trigger_support_performance_flow()

    def test_only_new_or_changed_lineage_periods_are_selected(self) -> None:
        first_period = "2026-10-25T23:00:00+00:00"
        second_period = "2026-11-01T23:00:00+00:00"
        transformed = {
            "performance_rows": [
                {"period_start": first_period},
                {"period_start": second_period},
            ],
            "lineage": [
                {"period_start": first_period, "event_id": "same-event"},
                {"period_start": second_period, "event_id": "new-event"},
            ],
        }

        changed = _changed_period_starts(
            transformed,
            {first_period: {"same-event"}},
        )

        self.assertEqual(changed, [second_period])

    def test_run_start_persists_running_status_and_unknown_source_health(self) -> None:
        period = {
            "period_start": datetime(2026, 11, 1, 23, tzinfo=UTC),
            "period_end": datetime(2026, 11, 8, 23, tzinfo=UTC),
        }
        connection = _RecordingConnection()
        with (
            patch.dict("os.environ", {"REPORTING_DATABASE_URL": "postgresql://test"}),
            patch(
                "data.pipelines.support_performance.psycopg.connect",
                return_value=connection,
            ),
        ):
            result = record_pipeline_run_start.fn("run-1", [period], "manual")

        statement, parameters = connection.statements[0]
        self.assertIn("'Running'", statement)
        self.assertIn("'unknown'", statement)
        self.assertEqual(parameters[0], "run-1")
        self.assertEqual(result["started_at"].endswith("+00:00"), True)

    def test_lock_loser_releases_any_locks_already_acquired(self) -> None:
        first_period = {
            "period_start": datetime(2026, 10, 25, 23, tzinfo=UTC),
            "period_end": datetime(2026, 11, 1, 23, tzinfo=UTC),
        }
        second_period = {
            "period_start": datetime(2026, 11, 1, 23, tzinfo=UTC),
            "period_end": datetime(2026, 11, 8, 23, tzinfo=UTC),
        }
        connection = _RecordingConnection(lock_results=[True, False])
        with (
            patch.dict("os.environ", {"REPORTING_DATABASE_URL": "postgresql://test"}),
            patch(
                "data.pipelines.support_performance.psycopg.connect",
                return_value=connection,
            ),
            self.assertRaises(ReportingPeriodLocked),
        ):
            with _hold_reporting_period_locks([first_period, second_period]):
                self.fail("lock acquisition should have failed")

        self.assertTrue(
            any("pg_advisory_unlock" in statement for statement, _ in connection.statements)
        )

    def test_period_output_and_lineage_commit_together(self) -> None:
        period_start = "2026-11-01T23:00:00+00:00"
        period_end = "2026-11-08T23:00:00+00:00"
        transformed = {
            "performance_rows": [
                {
                    "period_start": period_start,
                    "period_end": period_end,
                    "sla_met_ticket_count": 1,
                    "resolved_ticket_count": 1,
                    "sla_attainment_rate": 100.0,
                    "sla_attainment_status": "ok",
                    "resolution_time_sum_seconds": 60,
                    "resolution_sample_count": 1,
                    "average_resolution_seconds": 60.0,
                    "average_resolution_hours": 1 / 60,
                    "resolution_time_status": "ok",
                    "bot_resolved_eligible_query_count": 1,
                    "eligible_query_count": 1,
                    "automation_resolution_rate": 100.0,
                    "automation_resolution_status": "ok",
                }
            ],
            "queue_rows": [
                {
                    "period_start": period_start,
                    "period_end": period_end,
                    "queue_id": "billing",
                    "open_ticket_count": 2,
                    "overdue_ticket_count": 0,
                    "unassigned_ticket_count": 0,
                    "active_agent_count": 1,
                    "oldest_open_age_seconds": 30,
                    "snapshot_at": "2026-11-08T22:59:00+00:00",
                    "snapshot_age_seconds": 60,
                    "freshness_status": "fresh",
                }
            ],
            "lineage": [
                {
                    "event_id": "ticket-event",
                    "event_type": "support_ticket_resolved",
                    "event_timestamp": "2026-11-04T12:00:00+00:00",
                    "period_start": period_start,
                    "output_table": "reporting.support_performance_weekly",
                    "output_key": period_start,
                }
            ],
        }
        connection = _RecordingConnection()
        with (
            patch.dict("os.environ", {"REPORTING_DATABASE_URL": "postgresql://test"}),
            patch(
                "data.pipelines.support_performance.psycopg.connect",
                return_value=connection,
            ),
        ):
            result = load_reporting_partitions.fn(transformed, "run-1")

        self.assertEqual(result["rows_written"], 2)
        self.assertEqual(connection.transaction_outcomes, ["commit"])
        statements = [statement for statement, _ in connection.statements]
        self.assertTrue(any(statement.startswith("DELETE FROM reporting.support_queue_weekly") for statement in statements))
        self.assertTrue(any("INSERT INTO reporting.support_performance_weekly" in statement for statement in statements))
        self.assertTrue(any("INSERT INTO reporting.pipeline_run_events" in statement for statement in statements))
        self.assertTrue(any("UPDATE reporting.pipeline_runs" in statement for statement in statements))

    def test_partition_load_rolls_back_output_when_a_write_fails(self) -> None:
        period_start = "2026-11-01T23:00:00+00:00"
        period_end = "2026-11-08T23:00:00+00:00"
        performance_row = {
            "period_start": period_start,
            "period_end": period_end,
            "sla_met_ticket_count": 0,
            "resolved_ticket_count": 0,
            "sla_attainment_rate": None,
            "sla_attainment_status": "no_data",
            "resolution_time_sum_seconds": 0,
            "resolution_sample_count": 0,
            "average_resolution_seconds": None,
            "average_resolution_hours": None,
            "resolution_time_status": "no_data",
            "bot_resolved_eligible_query_count": 0,
            "eligible_query_count": 0,
            "automation_resolution_rate": None,
            "automation_resolution_status": "no_data",
        }
        transformed = {
            "performance_rows": [performance_row],
            "queue_rows": [
                {
                    "period_start": period_start,
                    "period_end": period_end,
                    "queue_id": "billing",
                    "open_ticket_count": None,
                    "overdue_ticket_count": None,
                    "unassigned_ticket_count": None,
                    "active_agent_count": None,
                    "oldest_open_age_seconds": None,
                    "snapshot_at": None,
                    "snapshot_age_seconds": None,
                    "freshness_status": "missing",
                }
            ],
            "lineage": [],
        }
        connection = _RecordingConnection(fail_on="INSERT INTO reporting.support_queue_weekly")
        with (
            patch.dict("os.environ", {"REPORTING_DATABASE_URL": "postgresql://test"}),
            patch(
                "data.pipelines.support_performance.psycopg.connect",
                return_value=connection,
            ),
            self.assertRaisesRegex(RuntimeError, "synthetic database failure"),
        ):
            load_reporting_partitions.fn(transformed, "run-1")

        self.assertEqual(connection.transaction_outcomes, ["rollback"])

    def test_optional_task_failure_does_not_fail_the_main_flow(self) -> None:
        extracted = {
            "events": [],
            "records_read": 0,
            "records_validated": 0,
            "records_processed": 0,
            "records_rejected": 0,
            "rejected_by_reason": {},
        }
        transformed = {
            "performance_rows": [],
            "queue_rows": [],
            "lineage": [],
            "records_processed": 0,
            "records_rejected": 0,
            "quality_issues": {},
            "source_counts": [],
        }
        with (
            patch(
                "data.pipelines.support_performance.extract_support_events",
                return_value=Completed(data=extracted),
            ),
            patch(
                "data.pipelines.support_performance._hold_reporting_period_locks",
                return_value=nullcontext(),
            ),
            patch(
                "data.pipelines.support_performance.record_pipeline_run_start",
                return_value=Completed(data={"started_at": "2026-11-07T00:00:00+00:00"}),
            ),
            patch(
                "data.pipelines.support_performance.record_pipeline_run_progress",
                return_value=Completed(data={"updated": True}),
            ),
            patch(
                "data.pipelines.support_performance.transform_support_metrics",
                return_value=Completed(data=transformed),
            ),
            patch(
                "data.pipelines.support_performance.identify_changed_reporting_periods",
                return_value=Completed(data={"changed_period_starts": []}),
            ),
            patch(
                "data.pipelines.support_performance.load_reporting_partitions",
                return_value=Completed(data={"rows_written": 0}),
            ),
            patch(
                "data.pipelines.support_performance.record_pipeline_run_terminal_state",
                return_value=Completed(data={"updated": True}),
            ),
            patch(
                "data.pipelines.support_performance.write_optional_eval_snapshot",
                return_value=Failed(message="optional output unavailable"),
            ),
        ):
            result = support_performance_flow.fn()

        self.assertEqual(result["records_processed"], 0)
        self.assertEqual(result["rows_written"], 0)

    def test_critical_task_failure_persists_sanitized_failed_run(self) -> None:
        with (
            patch(
                "data.pipelines.support_performance._hold_reporting_period_locks",
                return_value=nullcontext(),
            ),
            patch(
                "data.pipelines.support_performance.record_pipeline_run_start",
                return_value=Completed(data={"started_at": "2026-11-07T00:00:00+00:00"}),
            ),
            patch(
                "data.pipelines.support_performance.extract_support_events",
                return_value=Failed(message="sensitive database detail"),
            ),
            patch(
                "data.pipelines.support_performance.record_pipeline_run_terminal_state",
                return_value=Completed(data={"updated": True}),
            ) as terminal_state,
        ):
            with self.assertRaisesRegex(RuntimeError, "extract_support_events"):
                support_performance_flow.fn()

        terminal_args = terminal_state.call_args.args
        self.assertEqual(terminal_args[1], "Failed")
        self.assertEqual(terminal_args[3], "pipeline_task_failed")
        self.assertEqual(terminal_args[4], "extract phase failed")
        self.assertNotIn("sensitive database detail", repr(terminal_args))

    def test_lock_conflict_records_skipped_run(self) -> None:
        with (
            patch(
                "data.pipelines.support_performance._hold_reporting_period_locks",
                side_effect=ReportingPeriodLocked(),
            ),
            patch(
                "data.pipelines.support_performance.record_skipped_pipeline_run",
                return_value=Completed(data={"updated": True}),
            ) as skipped_run,
        ):
            result = support_performance_flow.fn()

        self.assertEqual(result["status"], "Skipped")
        skipped_run.assert_called_once()


class PrefectStateHandlingTests(unittest.TestCase):
    def test_critical_failed_task_raises_without_suppressing_flow_failure(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "extract_support_events"):
            _require_completed_task_state(
                Failed(message="database failure"),
                "extract_support_events",
            )

    def test_optional_failed_task_state_is_noncritical(self) -> None:
        _warn_if_optional_task_failed(
            Failed(message="local file unavailable"),
            "write_optional_eval_snapshot",
        )

    def test_fallback_log_contains_required_sanitized_metadata(self) -> None:
        started_at = datetime(2026, 11, 7, 12, tzinfo=UTC)
        with self.assertLogs("data.pipelines.support_performance", level="ERROR") as captured:
            _emit_fallback_run_log(
                "run-1",
                started_at,
                "Failed",
                12,
                "database_unavailable",
                "load",
            )

        payload = json.loads(captured.records[0].getMessage())
        self.assertEqual(payload["started_at"], started_at.isoformat())
        self.assertIn("ended_at", payload)
        self.assertEqual(payload["records_processed"], 12)
        self.assertEqual(payload["status"], "Failed")
        self.assertEqual(payload["errors"], {"code": "database_unavailable", "phase": "load"})


if __name__ == "__main__":
    unittest.main()