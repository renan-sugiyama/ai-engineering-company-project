from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime, time, timedelta, timezone
from math import isfinite
from typing import Any, Generator, Literal
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import psycopg
from prefect import flow, task
from prefect.deployments import run_deployment
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

REPORTING_TIMEZONE = ZoneInfo("Europe/Madrid")
UTC = timezone.utc
EVENT_TYPES = (
    "support_ticket_resolved",
    "support_queue_snapshot_recorded",
    "support_query_closed",
)
TRANSFORM_VERSION = "support-performance-v1"
FLOW_VERSION = "1"
QUEUE_ID_PATTERN = re.compile(r"^[a-z0-9_]+$")
QUEUE_FRESHNESS_SECONDS = 5 * 60

# Three retries after the initial attempt absorb brief database outages while
# keeping each task's retry window bounded; contract and configuration errors do not retry.
DATABASE_RETRIES = 3
DATABASE_RETRY_DELAY_SECONDS = 10


def _retry_transient_database_error(_task: Any, _task_run: Any, state: Any) -> bool:
    if not state.is_failed():
        return False
    error = state.result(raise_on_failure=False)
    return isinstance(error, (psycopg.OperationalError, psycopg.InterfaceError))


class EventValidationError(ValueError):
    """An event does not satisfy the registered support-event contract."""


class ReportingPeriodLocked(RuntimeError):
    """Another flow execution owns at least one requested reporting-period lock."""

    def __init__(
        self,
        message: str = "A requested reporting period is already running",
        active_run: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.active_run = active_run


class ReportingPeriodNotPublished(LookupError):
    """The requested week has not been successfully materialized."""


class ReportingDeploymentNotConfigured(RuntimeError):
    """The Prefect API or deployment name is not configured for manual runs."""


def _reporting_database_url() -> str:
    database_url = os.getenv("REPORTING_DATABASE_URL")
    if not database_url:
        raise RuntimeError("REPORTING_DATABASE_URL must be configured")
    return database_url


@contextmanager
def _hold_reporting_period_locks(
    periods: list[dict[str, datetime]],
) -> Generator[None, None, None]:
    normalized_periods = [_normalize_period(period) for period in periods]
    lock_keys = [
        f"nexova:support_performance:{period['period_start'].isoformat()}"
        for period in sorted(normalized_periods, key=lambda item: item["period_start"])
    ]
    with psycopg.connect(_reporting_database_url(), autocommit=True) as connection:
        acquired_keys: list[str] = []
        try:
            for lock_key in lock_keys:
                acquired = connection.execute(
                    "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                    (lock_key,),
                ).fetchone()[0]
                if not acquired:
                    raise ReportingPeriodLocked("A requested reporting period is already running")
                acquired_keys.append(lock_key)
            yield
        finally:
            for lock_key in reversed(acquired_keys):
                connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                    (lock_key,),
                )


@task(
    name="record_pipeline_run_start",
    retries=DATABASE_RETRIES,
    retry_delay_seconds=DATABASE_RETRY_DELAY_SECONDS,
    retry_condition_fn=_retry_transient_database_error,
)
def record_pipeline_run_start(
    run_id: str,
    periods: list[dict[str, datetime]],
    trigger_source: str,
) -> dict[str, str]:
    normalized_periods = [_normalize_period(period) for period in periods]
    started_at = datetime.now(UTC)
    with psycopg.connect(_reporting_database_url()) as connection:
        connection.execute(
            """
            INSERT INTO reporting.pipeline_runs (
                run_id, flow_name, flow_version, trigger_source,
                period_start, period_end, started_at, status,
                phase_checkpoint, heartbeat_at, source_health_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, 'Running', 'started', %s, 'unknown'
            )
            """,
            (
                run_id,
                "support_performance_flow",
                FLOW_VERSION,
                trigger_source,
                min(period["period_start"] for period in normalized_periods),
                max(period["period_end"] for period in normalized_periods),
                started_at,
                started_at,
            ),
        )
    return {"started_at": started_at.isoformat()}


@task(
    name="record_pipeline_run_progress",
    retries=DATABASE_RETRIES,
    retry_delay_seconds=DATABASE_RETRY_DELAY_SECONDS,
    retry_condition_fn=_retry_transient_database_error,
)
def record_pipeline_run_progress(run_id: str, progress: dict[str, Any]) -> dict[str, bool]:
    now = datetime.now(UTC)
    with psycopg.connect(_reporting_database_url()) as connection:
        connection.execute(
            """
            UPDATE reporting.pipeline_runs
            SET phase_checkpoint = %s,
                heartbeat_at = %s,
                records_read = %s,
                records_validated = %s,
                records_processed = %s,
                records_rejected = %s,
                source_event_counts = %s,
                last_source_event_at_by_type = %s,
                data_quality_summary = %s,
                source_health_status = 'unknown'
            WHERE run_id = %s
            """,
            (
                progress["phase_checkpoint"],
                now,
                progress.get("records_read", 0),
                progress.get("records_validated", 0),
                progress.get("records_processed", 0),
                progress.get("records_rejected", 0),
                Jsonb(progress.get("source_event_counts", {})),
                Jsonb(progress.get("last_source_event_at_by_type", {})),
                Jsonb(progress.get("data_quality_summary", {})),
                run_id,
            ),
        )
    return {"updated": True}


@task(
    name="record_pipeline_run_terminal_state",
    retries=DATABASE_RETRIES,
    retry_delay_seconds=DATABASE_RETRY_DELAY_SECONDS,
    retry_condition_fn=_retry_transient_database_error,
)
def record_pipeline_run_terminal_state(
    run_id: str,
    status: Literal["Completed", "Failed"],
    phase_checkpoint: str,
    error_code: str | None = None,
    error_summary: str | None = None,
) -> dict[str, bool]:
    now = datetime.now(UTC)
    with psycopg.connect(_reporting_database_url()) as connection:
        connection.execute(
            """
            UPDATE reporting.pipeline_runs
            SET status = %s,
                ended_at = %s,
                heartbeat_at = %s,
                phase_checkpoint = CASE
                    WHEN %s = 'Failed' AND phase_checkpoint = 'load_committed'
                    THEN phase_checkpoint
                    ELSE %s
                END,
                error_code = %s,
                error_summary = %s
            WHERE run_id = %s
            """,
            (
                status,
                now,
                now,
                status,
                phase_checkpoint,
                error_code,
                error_summary,
                run_id,
            ),
        )
    return {"updated": True}


@task(
    name="record_skipped_pipeline_run",
    retries=DATABASE_RETRIES,
    retry_delay_seconds=DATABASE_RETRY_DELAY_SECONDS,
    retry_condition_fn=_retry_transient_database_error,
)
def record_skipped_pipeline_run(
    run_id: str,
    periods: list[dict[str, datetime]],
    trigger_source: str,
) -> None:
    normalized_periods = [_normalize_period(period) for period in periods]
    now = datetime.now(UTC)
    with psycopg.connect(_reporting_database_url()) as connection:
        connection.execute(
            """
            INSERT INTO reporting.pipeline_runs (
                run_id, flow_name, flow_version, trigger_source,
                period_start, period_end, started_at, ended_at, status,
                phase_checkpoint, heartbeat_at, source_health_status,
                error_code, error_summary
            ) VALUES (
                %s, 'support_performance_flow', %s, %s, %s, %s,
                %s, %s, 'Skipped', 'lock_skipped', %s, 'unknown',
                'period_lock_held', 'Another run owns a requested reporting period'
            )
            """,
            (
                run_id,
                FLOW_VERSION,
                trigger_source,
                min(period["period_start"] for period in normalized_periods),
                max(period["period_end"] for period in normalized_periods),
                now,
                now,
                now,
            ),
        )


def _source_run_metadata(events: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {event_type: 0 for event_type in EVENT_TYPES}
    latest: dict[str, str] = {}
    for event in events:
        event_type = event["event_type"]
        event_timestamp = _parse_timestamp(event["timestamp"], "timestamp")
        counts[event_type] += 1
        current_latest = latest.get(event_type)
        if current_latest is None or event_timestamp > _parse_timestamp(current_latest, "latest_event_at"):
            latest[event_type] = event_timestamp.isoformat()
    return {"source_event_counts": counts, "last_source_event_at_by_type": latest}


def _emit_fallback_run_log(
    run_id: str,
    started_at: datetime,
    status: str,
    records_processed: int,
    error_code: str,
    phase: str,
) -> None:
    logger.error(
        json.dumps(
            {
                "event": "pipeline_run_metadata_unavailable",
                "run_id": run_id,
                "started_at": started_at.isoformat(),
                "ended_at": datetime.now(UTC).isoformat(),
                "records_processed": records_processed,
                "status": status,
                "errors": {"code": error_code, "phase": phase},
            },
            sort_keys=True,
        )
    )


def _parse_timestamp(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise EventValidationError(f"invalid_{field_name}") from error
    else:
        raise EventValidationError(f"invalid_{field_name}")

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EventValidationError(f"naive_{field_name}")
    return parsed.astimezone(UTC)


def _require_string(properties: dict[str, Any], name: str) -> str:
    value = properties.get(name)
    if not isinstance(value, str) or not value.strip():
        raise EventValidationError(f"invalid_{name}")
    return value


def _require_boolean(properties: dict[str, Any], name: str) -> bool:
    value = properties.get(name)
    if not isinstance(value, bool):
        raise EventValidationError(f"invalid_{name}")
    return value


def _require_nonnegative_integer(properties: dict[str, Any], name: str) -> int:
    value = properties.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EventValidationError(f"invalid_{name}")
    return value


def _validate_properties(event_type: str, properties: dict[str, Any]) -> None:
    if event_type == "support_ticket_resolved":
        if set(properties) != {
            "ticketId",
            "clientId",
            "channel",
            "openedAt",
            "resolvedAt",
            "resolutionSeconds",
            "slaTargetSeconds",
            "slaMet",
        }:
            raise EventValidationError("invalid_ticket_fields")
        _require_string(properties, "ticketId")
        _require_string(properties, "clientId")
        channel = properties.get("channel")
        if not isinstance(channel, str) or channel not in {"phone", "email", "chat", "other"}:
            raise EventValidationError("invalid_channel")
        _parse_timestamp(properties.get("openedAt"), "opened_at")
        _parse_timestamp(properties.get("resolvedAt"), "resolved_at")
        _require_nonnegative_integer(properties, "resolutionSeconds")
        target = properties.get("slaTargetSeconds")
        if isinstance(target, bool) or not isinstance(target, int) or target != 86400:
            raise EventValidationError("invalid_sla_target")
        _require_boolean(properties, "slaMet")
        return

    if event_type == "support_query_closed":
        if set(properties) != {
            "queryId",
            "eligibleForAutomation",
            "resolutionMethod",
            "humanHandoff",
            "resolved",
        }:
            raise EventValidationError("invalid_query_fields")
        _require_string(properties, "queryId")
        _require_boolean(properties, "eligibleForAutomation")
        resolution_method = properties.get("resolutionMethod")
        if not isinstance(resolution_method, str) or resolution_method not in {
            "bot",
            "agent",
            "unresolved",
        }:
            raise EventValidationError("invalid_resolution_method")
        _require_boolean(properties, "humanHandoff")
        _require_boolean(properties, "resolved")
        return

    if event_type == "support_queue_snapshot_recorded":
        if set(properties) != {
            "queueId",
            "openTicketCount",
            "overdueTicketCount",
            "unassignedTicketCount",
            "activeAgentCount",
            "oldestOpenAgeSeconds",
        }:
            raise EventValidationError("invalid_queue_fields")
        queue_id = _require_string(properties, "queueId")
        if QUEUE_ID_PATTERN.fullmatch(queue_id) is None:
            raise EventValidationError("invalid_queue_id")
        for field_name in (
            "openTicketCount",
            "overdueTicketCount",
            "unassignedTicketCount",
            "activeAgentCount",
            "oldestOpenAgeSeconds",
        ):
            _require_nonnegative_integer(properties, field_name)
        return

    raise EventValidationError("unapproved_event_type")


def _validate_source_record(row: dict[str, Any]) -> dict[str, Any]:
    event_type = row.get("event_type")
    if event_type not in EVENT_TYPES:
        raise EventValidationError("unapproved_event_type")

    tags = row.get("tags")
    if not isinstance(tags, dict):
        raise EventValidationError("invalid_tags")
    event_id = tags.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        raise EventValidationError("invalid_event_id")

    properties = {key: value for key, value in tags.items() if key != "event_id"}
    if "message" in properties:
        if properties.pop("message") is not None:
            raise EventValidationError("invalid_message")
    _validate_properties(event_type, properties)

    return {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": _parse_timestamp(row.get("timestamp"), "timestamp").isoformat(),
        "properties": properties,
    }


def _event_in_requested_periods(
    event: dict[str, Any],
    periods: list[dict[str, datetime]],
) -> bool:
    event_time = _event_business_time(event)
    if event["event_type"] == "support_queue_snapshot_recorded":
        return event_time <= max(period["period_end"] for period in periods)
    return any(
        period["period_start"] <= event_time < period["period_end"]
        for period in periods
    )


def _normalize_period(period: dict[str, datetime]) -> dict[str, datetime]:
    start = _parse_timestamp(period.get("period_start"), "period_start")
    end = _parse_timestamp(period.get("period_end"), "period_end")
    local_start = start.astimezone(REPORTING_TIMEZONE)
    local_end = end.astimezone(REPORTING_TIMEZONE)
    if (
        local_start.weekday() != 0
        or local_start.time() != time.min
        or local_end.time() != time.min
        or (local_end.date() - local_start.date()).days != 7
    ):
        raise ValueError("Reporting periods must be complete Monday-to-Monday Europe/Madrid weeks")
    return {"period_start": start, "period_end": end}


def get_reporting_periods(
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    now: datetime | None = None,
) -> list[dict[str, datetime]]:
    if (period_start is None) != (period_end is None):
        raise ValueError("period_start and period_end must be supplied together")

    if period_start is not None and period_end is not None:
        return [_normalize_period({"period_start": period_start, "period_end": period_end})]

    current_time = now or datetime.now(UTC)
    if current_time.tzinfo is None or current_time.utcoffset() is None:
        raise ValueError("now must include a timezone")
    local_now = current_time.astimezone(REPORTING_TIMEZONE)
    current_week_monday = local_now.date() - timedelta(days=local_now.weekday())
    latest_completed_end = datetime.combine(
        current_week_monday,
        time.min,
        tzinfo=REPORTING_TIMEZONE,
    )

    periods: list[dict[str, datetime]] = []
    for weeks_ago in range(5, 0, -1):
        start_local = latest_completed_end - timedelta(days=7 * weeks_ago)
        end_local = start_local + timedelta(days=7)
        periods.append(
            {
                "period_start": start_local.astimezone(UTC),
                "period_end": end_local.astimezone(UTC),
            }
        )
    return periods


@task(
    name="extract_support_events",
    retries=DATABASE_RETRIES,
    retry_delay_seconds=DATABASE_RETRY_DELAY_SECONDS,
    retry_condition_fn=_retry_transient_database_error,
)
def extract_support_events(periods: list[dict[str, datetime]]) -> dict[str, Any]:
    normalized_periods = [_normalize_period(period) for period in periods]
    if not normalized_periods:
        raise ValueError("At least one reporting period is required")
    database_url = os.getenv("TELEMETRY_READ_DATABASE_URL")
    if not database_url:
        raise RuntimeError("TELEMETRY_READ_DATABASE_URL must be configured")

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        rows = connection.execute(
            """
            SELECT event_type, timestamp, tags
            FROM public.telemetry_events
            WHERE event_type = ANY(%s)
            """,
            (list(EVENT_TYPES),),
        ).fetchall()

    validated_events: dict[str, dict[str, Any]] = {}
    rejected_by_reason: dict[str, int] = {}
    conflicting_event_ids: set[str] = set()
    for row in rows:
        try:
            event = _validate_source_record(row)
        except EventValidationError as error:
            reason = str(error)
            rejected_by_reason[reason] = rejected_by_reason.get(reason, 0) + 1
            continue

        event_id = event["event_id"]
        prior_event = validated_events.get(event_id)
        if prior_event is not None:
            if prior_event != event:
                conflicting_event_ids.add(event_id)
            continue
        validated_events[event_id] = event

    for event_id in conflicting_event_ids:
        validated_events.pop(event_id, None)
    if conflicting_event_ids:
        rejected_by_reason["duplicate_event_id"] = len(conflicting_event_ids)

    all_valid_events = list(validated_events.values())
    events = [
        event
        for event in all_valid_events
        if _event_in_requested_periods(event, normalized_periods)
    ]
    events.sort(key=lambda event: (event["timestamp"], event["event_id"]))
    return {
        "events": events,
        "periods": normalized_periods,
        "records_read": len(rows),
        "records_validated": len(all_valid_events),
        "records_processed": len(events),
        "records_rejected": sum(rejected_by_reason.values()),
        "records_excluded_by_window": len(all_valid_events) - len(events),
        "rejected_by_reason": rejected_by_reason,
    }


def _event_business_time(event: dict[str, Any]) -> datetime:
    if event["event_type"] == "support_ticket_resolved":
        value = event["properties"]["resolvedAt"]
        return _parse_timestamp(value, "resolved_at")
    return _parse_timestamp(event["timestamp"], "timestamp")


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return 100.0 * numerator / denominator


def _validate_normalized_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(events, list):
        raise EventValidationError("invalid_events")

    validated_by_event_id: dict[str, dict[str, Any]] = {}
    for event in events:
        if not isinstance(event, dict):
            raise EventValidationError("invalid_event")
        event_type = event.get("event_type")
        if event_type not in EVENT_TYPES:
            continue
        properties = event.get("properties")
        if not isinstance(properties, dict):
            raise EventValidationError("invalid_properties")
        tags = dict(properties)
        tags["event_id"] = event.get("event_id")
        normalized = _validate_source_record(
            {
                "event_type": event_type,
                "timestamp": event.get("timestamp"),
                "tags": tags,
            }
        )
        prior = validated_by_event_id.get(normalized["event_id"])
        if prior is not None and prior != normalized:
            raise EventValidationError("conflicting_event_id")
        validated_by_event_id[normalized["event_id"]] = normalized
    return list(validated_by_event_id.values())


def _validate_metric_events(
    events: list[dict[str, Any]],
    event_type: str,
) -> list[dict[str, Any]]:
    if not isinstance(events, list):
        raise EventValidationError("invalid_events")
    selected_events: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            raise EventValidationError("invalid_event")
        if event.get("event_type") == event_type:
            selected_events.append(event)
    return _validate_normalized_events(selected_events)


def calculate_sla_attainment(ticket_events: list[dict[str, Any]]) -> dict[str, Any]:
    tickets = _validate_metric_events(ticket_events, "support_ticket_resolved")
    resolved_count = len(tickets)
    sla_met_count = sum(event["properties"]["slaMet"] for event in tickets)
    return {
        "sla_met_ticket_count": sla_met_count,
        "resolved_ticket_count": resolved_count,
        "sla_attainment_rate": _rate(sla_met_count, resolved_count),
        "sla_attainment_status": "ok" if resolved_count else "no_data",
    }


def calculate_average_resolution_time(
    ticket_events: list[dict[str, Any]],
) -> dict[str, Any]:
    tickets = _validate_metric_events(ticket_events, "support_ticket_resolved")
    resolution_seconds = [
        event["properties"]["resolutionSeconds"]
        for event in tickets
        if isinstance(event["properties"].get("resolutionSeconds"), (int, float))
        and not isinstance(event["properties"].get("resolutionSeconds"), bool)
        and isfinite(event["properties"]["resolutionSeconds"])
        and event["properties"]["resolutionSeconds"] >= 0
    ]
    resolution_sum = sum(resolution_seconds)
    sample_count = len(resolution_seconds)
    average_seconds = resolution_sum / sample_count if sample_count else None
    return {
        "resolution_time_sum_seconds": resolution_sum,
        "resolution_sample_count": sample_count,
        "average_resolution_seconds": average_seconds,
        "average_resolution_hours": average_seconds / 3600 if average_seconds is not None else None,
        "resolution_time_status": "ok" if sample_count else "no_data",
    }


def calculate_eligible_query_automation_resolution(
    query_events: list[dict[str, Any]],
) -> dict[str, Any]:
    queries = _validate_metric_events(query_events, "support_query_closed")
    eligible_queries = [
        event for event in queries if event["properties"]["eligibleForAutomation"]
    ]
    bot_resolved_count = sum(
        event["properties"]["resolved"]
        and event["properties"]["resolutionMethod"] == "bot"
        for event in eligible_queries
    )
    eligible_count = len(eligible_queries)
    return {
        "bot_resolved_eligible_query_count": bot_resolved_count,
        "eligible_query_count": eligible_count,
        "automation_resolution_rate": _rate(bot_resolved_count, eligible_count),
        "automation_resolution_status": "ok" if eligible_count else "no_data",
    }


def calculate_week_end_support_backlog(
    queue_events: list[dict[str, Any]],
    period: dict[str, datetime],
) -> dict[str, Any]:
    normalized_period = _normalize_period(period)
    period_start = normalized_period["period_start"]
    period_end = normalized_period["period_end"]
    snapshots = _validate_metric_events(queue_events, "support_queue_snapshot_recorded")
    queue_ids = sorted({event["properties"]["queueId"] for event in snapshots})
    queue_rows: list[dict[str, Any]] = []
    lineage: list[dict[str, Any]] = []
    quality_issues: dict[str, int] = {}

    for queue_id in queue_ids:
        candidates = [
            event
            for event in snapshots
            if event["properties"]["queueId"] == queue_id
            and _event_business_time(event) <= period_end
        ]
        if not candidates:
            queue_rows.append(
                {
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                    "queue_id": queue_id,
                    "open_ticket_count": None,
                    "overdue_ticket_count": None,
                    "unassigned_ticket_count": None,
                    "active_agent_count": None,
                    "oldest_open_age_seconds": None,
                    "snapshot_at": None,
                    "snapshot_age_seconds": None,
                    "freshness_status": "missing",
                }
            )
            continue

        latest_time = max(_event_business_time(event) for event in candidates)
        latest_events = [
            event for event in candidates if _event_business_time(event) == latest_time
        ]
        if len(latest_events) != 1:
            quality_issues["ambiguous_queue_snapshot"] = (
                quality_issues.get("ambiguous_queue_snapshot", 0) + 1
            )
            selected_snapshot = None
        else:
            selected_snapshot = latest_events[0]

        if selected_snapshot is None:
            queue_rows.append(
                {
                    "period_start": period_start.isoformat(),
                    "period_end": period_end.isoformat(),
                    "queue_id": queue_id,
                    "open_ticket_count": None,
                    "overdue_ticket_count": None,
                    "unassigned_ticket_count": None,
                    "active_agent_count": None,
                    "oldest_open_age_seconds": None,
                    "snapshot_at": latest_time.isoformat(),
                    "snapshot_age_seconds": int((period_end - latest_time).total_seconds()),
                    "freshness_status": "ambiguous",
                }
            )
            continue

        snapshot_time = _event_business_time(selected_snapshot)
        snapshot_age = max(0, int((period_end - snapshot_time).total_seconds()))
        properties = selected_snapshot["properties"]
        snapshot_is_fresh = snapshot_age <= QUEUE_FRESHNESS_SECONDS
        queue_rows.append(
            {
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
                "queue_id": queue_id,
                "open_ticket_count": properties["openTicketCount"] if snapshot_is_fresh else None,
                "overdue_ticket_count": properties["overdueTicketCount"] if snapshot_is_fresh else None,
                "unassigned_ticket_count": properties["unassignedTicketCount"] if snapshot_is_fresh else None,
                "active_agent_count": properties["activeAgentCount"] if snapshot_is_fresh else None,
                "oldest_open_age_seconds": properties["oldestOpenAgeSeconds"] if snapshot_is_fresh else None,
                "snapshot_at": snapshot_time.isoformat(),
                "snapshot_age_seconds": snapshot_age,
                "freshness_status": "fresh" if snapshot_is_fresh else "stale",
            }
        )
        if snapshot_is_fresh:
            lineage.append(
                {
                    "event_id": selected_snapshot["event_id"],
                    "event_type": selected_snapshot["event_type"],
                    "event_timestamp": selected_snapshot["timestamp"],
                    "period_start": period_start.isoformat(),
                    "output_table": "reporting.support_queue_weekly",
                    "output_key": f"{period_start.isoformat()}:{queue_id}",
                }
            )

    return {"queue_rows": queue_rows, "lineage": lineage, "quality_issues": quality_issues}


def _canonical_json_value(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported cache-key value: {type(value).__name__}")


def _transformation_cache_key(_context: Any, parameters: dict[str, Any]) -> str:
    # Period bounds, full source payloads/event IDs, and this version define the
    # key; the one-hour expiration only suppresses duplicate recent transforms.
    fingerprint = {
        "version": TRANSFORM_VERSION,
        "periods": parameters["periods"],
        "events": parameters["events"],
    }
    encoded = json.dumps(
        fingerprint,
        sort_keys=True,
        separators=(",", ":"),
        default=_canonical_json_value,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@task(
    name="transform_support_metrics",
    cache_key_fn=_transformation_cache_key,
    cache_expiration=timedelta(hours=1),
    persist_result=True,
)
def transform_support_metrics(
    events: list[dict[str, Any]],
    periods: list[dict[str, datetime]],
) -> dict[str, Any]:
    normalized_periods = [_normalize_period(period) for period in periods]
    valid_events = _validate_normalized_events(events)
    queue_events = [
        event for event in valid_events if event["event_type"] == "support_queue_snapshot_recorded"
    ]

    performance_rows: list[dict[str, Any]] = []
    queue_rows: list[dict[str, Any]] = []
    lineage: list[dict[str, Any]] = []
    quality_issues: dict[str, int] = {}
    source_counts: list[dict[str, Any]] = []

    for period in normalized_periods:
        period_start = period["period_start"]
        period_end = period["period_end"]
        ticket_groups: dict[str, list[dict[str, Any]]] = {}
        query_groups: dict[str, list[dict[str, Any]]] = {}
        period_counts = {event_type: 0 for event_type in EVENT_TYPES}

        for event in valid_events:
            event_time = _event_business_time(event)
            event_type = event["event_type"]
            if event_type == "support_queue_snapshot_recorded":
                if event_time <= period_end:
                    period_counts[event_type] += 1
            elif period_start <= event_time < period_end:
                period_counts[event_type] += 1
                if event_type == "support_ticket_resolved":
                    ticket_groups.setdefault(event["properties"]["ticketId"], []).append(event)
                elif event_type == "support_query_closed":
                    query_groups.setdefault(event["properties"]["queryId"], []).append(event)

        selected_tickets: list[dict[str, Any]] = []
        for ticket_events in ticket_groups.values():
            latest_time = max(_event_business_time(event) for event in ticket_events)
            latest_events = [
                event for event in ticket_events if _event_business_time(event) == latest_time
            ]
            if len(latest_events) != 1:
                quality_issues["ambiguous_ticket_resolution"] = (
                    quality_issues.get("ambiguous_ticket_resolution", 0) + 1
                )
                continue
            selected_tickets.append(latest_events[0])

        selected_queries: list[dict[str, Any]] = []
        for query_events in query_groups.values():
            latest_time = max(_event_business_time(event) for event in query_events)
            latest_events = [
                event for event in query_events if _event_business_time(event) == latest_time
            ]
            if len(latest_events) != 1:
                quality_issues["ambiguous_query_outcome"] = (
                    quality_issues.get("ambiguous_query_outcome", 0) + 1
                )
                continue
            selected_queries.append(latest_events[0])

        sla_metrics = calculate_sla_attainment(selected_tickets)
        resolution_metrics = calculate_average_resolution_time(selected_tickets)
        automation_metrics = calculate_eligible_query_automation_resolution(selected_queries)

        performance_rows.append(
            {
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
                **sla_metrics,
                **resolution_metrics,
                **automation_metrics,
            }
        )

        for event in selected_tickets:
            lineage.append(
                {
                    "event_id": event["event_id"],
                    "event_type": event["event_type"],
                    "event_timestamp": event["timestamp"],
                    "period_start": period_start.isoformat(),
                    "output_table": "reporting.support_performance_weekly",
                    "output_key": period_start.isoformat(),
                }
            )
        eligible_queries = [
            event
            for event in selected_queries
            if event["properties"]["eligibleForAutomation"]
        ]
        for event in eligible_queries:
            lineage.append(
                {
                    "event_id": event["event_id"],
                    "event_type": event["event_type"],
                    "event_timestamp": event["timestamp"],
                    "period_start": period_start.isoformat(),
                    "output_table": "reporting.support_performance_weekly",
                    "output_key": period_start.isoformat(),
                }
            )

        queue_result = calculate_week_end_support_backlog(queue_events, period)
        queue_rows.extend(queue_result["queue_rows"])
        lineage.extend(queue_result["lineage"])
        for issue, count in queue_result["quality_issues"].items():
            quality_issues[issue] = quality_issues.get(issue, 0) + count

        source_counts.append(
            {
                "period_start": period_start.isoformat(),
                "counts": period_counts,
            }
        )

    return {
        "performance_rows": performance_rows,
        "queue_rows": queue_rows,
        "lineage": lineage,
        "records_processed": len(valid_events),
        "records_rejected": 0,
        "quality_issues": quality_issues,
        "source_counts": source_counts,
    }


def _changed_period_starts(
    transformed: dict[str, Any],
    previous_event_ids: dict[str, set[str]],
) -> list[str]:
    current_event_ids: dict[str, set[str]] = {}
    for lineage_row in transformed["lineage"]:
        current_event_ids.setdefault(lineage_row["period_start"], set()).add(
            lineage_row["event_id"]
        )

    changed_periods: list[str] = []
    for performance_row in transformed["performance_rows"]:
        period_start = performance_row["period_start"]
        if period_start not in previous_event_ids:
            changed_periods.append(period_start)
        elif current_event_ids.get(period_start, set()) != previous_event_ids[period_start]:
            changed_periods.append(period_start)
    return changed_periods


@task(
    name="identify_changed_reporting_periods",
    retries=DATABASE_RETRIES,
    retry_delay_seconds=DATABASE_RETRY_DELAY_SECONDS,
    retry_condition_fn=_retry_transient_database_error,
)
def identify_changed_reporting_periods(
    transformed: dict[str, Any],
    periods: list[dict[str, datetime]],
) -> dict[str, list[str]]:
    database_url = os.getenv("REPORTING_DATABASE_URL")
    if not database_url:
        raise RuntimeError("REPORTING_DATABASE_URL must be configured")
    normalized_periods = [_normalize_period(period) for period in periods]
    period_starts = [period["period_start"] for period in normalized_periods]

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        rows = connection.execute(
            """
            SELECT output.period_start,
                   COALESCE(
                       array_agg(DISTINCT lineage.event_id)
                           FILTER (WHERE lineage.event_id IS NOT NULL),
                       ARRAY[]::text[]
                   ) AS event_ids
            FROM reporting.support_performance_weekly AS output
                        JOIN reporting.pipeline_runs AS completed_run
                            ON completed_run.run_id = output.run_id
                         AND completed_run.status = 'Completed'
            LEFT JOIN reporting.pipeline_run_events AS lineage
                            ON lineage.run_id = completed_run.run_id
             AND lineage.period_start = output.period_start
            WHERE output.period_start = ANY(%s)
            GROUP BY output.period_start, output.run_id
            """,
            (period_starts,),
        ).fetchall()

    previous_event_ids = {
        row["period_start"].astimezone(UTC).isoformat(): set(row["event_ids"] or [])
        for row in rows
    }
    changed = _changed_period_starts(transformed, previous_event_ids)
    unchanged = [
        row["period_start"]
        for row in transformed["performance_rows"]
        if row["period_start"] not in changed
    ]
    return {"changed_period_starts": changed, "unchanged_period_starts": unchanged}


def _only_changed_periods(
    transformed: dict[str, Any],
    changed_period_starts: list[str],
) -> dict[str, Any]:
    changed = set(changed_period_starts)
    return {
        **transformed,
        "performance_rows": [
            row for row in transformed["performance_rows"] if row["period_start"] in changed
        ],
        "queue_rows": [
            row for row in transformed["queue_rows"] if row["period_start"] in changed
        ],
        "lineage": [row for row in transformed["lineage"] if row["period_start"] in changed],
        "source_counts": [
            row for row in transformed["source_counts"] if row["period_start"] in changed
        ],
    }


@task(
    name="load_reporting_partitions",
    retries=DATABASE_RETRIES,
    retry_delay_seconds=DATABASE_RETRY_DELAY_SECONDS,
    retry_condition_fn=_retry_transient_database_error,
)
def load_reporting_partitions(
    transformed: dict[str, Any],
    run_id: str,
) -> dict[str, int]:
    database_url = _reporting_database_url()
    generated_at = datetime.now(UTC)
    performance_sql = """
        INSERT INTO reporting.support_performance_weekly (
            period_start, period_end,
            sla_met_ticket_count, resolved_ticket_count, sla_attainment_rate,
            sla_attainment_status,
            resolution_time_sum_seconds, resolution_sample_count,
            average_resolution_seconds, average_resolution_hours, resolution_time_status,
            bot_resolved_eligible_query_count, eligible_query_count,
            automation_resolution_rate, automation_resolution_status, run_id, generated_at
        ) VALUES (
            %(period_start)s, %(period_end)s,
            %(sla_met_ticket_count)s, %(resolved_ticket_count)s, %(sla_attainment_rate)s,
            %(sla_attainment_status)s,
            %(resolution_time_sum_seconds)s, %(resolution_sample_count)s,
            %(average_resolution_seconds)s, %(average_resolution_hours)s,
            %(resolution_time_status)s,
            %(bot_resolved_eligible_query_count)s, %(eligible_query_count)s,
            %(automation_resolution_rate)s, %(automation_resolution_status)s,
            %(run_id)s, %(generated_at)s
        )
        ON CONFLICT (period_start) DO UPDATE SET
            period_end = EXCLUDED.period_end,
            sla_met_ticket_count = EXCLUDED.sla_met_ticket_count,
            resolved_ticket_count = EXCLUDED.resolved_ticket_count,
            sla_attainment_rate = EXCLUDED.sla_attainment_rate,
            sla_attainment_status = EXCLUDED.sla_attainment_status,
            resolution_time_sum_seconds = EXCLUDED.resolution_time_sum_seconds,
            resolution_sample_count = EXCLUDED.resolution_sample_count,
            average_resolution_seconds = EXCLUDED.average_resolution_seconds,
            average_resolution_hours = EXCLUDED.average_resolution_hours,
            resolution_time_status = EXCLUDED.resolution_time_status,
            bot_resolved_eligible_query_count = EXCLUDED.bot_resolved_eligible_query_count,
            eligible_query_count = EXCLUDED.eligible_query_count,
            automation_resolution_rate = EXCLUDED.automation_resolution_rate,
            automation_resolution_status = EXCLUDED.automation_resolution_status,
            run_id = EXCLUDED.run_id,
            generated_at = EXCLUDED.generated_at
    """
    queue_sql = """
        INSERT INTO reporting.support_queue_weekly (
            period_start, period_end, queue_id, open_ticket_count,
            overdue_ticket_count, unassigned_ticket_count, active_agent_count,
            oldest_open_age_seconds, snapshot_at, snapshot_age_seconds,
            freshness_status, run_id, generated_at
        ) VALUES (
            %(period_start)s, %(period_end)s, %(queue_id)s, %(open_ticket_count)s,
            %(overdue_ticket_count)s, %(unassigned_ticket_count)s, %(active_agent_count)s,
            %(oldest_open_age_seconds)s, %(snapshot_at)s, %(snapshot_age_seconds)s,
            %(freshness_status)s, %(run_id)s, %(generated_at)s
        )
        ON CONFLICT (period_start, queue_id) DO UPDATE SET
            period_end = EXCLUDED.period_end,
            open_ticket_count = EXCLUDED.open_ticket_count,
            overdue_ticket_count = EXCLUDED.overdue_ticket_count,
            unassigned_ticket_count = EXCLUDED.unassigned_ticket_count,
            active_agent_count = EXCLUDED.active_agent_count,
            oldest_open_age_seconds = EXCLUDED.oldest_open_age_seconds,
            snapshot_at = EXCLUDED.snapshot_at,
            snapshot_age_seconds = EXCLUDED.snapshot_age_seconds,
            freshness_status = EXCLUDED.freshness_status,
            run_id = EXCLUDED.run_id,
            generated_at = EXCLUDED.generated_at
    """
    lineage_sql = """
        INSERT INTO reporting.pipeline_run_events (
            run_id, event_id, event_type, event_timestamp,
            period_start, output_table, output_key
        ) VALUES (
            %(run_id)s, %(event_id)s, %(event_type)s, %(event_timestamp)s,
            %(period_start)s, %(output_table)s, %(output_key)s
        )
        ON CONFLICT (run_id, event_id) DO NOTHING
    """

    performance_by_period = {
        row["period_start"]: {**row, "run_id": run_id, "generated_at": generated_at}
        for row in transformed["performance_rows"]
    }
    queue_by_period: dict[str, list[dict[str, Any]]] = {}
    for row in transformed["queue_rows"]:
        queue_by_period.setdefault(row["period_start"], []).append(
            {**row, "run_id": run_id, "generated_at": generated_at}
        )
    lineage_by_period: dict[str, list[dict[str, Any]]] = {}
    for row in transformed["lineage"]:
        lineage_by_period.setdefault(row["period_start"], []).append(
            {**row, "run_id": run_id}
        )

    rows_written = 0
    with psycopg.connect(database_url, autocommit=True) as connection:
        for period_start in sorted(performance_by_period):
            period_performance = performance_by_period[period_start]
            period_queues = queue_by_period.get(period_start, [])
            period_lineage = lineage_by_period.get(period_start, [])
            period_rows_written = 1 + len(period_queues)
            with connection.transaction():
                connection.execute(
                    "DELETE FROM reporting.support_queue_weekly WHERE period_start = %s",
                    (period_start,),
                )
                connection.execute(performance_sql, period_performance)
                with connection.cursor() as cursor:
                    if period_queues:
                        cursor.executemany(queue_sql, period_queues)
                    if period_lineage:
                        cursor.executemany(lineage_sql, period_lineage)
                connection.execute(
                    """
                    UPDATE reporting.pipeline_runs
                    SET rows_written = (
                            SELECT COUNT(*) FROM reporting.support_performance_weekly
                            WHERE run_id = %s
                        ) + (
                            SELECT COUNT(*) FROM reporting.support_queue_weekly
                            WHERE run_id = %s
                        ),
                        phase_checkpoint = 'load_committed',
                        heartbeat_at = %s
                    WHERE run_id = %s
                    """,
                    (run_id, run_id, datetime.now(UTC), run_id),
                )
            rows_written += period_rows_written

    return {"rows_written": rows_written}


def _require_completed_task_state(state: Any, task_name: str) -> Any:
    if not state.is_completed():
        logger.error("Critical pipeline task failed: %s", task_name)
        raise RuntimeError(f"Critical pipeline task failed: {task_name}") from None
    return state.result()


def _warn_if_optional_task_failed(state: Any, task_name: str) -> None:
    if not state.is_completed():
        logger.warning("Optional pipeline task failed and was ignored: %s", task_name)


@task(name="write_optional_eval_snapshot")
def write_optional_eval_snapshot(
    transformed: dict[str, Any],
    run_id: str,
) -> str:
    from pathlib import Path

    artifact_path = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "eval"
        / f"support_performance_{run_id}.json"
    )
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact = {
        "run_id": run_id,
        "records_processed": transformed["records_processed"],
        "records_rejected": transformed["records_rejected"],
        "quality_issues": transformed["quality_issues"],
        "performance_rows": transformed["performance_rows"],
        "queue_rows": transformed["queue_rows"],
    }
    artifact_path.write_text(json.dumps(artifact, sort_keys=True, indent=2), encoding="utf-8")
    return str(artifact_path)


@flow(name="support_event_extraction_flow")
def support_event_extraction_flow(
    periods: list[dict[str, datetime]],
) -> dict[str, Any]:
    extraction_state = extract_support_events(periods, return_state=True)
    return _require_completed_task_state(extraction_state, "extract_support_events")


@flow(name="support_kpi_transformation_flow")
def support_kpi_transformation_flow(
    events: list[dict[str, Any]],
    periods: list[dict[str, datetime]],
) -> dict[str, Any]:
    transformation_state = transform_support_metrics(
        events,
        periods,
        return_state=True,
    )
    return _require_completed_task_state(
        transformation_state,
        "transform_support_metrics",
    )


@flow(name="support_performance_publication_flow")
def support_performance_publication_flow(
    transformed: dict[str, Any],
    periods: list[dict[str, datetime]],
    run_id: str,
) -> dict[str, Any]:
    comparison_state = identify_changed_reporting_periods(
        transformed,
        periods,
        return_state=True,
    )
    comparison = _require_completed_task_state(
        comparison_state,
        "identify_changed_reporting_periods",
    )
    changed_transformed = _only_changed_periods(
        transformed,
        comparison["changed_period_starts"],
    )
    load_state = load_reporting_partitions(
        changed_transformed,
        run_id,
        return_state=True,
    )
    loaded = _require_completed_task_state(load_state, "load_reporting_partitions")
    return {
        "changed_period_starts": comparison["changed_period_starts"],
        "rows_written": loaded["rows_written"],
    }


@flow(name="support_evaluation_snapshot_flow")
def support_evaluation_snapshot_flow(
    transformed: dict[str, Any],
    run_id: str,
) -> str:
    return write_optional_eval_snapshot(transformed, run_id)


@flow(name="support_performance_flow")
def support_performance_flow(
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    trigger_source: str = "manual",
    run_id: str | None = None,
) -> dict[str, Any]:
    if trigger_source not in {"schedule", "manual", "backfill"}:
        raise ValueError("trigger_source must be schedule, manual, or backfill")
    periods = get_reporting_periods(period_start, period_end)
    run_id = str(UUID(run_id)) if run_id is not None else str(uuid4())
    flow_started_at = datetime.now(UTC)
    records_processed = 0
    run_started = False
    last_checkpoint = "lock_pending"
    current_phase = "lock"

    try:
        with _hold_reporting_period_locks(periods):
            start_state = record_pipeline_run_start(
                run_id,
                periods,
                trigger_source,
                return_state=True,
            )
            _require_completed_task_state(start_state, "record_pipeline_run_start")
            run_started = True
            last_checkpoint = "started"

            current_phase = "extract"
            extraction_state = support_event_extraction_flow(
                periods,
                return_state=True,
            )
            extracted = _require_completed_task_state(
                extraction_state,
                "extract_support_events",
            )
            records_processed = extracted["records_processed"]
            source_metadata = _source_run_metadata(extracted["events"])
            extraction_progress = {
                **source_metadata,
                "phase_checkpoint": "extract_complete",
                "records_read": extracted["records_read"],
                "records_validated": extracted["records_validated"],
                "records_processed": extracted["records_processed"],
                "records_rejected": extracted["records_rejected"],
                "data_quality_summary": {
                    "rejected_by_reason": extracted["rejected_by_reason"],
                },
            }
            progress_state = record_pipeline_run_progress(
                run_id,
                extraction_progress,
                return_state=True,
            )
            _require_completed_task_state(progress_state, "record_pipeline_run_progress")
            last_checkpoint = "extract_complete"

            current_phase = "transform"
            transformation_state = support_kpi_transformation_flow(
                extracted["events"],
                periods,
                return_state=True,
            )
            transformed = _require_completed_task_state(
                transformation_state,
                "transform_support_metrics",
            )
            transformed["records_rejected"] = extracted["records_rejected"]
            transform_progress = {
                **extraction_progress,
                "phase_checkpoint": "transform_complete",
                "data_quality_summary": {
                    "rejected_by_reason": extracted["rejected_by_reason"],
                    "transformation_issues": transformed["quality_issues"],
                },
            }
            progress_state = record_pipeline_run_progress(
                run_id,
                transform_progress,
                return_state=True,
            )
            _require_completed_task_state(progress_state, "record_pipeline_run_progress")
            last_checkpoint = "transform_complete"

            current_phase = "publish"
            publication_state = support_performance_publication_flow(
                transformed,
                periods,
                run_id,
                return_state=True,
            )
            published = _require_completed_task_state(
                publication_state,
                "support_performance_publication_flow",
            )

            terminal_state = record_pipeline_run_terminal_state(
                run_id,
                "Completed",
                "load_committed",
                return_state=True,
            )
            _require_completed_task_state(
                terminal_state,
                "record_pipeline_run_terminal_state",
            )

            optional_state = support_evaluation_snapshot_flow(
                transformed,
                run_id,
                return_state=True,
            )
            _warn_if_optional_task_failed(optional_state, "support_evaluation_snapshot_flow")

            return {
                "run_id": run_id,
                "status": "Completed",
                "records_read": extracted["records_read"],
                "records_processed": extracted["records_processed"],
                "records_rejected": extracted["records_rejected"],
                "rows_written": published["rows_written"],
                "changed_periods": published["changed_period_starts"],
                "source_health_status": "unknown",
                "quality_issues": transformed["quality_issues"],
            }
    except ReportingPeriodLocked:
        skipped_state = record_skipped_pipeline_run(
            run_id,
            periods,
            trigger_source,
            return_state=True,
        )
        if not skipped_state.is_completed():
            _emit_fallback_run_log(
                run_id,
                flow_started_at,
                "Skipped",
                records_processed,
                "period_lock_held",
                "lock",
            )
        return {"run_id": run_id, "status": "Skipped", "reason": "period_lock_held"}
    except Exception as error:
        if run_started:
            error_code = (
                "database_unavailable"
                if isinstance(error, (psycopg.OperationalError, psycopg.InterfaceError))
                else "pipeline_task_failed"
            )
            failure_state = record_pipeline_run_terminal_state(
                run_id,
                "Failed",
                last_checkpoint,
                error_code,
                f"{current_phase} phase failed",
                return_state=True,
            )
            if not failure_state.is_completed():
                _emit_fallback_run_log(
                    run_id,
                    flow_started_at,
                    "Failed",
                    records_processed,
                    error_code,
                    current_phase,
                )
        else:
            _emit_fallback_run_log(
                run_id,
                flow_started_at,
                "Failed",
                records_processed,
                "run_metadata_unavailable",
                current_phase,
            )
        raise


def get_latest_run_status() -> dict[str, Any]:
    with psycopg.connect(_reporting_database_url(), row_factory=dict_row) as connection:
        latest_run = connection.execute(
            """
            SELECT run_id, flow_name, trigger_source, period_start, period_end,
                   started_at, ended_at, status, phase_checkpoint, heartbeat_at,
                   records_read, records_processed, records_rejected, rows_written,
                   source_health_status, error_code, error_summary
            FROM reporting.pipeline_runs
            ORDER BY started_at DESC, run_id DESC
            LIMIT 1
            """
        ).fetchone()
        latest_publication = connection.execute(
            """
            SELECT output.run_id, output.period_start, output.period_end, output.generated_at
            FROM reporting.support_performance_weekly AS output
            JOIN reporting.pipeline_runs AS completed_run
              ON completed_run.run_id = output.run_id
             AND completed_run.status = 'Completed'
            ORDER BY output.generated_at DESC, output.period_start DESC
            LIMIT 1
            """
        ).fetchone()

    publication = None
    if latest_publication is not None:
        publication = {
            "run_id": str(latest_publication["run_id"]),
            "period_start": latest_publication["period_start"].astimezone(UTC).isoformat(),
            "period_end": latest_publication["period_end"].astimezone(UTC).isoformat(),
            "generated_at": latest_publication["generated_at"].astimezone(UTC).isoformat(),
        }

    if latest_run is None:
        return {
            "status": "not_run",
            "run_id": None,
            "period_start": None,
            "period_end": None,
            "started_at": None,
            "ended_at": None,
            "records_processed": 0,
            "records_read": 0,
            "records_rejected": 0,
            "rows_written": 0,
            "phase_checkpoint": None,
            "heartbeat_at": None,
            "errors": None,
            "source_health_status": "unknown",
            "last_successful_publication": publication,
        }

    return {
        "status": latest_run["status"],
        "run_id": str(latest_run["run_id"]),
        "trigger_source": latest_run["trigger_source"],
        "period_start": latest_run["period_start"].astimezone(UTC).isoformat(),
        "period_end": latest_run["period_end"].astimezone(UTC).isoformat(),
        "started_at": latest_run["started_at"].astimezone(UTC).isoformat(),
        "ended_at": (
            latest_run["ended_at"].astimezone(UTC).isoformat()
            if latest_run["ended_at"] is not None
            else None
        ),
        "records_read": latest_run["records_read"],
        "records_processed": latest_run["records_processed"],
        "records_rejected": latest_run["records_rejected"],
        "rows_written": latest_run["rows_written"],
        "phase_checkpoint": latest_run["phase_checkpoint"],
        "heartbeat_at": (
            latest_run["heartbeat_at"].astimezone(UTC).isoformat()
            if latest_run["heartbeat_at"] is not None
            else None
        ),
        "errors": (
            {
                "code": latest_run["error_code"],
                "summary": latest_run["error_summary"],
            }
            if latest_run["error_code"] is not None
            else None
        ),
        "source_health_status": latest_run["source_health_status"],
        "last_successful_publication": publication,
    }


def _find_active_reporting_run(periods: list[dict[str, datetime]]) -> dict[str, Any] | None:
    start = min(period["period_start"] for period in periods)
    end = max(period["period_end"] for period in periods)
    with psycopg.connect(_reporting_database_url(), row_factory=dict_row) as connection:
        return connection.execute(
            """
            SELECT run_id, period_start, period_end
            FROM reporting.pipeline_runs
            WHERE status = 'Running'
              AND period_start < %s
              AND period_end > %s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (end, start),
        ).fetchone()


def trigger_support_performance_flow(
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    trigger_source: str = "manual",
) -> dict[str, Any]:
    if trigger_source not in {"manual", "backfill"}:
        raise ValueError("API triggers must use manual or backfill as trigger_source")
    prefect_api_url = os.getenv("PREFECT_API_URL")
    deployment_name = os.getenv("PREFECT_SUPPORT_PERFORMANCE_DEPLOYMENT")
    if not prefect_api_url or not deployment_name:
        raise ReportingDeploymentNotConfigured(
            "Prefect API URL and support performance deployment must be configured"
        )

    periods = get_reporting_periods(period_start, period_end)
    active_run = _find_active_reporting_run(periods)
    if active_run is not None:
        raise ReportingPeriodLocked(
            "A requested reporting period is already running",
            {
                "run_id": str(active_run["run_id"]),
                "period_start": active_run["period_start"].astimezone(UTC).isoformat(),
                "period_end": active_run["period_end"].astimezone(UTC).isoformat(),
            },
        )

    run_id = str(uuid4())
    flow_run = run_deployment(
        deployment_name,
        parameters={
            "period_start": period_start,
            "period_end": period_end,
            "trigger_source": trigger_source,
            "run_id": run_id,
        },
        timeout=0,
        as_subflow=False,
        idempotency_key=run_id,
    )
    return {
        "run_id": run_id,
        "prefect_flow_run_id": str(flow_run.id),
        "status": "Scheduled",
        "period_start": min(period["period_start"] for period in periods).isoformat(),
        "period_end": max(period["period_end"] for period in periods).isoformat(),
    }


def query_support_performance_kpis(
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    queue_id: str | None = None,
) -> dict[str, Any]:
    if (period_start is None) != (period_end is None):
        raise ValueError("period_start and period_end must be supplied together")

    requested_period = None
    if period_start is not None and period_end is not None:
        requested_period = _normalize_period(
            {"period_start": period_start, "period_end": period_end}
        )

    with psycopg.connect(_reporting_database_url(), row_factory=dict_row) as connection:
        if requested_period is None:
            performance = connection.execute(
                """
                SELECT output.*
                FROM reporting.support_performance_weekly AS output
                JOIN reporting.pipeline_runs AS completed_run
                  ON completed_run.run_id = output.run_id
                 AND completed_run.status = 'Completed'
                ORDER BY output.period_end DESC, output.generated_at DESC
                LIMIT 1
                """
            ).fetchone()
        else:
            performance = connection.execute(
                """
                SELECT output.*
                FROM reporting.support_performance_weekly AS output
                JOIN reporting.pipeline_runs AS completed_run
                  ON completed_run.run_id = output.run_id
                 AND completed_run.status = 'Completed'
                WHERE output.period_start = %s AND output.period_end = %s
                LIMIT 1
                """,
                (requested_period["period_start"], requested_period["period_end"]),
            ).fetchone()

        if performance is None:
            raise ReportingPeriodNotPublished("Requested reporting week is not published")

        queue_query = """
            SELECT queue_id, open_ticket_count, overdue_ticket_count,
                   unassigned_ticket_count, active_agent_count,
                   oldest_open_age_seconds, snapshot_at, snapshot_age_seconds,
                   freshness_status
            FROM reporting.support_queue_weekly
            WHERE period_start = %s
        """
        parameters: tuple[Any, ...] = (performance["period_start"],)
        if queue_id is not None:
            queue_query += " AND queue_id = %s"
            parameters += (queue_id,)
        queue_query += " ORDER BY queue_id"
        queue_rows = connection.execute(queue_query, parameters).fetchall()

    return {
        "period_start": performance["period_start"].astimezone(UTC).isoformat(),
        "period_end": performance["period_end"].astimezone(UTC).isoformat(),
        "period_timezone": REPORTING_TIMEZONE.key,
        "generated_at": performance["generated_at"].astimezone(UTC).isoformat(),
        "run_id": str(performance["run_id"]),
        "sla_attainment": {
            "numerator": performance["sla_met_ticket_count"],
            "denominator": performance["resolved_ticket_count"],
            "rate_percent": performance["sla_attainment_rate"],
            "status": performance["sla_attainment_status"],
        },
        "average_resolution_time": {
            "sum_seconds": performance["resolution_time_sum_seconds"],
            "sample_count": performance["resolution_sample_count"],
            "average_seconds": performance["average_resolution_seconds"],
            "average_hours": performance["average_resolution_hours"],
            "status": performance["resolution_time_status"],
        },
        "eligible_query_automation_resolution": {
            "numerator": performance["bot_resolved_eligible_query_count"],
            "denominator": performance["eligible_query_count"],
            "rate_percent": performance["automation_resolution_rate"],
            "status": performance["automation_resolution_status"],
        },
        "queues": [
            {
                "queue_id": row["queue_id"],
                "open_ticket_count": row["open_ticket_count"],
                "overdue_ticket_count": row["overdue_ticket_count"],
                "unassigned_ticket_count": row["unassigned_ticket_count"],
                "active_agent_count": row["active_agent_count"],
                "oldest_open_age_seconds": row["oldest_open_age_seconds"],
                "snapshot_at": (
                    row["snapshot_at"].astimezone(UTC).isoformat()
                    if row["snapshot_at"] is not None
                    else None
                ),
                "snapshot_age_seconds": row["snapshot_age_seconds"],
                "freshness_status": row["freshness_status"],
            }
            for row in queue_rows
            ],
            }