from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from prefect.states import Completed

from data.pipelines.support_performance import (
    EventValidationError,
    calculate_average_resolution_time,
    calculate_eligible_query_automation_resolution,
    calculate_sla_attainment,
    calculate_week_end_support_backlog,
    support_event_extraction_flow,
    support_kpi_transformation_flow,
    support_performance_publication_flow,
    transform_support_metrics,
)

UTC = timezone.utc
PERIOD = {
    "period_start": datetime(2026, 11, 1, 23, tzinfo=UTC),
    "period_end": datetime(2026, 11, 8, 23, tzinfo=UTC),
}


def _ticket_event(
    event_id: str,
    ticket_id: str,
    resolved_at: datetime,
    resolution_seconds: int,
    sla_met: bool | None,
) -> dict:
    return {
        "event_id": event_id,
        "event_type": "support_ticket_resolved",
        "timestamp": resolved_at.isoformat(),
        "properties": {
            "ticketId": ticket_id,
            "clientId": "client-a",
            "channel": "email",
            "openedAt": (resolved_at - timedelta(seconds=resolution_seconds)).isoformat(),
            "resolvedAt": resolved_at.isoformat(),
            "resolutionSeconds": resolution_seconds,
            "slaTargetSeconds": 86400,
            "slaMet": sla_met,
        },
    }


def _query_event(
    event_id: str,
    query_id: str,
    eligible: bool,
    resolved: bool,
    resolution_method: str,
    timestamp: datetime,
) -> dict:
    return {
        "event_id": event_id,
        "event_type": "support_query_closed",
        "timestamp": timestamp.isoformat(),
        "properties": {
            "queryId": query_id,
            "eligibleForAutomation": eligible,
            "resolutionMethod": resolution_method,
            "humanHandoff": resolution_method == "agent",
            "resolved": resolved,
        },
    }


def _queue_event(event_id: str, queue_id: str, timestamp: datetime, open_count: int) -> dict:
    return {
        "event_id": event_id,
        "event_type": "support_queue_snapshot_recorded",
        "timestamp": timestamp.isoformat(),
        "properties": {
            "queueId": queue_id,
            "openTicketCount": open_count,
            "overdueTicketCount": 1,
            "unassignedTicketCount": 2,
            "activeAgentCount": 3,
            "oldestOpenAgeSeconds": 120,
        },
    }


def test_24_hour_sla_attainment_counts_distinct_valid_tickets() -> None:
    resolved_at = PERIOD["period_end"] - timedelta(days=1)
    met_ticket = _ticket_event("ticket-event-1", "ticket-1", resolved_at, 3600, True)
    events = [
        met_ticket,
        {**met_ticket, "properties": dict(met_ticket["properties"])},
        _ticket_event("ticket-event-2", "ticket-2", resolved_at, 7200, False),
    ]

    result = calculate_sla_attainment(events)

    assert result == {
        "sla_met_ticket_count": 1,
        "resolved_ticket_count": 2,
        "sla_attainment_rate": 50.0,
        "sla_attainment_status": "ok",
    }


def test_average_resolution_time_returns_hand_calculated_hours() -> None:
    resolved_at = PERIOD["period_end"] - timedelta(days=1)
    events = [
        _ticket_event("ticket-event-1", "ticket-1", resolved_at, 3600, True),
        _ticket_event("ticket-event-2", "ticket-2", resolved_at, 7200, False),
    ]

    result = calculate_average_resolution_time(events)

    assert result["resolution_time_sum_seconds"] == 10800
    assert result["resolution_sample_count"] == 2
    assert result["average_resolution_seconds"] == 5400
    assert result["average_resolution_hours"] == 1.5
    assert result["resolution_time_status"] == "ok"


def test_eligible_query_automation_resolution_uses_eligible_denominator() -> None:
    close_time = PERIOD["period_end"] - timedelta(hours=1)
    events = [
        _query_event("query-event-1", "query-1", True, True, "bot", close_time),
        _query_event("query-event-2", "query-2", True, True, "agent", close_time),
        _query_event("query-event-3", "query-3", False, True, "bot", close_time),
    ]

    result = calculate_eligible_query_automation_resolution(events)

    assert result["bot_resolved_eligible_query_count"] == 1
    assert result["eligible_query_count"] == 2
    assert result["automation_resolution_rate"] == 50.0
    assert result["automation_resolution_status"] == "ok"


def test_week_end_support_backlog_uses_latest_snapshot_and_freshness() -> None:
    period_end = PERIOD["period_end"]
    events = [
        _queue_event("billing-old", "billing", period_end - timedelta(minutes=4), 9),
        _queue_event("billing-latest", "billing", period_end - timedelta(minutes=2), 3),
        _queue_event("technical-stale", "technical", period_end - timedelta(minutes=7), 5),
        _queue_event("future-only", "future", period_end + timedelta(seconds=1), 8),
    ]

    result = calculate_week_end_support_backlog(events, PERIOD)
    queues = {row["queue_id"]: row for row in result["queue_rows"]}

    assert queues["billing"]["open_ticket_count"] == 3
    assert queues["billing"]["freshness_status"] == "fresh"
    assert queues["technical"]["open_ticket_count"] is None
    assert queues["technical"]["freshness_status"] == "stale"
    assert queues["future"]["open_ticket_count"] is None
    assert queues["future"]["freshness_status"] == "missing"
    assert queues["billing"]["active_agent_count"] == 3


def test_transformation_rejects_null_sla_field_defensively() -> None:
    resolved_at = PERIOD["period_end"] - timedelta(hours=1)
    malformed_event = _ticket_event("ticket-event-1", "ticket-1", resolved_at, 3600, None)

    with pytest.raises(EventValidationError, match="invalid_slaMet"):
        transform_support_metrics.fn([malformed_event], [PERIOD])


def test_rate_and_duration_helpers_report_no_data_without_fabricating_zero() -> None:
    sla = calculate_sla_attainment([])
    resolution = calculate_average_resolution_time([])
    automation = calculate_eligible_query_automation_resolution([])

    assert sla["sla_attainment_rate"] is None
    assert sla["sla_attainment_status"] == "no_data"
    assert resolution["average_resolution_hours"] is None
    assert resolution["resolution_time_status"] == "no_data"
    assert automation["automation_resolution_rate"] is None
    assert automation["automation_resolution_status"] == "no_data"


def test_weekly_transformation_uses_latest_ticket_and_query_outcomes() -> None:
    period_end = PERIOD["period_end"]
    events = [
        _ticket_event("ticket-old", "ticket-1", period_end - timedelta(days=2), 7200, False),
        _ticket_event("ticket-latest", "ticket-1", period_end - timedelta(days=1), 1800, True),
        _query_event("query-old", "query-1", True, True, "bot", period_end - timedelta(days=2)),
        _query_event("query-latest", "query-1", True, False, "agent", period_end - timedelta(days=1)),
    ]

    result = transform_support_metrics.fn(events, [PERIOD])
    performance = result["performance_rows"][0]

    assert performance["resolved_ticket_count"] == 1
    assert performance["sla_attainment_rate"] == 100.0
    assert performance["average_resolution_seconds"] == 1800
    assert performance["eligible_query_count"] == 1
    assert performance["bot_resolved_eligible_query_count"] == 0
    assert performance["automation_resolution_rate"] == 0.0


def test_extraction_transformation_and_publication_subflows_are_independently_callable() -> None:
    extracted = {"events": [], "records_read": 0, "records_processed": 0}
    transformed = {
        "performance_rows": [],
        "queue_rows": [],
        "lineage": [],
        "source_counts": [],
    }
    period_start = PERIOD["period_start"].isoformat()

    with patch(
        "data.pipelines.support_performance.extract_support_events",
        return_value=Completed(data=extracted),
    ):
        assert support_event_extraction_flow.fn([PERIOD]) == extracted

    with patch(
        "data.pipelines.support_performance.transform_support_metrics",
        return_value=Completed(data=transformed),
    ):
        assert support_kpi_transformation_flow.fn([], [PERIOD]) == transformed

    with (
        patch(
            "data.pipelines.support_performance.identify_changed_reporting_periods",
            return_value=Completed(data={"changed_period_starts": [period_start]}),
        ),
        patch(
            "data.pipelines.support_performance.load_reporting_partitions",
            return_value=Completed(data={"rows_written": 2}),
        ),
    ):
        result = support_performance_publication_flow.fn(transformed, [PERIOD], "run-1")

    assert result == {"changed_period_starts": [period_start], "rows_written": 2}
