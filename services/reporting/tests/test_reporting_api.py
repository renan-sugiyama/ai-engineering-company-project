from __future__ import annotations

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi import HTTPException, Request
from fastapi.testclient import TestClient

from services.reporting.app.main import create_reporting_app

UTC = timezone.utc


def _authenticated_user(request: Request) -> dict[str, str]:
    user_id = request.headers.get("X-Backoffice-User")
    if not user_id:
        raise HTTPException(status_code=401, detail="Backoffice user is required")
    return {"id": user_id}


def _reporting_operator(request: Request) -> dict[str, str]:
    user_id = request.headers.get("X-Backoffice-User")
    if not user_id:
        raise HTTPException(status_code=401, detail="Backoffice user is required")
    if user_id != "reporting-operator":
        raise HTTPException(status_code=403, detail="Reporting operator role required")
    return {"id": user_id, "role": "reporting_operator"}


class ReportingApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(
            create_reporting_app(
                authenticate_user=_authenticated_user,
                authorize_reporting_run=_reporting_operator,
            )
        )

    @patch("services.reporting.app.main.get_latest_run_status")
    def test_status_requires_auth_and_returns_run_metadata(self, get_status) -> None:
        get_status.return_value = {
            "status": "Completed",
            "run_id": "run-1",
            "started_at": "2026-11-02T05:00:00+00:00",
            "ended_at": "2026-11-02T05:01:00+00:00",
            "records_processed": 12,
            "errors": None,
        }

        unauthorized = self.client.get("/reporting/status")
        response = self.client.get(
            "/reporting/status",
            headers={"X-Backoffice-User": "support-user"},
        )

        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["records_processed"], 12)
        get_status.assert_called_once_with()

    @patch("services.reporting.app.main.query_support_performance_kpis")
    def test_kpi_route_forwards_period_and_queue_filter(self, query_kpis) -> None:
        query_kpis.return_value = {
            "period_start": "2026-10-25T23:00:00+00:00",
            "period_end": "2026-11-01T23:00:00+00:00",
            "period_timezone": "Europe/Madrid",
            "generated_at": "2026-11-02T05:00:00+00:00",
            "run_id": "run-1",
            "sla_attainment": {"numerator": 8, "denominator": 10, "rate_percent": 80, "status": "ok"},
            "average_resolution_time": {"sum_seconds": 3600, "sample_count": 1, "average_seconds": 3600, "average_hours": 1, "status": "ok"},
            "eligible_query_automation_resolution": {"numerator": 2, "denominator": 4, "rate_percent": 50, "status": "ok"},
            "queues": [{"queue_id": "billing", "open_ticket_count": 3}],
        }
        response = self.client.get(
            "/reporting/kpis",
            params={
                "period_start": "2026-10-25T23:00:00Z",
                "period_end": "2026-11-01T23:00:00Z",
                "queue_id": "billing",
            },
            headers={"X-Backoffice-User": "support-user"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["queues"][0]["queue_id"], "billing")
        self.assertNotIn("event_id", response.text)
        query_kpis.assert_called_once_with(
            datetime(2026, 10, 25, 23, tzinfo=UTC),
            datetime(2026, 11, 1, 23, tzinfo=UTC),
            "billing",
        )

    @patch("services.reporting.app.main.query_support_performance_kpis")
    def test_kpi_no_data_response_is_preserved_and_requires_auth(self, query_kpis) -> None:
        query_kpis.return_value = {
            "period_start": "2026-10-25T23:00:00+00:00",
            "period_end": "2026-11-01T23:00:00+00:00",
            "period_timezone": "Europe/Madrid",
            "generated_at": "2026-11-02T05:00:00+00:00",
            "run_id": "run-1",
            "sla_attainment": {
                "numerator": 0,
                "denominator": 0,
                "rate_percent": None,
                "status": "no_data",
            },
            "average_resolution_time": {
                "sum_seconds": 0,
                "sample_count": 0,
                "average_seconds": None,
                "average_hours": None,
                "status": "no_data",
            },
            "eligible_query_automation_resolution": {
                "numerator": 0,
                "denominator": 0,
                "rate_percent": None,
                "status": "no_data",
            },
            "queues": [],
        }

        unauthorized = self.client.get("/reporting/kpis")
        response = self.client.get(
            "/reporting/kpis",
            headers={"X-Backoffice-User": "support-user"},
        )

        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["sla_attainment"]["rate_percent"])
        self.assertEqual(response.json()["sla_attainment"]["status"], "no_data")
        self.assertIsNone(
            response.json()["eligible_query_automation_resolution"]["rate_percent"]
        )
        self.assertEqual(response.json()["queues"], [])
        query_kpis.assert_called_once_with(None, None, None)

    @patch("services.reporting.app.main.trigger_support_performance_flow")
    def test_manual_trigger_requires_operator_and_returns_accepted_run(self, trigger) -> None:
        trigger.return_value = {
            "run_id": "run-1",
            "prefect_flow_run_id": "prefect-run-1",
            "status": "Scheduled",
            "period_start": "2026-10-25T23:00:00+00:00",
            "period_end": "2026-11-01T23:00:00+00:00",
        }
        body = {
            "period_start": "2026-10-25T23:00:00Z",
            "period_end": "2026-11-01T23:00:00Z",
        }

        forbidden = self.client.post(
            "/reporting/runs",
            json=body,
            headers={"X-Backoffice-User": "support-user"},
        )
        accepted = self.client.post(
            "/reporting/runs",
            json=body,
            headers={"X-Backoffice-User": "reporting-operator"},
        )

        self.assertEqual(forbidden.status_code, 403)
        self.assertEqual(accepted.status_code, 202)
        self.assertEqual(accepted.json()["run_id"], "run-1")
        trigger.assert_called_once_with(
            datetime(2026, 10, 25, 23, tzinfo=UTC),
            datetime(2026, 11, 1, 23, tzinfo=UTC),
            trigger_source="manual",
        )

    @patch("services.reporting.app.main.trigger_support_performance_flow")
    def test_trigger_maps_unconfigured_deployment_to_sanitized_503(self, trigger) -> None:
        from data.pipelines.support_performance import ReportingDeploymentNotConfigured

        trigger.side_effect = ReportingDeploymentNotConfigured("private deployment details")
        response = self.client.post(
            "/reporting/runs",
            headers={"X-Backoffice-User": "reporting-operator"},
        )

        self.assertEqual(response.status_code, 503)
        self.assertNotIn("private deployment details", response.text)

    @patch("services.reporting.app.main.trigger_support_performance_flow")
    def test_active_period_maps_to_conflict_with_run_metadata(self, trigger) -> None:
        from data.pipelines.support_performance import ReportingPeriodLocked

        trigger.side_effect = ReportingPeriodLocked(
            "A requested reporting period is already running",
            {"run_id": "active-run", "period_start": "2026-10-25T23:00:00+00:00"},
        )
        response = self.client.post(
            "/reporting/runs",
            headers={"X-Backoffice-User": "reporting-operator"},
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["detail"]["active_run"]["run_id"], "active-run")

    @patch("services.reporting.app.main.query_support_performance_kpis")
    def test_unpublished_period_maps_to_404(self, query_kpis) -> None:
        from data.pipelines.support_performance import ReportingPeriodNotPublished

        query_kpis.side_effect = ReportingPeriodNotPublished("private period details")
        response = self.client.get(
            "/reporting/kpis",
            headers={"X-Backoffice-User": "support-user"},
        )

        self.assertEqual(response.status_code, 404)
        self.assertNotIn("private period details", response.text)