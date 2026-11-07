from __future__ import annotations

from collections.abc import Callable
from typing import Any

import psycopg
from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Query, status
from prefect.exceptions import PrefectException
from pydantic import AwareDatetime, BaseModel, ConfigDict

from data.pipelines.support_performance import (
    ReportingDeploymentNotConfigured,
    ReportingPeriodLocked,
    ReportingPeriodNotPublished,
    get_latest_run_status,
    query_support_performance_kpis,
    trigger_support_performance_flow,
)


class TriggerRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    period_start: AwareDatetime | None = None
    period_end: AwareDatetime | None = None


def create_reporting_app(
    *,
    authenticate_user: Callable[..., Any],
    authorize_reporting_run: Callable[..., Any],
) -> FastAPI:
    """Build the reporting API with identity and operator checks supplied by its host."""
    application = FastAPI(title="Nexova Reporting API", version="0.1.0")
    router = APIRouter(prefix="/reporting")

    @router.get("/status")
    def reporting_status(
        _principal: Any = Depends(authenticate_user),
    ) -> dict[str, Any]:
        try:
            return get_latest_run_status()
        except (psycopg.Error, RuntimeError) as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Reporting status is unavailable",
            ) from error

    @router.post("/runs", status_code=status.HTTP_202_ACCEPTED)
    def trigger_reporting_run(
        payload: TriggerRunRequest | None = Body(default=None),
        _principal: Any = Depends(authorize_reporting_run),
    ) -> dict[str, Any]:
        request = payload or TriggerRunRequest()
        try:
            return trigger_support_performance_flow(
                request.period_start,
                request.period_end,
                trigger_source="manual",
            )
        except ReportingPeriodLocked as error:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"message": str(error), "active_run": error.active_run},
            ) from error
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(error),
            ) from error
        except ReportingDeploymentNotConfigured as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Manual reporting runs are not configured",
            ) from error
        except (psycopg.Error, PrefectException, RuntimeError) as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Manual reporting run could not be submitted",
            ) from error

    @router.get("/kpis")
    def reporting_kpis(
        period_start: AwareDatetime | None = Query(default=None),
        period_end: AwareDatetime | None = Query(default=None),
        queue_id: str | None = Query(default=None, min_length=1),
        _principal: Any = Depends(authenticate_user),
    ) -> dict[str, Any]:
        try:
            return query_support_performance_kpis(period_start, period_end, queue_id)
        except ReportingPeriodNotPublished as error:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Requested reporting week is not published",
            ) from error
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(error),
            ) from error
        except (psycopg.Error, RuntimeError) as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Reporting KPIs are unavailable",
            ) from error

    application.include_router(router)
    return application