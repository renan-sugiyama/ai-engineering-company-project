from __future__ import annotations

from fastapi import Depends

from .auth import authenticate_internal_user, require_reporting_operator
from .main import create_reporting_app


def authenticate_user(
    principal: dict[str, str] = Depends(authenticate_internal_user),
) -> dict[str, str]:
    return principal


def authorize_reporting_run(
    principal: dict[str, str] = Depends(authenticate_internal_user),
) -> dict[str, str]:
    return require_reporting_operator(principal)


app = create_reporting_app(
    authenticate_user=authenticate_user,
    authorize_reporting_run=authorize_reporting_run,
)