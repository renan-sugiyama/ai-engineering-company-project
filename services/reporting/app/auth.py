from __future__ import annotations

import os
from typing import Any

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

bearer = HTTPBearer(auto_error=False)


def authenticate_internal_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> dict[str, str]:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication is required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    auth_api_url = os.getenv("AUTH_API_URL", "http://127.0.0.1:8003").rstrip("/")
    try:
        with httpx.Client(timeout=5.0, follow_redirects=False) as client:
            response = client.post(
                f"{auth_api_url}/login",
                headers={"Authorization": f"Bearer {credentials.credentials}"},
            )
    except httpx.RequestError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service is unavailable",
        ) from error

    if response.status_code == status.HTTP_401_UNAUTHORIZED:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session is invalid or expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if response.status_code != status.HTTP_200_OK:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service is unavailable",
        )

    try:
        payload: Any = response.json()
    except ValueError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service returned an invalid response",
        ) from error

    user_id = payload.get("userId") if isinstance(payload, dict) else None
    if not isinstance(user_id, str) or not user_id:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service returned an invalid response",
        )
    return {"userId": user_id}


def require_reporting_operator(principal: dict[str, str]) -> dict[str, str]:
    configured_ids = os.getenv("REPORTING_OPERATOR_USER_IDS", "")
    operator_ids = {user_id.strip() for user_id in configured_ids.split(",") if user_id.strip()}
    if not operator_ids:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Reporting operator access is not configured",
        )
    if principal.get("userId") not in operator_ids:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Reporting operator access is required",
        )
    return principal