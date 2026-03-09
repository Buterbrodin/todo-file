import logging
from typing import Any, Optional, Union

import httpx

from app.core.constants import FileAction
from app.core.exceptions import CoreServiceError
from app.settings import settings

logger = logging.getLogger(__name__)

# Global HTTP client for connection pooling
_http_client: Optional[httpx.AsyncClient] = None


async def get_http_client() -> httpx.AsyncClient:
    """Get or create global HTTP client with connection pooling."""
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=3.0)
    return _http_client


async def close_http_client() -> None:
    """Close global HTTP client."""
    global _http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None


class CoreServiceClient:
    def __init__(self) -> None:
        if not settings.CORE_SERVICE_BASE_URL:
            raise CoreServiceError("Core service base URL not configured")
        if not settings.CORE_INTERNAL_TOKEN:
            raise CoreServiceError("Core internal token not configured")

    def _build_url(self, path: str) -> str:
        base = str(settings.CORE_SERVICE_BASE_URL).rstrip("/")
        return f"{base}/{path.lstrip('/')}"

    async def check_project_access(
        self,
        user_id: int,
        project_id: int,
        action: Union[str, FileAction] = FileAction.READ,
        email: str | None = None,
    ) -> bool:
        url = self._build_url(f"internal/projects/{project_id}/check-access")
        headers = {"X-Internal-Token": str(settings.CORE_INTERNAL_TOKEN)}
        action_str = action.value if isinstance(action, FileAction) else action
        payload: dict[str, Any] = {"user_id": user_id, "action": action_str}
        if email:
            payload["email"] = email

        logger.debug(
            "Checking project access: url=%s, user_id=%s, project_id=%s, action=%s",
            url,
            user_id,
            project_id,
            action,
        )

        try:
            client = await get_http_client()
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data: dict[str, Any] = response.json()
            has_access = data.get("has_access", False)
            logger.debug(
                "Project access check result: project_id=%s, user_id=%s, has_access=%s",
                project_id,
                user_id,
                has_access,
            )
            return has_access
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                logger.debug("Project not found: project_id=%s", project_id)
                return False
            logger.error(
                "Core service error checking project access: "
                "status=%s, url=%s, response=%s",
                exc.response.status_code,
                url,
                exc.response.text,
            )
            raise CoreServiceError(
                f"Core service error: {exc.response.status_code}"
            ) from exc
        except httpx.RequestError as exc:
            logger.error("Core service unreachable: url=%s, error=%s", url, exc)
            raise CoreServiceError("Core service unreachable") from exc

    async def check_task_access(
        self,
        user_id: int,
        task_id: int,
        action: Union[str, FileAction] = FileAction.READ,
        email: str | None = None,
    ) -> bool:
        url = self._build_url(f"internal/tasks/{task_id}/check-access")
        headers = {"X-Internal-Token": str(settings.CORE_INTERNAL_TOKEN)}
        action_str = action.value if isinstance(action, FileAction) else action
        payload: dict[str, Any] = {"user_id": user_id, "action": action_str}
        if email:
            payload["email"] = email

        try:
            client = await get_http_client()
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data: dict[str, Any] = response.json()
            return data.get("has_access", False)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return False
            logger.error(
                "Core service error checking task access: %s", exc.response.status_code
            )
            raise CoreServiceError(
                f"Core service error: {exc.response.status_code}"
            ) from exc
        except httpx.RequestError as exc:
            logger.error("Core service unreachable: %s", exc)
            raise CoreServiceError("Core service unreachable") from exc
