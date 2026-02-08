from typing import Optional

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.db import get_db
from app.core.security import UserPrincipal, decode_token
from app.services.core_client import CoreServiceClient

bearer_scheme = HTTPBearer(
    auto_error=False,
    scheme_name="BearerAuth",
    description="JWT access token. Format: Bearer <token>",
)


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
) -> UserPrincipal:
    """
    Get current authenticated user from JWT token.

    Args:
        credentials: Parsed Authorization header credentials.

    Returns:
        UserPrincipal with user information.

    Raises:
        HTTPException: If not authenticated or token is invalid.
    """
    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )
    return decode_token(credentials.credentials)


async def get_db_session(session: AsyncSession = Depends(get_db)) -> AsyncSession:
    """
    Get database session dependency.

    Args:
        session: Injected database session.

    Returns:
        AsyncSession instance.
    """
    return session


def get_core_service_client() -> CoreServiceClient:
    """
    Get core service client dependency.

    Returns:
        CoreServiceClient instance.

    Raises:
        HTTPException: If core service is not configured.
    """
    try:
        return CoreServiceClient()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Core service not configured",
        ) from exc
