from typing import Optional

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.backend.db import get_db
from app.core.security import UserPrincipal, decode_token


async def get_current_user(
    authorization: Optional[str] = Header(default=None, convert_underscores=False),
) -> UserPrincipal:
    """
    Get current authenticated user from JWT token.

    Args:
        authorization: Authorization header value.

    Returns:
        UserPrincipal with user information.

    Raises:
        HTTPException: If not authenticated or token is invalid.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )
    token = authorization.split(" ", 1)[1]
    return decode_token(token)


async def get_db_session(session: AsyncSession = Depends(get_db)) -> AsyncSession:
    """
    Get database session dependency.

    Args:
        session: Injected database session.

    Returns:
        AsyncSession instance.
    """
    return session
