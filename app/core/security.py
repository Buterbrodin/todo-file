from typing import Optional

import jwt
from fastapi import HTTPException, status

from app.settings import settings


class UserPrincipal:
    """Represents authenticated user information from JWT token."""

    def __init__(
        self,
        user_id: int,
        roles: Optional[list[str]],
        email: Optional[str],
    ) -> None:
        """
        Initialize UserPrincipal.

        Args:
            user_id: User's unique identifier.
            roles: List of user's roles.
            email: User's email address.
        """
        self.id = user_id
        self.roles = roles or []
        self.role = "admin" if "admin" in self.roles else "member"
        self.email = email
        self.is_authenticated = True

    @property
    def is_admin(self) -> bool:
        """Check if user has admin role."""
        return "admin" in self.roles


def decode_token(token: str) -> UserPrincipal:
    """
    Decode and validate JWT token.

    Args:
        token: JWT token string.

    Returns:
        UserPrincipal with decoded user information.

    Raises:
        HTTPException: If token is invalid or expired.
    """
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET,
            algorithms=[settings.JWT_ALG],
            options={"require": ["sub"]},
        )
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired",
        ) from exc
    except jwt.PyJWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        ) from exc

    token_type = payload.get("type")
    if token_type == "verification_access":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Email verification required. Please verify your email to access this resource.",
        )
    if token_type not in ("access", None):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type",
        )

    user_id = payload.get("sub")
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing subject",
        )

    try:
        user_id_int = int(user_id)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token subject invalid",
        ) from exc

    roles = payload.get("roles") or payload.get("role") or []
    if isinstance(roles, str):
        roles = [roles]

    email = payload.get("email")

    return UserPrincipal(user_id_int, roles, email)