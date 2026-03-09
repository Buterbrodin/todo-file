"""Utility functions for permission checks."""

import logging

from app.core.security import UserPrincipal

logger = logging.getLogger(__name__)


def is_global_admin(user: UserPrincipal) -> bool:
    """
    Check if user is a global administrator.

    Args:
        user: User principal to check.

    Returns:
        True if user is a global admin, False otherwise.
    """
    return user.is_admin
