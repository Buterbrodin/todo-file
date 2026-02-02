from datetime import datetime, timedelta
from unittest.mock import patch

import jwt
import pytest
from fastapi import HTTPException

from app.core.deps import get_current_user


@pytest.mark.asyncio
async def test_get_current_user_success():
    payload = {
        "sub": "1",
        "type": "access",
        "roles": ["member"],
        "email": "test@example.com",
        "exp": datetime.utcnow() + timedelta(hours=1),
    }
    token = jwt.encode(payload, "change_me_change_me", algorithm="HS256")

    with patch("app.core.deps.decode_token") as mock_decode:
        from app.core.security import UserPrincipal

        mock_decode.return_value = UserPrincipal(
            user_id=1,
            roles=["member"],
            email="test@example.com",
        )

        user = await get_current_user(authorization=f"Bearer {token}")

        assert user.id == 1
        assert user.roles == ["member"]
        mock_decode.assert_called_once_with(token)


@pytest.mark.asyncio
async def test_get_current_user_no_header():
    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(authorization=None)

    assert exc_info.value.status_code == 401
    assert "Not authenticated" in exc_info.value.detail


@pytest.mark.asyncio
async def test_get_current_user_invalid_header_format():
    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(authorization="InvalidFormat token123")

    assert exc_info.value.status_code == 401
    assert "Not authenticated" in exc_info.value.detail


@pytest.mark.asyncio
async def test_get_current_user_empty_bearer():
    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(authorization="Basic token123")

    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_get_current_user_token_decode_error():
    with patch("app.core.deps.decode_token") as mock_decode:
        mock_decode.side_effect = HTTPException(status_code=401, detail="Invalid token")

        with pytest.raises(HTTPException) as exc_info:
            await get_current_user(authorization="Bearer invalid_token")

        assert exc_info.value.status_code == 401
