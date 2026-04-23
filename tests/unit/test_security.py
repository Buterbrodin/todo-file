from datetime import datetime, timedelta
from unittest.mock import patch

import jwt
import pytest
from fastapi import HTTPException

from app.core.security import UserPrincipal, decode_token


class TestUserPrincipal:
    def test_user_principal_member(self):
        user = UserPrincipal(user_id=1, roles=["member"], email="test@example.com")

        assert user.id == 1
        assert user.roles == ["member"]
        assert user.role == "member"
        assert user.email == "test@example.com"
        assert user.is_authenticated is True
        assert user.is_internal_service is False
        assert user.is_admin is False

    def test_user_principal_admin(self):
        user = UserPrincipal(user_id=2, roles=["admin"], email="admin@example.com")

        assert user.id == 2
        assert user.roles == ["admin"]
        assert user.role == "admin"
        assert user.is_admin is True

    def test_user_principal_multiple_roles(self):
        user = UserPrincipal(
            user_id=3, roles=["member", "admin"], email="test@example.com"
        )

        assert user.roles == ["member", "admin"]
        assert user.role == "admin"
        assert user.is_admin is True

    def test_user_principal_no_roles(self):
        user = UserPrincipal(user_id=1, roles=None, email="test@example.com")

        assert user.roles == []
        assert user.role == "member"
        assert user.is_admin is False

    def test_user_principal_no_email(self):
        user = UserPrincipal(user_id=1, roles=["member"], email=None)

        assert user.email is None

    def test_user_principal_internal_service(self):
        user = UserPrincipal(
            user_id=7,
            roles=["member"],
            email=None,
            is_internal_service=True,
        )

        assert user.id == 7
        assert user.is_internal_service is True


class TestDecodeToken:
    @pytest.fixture
    def valid_token(self):
        payload = {
            "sub": "1",
            "type": "access",
            "roles": ["member"],
            "email": "test@example.com",
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        return jwt.encode(payload, "change_me_change_me", algorithm="HS256")

    @pytest.fixture
    def expired_token(self):
        payload = {
            "sub": "1",
            "type": "access",
            "exp": datetime.utcnow() - timedelta(hours=1),
        }
        return jwt.encode(payload, "change_me_change_me", algorithm="HS256")

    @pytest.fixture
    def verification_token(self):
        payload = {
            "sub": "1",
            "type": "verification_access",
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        return jwt.encode(payload, "change_me_change_me", algorithm="HS256")

    def test_decode_valid_token(self, valid_token):
        with patch("app.core.security.settings") as mock_settings:
            mock_settings.JWT_SECRET = "change_me_change_me"
            mock_settings.JWT_ALG = "HS256"

            user = decode_token(valid_token)

            assert user.id == 1
            assert user.roles == ["member"]
            assert user.email == "test@example.com"

    def test_decode_expired_token(self, expired_token):
        with patch("app.core.security.settings") as mock_settings:
            mock_settings.JWT_SECRET = "change_me_change_me"
            mock_settings.JWT_ALG = "HS256"

            with pytest.raises(HTTPException) as exc_info:
                decode_token(expired_token)

            assert exc_info.value.status_code == 401
            assert "expired" in exc_info.value.detail.lower()

    def test_decode_invalid_token(self):
        with patch("app.core.security.settings") as mock_settings:
            mock_settings.JWT_SECRET = "change_me_change_me"
            mock_settings.JWT_ALG = "HS256"

            with pytest.raises(HTTPException) as exc_info:
                decode_token("invalid.token.here")

            assert exc_info.value.status_code == 401

    def test_decode_verification_token_rejected(self, verification_token):
        with patch("app.core.security.settings") as mock_settings:
            mock_settings.JWT_SECRET = "change_me_change_me"
            mock_settings.JWT_ALG = "HS256"

            with pytest.raises(HTTPException) as exc_info:
                decode_token(verification_token)

            assert exc_info.value.status_code == 403
            assert "verification" in exc_info.value.detail.lower()

    def test_decode_token_invalid_type(self):
        payload = {
            "sub": "1",
            "type": "refresh",
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        token = jwt.encode(payload, "change_me_change_me", algorithm="HS256")

        with patch("app.core.security.settings") as mock_settings:
            mock_settings.JWT_SECRET = "change_me_change_me"
            mock_settings.JWT_ALG = "HS256"

            with pytest.raises(HTTPException) as exc_info:
                decode_token(token)

            assert exc_info.value.status_code == 401
            assert "Invalid token type" in exc_info.value.detail

    def test_decode_token_no_sub(self):
        payload = {
            "type": "access",
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        token = jwt.encode(payload, "change_me_change_me", algorithm="HS256")

        with patch("app.core.security.settings") as mock_settings:
            mock_settings.JWT_SECRET = "change_me_change_me"
            mock_settings.JWT_ALG = "HS256"

            with pytest.raises(HTTPException) as exc_info:
                decode_token(token)

            assert exc_info.value.status_code == 401

    def test_decode_token_invalid_sub(self):
        payload = {
            "sub": "not_a_number",
            "type": "access",
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        token = jwt.encode(payload, "change_me_change_me", algorithm="HS256")

        with patch("app.core.security.settings") as mock_settings:
            mock_settings.JWT_SECRET = "change_me_change_me"
            mock_settings.JWT_ALG = "HS256"

            with pytest.raises(HTTPException) as exc_info:
                decode_token(token)

            assert exc_info.value.status_code == 401
            assert "invalid" in exc_info.value.detail.lower()

    def test_decode_token_with_role_as_string(self):
        payload = {
            "sub": "1",
            "type": "access",
            "role": "admin",
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        token = jwt.encode(payload, "change_me_change_me", algorithm="HS256")

        with patch("app.core.security.settings") as mock_settings:
            mock_settings.JWT_SECRET = "change_me_change_me"
            mock_settings.JWT_ALG = "HS256"

            user = decode_token(token)

            assert user.roles == ["admin"]
            assert user.is_admin is True

    def test_decode_token_no_type(self):
        payload = {
            "sub": "1",
            "exp": datetime.utcnow() + timedelta(hours=1),
        }
        token = jwt.encode(payload, "change_me_change_me", algorithm="HS256")

        with patch("app.core.security.settings") as mock_settings:
            mock_settings.JWT_SECRET = "change_me_change_me"
            mock_settings.JWT_ALG = "HS256"

            user = decode_token(token)

            assert user.id == 1
