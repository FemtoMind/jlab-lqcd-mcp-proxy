# dashboard_auth.py
# Authentication and short-lived ticket management for the Slurm Web Dashboard
import asyncio
import os
import secrets
import time
from typing import Optional
from fastapi import Request, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from lqcd_logger import lqcd_logger

security_bearer = HTTPBearer(auto_error=False)


class DashboardAuthManager:
    """Manages short-lived authentication tickets for the web dashboard."""

    def __init__(self, default_ttl_seconds: int = 1800):
        self._tickets: dict[str, dict] = {}
        self._lock = asyncio.Lock()
        self.default_ttl_seconds = default_ttl_seconds

    async def create_ticket(
        self, username: str, session_id: str = "", ttl_seconds: Optional[int] = None
    ) -> str:
        """Create a secure short-lived ticket associated with an authenticated username."""
        ttl = ttl_seconds or self.default_ttl_seconds
        ticket = secrets.token_urlsafe(32)
        now = time.time()

        async with self._lock:
            # Clean up expired tickets periodically
            self._purge_expired_locked(now)
            self._tickets[ticket] = {
                "username": username,
                "session_id": session_id,
                "created_at": now,
                "expires_at": now + ttl,
            }

        lqcd_logger.info(
            f"Created dashboard ticket for user '{username}', expires in {ttl}s."
        )
        return ticket

    async def validate_ticket(self, ticket: str) -> Optional[dict]:
        """Validate a ticket and return user info if valid, else None."""
        if not ticket:
            return None

        now = time.time()
        async with self._lock:
            data = self._tickets.get(ticket)
            if data is None:
                return None
            if now > data["expires_at"]:
                del self._tickets[ticket]
                lqcd_logger.warning(f"Dashboard ticket {ticket[:8]}... expired.")
                return None
            return data

    async def revoke_ticket(self, ticket: str):
        """Revoke an active ticket."""
        async with self._lock:
            if ticket in self._tickets:
                del self._tickets[ticket]

    def _purge_expired_locked(self, now: float):
        expired = [k for k, v in self._tickets.items() if now > v["expires_at"]]
        for k in expired:
            del self._tickets[k]


# Global singleton instance
_dashboard_auth_manager = DashboardAuthManager()


def get_dashboard_auth_manager() -> DashboardAuthManager:
    return _dashboard_auth_manager


async def get_dashboard_user(
    request: Request,
    auth_credentials: Optional[HTTPAuthorizationCredentials] = Security(security_bearer),
) -> str:
    """
    FastAPI dependency to extract and validate dashboard user identity.
    Accepts token via Bearer header, query parameter (?token=...), or cookie.
    """
    token = None

    # 1. Check Bearer Authorization Header
    if auth_credentials and auth_credentials.credentials:
        token = auth_credentials.credentials

    # 2. Check query parameter 'token'
    if not token:
        token = request.query_params.get("token")

    # 3. Check cookie
    if not token:
        token = request.cookies.get("mcp_dashboard_token")

    # If no token provided
    if not token:
        # Check if local test user is allowed when debug mode is enabled
        debug_mode = os.getenv("DEBUG", "false").lower() == "true"
        allow_anonymous = os.getenv("MCP_ALLOW_ANONYMOUS_DASHBOARD", "false").lower() == "true"
        if debug_mode and allow_anonymous:
            return "test_user"

        raise HTTPException(
            status_code=401,
            detail="Missing dashboard authorization ticket. Please launch the dashboard via your MCP client.",
        )

    # Validate against dashboard auth manager
    mgr = get_dashboard_auth_manager()
    ticket_data = await mgr.validate_ticket(token)
    if ticket_data and ticket_data.get("username"):
        return ticket_data["username"]

    # Also check if it's a valid OIDC bearer token directly (e.g. from an API client)
    try:
        from lqcd_oidc_auth import validate_authorized_token, get_local_account

        valid, user_info = validate_authorized_token(token)
        if valid and user_info:
            user_login = (
                user_info.get("email")
                or user_info.get("preferred_username")
                or user_info.get("sub")
                or user_info.get("login")
            )

            if user_login is None:
                raise HTTPException(
                    status_code=401,
                    detail="OIDC token did not contain any usable user identifier.",
                )

            local_account = get_local_account(user_login)
            if local_account:
                lqcd_logger.info(f"User '{user_login}' found in local accounts.")
                return local_account

            lqcd_logger.error(f"No local account found for OIDC user '{user_login}'.")
            raise HTTPException(
                status_code=401,
                detail=f"User '{user_login}' is authenticated but does not have an account on this system.",
            )

    except Exception as e:
        lqcd_logger.debug(f"Direct OIDC validation error for dashboard: {e}")

    raise HTTPException(
        status_code=401,
        detail="Invalid or expired dashboard authorization ticket. Please re-open the dashboard from your MCP client.",
    )
