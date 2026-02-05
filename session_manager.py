# This is a session manager to handle resource cleanup on user disconnects.
# It works with FastMCP to track resources per user session.
# It supports graceful disconnects via HTTP DELETE and abrupt disconnects
# via network drops or timeouts.

# This code is mostly from Gemini Pro
import asyncio
from typing import Any
from urllib import request


from fastmcp import FastMCP, Context

from lqcd_logger import lqcd_logger


# --- 1. The Session Manager ---
class SessionManager:
    def __init__(self):
        # Stores active resources: { session_id: { "db_conn": ..., "file_handle": ... } }
        self._sessions = {}
        self._lock = asyncio.Lock()
        lqcd_logger.info("🗄️  [Manager] SessionManager initialized.")

    async def register(self, session_id: str, resource_name: str, resource_obj):
        """Register a resource to a session so we can clean it up later."""
        async with self._lock:
            lqcd_logger.info(
                f"🔖 [Manager] Registering '{resource_name}' for session {session_id}..."
            )
            if session_id not in self._sessions:
                self._sessions[session_id] = {}
            self._sessions[session_id][resource_name] = resource_obj
            lqcd_logger.info(
                f"✅ [Manager] Registered '{resource_name}' for session {session_id}"
            )

    async def get_resources(self, session_id: str) -> dict:
        """Get all resources associated with a session."""
        async with self._lock:
            return self._sessions.get(session_id, {})

    # Check if a session exists
    async def session_exists(self, session_id: str) -> bool:
        async with self._lock:
            return session_id in self._sessions

    async def get_resource(self, session_id: str, resource_name: str) -> Any:
        """Get a specific resource associated with a session."""
        async with self._lock:
            session_resources = self._sessions.get(session_id, {})
            return session_resources.get(resource_name, None)

    async def cleanup(self, session_id: str):
        """
        Idempotent cleanup. Safe to call multiple times.
        """
        async with self._lock:
            if session_id not in self._sessions:
                # Already cleaned up, or never existed
                return

            resources = self._sessions[session_id]
            lqcd_logger.info(f"🧹 [Manager] Cleaning up session {session_id}...")

            # --- CUSTOM CLEANUP LOGIC HERE ---
            # Example: Close a mock database connection
            # if "db_conn" in resources:
            #    lqcd_logger.info(f"   -> Closing database connection: {resources['db_conn']}")
            # ---------------------------------

            # Remove session from tracker
            del self._sessions[session_id]
            lqcd_logger.info(f"✨ [Manager] Session {session_id} fully purged.")


# Initialize the global manager
lqcd_session_manager = SessionManager()
