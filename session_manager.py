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
        
    async def unregister(self, session_id: str, resource_name: str):
        """Unregister a specific resource from a session."""
        async with self._lock:
            lqcd_logger.info(
                f"🧹 [Manager] Unregistering '{resource_name}' from session {session_id}..."
            )
            if session_id in self._sessions and resource_name in self._sessions[session_id]:
                # --- CUSTOM CLEANUP LOGIC HERE ---
                # Example: Close a mock database connection
                # lqcd_logger.info(f"   -> Closing '{resource_name}' for session {session_id}...")
                # ---------------------------------
                del self._sessions[session_id][resource_name]
                lqcd_logger.info(
                    f"✅ [Manager] Unregistered '{resource_name}' from session {session_id}"
                )
            else:
                lqcd_logger.warning(
                    f"⚠️  [Manager] Resource '{resource_name}' not found for session {session_id}"
                )
        
    # clean up a particular resource for all sessions
    # useful for cleaning up shared resources like a connection pool
    async def cleanup_resource_all_sessions(self, resource_name: str):
        async with self._lock:
            lqcd_logger.info(f"🧹 [Manager] Cleaning up resource '{resource_name}' for all sessions...")
            for session_id, resources in self._sessions.items():
                if resource_name in resources:
                    # --- CUSTOM CLEANUP LOGIC HERE ---
                    # Example: Close a mock database connection
                    # lqcd_logger.info(f"   -> Closing '{resource_name}' for session {session_id}...")
                    # ---------------------------------
                    del resources[resource_name]
            lqcd_logger.info(f"✨ [Manager] Resource '{resource_name}' cleaned up for all sessions.")

    # Remove a specific resource from all sessions which contains a set of values, e.g., a list of connected backend servers. This is useful for cleaning up the session state on UI when a backend server is removed from the system.
    async def remove_resource_all_sessions_set(self, resource_name: str, value):
        async with self._lock:
            lqcd_logger.info(f"🧹 [Manager] Removing value '{value}' from resource '{resource_name}' for all sessions...")
            for session_id, resources in self._sessions.items():
                if resource_name in resources and isinstance(resources[resource_name], set):
                    resources[resource_name].discard(value)
            lqcd_logger.info(f"✨ [Manager] Value '{value}' removed from resource '{resource_name}' for all sessions.")
    
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


# factory method to retrive the session manager
_lqcd_session_manager = None


def lqcd_session_manager() -> SessionManager:
    """Get the global session manager."""
    global _lqcd_session_manager
    if _lqcd_session_manager is None:
        _lqcd_session_manager = SessionManager()
    return _lqcd_session_manager
