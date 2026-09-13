# This file contains UI helpers and the browser-based dashboard tool for slurm mcp server.
import asyncio
import json
import os
from pydantic import BaseModel, Field

from fastmcp import FastMCP
from fastmcp.server.context import Context as ServerContext
from fastmcp.apps import AppConfig

from slurm_mcp_server import (
    lqcd_slurm_manager,
    number_of_idle_nodes,
    get_all_mcp_servers,
    lqcd_mcp_servers,
    slurm_mcp,
)
import common_data as cdata

from lqcd_logger import lqcd_logger
from session_manager import lqcd_session_manager

# A subclass of cdata.SlurmMcpServer to define the data structure for MCP server 
# info that we want to display in the UI.
class MCPServerInfo(cdata.SlurmMcpServer):
    is_connected: bool = Field(False, 
                               description="Whether the current session is connected to this MCP server.")
    
    @classmethod
    def from_slurm_mcp_server(cls, server: cdata.SlurmMcpServer, connected: bool = False):
        return cls(**server.model_dump(), is_connected=connected)


# This routine is used by UI only when connect button is clicked.
@slurm_mcp.tool(
    name="connect_mcp_server",
    description="Connect to a specific MCP server by adding its name to the session.",
    tags={"slurm", "ui"},
)
async def connect_mcp_server(mcp_name: str, ctx: ServerContext) -> list[MCPServerInfo]:
    lqcd_logger.info(f"Connecting to MCP server {mcp_name}")
    session_id = ctx.session_id
    if session_id is None:
        lqcd_logger.error(
            "No session id found in context, cannot connect to MCP server."
        )
        return []
    
    connected_mcp_servers: set[str] = await lqcd_session_manager().get_resource(
        session_id, "connected_servers"
    )
    if connected_mcp_servers is None:
        connected_mcp_servers = set()
        connected_mcp_servers.add(mcp_name)
        await lqcd_session_manager().register(
            session_id, "connected_servers", connected_mcp_servers
        )
        lqcd_logger.info(f"Session {session_id} connected to backend {mcp_name}")
    else:
        if mcp_name in connected_mcp_servers:
            lqcd_logger.info(
                f"Session {session_id} already connected to backend {mcp_name}"
            )
        else:
            connected_mcp_servers.add(mcp_name)
            await lqcd_session_manager().register(
                session_id, "connected_servers", connected_mcp_servers
            )
            lqcd_logger.info(f"Session {session_id} connected to backend {mcp_name}")

    # To get mcp server name back again
    connected_mcp_servers_set: set[str] = await lqcd_session_manager().get_resource(
        session_id, "connected_servers"
    )
    lqcd_logger.info(
        f"Session {session_id} currently connected to backends: {connected_mcp_servers_set}"
    )

    # update the mcp server list
    all_servers: list[cdata.SlurmMcpServer] = await get_all_mcp_servers()
    server_info_list:list[MCPServerInfo] = []

    # No need to check user information because connected servers already
    # checked user information when building the list.
    for s in all_servers:
        is_connected = False
        if s.slurm_job_state == "RUNNING":
            if connected_mcp_servers_set and s.mcp_name in connected_mcp_servers_set:
                is_connected = True
            
        server_info = MCPServerInfo.from_slurm_mcp_server(s, connected=is_connected)
        server_info_list.append(server_info)
    
    return server_info_list
    

# return all connected mcp servers for this session.
@slurm_mcp.tool(
    name="all_connected_mcp_servers",
    description="Return all connected MCP servers for the current session.",
    tags={"slurm", "ui"},
)
async def all_connected_mcp_servers(ctx: ServerContext) -> list[str]:
    lqcd_logger.info(f"Fetching all connected MCP servers for session {ctx.session_id}")
    session_id = ctx.session_id
    if session_id is None:
        lqcd_logger.error(
            "No session id found in context, cannot check MCP server connection."
        )
        return []
    connected_mcp_servers: set[str] = await lqcd_session_manager().get_resource(
        session_id, "connected_servers"
    )
    lqcd_logger.info(
        f"Session {session_id} all connected backends: {connected_mcp_servers}"
    )
    if connected_mcp_servers is None:
        return []
    return list(connected_mcp_servers)


# Disconnect from a specific MCP server by removing its name from the session
# resource "connected_servers". Return the updated list of connected MCP servers after disconnection.
@slurm_mcp.tool(
    name="disconnect_mcp_server",
    description="Disconnect from a specific MCP server by removing its name from the session.",
    tags={"slurm", "ui"},
)
async def disconnect_mcp_server(mcp_name: str, ctx: ServerContext) -> list[str]:
    session_id = ctx.session_id
    if session_id is None:
        lqcd_logger.error(
            "No session id found in context, cannot disconnect from MCP server."
        )
        return []
    connected_mcp_servers: set[str] = await lqcd_session_manager().get_resource(
        session_id, "connected_servers"
    )
    if connected_mcp_servers and mcp_name in connected_mcp_servers:
        connected_mcp_servers.remove(mcp_name)
        if len(connected_mcp_servers) == 0:
            await lqcd_session_manager().unregister(session_id, "connected_servers")
            lqcd_logger.info(
                f"Session {session_id} disconnected from backend {mcp_name} and no more connected servers left, so unregistered the resource."
            )
        else:
            await lqcd_session_manager().register(
                session_id, "connected_servers", connected_mcp_servers
            )
        lqcd_logger.info(f"Session {session_id} disconnected from backend {mcp_name}")
    else:
        lqcd_logger.info(f"Session {session_id} is not connected to backend {mcp_name}")

    # To get mcp server name back again
    connected_mcp_servers_set: set[str] = await lqcd_session_manager().get_resource(
        session_id, "connected_servers"
    )

    # update the mcp server list
    all_servers: list[cdata.SlurmMcpServer] = await get_all_mcp_servers()
    server_info_list:list[str] = []

    # No need to check user information because connected servers already
    # checked user information when building the list.
    for s in all_servers:
        is_connected = False
        if s.slurm_job_state == "RUNNING":
            if connected_mcp_servers_set and s.mcp_name in connected_mcp_servers_set:
                is_connected = True
            
        if is_connected:
            server_info_list.append(s.mcp_name)

    return server_info_list
    
# Get all mcp server info and the connection status for this session, and return to UI for display.
@slurm_mcp.tool(
    name="get_all_mcp_server_info",
    description="Get all MCP server info and the connection status for this session, and return to UI for display.",
    tags={"slurm", "ui"},
)
async def get_all_mcp_server_info(ctx: ServerContext) -> list[MCPServerInfo]:
    all_servers: list[cdata.SlurmMcpServer] = await get_all_mcp_servers()
    connected_mcp_servers: list[str] = await all_connected_mcp_servers(ctx)

    server_info_list:list[MCPServerInfo] = []

    # No need to check user information because connected servers already
    # checked user information when building the list.
    for s in all_servers:
        is_connected = False
        if s.slurm_job_state == "RUNNING":
            if connected_mcp_servers and s.mcp_name in connected_mcp_servers:
                is_connected = True

        server_info = MCPServerInfo.from_slurm_mcp_server(s, connected=is_connected)
        server_info_list.append(server_info)

    return server_info_list


# Helper to silently launch browser locally if available
def _open_browser_silently(url: str) -> bool:
    import webbrowser
    devnull_fd = None
    saved_stdout_fd = None
    saved_stderr_fd = None
    try:
        devnull_fd = os.open(os.devnull, os.O_WRONLY)
        saved_stdout_fd = os.dup(1)
        saved_stderr_fd = os.dup(2)
        os.dup2(devnull_fd, 1)
        os.dup2(devnull_fd, 2)
    except Exception:
        pass

    try:
        res = webbrowser.open(url)
    except Exception:
        res = False
    finally:
        if saved_stdout_fd is not None:
            try:
                os.dup2(saved_stdout_fd, 1)
                os.close(saved_stdout_fd)
            except Exception:
                pass
        if saved_stderr_fd is not None:
            try:
                os.dup2(saved_stderr_fd, 2)
                os.close(saved_stderr_fd)
            except Exception:
                pass
        if devnull_fd is not None:
            try:
                os.close(devnull_fd)
            except Exception:
                pass
    return res


@slurm_mcp.tool(
    name="slurm_dashboard",
    description="Launch the interactive web-based Slurm MCP Server Dashboard in your browser.",
    tags={"slurm", "ui"},
)
async def slurm_dashboard(ctx: ServerContext) -> str:
    """Launch the Slurm MCP Server Web Dashboard with pre-authenticated ticket."""
    sid = ctx.session_id or ""

    user = await lqcd_session_manager().get_resource(sid, "username")
    if user is None:
        # Call validate_user tool
        from slurm_mcp_server import validate_user
        await validate_user(username="", ctx=ctx)
        user = await lqcd_session_manager().get_resource(sid, "username")

    if user is None:
        user = "unknown"

    from dashboard_auth import get_dashboard_auth_manager
    ticket = await get_dashboard_auth_manager().create_ticket(user, sid)

    # Derive base URL directly from the MCP proxy server request context
    base_url = ""
    if hasattr(ctx, "request_context") and ctx.request_context is not None:
        req = getattr(ctx.request_context, "request", None)
        if req is not None:
            scheme = req.headers.get("x-forwarded-proto", req.url.scheme if hasattr(req, "url") and hasattr(req.url, "scheme") else "http")
            host = req.headers.get("x-forwarded-host", req.headers.get("host"))
            if host:
                base_url = f"{scheme}://{host}".rstrip("/")
            elif hasattr(req, "base_url"):
                base_url = str(req.base_url).rstrip("/")

    if not base_url:
        base_url = (os.getenv("PROXY_URL") or os.getenv("DASHBOARD_PUBLIC_URL") or "http://localhost:8123").rstrip("/")

    dashboard_url = f"{base_url}/jlab/lqcd/mcp/dashboard?token={ticket}"

    # Try opening locally
    opened = _open_browser_silently(dashboard_url)

    # Query brief cluster info for LLM response
    num_idle = await number_of_idle_nodes()
    all_servers = await get_all_mcp_servers()
    running_count = sum(1 for s in all_servers if s.slurm_job_state == "RUNNING")

    status_msg = "Browser window opened automatically." if opened else "Please open the direct link below in your browser."

    return (
        f"### 🚀 Slurm MCP Web Dashboard\n\n"
        f"The Slurm MCP Dashboard has been prepared for user **`{user}`**.\n\n"
        f"👉 **[Click Here to Open Dashboard]({dashboard_url})**\n\n"
        f"- **Direct URL:** `{dashboard_url}`\n"
        f"- **Idle Compute Nodes:** {num_idle}\n"
        f"- **Running MCP Servers:** {running_count}\n"
        f"- **Status:** {status_msg}\n\n"
        f"Use the dashboard to monitor Slurm cluster capacity, launch GPU/CPU MCP servers, introspect tools, view logs, and copy MCP client configurations."
    )
