# dashboard_router.py
# FastAPI router providing web dashboard endpoints and tool introspection under /jlab/lqcd/mcp/dashboard
import asyncio
import glob
import json
import os
import pwd
import subprocess
from typing import Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from lqcd_logger import lqcd_logger
from dashboard_auth import get_dashboard_user, get_dashboard_auth_manager
import common_data as cdata
from session_manager import lqcd_session_manager

dashboard_router = APIRouter(prefix="/jlab/lqcd/mcp/dashboard", tags=["Slurm MCP Dashboard"])

STATIC_INDEX_HTML_PATH = os.path.join(
    os.path.dirname(__file__), "static", "slurm_dashboard", "index.html"
)


async def get_user_slurm_project_details(user: str) -> dict[str, Any]:
    """Retrieve Slurm default account and associated project accounts for a user."""
    default_account = ""
    accounts = []
    try:
        cmd_user = ["sacctmgr", "show", "user", user, "format=DefaultAccount", "-P", "-n"]
        proc = subprocess.Popen(cmd_user, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        proc.wait()
        if proc.stdout:
            out = proc.stdout.read().strip()
            if out:
                default_account = out.split("\n")[0].strip()
    except Exception as e:
        lqcd_logger.debug(f"Error checking default Slurm account for {user}: {e}")

    try:
        cmd_assoc = ["sacctmgr", "show", "associations", "where", f"user={user}", "format=Account", "-P", "-n"]
        proc = subprocess.Popen(cmd_assoc, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        proc.wait()
        if proc.stdout:
            for line in proc.stdout:
                ac = line.strip()
                if ac and ac not in accounts:
                    accounts.append(ac)
    except Exception as e:
        lqcd_logger.debug(f"Error checking Slurm project associations for {user}: {e}")

    if not default_account and accounts:
        default_account = accounts[0]

    return {
        "user": user,
        "default_account": default_account,
        "accounts": accounts,
    }


# Request Models
class LaunchServerRequest(BaseModel):
    mcp_name: str = Field(..., description="Unique name for the MCP server")
    run_script: Optional[str] = Field(None, description="Path to existing local slurm batch script file on server")
    custom_script_content: Optional[str] = Field(None, description="Slurm batch script text content")


class CancelServerRequest(BaseModel):
    job_id: Optional[int] = Field(None, description="Slurm job ID")
    mcp_name: Optional[str] = Field(None, description="MCP server name")


class SubmitRegularJobRequest(BaseModel):
    job_name: str = Field(..., description="Name for the Slurm job")
    submission_script: Optional[str] = Field(
        None, description="Slurm batch script text content"
    )
    script_file: Optional[str] = Field(
        None, description="Path to existing script file on server"
    )


class CancelRegularJobRequest(BaseModel):
    job_id: int = Field(..., description="Slurm job ID to cancel")


class ExecuteToolRequest(BaseModel):
    arguments: Optional[dict[str, Any]] = Field(
        default_factory=dict, description="Tool execution arguments dictionary"
    )


# --- HTML Dashboard View ---
@dashboard_router.get("", response_class=HTMLResponse)
@dashboard_router.get("/", response_class=HTMLResponse)
async def serve_dashboard_page(request: Request):
    """Serve the static Single Page Dashboard HTML."""
    if not os.path.isfile(STATIC_INDEX_HTML_PATH):
        raise HTTPException(
            status_code=404,
            detail=f"Dashboard static asset not found at {STATIC_INDEX_HTML_PATH}",
        )
    with open(STATIC_INDEX_HTML_PATH, "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)


# --- REST API Endpoints ---
@dashboard_router.get("/status")
async def get_dashboard_status(
    request: Request, current_user: str = Depends(get_dashboard_user)
) -> dict[str, Any]:
    """Get Slurm cluster capacity, idle nodes, partitions, and user accounts."""
    try:
        from slurm_mcp_server import lqcd_slurm_manager

        # Query system computing info
        system_info: cdata.FullSystemResource = await lqcd_slurm_manager.get_slurm_info()
        
        # Calculate idle nodes
        idle_nodes = 0
        partitions_data = []
        if system_info and system_info.partitions:
            for p in system_info.partitions:
                idle_nodes += getattr(p, "num_free_nodes", 0)
                partitions_data.append({
                    "partition_name": getattr(p, "partition_name", ""),
                    "type": getattr(p, "type", "CPU"),
                    "total_nodes": getattr(p, "num_nodes", 0),
                    "num_free_nodes": getattr(p, "num_free_nodes", 0),
                    "num_busy_nodes": getattr(p, "num_busy_nodes", 0),
                    "cpus_per_node": getattr(p, "num_cpus_per_node", 0),
                    "memory": f"{getattr(p, 'memory_per_node_gb', 0)}G" if getattr(p, "memory_per_node_gb", None) else "N/A",
                    "gres": getattr(p, "generic_resource", ""),
                    "time_limit": getattr(p, "wall_time_limit", ""),
                })

        # Get accounts & project info for this user
        from lqcd_oidc_auth import can_user_launch_mcp
        allow_mcp = can_user_launch_mcp(current_user)
        project_details = await get_user_slurm_project_details(current_user)
        accounts = project_details.get("accounts", [])
        default_account = project_details.get("default_account", "")

        return {
            "status": "success",
            "user": current_user,
            "allow_mcp": allow_mcp,
            "idle_nodes": idle_nodes,
            "cluster_name": "Jefferson Lab LQCD Cluster",
            "partitions": partitions_data,
            "accounts": accounts,
            "default_account": default_account,
            "project_info": {
                "user": current_user,
                "default_account": default_account,
                "accounts": accounts,
                "allow_mcp": allow_mcp,
            },
        }
    except Exception as e:
        lqcd_logger.error(f"Error fetching dashboard status: {e}")
        return {
            "status": "error",
            "user": current_user,
            "idle_nodes": 0,
            "cluster_name": "Unknown",
            "partitions": [],
            "accounts": [],
            "error": str(e),
        }


@dashboard_router.get("/servers")
async def list_dashboard_servers(
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """List all Slurm MCP servers and current job states."""
    try:
        from slurm_mcp_server import lqcd_mcp_servers, lqcd_slurm_manager

        all_servers: list[cdata.SlurmMcpServer] = (
            await lqcd_mcp_servers.get_all_slurm_mcp_servers()
        )

        servers_data = []
        for s in all_servers:
            # Sync / verify job state if job_id is valid
            state = s.slurm_job_state
            if s.slurm_job_id and s.slurm_job_id > 0:
                try:
                    checked_state = await lqcd_slurm_manager.check_slurm_job_state(
                        str(s.slurm_job_id)
                    )
                    if checked_state:
                        state = checked_state
                        s.slurm_job_state = checked_state
                except Exception:
                    pass

            node = getattr(s, "assigned_node", "")
            port = getattr(s, "assigned_port", "")
            if (not node or not port) and s.url:
                try:
                    from urllib.parse import urlparse
                    u = urlparse(s.url)
                    if not node:
                        node = u.hostname or ""
                    if not port and u.port:
                        port = str(u.port)
                except Exception:
                    pass

            servers_data.append({
                "mcp_name": s.mcp_name,
                "job_id": s.slurm_job_id,
                "job_name": s.slurm_job_name,
                "owner": s.owner,
                "state": state,
                "node": node or "Pending",
                "port": port or "Pending",
                "url": s.url,
                "is_owner": (s.owner == current_user),
                "error_message": s.error_message,
                "valid": s.valid,
            })

        return {"status": "success", "servers": servers_data}
    except Exception as e:
        lqcd_logger.error(f"Error listing dashboard servers: {e}")
        return {"status": "error", "servers": [], "error": str(e)}


@dashboard_router.get("/servers/{mcp_name}/tools")
async def get_server_tools(
    mcp_name: str,
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """
    Connect to a running backend MCP server and introspect its available tools,
    docstrings, and input schemas.
    """
    try:
        from slurm_mcp_server import lqcd_mcp_servers
        from fastmcp import Client
        from fastmcp.client.transports import StreamableHttpTransport

        server: Optional[cdata.SlurmMcpServer] = (
            await lqcd_mcp_servers.get_slurm_mcp_server(mcp_name)
        )
        if not server:
            raise HTTPException(
                status_code=404, detail=f"MCP server '{mcp_name}' not found."
            )

        if server.slurm_job_state != "RUNNING" or not server.url:
            return {
                "status": "pending",
                "mcp_name": mcp_name,
                "state": server.slurm_job_state,
                "message": f"Server '{mcp_name}' is currently {server.slurm_job_state}. Tools will become available once running.",
                "tools": [],
            }

        # Connect to backend MCP server with timeout
        lqcd_logger.info(f"Querying tools for backend MCP '{mcp_name}' at {server.url}...")
        transport = StreamableHttpTransport(url=server.url)

        tools_list = []
        try:
            async with asyncio.timeout(10):
                async with Client(transport=transport) as client:
                    tools = await client.list_tools()
                    for t in tools:
                        params = (
                            getattr(t, "inputSchema", None)
                            or getattr(t, "input_schema", None)
                            or getattr(t, "parameters", None)
                        )
                        if params is None and isinstance(t, dict):
                            params = (
                                t.get("inputSchema")
                                or t.get("input_schema")
                                or t.get("parameters")
                                or {}
                            )
                        if params is not None and not isinstance(params, dict):
                            try:
                                params = (
                                    params.model_dump()
                                    if hasattr(params, "model_dump")
                                    else dict(params)
                                )
                            except Exception:
                                params = {}

                        tool_name = (
                            getattr(t, "name", None)
                            or (t.get("name") if isinstance(t, dict) else None)
                            or "unknown"
                        )
                        tool_desc = (
                            getattr(t, "description", None)
                            or (t.get("description") if isinstance(t, dict) else None)
                            or "No description provided."
                        )

                        tools_list.append({
                            "name": str(tool_name),
                            "description": str(tool_desc),
                            "parameters": params or {},
                        })
        except asyncio.TimeoutError:
            lqcd_logger.warning(f"Timeout querying tools from backend server '{mcp_name}' at {server.url}")
            return {
                "status": "timeout",
                "mcp_name": mcp_name,
                "message": "Timed out connecting to backend server to list tools.",
                "tools": [],
            }
        except Exception as e:
            lqcd_logger.error(f"Error querying tools from backend '{mcp_name}': {e}")
            return {
                "status": "error",
                "mcp_name": mcp_name,
                "message": f"Could not fetch tools: {str(e)}",
                "tools": [],
            }

        return {
            "status": "success",
            "mcp_name": mcp_name,
            "tool_count": len(tools_list),
            "tools": tools_list,
        }
    except HTTPException:
        raise
    except Exception as e:
        lqcd_logger.error(f"Error introspecting tools for {mcp_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_router.post("/servers/{mcp_name}/tools/{tool_name}/execute")
async def execute_server_tool(
    mcp_name: str,
    tool_name: str,
    payload: ExecuteToolRequest = ExecuteToolRequest(),
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """Execute a specific MCP tool on a running backend server and return the formatted result."""
    try:
        from slurm_mcp_server import lqcd_mcp_servers
        from fastmcp import Client
        from fastmcp.client.transports import StreamableHttpTransport

        server: Optional[cdata.SlurmMcpServer] = (
            await lqcd_mcp_servers.get_slurm_mcp_server(mcp_name)
        )
        if not server:
            raise HTTPException(
                status_code=404, detail=f"MCP server '{mcp_name}' not found."
            )

        if server.slurm_job_state != "RUNNING" or not server.url:
            raise HTTPException(
                status_code=400,
                detail=f"Server '{mcp_name}' is not running (state: {server.slurm_job_state}).",
            )

        args = payload.arguments or {}
        lqcd_logger.info(
            f"User '{current_user}' executing tool '{tool_name}' on backend '{mcp_name}' at {server.url} with args: {args}"
        )
        transport = StreamableHttpTransport(url=server.url)
        try:
            async with asyncio.timeout(60):
                async with Client(transport=transport) as client:
                    result = await client.call_tool(tool_name, args)
                    is_error = getattr(result, "isError", False)
                    outputs = []
                    if hasattr(result, "content") and result.content:
                        for c in result.content:
                            text_val = getattr(c, "text", None)
                            if text_val is not None:
                                outputs.append(str(text_val))
                            else:
                                outputs.append(str(c))
                        text_output = "\n".join(outputs) if outputs else str(result)
                    else:
                        text_output = str(result)

                    return {
                        "status": "error" if is_error else "success",
                        "mcp_name": mcp_name,
                        "tool_name": tool_name,
                        "is_error": is_error,
                        "output": text_output,
                    }
        except asyncio.TimeoutError:
            lqcd_logger.warning(
                f"Timeout executing tool '{tool_name}' on backend '{mcp_name}'"
            )
            return {
                "status": "timeout",
                "mcp_name": mcp_name,
                "tool_name": tool_name,
                "is_error": True,
                "output": f"Timeout (60s) executing tool '{tool_name}' on '{mcp_name}'.",
            }
        except Exception as e:
            lqcd_logger.error(
                f"Error executing tool '{tool_name}' on '{mcp_name}': {e}"
            )
            return {
                "status": "error",
                "mcp_name": mcp_name,
                "tool_name": tool_name,
                "is_error": True,
                "output": f"Error: {str(e)}",
            }
    except HTTPException:
        raise
    except Exception as e:
        lqcd_logger.error(f"Error handling tool execution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_router.post("/servers/launch")
async def launch_dashboard_server(
    payload: LaunchServerRequest,
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """Submit a Slurm batch job to launch a new MCP server using a local script file or script content."""
    try:
        from slurm_mcp_server import lqcd_slurm_manager, lqcd_mcp_servers
        from lqcd_oidc_auth import can_user_launch_mcp

        # Check MCP launch permission
        if not can_user_launch_mcp(current_user):
            lqcd_logger.warning(
                f"SECURITY ALERT: Dashboard user '{current_user}' attempted unauthorized MCP server launch."
            )
            raise HTTPException(
                status_code=403,
                detail=f"Permission Denied: User '{current_user}' is not authorized to launch interactive MCP servers on cluster nodes. Please submit regular Slurm batch jobs instead.",
            )

        mcp_name = payload.mcp_name.strip()
        if not mcp_name:
            raise HTTPException(status_code=400, detail="mcp_name cannot be empty")

        # Check existing
        existing = await lqcd_mcp_servers.get_slurm_mcp_server(mcp_name)
        if existing and existing.slurm_job_state in ["RUNNING", "PENDING"]:
            raise HTTPException(
                status_code=409,
                detail=f"An active MCP server with name '{mcp_name}' already exists.",
            )

        # Resolve submission script content
        submission_script = ""
        if payload.custom_script_content and payload.custom_script_content.strip():
            submission_script = payload.custom_script_content.strip()
        elif payload.run_script and payload.run_script.strip():
            script_path = os.path.expanduser(payload.run_script.strip())
            if not os.path.isfile(script_path):
                raise HTTPException(
                    status_code=400,
                    detail=f"Script file '{payload.run_script}' was not found on the server.",
                )
            try:
                with open(script_path, "r", encoding="utf-8", errors="replace") as f:
                    submission_script = f.read().strip()
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to read script file '{payload.run_script}': {e}",
                )
        else:
            raise HTTPException(
                status_code=400,
                detail="Please specify either a local Slurm script file path or paste the script content.",
            )

        if not submission_script:
            raise HTTPException(status_code=400, detail="Submission script content is empty.")

        # Ensure valid bash header
        if not submission_script.startswith("#!"):
            submission_script = "#!/bin/bash\n" + submission_script

        # Submit via slurm manager
        try:
            pw = pwd.getpwnam(current_user)
            gid = pw.pw_gid
        except Exception:
            gid = os.getgid()

        jobid = await lqcd_slurm_manager.submit_slurm_job(
            current_user, gid, mcp_name, submission_script
        )
        if not jobid:
            raise HTTPException(
                status_code=500, detail="Failed to submit Slurm job to cluster. Please check script syntax and user permissions."
            )

        # Register in tracking map
        new_server = cdata.SlurmMcpServer(
            mcp_name=mcp_name,
            slurm_job_id=int(jobid),
            slurm_job_name=mcp_name,
            owner=current_user,
            slurm_job_state="PENDING",
            valid=True,
        )
        await lqcd_mcp_servers.add_slurm_mcp_server(new_server)

        lqcd_logger.info(
            f"User '{current_user}' launched MCP server '{mcp_name}' with Slurm job ID {jobid}."
        )

        return {
            "status": "success",
            "message": f"Successfully submitted Slurm job for '{mcp_name}'.",
            "job_id": int(jobid),
            "mcp_name": mcp_name,
        }
    except HTTPException:
        raise
    except Exception as e:
        lqcd_logger.error(f"Error launching server: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_router.post("/servers/cancel")
async def cancel_dashboard_server(
    payload: CancelServerRequest,
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """Cancel / terminate a Slurm MCP server."""
    try:
        from slurm_mcp_server import lqcd_mcp_servers, lqcd_slurm_manager

        target_server: Optional[cdata.SlurmMcpServer] = None
        if payload.job_id:
            target_server = await lqcd_mcp_servers.get_slurm_mcp_server_by_jobid(
                payload.job_id
            )
        elif payload.mcp_name:
            target_server = await lqcd_mcp_servers.get_slurm_mcp_server(payload.mcp_name)

        if not target_server:
            # If job_id was directly passed, attempt direct slurm cancel
            if payload.job_id:
                res = await lqcd_slurm_manager.stop_slurm_job(str(payload.job_id))
                return {
                    "status": "success",
                    "message": f"Issued stop for Slurm job {payload.job_id}.",
                }
            raise HTTPException(status_code=404, detail="Server/Job not found.")

        # Check permissions (only owner or root)
        if target_server.owner != current_user and current_user != "root":
            raise HTTPException(
                status_code=403,
                detail=f"Permission denied: You are not the owner of {target_server.mcp_name}.",
            )

        job_id_str = str(target_server.slurm_job_id)
        stop_res = await lqcd_slurm_manager.stop_slurm_job(job_id_str)
        await lqcd_mcp_servers.remove_slurm_mcp_server_by_jobid(target_server.slurm_job_id)

        # Cleanup any active session registrations for this server
        await lqcd_session_manager().remove_resource_all_sessions_set(
            "connected_servers", target_server.mcp_name
        )

        lqcd_logger.info(
            f"User '{current_user}' cancelled MCP server '{target_server.mcp_name}' (Job {job_id_str})."
        )
        return {
            "status": "success",
            "message": f"MCP server '{target_server.mcp_name}' cancelled.",
            "job_id": target_server.slurm_job_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        lqcd_logger.error(f"Error cancelling server: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_router.get("/servers/{job_id}/logs")
async def get_server_logs(
    job_id: int,
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """Retrieve Slurm output log contents for a specific job."""
    try:
        patterns = [
            f"slurm-{job_id}.out",
            f"slurm-{job_id}.log",
            f"mcp_server_{job_id}.out",
            f"/tmp/slurm-{job_id}.out",
            f"/tmp/mcp_server_{job_id}.log",
            f"/home/{current_user}/slurm-{job_id}.out",
            f"/home/{current_user}/mcp_server_{job_id}.out",
            f"{os.path.expanduser('~')}/slurm-{job_id}.out",
        ]

        # Query scontrol to get exact StdOut path assigned by Slurm
        try:
            import subprocess
            proc = subprocess.Popen(
                ["scontrol", "show", "job", str(job_id)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            proc.wait()
            if proc.stdout:
                for line in proc.stdout:
                    if "StdOut=" in line:
                        for token in line.split():
                            if token.startswith("StdOut="):
                                stdout_file = token.split("=", 1)[1].strip()
                                if stdout_file and stdout_file not in patterns:
                                    patterns.insert(0, stdout_file)
        except Exception as e:
            lqcd_logger.debug(f"Could not inspect scontrol for job {job_id}: {e}")

        found_content = None
        found_path = None
        for p in patterns:
            matches = glob.glob(p)
            if matches:
                found_path = matches[0]
                try:
                    with open(found_path, "r", encoding="utf-8", errors="replace") as f:
                        found_content = f.read()
                    break
                except Exception:
                    pass

        if found_content is None:
            return {
                "status": "not_found",
                "job_id": job_id,
                "logs": f"Log file for job {job_id} not found or not yet flushed by Slurm.\nChecked locations:\n" + "\n".join(patterns[:5]),
            }

        return {
            "status": "success",
            "job_id": job_id,
            "path": found_path,
            "logs": found_content,
        }
    except Exception as e:
        lqcd_logger.error(f"Error reading logs for job {job_id}: {e}")
        return {"status": "error", "job_id": job_id, "logs": f"Error: {str(e)}"}


@dashboard_router.get("/mcp-config/{mcp_name}")
async def get_mcp_client_config(
    mcp_name: str,
    request: Request,
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """
    Generate ready-to-use JSON configuration snippets for Claude Desktop,
    VSCode (.vscode/mcp.json), and Codex.
    """
    try:
        from slurm_mcp_server import lqcd_mcp_servers

        server: Optional[cdata.SlurmMcpServer] = (
            await lqcd_mcp_servers.get_slurm_mcp_server(mcp_name)
        )

        base_url = str(request.base_url).rstrip("/")
        # Remote proxied endpoint
        proxied_url = f"{base_url}/cloud/{mcp_name}/mcp"
        direct_url = server.url if server else ""

        # VSCode mcp.json format
        vscode_config = {
            "mcpServers": {
                mcp_name: {
                    "url": proxied_url,
                    "type": "http",
                }
            }
        }

        # Claude Desktop format
        claude_config = {
            "mcpServers": {
                mcp_name: {
                    "url": proxied_url,
                }
            }
        }

        return {
            "status": "success",
            "mcp_name": mcp_name,
            "proxied_url": proxied_url,
            "direct_url": direct_url,
            "vscode_mcp_json": vscode_config,
            "claude_desktop_json": claude_config,
        }
    except Exception as e:
        lqcd_logger.error(f"Error generating MCP config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Regular Slurm Jobs Endpoints ---


@dashboard_router.get("/regular-jobs")
async def list_dashboard_regular_jobs(
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """List regular Slurm batch jobs for the authenticated user (excluding MCP server jobs)."""
    try:
        from slurm_mcp_server import lqcd_slurm_manager, lqcd_mcp_servers

        jobs = await lqcd_slurm_manager.get_user_regular_jobs(current_user)

        # Exclude active and registered MCP server jobs
        mcp_servers = await lqcd_mcp_servers.get_all_slurm_mcp_servers()
        mcp_job_ids = {
            s.slurm_job_id for s in mcp_servers if s.slurm_job_id and s.slurm_job_id > 0
        }
        filtered_jobs = [j for j in jobs if j.get("job_id") not in mcp_job_ids]

        return {
            "status": "success",
            "user": current_user,
            "jobs": filtered_jobs,
        }
    except Exception as e:
        lqcd_logger.error(f"Error fetching regular Slurm jobs: {e}")
        return {
            "status": "error",
            "user": current_user,
            "jobs": [],
            "error": str(e),
        }


@dashboard_router.post("/regular-jobs/submit")
async def submit_dashboard_regular_job(
    payload: SubmitRegularJobRequest,
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """Submit a regular Slurm batch job."""
    try:
        from slurm_mcp_server import lqcd_slurm_manager

        script_content = payload.submission_script
        if not script_content and payload.script_file:
            if not os.path.exists(payload.script_file):
                raise HTTPException(
                    status_code=400,
                    detail=f"Script file '{payload.script_file}' not found.",
                )
            with open(payload.script_file, "r", encoding="utf-8") as f:
                script_content = f.read()

        if not script_content:
            raise HTTPException(
                status_code=400,
                detail="Either submission_script or script_file must be provided.",
            )

        try:
            gid = pwd.getpwnam(current_user).pw_gid
        except Exception:
            gid = 0

        job_id = await lqcd_slurm_manager.submit_slurm_job(
            current_user, gid, payload.job_name, script_content
        )
        if not job_id:
            raise HTTPException(
                status_code=500,
                detail="Slurm rejected the batch submission script.",
            )

        job_state = (
            await lqcd_slurm_manager.check_slurm_job_state(str(job_id))
            or "PENDING"
        )
        return {
            "status": "success",
            "message": f"Successfully submitted regular Slurm job '{payload.job_name}'.",
            "job_id": int(job_id) if str(job_id).isdigit() else job_id,
            "job_name": payload.job_name,
            "state": job_state,
        }
    except HTTPException:
        raise
    except Exception as e:
        lqcd_logger.error(f"Error submitting regular Slurm job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_router.post("/regular-jobs/cancel")
async def cancel_dashboard_regular_job(
    payload: CancelRegularJobRequest,
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """Cancel a regular Slurm job."""
    try:
        from slurm_mcp_server import lqcd_slurm_manager

        stop_res = await lqcd_slurm_manager.stop_slurm_job(str(payload.job_id))
        is_success = (
            getattr(stop_res, "status", None)
            == cdata.SlurmJobCancelStatus._status_value.SUCCESS
        )
        return {
            "status": "success" if is_success else "error",
            "job_id": payload.job_id,
            "message": getattr(
                stop_res,
                "error_message",
                f"Issued cancellation for job {payload.job_id}.",
            ),
        }
    except Exception as e:
        lqcd_logger.error(f"Error cancelling regular Slurm job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_router.get("/regular-jobs/{job_id}/logs")
async def get_dashboard_regular_job_logs(
    job_id: int,
    current_user: str = Depends(get_dashboard_user),
) -> dict[str, Any]:
    """Fetch stdout/stderr logs for a regular Slurm job."""
    try:
        from slurm_mcp_server import lqcd_slurm_manager

        return await lqcd_slurm_manager.get_regular_job_logs(
            job_id, current_user
        )
    except Exception as e:
        lqcd_logger.error(f"Error reading regular job logs for {job_id}: {e}")
        return {"status": "error", "job_id": job_id, "logs": f"Error: {str(e)}"}

