# This file contains UI for slurm mcp server.
# We are using FastMCP/prefab ui to build this slurm dashboard
# which allows users to launch slurm mcp servers without calling
# directly to client_ui code. Instead, we ask AI agents to edit
# mcp configuration files inside the ides.
import asyncio

from fastmcp import FastMCP
from fastmcp.server.context import Context as ServerContext
from fastmcp.apps import AppConfig
from prefab_ui import PrefabApp
from prefab_ui.themes import Presentation
import prefab_ui.components as pc
from prefab_ui.actions.mcp import CallTool, SendMessage
from prefab_ui.actions import OpenFilePicker, SetState, AppendState, ShowToast
from prefab_ui.actions import FileUpload, SetInterval
from prefab_ui.rx import EVENT, RESULT, Rx
from pydantic import BaseModel, Field
import json

from requests import session

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

@slurm_mcp.tool(
    name="slurm_dashboard",
    description="Interactive UI to view and manage Slurm backend servers.",
    tags={"slurm", "ui"},
    app=True,
)
async def slurm_dashboard(ctx: ServerContext) -> PrefabApp:
    """Return a PrefabApp containing the Slurm Servers Dashboard."""
    sid = ctx.session_id

    user = await lqcd_session_manager().get_resource(sid, "username")
    if user is None:
        # Call validate_user tool
        from slurm_mcp_server import validate_user

        await validate_user(username="", ctx=ctx)
        # check user again
        user = await lqcd_session_manager().get_resource(sid, "username")
        if user is None:
            ctx.error("Cannot find username associated with this session.")
            lqcd_logger.error("Cannot find username associated with this session.")
            return PrefabApp(
                title="LQCD Slurm Dashboard",
                view=pc.Column(
                    children=[
                        pc.Text(
                            content="Error: Cannot find username associated with this session."
                        ),
                    ],
                ),
            )

    # Get number of idle nodes in the cluster
    num_idle_nodes = await number_of_idle_nodes()
    # Get all mcp servers at this moment
    all_mcp_servers:list[MCPServerInfo] = await get_all_mcp_server_info(ctx)

    with pc.Column(gap=4) as view:
        # --- Blocking Modal Overlay ---
        with pc.If(Rx("is_launching")):
            with pc.Column(
                css_class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm"
            ):
                with pc.Card(css_class="w-96 shadow-xl"):
                    with pc.CardHeader():
                        pc.CardTitle(content="Launching Server...")
                    with pc.CardContent():
                        pc.Text(
                            content="Please wait while the server is being provisioned. This may take a few moments."
                        )
        # ------------------------------

        pc.H2(content="LQCD Slurm Proxy Dashboard")
        pc.Separator()
        # Display of idle nodes and launch button to launch a server
        num = Rx("idle_nodes")
        # Waiting counter to show the launcned job state changing from PENDING to RUNNING
        waiting_counter = Rx("launch_waiting_counter")

        with pc.Row(gap=2, justify="between", align="center") as idle_nodes_disaplay:
            pc.Badge(
                label=f"{num} Idle Nodes Available", variant="info", css_class="text-lg"
            )

            # Refresh button to fetch the latest Slurm job states
            pc.Button(
                label="Refresh",
                variant="outline",
                icon="refresh-cw",
                on_click=[
                    CallTool(
                        "get_all_mcp_server_info",
                        on_success=SetState("mcp_servers", RESULT),
                        on_error=ShowToast("Failed to refresh status", variant="error"),
                    ),
                ],
            )
        with pc.Row(gap=2, justify="start", align="center") as launch_row:
            pc.Input(
                name="launch_mcp_name",
                placeholder="Enter MCP server name...",
                required=True,
                maxLength=30,
                css_class="w-64",
            )
            mcp_name_text = Rx("launch_mcp_name")
            with pc.If(Rx("launch_mcp_name") != ""):
                pc.Button(
                    label="1. Select Script",
                    variant="default",
                    icon="file-search",
                    on_click=OpenFilePicker(
                        accept=".sh", on_success=SetState("selected_files", RESULT)
                    ),
                )
            with pc.Else():
                pc.Button(
                    label="1. Select Script",
                    variant="default",
                    disabled=True,
                    icon="file-search",
                )

            with pc.If(Rx("selected_files") != None):
                pc.Button(
                    label="2. Launch MCP Server",
                    variant="default",
                    icon="rocket",
                    on_click=[
                        SetState("is_launching", True),
                        SetState("launch_waiting_counter", 6),
                        CallTool(
                            "launch_mcp_server_using_script",
                            arguments={
                                "mcp_name": mcp_name_text,
                                "wait": False,
                                "submission_script": Rx("selected_files")[0].data,
                                "base64_content": True,
                            },
                            on_success=[
                                AppendState("mcp_servers", RESULT),
                                SetState("selected_files", None),
                                SetState("is_launching", RESULT.slurm_job_state != "RUNNING"),
                                ShowToast("MCP server launched!", variant="success"),
                            ],
                            on_error=[
                                ShowToast("Launch mcp server failed", variant="error"),
                                SetState("is_launching", False),
                            ],
                        ),
                        SetInterval(
                            5000,
                            while_=waiting_counter > 0,
                            on_tick=[
                                CallTool(
                                    "get_all_mcp_server_info",
                                    on_success=[
                                        SetState("mcp_servers", RESULT),
                                        SetState(
                                            "launch_waiting_counter",
                                            waiting_counter - 1,
                                        ),
                                    ],
                                    on_error=[ShowToast("Failed to check server status", variant="error"),
                                              SetState("is_launching", False),
                                              SetState("launch_waiting_counter",0),
                                              SetState("launch_mcp_name",""),
                                            ],
                                ),
                            ],
                            on_complete=[
                                SetState("launch_mcp_name", ""),
                                SetState("is_launching", False),
                                ShowToast(
                                    "MCP server launch polling completed.",
                                    variant="info",
                                ),
                            ],
                        ),
                    ],
                )
            with pc.Else():
                pc.Button(
                    label="2. Launch MCP Server",
                    variant="default",
                    disabled=True,
                    icon="rocket",
                )
        pc.Separator()

        # table of active servers
        with pc.Table() as servers_table:
            with pc.TableHeader():
                with pc.TableRow():
                    pc.TableHead(content="MCP Name")
                    pc.TableHead(content="Status")
                    pc.TableHead(content="Job ID")
                    pc.TableHead(content="Owner")
                    pc.TableHead(content="Actions")
            with pc.TableBody():
                with pc.ForEach("mcp_servers") as s:
                    with pc.TableRow():
                        pc.TableCell(s.mcp_name)
                        with pc.TableCell():
                            with pc.If(s.slurm_job_state == "RUNNING"):
                                pc.Badge(label=s.slurm_job_state, variant="success")
                            with pc.Elif(s.slurm_job_state == "PENDING"):
                                pc.Badge(label=s.slurm_job_state, variant="info")
                            with pc.Else():
                                pc.Badge(label=s.slurm_job_state, variant="destructive")

                        pc.TableCell(s.slurm_job_id)
                        with pc.TableCell():
                            with pc.If(s.owner == user):
                                pc.Badge(label=s.owner, variant="success")
                            with pc.Else():
                                pc.Badge(label=s.owner, variant="warning")

                        with pc.TableCell():
                            with pc.If(s.owner == user):
                                with pc.ButtonGroup():
                                    # Here is a tricky one, which I haven't figured out how to do it in reflex yet.
                                    with pc.If(s.is_connected):
                                        pc.Button(
                                            label="Connect",
                                            variant="outline",
                                            icon="bot",
                                            disabled=True,
                                        )
                                        pc.Button(
                                            label="Disconnect",
                                            variant="outline",
                                            icon="bot",
                                            on_click=[
                                                CallTool(
                                                    "disconnect_mcp_server",
                                                    arguments={"mcp_name": s.mcp_name},
                                                    on_success=[
                                                        SetState(
                                                            "connected_mcp_servers",
                                                            RESULT,
                                                        ),
                                                        SendMessage(
                                                            f"Please find and remove the MCP server named '{s.mcp_name}' "
                                                            f"from my IDE's MCP configuration file (e.g. ~/.config/Code/User/mcp.json or claude_desktop_config.json)."
                                                        ),
                                                    ],
                                                ),
                                            ],
                                        )
                                        pc.Button(
                                            label="Kill",
                                            variant="destructive",
                                            on_click=CallTool(
                                                "cancel_mcp_server_by_jobid",
                                                arguments={"job_id": s.slurm_job_id},
                                                on_success=[
                                                    SetState("connected_server", ""),
                                                    SendMessage(
                                                        f"Please find and remove the MCP server named '{s.mcp_name}' "
                                                        f"from my IDE's MCP configuration file (e.g. ~/.config/Code/User/mcp.json or claude_desktop_config.json)."
                                                    ),
                                                ],
                                            ),
                                        )
                                    with pc.Else():
                                        pc.Button(
                                            label="Connect",
                                            variant="outline",
                                            icon="bot",
                                            disabled=(s.slurm_job_state == "PENDING"),
                                            on_click=[
                                                CallTool(
                                                    "connect_mcp_server",
                                                    arguments={"mcp_name": s.mcp_name},
                                                    on_success=[
                                                        SetState(
                                                            "connected_mcp_servers",
                                                            RESULT,
                                                        ),
                                                        SendMessage(
                                                            f"Please add a new StreamableHttp MCP server named '{s.mcp_name}' "
                                                            f"to my IDE's MCP configuration file (e.g. ~/.config/Code/User/mcp.json or claude_desktop_config.json). "
                                                            f"The URL for this server should use the same host and port as the current proxy server, "
                                                            f"but with the path '/cloud/{s.mcp_name}/mcp'. Also, use the exact same authentication "
                                                            f"token and headers as the proxy server."
                                                        ),
                                                        ShowToast(
                                                            "Connected to MCP server!",
                                                            variant="success",
                                                        ),
                                                    ],
                                                    on_error=ShowToast(
                                                        "Failed to connect to MCP server. Please make sure you have added this MCP server to your IDE configuration file and try again.",
                                                        variant="error",
                                                    ),
                                                ),
                                            ],
                                        )
                                        pc.Button(
                                            label="Disconnect",
                                            variant="outline",
                                            icon="bot",
                                            disabled=True,
                                        )
                                        pc.Button(
                                            label="Kill",
                                            variant="destructive",
                                            on_click=CallTool(
                                                "cancel_mcp_server_by_jobid",
                                                arguments={"job_id": s.slurm_job_id},
                                            ),
                                        )
                            with pc.Else():
                                pc.Text("")
        return PrefabApp(
            title="LQCD Slurm Dashboard",
            view=view,
            theme=Presentation(accent="sky"),
            state={
                "mcp_servers": all_mcp_servers,
                "selected_files": None,
                "launch_mcp_name": "",
                "mcp_name_entered": False,
                "launch_waiting_counter": 6,
                "idle_nodes": num_idle_nodes,
                "is_launching": False,
            },
        )
