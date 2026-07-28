# This is the top level mcp server for lqcd ananlysis tasks.
# This server uses FastAPI to route http requests to different mcp servers.
# This implementation is better than the FastMCP Proxy implementation.
# In addtion, a client can directly connect to a backend server once the
# the client specify the backend server in the request. This is very useful
# to launch mcp severs on remote cluster using slurm.
# Furthermore, this proxy server keeps the session state of connected pairs.
import argparse
import asyncio
import time
import json
import os
import sys
from dotenv import load_dotenv
from fastapi.security import HTTPBearer
import httpx
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import Response
from fastmcp import FastMCP, Context
from fastmcp.utilities.logging import configure_logging as fastmcp_configure_logging
from fastmcp.server.middleware import MiddlewareContext
from starlette.types import Message
import uvicorn
import importlib.metadata
from packaging import version
from uvicorn.config import LOGGING_CONFIG

# Our own logger
import logging
from lqcd_logger import lqcd_logger, setup_lqcd_file_logger, is_logging_to_file

# Global settings
import lqcd_mcp_settings

# load .env file
load_dotenv(os.path.join(os.path.dirname(__file__), ".server_env"), override=True)

# check log file name
log_file_name = os.getenv("PROXY_LOG_FILE")
if log_file_name:
    setup_lqcd_file_logger(log_file_name)

lqcd_logger.info("Loading .server_env file...")
# Check whether we are running inside jlab network
_jlab_slurm = os.getenv("MCP_USE_JLAB_SLURM", "true").lower() == "true"

# check debug mode
_debug = os.getenv("DEBUG", "false").lower() == "true"

if _jlab_slurm:
    lqcd_logger.info("Our server uses Jlab slurm system.")
else:
    lqcd_logger.info("Our server uses local test slurm system.")

if _debug:
    lqcd_logger.setLevel(logging.DEBUG)
    if is_logging_to_file():
        logging.getLogger("fastmcp").setLevel(logging.DEBUG)
        logging.getLogger("mcp").setLevel(logging.DEBUG)
    else:
        fastmcp_configure_logging(level=logging.DEBUG)
    lqcd_logger.debug("Debug mode is enabled.")
else:
    lqcd_logger.setLevel(logging.INFO)
    if is_logging_to_file():
        logging.getLogger("fastmcp").setLevel(logging.INFO)
        logging.getLogger("mcp").setLevel(logging.INFO)
    else:
        fastmcp_configure_logging(level=logging.INFO)
    lqcd_logger.info("Debug mode is disabled.")

# check whether we are integrated inside IRI
_iri_integration = os.getenv("IRI_INTEGRATION", "true").lower() == "true"
if _iri_integration:
    lqcd_logger.info("Our server is integrated with IRI.")
else:
    lqcd_logger.info("Our server is not integrated with IRI.")

# get IRI path from .server_env
if _iri_integration:
    _iri_path = os.getenv("IRI_DIR")
    if not _iri_path:
        lqcd_logger.error("IRI_DIR is not set")
        sys.exit(1)
    lqcd_logger.info(f"IRI directory: {_iri_path}")


lqcd_logger.info("Loading .server_env file... done.")


# check whether fastmcp is version 3.0.0 or higher
def fastmcp_version_3() -> bool:
    """Check whether fastmcp is version 3.0.0 or higher."""
    try:
        fastmcp_version = importlib.metadata.version("fastmcp")
        return version.parse(fastmcp_version) >= version.parse("3.0.0")
    except importlib.metadata.PackageNotFoundError:
        return False


if fastmcp_version_3() == True:
    lqcd_mcp_settings.is_fastmcp_version_3 = True

from lqcd_mcp_main import lqcd_mcp_main, lqcd_mcp_main_app
from lqcd_mcp_main import setup_mcp_servers

# our own session manager
from session_manager import lqcd_session_manager

# Data class to define backend serve url
from common_data import SlurmMcpServer

from contextlib import asynccontextmanager
from slurm_mcp_server import lqcd_mcp_servers, lqcd_slurm_manager


# Scan slurm jobs every 60 seconds
async def scan_slurm_jobs():
    """Background task running every 60 seconds to scan slurm jobs."""
    # Add any setup before the loop if needed
    lqcd_logger.info(
        "Starting periodic background task (every 60 seconds) to scan slurm jobs."
    )
    while True:
        try:
            await asyncio.sleep(60)
            # scan all available slurm jobs to see whether they are still running
            # if not running, remove them from the map
            lqcd_logger.debug("Scanning slurm jobs...")
            await lqcd_slurm_manager.scan_slurm_jobs()
        except asyncio.CancelledError:
            lqcd_logger.info("Slurm job scanning task cancelled during shutdown.")
            break
        except Exception as e:
            lqcd_logger.error(f"Error in slurm job scanning task: {e}")
            await asyncio.sleep(60)


# Eventual task to check idel clients and close them
async def check_idle_clients():
    """Another background task running every 10 seconds."""
    lqcd_logger.info("Starting background task to check idel clients.")
    while True:
        try:
            # Your 10s logic here
            await asyncio.sleep(300)
        except asyncio.CancelledError:
            lqcd_logger.info("Idle client checking task cancelled during shutdown.")
            break
        except Exception as e:
            lqcd_logger.error(f"Error in idle client checking task: {e}")
            await asyncio.sleep(300)


@asynccontextmanager
async def proxy_app_lifespan(app):
    """Custom lifespan to attach our background tasks alongside FastMCP's."""
    # Initialize session manager
    lqcd_session_manager()

    # Read OIDC authentication configuration from file
    read_oidc_auth_info()
    # Load user account mapping from file
    load_user_account_mapping()

    # Setup MCP servers
    await setup_mcp_servers()

    # Log all routes to diagnose routing precedence
    lqcd_logger.info("Registered routes:")
    for r in app.routes:
        lqcd_logger.info(f"Route: {getattr(r, 'path', 'N/A')} ({type(r).__name__})")

    async with lqcd_mcp_main_app.router.lifespan_context(app):
        # Now the event loop is ready; launch periodic background task
        scan_slurm_task = asyncio.create_task(scan_slurm_jobs())
        # check_idle_clients_task = asyncio.create_task(check_idle_clients())
        try:
            yield
        finally:
            scan_slurm_task.cancel()
            # check_idle_clients_task.cancel()

            try:
                await asyncio.gather(
                    scan_slurm_task,
                    # check_idle_clients_task,
                    return_exceptions=True,
                )
            except asyncio.CancelledError:
                pass


# Initialize FastAPI app
proxy_app = FastAPI(
    title="Jefferson Lab LQCD MCP FastAPI Dynamic Proxy Server",
    lifespan=proxy_app_lifespan,
)
security = HTTPBearer()


# Add a backend server to the map (this is called when the mcp server is started)
async def register_cloud_server(
    mcp_name: str, server_url: str, owner: str, job_id: int, job_name: str
):
    server = SlurmMcpServer(
        mcp_name=mcp_name,
        url=server_url,
        owner=owner,
        slurm_job_id=job_id,
        slurm_job_name=job_name,
        slurm_job_state="RUNNING",
        error_message="",
        valid=True,
    )
    await lqcd_mcp_servers.add_slurm_mcp_server(server)
    lqcd_logger.info(f"Added backend server: {server}")


# Remove a backend server from the map
async def delete_cloud_server(mcp_name: str):
    done = await lqcd_mcp_servers.remove_slurm_mcp_server(mcp_name)
    if done:
        lqcd_logger.info(f"Removed backend server: {mcp_name}")
    else:
        lqcd_logger.warning(f"Backend server not found: {mcp_name}")


# Get a backend server from the map
async def get_cloud_server(mcp_name: str) -> SlurmMcpServer | None:
    server = await lqcd_mcp_servers.get_slurm_mcp_server(mcp_name)
    if server:
        return server
    else:
        lqcd_logger.warning(f"Backend server not found: {mcp_name}")
        return None


# Patch the server's listing brain
# We override the instance method to filter tools based on tags
if lqcd_mcp_settings.is_fastmcp_version_3 == False:
    _original_list = lqcd_mcp_main._list_tools


async def filtered_list_tools():
    all_tools = await _original_list()
    # Return only tools that DON'T have the 'hidden' tag
    return [t for t in all_tools if "internal" not in (t.tags or [])]


if lqcd_mcp_settings.is_fastmcp_version_3 == False:
    lqcd_mcp_main._list_tools = filtered_list_tools

# Mount the main mcp server
# Something needs to be decided on the path
proxy_app.mount("/jlab", lqcd_mcp_main_app)


# Add some endpoints to the proxy server
# curl -X 'GET' \
#   'http://127.0.0.1:8000/server' \
#   -H 'accept: application/json'
# Get all backend servers
@proxy_app.get("/server")
async def get_cloud_servers():
    return await lqcd_mcp_servers.get_all_slurm_mcp_servers()


# curl -X 'POST' \
#   'http://127.0.0.1:8000/server?mcp_name=test&server_url=http%3A%2F%2F127.0.0.1%3A8123%2Fmcp&owner=chen' \
#   -H 'accept: application/json' \
#   -d ''
# Add a backend server
@proxy_app.post("/server")
async def add_cloud_server(
    mcp_name: str, server_url: str, owner: str, job_id: str, job_name: str
):
    lqcd_logger.info("Adding cloud server: {}".format(mcp_name))
    await register_cloud_server(mcp_name, server_url, owner, int(job_id), job_name)
    return {"status": "success"}


# curl -X 'DELETE' \
#   'http://127.0.0.1:8000/server/test' \
#   -H 'accept: application/json'
# Delete a backend server
@proxy_app.delete("/server/{mcp_name}")
async def remove_cloud_server(mcp_name: str):
    await delete_cloud_server(mcp_name)
    return {"status": "success"}


# curl -X 'GET' \
#   'http://127.0.0.1:8000/server/test' \
#   -H 'accept: application/json'
# Get a backend server
@proxy_app.get("/server/{mcp_name}")
async def get_cloud_server_url(mcp_name: str):
    server = await get_cloud_server(mcp_name)
    if server:
        return server
    else:
        raise HTTPException(status_code=404, detail="Backend server not found")


# Health check
@proxy_app.get("/")
async def root():
    return {"message": "LQCD MCP FastAPI Dynamic Proxy Server is healthy."}


# include the authentication router
from lqcd_oidc_auth import auth_router

# Read a json file pointed by en env variable
from lqcd_oidc_auth import read_oidc_auth_info, load_user_account_mapping

# include the auth router
proxy_app.include_router(auth_router, prefix="", tags=["auth"])


# Get session id from a request
def get_session_id_from_request(request: Request) -> str | None:
    """Extract session id from the request headers or query parameters."""
    # 1. Try to get ID from Custom Header (Best for internal services)
    # mcp-session-id header is set by FastMCP clients ?
    session_id = request.headers.get("mcp-session-id")

    # 2. If not found, try Authorization Header (Standard for APIs)
    # Format usually: "Bearer session_123"
    if not session_id:
        auth = request.headers.get("authorization")
        if auth and auth.startswith("Bearer "):
            session_id = auth.split(" ")[1]

    # 3. Fallback to Query Params (e.g. DELETE /url?session_id=123)
    if not session_id:
        session_id = request.query_params.get("session_id")

    # 4. Final Fallback: Cookies (for when you do have browsers)
    if not session_id:
        session_id = request.cookies.get("session_id")

    return session_id


# Add custom  middleware
@proxy_app.middleware("http")
async def request_middleware(request: Request, call_next):
    # Redirect exactly /jlab to /jlab/ (using 307 to preserve POST method and body)
    # to prevent the root mount "/" from hijacking the request.
    if request.url.path == "/jlab":
        lqcd_logger.info("Redirecting /jlab to /jlab/ to prevent root mount hijack")
        from fastapi.responses import RedirectResponse

        return RedirectResponse(url="/jlab/", status_code=307)

    start_time = time.time()

    # Log request details
    client_host = request.client.host if request.client else "unknown"
    lqcd_logger.debug(
        f"-> Request: {request.method} {request.url.path} from {client_host}"
    )
    if request.method == "DELETE":
        lqcd_logger.info(
            f"-> DELETE Request: {request.method} {request.url.path} from {client_host}"
        )

        lqcd_logger.debug(f"Headers: {dict(request.headers)}")
        lqcd_logger.debug(f"Query Params: {dict(request.query_params)}")
        lqcd_logger.debug(f"Cookies: {request.cookies}")

        session_id = get_session_id_from_request(request)
        lqcd_logger.info(f"session id from request: {session_id}")

        # Client disconnects from cloud server
        if request.url.path.startswith("/cloud"):
            lqcd_logger.info(f"Client disconnects from backend server")
            if session_id:
                lqcd_logger.info(
                    f"Cleaning up cloud server data for session id: {session_id}"
                )
                await lqcd_session_manager().cleanup(session_id)
        elif request.url.path.startswith("/jlab"):
            lqcd_logger.info(f"Client disconnects from jlab proxy server")
            if session_id and await lqcd_session_manager().session_exists(session_id):
                lqcd_logger.info(
                    f"Cleaning up jlab proxy server data for session id: {session_id}"
                )
                await lqcd_session_manager().cleanup(session_id)

    # Process the request
    has_error = False
    try:
        response = await call_next(request)
    except asyncio.CancelledError:
        lqcd_logger.error("Request cancelled")
        response = Response("Request cancelled", status_code=499)
        has_error = True
    except Exception as e:
        lqcd_logger.error("Request failed: {}".format(e))
        response = Response("Request failed", status_code=500)
        has_error = True

    process_time = time.time() - start_time

    # Log response details
    lqcd_logger.info(
        f"<- Response: {request.method} {request.url.path} returned {response.status_code} in {process_time:.4f}s"
    )

    if has_error:
        session_id = get_session_id_from_request(request)
        if session_id and await lqcd_session_manager().session_exists(session_id):
            lqcd_logger.info(
                f"Cleaning up jlab proxy server data for session id: {session_id}"
            )
            await lqcd_session_manager().cleanup(session_id)

    return response


# generate a error reporting stream used to construct StreamingResponse
async def _proxy_error_stream(error: str, message: str):
    """Generate a stream of error messages."""
    yield json.dumps({"error": error, "message": message})


# Routing to backend servers.
# DYNAMIC ROUTING & INJECTION LOGIC
@proxy_app.api_route("/cloud/{mcp_name}/{path:path}", methods=["GET", "POST", "DELETE"])
async def mcp_proxy_route(mcp_name: str, path: str, request: Request):
    """
    Catch-all route to forward requests to a specific backend
    while injecting user information into the MCP protocol.
    """

    from starlette.background import BackgroundTask
    from fastapi.responses import StreamingResponse

    # Resolve backend endpoint
    lqcd_logger.debug(f"Resolving backend endpoint for {mcp_name}")
    backend_server: SlurmMcpServer | None = await get_cloud_server(mcp_name)
    if not backend_server:
        lqcd_logger.error(f"Backend '{mcp_name}' not found")
        return StreamingResponse(
            _proxy_error_stream("Backend not found", "Backend not found"),
            status_code=404,
            headers={"Content-Type": "application/json"},
        )

    # The full URL for the backend
    backend_url = backend_server.url
    # target_url = f"{backend_url}/{path}" if path else backend_url
    target_url = f"{backend_url}"

    lqcd_logger.debug(f"Forwarding {request.method} request to {mcp_name} {target_url}")

    # Extract context to pass to the backend (e.g., from Proxy Auth)
    # This will be used to inject user information into the MCP protocol  ***
    # Need more discussion on this ***
    user_context = {"connection_id": f"id_{mcp_name}", "authenticated_by": "jlab-proxy"}

    # Handle JSON-RPC Body Injection
    # Make sure to remove host and content-length headers,
    # as the backend server expects the hostname of its own not the proxy's.
    method = request.method
    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)

    forward_content = await request.body()
    if method == "POST":
        try:
            body = await request.json()
            if "params" not in body:
                body["params"] = {}
            body["params"]["_meta"] = {"proxy_provided": user_context}
            forward_content = json.dumps(body).encode("utf-8")
        except json.JSONDecodeError:
            pass

    # keep tracking error and message and status
    error = "Unknown error"
    message = "Unknown error occurred during proxying"
    status = 200

    # Forward the request with Streaming
    client = httpx.AsyncClient()
    try:
        rp_req = client.build_request(
            method=method,
            url=target_url,
            content=forward_content,
            headers=headers,
            params=request.query_params,
            timeout=30.0,
        )
        rp_resp = await client.send(rp_req, stream=True)
    except httpx.ConnectError as e:
        await client.aclose()
        lqcd_logger.error(f"Connection error to backend {mcp_name}: {e}")
        error = "Connection error"
        message = f"Backend server {mcp_name} unavailable"
        status = 503
    except httpx.TimeoutException as e:
        await client.aclose()
        lqcd_logger.error(f"Timeout error to backend {mcp_name}: {e}")
        error = "Timeout error"
        message = f"Backend server {mcp_name} timeout"
        status = 504
    except Exception as e:
        await client.aclose()
        lqcd_logger.error(f"Error forwarding request to backend {mcp_name}: {e}")
        error = "Proxy error"
        message = f"Proxy error: {str(e)}"
        status = 500

    if status != 200:
        return StreamingResponse(
            _proxy_error_stream(error, message),
            status_code=status,
            headers={"Content-Type": "application/json"},
        )

    # generate a response stream used to construct StreamingResponse
    # This is used to check whether the stream from proxy to the backend is broken
    async def _proxy_response_stream(rp_resp: httpx.Response, mcp_name: str):
        try:
            async for chunk in rp_resp.aiter_bytes():
                yield chunk
        except httpx.RemoteProtocolError as e:
            lqcd_logger.error(f"Backend {mcp_name} connection dropped/incomplete: {e}")
            # remove this mcp server from the cloud server list
            await delete_cloud_server(mcp_name)
            lqcd_logger.info(f"Backend {mcp_name} removed from cloud server list")
            # clean up all sessions that are connected to this backend server
            # This happens when the backend server is crashed, so we need to clean up the session
            # state on UI by removing the backend server from the session resource "connected_servers".
            # This will make sure the UI can reflect the correct state of connections.
            await lqcd_session_manager().remove_resource_all_sessions_set(
                "connected_servers", mcp_name
            )
        except Exception as e:
            lqcd_logger.error(f"Backend {mcp_name} streaming error: {e}")
            # We cannot change the status code here as the headers are already sent.
            # But the client will see an incomplete stream.

    return StreamingResponse(
        _proxy_response_stream(rp_resp, mcp_name),
        status_code=rp_resp.status_code,
        headers=dict(rp_resp.headers),
        background=BackgroundTask(client.aclose),
    )


# Mount the IRI FastAPI app at /
if _iri_integration:
    import sys

    if _iri_path not in sys.path:
        sys.path.append(_iri_path)

    import importlib

    app_main = importlib.import_module("app.main")
    iri_app = app_main.APP

    proxy_app.mount("/", iri_app)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MCP Streamable HTTP LQCD Analysis proxy server"
    )
    parser.add_argument(
        "--port", type=int, default=8123, help="Server port to listen on"
    )

    args = parser.parse_args()

    # check log file is set
    if is_logging_to_file():
        lqcd_logger.info("Log file is set, using file logger")
        custom_log_config = None
    else:
        lqcd_logger.info("Log file is not set, using console logger")
        custom_log_config = LOGGING_CONFIG

    # check whether we are running https
    _use_https = os.getenv("USE_HTTPS", "false").lower() == "true"

    # Run
    # need to specify 0.0.0.0 as host to allow connections from anywhere
    if _use_https:
        lqcd_logger.info("Running with HTTPS")

        # redirect everything to HTTPS
        from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware

        proxy_app.add_middleware(HTTPSRedirectMiddleware)

        ssl_keyfile = os.getenv("PROXY_KEY_FILE")
        ssl_certfile = os.getenv("PROXY_CERT_FILE")
        if not ssl_keyfile or not ssl_certfile:
            lqcd_logger.error(
                "PROXY_KEY_FILE and PROXY_CERT_FILE must be set when USE_HTTPS is true"
            )
            sys.exit(1)
        uvicorn.run(
            proxy_app,
            host="0.0.0.0",
            port=args.port,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            log_config=custom_log_config,  # Prevent uvicorn from overwriting our logging config
        )
    else:
        lqcd_logger.info("Running without HTTPS")
        uvicorn.run(
            proxy_app, host="0.0.0.0", port=args.port, log_config=custom_log_config
        )
