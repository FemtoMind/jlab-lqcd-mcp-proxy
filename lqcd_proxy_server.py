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
from fastapi.security import HTTPBearer
import httpx
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import Response
from fastmcp import FastMCP, Context
from fastmcp.server.middleware import MiddlewareContext
from starlette.types import Message
import uvicorn

# Our own logger
import logging
from lqcd_logger import lqcd_logger

lqcd_logger.setLevel(logging.DEBUG)

from lqcd_mcp_main import lqcd_mcp_main, lqcd_mcp_main_app
from lqcd_mcp_main import setup_mcp_servers

# our own session manager
from session_manager import lqcd_session_manager

# Data class to define backend serve url
from common_data import SlurmMcpServer

# Global backend servers: server id -> backend server
from slurm_mcp_server import lqcd_mcp_servers

# Initialize FastAPI app
proxy_app = FastAPI(
    title="Jefferson Lab LQCD MCP FastAPI Dynamic Proxy Server",
    lifespan=lqcd_mcp_main_app.router.lifespan_context,
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
_original_list = lqcd_mcp_main._list_tools


async def filtered_list_tools(ctx: MiddlewareContext):
    all_tools = await _original_list(ctx)
    # Return only tools that DON'T have the 'hidden' tag
    return [t for t in all_tools if "internal" not in (t.tags or [])]


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
    return lqcd_mcp_servers.get_all_slurm_mcp_servers()


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
    start_time = time.time()
    # Log request details
    """
    lqcd_logger.info(
        f"-> Request: {request.method} {request.url.path} from {request.client.host}"
    )

    json_body = await request.json()
    print("request json = {}".format(json_body))
    """
    # Need to check the above three things to find out whether there is session id
    if request.method == "DELETE":
        # Log request details
        lqcd_logger.info(
            f"-> Request: {request.method} {request.url.path} from {request.client.host}"
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
                await lqcd_session_manager.cleanup(session_id)
        elif request.url.path.startswith("/jlab"):
            lqcd_logger.info(f"Client disconnects from jlab proxy server")
            if session_id and await lqcd_session_manager.session_exists(session_id):
                lqcd_logger.info(
                    f"Cleaning up jlab proxy server data for session id: {session_id}"
                )
                await lqcd_session_manager.cleanup(session_id)

    # Process the request
    response = await call_next(request)
    process_time = time.time() - start_time

    # Log response details
    lqcd_logger.info(
        f"<- Response: {request.method} {request.url.path} returned {response.status_code} in {process_time:.4f}s"
    )

    return response


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
    backend_server: SlurmMcpServer = await get_cloud_server(mcp_name)
    if not backend_server:
        raise HTTPException(status_code=404, detail=f"Backend '{mcp_name}' not found")

    # The full URL for the backend
    backend_url = backend_server.url
    # target_url = f"{backend_url}/{path}" if path else backend_url
    target_url = f"{backend_url}"

    lqcd_logger.info(f"Forwarding {request.method} request to {mcp_name} {target_url}")

    # Extract context to pass to the backend (e.g., from Proxy Auth)
    # This will be used to inject user information into the MCP protocol  ***
    # Need more discussion on this ***
    user_context = {"user_id": f"id_{mcp_name}", "authenticated_by": "FastAPI-Proxy"}

    # Handle JSON-RPC Body Injection
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
    except Exception as e:
        await client.aclose()
        raise e

    return StreamingResponse(
        rp_resp.aiter_bytes(),
        status_code=rp_resp.status_code,
        headers=dict(rp_resp.headers),
        background=BackgroundTask(client.aclose),
    )


if __name__ == "__main__":
    # read OIDC authentication configuration from file
    read_oidc_auth_info()
    # load user account mapping from file
    load_user_account_mapping()

    parser = argparse.ArgumentParser(
        description="MCP Streamable HTTP LQCD Analysis server"
    )
    parser.add_argument(
        "--port", type=int, default=8123, help="Server port to listen on"
    )
    args = parser.parse_args()
    # You run this server using uvicorn, which runs the FastAPI app
    # fastmcp run server.py automatically handles this if you use its CLI
    # but for local testing you can use uvicorn directly on the 'app' object
    asyncio.run(setup_mcp_servers())

    # Run
    # need to specify 0.0.0.0 as host to allow connections from anywhere
    uvicorn.run(proxy_app, host="0.0.0.0", port=args.port)
