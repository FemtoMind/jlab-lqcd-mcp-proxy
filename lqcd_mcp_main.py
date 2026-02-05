# This is the top level mcp server for lqcd ananlysis tasks.
# This server is just a holder to import and mount other other mcp servers
# developed by other physicits in the collaboration.
import argparse
import asyncio
import uvicorn
from fastmcp import FastMCP
from fastmcp.server.context import Context as ServerContext
from lqcd_logger import lqcd_logger

# Import our servers here
from slurm_mcp_server import slurm_mcp

# Ceate the main FastMCP app
lqcd_mcp_main = FastMCP(name="lqcd_mcp_main")

# Get starletee apps from this fastmcp server
lqcd_mcp_main_app = lqcd_mcp_main.http_app(
    path="/", json_response=False, stateless_http=False, transport="http"
)


# Import other mcp servers and mount them
async def setup_mcp_servers():
    await lqcd_mcp_main.import_server(slurm_mcp)


# Main entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LQCD MCP Main Server")
    parser.add_argument(
        "--port", type=int, default=8123, help="Localhost port to listen on"
    )
    args = parser.parse_args()

    # setup servers
    asyncio.run(setup_mcp_servers())
    # Start the server with Streamable HTTP transport
    uvicorn.run(lqcd_mcp_main_app, host="0.0.0.0", port=args.port)
