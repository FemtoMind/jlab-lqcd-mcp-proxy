# This just LQCD test mcp server to be launched as a sluem job

"""
FastMCP LQCD test server
"""
import asyncio

import argparse
import uvicorn
import os
import random
from fastmcp import FastMCP
from fastmcp import Context
from dataclasses import dataclass

from lqcd_logger import lqcd_logger
from server_util import get_hostname_and_ip_address, register_mcp_slurm_server


@dataclass
class BinaryInput:
    a: int
    b: int


# Create server
test_mcp = FastMCP("LQCD Test Server")


# Greeting users
@test_mcp.prompt()
async def welcome_user(user: str) -> str:
    """Generate a welcome message for the user."""

    return f"Welcome, {user}, to the test server!"


@test_mcp.tool()
async def generate_random_number(ctx: Context) -> int:
    """Return a random integer between 1 and 100"""
    lqcd_logger.info("session id in random tool: {}".format(ctx.session_id))
    return random.randint(1, 100)


@test_mcp.tool()
async def sum(a: int, b: int, ctx: Context) -> int:
    """simple addition"""
    lqcd_logger.info("session id in sum tool: {}".format(ctx.session_id))
    return a + b


# add ecilitation test tool
@test_mcp.tool()
async def interactiveMultiplication(ctx: Context) -> int:
    """Echo the input text"""
    lqcd_logger.info("session id in times tool: {}".format(ctx.session_id))

    input_data = await ctx.elicit(
        message="Please provide two numbers to multiply.",
        response_type=BinaryInput,
    )

    if input_data.action == "accept":
        return input_data.data.a * input_data.data.b
    else:
        lqcd_logger.info("User rejected the input.")
        return -1


# Get startlette app for uvicorn
test_mcp_app = test_mcp.http_app(
    path="/mcp", json_response=False, stateless_http=False, transport="http"
)


# Setup run with proxy server
# mcp name is the slurm job name
async def setup_run(proxy_url: str, port: int) -> str:
    # before start, we need to register the server to the proxy server
    job_id = os.getenv("SLURM_JOB_ID")
    mcp_name = os.getenv("SLURM_JOB_NAME")
    if not job_id is None:
        hostname, ip_address = get_hostname_and_ip_address()
        mcp_url = f"http://{ip_address}:{port}/mcp"
        status = await register_mcp_slurm_server(proxy_url, mcp_name, mcp_url)
        while status is None:
            await asyncio.sleep(5)
            lqcd_logger.info("Retrying to register mcp slurm server")
            status = await register_mcp_slurm_server(proxy_url, mcp_name, mcp_url)
        if status is None:
            lqcd_logger.error("Failed to register mcp slurm server")
            exit(1)
    else:
        lqcd_logger.error("This server should be launched as a slurm job.")
        exit(1)
    return mcp_name


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LQCD MCP Test Server")
    parser.add_argument(
        "--port", type=int, default=8123, help="Localhost port to listen on"
    )
    parser.add_argument(
        "--proxy-url",
        type=str,
        default="http://127.0.0.1:8000",
        help="Proxy server URL",
    )
    args = parser.parse_args()

    mcp_name = asyncio.run(setup_run(args.proxy_url, args.port))
    lqcd_logger.info(
        "LQCD MCP server {} is registered to the proxy server.".format(mcp_name)
    )

    lqcd_logger.info("Server {} started on port {}".format(mcp_name, args.port))
    uvicorn.run(test_mcp_app, host="0.0.0.0", port=args.port)
