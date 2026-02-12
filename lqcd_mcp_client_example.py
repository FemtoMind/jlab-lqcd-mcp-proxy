# This is a sample client for testing connection to a backend mcp server
# through a well known proxy server which is also a mcp server.
# It uses the MCPClientManager to connect to the proxy and backend servers
# and then it could talk to a LLM to drive a workflow.
import asyncio
import argparse
import os
import json
import logging
from urllib import request, response
from dotenv import load_dotenv
from fastmcp import Client
from fastmcp.exceptions import ToolError
from client_util import LQCDMCPClient
from common_data import FullSystemResource

from lqcd_logger import lqcd_logger


# Show computing resources
async def show_computing_resources(proxy_client: Client) -> FullSystemResource:
    """Return static information about computing resources."""

    info = await proxy_client.session.read_resource("resource://system_info")
    lqcd_logger.debug(f"System resource information:\n {info}")

    system_info: FullSystemResource = FullSystemResource.model_validate_json(
        info.contents[0].text
    )
    return system_info


# Load env variables from .client_env file if present
load_dotenv(os.path.join(os.path.dirname(__file__), ".client_env"), override=True)


async def main():
    parser = argparse.ArgumentParser(description="LQCD MCP Client Sample")
    parser.add_argument("--proxy-url", required=True, help="Proxy Server URL")
    parser.add_argument("--mcp-name", required=True, help="Backend MCP name")
    parser.add_argument(
        "--direct-deploy",
        default="true",
        help="Enable direct deployment of a MCP server without talking to a LLM",
    )
    args = parser.parse_args()

    lqcd_logger.info(f"Starting LQCD MCP client sample...")

    # The proxy's MCP endpoint (mounted at /jlab)
    proxy_mcp_url = f"{args.proxy_url}/jlab"

    lqcd_logger.info(f"Proxy MCP URL: {proxy_mcp_url}")

    client_manager = LQCDMCPClient(args.proxy_url)

    if await client_manager.do_debug():
        lqcd_logger.info("Debug mode enabled.")
        lqcd_logger.setLevel(logging.DEBUG)

    await client_manager.connect_to_proxy()
    proxy_client = await client_manager.get_proxy_client()

    lqcd_logger.info(f"Connected to Proxy at: {proxy_mcp_url}")

    # Check if backend mcp server already exists
    server_exists = await client_manager.backend_mcp_server_exists(args.mcp_name)

    if server_exists:
        lqcd_logger.info(f"Backend MCP server {args.mcp_name} already exists.")
    else:
        lqcd_logger.info(f"Backend MCP server {args.mcp_name} does not exist.")
        lqcd_logger.info("Deploying backend MCP server...")

        print("Enter a local file name for job script...")
        user_input = input()
        lqcd_logger.info(f"Using local file name: {user_input}")
        mcp_server = await client_manager.launch_backend_mcp_server(
            args.mcp_name, user_input, True, False
        )

        if mcp_server.slurm_job_id == -1:
            lqcd_logger.error(
                f"Server {mcp_server.mcp_name} launch failed. Error message: {mcp_server.error_message}"
            )
            exit(1)

    # Wait for the backend mcp server to be ready
    lqcd_logger.info(f"Waiting for backend MCP server {args.mcp_name} to be ready...")
    server_status = await client_manager.backend_mcp_server_status(args.mcp_name)
    lqcd_logger.info(f"Backend MCP server {args.mcp_name} status: {server_status}")
    while server_status != "RUNNING":
        await asyncio.sleep(5)
        server_status = await client_manager.backend_mcp_server_status(args.mcp_name)
        lqcd_logger.info(f"Backend MCP server {args.mcp_name} status: {server_status}")

    lqcd_logger.info(f"Backend MCP server {args.mcp_name} is ready.")

    # Connect to the backend mcp server
    backend_client = await client_manager.connect_to_backend_mcp_server(
        args.mcp_name, 60
    )

    lqcd_logger.info("\n--- Connected to BOTH servers! ---")

    # Interact with Proxy
    lqcd_logger.info("\n[Proxy] Listing tools:")
    proxy_tools = await proxy_client.list_tools()
    for t in proxy_tools:
        lqcd_logger.info(f"  - {t.name}")

    # Interact with Backend
    lqcd_logger.info("\n[Backend] Listing tools:")

    backend_tools = await backend_client.list_tools()
    for t in backend_tools:
        lqcd_logger.info(f"  - {t.name}")

    print("Now you can work with your backend mcp server.")
    print("Type 'exit' to exit.")
    while True:
        user_input = input()
        if user_input == "exit":
            break
        async with backend_client as cl:
            tool_name = "generate_random_number"
            tool_args = {}
            try:
                result = await cl.call_tool(tool_name, tool_args, timeout=10)
                lqcd_logger.info(f"Tool result: {result}")
            except ToolError as e:
                lqcd_logger.error(f"Tool error: {e}")
                break
            except RuntimeError as e:
                lqcd_logger.error(f"Runtime error: {e}")
                break
            except Exception as e:
                lqcd_logger.error(f"Unexpected error: {e}")
                break

    lqcd_logger.info("\nExiting and closing all connections...")
    await client_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
