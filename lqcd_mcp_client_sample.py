# This is a sample client for testing connection to a backend mcp server
# through a well known proxy server which is also a mcp server.
# It uses the MCPClientManager to connect to the proxy and backend servers
# and then it could talk to a LLM to drive a workflow.
import asyncio
import argparse
import json
import logging
import os
from typing import Any
from dotenv import load_dotenv
from fastmcp import Client
from fastmcp.client.elicitation import ElicitResult
from pydantic import AnyUrl
from mcp.types import TextContent, TextResourceContents

from client_util import LQCDMCPClient
from common_data import FullSystemResource
from common_data import SlurmMcpServer
from common_data import SlurmMcpJob
from common_data import SlurmMcpScriptFile

from lqcd_logger import lqcd_logger


BackendMCPClient: Client | None = None


# Test all the tools available on the server
async def call_backend_tools(backend_client: Client) -> str:
    """Test all the tools available on the server."""

    tool_response = await backend_client.list_tools()

    lqcd_logger.debug("response from list_tools: {}".format(tool_response))

    final_text = []

    for tool in tool_response:
        tool_name = tool.name
        tool_args = {}

        if tool.inputSchema:
            # If the tool has an input schema, we can pass empty arguments
            tool_inputs = {k: "" for k in tool.inputSchema.get("properties", {}).keys()}
            for types in tool.inputSchema.get("properties", {}).values():
                if types.get("type") == "integer":
                    tool_inputs = {
                        k: 124 for k in tool.inputSchema.get("properties", {}).keys()
                    }
                elif types.get("type") == "string":
                    tool_inputs = {
                        k: "Hello again and again"
                        for k in tool.inputSchema.get("properties", {}).keys()
                    }

                tool_args = tool_inputs

        lqcd_logger.debug(f"Using empty arguments for tool {tool_name}: {tool_args}")

        # For demonstration, we use empty arguments
        result = await backend_client.call_tool(tool_name, tool_args)

        lqcd_logger.debug(f"Tool {tool_name} returned: {result}")

        first_content = result.content[0] if result.content else None
        if isinstance(first_content, TextContent):
            final_text.append(f"Tool {tool_name} output: {first_content.text}")

    return "\n".join(final_text)


# handle elicitation
async def elicitation_handle_method(
    message: str, response_type: type, params, context
) -> Any:
    print(message)

    if response_type.__name__ == "SlurmMcpScriptFile":  # This is a local script file
        print("Enter a file containing a submission scirpt.")
        filename = input("Filename: ")
        if filename == "":
            print("Looks like you are not ready to submit a job.")
            return ElicitResult(action="cancel")
        else:
            print("Is this a local file or remote file? (local/remote)")
            answer = input()
            if answer == "local":
                if os.path.exists(filename):
                    # read the file and return the content
                    with open(filename, "r") as f:
                        content = f.read()
                    return SlurmMcpScriptFile(
                        filename=filename, local=True, content=content
                    )
                else:
                    print("File not found: {}".format(filename))
                    return ElicitResult(action="cancel")
            else:
                return SlurmMcpScriptFile(filename=filename, local=False)

    elif response_type.__name__ == "SlurmMcpJob":  # This is a Slurm job
        account = input("Slurm account: ")
        partition = input("Slurm partition: ")
        run_script = input("Run script: ")
        num_nodes = input("Number of nodes: ")
        num_tasks = input("Number of tasks: ")
        num_cpus_per_task = input("Number of CPUs per task: ")
        time = input("Wall clock Time in format DD-HH:MM:SS: ")
        output_dir = input("Output directory: ")

        job: SlurmMcpJob = SlurmMcpJob(
            account=account,
            partition=partition,
            run_script=run_script,
            nodes=int(num_nodes),
            ntasks=int(num_tasks),
            cpus_per_task=int(num_cpus_per_task),
            time=time,
            output=output_dir,
        )
        return job
    else:
        lqcd_logger.error("Unknown response type: {}".format(response_type.__name__))
        return ElicitResult(action="cancel")


# Show computing resources
async def show_computing_resources(proxy_client: Client) -> FullSystemResource:
    """Return static information about computing resources."""

    info = await proxy_client.session.read_resource(AnyUrl("resource://system_info"))
    lqcd_logger.debug(f"System resource information:\n {info}")

    resource = info.contents[0]
    if isinstance(resource, TextResourceContents):
        system_info: FullSystemResource = FullSystemResource.model_validate_json(
            resource.text
        )
    else:
        raise ValueError(f"Expected TextResourceContents, got {type(resource)}")
    return system_info


# Call all proxy server tools for testing
async def call_all_proxy_tools(client_manager: LQCDMCPClient) -> str:
    """call all proxy server tools for testing."""
    proxy_client = await client_manager.get_proxy_client()

    if proxy_client is None:
        raise Exception("Proxy client is None")
    tool_response = await proxy_client.list_tools()

    lqcd_logger.debug("response from list_tools: {}".format(tool_response))

    final_text = []

    for tool in tool_response:
        tool_name = tool.name
        tool_args = {}

        if tool.inputSchema:
            # If the tool has an input schema, we can pass empty arguments
            tool_inputs = {k: "" for k in tool.inputSchema.get("properties", {}).keys()}
            for types in tool.inputSchema.get("properties", {}).values():
                if types.get("type") == "integer":
                    tool_inputs = {
                        k: 124 for k in tool.inputSchema.get("properties", {}).keys()
                    }
                elif types.get("type") == "string":
                    tool_inputs = {
                        k: "Hello again and again"
                        for k in tool.inputSchema.get("properties", {}).keys()
                    }

                tool_args = tool_inputs

        lqcd_logger.debug(f"Using empty arguments for tool {tool_name}: {tool_args}")

        # For demonstration, we use empty arguments
        result = await proxy_client.call_tool(tool_name, tool_args)

        lqcd_logger.debug(f"Tool {tool_name} returned: {result.content}")

        first_content = result.content[0] if result.content else None
        if isinstance(first_content, TextContent):
            final_text.append(f"Tool {tool_name} output: {first_content.text}")

    return "\n".join(final_text)


# Test a job submisstion without LLM
async def submit_job_test(
    client_manager: LQCDMCPClient, mcp_name: str, filename: str
) -> str:
    """Submit a job to the server without LLM."""
    proxy_client = await client_manager.get_proxy_client()

    mcp_server: SlurmMcpServer = await client_manager.launch_backend_mcp_server(
        mcp_name, filename, True, True
    )

    lqcd_logger.debug(f"MCP server launched: {mcp_server}")

    return mcp_server.to_json()


# Talk to an LLM and the proxy
async def get_tool_calls_from_llm(client_manager: LQCDMCPClient, request: str):
    """Get tools from proxy and talk to an LLM to decide what to do next."""
    messages: list[Any] = [{"role": "user", "content": request}]
    global BackendMCPClient

    proxy_client = await client_manager.get_proxy_client()
    if proxy_client is None:
        raise Exception("Proxy client is None")
    # Get tool response from the server
    tool_response = await proxy_client.list_tools()
    lqcd_logger.debug("response from list_tools: {}".format(tool_response))

    backend_tool_names = []
    if BackendMCPClient is not None:
        backend_tool_response = await BackendMCPClient.list_tools()
        backend_tool_names = [tool.name for tool in backend_tool_response]
        lqcd_logger.debug(
            "response from backend list_tools: {}".format(backend_tool_response)
        )

    # Build the tool call request
    available_tools: list[Any] = [
        {
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": {
                    "type": "object",
                    "properties": tool.inputSchema.get("properties", {}),
                    "required": tool.inputSchema.get("required", []),
                },
            },
        }
        for tool in tool_response
    ]
    if BackendMCPClient is not None:
        available_tools.extend(
            [
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": {
                            "type": "object",
                            "properties": tool.inputSchema.get("properties", {}),
                            "required": tool.inputSchema.get("required", []),
                        },
                    },
                }
                for tool in backend_tool_response
            ]
        )
    lqcd_logger.debug("Available tools for LLM: {}".format(available_tools))

    # Get LLM model which is inside the client manager
    llm_model = await client_manager.get_llm_model()
    # Get openai client
    openai_client = await client_manager.get_openai_client()

    response = openai_client.chat.completions.create(
        extra_headers={
            "HTTP-Referer": "www.jlab.org",  # Optional. Site URL for rankings on openrouter.ai.
            "X-Title": "Jefferson Lab",  # Optional. Site title for rankings on openrouter.ai.
        },
        extra_body={},
        model=llm_model,
        max_tokens=4096,
        messages=messages,
        tools=available_tools,
        tool_choice="required",  # Let the model decide whether to call a tool
    )

    return response, messages, backend_tool_names


# Handll proxy tool calls according to LLM response
async def handle_tool_calls(
    client_manager: LQCDMCPClient,
    llm_response,
    messages: list[Any],
    backend_tool_names: list[str] |None = None,
) -> str:
    """Handle tool calls according to LLM response."""
    proxy_client = await client_manager.get_proxy_client()

    if proxy_client is None:
        raise Exception("Proxy client is None")

    llm_model = await client_manager.get_llm_model()
    openai_client = await client_manager.get_openai_client()

    response_message = llm_response.choices[0].message
    lqcd_logger.debug("LLM response: {}".format(response_message))

    # whether there is any content from the response message
    content = response_message.content
    final_text = []

    if response_message.tool_calls:
        for tool_call in response_message.tool_calls:
            tool_name = tool_call.function.name
            tool_args = tool_call.function.arguments

            lqcd_logger.debug(f"Tool call from LLM: {tool_name} with args {tool_args}")
            # Execute tool call
            real_args = json.loads(tool_args)

            global BackendMCPClient
            backend_tool_names = backend_tool_names or []

            if tool_name in backend_tool_names and BackendMCPClient is not None:
                lqcd_logger.info(
                    f"Routing tool call '{tool_name}' to BACKEND MCP SERVER"
                )
                result = await BackendMCPClient.call_tool(tool_name, real_args)
            else:
                lqcd_logger.info(f"Routing tool call '{tool_name}' to PROXY SERVER")
                result = await proxy_client.call_tool(tool_name, real_args)

            lqcd_logger.debug(f"Tool {tool_name} returned: {result}")

            if content:
                final_text.append(f"[Calling tool {tool_name} with args {tool_args}]")

            # Continue conversation with tool results
            messages.append({"role": "user", "content": result.content})

            # Get next response from OpenAI
            response = openai_client.chat.completions.create(
                extra_headers={
                    "HTTP-Referer": "www.jlab.org",  # Optional. Site URL for rankings on openrouter.ai.
                    "X-Title": "Jefferson Lab",  # Optional. Site title for rankings on openrouter.ai.
                },
                extra_body={},
                model=llm_model,
                max_tokens=4096,
                messages=messages,
            )
            final_text.append(response.choices[0].message.content)
    elif content:
        final_text.append(content)

    return "\n".join(final_text)


# Talk to an LLM and the proxy to launch and connect to a backend MCP server
async def proxy_llm_loop(client_manager: LQCDMCPClient, mcp_name: str):
    """Talk to an LLM and the proxy to launch and connect to a backend MCP server."""
    global BackendMCPClient
    while True:
        print("Enter your request to the proxy LLM (type 'exit' to quit):")
        user_input = input()
        if user_input.strip().lower() == "exit":
            print("I am done. Goodbye!")
            break
        elif user_input.strip().lower() == "submit_job":
            lqcd_logger.info(
                "Submitting a job test, Enter a local filename as job script..."
            )
            filename = input()
            final_response_text = await submit_job_test(
                client_manager, mcp_name, filename
            )
            lqcd_logger.debug(
                "Final response from proxy:\n{}".format(final_response_text)
            )
        elif user_input.strip().lower() == "backend":
            lqcd_logger.info("Including backend tools in the conversation...")
            # check whether mcp name is available
            if BackendMCPClient is None:
                lqcd_logger.info(
                    "Not connected to backend MCP server. Check whether {} is running.".format(
                        mcp_name
                    )
                )
                status = await client_manager.backend_mcp_server_status_by_name(
                    mcp_name
                )
                lqcd_logger.info(
                    "Backend MCP server {} status: {}".format(mcp_name, status)
                )
                if status == "RUNNING":
                    lqcd_logger.info(
                        "Backend MCP server {} is running, connecting to it...".format(
                            mcp_name
                        )
                    )
                    BackendMCPClient = (
                        await client_manager.connect_to_backend_mcp_server(mcp_name)
                    )

                    if BackendMCPClient is None:
                        raise Exception("Backend MCP client is None")
                    
                    tool_response = await BackendMCPClient.list_tools()
                    lqcd_logger.debug(
                        "response from backend list_tools: {}".format(tool_response)
                    )
                else:
                    lqcd_logger.info(
                        "Backend MCP server {} is not running, you ned to ask proxy to launch it.".format(
                            mcp_name
                        )
                    )
        else:
            llm_response, messages, backend_tool_names = await get_tool_calls_from_llm(
                client_manager, user_input
            )

            lqcd_logger.debug("LLM response: {}".format(llm_response))

            # Process the response and handle tool calls
            final_response_text = await handle_tool_calls(
                client_manager, llm_response, messages, backend_tool_names
            )
            lqcd_logger.debug(
                "Final response from LLM and tools:\n{}".format(final_response_text)
            )


# Talk to proxy server first
async def interact_with_proxy(client_manager: LQCDMCPClient, mcp_name: str):
    """
    Talk to proxy server and get some information and
    launch and connect to a backend mcp server.
    """
    proxy_client = await client_manager.get_proxy_client()
    if proxy_client is None:
        raise Exception("Proxy client is None")

    # Talk to proxy server and get welcome message
    welcome_message = await proxy_client.get_prompt("welcome_user")

    # returned message is a jsonrpc response
    welcome_content = welcome_message.messages[0].content
    if isinstance(welcome_content, TextContent):
        print(welcome_content.text)

    print("")
    resources = await show_computing_resources(proxy_client)

    print("Jefferson Lab LQCD System Computing Resources:")
    print(resources)

    print("")
    print("")
    print("How can I help you to deploy your MCP server on Jlab LQCD system?")

    # talk to the proxy server and an LLM to decide what to do next
    await proxy_llm_loop(client_manager, mcp_name)


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
    else:
        lqcd_logger.info("Debug mode disabled.")
        lqcd_logger.setLevel(logging.INFO)

    await client_manager.connect_to_proxy(elicitation_handle_method)
    proxy_client = await client_manager.get_proxy_client()

    lqcd_logger.info(f"Connected to Proxy at: {proxy_mcp_url}")

    # Talk to the proxy server and decide what to do next
    await interact_with_proxy(client_manager, args.mcp_name)
    await client_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
