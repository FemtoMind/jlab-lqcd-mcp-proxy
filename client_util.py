# Some common utilities for LQCD MCP client/server
import socket
import os
import pwd
import httpx
import asyncio
import json
from datetime import timedelta
from contextlib import AsyncExitStack
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from openai import OpenAI

from common_data import SlurmMcpServer
from common_data import FullSystemResource
from common_data import UserComputingAccounts
from lqcd_logger import lqcd_logger


# Get process owner
def _client_process_owner() -> str | None:
    try:
        # Get the real user ID of the current process
        uid = os.getuid()
        # Look up the username associated with that UID
        user_info = pwd.getpwuid(uid)
        return user_info.pw_name
    except Exception as e:
        print(f"Error getting process owner: {e}")
        return None


# Create a custom httpx.AsyncClient factory with verify=False
# Create a custom httpx.AsyncClient factory with verify=False
def _insecure_httpx_client_factory(**kwargs) -> httpx.AsyncClient:
    kwargs.setdefault("follow_redirects", True)
    if not kwargs.get("timeout"):
        kwargs["timeout"] = httpx.Timeout(30.0, read=300.0)
    return httpx.AsyncClient(verify=False, **kwargs)


# LQCD MCP client class
# This class manages both proxy and backend clients
# It also handles the connection to the proxy and backend servers and closing them
# Note: mcp_name is also the backend slurm job name
class LQCDMCPClient:
    # inner class for backend client
    class BackendClient:
        def __init__(self, https_connect: bool, server_self_signed_cert: bool):
            self.client_handle: Client | None = None
            self.exit_stack = AsyncExitStack()
            self._https_connect = https_connect
            self._server_self_signed_cert = server_self_signed_cert

        async def close(self):
            try:
                await self.exit_stack.aclose()
            except Exception as e:
                lqcd_logger.error(f"Error closing backend client: {e}")
            finally:
                self.client_handle = None

        async def connect(self, backend_url: str, elicitation_handler=None):
            do_verify: bool = True
            if self._https_connect and self._server_self_signed_cert:
                do_verify = False

            if not do_verify:
                transport = StreamableHttpTransport(
                    backend_url, httpx_client_factory=_insecure_httpx_client_factory
                )
            else:
                transport = StreamableHttpTransport(backend_url)

            self.client_handle = await self.exit_stack.enter_async_context(
                Client(
                    transport,
                    elicitation_handler=elicitation_handler,
                )
            )

        @property
        def client(self) -> Client:
            return self.client_handle

    def __init__(self, proxy_url: str):
        self.proxy_url = proxy_url
        self.proxy_mcp_url = proxy_url + "/jlab"
        self.proxy_client: Client = None
        self.backend_clients: dict[str, LQCDMCPClient.BackendClient] = {}
        self.slurm_mcp_servers: dict[str, SlurmMcpServer] = {}
        self.exit_stack = AsyncExitStack()
        self._lock: asyncio.Lock = asyncio.Lock()
        self._server_self_signed_cert = False
        self._trust_client = False
        self._https_connect = self.proxy_url.startswith("https://")

        # set up run environment
        self.jlab_run = False
        # Check whether we running inside jlab
        run_env = os.getenv("MCP_RUN_ENV", "global").lower()
        if run_env == "jlab":
            self.jlab_run = True

        self.debug = os.getenv("DEBUG", "false").lower() == "true"

        # check whether server use self-signed cert
        self._server_self_signed_cert = (
            os.getenv("SERVER_SELF_SIGNED_CERT", "false").lower() == "true"
        )
        lqcd_logger.info(
            "Server is using self-signed cert: {}".format(self._server_self_signed_cert)
        )

        # If my server is using self-signed cert, I need to set verify=False
        # Otherwise, I need to set verify=True
        if self._https_connect and self._server_self_signed_cert:
            http_client = httpx.Client(verify=False)
        else:
            http_client = httpx.Client()

        if self.jlab_run == False:
            lqcd_logger.info("MCP Application is running in an open environment.")
            self.openai = OpenAI(
                base_url="https://openrouter.ai/api/v1",
                api_key=os.getenv("OPENROUTER_API_KEY"),
                http_client=http_client,
            )
        else:
            lqcd_logger.info("MCP Application is running inside Jlab.")
            self.openai = OpenAI(
                base_url="https://llm-vm1.jlab.org/api/v1/",
                api_key=os.getenv("JLAB_LLM_KEY"),
                http_client=http_client,
            )

        # Set LLM model
        self.llm_model = os.getenv("LLM_MODEL")
        if self.jlab_run:
            self.llm_model = os.getenv("JLAB_LLM_MODEL")

        # Check if no authentication is needed
        self._trust_client = os.getenv("TRUST_CLIENT", "false").lower() == "true"

    # Authenticate to OIDC provider
    async def __login(self) -> str | None:
        do_verify: bool = True
        if self._https_connect and self._server_self_signed_cert:
            do_verify = False

        async with httpx.AsyncClient(verify=do_verify) as client:
            # 1. Ask Proxy for the login link and code
            res = (await client.post(f"{self.proxy_url}/auth/device-code")).json()

            if res is None:
                return None

            print(f"\n1. Open: {res['verification_uri']}")
            print(f"2. Enter Code: {res['user_code']}")

            # 2. Poll Proxy for the result
            print("\nWaiting for you to authorize in your browser...")
            while True:
                poll = (
                    await client.post(
                        f"{self.proxy_url}/auth/poll",
                        params={"device_code": res["device_code"]},
                    )
                ).json()

                if "access_token" in poll:
                    print("✅ Login Successful!")
                    return poll["access_token"]

                # GitHub sends 'authorization_pending' while user is busy
                if poll.get("error") not in ["authorization_pending", "slow_down"]:
                    raise Exception(f"Auth failed: {poll}")

                await asyncio.sleep(res.get("interval", 5))

    # Show computing resources
    async def show_computing_resources(self) -> FullSystemResource:
        """Return static information about computing resources."""

        info = await self.proxy_client.session.read_resource("resource://system_info")

        system_info: FullSystemResource = FullSystemResource.model_validate_json(
            info.contents[0].text
        )

        return system_info

    # Show user and computing resources
    async def show_user_accounts(self) -> UserComputingAccounts:
        """Return information about user projects and computing resources."""
        tool_name = "get_user_slurm_accounts"
        tool_args = {}

        result = await self.proxy_client.call_tool(tool_name, tool_args)

        # Explicitly validate/convert to ensure we have an instance of our local class
        # fastmcp might return a dynamic model or dict
        user_info: UserComputingAccounts = result.data

        return user_info

    # connect to proxy server
    async def connect_to_proxy(self, elicitation_handler=None):
        """Connect to proxy server and authenticate the client."""
        # First, authenticate to get access token
        access_token = None
        if self._trust_client == False:
            access_token = await self.__login()

            # check token
            if access_token is None:
                lqcd_logger.error("Failed to obtain access token from OIDC provider.")
                raise Exception("Failed to authenticate to OIDC provider.")

        if self._trust_client:
            client_args = {
                "url": self.proxy_mcp_url,
            }
        else:
            client_args = {
                "url": self.proxy_mcp_url,
                "headers": {"Authorization": f"Bearer {access_token}"},
            }

        if self._https_connect and self._server_self_signed_cert:
            client_args["httpx_client_factory"] = _insecure_httpx_client_factory

        lqcd_logger.info(f"Connecting to Proxy at: {self.proxy_mcp_url}")

        try:
            self.proxy_client = await self.exit_stack.enter_async_context(
                Client(
                    StreamableHttpTransport(**client_args),
                    elicitation_handler=elicitation_handler,
                )
            )
        except Exception as e:
            lqcd_logger.error(f"Error connecting to proxy server: {self.proxy_mcp_url}")
            raise e

        # call proxy server to verify connection and get local account info
        # if we just do test without authentication (access_token is None), pass username
        if not access_token is None:
            tool_args = {"username": ""}
        else:
            tool_args = {"username": _client_process_owner()}

        result = await self.proxy_client.call_tool("validate_user", tool_args)

        lqcd_logger.info(f"User access token: {access_token}")

        lqcd_logger.debug(f"Connected to Proxy. User Info: {result}")

        user_info_json = result.content[0].text
        user_info = json.loads(user_info_json)
        user_account = user_info.get("user_account", "unknown")
        user_id = user_info.get("user_id", "unknown")
        lqcd_logger.info(f"✅ Connected to Proxy as user id: {user_id}")
        lqcd_logger.info(f"✅ Connected to Proxy as local user: {user_account}")
        if user_account == "unknown":
            lqcd_logger.error(
                "User account mapping not found. Please ensure your identity is mapped to a local account."
            )
            await self.proxy_client.close()
            raise Exception("User account mapping not found.")
        else:
            lqcd_logger.info(
                f"✅ Connected to Proxy {self.proxy_mcp_url} as user: {user_account}"
            )

        # Get welcome message from proxy server
        # Talk to proxy server and get welcome message
        welcome_message = await self.proxy_client.get_prompt("welcome_user")

        # returned message is a jsonrpc response
        print(welcome_message.messages[0].content.text)

        print("")
        resources: FullSystemResource = await self.show_computing_resources()
        cinfo: UserComputingAccounts = await self.show_user_accounts()

        if cinfo.valid:
            print(
                "User {} has slurm accounts: {}".format(
                    cinfo.user_name, cinfo.slurm_accounts
                )
            )
        else:
            print(
                "User {} has no slurm accounts and error message: {}".format(
                    cinfo.user_name, cinfo.error_message
                )
            )

        print("")
        print("Jefferson Lab Current Computing Resources:")
        print(resources)

        print("")

    # Check whether a backend mcp server already exists
    async def backend_mcp_server_exists(self, mcp_name: str) -> bool:
        async with self._lock:
            if mcp_name in self.slurm_mcp_servers:
                return True

        # talk to proxy server to check if the backend mcp server exists
        if self.proxy_client is None:
            lqcd_logger.error("Proxy client is not initialized.")
            return False

        tool_name = "get_backend_url"
        tool_args = {"mcp_name": mcp_name}
        result = await self.proxy_client.call_tool(tool_name, tool_args)
        lqcd_logger.debug(f"Backend mcp server info: {result}")
        mcp_server: SlurmMcpServer = result.data
        lqcd_logger.info(f"Backend mcp server info: {mcp_server}")

        if mcp_server.slurm_job_id != -1:
            async with self._lock:
                self.slurm_mcp_servers[mcp_name] = mcp_server
            return True
        else:
            return False

    # Check whether there are any free computing nodes so that one can start
    # a new backend mcp server
    async def computing_nodes_available(self) -> int:
        if self.proxy_client is None:
            lqcd_logger.error("Proxy client is not initialized.")
            return 0

        tool_name = "number_of_idle_nodes"
        tool_args = {}
        result = await self.proxy_client.call_tool(tool_name, tool_args)
        lqcd_logger.debug(f"Available computing nodes: {result}")
        idle_nodes = result.data

        return idle_nodes

    # Check the status of a backend mcp server
    async def backend_mcp_server_status(self, mcp_name: str) -> str:
        lqcd_logger.debug(f"Checking status of backend mcp server {mcp_name}...")
        async with self._lock:
            if mcp_name not in self.slurm_mcp_servers:
                return "N/A"

        # Check the status of the backend mcp server by contacting the proxy server
        if self.proxy_client is None:
            lqcd_logger.error("Proxy client is not initialized.")
            return "N/A"

        tool_name = "check_mcp_server_status"
        job_id = self.slurm_mcp_servers[mcp_name].slurm_job_id
        tool_args = {"job_id": job_id}
        result = await self.proxy_client.call_tool(tool_name, tool_args)
        lqcd_logger.debug(f"Server status info: {result}")
        mcp_server: SlurmMcpServer = result.data

        lqcd_logger.debug(f"Backend MCP server {mcp_name} info: {mcp_server}")

        async with self._lock:
            # We may need to update the cached mcp server info
            self.slurm_mcp_servers[mcp_name] = mcp_server

        return mcp_server.slurm_job_state

    # Check the status of mcp server by name
    async def backend_mcp_server_status_by_name(self, mcp_name: str) -> str:
        lqcd_logger.debug(f"Checking status of backend mcp server {mcp_name}...")

        # Check the status of the backend mcp server by contacting the proxy server
        if self.proxy_client is None:
            lqcd_logger.error("Proxy client is not initialized.")
            return "N/A"

        tool_name = "check_mcp_server_status_by_name"
        tool_args = {"mcp_name": mcp_name}
        result = await self.proxy_client.call_tool(tool_name, tool_args)
        lqcd_logger.debug(f"Server status info: {result}")
        mcp_server: SlurmMcpServer = result.data

        lqcd_logger.debug(f"Backend MCP server {mcp_name} info: {mcp_server}")

        async with self._lock:
            # We may need to update the cached mcp server info
            self.slurm_mcp_servers[mcp_name] = mcp_server

        return mcp_server.slurm_job_state

    # Launch mcp backend server in serveral flavors
    async def launch_backend_mcp_server(
        self, mcp_name: str, filename: str, local_file: bool = True, wait: bool = True
    ) -> SlurmMcpServer:
        """Launch an mcp server on the slurm cluster using a specified file.
        This file can be a local file or a file on the machine hosting the proxy server.
        One can wait for the server to be ready or not.
        Note: mcp_name should be unique for each server and it is the same as the job name.
        """
        """args:
            mcp_name: The name of the mcp server. It is the same as the job name.
            filename: The name of the file to use for the mcp server.
            local_file: Whether the file is a local file or a file on the machine 
                        hosting the proxy server.
            wait: Whether to wait for the server to be ready or not.
        """
        # empty backend mcp server
        backend_mcp_server = SlurmMcpServer(
            mcp_name="N/A",
            url="",
            owner="",
            slurm_job_id=-1,
            slurm_job_name="N/A",
            slurm_job_state="UNKNOWN",
            error_message="",
            valid=False,
        )
        if self.proxy_client is None:
            lqcd_logger.error("Proxy client is not initialized.")
            return backend_mcp_server

        # check whether the server is already running
        if await self.backend_mcp_server_exists(mcp_name):
            lqcd_logger.info(f"Server {mcp_name} is already submitted.")
            return self.slurm_mcp_servers[mcp_name]

        if local_file == True:
            tool_name = "launch_mcp_server_using_local_file"
            tool_args = {
                "mcp_name": mcp_name,
                "wait": wait,
                "local_script_file": filename,
            }
        else:
            tool_name = "launch_mcp_server_using_remote_file"
            tool_args = {"mcp_name": mcp_name, "wait": wait, "script_file": filename}

        result = await self.proxy_client.call_tool(tool_name, tool_args)
        mcp_server: SlurmMcpServer = result.data
        lqcd_logger.debug(f"Server launch result: {mcp_server}")

        if mcp_server.valid:
            lqcd_logger.info(f"Server {mcp_server.mcp_name} launched successfully.")
        elif mcp_server.slurm_job_id == -1:
            lqcd_logger.error(
                f"Server {mcp_server.mcp_name} launch failed. Error message: {mcp_server.error_message}"
            )
            return bmcp_server

        # add the server to the cache
        async with self._lock:
            lqcd_logger.debug(f"Adding server {mcp_server.mcp_name} to cache.")
            self.slurm_mcp_servers[mcp_server.mcp_name] = mcp_server

        return mcp_server

    # Connect to a backend server
    async def connect_to_backend_mcp_server(
        self, mcp_name: str, timeout: int = 60, elicitation_handler=None
    ) -> Client | None:
        """Connect to a backend server.
        args:
            mcp_name: The name of the mcp server.
            timeout: The timeout in seconds to wait for the server to be ready.
            elicitation_handler: The elicitation handler to use for the connection.
        """
        if self.proxy_client is None:
            lqcd_logger.error("Proxy client is not initialized.")
            return None

        # check if the server is already cached
        async with self._lock:
            if mcp_name not in self.slurm_mcp_servers:
                lqcd_logger.error(f"Server {mcp_name} is not launched yet.")
                return None

        # Check whether we already connected to the backend server
        async with self._lock:
            if mcp_name in self.backend_clients:
                lqcd_logger.info(f"Server {mcp_name} is already connected.")
                return self.backend_clients[mcp_name]

        # Wait for the server to be ready
        server_status = await self.backend_mcp_server_status(mcp_name)
        if server_status == "NOT_REGISTERED":
            lqcd_logger.error(
                f"Server {mcp_name} is not registered. Please check it manually."
            )
            return None

        while server_status != "RUNNING" and timeout > 0:
            lqcd_logger.info(
                f"Server {mcp_name} is not running. Waiting for it to be ready."
            )
            await asyncio.sleep(5)
            server_status = await self.backend_mcp_server_status(mcp_name)
            timeout -= 5

        if timeout <= 0:
            lqcd_logger.error(
                f"Server {mcp_name} is not running after {timeout} seconds."
            )
            return None

        # Now get mcp server from our cache which should be updated by the calling
        # server status checker
        async with self._lock:
            backend_info = self.slurm_mcp_servers[mcp_name]
            # sanity check
            if backend_info.url == "" or backend_info.valid == False:
                lqcd_logger.error(
                    f"Strange: backend server {mcp_name} is not ready. Error message is {backend_info.error_message}."
                )
                return None

        # Construct full backend URL (routed through proxy)
        # Proxy path: /cloud/{id}/mcp routes to Backend: /mcp
        lqcd_logger.debug(f"Backend URL: {backend_info.url}")
        backend_url = f"{self.proxy_url}/cloud/{mcp_name}/mcp"
        lqcd_logger.debug(f"Resolved Backend URL: {backend_url}")

        # 3. Connect to Backend (while keeping Proxy connection alive)
        lqcd_logger.debug(f"Connecting to Backend at: {backend_url}")
        try:
            backend_client = LQCDMCPClient.BackendClient(
                self._https_connect, self._server_self_signed_cert
            )
            await backend_client.connect(backend_url, elicitation_handler)
        except Exception as e:
            lqcd_logger.error(
                "Error connecting to backend server: {}. Error message: {}".format(
                    backend_url, e
                )
            )
            return None

        # add the backend client to the cache
        async with self._lock:
            self.backend_clients[mcp_name] = backend_client

        return backend_client.client

    # Disconnect from a backend server
    async def disconnect_from_backend_mcp_server(self, mcp_name: str):
        """Disconnect from a backend server.
        args:
            mcp_name: The name of the mcp server.
        """
        async with self._lock:
            if mcp_name not in self.backend_clients:
                lqcd_logger.error(f"Server {mcp_name} is not connected.")
                return
            backend_client = self.backend_clients[mcp_name]
            await backend_client.close()
            del self.backend_clients[mcp_name]
            del self.slurm_mcp_servers[mcp_name]

    async def close(self):
        try:
            await self.exit_stack.aclose()
        except Exception as e:
            lqcd_logger.error(f"Error closing backend client: {e}")
        finally:
            self.proxy_client = None

    async def get_proxy_client(self) -> Client:
        return self.proxy_client

    async def get_backend_client(self, mcp_name: str) -> Client | None:
        async with self._lock:
            if mcp_name not in self.backend_clients:
                lqcd_logger.error(f"Server {mcp_name} is not connected.")
                return None
            return self.backend_clients[mcp_name]

    async def get_proxy_url(self) -> str:
        return self.proxy_mcpurl

    async def get_mcp_name(self) -> str:
        return self.mcp_name

    async def get_openai_client(self) -> OpenAI:
        return self.openai

    async def get_llm_model(self) -> str:
        return self.llm_model

    async def get_exit_stack(self):
        return self.exit_stack

    async def do_debug(self) -> bool:
        return self.debug
