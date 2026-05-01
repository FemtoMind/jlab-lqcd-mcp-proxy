#!/usr/bin/env python3
import asyncio
import argparse
import os
import json
import httpx
from contextlib import AsyncExitStack
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from rich.console import Console
from rich.prompt import Prompt
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
import platform

console = Console()


async def do_login(proxy_url: str, verify_ssl: bool) -> str:
    console.print(
        f"[bold blue]Initiating authentication with proxy:[/bold blue] {proxy_url}"
    )

    async with httpx.AsyncClient(verify=verify_ssl, timeout=30.0) as client:
        try:
            res = await client.post(f"{proxy_url}/auth/device-code")
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            console.print(f"[bold red]Failed to get device code:[/bold red] {e}")
            raise SystemExit(1)

        verification_uri = data.get("verification_uri")
        user_code = data.get("user_code")
        device_code = data.get("device_code")
        interval = data.get("interval", 5)

        panel = Panel(
            f"1. Open your browser to: [bold green]{verification_uri}[/bold green]\n"
            f"2. Enter the code: [bold yellow]{user_code}[/bold yellow]",
            title="Authentication Required",
            expand=False,
        )
        console.print(panel)

        import webbrowser

        try:
            webbrowser.open(verification_uri)
        except Exception:
            pass

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            progress.add_task(
                description="Waiting for browser authorization...", total=None
            )
            while True:
                try:
                    poll = await client.post(
                        f"{proxy_url}/auth/poll", params={"device_code": device_code}
                    )
                    poll_data = poll.json()

                    if "access_token" in poll_data:
                        console.print("[bold green]✅ Login Successful![/bold green]")
                        return poll_data["access_token"]

                    error = poll_data.get("error")
                    if error not in ["authorization_pending", "slow_down"]:
                        console.print(
                            f"[bold red]Authentication failed:[/bold red] {poll_data}"
                        )
                        raise SystemExit(1)
                except httpx.ReadError:
                    pass  # Ignore temporary network glitches
                except Exception as e:
                    console.print(f"[bold red]Error during polling:[/bold red] {e}")
                    raise SystemExit(1)

                await asyncio.sleep(interval)


def update_mcp_json(
    proxy_url: str, token: str, transport_type: str, custom_path: str = None
):
    # Determine correct endpoint based on transport type
    if transport_type == "sse":
        url = f"{proxy_url}/jlab_sse/sse"
    else:
        url = f"{proxy_url}/jlab"

    # find out I am on Mac or Linux for better instructions
    system = platform.system()
    if system == "Linux":
        target_configs = [
            {
                "path": os.path.expanduser("~/.config/Code/User/mcp.json"),
                "entry_key": "servers",
                "server_name": "jlab-lqcd-mcp-proxy",
                "format": "vscode",
            },
            {
                "path": os.path.expanduser("~/.gemini/antigravity/mcp_config.json"),
                "entry_key": "mcpServers",
                "server_name": "lqcd-mcp-proxy",
                "format": "antigravity",
            },
        ]
    elif system == "Darwin":
        target_configs = [
            {
                "path": os.path.expanduser("~/Library/Application Support/Code/User/mcp.json"),
                "entry_key": "servers",
                "server_name": "jlab-lqcd-mcp-proxy",
                "format": "vscode",
            },
            {
                "path": os.path.expanduser("~/.gemini/antigravity/mcp_config.json"),
                "entry_key": "mcpServers",
                "server_name": "lqcd-mcp-proxy",
                "format": "antigravity",
            },
        ]
    else:
        console.print("[bold yellow]Only Linux and MacOS are officially supported for automatic configuration updates.[/bold yellow]")
        console.print(f"[bold yellow]Detected system: {system}. You need to copy token manually. You may need to manually update your MCP client configuration.[/bold yellow]")

    if custom_path:
        target_configs.append(
            {
                "path": os.path.expanduser(custom_path),
                "entry_key": "servers",
                "server_name": "jlab-lqcd-mcp-proxy",
                "format": "vscode",
            }
        )

    for tc in target_configs:
        path = tc["path"]
        console.print(f"[bold blue]Updating configuration file at:[/bold blue] {path}")

        config = {tc["entry_key"]: {}}
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    file_content = f.read().strip()
                    if file_content:
                        config = json.loads(file_content)
            except json.JSONDecodeError:
                console.print(
                    f"[bold yellow]Warning: {path} is malformed. Assuming empty layout.[/bold yellow]"
                )

        if tc["entry_key"] not in config:
            config[tc["entry_key"]] = {}

        # Remove previous backend mcp servers launched by the proxy server
        entries = config[tc["entry_key"]]
        servers_to_remove = []
        for s_name, s_config in entries.items():
            if not isinstance(s_config, dict):
                continue
            
            # Skip the main proxy server itself as it gets overwritten
            if s_name == tc["server_name"]:
                continue
                
            s_url = s_config.get("url", s_config.get("serverUrl", ""))
            
            # Check if this server points to the current proxy_url AND has the '/cloud/' subpath
            if s_url and "/cloud/" in s_url:
                servers_to_remove.append(s_name)
                
        for s_name in servers_to_remove:
            del entries[s_name]
            console.print(f"[bold yellow]Removed previous backend MCP server from config:[/bold yellow] {s_name}")

        if tc["format"] == "vscode":
            config[tc["entry_key"]][tc["server_name"]] = {
                "type": transport_type,
                "url": url,
                "headers": {"Authorization": f"Bearer {token}"},
            }
        else:  # antigravity format
            config[tc["entry_key"]][tc["server_name"]] = {
                "serverUrl": url,
                "headers": {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            }

        # Write back to file
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                json.dump(config, f, indent=4)
            console.print(f"[bold green]✅ Successfully updated {path}![/bold green]")
        except Exception as e:
            console.print(f"[bold red]Failed to write to {path}:[/bold red] {e}")


async def verify_local_account(proxy_url: str, token: str, insecure: bool):
    console.print(
        f"\n[bold blue]Verifying local account mapping on proxy...[/bold blue]"
    )
    try:
        client_args = {
            "url": f"{proxy_url}/jlab",
            "headers": {"Authorization": f"Bearer {token}"},
        }

        if insecure:

            def _insecure_httpx_client_factory(**kwargs) -> httpx.AsyncClient:
                kwargs.setdefault("follow_redirects", True)
                if not kwargs.get("timeout"):
                    kwargs["timeout"] = httpx.Timeout(30.0, read=300.0)
                return httpx.AsyncClient(verify=False, **kwargs)

            client_args["httpx_client_factory"] = _insecure_httpx_client_factory

        async with AsyncExitStack() as stack:
            transport = StreamableHttpTransport(**client_args)
            client = await stack.enter_async_context(Client(transport))

            result = await client.call_tool("validate_user", {"username": ""})

            # FastMCP v3 returns data mapping or content list. we can check content explicitly
            # Assuming tool returned json-string or dict inside contents

            user_info = None
            if hasattr(result, "content") and len(result.content) > 0:
                user_info_json = result.content[0].text
                user_info = json.loads(user_info_json)
            elif isinstance(result, dict):
                user_info = result
            elif hasattr(result, "data"):
                user_info = result.data

            if isinstance(user_info, dict):
                user_id = user_info.get("user_id", "unknown")
                user_account = user_info.get("user_account", "unknown")
                if user_account == "unknown":
                    console.print(
                        f"[bold red]User account mapping not found. Your identity '{user_id}' has no local slurm account.[/bold red]"
                    )
                else:
                    console.print(
                        f"[bold green]✅ Success! Mapped to local JLab user account: [white]{user_account}[/white][/bold green]"
                    )
                    console.print(
                        f"You're all set to launch and manage interactive Slurm tasks!"
                    )
            else:
                console.print(
                    f"[bold yellow]Received unexpected mapping response format: {result}[/bold yellow]"
                )

    except Exception as e:
        console.print(
            f"[bold red]Warning: Failed to fetch local user account mapping:[/bold red] {e}"
        )


async def main():
    parser = argparse.ArgumentParser(
        description="Authenticate and configure VS Code / MCP clients."
    )
    parser.add_argument(
        "--proxy-url", type=str, default="", help="URL of the proxy server"
    )
    parser.add_argument(
        "--insecure", action="store_true", help="Disable SSL certificate verification"
    )
    parser.add_argument(
        "--transport",
        type=str,
        choices=["sse", "http"],
        default="http",
        help="Transport type for MCP (default: http)",
    )
    parser.add_argument(
        "--extra-config",
        type=str,
        default=None,
        help="Additional custom mcp.json file to update",
    )
    parser.add_argument(
        "--show-token",
        action="store_true",
        help="Show the authentication token after login (not recommended for shared environments)",
    )
    args = parser.parse_args()

    proxy_url = args.proxy_url
    if not proxy_url:
        console.print()
        console.print(
            Panel(
                "[bold yellow]Proxy Server URL is required.[/bold yellow]\n"
                "Please enter the URL of the LQCD MCP Proxy Server you wish to connect to.",
                title="Configuration",
                expand=False,
                border_style="yellow",
            )
        )
        proxy_url = Prompt.ask(
            "[bold cyan]➤ Enter proxy server URL[/bold cyan]", default="http://localhost:8123"
        )

    # Strip trailing slash
    proxy_url = proxy_url.rstrip("/")

    token = await do_login(proxy_url, not args.insecure)
    if args.show_token:
        console.print(f"\n[bold magenta]Your authentication token:[/bold magenta] {token}\n")
        
    update_mcp_json(proxy_url, token, args.transport, args.extra_config)
    await verify_local_account(proxy_url, token, args.insecure)


if __name__ == "__main__":
    asyncio.run(main())
