#!/usr/bin/env python3
import asyncio
import argparse
import os
import json
import httpx
import time
import datetime
from contextlib import AsyncExitStack
from typing import Any
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from mcp.types import TextContent
from rich.console import Console
from rich.prompt import Prompt
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
import platform

console = Console()


# Try to open the browser, but redirect stdout and stderr at the OS level
# to /dev/null to hide any error/warning messages from xdg-open/browsers
# in headless or misconfigured environments.
def _open_browser_silently(url: str) -> bool:
    import os
    import sys
    import webbrowser

    devnull_fd = None
    saved_stdout_fd = None
    saved_stderr_fd = None
    try:
        devnull_fd = os.open(os.devnull, os.O_WRONLY)
        saved_stdout_fd = os.dup(1)
        saved_stderr_fd = os.dup(2)
        os.dup2(devnull_fd, 1)
        os.dup2(devnull_fd, 2)
    except Exception:
        pass

    try:
        res = webbrowser.open(url)
    except Exception:
        res = False
    finally:
        if saved_stdout_fd is not None:
            try:
                os.dup2(saved_stdout_fd, 1)
                os.close(saved_stdout_fd)
            except Exception:
                pass
        if saved_stderr_fd is not None:
            try:
                os.dup2(saved_stderr_fd, 2)
                os.close(saved_stderr_fd)
            except Exception:
                pass
        if devnull_fd is not None:
            try:
                os.close(devnull_fd)
            except Exception:
                pass

    return res


async def do_login(proxy_url: str, verify_ssl: bool) -> str:
    console.print(
        f"[bold blue]Initiating authentication with proxy:[/bold blue] {proxy_url}"
    )

    flow_type = "device_code"
    authorization_url = ""
    redirect_uri = ""
    client_id = ""
    scope = ""

    async with httpx.AsyncClient(verify=verify_ssl, timeout=30.0) as client:
        try:
            info_res = await client.get(f"{proxy_url}/auth/info")
            if info_res.status_code == 200:
                info_data = info_res.json()
                flow_type = info_data.get("flow_type", "device_code")
                authorization_url = info_data.get("authorization_url", "")
                redirect_uri = info_data.get("redirect_uri", "")
                client_id = info_data.get("client_id", "")
                scope = info_data.get("scope", "")
        except Exception:
            pass

        if flow_type == "auth_code":
            # Authorization Code Flow with PKCE (required by Globus for native apps)
            import secrets
            import hashlib
            import base64

            code_verifier = secrets.token_urlsafe(64)
            code_challenge_hash = hashlib.sha256(code_verifier.encode('utf-8')).digest()
            code_challenge = base64.urlsafe_b64encode(code_challenge_hash).decode('utf-8').replace('=', '')

            auth_url = (
                f"{authorization_url}?client_id={client_id}"
                f"&redirect_uri={redirect_uri}"
                f"&scope={scope}"
                f"&response_type=code"
                f"&code_challenge={code_challenge}"
                f"&code_challenge_method=S256"
            )
            
            panel = Panel(
                f"1. Open your browser to: [bold green]{auth_url}[/bold green]\n"
                f"2. Log in and copy the authorization code.\n"
                f"3. Paste the authorization code below.",
                title="Authentication Required",
                expand=False,
            )
            console.print(panel)

            _open_browser_silently(auth_url)

            auth_code = Prompt.ask("[bold cyan]➤ Enter the authorization code[/bold cyan]").strip()

            try:
                res = await client.post(
                    f"{proxy_url}/auth/code-exchange", 
                    params={"code": auth_code, "code_verifier": code_verifier}
                )
                res.raise_for_status()
                poll_data = res.json()

                if "access_token" in poll_data:
                    console.print("[bold green]✅ Login Successful![/bold green]")
                    _cache_poll_data(poll_data)
                    return poll_data["access_token"]
                else:
                    console.print(f"[bold red]Authentication failed:[/bold red] {poll_data}")
                    raise SystemExit(1)
            except Exception as e:
                console.print(f"[bold red]Failed to exchange code:[/bold red] {e}")
                raise SystemExit(1)


        else:
            # Device Code Flow (e.g. CILogon / GitHub)
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

            _open_browser_silently(verification_uri)

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
                            _cache_poll_data(poll_data)
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


def get_cache_file_path() -> str:
    return os.path.expanduser("~/.lqcd_token_cache.json")


def load_token_cache() -> dict:
    path = get_cache_file_path()
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_token_cache(cache: dict):
    path = get_cache_file_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(cache, f, indent=4)
        os.chmod(path, 0o600)  # Keep it private
    except Exception:
        pass


def get_cached_token_info(token: str) -> dict | None:
    import hashlib
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    cache = load_token_cache()
    return cache.get(token_hash)


def cache_token_info(token: str, info: dict):
    import hashlib
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    cache = load_token_cache()
    
    # Clean up expired tokens to prevent cache growth
    now = time.time()
    cleaned_cache = {}
    for k, v in cache.items():
        if v.get("exp", 0) == 0 or v.get("exp", 0) > now:
            cleaned_cache[k] = v
            
    cleaned_cache[token_hash] = info
    save_token_cache(cleaned_cache)


def get_token_info(token: str) -> dict | None:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        import base64
        import json
        payload_bytes = base64.urlsafe_b64decode(payload_b64)
        return json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        return None


def _cache_poll_data(poll_data: dict):
    if "access_token" not in poll_data:
        return
    token = poll_data["access_token"]
    expires_in = poll_data.get("expires_in")
    scope = poll_data.get("scope", "")
    
    info = {}
    if expires_in:
        info["exp"] = int(time.time()) + int(expires_in)
        info["iat"] = int(time.time())
    if scope:
        info["scope"] = scope
        
    id_token = poll_data.get("id_token")
    if id_token:
        id_info = get_token_info(id_token)
        if id_info:
            for key in ("sub", "email", "preferred_username", "name", "iss"):
                if key in id_info:
                    info[key] = id_info[key]
                    
    info["active"] = True
    
    if info:
        cache_token_info(token, info)


def format_timestamp(epoch: int) -> str:
    dt_utc = datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc)
    dt_local = datetime.datetime.fromtimestamp(epoch)
    return f"{dt_local.strftime('%Y-%m-%d %H:%M:%S')} Local ({dt_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC)"


def format_duration(seconds: float) -> str:
    if seconds <= 0:
        return "[bold red]Expired[/bold red]"
    
    parts = []
    days = int(seconds // 86400)
    hours = int((seconds % 86400) // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
        
    return " ".join(parts)


async def introspect_token_via_proxy(proxy_url: str, token: str, verify_ssl: bool) -> dict | None:
    # First check local cache
    cached_info = get_cached_token_info(token)
    if cached_info:
        exp = cached_info.get("exp")
        if not exp or exp > time.time():
            return cached_info

    # Second try local JWT decoding
    info = get_token_info(token)
    if info:
        cache_token_info(token, info)
        return info
        
    # If it's not a JWT, call the proxy's introspect endpoint
    try:
        async with httpx.AsyncClient(verify=verify_ssl, timeout=10.0) as client:
            resp = await client.post(
                f"{proxy_url}/auth/introspect",
                json={"token": token}
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("active") is not False:
                    cache_token_info(token, data)
                    return data
    except Exception:
        pass
    return None


def update_mcp_json(
    proxy_url: str, token: str, transport_type: str, custom_path: str | None = None
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
                "path": os.path.expanduser("~/.gemini/config/mcp_config.json"),
                "entry_key": "mcpServers",
                "server_name": "lqcd-mcp-proxy",
                "format": "antigravity",
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
                "path": os.path.expanduser("~/.gemini/config/mcp_config.json"),
                "entry_key": "mcpServers",
                "server_name": "lqcd-mcp-proxy",
                "format": "antigravity",
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
        client_args: dict[str, Any] = {
            "url": f"{proxy_url}/jlab",
            "headers": {"Authorization": f"Bearer {token}"},
        }

        if insecure:

            def _insecure_httpx_client_factory(
                headers: dict[str, str] | None = None,
                timeout: httpx.Timeout | None = None,
                auth: httpx.Auth | None = None,
                **kwargs,
            ) -> httpx.AsyncClient:
                kwargs.setdefault("follow_redirects", True)
                if timeout is None and not kwargs.get("timeout"):
                    timeout = httpx.Timeout(30.0, read=300.0)
                return httpx.AsyncClient(
                    verify=False, headers=headers, timeout=timeout, auth=auth, **kwargs
                )

            client_args["httpx_client_factory"] = _insecure_httpx_client_factory

        async with AsyncExitStack() as stack:
            transport = StreamableHttpTransport(**client_args)
            client = await stack.enter_async_context(Client(transport))

            result = await client.call_tool("validate_user", {"username": ""})

            # FastMCP v3 returns data mapping or content list. we can check content explicitly
            # Assuming tool returned json-string or dict inside contents

            user_info = None
            if hasattr(result, "content") and len(result.content) > 0:
                first_item = result.content[0]
                if isinstance(first_item, TextContent):
                    user_info = json.loads(first_item.text)
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
    parser.add_argument(
        "--token",
        type=str,
        default="",
        help="Directly use a copied authentication token instead of logging in",
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

    # Check command-line flag or environment variable for manual token override
    token = args.token or os.getenv("LQCDMCP_TOKEN", "")
    if token:
        console.print("[bold green]Using manually provided token. Skipping login flow...[/bold green]")
    else:
        console.print("\n[bold cyan]Select Authentication Method:[/bold cyan]")
        console.print("  [green]1)[/green] Globus / OIDC (automatic browser login)")
        console.print("  [green]2)[/green] American Science Cloud Portal (copy-paste token)")
        
        choice = Prompt.ask(
            "[bold cyan]➤ Choose an option[/bold cyan]",
            choices=["1", "2"],
            default="1"
        )
        
        if choice == "2":
            asc_url = "https://my.american-science-cloud.org"
            console.print(f"\n1. Open your browser to: [bold green]{asc_url}[/bold green]")
            console.print("2. Log in and copy the access token from your profile page.")
            import webbrowser
            try:
                webbrowser.open(asc_url)
            except Exception:
                pass
            token = Prompt.ask("[bold cyan]➤ Enter the access token[/bold cyan]").strip()
        else:
            token = await do_login(proxy_url, not args.insecure)


    # Show token validity details
    token_info = await introspect_token_via_proxy(proxy_url, token, not args.insecure)
    if token_info:
        exp = token_info.get("exp")
        if exp:
            time_left = exp - time.time()
            console.print(f"\n[bold blue]Token validity period:[/bold blue] Expires at {format_timestamp(exp)} ({format_duration(time_left)} remaining)")

    if args.show_token:
        if token_info:
            lines = []
            sub = token_info.get("sub") or token_info.get("login") or token_info.get("username") or token_info.get("email") or token_info.get("preferred_username") or token_info.get("name")
            if sub:
                lines.append(f"[bold cyan]User/Subject:[/bold cyan] {sub}")
            iss = token_info.get("iss")
            if iss:
                lines.append(f"[bold cyan]Issuer:[/bold cyan] {iss}")
            iat = token_info.get("iat")
            if iat:
                lines.append(f"[bold cyan]Issued At:[/bold cyan] {format_timestamp(iat)}")
            exp = token_info.get("exp")
            if exp:
                lines.append(f"[bold cyan]Expires At:[/bold cyan] {format_timestamp(exp)}")
                time_left = exp - time.time()
                lines.append(f"[bold cyan]Time Remaining:[/bold cyan] {format_duration(time_left)}")
            scope = token_info.get("scope") or token_info.get("scp")
            if scope:
                lines.append(f"[bold cyan]Scope:[/bold cyan] {scope}")
            active = token_info.get("active")
            if active is not None:
                lines.append(f"[bold cyan]Status:[/bold cyan] {'[bold green]Active[/bold green]' if active else '[bold red]Inactive[/bold red]'}")
            
            panel_content = "\n".join(lines)
            panel = Panel(
                panel_content,
                title="[bold green]Token Information[/bold green]",
                expand=False,
                border_style="green"
            )
            console.print(panel)
        else:
            console.print("[bold yellow]Token is opaque and introspection was unsuccessful. Validity details cannot be parsed locally.[/bold yellow]")

        console.print(f"\n[bold magenta]Your authentication token:[/bold magenta] {token}\n")
        
    update_mcp_json(proxy_url, token, args.transport, args.extra_config)
    await verify_local_account(proxy_url, token, args.insecure)



if __name__ == "__main__":
    asyncio.run(main())
