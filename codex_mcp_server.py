# FastMCP server wrapping Codex and Lite Codex CLI tools.
# Designed to be launched as a Slurm job on behalf of a user and registered
# with the JLab LQCD MCP proxy server.
#
# Features:
#   - Stateful in-memory configuration & session caching across tool invocations.
#   - Generic extra_env dictionary to pass provider secrets and endpoints
#     (e.g., LITELLM_*, OPENAI_*, OPENROUTER_*) in memory without exposing them
#     to sbatch, sacct, or Slurm logs.
#   - Simple workflow: codex_configure / codex_start -> codex_run (multi-turn) -> codex_cancel.

import argparse
import asyncio
import json
import os
import shlex
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Union

import uvicorn
from fastmcp import Context, FastMCP

from lqcd_logger import lqcd_logger
from server_util import get_hostname_and_ip_address, register_mcp_slurm_server

# Initialize FastMCP Server
codex_mcp = FastMCP("LQCD Codex Slurm Server")


def normalize_extra_env(extra_env: Any) -> dict[str, str]:
    """Parse extra_env whether passed as a dict, JSON string, or KEY=VAL lines."""
    if not extra_env:
        return {}
    if isinstance(extra_env, dict):
        return {str(k): str(v) for k, v in extra_env.items()}
    if isinstance(extra_env, str):
        text = extra_env.strip()
        if not text:
            return {}
        # Try JSON first
        if (text.startswith("{") and text.endswith("}")) or (text.startswith("[") and text.endswith("]")):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    return {str(k): str(v) for k, v in parsed.items()}
            except Exception:
                pass
        # Fallback: parse KEY=VAL (delimited by newline, semicolon, or comma)
        env_map = {}
        delimiter = "\n" if "\n" in text else (";" if ";" in text else ",")
        for line in text.split(delimiter):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                env_map[k.strip()] = v.strip().strip("'\"")
        return env_map
    return {}


# ===========================================================================
# Stateful Session & Configuration Cache
# ===========================================================================
@dataclass
class CodexState:
    """In-memory cache for user configuration and active session context."""

    cli_command: str = "lite codex"
    profile: Optional[str] = None
    workdir: Optional[str] = None
    model: Optional[str] = None
    config_overrides: list[str] = field(default_factory=list)
    extra_env: dict[str, str] = field(default_factory=dict)
    current_session_id: Optional[str] = None
    active_process: Optional[asyncio.subprocess.Process] = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def update_config(
        self,
        cli_command: Optional[str] = None,
        profile: Optional[str] = None,
        workdir: Optional[str] = None,
        model: Optional[str] = None,
        config_overrides: Optional[list[str]] = None,
        extra_env: Optional[Union[dict[str, str], str]] = None,
        session_id: Optional[str] = None,
    ):
        async with self._lock:
            if cli_command is not None:
                self.cli_command = cli_command
            if profile is not None:
                self.profile = profile
            if workdir is not None:
                self.workdir = workdir
            if model is not None:
                self.model = model
            if config_overrides is not None:
                self.config_overrides = config_overrides
            if session_id is not None:
                self.current_session_id = session_id
            if extra_env is not None:
                parsed_env = normalize_extra_env(extra_env)
                if parsed_env:
                    self.extra_env.update(parsed_env)

    async def set_active_process(self, proc: Optional[asyncio.subprocess.Process], session_id: str):
        async with self._lock:
            self.active_process = proc
            if session_id:
                self.current_session_id = session_id

    async def cancel_active(self, reset_session: bool = False) -> bool:
        async with self._lock:
            proc = self.active_process
            if reset_session:
                self.current_session_id = None

        if proc and proc.returncode is None:
            lqcd_logger.info(f"Terminating active Codex process (pid={proc.pid})")
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                proc.kill()
            async with self._lock:
                self.active_process = None
            return True
        return False

    async def get_summary(self) -> dict[str, Any]:
        async with self._lock:
            is_running = self.active_process is not None and self.active_process.returncode is None
            configured_env_keys = list(self.extra_env.keys())
            return {
                "cli_command": self.cli_command,
                "current_session_id": self.current_session_id,
                "is_running": is_running,
                "profile": self.profile,
                "model": self.model,
                "workdir": self.workdir or os.getcwd(),
                "config_overrides": self.config_overrides,
                "configured_env_vars": configured_env_keys,
            }


# Global state singleton for this server instance
state = CodexState()


def resolve_cli_command(cli_command: str) -> list[str]:
    """
    Parse and validate the executable command tokens.
    Supports single binaries ('codex', 'lite-codex') and multi-token commands ('lite codex').
    """
    tokens = shlex.split(cli_command.strip())
    if not tokens:
        tokens = ["codex"]

    primary_bin = tokens[0]

    # Look for the executable in PATH or via CODEX_CLI_PATH
    if primary_bin == "codex" and os.getenv("CODEX_CLI_PATH"):
        tokens[0] = os.getenv("CODEX_CLI_PATH", "codex")
    elif not shutil.which(primary_bin):
        # Fallback attempts for common variants
        if primary_bin in ("lite-codex", "lite_codex"):
            for alt in ("lite-codex", "lite_codex", "litecodex", "codex"):
                if shutil.which(alt):
                    tokens[0] = alt
                    break
        elif primary_bin == "lite" and len(tokens) > 1 and tokens[1] == "codex":
            # If 'lite codex' is requested but 'lite' binary doesn't exist, check 'lite-codex' or 'codex'
            for alt in ("lite-codex", "lite_codex", "codex"):
                if shutil.which(alt):
                    tokens = [alt]
                    break

    # Final check
    resolved = shutil.which(tokens[0])
    if not resolved:
        raise FileNotFoundError(
            f"Could not resolve CLI executable '{tokens[0]}' in PATH. "
            "Please ensure the tool is installed or set CODEX_CLI_PATH."
        )

    tokens[0] = resolved
    return tokens


async def run_codex_process(
    prompt: str,
    cli_command: Optional[str] = None,
    profile: Optional[str] = None,
    config_overrides: Optional[list[str]] = None,
    session_id: Optional[str] = None,
    workdir: Optional[str] = None,
    model: Optional[str] = None,
    bypass_sandbox: bool = True,
    ctx: Optional[Context] = None,
) -> dict[str, Any]:
    """Execute Codex CLI via subprocess using cached configuration and active session context."""
    # Merge overrides with cached state
    active_cli = cli_command or state.cli_command
    active_profile = profile if profile is not None else state.profile
    active_workdir = workdir or state.workdir or os.getcwd()
    active_model = model if model is not None else state.model
    active_session_id = session_id if session_id is not None else state.current_session_id
    active_overrides = config_overrides if config_overrides is not None else state.config_overrides

    cmd_tokens = resolve_cli_command(active_cli)

    args = list(cmd_tokens)
    args.append("exec")

    # If an active session exists (either passed or cached), resume it
    if active_session_id:
        args.extend(["resume", active_session_id])

    # Enforce JSON output for programmatic parsing
    args.append("--json")
    args.append("--skip-git-repo-check")

    if active_profile:
        args.extend(["-p", active_profile])

    if active_model:
        args.extend(["-m", active_model])

    for override in active_overrides:
        args.extend(["-c", override])

    if os.path.isdir(active_workdir):
        args.extend(["-C", active_workdir])

    if bypass_sandbox:
        args.append("--dangerously-bypass-approvals-and-sandbox")

    args.append(prompt)

    # Prepare subprocess environment (inherits current env + cached extra env vars)
    env = os.environ.copy()
    if state.extra_env:
        env.update(state.extra_env)

    lqcd_logger.info(
        f"Spawning: {' '.join(shlex.quote(a) for a in args[:6])}... [resume={active_session_id is not None}] in {active_workdir}"
    )

    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=active_workdir,
        env=env,
    )

    tracked_session_id = active_session_id or f"codex-proc-{proc.pid}"
    await state.set_active_process(proc, tracked_session_id)

    messages = []
    events = []

    try:
        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            line_str = line.decode("utf-8", errors="replace").strip()
            if not line_str:
                continue

            try:
                event_data = json.loads(line_str)
                events.append(event_data)

                # Capture session ID emitted by Codex runtime
                if isinstance(event_data, dict):
                    if "session_id" in event_data:
                        tracked_session_id = str(event_data["session_id"])
                        state.current_session_id = tracked_session_id
                    elif event_data.get("type") == "session_started" and "id" in event_data:
                        tracked_session_id = str(event_data["id"])
                        state.current_session_id = tracked_session_id

                    # Extract assistant message contents
                    if event_data.get("type") == "item_created":
                        item = event_data.get("item", {})
                        if item.get("type") == "message":
                            messages.append(item.get("content", ""))

                    # Stream text delta back to the MCP caller
                    if ctx and event_data.get("type") == "item_delta":
                        delta_text = event_data.get("delta", {}).get("text", "")
                        if delta_text:
                            await ctx.info(delta_text)
            except json.JSONDecodeError:
                messages.append(line_str)

        _, stderr_data = await proc.communicate()
    finally:
        await state.set_active_process(None, tracked_session_id)

    stderr_text = stderr_data.decode("utf-8", errors="replace").strip() if stderr_data else ""

    return {
        "session_id": tracked_session_id,
        "cli_command": active_cli,
        "profile": active_profile,
        "model": active_model,
        "returncode": proc.returncode,
        "success": proc.returncode == 0,
        "output": "\n".join([m for m in messages if m]).strip(),
        "stderr": stderr_text if stderr_text else None,
        "events_count": len(events),
    }


# ===========================================================================
# MCP Prompts & Tools
# ===========================================================================
@codex_mcp.prompt()
async def codex_usage_guide() -> str:
    """Instructions and workflow for using the Codex Slurm runner."""
    return (
        "Interactive Codex runner on Slurm:\n"
        "1. (Optional) Call 'codex_start' or 'codex_configure' to set custom extra_env (e.g. for LiteLLM, "
        "OpenRouter, OpenAI), profile, or default working directory.\n"
        "2. Call 'codex_run' with your prompt. The session ID is automatically cached and reused "
        "for subsequent calls (multi-turn conversation).\n"
        "3. Check status with 'codex_status', or end/reset with 'codex_cancel'."
    )


@codex_mcp.tool(
    description=(
        "Initialize or update the Codex runner settings. Allows setting extra_env "
        "(as a dict, JSON string, or KEY=VAL lines, e.g. 'LITELLM_PROXY_URL=...\\nLITELLM_PROXY_API_KEY=...'), "
        "CLI command (e.g. 'lite codex' or 'codex'), profile, model, and working directory. "
        "All provided settings will be cached for subsequent 'codex_run' calls."
    )
)
async def codex_configure(
    cli_command: Optional[str] = None,
    profile: Optional[str] = None,
    workdir: Optional[str] = None,
    model: Optional[str] = None,
    config_overrides: Optional[list[str]] = None,
    reset_session_id: bool = False,
    extra_env: Optional[Union[dict[str, str], str]] = None,
) -> dict[str, Any]:
    """Configure Codex settings and environment variables into memory."""
    new_session_id = None if reset_session_id else state.current_session_id
    await state.update_config(
        cli_command=cli_command,
        profile=profile,
        workdir=workdir,
        model=model,
        config_overrides=config_overrides,
        extra_env=extra_env,
        session_id=new_session_id,
    )
    summary = await state.get_summary()
    return {"status": "configured", "current_state": summary}


@codex_mcp.tool(
    description=(
        "Start a new Codex session or initialize settings and immediately run the first prompt. "
        "Accepts optional extra_env for provider secrets and endpoints. Caches the session ID "
        "and configurations for follow-up 'codex_run' calls."
    )
)
async def codex_start(
    prompt: str,
    cli_command: str = "lite codex",
    profile: Optional[str] = None,
    workdir: Optional[str] = None,
    model: Optional[str] = None,
    config_overrides: Optional[list[str]] = None,
    bypass_sandbox: bool = True,
    extra_env: Optional[Union[dict[str, str], str]] = None,
    ctx: Optional[Context] = None,
) -> dict[str, Any]:
    """Start a fresh session with optional environment variable configuration."""
    # Reset existing session ID to force a new session
    await state.update_config(
        cli_command=cli_command,
        profile=profile,
        workdir=workdir,
        model=model,
        config_overrides=config_overrides,
        extra_env=extra_env,
        session_id="",
    )

    return await run_codex_process(
        prompt=prompt,
        cli_command=state.cli_command,
        profile=state.profile,
        config_overrides=state.config_overrides,
        session_id=None,  # start fresh
        workdir=state.workdir,
        model=state.model,
        bypass_sandbox=bypass_sandbox,
        ctx=ctx,
    )


@codex_mcp.tool(
    description=(
        "Send a prompt to Codex. Automatically reuses the cached session ID, working directory, "
        "and environment variables from prior calls for seamless multi-turn interaction."
    )
)
async def codex_run(
    prompt: str,
    session_id: Optional[str] = None,
    model: Optional[str] = None,
    workdir: Optional[str] = None,
    bypass_sandbox: bool = True,
    ctx: Optional[Context] = None,
) -> dict[str, Any]:
    """Run Codex using cached session state."""
    return await run_codex_process(
        prompt=prompt,
        cli_command=state.cli_command,
        profile=state.profile,
        config_overrides=state.config_overrides,
        session_id=session_id,  # if None, run_codex_process uses state.current_session_id
        workdir=workdir,
        model=model,
        bypass_sandbox=bypass_sandbox,
        ctx=ctx,
    )


# Alias codex_exec to codex_run for backwards compatibility
codex_exec = codex_mcp.tool(
    name="codex_exec",
    description="Alias for codex_run: execute or continue a task with the cached session.",
)(codex_run)


@codex_mcp.tool(description="Check current Codex runner status, active session ID, and cached settings.")
async def codex_status() -> dict[str, Any]:
    """Get the current state and session summary."""
    return await state.get_summary()


@codex_mcp.tool(
    description=(
        "Cancel the active running Codex process. Optionally reset the cached session ID "
        "so the next call starts a fresh conversation."
    )
)
async def codex_cancel(reset_session: bool = False) -> dict[str, Any]:
    """Cancel in-flight task and optionally clear session ID."""
    was_running = await state.cancel_active(reset_session=reset_session)
    summary = await state.get_summary()
    return {
        "cancelled": was_running,
        "session_reset": reset_session,
        "current_state": summary,
    }


@codex_mcp.tool(description="List available configuration profiles found in user's ~/.codex directory.")
async def codex_list_profiles() -> dict[str, Any]:
    """Discover available profile config files in ~/.codex."""
    codex_home = Path(os.getenv("CODEX_HOME", Path.home() / ".codex"))
    profiles = []
    has_base_config = (codex_home / "config.toml").is_file()

    if codex_home.is_dir():
        for f in codex_home.glob("*.config.toml"):
            profile_name = f.name[:-12]  # strip '.config.toml'
            profiles.append(profile_name)

    return {
        "codex_home": str(codex_home),
        "has_base_config": has_base_config,
        "profiles": sorted(profiles),
    }


# ===========================================================================
# Server Startup & Slurm Proxy Registration
# ===========================================================================
codex_mcp_app = codex_mcp.http_app(
    path="/mcp", json_response=False, stateless_http=False, transport="http"
)


async def setup_run(proxy_url: str, port: int) -> str:
    """Register this Slurm-launched MCP server with the central MCP proxy."""
    job_id = os.getenv("SLURM_JOB_ID")
    mcp_name = os.getenv("SLURM_JOB_NAME")
    if mcp_name is None:
        raise ValueError("SLURM_JOB_NAME environment variable is not set.")

    if job_id is not None:
        hostname, ip_address = get_hostname_and_ip_address()
        mcp_url = f"http://{ip_address}:{port}/mcp"
        lqcd_logger.info(
            f"Registering Codex Slurm server '{mcp_name}' to proxy '{proxy_url}' at {mcp_url}"
        )
        status = await register_mcp_slurm_server(proxy_url, mcp_name, mcp_url)
        num_tries = 0
        while status is None and num_tries < 10:
            await asyncio.sleep(5)
            lqcd_logger.info(
                f"Retrying registration of '{mcp_name}' to proxy '{proxy_url}' (attempt {num_tries + 1}/10)"
            )
            status = await register_mcp_slurm_server(proxy_url, mcp_name, mcp_url)
            num_tries += 1
        if status is None:
            lqcd_logger.error("Failed to register Codex Slurm server with proxy.")
            exit(1)
    else:
        lqcd_logger.warning("SLURM_JOB_ID not found; running in standalone/test mode.")

    return mcp_name


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="JLab LQCD Codex Slurm MCP Server")
    parser.add_argument(
        "--port", type=int, default=8080, help="Port to listen on (default: 8080)"
    )
    parser.add_argument(
        "--proxy-url",
        type=str,
        default="http://127.0.0.1:8000",
        help="LQCD MCP Proxy Server URL",
    )
    args = parser.parse_args()

    mcp_name = asyncio.run(setup_run(args.proxy_url, args.port))
    lqcd_logger.info(f"Codex Slurm MCP server '{mcp_name}' ready on port {args.port}.")

    uvicorn.run(codex_mcp_app, host="0.0.0.0", port=args.port)
