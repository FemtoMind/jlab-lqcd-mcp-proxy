# This is a mcp server handling lqcd analysis tasks using
# Jefferson Lab LQCD batching SLURM batch system.
#
# Idea is to launch user mcp server on a remote slurm node
# and let user connect to it. The user can launch mcp tools
# and come back or wait for the available of the mcp server.
# The mcp server will be automatically terminated when the
# user disconnects. The user can also manually terminate the
# mcp server.
# In the future, we may allow the user to leave the mcp server
# and come back later to continue their work.
# The mcp server can be only used by the same user who launched it.
import argparse

import subprocess
import asyncio
import os
import uvicorn
import pwd
import logging

# load .env
from dotenv import load_dotenv

from lqcd_logger import lqcd_logger


import common_data as cdata

# our own session manager
from session_manager import lqcd_session_manager

from fastmcp import FastMCP
from fastmcp.server.context import Context as ServerContext
from fastmcp.server.middleware import Middleware, MiddlewareContext

# import token validation helper
from lqcd_oidc_auth import validate_authorized_token
from lqcd_oidc_auth import get_local_account

# load .env file
load_dotenv(os.path.join(os.path.dirname(__file__), ".server_env"), override=True)

lqcd_logger.info("Loading .server_env file...")
# Check whether we are running inside jlab network
_jlab_slurm = os.getenv("MCP_USE_JLAB_SLURM", "true").lower() == "true"

# check debug mode
_debug = os.getenv("DEBUG", "false").lower() == "true"

if _jlab_slurm:
    lqcd_logger.info("Our servers use Jlab slurm system.")
else:
    lqcd_logger.info("Our servers use local test slurm system.")

if _debug:
    lqcd_logger.setLevel(logging.DEBUG)
else:
    lqcd_logger.setLevel(logging.INFO)
lqcd_logger.info("Loading .server_env file... done.")


# A class manage slurm backend servers
class SlurmMcpServers:
    """Manage slurm backend servers."""

    def __init__(self):
        """Initialize the slurm backend server manager."""
        self.slurm_mcp_servers: dict[str, cdata.SlurmMcpServer] = {}
        self.slurm_mcp_servers_by_jobid: dict[int, cdata.SlurmMcpServer] = {}
        self._lock = asyncio.Lock()

    async def add_slurm_mcp_server(
        self, slurm_mcp_server: cdata.SlurmMcpServer
    ) -> bool:
        """Add a slurm backend server."""
        async with self._lock:
            if slurm_mcp_server.mcp_name in self.slurm_mcp_servers:
                existing_server = self.slurm_mcp_servers[slurm_mcp_server.mcp_name]
                if existing_server.slurm_job_state == "RUNNING":
                    lqcd_logger.warning(
                        f"Slurm mcp server {slurm_mcp_server.mcp_name} already exists."
                    )
                    return False
            # if the existing server is not running, update it
            self.slurm_mcp_servers[slurm_mcp_server.mcp_name] = slurm_mcp_server
            # update the slurm mcp server by job id
            self.slurm_mcp_servers_by_jobid[slurm_mcp_server.slurm_job_id] = (
                slurm_mcp_server
            )
            lqcd_logger.debug("All slurm mcp servers:{}".format(self.slurm_mcp_servers))
            lqcd_logger.debug(
                "All slurm mcp servers by job id:{}".format(
                    self.slurm_mcp_servers_by_jobid
                )
            )

        return True

    async def update_slurm_mcp_server(
        self, slurm_mcp_server: cdata.SlurmMcpServer
    ) -> bool:
        """Update a slurm backend server."""
        async with self._lock:
            if slurm_mcp_server.mcp_name not in self.slurm_mcp_servers:
                lqcd_logger.warning(
                    f"Slurm mcp server {slurm_mcp_server.mcp_name} not found."
                )
                return False
            self.slurm_mcp_servers[slurm_mcp_server.mcp_name] = slurm_mcp_server
            # update the slurm mcp server by job id
            self.slurm_mcp_servers_by_jobid[slurm_mcp_server.slurm_job_id] = (
                slurm_mcp_server
            )
        return True

    async def is_slurm_mcp_server_valid(self, mcp_name: str) -> bool:
        """Check if a slurm backend server is valid."""
        async with self._lock:
            if mcp_name not in self.slurm_mcp_servers:
                lqcd_logger.warning(f"Slurm mcp server {mcp_name} not found.")
                return False
            return self.slurm_mcp_servers[mcp_name].url != ""

    async def get_slurm_mcp_server(self, mcp_name: str) -> cdata.SlurmMcpServer | None:
        """Get a slurm backend server."""
        async with self._lock:
            if mcp_name not in self.slurm_mcp_servers:
                lqcd_logger.warning(f"Slurm mcp server {mcp_name} not found.")
                return None
            return self.slurm_mcp_servers.get(mcp_name)

    async def get_slurm_mcp_server_by_jobid(
        self, jobid: int
    ) -> cdata.SlurmMcpServer | None:
        """Get a slurm backend server by job id."""
        async with self._lock:
            if jobid not in self.slurm_mcp_servers_by_jobid:
                lqcd_logger.warning(f"Slurm mcp server {jobid} not found.")
                return None
            return self.slurm_mcp_servers_by_jobid.get(jobid)

    async def remove_slurm_mcp_server(self, mcp_name: str) -> bool:
        """Remove a slurm backend server."""
        async with self._lock:
            if mcp_name in self.slurm_mcp_servers:
                jobid = self.slurm_mcp_servers[mcp_name].slurm_job_id
                del self.slurm_mcp_servers[mcp_name]
                # remove the slurm mcp server by job id
                del self.slurm_mcp_servers_by_jobid[jobid]
            return True
        return False

    async def remove_slurm_mcp_server_by_jobid(self, jobid: int) -> bool:
        """Remove a slurm backend server by job id."""
        async with self._lock:
            if jobid in self.slurm_mcp_servers_by_jobid:
                mcp_name = self.slurm_mcp_servers_by_jobid[jobid].mcp_name
                del self.slurm_mcp_servers_by_jobid[jobid]
                # remove the slurm mcp server by server id
                del self.slurm_mcp_servers[mcp_name]
            return True
        return False

    async def get_slurm_mcp_servers_by_owner(
        self, owner: str
    ) -> list[cdata.SlurmMcpServer]:
        """Get all slurm backend servers owned by the user."""
        async with self._lock:
            return [
                server
                for server in self.slurm_mcp_servers.values()
                if server.owner == owner
            ]

    async def get_all_slurm_mcp_servers(self) -> list[cdata.SlurmMcpServer]:
        """Get all slurm backend servers."""
        async with self._lock:
            return list(self.slurm_mcp_servers.values())

    async def get_slurm_mcp_server_count(self) -> int:
        """Get the number of slurm backend servers."""
        async with self._lock:
            return len(self.slurm_mcp_servers)

    async def get_slurm_mcp_server_names(self) -> list[str]:
        """Get the list of slurm backend server names."""
        async with self._lock:
            return list(self.slurm_mcp_servers.keys())

    async def get_slurm_mcp_servers_by_owner(
        self, owner: str
    ) -> list[cdata.SlurmMcpServer]:
        """Get all slurm backend servers owned by the user."""
        async with self._lock:
            return [
                server
                for server in self.slurm_mcp_servers.values()
                if server.owner == owner
            ]


# Class handling slurm related tasks
# Some of the code taken from Jupyterhub slurm spawner
import os
import subprocess
from string import Template


class SlurmSpawner:

    def __init__(self):
        """Initialize the slurm spawner."""
        # Initialize the slurm mcp server
        self.slurm_mcp_servers: SlurmMcpServers = SlurmMcpServers()

        # Get slurm resource information
        self.system_slurm_info = None
        # Get slurm config information from above information
        self.slurm_config_info = None

    # get slurm servers as an attribute
    @property
    def mcpservers(self) -> SlurmMcpServers:
        """Get the slurm backend servers."""
        return self.slurm_mcp_servers

    async def get_slurm_info(self) -> cdata.FullSystemResource:
        """Get slurm computing resources."""
        # Use subprocess to get system information
        jlab_commmand = (
            "sinfo --format='%8P %20N %20G %4c %16m %20F %16L %16l' -p 24s,21g -h"
        )

        local_commmand = (
            "sinfo --format='%8P %20N %20G %4c %16m %20F %16L %16l' -p debug -h"
        )

        system_info = cdata.FullSystemResource()
        try:
            command = jlab_commmand
            if _jlab_slurm == False:
                command = local_commmand

            result = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=True,
            )

            # keep track of cluster name
            cluster_name = ""
            # Read the output line by line
            for line in result.stdout:
                tokens = line.strip().split()
                cluster_name = tokens[0]
                # remove characters like ''' and '*' from the name
                cluster_name = cluster_name.replace("'", "").replace("*", "")
                gres = tokens[2]
                num_cores = int(tokens[3])
                memory = int(tokens[4]) / 1000  # Convert MB to GB
                node_info = tokens[5].split("/")
                total_num_nodes = int(node_info[3])
                free_num_nodes = int(node_info[1])
                used_num_nodes = int(node_info[0])
                other_num_nodes = int(node_info[2])
                default_wall_time = tokens[6]
                wall_time_limit = tokens[7]

                if gres.find("null") != -1:
                    gres = ""

                if gres.find("gpu") != -1:
                    cluster_type = "GPU"
                else:
                    cluster_type = "CPU"

                partition_info = cdata.FullSystemResource._partition_resource(
                    partition_name=cluster_name,
                    type=cluster_type,
                    num_nodes=total_num_nodes,
                    num_free_nodes=free_num_nodes,
                    num_busy_nodes=used_num_nodes,
                    num_other_nodes=other_num_nodes,
                    num_cpus_per_node=num_cores,
                    memory_per_node_gb=memory,
                    generic_resource=gres,
                    default_wall_time=default_wall_time,
                    wall_time_limit=wall_time_limit,
                )

                system_info.partitions.append(partition_info)

            # Wait for the process to complete
            result.wait()
        except Exception as e:
            lqcd_logger.error(f"Failed to get slurm system information: {e}")

        # update internal system information
        self.system_slurm_info = system_info

        # get static configuration information
        self.slurm_config_info = cdata.SlurmConfigInfo()
        self.slurm_config_info.update(self.system_slurm_info)
        return system_info

    # Return user account information
    async def get_slurm_accounts(self, user: str) -> list[str] | None:
        """Get available slurm user accounts."""
        userarg = f"user={user}"
        command = ["sacctmgr", "show", "associations", "where", userarg, "-n"]

        try:
            result = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            accounts = []

            # read line one after another
            for line in result.stdout:
                tokens = line.strip().split()
                accounts.append(tokens[1])

            # Wait for the process to finish
            result.wait()
        except Exception as e:
            lqcd_logger.error(f"Failed to get slurm account information: {e}")
            return None

        return accounts

    # Get slurm job information by job id
    async def get_slurm_job_info(self, job_id: str) -> cdata.SlurmJobInfo | None:
        """Get slurm job information."""
        # Build a shell command to get slurm job information
        # make sure to use shell=True
        command = "squeue --job=" + job_id + " --format='%j %T %N %S %l' -h"
        try:
            result = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=True,
            )
            job_info = result.stdout.read().strip().split()
            result.wait()
        except Exception as e:
            lqcd_logger.error(f"Failed to get slurm job information: {e}")
            return None

        sjob: cdata.SlurmJobInfo = None

        state = job_info[1]
        if state == "RUNNING":
            sjob = cdata.SlurmJobInfo(
                job_id=job_id,
                job_name=job_info[0],
                job_state=state,
                job_start_time=job_info[3],
                job_wall_or_end_time=job_info[4],
                job_nodes=job_info[2],
            )
        elif state == "PENDING":
            sjob = cdata.SlurmJobInfo(
                job_id=job_id,
                job_name=job_info[0],
                job_state=state,
                job_start_time="",
                job_wall_or_end_time=job_info[3],
                job_nodes="",
            )
        else:
            sacct_command = (
                "sacct --job="
                + job_id
                + " --format='State,NodeList,jobname,Start,End' -n"
            )
            result = subprocess.Popen(
                sacct_command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                shell=True,
            )

            job_info_lines = result.stdout.splitlines()
            # deal with the first line
            job_info = job_info_lines[0].split(",")

            result.wait()

            sjob = cdata.SlurmJobInfo(
                job_id=job_id,
                job_name=job_info[2],
                job_state=job_info[0],
                job_start_time=job_info[3],
                job_wall_or_end_time=job_info[4],
                job_nodes=job_info[1],
            )
        return sjob

    # Just check a job state
    async def check_slurm_job_state(self, job_id: str) -> str | None:
        """Check the status of a slurm job."""
        try:
            result = subprocess.Popen(
                ["squeue", "--job=" + job_id, "--format=%T", "-h"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            job_state = result.stdout.read().strip()
            result.wait()
        except Exception as e:
            lqcd_logger.error(f"Failed to check slurm job: {e}")
            return None
        return job_state

    # Stop a mcp server by cancelling it
    async def stop_slurm_job(self, job_id: str) -> bool:
        """Stop a slurm job."""
        try:
            result = subprocess.Popen(
                ["scancel", job_id],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            result.wait()

            if result.returncode != 0:
                lqcd_logger.warning(f"Failed to stop slurm job: {result.stderr.read()}")
                return False
            else:
                lqcd_logger.info(f"Successfully stopped slurm job: {job_id}")
                return True

            output = result.stdout.read().strip()
            if ouput not in (None, ""):  # scancel return nothing if success
                lqcd_logger.warning(f"Failed to stop slurm job: {output}")
                return False

        except Exception as e:
            lqcd_logger.error(f"Failed to stop slurm job: {e}")
            return False

        # check job state
        job_state = await self.check_slurm_job_state(job_id)
        if job_state in ("CANCELLED", "COMPLETED", "FAILED", "COMPLETING", ""):
            lqcd_logger.info(f"Successfully stopped slurm job: {job_id}")
            return True
        else:
            lqcd_logger.warning(f"Failed to stop slurm job: {job_state}")
            return False

    # Build a slurm job script
    def build_slurm_job_script(self, user: str, job: cdata.SlurmMcpJob) -> str:
        """Build a slurm job script. We do not set memory limit here since
        mcp server will get a whole node
        """
        gid = pwd.getpwnam(user).pw_gid  # get the group id of the user
        job_script = Template(
            """#!/bin/bash
#SBATCH --job-name=$job_name
#SBATCH --output=/home/$user/$output/$job_name.out
#SBATCH --error=/home/$user/$output/$job_name.err
#SBATCH --time=$time
#SBATCH --nodes=$nodes
#SBATCH --ntasks=$ntasks
#SBATCH --cpus-per-task=$cpus_per_task
#SBATCH --partition=$partition
#SBATCH --account=$account
#SBATCH --uid=$user
#SBATCH --gid=$gid
#SBATCH --export=none
#SBATCH --get-user-env=L

$cmd
        """
        )

        # Fill out the template
        slurm_script = job_script.substitute(
            job_name=job.job_name,
            time=job.time,
            nodes=job.nodes,
            ntasks=job.ntasks,
            cpus_per_task=job.cpus_per_task,
            partition=job.partition,
            account=job.account,
            output=job.output,
            user=user,
            gid=gid,
            cmd=job.run_script,
        )
        return slurm_script

    # Submit a slurm job
    async def submit_slurm_job(
        self, user: str, gid: int, mcp_name: str, job_script: str
    ) -> str | None:
        """Submit a slurm job."""
        # check whether this process is running as root or not
        if os.geteuid() != 0:
            lqcd_logger.warning("This process is not running as root!")
            cmd = "sudo sbatch"
        else:
            cmd = "sbatch"

        """
        # dump job script to file
        rndfile = "/tmp/slurm_mcp_server_" + str(uuid.uuid4()) + ".sh"
        with open(rndfile, "w") as f:
            f.write(job_script)

        try:
            result = subprocess.Popen(
                ["sudo", "sbatch", "--uid", user, "--gid", str(gid), rndfile],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            output = result.stdout.read().strip()
            err = result.stderr.read().strip()
            result.wait()
        except Exception as e:
            lqcd_logger.error(f"Failed to submit slurm job: {e}")
            raise e

        """
        # check first characters of the script to see if it is a valid script
        job_script = job_script.lstrip()
        if job_script[:2] != "#!":
            lqcd_logger.error("Invalid script: missing #!")
            lqcd_logger.error(job_script)
            return None

        lqcd_logger.info("User {} and gid {} submitting slurm job".format(user, gid))
        # submit job using --job-name to override the job name in the script
        try:
            pipe = subprocess.Popen(
                # ["sudo", "sbatch", "--uid", user, "--gid", str(gid), "--job-name", mcp_name],
                ["sudo", "-u", user, "sbatch", "--job-name", mcp_name],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            output = pipe.communicate(input=job_script.encode())[0].strip()
            output = output.decode()
            pipe.wait()

        except Exception as e:
            lqcd_logger.error(f"Failed to submit slurm job: {e}")
            raise e

        if output == "" or len(output) == 0:
            error = "Slurm did not attempt to start the job! Check Slurm logs"
            lqcd_logger.error(error)
            return None

        lqcd_logger.debug("Stdout of trying to launch with sbatch: %s" % output)
        # the job id should be the very last part of the string
        job_id = output.split(" ")[-1]

        lqcd_logger.info("Job id {} submitted successfully".format(job_id))

        # check job state
        job_state = await self.check_slurm_job_state(job_id)
        job_state = job_state.strip()
        lqcd_logger.debug("Job state: {}".format(job_state))
        if job_state is None:
            lqcd_logger.warning(f"Failed to check slurm job: {job_id}")
            return None

        good_states = ["RUNNING", "PENDING"]
        if job_state in good_states:
            lqcd_logger.info(f"Successfully submitted slurm job: {job_id}")
        else:
            lqcd_logger.warning(
                f"Failed to submit slurm job: {job_id} and job state is {job_state}"
            )
            return None

        return job_id


"""
Create a global slurm mcp server
"""
lqcd_slurm_manager: SlurmSpawner = SlurmSpawner()
lqcd_mcp_servers: SlurmMcpServers = lqcd_slurm_manager.mcpservers


# Need to disable some warning messages
slurm_mcp = FastMCP(name="jlab_lqcd_slurm_mcp", log_level="error")


# Velidate access token and map email or other identity
# to local user account
@slurm_mcp.tool(
    description="Validate user identity",
    tags={"auth", "token", "internal"},
)
async def validate_user(username: str, ctx: ServerContext):
    """Returns the authenticated proxy URL and user identity."""
    # check user name is empty or not. If it is not empty, just use it for test
    real_username = username.strip()
    if len(real_username) > 0:
        # Need to register local account to session manager
        await lqcd_session_manager.register(ctx.session_id, "username", username)

        # output some information
        lqcd_logger.info(
            "Use provided username {} as local account for test.".format(username)
        )
        ctx.info("Use provided username {} as local account for test.".format(username))

        return {"user_id": username, "user_account": username}

    # Retrieve authorization header from request
    user_login = "unknown"
    if hasattr(ctx.request_context, "request"):
        req = ctx.request_context.request
        auth_header = req.headers.get("authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            lqcd_logger.debug(f"Token: {token}")
            try:
                # Reuse our existing validation helper
                valid, user_info = validate_authorized_token(token)
                # OIDC provider may return 'sub', 'email', 'eppn', etc.
                # The debug output shows 'sub' is present, but 'email' is missing despite scope.
                # We prioritize friendly names if available, but fallback to 'sub'.
                user_login = (
                    user_info.get("email")
                    or user_info.get("preferred_username")
                    or user_info.get("sub")
                    or user_info.get("login")
                    or "unknown"
                )
                lqcd_logger.debug(
                    f"DEBUG: Token validation successful. ID: {user_login}, Info: {user_info}"
                )
            except Exception as e:
                lqcd_logger.debug(f"DEBUG: Token validation failed: {e}")

    # Convert user identity to local account
    local_account = get_local_account(user_login)
    response = {"user_id": user_login, "user_account": local_account}
    if local_account is not None:
        # Need to register local account to session manager
        await lqcd_session_manager.register(ctx.session_id, "username", local_account)
        lqcd_logger.info(
            f"Mapped user identity '{user_login}' to local account '{local_account}'."
        )
        ctx.info(
            f"Mapped user identity '{user_login}' to local account '{local_account}'."
        )
    else:
        lqcd_logger.warning(
            f"Could not map user identity '{user_login}' to any local account."
        )
        ctx.error(f"Could not map user identity '{user_login}' to any local account.")
    return response


# Get the backend serve url by server id
@slurm_mcp.tool(
    description="Get the backend serve url by mcp name",
    tags={"slurm", "Jlab"},
)
async def get_backend_url(mcp_name: str) -> cdata.SlurmMcpServer:
    """Get the proxied URL of a backend server by its ID."""
    if _debug:
        all_servers: list[str] = await lqcd_mcp_servers.get_slurm_mcp_server_names()
        lqcd_logger.debug("backend servers ={}".format(all_servers))

    server: cdata.SlurmMcpServer = await lqcd_mcp_servers.get_slurm_mcp_server(mcp_name)

    if server is not None:
        # check the server is really running
        if server.slurm_job_id != -1:
            job_state = await lqcd_slurm_manager.check_slurm_job_state(
                str(server.slurm_job_id)
            )
            if job_state in ["RUNNING", "PENDING"]:
                return server
            else:
                lqcd_logger.info(
                    "Job {} has finished or failed. The mcp server has been removed.".format(
                        server.slurm_job_id
                    )
                )
                await lqcd_mcp_servers.remove_slurm_mcp_server_by_jobid(
                    server.slurm_job_id
                )

    return cdata.SlurmMcpServer(
        mcp_name=mcp_name,
        error_message="Cannot find backend mcp server. Please check the mcp name.",
        valid=False,
    )


# Greeting users
@slurm_mcp.prompt(description="Welcome users")
async def welcome_user(ctx: ServerContext) -> str:
    """A elcome message for the user."""

    sid = ctx.session_id

    user = await lqcd_session_manager.get_resource(sid, "username")

    lqcd_logger.info(f"User '{user}' has connected with session id: {sid}")
    lqcd_logger.info(
        f"Number of active sessions: {len(lqcd_session_manager._sessions)}"
    )

    return f"Welcome, {user}, to the LQCD Analysis MCP Server!"


# Return resource static information
@slurm_mcp.resource("resource://system_info")
async def show_computing_resources(ctx: ServerContext) -> cdata.FullSystemResource:
    total_resource = await lqcd_slurm_manager.get_slurm_info()

    return total_resource


# Return computing resources information
@slurm_mcp.tool(
    description="Provide slurm project account information and computing resources available at Jlab.",
    tags={"slurm", "resource", "Jlab"},
)
async def get_user_slurm_accounts(
    ctx: ServerContext,
) -> cdata.UserComputingAccounts:
    """Return information about user slurm project account names and total computing resources
    auch as partitions, nodes, cpus, memory, etc.
    """
    sid = ctx.session_id

    user = await lqcd_session_manager.get_resource(sid, "username")
    if user is None:
        return cdata.UserComputingAccounts(
            user_name="unknown",
            slurm_accounts=["N/A"],
            valid=False,
            error_message="User not found in session.",
        )

    user_accounts = await lqcd_slurm_manager.get_slurm_accounts(user)

    if len(user_accounts) == 0:
        lqcd_logger.warning("User {} does not have any slurm accounts.".format(user))
        ctx.warning("User {} does not have any slurm accounts.".format(user))
        return cdata.UserComputingAccounts(
            user_name=user,
            slurm_accounts=["N/A"],
            valid=False,
            error_message=f"User {user} does not have any slurm accounts.",
        )

    # Register slurm accounts to this session
    await lqcd_session_manager.register(sid, "slurm_accounts", user_accounts)

    cinfo = cdata.UserComputingAccounts(user_name=user, slurm_accounts=user_accounts)
    return cinfo


# Build a slurm submissition
async def build_slurm_submission(
    sid: str, user: str, mcp_name: str, ctx: ServerContext
) -> cdata.SlurmMcpJob | str | None:
    """Build a slurm submission script"""
    # get everything related to slurm here
    system_info = await lqcd_slurm_manager.get_slurm_info()
    slurm_accounts = await lqcd_slurm_manager.get_slurm_accounts(user)
    if len(slurm_accounts) == 0:
        lqcd_logger.error("User {} does not have any slurm accounts.".format(user))
        ctx.error("User {} does not have any slurm accounts.".format(user))
        return None

    # Register slurm accounts to this session
    await lqcd_session_manager.register(sid, "slurm_accounts", slurm_accounts)

    # ask user to provide script or go through the interactive mode
    result = await ctx.elicit(
        message="Do you want to provide a slurm submission script or go through the interactive mode?",
        response_type=str,
    )

    if result.action == "accept":
        return result.response
    elif result.action == "cancel":
        lqcd_logger.warning(
            "User {} decided not to proceed to launch a mcp server.".format(user)
        )
        ctx.warning(
            "User {} decided not to proceed to launch a mcp server.".format(user)
        )
        return None

    # create an empty slurm job
    slurm_job: cdata.SlurmMcpJob = cdata.SlurmMcpJob()
    slurm_job.account = slurm_accounts[0]
    slurm_job.partition = system_info.partitions[0].partition_name

    # Send message to the client
    result = await ctx.elicit(
        message="Your slurm accounts: {}\nAvailable partitions: {}\n Please provide/modify the following slurm job information:\n {}".format(
            slurm_accounts, system_info, slurm_job
        ),
        response_type=cdata.SlurmMcpJob,
    )

    if result.action == "accept":
        slurm_job = result.data
    else:
        lqcd_logger.warning(
            "User {} decided not to proceed to launch a mcp server.".format(user)
        )
        ctx.warning(
            "User {} decided not to proceed to launch a mcp server.".format(user)
        )
        return None

    # Check run script valid or not
    if not os.path.isfile(slurm_job.run_script):
        lqcd_logger.error(
            "User {} provided script {} does not exist".format(
                user, slurm_job.run_script
            )
        )
        ctx.error(
            "User {} provided script {} does not exist".format(
                user, slurm_job.run_script
            )
        )
        return None

    # Make sure slurm job name is the mcp name
    slurm_job.job_name = mcp_name

    return slurm_job


# Check whether a mcp server has started running and registered
@slurm_mcp.tool(
    description="Check whether a mcp server has started running and been registered",
    tags={"slurm", "Jlab"},
)
async def check_mcp_server_status(
    job_id: int, ctx: ServerContext
) -> cdata.SlurmMcpServer:
    """Check whether a mcp server has started running and been registered."""
    sid = ctx.session_id
    user = await lqcd_session_manager.get_resource(sid, "username")
    if user is None:
        ctx.error("Cannot find username associated with this session.")
        lqcd_logger.error("Cannot find username associated with this session.")
        return cdata.SlurmMcpServer(
            slurm_job_id=job_id,
            error_message="Cannot find username associated with this session.",
            valid=False,
        )

    backend_mcp_server = await lqcd_mcp_servers.get_slurm_mcp_server_by_jobid(job_id)
    lqcd_logger.debug(
        "MCP server returned from job id {} info: {}".format(job_id, backend_mcp_server)
    )
    if backend_mcp_server is None:
        ctx.error("Cannot find mcp server associated with this jobid {}".format(job_id))
        lqcd_logger.error(
            "Cannot find mcp server associated with this jobid {}".format(job_id)
        )
        return cdata.SlurmMcpServer(
            slurm_job_id=job_id,
            error_message="Cannot find mcp server associated with this jobid {}".format(
                job_id
            ),
            valid=False,
        )

    if user != backend_mcp_server.owner:
        ctx.error(
            "User {} does not have permission to check the mcp server status.".format(
                user
            )
        )
        lqcd_logger.error(
            "User {} does not have permission to check the mcp server status.".format(
                user
            )
        )
        return cdata.SlurmMcpServer(
            slurm_job_id=job_id,
            error_message="User {} does not have permission to check the mcp server status.".format(
                user
            ),
            valid=False,
        )

    # Noe check the status of the mcp server
    job_state = await lqcd_slurm_manager.check_slurm_job_state(str(job_id))
    if job_state == "RUNNING" and backend_mcp_server.url == "":
        # Not updating the state
        pass
    else:
        backend_mcp_server.slurm_job_state = job_state

    # Need to take care the cases that job state failed or completed
    # These cases should not happen, but just in case
    if job_state == "PENDING":
        backend_mcp_server.valid = True
        return backend_mcp_server
    elif job_state == "RUNNING":
        # Now the backend server should be running and has been registered
        backend_mcp_server = await lqcd_mcp_servers.get_slurm_mcp_server_by_jobid(
            job_id
        )
        # Make sure the backend server has been registered
        while backend_mcp_server.slurm_job_state != "RUNNING":
            await asyncio.sleep(2)
            backend_mcp_server = await lqcd_mcp_servers.get_slurm_mcp_server_by_jobid(
                job_id
            )

        # This should never happen
        if backend_mcp_server is None:
            ctx.error(
                "Cannot find mcp server associated with this jobid {}".format(job_id)
            )
            lqcd_logger.error(
                "Cannot find mcp server associated with this jobid {}".format(job_id)
            )
            return cdata.SlurmMcpServer(
                slurm_job_id=job_id,
                error_message="Cannot find mcp server associated with this jobid {}".format(
                    job_id
                ),
                valid=False,
            )
    else:
        await lqcd_mcp_servers.remove_slurm_mcp_server_by_jobid(job_id)
        ctx.error(
            "Job {} has finished or failed. The mcp server has been removed.".format(
                job_id
            )
        )
        lqcd_logger.error(
            "Job {} has finished or failed. The mcp server has been removed.".format(
                job_id
            )
        )
        return cdata.SlurmMcpServer(
            slurm_job_id=job_id,
            error_message="Job {} has finished or failed. The mcp server has been removed.".format(
                job_id
            ),
            valid=False,
        )

    backend_mcp_server.valid = True
    return backend_mcp_server


# Check whether there is idle computing nodes for the client
@slurm_mcp.tool(
    description="""
    Return the number of idle computing nodes at Jlab so the client can decide whether to 
    launch an mcp server by submitting a slurm job.
    """,
    tags={"slurm", "Jlab"},
)
async def number_of_idle_nodes(ctx: ServerContext) -> int:
    """Return the number of idle computing nodes at Jlab so the client can decide
    whether to launch an mcp server by submitting a slurm job."""
    system_info = await lqcd_slurm_manager.get_slurm_info()
    idle_nodes: int = 0
    for partition in system_info.partitions:
        idle_nodes += partition.num_free_nodes
    return idle_nodes


# Submit an mcp server as a slurm job with a given slurm script
async def submit_mcp_server_as_slurm_job(
    user: str,
    mcp_name: str,
    submission_script: str,
    backend_mcp_server: cdata.SlurmMcpServer,
    ctx: ServerContext,
) -> cdata.SlurmMcpServer:
    "Submit an mcp server as a slurm job with a given slurm script."

    # submit job using the script
    gid = pwd.getpwnam(user).pw_gid  # get the group id of the user
    jobid = await lqcd_slurm_manager.submit_slurm_job(
        user, gid, mcp_name, submission_script
    )
    if jobid is None:
        lqcd_logger.error("Failed to submit a slurm job.")
        ctx.error("Failed to submit a slurm job.")
        backend_mcp_server.error_message = "Failed to submit a slurm job."
        return backend_mcp_server

    lqcd_logger.info("User {} submitted a slurm job: {}".format(user, jobid))
    ctx.info("User {} submitted a slurm job: {}".format(user, jobid))

    # update jobid and name
    backend_mcp_server.slurm_job_id = int(jobid)
    backend_mcp_server.owner = user

    # update job name
    job_info = await lqcd_slurm_manager.get_slurm_job_info(jobid)
    if job_info is None:
        lqcd_logger.error("Failed to get slurm job info.")
        ctx.error("Failed to get slurm job info.")
        backend_mcp_server.error_message = "Failed to get slurm job info."
        return backend_mcp_server

    backend_mcp_server.slurm_job_name = job_info.job_name
    backend_mcp_server.mcp_name = job_info.job_name

    # Now we need to check the job status
    job_status = await lqcd_slurm_manager.check_slurm_job_state(jobid)
    if job_status is None:
        lqcd_logger.error("Failed to get slurm job status.")
        ctx.error("Failed to get slurm job status.")
        backend_mcp_server.error_message = "Failed to get slurm job status."
        return backend_mcp_server

    backend_mcp_server.slurm_job_state = job_status

    lqcd_logger.debug("Update backend server status to {}".format(job_status))

    if job_status == "RUNNING":
        lqcd_logger.info("User {} submitted a slurm job: {}".format(user, jobid))
        ctx.info("User {} submitted a slurm job: {}".format(user, jobid))
        # check whether the backend server with this jobid is registered
        backend_mcp_server = await lqcd_mcp_servers.get_slurm_mcp_server_by_jobid(jobid)
        while backend_mcp_server is None:
            await asyncio.sleep(2)
            backend_mcp_server = await lqcd_mcp_servers.get_slurm_mcp_server_by_jobid(
                jobid
            )

        if backend_mcp_server is not None:
            lqcd_logger.info(
                "User {} registered a backend mcp server {}".format(
                    user, backend_mcp_server
                )
            )
            ctx.info(
                "User {} registered a backend mcp server {}".format(
                    user, backend_mcp_server
                )
            )
            backend_mcp_server.valid = True

    return backend_mcp_server


# Launch a mcp server on slurm or cloud and return the backend server information
async def launch_mcp_server_on_slurm_or_cloud(
    mcp_name: str,
    ctx: ServerContext,
) -> cdata.SlurmMcpServer:
    "User launch a mcp server on slurm or cloud."

    # empty backend mcp server
    backend_mcp_server = cdata.SlurmMcpServer(
        mcp_name=mcp_name,
        url="",
        owner="",
        slurm_job_id=-1,
        slurm_job_name=mcp_name,
        slurm_job_state="UNKNOWN",
        error_message="",
        valid=False,
    )
    # get session id
    sid = ctx.session_id

    # Get user name
    user = await lqcd_session_manager.get_resource(sid, "username")
    if user is None:
        ctx.error("Cannot find username associated with this session.")
        lqcd_logger.error("Cannot find username associated with this session.")
        backend_mcp_server.error_message = (
            "Cannot find username associated with this session."
        )
        return backend_mcp_server

    lqcd_logger.info("User {} is submitting a mcp analysis server.".format(user))

    # Build slurm job by the client
    slurm_job = await build_slurm_submission(sid, user, mcp_name, ctx)
    if slurm_job is None:
        backend_mcp_server.error_message = (
            "User {} decided not to proceed to launch a mcp server.".format(user)
        )
        return backend_mcp_server

    # Build submission script
    if isinstance(slurm_job, cdata.SlurmMcpJob):
        submission_script = lqcd_slurm_manager.build_slurm_job_script(user, slurm_job)
    elif isinstance(slurm_job, str):
        submission_script = slurm_job
    else:
        ctx.error("Invalid slurm job.")
        lqcd_logger.error("Invalid slurm job.")
        backend_mcp_server.error_message = "Invalid slurm job."
        return backend_mcp_server

    lqcd_logger.debug(
        "User {} create a slurm submissoin script:\n{}".format(user, submission_script)
    )

    # Submit the job
    mcp_server = await submit_mcp_server_as_slurm_job(
        user, submission_script, backend_mcp_server, ctx
    )
    return mcp_server


# Post process after an mcp slurm server is submitted
async def epilogue_mcp_slurm_server(
    backend_mcp_server: cdata.SlurmMcpServer,
    wait: bool,
    mcp_name: str,
    ctx: ServerContext,
) -> cdata.SlurmMcpServer:
    "Post process after an mcp slurm server is submitted."
    # Get user name
    user = await lqcd_session_manager.get_resource(ctx.session_id, "username")
    # backend_server job id is an integer
    job_id_str = str(backend_mcp_server.slurm_job_id)
    job_id = backend_mcp_server.slurm_job_id

    lqcd_logger.info(
        "User {} has slurm jobid {} and job name {} in state {}.".format(
            user,
            job_id_str,
            backend_mcp_server.slurm_job_name,
            backend_mcp_server.slurm_job_state,
        )
    )
    if backend_mcp_server.slurm_job_state == "RUNNING":
        return backend_mcp_server
    elif backend_mcp_server.slurm_job_state == "PENDING":
        # Wait for the job status to change to running
        if wait:
            while backend_mcp_server.slurm_job_state != "RUNNING":
                await asyncio.sleep(2)
                job_status = await lqcd_slurm_manager.check_slurm_job_state(job_id_str)
                if job_status is None:
                    lqcd_logger.error("Failed to get slurm job status.")
                    ctx.error("Failed to get slurm job status.")
                    backend_mcp_server.error_message = "Failed to get slurm job status."
                    return backend_mcp_server
                else:
                    lqcd_logger.info(
                        "User {} mcp slurm job with job id {} and job name {} is in state {}".format(
                            user,
                            job_id_str,
                            backend_mcp_server.slurm_job_name,
                            job_status,
                        )
                    )
                    ctx.info(
                        "User {} mcp slurm job with job id {} and job name {} is in state {}".format(
                            user,
                            job_id_str,
                            backend_mcp_server.slurm_job_name,
                            job_status,
                        )
                    )
                backend_mcp_server.slurm_job_state = job_status

            # Now wait for the backend server to be registered
            backend_mcp_server = None
            while backend_mcp_server is None:
                await asyncio.sleep(2)
                backend_mcp_server = (
                    await lqcd_mcp_servers.get_slurm_mcp_server_by_jobid(job_id)
                )
            # Now the backend server is registered
            lqcd_logger.info(
                "User {} registered a backend mcp server {}".format(
                    user, backend_mcp_server
                )
            )
            ctx.info(
                "User {} registered a backend mcp server {}".format(
                    user, backend_mcp_server
                )
            )
        else:
            job_status = backend_mcp_server.slurm_job_state
            lqcd_logger.info(
                "User {} mcp slurm job with job id {} and job name {} is in state {}".format(
                    user,
                    job_id_str,
                    backend_mcp_server.slurm_job_name,
                    job_status,
                )
            )
            ctx.info(
                "User {} mcp slurm job with job id {} and job name {} is in state {}, check back later.".format(
                    user,
                    job_id_str,
                    backend_mcp_server.slurm_job_name,
                    job_status,
                )
            )
            # register the backend server
            await lqcd_mcp_servers.add_slurm_mcp_server(backend_mcp_server)
    else:
        lqcd_logger.error("Surm job {} has status {}".format(job_id_str, job_status))
        ctx.error("Surm job {} has status {}".format(job_id_str, job_status))
        backend_mcp_server.error_message = "Surm job {} has status {}".format(
            job_id_str, job_status
        )

    # return the backend mcp server
    return backend_mcp_server


# Launch MCP Server on Slurm or Cloud
# Now we just return slurm backend server information
@slurm_mcp.tool(
    description="""
    Launch an MCP Server on Slurm cluster at Jlab. 
    The server will be registered with the Proxy server.
    """,
    tags={"slurm", "Jlab"},
)
async def launch_mcp_server(
    mcp_name: str, wait: bool, ctx: ServerContext
) -> cdata.SlurmMcpServer:
    "User launch a mcp server on slurm or cloud."

    # submitted backend mcp server
    backend_mcp_server: cdata.SlurmMcpServer = (
        await launch_mcp_server_on_slurm_or_cloud(mcp_name, ctx)
    )
    # post process
    mcp_server: cdata.SlurmMcpServer = await epilogue_mcp_slurm_server(
        backend_mcp_server, wait, mcp_name, ctx
    )
    return mcp_server


# Launch MCP Server on Slurm using a complete slurm submission script
# Now we just return slurm backend server information
@slurm_mcp.tool(
    description="""
    Launch an MCP Server on Slurm cluster at Jlab using a complete slurm submission script. 
    The MCP name is the same as the job name which is specified inside the script.
    The server will be registered with the Proxy server.
    """,
    tags={"slurm", "Jlab"},
)
async def launch_mcp_server_on_slurm_with_script(
    mcp_name: str, wait: bool, submission_script: str, ctx: ServerContext
) -> cdata.SlurmMcpServer:
    """User launch a mcp server on slurm using a complete slurm submission script."""
    # empty backend mcp server
    backend_mcp_server = cdata.SlurmMcpServer(
        mcp_name="N/A",
        url="",
        owner="",
        slurm_job_id=-1,
        slurm_job_name="N/A",
        slurm_job_state="UNKNOWN",
        error_message="",
        valid=False,
    )

    # get session id
    sid = ctx.session_id

    # Get user name
    user = await lqcd_session_manager.get_resource(sid, "username")
    if user is None:
        ctx.error("Cannot find username associated with this session.")
        lqcd_logger.error("Cannot find username associated with this session.")
        backend_mcp_server.error_message = (
            "Cannot find username associated with this session."
        )
        return backend_mcp_server

    lqcd_logger.info("User {} is submitting a mcp analysis server.".format(user))

    lqcd_logger.debug(
        "User {} is submitting a mcp server with submission script {}.".format(
            user, submission_script
        )
    )
    # submit the job
    submitted_mcp_server = await submit_mcp_server_as_slurm_job(
        user, mcp_name, submission_script, backend_mcp_server, ctx
    )

    lqcd_logger.debug(
        "User {} submitted a mcp server with job id {} and job name {}.".format(
            user,
            submitted_mcp_server.slurm_job_id,
            submitted_mcp_server.slurm_job_name,
        )
    )

    if mcp_name != submitted_mcp_server.mcp_name:
        lqcd_logger.warning(
            "User {} submitted a mcp server with job id {} and job name {} does not match the requested mcp name {}.".format(
                user,
                submitted_mcp_server.slurm_job_id,
                submitted_mcp_server.slurm_job_name,
                mcp_name,
            )
        )
        ctx.warning(
            "User {} submitted a mcp server with job id {} and job name {} does not match the requested mcp name {}.".format(
                user,
                submitted_mcp_server.slurm_job_id,
                submitted_mcp_server.slurm_job_name,
                mcp_name,
            )
        )
        mcp_name = submitted_mcp_server.mcp_name

    # post process
    mcp_server = await epilogue_mcp_slurm_server(
        submitted_mcp_server, wait, mcp_name, ctx
    )
    return mcp_server


# Launch MCP Server on Slurm using a complete slurm submission file that
# is available on the machine where the proxy server is running.
# Now we just return slurm backend server information
@slurm_mcp.tool(
    description="""
    Launch an MCP Server on Slurm cluster at Jlab using a complete slurm submission file
    which is available on the machine where the proxy server is running. 
    The MCP name is the same as the job name which is specified inside the script.
    The server will be registered with the Proxy server.
    """,
    tags={"slurm", "Jlab"},
)
async def launch_mcp_server_on_slurm_with_script_file(
    mcp_name: str, wait: bool, script_file: str, ctx: ServerContext
) -> cdata.SlurmMcpServer:
    """User launch a mcp server on slurm using a complete slurm submission file."""

    # empty backend mcp server
    backend_mcp_server = cdata.SlurmMcpServer(
        mcp_name="N/A",
        url="",
        owner="",
        slurm_job_id=-1,
        slurm_job_name="N/A",
        slurm_job_state="UNKNOWN",
        error_message="",
        valid=False,
    )

    # Check file is available and readable
    if not os.path.exists(script_file):
        ctx.error("Script file {} does not exist.".format(script_file))
        lqcd_logger.error("Script file {} does not exist.".format(script_file))
        backend_mcp_server.error_message = "Script file {} does not exist.".format(
            script_file
        )
        return backend_mcp_server

    if not os.access(script_file, os.R_OK):
        ctx.error("Script file {} is not readable.".format(script_file))
        lqcd_logger.error("Script file {} is not readable.".format(script_file))
        backend_mcp_server.error_message = "Script file {} is not readable.".format(
            script_file
        )
        return backend_mcp_server

    # get session id
    sid = ctx.session_id

    # Get user name
    user = await lqcd_session_manager.get_resource(sid, "username")
    if user is None:
        ctx.error("Cannot find username associated with this session.")
        lqcd_logger.error("Cannot find username associated with this session.")
        backend_mcp_server.error_message = (
            "Cannot find username associated with this session."
        )
        return backend_mcp_server

    # Read the script file
    with open(script_file, "r") as f:
        submission_script = f.read()

    lqcd_logger.info("User {} is submitting a mcp analysis server.".format(user))

    lqcd_logger.debug(
        "User {} is submitting a mcp server with submission script file {}.".format(
            user, script_file
        )
    )
    # submit the job
    submitted_mcp_server = await submit_mcp_server_as_slurm_job(
        user, mcp_name, submission_script, backend_mcp_server, ctx
    )

    if mcp_name != submitted_mcp_server.mcp_name:
        lqcd_logger.warning(
            "User {} submitted a mcp server with job id {} and job name {} does not match the requested mcp name {}.".format(
                user,
                submitted_mcp_server.slurm_job_id,
                submitted_mcp_server.slurm_job_name,
                mcp_name,
            )
        )
        ctx.warning(
            "User {} submitted a mcp server with job id {} and job name {} does not match the requested mcp name {}.".format(
                user,
                submitted_mcp_server.slurm_job_id,
                submitted_mcp_server.slurm_job_name,
                mcp_name,
            )
        )
        mcp_name = submitted_mcp_server.mcp_name

    # post process
    mcp_server = await epilogue_mcp_slurm_server(
        submitted_mcp_server, wait, mcp_name, ctx
    )
    return mcp_server


# Finally main entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MCP Streamable HTTP LQCD Analysis server"
    )
    parser.add_argument(
        "--port", type=int, default=8123, help="Localhost port to listen on"
    )
    args = parser.parse_args()

    # Get startlette app for uvicorn
    batch_mcp_app = slurm_mcp.http_app(
        path="/mcp", json_response=False, stateless_http=False, transport="http"
    )

    uvicorn.run(batch_mcp_app, host="0.0.0.0", port=args.port)
