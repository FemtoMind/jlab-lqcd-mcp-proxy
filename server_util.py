# Back end server utilities
import socket
import os
import pwd
import httpx


# Get hostname and ip address of the local machine
def get_hostname_and_ip_address():
    try:
        # Get the local hostname
        hostname = socket.gethostname()

        # Get the IP address associated with the hostname
        # This may return the loopback address (127.0.0.1) in some environments
        # without proper network configuration
        ip_address = socket.gethostbyname(hostname)

        return hostname, ip_address
    except socket.error as e:
        print(f"Error getting network info: {e}")
        return None, None


# Get process owner
def get_process_owner() -> str | None:
    try:
        # Get the real user ID of the current process
        uid = os.getuid()
        # Look up the username associated with that UID
        user_info = pwd.getpwuid(uid)
        return user_info.pw_name
    except Exception as e:
        print(f"Error getting process owner: {e}")
        return None


# Register slurm mcp server to slurm manager
# This function is called by mcp server launched as a slurm job
async def register_mcp_slurm_server(
    proxy_url: str, mcp_name: str, server_url: str
) -> dict | None:
    owner: str | None = get_process_owner()
    if owner is None:
        print("Error getting process owner")
        return None
    try:
        job_id: str | None = os.getenv("SLURM_JOB_ID")
        job_name: str | None = os.getenv("SLURM_JOB_NAME")
        if job_id is None or job_name is None:
            print("Error getting slurm job id or job name")
            return None

        if job_name != mcp_name:
            print(
                f"Error: SLURM_JOB_NAME '{job_name}' does not match mcp_name '{mcp_name}'"
            )
            return None

        # Send a POST request to the proxy server
        print(
            f"Registering slurm mcp server '{mcp_name}' to slurm manager at {proxy_url}..."
        )
        response = httpx.post(
            f"{proxy_url}/server",
            params={
                "mcp_name": mcp_name,
                "server_url": server_url,
                "owner": owner,
                "job_id": job_id,
                "job_name": job_name,
            },
        )
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as e:
        print(f"Error registering slurm mcp server: {e}")
        return None
