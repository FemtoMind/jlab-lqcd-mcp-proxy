# Data used across analysis MCP server and client
import json
import enum
import re
from pydantic import BaseModel, Field
from datetime import timedelta

from dataclasses import dataclass


# convert time string to minutes (either in format "D-HH:MM:SS" or "HH:MM:SS")
def _convert_time_str_to_minutes(time_str) -> int:
    try:
        # Split the string into days and the rest of the time (HH:MM:SS)
        if "-" in time_str:
            days_str, time_part = time_str.split("-")
            days = int(days_str)
        else:
            days = 0

        # Split the time part into hours, minutes, and seconds
        h, m, s = map(int, time_part.split(":"))

        # Create a timedelta object
        delta = timedelta(days=days, hours=h, minutes=m, seconds=s)

        # Get the total seconds and convert to minutes
        total_minutes = delta.total_seconds() // 60
        return total_minutes
    except ValueError:
        raise ValueError("Invalid input format. Use 'D-HH:MM:SS' or 'HH:MM:SS'.")


# Full system resource information
class FullSystemResource(BaseModel):
    """Model to represent full system resource information."""

    class _partition_resource(BaseModel):
        partition_name: str = Field(
            description="Name of the compute partition.", default=""
        )
        type: str = Field(
            description="Type of the partition (e.g., GPU, CPU).", default="CPU"
        )
        num_nodes: int = Field(
            description="Total Number of compute nodes available.", default=0
        )
        num_free_nodes: int = Field(
            description="Number of free compute nodes.", default=0
        )
        num_busy_nodes: int = Field(
            description="Number of busy compute nodes.", default=0
        )
        num_other_nodes: int = Field(
            description="Number of compute nodes in other states.", default=0
        )
        num_cpus_per_node: int = Field(
            description="Number of CPUs per compute node.", default=0
        )
        memory_per_node_gb: float = Field(
            description="Memory per compute node in GB.", default=0.0
        )
        generic_resource: str = Field(description="Generic resource.", default="")
        default_wall_time: str = Field(
            description="Default wall time for a job.", default=""
        )
        wall_time_limit: str = Field(description="Wall time limit.", default="")

    partitions: list[_partition_resource] = Field(
        description="List of partitions and their resource information.", default=[]
    )

    # Convert to nice string format for display
    def __str__(self):
        total_str = ""
        for p in self.partitions:
            pstr = "\n".join(
                [
                    f"Partition: {p.partition_name}",
                    f"\tType: {p.type}",
                    f"\tNumber of nodes: {p.num_nodes}",
                    f"\tNumber of free nodes: {p.num_free_nodes}",
                    f"\tNumber of busy nodes: {p.num_busy_nodes}",
                    f"\tNumber of other nodes: {p.num_other_nodes}",
                    f"\tNumber of CPUs per node: {p.num_cpus_per_node}",
                    f"\tMemory per node: {p.memory_per_node_gb} GB",
                    f"\tGeneric resource: {p.generic_resource}",
                    f"\tDefault wall time: {p.default_wall_time}",
                    f"\tWall time limit: {p.wall_time_limit}",
                ]
            )
            total_str += pstr
        return total_str

    # Convert to json string
    def to_json(self):
        return json.dumps(self.dict())

    # Convert from json string
    @staticmethod
    def from_json(json_str: str):
        return FullSystemResource(**json.loads(json_str))

    # get partition names in a list
    def get_partition_names(self):
        return [p.partition_name for p in self.partitions]

    # get partition resources in a list
    def get_partition_resources(self):
        return [p for p in self.partitions]


# Slurm job information
class SlurmJobInfo(BaseModel):
    job_id: int = Field(description="ID of the job.")
    job_name: str = Field(description="Name of the job.")
    job_state: str = Field(description="State of the job.")
    job_start_time: str = Field(description="Start time of the job.", default="")
    job_wall_or_end_time: str = Field(
        description="Wall time or end time of the job.", default=""
    )
    job_nodes: str = Field(description="Nodes of the job.", default="")

    # Convert to nice string format for display
    def __str__(self):
        if self.job_state == "RUNNING":
            return "\n".join(
                [
                    f"Job ID: {self.job_id}",
                    f"\tJob Name: {self.job_name}",
                    f"\tJob State: {self.job_state}",
                    f"\tJob Start Time: {self.job_start_time}",
                    f"\tJob Wall Time: {self.job_wall_or_end_time}",
                    f"\tJob on Nodes: {self.job_nodes}",
                ]
            )
        elif self.job_state == "PENDING":
            return "\n".join(
                [
                    f"Job ID: {self.job_id}",
                    f"\tJob Name: {self.job_name}",
                    f"\tJob State: {self.job_state}",
                    f"\tJob Wall Time: {self.job_wall_or_end_time}",
                ]
            )
        elif self.job_state == "COMPLETED":
            return "\n".join(
                [
                    f"Job ID: {self.job_id}",
                    f"\tJob Name: {self.job_name}",
                    f"\tJob State: {self.job_state}",
                    f"\tJob Start Time: {self.job_start_time}",
                    f"\tJob End Time: {self.job_wall_or_end_time}",
                ]
            )
        else:
            return "\n".join(
                [
                    f"Job ID: {self.job_id}",
                    f"\tJob Name: {self.job_name}",
                    f"\tJob State: {self.job_state}",
                    f"\tJob Start Time: {self.job_start_time}",
                    f"\tJob Wall or End Time: {self.job_wall_or_end_time}",
                    f"\tJob Nodes: {self.job_nodes}",
                ]
            )

    # Convert to json string
    def to_json(self):
        return json.dumps(self.dict())

    # Convert from json string
    @staticmethod
    def from_json(json_str: str):
        return SlurmJobInfo(**json.loads(json_str))


# static computing resource information derived from the above
class SlurmConfigInfo(BaseModel):
    """Static computing resource information derived from the full system resource information."""

    class PartitionConfigInfo(BaseModel):
        partition_name: str = Field(description="Name of the partition.")
        generic_resource: str = Field(description="Generic resource.")
        default_wall_time: str = Field(description="Default wall time in minutes.")
        wall_time_limit: str = Field(description="Wall time limit in minutes.")

    partitions: dict[str, PartitionConfigInfo] = Field(
        description="Dictionary of partitions and their configuration information.",
        default={},
    )

    def update(self, other: FullSystemResource):
        for p in other.partitions:
            config: SlurmConfigInfo.PartitionConfigInfo = (
                SlurmConfigInfo.PartitionConfigInfo(
                    partition_name=p.partition_name,
                    generic_resource=p.generic_resource,
                    default_wall_time=p.default_wall_time,
                    wall_time_limit=p.wall_time_limit,
                )
            )
            self.partitions[p.partition_name] = config


# User Cloud computing resource information
class UserComputingAccounts(BaseModel):
    """Data to report to user about available cloud computing resource."""

    user_name: str = Field(description="Name of the user.")

    slurm_accounts: list[str] = Field(
        min_length=1, description="Available slurm accounts"
    )

    valid: bool = Field(description="Is valid", default=True)
    error_message: str = Field(description="Error message", default="")

    def to_json(self):
        return json.dumps(self.dict())

    @staticmethod
    def from_json(json_str: str) -> "UserComputingAccounts":
        return UserComputingAccounts(**json.loads(json_str))


# define data class for authentication information
class OIDCAuthInfo(BaseModel):
    device_authorization_url: str | None = Field(
        description="Device Authorization URL", default="")
    token_url: str = Field(description="Token Retrieval URL")
    token_verify_url: str = Field(description="Token Verification URL")
    client_id: str = Field(description="Client ID")
    client_secret: str = Field(description="Client Secret")
    scope: str = Field(description="Scope")
    grant_type: str = Field(description="Grant Type")
    provider: str = Field(description="Provider name")
    authorization_url: str | None = Field(
        description="Authorization URL for Auth Code Flow", default=""
    )
    redirect_uri: str | None = Field(
        description="Redirect URI for Auth Code Flow", default=""
    )



# Slurm mcp server information
class SlurmMcpServer(BaseModel):
    mcp_name: str = Field(description="Slurm MCP server name", default="UNKNOWN")
    url: str = Field(description="Slurm MCP server URL", default="")
    owner: str = Field(description="Slurm MCP server owner", default="UNKNOWN")
    slurm_job_id: int = Field(description="Slurm MCP server job ID", default=-1)
    slurm_job_name: str = Field(description="Slurm MCP server job name", default="")
    slurm_job_state: str = Field(
        description="Slurm MCP server job state", default="UNKNOWN"
    )
    error_message: str = Field(description="Error message", default="N/A")
    valid: bool = Field(description="Valid", default=True)

    @staticmethod
    def from_json(json_str: str) -> "SlurmMcpServer":
        json_data = json.loads(json_str)
        return SlurmMcpServer(**json_data)

    def to_json(self):
        return json.dumps(self.dict())


# Slurm job of MCP server
class SlurmMcpJob(BaseModel):
    account: str = Field(description="Slurm account")
    partition: str = Field(description="Slurm partition")
    name: str = Field(description="Slurm job name is also the MCP name")
    run_script: str = Field(description="Slurm run script")
    nodes: int = Field(description="Number of nodes", default=1)
    ntasks: int = Field(description="Number of tasks", default=4)
    cpus_per_task: int = Field(description="Number of cpus per task", default=16)
    time: str = Field(description="Time limit", default="04:00:00")
    memory: str = Field(description="Memory", default="")
    output: str = Field(description="Output", default=".mcp")

    @staticmethod
    def from_json(json_str: str) -> "SlurmMcpJob":
        json_data = json.loads(json_str)
        return SlurmMcpJob(**json_data)

    def to_json(self):
        return json.dumps(self.dict())

    def __str__(self):
        return "\n".join(
            [
                f"Account: {self.account}",
                f"Partition: {self.partition}",
                f"Name: {self.name}",
                f"Time: {self.time}",
                f"Nodes: {self.nodes}",
                f"Ntasks: {self.ntasks}",
                f"Cpus per task: {self.cpus_per_task}",
                f"Memory: {self.memory}",
                f"Output: {self.output}",
                f"Run script: {self.run_script}",
            ]
        )


# Slurm script file information
class SlurmMcpScriptFile(BaseModel):
    filename: str = Field(description="Slurm script full file path name")
    local: bool = Field(description="Local file or not", default=True)
    content: str = Field(description="Slurm script content", default="")

    @staticmethod
    def from_json(json_str: str) -> "SlurmMcpScriptFile":
        json_data = json.loads(json_str)
        return SlurmMcpScriptFile(**json_data)

    def to_json(self):
        return json.dumps(self.dict())

    def __str__(self):
        return "\n".join(
            [
                f"Filename: {self.filename}",
                f"Local: {self.local}",
                f"Content: {self.content}",
            ]
        )


# Slurm job cancel status
class SlurmJobCancelStatus(BaseModel):
    class _status_value(enum.Enum):
        UNKNOWN = "UNKNOWN"
        SUCCESS = "SUCCESS"
        CANCELED = "CANCELED"
        NOT_FOUND = "NOT_FOUND"
        FAILED = "FAILED"
        NOT_OWNER = "NOT_OWNER"

    job_id: str = Field(description="Slurm job id in string format")
    status: _status_value = Field(description="Slurm job cancel status")
    error_message: str = Field(description="Error message", default="")

    @staticmethod
    def from_json(json_str: str) -> "SlurmJobCancelStatus":
        json_data = json.loads(json_str)
        return SlurmJobCancelStatus(**json_data)

    def to_json(self):
        return json.dumps(self.dict())

    def __str__(self):
        return "\n".join(
            [
                f"Job id: {self.job_id}",
                f"Status: {self.status}",
                f"Error message: {self.error_message}",
            ]
        )


# Verify input time format (hh:mm:ss)
def time_interval_valid(input: str) -> bool:
    pattern = re.compile(r"[0-4][0-8]|[1-9]:[0-5][0-9]:[0-5][0-9]$")
    return pattern.match(input)


# check a string is an integer
def is_integer(s: str) -> bool:
    try:
        int(s)
        return True
    except ValueError:
        return False
