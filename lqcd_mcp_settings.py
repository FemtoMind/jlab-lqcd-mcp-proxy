# Settings for both server and client
import os
from dotenv import find_dotenv, load_dotenv

is_fastmcp_version_3: bool = False


def find_server_env_file(custom_env_file: str | None = None) -> str | None:
    """
    Locate .server_env file in the following order:
    1. custom_env_file (if provided and exists)
    2. Local source directory (.server_env) - for testing / dev
    3. Production etc directory (../etc/.server_env) - for production mode
    4. Parent directories using find_dotenv ('.server_env' or 'etc/.server_env')
    """
    if custom_env_file and os.path.isfile(custom_env_file):
        return os.path.abspath(custom_env_file)

    current_dir = os.path.dirname(os.path.abspath(__file__))

    candidates = [
        os.path.join(current_dir, ".server_env"),
        os.path.abspath(os.path.join(current_dir, "..", "etc", ".server_env")),
    ]

    for candidate in candidates:
        if os.path.isfile(candidate):
            return candidate

    found = find_dotenv(".server_env") or find_dotenv("etc/.server_env")
    if found and os.path.isfile(found):
        return os.path.abspath(found)

    return None


def load_server_env(
    override: bool = True, custom_env_file: str | None = None
) -> str | None:
    """
    Locate and load .server_env into environment variables.
    Returns the resolved path if loaded, or None if not found.
    """
    env_file = find_server_env_file(custom_env_file)
    if env_file:
        load_dotenv(env_file, override=override)
    return env_file

