# OIDC Server side authentication for LQCD MCP servers
import httpx
import requests
import os
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse

# lqcd logger
from lqcd_logger import lqcd_logger

# import common data models
from common_data import OIDCAuthInfo

# Global authentication information
__auth_info: OIDCAuthInfo = None

# Read a json file pointed by en env variable
# to get authentication information and convert to OIDCAuthInfo
def read_oidc_auth_info():
    global __auth_info
    file_path = os.getenv("LQCDMCP_OIDC_INFO_FILE", None)
    if file_path is None:
        lqcd_logger.error("Error: LQCDMCP_OIDC_INFO_FILE is not set.")
        __auth_info = None
        return

    try:
        with open(file_path, "r") as f:
            auth_info = json.load(f)
    except FileNotFoundError:
        lqcd_logger.error(f"Error: The file '{file_path}' was not found.")
        __auth_info = None
        return
    except IOError as e:
        lqcd_logger.error(f"Error reading file: {e}")
        __auth_info = None
        return

    try:
        info: OIDCAuthInfo = OIDCAuthInfo(**auth_info)
    except ValueError as e:
        lqcd_logger.error(f"Error parsing JSON: {e}")
        __auth_info = None
        return
        
    # Finally set the global auth info
    __auth_info = info
    lqcd_logger.info("OIDC authentication information loaded successfully.")


# Load user account mapping from a JSON file
__user_account_mapping: dict[str, str] = {}
def load_user_account_mapping():
    global __user_account_mapping
    file_path = os.getenv("LQCDMCP_USERID_MAP_FILE", None)
    if file_path is None:
        lqcd_logger.error("Error: LQCDMCP_USERID_MAP_FILE is not set.")
        return {}

    try:
        with open(file_path, "r") as f:
            __user_account_mapping = json.load(f)
    except Exception as e:
        lqcd_logger.error(f"Error loading user account mapping: {e}")
        return {}
    lqcd_logger.info("User account mapping loaded successfully.")
    
# Look up local account by user id
def get_local_account(user_id: str) -> str | None:
    return __user_account_mapping.get(user_id, None)

# Helper function to validate OIDC token
def validate_authorized_token(token):
    """
    Validates an  OIDC token by attempting a simple API request.

    Args:
        token (str): The OIDC token to validate.

    Returns:
        bool: True if the token is valid, False otherwise.
        dict or None: User data if valid, error info if invalid.
    """
    # Use a simple, low-permission endpoint like 'user' or 'octocat'
    url = __auth_info.token_verify_url

    headers = {
        "Authorization": f"Bearer {token}",  # Use Bearer for fine-grained PATs and JWTs
    }

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            print(f"DEBUG: UserInfo Response: {response.text}")
            print("Token is valid and active.")
            return True, response.json()
        elif response.status_code == 401:
            print(
                f"Token is invalid or expired: {response.json().get('message', 'Unauthorized')}"
            )
            return False, response.json()
        elif response.status_code == 403:
            # A 403 can mean insufficient scope or rate limiting
            message = response.json().get("message", "Forbidden")
            print(
                f"Token is valid but lacks sufficient permissions or is rate-limited: {message}"
            )
            return False, response.json()
        else:
            print(
                f"An unexpected error occurred. Status code: {response.status_code}, Message: {response.text}"
            )
            return False, {
                "status_code": response.status_code,
                "message": response.text,
            }

    except requests.exceptions.RequestException as e:
        print(f"A connection error occurred: {e}")
        return False, None
    
# Use fastapi router to define a router to handle authentication here
from fastapi import APIRouter
auth_router = APIRouter()

@auth_router.post("/auth/device-code")
async def start_auth_device_flow():
    """Step 1: Proxy requests verification codes from OIDC provider."""
    if __auth_info is None:
        return None

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            __auth_info.device_authorization_url,
            data={
                "client_id": __auth_info.client_id,
                "client_secret": __auth_info.client_secret,
                "scope": __auth_info.scope,
            },
            headers={"Accept": "application/json"},
        )
        return resp.json()

@auth_router.post("/auth/poll")
async def poll_auth_token(device_code: str):
    if __auth_info is None:
        return None

    """Step 2: Proxy polls OIDC provider for the access token using its Secret."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            __auth_info.token_url,
            data={
                "client_id": __auth_info.client_id,
                "client_secret": __auth_info.client_secret,
                "device_code": device_code,
                "grant_type": __auth_info.grant_type,
            },
            headers={"Accept": "application/json"},
        )
        return resp.json()



