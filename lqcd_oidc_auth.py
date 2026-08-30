# OIDC Server side authentication for LQCD MCP servers
import httpx
import requests
import os
import json
import time
import globus_sdk
from typing import Any
from fastapi import FastAPI, Request, HTTPException, Body
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse

# lqcd logger
from lqcd_logger import lqcd_logger

# import common data models
from common_data import OIDCAuthInfo

# Global authentication information
__auth_info: OIDCAuthInfo | None = None


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


def validate_globus_rs_token(token: str) -> tuple[bool, dict | None]:
    """
    Validates a Globus token using Resource Server (RS) Introspection.
    Returns (True, user_info) if valid, or (False, error_dict) if invalid/not configured.
    """
    if __auth_info is None:
        return False, None
    rs_id = __auth_info.globus_rs_id
    rs_secret = __auth_info.globus_rs_secret
    scope_suffix = __auth_info.globus_rs_scope_suffix
    
    if not rs_id or not rs_secret:
        lqcd_logger.info("Globus RS ID or Secret is not set.")
        return False, None

    try:
        client = globus_sdk.ConfidentialAppAuthClient(rs_id, rs_secret)

        introspect = client.oauth2_token_introspect(
            token, include="identity_set_detail,session_info"
        )

        if not introspect.get("active"):
            lqcd_logger.warning("Globus RS token is not active")
            return False, {"message": "Globus RS token is inactive"}

        exp = introspect.get("exp")
        if exp and time.time() >= exp:
            lqcd_logger.warning("Globus RS token has expired")
            return False, {"message": "Globus RS token has expired"}

        nbf = introspect.get("nbf")
        if nbf and time.time() < nbf:
            lqcd_logger.warning("Globus RS token not yet valid")
            return False, {"message": "Globus RS token not yet valid"}

        token_scopes = introspect.get("scope", "").split()
        expected_scope = f"https://auth.globus.org/scopes/{rs_id}/{scope_suffix}" if scope_suffix else None
        if expected_scope and expected_scope not in token_scopes:
            lqcd_logger.warning(
                f"Globus RS token missing required scope: {expected_scope}"
            )
            return False, {"message": f"Token missing scope: {expected_scope}"}

        # Extract user identity
        user_identity = introspect.get("username")
        session_info = introspect.get("session_info", {})
        authentications = session_info.get("authentications", {})
        if not user_identity and authentications:
            first_auth = next(iter(authentications.values()), {})
            user_identity = first_auth.get("email") or first_auth.get("username")
        if not user_identity:
            user_identity = introspect.get("sub")

        if user_identity:
            user_info = {
                "username": user_identity,
                "email": user_identity if "@" in user_identity else None,
                "sub": introspect.get("sub"),
                "preferred_username": user_identity,
                "globus_introspect": introspect,
            }
            lqcd_logger.info(f"Globus RS Token is valid for identity: {user_identity}")
            return True, user_info

        return False, {"message": "Could not identify Globus user from token"}
    except Exception as e:
        lqcd_logger.warning(f"Globus RS Introspection error: {e}")
        return False, None


# Helper function to validate OIDC or Globus RS token
def validate_authorized_token(token: str):
    """
    Validates an OIDC token by trying Globus RS Introspection first (if configured),
    falling back to OIDC userinfo verification.

    Args:
        token (str): The token to validate.

    Returns:
        tuple[bool, dict | None]: (is_valid, user_data_or_error_info)
    """
    token = token.strip()
    if token.startswith("Bearer "):
        token = token[len("Bearer ") :].strip()

    # 1. Try Globus RS Introspection if configured
    is_rs_valid, rs_user_info = validate_globus_rs_token(token)
    if is_rs_valid and rs_user_info:
        return True, rs_user_info

    # 2. Fall back to standard OIDC UserInfo validation
    if __auth_info is None:
        lqcd_logger.error("OIDC authentication information is not loaded.")
        return False, {"message": "OIDC auth info not loaded"}

    # Use a simple, low-permission endpoint like 'user' or 'octocat'
    url = __auth_info.token_verify_url

    headers = {
        "Authorization": f"Bearer {token}",  # Use Bearer for fine-grained PATs and JWTs
        "User-Agent": "jlab-lqcd-mcp-proxy/1.0",
    }

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            lqcd_logger.debug(f"UserInfo Response: {response.text}")
            lqcd_logger.info("Token is valid and active.")
            return True, response.json()
        elif response.status_code == 401:
            lqcd_error_msg = response.json().get("message", "Unauthorized") if response.headers.get("content-type", "").startswith("application/json") else response.text
            lqcd_logger.error(
                f"Token is invalid or expired: {lqcd_error_msg}"
            )
            return False, response.json() if response.headers.get("content-type", "").startswith("application/json") else {"message": response.text}
        elif response.status_code == 403:
            # A 403 can mean insufficient scope or rate limiting
            message = response.json().get("message", "Forbidden") if response.headers.get("content-type", "").startswith("application/json") else response.text
            lqcd_logger.error(
                f"Token is valid but lacks sufficient permissions or is rate-limited: {message}"
            )
            return False, response.json() if response.headers.get("content-type", "").startswith("application/json") else {"message": response.text}
        else:
            lqcd_logger.error(
                f"An unexpected error occurred. Status code: {response.status_code}, Message: {response.text}"
            )
            return False, {
                "status_code": response.status_code,
                "message": response.text,
            }

    except requests.exceptions.RequestException as e:
        lqcd_logger.error(f"A connection error occurred: {e}")
        return False, None


def validate_and_map_user_token(token: str) -> str | None:
    """
    Validates token via Globus RS Introspection or OIDC UserInfo,
    and returns the mapped local Linux account username.
    """
    valid, user_info = validate_authorized_token(token)
    if not valid or not user_info:
        return None

    user_identity = (
        user_info.get("username")
        or user_info.get("email")
        or user_info.get("preferred_username")
        or user_info.get("sub")
        or user_info.get("login")
    )
    if not user_identity:
        return None

    return get_local_account(user_identity)



# Use fastapi router to define a router to handle authentication here
from fastapi import APIRouter

auth_router = APIRouter()


@auth_router.get("/auth/info")
async def get_auth_flow_info():
    """Returns OIDC settings including the flow type to guide the client."""
    if __auth_info is None:
        raise HTTPException(status_code=500, detail="OIDC is not loaded")

    flow_type = (
        "auth_code" if not __auth_info.device_authorization_url else "device_code"
    )
    return {
        "provider": __auth_info.provider,
        "flow_type": flow_type,
        "authorization_url": __auth_info.authorization_url,
        "redirect_uri": __auth_info.redirect_uri,
        "client_id": __auth_info.client_id,
        "scope": __auth_info.scope,
        "globus_rs_id": __auth_info.globus_rs_id,
        "globus_rs_scope_suffix": __auth_info.globus_rs_scope_suffix,
    }


@auth_router.post("/auth/code-exchange")
async def exchange_auth_code(code: str, code_verifier: str | None = None):
    """Exchanges an authorization code for an OIDC token with optional PKCE verification."""
    if __auth_info is None:
        raise HTTPException(status_code=500, detail="OIDC is not loaded")

    async with httpx.AsyncClient() as client:
        data = {
            "client_id": __auth_info.client_id,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": __auth_info.redirect_uri
            or "https://auth.globus.org/v2/web/auth-code",
        }
        if __auth_info.client_secret:
            data["client_secret"] = __auth_info.client_secret
        if code_verifier:
            data["code_verifier"] = code_verifier

        resp = await client.post(
            __auth_info.token_url,
            data=data,
            headers={"Accept": "application/json"},
        )
        return resp.json()


@auth_router.post("/auth/device-code")
async def start_auth_device_flow():
    """Step 1: Proxy requests verification codes from OIDC provider."""
    if __auth_info is None:
        raise HTTPException(status_code=500, detail="OIDC is not loaded")
    if not __auth_info.device_authorization_url:
        raise HTTPException(
            status_code=400, detail="Device flow not supported for this provider"
        )

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
        raise HTTPException(status_code=500, detail="OIDC is not loaded")
    if not __auth_info.device_authorization_url:
        raise HTTPException(
            status_code=400, detail="Device flow not supported for this provider"
        )

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


@auth_router.post("/auth/introspect")
async def introspect_token(token: str = Body(..., embed=True)):
    """Validates token, retrieves user identity and expiration details."""
    if __auth_info is None:
        raise HTTPException(status_code=500, detail="OIDC is not loaded")

    # 1. Use the existing validate_authorized_token to verify the token and get identity details
    valid, user_info = validate_authorized_token(token)
    if not valid:
        return {"active": False}

    response_data: dict[str, Any] = {"active": True}
    if isinstance(user_info, dict):
        response_data.update(user_info)

    # 2. For providers supporting standard introspection (Globus, CILogon),
    # query their introspection endpoint to get expiration, scope, etc.
    provider = __auth_info.provider.lower()
    if provider in ("globus", "cilogon"):
        if provider == "globus":
            introspect_url = "https://auth.globus.org/v2/oauth2/token/introspect"
        else:
            introspect_url = "https://cilogon.org/oauth2/introspect"

        async with httpx.AsyncClient() as client:
            data = {
                "token": token,
                "client_id": __auth_info.client_id,
            }
            if __auth_info.client_secret:
                data["client_secret"] = __auth_info.client_secret

            try:
                headers = {"Accept": "application/json"}
                if __auth_info.client_id and __auth_info.client_secret:
                    import base64
                    creds = f"{__auth_info.client_id}:{__auth_info.client_secret}"
                    encoded_creds = base64.b64encode(creds.encode("utf-8")).decode("utf-8")
                    headers["Authorization"] = f"Basic {encoded_creds}"

                resp = await client.post(
                    introspect_url,
                    data=data,
                    headers=headers,
                    timeout=10.0
                )
                if resp.status_code == 200:
                    intro_data = resp.json()
                    # Merge introspection fields (exp, iat, scope, etc.)
                    for key in ("exp", "iat", "scope", "client_id", "iss", "sub"):
                        if key in intro_data:
                            response_data[key] = intro_data[key]
            except Exception as e:
                lqcd_logger.warning(f"Failed to query provider introspection endpoint: {e}")

    return response_data
