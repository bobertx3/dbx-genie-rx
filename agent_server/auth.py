"""
Authentication utilities for Databricks Apps deployment.

Uses OBO (On-Behalf-Of) user authentication when running on Databricks Apps,
and falls back to PAT token or CLI authentication for local development.
"""

import contextvars
import logging
import os

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# Per-request OBO token set by middleware
_obo_token: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_obo_token", default=None
)


def set_obo_token(token: str | None) -> None:
    """Set the OBO token for the current request context."""
    _obo_token.set(token)


def get_obo_token() -> str | None:
    """Get the OBO token for the current request context."""
    return _obo_token.get()


def is_running_on_databricks_apps() -> bool:
    """Check if running on Databricks Apps (vs local development)."""
    return os.environ.get("DATABRICKS_APP_PORT") is not None


def get_workspace_client(
    require_user_token: bool = False, ignore_user_token: bool = False
) -> WorkspaceClient:
    """Get a Databricks WorkspaceClient with appropriate authentication.

    When running on Databricks Apps:
        Uses the OBO token from the request context to act on behalf of
        the logged-in user.

    When running locally:
        Uses PAT token from DATABRICKS_TOKEN or CLI profile.

    Args:
        require_user_token: When True on Databricks Apps, require a forwarded
            user token and raise if missing instead of falling back to app auth.
        ignore_user_token: When True, never use forwarded user token even if
            present; prefer app/local auth instead.

    Returns:
        WorkspaceClient configured for the current environment
    """
    token = None if ignore_user_token else get_obo_token()

    if token:
        host = get_databricks_host()
        logger.debug("Creating OBO WorkspaceClient for host: %s", host)
        return WorkspaceClient(host=host, token=token, auth_type="pat")

    # On Databricks Apps, some APIs (for example Genie space APIs) require
    # user authorization scopes and should never run as the app principal.
    if require_user_token and is_running_on_databricks_apps():
        raise PermissionError(
            "Missing user authorization token. Re-open the app and "
            "re-authorize requested scopes, then retry."
        )

    # Local dev fallback — let SDK auto-detect auth
    return WorkspaceClient()


def get_databricks_host() -> str:
    """Get the Databricks workspace host URL.

    Returns:
        The Databricks host URL (without trailing slash)
    """
    # On Apps, DATABRICKS_HOST is always set by the platform
    host = os.environ.get("DATABRICKS_HOST", "")
    if host:
        return host.rstrip("/")

    # Local dev fallback — let SDK resolve it
    client = WorkspaceClient()
    return (client.config.host or "").rstrip("/")


def get_llm_api_key() -> str:
    """Get the API key for LLM serving endpoints.

    When running on Databricks Apps:
        Uses the OBO token so LLM calls run as the logged-in user.

    When running locally:
        Uses PAT token from environment.

    Returns:
        API key/token for authenticating with serving endpoints
    """
    token = get_obo_token()
    if token:
        return token

    return os.environ.get("DATABRICKS_TOKEN", "")
