"""
Databricks client factory.

get_client() uses the SDK's own credential resolution order:
  1. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN, etc.)
  2. ~/.databrickscfg profile (DATABRICKS_CONFIG_PROFILE selects a non-default profile)
  3. Cloud-native auth (Azure CLI, AWS IAM, GCP service account)

get_client_for_env() resolves an explicit host+token pair via db.secrets.get_secret(),
which reads from .env locally or from a Databricks secret scope when running inside a
Databricks Job (auto-detected via db.secrets.is_databricks_runtime()) -- use this when
targeting a specific workspace (e.g. separate prod/test catalogs) rather than the
SDK's default-resolved one.
"""

import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

from db.secrets import get_secret


def load_env() -> None:
    """Load .env from the project root if python-dotenv is available.

    Call this before any os.getenv() calls in scripts so that .env values
    are available when argument defaults are resolved.
    """
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path, override=True)
    except ImportError:
        pass


def get_client() -> WorkspaceClient:
    """Return an authenticated WorkspaceClient using env/config credential chain."""
    load_env()
    return WorkspaceClient()


def get_client_for(host: str, token: str) -> WorkspaceClient:
    """Return an authenticated WorkspaceClient for a specific host and token.

    Use this when targeting a workspace other than the default (e.g. a test
    workspace with a different URL and token).
    """
    load_env()
    return WorkspaceClient(host=host, token=token)


def get_client_for_env(workspace: str) -> WorkspaceClient:
    """Return an authenticated WorkspaceClient for "prod" or "test", resolving
    host+token via db.secrets.get_secret() -- .env locally, a Databricks secret
    scope when running inside a Databricks Job. Composes get_client_for() rather
    than duplicating the WorkspaceClient construction.
    """
    suffix = workspace.upper()
    host = get_secret(f"databricks-host-{workspace}", env_var=f"DATABRICKS_HOST_{suffix}")
    token = get_secret(f"databricks-token-{workspace}", env_var=f"DATABRICKS_TOKEN_{suffix}")
    return get_client_for(host, token)


def get_config() -> Config:
    """Return the resolved SDK Config without constructing a full client.

    Useful for inspecting which host/auth method was resolved.
    """
    load_env()
    return Config()
