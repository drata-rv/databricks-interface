"""
Databricks client factory.

Credential resolution order (handled entirely by the SDK):
  1. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN, etc.)
  2. ~/.databrickscfg profile (DATABRICKS_CONFIG_PROFILE selects a non-default profile)
  3. Cloud-native auth (Azure CLI, AWS IAM, GCP service account)

No credentials are accepted or stored here — call sites simply call get_client().
"""

import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config


def _load_dotenv() -> None:
    """Load .env from the project root if python-dotenv is available."""
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass


def get_client() -> WorkspaceClient:
    """Return an authenticated WorkspaceClient.

    Works identically in local dev (reads .env or ~/.databrickscfg) and
    in deployed/CI contexts (env vars injected by the orchestrator).
    """
    _load_dotenv()
    return WorkspaceClient()


def get_config() -> Config:
    """Return the resolved SDK Config without constructing a full client.

    Useful for inspecting which host/auth method was resolved.
    """
    _load_dotenv()
    return Config()
