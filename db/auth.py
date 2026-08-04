"""
Databricks client factory.

get_client() uses the SDK's own credential resolution order:
  1. Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN, etc.)
  2. ~/.databrickscfg profile (DATABRICKS_CONFIG_PROFILE selects a non-default profile)
  3. Cloud-native auth (Azure CLI, AWS IAM, GCP service account)

get_client_for_env() resolves credentials for a named workspace ("prod"/"test") via
db.secrets.get_secret(), which reads from .env locally or from a Databricks secret scope
when running inside a Databricks Job (auto-detected via db.secrets.is_databricks_runtime()).
It prefers OAuth M2M (service-principal client_id/client_secret) when both are present,
and falls back to a personal access token otherwise -- Nationwide's security policy prefers
OAuth, but PAT stays supported so local dev and any environment without a provisioned
service principal keep working unchanged.
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

    Raises if host or token is missing rather than constructing WorkspaceClient(host=None,
    token=None) -- the SDK would silently fall through to its own ambient credential chain
    (env vars, ~/.databrickscfg, cloud-native auth) in that case, meaning a caller who thinks
    they're targeting a specific workspace could silently authenticate against a different
    one with no error.
    """
    if not host or not token:
        raise ValueError(
            f"get_client_for requires both host and token; got host={host!r}, "
            f"token={'<set>' if token else token!r}"
        )
    load_env()
    return WorkspaceClient(host=host, token=token)


def get_client_for_oauth(host: str, client_id: str, client_secret: str) -> WorkspaceClient:
    """Return an authenticated WorkspaceClient using OAuth M2M (service principal) credentials.

    Databricks service-principal OAuth M2M tokens only support a single scope, "all-apis" --
    there is no finer-grained scope to request. The SDK handles the client_credentials token
    exchange (and refresh) itself once given host/client_id/client_secret; auth_type is passed
    explicitly so this never silently falls through to a different credential provider.
    """
    load_env()
    return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret, auth_type="oauth-m2m")


def get_client_for_env(workspace: str) -> WorkspaceClient:
    """Return an authenticated WorkspaceClient for "prod" or "test", resolving credentials
    via db.secrets.get_secret() -- .env locally, a Databricks secret scope when running
    inside a Databricks Job. Prefers OAuth M2M (service-principal client_id/client_secret)
    when both are set; falls back to a personal access token otherwise. Composes
    get_client_for()/get_client_for_oauth() rather than duplicating client construction.

    Each workspace's secret scope name resolves independently (DATABRICKS_SECRET_SCOPE_PROD/
    _TEST, falling back to the single DATABRICKS_SECRET_SCOPE) since Nationwide provisions a
    separate scope per workspace rather than one shared scope.
    """
    suffix = workspace.upper()
    scope = os.getenv(f"DATABRICKS_SECRET_SCOPE_{suffix}") or os.getenv("DATABRICKS_SECRET_SCOPE")
    host = get_secret(f"databricks-host-{workspace}", scope=scope, env_var=f"DATABRICKS_HOST_{suffix}")
    client_id = get_secret(f"databricks-client-id-{workspace}", scope=scope, env_var=f"DATABRICKS_CLIENT_ID_{suffix}")
    client_secret = get_secret(f"databricks-client-secret-{workspace}", scope=scope, env_var=f"DATABRICKS_CLIENT_SECRET_{suffix}")
    if client_id and client_secret:
        return get_client_for_oauth(host, client_id, client_secret)
    token = get_secret(f"databricks-token-{workspace}", scope=scope, env_var=f"DATABRICKS_TOKEN_{suffix}")
    return get_client_for(host, token)


def get_config() -> Config:
    """Return the resolved SDK Config without constructing a full client.

    Useful for inspecting which host/auth method was resolved.
    """
    load_env()
    return Config()
