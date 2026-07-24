"""
Credential resolution that works identically whether this code runs locally (.env) or
inside a Databricks Job (Databricks secret scope).

This is the one place that knows "where do credentials come from" -- every other module
(db/auth.py, etl/extract_devices.py) just calls get_secret() and gets the right value
back regardless of environment.
"""

import os
from typing import Optional


def is_databricks_runtime() -> bool:
    """True when running on a Databricks cluster or serverless compute.

    DATABRICKS_RUNTIME_VERSION is auto-populated by every Databricks cluster and serverless
    runtime, and is absent locally and in an externally-launched process -- a more reliable
    signal here than checking for dbutils, which isn't ambiently available in a
    python_wheel_task the way it is in a notebook.
    """
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def get_secret(name: str, *, scope: Optional[str] = None, env_var: Optional[str] = None) -> Optional[str]:
    """Resolve a credential by logical name.

    Inside Databricks: reads from a secret scope via dbutils.secrets.get(). A missing
    scope/key raises rather than returning None -- a Databricks Job should fail loudly at
    the point of use, not produce a confusing downstream error from a silently-empty credential.

    Locally: falls back to os.getenv(env_var), preserving today's exact .env-based behavior.

    name: logical secret name, e.g. "drata-api-key".
    scope: secret scope name; defaults to DATABRICKS_SECRET_SCOPE if not passed.
    env_var: the .env/os.getenv() key to use locally; defaults to name.upper() with '-' -> '_'.
    """
    if env_var is None:
        env_var = name.upper().replace('-', '_')

    if is_databricks_runtime():
        from databricks.sdk import WorkspaceClient
        resolved_scope = scope or os.getenv("DATABRICKS_SECRET_SCOPE")
        client = WorkspaceClient()
        return client.dbutils.secrets.get(scope=resolved_scope, key=name)

    return os.getenv(env_var)
