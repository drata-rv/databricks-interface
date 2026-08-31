"""
db/auth.py and db/secrets.py tests. get_client_for() takes host/token as explicit arguments
and WorkspaceClient construction is the only side effect, so it's mockable with zero
refactoring. Environment-variable state is restored after each test that mutates it.
"""

import os
import sys
from unittest import mock

import pytest

from db import auth, secrets


@pytest.fixture(autouse=True)
def _clean_env():
    """Ensure DATABRICKS_RUNTIME_VERSION/DATABRICKS_SECRET_SCOPE don't leak between tests."""
    saved = {k: os.environ.get(k) for k in ("DATABRICKS_RUNTIME_VERSION", "DATABRICKS_SECRET_SCOPE")}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def test_get_client_for_passes_through_host_and_token():
    with mock.patch("db.auth.WorkspaceClient") as MockWC:
        auth.get_client_for("https://x.example.com", "tok-123")
        MockWC.assert_called_once_with(host="https://x.example.com", token="tok-123")


@pytest.mark.parametrize("host,token", [(None, "tok-123"), ("https://x.example.com", None), (None, None), ("", "")])
def test_get_client_for_raises_on_missing_host_or_token(host, token):
    """Must fail loudly rather than constructing WorkspaceClient(host=None, token=None),
    which would silently fall through to the SDK's own ambient credential chain."""
    with mock.patch("db.auth.WorkspaceClient") as MockWC:
        with pytest.raises(ValueError):
            auth.get_client_for(host, token)
        MockWC.assert_not_called()


def test_load_env_missing_dotenv_is_a_noop():
    with mock.patch.dict(sys.modules, {"dotenv": None}):
        auth.load_env()  # must not raise


def test_is_databricks_runtime_true_when_env_var_set():
    os.environ["DATABRICKS_RUNTIME_VERSION"] = "14.3.x-scala2.12"
    assert secrets.is_databricks_runtime() is True


def test_is_databricks_runtime_false_when_unset():
    os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
    assert secrets.is_databricks_runtime() is False


def test_get_secret_local_fallback():
    os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
    os.environ["DRATA_API_KEY"] = "local-key-value"
    try:
        assert secrets.get_secret("drata-api-key", env_var="DRATA_API_KEY") == "local-key-value"
        # default env_var derivation: 'drata-api-key' -> 'DRATA_API_KEY'
        assert secrets.get_secret("drata-api-key") == "local-key-value"
    finally:
        del os.environ["DRATA_API_KEY"]


def test_get_secret_local_fallback_missing_returns_none():
    os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
    os.environ.pop("SOME_UNSET_VAR", None)
    assert secrets.get_secret("some-unset-var", env_var="SOME_UNSET_VAR") is None


def test_get_secret_databricks_path():
    os.environ["DATABRICKS_RUNTIME_VERSION"] = "14.3.x-scala2.12"
    os.environ["DATABRICKS_SECRET_SCOPE"] = "my-scope"
    with mock.patch("databricks.sdk.WorkspaceClient") as MockWC:
        MockWC.return_value.dbutils.secrets.get.return_value = "secret-value-xyz"
        result = secrets.get_secret("drata-api-key")
        assert result == "secret-value-xyz"
        MockWC.return_value.dbutils.secrets.get.assert_called_once_with(scope="my-scope", key="drata-api-key")


def test_get_secret_databricks_path_missing_key_raises_by_default():
    os.environ["DATABRICKS_RUNTIME_VERSION"] = "14.3.x-scala2.12"
    os.environ["DATABRICKS_SECRET_SCOPE"] = "my-scope"
    with mock.patch("databricks.sdk.WorkspaceClient") as MockWC:
        MockWC.return_value.dbutils.secrets.get.side_effect = Exception("Secret does not exist")
        with pytest.raises(Exception):
            secrets.get_secret("drata-host-prod")


def test_get_secret_databricks_path_missing_key_required_false_returns_none():
    os.environ["DATABRICKS_RUNTIME_VERSION"] = "14.3.x-scala2.12"
    os.environ["DATABRICKS_SECRET_SCOPE"] = "my-scope"
    with mock.patch("databricks.sdk.WorkspaceClient") as MockWC:
        MockWC.return_value.dbutils.secrets.get.side_effect = Exception("Secret does not exist")
        assert secrets.get_secret("drata-host-prod", required=False) is None


def test_get_client_for_env_resolves_per_workspace():
    os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
    os.environ.update({
        "DATABRICKS_HOST_PROD": "https://prod.example.com", "DATABRICKS_TOKEN_PROD": "prod-tok",
        "DATABRICKS_HOST_TEST": "https://test.example.com", "DATABRICKS_TOKEN_TEST": "test-tok",
    })
    try:
        with mock.patch("db.auth.WorkspaceClient") as MockWC:
            auth.get_client_for_env("prod")
            auth.get_client_for_env("test")
            assert MockWC.call_args_list[0] == mock.call(host="https://prod.example.com", token="prod-tok")
            assert MockWC.call_args_list[1] == mock.call(host="https://test.example.com", token="test-tok")
    finally:
        for k in ("DATABRICKS_HOST_PROD", "DATABRICKS_TOKEN_PROD", "DATABRICKS_HOST_TEST", "DATABRICKS_TOKEN_TEST"):
            os.environ.pop(k, None)


def test_get_client_for_oauth_passes_through_credentials():
    with mock.patch("db.auth.WorkspaceClient") as MockWC:
        auth.get_client_for_oauth("https://x.example.com", "client-id-123", "client-secret-abc")
        MockWC.assert_called_once_with(
            host="https://x.example.com", client_id="client-id-123",
            client_secret="client-secret-abc", auth_type="oauth-m2m",
        )


def test_get_client_for_env_prefers_oauth_over_token_when_both_set():
    os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
    os.environ.update({
        "DATABRICKS_HOST_PROD": "https://prod.example.com",
        "DATABRICKS_TOKEN_PROD": "prod-tok",
        "DATABRICKS_CLIENT_ID_PROD": "prod-client-id",
        "DATABRICKS_CLIENT_SECRET_PROD": "prod-client-secret",
    })
    try:
        with mock.patch("db.auth.WorkspaceClient") as MockWC:
            auth.get_client_for_env("prod")
            MockWC.assert_called_once_with(
                host="https://prod.example.com", client_id="prod-client-id",
                client_secret="prod-client-secret", auth_type="oauth-m2m",
            )
    finally:
        for k in ("DATABRICKS_HOST_PROD", "DATABRICKS_TOKEN_PROD",
                  "DATABRICKS_CLIENT_ID_PROD", "DATABRICKS_CLIENT_SECRET_PROD"):
            os.environ.pop(k, None)


def test_get_client_for_env_falls_back_to_token_when_client_credentials_partial():
    """Only client_id set (no secret) must not attempt OAuth -- falls back to token."""
    os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
    os.environ.update({
        "DATABRICKS_HOST_PROD": "https://prod.example.com",
        "DATABRICKS_TOKEN_PROD": "prod-tok",
        "DATABRICKS_CLIENT_ID_PROD": "prod-client-id",
    })
    try:
        with mock.patch("db.auth.WorkspaceClient") as MockWC:
            auth.get_client_for_env("prod")
            MockWC.assert_called_once_with(host="https://prod.example.com", token="prod-tok")
    finally:
        for k in ("DATABRICKS_HOST_PROD", "DATABRICKS_TOKEN_PROD", "DATABRICKS_CLIENT_ID_PROD"):
            os.environ.pop(k, None)
