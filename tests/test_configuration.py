import pickle

import pytest

from metrics_layer import MetricsLayerConnection
from metrics_layer.core.parse import ProjectLoader
from metrics_layer.core.parse import ConfigError
from metrics_layer.core.parse.connections import (
    BigQueryConnection,
    RedshiftConnection,
    SnowflakeConnection,
)


def test_config_explicit_metrics_layer_single_local():
    loader = ProjectLoader(location="./tests/config/metrics_layer_config/")

    assert loader.repo.repo_path == "./tests/config/metrics_layer_config/"


def test_config_explicit_metrics_layer_single():
    loader = ProjectLoader(location="https://github.com", branch="dev")

    assert loader.repo.branch == "dev"
    assert loader.repo.repo_url == "https://github.com"


def test_config_explicit_metrics_layer_pickle(project):
    loader = ProjectLoader(location="https://github.com", branch="dev")
    loader._project = project

    # We should be able to pickle the loader
    pickle.dumps(loader)


def test_config_explicit_metrics_layer_single_with_connections():
    connections = [
        {
            "type": "SNOWFLAKE",
            "name": "sf_name",
            "account": "sf_account",
            "username": "sf_username",
            "password": "sf_password",
            "role": "my_role",
            "warehouse": "compute_wh",
            "database": "company",
            "schema": "testing",
        },
        {
            "type": "BIGQUERY",
            "name": "bq_name",
            "credentials": '{"key": "value", "project_id": "test-1234"}',
        },
        {
            "type": "REDSHIFT",
            "name": "rs_name",
            "host": "rs_host",
            "username": "rs_username",
            "password": "rs_password",
            "database": "company",
        },
    ]
    conn = MetricsLayerConnection(location="https://github.com", branch="dev", connections=connections)

    assert conn.branch == "dev"
    assert conn.location == "https://github.com"

    sf_connection = conn.get_connection("sf_name")
    assert isinstance(sf_connection, SnowflakeConnection)
    assert sf_connection.to_dict() == {
        "user": "sf_username",
        "password": "sf_password",
        "account": "sf_account",
        "role": "my_role",
        "warehouse": "compute_wh",
        "database": "company",
        "schema": "testing",
        "type": "SNOWFLAKE",
        "name": "sf_name",
    }

    bq_connection = conn.get_connection("bq_name")
    assert isinstance(bq_connection, BigQueryConnection)
    assert bq_connection.to_dict() == {
        "project_id": "test-1234",
        "credentials": {"key": "value", "project_id": "test-1234"},
        "type": "BIGQUERY",
        "name": "bq_name",
    }

    rs_connection = conn.get_connection("rs_name")
    assert isinstance(rs_connection, RedshiftConnection)
    assert rs_connection.to_dict() == {
        "type": "REDSHIFT",
        "name": "rs_name",
        "host": "rs_host",
        "port": 5439,
        "username": "rs_username",
        "password": "rs_password",
        "database": "company",
    }

    result = conn.get_connection("does_not_exist")
    assert result is None


def test_config_env_metrics_layer(monkeypatch):
    monkeypatch.setenv("METRICS_LAYER_LOCATION", "https://github.com")
    monkeypatch.setenv("METRICS_LAYER_BRANCH", "dev")
    monkeypatch.setenv("METRICS_LAYER_REPO_TYPE", "metrics_layer")
    loader = ProjectLoader(None)

    assert loader.repo.repo_type == "metrics_layer"
    assert loader.repo.repo_url == "https://github.com"
    assert loader.repo.branch == "dev"


def test_config_explicit_env_config(monkeypatch):
    # Explicit should take priority
    monkeypatch.setenv("METRICS_LAYER_LOCATION", "https://github.com")
    monkeypatch.setenv("METRICS_LAYER_BRANCH", "dev")
    monkeypatch.setenv("METRICS_LAYER_REPO_TYPE", "metrics_layer")
    loader = ProjectLoader(location="https://correct.com", branch="master")

    assert loader.repo.branch == "master"
    assert loader.repo.repo_url == "https://correct.com"


def test_config_does_not_exist():
    # Should raise ConfigError
    with pytest.raises(ConfigError) as exc_info:
        ProjectLoader(None)

    assert exc_info.value
