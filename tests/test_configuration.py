import os
import pickle

import pytest

from metrics_layer.core.parse.config import ConfigError, MetricsLayerConfiguration
from metrics_layer.core.parse.connections import BigQueryConnection, SnowflakeConnection
from metrics_layer.core.parse.github_repo import LookerGithubRepo
from metrics_layer.core.parse.project_reader import ProjectReader


def test_config_explicit_metrics_layer_single_local():
    repo_config = {"repo_path": "./tests/config/metrics_layer_config/", "repo_type": "metrics_layer"}
    config = MetricsLayerConfiguration(repo_config=repo_config)

    assert config.repo.repo_type == "metrics_layer"
    assert config.repo.repo_path == "./tests/config/metrics_layer_config/"


def test_config_explicit_metrics_layer_single():
    repo_config = {"repo_url": "https://github.com", "branch": "dev", "repo_type": "metrics_layer"}
    config = MetricsLayerConfiguration(repo_config=repo_config)

    assert config.repo.repo_type == "metrics_layer"
    assert config.repo.branch == "dev"
    assert config.repo.repo_url == "https://github.com"


def test_config_explicit_metrics_layer_pickle(project):
    repo_config = {"repo_url": "https://github.com", "branch": "dev", "repo_type": "metrics_layer"}
    config = MetricsLayerConfiguration(repo_config=repo_config)
    config._project = project

    # We should be able to pickle the config
    pickle.dumps(config)


def test_config_explicit_metrics_layer_single_with_connections():
    repo_config = {"repo_url": "https://github.com", "branch": "dev", "repo_type": "metrics_layer"}
    connections = [
        {
            "type": "SNOWFLAKE",
            "name": "sf_name",
            "account": "sf_account",
            "username": "sf_username",
            "password": "sf_password",
        },
        {
            "type": "BIGQUERY",
            "name": "bq_name",
            "credentials": '{"key": "value", "project_id": "test-1234"}',
        },
    ]
    config = MetricsLayerConfiguration(repo_config=repo_config, connections=connections)

    assert config.repo.repo_type == "metrics_layer"
    assert config.repo.branch == "dev"
    assert config.repo.repo_url == "https://github.com"

    sf_connection = config.get_connection("sf_name")
    assert isinstance(sf_connection, SnowflakeConnection)
    assert sf_connection.to_dict() == {
        "user": "sf_username",
        "password": "sf_password",
        "account": "sf_account",
        "type": "SNOWFLAKE",
    }

    bq_connection = config.get_connection("bq_name")
    assert isinstance(bq_connection, BigQueryConnection)
    assert bq_connection.to_dict() == {
        "project_id": "test-1234",
        "credentials": {"key": "value", "project_id": "test-1234"},
        "type": "BIGQUERY",
    }

    # Should raise ConfigError
    with pytest.raises(ConfigError) as exc_info:
        config.get_connection("does_not_exist")

    assert exc_info.value


def test_config_explicit_metrics_layer_multiple():
    repo_config = {"repo_url": "https://github.com", "branch": "dev", "repo_type": "metrics_layer"}
    additional_repo_config = {
        "repo_url": "https://example.com",
        "branch": "master",
        "repo_type": "metrics_layer",
    }
    config = MetricsLayerConfiguration(repo_config=repo_config, additional_repo_config=additional_repo_config)

    assert config.repo.repo_type == "metrics_layer"
    assert config.repo.branch == "dev"
    assert config.repo.repo_url == "https://github.com"
    assert config.additional_repo.repo_type == "metrics_layer"
    assert config.additional_repo.branch == "master"
    assert config.additional_repo.repo_url == "https://example.com"


def test_config_explicit_lookml(monkeypatch):
    def mock_looker_api(slf):
        return "https://example.com", "dev"

    monkeypatch.setattr(LookerGithubRepo, "get_looker_github_info", mock_looker_api)

    repo_config = {
        "looker_url": "https://looker.com",
        "client_id": "blah",
        "client_secret": "bloop",
        "project_name": "example",
        "repo_type": "lookml",
    }
    config = MetricsLayerConfiguration(repo_config=repo_config)

    assert config.repo.repo_type == "lookml"
    assert config.repo.looker_url == "https://looker.com"
    assert config.repo.client_id == "blah"
    assert config.repo.client_secret == "bloop"
    assert config.repo.project_name == "example"
    assert config.repo.repo.repo_url == "https://example.com"
    assert config.repo.repo.branch == "dev"


def test_config_env_metrics_layer(monkeypatch):
    monkeypatch.setenv("METRICS_LAYER_REPO_URL", "https://github.com")
    monkeypatch.setenv("METRICS_LAYER_BRANCH", "dev")
    monkeypatch.setenv("METRICS_LAYER_REPO_TYPE", "metrics_layer")
    config = MetricsLayerConfiguration()

    assert config.repo.repo_type == "metrics_layer"
    assert config.repo.branch == "dev"
    assert config.repo.repo_url == "https://github.com"


def test_config_env_lookml(monkeypatch):
    def mock_looker_api(slf):
        return "https://example.com", "dev"

    monkeypatch.setattr(LookerGithubRepo, "get_looker_github_info", mock_looker_api)

    monkeypatch.setenv("SECRET", "top_secret")
    monkeypatch.setenv("METRICS_LAYER_LOOKER_URL", "https://looker.com")
    monkeypatch.setenv("METRICS_LAYER_CLIENT_ID", "blah")
    monkeypatch.setenv("METRICS_LAYER_CLIENT_SECRET", "bloop")
    monkeypatch.setenv("METRICS_LAYER_PROJECT_NAME", "example")
    monkeypatch.setenv("METRICS_LAYER_REPO_TYPE", "lookml")

    config = MetricsLayerConfiguration()

    assert config.repo.repo_type == "lookml"
    assert config.repo.looker_url == "https://looker.com"
    assert config.repo.client_id == "blah"
    assert config.repo.client_secret == "bloop"
    assert config.repo.project_name == "example"
    assert config.repo.repo.repo_url == "https://example.com"
    assert config.repo.repo.branch == "dev"


def test_config_explicit_env_config(monkeypatch):
    # Explicit should take priority
    monkeypatch.setenv("METRICS_LAYER_REPO_URL", "https://github.com")
    monkeypatch.setenv("METRICS_LAYER_BRANCH", "dev")
    monkeypatch.setenv("METRICS_LAYER_REPO_TYPE", "metrics_layer")
    repo_config = {"repo_url": "https://correct.com", "branch": "master", "repo_type": "metrics_layer"}
    config = MetricsLayerConfiguration(repo_config=repo_config)

    assert config.repo.repo_type == "metrics_layer"
    assert config.repo.branch == "master"
    assert config.repo.repo_url == "https://correct.com"


def test_config_file_metrics_layer(monkeypatch):
    test_repo_path = os.path.abspath("./tests/config/metrics_layer_config")

    monkeypatch.setenv("METRICS_LAYER_PROFILES_DIR", "./profiles")
    monkeypatch.setattr(os, "getcwd", lambda *args: test_repo_path)
    config = MetricsLayerConfiguration("sf_creds")

    assert config.project
    assert len(config.project.models()) == 2
    assert config.repo.repo_path == os.getcwd()
    assert len(config.connections()) == 1
    assert all(c.name in {"sf_creds"} for c in config.connections())

    sf = config.get_connection("sf_creds")
    assert sf.account == "xyz.us-east-1"
    assert sf.username == "test_user"
    assert sf.password == "test_password"
    assert sf.role == "test_role"

    config = MetricsLayerConfiguration("bq_creds")

    assert config.repo.repo_path == os.getcwd()
    assert len(config.connections()) == 1
    assert all(c.name in {"bq_creds"} for c in config.connections())

    bq = config.get_connection("bq_creds")
    assert bq.project_id == "test-data-warehouse"
    assert bq.credentials == {
        "client_email": "metrics_layer@testing.iam.gserviceaccount.com",
        "project_id": "test-data-warehouse",
        "type": "service_account",
    }


def test_config_file_metrics_layer_dbt_run(monkeypatch, mocker):
    test_repo_path = os.path.abspath("./tests/config/metrics_layer_config")
    monkeypatch.setattr(os, "getcwd", lambda *args: test_repo_path)
    mocker.patch("metrics_layer.core.parse.project_reader.ProjectReader._dump_yaml_file")
    mocker.patch("metrics_layer.core.parse.project_reader.ProjectReader._run_dbt")

    # This references the metrics_layer_config/ directory
    repo_config = {"repo_path": "./", "repo_type": "metrics_layer"}
    config = MetricsLayerConfiguration(repo_config)

    assert config.project

    ProjectReader._run_dbt.assert_called_once()
    ProjectReader._dump_yaml_file.assert_called_once()


def test_config_does_not_exist():
    # Should raise ConfigError
    with pytest.raises(ConfigError) as exc_info:
        MetricsLayerConfiguration()

    assert exc_info.value
