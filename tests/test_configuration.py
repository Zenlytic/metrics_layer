import pytest

from granite.core.parse.config import ConfigError, GraniteConfiguration
from granite.core.parse.connections import BigQueryConnection, SnowflakeConnection
from granite.core.parse.github_repo import LookerGithubRepo


def test_config_explicit_granite_single():
    repo_config = {"repo_url": "https://github.com", "branch": "dev", "repo_type": "granite"}
    config = GraniteConfiguration(repo_config=repo_config)

    assert config.repo.repo_type == "granite"
    assert config.repo.branch == "dev"
    assert config.repo.repo_url == "https://github.com"


def test_config_explicit_granite_single_with_connections():
    repo_config = {"repo_url": "https://github.com", "branch": "dev", "repo_type": "granite"}
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
    config = GraniteConfiguration(repo_config=repo_config, connections=connections)

    assert config.repo.repo_type == "granite"
    assert config.repo.branch == "dev"
    assert config.repo.repo_url == "https://github.com"

    sf_connection = config.get_connection("sf_name")
    assert isinstance(sf_connection, SnowflakeConnection)
    assert sf_connection.to_dict() == {
        "user": "sf_username",
        "password": "sf_password",
        "account": "sf_account",
    }

    bq_connection = config.get_connection("bq_name")
    assert isinstance(bq_connection, BigQueryConnection)
    assert bq_connection.to_dict() == {
        "project_id": "test-1234",
        "credentials": {"key": "value", "project_id": "test-1234"},
    }

    # Should raise ConfigError
    with pytest.raises(ConfigError) as exc_info:
        config.get_connection("does_not_exist")

    assert exc_info.value


def test_config_explicit_granite_multiple():
    repo_config = {"repo_url": "https://github.com", "branch": "dev", "repo_type": "granite"}
    additional_repo_config = {"repo_url": "https://example.com", "branch": "master", "repo_type": "granite"}
    config = GraniteConfiguration(repo_config=repo_config, additional_repo_config=additional_repo_config)

    assert config.repo.repo_type == "granite"
    assert config.repo.branch == "dev"
    assert config.repo.repo_url == "https://github.com"
    assert config.additional_repo.repo_type == "granite"
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
    config = GraniteConfiguration(repo_config=repo_config)

    assert config.repo.repo_type == "lookml"
    assert config.repo.looker_url == "https://looker.com"
    assert config.repo.client_id == "blah"
    assert config.repo.client_secret == "bloop"
    assert config.repo.project_name == "example"
    assert config.repo.repo.repo_url == "https://example.com"
    assert config.repo.repo.branch == "dev"


def test_config_env_granite(monkeypatch):
    monkeypatch.setenv("GRANITE_REPO_URL", "https://github.com")
    monkeypatch.setenv("GRANITE_BRANCH", "dev")
    monkeypatch.setenv("GRANITE_REPO_TYPE", "granite")
    config = GraniteConfiguration()

    assert config.repo.repo_type == "granite"
    assert config.repo.branch == "dev"
    assert config.repo.repo_url == "https://github.com"


def test_config_env_lookml(monkeypatch):
    def mock_looker_api(slf):
        return "https://example.com", "dev"

    monkeypatch.setattr(LookerGithubRepo, "get_looker_github_info", mock_looker_api)

    monkeypatch.setenv("SECRET", "top_secret")
    monkeypatch.setenv("GRANITE_LOOKER_URL", "https://looker.com")
    monkeypatch.setenv("GRANITE_CLIENT_ID", "blah")
    monkeypatch.setenv("GRANITE_CLIENT_SECRET", "bloop")
    monkeypatch.setenv("GRANITE_PROJECT_NAME", "example")
    monkeypatch.setenv("GRANITE_REPO_TYPE", "lookml")

    config = GraniteConfiguration()

    assert config.repo.repo_type == "lookml"
    assert config.repo.looker_url == "https://looker.com"
    assert config.repo.client_id == "blah"
    assert config.repo.client_secret == "bloop"
    assert config.repo.project_name == "example"
    assert config.repo.repo.repo_url == "https://example.com"
    assert config.repo.repo.branch == "dev"


def test_config_explicit_env_config(monkeypatch):
    # Explicit should take priority
    monkeypatch.setenv("GRANITE_REPO_URL", "https://github.com")
    monkeypatch.setenv("GRANITE_BRANCH", "dev")
    monkeypatch.setenv("GRANITE_REPO_TYPE", "granite")
    repo_config = {"repo_url": "https://correct.com", "branch": "master", "repo_type": "granite"}
    config = GraniteConfiguration(repo_config=repo_config)

    assert config.repo.repo_type == "granite"
    assert config.repo.branch == "master"
    assert config.repo.repo_url == "https://correct.com"


def test_config_does_not_exist():
    # Should raise ConfigError
    with pytest.raises(ConfigError) as exc_info:
        GraniteConfiguration()

    assert exc_info.value
