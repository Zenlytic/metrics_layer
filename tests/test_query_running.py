import pandas as pd
import pytest

from granite.core.model.project import Project
from granite.core.parse.config import ConfigError, GraniteConfiguration
from granite.core.query import query
from granite.core.sql import QueryRunner


def test_run_query_snowflake(monkeypatch, models, views):
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
    # Add reference to snowflake creds
    sf_models = [{**m, "connection": "sf_name"} for m in models]
    project = Project(models=sf_models, views=views, looker_env="prod")
    config._project = project

    correct_df = pd.DataFrame({"dimension": ["cat1", "cat2", "cat3"], "metric": [12, 21, 34]})
    monkeypatch.setattr(QueryRunner, "_run_snowflake_query", lambda *args, **kwargs: correct_df)

    df = query(metrics=["total_item_revenue"], dimensions=["channel"], config=config)

    assert df.equals(correct_df)


def test_run_query_bigquery(monkeypatch, models, views):
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
    # Add reference to BigQuery creds
    bq_models = [{**m, "connection": "bq_name"} for m in models]
    project = Project(models=bq_models, views=views, looker_env="prod")
    config._project = project

    correct_df = pd.DataFrame({"dimension": ["cat7", "cat8", "cat9"], "metric": [98, 86, 65]})
    monkeypatch.setattr(QueryRunner, "_run_bigquery_query", lambda *args, **kwargs: correct_df)
    df = query(metrics=["total_item_revenue"], dimensions=["channel"], config=config)

    assert df.equals(correct_df)


def test_run_query_no_connection_error(project):
    repo_config = {"repo_url": "https://github.com", "branch": "dev", "repo_type": "granite"}
    config = GraniteConfiguration(repo_config=repo_config)
    config._project = project
    with pytest.raises(ConfigError) as exc_info:
        query(metrics=["total_item_revenue"], dimensions=["channel"], config=config)

    assert exc_info.value
