import pandas as pd
import pytest

from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.model.project import Project
from metrics_layer.core.parse.config import ConfigError, MetricsLayerConfiguration
from metrics_layer.core.sql import QueryRunner


def test_run_query_snowflake(monkeypatch, models, views):
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
    # Add reference to snowflake creds
    sf_models = [{**m, "connection": "sf_name"} for m in models]
    project = Project(models=sf_models, views=views, looker_env="prod")
    config._project = project

    correct_df = pd.DataFrame({"dimension": ["cat1", "cat2", "cat3"], "metric": [12, 21, 34]})
    monkeypatch.setattr(QueryRunner, "_run_snowflake_query", lambda *args, **kwargs: correct_df)

    conn = MetricsLayerConnection(config=config)
    df = conn.query(metrics=["total_item_revenue"], dimensions=["channel"])

    assert df.equals(correct_df)


def test_run_query_bigquery(monkeypatch, models, views):
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
    # Add reference to BigQuery creds
    bq_models = [{**m, "connection": "bq_name"} for m in models]
    project = Project(models=bq_models, views=views, looker_env="prod")
    config._project = project

    correct_df = pd.DataFrame({"dimension": ["cat7", "cat8", "cat9"], "metric": [98, 86, 65]})
    monkeypatch.setattr(QueryRunner, "_run_bigquery_query", lambda *args, **kwargs: correct_df)

    conn = MetricsLayerConnection(config=config)
    df = conn.query(metrics=["total_item_revenue"], dimensions=["channel"])

    assert df.equals(correct_df)


def test_run_query_no_connection_error(project):
    repo_config = {"repo_url": "https://github.com", "branch": "dev", "repo_type": "metrics_layer"}
    config = MetricsLayerConfiguration(repo_config=repo_config)
    config._project = project
    conn = MetricsLayerConnection(config=config)
    with pytest.raises(ConfigError) as exc_info:
        conn.query(metrics=["total_item_revenue"], dimensions=["channel"])

    assert exc_info.value
