import pandas as pd
import pytest

from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.model.project import Project
from metrics_layer.core.exceptions import QueryError
from metrics_layer.core.sql import QueryRunner


@pytest.mark.running
def test_run_query_snowflake(monkeypatch, models, views):
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
    # Add reference to snowflake creds
    sf_models = [{**m, "connection": "sf_name"} for m in models]
    project = Project(models=sf_models, views=views)

    correct_df = pd.DataFrame({"dimension": ["cat1", "cat2", "cat3"], "metric": [12, 21, 34]})
    monkeypatch.setattr(QueryRunner, "_run_snowflake_query", lambda *args, **kwargs: correct_df)

    conn = MetricsLayerConnection(project=project, connections=connections)
    df = conn.query(metrics=["total_item_revenue"], dimensions=["channel"])

    assert df.equals(correct_df)


@pytest.mark.running
def test_run_query_bigquery(monkeypatch, models, views):
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
    # Add reference to BigQuery creds
    bq_models = [{**m, "connection": "bq_name"} for m in models]
    project = Project(models=bq_models, views=views, looker_env="prod")

    correct_df = pd.DataFrame({"dimension": ["cat7", "cat8", "cat9"], "metric": [98, 86, 65]})
    monkeypatch.setattr(QueryRunner, "_run_bigquery_query", lambda *args, **kwargs: correct_df)

    conn = MetricsLayerConnection(project=project, connections=connections)
    df = conn.query(metrics=["total_item_revenue"], dimensions=["channel"])

    assert df.equals(correct_df)


@pytest.mark.running
def test_run_query_no_connection_error(project):
    conn = MetricsLayerConnection(project=project, connections=[])
    with pytest.raises(QueryError) as exc_info:
        conn.query(metrics=["total_item_revenue"], dimensions=["channel"])

    assert exc_info.value
