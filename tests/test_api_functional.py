import pandas as pd

from granite.core.model import Project
from granite.core.parse import GraniteConfiguration
from granite.core.sql import QueryRunner


def test_api_query(client, monkeypatch, models, views, add_user_and_get_auth):
    _, token = add_user_and_get_auth("query@test.com", "test")
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
    project = Project(models=sf_models, views=views)
    config._project = project

    monkeypatch.setattr(GraniteConfiguration, "get_granite_configuration", lambda *args, **kwargs: config)

    correct_df = pd.DataFrame({"dimension": ["cat1", "cat2", "cat3"], "metric": [12, 21, 34]})
    monkeypatch.setattr(QueryRunner, "_run_snowflake_query", lambda *args, **kwargs: correct_df)

    query_args = {"metrics": ["total_item_revenue"], "dimensions": ["channel"]}
    response = client.post(f"api/v1/query", json=query_args, headers={"Authorization": f"Bearer {token}"})
    data = response.get_json()

    assert data["data"] == correct_df.to_dict("records")


def test_api_convert_sql(client, monkeypatch, project, add_user_and_get_auth):
    _, token = add_user_and_get_auth("convert@test.com", "test")
    monkeypatch.setenv("GRANITE_REPO_URL", "https://github.com")
    monkeypatch.setenv("GRANITE_BRANCH", "dev")
    monkeypatch.setenv("GRANITE_REPO_TYPE", "granite")

    monkeypatch.setattr(GraniteConfiguration, "_get_project", lambda *args, **kwargs: project)

    mql_query = "SELECT * FROM MQL(total_item_revenue BY channel)"
    response = client.post(
        f"api/v1/convert", json={"query": mql_query}, headers={"Authorization": f"Bearer {token}"}
    )
    data = response.get_json()

    correct = (
        "SELECT * FROM (SELECT order_lines.sales_channel as channel,SUM(order_lines.revenue) as "
        "total_item_revenue FROM analytics.order_line_items order_lines GROUP BY order_lines.sales_channel);"
    )

    assert data["data"] == correct


def test_api_list_metrics(client, monkeypatch, project, add_user_and_get_auth):
    _, token = add_user_and_get_auth("list_metrics@test.com", "test")
    monkeypatch.setenv("GRANITE_REPO_URL", "https://github.com")
    monkeypatch.setenv("GRANITE_BRANCH", "dev")
    monkeypatch.setenv("GRANITE_REPO_TYPE", "granite")

    monkeypatch.setattr(GraniteConfiguration, "_get_project", lambda *args, **kwargs: project)

    response = client.get(f"api/v1/metrics", headers={"Authorization": f"Bearer {token}"})
    data = response.get_json()

    assert len(data["data"]) == 10


def test_api_list_dimensions(client, monkeypatch, project, add_user_and_get_auth):
    _, token = add_user_and_get_auth("list_dimensions@test.com", "test")
    monkeypatch.setenv("GRANITE_REPO_URL", "https://github.com")
    monkeypatch.setenv("GRANITE_BRANCH", "dev")
    monkeypatch.setenv("GRANITE_REPO_TYPE", "granite")

    monkeypatch.setattr(GraniteConfiguration, "_get_project", lambda *args, **kwargs: project)

    response = client.get(f"api/v1/dimensions", headers={"Authorization": f"Bearer {token}"})
    data = response.get_json()

    assert len(data["data"]) == 26


def test_api_get_metric(client, monkeypatch, project, add_user_and_get_auth):
    _, token = add_user_and_get_auth("get_metric@test.com", "test")
    monkeypatch.setenv("GRANITE_REPO_URL", "https://github.com")
    monkeypatch.setenv("GRANITE_BRANCH", "dev")
    monkeypatch.setenv("GRANITE_REPO_TYPE", "granite")

    monkeypatch.setattr(GraniteConfiguration, "_get_project", lambda *args, **kwargs: project)

    response = client.get(f"api/v1/metrics/total_item_revenue", headers={"Authorization": f"Bearer {token}"})
    data = response.get_json()

    assert isinstance(data["data"], dict)
    assert data["data"]["name"] == "total_item_revenue"
    assert data["data"]["sql_raw"] == "${TABLE}.revenue"
    assert data["data"]["type"] == "sum"
    assert data["data"]["field_type"] == "measure"


def test_api_get_dimension(client, monkeypatch, project, add_user_and_get_auth):
    _, token = add_user_and_get_auth("get_dimension@test.com", "test")
    monkeypatch.setenv("GRANITE_REPO_URL", "https://github.com")
    monkeypatch.setenv("GRANITE_BRANCH", "dev")
    monkeypatch.setenv("GRANITE_REPO_TYPE", "granite")

    monkeypatch.setattr(GraniteConfiguration, "_get_project", lambda *args, **kwargs: project)

    response = client.get(f"api/v1/dimensions/new_vs_repeat", headers={"Authorization": f"Bearer {token}"})
    data = response.get_json()

    assert isinstance(data["data"], dict)
    assert data["data"]["name"] == "new_vs_repeat"
    assert data["data"]["sql_raw"] == "${TABLE}.new_vs_repeat"
    assert data["data"]["type"] == "string"
    assert data["data"]["field_type"] == "dimension"
