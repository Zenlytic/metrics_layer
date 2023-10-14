import os

import pytest

from metrics_layer.core.parse.github_repo import BaseRepo
from metrics_layer.core.query.query import MetricsLayerConnection
from metrics_layer.core.parse import (
    dbtProjectReader,
    MetricsLayerProjectReader,
    ProjectLoader,
    MetricflowProjectReader,
)

BASE_PATH = os.path.dirname(__file__)


class repo_mock(BaseRepo):
    def __init__(self, repo_type: str = None):
        self.repo_type = repo_type
        self.dbt_path = None

    @property
    def folder(self):
        if self.repo_type == "dbt":
            return os.path.join(BASE_PATH, "config/dbt/")
        return os.path.join(BASE_PATH, "config/metrics_layer/")

    @property
    def warehouse_type(self):
        if self.repo_type == "dbt":
            return "SNOWFLAKE"
        raise NotImplementedError()

    def fetch(self, private_key=None):
        return

    def search(self, pattern, folders):
        if pattern == "*.yml":
            view = os.path.join(BASE_PATH, "config/metrics_layer_config/data_model/view_with_all_fields.yml")
            model = os.path.join(
                BASE_PATH, "config/metrics_layer_config/data_model/model_with_all_fields.yml"
            )  # noqa
            return [model, view]
        elif pattern == "manifest.json":
            return [os.path.join(BASE_PATH, "config/dbt/target/manifest.json")]
        return []

    def delete(self):
        return


def mock_dbt_search(pattern):
    if pattern == "manifest.json":
        return [os.path.join(BASE_PATH, "config/dbt/target/manifest.json")]
    return []


def test_get_branch_options():
    loader = MetricsLayerConnection(location=os.path.join(BASE_PATH, "config/metrics_layer/"))
    loader.load()
    assert loader.get_branch_options() == []


def test_config_load_yaml():
    reader = MetricsLayerProjectReader(repo=repo_mock(repo_type="metrics_layer"))
    models, views, dashboards = reader.load()

    model = models[0]

    assert model["type"] == "model"
    assert isinstance(model["name"], str)
    assert isinstance(model["connection"], str)

    view = views[0]

    assert view["type"] == "view"
    assert isinstance(view["name"], str)
    assert isinstance(view["sets"], list)
    assert isinstance(view["sets"][0], dict)
    assert view["sets"][0]["name"] == "set_name"
    assert isinstance(view["sql_table_name"], str)
    assert isinstance(view["fields"], list)

    field = view["fields"][0]

    assert isinstance(field["name"], str)
    assert isinstance(field["field_type"], str)
    assert isinstance(field["type"], str)
    assert isinstance(field["sql"], str)

    assert len(dashboards) == 0


def test_automatic_choosing():
    assert repo_mock().get_repo_type() == "metrics_layer"


def test_bad_repo_type(monkeypatch):
    monkeypatch.setattr(ProjectLoader, "_get_repo", lambda *args: repo_mock(repo_type="dne"))

    reader = ProjectLoader(location=None)
    with pytest.raises(TypeError) as exc_info:
        reader.load()

    assert exc_info.value


@pytest.mark.dbt
def test_config_load_dbt():
    mock = repo_mock(repo_type="dbt")
    mock.dbt_path = os.path.join(BASE_PATH, "config/dbt/")
    reader = dbtProjectReader(repo=mock)
    models, views, dashboards = reader.load()

    model = models[0]

    assert model["type"] == "model"
    assert isinstance(model["name"], str)
    assert model["connection"] == "test_dbt_project"

    view = next(v for v in views if v["name"] == "order_lines")

    assert view["type"] == "view"
    assert isinstance(view["name"], str)
    assert isinstance(view["identifiers"], list)
    assert view["sql_table_name"] == "ref('order_LINES')"
    assert view["default_date"] == "order_date"
    assert view["row_label"] == "Order line"
    assert isinstance(view["fields"], list)

    total_revenue_measure = next((f for f in view["fields"] if f["name"] == "new_customer_revenue"))
    duration = next((f for f in view["fields"] if f["name"] == "between_first_order_and_now"))
    nested_metric = next((f for f in view["fields"] if f["name"] == "test_nested_names"))
    date_filter_metric = next((f for f in view["fields"] if f["name"] == "new_customer_date_filter"))

    assert duration["type"] == "duration"
    assert "sql" not in duration
    assert "sql_start" in duration and "sql_end" in duration
    assert nested_metric["sql"] == "${total_revenue} / ${total_rev}"
    assert total_revenue_measure["name"] == "new_customer_revenue"
    assert total_revenue_measure["field_type"] == "measure"
    assert total_revenue_measure["type"] == "sum"
    assert total_revenue_measure["label"] == "New customer revenue"
    assert total_revenue_measure["description"] == "Total revenue from new customers"
    correct_sql = "case when ${new_vs_repeat} = 'New' then product_revenue else null end"
    assert total_revenue_measure["sql"] == correct_sql
    assert total_revenue_measure["team"] == "Finance"

    correct_sql = "case when ${order_date_date} >= '2023-08-02' then product_revenue else null end"
    assert date_filter_metric["sql"] == correct_sql

    dash = dashboards[0]

    assert dash["type"] == "dashboard"
    assert dash["name"] == "sales_dashboard"
    assert dash["elements"][0]["title"] == "First element"


@pytest.mark.dbt
def test_config_load_metricflow():
    mock = repo_mock(repo_type="metricflow")
    mock.dbt_path = os.path.join(BASE_PATH, "config/metricflow/")
    reader = MetricflowProjectReader(repo=mock)
    models, views, dashboards = reader.load()

    model = models[0]

    assert model["type"] == "model"
    assert isinstance(model["name"], str)
    assert model["connection"] == "test_dbt_project"

    view = next(v for v in views if v["name"] == "order_item")

    assert view["type"] == "view"
    assert isinstance(view["name"], str)
    assert isinstance(view["identifiers"], list)
    # assert view["sql_table_name"] == "order_item"
    assert view["default_date"] == "ordered_at"
    assert isinstance(view["fields"], list)

    ordered_at = next((f for f in view["fields"] if f["name"] == "ordered_at"))
    food_bool = next((f for f in view["fields"] if f["name"] == "is_food_item"))
    revenue_measure = next((f for f in view["fields"] if f["name"] == "_revenue"))
    median_revenue_metric = next((f for f in view["fields"] if f["name"] == "median_revenue"))

    assert ordered_at["type"] == "time"
    assert ordered_at["field_type"] == "dimension_group"
    assert "sql" in ordered_at
    assert isinstance(ordered_at["timeframes"], list)
    assert ordered_at["timeframes"][1] == "date"

    assert food_bool["type"] == "string"
    assert food_bool["field_type"] == "dimension"
    assert food_bool["sql"] == "is_food_item"

    assert revenue_measure["type"] == "sum"
    assert revenue_measure["field_type"] == "measure"
    assert revenue_measure["sql"] == "product_price"
    assert revenue_measure["hidden"]

    assert median_revenue_metric["type"] == "median"
    assert median_revenue_metric["field_type"] == "measure"
    assert median_revenue_metric["sql"] == "product_price"
    assert median_revenue_metric["label"] == "Median Revenue"
    assert not median_revenue_metric["hidden"]

    assert len(dashboards) == 0
