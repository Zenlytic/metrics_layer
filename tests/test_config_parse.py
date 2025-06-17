import os

import pytest

from metrics_layer.core.parse import (
    MetricflowProjectReader,
    MetricsLayerProjectReader,
    ProjectLoader,
)
from metrics_layer.core.parse.github_repo import BaseRepo
from metrics_layer.core.query.query import MetricsLayerConnection

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
    models, views, dashboards, topics, conversion_errors = reader.load()

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

    assert len(topics) == 0

    assert len(dashboards) == 0

    assert len(conversion_errors) == 0


def test_automatic_choosing():
    assert repo_mock().get_repo_type() == "metrics_layer"


def test_bad_repo_type(monkeypatch):
    monkeypatch.setattr(ProjectLoader, "_get_repo", lambda *args: repo_mock(repo_type="dne"))

    reader = ProjectLoader(location=None)
    with pytest.raises(TypeError) as exc_info:
        reader.load()

    assert exc_info.value


@pytest.mark.dbt
def test_config_load_dbt(monkeypatch):
    mock = repo_mock(repo_type="dbt")
    mock.dbt_path = os.path.join(BASE_PATH, "config/dbt/")
    monkeypatch.setattr(ProjectLoader, "_get_repo", lambda *args: mock)

    reader = ProjectLoader(location=None)
    with pytest.raises(TypeError) as exc_info:
        reader.load()

    assert exc_info.value


@pytest.mark.dbt
def test_config_load_metricflow():
    mock = repo_mock(repo_type="metricflow")
    mock.dbt_path = os.path.join(BASE_PATH, "config/metricflow/")
    reader = MetricflowProjectReader(repo=mock)
    models, views, dashboards, topics, conversion_errors = reader.load()

    assert len(conversion_errors) == 3

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
    assert food_bool["sql"] == "${TABLE}.is_food_item"

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
