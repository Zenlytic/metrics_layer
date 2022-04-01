import os

import pytest

from metrics_layer.core.model.project import Project
from metrics_layer.core.parse.github_repo import BaseRepo
from metrics_layer.core.parse.project_reader import ProjectReader

BASE_PATH = os.path.dirname(__file__)


class repo_mock(BaseRepo):
    def __init__(self, repo_type: str = None):
        self.repo_type = repo_type

    @property
    def folder(self):
        if self.repo_type == "dbt":
            return os.path.join(BASE_PATH, "config/dbt/")
        raise NotImplementedError()

    @property
    def warehouse_type(self):
        if self.repo_type == "dbt":
            return "SNOWFLAKE"
        raise NotImplementedError()

    def fetch(self):
        return

    def search(self, pattern):
        if pattern == "*.model.*":
            return [os.path.join(BASE_PATH, "config/lookml/models/model_with_all_fields.model.lkml")]
        elif pattern == "*.view.*":
            return [os.path.join(BASE_PATH, "config/lookml/views/view_with_all_fields.view.lkml")]
        elif pattern == "*.yml":
            view = os.path.join(BASE_PATH, "config/metrics_layer_config/views/view_with_all_fields.yml")
            model = os.path.join(BASE_PATH, "config/metrics_layer_config/models/model_with_all_fields.yml")
            return [model, view]
        elif pattern == "manifest.json":
            return [os.path.join(BASE_PATH, "config/dbt/target/manifest.json")]
        return []

    def delete(self):
        return


def test_config_load_yaml():
    reader = ProjectReader(repo=repo_mock(repo_type="metrics_layer"))
    reader.load()

    model = reader.models[0]

    assert model["type"] == "model"
    assert isinstance(model["name"], str)
    assert isinstance(model["connection"], str)
    assert isinstance(model["explores"], list)

    explore = model["explores"][0]

    assert isinstance(explore["name"], str)
    assert isinstance(explore["from"], str)
    assert isinstance(explore["joins"], list)
    assert isinstance(explore["always_filter"], dict)
    assert isinstance(explore["always_filter"]["filters"], list)
    assert isinstance(explore["always_filter"]["filters"][0], dict)
    assert "field" in explore["always_filter"]["filters"][0]
    assert "value" in explore["always_filter"]["filters"][0]

    join = explore["joins"][0]

    assert isinstance(join["name"], str)
    assert isinstance(join["sql_on"], str)
    assert isinstance(join["type"], str)
    assert isinstance(join["relationship"], str)

    view = reader.views[0]

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


def test_config_load_lkml():
    reader = ProjectReader(repo=repo_mock(repo_type="lookml"))
    reader.load()

    model = reader.models[0]

    assert model["type"] == "model"
    assert isinstance(model["name"], str)
    assert isinstance(model["connection"], str)
    assert isinstance(model["explores"], list)

    explore = model["explores"][0]

    assert isinstance(explore["name"], str)
    assert isinstance(explore["from"], str)
    assert isinstance(explore["joins"], list)
    assert isinstance(explore["always_filter"], dict)
    assert isinstance(explore["always_filter"]["filters"], list)
    assert isinstance(explore["always_filter"]["filters"][0], dict)
    assert "field" in explore["always_filter"]["filters"][0]
    assert "value" in explore["always_filter"]["filters"][0]

    join = explore["joins"][0]

    assert isinstance(join["name"], str)
    assert isinstance(join["sql_on"], str)
    assert isinstance(join["type"], str)
    assert isinstance(join["relationship"], str)

    view = reader.views[0]

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


def test_automatic_choosing():
    reader = ProjectReader(repo=repo_mock())
    reader.load()
    assert reader.base_repo.get_repo_type() == "metrics_layer"


def test_bad_repo_type():
    reader = ProjectReader(repo=repo_mock(repo_type="dne"))
    with pytest.raises(TypeError) as exc_info:
        reader.load()

    assert exc_info.value


def test_config_load_multiple():

    base_mock = repo_mock(repo_type="lookml")
    additional_mock = repo_mock(repo_type="metrics_layer")
    reader = ProjectReader(repo=base_mock, additional_repo=additional_mock)

    model = reader.models[0]

    assert model["type"] == "model"
    assert isinstance(model["name"], str)
    assert isinstance(model["connection"], str)
    assert isinstance(model["explores"], list)

    explore = model["explores"][0]

    assert isinstance(explore["name"], str)
    assert isinstance(explore["from"], str)
    assert isinstance(explore["joins"], list)

    join = explore["joins"][0]

    assert isinstance(join["name"], str)
    assert isinstance(join["sql_on"], str)
    assert isinstance(join["type"], str)
    assert isinstance(join["relationship"], str)

    view = reader.views[0]

    assert view["type"] == "view"
    assert isinstance(view["sets"], list)
    assert isinstance(view["sets"][0], dict)
    assert view["sets"][0]["name"] == "set_name"
    assert isinstance(view["name"], str)
    assert isinstance(view["sql_table_name"], str)
    assert isinstance(view["fields"], list)

    field_with_all = next((f for f in view["fields"] if f["name"] == "field_name"))
    field_with_newline = next((f for f in view["fields"] if f["name"] == "parent_channel"))
    field_with_filter = next((f for f in view["fields"] if f["name"] == "filter_testing"))
    field_with_new_filter = next((f for f in view["fields"] if f["name"] == "filter_testing_new"))

    assert isinstance(field_with_all["name"], str)
    assert isinstance(field_with_all["field_type"], str)
    assert isinstance(field_with_all["type"], str)
    assert isinstance(field_with_all["sql"], str)
    assert field_with_all["view_label"] == "desired looker label name"
    assert field_with_all["parent"] == "parent_field"
    assert field_with_all["extra"]["zenlytic.exclude"] == ["field_name"]

    # This is in here to make sure we recognize the newlines so the comment is properly ignored
    correct_sql = (
        "CASE\n        --- parent channel\n        WHEN channel ilike "
        "'%social%' then 'Social'\n        ELSE 'Not Social'\n        END"
    )
    assert field_with_newline["sql"] == correct_sql

    # This is in here to make sure we recognize and adjust the default lkml filter dict label
    assert field_with_filter["filters"][0] == {"field": "new_vs_repeat", "value": "Repeat"}

    assert field_with_new_filter["filters"][0] == {"field": "new_vs_repeat", "value": "Repeat"}
    assert field_with_new_filter["filters"][-1] == {"field": "is_churned", "value": "TRUE"}

    project = Project(
        models=reader.models,
        views=reader.views,
        dashboards=[],
        connection_lookup={"connection_name": "SNOWFLAKE"},
    )
    field = project.get_field("filter_testing_new")
    query = field.sql_query(query_type="SNOWFLAKE")
    correct = (
        "SUM(case when view_name.new_vs_repeat='Repeat' and "
        "view_name.is_churned=true then view_name.revenue end)"
    )
    assert query == correct


def test_config_use_view_name(project):
    explore = project.get_explore("discounts_only")
    assert explore.from_ == "discounts"


@pytest.mark.skip("slow")
def test_config_load_dbt():
    reader = ProjectReader(repo=repo_mock(repo_type="dbt"))
    reader.load()

    model = reader.models[0]

    assert model["type"] == "model"
    assert isinstance(model["name"], str)
    assert isinstance(model["connection"], str)
    assert isinstance(model["explores"], list)

    explore = model["explores"][0]

    assert isinstance(explore["name"], str)

    view = reader.views[0]

    assert view["type"] == "view"
    assert isinstance(view["name"], str)
    assert view["sql_table_name"] == "fake.order_lines"
    assert isinstance(view["default_date"], str)
    assert view["row_label"] == "Order line"
    assert isinstance(view["fields"], list)

    total_revenue_measure = next((f for f in view["fields"] if f["name"] == "new_customer_revenue"))

    assert total_revenue_measure["name"] == "new_customer_revenue"
    assert total_revenue_measure["field_type"] == "measure"
    assert total_revenue_measure["type"] == "sum"
    assert total_revenue_measure["label"] == "New customer revenue"
    assert total_revenue_measure["description"] == "Total revenue from new customers"
    assert total_revenue_measure["sql"] == "${TABLE}.product_revenue"
    assert total_revenue_measure["extra"] == {"team": "Finance"}
    assert total_revenue_measure["filters"] == [{"field": "new_vs_repeat", "value": "New"}]

    os.chdir("../../..")
