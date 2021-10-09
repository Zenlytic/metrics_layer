import os

import pytest

from granite.core.parse.github_repo import BaseRepo
from granite.core.parse.project_reader import ProjectReader

BASE_PATH = os.path.dirname(__file__)


class repo_mock(BaseRepo):
    def __init__(self, repo_type: str = None):
        self.repo_type = repo_type

    def fetch(self):
        return

    def search(self, pattern):
        if pattern == "*.model.*":
            return [os.path.join(BASE_PATH, "config/lookml/models/model_with_all_fields.model.lkml")]
        elif pattern == "*.view.*":
            return [os.path.join(BASE_PATH, "config/lookml/views/view_with_all_fields.view.lkml")]
        elif pattern == "*.yml":
            view = os.path.join(BASE_PATH, "config/granite_config/views/view_with_all_fields.yml")
            model = os.path.join(BASE_PATH, "config/granite_config/models/model_with_all_fields.yml")
            return [model, view]
        return []

    def delete(self):
        return


def test_config_load_yaml():
    reader = ProjectReader(repo=repo_mock(repo_type="granite"))
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
    assert reader.base_repo.get_repo_type() == "granite"


def test_bad_repo_type():
    reader = ProjectReader(repo=repo_mock(repo_type="dne"))
    with pytest.raises(TypeError) as exc_info:
        reader.load()

    assert exc_info.value


def test_config_load_multiple():

    base_mock = repo_mock(repo_type="lookml")
    additional_mock = repo_mock(repo_type="granite")
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
    assert isinstance(view["name"], str)
    assert isinstance(view["sql_table_name"], str)
    assert isinstance(view["fields"], list)

    field = view["fields"][0]

    assert isinstance(field["name"], str)
    assert isinstance(field["field_type"], str)
    assert isinstance(field["type"], str)
    assert isinstance(field["sql"], str)
    assert field["view_label"] == "desired looker label name"
    assert field["parent"] == "parent_field"
    assert field["extra"]["zenlytic.exclude"] == ["field_name"]


def test_config_use_view_name(project):
    explore = project.get_explore("discounts_only")
    assert explore.from_ == "discounts"
