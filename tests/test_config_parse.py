import os

from granite.core.parse.parse_granite_config import GraniteProjectReader
from granite.core.parse.parse_lookml import LookMLProjectReader

BASE_PATH = os.path.dirname(__file__)


class repo_mock:
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
    reader = GraniteProjectReader(repo=repo_mock())
    reader.load()

    model = reader._models[0]

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

    view = reader._views[0]

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
    reader = LookMLProjectReader(repo=repo_mock())
    reader.load()

    model = reader._models[0]

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

    view = reader._views[0]

    assert view["type"] == "view"
    assert isinstance(view["name"], str)
    assert isinstance(view["sql_table_name"], str)
    assert isinstance(view["fields"], list)

    field = view["fields"][0]

    assert isinstance(field["name"], str)
    assert isinstance(field["field_type"], str)
    assert isinstance(field["type"], str)
    assert isinstance(field["sql"], str)
