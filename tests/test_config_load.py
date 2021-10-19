import os

import lkml
import yaml

BASE_PATH = os.path.dirname(__file__)


def test_config_load_yaml_model():
    model_path = os.path.join(BASE_PATH, "config/metrics_layer_config/models/model_with_all_fields.yml")
    with open(model_path, "r") as f:
        model_dict = yaml.safe_load(f)
    assert isinstance(model_dict, dict)
    assert model_dict["connection"] == "connection_name"


def test_config_load_yaml_view():
    view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/view_with_all_fields.yml")
    with open(view_path, "r") as f:
        view_dict = yaml.safe_load(f)
    assert isinstance(view_dict, dict)
    assert view_dict["name"] == "view_name"


def test_config_load_lkml_model():
    model_path = os.path.join(BASE_PATH, "config/lookml/models/model_with_all_fields.model.lkml")
    with open(model_path, "r") as f:
        model_dict = lkml.load(f)
    assert isinstance(model_dict, dict)
    assert model_dict["connection"] == "connection_name"


def test_config_load_lkml_view():
    view_path = os.path.join(BASE_PATH, "config/lookml/views/view_with_all_fields.view.lkml")
    with open(view_path, "r") as f:
        view_dict = lkml.load(f)
    assert isinstance(view_dict, dict)
    assert view_dict["views"][0]["name"] == "view_name"
