import os

import pytest

from metrics_layer import MetricsLayerConnection
from metrics_layer.api import create_app, db
from metrics_layer.api.models import User
from metrics_layer.core.model.project import Project
from metrics_layer.core.parse.project_reader import ProjectReader

BASE_PATH = os.path.dirname(__file__)

model_path = os.path.join(BASE_PATH, "config/metrics_layer_config/models/commerce_test_model.yml")
order_lines_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/test_order_lines.yml")
orders_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/test_orders.yml")
customers_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/test_customers.yml")
discounts_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/test_discounts.yml")
discount_detail_view_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/views/test_discount_detail.yml"
)
country_detail_view_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/views/test_country_detail.yml"
)
view_paths = [
    order_lines_view_path,
    orders_view_path,
    customers_view_path,
    discounts_view_path,
    discount_detail_view_path,
    country_detail_view_path,
]


@pytest.fixture(scope="module")
def test_app():
    app = create_app(config="metrics_layer.api.api_config.TestingConfig")
    with app.app_context():
        yield app  # testing happens here


@pytest.fixture(scope="module")
def client(test_app):
    return test_app.test_client()


@pytest.fixture(scope="module")
def test_database(test_app):
    db.create_all()
    yield db  # testing happens here
    db.session.remove()
    db.drop_all()


@pytest.fixture(scope="function")
def add_user():
    def _add_user(email, password):
        user = User.create(email=email, password=password)
        return user

    return _add_user


@pytest.fixture(scope="function")
def add_user_and_get_auth(client, test_database, add_user):
    def _add_user_and_get_auth(email, password):
        user = add_user(email, password)
        response = client.post("/api/v1/login", json={"email": email, "password": password})
        return user, response.get_json()["auth_token"]

    return _add_user_and_get_auth


@pytest.fixture(scope="module")
def models():
    models = [ProjectReader.read_yaml_file(model_path)]
    return models


@pytest.fixture(scope="module")
def views():
    views = [ProjectReader.read_yaml_file(path) for path in view_paths]
    return views


@pytest.fixture(scope="module")
def project(models, views):
    project = Project(models=models, views=views, looker_env="prod")
    return project


@pytest.fixture(scope="module")
def config(project):
    class bq_mock:
        type = "BIGQUERY"

    class sf_mock:
        type = "BIGQUERY"

    class config_mock:
        def get_connection(name: str):
            if name == "bq_creds":
                return bq_mock
            else:
                return sf_mock

    config_mock.project = project
    return config_mock


@pytest.fixture(scope="module")
def connection(config):
    return MetricsLayerConnection(config=config)
