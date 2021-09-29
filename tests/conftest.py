import os

import pytest

from granite.api import create_app, db
from granite.api.models import User
from granite.core.model.project import Project
from granite.core.parse.project_reader import ProjectReader

BASE_PATH = os.path.dirname(__file__)

model_path = os.path.join(BASE_PATH, "config/granite_config/models/commerce_test_model.yml")
order_lines_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_order_lines.yml")
orders_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_orders.yml")
customers_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_customers.yml")
discounts_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_discounts.yml")
view_paths = [order_lines_view_path, orders_view_path, customers_view_path, discounts_view_path]


@pytest.fixture(scope="module")
def test_app():
    app = create_app(config="granite.api.api_config.TestingConfig")
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
    project = Project(models=models, views=views)
    return project
