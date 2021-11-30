import os

import pandas as pd
import pytest

from metrics_layer.api import create_app, db
from metrics_layer.api.models import User
from metrics_layer.core import MetricsLayerConnection
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
def seed_tables_data():
    df = pd.DataFrame({"NAME": [], "DATABASE_NAME": [], "SCHEMA_NAME": []})
    return df


@pytest.fixture(scope="function")
def seed_views_data():
    raw_data = [
        {"NAME": "ORDERS", "DATABASE_NAME": "DEMO", "SCHEMA_NAME": "ANALYTICS"},
        {"NAME": "SESSIONS", "DATABASE_NAME": "DEMO", "SCHEMA_NAME": "ANALYTICS"},
    ]
    return pd.DataFrame(raw_data)


@pytest.fixture(scope="function")
def get_seed_columns_data():
    def _get_seed_columns_data(table_name):
        if table_name == "ORDERS":
            order_records = [
                {
                    "DATA_TYPE": '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "COLUMN_NAME": "ORDER_ID",
                },
                {"DATA_TYPE": '{"type":"DATE","nullable":true}', "COLUMN_NAME": "ORDER_CREATED_AT"},
                {
                    "DATA_TYPE": '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "COLUMN_NAME": "REVENUE",
                },
                {
                    "DATA_TYPE": '{"type":"TIMESTAMP_NTZ","nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "ACQUISITION_DATE",
                },
                {
                    "DATA_TYPE": '{"type":"BOOLEAN","nullable":true}',  # noqa
                    "COLUMN_NAME": "ON_SOCIAL_NETWORK",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "CAMPAIGN",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "NEW_VS_REPEAT",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "PRODUCT",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "DAY_OF_WEEK",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "TWITTER",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "LAST_VIEWED_PAGE",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "CUSTOMER_ID",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "TOP_CUSTOMERS",
                },
            ]
            return pd.DataFrame(order_records)
        elif table_name == "SESSIONS":
            session_records = [
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "SESSION_ID",
                },
                {"DATA_TYPE": '{"type":"DATE","nullable":true}', "COLUMN_NAME": "SESSION_DATE"},
                {
                    "DATA_TYPE": '{"type":"FIXED","precision":38,"scale":0,"nullable":true}',
                    "COLUMN_NAME": "ADD_TO_CART",
                },
                {
                    "DATA_TYPE": '{"type":"REAL","precision":38,"scale":0,"nullable":true}',
                    "COLUMN_NAME": "CONVERSION",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "CROSSSELL_PRODUCT",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "ACQUISITION_CHANNEL",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "SOCIAL_NETWORK",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "CAMPAIGN",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "NEW_VS_REPEAT",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "PRODUCT",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "DAY_OF_WEEK",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "TWITTER",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK",
                },
                {
                    "DATA_TYPE": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',  # noqa
                    "COLUMN_NAME": "LAST_VIEWED_PAGE",
                },
            ]
            return pd.DataFrame(session_records)
        else:
            raise NotImplementedError(f"This should never be hit in testing with table: {table_name}")

    return _get_seed_columns_data


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
    project = Project(
        models=models, views=views, looker_env="prod", connection_lookup={"connection_name": "SNOWFLAKE"}
    )
    return project


@pytest.fixture(scope="module")
def config(project):
    class bq_mock:
        name = "testing_bigquery"
        type = "BIGQUERY"

    class sf_mock:
        name = "testing_snowflake"
        type = "SNOWFLAKE"

    class config_mock:
        def get_connection(name: str):
            if name == "bq_creds":
                return bq_mock
            else:
                return sf_mock

        def connections():
            return [sf_mock]

    config_mock.project = project
    return config_mock


@pytest.fixture(scope="module")
def connection(config):
    return MetricsLayerConnection(config=config)
