import os

import pytest

from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.model.project import Project
from metrics_layer.core.parse.manifest import Manifest
from metrics_layer.core.parse.project_reader import ProjectReader

BASE_PATH = os.path.dirname(__file__)

model_path = os.path.join(BASE_PATH, "config/metrics_layer_config/models/commerce_test_model.yml")
sales_dashboard_path = os.path.join(BASE_PATH, "config/metrics_layer_config/dashboards/sales_dashboard.yml")
sales_dashboard_v2_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/dashboards/sales_dashboard_v2.yml"
)
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
dashboard_paths = [sales_dashboard_path, sales_dashboard_v2_path]


@pytest.fixture(scope="function")
def seed_views_data():
    return []


@pytest.fixture(scope="function")
def seed_tables_data():
    raw_data = [
        (None, "ORDERS", "DEMO", "ANALYTICS"),
        (None, "SESSIONS", "DEMO", "ANALYTICS"),
    ]
    return raw_data


@pytest.fixture(scope="function")
def get_seed_columns_data():
    def _get_seed_columns_data(table_name):
        if table_name == "ORDERS":
            order_records = [
                (None, None, "ORDER_ID", '{"type":"FIXED","precision":38,"scale":0,"nullable":true}'),
                (None, None, "ORDER_CREATED_AT", '{"type":"DATE","nullable":true}'),
                (None, None, "REVENUE", '{"type":"FIXED","precision":38,"scale":0,"nullable":true}'),
                (None, None, "ACQUISITION_DATE", '{"type":"TIMESTAMP_NTZ","nullable":true,"fixed":false}'),
                (None, None, "ON_SOCIAL_NETWORK", '{"type":"BOOLEAN","nullable":true}'),
                (
                    None,
                    None,
                    "CAMPAIGN",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "NEW_VS_REPEAT",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "PRODUCT",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "DAY_OF_WEEK",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "TWITTER",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "EMAILS_FROM_US_IN_THE_LAST_WEEK",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "LAST_VIEWED_PAGE",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "CUSTOMER_ID",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "TOP_CUSTOMERS",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
            ]
            return order_records
        elif table_name == "SESSIONS":
            session_records = [
                (
                    None,
                    None,
                    "SESSION_ID",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (None, None, "SESSION_DATE", '{"type":"DATE","nullable":true}'),
                (None, None, "ADD_TO_CART", '{"type":"FIXED","precision":38,"scale":0,"nullable":true}'),
                (None, None, "CONVERSION", '{"type":"REAL","precision":38,"scale":0,"nullable":true}'),
                (
                    None,
                    None,
                    "CROSSSELL_PRODUCT",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "ACQUISITION_CHANNEL",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "SOCIAL_NETWORK",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "CAMPAIGN",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "NEW_VS_REPEAT",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "PRODUCT",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "DAY_OF_WEEK",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "TWITTER",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "EMAILS_FROM_US_IN_THE_LAST_WEEK",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
                (
                    None,
                    None,
                    "LAST_VIEWED_PAGE",
                    '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}',
                ),
            ]
            return session_records
        else:
            raise NotImplementedError(f"This should never be hit in testing with table: {table_name}")

    return _get_seed_columns_data


@pytest.fixture(scope="function")
def fresh_models():
    models = [ProjectReader.read_yaml_file(model_path)]
    return models


@pytest.fixture(scope="function")
def fresh_views():
    views = [ProjectReader.read_yaml_file(path) for path in view_paths]
    return views


@pytest.fixture(scope="function")
def fresh_dashboards():
    dashboards = [ProjectReader.read_yaml_file(path) for path in dashboard_paths]
    return dashboards


@pytest.fixture(scope="module")
def models():
    models = [ProjectReader.read_yaml_file(model_path)]
    return models


@pytest.fixture(scope="module")
def views():
    views = [ProjectReader.read_yaml_file(path) for path in view_paths]
    return views


@pytest.fixture(scope="module")
def dashboards():
    dashboards = [ProjectReader.read_yaml_file(path) for path in dashboard_paths]
    return dashboards


@pytest.fixture(scope="module")
def manifest():
    mock_manifest = {
        "nodes": {
            "models.test_project.customers": {
                "database": "transformed",
                "schema": "analytics",
                "alias": "customers",
            }
        }
    }
    return Manifest(mock_manifest)


@pytest.fixture(scope="function")
def fresh_project(fresh_models, fresh_views, fresh_dashboards, manifest):
    project = Project(
        models=fresh_models,
        views=fresh_views,
        dashboards=fresh_dashboards,
        looker_env="prod",
        connection_lookup={"connection_name": "SNOWFLAKE"},
        manifest=manifest,
    )
    return project


@pytest.fixture(scope="module")
def project(models, views, dashboards, manifest):
    project = Project(
        models=models,
        views=views,
        dashboards=dashboards,
        looker_env="prod",
        connection_lookup={"connection_name": "SNOWFLAKE"},
        manifest=manifest,
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

        def printable_attributes():
            return {
                "name": "testing_snowflake",
                "account": "blahblah.us-east-1",
                "user": "paul",
                "database": "analytics",
                "warehouse": "compute_wh",
                "role": "reporting",
            }

    class config_mock:
        profiles_path = "test_profiles_file.yml"

        def set_user(user: dict):
            pass

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
