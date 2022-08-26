import os

import pandas as pd
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
session_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/test_sessions.yml")
event_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/test_events.yml")
traffic_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/traffic.yml")
view_paths = [
    order_lines_view_path,
    orders_view_path,
    customers_view_path,
    discounts_view_path,
    discount_detail_view_path,
    country_detail_view_path,
    session_view_path,
    event_view_path,
    traffic_view_path,
]
dashboard_paths = [sales_dashboard_path, sales_dashboard_v2_path]


@pytest.fixture(scope="function")
def seed_snowflake_tables_data():
    order_records = [
        {"COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "FIXED"},
        {"COLUMN_NAME": "ORDER_CREATED_AT", "DATA_TYPE": "DATE"},
        {"COLUMN_NAME": "REVENUE", "DATA_TYPE": "FIXED"},
        {"COLUMN_NAME": "ACQUISITION_DATE", "DATA_TYPE": "TIMESTAMP_NTZ"},
        {"COLUMN_NAME": "ON_SOCIAL_NETWORK", "DATA_TYPE": "BOOLEAN"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "CUSTOMER_ID", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "TOP_CUSTOMERS", "DATA_TYPE": "TEXT"},
    ]
    order_records = [{"TABLE_NAME": "ORDERS", **o} for o in order_records]
    session_records = [
        {"COLUMN_NAME": "SESSION_ID", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "SESSION_DATE", "DATA_TYPE": "DATE"},
        {"COLUMN_NAME": "ADD_TO_CART", "DATA_TYPE": "FIXED"},
        {"COLUMN_NAME": "CONVERSION", "DATA_TYPE": "REAL"},
        {"COLUMN_NAME": "CROSSSELL_PRODUCT", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "ACQUISITION_CHANNEL", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "SOCIAL_NETWORK", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "TEXT"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "TEXT"},
    ]
    session_records = [{"TABLE_NAME": "SESSIONS", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"TABLE_CATALOG": "DEMO", "TABLE_SCHEMA": "ANALYTICS", **r} for r in all_records]
    return pd.DataFrame(records)


@pytest.fixture(scope="function")
def seed_bigquery_tables_data():
    order_records = [
        {"column_name": "ORDER_ID", "data_type": "INT64"},
        {"column_name": "ORDER_CREATED_AT", "data_type": "TIMESTAMP"},
        {"column_name": "REVENUE", "data_type": "INT64"},
        {"column_name": "ACQUISITION_DATE", "data_type": "TIMESTAMP"},
        {"column_name": "ON_SOCIAL_NETWORK", "data_type": "BOOL"},
        {"column_name": "CAMPAIGN", "data_type": "STRING"},
        {"column_name": "NEW_VS_REPEAT", "data_type": "STRING"},
        {"column_name": "PRODUCT", "data_type": "STRING"},
        {"column_name": "DAY_OF_WEEK", "data_type": "STRING"},
        {"column_name": "TWITTER", "data_type": "STRING"},
        {"column_name": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "data_type": "STRING"},
        {"column_name": "LAST_VIEWED_PAGE", "data_type": "STRING"},
        {"column_name": "CUSTOMER_ID", "data_type": "STRING"},
        {"column_name": "TOP_CUSTOMERS", "data_type": "STRING"},
    ]
    order_records = [{"table_name": "orders", **o} for o in order_records]
    session_records = [
        {"column_name": "SESSION_ID", "data_type": "STRING"},
        {"column_name": "SESSION_DATE", "data_type": "TIMESTAMP"},
        {"column_name": "ADD_TO_CART", "data_type": "INT64"},
        {"column_name": "CONVERSION", "data_type": "NUMERIC"},
        {"column_name": "CROSSSELL_PRODUCT", "data_type": "STRING"},
        {"column_name": "ACQUISITION_CHANNEL", "data_type": "STRING"},
        {"column_name": "SOCIAL_NETWORK", "data_type": "STRING"},
        {"column_name": "CAMPAIGN", "data_type": "STRING"},
        {"column_name": "NEW_VS_REPEAT", "data_type": "STRING"},
        {"column_name": "PRODUCT", "data_type": "STRING"},
        {"column_name": "DAY_OF_WEEK", "data_type": "STRING"},
        {"column_name": "TWITTER", "data_type": "STRING"},
        {"column_name": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "data_type": "STRING"},
        {"column_name": "LAST_VIEWED_PAGE", "data_type": "STRING"},
    ]
    session_records = [{"table_name": "sessions", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"table_catalog": "demo", "table_schema": "analytics", **r} for r in all_records]
    return pd.DataFrame(records)


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
        _connections = [sf_mock]

        def set_user(user: dict):
            pass

        def get_connection(name: str):
            if name == "bq_creds":
                return bq_mock
            else:
                return config_mock._connections[0]

        def connections():
            return config_mock._connections

    config_mock.project = project
    return config_mock


@pytest.fixture(scope="function")
def fresh_config(fresh_project):
    class sf_mock:
        name = "testing_snowflake"
        type = "SNOWFLAKE"

    class config_mock:
        _connections = [sf_mock]

        def set_user(user: dict):
            pass

        def get_connection(name: str):
            return config_mock._connections[0]

        def connections():
            return config_mock._connections

    config_mock.project = fresh_project
    return config_mock


@pytest.fixture(scope="module")
def connection(config):
    return MetricsLayerConnection(config=config)
