import os

import pandas as pd
import pytest

from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.model.project import Project
from metrics_layer.core.parse.connections import BaseConnection
from metrics_layer.core.parse.manifest import Manifest
from metrics_layer.core.parse.project_reader_base import ProjectReaderBase

BASE_PATH = os.path.dirname(__file__)

model_path = os.path.join(BASE_PATH, "config/metrics_layer_config/models/commerce_test_model.yml")
new_model_path = os.path.join(BASE_PATH, "config/metrics_layer_config/models/new_model.yml")
sales_dashboard_path = os.path.join(BASE_PATH, "config/metrics_layer_config/dashboards/sales_dashboard.yml")
sales_dashboard_v2_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/dashboards/sales_dashboard_v2.yml"
)
monthly_aggregates_view_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/views/monthly_aggregates.yml"
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
clicked_on_page_view_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/views/test_clicked_on_page.yml"
)
submitted_form_view_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/views/test_submitted_form.yml"
)
event_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/test_events.yml")
login_event_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/test_login_events.yml")
traffic_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/traffic.yml")
accounts_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/accounts.yml")
acquired_accounts_view_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/views/acquired_accounts.yml"
)
mrr_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/mrr.yml")
customer_accounts_view_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/views/customer_accounts.yml"
)
created_workspaces_view_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/views/test_created_workspace.yml"
)
other_db_view_path = os.path.join(BASE_PATH, "config/metrics_layer_config/views/other_db_traffic.yml")
order_lines_topic_path = os.path.join(BASE_PATH, "config/metrics_layer_config/topics/order_lines_topic.yml")
order_lines_topic_no_always_filters_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/topics/order_lines_topic_no_always_filters.yml"
)
recurring_revenue_topic_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/topics/recurring_revenue_topic.yml"
)
order_lines_only_topic_path = os.path.join(
    BASE_PATH, "config/metrics_layer_config/topics/order_lines_only_topic.yml"
)
model_paths = [model_path, new_model_path]
topic_paths = [
    order_lines_topic_path,
    order_lines_topic_no_always_filters_path,
    recurring_revenue_topic_path,
    order_lines_only_topic_path,
]
view_paths = [
    order_lines_view_path,
    orders_view_path,
    customers_view_path,
    discounts_view_path,
    discount_detail_view_path,
    country_detail_view_path,
    session_view_path,
    event_view_path,
    login_event_view_path,
    traffic_view_path,
    clicked_on_page_view_path,
    submitted_form_view_path,
    accounts_view_path,
    acquired_accounts_view_path,
    customer_accounts_view_path,
    other_db_view_path,
    created_workspaces_view_path,
    mrr_view_path,
    monthly_aggregates_view_path,
]
dashboard_paths = [sales_dashboard_path, sales_dashboard_v2_path]


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture(scope="function")
def seed_snowflake_tables_data():
    order_records = [
        {"COMMENT": "I am an order id", "COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "FIXED"},
        {"COMMENT": None, "COLUMN_NAME": "ORDER_CREATED_AT", "DATA_TYPE": "DATE"},
        {"COMMENT": None, "COLUMN_NAME": "REVENUE", "DATA_TYPE": "FIXED"},
        {"COMMENT": None, "COLUMN_NAME": "ACQUISITION_DATE", "DATA_TYPE": "TIMESTAMP_NTZ"},
        {"COMMENT": None, "COLUMN_NAME": "ON_SOCIAL_NETWORK", "DATA_TYPE": "BOOLEAN"},
        {"COMMENT": None, "COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "PRODUCT", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "TWITTER", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "CUSTOMER_ID", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "TOP_CUSTOMERS", "DATA_TYPE": "TEXT"},
    ]
    order_records = [{"TABLE_NAME": "ORDERS", **o} for o in order_records]
    session_records = [
        {"COMMENT": None, "COLUMN_NAME": "SESSION_ID", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "SESSION_DATE", "DATA_TYPE": "DATE"},
        {"COMMENT": None, "COLUMN_NAME": "ADD_TO_CART", "DATA_TYPE": "FIXED"},
        {"COMMENT": None, "COLUMN_NAME": "CONVERSION", "DATA_TYPE": "REAL"},
        {"COMMENT": None, "COLUMN_NAME": "@CRoSSell P-roduct:", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "ACQUISITION_CHANNEL", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "SOCIAL_NETWORK", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "PRODUCT", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "TWITTER", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "TEXT"},
    ]
    session_records = [{"TABLE_NAME": "SESSIONS", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"TABLE_CATALOG": "DEMO", "TABLE_SCHEMA": "ANALYTICS", **r} for r in all_records]
    return pd.DataFrame(records)


@pytest.fixture(scope="function")
def seed_databricks_tables_data():
    order_records = [
        {"COMMENT": "I am an order id", "COLUMN_NAME": "order_id", "DATA_TYPE": "BIGINT"},
        {"COMMENT": None, "COLUMN_NAME": "order_created_at", "DATA_TYPE": "TIMESTAMP"},
        {"COMMENT": None, "COLUMN_NAME": "revenue", "DATA_TYPE": "FLOAT"},
        {"COMMENT": None, "COLUMN_NAME": "acquisition_date", "DATA_TYPE": "TIMESTAMP_NTZ"},
        {"COMMENT": None, "COLUMN_NAME": "on_social_network", "DATA_TYPE": "BOOLEAN"},
        {"COMMENT": None, "COLUMN_NAME": "campaign", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "new_vs_repeat", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "product", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "day_of_week", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "twitter", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "emails_from_us_in_the_last_week", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "last_viewed_page", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "customer_id", "DATA_TYPE": "TEXT"},
        {"COMMENT": None, "COLUMN_NAME": "top_customers", "DATA_TYPE": "TEXT"},
    ]
    order_records = [{"TABLE_NAME": "orders", **o} for o in order_records]
    session_records = [
        {"COMMENT": None, "COLUMN_NAME": "session_id", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "session_date", "DATA_TYPE": "DATE"},
        {"COMMENT": None, "COLUMN_NAME": "add_to_cart", "DATA_TYPE": "INT"},
        {"COMMENT": None, "COLUMN_NAME": "conversion", "DATA_TYPE": "LONG"},
        {"COMMENT": None, "COLUMN_NAME": "@CRoSSell P-roduct:", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "acquisition_channel", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "social_network", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "campaign", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "new_vs_repeat", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "product", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "day_of_week", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "twitter", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "emails_from_us_in_the_last_week", "DATA_TYPE": "STRING"},
        {"COMMENT": None, "COLUMN_NAME": "last_viewed_page", "DATA_TYPE": "STRING"},
    ]
    session_records = [{"TABLE_NAME": "sessions", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"TABLE_CATALOG": "demo", "TABLE_SCHEMA": "analytics", **r} for r in all_records]
    return pd.DataFrame(records)


@pytest.fixture(scope="function")
def seed_mysql_tables_data():
    order_records = [
        {"COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "numeric"},
        {"COLUMN_NAME": "ORDER_CREATED_AT", "DATA_TYPE": "date"},
        {"COLUMN_NAME": "REVENUE", "DATA_TYPE": "double"},
        {"COLUMN_NAME": "ACQUISITION_DATE", "DATA_TYPE": "timestamp"},
        {"COLUMN_NAME": "ON_SOCIAL_NETWORK", "DATA_TYPE": "boolean"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "varchar(256)"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "varchar(128)"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "CHAR"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "CUSTOMER_ID", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "TOP_CUSTOMERS", "DATA_TYPE": "varchar"},
    ]
    order_records = [{"TABLE_NAME": "orders", **o} for o in order_records]
    session_records = [
        {"COLUMN_NAME": "SESSION_ID", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "SESSION_DATE", "DATA_TYPE": "date"},
        {"COLUMN_NAME": "ADD_TO_CART", "DATA_TYPE": "bigint"},
        {"COLUMN_NAME": "CONVERSION", "DATA_TYPE": "int"},
        {"COLUMN_NAME": "@CRoSSell P-roduct:", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "ACQUISITION_CHANNEL", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "SOCIAL_NETWORK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "varchar"},
    ]
    session_records = [{"TABLE_NAME": "sessions", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"TABLE_CATALOG": "demo", "TABLE_SCHEMA": "analytics", **r} for r in all_records]
    return pd.DataFrame(records)


@pytest.fixture(scope="function")
def seed_postgres_tables_data():
    order_records = [
        {"COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "numeric"},
        {"COLUMN_NAME": "ORDER_CREATED_AT", "DATA_TYPE": "date"},
        {"COLUMN_NAME": "REVENUE", "DATA_TYPE": "double precision"},
        {"COLUMN_NAME": "ACQUISITION_DATE", "DATA_TYPE": "timestamp with time zone"},
        {"COLUMN_NAME": "ON_SOCIAL_NETWORK", "DATA_TYPE": "boolean"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "varchar(256)"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "varchar(128)"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "CHAR"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "CUSTOMER_ID", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "TOP_CUSTOMERS", "DATA_TYPE": "varchar"},
    ]
    order_records = [{"TABLE_NAME": "orders", **o} for o in order_records]
    session_records = [
        {"COLUMN_NAME": "SESSION_ID", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "SESSION_DATE", "DATA_TYPE": "date"},
        {"COLUMN_NAME": "ADD_TO_CART", "DATA_TYPE": "bigint"},
        {"COLUMN_NAME": "CONVERSION", "DATA_TYPE": "real"},
        {"COLUMN_NAME": "@CRoSSell P-roduct:", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "ACQUISITION_CHANNEL", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "SOCIAL_NETWORK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "varchar"},
    ]
    session_records = [{"TABLE_NAME": "sessions", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"TABLE_CATALOG": "demo", "TABLE_SCHEMA": "analytics", **r} for r in all_records]
    return pd.DataFrame(records)


@pytest.fixture(scope="function")
def seed_sql_server_tables_data():
    order_records = [
        {"COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "int"},
        {"COLUMN_NAME": "ORDER_CREATED_AT", "DATA_TYPE": "date"},
        {"COLUMN_NAME": "REVENUE", "DATA_TYPE": "float"},
        {"COLUMN_NAME": "ACQUISITION_DATE", "DATA_TYPE": "datetime2"},
        {"COLUMN_NAME": "ON_SOCIAL_NETWORK", "DATA_TYPE": "bit"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "varchar(256)"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "varchar(128)"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "CHAR"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "CUSTOMER_ID", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "TOP_CUSTOMERS", "DATA_TYPE": "varchar"},
    ]
    order_records = [{"TABLE_NAME": "orders", **o} for o in order_records]
    session_records = [
        {"COLUMN_NAME": "SESSION_ID", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "SESSION_DATE", "DATA_TYPE": "date"},
        {"COLUMN_NAME": "ADD_TO_CART", "DATA_TYPE": "bigint"},
        {"COLUMN_NAME": "CONVERSION", "DATA_TYPE": "real"},
        {"COLUMN_NAME": "@CRoSSell P-roduct:", "DATA_TYPE": "char"},
        {"COLUMN_NAME": "ACQUISITION_CHANNEL", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "SOCIAL_NETWORK", "DATA_TYPE": "text"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "variant"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "varchar"},
    ]
    session_records = [{"TABLE_NAME": "sessions", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"TABLE_CATALOG": "demo", "TABLE_SCHEMA": "analytics", **r} for r in all_records]
    return pd.DataFrame(records)


@pytest.fixture(scope="function")
def seed_druid_tables_data():
    order_records = [
        {"COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "REAL"},
        {"COLUMN_NAME": "ORDER_CREATED_AT", "DATA_TYPE": "DATE"},
        {"COLUMN_NAME": "REVENUE", "DATA_TYPE": "DECIMAL"},
        {"COLUMN_NAME": "ACQUISITION_DATE", "DATA_TYPE": "TIMESTAMP"},
        {"COLUMN_NAME": "ON_SOCIAL_NETWORK", "DATA_TYPE": "BOOLEAN"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "CHAR"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "CHAR"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "CUSTOMER_ID", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "TOP_CUSTOMERS", "DATA_TYPE": "VARCHAR"},
    ]
    order_records = [{"TABLE_NAME": "orders", **o} for o in order_records]
    session_records = [
        {"COLUMN_NAME": "SESSION_ID", "DATA_TYPE": "OTHER"},
        {"COLUMN_NAME": "SESSION_DATE", "DATA_TYPE": "DATE"},
        {"COLUMN_NAME": "ADD_TO_CART", "DATA_TYPE": "BIGINT"},
        {"COLUMN_NAME": "CONVERSION", "DATA_TYPE": "FLOAT"},
        {"COLUMN_NAME": "@CRoSSell P-roduct:", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "ACQUISITION_CHANNEL", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "SOCIAL_NETWORK", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "VARCHAR"},
    ]
    session_records = [{"TABLE_NAME": "sessions", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"TABLE_CATALOG": "druid", "TABLE_SCHEMA": "druid", **r} for r in all_records]
    return pd.DataFrame(records)


@pytest.fixture(scope="function")
def seed_trino_tables_data():
    order_records = [
        {"COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "double"},
        {"COLUMN_NAME": "ORDER_CREATED_AT", "DATA_TYPE": "date"},
        {"COLUMN_NAME": "REVENUE", "DATA_TYPE": "decimal(12,3)"},
        {"COLUMN_NAME": "ACQUISITION_DATE", "DATA_TYPE": "timestamp with time zone"},
        {"COLUMN_NAME": "ON_SOCIAL_NETWORK", "DATA_TYPE": "boolean"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "json"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "char"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "CHAR"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "CUSTOMER_ID", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "TOP_CUSTOMERS", "DATA_TYPE": "varchar"},
    ]
    order_records = [{"TABLE_NAME": "orders", **o} for o in order_records]
    session_records = [
        {"COLUMN_NAME": "SESSION_ID", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "SESSION_DATE", "DATA_TYPE": "date"},
        {"COLUMN_NAME": "ADD_TO_CART", "DATA_TYPE": "bigint"},
        {"COLUMN_NAME": "CONVERSION", "DATA_TYPE": "real"},
        {"COLUMN_NAME": "@CRoSSell P-roduct:", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "ACQUISITION_CHANNEL", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "SOCIAL_NETWORK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "varchar"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "varchar"},
    ]
    session_records = [{"TABLE_NAME": "sessions", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"TABLE_CATALOG": "demo", "TABLE_SCHEMA": "analytics", **r} for r in all_records]
    return pd.DataFrame(records)


@pytest.fixture(scope="function")
def seed_redshift_tables_data():
    order_records = [
        {"COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "DOUBLE"},
        {"COLUMN_NAME": "ORDER_CREATED_AT", "DATA_TYPE": "DATE"},
        {"COLUMN_NAME": "REVENUE", "DATA_TYPE": "DOUBLE PRECISION"},
        {"COLUMN_NAME": "ACQUISITION_DATE", "DATA_TYPE": "TIMESTAMPTZ"},
        {"COLUMN_NAME": "ON_SOCIAL_NETWORK", "DATA_TYPE": "BOOLEAN"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "CHAR"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "CUSTOMER_ID", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "TOP_CUSTOMERS", "DATA_TYPE": "VARCHAR"},
    ]
    order_records = [{"TABLE_NAME": "ORDERS", **o} for o in order_records]
    session_records = [
        {"COLUMN_NAME": "SESSION_ID", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "SESSION_DATE", "DATA_TYPE": "DATE"},
        {"COLUMN_NAME": "ADD_TO_CART", "DATA_TYPE": "BIGINT"},
        {"COLUMN_NAME": "CONVERSION", "DATA_TYPE": "REAL"},
        {"COLUMN_NAME": "@CRoSSell P-roduct:", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "ACQUISITION_CHANNEL", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "SOCIAL_NETWORK", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "CAMPAIGN", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "NEW_VS_REPEAT", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "PRODUCT", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "DAY_OF_WEEK", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "TWITTER", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "DATA_TYPE": "VARCHAR"},
        {"COLUMN_NAME": "LAST_VIEWED_PAGE", "DATA_TYPE": "VARCHAR"},
    ]
    session_records = [{"TABLE_NAME": "SESSIONS", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"TABLE_CATALOG": "DEMO", "TABLE_SCHEMA": "ANALYTICS", **r} for r in all_records]
    return pd.DataFrame(records)


@pytest.fixture(scope="function")
def seed_bigquery_tables_data():
    order_records = [
        {"column_name": "ORDER_ID", "data_type": "INT64", "COMMENT": "I am an order id"},
        {"column_name": "ORDER_CREATED_AT", "data_type": "TIMESTAMP", "COMMENT": "Order Created At"},
        {"column_name": "REVENUE", "data_type": "INT64", "COMMENT": "Revenue"},
        {"column_name": "ACQUISITION_DATE", "data_type": "TIMESTAMP", "COMMENT": "Acquisition Date"},
        {"column_name": "ON_SOCIAL_NETWORK", "data_type": "BOOL", "COMMENT": None},
        {"column_name": "CAMPAIGN", "data_type": "STRING", "COMMENT": None},
        {"column_name": "NEW_VS_REPEAT", "data_type": "STRING", "COMMENT": None},
        {"column_name": "PRODUCT", "data_type": "STRING", "COMMENT": None},
        {"column_name": "DAY_OF_WEEK", "data_type": "STRING", "COMMENT": None},
        {"column_name": "TWITTER", "data_type": "STRING", "COMMENT": None},
        {"column_name": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "data_type": "STRING", "COMMENT": None},
        {"column_name": "LAST_VIEWED_PAGE", "data_type": "STRING", "COMMENT": None},
        {"column_name": "CUSTOMER_ID", "data_type": "STRING", "COMMENT": None},
        {"column_name": "TOP_CUSTOMERS", "data_type": "STRING", "COMMENT": None},
    ]
    order_records = [{"table_name": "orders", **o} for o in order_records]
    session_records = [
        {"column_name": "SESSION_ID", "data_type": "STRING", "COMMENT": None},
        {"column_name": "SESSION_DATE", "data_type": "TIMESTAMP", "COMMENT": None},
        {"column_name": "ADD_TO_CART", "data_type": "INT64", "COMMENT": None},
        {"column_name": "CONVERSION", "data_type": "NUMERIC", "COMMENT": None},
        {"column_name": "@CRoSSell P-roduct:", "data_type": "STRING", "COMMENT": None},
        {"column_name": "ACQUISITION_CHANNEL", "data_type": "STRING", "COMMENT": None},
        {"column_name": "SOCIAL_NETWORK", "data_type": "STRING", "COMMENT": None},
        {"column_name": "CAMPAIGN", "data_type": "STRING", "COMMENT": None},
        {"column_name": "NEW_VS_REPEAT", "data_type": "STRING", "COMMENT": None},
        {"column_name": "PRODUCT", "data_type": "STRING", "COMMENT": None},
        {"column_name": "DAY_OF_WEEK", "data_type": "STRING", "COMMENT": None},
        {"column_name": "TWITTER", "data_type": "STRING", "COMMENT": None},
        {"column_name": "EMAILS_FROM_US_IN_THE_LAST_WEEK", "data_type": "STRING", "COMMENT": None},
        {"column_name": "LAST_VIEWED_PAGE", "data_type": "STRING", "COMMENT": None},
    ]
    session_records = [{"table_name": "sessions", **o} for o in session_records]
    all_records = order_records + session_records
    records = [{"table_catalog": "demo", "table_schema": "analytics", **r} for r in all_records]
    return pd.DataFrame(records)


@pytest.fixture(scope="function")
def fresh_models():
    models = [ProjectReaderBase.read_yaml_file(p) for p in model_paths]
    return models


@pytest.fixture(scope="function")
def fresh_topics():
    topics = [ProjectReaderBase.read_yaml_file(p) for p in topic_paths]
    return topics


@pytest.fixture(scope="function")
def fresh_views():
    views = [ProjectReaderBase.read_yaml_file(path) for path in view_paths]
    return views


@pytest.fixture(scope="function")
def fresh_dashboards():
    dashboards = [ProjectReaderBase.read_yaml_file(path) for path in dashboard_paths]
    return dashboards


@pytest.fixture(scope="module")
def models():
    models = [ProjectReaderBase.read_yaml_file(p) for p in model_paths]
    return models


@pytest.fixture(scope="module")
def topics():
    topics = [ProjectReaderBase.read_yaml_file(p) for p in topic_paths]
    return topics


@pytest.fixture(scope="module")
def views():
    views = [ProjectReaderBase.read_yaml_file(path) for path in view_paths]
    return views


@pytest.fixture(scope="module")
def dashboards():
    dashboards = [ProjectReaderBase.read_yaml_file(path) for path in dashboard_paths]
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
def fresh_project(fresh_models, fresh_views, fresh_dashboards, fresh_topics, manifest):
    project = Project(
        models=fresh_models,
        views=fresh_views,
        dashboards=fresh_dashboards,
        topics=fresh_topics,
        looker_env="prod",
        connection_lookup={"connection_name": "SNOWFLAKE"},
        manifest=manifest,
    )
    return project


@pytest.fixture(scope="module")
def project(models, views, dashboards, topics, manifest):
    project = Project(
        models=models,
        views=views,
        dashboards=dashboards,
        topics=topics,
        looker_env="prod",
        connection_lookup={"connection_name": "SNOWFLAKE"},
        manifest=manifest,
    )
    return project


@pytest.fixture(scope="module")
def connections():
    class bq_mock(BaseConnection):
        name = "testing_bigquery"
        type = "BIGQUERY"
        database = "analytics"
        schema = "test_schema"

        def printable_attributes(self):
            return {"name": self.name, "type": self.type, "project_id": "fake-proj-id"}

    class sf_mock(BaseConnection):
        name = "testing_snowflake"
        type = "SNOWFLAKE"
        database = "analytics"
        schema = None

        def printable_attributes(self):
            return {
                "name": "testing_snowflake",
                "account": "blahblah.us-east-1",
                "user": "paul",
                "database": "analytics",
                "warehouse": "compute_wh",
                "role": "reporting",
            }

    class db_mock(BaseConnection):
        name = "testing_databricks"
        type = "DATABRICKS"
        database = None
        schema = None

        def printable_attributes(self):
            return {
                "name": "testing_databricks",
                "host": "blah.cloud.databricks.com",
                "http_path": "paul/testing/now",
            }

    sf = sf_mock()
    bq = bq_mock()
    db = db_mock()
    return [sf, bq, db]


@pytest.fixture(scope="module")
def connection(project, connections):
    return MetricsLayerConnection(project=project, connections=connections, verbose=True)
