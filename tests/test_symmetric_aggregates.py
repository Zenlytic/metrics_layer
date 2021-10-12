import pytest

from granite.core.model.definitions import Definitions
from granite.core.query import get_sql_query


def test_query_count_no_sql(config):

    query = get_sql_query(metrics=["number_of_customers"], dimensions=["channel"], config=config)

    correct = (
        "SELECT order_lines.sales_channel as channel,COUNT(DISTINCT(customers.customer_id))"
        " as number_of_customers FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery])
def test_query_sum_with_sql(config, query_type):
    query = get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], config=config, query_type=query_type
    )

    if query_type == Definitions.snowflake:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.order_id), "
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(orders.order_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
    elif query_type == Definitions.bigquery:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS FLOAT64)) + "
            "CAST(FARM_FINGERPRINT(orders.order_id) AS BIGNUMERIC)) "
            "- SUM(DISTINCT CAST(FARM_FINGERPRINT(orders.order_id) AS BIGNUMERIC))) AS FLOAT64) "
            "/ CAST((1000000*1.0) AS FLOAT64), 0)"
        )
    correct = (
        "SELECT order_lines.sales_channel as channel,"
        f"{sa} as total_revenue "
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


def test_query_count_with_sql(config):

    query = get_sql_query(metrics=["number_of_orders"], dimensions=["channel"], config=config)

    correct = (
        "SELECT order_lines.sales_channel as channel,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.order_id)  IS NOT NULL THEN  orders.order_id  ELSE NULL END), 0)"
        " as number_of_orders FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery])
def test_query_average_with_sql(config, query_type: str):
    query = get_sql_query(
        metrics=["average_order_value"], dimensions=["channel"], config=config, query_type=query_type
    )

    if query_type == Definitions.snowflake:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.order_id), "
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(orders.order_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
    elif query_type == Definitions.bigquery:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS FLOAT64)) + "
            "CAST(FARM_FINGERPRINT(orders.order_id) AS BIGNUMERIC)) "
            "- SUM(DISTINCT CAST(FARM_FINGERPRINT(orders.order_id) AS BIGNUMERIC))) AS FLOAT64) "
            "/ CAST((1000000*1.0) AS FLOAT64), 0)"
        )

    correct = (
        "SELECT order_lines.sales_channel as channel,"
        f"({sa_sum} / NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.revenue)  IS NOT NULL THEN  orders.order_id  ELSE NULL END), 0))"
        " as average_order_value FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.bigquery, Definitions.snowflake])
def test_query_number_with_sql(config, query_type):
    query = get_sql_query(
        metrics=["total_sessions_divide"], dimensions=["channel"], config=config, query_type=query_type
    )

    if query_type == Definitions.snowflake:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(customers.total_sessions, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(customers.customer_id), "
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(customers.customer_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
    elif query_type == Definitions.bigquery:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(customers.total_sessions, 0) "
            "* (1000000 * 1.0)) AS FLOAT64)) + "
            "CAST(FARM_FINGERPRINT(customers.customer_id) AS BIGNUMERIC)) "
            "- SUM(DISTINCT CAST(FARM_FINGERPRINT(customers.customer_id) AS BIGNUMERIC))) AS FLOAT64) "
            "/ CAST((1000000*1.0) AS FLOAT64), 0)"
        )

    correct = (
        "SELECT order_lines.sales_channel as channel,"
        f"{sa_sum} / (100 * 1.0)"
        " as total_sessions_divide FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct
