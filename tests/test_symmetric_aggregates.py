import pytest

from metrics_layer.core.model.definitions import Definitions


def test_query_count_no_sql(connection):
    query = connection.get_sql_query(metrics=["number_of_customers"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as channel,COUNT(DISTINCT(customers.customer_id))"
        " as number_of_customers FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery])
def test_query_sum_with_sql(connection, query_type):
    query = connection.get_sql_query(metrics=["total_revenue"], dimensions=["channel"], query_type=query_type)

    if query_type == Definitions.snowflake:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), "
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
    elif query_type == Definitions.bigquery:
        sa = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS FLOAT64)) + "
            "CAST(FARM_FINGERPRINT(orders.id) AS BIGNUMERIC)) "
            "- SUM(DISTINCT CAST(FARM_FINGERPRINT(orders.id) AS BIGNUMERIC))) AS FLOAT64) "
            "/ CAST((1000000*1.0) AS FLOAT64), 0)"
        )
    correct = (
        "SELECT order_lines.sales_channel as channel,"
        f"{sa} as total_revenue "
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


def test_query_count_with_sql(connection):
    query = connection.get_sql_query(metrics=["number_of_orders"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as channel,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.id)  IS NOT NULL THEN  orders.id  ELSE NULL END), 0)"
        " as number_of_orders FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


def test_query_count_with_one_to_many(connection):
    query = connection.get_sql_query(
        metrics=["number_of_email_purchased_items"], dimensions=["discount_code"]
    )

    correct = (
        "SELECT discounts.code as discount_code,NULLIF(COUNT(DISTINCT "
        "CASE WHEN  (case when order_lines.sales_channel = 'Email' then "
        "order_lines.order_id end)  IS NOT NULL THEN  order_lines.order_line_id  ELSE NULL "
        "END), 0) as number_of_email_purchased_items "
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics_live.discounts discounts ON orders.id=discounts.order_id "
        "GROUP BY discounts.code;"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery])
def test_query_average_with_sql(connection, query_type: str):
    query = connection.get_sql_query(
        metrics=["average_order_value"], dimensions=["channel"], query_type=query_type
    )

    if query_type == Definitions.snowflake:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), "
            "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
            "- SUM(DISTINCT (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
            "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
            "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0)"
        )
    elif query_type == Definitions.bigquery:
        sa_sum = (
            "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
            "* (1000000 * 1.0)) AS FLOAT64)) + "
            "CAST(FARM_FINGERPRINT(orders.id) AS BIGNUMERIC)) "
            "- SUM(DISTINCT CAST(FARM_FINGERPRINT(orders.id) AS BIGNUMERIC))) AS FLOAT64) "
            "/ CAST((1000000*1.0) AS FLOAT64), 0)"
        )

    correct = (
        "SELECT order_lines.sales_channel as channel,"
        f"({sa_sum} / NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.revenue)  IS NOT NULL THEN  orders.id  ELSE NULL END), 0))"
        " as average_order_value FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.bigquery, Definitions.snowflake])
def test_query_number_with_sql(connection, query_type):
    query = connection.get_sql_query(
        metrics=["total_sessions_divide"], dimensions=["channel"], query_type=query_type
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
