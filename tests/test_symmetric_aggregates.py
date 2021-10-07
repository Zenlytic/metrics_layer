# import pytest

from granite.core.query import get_sql_query


class config_mock:
    pass


def test_query_count_no_sql(project):
    config_mock.project = project
    query = get_sql_query(metrics=["number_of_customers"], dimensions=["channel"], config=config_mock)

    correct = (
        "SELECT order_lines.sales_channel as channel,COUNT(DISTINCT(customers.customer_id))"
        " as number_of_customers FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


def test_query_sum_with_sql(project):
    config_mock.project = project
    query = get_sql_query(metrics=["total_revenue"], dimensions=["channel"], config=config_mock)

    correct = (
        "SELECT order_lines.sales_channel as channel,"
        "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
        "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.order_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
        "- SUM(DISTINCT (TO_NUMBER(MD5(orders.order_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
        "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as total_revenue "
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


def test_query_count_with_sql(project):
    config_mock.project = project
    query = get_sql_query(metrics=["number_of_orders"], dimensions=["channel"], config=config_mock)

    correct = (
        "SELECT order_lines.sales_channel as channel,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.order_id)  IS NOT NULL THEN  orders.order_id  ELSE NULL END), 0)"
        " as number_of_orders FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


def test_query_average_with_sql(project):
    config_mock.project = project
    query = get_sql_query(metrics=["average_order_value"], dimensions=["channel"], config=config_mock)

    correct = (
        "SELECT order_lines.sales_channel as channel,"
        "(COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue, 0) "
        "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.order_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
        "- SUM(DISTINCT (TO_NUMBER(MD5(orders.order_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
        "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0) / NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.revenue)  IS NOT NULL THEN  orders.order_id  ELSE NULL END), 0))"
        " as average_order_value FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct


def test_query_number_with_sql(project):
    config_mock.project = project
    query = get_sql_query(metrics=["total_sessions_divide"], dimensions=["channel"], config=config_mock)

    correct = (
        "SELECT order_lines.sales_channel as channel,"
        "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(customers.total_sessions, 0) "
        "* (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(customers.customer_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
        "- SUM(DISTINCT (TO_NUMBER(MD5(customers.customer_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
        "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0) / (100 * 1.0)"
        " as total_sessions_divide FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY order_lines.sales_channel;"
    )
    assert query == correct
