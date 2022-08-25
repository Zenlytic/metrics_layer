import datetime

import pytest


@pytest.mark.query
def test_mapping_dimension_only(connection):
    query = connection.get_sql_query(metrics=[], dimensions=["source"])

    correct = (
        "SELECT sessions.utm_source as sessions_utm_source FROM analytics.sessions "
        "sessions GROUP BY sessions.utm_source ORDER BY sessions_utm_source ASC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_metric_mapped_dim(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["source"],
        where=[
            {
                "field": "order_lines.order_raw",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
        verbose=True,
    )

    correct = (
        "SELECT orders.sub_channel as orders_sub_channel,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.id)  IS NOT NULL THEN  orders.id  ELSE NULL END), 0) as orders_number_of_orders "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id WHERE order_lines.order_date"
        ">='2022-01-05T00:00:00' GROUP BY orders.sub_channel "
        "ORDER BY orders_number_of_orders DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_mapped_dim(connection):
    query = connection.get_sql_query(
        metrics=["gross_revenue"],
        dimensions=["source"],
        where=[{"field": "source", "expression": "equal_to", "value": "google"}],
    )

    correct = (
        "SELECT orders.sub_channel as orders_sub_channel,SUM(order_lines.revenue) as "
        "order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY orders.sub_channel ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_non_mapped_dim(connection):
    query = connection.get_sql_query(metrics=["gross_revenue"], dimensions=["channel"])

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) "
        "as order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "GROUP BY order_lines.sales_channel ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_mapped_dim_having(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["source"],
        having=[{"field": "gross_revenue", "expression": "greater_than", "value": 200}],
        order_by=[{"field": "gross_revenue", "sort": "desc"}],
    )

    correct = (
        "SELECT orders.sub_channel as orders_sub_channel,SUM(order_lines.revenue) as "
        "order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY orders.sub_channel ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_mapped_merged_results(connection):
    query = connection.get_sql_query(metrics=["gross_revenue", "number_of_sessions"], dimensions=["source"])

    correct = (
        "SELECT orders.sub_channel as orders_sub_channel,SUM(order_lines.revenue) as "
        "order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY orders.sub_channel ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct
