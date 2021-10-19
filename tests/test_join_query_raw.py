import pytest

from metrics_layer.core.sql.query_errors import ArgumentError


def test_query_no_join_raw(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_lines.order_line_id", "channel"],
    )

    correct = "SELECT order_lines.order_line_id as order_line_id,order_lines.sales_channel as channel"
    correct += ",order_lines.revenue as total_item_revenue FROM analytics.order_line_items order_lines;"
    assert query == correct


def test_query_single_join_non_base_primary_key(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["orders.order_id", "channel", "new_vs_repeat"],
    )

    correct = "SELECT orders.order_id as order_id,order_lines.sales_channel as channel,"
    correct += "orders.new_vs_repeat as new_vs_repeat,SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
    correct += "order_lines.order_id=orders.order_id GROUP BY orders.order_id,order_lines.sales_channel,"
    correct += "orders.new_vs_repeat;"
    assert query == correct


def test_query_single_join_raw(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_lines.order_line_id", "channel", "new_vs_repeat"],
    )

    correct = "SELECT order_lines.order_line_id as order_line_id,order_lines.sales_channel as channel,"
    correct += "orders.new_vs_repeat as new_vs_repeat,order_lines.revenue as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
    correct += "order_lines.order_id=orders.order_id;"
    assert query == correct


def test_query_single_join_raw_select_args(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_lines.order_line_id", "channel", "new_vs_repeat"],
        select_raw_sql=[
            "CAST(new_vs_repeat = 'Repeat' AS INT) as group_1",
            "CAST(date_created > '2021-04-02' AS INT) as period",
        ],
    )

    correct = "SELECT order_lines.order_line_id as order_line_id,order_lines.sales_channel as channel,"
    correct += "orders.new_vs_repeat as new_vs_repeat,"
    correct += "order_lines.revenue as total_item_revenue,"
    correct += "CAST(new_vs_repeat = 'Repeat' AS INT) as group_1,"
    correct += "CAST(date_created > '2021-04-02' AS INT) as period FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
    correct += "order_lines.order_id=orders.order_id;"

    assert query == correct


def test_query_single_join_having_error(connection):
    with pytest.raises(ArgumentError) as exc_info:
        connection.get_sql_query(
            metrics=["total_item_revenue"],
            dimensions=["order_lines.order_line_id", "channel", "new_vs_repeat"],
            having=[{"field": "total_item_revenue", "expression": "less_than", "value": 22}],
        )

    assert exc_info.value


def test_query_single_join_order_by_error(connection):
    with pytest.raises(ArgumentError) as exc_info:
        connection.get_sql_query(
            metrics=["total_item_revenue"],
            dimensions=["order_lines.order_line_id", "channel", "new_vs_repeat"],
            order_by=[{"field": "total_item_revenue"}],
        )

    assert exc_info.value


def test_query_single_join_raw_all(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_lines.order_line_id", "channel", "new_vs_repeat"],
        where=[{"field": "new_vs_repeat", "expression": "equal_to", "value": "Repeat"}],
    )

    correct = "SELECT order_lines.order_line_id as order_line_id,order_lines.sales_channel as channel,"
    correct += "orders.new_vs_repeat as new_vs_repeat,order_lines.revenue as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
    correct += "order_lines.order_id=orders.order_id WHERE orders.new_vs_repeat='Repeat';"
    assert query == correct
