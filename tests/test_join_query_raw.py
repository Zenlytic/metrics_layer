import pytest

from metrics_layer.core.sql.query_errors import ArgumentError


@pytest.mark.query
def test_query_no_join_raw(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_lines.order_line_id", "channel"],
    )

    correct = (
        "SELECT order_lines.order_line_id as order_lines_order_line_id,"
        "order_lines.sales_channel as order_lines_channel"
        ",order_lines.revenue as order_lines_total_item_revenue FROM analytics.order_line_items order_lines;"
    )
    assert query == correct


@pytest.mark.query
def test_query_join_raw_force_group_by_pretty(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_lines.order_line_id", "channel"],
        force_group_by=True,
        pretty=True,
    )

    correct = """select order_lines.order_line_id as order_lines_order_line_id,
       order_lines.sales_channel as order_lines_channel,
       SUM(order_lines.revenue) as order_lines_total_item_revenue
from analytics.order_line_items order_lines
group by order_lines.order_line_id,
         order_lines.sales_channel
order by order_lines_total_item_revenue desc;"""

    assert query == correct


@pytest.mark.query
def test_query_single_join_non_base_primary_key(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["orders.order_id", "channel", "new_vs_repeat"],
    )

    correct = (
        "SELECT orders.id as orders_order_id,order_lines.sales_channel as order_lines_channel,"
        "orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
        "order_lines.order_unique_id=orders.id GROUP BY orders.id,order_lines.sales_channel,"
        "orders.new_vs_repeat ORDER BY order_lines_total_item_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_raw(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_lines.order_line_id", "channel", "new_vs_repeat"],
    )

    correct = (
        "SELECT order_lines.order_line_id as order_lines_order_line_id,"
        "order_lines.sales_channel as order_lines_channel,"
        "orders.new_vs_repeat as orders_new_vs_repeat,"
        "order_lines.revenue as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
        "order_lines.order_unique_id=orders.id;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_join_raw_select_args(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_lines.order_line_id", "channel", "new_vs_repeat"],
        select_raw_sql=[
            "CAST(new_vs_repeat = 'Repeat' AS INT) as group_1",
            "CAST(date_created > '2021-04-02' AS INT) as period",
        ],
    )

    correct = (
        "SELECT order_lines.order_line_id as order_lines_order_line_id,"
        "order_lines.sales_channel as order_lines_channel,"
        "orders.new_vs_repeat as orders_new_vs_repeat,"
        "order_lines.revenue as order_lines_total_item_revenue,"
        "CAST(new_vs_repeat = 'Repeat' AS INT) as group_1,"
        "CAST(date_created > '2021-04-02' AS INT) as period FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
        "order_lines.order_unique_id=orders.id;"
    )

    assert query == correct


@pytest.mark.query
def test_query_single_join_having_error(connection):
    with pytest.raises(ArgumentError) as exc_info:
        connection.get_sql_query(
            metrics=["total_item_revenue"],
            dimensions=["order_lines.order_line_id", "channel", "new_vs_repeat"],
            having=[{"field": "total_item_revenue", "expression": "less_than", "value": 22}],
        )

    assert exc_info.value


@pytest.mark.query
def test_query_single_join_order_by_error(connection):
    with pytest.raises(ArgumentError) as exc_info:
        connection.get_sql_query(
            metrics=["total_item_revenue"],
            dimensions=["order_lines.order_line_id", "channel", "new_vs_repeat"],
            order_by=[{"field": "total_item_revenue"}],
        )

    assert exc_info.value


@pytest.mark.query
def test_query_single_join_raw_all(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_lines.order_line_id", "channel", "new_vs_repeat"],
        where=[{"field": "new_vs_repeat", "expression": "equal_to", "value": "Repeat"}],
    )

    correct = (
        "SELECT order_lines.order_line_id as order_lines_order_line_id,"
        "order_lines.sales_channel as order_lines_channel,"
        "orders.new_vs_repeat as orders_new_vs_repeat,"
        "order_lines.revenue as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON "
        "order_lines.order_unique_id=orders.id WHERE orders.new_vs_repeat='Repeat';"
    )
    assert query == correct
