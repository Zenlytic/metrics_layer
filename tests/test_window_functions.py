import pytest

from metrics_layer.core.exceptions import QueryError


@pytest.mark.query
def test_query_window_function_as_dimension(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_sequence"],
        where=[{"field": "sub_channel", "expression": "equal_to", "value": "Email"}],
    )

    correct = (
        "WITH order_lines_window_functions AS (SELECT dense_rank() over (partition by order_lines.customer_id"
        " order by DATE_TRUNC('DAY', order_lines.order_date) asc) as order_lines_order_sequence,order_lines.*"
        " FROM analytics.order_line_items order_lines) SELECT order_lines_order_sequence as"
        " order_lines_order_sequence,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " order_lines_window_functions order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id WHERE orders.sub_channel='Email' GROUP BY"
        " order_lines_order_sequence ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_window_function_as_filter_in_where(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["sub_channel"],
        where=[{"field": "order_sequence", "expression": "equal_to", "value": "1"}],
    )

    correct = (
        "WITH order_lines_window_functions AS (SELECT dense_rank() over (partition by "
        "order_lines.customer_id order by DATE_TRUNC('DAY', order_lines.order_date) asc) "
        "as order_lines_order_sequence,order_lines.* FROM analytics.order_line_items "
        "order_lines) SELECT orders.sub_channel as orders_sub_channel,SUM(order_lines.revenue) "
        "as order_lines_total_item_revenue FROM order_lines_window_functions order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "WHERE order_lines_order_sequence='1' GROUP BY orders.sub_channel "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_two_windows_in_same_view(connection):
    query = connection.get_sql_query(
        metrics=[],
        dimensions=["last_order_channel", "last_order_warehouse_location"],
        where=[{"field": "sub_channel", "expression": "equal_to", "value": "Email"}],
    )

    correct = (
        "WITH orders_window_functions AS (SELECT lag(orders.sub_channel) over (partition by"
        " customers.customer_id order by orders.order_date) as"
        " orders_last_order_channel,lag(orders.warehouselocation) over (partition by customers.customer_id"
        " order by orders.order_date) as orders_last_order_warehouse_location,orders.* FROM analytics.orders"
        " orders LEFT JOIN analytics.customers customers ON orders.customer_id=customers.customer_id) SELECT"
        " orders_last_order_channel as orders_last_order_channel,orders_last_order_warehouse_location as"
        " orders_last_order_warehouse_location FROM orders_window_functions orders LEFT JOIN"
        " analytics.customers customers ON orders.customer_id=customers.customer_id WHERE"
        " orders.sub_channel='Email' GROUP BY orders_last_order_channel,orders_last_order_warehouse_location"
        " ORDER BY orders_last_order_channel ASC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_two_windows_in_different_views(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_sequence", "last_order_channel"],
    )

    correct = (
        "WITH order_lines_window_functions AS (SELECT dense_rank() over (partition by order_lines.customer_id"
        " order by DATE_TRUNC('DAY', order_lines.order_date) asc) as order_lines_order_sequence,order_lines.*"
        " FROM analytics.order_line_items order_lines) ,orders_window_functions AS (SELECT"
        " lag(orders.sub_channel) over (partition by customers.customer_id order by orders.order_date) as"
        " orders_last_order_channel,orders.* FROM analytics.orders orders LEFT JOIN analytics.customers"
        " customers ON orders.customer_id=customers.customer_id) SELECT order_lines_order_sequence as"
        " order_lines_order_sequence,orders_last_order_channel as"
        " orders_last_order_channel,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " order_lines_window_functions order_lines LEFT JOIN orders_window_functions orders ON"
        " order_lines.order_unique_id=orders.id LEFT JOIN analytics.customers customers ON"
        " order_lines.customer_id=customers.customer_id GROUP BY"
        " order_lines_order_sequence,orders_last_order_channel ORDER BY order_lines_total_item_revenue DESC"
        " NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_window_function_as_measure(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue", "pct_of_total_item_revenue"],
        dimensions=["product_name"],
    )

    correct = (
        "SELECT order_lines.product_name as order_lines_product_name,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue,RATIO_TO_REPORT((SUM(order_lines.revenue))) OVER () as"
        " order_lines_pct_of_total_item_revenue FROM analytics.order_line_items order_lines GROUP BY"
        " order_lines.product_name ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_window_function_as_measure_and_dimension(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue", "pct_of_total_item_revenue"],
        dimensions=["new_vs_repeat_status"],
    )

    correct = (
        "WITH order_lines_window_functions AS (SELECT dense_rank() over (partition by order_lines.customer_id"
        " order by DATE_TRUNC('DAY', order_lines.order_date) asc) as order_lines_order_sequence,order_lines.*"
        " FROM analytics.order_line_items order_lines) SELECT case when order_lines_order_sequence = 1 then"
        " 'New' else 'Repeat' end as order_lines_new_vs_repeat_status,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue,RATIO_TO_REPORT((SUM(order_lines.revenue))) OVER () as"
        " order_lines_pct_of_total_item_revenue FROM order_lines_window_functions order_lines GROUP BY case"
        " when order_lines_order_sequence = 1 then 'New' else 'Repeat' end ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_window_function_as_dimension_in_where(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["product_name"],
        where=[{"field": "new_vs_repeat_status", "expression": "equal_to", "value": "New"}],
    )

    correct = (
        "WITH order_lines_window_functions AS (SELECT dense_rank() over (partition by order_lines.customer_id"
        " order by DATE_TRUNC('DAY', order_lines.order_date) asc) as order_lines_order_sequence,order_lines.*"
        " FROM analytics.order_line_items order_lines) SELECT order_lines.product_name as"
        " order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " order_lines_window_functions order_lines WHERE case when order_lines_order_sequence = 1 then 'New'"
        " else 'Repeat' end='New' GROUP BY order_lines.product_name ORDER BY order_lines_total_item_revenue"
        " DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_window_function_as_measure_in_where_with_nested_filter_syntax_error(connection):
    with pytest.raises(QueryError) as e:
        connection.get_sql_query(
            metrics=["pct_of_total_item_revenue"],
            dimensions=["product_name"],
            having=[
                {
                    "conditional_filter_logic": {
                        "conditions": [
                            {"field": "total_item_revenue", "expression": "less_than", "value": 100},
                            {
                                "conditions": [
                                    {
                                        "field": "pct_of_total_item_revenue",
                                        "expression": "less_than",
                                        "value": 0.1,
                                    },
                                    {
                                        "field": "total_item_revenue",
                                        "expression": "greater_than",
                                        "value": 100,
                                    },
                                ],
                                "logical_operator": "OR",
                            },
                        ],
                        "logical_operator": "AND",
                    }
                }
            ],
        )

    assert "Window functions filters cannot be nested. Please move the filter " in str(e.value)


@pytest.mark.query
def test_query_window_function_as_measure_in_where_with_nested_filter_syntax_error_or(connection):
    with pytest.raises(QueryError) as e:
        connection.get_sql_query(
            metrics=["pct_of_total_item_revenue"],
            dimensions=["product_name"],
            having=[
                {
                    "conditional_filter_logic": {
                        "conditions": [
                            {"field": "total_item_revenue", "expression": "equal_to", "value": 100},
                            {"field": "pct_of_total_item_revenue", "expression": "less_than", "value": 0.1},
                        ],
                        "logical_operator": "OR",
                    }
                }
            ],
        )

    assert "Window functions filters cannot be in OR statements." in str(e.value)


@pytest.mark.query
def test_query_window_function_as_measure_in_where_with_nested_filter_syntax_and_multiple_conditions(
    connection,
):
    query = connection.get_sql_query(
        metrics=["pct_of_total_item_revenue"],
        dimensions=["product_name"],
        having=[
            {
                "conditional_filter_logic": {
                    "conditions": [
                        {"field": "pct_of_total_item_revenue", "expression": "less_than", "value": 0.1},
                        {"field": "total_item_revenue", "expression": "equal_to", "value": 100},
                    ],
                    "logical_operator": "AND",
                }
            }
        ],
    )

    correct = (
        "WITH measure_window_functions AS (SELECT order_lines.product_name as"
        " order_lines_product_name,RATIO_TO_REPORT((SUM(order_lines.revenue))) OVER () as"
        " order_lines_pct_of_total_item_revenue FROM analytics.order_line_items order_lines GROUP BY"
        " order_lines.product_name HAVING SUM(order_lines.revenue)=100) SELECT * FROM"
        " measure_window_functions WHERE order_lines_pct_of_total_item_revenue<0.1 ORDER BY"
        " order_lines_pct_of_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_window_function_as_measure_in_having(connection):
    query = connection.get_sql_query(
        metrics=["pct_of_total_item_revenue"],
        dimensions=["product_name"],
        where=[{"field": "product_name", "expression": "equal_to", "value": "Product 1"}],
        having=[{"field": "pct_of_total_item_revenue", "expression": "greater_than", "value": 0.1}],
    )

    correct = (
        "WITH measure_window_functions AS (SELECT order_lines.product_name as"
        " order_lines_product_name,RATIO_TO_REPORT((SUM(order_lines.revenue))) OVER () as"
        " order_lines_pct_of_total_item_revenue FROM analytics.order_line_items order_lines WHERE"
        " order_lines.product_name='Product 1' GROUP BY order_lines.product_name) SELECT * FROM"
        " measure_window_functions WHERE order_lines_pct_of_total_item_revenue>0.1 ORDER BY"
        " order_lines_pct_of_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_window_function_as_measure_in_where(connection):
    with pytest.raises(QueryError) as e:
        connection.get_sql_query(
            metrics=["pct_of_total_item_revenue"],
            dimensions=["product_name"],
            where=[{"field": "pct_of_total_item_revenue", "expression": "less_than", "value": 0.1}],
        )

    assert "Window functions filters cannot be in WHERE clauses. Please move the filter" in str(e.value)


@pytest.mark.query
def test_query_window_function_as_measure_in_order_by(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue", "pct_of_total_item_revenue"],
        dimensions=["product_name"],
        order_by=[{"field": "pct_of_total_item_revenue", "direction": "asc"}],
    )

    correct = (
        "SELECT order_lines.product_name as order_lines_product_name,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue,RATIO_TO_REPORT((SUM(order_lines.revenue))) OVER () as"
        " order_lines_pct_of_total_item_revenue FROM analytics.order_line_items order_lines GROUP BY"
        " order_lines.product_name ORDER BY order_lines_pct_of_total_item_revenue ASC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_window_function_as_measure_dependent_on_dimension(connection):
    query = connection.get_sql_query(
        metrics=["number_of_new_purchased_items"],
        dimensions=["product_name"],
    )

    correct = (
        "WITH order_lines_window_functions AS (SELECT dense_rank() over (partition by order_lines.customer_id"
        " order by DATE_TRUNC('DAY', order_lines.order_date) asc) as order_lines_order_sequence,order_lines.*"
        " FROM analytics.order_line_items order_lines) SELECT order_lines.product_name as"
        " order_lines_product_name,COUNT(case when case when order_lines_order_sequence = 1 then 'New' else"
        " 'Repeat' end='New' then order_lines.order_id end) as order_lines_number_of_new_purchased_items FROM"
        " order_lines_window_functions order_lines GROUP BY order_lines.product_name ORDER BY"
        " order_lines_number_of_new_purchased_items DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_window_function_as_measure_dependent_on_dimension_in_having(connection):
    query = connection.get_sql_query(
        metrics=[],
        dimensions=["product_name"],
        having=[{"field": "number_of_new_purchased_items", "expression": "greater_than", "value": 3}],
    )

    correct = (
        "WITH order_lines_window_functions AS (SELECT dense_rank() over (partition by order_lines.customer_id"
        " order by DATE_TRUNC('DAY', order_lines.order_date) asc) as order_lines_order_sequence,order_lines.*"
        " FROM analytics.order_line_items order_lines) SELECT order_lines.product_name as"
        " order_lines_product_name FROM order_lines_window_functions order_lines GROUP BY"
        " order_lines.product_name HAVING COUNT(case when case when order_lines_order_sequence = 1 then 'New'"
        " else 'Repeat' end='New' then order_lines.order_id end)>3 ORDER BY order_lines_product_name ASC"
        " NULLS LAST;"
    )
    assert query == correct
