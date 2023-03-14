import datetime
import pytest

from metrics_layer.core.exceptions import QueryError


@pytest.mark.query
def test_mapping_date_only(connection):
    query = connection.get_sql_query(metrics=[], dimensions=["date"])

    correct = (
        "SELECT DATE_TRUNC('DAY', order_lines.order_date) as order_lines_order_date "
        "FROM analytics.order_line_items order_lines GROUP BY DATE_TRUNC('DAY', "
        "order_lines.order_date) ORDER BY order_lines_order_date ASC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_dimension_only(connection):
    query = connection.get_sql_query(metrics=[], dimensions=["source"])

    correct = (
        "SELECT sessions.utm_source as sessions_utm_source FROM analytics.sessions "
        "sessions GROUP BY sessions.utm_source ORDER BY sessions_utm_source ASC;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("time_grain", ["date", "week", "month", "quarter", "year"])
def test_mapping_metric_mapped_date_and_filter(connection, time_grain):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=[time_grain],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
        verbose=True,
    )

    if time_grain == "date":
        date_part = "DATE_TRUNC('DAY', orders.order_date)"
    elif time_grain == "week":
        date_part = "DATE_TRUNC('WEEK', CAST(orders.order_date AS DATE))"
    elif time_grain == "month":
        date_part = "DATE_TRUNC('MONTH', orders.order_date)"
    elif time_grain == "quarter":
        date_part = "DATE_TRUNC('QUARTER', orders.order_date)"
    elif time_grain == "year":
        date_part = "DATE_TRUNC('YEAR', orders.order_date)"

    correct = (
        f"SELECT {date_part} as orders_order_{time_grain},"
        "COUNT(orders.id) as orders_number_of_orders FROM analytics.orders orders "
        "WHERE DATE_TRUNC('DAY', orders.order_date)>='2022-01-05T00:00:00' "
        f"GROUP BY {date_part} ORDER BY orders_number_of_orders DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_multiple_metric_same_canon_date_mapped_date_and_filter(connection):
    query = connection.get_sql_query(
        metrics=["line_item_aov", "gross_revenue"],
        dimensions=["date"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
        verbose=True,
    )
    correct = (
        "SELECT DATE_TRUNC('DAY', order_lines.order_date) as order_lines_order_date,"
        "(SUM(order_lines.revenue)) / (NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id)  "
        "IS NOT NULL THEN  orders.id  ELSE NULL END), 0)) as order_lines_line_item_aov,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM analytics.order_line_items "
        "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id"
        "=orders.id WHERE DATE_TRUNC('DAY', order_lines.order_date)>='2022-01-05T00:00:00' "
        "GROUP BY DATE_TRUNC('DAY', order_lines.order_date) "
        "ORDER BY order_lines_line_item_aov DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_multiple_metric_different_canon_date_merged_mapped_date_and_filter(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "number_of_sessions"],
        dimensions=["date"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            },
            {
                "field": "date",
                "expression": "less_than",
                "value": datetime.datetime(2023, 3, 5, 0, 0),
            },
        ],
        verbose=True,
    )
    correct = (
        "WITH orders_order__subquery_0 AS (SELECT DATE_TRUNC('DAY', orders.order_date) as "
        "orders_order_date,COUNT(orders.id) as orders_number_of_orders FROM analytics.orders "
        "orders WHERE DATE_TRUNC('DAY', orders.order_date)>='2022-01-05T00:00:00' AND "
        "DATE_TRUNC('DAY', orders.order_date)<'2023-03-05T00:00:00' "
        "GROUP BY DATE_TRUNC('DAY', orders.order_date) ORDER BY orders_number_of_orders DESC) ,"
        "sessions_session__subquery_2 AS (SELECT DATE_TRUNC('DAY', sessions.session_date) "
        "as sessions_session_date,COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions WHERE DATE_TRUNC('DAY', sessions.session_date)"
        ">='2022-01-05T00:00:00' AND DATE_TRUNC('DAY', sessions.session_date)<'2023-03-05T00:00:00' "
        "GROUP BY DATE_TRUNC('DAY', sessions.session_date) "
        "ORDER BY sessions_number_of_sessions DESC) SELECT orders_order__subquery_0."
        "orders_number_of_orders as orders_number_of_orders,sessions_session__subquery_2."
        "sessions_number_of_sessions as sessions_number_of_sessions,orders_order__subquery_0."
        "orders_order_date as orders_order_date,sessions_session__subquery_2.sessions_session_date "
        "as sessions_session_date FROM orders_order__subquery_0 FULL OUTER JOIN sessions_session__subquery_2 "
        "ON orders_order__subquery_0.orders_order_date=sessions_session__subquery_2.sessions_session_date;"
    )

    assert query == correct


@pytest.mark.query
def test_mapping_multiple_metric_different_canon_date_joinable_mapped_date_dim_and_filter(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "gross_revenue"],
        dimensions=["source", "date"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            },
            {
                "field": "date",
                "expression": "less_than",
                "value": datetime.datetime(2023, 3, 5, 0, 0),
            },
        ],
        verbose=True,
    )
    correct = (
        "WITH orders_order__subquery_0 AS (SELECT orders.sub_channel as orders_sub_channel,"
        "DATE_TRUNC('DAY', orders.order_date) as orders_order_date,COUNT(orders.id) as "
        "orders_number_of_orders FROM analytics.orders orders WHERE DATE_TRUNC('DAY', "
        "orders.order_date)>='2022-01-05T00:00:00' AND DATE_TRUNC('DAY', orders.order_date)"
        "<'2023-03-05T00:00:00' GROUP BY orders.sub_channel,DATE_TRUNC('DAY', orders.order_date) "
        "ORDER BY orders_number_of_orders DESC) ,"
        "order_lines_order__subquery_0 AS ("
        "SELECT orders.sub_channel as orders_sub_channel,DATE_TRUNC('DAY', order_lines.order_date) "
        "as order_lines_order_date,SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id WHERE DATE_TRUNC('DAY', "
        "order_lines.order_date)>='2022-01-05T00:00:00' AND DATE_TRUNC('DAY', "
        "order_lines.order_date)<'2023-03-05T00:00:00' GROUP BY orders.sub_channel,"
        "DATE_TRUNC('DAY', order_lines.order_date) ORDER BY order_lines_total_item_revenue DESC) "
        "SELECT order_lines_order__subquery_0.order_lines_total_item_revenue as "
        "order_lines_total_item_revenue,orders_order__subquery_0.orders_number_of_orders "
        "as orders_number_of_orders,ifnull(order_lines_order__subquery_0.orders_sub_channel, "
        "orders_order__subquery_0.orders_sub_channel) as orders_sub_channel,"
        "order_lines_order__subquery_0.order_lines_order_date as order_lines_order_date,"
        "orders_order__subquery_0.orders_order_date as orders_order_date "
        "FROM order_lines_order__subquery_0 FULL OUTER JOIN orders_order__subquery_0 "
        "ON order_lines_order__subquery_0.orders_sub_channel=orders_order__subquery_0"
        ".orders_sub_channel and order_lines_order__subquery_0.order_lines_order_date"
        "=orders_order__subquery_0.orders_order_date;"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_mapped_metric_joined_dim(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "average_customer_ltv"],
        dimensions=["channel"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
        verbose=True,
    )
    correct = (
        "WITH orders_order__subquery_0 AS (SELECT order_lines.sales_channel as order_lines_channel,"
        "NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id)  IS NOT NULL THEN  orders.id  ELSE NULL END), "
        "0) as orders_number_of_orders FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE "
        "DATE_TRUNC('DAY', orders.order_date)>='2022-01-05T00:00:00' GROUP BY "
        "order_lines.sales_channel ORDER BY orders_number_of_orders DESC) ,"
        "customers_first_order__subquery_0_subquery_1_subquery_2 AS (SELECT "
        "order_lines.sales_channel as order_lines_channel,(COALESCE(CAST((SUM(DISTINCT "
        "(CAST(FLOOR(COALESCE(customers.customer_ltv, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) "
        "+ (TO_NUMBER(MD5(customers.customer_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)"
        "::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(customers.customer_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) "
        "/ CAST((1000000*1.0) AS DOUBLE PRECISION), 0) / NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(customers.customer_ltv)  IS NOT NULL THEN  customers.customer_id  ELSE NULL END), "
        "0)) as customers_average_customer_ltv FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE DATE_TRUNC('DAY', customers.first_order_date)>='2022-01-05T00:00:00' "
        "GROUP BY order_lines.sales_channel ORDER BY customers_average_customer_ltv DESC) "
        "SELECT customers_first_order__subquery_0_subquery_1_subquery_2.customers_average_customer_ltv "
        "as customers_average_customer_ltv,orders_order__subquery_0.orders_number_of_orders "
        "as orders_number_of_orders,ifnull(customers_first_order__subquery_0_subquery_1_subquery_2."
        "order_lines_channel, orders_order__subquery_0.order_lines_channel) as order_lines_channel FROM "
        "customers_first_order__subquery_0_subquery_1_subquery_2 FULL OUTER JOIN "
        "orders_order__subquery_0 ON customers_first_order__subquery_0_subquery_1_subquery_2"
        ".order_lines_channel=orders_order__subquery_0.order_lines_channel;"
    )
    assert query == correct


@pytest.mark.query
def test_mapping_mapped_metric_mapped_date_and_filter(connection):
    query = connection.get_sql_query(
        metrics=["gross_revenue"],
        dimensions=["date"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            },
            {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
        ],
        verbose=True,
    )
    correct = (
        "SELECT DATE_TRUNC('DAY', order_lines.order_date) as order_lines_order_date,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id WHERE DATE_TRUNC('DAY', "
        "order_lines.order_date)>='2022-01-05T00:00:00' AND orders.new_vs_repeat='New' "
        "GROUP BY DATE_TRUNC('DAY', order_lines.order_date) "
        "ORDER BY order_lines_total_item_revenue DESC;"
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
        "WHERE orders.sub_channel='google' GROUP BY orders.sub_channel "
        "ORDER BY order_lines_total_item_revenue DESC;"
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
        order_by=[{"field": "gross_revenue", "sort": "asc"}, {"field": "source", "sort": "desc"}],
    )

    correct = (
        "SELECT orders.sub_channel as orders_sub_channel,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders.id)  IS NOT NULL THEN  orders.id  ELSE NULL END), 0) "
        "as orders_number_of_orders FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY orders.sub_channel HAVING SUM(order_lines.revenue)>200 "
        "ORDER BY order_lines_total_item_revenue ASC,orders_sub_channel DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_mapped_merged_results(connection):
    query = connection.get_sql_query(metrics=["gross_revenue", "number_of_sessions"], dimensions=["source"])

    correct = (
        "WITH order_lines_order__subquery_0 AS (SELECT orders.sub_channel as orders_sub_channel,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM analytics.order_line_items "
        "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY orders.sub_channel ORDER BY order_lines_total_item_revenue DESC) ,"
        "sessions_session__subquery_2 AS (SELECT sessions.utm_source as sessions_utm_source,"
        "COUNT(sessions.id) as sessions_number_of_sessions FROM analytics.sessions sessions "
        "GROUP BY sessions.utm_source ORDER BY sessions_number_of_sessions DESC) "
        "SELECT order_lines_order__subquery_0.order_lines_total_item_revenue as "
        "order_lines_total_item_revenue,sessions_session__subquery_2.sessions_number_of_sessions "
        "as sessions_number_of_sessions,order_lines_order__subquery_0.orders_sub_channel as "
        "orders_sub_channel,sessions_session__subquery_2.sessions_utm_source as "
        "sessions_utm_source FROM order_lines_order__subquery_0 FULL OUTER JOIN sessions_session__subquery_2 "
        "ON order_lines_order__subquery_0.orders_sub_channel"
        "=sessions_session__subquery_2.sessions_utm_source;"
    )
    assert query == correct


@pytest.mark.query
def test_mapped_metric_incorrect_error_message_on_mapped_filter(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_orders", "number_of_sessions"],
            dimensions=["source"],
            where=[
                {"field": "source", "expression": "equal_to", "value": "google"},
                {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
            ],
        )

    correct_error = (
        f"The field number_of_sessions could not be either joined into the query or mapped "
        "and merged into the query as a merged result. \n\nCheck that you specify joins to join it "
        "in, or specify a mapping for a query with two tables that cannot be merged"
    )
    assert exc_info.value
    assert str(exc_info.value) == correct_error

    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_orders", "number_of_sessions"],
            dimensions=["source", "sessions.session_id"],
            where=[
                {"field": "source", "expression": "equal_to", "value": "google"},
                {"field": "new_vs_repeat", "expression": "equal_to", "value": "New"},
            ],
        )

    correct_error = (
        f"The query could not be either joined or mapped and merged into a valid query with the fields:"
        "\n\nnumber_of_orders, number_of_sessions, sessions.session_id, new_vs_repeat, source\n\n"
        "Check that those fields can be joined together or are mapped so they can be merged across tables"
    )
    assert exc_info.value
    assert str(exc_info.value) == correct_error
