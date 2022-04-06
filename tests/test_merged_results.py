import datetime

import pytest

from metrics_layer.core.model.definitions import Definitions


@pytest.mark.query
@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery, Definitions.redshift])
def test_merged_result_query_additional_metric(connection, query_type):
    query = connection.get_sql_query(
        metrics=["revenue_per_session", "total_item_revenue", "number_of_sessions"],
        dimensions=["order_lines.order_month"],
        query_type=query_type,
        merged_result=True,
        verbose=True,
    )

    if query_type == Definitions.bigquery:
        order_date = "CAST(DATE_TRUNC(CAST(order_lines.order_date as DATE), MONTH) AS TIMESTAMP)"
        session_date = "CAST(DATE_TRUNC(CAST(sessions.session_date as DATE), MONTH) AS TIMESTAMP)"
        order_by = ""
        session_by = ""
    else:
        order_date = "DATE_TRUNC('MONTH', order_lines.order_date)"
        session_date = "DATE_TRUNC('MONTH', sessions.session_date)"
        order_by = " ORDER BY order_lines_total_item_revenue DESC"
        session_by = " ORDER BY sessions_number_of_sessions DESC"
    correct = (
        "WITH order_lines_all AS ("
        f"SELECT {order_date} as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        f"GROUP BY {order_date}"
        f"{order_by}) ,"
        "sessions AS ("
        f"SELECT {session_date} as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        f"GROUP BY {session_date}"
        f"{session_by}) "
        "SELECT order_lines_all.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        "sessions.sessions_number_of_sessions as sessions_number_of_sessions,"
        "order_lines_all.order_lines_order_month as order_lines_order_month,"
        "sessions.sessions_session_month as sessions_session_month,"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        "FROM order_lines_all JOIN sessions "
        "ON order_lines_all.order_lines_order_month=sessions.sessions_session_month;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("dim", ["order_lines.order_month", "sessions.session_month"])
def test_merged_result_query_only_metric(connection, dim):
    query = connection.get_sql_query(
        metrics=["revenue_per_session"],
        dimensions=[dim],
        merged_result=True,
        verbose=True,
    )

    if "order_month" in dim:
        date_seq = (
            "order_lines_all.order_lines_order_month as order_lines_order_month,"
            "sessions.sessions_session_month as sessions_session_month"
        )
    else:
        date_seq = (
            "sessions.sessions_session_month as sessions_session_month,"
            "order_lines_all.order_lines_order_month as order_lines_order_month"
        )

    correct = (
        "WITH order_lines_all AS ("
        f"SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        f"GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        "ORDER BY order_lines_total_item_revenue DESC) ,"
        "sessions AS ("
        f"SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        f"GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        "ORDER BY sessions_number_of_sessions DESC) "
        "SELECT order_lines_all.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"sessions.sessions_number_of_sessions as sessions_number_of_sessions,{date_seq},"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        "FROM order_lines_all JOIN sessions "
        "ON order_lines_all.order_lines_order_month=sessions.sessions_session_month;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_result_query_only_metric_no_dim(connection):
    query = connection.get_sql_query(
        metrics=["revenue_per_session"],
        dimensions=[],
        merged_result=True,
        verbose=True,
    )

    correct = (
        "WITH order_lines_all AS ("
        "SELECT SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "ORDER BY order_lines_total_item_revenue DESC) ,"
        "sessions AS ("
        "SELECT COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        "ORDER BY sessions_number_of_sessions DESC) "
        "SELECT order_lines_all.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        "sessions.sessions_number_of_sessions as sessions_number_of_sessions,"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        "FROM order_lines_all JOIN sessions ON 1=1;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_result_query_only_metric_with_where(connection):
    query = connection.get_sql_query(
        metrics=["revenue_per_session"],
        dimensions=["order_lines.order_month"],
        where=[
            {
                "field": "order_lines.order_raw",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
        merged_result=True,
        verbose=True,
    )

    date_seq = (
        "order_lines_all.order_lines_order_month as order_lines_order_month,"
        "sessions.sessions_session_month as sessions_session_month"
    )

    correct = (
        "WITH order_lines_all AS ("
        f"SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "WHERE order_lines.order_date>='2022-01-05T00:00:00' "
        f"GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        "ORDER BY order_lines_total_item_revenue DESC) ,"
        "sessions AS ("
        f"SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        "WHERE sessions.session_date>='2022-01-05T00:00:00' "
        f"GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        "ORDER BY sessions_number_of_sessions DESC) "
        "SELECT order_lines_all.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"sessions.sessions_number_of_sessions as sessions_number_of_sessions,{date_seq},"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        "FROM order_lines_all JOIN sessions "
        "ON order_lines_all.order_lines_order_month=sessions.sessions_session_month;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_result_query_only_metric_with_having(connection):
    query = connection.get_sql_query(
        metrics=["revenue_per_session"],
        dimensions=["order_lines.order_month"],
        having=[
            {
                "field": "revenue_per_session",
                "expression": "greater_or_equal_than",
                "value": 40,
            },
            {
                "field": "number_of_sessions",
                "expression": "less_than",
                "value": 5400,
            },
        ],
        merged_result=True,
        verbose=True,
    )

    date_seq = (
        "order_lines_all.order_lines_order_month as order_lines_order_month,"
        "sessions.sessions_session_month as sessions_session_month"
    )

    correct = (
        "WITH order_lines_all AS ("
        f"SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        f"GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        "ORDER BY order_lines_total_item_revenue DESC) ,"
        "sessions AS ("
        f"SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        f"GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        "ORDER BY sessions_number_of_sessions DESC) "
        "SELECT order_lines_all.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"sessions.sessions_number_of_sessions as sessions_number_of_sessions,{date_seq},"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        "FROM order_lines_all JOIN sessions "
        "ON order_lines_all.order_lines_order_month=sessions.sessions_session_month "
        "WHERE order_lines_revenue_per_session>=40 AND sessions_number_of_sessions<5400;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_result_query_with_non_component(connection):
    with pytest.raises(NotImplementedError) as exc_info:
        connection.get_sql_query(
            metrics=["revenue_per_session", "average_days_between_orders"],
            dimensions=["order_lines.order_month"],
            merged_result=True,
            verbose=True,
        )

    assert exc_info.value

    # correct = (
    #     "WITH order_lines_all AS ("
    #     f"SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
    #     "SUM(order_lines.revenue) as order_lines_total_item_revenue,"
    #     "(COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE("
    #     "DATEDIFF('DAY', orders.previous_order_date, orders.order_date)"
    #     ", 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id), "
    #     "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
    #     "- SUM(DISTINCT (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
    #     "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) "
    #     "AS DOUBLE PRECISION), 0) / NULLIF(COUNT(DISTINCT CASE WHEN  "
    #     "(DATEDIFF('DAY', orders.previous_order_date, orders.order_date))  IS NOT NULL "
    #     "THEN  orders.id  ELSE NULL END), 0)) as orders_average_days_between_orders "
    #     "FROM analytics.order_line_items order_lines "
    #     "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
    #     f"GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
    #     "ORDER BY order_lines_total_item_revenue DESC) ,"
    #     "sessions AS ("
    #     f"SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
    #     "COUNT(sessions.id) as sessions_number_of_sessions "
    #     "FROM analytics.sessions sessions "
    #     f"GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
    #     "ORDER BY sessions_number_of_sessions DESC) "
    #     "SELECT order_lines_all.order_lines_total_item_revenue as order_lines_total_item_revenue,"
    #     "order_lines_all.orders_average_days_between_orders as orders_average_days_between_orders,"
    #     "sessions.sessions_number_of_sessions as sessions_number_of_sessions,"
    #     "order_lines_all.order_lines_order_month as order_lines_order_month,"
    #     "sessions.sessions_session_month as sessions_session_month,"
    #     "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
    #     "FROM order_lines_all JOIN sessions "
    #     "ON order_lines_all.order_lines_order_month=sessions.sessions_session_month;"
    # )
    # assert query == correct
