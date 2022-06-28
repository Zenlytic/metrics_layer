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
    cte_1, cte_2 = "order_lines_order__subquery_0", "sessions_session__subquery_2"
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
        f"WITH {cte_1} AS ("
        f"SELECT {order_date} as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        f"GROUP BY {order_date}"
        f"{order_by}) ,"
        f"{cte_2} AS ("
        f"SELECT {session_date} as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        f"GROUP BY {session_date}"
        f"{session_by}) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"{cte_1}.order_lines_order_month as order_lines_order_month,"
        f"{cte_2}.sessions_session_month as sessions_session_month,"
        f"order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} JOIN {cte_2} "
        f"ON {cte_1}.order_lines_order_month={cte_2}.sessions_session_month;"
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

    cte_1, cte_2 = "order_lines_order__subquery_0", "sessions_session__subquery_2"
    if "order_month" in dim:
        date_seq = (
            f"{cte_1}.order_lines_order_month as order_lines_order_month,"
            f"{cte_2}.sessions_session_month as sessions_session_month"
        )
    else:
        date_seq = (
            f"{cte_1}.order_lines_order_month as order_lines_order_month,"
            f"{cte_2}.sessions_session_month as sessions_session_month"
        )

    correct = (
        f"WITH {cte_1} AS ("
        f"SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        f"GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        "ORDER BY order_lines_total_item_revenue DESC) ,"
        f"{cte_2} AS ("
        f"SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        f"GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        "ORDER BY sessions_number_of_sessions DESC) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,{date_seq},"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} JOIN {cte_2} "
        f"ON {cte_1}.order_lines_order_month={cte_2}.sessions_session_month;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_result_join_graph(connection):
    field = connection.get_field("revenue_per_session")
    assert field.join_graphs() == ["merged_result_order_lines.revenue_per_session"]

    field = connection.get_field("total_item_revenue")
    assert field.join_graphs() == ["subquery_0", "merged_result_order_lines.revenue_per_session"]

    field = connection.get_field("order_lines.order_date")
    assert field.join_graphs() == ["subquery_0", "merged_result_order_lines.revenue_per_session"]

    field = connection.get_field("orders.order_date")
    assert field.join_graphs() == ["subquery_0", "merged_result_discounts.discount_per_order"]

    field = connection.get_field("sub_channel")
    assert field.join_graphs() == ["subquery_0", "merged_result_order_lines.revenue_per_session"]

    field = connection.get_field("new_vs_repeat")
    assert field.join_graphs() == ["subquery_0"]

    field = connection.get_field("gender")
    assert field.join_graphs() == ["subquery_0", "subquery_1", "subquery_2"]

    field = connection.get_field("number_of_sessions")
    assert field.join_graphs() == ["subquery_2", "merged_result_order_lines.revenue_per_session"]

    field = connection.get_field("session_id")
    assert field.join_graphs() == ["subquery_2"]

    field = connection.get_field("traffic_id")
    assert field.join_graphs() == ["subquery_3"]


@pytest.mark.query
def test_merged_result_query_only_metric_no_dim(connection):
    query = connection.get_sql_query(
        metrics=["revenue_per_session"],
        dimensions=[],
        merged_result=True,
        verbose=True,
    )
    cte_1, cte_2 = "order_lines_order__subquery_0", "sessions_session__subquery_2"
    correct = (
        f"WITH {cte_1} AS ("
        "SELECT SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "ORDER BY order_lines_total_item_revenue DESC) ,"
        f"{cte_2} AS ("
        "SELECT COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        "ORDER BY sessions_number_of_sessions DESC) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} JOIN {cte_2} ON 1=1;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_result_query_ambig_explore(connection):
    query = connection.get_sql_query(
        metrics=["discount_per_order"],
        dimensions=[],
        merged_result=True,
        verbose=True,
    )

    cte_1, cte_2 = "discounts_order__subquery_0", "orders_order__subquery_0"
    correct = (
        f"WITH {cte_1} AS (SELECT SUM(discounts.discount_amt) as discounts_total_discount_amt "
        "FROM analytics_live.discounts discounts ORDER BY discounts_total_discount_amt DESC) ,"
        f"{cte_2} AS (SELECT COUNT(orders.id) as orders_number_of_orders "
        "FROM analytics.orders orders ORDER BY orders_number_of_orders DESC) "
        f"SELECT {cte_1}.discounts_total_discount_amt as discounts_total_discount_amt,"
        f"{cte_2}.orders_number_of_orders as orders_number_of_orders,"
        "discounts_total_discount_amt / nullif(orders_number_of_orders, 0) "
        f"as discounts_discount_per_order FROM {cte_1} JOIN {cte_2} ON 1=1;"
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

    cte_1, cte_2 = "order_lines_order__subquery_0", "sessions_session__subquery_2"
    date_seq = (
        f"{cte_1}.order_lines_order_month as order_lines_order_month,"
        f"{cte_2}.sessions_session_month as sessions_session_month"
    )

    correct = (
        f"WITH {cte_1} AS ("
        f"SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "WHERE order_lines.order_date>='2022-01-05T00:00:00' "
        f"GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        f"ORDER BY order_lines_total_item_revenue DESC) ,"
        f"{cte_2} AS ("
        f"SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        "WHERE sessions.session_date>='2022-01-05T00:00:00' "
        f"GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        f"ORDER BY sessions_number_of_sessions DESC) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,{date_seq},"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} JOIN {cte_2} "
        f"ON {cte_1}.order_lines_order_month={cte_2}.sessions_session_month;"
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

    cte_1, cte_2 = "order_lines_order__subquery_0", "sessions_session__subquery_2"
    date_seq = (
        f"{cte_1}.order_lines_order_month as order_lines_order_month,"
        f"{cte_2}.sessions_session_month as sessions_session_month"
    )

    correct = (
        f"WITH {cte_1} AS ("
        f"SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        f"GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        f"ORDER BY order_lines_total_item_revenue DESC) ,"
        f"{cte_2} AS ("
        f"SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        f"GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        f"ORDER BY sessions_number_of_sessions DESC) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,{date_seq},"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} JOIN {cte_2} "
        f"ON {cte_1}.order_lines_order_month={cte_2}.sessions_session_month "
        "WHERE order_lines_revenue_per_session>=40 AND sessions_number_of_sessions<5400;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_result_query_with_non_component(connection):
    query = connection.get_sql_query(
        metrics=["revenue_per_session", "average_days_between_orders"],
        dimensions=["order_lines.order_month"],
        merged_result=True,
        verbose=True,
    )

    cte_1, cte_2 = "order_lines_order__subquery_0", "sessions_session__subquery_2"
    cte_3 = "orders_previous_order__subquery_0"

    correct = (
        f"WITH {cte_1} AS (SELECT DATE_TRUNC('MONTH', order_lines.order_date) as "
        "order_lines_order_month,SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        "ORDER BY order_lines_total_item_revenue DESC) ,"
        f"{cte_2} AS (SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        "ORDER BY sessions_number_of_sessions DESC) ,"
        f"{cte_3} AS (SELECT DATE_TRUNC('MONTH', orders.previous_order_date) as orders_previous_order_month,"
        "AVG(DATEDIFF('DAY', orders.previous_order_date, orders.order_date)) as "
        "orders_average_days_between_orders FROM analytics.orders orders "
        "GROUP BY DATE_TRUNC('MONTH', orders.previous_order_date) "
        "ORDER BY orders_average_days_between_orders DESC) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_3}.orders_average_days_between_orders as orders_average_days_between_orders,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"{cte_1}.order_lines_order_month as order_lines_order_month,"
        f"{cte_3}.orders_previous_order_month as orders_previous_order_month,"
        f"{cte_2}.sessions_session_month as sessions_session_month,"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} JOIN {cte_3} "
        f"ON {cte_1}.order_lines_order_month={cte_3}.orders_previous_order_month "
        f"JOIN {cte_2} ON {cte_1}.order_lines_order_month={cte_2}.sessions_session_month;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_result_query_with_extra_dim(connection):
    query = connection.get_sql_query(
        metrics=["revenue_per_session", "average_days_between_orders"],
        dimensions=["order_lines.order_month", "utm_source"],  # maps to sub_channel in orders
        merged_result=True,
        verbose=True,
    )

    cte_1, cte_2 = "order_lines_order__subquery_0", "sessions_session__subquery_2"
    cte_3 = "orders_previous_order__subquery_0"

    correct = (
        f"WITH {cte_1} AS (SELECT DATE_TRUNC('MONTH', order_lines.order_date) as "
        "order_lines_order_month,orders.sub_channel as orders_sub_channel,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY DATE_TRUNC('MONTH', order_lines.order_date),orders.sub_channel "
        "ORDER BY order_lines_total_item_revenue DESC) ,"
        f"{cte_2} AS (SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "sessions.utm_source as sessions_utm_source,COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions GROUP BY DATE_TRUNC('MONTH', sessions.session_date),"
        "sessions.utm_source ORDER BY sessions_number_of_sessions DESC) ,"
        f"{cte_3} AS (SELECT DATE_TRUNC('MONTH', orders.previous_order_date) as orders_previous_order_month,"
        "orders.sub_channel as orders_sub_channel,"
        "AVG(DATEDIFF('DAY', orders.previous_order_date, orders.order_date)) as "
        "orders_average_days_between_orders FROM analytics.orders orders "
        "GROUP BY DATE_TRUNC('MONTH', orders.previous_order_date),orders.sub_channel "
        "ORDER BY orders_average_days_between_orders DESC) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_3}.orders_average_days_between_orders as orders_average_days_between_orders,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"{cte_1}.order_lines_order_month as order_lines_order_month,"
        f"{cte_1}.orders_sub_channel as orders_sub_channel,"
        f"{cte_3}.orders_previous_order_month as orders_previous_order_month,"
        f"{cte_3}.orders_sub_channel as orders_sub_channel,"
        f"{cte_2}.sessions_session_month as sessions_session_month,"
        f"{cte_2}.sessions_utm_source as sessions_utm_source,"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} JOIN {cte_3} "
        f"ON {cte_1}.order_lines_order_month={cte_3}.orders_previous_order_month "
        f"and {cte_1}.orders_sub_channel={cte_3}.orders_sub_channel "
        f"JOIN {cte_2} ON {cte_1}.order_lines_order_month={cte_2}.sessions_session_month "
        f"and {cte_1}.orders_sub_channel={cte_2}.sessions_utm_source;"
    )
    assert query == correct
