import datetime

import pytest

from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.exceptions import JoinError, QueryError
from metrics_layer.core.model.definitions import Definitions


@pytest.mark.query
@pytest.mark.parametrize(
    "query_type",
    [
        Definitions.snowflake,
        Definitions.bigquery,
        Definitions.redshift,
        Definitions.duck_db,
        Definitions.postgres,
        Definitions.trino,
        Definitions.databricks,
    ],
)
def test_merged_result_query_additional_metric(connection, query_type):
    query = connection.get_sql_query(
        metrics=["revenue_per_session", "total_item_revenue", "number_of_sessions"],
        dimensions=["order_lines.order_month"],
        query_type=query_type,
        merged_result=True,
        verbose=True,
    )
    cte_1, cte_2 = "order_lines_order__cte_subquery_0", "sessions_session__cte_subquery_1"
    if query_type == Definitions.bigquery:
        order_date = "CAST(DATE_TRUNC(CAST(order_lines.order_date AS DATE), MONTH) AS DATE)"
        session_date = "CAST(DATE_TRUNC(CAST(sessions.session_date AS DATE), MONTH) AS TIMESTAMP)"

        order_by = ""
        session_by = ""
    elif query_type in {Definitions.postgres, Definitions.trino, Definitions.databricks, Definitions.duck_db}:
        order_date = "DATE_TRUNC('MONTH', CAST(order_lines.order_date AS TIMESTAMP))"
        session_date = "DATE_TRUNC('MONTH', CAST(sessions.session_date AS TIMESTAMP))"
        if query_type == Definitions.duck_db:
            order_by = " ORDER BY order_lines_total_item_revenue DESC NULLS LAST"
            session_by = " ORDER BY sessions_number_of_sessions DESC NULLS LAST"
        else:
            order_by = ""
            session_by = ""
    else:
        order_date = "DATE_TRUNC('MONTH', order_lines.order_date)"
        session_date = "DATE_TRUNC('MONTH', sessions.session_date)"
        order_by = " ORDER BY order_lines_total_item_revenue DESC NULLS LAST"
        session_by = " ORDER BY sessions_number_of_sessions DESC NULLS LAST"

    if Definitions.bigquery == query_type:
        on_statement = f"CAST({cte_1}.order_lines_order_month AS TIMESTAMP)=CAST({cte_2}.sessions_session_month AS TIMESTAMP)"  # noqa
    else:
        on_statement = f"{cte_1}.order_lines_order_month={cte_2}.sessions_session_month"

    if query_type == Definitions.redshift:
        ifnull = "nvl"
    elif query_type in {Definitions.postgres, Definitions.trino, Definitions.databricks, Definitions.duck_db}:
        ifnull = "coalesce"
    else:
        ifnull = "ifnull"

    correct = (
        f"WITH {cte_1} AS ("
        f"SELECT {order_date} as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        f"GROUP BY {order_date if query_type != Definitions.bigquery else 'order_lines_order_month'}"
        f"{order_by}) ,"
        f"{cte_2} AS ("
        f"SELECT {session_date} as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        f"GROUP BY {session_date if query_type != Definitions.bigquery else 'sessions_session_month'}"
        f"{session_by}) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"{ifnull}({cte_1}.order_lines_order_month, {cte_2}.sessions_session_month) as order_lines_order_month,"  # noqa
        f"{ifnull}({cte_2}.sessions_session_month, {cte_1}.order_lines_order_month) as sessions_session_month,"  # noqa
        f"order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} FULL OUTER JOIN {cte_2} "
        f"ON {on_statement}{';' if query_type != Definitions.trino else ''}"
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

    cte_1, cte_2 = "order_lines_order__cte_subquery_0", "sessions_session__cte_subquery_1"
    if "order_month" in dim:
        date_seq = (
            f"ifnull({cte_1}.order_lines_order_month, {cte_2}.sessions_session_month) as order_lines_order_month,"  # noqa
            f"ifnull({cte_2}.sessions_session_month, {cte_1}.order_lines_order_month) as sessions_session_month"  # noqa
        )
    else:
        date_seq = (
            f"ifnull({cte_1}.order_lines_order_month, {cte_2}.sessions_session_month) as order_lines_order_month,"  # noqa
            f"ifnull({cte_2}.sessions_session_month, {cte_1}.order_lines_order_month) as sessions_session_month"  # noqa
        )

    correct = (
        f"WITH {cte_1} AS ("
        f"SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        f"GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST) ,"
        f"{cte_2} AS ("
        f"SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        f"GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        "ORDER BY sessions_number_of_sessions DESC NULLS LAST) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,{date_seq},"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} FULL OUTER JOIN {cte_2} "
        f"ON {cte_1}.order_lines_order_month={cte_2}.sessions_session_month;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_result_join_graph(connection):
    def _blow_out_by_time_frame(join_graph: str, tf: list):
        return [f"{join_graph}_{tf}" for tf in tf]

    tf = [
        "date",
        "day_of_week",
        "day_of_year",
        "hour_of_day",
        "month",
        "month_of_year",
        "quarter",
        "raw",
        "time",
        "week",
        "week_of_year",
        "year",
    ]
    core_tf = ["raw", "time", "date", "week", "month", "quarter", "year"]
    sub_q_cr = _blow_out_by_time_frame("m0_merged_result_canon_date_core", core_tf)
    sub_q_0_4 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_4", core_tf)
    sub_q_0_2 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_7", core_tf)
    sub_q_0_3 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_3", core_tf)
    sub_q_0_5 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_5", core_tf)
    sub_q_0_8 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_8", core_tf)
    sub_q_0_9 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_9", core_tf)
    sub_q_0_10 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_10", core_tf)
    sub_q_0_11 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_11", core_tf)
    sub_q_0_12 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_12", core_tf)
    sub_q_0_14 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_14", core_tf)
    sub_q_0_15 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_15", core_tf)
    sub_q_0_1 = _blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_1", core_tf)
    revenue_set = [
        *sub_q_cr,
        *sub_q_0_4,
        *sub_q_0_2,
        *sub_q_0_3,
        *sub_q_0_5,
        *sub_q_0_8,
        *sub_q_0_9,
        *sub_q_0_10,
        *sub_q_0_11,
        *sub_q_0_12,
        *sub_q_0_14,
        *sub_q_0_15,
        *sub_q_0_1,
    ]
    field = connection.get_field("revenue_per_session")
    assert sorted(field.join_graphs()) == sorted(revenue_set)

    field = connection.get_field("total_item_revenue")
    assert field.join_graphs() == list(sorted(["subquery_0", *revenue_set]))

    field = connection.get_field("order_lines.order_date")
    order_lines_date_graphs = [
        "subquery_0",
        "m0_merged_result_canon_date_core_date",
        "m0_merged_result_subquery_0_subquery_10_date",
        "m0_merged_result_subquery_0_subquery_11_date",
        "m0_merged_result_subquery_0_subquery_12_date",
        "m0_merged_result_subquery_0_subquery_14_date",
        "m0_merged_result_subquery_0_subquery_15_date",
        "m0_merged_result_subquery_0_subquery_1_date",
        "m0_merged_result_subquery_0_subquery_4_date",
        "m0_merged_result_subquery_0_subquery_7_date",
        "m0_merged_result_subquery_0_subquery_3_date",
        "m0_merged_result_subquery_0_subquery_5_date",
        "m0_merged_result_subquery_0_subquery_8_date",
        "m0_merged_result_subquery_0_subquery_9_date",
    ]
    assert field.join_graphs() == list(sorted(order_lines_date_graphs))

    field = connection.get_field("orders.order_date")
    order_date_graphs = [
        "subquery_0",
        "m0_merged_result_canon_date_core_date",
        "m0_merged_result_subquery_0_subquery_10_date",
        "m0_merged_result_subquery_0_subquery_11_date",
        "m0_merged_result_subquery_0_subquery_12_date",
        "m0_merged_result_subquery_0_subquery_14_date",
        "m0_merged_result_subquery_0_subquery_15_date",
        "m0_merged_result_subquery_0_subquery_1_date",
        "m0_merged_result_subquery_0_subquery_4_date",
        "m0_merged_result_subquery_0_subquery_7_date",
        "m0_merged_result_subquery_0_subquery_3_date",
        "m0_merged_result_subquery_0_subquery_5_date",
        "m0_merged_result_subquery_0_subquery_8_date",
        "m0_merged_result_subquery_0_subquery_9_date",
    ]
    assert field.join_graphs() == list(sorted(order_date_graphs))
    field = connection.get_field("sub_channel")

    sub_channel_graphs = [
        "subquery_0",
        *_blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_4", tf),
    ]
    assert field.join_graphs() == list(sorted(sub_channel_graphs))

    field = connection.get_field("new_vs_repeat")
    assert field.join_graphs() == sorted(["subquery_0"])

    # discount_tf = ["date", "month", "quarter", "raw", "time", "week", "year"]
    field = connection.get_field("gender")
    gender_graphs = [
        "subquery_0",
        "subquery_3",
        "subquery_1",
        "subquery_4",
        "subquery_11",
        "subquery_12",
        "subquery_10",
        *_blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_1", tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_1_subquery_3", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_1_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_1_subquery_12", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_1_subquery_11", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_3", tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_12", tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_12_subquery_3", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_12_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_11_subquery_12", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_11_subquery_3", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_3_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_4", tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_11", tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_10_subquery_3", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_10_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_10_subquery_11", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_10_subquery_12", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_1_subquery_10", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_10", tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_11_subquery_4", core_tf),
    ]
    assert field.join_graphs() == list(sorted(gender_graphs))

    field = connection.get_field("number_of_sessions")
    sessions_graphs = [
        "subquery_4",
        *_blow_out_by_time_frame("m0_merged_result_canon_date_core", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_1_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_10_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_11_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_12_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_0_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_3_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_4_subquery_7", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_4_subquery_5", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_14_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_15_subquery_4", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_4_subquery_8", core_tf),
        *_blow_out_by_time_frame("m0_merged_result_subquery_4_subquery_9", core_tf),
    ]
    assert field.join_graphs() == list(sorted(sessions_graphs))

    field = connection.get_field("sessions.session_id")
    assert field.join_graphs() == ["subquery_4"]

    field = connection.get_field("traffic_id")
    assert field.join_graphs() == ["subquery_2"]


@pytest.mark.query
@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.redshift])
def test_merged_result_query_only_metric_no_dim(connection, query_type):
    query = connection.get_sql_query(
        metrics=["revenue_per_session"],
        dimensions=[],
        merged_result=True,
        verbose=True,
        query_type=query_type,
    )
    cte_1, cte_2 = "order_lines_order__cte_subquery_0", "sessions_session__cte_subquery_1"

    if query_type == Definitions.redshift:
        join_statement = f"{cte_1} CROSS JOIN {cte_2}"
    else:
        join_statement = f"{cte_1} FULL OUTER JOIN {cte_2} ON 1=1"
    correct = (
        f"WITH {cte_1} AS ("
        "SELECT SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST) ,"
        f"{cte_2} AS ("
        "SELECT COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        "ORDER BY sessions_number_of_sessions DESC NULLS LAST) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {join_statement};"
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

    cte_1, cte_2 = "discounts_order__cte_subquery_0", "orders_order__cte_subquery_1"
    correct = (
        f"WITH {cte_1} AS (SELECT SUM(discounts.discount_amt) as discounts_total_discount_amt "
        "FROM analytics_live.discounts discounts ORDER BY discounts_total_discount_amt DESC NULLS LAST) ,"
        f"{cte_2} AS (SELECT COUNT(orders.id) as orders_number_of_orders "
        "FROM analytics.orders orders ORDER BY orders_number_of_orders DESC NULLS LAST) "
        f"SELECT {cte_1}.discounts_total_discount_amt as discounts_total_discount_amt,"
        f"{cte_2}.orders_number_of_orders as orders_number_of_orders,"
        "discounts_total_discount_amt / nullif(orders_number_of_orders, 0) "
        f"as discounts_discount_per_order FROM {cte_1} FULL OUTER JOIN {cte_2} ON 1=1;"
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

    cte_1, cte_2 = "order_lines_order__cte_subquery_0", "sessions_session__cte_subquery_1"
    date_seq = (
        f"ifnull({cte_1}.order_lines_order_month, {cte_2}.sessions_session_month) as order_lines_order_month,"
        f"ifnull({cte_2}.sessions_session_month, {cte_1}.order_lines_order_month) as sessions_session_month"
    )

    correct = (
        f"WITH {cte_1} AS ("
        f"SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "WHERE order_lines.order_date>='2022-01-05T00:00:00' "
        f"GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        f"ORDER BY order_lines_total_item_revenue DESC NULLS LAST) ,"
        f"{cte_2} AS ("
        f"SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        "WHERE sessions.session_date>='2022-01-05T00:00:00' "
        f"GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        f"ORDER BY sessions_number_of_sessions DESC NULLS LAST) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,{date_seq},"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} FULL OUTER JOIN {cte_2} "
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

    cte_1, cte_2 = "order_lines_order__cte_subquery_0", "sessions_session__cte_subquery_1"
    date_seq = (
        f"ifnull({cte_1}.order_lines_order_month, {cte_2}.sessions_session_month) as order_lines_order_month,"
        f"ifnull({cte_2}.sessions_session_month, {cte_1}.order_lines_order_month) as sessions_session_month"
    )

    correct = (
        f"WITH {cte_1} AS ("
        f"SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        f"GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        f"ORDER BY order_lines_total_item_revenue DESC NULLS LAST) ,"
        f"{cte_2} AS ("
        f"SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions "
        f"GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        f"ORDER BY sessions_number_of_sessions DESC NULLS LAST) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,{date_seq},"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} FULL OUTER JOIN {cte_2} "
        f"ON {cte_1}.order_lines_order_month={cte_2}.sessions_session_month "
        "WHERE order_lines_revenue_per_session>=40 AND sessions_number_of_sessions<5400;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_result_query_metric_with_having_non_selected(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["sessions.utm_source"],
        having=[
            {"field": "number_of_sessions", "expression": "less_than", "value": 5400},
        ],
    )

    cte_1, cte_2 = "order_lines_order__cte_subquery_0", "sessions_session__cte_subquery_1"

    correct = (
        f"WITH {cte_1} AS (SELECT orders.sub_channel as orders_sub_channel,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.orders orders ON order_lines.order_unique_id=orders.id GROUP BY orders.sub_channel ORDER"
        f" BY order_lines_total_item_revenue DESC NULLS LAST) ,{cte_2} AS (SELECT sessions.utm_source as"
        " sessions_utm_source,COUNT(sessions.id) as sessions_number_of_sessions FROM analytics.sessions"
        " sessions GROUP BY sessions.utm_source ORDER BY sessions_number_of_sessions DESC NULLS LAST) SELECT"
        f" {cte_1}.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue,sessions_session__cte_subquery_1.sessions_number_of_sessions as"
        f" sessions_number_of_sessions,ifnull({cte_1}.orders_sub_channel, {cte_2}.sessions_utm_source) as"
        f" orders_sub_channel,ifnull({cte_2}.sessions_utm_source, {cte_1}.orders_sub_channel) as"
        f" sessions_utm_source FROM {cte_1} FULL OUTER JOIN {cte_2} ON"
        f" {cte_1}.orders_sub_channel={cte_2}.sessions_utm_source WHERE sessions_number_of_sessions<5400;"
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

    cte_1, cte_2 = "order_lines_order__cte_subquery_0", "sessions_session__cte_subquery_2"
    cte_3 = "orders_previous_order__cte_subquery_1"

    prev_orders = "ifnull(orders_previous_order__cte_subquery_1.orders_previous_order_month, ifnull(order_lines_order__cte_subquery_0.order_lines_order_month, sessions_session__cte_subquery_2.sessions_session_month))"  # noqa
    orders = "ifnull(order_lines_order__cte_subquery_0.order_lines_order_month, ifnull(orders_previous_order__cte_subquery_1.orders_previous_order_month, sessions_session__cte_subquery_2.sessions_session_month))"  # noqa
    sessions = "ifnull(sessions_session__cte_subquery_2.sessions_session_month, ifnull(order_lines_order__cte_subquery_0.order_lines_order_month, orders_previous_order__cte_subquery_1.orders_previous_order_month))"  # noqa
    correct = (
        f"WITH {cte_1} AS (SELECT DATE_TRUNC('MONTH', order_lines.order_date) as "
        "order_lines_order_month,SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST) ,"
        f"{cte_2} AS (SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        "ORDER BY sessions_number_of_sessions DESC NULLS LAST) ,"
        f"{cte_3} AS (SELECT DATE_TRUNC('MONTH', orders.previous_order_date) as orders_previous_order_month,"
        "AVG(DATEDIFF('DAY', orders.previous_order_date, orders.order_date)) as "
        "orders_average_days_between_orders FROM analytics.orders orders "
        "GROUP BY DATE_TRUNC('MONTH', orders.previous_order_date) "
        "ORDER BY orders_average_days_between_orders DESC NULLS LAST) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_3}.orders_average_days_between_orders as orders_average_days_between_orders,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"{orders} as order_lines_order_month,"
        f"{prev_orders} as orders_previous_order_month,"
        f"{sessions} as sessions_session_month,"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} FULL OUTER JOIN {cte_3} "
        f"ON {cte_1}.order_lines_order_month={cte_3}.orders_previous_order_month "
        f"FULL OUTER JOIN {cte_2} ON {cte_1}.order_lines_order_month={cte_2}.sessions_session_month;"
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

    cte_1, cte_2 = "order_lines_order__cte_subquery_0", "sessions_session__cte_subquery_2"
    cte_3 = "orders_previous_order__cte_subquery_1"

    prev_orders = "ifnull(orders_previous_order__cte_subquery_1.orders_previous_order_month, ifnull(order_lines_order__cte_subquery_0.order_lines_order_month, sessions_session__cte_subquery_2.sessions_session_month))"  # noqa
    orders = "ifnull(order_lines_order__cte_subquery_0.order_lines_order_month, ifnull(orders_previous_order__cte_subquery_1.orders_previous_order_month, sessions_session__cte_subquery_2.sessions_session_month))"  # noqa
    sessions = "ifnull(sessions_session__cte_subquery_2.sessions_session_month, ifnull(order_lines_order__cte_subquery_0.order_lines_order_month, orders_previous_order__cte_subquery_1.orders_previous_order_month))"  # noqa

    sessions_source = "ifnull(sessions_session__cte_subquery_2.sessions_utm_source, ifnull(order_lines_order__cte_subquery_0.orders_sub_channel, orders_previous_order__cte_subquery_1.orders_sub_channel))"  # noqa
    order_lines_source = "ifnull(orders_previous_order__cte_subquery_1.orders_sub_channel, ifnull(sessions_session__cte_subquery_2.sessions_utm_source, sessions_session__cte_subquery_2.sessions_utm_source))"  # noqa
    correct = (
        f"WITH {cte_1} AS (SELECT DATE_TRUNC('MONTH', order_lines.order_date) as "
        "order_lines_order_month,orders.sub_channel as orders_sub_channel,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY DATE_TRUNC('MONTH', order_lines.order_date),orders.sub_channel "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST) ,"
        f"{cte_2} AS (SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "sessions.utm_source as sessions_utm_source,COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions GROUP BY DATE_TRUNC('MONTH', sessions.session_date),"
        "sessions.utm_source ORDER BY sessions_number_of_sessions DESC NULLS LAST) ,"
        f"{cte_3} AS (SELECT DATE_TRUNC('MONTH', orders.previous_order_date) as orders_previous_order_month,"
        "orders.sub_channel as orders_sub_channel,"
        "AVG(DATEDIFF('DAY', orders.previous_order_date, orders.order_date)) as "
        "orders_average_days_between_orders FROM analytics.orders orders "
        "GROUP BY DATE_TRUNC('MONTH', orders.previous_order_date),orders.sub_channel "
        "ORDER BY orders_average_days_between_orders DESC NULLS LAST) "
        f"SELECT {cte_1}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{cte_3}.orders_average_days_between_orders as orders_average_days_between_orders,"
        f"{cte_2}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"{orders} as order_lines_order_month,"
        f"{order_lines_source} as orders_sub_channel,"
        f"{prev_orders} as orders_previous_order_month,"
        f"{sessions} as sessions_session_month,"
        f"{sessions_source} as sessions_utm_source,"
        "order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0) as order_lines_revenue_per_session "  # noqa
        f"FROM {cte_1} FULL OUTER JOIN {cte_3} "
        f"ON {cte_1}.order_lines_order_month={cte_3}.orders_previous_order_month "
        f"and {cte_1}.orders_sub_channel={cte_3}.orders_sub_channel "
        f"FULL OUTER JOIN {cte_2} ON {cte_1}.order_lines_order_month={cte_2}.sessions_session_month "
        f"and {cte_1}.orders_sub_channel={cte_2}.sessions_utm_source;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_implicit_with_subgraph(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "number_of_sessions"], dimensions=["orders.order_month"]
    )

    orders_cte = "orders_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"

    correct = (
        f"WITH {orders_cte} AS (SELECT DATE_TRUNC('MONTH', orders.order_date) as"
        " orders_order_month,COUNT(orders.id) as orders_number_of_orders FROM analytics.orders orders GROUP"
        " BY DATE_TRUNC('MONTH', orders.order_date) ORDER BY orders_number_of_orders DESC NULLS LAST)"
        f" ,{sessions_cte} AS (SELECT DATE_TRUNC('MONTH', sessions.session_date) as"
        " sessions_session_month,COUNT(sessions.id) as sessions_number_of_sessions FROM analytics.sessions"
        " sessions GROUP BY DATE_TRUNC('MONTH', sessions.session_date) ORDER BY sessions_number_of_sessions"
        f" DESC NULLS LAST) SELECT {orders_cte}.orders_number_of_orders as"
        f" orders_number_of_orders,{sessions_cte}.sessions_number_of_sessions as"
        f" sessions_number_of_sessions,ifnull({orders_cte}.orders_order_month,"
        f" {sessions_cte}.sessions_session_month) as"
        f" orders_order_month,ifnull({sessions_cte}.sessions_session_month, {orders_cte}.orders_order_month)"
        f" as sessions_session_month FROM {orders_cte} FULL OUTER JOIN {sessions_cte} ON"
        f" {orders_cte}.orders_order_month={sessions_cte}.sessions_session_month;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_implicit_with_subgraph_and_mapping(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "number_of_sessions"],
        dimensions=["orders.order_month", "sub_channel", "utm_campaign"],
        where=[
            {
                "field": "sessions.session_raw",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
    )

    orders_cte = "orders_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"
    orders_source = f"ifnull({orders_cte}.orders_sub_channel, {sessions_cte}.sessions_utm_source)"
    orders_campaign = f"ifnull({orders_cte}.orders_campaign, {sessions_cte}.sessions_utm_campaign)"
    sessions_source = f"ifnull({sessions_cte}.sessions_utm_source, {orders_cte}.orders_sub_channel)"
    sessions_campaign = f"ifnull({sessions_cte}.sessions_utm_campaign, {orders_cte}.orders_campaign)"
    correct = (
        f"WITH {orders_cte} AS ("
        "SELECT DATE_TRUNC('MONTH', orders.order_date) as orders_order_month,"
        "orders.sub_channel as orders_sub_channel,orders.campaign as orders_campaign,"
        "COUNT(orders.id) as orders_number_of_orders FROM analytics.orders orders "
        "WHERE orders.order_date>='2022-01-05T00:00:00' "
        "GROUP BY DATE_TRUNC('MONTH', orders.order_date),orders.sub_channel,orders.campaign "
        f"ORDER BY orders_number_of_orders DESC NULLS LAST) ,{sessions_cte} AS ("
        "SELECT DATE_TRUNC('MONTH', sessions.session_date) as sessions_session_month,"
        "sessions.utm_source as sessions_utm_source,sessions.utm_campaign as "
        "sessions_utm_campaign,COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions WHERE sessions.session_date>='2022-01-05T00:00:00' "
        "GROUP BY DATE_TRUNC('MONTH', sessions.session_date)"
        ",sessions.utm_source,sessions.utm_campaign ORDER BY sessions_number_of_sessions DESC NULLS LAST) "
        f"SELECT {orders_cte}.orders_number_of_orders as orders_number_of_orders,"
        f"{sessions_cte}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"ifnull({orders_cte}.orders_order_month, {sessions_cte}.sessions_session_month) as orders_order_month,"  # noqa
        f"{orders_source} as orders_sub_channel,"
        f"{orders_campaign} as orders_campaign,"
        f"ifnull({sessions_cte}.sessions_session_month, {orders_cte}.orders_order_month) as sessions_session_month,"  # noqa
        f"{sessions_source} as sessions_utm_source,"
        f"{sessions_campaign} as sessions_utm_campaign "
        f"FROM {orders_cte} FULL OUTER JOIN {sessions_cte} "
        f"ON {orders_cte}.orders_order_month={sessions_cte}.sessions_session_month "
        f"and {orders_cte}.orders_sub_channel={sessions_cte}.sessions_utm_source "
        f"and {orders_cte}.orders_campaign={sessions_cte}.sessions_utm_campaign;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_dimension_mapping_single_metric(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["sub_channel", "orders.order_date", "sessions.utm_campaign", "sessions.session_date"],
    )

    orders_cte = "orders_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"
    orders_source = f"ifnull({orders_cte}.orders_sub_channel, {sessions_cte}.sessions_utm_source)"
    orders_campaign = f"ifnull({orders_cte}.orders_campaign, {sessions_cte}.sessions_utm_campaign)"
    sessions_source = f"ifnull({sessions_cte}.sessions_utm_source, {orders_cte}.orders_sub_channel)"
    sessions_campaign = f"ifnull({sessions_cte}.sessions_utm_campaign, {orders_cte}.orders_campaign)"
    correct = (
        f"WITH {orders_cte} AS (SELECT orders.sub_channel as orders_sub_channel,"
        f"DATE_TRUNC('DAY', orders.order_date) as orders_order_date,orders.campaign as orders_campaign,"
        f"COUNT(orders.id) as orders_number_of_orders FROM analytics.orders orders GROUP BY "
        f"orders.sub_channel,DATE_TRUNC('DAY', orders.order_date),orders.campaign ORDER BY "
        f"orders_number_of_orders DESC NULLS LAST) ,{sessions_cte} AS (SELECT sessions.utm_source "
        f"as sessions_utm_source,DATE_TRUNC('DAY', sessions.session_date) as sessions_session_date,"
        f"sessions.utm_campaign as sessions_utm_campaign FROM analytics.sessions sessions GROUP BY "
        f"sessions.utm_source,DATE_TRUNC('DAY', sessions.session_date),sessions.utm_campaign ORDER BY "
        f"sessions_utm_source ASC NULLS LAST) SELECT {orders_cte}.orders_number_of_orders as "
        f"orders_number_of_orders,{orders_source} as orders_sub_channel,"
        f"ifnull({orders_cte}.orders_order_date, {sessions_cte}.sessions_session_date) as orders_order_date,"
        f"{orders_campaign} as orders_campaign,{sessions_source} as "
        f"sessions_utm_source,ifnull({sessions_cte}.sessions_session_date, "
        f"{orders_cte}.orders_order_date) as sessions_session_date,"
        f"{sessions_campaign} as sessions_utm_campaign "
        f"FROM {orders_cte} FULL OUTER JOIN {sessions_cte} ON {orders_cte}."  # noqa
        f"orders_sub_channel={sessions_cte}.sessions_utm_source and {orders_cte}."
        f"orders_order_date={sessions_cte}.sessions_session_date and {orders_cte}."
        f"orders_campaign={sessions_cte}.sessions_utm_campaign;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_dimension_mapping_no_metric(connection):
    query = connection.get_sql_query(
        metrics=[],
        dimensions=["utm_campaign", "orders.order_date"],
        where=[
            {
                "field": "orders.order_raw",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
    )
    orders_cte = "orders_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"

    orders_campaign = f"ifnull({orders_cte}.orders_campaign, {sessions_cte}.sessions_utm_campaign)"
    sessions_campaign = f"ifnull({sessions_cte}.sessions_utm_campaign, {orders_cte}.orders_campaign)"
    orders_date = f"ifnull({orders_cte}.orders_order_date, {sessions_cte}.sessions_session_date)"
    sessions_date = f"ifnull({sessions_cte}.sessions_session_date, {orders_cte}.orders_order_date)"
    correct = (
        f"WITH {orders_cte} AS (SELECT orders.campaign as orders_campaign,"
        "DATE_TRUNC('DAY', orders.order_date) as orders_order_date FROM analytics.orders "
        "orders WHERE orders.order_date>='2022-01-05T00:00:00' GROUP BY orders.campaign,"
        f"DATE_TRUNC('DAY', orders.order_date) ORDER BY orders_campaign ASC NULLS LAST) ,{sessions_cte} "
        "AS (SELECT sessions.utm_campaign as sessions_utm_campaign,DATE_TRUNC('DAY', "
        "sessions.session_date) as sessions_session_date FROM analytics.sessions sessions "
        "WHERE sessions.session_date>='2022-01-05T00:00:00' GROUP BY sessions.utm_campaign,"
        "DATE_TRUNC('DAY', sessions.session_date) ORDER BY sessions_utm_campaign ASC NULLS LAST) "
        f"SELECT {orders_campaign} as orders_campaign,"
        f"{orders_date} as orders_order_date,{sessions_campaign} "
        f"as sessions_utm_campaign,{sessions_date} as "
        f"sessions_session_date FROM {orders_cte} FULL OUTER JOIN {sessions_cte} "
        f"ON {orders_cte}.orders_campaign={sessions_cte}.sessions_utm_campaign "
        f"and {orders_cte}.orders_order_date={sessions_cte}.sessions_session_date;"
    )
    print(query)
    assert query == correct


@pytest.mark.query
def test_merged_query_implicit_no_time(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["utm_campaign"],
        where=[{"field": "sessions.utm_source", "expression": "equal_to", "value": "Iterable"}],
    )

    orders_cte = "orders_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"
    correct = (
        f"WITH {orders_cte} AS (SELECT orders.campaign as orders_campaign,"
        f"COUNT(orders.id) as orders_number_of_orders FROM analytics.orders orders "
        f"WHERE orders.sub_channel='Iterable' GROUP BY orders.campaign "
        f"ORDER BY orders_number_of_orders DESC NULLS LAST) ,{sessions_cte} AS ("
        f"SELECT sessions.utm_campaign as sessions_utm_campaign "
        f"FROM analytics.sessions sessions WHERE sessions.utm_source='Iterable' "
        f"GROUP BY sessions.utm_campaign ORDER BY sessions_utm_campaign ASC NULLS LAST) "
        f"SELECT {orders_cte}.orders_number_of_orders as orders_number_of_orders,"
        f"ifnull({orders_cte}.orders_campaign, {sessions_cte}.sessions_utm_campaign) as orders_campaign,"
        f"ifnull({sessions_cte}.sessions_utm_campaign, {orders_cte}.orders_campaign) as sessions_utm_campaign "  # noqa
        f"FROM {orders_cte} FULL OUTER JOIN {sessions_cte} "
        f"ON {orders_cte}.orders_campaign"
        f"={sessions_cte}.sessions_utm_campaign;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_implicit_with_join(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "number_of_sessions"], dimensions=["gender"]
    )

    orders_cte = "orders_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"
    correct = (
        f"WITH {orders_cte} AS (SELECT customers.gender as customers_gender,"
        "COUNT(orders.id) as orders_number_of_orders FROM analytics.orders orders "
        "LEFT JOIN analytics.customers customers ON orders.customer_id=customers.customer_id "
        "GROUP BY customers.gender ORDER BY orders_number_of_orders DESC NULLS LAST) ,"
        f"{sessions_cte} AS (SELECT customers.gender as customers_gender,"
        "COUNT(sessions.id) as sessions_number_of_sessions FROM analytics.sessions sessions "
        "LEFT JOIN analytics.customers customers ON sessions.customer_id=customers.customer_id "
        "GROUP BY customers.gender ORDER BY sessions_number_of_sessions DESC NULLS LAST) "
        f"SELECT {orders_cte}.orders_number_of_orders as orders_number_of_orders,"
        f"{sessions_cte}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"ifnull({orders_cte}.customers_gender, "
        f"{sessions_cte}.customers_gender) as customers_gender "
        f"FROM {orders_cte} FULL OUTER JOIN {sessions_cte} "
        f"ON {orders_cte}.customers_gender={sessions_cte}.customers_gender;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_implicit_with_extra_dim_only(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders"], dimensions=["orders.order_date", "utm_source"]
    )

    orders_cte = "orders_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"
    orders_source = f"ifnull({orders_cte}.orders_sub_channel, {sessions_cte}.sessions_utm_source)"
    sessions_source = f"ifnull({sessions_cte}.sessions_utm_source, {orders_cte}.orders_sub_channel)"
    orders_date = f"ifnull({orders_cte}.orders_order_date, {sessions_cte}.sessions_session_date)"
    sessions_date = f"ifnull({sessions_cte}.sessions_session_date, {orders_cte}.orders_order_date)"
    correct = (
        f"WITH {orders_cte} AS (SELECT DATE_TRUNC('DAY', orders.order_date) as "
        "orders_order_date,orders.sub_channel as orders_sub_channel,COUNT(orders.id) as "
        "orders_number_of_orders FROM analytics.orders orders GROUP BY DATE_TRUNC('DAY', "
        "orders.order_date),orders.sub_channel ORDER BY orders_number_of_orders DESC NULLS LAST) ,"
        f"{sessions_cte} AS (SELECT DATE_TRUNC('DAY', sessions.session_date) as "
        "sessions_session_date,sessions.utm_source as sessions_utm_source FROM analytics.sessions "
        "sessions GROUP BY DATE_TRUNC('DAY', sessions.session_date),sessions.utm_source ORDER BY "
        f"sessions_session_date ASC NULLS LAST) SELECT {orders_cte}.orders_number_of_orders as "
        f"orders_number_of_orders,{orders_date} as orders_order_date,"
        f"{orders_source} as orders_sub_channel,"
        f"{sessions_date} as sessions_session_date,"
        f"{sessions_source} as sessions_utm_source FROM "
        f"{orders_cte} FULL OUTER JOIN {sessions_cte} ON {orders_cte}"
        f".orders_order_date={sessions_cte}.sessions_session_date "
        f"and {orders_cte}.orders_sub_channel={sessions_cte}.sessions_utm_source;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_implicit_3_way_merge(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "number_of_sessions", "number_of_events"],
        dimensions=["orders.order_date"],
        where=[
            {
                "field": "orders.order_raw",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2022, 1, 5, 0, 0),
            }
        ],
    )
    orders_cte = "orders_order__cte_subquery_1"
    sessions_cte = "sessions_session__cte_subquery_2"
    events_cte = "events_event__cte_subquery_0"
    sessions_date = f"ifnull({sessions_cte}.sessions_session_date, ifnull({events_cte}.events_event_date, {orders_cte}.orders_order_date))"  # noqa
    orders_date = f"ifnull({orders_cte}.orders_order_date, ifnull({events_cte}.events_event_date, {sessions_cte}.sessions_session_date))"  # noqa
    events_date = f"ifnull({events_cte}.events_event_date, ifnull({orders_cte}.orders_order_date, {sessions_cte}.sessions_session_date))"  # noqa
    correct = (
        f"WITH {orders_cte} AS (SELECT DATE_TRUNC('DAY', orders.order_date) as "
        "orders_order_date,COUNT(orders.id) as orders_number_of_orders FROM analytics.orders orders "
        "WHERE orders.order_date>='2022-01-05T00:00:00' GROUP BY DATE_TRUNC('DAY', orders.order_date)"
        f" ORDER BY orders_number_of_orders DESC NULLS LAST) ,{sessions_cte} AS (SELECT "
        "DATE_TRUNC('DAY', sessions.session_date) as sessions_session_date,COUNT(sessions.id) as "
        "sessions_number_of_sessions FROM analytics.sessions sessions WHERE sessions.session_date>="
        "'2022-01-05T00:00:00' GROUP BY DATE_TRUNC('DAY', sessions.session_date) ORDER BY "
        f"sessions_number_of_sessions DESC NULLS LAST) ,{events_cte} AS (SELECT DATE_TRUNC('DAY', "
        "events.event_date) as events_event_date,COUNT(DISTINCT(events.id)) as events_number_of_events "
        "FROM analytics.events events WHERE events.event_date>='2022-01-05T00:00:00' GROUP BY "
        "DATE_TRUNC('DAY', events.event_date) ORDER BY events_number_of_events DESC NULLS LAST) SELECT "
        f"{events_cte}.events_number_of_events as events_number_of_events,"
        f"{orders_cte}.orders_number_of_orders as orders_number_of_orders,"
        f"{sessions_cte}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"{events_date} as events_event_date,"
        f"{orders_date} as orders_order_date,{sessions_date} "
        f"as sessions_session_date FROM {events_cte} FULL OUTER JOIN {orders_cte} ON "
        f"{events_cte}.events_event_date={orders_cte}.orders_order_date "
        f"FULL OUTER JOIN {sessions_cte} ON {events_cte}.events_event_date"
        f"={sessions_cte}.sessions_session_date;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_merged_results_as_sub_reference(connection):
    query = connection.get_sql_query(
        metrics=["net_per_session", "costs_per_session", "total_item_revenue"],
        dimensions=["order_lines.order_month"],
    )

    order_lines_cte = "order_lines_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"
    correct = (
        f"WITH {order_lines_cte} AS (SELECT DATE_TRUNC('MONTH', order_lines.order_date) as "
        f"order_lines_order_month,SUM(order_lines.revenue) as order_lines_total_item_revenue,"
        f"SUM(case when order_lines.product_name='Portable Charger' and order_lines.product_name "
        f"IN ('Portable Charger','Dual Charger') and orders.revenue * 100>100 then order_lines.item_costs "
        f"end) as order_lines_total_item_costs,COUNT(case when order_lines.sales_channel='Email' "
        f"then order_lines.order_id end) as order_lines_number_of_email_purchased_items "
        f"FROM analytics.order_line_items order_lines LEFT JOIN "
        f"analytics.orders orders ON order_lines.order_unique_id=orders.id GROUP BY "
        f"DATE_TRUNC('MONTH', order_lines.order_date) ORDER BY order_lines_total_item_revenue DESC NULLS"
        f" LAST) ,"
        f"{sessions_cte} AS (SELECT DATE_TRUNC('MONTH', sessions.session_date) as "
        f"sessions_session_month,COUNT(sessions.id) as sessions_number_of_sessions "
        f"FROM analytics.sessions sessions GROUP BY DATE_TRUNC('MONTH', sessions.session_date) "
        f"ORDER BY sessions_number_of_sessions DESC NULLS LAST) "
        f"SELECT "
        f"{order_lines_cte}.order_lines_total_item_revenue as order_lines_total_item_revenue,"
        f"{order_lines_cte}.order_lines_total_item_costs as order_lines_total_item_costs,"
        f"{order_lines_cte}.order_lines_number_of_email_purchased_items "
        f"as order_lines_number_of_email_purchased_items,"
        f"{sessions_cte}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"ifnull({order_lines_cte}.order_lines_order_month, {sessions_cte}.sessions_session_month) as order_lines_order_month,"  # noqa
        f"ifnull({sessions_cte}.sessions_session_month, {order_lines_cte}.order_lines_order_month) as sessions_session_month,"  # noqa
        f"(order_lines_total_item_revenue / nullif(sessions_number_of_sessions, 0)) - "
        f"((order_lines_total_item_costs * order_lines_number_of_email_purchased_items) "
        f"/ nullif(sessions_number_of_sessions, 0)) as order_lines_net_per_session,"
        f"(order_lines_total_item_costs * order_lines_number_of_email_purchased_items) "
        f"/ nullif(sessions_number_of_sessions, 0) as order_lines_costs_per_session "
        f"FROM {order_lines_cte} FULL OUTER JOIN {sessions_cte} "
        f"ON {order_lines_cte}.order_lines_order_month"
        f"={sessions_cte}.sessions_session_month;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_merged_results_joined_filter(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "number_of_sessions"],
        dimensions=["orders.order_date"],
        where=[
            {"field": "customers.region", "expression": "isin", "value": ["West", "South"]},
            {"field": "sessions.utm_source", "expression": "equal_to", "value": "google"},
        ],
    )

    orders_cte = "orders_order__cte_subquery_0"
    sessions_cte = "sessions_session__cte_subquery_1"
    correct = (
        f"WITH {orders_cte} AS (SELECT DATE_TRUNC('DAY', orders.order_date) "
        "as orders_order_date,COUNT(orders.id) as orders_number_of_orders FROM analytics.orders "
        "orders LEFT JOIN analytics.customers customers ON orders.customer_id=customers.customer_id "
        "WHERE customers.region IN ('West','South') AND orders.sub_channel='google' "
        "GROUP BY DATE_TRUNC('DAY', orders.order_date) ORDER BY orders_number_of_orders DESC NULLS LAST) ,"
        f"{sessions_cte} AS (SELECT DATE_TRUNC('DAY', sessions.session_date) as "
        "sessions_session_date,COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions LEFT JOIN analytics.customers customers ON "
        "sessions.customer_id=customers.customer_id WHERE customers.region IN ('West','South') "
        "AND sessions.utm_source='google' GROUP BY DATE_TRUNC('DAY', sessions.session_date) "
        f"ORDER BY sessions_number_of_sessions DESC NULLS LAST) SELECT {orders_cte}."
        f"orders_number_of_orders as orders_number_of_orders,{sessions_cte}."
        "sessions_number_of_sessions as sessions_number_of_sessions,"
        f"ifnull({orders_cte}.orders_order_date, {sessions_cte}.sessions_session_date) "
        f"as orders_order_date,ifnull({sessions_cte}.sessions_session_date, {orders_cte}.orders_order_date) "
        f"as sessions_session_date FROM {orders_cte} FULL OUTER JOIN {sessions_cte} "
        f"ON {orders_cte}.orders_order_date={sessions_cte}.sessions_session_date;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_merged_results_3_way_third_date_only(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "number_of_sessions"],
        dimensions=["events.event_date"],
    )
    orders_cte = "orders_order__cte_subquery_1"
    sessions_cte = "sessions_session__cte_subquery_2"
    events_cte = "events_event__cte_subquery_0"
    sessions_date = f"ifnull({sessions_cte}.sessions_session_date, ifnull({events_cte}.events_event_date, {orders_cte}.orders_order_date))"  # noqa
    orders_date = f"ifnull({orders_cte}.orders_order_date, ifnull({events_cte}.events_event_date, {sessions_cte}.sessions_session_date))"  # noqa
    events_date = f"ifnull({events_cte}.events_event_date, ifnull({orders_cte}.orders_order_date, {sessions_cte}.sessions_session_date))"  # noqa
    correct = (
        f"WITH {orders_cte} AS (SELECT DATE_TRUNC('DAY', orders.order_date) as "
        "orders_order_date,COUNT(orders.id) as orders_number_of_orders FROM analytics.orders "
        "orders GROUP BY DATE_TRUNC('DAY', orders.order_date) ORDER BY orders_number_of_orders "
        f"DESC NULLS LAST) ,{sessions_cte} AS (SELECT DATE_TRUNC('DAY', sessions.session_date) "
        "as sessions_session_date,COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions GROUP BY DATE_TRUNC('DAY', sessions.session_date) "
        f"ORDER BY sessions_number_of_sessions DESC NULLS LAST) ,{events_cte} AS ("
        "SELECT DATE_TRUNC('DAY', events.event_date) as events_event_date FROM analytics.events "
        "events GROUP BY DATE_TRUNC('DAY', events.event_date) ORDER BY events_event_date ASC NULLS LAST) "
        f"SELECT {orders_cte}.orders_number_of_orders as orders_number_of_orders,"
        f"{sessions_cte}.sessions_number_of_sessions as sessions_number_of_sessions,"
        f"{events_date} as events_event_date,{orders_date} as orders_order_date,"
        f"{sessions_date} as sessions_session_date FROM {events_cte} FULL OUTER JOIN {orders_cte} "
        f"ON {events_cte}.events_event_date={orders_cte}.orders_order_date "
        f"FULL OUTER JOIN {sessions_cte} ON {events_cte}.events_event_date"
        f"={sessions_cte}.sessions_session_date;"
    )
    assert query == correct


@pytest.mark.query
def test_merged_query_three_field_link(connection):
    order_field = connection.get_field("number_of_orders")
    session_field = connection.get_field("number_of_sessions")
    event_field = connection.get_field("number_of_events")

    session_graphs = session_field.join_graphs()
    shared_with_orders = [j for j in order_field.join_graphs() if j in session_graphs]

    assert any(j in shared_with_orders for j in event_field.join_graphs())


@pytest.mark.query
def test_merged_query_implicit_query_kind(connection):
    _, query_kind_merged = connection.get_sql_query(
        metrics=["number_of_orders", "number_of_sessions"],
        dimensions=["orders.order_month", "sub_channel"],
        return_query_kind=True,
    )

    assert query_kind_merged == "MERGED"

    _, query_kind_normal = connection.get_sql_query(
        metrics=["number_of_orders"],
        dimensions=["orders.order_month"],
        return_query_kind=True,
    )
    assert query_kind_normal == "SINGLE"


@pytest.mark.query
def test_implicit_merge_subgraph(connection):
    order_field = connection.get_field("number_of_orders")
    session_field = connection.get_field("number_of_sessions")

    session_graphs = session_field.join_graphs()
    assert any(j in session_graphs for j in order_field.join_graphs())

    order_field = connection.get_field("number_of_orders")
    session_field = connection.get_field("utm_source")

    session_graphs = session_field.join_graphs()
    assert any(j in session_graphs for j in order_field.join_graphs())

    order_field = connection.get_field("number_of_orders")
    session_field = connection.get_field("number_of_sessions")
    gender_field = connection.get_field("gender")

    session_graphs = session_field.join_graphs()
    shared_with_orders = [j for j in order_field.join_graphs() if j in session_graphs]
    assert any(j in shared_with_orders for j in gender_field.join_graphs())

    traffic_field = connection.get_field("traffic_source")

    assert not any(j in shared_with_orders for j in traffic_field.join_graphs())

    cumulative_field = connection.get_field("cumulative_customers")

    assert not any(j in shared_with_orders for j in cumulative_field.join_graphs())


@pytest.mark.query
def test_implicit_merge_subgraph_shared_dimension(connection):
    session_source_field = connection.get_field("utm_source")
    session_source_graphs = session_source_field.join_graphs()

    # The canon date options should not be in the subgraph because they
    # include *all* the merge-able options based on date which
    # are *not* guaranteed to be in the subgraph of a mapped dimension.
    should_not_be_here = [
        "merged_result_canon_date_core_date",
        "merged_result_canon_date_core_day_of_week",
        "merged_result_canon_date_core_hour_of_day",
        "merged_result_canon_date_core_month",
        "merged_result_canon_date_core_quarter",
        "merged_result_canon_date_core_raw",
        "merged_result_canon_date_core_time",
        "merged_result_canon_date_core_week",
        "merged_result_canon_date_core_year",
    ]
    assert all(j not in session_source_graphs for j in should_not_be_here)


@pytest.mark.query
def test_implicit_merge_subgraph_dimension_group_check(connection):
    discount_field = connection.get_field("total_discount_amt")
    session_field = connection.get_field("number_of_sessions")
    session_time_field = connection.get_field("sessions.session_quarter")

    session_graphs = session_field.join_graphs()
    shared = [j for j in session_time_field.join_graphs() if j in session_graphs]
    assert all(j not in shared for j in discount_field.join_graphs())


@pytest.mark.query
def test_implicit_raise_join_errors(connection):
    with pytest.raises(JoinError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_orders", "number_of_sessions"],
            dimensions=["orders.order_month"],
            single_query=True,
        )

    assert exc_info.value

    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["number_of_orders", "number_of_sessions"], dimensions=["traffic_source"]
        )

    assert exc_info.value
    assert "Zenlytic tries to merge query results by default" in exc_info.value.message


@pytest.mark.query
def test_4_way_merge_with_joinable_canon_date(connection):
    metrics = ["number_of_orders", "number_of_customers", "total_item_revenue", "number_of_events"]
    query = connection.get_sql_query(
        metrics=metrics,
        dimensions=["orders.order_month"],
    )

    orders_cte = "orders_order__cte_subquery_3"
    order_lines_cte = "order_lines_order__cte_subquery_2"
    customers_cte = "customers_first_order__cte_subquery_0"
    events_cte = "events_event__cte_subquery_1"

    order_lines_month = f"ifnull({order_lines_cte}.order_lines_order_month, ifnull({customers_cte}.customers_first_order_month, ifnull({events_cte}.events_event_month, {orders_cte}.orders_order_month)))"  # noqa
    orders_month = f"ifnull({orders_cte}.orders_order_month, ifnull({customers_cte}.customers_first_order_month, ifnull({events_cte}.events_event_month, {order_lines_cte}.order_lines_order_month)))"  # noqa
    events_month = f"ifnull({events_cte}.events_event_month, ifnull({customers_cte}.customers_first_order_month, ifnull({order_lines_cte}.order_lines_order_month, {orders_cte}.orders_order_month)))"  # noqa
    customers_month = f"ifnull({customers_cte}.customers_first_order_month, ifnull({events_cte}.events_event_month, ifnull({order_lines_cte}.order_lines_order_month, {orders_cte}.orders_order_month)))"  # noqa
    correct = (
        f"WITH {orders_cte} AS (SELECT DATE_TRUNC('MONTH', orders.order_date) as "
        f"orders_order_month,COUNT(orders.id) as orders_number_of_orders FROM analytics.orders "
        f"orders GROUP BY DATE_TRUNC('MONTH', orders.order_date) ORDER BY orders_number_of_orders "
        f"DESC NULLS LAST) ,{customers_cte} AS (SELECT DATE_TRUNC('MONTH', "
        f"customers.first_order_date) as customers_first_order_month,COUNT(customers.customer_id) "
        f"as customers_number_of_customers FROM analytics.customers customers GROUP BY DATE_TRUNC('MONTH', "
        f"customers.first_order_date) ORDER BY customers_number_of_customers DESC NULLS LAST) ,"
        f"{order_lines_cte} AS (SELECT DATE_TRUNC('MONTH', order_lines.order_date) "
        f"as order_lines_order_month,SUM(order_lines.revenue) as order_lines_total_item_revenue "
        f"FROM analytics.order_line_items order_lines GROUP BY DATE_TRUNC('MONTH', order_lines.order_date) "
        f"ORDER BY order_lines_total_item_revenue DESC NULLS LAST) ,{events_cte} AS (SELECT "
        f"DATE_TRUNC('MONTH', events.event_date) as events_event_month,COUNT(DISTINCT(events.id)) "
        f"as events_number_of_events FROM analytics.events events GROUP BY DATE_TRUNC('MONTH', "
        f"events.event_date) ORDER BY events_number_of_events DESC NULLS LAST) SELECT "
        f"{customers_cte}.customers_number_of_customers as "
        f"customers_number_of_customers,{events_cte}.events_number_of_events as "
        f"events_number_of_events,{order_lines_cte}.order_lines_total_item_revenue "
        f"as order_lines_total_item_revenue,{orders_cte}.orders_number_of_orders "
        f"as orders_number_of_orders,{customers_month} "
        f"as customers_first_order_month,{events_month} as events_event_month,"
        f"{order_lines_month} as order_lines_order_month,"
        f"{orders_month} as orders_order_month FROM "
        f"{customers_cte} FULL OUTER JOIN {events_cte} ON "
        f"{customers_cte}.customers_first_order_month={events_cte}."  # noqa
        f"events_event_month FULL OUTER JOIN {order_lines_cte} ON "
        f"{customers_cte}.customers_first_order_month={order_lines_cte}"  # noqa
        f".order_lines_order_month FULL OUTER JOIN {orders_cte} ON "
        f"{customers_cte}.customers_first_order_month={orders_cte}"  # noqa
        f".orders_order_month;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merge_results_order_issue(connection):
    query = connection.get_sql_query(
        metrics=["number_of_customers", "number_of_orders"],
        dimensions=["month"],
        where=[
            {"field": "date", "expression": "greater_than", "value": "2022-04-03"},
        ],
    )

    orders_cte = "orders_order__cte_subquery_1"
    customers_cte = "customers_first_order__cte_subquery_0"
    correct = (
        f"WITH {customers_cte} AS (SELECT DATE_TRUNC('MONTH', "
        "customers.first_order_date) as customers_first_order_month,COUNT(customers.customer_id) as "
        "customers_number_of_customers FROM analytics.customers customers WHERE DATE_TRUNC('DAY', "
        "customers.first_order_date)>'2022-04-03' GROUP BY DATE_TRUNC('MONTH', customers.first_order_date) "
        f"ORDER BY customers_number_of_customers DESC NULLS LAST) ,{orders_cte} AS ("
        "SELECT DATE_TRUNC('MONTH', orders.order_date) as orders_order_month,COUNT(orders.id) as "
        "orders_number_of_orders FROM analytics.orders orders "
        "WHERE DATE_TRUNC('DAY', orders.order_date)>'2022-04-03' "
        "GROUP BY DATE_TRUNC('MONTH', orders.order_date) ORDER BY orders_number_of_orders DESC NULLS LAST) "
        f"SELECT {customers_cte}.customers_number_of_customers "
        f"as customers_number_of_customers,{orders_cte}.orders_number_of_orders as "
        f"orders_number_of_orders,ifnull({customers_cte}.customers_first_order_month, "
        f"{orders_cte}.orders_order_month) as customers_first_order_month,"
        f"ifnull({orders_cte}.orders_order_month, {customers_cte}.customers_first_order_month) "
        "as orders_order_month FROM "
        f"{customers_cte} FULL OUTER JOIN "
        f"{orders_cte} ON {customers_cte}"
        f".customers_first_order_month={orders_cte}.orders_order_month;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merge_results_default_date_raise_error(project, connections):
    canon_date = project._views[5]["fields"][-1].pop("canon_date")
    connection = MetricsLayerConnection(project=project, connections=connections)

    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(metrics=["number_of_customers", "avg_rainfall"], dimensions=["month"])

    assert exc_info.value
    assert "Could not find a date field associated with metric avg_rainfall" in exc_info.value.message

    project._views[5]["fields"][-1]["canon_date"] = canon_date


@pytest.mark.query
def test_query_default_date_from_join(connection):
    query = connection.get_sql_query(
        metrics=["number_of_orders", "number_of_login_events"],
        dimensions=["orders.order_date"],
        where=[
            {
                "field": "date",
                "expression": "greater_or_equal_than",
                "value": datetime.datetime(2023, 3, 29, 0, 0),
            },
            {
                "field": "date",
                "expression": "less_or_equal_than",
                "value": datetime.datetime(2023, 6, 26, 23, 59, 59),
            },
        ],
    )

    correct = (
        "WITH orders_order__cte_subquery_1 AS (SELECT DATE_TRUNC('DAY', orders.order_date) as "
        "orders_order_date,COUNT(orders.id) as orders_number_of_orders FROM analytics.orders orders "
        "WHERE DATE_TRUNC('DAY', orders.order_date)>='2023-03-29T00:00:00' AND DATE_TRUNC('DAY', "
        "orders.order_date)<='2023-06-26T23:59:59' GROUP BY DATE_TRUNC('DAY', orders.order_date) "
        "ORDER BY orders_number_of_orders DESC NULLS LAST) ,events_event__cte_subquery_0 AS (SELECT"
        " DATE_TRUNC('DAY', "
        "events.event_date) as events_event_date,COUNT(DISTINCT(login_events.id)) as "
        "login_events_number_of_login_events FROM analytics.login_events login_events "
        "LEFT JOIN analytics.events events ON login_events.id=events.id WHERE DATE_TRUNC('DAY', "
        "events.event_date)>='2023-03-29T00:00:00' AND DATE_TRUNC('DAY', events.event_date)"
        "<='2023-06-26T23:59:59' GROUP BY DATE_TRUNC('DAY', events.event_date) ORDER BY "
        "login_events_number_of_login_events DESC NULLS LAST) SELECT events_event__cte_subquery_0"
        ".login_events_number_of_login_events as login_events_number_of_login_events,"
        "orders_order__cte_subquery_1.orders_number_of_orders as orders_number_of_orders,"
        "ifnull(events_event__cte_subquery_0.events_event_date, orders_order__cte_subquery_1.orders_order_date) as events_event_date,"  # noqa
        "ifnull(orders_order__cte_subquery_1.orders_order_date, events_event__cte_subquery_0.events_event_date) as orders_order_date FROM "  # noqa
        "events_event__cte_subquery_0 FULL OUTER JOIN orders_order__cte_subquery_1 ON "
        "events_event__cte_subquery_0.events_event_date=orders_order__cte_subquery_1.orders_order_date;"
    )
    assert query == correct


@pytest.mark.query
def test_query_mapping_with_a_join(connection):
    query = connection.get_sql_query(
        metrics=["number_of_sessions", "number_of_login_events"], dimensions=["sessions.session_device"]
    )

    correct = (
        "WITH sessions_session__cte_subquery_1 AS (SELECT sessions.session_device as sessions_session_device,"
        "COUNT(sessions.id) as sessions_number_of_sessions FROM analytics.sessions sessions "
        "GROUP BY sessions.session_device ORDER BY sessions_number_of_sessions DESC NULLS LAST) ,"
        "events_event__cte_subquery_0 AS (SELECT events.device as events_device,"
        "COUNT(DISTINCT(login_events.id)) as login_events_number_of_login_events "
        "FROM analytics.login_events login_events LEFT JOIN analytics.events events "
        "ON login_events.id=events.id GROUP BY events.device ORDER BY login_events_number_of_login_events "
        "DESC NULLS LAST) SELECT events_event__cte_subquery_0.login_events_number_of_login_events as "
        "login_events_number_of_login_events,sessions_session__cte_subquery_1.sessions_number_of_sessions "
        "as sessions_number_of_sessions,ifnull(events_event__cte_subquery_0.events_device, "
        "sessions_session__cte_subquery_1.sessions_session_device) "
        "as events_device,ifnull(sessions_session__cte_subquery_1.sessions_session_device, "
        "events_event__cte_subquery_0.events_device) as sessions_session_device "
        "FROM events_event__cte_subquery_0 FULL OUTER JOIN "
        "sessions_session__cte_subquery_1 ON events_event__cte_subquery_0.events_device"
        "=sessions_session__cte_subquery_1.sessions_session_device;"
    )
    assert query == correct


@pytest.mark.query
def test_query_mapping_with_a_join_inverted_mapping(connection):
    query = connection.get_sql_query(
        metrics=["number_of_sessions", "number_of_login_events"], dimensions=["login_events.device"]
    )

    correct = (
        "WITH sessions_session__cte_subquery_1 AS (SELECT sessions.session_device as sessions_session_device,"
        "COUNT(sessions.id) as sessions_number_of_sessions FROM analytics.sessions sessions "
        "GROUP BY sessions.session_device ORDER BY sessions_number_of_sessions DESC NULLS LAST) ,"
        "events_event__cte_subquery_0 AS (SELECT events.device as login_events_device,"
        "COUNT(DISTINCT(login_events.id)) as login_events_number_of_login_events "
        "FROM analytics.login_events login_events LEFT JOIN analytics.events events "
        "ON login_events.id=events.id GROUP BY events.device ORDER BY "
        "login_events_number_of_login_events DESC NULLS LAST) SELECT events_event__cte_subquery_0"
        ".login_events_number_of_login_events as login_events_number_of_login_events,"
        "sessions_session__cte_subquery_1.sessions_number_of_sessions as sessions_number_of_sessions,"
        "ifnull(events_event__cte_subquery_0.login_events_device, sessions_session__cte_subquery_1.sessions_session_device) as login_events_device,"  # noqa
        "ifnull(sessions_session__cte_subquery_1.sessions_session_device, events_event__cte_subquery_0.login_events_device) as sessions_session_device "  # noqa
        "FROM events_event__cte_subquery_0 FULL OUTER JOIN sessions_session__cte_subquery_1 "
        "ON events_event__cte_subquery_0.login_events_device=sessions_session__cte_subquery_1"
        ".sessions_session_device;"
    )
    assert query == correct


@pytest.mark.query
def test_query_mapping_with_a_join_and_date(connection):
    query = connection.get_sql_query(
        metrics=["number_of_sessions", "number_of_login_events"],
        dimensions=["sessions.session_device"],
        where=[
            {
                "field": "date",
                "expression": "less_or_equal_than",
                "value": datetime.datetime(2023, 6, 26, 23, 59, 59),
            },
        ],
    )

    correct = (
        "WITH sessions_session__cte_subquery_1 AS (SELECT sessions.session_device as sessions_session_device,"
        "COUNT(sessions.id) as sessions_number_of_sessions FROM analytics.sessions sessions "
        "WHERE DATE_TRUNC('DAY', sessions.session_date)<='2023-06-26T23:59:59' "
        "GROUP BY sessions.session_device ORDER BY sessions_number_of_sessions DESC NULLS LAST) ,"
        "events_event__cte_subquery_0 AS (SELECT events.device as events_device,"
        "COUNT(DISTINCT(login_events.id)) as login_events_number_of_login_events FROM analytics.login_events "
        "login_events LEFT JOIN analytics.events events ON login_events.id=events.id "
        "WHERE DATE_TRUNC('DAY', events.event_date)<='2023-06-26T23:59:59' GROUP BY events.device "
        "ORDER BY login_events_number_of_login_events DESC NULLS LAST) SELECT events_event__cte_subquery_0"
        ".login_events_number_of_login_events as login_events_number_of_login_events,"
        "sessions_session__cte_subquery_1.sessions_number_of_sessions as sessions_number_of_sessions,"
        "ifnull(events_event__cte_subquery_0.events_device, sessions_session__cte_subquery_1.sessions_session_device) as events_device,"  # noqa
        "ifnull(sessions_session__cte_subquery_1.sessions_session_device, events_event__cte_subquery_0.events_device) as sessions_session_device "  # noqa
        "FROM events_event__cte_subquery_0 FULL OUTER JOIN sessions_session__cte_subquery_1 "
        "ON events_event__cte_subquery_0.events_device=sessions_session__cte_subquery_1"
        ".sessions_session_device;"
    )
    assert query == correct


@pytest.mark.query
def test_query_subquery_with_substring_in_name(connection):
    query = connection.get_sql_query(
        metrics=["number_of_account_customer_connections", "number_of_acquired_accounts"],
        dimensions=["aa_acquired_accounts.created_month", "aa_acquired_accounts.account_type"],
        where=[
            {
                "field": "date",
                "expression": "less_or_equal_than",
                "value": datetime.datetime(2023, 6, 26, 23, 59, 59),
            },
        ],
    )

    cte_1 = "z_customer_accounts_created__cte_subquery_1"
    cte_2 = "aa_acquired_accounts_created__cte_subquery_0"
    correct = (
        f"WITH {cte_1} AS (SELECT DATE_TRUNC('MONTH', "
        "z_customer_accounts.created_at) as z_customer_accounts_created_month,"
        "z_customer_accounts.account_type as z_customer_accounts_type_of_account,"
        "COUNT(z_customer_accounts.account_id || z_customer_accounts.customer_id) as "
        "z_customer_accounts_number_of_account_customer_connections FROM analytics.customer_accounts "
        "z_customer_accounts WHERE DATE_TRUNC('DAY', z_customer_accounts.created_at)"
        "<='2023-06-26T23:59:59' GROUP BY DATE_TRUNC('MONTH', z_customer_accounts.created_at),"
        "z_customer_accounts.account_type ORDER BY z_customer_accounts_number_of_account_"
        f"customer_connections DESC NULLS LAST) ,{cte_2} AS (SELECT DATE_TRUNC('MONTH', "
        "aa_acquired_accounts.created_at) as aa_acquired_accounts_created_month,"
        "aa_acquired_accounts.type as aa_acquired_accounts_account_type,"
        "COUNT(aa_acquired_accounts.account_id) as aa_acquired_accounts_number_of_acquired_accounts "
        "FROM analytics.accounts aa_acquired_accounts WHERE DATE_TRUNC('DAY', "
        "aa_acquired_accounts.created_at)<='2023-06-26T23:59:59' GROUP BY DATE_TRUNC('MONTH', "
        "aa_acquired_accounts.created_at),aa_acquired_accounts.type ORDER BY "
        "aa_acquired_accounts_number_of_acquired_accounts DESC NULLS LAST) SELECT "
        f"{cte_2}.aa_acquired_accounts_number_of_acquired_accounts as "
        f"aa_acquired_accounts_number_of_acquired_accounts,{cte_1}."
        "z_customer_accounts_number_of_account_customer_connections as z_customer_accounts_"
        f"number_of_account_customer_connections,ifnull({cte_2}.aa_acquired_accounts_created_month, "
        f"{cte_1}.z_customer_accounts_created_month) as aa_acquired_accounts_created_month,ifnull("
        f"{cte_2}.aa_acquired_accounts_account_type, {cte_1}.z_customer_accounts_type_of_account) "
        f"as aa_acquired_accounts_account_type,ifnull({cte_1}.z_customer_accounts_created_month, "
        f"{cte_2}.aa_acquired_accounts_created_month) as z_customer_accounts_created_month,"
        f"ifnull({cte_1}.z_customer_accounts_type_of_account, "
        f"{cte_2}.aa_acquired_accounts_account_type) as z_customer_accounts_type_of_account "
        f"FROM {cte_2} FULL OUTER JOIN {cte_1} ON {cte_2}.aa_acquired_accounts_created_month"
        f"={cte_1}.z_customer_accounts_created_month and {cte_2}.aa_acquired_accounts_account_type"
        f"={cte_1}.z_customer_accounts_type_of_account;"
    )
    assert query == correct


@pytest.mark.query
def test_query_number_metric_with_non_matching_canon_dates(connection):
    query = connection.get_sql_query(
        metrics=["unique_users_per_form_submission"],
        dimensions=["date"],
        where=[
            {
                "field": "date",
                "expression": "less_or_equal_than",
                "value": datetime.datetime(2023, 6, 26, 23, 59, 59),
            },
        ],
    )

    cte_1 = "submitted_form_sent_at__cte_subquery_0"
    cte_2 = "submitted_form_session__cte_subquery_1"
    correct = (
        f"WITH {cte_1} AS (SELECT DATE_TRUNC('DAY', "
        "submitted_form.sent_at) as submitted_form_sent_at_date,"
        "COUNT(DISTINCT(submitted_form.customer_id)) as submitted_form_unique_users_form_submissions "
        "FROM analytics.submitted_form submitted_form WHERE DATE_TRUNC('DAY', "
        "submitted_form.sent_at)<='2023-06-26T23:59:59' "
        "GROUP BY DATE_TRUNC('DAY', submitted_form.sent_at) "
        "ORDER BY submitted_form_unique_users_form_submissions DESC NULLS LAST) ,"
        f"{cte_2} AS (SELECT DATE_TRUNC('DAY', "
        "submitted_form.session_date) as submitted_form_session_date,"
        "COUNT(submitted_form.id) as submitted_form_number_of_form_submissions "
        "FROM analytics.submitted_form submitted_form WHERE DATE_TRUNC('DAY', "
        "submitted_form.session_date)<='2023-06-26T23:59:59' GROUP BY DATE_TRUNC('DAY', "
        "submitted_form.session_date) ORDER BY submitted_form_number_of_form_submissions DESC NULLS LAST) "
        f"SELECT {cte_1}.submitted_form_unique_users_form_submissions "
        f"as submitted_form_unique_users_form_submissions,{cte_2}."
        "submitted_form_number_of_form_submissions as submitted_form_number_of_form_submissions,"
        f"ifnull({cte_1}.submitted_form_sent_at_date, "
        f"{cte_2}.submitted_form_session_date) as "
        f"submitted_form_sent_at_date,ifnull({cte_2}."
        f"submitted_form_session_date, {cte_1}."
        "submitted_form_sent_at_date) as submitted_form_session_date,"
        "submitted_form_unique_users_form_submissions / submitted_form_number_of_form_submissions "
        f"as submitted_form_unique_users_per_form_submission FROM {cte_1} "
        f"FULL OUTER JOIN {cte_2} ON {cte_1}"
        f".submitted_form_sent_at_date={cte_2}.submitted_form_session_date;"
    )
    assert query == correct


@pytest.mark.query
def test_query_number_metric_with_non_matching_canon_dates_join_graphs(connection):
    field = connection.project.get_field("unique_users_per_form_submission")
    join_hash = connection.project.join_graph.join_graph_hash(field.view.name)
    join_graphs = field.join_graphs()
    assert join_hash in join_graphs


@pytest.mark.query
def test_query_merge_results_no_metric_date(connection):
    query = connection.get_sql_query(
        metrics=[],
        dimensions=["date", "customers.customer_id", "orders.order_id"],
        where=[
            {
                "field": "date",
                "expression": "greater_than",
                "value": "2023-02-01",
            },
        ],
    )

    correct = (
        "SELECT DATE_TRUNC('DAY', orders.order_date) as orders_order_date,"
        "customers.customer_id as customers_customer_id,orders.id as orders_order_id "
        "FROM analytics.orders orders LEFT JOIN analytics.customers customers "
        "ON orders.customer_id=customers.customer_id WHERE DATE_TRUNC('DAY', orders.order_date)>'2023-02-01' "
        "GROUP BY DATE_TRUNC('DAY', orders.order_date),customers.customer_id,orders.id "
        "ORDER BY orders_order_date ASC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_mapping_triple(connection):
    query = connection.get_sql_query(
        metrics=["number_of_sessions", "number_of_events"], dimensions=["device"]
    )

    correct = (
        "WITH sessions_session__cte_subquery_1 AS (SELECT sessions.session_device as "
        "sessions_session_device,COUNT(sessions.id) as sessions_number_of_sessions "
        "FROM analytics.sessions sessions GROUP BY sessions.session_device ORDER BY "
        "sessions_number_of_sessions DESC NULLS LAST) ,events_event__cte_subquery_0 AS ("
        "SELECT events.device as events_device,COUNT(DISTINCT(events.id)) as "
        "events_number_of_events FROM analytics.events events GROUP BY events.device "
        "ORDER BY events_number_of_events DESC NULLS LAST) SELECT events_event__cte_subquery_0."
        "events_number_of_events as events_number_of_events,sessions_session__cte_subquery_1"
        ".sessions_number_of_sessions as sessions_number_of_sessions,ifnull("
        "events_event__cte_subquery_0.events_device, sessions_session__cte_subquery_1."
        "sessions_session_device) as "
        "events_device,ifnull(sessions_session__cte_subquery_1.sessions_session_device, "
        "events_event__cte_subquery_0.events_device) "
        "as sessions_session_device FROM events_event__cte_subquery_0 "
        "FULL OUTER JOIN sessions_session__cte_subquery_1 ON events_event__cte_subquery_0."
        "events_device=sessions_session__cte_subquery_1.sessions_session_device;"
    )
    assert query == correct
