from datetime import datetime

import pytest

from metrics_layer.core.exceptions import JoinError, QueryError
from metrics_layer.core.model.definitions import Definitions
from metrics_layer.core.sql.arbitrary_merge_resolve import ArbitraryMergedQueryResolver


@pytest.mark.query
def test_query_no_inputs(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query()

    error_message = 'No metrics or dimensions specified. Please provide either "metrics" or "dimensions"'
    assert isinstance(exc_info.value, QueryError)
    assert str(exc_info.value) == error_message


@pytest.mark.query
@pytest.mark.parametrize(
    "query_2",
    [
        ["this should be a dict"],
        {"metrics": []},
        {"metrics": ["number_of_sessions"], "dimensions": ["device"]},
        {
            "metrics": ["number_of_events"],
            "dimensions": ["device"],
            "join_fields": [{"field": "events.device"}],
        },
        {
            "metrics": ["number_of_events"],
            "dimensions": ["device"],
            "join_fields": [{"source_field": "sessions.session_device"}],
        },
    ],
)
def test_query_bad_merged_queries(connection, query_2):
    with pytest.raises(QueryError) as exc_info:
        primary_query = {"metrics": ["number_of_sessions"], "dimensions": ["device"]}
        connection.get_sql_query(merged_queries=[primary_query, query_2])

    assert isinstance(exc_info.value, QueryError)


@pytest.mark.query
def test_query_merged_queries_simple_one_dimension(connection):
    query_2 = {
        "metrics": ["number_of_events"],
        "dimensions": ["device"],
        "join_fields": [{"field": "EVENTS.DEVICE", "source_field": "sessions.session_device"}],
    }

    primary_query = {"metrics": ["number_of_sessions"], "dimensions": ["device"]}
    query = connection.get_sql_query(merged_queries=[primary_query, query_2])

    correct = (
        "WITH merged_query_0 AS (SELECT sessions.session_device as sessions_session_device,COUNT(sessions.id)"
        " as sessions_number_of_sessions FROM analytics.sessions sessions GROUP BY sessions.session_device"
        " ORDER BY sessions_number_of_sessions DESC NULLS LAST) ,merged_query_1 AS (SELECT events.device as"
        " events_device,COUNT(DISTINCT(events.id)) as events_number_of_events FROM analytics.events events"
        " GROUP BY events.device ORDER BY events_number_of_events DESC NULLS LAST) SELECT"
        " merged_query_0.sessions_number_of_sessions as"
        " sessions_number_of_sessions,merged_query_0.sessions_session_device as"
        " sessions_session_device,merged_query_1.events_number_of_events as events_number_of_events FROM"
        " merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.sessions_session_device=merged_query_1.events_device;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_simple_two_dimension_all_mapped(connection):
    query_2 = {
        "metrics": ["number_of_events"],
        "dimensions": ["events.device", "event_campaign"],
        "join_fields": [
            {"field": "events.device", "source_field": "sessions.session_device"},
            {"field": "events.event_campaign", "source_field": "sessions.utm_campaign"},
        ],
    }

    primary_query = {
        "metrics": ["number_of_sessions"],
        "dimensions": ["utm_campaign", "sessions.session_device"],
    }
    query = connection.get_sql_query(merged_queries=[primary_query, query_2])

    correct = (
        "WITH merged_query_0 AS (SELECT sessions.utm_campaign as"
        " sessions_utm_campaign,sessions.session_device as sessions_session_device,COUNT(sessions.id) as"
        " sessions_number_of_sessions FROM analytics.sessions sessions GROUP BY"
        " sessions.utm_campaign,sessions.session_device ORDER BY sessions_number_of_sessions DESC NULLS LAST)"
        " ,merged_query_1 AS (SELECT events.device as events_device,events.campaign as"
        " events_event_campaign,COUNT(DISTINCT(events.id)) as events_number_of_events FROM analytics.events"
        " events GROUP BY events.device,events.campaign ORDER BY events_number_of_events DESC NULLS LAST)"
        " SELECT merged_query_0.sessions_number_of_sessions as"
        " sessions_number_of_sessions,merged_query_0.sessions_utm_campaign as"
        " sessions_utm_campaign,merged_query_0.sessions_session_device as"
        " sessions_session_device,merged_query_1.events_number_of_events as events_number_of_events FROM"
        " merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.sessions_session_device=merged_query_1.events_device and"
        " merged_query_0.sessions_utm_campaign=merged_query_1.events_event_campaign;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("join_type", ["inner", "left_outer", "full_outer"])
def test_query_merged_queries_simple_two_dimension_one_mapped(connection, join_type):
    query_2 = {
        "metrics": ["number_of_events"],
        "dimensions": ["event_campaign"],
        "join_fields": [{"field": "events.event_campaign", "source_field": "sessions.utm_campaign"}],
        "join_type": join_type,
    }

    primary_query = {
        "metrics": ["number_of_sessions"],
        "dimensions": ["utm_campaign", "sessions.session_device"],
    }
    query = connection.get_sql_query(merged_queries=[primary_query, query_2])

    join_lookup = {
        "inner": "JOIN",
        "left_outer": "LEFT JOIN",
        "full_outer": "FULL OUTER JOIN",
    }
    join_logic = join_lookup[join_type]
    correct = (
        "WITH merged_query_0 AS (SELECT sessions.utm_campaign as"
        " sessions_utm_campaign,sessions.session_device as sessions_session_device,COUNT(sessions.id) as"
        " sessions_number_of_sessions FROM analytics.sessions sessions GROUP BY"
        " sessions.utm_campaign,sessions.session_device ORDER BY sessions_number_of_sessions DESC NULLS LAST)"
        " ,merged_query_1 AS (SELECT events.campaign as events_event_campaign,COUNT(DISTINCT(events.id)) as"
        " events_number_of_events FROM analytics.events events GROUP BY events.campaign ORDER BY"
        " events_number_of_events DESC NULLS LAST) SELECT merged_query_0.sessions_number_of_sessions as"
        " sessions_number_of_sessions,merged_query_0.sessions_utm_campaign as"
        " sessions_utm_campaign,merged_query_0.sessions_session_device as"
        " sessions_session_device,merged_query_1.events_number_of_events as events_number_of_events FROM"
        f" merged_query_0 {join_logic} merged_query_1 ON"
        " merged_query_0.sessions_utm_campaign=merged_query_1.events_event_campaign;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_same_dimension_one_mapped_filter(connection):
    primary_query = {"metrics": ["line_item_aov"], "dimensions": ["product_name"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "where": [{"field": "new_vs_repeat", "expression": "not_equal_to", "value": "New"}],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }

    query = connection.get_sql_query(merged_queries=[primary_query, query_2])

    correct = (
        "WITH merged_query_0 AS (WITH order_lines_order__cte_subquery_0 AS (SELECT order_lines.product_name"
        " as order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines GROUP BY order_lines.product_name ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST) ,orders_order__cte_subquery_1 AS (SELECT"
        " order_lines.product_name as order_lines_product_name,NULLIF(COUNT(DISTINCT CASE WHEN  (orders.id) "
        " IS NOT NULL THEN  orders.id  ELSE NULL END), 0) as orders_number_of_orders FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id GROUP BY order_lines.product_name ORDER BY"
        " orders_number_of_orders DESC NULLS LAST) SELECT"
        " order_lines_order__cte_subquery_0.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue,orders_order__cte_subquery_1.orders_number_of_orders as"
        " orders_number_of_orders,ifnull(order_lines_order__cte_subquery_0.order_lines_product_name,"
        " orders_order__cte_subquery_1.order_lines_product_name) as"
        " order_lines_product_name,order_lines_total_item_revenue / orders_number_of_orders as"
        " order_lines_line_item_aov FROM order_lines_order__cte_subquery_0 FULL OUTER JOIN"
        " orders_order__cte_subquery_1 ON"
        " order_lines_order__cte_subquery_0.order_lines_product_name=orders_order__cte_subquery_1.order_lines_product_name)"  # noqa
        " ,merged_query_1 AS (SELECT order_lines.product_name as"
        " order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id WHERE orders.new_vs_repeat<>'New' GROUP BY"
        " order_lines.product_name ORDER BY order_lines_total_item_revenue DESC NULLS LAST) SELECT"
        " merged_query_0.order_lines_line_item_aov as"
        " order_lines_line_item_aov,merged_query_0.order_lines_product_name as"
        " order_lines_product_name,merged_query_1.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.order_lines_product_name=merged_query_1.order_lines_product_name;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_same_dimension_same_measure(connection):
    primary_query = {"metrics": ["total_item_revenue"], "dimensions": ["product_name"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "where": [{"field": "new_vs_repeat", "expression": "not_equal_to", "value": "New"}],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }

    query = connection.get_sql_query(merged_queries=[primary_query, query_2])

    correct = (
        "WITH merged_query_0 AS (SELECT order_lines.product_name as"
        " order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines GROUP BY order_lines.product_name ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST) ,merged_query_1 AS (SELECT order_lines.product_name"
        " as order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id WHERE orders.new_vs_repeat<>'New' GROUP BY"
        " order_lines.product_name ORDER BY order_lines_total_item_revenue DESC NULLS LAST) SELECT"
        " merged_query_0.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue,merged_query_0.order_lines_product_name as order_lines_product_name"
        " FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.order_lines_product_name=merged_query_1.order_lines_product_name;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_same_dimension_same_measure_with_extra(connection):
    primary_query = {"metrics": ["total_item_revenue"], "dimensions": ["product_name"]}
    query_2 = {
        "metrics": ["total_item_revenue", "number_of_email_purchased_items"],
        "dimensions": ["product_name"],
        "where": [{"field": "new_vs_repeat", "expression": "not_equal_to", "value": "New"}],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }

    query = connection.get_sql_query(merged_queries=[primary_query, query_2])

    correct = (
        "WITH merged_query_0 AS (SELECT order_lines.product_name as"
        " order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines GROUP BY order_lines.product_name ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST) ,merged_query_1 AS (SELECT order_lines.product_name"
        " as order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue,COUNT(case"
        " when order_lines.sales_channel='Email' then order_lines.order_id end) as"
        " order_lines_number_of_email_purchased_items FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE orders.new_vs_repeat<>'New'"
        " GROUP BY order_lines.product_name ORDER BY order_lines_total_item_revenue DESC NULLS LAST) SELECT"
        " merged_query_0.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue,merged_query_0.order_lines_product_name as"
        " order_lines_product_name,merged_query_1.order_lines_number_of_email_purchased_items as"
        " order_lines_number_of_email_purchased_items FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.order_lines_product_name=merged_query_1.order_lines_product_name;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_all_options_in_second_query(connection):
    primary_query = {
        "metrics": ["number_of_email_purchased_items"],
        "dimensions": ["product_name"],
        "where": [{"field": "product_name", "expression": "is_not_null", "value": None}],
    }
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "where": [{"field": "new_vs_repeat", "expression": "not_equal_to", "value": "New"}],
        "having": [{"field": "total_item_revenue", "expression": "greater_than", "value": 100}],
        "order_by": [{"field": "total_item_revenue", "sort": "desc"}],
        "limit": 10,
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }

    query = connection.get_sql_query(merged_queries=[primary_query, query_2])

    correct = (
        "WITH merged_query_0 AS (SELECT order_lines.product_name as order_lines_product_name,COUNT(case when"
        " order_lines.sales_channel='Email' then order_lines.order_id end) as"
        " order_lines_number_of_email_purchased_items FROM analytics.order_line_items order_lines WHERE NOT"
        " order_lines.product_name IS NULL GROUP BY order_lines.product_name ORDER BY"
        " order_lines_number_of_email_purchased_items DESC NULLS LAST) ,merged_query_1 AS (SELECT"
        " order_lines.product_name as order_lines_product_name,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE orders.new_vs_repeat<>'New'"
        " GROUP BY order_lines.product_name HAVING SUM(order_lines.revenue)>100 ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST LIMIT 10) SELECT"
        " merged_query_0.order_lines_number_of_email_purchased_items as"
        " order_lines_number_of_email_purchased_items,merged_query_0.order_lines_product_name as"
        " order_lines_product_name,merged_query_1.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.order_lines_product_name=merged_query_1.order_lines_product_name;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_order_by_asc_post_merge(connection):
    primary_query = {"metrics": ["number_of_email_purchased_items"], "dimensions": ["product_name"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }

    query = connection.get_sql_query(
        merged_queries=[primary_query, query_2],
        order_by=[{"field": "number_of_email_purchased_items"}],
    )

    correct = (
        "WITH merged_query_0 AS (SELECT order_lines.product_name as order_lines_product_name,COUNT(case when"
        " order_lines.sales_channel='Email' then order_lines.order_id end) as"
        " order_lines_number_of_email_purchased_items FROM analytics.order_line_items order_lines GROUP BY"
        " order_lines.product_name ORDER BY order_lines_number_of_email_purchased_items DESC NULLS LAST)"
        " ,merged_query_1 AS (SELECT order_lines.product_name as"
        " order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines GROUP BY order_lines.product_name ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST) SELECT"
        " merged_query_0.order_lines_number_of_email_purchased_items as"
        " order_lines_number_of_email_purchased_items,merged_query_0.order_lines_product_name as"
        " order_lines_product_name,merged_query_1.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.order_lines_product_name=merged_query_1.order_lines_product_name ORDER BY"
        " merged_query_0.order_lines_number_of_email_purchased_items ASC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_order_by_limit_post_merge(connection):
    primary_query = {"metrics": ["number_of_email_purchased_items"], "dimensions": ["product_name"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }

    query = connection.get_sql_query(
        merged_queries=[primary_query, query_2],
        order_by=[{"field": "total_item_revenue", "sort": "desc"}],
        limit=5,
    )

    correct = (
        "WITH merged_query_0 AS (SELECT order_lines.product_name as order_lines_product_name,COUNT(case when"
        " order_lines.sales_channel='Email' then order_lines.order_id end) as"
        " order_lines_number_of_email_purchased_items FROM analytics.order_line_items order_lines GROUP BY"
        " order_lines.product_name ORDER BY order_lines_number_of_email_purchased_items DESC NULLS LAST)"
        " ,merged_query_1 AS (SELECT order_lines.product_name as"
        " order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines GROUP BY order_lines.product_name ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST) SELECT"
        " merged_query_0.order_lines_number_of_email_purchased_items as"
        " order_lines_number_of_email_purchased_items,merged_query_0.order_lines_product_name as"
        " order_lines_product_name,merged_query_1.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.order_lines_product_name=merged_query_1.order_lines_product_name ORDER BY"
        " merged_query_1.order_lines_total_item_revenue DESC NULLS LAST LIMIT 5;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery])
def test_query_merged_queries_dim_group(connection, query_type):
    primary_query = {"metrics": ["number_of_orders"], "dimensions": ["date"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name", "date"],
        "where": [{"field": "date", "expression": "greater_than", "value": datetime(2018, 1, 2)}],
        "join_fields": [{"field": "order_lines.order_date", "source_field": "orders.order_date"}],
    }
    query = connection.get_sql_query(merged_queries=[primary_query, query_2], query_type=query_type)

    if query_type == Definitions.bigquery:
        lines_date_trunc = "CAST(DATE_TRUNC(CAST(order_lines.order_date AS DATE), DAY) AS DATE)"
        lines_date_trunc_group = "order_lines_order_date"
        orders_date_trunc = "CAST(DATE_TRUNC(CAST(orders.order_date AS DATE), DAY) AS TIMESTAMP)"
        orders_date_trunc_group = "orders_order_date"
        product_group = "order_lines_product_name"
        lines_order_by = ""
        orders_order_by = ""
        time = "CAST(CAST('2018-01-02 00:00:00' AS TIMESTAMP) AS DATE)"
        condition = (
            "CAST(merged_query_0.orders_order_date AS TIMESTAMP)=CAST(merged_query_1.order_lines_order_date"
            " AS TIMESTAMP)"
        )
    else:
        lines_date_trunc_group = lines_date_trunc = "DATE_TRUNC('DAY', order_lines.order_date)"
        orders_date_trunc_group = orders_date_trunc = "DATE_TRUNC('DAY', orders.order_date)"
        lines_order_by = " ORDER BY order_lines_total_item_revenue DESC NULLS LAST"
        orders_order_by = " ORDER BY orders_number_of_orders DESC NULLS LAST"
        product_group = "order_lines.product_name"
        time = "'2018-01-02T00:00:00'"
        condition = "merged_query_0.orders_order_date=merged_query_1.order_lines_order_date"
    correct = (
        f"WITH merged_query_0 AS (SELECT {orders_date_trunc} as orders_order_date,COUNT(orders.id) as"
        " orders_number_of_orders FROM analytics.orders orders GROUP BY"
        f" {orders_date_trunc_group}{orders_order_by}) ,merged_query_1 AS (SELECT order_lines.product_name"
        f" as order_lines_product_name,{lines_date_trunc} as order_lines_order_date,SUM(order_lines.revenue)"
        " as order_lines_total_item_revenue FROM analytics.order_line_items order_lines WHERE"
        f" {lines_date_trunc}>{time} GROUP BY"
        f" {product_group},{lines_date_trunc_group}{lines_order_by}) SELECT"
        " merged_query_0.orders_number_of_orders as orders_number_of_orders,merged_query_0.orders_order_date"
        " as orders_order_date,merged_query_1.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue,merged_query_1.order_lines_product_name as order_lines_product_name"
        " FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        f" {condition};"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_three_way(connection):
    primary_query = {"metrics": ["number_of_email_purchased_items"], "dimensions": ["product_name", "date"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["date"],
        "join_fields": [{"field": "order_lines.order_date", "source_field": "order_lines.order_date"}],
    }
    query_3 = {
        "metrics": ["total_item_costs"],
        "dimensions": ["product_name"],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }

    query = connection.get_sql_query(merged_queries=[primary_query, query_2, query_3])

    correct = (
        "WITH merged_query_0 AS (SELECT order_lines.product_name as"
        " order_lines_product_name,DATE_TRUNC('DAY', order_lines.order_date) as"
        " order_lines_order_date,COUNT(case when order_lines.sales_channel='Email' then order_lines.order_id"
        " end) as order_lines_number_of_email_purchased_items FROM analytics.order_line_items order_lines"
        " GROUP BY order_lines.product_name,DATE_TRUNC('DAY', order_lines.order_date) ORDER BY"
        " order_lines_number_of_email_purchased_items DESC NULLS LAST) ,merged_query_1 AS (SELECT"
        " DATE_TRUNC('DAY', order_lines.order_date) as order_lines_order_date,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines GROUP BY"
        " DATE_TRUNC('DAY', order_lines.order_date) ORDER BY order_lines_total_item_revenue DESC NULLS LAST)"
        " ,merged_query_2 AS (SELECT order_lines.product_name as order_lines_product_name,SUM(case when"
        " order_lines.product_name='Portable Charger' and order_lines.product_name IN ('Portable"
        " Charger','Dual Charger') and orders.revenue * 100>100 then order_lines.item_costs end) as"
        " order_lines_total_item_costs FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders"
        " orders ON order_lines.order_unique_id=orders.id GROUP BY order_lines.product_name ORDER BY"
        " order_lines_total_item_costs DESC NULLS LAST) SELECT"
        " merged_query_0.order_lines_number_of_email_purchased_items as"
        " order_lines_number_of_email_purchased_items,merged_query_0.order_lines_product_name as"
        " order_lines_product_name,merged_query_0.order_lines_order_date as"
        " order_lines_order_date,merged_query_1.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue,merged_query_2.order_lines_total_item_costs as"
        " order_lines_total_item_costs FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.order_lines_order_date=merged_query_1.order_lines_order_date LEFT JOIN"
        " merged_query_2 ON merged_query_0.order_lines_product_name=merged_query_2.order_lines_product_name;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_where_having_post_merge(connection):
    primary_query = {"metrics": ["number_of_email_purchased_items"], "dimensions": ["product_name"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }

    query = connection.get_sql_query(
        merged_queries=[primary_query, query_2],
        where=[{"field": "product_name", "expression": "not_equal_to", "value": "East"}],
        having=[{"field": "total_item_revenue", "expression": "greater_than", "value": 100}],
    )

    correct = (
        "WITH merged_query_0 AS (SELECT order_lines.product_name as order_lines_product_name,COUNT(case when"
        " order_lines.sales_channel='Email' then order_lines.order_id end) as"
        " order_lines_number_of_email_purchased_items FROM analytics.order_line_items order_lines GROUP BY"
        " order_lines.product_name ORDER BY order_lines_number_of_email_purchased_items DESC NULLS LAST)"
        " ,merged_query_1 AS (SELECT order_lines.product_name as"
        " order_lines_product_name,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines GROUP BY order_lines.product_name ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST) SELECT"
        " merged_query_0.order_lines_number_of_email_purchased_items as"
        " order_lines_number_of_email_purchased_items,merged_query_0.order_lines_product_name as"
        " order_lines_product_name,merged_query_1.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.order_lines_product_name=merged_query_1.order_lines_product_name WHERE"
        " merged_query_0.order_lines_product_name<>'East' AND"
        " merged_query_1.order_lines_total_item_revenue>100;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_mapping_lookup_on_resolver(connection):
    query_1 = {
        "metrics": ["number_of_events"],
        "dimensions": ["events.device", "date"],
    }
    query_2 = {
        "metrics": ["number_of_login_events"],
        "dimensions": ["login_events.device"],
        "join_fields": [{"field": "device", "source_field": "device"}],
    }
    resolver = ArbitraryMergedQueryResolver(
        merged_queries=[query_1, query_2], query_type="SNOWFLAKE", project=connection.project
    )
    assert resolver.mapping_lookup == {"date": "events.event_date", "device": "events.device"}


@pytest.mark.query
def test_query_merged_queries_mapped_where_post_merge(connection):
    primary_query = {"metrics": ["number_of_email_purchased_items"], "dimensions": ["campaign"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["campaign"],
        "join_fields": [{"field": "campaign", "source_field": "campaign"}],
    }

    query = connection.get_sql_query(
        merged_queries=[primary_query, query_2],
        where=[{"field": "campaign", "expression": "not_equal_to", "value": "Facebook-Promo"}],
    )

    correct = (
        "WITH merged_query_0 AS (SELECT orders.campaign as orders_campaign,COUNT(case when"
        " order_lines.sales_channel='Email' then order_lines.order_id end) as"
        " order_lines_number_of_email_purchased_items FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.orders orders ON order_lines.order_unique_id=orders.id GROUP BY orders.campaign ORDER BY"
        " order_lines_number_of_email_purchased_items DESC NULLS LAST) ,merged_query_1 AS (SELECT"
        " orders.campaign as orders_campaign,SUM(order_lines.revenue) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id GROUP BY orders.campaign ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST) SELECT"
        " merged_query_0.order_lines_number_of_email_purchased_items as"
        " order_lines_number_of_email_purchased_items,merged_query_0.orders_campaign as"
        " orders_campaign,merged_query_1.order_lines_total_item_revenue as order_lines_total_item_revenue"
        " FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.orders_campaign=merged_query_1.orders_campaign WHERE"
        " merged_query_0.orders_campaign<>'Facebook-Promo';"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_handle_mappings_in_join_fields(connection):
    primary_query = {"metrics": ["number_of_orders"], "dimensions": ["date"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name", "date"],
        "join_fields": [{"field": "date", "source_field": "date"}],
    }
    query = connection.get_sql_query(merged_queries=[primary_query, query_2])

    correct = (
        "WITH merged_query_0 AS (SELECT DATE_TRUNC('DAY', orders.order_date) as"
        " orders_order_date,COUNT(orders.id) as orders_number_of_orders FROM analytics.orders orders GROUP BY"
        " DATE_TRUNC('DAY', orders.order_date) ORDER BY orders_number_of_orders DESC NULLS LAST)"
        " ,merged_query_1 AS (SELECT order_lines.product_name as order_lines_product_name,DATE_TRUNC('DAY',"
        " order_lines.order_date) as order_lines_order_date,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines GROUP BY"
        " order_lines.product_name,DATE_TRUNC('DAY', order_lines.order_date) ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST) SELECT merged_query_0.orders_number_of_orders as"
        " orders_number_of_orders,merged_query_0.orders_order_date as"
        " orders_order_date,merged_query_1.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue,merged_query_1.order_lines_product_name as order_lines_product_name"
        " FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.orders_order_date=merged_query_1.order_lines_order_date;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_handle_non_date_mappings_in_join_fields(connection):
    query_1 = {
        "metrics": ["number_of_events"],
        "dimensions": ["events.device", "date"],
    }
    query_2 = {
        "metrics": ["number_of_login_events"],
        "dimensions": ["login_events.device"],
        "join_fields": [{"field": "device", "source_field": "device"}],
    }
    query = connection.get_sql_query(merged_queries=[query_1, query_2])

    correct = (
        "WITH merged_query_0 AS (SELECT events.device as events_device,DATE_TRUNC('DAY', events.event_date)"
        " as events_event_date,COUNT(DISTINCT(events.id)) as events_number_of_events FROM analytics.events"
        " events GROUP BY events.device,DATE_TRUNC('DAY', events.event_date) ORDER BY events_number_of_events"
        " DESC NULLS LAST) ,merged_query_1 AS (SELECT events.device as"
        " login_events_device,COUNT(DISTINCT(login_events.id)) as login_events_number_of_login_events FROM"
        " analytics.login_events login_events LEFT JOIN analytics.events events ON login_events.id=events.id"
        " GROUP BY events.device ORDER BY login_events_number_of_login_events DESC NULLS LAST) SELECT"
        " merged_query_0.events_number_of_events as events_number_of_events,merged_query_0.events_device as"
        " events_device,merged_query_0.events_event_date as"
        " events_event_date,merged_query_1.login_events_number_of_login_events as"
        " login_events_number_of_login_events FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.events_device=merged_query_1.login_events_device;"
    )
    assert query == correct


@pytest.mark.query
def test_query_merged_queries_invalid_join_in_field(connection):
    primary_query = {"metrics": ["number_of_orders"], "dimensions": ["date"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name", "date"],
        "join_fields": [{"field": "orders.order_date", "source_field": "order_lines.order_date"}],
    }
    with pytest.raises(JoinError) as exc_info:
        connection.get_sql_query(merged_queries=[primary_query, query_2])

    assert isinstance(exc_info.value, JoinError)
    assert (
        str(exc_info.value)
        == "Join field orders.order_date not found in the query number 2. To be used as a join the field must"
        " be included in query 2."
    )


@pytest.mark.query
def test_query_merged_queries_invalid_join_in_source_field(connection):
    primary_query = {"metrics": ["number_of_email_purchased_items"], "dimensions": ["new_vs_repeat"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }
    with pytest.raises(JoinError) as exc_info:
        connection.get_sql_query(merged_queries=[primary_query, query_2])

    assert isinstance(exc_info.value, JoinError)
    assert (
        str(exc_info.value)
        == "Join field order_lines.product_name not found in the query number 1. To be used as a join the"
        " field must be included in query 1."
    )


@pytest.mark.query
def test_query_merged_queries_invalid_where_post_merge(connection):
    primary_query = {"metrics": ["number_of_email_purchased_items"], "dimensions": ["product_name"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            merged_queries=[primary_query, query_2],
            where=[{"field": "new_vs_repeat", "expression": "equal_to", "value": "New"}],
        )

    assert isinstance(exc_info.value, QueryError)
    assert (
        str(exc_info.value)
        == "Field orders.NEW_VS_REPEAT is not present in either source query, so it cannot be applied as a"
        " filter. Please add it to one of the source queries."
    )


@pytest.mark.query
def test_query_merged_queries_invalid_having_post_merge(connection):
    primary_query = {"metrics": ["number_of_email_purchased_items"], "dimensions": ["product_name"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            merged_queries=[primary_query, query_2],
            having=[{"field": "total_item_costs", "expression": "equal_to", "value": 10}],
        )

    assert isinstance(exc_info.value, QueryError)
    assert (
        str(exc_info.value)
        == "Field order_lines.TOTAL_ITEM_COSTS is not present in either source query, so it cannot be applied"
        " as a filter. Please add it to one of the source queries."
    )


@pytest.mark.query
def test_query_merged_queries_invalid_order_by_post_merge(connection):
    primary_query = {"metrics": ["number_of_email_purchased_items"], "dimensions": ["product_name"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            merged_queries=[primary_query, query_2], order_by=[{"field": "total_item_costs"}]
        )

    assert isinstance(exc_info.value, QueryError)
    assert (
        str(exc_info.value)
        == "Field order_lines.TOTAL_ITEM_COSTS is not present in either source query, so it cannot be applied"
        " as a filter. Please add it to one of the source queries."
    )


@pytest.mark.query
@pytest.mark.parametrize(
    "query_type", [Definitions.snowflake, Definitions.bigquery, Definitions.redshift, Definitions.duck_db]
)
def test_query_merged_queries_all_db_flavors(connection, query_type):
    primary_query = {"metrics": ["number_of_email_purchased_items"], "dimensions": ["product_name"]}
    query_2 = {
        "metrics": ["total_item_revenue"],
        "dimensions": ["product_name"],
        "join_fields": [{"field": "order_lines.product_name", "source_field": "order_lines.product_name"}],
    }

    query = connection.get_sql_query(merged_queries=[primary_query, query_2], query_type=query_type)

    if query_type != Definitions.bigquery:
        order_by_1 = " ORDER BY order_lines_number_of_email_purchased_items DESC NULLS LAST"
        order_by_2 = " ORDER BY order_lines_total_item_revenue DESC NULLS LAST"
        group_by_product = "order_lines.product_name"
    else:
        order_by_1 = ""
        order_by_2 = ""
        group_by_product = "order_lines_product_name"

    correct = (
        "WITH merged_query_0 AS (SELECT order_lines.product_name as order_lines_product_name,COUNT(case when"
        " order_lines.sales_channel='Email' then order_lines.order_id end) as"
        " order_lines_number_of_email_purchased_items FROM analytics.order_line_items order_lines GROUP BY"
        f" {group_by_product}{order_by_1}) ,merged_query_1"
        " AS (SELECT order_lines.product_name as order_lines_product_name,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines GROUP BY"
        f" {group_by_product}{order_by_2}) SELECT"
        " merged_query_0.order_lines_number_of_email_purchased_items as"
        " order_lines_number_of_email_purchased_items,merged_query_0.order_lines_product_name as"
        " order_lines_product_name,merged_query_1.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue FROM merged_query_0 LEFT JOIN merged_query_1 ON"
        " merged_query_0.order_lines_product_name=merged_query_1.order_lines_product_name;"
    )
    assert query == correct
