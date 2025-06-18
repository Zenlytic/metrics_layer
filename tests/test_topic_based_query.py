import pytest

from metrics_layer import MetricsLayerConnection
from metrics_layer.core.exceptions import (
    AccessDeniedOrDoesNotExistException,
    QueryError,
)


@pytest.mark.query
def test_topic_attributes(connection):
    topic = connection.project.get_topic("Order lines Topic")
    assert topic.base_view == "order_lines"
    assert topic.label == "Order lines Topic"
    assert topic.description == "Vanilla order lines topic description"
    assert topic.zoe_description == "Secret info that is only shown to zoe"
    assert not topic.hidden
    assert topic.required_access_grants == ["test_access_grant_department_topic"]


@pytest.mark.query
def test_query_topic_with_one_view(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"], dimensions=["channel"], topic="Order lines ONLY"
    )

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines"
        " GROUP BY order_lines.sales_channel ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_no_join_with_limit_in_topic_no_access_filter(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"], dimensions=["channel"], limit=300, topic="Order lines Topic"
    )

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE NOT orders.revenue IS NULL"
        " GROUP BY order_lines.sales_channel ORDER BY order_lines_total_item_revenue DESC NULLS LAST LIMIT"
        " 300;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("dept", ["sales", "executive"])
def test_query_access_grants_in_topic(connection, dept):
    connection.project.set_user({"department": dept})
    if dept == "executive":
        connection.get_sql_query(
            metrics=["total_item_revenue"],
            dimensions=["channel"],
            topic="Order lines Topic",
        )
    else:
        with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
            connection.get_sql_query(
                metrics=["total_item_revenue"],
                dimensions=["channel"],
                topic="Order lines Topic",
            )

        assert "Could not find or you do not have access to topic Order lines Topic" in str(exc_info.value)

    connection.project.set_user({})


@pytest.mark.query
def test_query_no_join_with_limit_in_topic_no_implicit_join(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"], dimensions=["channel"], limit=300, topic="Order lines unfiltered"
    )

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines GROUP BY"
        " order_lines.sales_channel ORDER BY order_lines_total_item_revenue DESC NULLS LAST LIMIT 300;"
    )

    assert query == correct


@pytest.mark.query
def test_query_no_join_with_limit_in_topic_with_access_filter(connection):
    connection.project.set_user({"warehouse_location": "New Jersey"})
    query = connection.get_sql_query(
        metrics=["total_item_revenue"], dimensions=["channel"], limit=300, topic="Order lines unfiltered"
    )

    correct = (
        "SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE"
        " orders.warehouselocation='New Jersey' GROUP BY"
        " order_lines.sales_channel ORDER BY order_lines_total_item_revenue DESC NULLS LAST LIMIT 300;"
    )
    connection.project.set_user({})
    assert query == correct


@pytest.mark.query
def test_query_topic_with_no_override_access_filter(fresh_project, connections):
    # Without the topic overriding the view level access filter, the query
    # will contain both the topic and view level access filters
    fresh_project._topics[1]["views"]["orders"]["override_access_filters"] = False
    conn = MetricsLayerConnection(
        project=fresh_project, connections=connections, user={"allowed_order_ids": "1234567890,1344311"}
    )

    query = conn.get_sql_query(
        metrics=["total_item_revenue"], dimensions=["new_vs_repeat"], topic="Order lines unfiltered"
    )

    correct = (
        "SELECT orders.new_vs_repeat as orders_new_vs_repeat,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE order_lines.order_unique_id"
        " IN ('1234567890','1344311') and orders.id IN ('1234567890','1344311') GROUP BY orders.new_vs_repeat"
        " ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_topic_with_override_access_filter(connection):
    # With the topic overriding the view level access filter, the query
    # will contain only the topic access filter for that user_attribute
    connection.project.set_user({"allowed_order_ids": "1234567890,1344311"})
    query = connection.get_sql_query(
        metrics=["total_item_revenue"], dimensions=["new_vs_repeat"], topic="Order lines unfiltered"
    )

    correct = (
        "SELECT orders.new_vs_repeat as orders_new_vs_repeat,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE order_lines.order_unique_id"
        " IN ('1234567890','1344311') GROUP BY orders.new_vs_repeat ORDER BY order_lines_total_item_revenue"
        " DESC NULLS LAST;"
    )
    connection.project.set_user({})
    assert query == correct


@pytest.mark.query
def test_query_single_join_default_logic_in_topic(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["new_vs_repeat"],
        topic="Order lines unfiltered",
    )

    correct = (
        "SELECT orders.new_vs_repeat as orders_new_vs_repeat,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.orders orders ON order_lines.order_unique_id=orders.id GROUP BY orders.new_vs_repeat"
        " ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_custom_join_in_topic(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue", "total_discount_amt"],
        dimensions=["discount_code"],
        topic="Order lines unfiltered",
    )

    correct = (
        "SELECT discounts.code as discounts_discount_code,COALESCE(CAST((SUM(DISTINCT"
        " (CAST(FLOOR(COALESCE(order_lines.revenue, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) +"
        " (TO_NUMBER(MD5(order_lines.order_line_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
        " 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(order_lines.order_line_id),"
        " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) /"
        " CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as"
        " order_lines_total_item_revenue,COALESCE(CAST((SUM(DISTINCT"
        " (CAST(FLOOR(COALESCE(discounts.discount_amt, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) +"
        " (TO_NUMBER(MD5(discounts.discount_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38,"
        " 0)) - SUM(DISTINCT (TO_NUMBER(MD5(discounts.discount_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
        " 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as"
        " discounts_total_discount_amt FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics_live.discounts discounts ON order_lines.order_unique_id = discounts.order_id and"
        " DATE_TRUNC('DAY', discounts.order_date) is not null GROUP BY discounts.code ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_single_custom_join_with_hop_in_topic(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["discount_promo_name"],
        topic="Order lines unfiltered",
    )

    correct = (
        "SELECT discount_detail.promo_name as discount_detail_discount_promo_name,COALESCE(CAST((SUM(DISTINCT"
        " (CAST(FLOOR(COALESCE(order_lines.revenue, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) +"
        " (TO_NUMBER(MD5(order_lines.order_line_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
        " 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(order_lines.order_line_id),"
        " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) /"
        " CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as order_lines_total_item_revenue FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id LEFT JOIN analytics_live.discounts discounts ON"
        " order_lines.order_unique_id = discounts.order_id and DATE_TRUNC('DAY', discounts.order_date) is not"
        " null LEFT JOIN analytics.discount_detail discount_detail ON discounts.discount_id ="
        " discount_detail.discount_id and orders.id = discount_detail.order_id GROUP BY"
        " discount_detail.promo_name ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_invalid_field_in_topic(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["total_item_revenue"],
            dimensions=["rainfall"],
            topic="Order lines unfiltered",
        )

    error_message = "The following views are not included in the topic Order lines unfiltered: country_detail"
    assert isinstance(exc_info.value, QueryError)
    assert error_message in str(exc_info.value)


@pytest.mark.query
def test_query_join_as_in_topic(connection):
    query = connection.get_sql_query(
        metrics=["number_of_billed_accounts"],
        dimensions=["parent_account.account_name", "child_account.account_name"],
        topic="Recurring Revenue",
    )

    correct = (
        "SELECT parent_account.name as parent_account_account_name,child_account.name as"
        " child_account_account_name,COUNT(mrr.parent_account_id) as mrr_number_of_billed_accounts FROM"
        " analytics.mrr_by_customer mrr LEFT JOIN analytics.accounts child_account ON"
        " mrr.child_account_id=child_account.account_id LEFT JOIN analytics.accounts parent_account ON"
        " mrr.parent_account_id=parent_account.account_id GROUP BY parent_account.name,child_account.name"
        " ORDER BY mrr_number_of_billed_accounts DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_with_merged_result_outside_of_topic(connection):
    query = connection.get_sql_query(
        metrics=["costs_per_session"],
        dimensions=["sessions.utm_source"],
        topic="Order lines unfiltered",
    )

    correct = (
        "WITH order_lines_order__cte_subquery_0 AS (SELECT orders.sub_channel as "
        "orders_sub_channel,SUM(case when order_lines.product_name='Portable Charger'"
        " and order_lines.product_name IN ('Portable Charger','Dual Charger') "
        "and orders.revenue * 100>100 then order_lines.item_costs end) as "
        "order_lines_total_item_costs,COUNT(case when order_lines.sales_channel"
        "='Email' then order_lines.order_id end) as "
        "order_lines_number_of_email_purchased_items FROM analytics.order_line_items "
        "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id"
        "=orders.id GROUP BY orders.sub_channel ORDER BY order_lines_total_item_costs "
        "DESC NULLS LAST) ,sessions_session__cte_subquery_1 AS (SELECT sessions.utm_source"
        " as sessions_utm_source,COUNT(sessions.id) as sessions_number_of_sessions FROM "
        "analytics.sessions sessions GROUP BY sessions.utm_source "
        "ORDER BY sessions_number_of_sessions DESC NULLS LAST) SELECT "
        "order_lines_order__cte_subquery_0.order_lines_total_item_costs as "
        "order_lines_total_item_costs,order_lines_order__cte_subquery_0."
        "order_lines_number_of_email_purchased_items as order_lines_number_of_email_purchased_items,"
        "sessions_session__cte_subquery_1.sessions_number_of_sessions as sessions_number_of_sessions,"
        "ifnull(order_lines_order__cte_subquery_0.orders_sub_channel, "
        "sessions_session__cte_subquery_1.sessions_utm_source) as orders_sub_channel,"
        "ifnull(sessions_session__cte_subquery_1.sessions_utm_source, order_lines_order__cte_subquery_0"
        ".orders_sub_channel) as sessions_utm_source,(order_lines_total_item_costs * "
        "order_lines_number_of_email_purchased_items) / nullif(sessions_number_of_sessions, 0)"
        " as order_lines_costs_per_session FROM order_lines_order__cte_subquery_0 FULL OUTER "
        "JOIN sessions_session__cte_subquery_1 ON order_lines_order__cte_subquery_0."
        "orders_sub_channel=sessions_session__cte_subquery_1.sessions_utm_source;"
    )
    assert query == correct


@pytest.mark.query
def test_query_with_merged_result_outside_of_topic_join_implicit_merge(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["sessions.utm_source"],
        topic="Order lines unfiltered",
    )

    correct = (
        "WITH order_lines_order__cte_subquery_0 AS (SELECT orders.sub_channel as "
        "orders_sub_channel,SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
        "ON order_lines.order_unique_id=orders.id GROUP BY orders.sub_channel "
        "ORDER BY order_lines_total_item_revenue DESC NULLS LAST) ,"
        "sessions_session__cte_subquery_1 AS (SELECT sessions.utm_source as "
        "sessions_utm_source FROM analytics.sessions sessions GROUP BY "
        "sessions.utm_source ORDER BY sessions_utm_source ASC NULLS LAST) "
        "SELECT order_lines_order__cte_subquery_0.order_lines_total_item_revenue "
        "as order_lines_total_item_revenue,ifnull(order_lines_order__cte_subquery_0."
        "orders_sub_channel, sessions_session__cte_subquery_1.sessions_utm_source) "
        "as orders_sub_channel,ifnull(sessions_session__cte_subquery_1.sessions_utm_source,"
        " order_lines_order__cte_subquery_0.orders_sub_channel) as sessions_utm_source "
        "FROM order_lines_order__cte_subquery_0 FULL OUTER JOIN "
        "sessions_session__cte_subquery_1 ON order_lines_order__cte_subquery_0."
        "orders_sub_channel=sessions_session__cte_subquery_1.sessions_utm_source;"
    )
    assert query == correct


@pytest.mark.query
def test_query_with_merged_result_outside_of_topic_join_error(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["total_item_revenue"],
            dimensions=["sessions.session_device"],
            topic="Order lines unfiltered",
        )

    error_message = "The following views are not included in the topic Order lines unfiltered: sessions"
    assert isinstance(exc_info.value, QueryError)
    assert error_message in str(exc_info.value)
