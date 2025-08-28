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
        " analytics.orders orders ON order_lines.order_unique_id=orders.id WHERE"
        " order_lines.order_unique_id IN ('1234567890','1344311') and orders.id IN"
        " ('1234567890','1344311') GROUP BY orders.new_vs_repeat"
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
        " analytics.order_line_items order_lines LEFT JOIN analytics_live.discounts discounts ON"
        " order_lines.order_unique_id = discounts.order_id and DATE_TRUNC('DAY', discounts.order_date) is not"
        " null LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id LEFT JOIN"
        " analytics.discount_detail discount_detail ON discounts.discount_id = discount_detail.discount_id"
        " and orders.id = discount_detail.order_id GROUP BY discount_detail.promo_name ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_query_invalid_field_in_topic(connection):
    with pytest.raises(QueryError) as exc_info:
        connection.get_sql_query(
            metrics=["total_item_revenue"],
            dimensions=["rainfall"],
            topic="Order lines Topic",
        )

    error_message = "The following views are not included in the topic Order lines Topic: country_detail"
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


@pytest.mark.query
def test_query_with_date_mapping_in_filter_in_topic(connection):
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["order_lines.order_month", "monthly_aggregates.division"],
        where=[
            {"field": "date", "expression": "greater_than", "value": "2024-10-01"},
            {"field": "date", "expression": "less_than", "value": "2024-10-31"},
        ],
        topic="Order lines unfiltered",
    )

    correct = (
        "SELECT DATE_TRUNC('MONTH', order_lines.order_date) as order_lines_order_month,"
        "monthly_aggregates.division as monthly_aggregates_division,SUM(order_lines.revenue) "
        "as order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.monthly_rollup monthly_aggregates ON DATE_TRUNC('MONTH', "
        "monthly_aggregates.record_date) = order_lines.order_unique_id "
        "WHERE DATE_TRUNC('DAY', order_lines.order_date)>'2024-10-01' AND DATE_TRUNC('DAY', "
        "order_lines.order_date)<'2024-10-31' GROUP BY DATE_TRUNC('MONTH', order_lines.order_date),"
        "monthly_aggregates.division ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_symmetric_aggregate_state_many_to_one_join_unfiltered_topic(connection):
    """
    Test state 1: Many_to_many joins should trigger symmetric aggregates.
    Uses existing "Order lines unfiltered" which has many_to_many and
    many_to_one joins.
    """
    # Generate SQL using existing topic with multiple join relationships
    query = connection.get_sql_query(
        metrics=["order_lines.total_item_revenue", "orders.average_order_value"],
        dimensions=["channel"],
        topic="Order lines unfiltered",
    )

    # This will fail and show the actual SQL generated
    assert (
        query == "SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue,(COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.revenue,"
        " 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id),"
        " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT"
        " (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS"
        " DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) / NULLIF(COUNT(DISTINCT CASE"
        " WHEN  (orders.revenue)  IS NOT NULL THEN  orders.id  ELSE NULL END), 0)) as"
        " orders_average_order_value FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.orders orders ON order_lines.order_unique_id=orders.id GROUP BY"
        " order_lines.sales_channel ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )


@pytest.mark.query
def test_symmetric_aggregate_state_one_to_one_join_unfiltered_topic(connection):
    """
    Test using existing "Order lines unfiltered" topic.
    This topic uses accounts as base_view with one_to_one joins.
    """
    # Generate SQL using existing order lines unfiltered topic
    query = connection.get_sql_query(
        metrics=["order_lines.total_item_revenue", "accounts.n_created_accounts"],
        dimensions=["accounts.account_name"],
        topic="Order lines unfiltered",
    )

    # This will fail and show the actual SQL generated
    assert (
        query == "SELECT accounts.name as accounts_account_name,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue,COUNT(accounts.account_id) as accounts_n_created_accounts FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.accounts accounts ON"
        " accounts.account_id = order_lines.customer_id GROUP BY accounts.name ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST;"
    )


@pytest.mark.query
def test_symmetric_aggregate_state_many_to_many_join_unfiltered_topic(connection):
    """
    Test symmetric aggregates with discount metrics in existing topic.
    Uses "Order lines unfiltered" to test metrics from fanned out views.
    This test will fail to show the actual generated SQL.
    """
    # Generate SQL with discount metric (should use symmetric aggregates)
    query = connection.get_sql_query(
        metrics=["order_lines.total_item_revenue", "discounts.total_discount_amt"],
        dimensions=["discount_code"],
        topic="Order lines unfiltered",
    )

    # This will fail and show the actual SQL generated
    assert (
        query == "SELECT discounts.code as discounts_discount_code,COALESCE(CAST((SUM(DISTINCT"
        " (CAST(FLOOR(COALESCE(order_lines.revenue, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) +"
        " (TO_NUMBER(MD5(order_lines.order_line_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
        " 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(order_lines.order_line_id),"
        " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) /"
        " CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as"
        " order_lines_total_item_revenue,COALESCE(CAST((SUM(DISTINCT"
        " (CAST(FLOOR(COALESCE(discounts.discount_amt, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) +"
        " (TO_NUMBER(MD5(discounts.discount_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
        " 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(discounts.discount_id),"
        " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) /"
        " CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as discounts_total_discount_amt FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics_live.discounts discounts ON"
        " order_lines.order_unique_id = discounts.order_id and DATE_TRUNC('DAY', discounts.order_date) is"
        " not null GROUP BY discounts.code ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )


@pytest.mark.query
def test_symmetric_aggregate_state_one_to_many_join_unfiltered_topic(connection):
    """
    Test with metrics from different views in existing topic.
    Uses "Order lines unfiltered" to test how different metrics behave.
    This test will fail to show the actual generated SQL.
    """
    # Generate SQL with metrics from both order_lines and discounts views
    query = connection.get_sql_query(
        metrics=["order_lines.total_item_revenue", "country_detail.avg_rainfall"],
        dimensions=["channel"],
        topic="Order lines unfiltered",
    )

    # This will fail and show the actual SQL generated
    assert (
        query == "SELECT order_lines.sales_channel as order_lines_channel,COALESCE(CAST((SUM(DISTINCT"
        " (CAST(FLOOR(COALESCE(order_lines.revenue, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) +"
        " (TO_NUMBER(MD5(order_lines.order_line_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
        " 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(order_lines.order_line_id),"
        " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) /"
        " CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as"
        " order_lines_total_item_revenue,AVG(country_detail.rain) as country_detail_avg_rainfall FROM"
        " analytics.order_line_items order_lines LEFT JOIN (SELECT * FROM ANALYTICS.COUNTRY_DETAIL WHERE"
        " '{{ user_attributes['owned_region'] }}' = COUNTRY_DETAIL.REGION) as country_detail ON"
        " country_detail.country = order_lines.sales_channel GROUP BY order_lines.sales_channel ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST;"
    )


@pytest.mark.query
def test_symmetric_aggregate_state_hop_join_in_topic(connection):
    """
    Test with hop join (discount_detail via discounts) in existing topic.
    Uses "Order lines unfiltered" to test complex join paths.
    This test will fail to show the actual generated SQL.
    """
    # Generate SQL with metric requiring hop through discounts to discount_detail
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
        " analytics.order_line_items order_lines LEFT JOIN analytics_live.discounts discounts ON"
        " order_lines.order_unique_id = discounts.order_id and DATE_TRUNC('DAY', discounts.order_date) is not"
        " null LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id LEFT JOIN"
        " analytics.discount_detail discount_detail ON discounts.discount_id = discount_detail.discount_id"
        " and orders.id = discount_detail.order_id GROUP BY discount_detail.promo_name ORDER BY"
        " order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_topic_default_date_join_in_topic(connection):
    query = connection.get_sql_query(
        metrics=[
            "monthly_aggregates.count_new_employees",
            "discount_detail.discount_usd",
            "accounts.n_created_accounts",
        ],
        dimensions=["accounts.created_month", "accounts.account_name"],
        where=[
            {"field": "date", "expression": "greater_than", "value": "2024-10-01"},
            {"field": "date", "expression": "less_than", "value": "2024-10-31"},
        ],
        topic="Order lines unfiltered",
    )

    correct = (
        "WITH monthly_aggregates_record__cte_subquery_2 AS (SELECT DATE_TRUNC('MONTH',"
        " monthly_aggregates.record_date) as monthly_aggregates_record_month,accounts.name as"
        " accounts_account_name,monthly_aggregates.n_new_employees as monthly_aggregates_count_new_employees"
        " FROM analytics.order_line_items order_lines LEFT JOIN analytics.accounts accounts ON"
        " accounts.account_id = order_lines.customer_id LEFT JOIN analytics.monthly_rollup monthly_aggregates"
        " ON DATE_TRUNC('MONTH', monthly_aggregates.record_date) = order_lines.order_unique_id WHERE"
        " DATE_TRUNC('DAY', monthly_aggregates.record_date)>'2024-10-01' AND DATE_TRUNC('DAY',"
        " monthly_aggregates.record_date)<'2024-10-31') ,discounts_order__cte_subquery_1 AS (SELECT"
        " DATE_TRUNC('MONTH', discounts.order_date) as discounts_order_month,accounts.name as"
        " accounts_account_name,COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(discount_detail.total_usd,"
        " 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(discount_detail.discount_id),"
        " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT"
        " (TO_NUMBER(MD5(discount_detail.discount_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
        " 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as"
        " discount_detail_discount_usd FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.accounts accounts ON accounts.account_id = order_lines.customer_id LEFT JOIN"
        " analytics_live.discounts discounts ON order_lines.order_unique_id = discounts.order_id and"
        " DATE_TRUNC('DAY', discounts.order_date) is not null LEFT JOIN analytics.orders orders ON"
        " order_lines.order_unique_id=orders.id LEFT JOIN analytics.discount_detail discount_detail ON"
        " discounts.discount_id = discount_detail.discount_id and orders.id = discount_detail.order_id WHERE"
        " DATE_TRUNC('DAY', discounts.order_date)>'2024-10-01' AND DATE_TRUNC('DAY',"
        " discounts.order_date)<'2024-10-31' GROUP BY DATE_TRUNC('MONTH', discounts.order_date),accounts.name"
        " ORDER BY discount_detail_discount_usd DESC NULLS LAST) ,accounts_created__cte_subquery_0 AS (SELECT"
        " DATE_TRUNC('MONTH', accounts.created_at) as accounts_created_month,accounts.name as"
        " accounts_account_name,COUNT(accounts.account_id) as accounts_n_created_accounts FROM"
        " analytics.order_line_items order_lines LEFT JOIN analytics.accounts accounts ON accounts.account_id"
        " = order_lines.customer_id WHERE DATE_TRUNC('DAY', accounts.created_at)>'2024-10-01' AND"
        " DATE_TRUNC('DAY', accounts.created_at)<'2024-10-31' GROUP BY DATE_TRUNC('MONTH',"
        " accounts.created_at),accounts.name ORDER BY accounts_n_created_accounts DESC NULLS LAST) SELECT"
        " accounts_created__cte_subquery_0.accounts_n_created_accounts as"
        " accounts_n_created_accounts,discounts_order__cte_subquery_1.discount_detail_discount_usd as"
        " discount_detail_discount_usd,monthly_aggregates_record__cte_subquery_2.monthly_aggregates_count_new_employees"  # noqa
        " as monthly_aggregates_count_new_employees,ifnull(accounts_created__cte_subquery_0.accounts_created_month,"  # noqa
        " ifnull(discounts_order__cte_subquery_1.discounts_order_month,"
        " monthly_aggregates_record__cte_subquery_2.monthly_aggregates_record_month)) as"
        " accounts_created_month,ifnull(ifnull(accounts_created__cte_subquery_0.accounts_account_name,"
        " discounts_order__cte_subquery_1.accounts_account_name),"
        " monthly_aggregates_record__cte_subquery_2.accounts_account_name) as"
        " accounts_account_name,ifnull(discounts_order__cte_subquery_1.discounts_order_month,"
        " ifnull(accounts_created__cte_subquery_0.accounts_created_month,"
        " monthly_aggregates_record__cte_subquery_2.monthly_aggregates_record_month)) as"
        " discounts_order_month,ifnull(monthly_aggregates_record__cte_subquery_2.monthly_aggregates_record_month,"  # noqa
        " ifnull(accounts_created__cte_subquery_0.accounts_created_month,"
        " discounts_order__cte_subquery_1.discounts_order_month)) as monthly_aggregates_record_month FROM"
        " accounts_created__cte_subquery_0 FULL OUTER JOIN discounts_order__cte_subquery_1 ON"
        " accounts_created__cte_subquery_0.accounts_created_month=discounts_order__cte_subquery_1.discounts_order_month"  # noqa
        " and accounts_created__cte_subquery_0.accounts_account_name=discounts_order__cte_subquery_1.accounts_account_name"  # noqa
        " FULL OUTER JOIN monthly_aggregates_record__cte_subquery_2 ON"
        " accounts_created__cte_subquery_0.accounts_created_month=monthly_aggregates_record__cte_subquery_2.monthly_aggregates_record_month"  # noqa
        " and accounts_created__cte_subquery_0.accounts_account_name=monthly_aggregates_record__cte_subquery_2.accounts_account_name;"  # noqa
    )
    assert query == correct


@pytest.mark.query
def test_topic_default_date_join_in_topic_dim_no_presence_in_measures(connection):
    query = connection.get_sql_query(
        metrics=[
            "orders.total_on_hand_items",
            "discounts.total_discount_amt",
        ],
        dimensions=["monthly_aggregates.division", "discounts.order_month"],
        where=[
            {"field": "monthly_aggregates.division", "expression": "equal_to", "value": "Grainger"},
            {"field": "date", "expression": "greater_than", "value": "2024-10-01"},
            {"field": "date", "expression": "less_than", "value": "2024-10-31"},
        ],
        topic="orders_chained_topic",
    )

    correct = (
        "WITH order_lines_order__cte_subquery_2 AS (SELECT monthly_aggregates.division as"
        " monthly_aggregates_division,DATE_TRUNC('MONTH', order_lines.order_date) as"
        " order_lines_order_month,COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(orders.inventory_qty, 0) *"
        " (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(orders.id),"
        " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT"
        " (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS"
        " DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as orders_total_on_hand_items FROM"
        " analytics.orders orders JOIN analytics.discount_detail discount_detail ON"
        " orders.id=discount_detail.order_id JOIN analytics.customers customers ON"
        " customers.customer_id=discount_detail.order_id LEFT JOIN analytics.monthly_rollup"
        " monthly_aggregates ON customers.customer_id=monthly_aggregates.division JOIN"
        " analytics.order_line_items order_lines ON customers.customer_id=order_lines.customer_id WHERE"
        " monthly_aggregates.division='Grainger' AND DATE_TRUNC('DAY', order_lines.order_date)>'2024-10-01'"
        " AND DATE_TRUNC('DAY', order_lines.order_date)<'2024-10-31' GROUP BY"
        " monthly_aggregates.division,DATE_TRUNC('MONTH', order_lines.order_date) ORDER BY"
        " orders_total_on_hand_items DESC NULLS LAST) ,discounts_order__cte_subquery_0 AS (SELECT"
        " monthly_aggregates.division as monthly_aggregates_division,DATE_TRUNC('MONTH',"
        " discounts.order_date) as discounts_order_month,COALESCE(CAST((SUM(DISTINCT"
        " (CAST(FLOOR(COALESCE(discounts.discount_amt, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) +"
        " (TO_NUMBER(MD5(discounts.discount_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38,"
        " 0)) - SUM(DISTINCT (TO_NUMBER(MD5(discounts.discount_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
        " 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as"
        " discounts_total_discount_amt FROM analytics.orders orders LEFT JOIN analytics.accounts accounts ON"
        " orders.account_id=accounts.account_id JOIN analytics.discount_detail discount_detail ON"
        " orders.id=discount_detail.order_id JOIN analytics.customers customers ON"
        " customers.customer_id=discount_detail.order_id LEFT JOIN analytics_live.discounts discounts ON"
        " discount_detail.order_id=discounts.order_id and DATE_TRUNC('MONTH',"
        " accounts.created_at)=DATE_TRUNC('MONTH', discounts.order_date) LEFT JOIN analytics.monthly_rollup"
        " monthly_aggregates ON customers.customer_id=monthly_aggregates.division WHERE"
        " monthly_aggregates.division='Grainger' AND DATE_TRUNC('DAY', discounts.order_date)>'2024-10-01' AND"
        " DATE_TRUNC('DAY', discounts.order_date)<'2024-10-31' GROUP BY"
        " monthly_aggregates.division,DATE_TRUNC('MONTH', discounts.order_date) ORDER BY"
        " discounts_total_discount_amt DESC NULLS LAST) ,monthly_aggregates_record__cte_subquery_1 AS (SELECT"
        " monthly_aggregates.division as monthly_aggregates_division,DATE_TRUNC('MONTH',"
        " monthly_aggregates.record_date) as monthly_aggregates_record_month FROM analytics.orders orders"
        " JOIN analytics.discount_detail discount_detail ON orders.id=discount_detail.order_id JOIN"
        " analytics.customers customers ON customers.customer_id=discount_detail.order_id LEFT JOIN"
        " analytics.monthly_rollup monthly_aggregates ON customers.customer_id=monthly_aggregates.division"
        " WHERE monthly_aggregates.division='Grainger' AND DATE_TRUNC('DAY',"
        " monthly_aggregates.record_date)>'2024-10-01' AND DATE_TRUNC('DAY',"
        " monthly_aggregates.record_date)<'2024-10-31' GROUP BY"
        " monthly_aggregates.division,DATE_TRUNC('MONTH', monthly_aggregates.record_date) ORDER BY"
        " monthly_aggregates_division ASC NULLS LAST) SELECT"
        " discounts_order__cte_subquery_0.discounts_total_discount_amt as"
        " discounts_total_discount_amt,order_lines_order__cte_subquery_2.orders_total_on_hand_items as"
        " orders_total_on_hand_items,ifnull(ifnull(discounts_order__cte_subquery_0.monthly_aggregates_division,"
        " monthly_aggregates_record__cte_subquery_1.monthly_aggregates_division),"
        " order_lines_order__cte_subquery_2.monthly_aggregates_division) as"
        " monthly_aggregates_division,ifnull(discounts_order__cte_subquery_0.discounts_order_month,"
        " ifnull(monthly_aggregates_record__cte_subquery_1.monthly_aggregates_record_month,"
        " order_lines_order__cte_subquery_2.order_lines_order_month)) as"
        " discounts_order_month,ifnull(monthly_aggregates_record__cte_subquery_1.monthly_aggregates_record_month,"
        " ifnull(discounts_order__cte_subquery_0.discounts_order_month,"
        " order_lines_order__cte_subquery_2.order_lines_order_month)) as"
        " monthly_aggregates_record_month,ifnull(order_lines_order__cte_subquery_2.order_lines_order_month,"
        " ifnull(discounts_order__cte_subquery_0.discounts_order_month,"
        " monthly_aggregates_record__cte_subquery_1.monthly_aggregates_record_month)) as"
        " order_lines_order_month FROM discounts_order__cte_subquery_0 FULL OUTER JOIN"
        " monthly_aggregates_record__cte_subquery_1 ON"
        " discounts_order__cte_subquery_0.monthly_aggregates_division="
        "monthly_aggregates_record__cte_subquery_1.monthly_aggregates_division"
        " and discounts_order__cte_subquery_0.discounts_order_month=monthly_aggregates_record__cte_subquery_1.monthly_aggregates_record_month"
        " FULL OUTER JOIN order_lines_order__cte_subquery_2 ON"
        " discounts_order__cte_subquery_0.monthly_aggregates_division=order_lines_order__cte_subquery_2.monthly_aggregates_division"
        " and discounts_order__cte_subquery_0.discounts_order_month=order_lines_order__cte_subquery_2.order_lines_order_month;"
    )
    assert query == correct


@pytest.mark.query
def test_topic_default_date_join_in_topic_dim_no_presence_in_measures_2(connection):
    query = connection.get_sql_query(
        metrics=[
            "order_lines.total_item_revenue",
            "discounts.total_discount_amt",
        ],
        dimensions=["monthly_aggregates.division", "orders.sub_channel"],
        where=[
            {"field": "monthly_aggregates.division", "expression": "equal_to", "value": "Grainger"},
            {"field": "monthly_aggregates.division", "expression": "isin", "value": ["Grainger", "Tomato"]},
            {"field": "date", "expression": "greater_than", "value": "2024-10-01"},
            {"field": "date", "expression": "less_than", "value": "2024-10-31"},
        ],
        order_by=[{"field": "discounts.total_discount_amt", "sort": "asc"}],
        topic="orders_chained_topic",
    )

    correct = (
        "WITH order_lines_order__cte_subquery_1 AS (SELECT monthly_aggregates.division as"
        " monthly_aggregates_division,orders.sub_channel as orders_sub_channel,COALESCE(CAST((SUM(DISTINCT"
        " (CAST(FLOOR(COALESCE(order_lines.revenue, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) +"
        " (TO_NUMBER(MD5(order_lines.order_line_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') %"
        " 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT (TO_NUMBER(MD5(order_lines.order_line_id),"
        " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) /"
        " CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as order_lines_total_item_revenue FROM analytics.orders"
        " orders JOIN analytics.discount_detail discount_detail ON orders.id=discount_detail.order_id JOIN"
        " analytics.customers customers ON customers.customer_id=discount_detail.order_id LEFT JOIN"
        " analytics.monthly_rollup monthly_aggregates ON customers.customer_id=monthly_aggregates.division"
        " JOIN analytics.order_line_items order_lines ON customers.customer_id=order_lines.customer_id WHERE"
        " monthly_aggregates.division='Grainger' AND monthly_aggregates.division IN ('Grainger','Tomato') AND"
        " DATE_TRUNC('DAY', order_lines.order_date)>'2024-10-01' AND DATE_TRUNC('DAY',"
        " order_lines.order_date)<'2024-10-31' GROUP BY monthly_aggregates.division,orders.sub_channel ORDER"
        " BY order_lines_total_item_revenue DESC NULLS LAST) ,discounts_order__cte_subquery_0 AS (SELECT"
        " monthly_aggregates.division as monthly_aggregates_division,orders.sub_channel as"
        " orders_sub_channel,COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(discounts.discount_amt, 0) *"
        " (1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(discounts.discount_id),"
        " 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT"
        " (TO_NUMBER(MD5(discounts.discount_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38,"
        " 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) as"
        " discounts_total_discount_amt FROM analytics.orders orders LEFT JOIN analytics.accounts accounts ON"
        " orders.account_id=accounts.account_id JOIN analytics.discount_detail discount_detail ON"
        " orders.id=discount_detail.order_id JOIN analytics.customers customers ON"
        " customers.customer_id=discount_detail.order_id LEFT JOIN analytics_live.discounts discounts ON"
        " discount_detail.order_id=discounts.order_id and DATE_TRUNC('MONTH',"
        " accounts.created_at)=DATE_TRUNC('MONTH', discounts.order_date) LEFT JOIN analytics.monthly_rollup"
        " monthly_aggregates ON customers.customer_id=monthly_aggregates.division WHERE"
        " monthly_aggregates.division='Grainger' AND monthly_aggregates.division IN ('Grainger','Tomato') AND"
        " DATE_TRUNC('DAY', discounts.order_date)>'2024-10-01' AND DATE_TRUNC('DAY',"
        " discounts.order_date)<'2024-10-31' GROUP BY monthly_aggregates.division,orders.sub_channel ORDER BY"
        " discounts_total_discount_amt DESC NULLS LAST) SELECT"
        " discounts_order__cte_subquery_0.discounts_total_discount_amt as"
        " discounts_total_discount_amt,order_lines_order__cte_subquery_1.order_lines_total_item_revenue as"
        " order_lines_total_item_revenue,ifnull(discounts_order__cte_subquery_0.monthly_aggregates_division,"
        " order_lines_order__cte_subquery_1.monthly_aggregates_division) as"
        " monthly_aggregates_division,ifnull(discounts_order__cte_subquery_0.orders_sub_channel,"
        " order_lines_order__cte_subquery_1.orders_sub_channel) as orders_sub_channel FROM"
        " discounts_order__cte_subquery_0 FULL OUTER JOIN order_lines_order__cte_subquery_1 ON"
        " discounts_order__cte_subquery_0.monthly_aggregates_division=order_lines_order__cte_subquery_1.monthly_aggregates_division"
        " and discounts_order__cte_subquery_0.orders_sub_channel=order_lines_order__cte_subquery_1.orders_sub_channel"
        " ORDER BY discounts_total_discount_amt ASC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_topic_default_date_in_topic_join_only_dim_base_view(connection):
    query = connection.get_sql_query(
        metrics=["monthly_aggregates.count_new_employees_per_revenue"],
        dimensions=["order_lines.customer_id"],
        where=[
            {"field": "date", "expression": "greater_than", "value": "2024-10-01"},
            {"field": "date", "expression": "less_than", "value": "2024-10-31"},
        ],
        topic="Order lines unfiltered",
    )

    correct = (
        "SELECT order_lines.customer_id as order_lines_customer_id,(NULLIF(COUNT("
        "DISTINCT CASE WHEN  (monthly_aggregates.n_new_employees)  IS NOT NULL THEN"
        "  monthly_aggregates.record_date  ELSE NULL END), 0)) / (COALESCE(CAST("
        "(SUM(DISTINCT (CAST(FLOOR(COALESCE(order_lines.revenue, 0) * (1000000 * "
        "1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(order_lines.order_line_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) - SUM("
        "DISTINCT (TO_NUMBER(MD5(order_lines.order_line_id), 'XXXXXXXXXXXXXXXXX"
        "XXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / "
        "CAST((1000000*1.0) AS DOUBLE PRECISION), 0)) as monthly_aggregates_"
        "count_new_employees_per_revenue FROM analytics.order_line_items order_lines"
        " LEFT JOIN analytics.monthly_rollup monthly_aggregates ON DATE_TRUNC('"
        "MONTH', monthly_aggregates.record_date) = order_lines.order_unique_id "
        "WHERE DATE_TRUNC('DAY', monthly_aggregates.record_date)>'2024-10-01' "
        "AND DATE_TRUNC('DAY', monthly_aggregates.record_date)<'2024-10-31' "
        "GROUP BY order_lines.customer_id ORDER BY monthly_aggregates_count_new"
        "_employees_per_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_always_filter_literal_no_filter(connection):
    """Test always_filter_literal with topic that has no always_filter."""
    topic = connection.project.get_topic("Order lines unfiltered")
    assert topic.always_filter_literal() is None


@pytest.mark.query
def test_always_filter_literal_with_valid_filter(connection):
    """Test always_filter_literal with topic that has valid always_filter."""
    topic = connection.project.get_topic("Order lines Topic")
    result = topic.always_filter_literal()
    # Should return a SQL string for the -NULL filter
    assert result is not None
    assert isinstance(result, str)
    correct = "NOT orders.revenue_dimension IS NULL"
    assert result == correct


@pytest.mark.query
def test_always_filter_literal_invalid_field_format(fresh_project, connections):
    """Test always_filter_literal raises error for field without view name."""
    # Create a topic with invalid always_filter (missing view name prefix)
    fresh_project._topics[0]["always_filter"] = [{"field": "invalid_field", "value": "test"}]
    conn = MetricsLayerConnection(project=fresh_project, connections=connections)

    topic = conn.project.get_topic("Order lines Topic")
    with pytest.raises(QueryError) as exc_info:
        topic.always_filter_literal()

    assert "needs to contain the view name like view_name.field_name" in str(exc_info.value)


@pytest.mark.query
def test_always_filter_literal_greater_than_filter(fresh_project, connections):
    """Test always_filter_literal with greater_than filter."""
    fresh_project._topics[0]["always_filter"] = [
        {"field": "order_lines.channel", "value": "Email"},
        {"field": "orders.total_revenue", "value": ">100"},
        {"field": "order_lines.order_id", "value": "-1234567890, -45234553"},
    ]
    conn = MetricsLayerConnection(project=fresh_project, connections=connections)

    topic = conn.project.get_topic("Order lines Topic")
    result = topic.always_filter_literal()

    assert result is not None
    assert isinstance(result, str)
    correct = (
        "order_lines.channel='Email' and orders.total_revenue>100 and "
        "order_lines.order_id NOT IN ('1234567890','45234553')"
    )
    assert result == correct


@pytest.mark.query
def test_topic_from_syntax_complex(connection):
    """Test complex view-level from syntax with multiple aliases"""
    topic = connection.project.get_topic("From Syntax Complex Test Topic")
    assert topic.base_view == "order_lines"
    assert topic.label == "From Syntax Complex Test Topic"

    # Check that both aliases are configured properly
    assert topic.views is not None
    assert "customers" in topic.views
    assert "customer_accounts" in topic.views

    # Check customers configuration
    customers_config = topic.views["customers"]
    assert customers_config["from"] == "customers"
    assert customers_config["label"] == "Customer Info"
    assert customers_config["field_prefix"] == "Customer"
    assert customers_config.get("include_metrics", True) is False

    # Check customer_accounts configuration
    accounts_config = topic.views["customer_accounts"]
    assert accounts_config["from"] == "customers"
    assert accounts_config["label"] == "Account Holder"
    assert accounts_config["field_prefix"] == "Account"
    assert accounts_config.get("include_metrics", True) is True

    # Check that virtual views are created
    topic_views = topic._views()
    view_names = [v.name for v in topic_views]
    assert "customers" in view_names
    assert "customer_accounts" in view_names
    assert "order_lines" in view_names
    assert "orders" in view_names


@pytest.mark.query
def test_topic_from_syntax_query_generation(connection):
    """Test that queries work correctly with view-level from syntax"""
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["customers.region"],
        topic="From Syntax Complex Test Topic",
    )

    # Should include a join to the customers table
    correct = (
        "SELECT customers.region as customers_region,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.customers customers ON order_lines.customer_id = customers.customer_id GROUP BY"
        " customers.region ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct


@pytest.mark.query
def test_topic_from_syntax_query_generation_reference_both_joins(connection):
    """Test that queries work correctly with view-level from syntax"""
    query = connection.get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["customers.gender", "customer_accounts.gender"],
        topic="From Syntax Complex Test Topic",
    )

    # Should include a join to the customers table
    correct = (
        "SELECT customers.gender as customers_gender,customer_accounts.gender as"
        " customer_accounts_gender,SUM(order_lines.revenue) as"
        " order_lines_total_item_revenue FROM analytics.order_line_items order_lines LEFT JOIN"
        " analytics.customers customer_accounts "
        "ON order_lines.customer_id = customer_accounts.last_product_purchased"
        " LEFT JOIN analytics.customers customers ON order_lines.customer_id = customers.customer_id GROUP BY"
        " customers.gender,customer_accounts.gender ORDER BY order_lines_total_item_revenue DESC NULLS LAST;"
    )
    assert query == correct
