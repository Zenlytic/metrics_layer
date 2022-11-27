import pytest

from metrics_layer.core.sql.query_errors import ParseError


@pytest.mark.query
def test_query_no_join_mql(connection):

    query = connection.get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue BY channel)",
    )

    correct = (
        "SELECT * FROM (SELECT order_lines.sales_channel as order_lines_channel,SUM(order_lines.revenue) "
        "as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines GROUP BY order_lines.sales_channel "
        "ORDER BY order_lines_total_item_revenue DESC);"
    )
    assert query == correct

    # Test lowercase
    query = connection.get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue by channel)",
    )
    assert query == correct

    # Test mixed case
    query = connection.get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue By channel)",
    )
    assert query == correct


@pytest.mark.query
def test_query_no_join_mql_syntax_error(connection):
    with pytest.raises(ParseError) as exc_info:
        connection.get_sql_query(
            sql="SELECT * FROM MQL(total_item_revenue by channel",
        )

    assert exc_info.value


@pytest.mark.query
def test_query_single_join_mql(connection):

    query = connection.get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue BY channel, new_vs_repeat) as rev_group",
    )

    correct = (
        "SELECT * FROM (SELECT order_lines.sales_channel as order_lines_channel,"
        "orders.new_vs_repeat as orders_new_vs_repeat,SUM(order_lines.revenue) as "
        "order_lines_total_item_revenue FROM analytics.order_line_items "
        "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY order_lines.sales_channel,orders.new_vs_repeat "
        "ORDER BY order_lines_total_item_revenue DESC) as rev_group;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_mql(connection):

    query = connection.get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue BY region, new_vs_repeat) as rev_group",
    )

    correct = (
        "SELECT * FROM (SELECT customers.region as customers_region,"
        "orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat "
        "ORDER BY order_lines_total_item_revenue DESC) as rev_group;"
    )
    assert query == correct


@pytest.mark.query
def test_query_multiple_join_all_mql(connection):

    query = connection.get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue BY region, new_vs_repeat WHERE region != 'West' AND new_vs_repeat <> 'New' HAVING total_item_revenue > -12 AND total_item_revenue < 122 ORDER BY total_item_revenue ASC, new_vs_repeat) as rev_group",  # noqa
    )

    correct = (
        "SELECT * FROM (SELECT customers.region as customers_region,"
        "orders.new_vs_repeat as orders_new_vs_repeat,"
        "SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE customers.region != 'West' AND orders.new_vs_repeat <>"
        " 'New' GROUP BY customers.region,orders.new_vs_repeat HAVING (SUM(order_lines.revenue)) > -12 AND "
        "(SUM(order_lines.revenue)) < 122 ORDER BY order_lines_total_item_revenue ASC,orders_new_vs_repeat"
        " ASC) as rev_group;"
    )
    assert query == correct


@pytest.mark.query
def test_query_mql_sequence(connection):
    query = connection.get_sql_query(
        sql="SELECT * FROM MQL(number_of_orders, total_item_revenue FOR orders FUNNEL channel = 'Paid' THEN channel = 'Organic' THEN channel = 'Paid' or region = 'West' WITHIN 3 days WHERE region != 'West') as sequence_group",  # noqa
    )

    revenue_calc = (
        "COALESCE(CAST((SUM(DISTINCT (CAST(FLOOR(COALESCE(order_lines_total_item_revenue, 0) * "
        "(1000000 * 1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(order_lines_order_line_id), "
        "'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) - SUM(DISTINCT "
        "(TO_NUMBER(MD5(order_lines_order_line_id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') "
        "% 1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS "
        "DOUBLE PRECISION), 0)"
    )

    correct = (
        "SELECT * FROM (WITH base AS (SELECT order_lines.sales_channel as order_lines_channel,"
        "customers.customer_id as customers_customer_id,order_lines.order_line_id as "
        "order_lines_order_line_id,orders.id as orders_order_id,orders.order_date as orders_order_raw,"
        "customers.region as customers_region,orders.id as orders_number_of_orders,order_lines.revenue "
        "as order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE customers.region != 'West') ,step_1 AS (SELECT *,orders_order_raw as step_1_time "
        "FROM base WHERE order_lines.sales_channel = 'Paid') ,step_2 AS ("
        "SELECT base.*,step_1.step_1_time as step_1_time FROM base JOIN step_1 "
        "ON base.customers_customer_id=step_1.customers_customer_id and step_1.orders_order_raw"
        "<base.orders_order_raw WHERE order_lines.sales_channel = 'Organic' AND "
        "DATEDIFF('DAY', step_1.step_1_time, base.orders_order_raw) <= 3) ,"
        "step_3 AS (SELECT base.*,step_2.step_1_time as step_1_time FROM base "
        "JOIN step_2 ON base.customers_customer_id=step_2.customers_customer_id and "
        "step_2.orders_order_raw<base.orders_order_raw WHERE order_lines.sales_channel = 'Paid' "
        "or customers.region = 'West' AND DATEDIFF('DAY', step_2.step_1_time, base.orders_order_raw) <= 3) ,"
        "result_cte AS ((SELECT 'Step 1' as step,1 as step_order,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders_number_of_orders)  IS NOT NULL THEN  orders_order_id  ELSE NULL END), 0) as "
        f"orders_number_of_orders,{revenue_calc} as order_lines_total_item_revenue "
        "FROM step_1) UNION ALL (SELECT 'Step 2' as step,2 as step_order,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders_number_of_orders)  IS NOT NULL THEN  orders_order_id  ELSE NULL END), 0) as "
        f"orders_number_of_orders,{revenue_calc} as order_lines_total_item_revenue "
        "FROM step_2) UNION ALL (SELECT 'Step 3' as step,3 as step_order,NULLIF(COUNT(DISTINCT CASE WHEN  "
        "(orders_number_of_orders)  IS NOT NULL THEN  orders_order_id  ELSE NULL END), 0) as "
        f"orders_number_of_orders,{revenue_calc} as order_lines_total_item_revenue "
        "FROM step_3)) SELECT * FROM result_cte) as sequence_group;"
    )
    assert query == correct


@pytest.mark.query
def test_query_mql_as_subset(connection):

    mql = (
        "SELECT channelinfo.channel, channelinfo.channel_owner, rev_group.total_item_revenue FROM "
        "MQL(total_item_revenue BY channel, new_vs_repeat) as rev_group LEFT JOIN analytics.channeldata "
        "channelinfo on rev_group.channel = channelinfo.channel;"
    )
    query = connection.get_sql_query(
        sql=mql,
    )

    correct = (
        "SELECT channelinfo.channel, channelinfo.channel_owner, rev_group.total_item_revenue FROM "
        "(SELECT order_lines.sales_channel as order_lines_channel,orders.new_vs_repeat as "
        "orders_new_vs_repeat,SUM(order_lines.revenue) as order_lines_total_item_revenue "
        "FROM analytics.order_line_items "
        "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY order_lines.sales_channel,orders.new_vs_repeat "
        "ORDER BY order_lines_total_item_revenue DESC) as rev_group LEFT JOIN "
        "analytics.channeldata channelinfo on rev_group.channel = channelinfo.channel;"
    )
    assert query == correct


@pytest.mark.query
def test_query_mql_pass_through_query(connection):
    correct = "SELECT channelinfo.channel, channelinfo.channel_owner FROM analytics.channeldata channelinfo;"
    query = connection.get_sql_query(sql=correct, connection_name="connection_name")
    assert query == correct


@pytest.mark.query
def test_query_mql_mapping_query(connection):
    correct = (
        "SELECT * FROM (SELECT orders.sub_channel as orders_sub_channel,SUM(order_lines.revenue) "
        "as order_lines_total_item_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "GROUP BY orders.sub_channel ORDER BY order_lines_total_item_revenue DESC);"
    )
    query = connection.get_sql_query(sql="SELECT * FROM MQL(total_item_revenue by source)")
    assert query == correct
