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
        "(SUM(order_lines.revenue)) < 122 ORDER BY total_item_revenue ASC,new_vs_repeat ASC) as rev_group;"
    )
    assert query == correct


@pytest.mark.query
def test_query_mql_sequence(connection):
    query = connection.get_sql_query(
        sql="SELECT * FROM MQL(number_of_orders, total_item_revenue FOR events FUNNEL channel = 'Paid' THEN channel = 'Organic' THEN channel = 'Paid' or region = 'West' WITHIN 3 days WHERE region != 'West') as sequence_group",  # noqa
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
        "(SUM(order_lines.revenue)) < 122 ORDER BY total_item_revenue ASC,new_vs_repeat ASC) as rev_group;"
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
