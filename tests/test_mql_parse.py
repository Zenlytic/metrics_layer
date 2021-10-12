import pytest

from granite.core.query import get_sql_query
from granite.core.sql.query_errors import ParseError


def test_query_no_join_mql(config):

    query = get_sql_query(sql="SELECT * FROM MQL(total_item_revenue BY channel)", config=config)

    correct = "SELECT * FROM (SELECT order_lines.sales_channel as channel,SUM(order_lines.revenue) "
    correct += "as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines GROUP BY order_lines.sales_channel);"
    assert query == correct

    # Test lowercase
    query = get_sql_query(sql="SELECT * FROM MQL(total_item_revenue by channel)", config=config)
    assert query == correct

    # Test mixed case
    query = get_sql_query(sql="SELECT * FROM MQL(total_item_revenue By channel)", config=config)
    assert query == correct


def test_query_no_join_mql_syntax_error(config):

    with pytest.raises(ParseError) as exc_info:
        get_sql_query(sql="SELECT * FROM MQL(total_item_revenue by channel", config=config)

    assert exc_info.value


def test_query_single_join_mql(config):

    query = get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue BY channel, new_vs_repeat) as rev_group", config=config
    )

    correct = "SELECT * FROM (SELECT order_lines.sales_channel as channel,orders.new_vs_repeat as "
    correct += "new_vs_repeat,SUM(order_lines.revenue) as total_item_revenue FROM analytics.order_line_items "
    correct += "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
    correct += "GROUP BY order_lines.sales_channel,orders.new_vs_repeat) as rev_group;"
    assert query == correct


def test_query_multiple_join_mql(config):

    query = get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue BY region, new_vs_repeat) as rev_group", config=config
    )

    correct = (
        "SELECT * FROM (SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
        "SUM(order_lines.revenue) as total_item_revenue FROM "
        "analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "GROUP BY customers.region,orders.new_vs_repeat) as rev_group;"
    )
    assert query == correct


def test_query_multiple_join_all_mql(config):

    query = get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue BY region, new_vs_repeat WHERE region != 'West' AND new_vs_repeat <> 'New' HAVING total_item_revenue > -12 AND total_item_revenue < 122 ORDER BY total_item_revenue DESC, new_vs_repeat) as rev_group",  # noqa
        config=config,
    )

    correct = (
        "SELECT * FROM (SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
        "SUM(order_lines.revenue) as total_item_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE customers.region != 'West' AND orders.new_vs_repeat <>"
        " 'New' GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue) > -12 AND "
        "SUM(order_lines.revenue) < 122 ORDER BY total_item_revenue DESC,new_vs_repeat ASC) as rev_group;"
    )
    assert query == correct


def test_query_mql_as_subset(config):

    mql = (
        "SELECT channelinfo.channel, channelinfo.channel_owner, rev_group.total_item_revenue FROM "
        "MQL(total_item_revenue BY channel, new_vs_repeat) as rev_group LEFT JOIN analytics.channeldata "
        "channelinfo on rev_group.channel = channelinfo.channel;"
    )
    query = get_sql_query(sql=mql, config=config)

    correct = (
        "SELECT channelinfo.channel, channelinfo.channel_owner, rev_group.total_item_revenue FROM "
        "(SELECT order_lines.sales_channel as channel,orders.new_vs_repeat as "
        "new_vs_repeat,SUM(order_lines.revenue) as total_item_revenue FROM analytics.order_line_items "
        "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
        "GROUP BY order_lines.sales_channel,orders.new_vs_repeat) as rev_group LEFT JOIN "
        "analytics.channeldata channelinfo on rev_group.channel = channelinfo.channel;"
    )
    assert query == correct


@pytest.mark.skip("TODO add list dimensions support")
def test_query_mql_list_dimensions(config):

    query = get_sql_query(sql="SELECT * FROM MQL(LIST_DIMENSIONS)", config=config)

    correct = "SELECT * FROM (SELECT ... TODO"
    assert query == correct


@pytest.mark.skip("TODO add list metrics support")
def test_query_mql_list_metrics(config):

    query = get_sql_query(sql="SELECT * FROM MQL(LIST_METRICS)", config=config)

    correct = "SELECT * FROM (SELECT ... TODO"
    assert query == correct


@pytest.mark.skip("TODO add define metric support")
def test_query_mql_define(config):

    query = get_sql_query(sql="SELECT * FROM MQL(DEFINE total_item_revenue)", config=config)

    correct = "SELECT * FROM (SELECT ... TODO"
    assert query == correct


def test_query_mql_pass_through_query(config):

    correct = "SELECT channelinfo.channel, channelinfo.channel_owner FROM analytics.channeldata channelinfo;"
    query = get_sql_query(sql=correct, config=config)
    assert query == correct
