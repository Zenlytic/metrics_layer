# import pytest
import os

from granite.core.model.project import Project
from granite.core.parse.project_reader import ProjectReader
from granite.core.query import get_sql_query

BASE_PATH = os.path.dirname(__file__)


model_path = os.path.join(BASE_PATH, "config/granite_config/models/commerce_test_model.yml")
order_lines_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_order_lines.yml")
orders_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_orders.yml")
customers_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_customers.yml")
discounts_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_discounts.yml")
view_paths = [order_lines_view_path, orders_view_path, customers_view_path, discounts_view_path]

models = [ProjectReader.read_yaml_file(model_path)]
views = [ProjectReader.read_yaml_file(path) for path in view_paths]


class config_mock:
    pass


# TODO
def test_query_no_join_mql():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(sql="SELECT * FROM MQL(total_item_revenue BY channel)", config=config_mock)

    correct = "SELECT * FROM (SELECT order_lines.sales_channel as channel,SUM(order_lines.revenue) "
    correct += "as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines GROUP BY order_lines.sales_channel);"
    assert query == correct

    # Test lowercase
    query = get_sql_query(sql="SELECT * FROM MQL(total_item_revenue by channel)", config=config_mock)
    assert query == correct

    # Test mixed case
    query = get_sql_query(sql="SELECT * FROM MQL(total_item_revenue By channel)", config=config_mock)
    assert query == correct


def test_query_no_join_mql_syntax_error():
    project = Project(models=models, views=views)
    config_mock.project = project
    get_sql_query(sql="SELECT * FROM MQL(total_item_revenue by channel", config=config_mock)

    assert False


def test_query_single_join_mql():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue BY channel, new_vs_repeat) as rev_group", config=config_mock
    )

    correct = "SELECT * FROM (SELECT order_lines.sales_channel as channel,orders.new_vs_repeat as "
    correct += "new_vs_repeat,SUM(order_lines.revenue) as total_item_revenue FROM analytics.order_line_items "
    correct += "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id "
    correct += "GROUP BY order_lines.sales_channel,orders.new_vs_repeat) as rev_group;"
    assert query == correct


def test_query_multiple_join_mql():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue BY region, new_vs_repeat) as rev_group", config=config_mock
    )

    correct = "SELECT * FROM (SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM analytics.order_line_items "
    correct += "order_lines LEFT JOIN analytics.customers customers ON order_lines.customer_id"
    correct += "=customers.customer_id LEFT JOIN analytics.orders orders ON order_lines.order_id"
    correct += "=orders.order_id GROUP BY customers.region,orders.new_vs_repeat) as rev_group;"
    assert query == correct


def test_query_multiple_join_all_mql():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        where=[{"field": "region", "expression": "not_equal_to", "value": "West"}],
        having=[{"field": "total_item_revenue", "expression": "greater_than", "value": -12}],
        order_by=[{"field": "total_item_revenue", "sort": "desc"}],
        config=config_mock,
    )
    query = get_sql_query(
        sql="SELECT * FROM MQL(total_item_revenue BY region, new_vs_repeat WHERE region != 'West' HAVING total_item_revenue > -12 ORDER BY total_item_revenue DESC) as rev_group",  # noqa
        config=config_mock,
    )

    correct = "SELECT * FROM (SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id WHERE customers.region<>'West' "
    correct += "GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue)>-12 "
    correct += "ORDER BY total_item_revenue DESC) as rev_group;"
    assert query == correct


def test_query_mql_as_subset():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"], dimensions=["region", "new_vs_repeat"], config=config_mock
    )

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id GROUP BY customers.region,orders.new_vs_repeat;"
    assert query == correct


def test_query_mql_list_dimensions():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"], dimensions=["region", "new_vs_repeat"], config=config_mock
    )

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id GROUP BY customers.region,orders.new_vs_repeat;"
    assert query == correct


def test_query_mql_list_metrics():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"], dimensions=["region", "new_vs_repeat"], config=config_mock
    )

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id GROUP BY customers.region,orders.new_vs_repeat;"
    assert query == correct


def test_query_mql_define():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"], dimensions=["region", "new_vs_repeat"], config=config_mock
    )

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id GROUP BY customers.region,orders.new_vs_repeat;"
    assert query == correct


def test_query_mql_pass_through_query():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"], dimensions=["region", "new_vs_repeat"], config=config_mock
    )

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id GROUP BY customers.region,orders.new_vs_repeat;"
    assert query == correct
