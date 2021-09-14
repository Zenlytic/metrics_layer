# import pytest
import os

from granite.core.model.project import Project
from granite.core.parse.parse_granite_config import GraniteProjectReader
from granite.core.sql.resolve import SQLResolverByQuery

BASE_PATH = os.path.dirname(__file__)


model_path = os.path.join(BASE_PATH, "config/granite_config/models/commerce_test_model.yml")
order_lines_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_order_lines.yml")
orders_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_orders.yml")
customers_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_customers.yml")
discounts_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_discounts.yml")
view_paths = [order_lines_view_path, orders_view_path, customers_view_path, discounts_view_path]

models = [GraniteProjectReader.read_yaml_file(model_path)]
views = [GraniteProjectReader.read_yaml_file(path) for path in view_paths]


def test_query_no_join():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(metrics=["total_item_revenue"], dimensions=["channel"], project=project)
    query = resolver.get_query()

    correct = (
        "SELECT order_lines.sales_channel as channel,SUM(order_lines.revenue) as total_line_revenue FROM "
    )
    correct += "analytics.order_line_items order_lines GROUP BY order_lines.sales_channel;"
    assert query == correct


def test_simple_query_two_group_by():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"], dimensions=["channel", "new_vs_repeat"], project=project
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,simple.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel,simple.new_vs_repeat;"
    assert query == correct


def test_simple_query_two_metric():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(
        metrics=["total_revenue", "average_order_value"],
        dimensions=["channel", "new_vs_repeat"],
        project=project,
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,simple.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(simple.revenue) as total_revenue,AVG(simple.revenue) as average_order_value FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel,simple.new_vs_repeat;"
    assert query == correct


def test_simple_query_custom_dimension():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(metrics=["total_revenue"], dimensions=["is_valid_order"], project=project)
    query = resolver.get_query()

    correct = "SELECT CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END as is_valid_order,"
    correct += "SUM(simple.revenue) as total_revenue FROM analytics.orders simple"
    correct += " GROUP BY CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END;"
    assert query == correct


def test_simple_query_custom_metric():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(metrics=["revenue_per_aov"], dimensions=["channel"], project=project)
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,CASE WHEN AVG(simple.revenue) = 0 THEN 0 ELSE SUM(simple.revenue) / AVG(simple.revenue) END as revenue_per_aov FROM "  # noqa
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_where_dict():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field": "channel", "expression": "not_equal_to", "value": "Email"}],
        project=project,
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel<>'Email' GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_where_literal():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"], dimensions=["channel"], where="channel != 'Email'", project=project
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel != 'Email' GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_having_dict():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"],
        dimensions=["channel"],
        having=[{"field": "total_revenue", "expression": "greater_than", "value": 12}],
        project=project,
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel HAVING SUM(simple.revenue)>12;"
    assert query == correct


def test_simple_query_with_having_literal():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"], dimensions=["channel"], having="total_revenue > 12", project=project
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel HAVING SUM(simple.revenue) > 12;"
    assert query == correct


def test_simple_query_with_order_by_dict():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(
        metrics=["total_revenue", "average_order_value"],
        dimensions=["channel"],
        order_by=[{"field": "total_revenue", "sort": "desc"}, {"field": "average_order_value"}],
        project=project,
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue,AVG(simple.revenue) as average_order_value FROM "  # noqa
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY total_revenue DESC,average_order_value ASC;"  # noqa
    assert query == correct


def test_simple_query_with_order_by_literal():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"], dimensions=["channel"], order_by="total_revenue asc", project=project
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY total_revenue ASC;"
    assert query == correct


def test_simple_query_with_all():
    project = Project(models=models, views=views)
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field": "channel", "expression": "not_equal_to", "value": "Email"}],
        having=[{"field": "total_revenue", "expression": "greater_than", "value": 12}],
        order_by=[{"field": "total_revenue", "sort": "asc"}],
        project=project,
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel<>'Email' "
    correct += "GROUP BY simple.sales_channel HAVING SUM(simple.revenue)>12 ORDER BY total_revenue ASC;"
    assert query == correct
