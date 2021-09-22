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


def test_query_no_join():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(metrics=["total_item_revenue"], dimensions=["channel"], config=config_mock)

    correct = (
        "SELECT order_lines.sales_channel as channel,SUM(order_lines.revenue) as total_item_revenue FROM "
    )
    correct += "analytics.order_line_items order_lines GROUP BY order_lines.sales_channel;"
    assert query == correct


def test_query_single_join():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"], dimensions=["channel", "new_vs_repeat"], config=config_mock
    )

    correct = "SELECT order_lines.sales_channel as channel,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
    correct += (
        "ON order_lines.order_id=orders.order_id GROUP BY order_lines.sales_channel,orders.new_vs_repeat;"
    )
    assert query == correct


def test_query_single_join_select_args():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["channel", "new_vs_repeat"],
        select_raw_sql=[
            "CAST(new_vs_repeat = 'Repeat' AS INT) as group_1",
            "CAST(date_created > '2021-04-02' AS INT) as period",
        ],
        config=config_mock,
    )

    correct = "SELECT order_lines.sales_channel as channel,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue,"
    correct += "CAST(new_vs_repeat = 'Repeat' AS INT) as group_1,"
    correct += "CAST(date_created > '2021-04-02' AS INT) as period FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
    correct += (
        "ON order_lines.order_id=orders.order_id GROUP BY order_lines.sales_channel,orders.new_vs_repeat;"
    )
    assert query == correct


def test_query_single_join_with_case_raw_sql():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"], dimensions=["is_on_sale_sql", "new_vs_repeat"], config=config_mock
    )

    correct = "SELECT CASE WHEN order_lines.product_name ilike '%sale%' then TRUE else FALSE end "
    correct += "as is_on_sale_sql,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id GROUP BY CASE WHEN order_lines.product_name "
    correct += "ilike '%sale%' then TRUE else FALSE end,orders.new_vs_repeat;"
    assert query == correct


def test_query_single_join_with_case():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"], dimensions=["is_on_sale_case", "new_vs_repeat"], config=config_mock
    )

    correct = "SELECT case when order_lines.product_name ilike '%sale%' then 'On sale' else 'Not on sale' end "  # noqa
    correct += "as is_on_sale_case,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id GROUP BY case when order_lines.product_name "
    correct += "ilike '%sale%' then 'On sale' else 'Not on sale' end,orders.new_vs_repeat;"
    assert query == correct


def test_query_single_join_with_tier():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"], dimensions=["order_tier", "new_vs_repeat"], config=config_mock
    )

    tier_case_query = "case when order_lines.revenue < 0 then 'Below 0' when order_lines.revenue >= 0 "
    tier_case_query += "and order_lines.revenue < 20 then '[0,20)' when order_lines.revenue >= 20 and "
    tier_case_query += "order_lines.revenue < 50 then '[20,50)' when order_lines.revenue >= 50 and "
    tier_case_query += "order_lines.revenue < 100 then '[50,100)' when order_lines.revenue >= 100 and "
    tier_case_query += "order_lines.revenue < 300 then '[100,300)' when order_lines.revenue >= 300 "
    tier_case_query += "then '[300,inf)' else 'Unknown' end"

    correct = f"SELECT {tier_case_query} as order_tier,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.orders orders "
    correct += f"ON order_lines.order_id=orders.order_id GROUP BY {tier_case_query},orders.new_vs_repeat;"
    assert query == correct


def test_query_single_join_with_filter():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["number_of_email_purchased_items"],
        dimensions=["channel", "new_vs_repeat"],
        config=config_mock,
    )

    correct = "SELECT order_lines.sales_channel as channel,orders.new_vs_repeat as new_vs_repeat,"
    correct += "COUNT(case when order_lines.sales_channel = 'Email' then order_lines.order_id end) "
    correct += "as number_of_email_purchased_items FROM analytics.order_line_items "
    correct += "order_lines LEFT JOIN analytics.orders orders ON order_lines.order_id=orders.order_id"
    correct += " GROUP BY order_lines.sales_channel,orders.new_vs_repeat;"
    assert query == correct


def test_query_multiple_join():
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


def test_query_multiple_join_where_dict():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        where=[{"field": "region", "expression": "not_equal_to", "value": "West"}],
        config=config_mock,
    )

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id WHERE customers.region<>'West' "
    correct += "GROUP BY customers.region,orders.new_vs_repeat;"
    assert query == correct


def test_query_multiple_join_where_literal():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        where="region != 'West'",
        config=config_mock,
    )

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id WHERE customers.region != 'West' "
    correct += "GROUP BY customers.region,orders.new_vs_repeat;"
    assert query == correct


def test_query_multiple_join_having_dict():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        having=[{"field": "total_item_revenue", "expression": "greater_than", "value": -12}],
        config=config_mock,
    )

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id "
    correct += "GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue)>-12;"
    assert query == correct


def test_query_multiple_join_having_literal():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        having="total_item_revenue > -12",
        config=config_mock,
    )

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id "
    correct += "GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue) > -12;"
    assert query == correct


def test_query_multiple_join_order_by_literal():
    project = Project(models=models, views=views)
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_item_revenue"],
        dimensions=["region", "new_vs_repeat"],
        order_by="total_item_revenue",
        config=config_mock,
    )

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id "
    correct += "GROUP BY customers.region,orders.new_vs_repeat ORDER BY total_item_revenue ASC;"
    assert query == correct


def test_query_multiple_join_all():
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

    correct = "SELECT customers.region as region,orders.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(order_lines.revenue) as total_item_revenue FROM "
    correct += "analytics.order_line_items order_lines LEFT JOIN analytics.customers customers "
    correct += "ON order_lines.customer_id=customers.customer_id LEFT JOIN analytics.orders orders "
    correct += "ON order_lines.order_id=orders.order_id WHERE customers.region<>'West' "
    correct += "GROUP BY customers.region,orders.new_vs_repeat HAVING SUM(order_lines.revenue)>-12 "
    correct += "ORDER BY total_item_revenue DESC;"
    assert query == correct
