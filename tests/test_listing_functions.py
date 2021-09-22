import os

from granite.core.model.project import Project
from granite.core.parse.project_reader import ProjectReader
from granite.core.query import list_dimensions, list_metrics

BASE_PATH = os.path.dirname(__file__)


model_path = os.path.join(BASE_PATH, "config/granite_config/models/commerce_test_model.yml")
order_lines_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_order_lines.yml")
orders_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_orders.yml")
customers_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_customers.yml")
discounts_view_path = os.path.join(BASE_PATH, "config/granite_config/views/test_discounts.yml")
view_paths = [order_lines_view_path, orders_view_path, customers_view_path, discounts_view_path]

models = [ProjectReader.read_yaml_file(model_path)]
views = [ProjectReader.read_yaml_file(path) for path in view_paths]


def test_list_metrics():
    project = Project(models=models, views=views)

    metrics = list_metrics(project=project)
    assert len(metrics) == 10

    metrics = list_metrics(explore_name="order_lines", project=project)
    assert len(metrics) == 10

    metrics = list_metrics(view_name="order_lines", names_only=True, project=project)
    assert len(metrics) == 3
    assert set(metrics) == {"number_of_email_purchased_items", "total_item_revenue", "total_item_costs"}


def test_list_dimensions():
    project = Project(models=models, views=views)
    dimensions = list_dimensions(project=project)
    assert len(dimensions) == 26

    dimensions = list_dimensions(explore_name="order_lines", project=project)
    assert len(dimensions) == 26

    dimensions = list_dimensions(view_name="order_lines", names_only=True, project=project)
    dimensions_present = {
        "order_line_id",
        "order_id",
        "customer_id",
        "order",
        "waiting",
        "channel",
        "product_name",
        "is_on_sale_sql",
        "is_on_sale_case",
        "order_tier",
    }
    assert len(dimensions) == 10
    assert set(dimensions) == dimensions_present
