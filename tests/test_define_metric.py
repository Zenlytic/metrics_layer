import os

from granite.core.model.project import Project
from granite.core.parse.project_reader import ProjectReader
from granite.core.query import define

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


def test_define_call():
    project = Project(models=models, views=views)
    config_mock.project = project
    metric_definition = define(metric="total_item_revenue", config=config_mock)
    assert metric_definition == "SUM(order_lines.revenue)"

    metric_definition = define(metric="number_of_email_purchased_items", config=config_mock)
    correct = "COUNT(case when order_lines.sales_channel = 'Email' then order_lines.order_id end)"
    assert metric_definition == correct
