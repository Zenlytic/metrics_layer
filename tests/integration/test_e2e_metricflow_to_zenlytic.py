import pytest
from metrics_layer.integrations.metricflow.metricflow_to_zenlytic import (
    load_mf_project,
    convert_mf_project_to_zenlytic_project,
)
import os

BASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../config")


@pytest.mark.metricflow
def test_e2e_read_project():
    metricflow_folder = os.path.join(BASE_PATH, "metricflow")

    metricflow_project = load_mf_project(metricflow_folder)
    print(metricflow_project)

    assert set(metricflow_project.keys()) == {"customers", "orders", "order_item"}
    assert metricflow_project["customers"]["metrics"] == []
    assert any("customers_with_orders" in m["name"] for m in metricflow_project["orders"]["metrics"])


@pytest.mark.metricflow
def test_e2e_conversions():
    metricflow_folder = os.path.join(BASE_PATH, "metricflow")

    metricflow_project = load_mf_project(metricflow_folder)
    models, views = convert_mf_project_to_zenlytic_project(metricflow_project, "my_model", "my_company")

    print(views)

    assert len(models) == 1
    assert models[0]["name"] == "my_model"
    assert models[0]["connection"] == "my_company"

    assert len(views) == 3

    customers_view = next(v for v in views if v["name"] == "customers")

    # All measures are hidden in this view, because there are no metrics
    assert all(f["hidden"] for f in customers_view["fields"] if f["field_type"] == "measure")
    assert customers_view["model_name"] == "my_model"
    assert customers_view["sql_table_name"] == "my-bigquery-project.my_dataset.customers"

    orders_view = next(v for v in views if v["name"] == "orders")

    assert orders_view["model_name"] == "my_model"
    assert orders_view["sql_table_name"] == "orders"
    order_count = next(m for m in orders_view["fields"] if "_order_count" == m["name"])
    assert order_count["sql"] == "1"

    customers_with_fields_metric = next(
        m for m in orders_view["fields"] if "customers_with_orders" == m["name"]
    )
    assert customers_with_fields_metric["field_type"] == "measure"
    assert not customers_with_fields_metric["hidden"]
    assert customers_with_fields_metric["sql"] == "customer_id"
    assert customers_with_fields_metric["type"] == "count_distinct"
    assert customers_with_fields_metric["label"] == "Customers w/ Orders"
    assert customers_with_fields_metric["description"] == "Unique count of customers placing orders"
    assert customers_with_fields_metric["zoe_description"] == "Distinct count of customers placing orders"

    order_total_dim = next(m for m in orders_view["fields"] if "order_total_dim" == m["name"])
    assert order_total_dim["field_type"] == "dimension"
    assert order_total_dim["sql"] == "order_total"
    assert order_total_dim["type"] == "string"

    large_orders_metric = next(m for m in orders_view["fields"] if "large_order" == m["name"])
    assert large_orders_metric["field_type"] == "measure"
    assert not large_orders_metric["hidden"]
    assert large_orders_metric["sql"] == "case when  ${orders.order_total_dim}  >= 20\n then 1 else null end"

    order_item_view = next(v for v in views if v["name"] == "order_item")

    assert order_item_view["identifiers"][0]["name"] == "order_item"
    assert order_item_view["identifiers"][0]["type"] == "primary"
    assert order_item_view["identifiers"][0]["sql"] == "${order_item_id}"
    assert order_item_view["identifiers"][1]["name"] == "order_id"
    assert order_item_view["identifiers"][1]["type"] == "foreign"
    assert order_item_view["identifiers"][1]["sql"] == "${order_id}"
