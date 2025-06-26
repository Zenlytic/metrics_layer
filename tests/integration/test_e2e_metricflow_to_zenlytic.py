import os

import pytest

from metrics_layer.integrations.metricflow.metricflow_to_zenlytic import (
    convert_mf_project_to_zenlytic_project,
    load_mf_project,
)

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
    models, views, errors = convert_mf_project_to_zenlytic_project(
        metricflow_project, "my_model", "my_company"
    )

    print(views)
    assert len(errors) == 3
    percentile_error = next(e for e in errors if "p99_order_total" in e["message"])
    assert percentile_error == {
        "message": "In view orders discrete percentile is not supported for the measure p99_order_total",
        "view_name": "orders",
    }
    food_customers_error = next(e for e in errors if "food_customers" in e["message"])
    assert food_customers_error == {
        "message": (
            "In view orders metric conversion failed for food_customers: Metric type filters are"
            " not supported"
        ),
        "view_name": "orders",
    }
    cumulative_revenue_error = next(e for e in errors if "cumulative_revenue" in e["message"])
    assert cumulative_revenue_error == {
        "message": (
            "In view order_item metric conversion failed for cumulative_revenue: It is a cumulative metric,"
            " which is not supported."
        ),
        "view_name": "order_item",
    }

    assert len(models) == 1
    assert models[0]["name"] == "my_model"
    assert models[0]["connection"] == "my_company"

    assert len(views) == 3

    customers_view = next(v for v in views if v["name"] == "customers")

    # All measures are hidden in this view, because there are no metrics
    assert all(f["hidden"] for f in customers_view["fields"] if f["field_type"] == "measure")
    assert customers_view["model_name"] == "my_model"
    assert customers_view["sql_table_name"] == "my-bigquery-project.my_dataset.customers"
    assert "description" not in customers_view
    assert customers_view["zoe_description"] == "Customers table"
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
    assert customers_with_fields_metric["sql"] == "${TABLE}.customer_id"
    assert customers_with_fields_metric["type"] == "count_distinct"
    assert customers_with_fields_metric["label"] == "Customers w/ Orders"
    assert customers_with_fields_metric["description"] == "Unique count of customers placing orders"
    assert customers_with_fields_metric["zoe_description"] == "Distinct count of customers placing orders"

    order_total_dim = next(m for m in orders_view["fields"] if "order_total_dim" == m["name"])
    assert order_total_dim["field_type"] == "dimension"
    assert order_total_dim["sql"] == "${TABLE}.order_total"
    assert order_total_dim["type"] == "string"

    large_orders_metric = next(m for m in orders_view["fields"] if "large_order" == m["name"])
    assert large_orders_metric["field_type"] == "measure"
    assert not large_orders_metric["hidden"]
    assert large_orders_metric["sql"] == "case when  ${orders.order_total_dim}  >= 20\n then 1 else null end"

    order_item_view = next(v for v in views if v["name"] == "order_item")

    # Validate the outputs are defined in the view
    assert next(f for f in order_item_view["fields"] if "ad_revenue" == f["name"])
    assert next(f for f in order_item_view["fields"] if "revenue" == f["name"])

    pct_rev_from_ads_field = next(f for f in order_item_view["fields"] if "pct_rev_from_ads" == f["name"])
    assert pct_rev_from_ads_field["field_type"] == "measure"
    assert pct_rev_from_ads_field["type"] == "number"
    assert pct_rev_from_ads_field["sql"] == "${ad_revenue} / ${revenue}"
    assert pct_rev_from_ads_field["label"] == "Percentage of Revenue from Advertising"
    assert not pct_rev_from_ads_field.get("hidden", False)

    food_revenue_field = next(f for f in order_item_view["fields"] if "food_revenue" == f["name"])
    assert food_revenue_field["field_type"] == "measure"
    assert food_revenue_field["type"] == "sum"
    assert (
        food_revenue_field["sql"]
        == "CASE WHEN ${TABLE}.is_food_item = 1 THEN ${TABLE}.product_price ELSE 0 END"
    )
    assert food_revenue_field["label"] == "Food Revenue"
    assert not food_revenue_field.get("hidden", False)

    # Validate the inputs are defined in the view
    assert next(f for f in order_item_view["fields"] if "number_of_repeat_orders" == f["name"])
    assert next(f for f in order_item_view["fields"] if "number_of_orders" == f["name"])

    rep_rate_ratio_field = next(f for f in order_item_view["fields"] if "repurchase_rate" == f["name"])
    assert rep_rate_ratio_field["field_type"] == "measure"
    assert rep_rate_ratio_field["type"] == "number"
    assert rep_rate_ratio_field["sql"] == "${number_of_repeat_orders} / ${number_of_orders}"
    assert rep_rate_ratio_field["label"] == "Repurchase Rate"
    assert not rep_rate_ratio_field.get("hidden", False)

    revenue_field = next(f for f in order_item_view["fields"] if "revenue" == f["name"])
    assert revenue_field["field_type"] == "measure"
    assert revenue_field["type"] == "sum"
    assert revenue_field["sql"] == "${TABLE}.product_price"
    assert revenue_field["label"] == "Revenue"
    assert not revenue_field.get("hidden", False)

    # Validate the inputs are defined in the view
    assert next(f for f in order_item_view["fields"] if "food_revenue" == f["name"])
    assert next(f for f in order_item_view["fields"] if "revenue" == f["name"])

    food_ratio_field = next(f for f in order_item_view["fields"] if "food_revenue_pct" == f["name"])
    assert food_ratio_field["field_type"] == "measure"
    assert food_ratio_field["type"] == "number"
    assert food_ratio_field["sql"] == "${food_revenue} / ${revenue}"
    assert food_ratio_field["label"] == "Food Revenue %"
    assert not food_ratio_field.get("hidden", False)

    assert order_item_view["identifiers"][0]["name"] == "order_item"
    assert order_item_view["identifiers"][0]["type"] == "primary"
    assert order_item_view["identifiers"][0]["sql"] == "${TABLE}.order_item_id"
    assert order_item_view["identifiers"][1]["name"] == "order_id"
    assert order_item_view["identifiers"][1]["type"] == "foreign"
    assert order_item_view["identifiers"][1]["sql"] == "CAST(${TABLE}.order_id AS VARCHAR)"
