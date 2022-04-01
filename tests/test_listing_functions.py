import pytest

from metrics_layer.core import MetricsLayerConnection


@pytest.mark.project
def test_list_metrics(config):
    conn = MetricsLayerConnection(config=config)
    metrics = conn.list_metrics()
    assert len(metrics) == 23

    metrics = conn.list_metrics(explore_name="order_lines_all")
    assert len(metrics) == 22

    metrics = conn.list_metrics(view_name="order_lines", names_only=True)
    assert len(metrics) == 7
    assert set(metrics) == {
        "number_of_email_purchased_items",
        "average_order_revenue",
        "total_item_revenue",
        "total_item_costs",
        "line_item_aov",
        "ending_on_hand_qty",
        "revenue_per_session",
    }


@pytest.mark.project
def test_list_dimensions(config):
    conn = MetricsLayerConnection(config=config)
    dimensions = conn.list_dimensions(show_hidden=True)
    assert len(dimensions) == 42

    dimensions = conn.list_dimensions()
    assert len(dimensions) == 30

    dimensions = conn.list_dimensions(explore_name="order_lines_all", show_hidden=True)
    assert len(dimensions) == 29

    dimensions = conn.list_dimensions(explore_name="order_lines_all")
    assert len(dimensions) == 18

    dimensions = conn.list_dimensions(view_name="order_lines", names_only=True, show_hidden=True)
    dimensions_present = {
        "order_line_id",
        "order_id",
        "customer_id",
        "order",
        "waiting",
        "channel",
        "parent_channel",
        "product_name",
        "inventory_qty",
        "is_on_sale_sql",
        "is_on_sale_case",
        "order_tier",
    }
    assert len(dimensions) == 12
    assert set(dimensions) == dimensions_present


@pytest.mark.project
def test_project_expand_fields(config):
    fields = config.project.fields(
        explore_name="order_lines_all", show_hidden=False, expand_dimension_groups=True
    )

    dim_groups_alias = [f.alias() for f in fields if f.view.name == "orders" and f.name == "order"]

    assert dim_groups_alias == [
        "order_time",
        "order_date",
        "order_week",
        "order_month",
        "order_quarter",
        "order_year",
        "order_day_of_week",
        "order_hour_of_day",
    ]
