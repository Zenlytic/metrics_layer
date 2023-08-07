import pytest


@pytest.mark.project
def test_list_metrics(connection):
    metrics = connection.list_metrics()
    assert len(metrics) == 39

    metrics = connection.list_metrics(view_name="order_lines", names_only=True)
    assert len(metrics) == 11
    assert set(metrics) == {
        "average_order_revenue",
        "costs_per_session",
        "ending_on_hand_qty",
        "line_item_aov",
        "should_be_number",
        "net_per_session",
        "number_of_email_purchased_items",
        "revenue_per_session",
        "total_item_costs",
        "total_item_costs_pct",
        "total_item_revenue",
    }


@pytest.mark.project
def test_list_dimensions(connection):
    dimensions = connection.list_dimensions(show_hidden=True)
    assert len(dimensions) == 69

    dimensions = connection.list_dimensions()
    assert len(dimensions) == 46

    dimensions = connection.list_dimensions(view_name="order_lines", names_only=True, show_hidden=True)
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
def test_project_expand_fields(connection):
    fields = connection.project.fields(show_hidden=False, expand_dimension_groups=True)

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
