import pytest


@pytest.mark.project
def test_list_views(connection):
    views = connection.list_views(names_only=True)
    assert len(views) == 23

    assert set(views) == {
        "aa_acquired_accounts",
        "accounts",
        "child_account",
        "clicked_on_page",
        "country_detail",
        "created_workspace",
        "customers",
        "customer_accounts",
        "discount_detail",
        "discounts",
        "events",
        "login_events",
        "monthly_aggregates",
        "mrr",
        "order_lines",
        "orders",
        "other_db_traffic",
        "parent_account",
        "query_in_workspace",
        "sessions",
        "submitted_form",
        "traffic",
        "z_customer_accounts",
    }


@pytest.mark.project
def test_list_metrics(connection):
    metrics = connection.list_metrics()
    assert len(metrics) == 79

    metrics = connection.list_metrics(view_name="order_lines", names_only=True)
    assert len(metrics) == 13
    assert set(metrics) == {
        "average_order_revenue",
        "costs_per_session",
        "ending_on_hand_qty",
        "line_item_aov",
        "should_be_number",
        "net_per_session",
        "number_of_email_purchased_items",
        "number_of_new_purchased_items",
        "pct_of_total_item_revenue",
        "revenue_per_session",
        "total_item_costs",
        "total_item_costs_pct",
        "total_item_revenue",
    }


@pytest.mark.project
def test_list_dimensions(connection):
    dimensions = connection.list_dimensions(show_hidden=True)
    assert len(dimensions) == 122

    dimensions = connection.list_dimensions()
    assert len(dimensions) == 86

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
        "product_name_lang",
        "inventory_qty",
        "is_on_sale_sql",
        "order_sequence",
        "new_vs_repeat_status",
        "is_on_sale_case",
        "order_tier",
    }
    assert len(dimensions) == 15
    assert set(dimensions) == dimensions_present


@pytest.mark.project
def test_project_expand_fields(connection):
    fields = connection.project.fields(show_hidden=False, expand_dimension_groups=True)

    dim_groups_alias = [f.alias() for f in fields if f.view.name == "orders" and f.name == "order"]

    assert dim_groups_alias == [
        "order_time",
        "order_date",
        "order_day_of_year",
        "order_week",
        "order_week_of_year",
        "order_month",
        "order_month_of_year",
        "order_quarter",
        "order_year",
        "order_day_of_week",
        "order_hour_of_day",
    ]
