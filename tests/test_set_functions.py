import pytest


@pytest.mark.project
def test_sets(connection):
    sets = connection.project.sets()
    assert len(sets) == 5

    sets = connection.project.sets(view_name="orders")
    assert len(sets) == 5

    sets = connection.project.sets(view_name="order_lines")
    assert len(sets) == 0

    _set = connection.project.get_set(set_name="test_set")
    assert _set.field_names() == ["orders.order_id", "orders.customer_id", "orders.total_revenue"]

    _set = connection.project.get_set(set_name="test_set2")
    assert _set.field_names() == [
        "orders.order_id",
        "orders.new_vs_repeat",
        "orders.sub_channel",
        "orders.average_order_value",
        "orders.order_time",
    ]

    _set = connection.project.get_set(set_name="test_set_composed")
    assert _set.field_names() == [
        "orders.order_id",
        "orders.customer_id",
        "orders.total_revenue",
        "orders.average_order_value",
        "orders.order_time",
    ]

    _set = connection.project.get_set(set_name="test_set_all_fields")
    assert _set.field_names() == [
        "orders.customer_id",
        "orders.account_id",
        "orders.do_not_use",
        "orders.order_raw",
        "orders.order_date",
        "orders.order_day_of_year",
        "orders.order_week",
        "orders.order_week_of_year",
        "orders.order_month",
        "orders.order_month_of_year",
        "orders.order_quarter",
        "orders.order_year",
        "orders.order_day_of_week",
        "orders.order_hour_of_day",
        "orders.previous_order_raw",
        "orders.previous_order_time",
        "orders.previous_order_date",
        "orders.previous_order_week",
        "orders.previous_order_month",
        "orders.previous_order_quarter",
        "orders.previous_order_year",
        "orders.hours_between_orders",
        "orders.days_between_orders",
        "orders.weeks_between_orders",
        "orders.months_between_orders",
        "orders.quarters_between_orders",
        "orders.years_between_orders",
        "orders.revenue_in_cents",
        "orders.warehouse_location",
        "orders.campaign",
        "orders.number_of_orders",
        "orders.average_days_between_orders",
        "orders.total_revenue",
        "orders.total_lifetime_revenue",
        "orders.cumulative_customers",
        "orders.cumulative_customers_no_change_grain",
        "orders.cumulative_aov",
        "orders.ltv",
        "orders.ltr",
        "orders.total_modified_revenue",
        "orders.average_order_value_custom",
        "orders.new_order_count",
    ]


@pytest.mark.project
def test_drill_fields(connection):
    field = connection.project.get_field("orders.number_of_orders")

    drill_field_names = field.drill_fields
    assert field.id() == "orders.number_of_orders"
    assert drill_field_names == [
        "orders.order_id",
        "orders.customer_id",
        "orders.total_revenue",
        "orders.new_vs_repeat",
    ]

    field = connection.project.get_field("orders.total_revenue")
    assert field.drill_fields is None
    assert field.id() == "orders.total_revenue"


@pytest.mark.project
def test_joinable_fields_join(connection):
    field = connection.project.get_field("orders.number_of_orders")

    joinable_fields = connection.project.joinable_fields([field], expand_dimension_groups=True)
    names = [f.id() for f in joinable_fields]
    must_exclude = ["traffic.traffic_source"]
    must_include = [
        "orders.order_date",
        "order_lines.order_id",
        "customers.number_of_customers",
        "country_detail.rainfall",
        "discount_detail.discount_usd",
        "discounts.discount_code",
        "order_lines.revenue_per_session",
        "sessions.number_of_sessions",
    ]
    assert all(name in names for name in must_include)
    assert all(name not in names for name in must_exclude)

    test_fields = [field]
    add_fields = [
        "order_lines.order_month",
        "order_lines.product_name",
        "customers.gender",
        "order_lines.ending_on_hand_qty",
    ]
    for field_name in add_fields:
        test_fields.append(connection.project.get_field(field_name))

    joinable_fields = connection.project.joinable_fields(test_fields, expand_dimension_groups=True)
    names = [f.id() for f in joinable_fields]
    assert all(name in names for name in must_include)


@pytest.mark.project
def test_joinable_fields_merged(connection):
    field = connection.project.get_field("order_lines.revenue_per_session")

    # Add tests for exclusions here
    joinable_fields = connection.project.joinable_fields([field], expand_dimension_groups=True)
    names = [f.id() for f in joinable_fields]
    must_exclude = ["traffic.traffic_source", "discounts.discount_code"]
    must_include = [
        "order_lines.order_date",
        "customers.gender",
        "order_lines.total_item_revenue",
        "order_lines.revenue_per_session",
        "orders.sub_channel",
        "sessions.utm_source",
        "sessions.session_year",
        "sessions.number_of_sessions",
    ]
    assert all(name in names for name in must_include)
    assert all(name not in names for name in must_exclude)
