import pytest


@pytest.mark.project
def test_sets(connection):
    sets = connection.config.project.sets()
    assert len(sets) == 5

    sets = connection.config.project.sets(explore_name="order_lines_all")
    assert len(sets) == 5

    sets = connection.config.project.sets(view_name="orders")
    assert len(sets) == 5

    sets = connection.config.project.sets(view_name="order_lines")
    assert len(sets) == 0

    _set = connection.config.project.get_set(set_name="test_set")
    assert _set.field_names() == ["orders.order_id", "orders.customer_id", "orders.total_revenue"]

    _set = connection.config.project.get_set(set_name="test_set2")
    assert _set.field_names() == [
        "orders.order_id",
        "orders.new_vs_repeat",
        "orders.sub_channel",
        "orders.average_order_value",
        "orders.order_time",
    ]

    _set = connection.config.project.get_set(set_name="test_set_composed")
    assert _set.field_names() == [
        "orders.order_id",
        "orders.customer_id",
        "orders.total_revenue",
        "orders.average_order_value",
        "orders.order_time",
    ]

    _set = connection.config.project.get_set(set_name="test_set_all_fields")
    assert _set.field_names() == [
        "orders.customer_id",
        "orders.do_not_use",
        "orders.order_raw",
        "orders.order_date",
        "orders.order_week",
        "orders.order_month",
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
        "orders.number_of_orders",
        "orders.average_days_between_orders",
        "orders.total_revenue",
        "orders.total_modified_revenue",
        "orders.average_order_value_custom",
        "orders.new_order_count",
    ]


def test_explore_sets(connection):
    explore = connection.config.project.get_explore("order_lines_all")

    explore_field_names = explore.field_names()
    excluded = ["discounts.country", "orders.do_not_use"]
    assert not any(fn in explore_field_names for fn in excluded)


def test_drill_fields(connection):
    field = connection.config.project.get_field("orders.number_of_orders", explore_name="order_lines_all")

    drill_field_names = field.drill_fields
    assert field.id() == "order_lines_all.orders.number_of_orders"
    assert drill_field_names == [
        "orders.order_id",
        "orders.customer_id",
        "orders.total_revenue",
        "orders.new_vs_repeat",
    ]

    field = connection.config.project.get_field("orders.total_revenue")
    assert field.drill_fields is None
    assert field.id() == "orders.total_revenue"
