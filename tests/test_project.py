import pytest

from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException


@pytest.mark.project
def test_result_types(connection):
    """
    Tests the result_type property for all possible ZenlyticType options.
    The result_type property should map the native field type to one of four values:
    - string (for string, tier)
    - yesno
    - time
    - number (for number, duration, cumulative, and all measure types)
    """
    # Test string type (includes tier)
    string_field = connection.project.get_field("order_lines.product_name")
    assert string_field.type == "string"
    assert string_field.result_type == "string"

    tier_field = connection.project.get_field("order_lines.order_tier")
    assert tier_field.type == "tier"
    assert tier_field.result_type == "string"

    dim_number_field = connection.project.get_field("orders.anon_id")
    assert dim_number_field.type == "number"
    assert dim_number_field.result_type == "number"

    # Test yesno type
    yesno_field = connection.project.get_field("order_lines.is_on_sale_sql")
    assert yesno_field.type == "yesno"
    assert yesno_field.result_type == "yesno"

    # Test time type
    time_field = connection.project.get_field("order_lines.order_date")
    assert time_field.type == "time"
    assert time_field.result_type == "time"

    # Test duration type (should map to number result type)
    duration_field = connection.project.get_field("order_lines.days_waiting")
    assert duration_field.type == "duration"
    assert duration_field.result_type == "number"

    # Test number types (includes number, duration, cumulative, and all measure types)
    # Number field
    number_field = connection.project.get_field("order_lines.line_item_aov")
    assert number_field.type == "number"
    assert number_field.result_type == "number"

    # Count field
    count_field = connection.project.get_field("aa_acquired_accounts.number_of_acquired_accounts_missing")
    assert count_field.type == "count"
    assert count_field.result_type == "number"

    # Count distinct field
    count_distinct_field = connection.project.get_field("events.number_of_events")
    assert count_distinct_field.type == "count_distinct"
    assert count_distinct_field.result_type == "number"

    # Sum field
    sum_field = connection.project.get_field("order_lines.total_item_revenue")
    assert sum_field.type == "sum"
    assert sum_field.result_type == "number"

    sum_distinct_field = connection.project.get_field("sessions.number_of_in_session_clicks")
    assert sum_distinct_field.type == "sum_distinct"
    assert sum_distinct_field.result_type == "number"

    # Average field
    avg_field = connection.project.get_field("orders.average_order_value")
    assert avg_field.type == "average"
    assert avg_field.result_type == "number"

    # Average distinct field
    avg_distinct_field = connection.project.get_field("order_lines.average_order_revenue")
    assert avg_distinct_field.type == "average_distinct"
    assert avg_distinct_field.result_type == "number"

    # Cumulative field
    cumulative_field = connection.project.get_field("orders.total_lifetime_revenue")
    assert cumulative_field.type == "cumulative"
    assert cumulative_field.result_type == "number"

    # Special measure types with non-standard result types
    # Measure with yesno type
    measure_yesno_field = connection.project.get_field("sessions.most_recent_session_date_is_today")
    assert measure_yesno_field.type == "yesno"
    assert measure_yesno_field.result_type == "yesno"
    assert measure_yesno_field.field_type == "measure"

    # Measure with string type
    measure_string_field = connection.project.get_field("sessions.list_of_devices_used")
    assert measure_string_field.type == "string"
    assert measure_string_field.result_type == "string"
    assert measure_string_field.field_type == "measure"

    # Measure with time type
    measure_time_field = connection.project.get_field("sessions.most_recent_session_date")
    assert measure_time_field.type == "time"
    assert measure_time_field.result_type == "time"
    assert measure_time_field.field_type == "measure"


@pytest.mark.project
def test_null_values_filters_canon_date(connection):
    revenue_field = connection.project.get_field("total_revenue")

    assert revenue_field.filters == []
    assert revenue_field.canon_date == "orders.order"

    orders_field = connection.project.get_field("number_of_orders")

    assert orders_field.filters == []
    assert orders_field.canon_date == "orders.order"


@pytest.mark.project
def test_get_joinable_views(connection):
    orders_join_views = connection.project.get_joinable_views("orders")

    assert orders_join_views == [
        "accounts",
        "country_detail",
        "customers",
        "discount_detail",
        "discounts",
        "order_lines",
    ]

    sessions_join_views = connection.project.get_joinable_views("sessions")

    assert sessions_join_views == ["customers"]


@pytest.mark.project
def test_hidden_views(connection):
    all_views = connection.project.views()

    assert "traffic" in [v.name for v in all_views]

    all_views = connection.project.views(show_hidden=False)

    assert "traffic" not in [v.name for v in all_views]


@pytest.mark.project
def test_add_field_bad_view(connection):
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.project.add_field(
            {"name": "total_new_revenue", "type": "sum", "field_type": "measure", "sql": "${TABLE}.revenue"},
            view_name="orders_does_not_exist",
        )

    assert exc_info.value
    assert exc_info.value.object_name == "orders_does_not_exist"
    assert exc_info.value.object_type == "view"


@pytest.mark.project
def test_add_and_remove_field_cache(connection):
    connection.project.add_field(
        {"name": "total_new_revenue", "type": "sum", "field_type": "measure", "sql": "${TABLE}.revenue"},
        view_name="orders",
    )
    field = connection.project.get_field("total_new_revenue")

    assert field.name == "total_new_revenue"
    assert field.type == "sum"
    assert field.field_type == "measure"
    assert field.sql == "${TABLE}.revenue"

    connection.project.remove_field("total_new_revenue", view_name="orders")
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.project.get_field("total_new_revenue")

    assert exc_info.value
    assert exc_info.value.object_name == "total_new_revenue"
    assert exc_info.value.object_type == "field"


@pytest.mark.project
def test_add_with_join_graphs_field_cache(connection):
    connection.project.add_field(
        {
            "name": "rps",
            "type": "number",
            "is_merged_result": True,
            "field_type": "measure",
            "sql": "sql: ${total_item_revenue} / nullif(${sessions.number_of_sessions}, 0)",
        },
        view_name="order_lines",
    )
    field = connection.project.get_field("rps")

    date_field = connection.project.get_field("order_lines.order_date")
    revenue_field = connection.project.get_field("order_lines.total_item_revenue")
    connections = (
        set(field.join_graphs())
        .intersection(date_field.join_graphs())
        .intersection(revenue_field.join_graphs())
    )

    assert field.name == "rps"
    assert connections != set()

    connection.project.remove_field("rps", view_name="order_lines")


@pytest.mark.project
def test_add_field_personal_fields_are_warnings(connection):
    connection.project.add_field(
        {
            "name": "total_new_revenue!",
            "type": "sum",
            "field_type": "measure",
            "sql": "${TABLE}.revenue",
            "is_personal_field": True,
        },
        view_name="orders",
    )
    field = connection.project.get_field("total_new_revenue!")
    errors = field.collect_errors()
    assert errors != []
    assert all("Warning:" in e["message"] for e in errors)

    connection.project.remove_field("total_new_revenue", view_name="orders")
