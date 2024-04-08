import pytest

from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException


@pytest.mark.project
def test_null_values_filters_canon_date(connection):
    revenue_field = connection.project.get_field("total_revenue")

    assert revenue_field.filters == []
    assert revenue_field.canon_date == "orders.order"

    orders_field = connection.project.get_field("number_of_orders")

    assert orders_field.filters == []
    assert orders_field.canon_date == "orders.order"


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
    assert all("Warning:" in e for e in errors)

    connection.project.remove_field("total_new_revenue", view_name="orders")
