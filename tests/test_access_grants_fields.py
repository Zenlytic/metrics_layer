import pytest

from metrics_layer.core.exceptions import (
    AccessDeniedOrDoesNotExistException,
    MetricsLayerException,
)


def test_access_grants_exist(connection):
    model = connection.get_model("test_model")
    connection.project.set_user({"email": "user@example.com"})

    assert isinstance(model.access_grants, list)
    assert model.access_grants[1]["name"] == "test_access_grant_department_view"
    assert model.access_grants[1]["user_attribute"] == "department"
    assert model.access_grants[1]["allowed_values"] == ["finance", "executive", "sales"]


def test_access_grants_model_visible(connection):
    connection.project.set_user(None)
    connection.get_model("test_model")

    connection.project.set_user({"region": "east"})
    connection.get_model("test_model")

    connection.project.set_user({"region": "south"})

    connection.project.get_model("new_model")
    connection.project.get_topic("Other DB Traffic")
    connection.project.get_view("other_db_traffic")
    connection.project.get_field("other_db_traffic.other_traffic_source")

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_model("test_model")

    assert exc_info.value
    assert exc_info.value.object_name == "test_model"
    assert exc_info.value.object_type == "model"

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_view("orders")

    assert exc_info.value
    assert exc_info.value.object_name == "orders"
    assert exc_info.value.object_type == "view"

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_field("order_lines.customer_id")

    assert isinstance(exc_info.value, MetricsLayerException)
    assert exc_info.value.object_name == "order_lines"
    assert exc_info.value.object_type == "view"


def test_access_grants_view_visible(connection):
    connection.project.set_user(None)
    connection.get_view("orders")

    connection.project.set_user({"department": "sales"})
    connection.get_view("orders")

    connection.project.set_user({"department": "marketing"})

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_view("orders")

    assert exc_info.value
    assert exc_info.value.object_name == "orders"
    assert exc_info.value.object_type == "view"


def test_access_grants_field_visible(connection):
    # None always allows access
    connection.project.set_user({"department": None})
    connection.get_field("orders.total_revenue")

    connection.project.set_user({"department": "executive"})
    connection.get_field("orders.total_revenue")

    connection.project.set_user({"department": "sales"})
    connection.get_field("orders.total_revenue")

    # Having permissions on the field isn't enough, you must also have permissions on the view to see field
    connection.project.set_user({"department": "engineering"})

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_field("orders.total_revenue")

    assert exc_info.value
    assert exc_info.value.object_name == "orders"
    assert exc_info.value.object_type == "view"

    connection.project.set_user({"department": "operations"})

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_field("orders.total_revenue")

    assert exc_info.value
    assert exc_info.value.object_name == "orders"
    assert exc_info.value.object_type == "view"

    connection.project.set_user({"department": "finance"})

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_field("orders.total_revenue")

    assert exc_info.value
    assert exc_info.value.object_name == "total_revenue"
    assert exc_info.value.object_type == "field"


def test_access_grants_field_join_graphs(connection):
    connection.project.set_user({"department": "executive"})
    field = connection.get_field("sessions.number_of_sessions")
    field.join_graphs()

    # Having permissions on the field isn't enough, you must also have permissions on the view to see field
    connection.project.set_user({"department": "engineering"})

    # This is a regression test for a bug where the call to join_graphs would raise an access denied
    # exception because one of the mapped fields was not visible to this user
    field = connection.get_field("sessions.number_of_sessions")
    field.join_graphs()


def test_access_grants_dashboard_visible(connection):
    # None always allows access
    connection.project.set_user({"department": None})
    connection.get_dashboard("sales_dashboard_v2")

    connection.project.set_user({"department": "sales"})
    connection.get_dashboard("sales_dashboard_v2")

    connection.project.set_user({"department": "operations"})
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_dashboard("sales_dashboard_v2")

    assert exc_info.value
    assert exc_info.value.object_name == "sales_dashboard_v2"
    assert exc_info.value.object_type == "dashboard"
