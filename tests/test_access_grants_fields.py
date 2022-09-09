import pytest

from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException


def test_access_grants_exist(connection):
    model = connection.get_model("test_model")
    connection.project.set_user({"email": "user@example.com"})

    assert isinstance(model.access_grants, list)
    assert model.access_grants[0]["name"] == "test_access_grant_department_view"
    assert model.access_grants[0]["user_attribute"] == "department"
    assert model.access_grants[0]["allowed_values"] == ["finance", "executive", "sales"]


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
