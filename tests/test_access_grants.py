import pytest


@pytest.mark.mmm
def test_access_grants_exist(connection):
    model = connection.get_model("test_model")
    connection.config.project.set_user({"email": "user@example.com"})

    assert isinstance(model.access_grants, list)
    assert model.access_grants[0]["name"] == "test_access_grant_department_explore"
    assert model.access_grants[0]["user_attribute"] == "department"
    assert model.access_grants[0]["allowed_values"] == ["finance", "executive", "marketing"]


@pytest.mark.mmm
def test_access_grants_explore_visible(connection):
    # No set user has access to everything
    connection.config.project.set_user(None)
    explores = connection.list_explores()

    assert len(explores) == 2

    connection.get_explore("order_lines_all")

    # Users with department finance have access to this explore
    connection.config.project.set_user({"department": "finance"})

    explores = connection.list_explores()

    assert len(explores) == 2

    connection.get_explore("order_lines_all")

    # Users with department operations do NOT have access to this explore
    connection.config.project.set_user({"department": "operations"})
    explores = connection.list_explores()

    assert len(explores) == 1

    with pytest.raises(ValueError) as exc_info:
        connection.get_explore("order_lines_all")

    assert exc_info.value


@pytest.mark.mmm
def test_access_grants_joins_visible(connection):
    n_joins = 5
    # No set user has access to everything
    connection.config.project.set_user(None)
    explore = connection.get_explore("order_lines_all")

    assert len(explore.joins()) == n_joins
    assert explore.get_join("customers") is not None

    # Users with department finance have access to this explore, but not one join
    connection.config.project.set_user({"department": "finance"})

    explore = connection.get_explore("order_lines_all")

    assert len(explore.joins()) == (n_joins - 1)
    assert explore.get_join("customers") is None


@pytest.mark.mmm
def test_access_grants_view_visible(connection):
    connection.config.project.set_user(None)
    connection.get_view("orders")

    # The permission limitation on the Explore doesn't effect the view
    connection.config.project.set_user({"department": "sales"})
    connection.get_view("orders")

    # Just because the user has access to the explore doesn't mean they have access to all views
    connection.config.project.set_user({"department": "marketing"})

    with pytest.raises(ValueError) as exc_info:
        connection.get_view("orders")

    assert exc_info.value


@pytest.mark.mmm
def test_access_grants_field_visible(connection):
    # None always allows access
    connection.config.project.set_user({"department": None})
    connection.get_field("orders.total_revenue")

    connection.config.project.set_user({"department": "executive"})
    connection.get_field("orders.total_revenue")

    # The permission limitation on the Explore doesn't effect the view containing this field or the field
    connection.config.project.set_user({"department": "sales"})
    connection.get_field("orders.total_revenue")

    # Having permissions on the field isn't enough, you must also have permissions on the view to see field
    connection.config.project.set_user({"department": "engineering"})

    with pytest.raises(ValueError) as exc_info:
        connection.get_field("orders.total_revenue")

    assert exc_info.value

    connection.config.project.set_user({"department": "operations"})

    with pytest.raises(ValueError) as exc_info:
        connection.get_field("orders.total_revenue")

    assert exc_info.value
