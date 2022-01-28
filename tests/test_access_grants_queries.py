import pytest

from metrics_layer.core.model.project import AccessDeniedOrDoesNotExistException


@pytest.mark.mmm
def test_access_grants_explore_permission_block(connection):
    connection.config.project.set_user({"department": "marketing"})

    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["channel"])

    connection.config.project.set_user({"department": "operations"})

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["channel"])

    assert exc_info.value
    assert exc_info.value.object_name == "total_item_revenue"
    assert exc_info.value.object_type == "field"

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.query(metrics=["total_item_revenue"], dimensions=["channel"])

    assert exc_info.value
    assert exc_info.value.object_name == "total_item_revenue"
    assert exc_info.value.object_type == "field"

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(sql="SELECT * FROM MQL(total_item_revenue BY channel)")

    assert exc_info.value
    assert exc_info.value.object_name == "total_item_revenue"
    assert exc_info.value.object_type == "field"


@pytest.mark.mmm
def test_access_grants_join_permission_block(connection):
    connection.config.project.set_user({"department": "executive"})
    connection.get_sql_query(sql="SELECT * FROM MQL(total_item_revenue BY gender)")
    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["product_name"])

    connection.config.project.set_user({"department": "marketing"})

    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["product_name"])

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["gender"])

    assert exc_info.value
    assert exc_info.value.object_name == "gender"
    assert exc_info.value.object_type == "field"

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(sql="SELECT * FROM MQL(total_item_revenue BY gender)")

    assert exc_info.value
    assert exc_info.value.object_name == "gender"
    assert exc_info.value.object_type == "field"


@pytest.mark.mmm
def test_access_grants_view_permission_block(connection):
    connection.config.project.set_user({"department": "finance"})

    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["new_vs_repeat"])

    # Even though they have view access, they don't have explore access so still can't run this query
    connection.config.project.set_user({"department": "sales"})
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["new_vs_repeat"])

    assert exc_info.value
    assert exc_info.value.object_name == "total_item_revenue"
    assert exc_info.value.object_type == "field"

    connection.config.project.set_user({"department": "marketing"})

    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["product_name"])

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["new_vs_repeat"])

    assert exc_info.value
    assert exc_info.value.object_name == "new_vs_repeat"
    assert exc_info.value.object_type == "field"


@pytest.mark.mmm
def test_access_grants_field_permission_block(connection):
    connection.config.project.set_user({"department": "executive"})

    connection.get_sql_query(metrics=["total_revenue"], dimensions=["new_vs_repeat"])
    connection.get_sql_query(metrics=["total_revenue"], dimensions=["product_name"])
    connection.get_sql_query(metrics=["total_revenue"], dimensions=["gender"])

    connection.config.project.set_user({"department": "finance"})
    connection.get_sql_query(metrics=["number_of_orders"], dimensions=["product_name"])

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_revenue"], dimensions=["product_name"])

    assert exc_info.value
    assert exc_info.value.object_name == "total_revenue"
    assert exc_info.value.object_type == "field"

    connection.config.project.set_user({"department": "engineering"})
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.query(metrics=["total_revenue"], dimensions=["product_name"])

    assert exc_info.value
    assert exc_info.value.object_name == "total_revenue"
    assert exc_info.value.object_type == "field"
