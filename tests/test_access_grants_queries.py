import pytest

from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException


def test_access_grants_join_permission_block(connection):
    connection.config.project.set_user({"department": "executive"})
    connection.get_sql_query(sql="SELECT * FROM MQL(total_item_revenue BY gender)")
    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["product_name"])

    connection.config.project.set_user({"department": "marketing"})

    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["product_name"])

    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["gender"])

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_revenue"], dimensions=["gender"])

    assert exc_info.value
    assert exc_info.value.object_name == "total_revenue"
    assert exc_info.value.object_type == "field"

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(sql="SELECT * FROM MQL(total_revenue BY gender)")

    assert exc_info.value
    assert exc_info.value.object_name == "total_revenue"
    assert exc_info.value.object_type == "field"


def test_access_grants_view_permission_block(connection):
    connection.config.project.set_user({"department": "finance"})

    # Even with explore and view access, they cant run the query due to the
    # conditional filter that they don't have access to
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["new_vs_repeat"])

    assert exc_info.value
    assert exc_info.value.object_name == "customers"
    assert exc_info.value.object_type == "view"

    # Even though they have view access, they don't have explore access so still can't run this query
    connection.config.project.set_user({"department": "sales"})
    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["new_vs_repeat"])

    connection.config.project.set_user({"department": "marketing"})

    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["product_name"])

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["new_vs_repeat"])

    assert exc_info.value
    assert exc_info.value.object_name == "new_vs_repeat"
    assert exc_info.value.object_type == "field"


def test_access_grants_field_permission_block(connection):
    connection.config.project.set_user({"department": "executive"})

    connection.get_sql_query(metrics=["total_revenue"], dimensions=["new_vs_repeat"])
    connection.get_sql_query(metrics=["total_revenue"], dimensions=["product_name"])
    connection.get_sql_query(metrics=["total_revenue"], dimensions=["gender"])

    connection.config.project.set_user({"department": "engineering"})
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.query(metrics=["total_revenue"], dimensions=["product_name"])

    assert exc_info.value
    assert exc_info.value.object_name == "total_revenue"
    assert exc_info.value.object_type == "field"


def test_access_filter(connection):
    connection.config.project.set_user({"department": "executive"})

    query = connection.get_sql_query(metrics=["total_revenue"], dimensions=["new_vs_repeat"])

    assert "region" not in query

    connection.config.project.set_user({"department": "executive", "owned_region": "US-West"})

    query = connection.get_sql_query(metrics=["total_revenue"], dimensions=["new_vs_repeat"])

    correct = (
        "SELECT orders.new_vs_repeat as orders_new_vs_repeat,SUM(orders.revenue) as orders_total_revenue "
        "FROM analytics.orders orders LEFT JOIN analytics.customers customers "
        "ON orders.customer_id=customers.customer_id WHERE customers.region = 'US-West' "
        "GROUP BY orders.new_vs_repeat ORDER BY orders_total_revenue DESC;"
    )
    assert correct == query
