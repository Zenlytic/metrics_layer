import pytest

from metrics_layer.core.model.project import AccessDeniedOrDoesNotExistException


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
    assert exc_info.value.object_name == "region"
    assert exc_info.value.object_type == "field"

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

    assert "sub_channel" not in query

    connection.config.project.set_user({"department": "executive", "owned_region": "US-West"})

    query = connection.get_sql_query(metrics=["total_revenue"], dimensions=["new_vs_repeat"])

    correct = (
        "SELECT orders.new_vs_repeat as orders_new_vs_repeat,COALESCE(CAST((SUM(DISTINCT "
        "(CAST(FLOOR(COALESCE(orders.revenue, 0) * (1000000 * 1.0)) AS DECIMAL(38,0))) + "
        "(TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) "
        "- SUM(DISTINCT (TO_NUMBER(MD5(orders.id), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % "
        "1.0e27)::NUMERIC(38, 0))) AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION),"
        " 0) as orders_total_revenue FROM analytics.order_line_items order_lines "
        "LEFT JOIN analytics.orders orders ON order_lines.order_unique_id=orders.id "
        "LEFT JOIN analytics.customers customers ON order_lines.customer_id=customers.customer_id "
        "WHERE customers.region = 'US-West' "
        "GROUP BY orders.new_vs_repeat ORDER BY orders_total_revenue DESC;"
    )
    assert correct == query
