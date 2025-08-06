import pytest

from metrics_layer.core.exceptions import (
    AccessDeniedOrDoesNotExistException,
    QueryError,
)


def test_access_grants_join_permission_block(connection):
    connection.project.set_user({"department": "executive"})
    connection.get_sql_query(sql="SELECT * FROM MQL(total_item_revenue BY customers.gender)")
    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["product_name"])

    connection.project.set_user({"department": "marketing"})

    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["product_name"])

    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["customers.gender"])

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_revenue"], dimensions=["customers.gender"])

    assert exc_info.value
    assert exc_info.value.object_name == "total_revenue"
    assert exc_info.value.object_type == "field"

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(sql="SELECT * FROM MQL(total_revenue BY customers.gender)")

    assert exc_info.value
    assert exc_info.value.object_name == "total_revenue"
    assert exc_info.value.object_type == "field"


def test_access_grants_view_permission_block(connection):
    connection.project.set_user({"department": "finance"})

    # Even with explore and view access, they cant run the query due to the
    # conditional filter that they don't have access to
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["new_vs_repeat"])

    assert exc_info.value
    assert exc_info.value.object_name == "customers"
    assert exc_info.value.object_type == "view"

    # Even though they have view access, they don't have explore access so still can't run this query
    connection.project.set_user({"department": "sales"})
    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["new_vs_repeat"])

    connection.project.set_user({"department": "marketing"})

    connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["product_name"])

    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.get_sql_query(metrics=["total_item_revenue"], dimensions=["new_vs_repeat"])

    assert exc_info.value
    assert exc_info.value.object_name == "new_vs_repeat"
    assert exc_info.value.object_type == "field"


def test_access_grants_field_permission_block(connection):
    connection.project.set_user({"department": "executive"})

    connection.get_sql_query(metrics=["total_revenue"], dimensions=["new_vs_repeat"])
    connection.get_sql_query(metrics=["total_revenue"], dimensions=["product_name"])
    connection.get_sql_query(metrics=["total_revenue"], dimensions=["customers.gender"])

    connection.project.set_user({"department": "engineering"})
    with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
        connection.query(metrics=["total_revenue"], dimensions=["product_name"])

    assert exc_info.value
    assert exc_info.value.object_name == "total_revenue"
    assert exc_info.value.object_type == "field"


@pytest.mark.query
def test_access_filters_on_view(connection):
    connection.project.set_user({"products": "Airplant Holder"})
    view = connection.get_view("submitted_form")
    result = view.secure_from_statement(query_type="SNOWFLAKE")
    correct = (
        "(select * from analytics.submitted_form as submitted_form WHERE submitted_form.id='Airplant Holder')"
        " as submitted_form"
    )
    assert result == correct

    connection.project.set_user({"products": "Airplant Holder, Cactus, Succulent"})
    view = connection.get_view("submitted_form")
    result = view.secure_from_statement(query_type="SNOWFLAKE")
    correct = (
        "(select * from analytics.submitted_form as submitted_form WHERE submitted_form.id IN ('Airplant"
        " Holder','Cactus','Succulent')) as submitted_form"
    )
    assert result == correct

    connection.project.set_user({})
    view = connection.get_view("submitted_form")
    result = view.secure_from_statement(query_type="SNOWFLAKE")
    correct = "analytics.submitted_form as submitted_form"
    assert result == correct


@pytest.mark.query
def test_access_filters_on_view_with_always_filter_and_access_filters(connection):
    connection.project.set_user({"os_ownership": "iOS"})
    view = connection.get_view("query_in_workspace")
    result = view.secure_from_statement(query_type="SNOWFLAKE")
    correct = (
        "(select * from analytics.query_in_workspace as query_in_workspace WHERE"
        " query_in_workspace.context_os='iOS' and NOT query_in_workspace.context_os IS NULL and"
        " query_in_workspace.context_os IN ('1','Google','os:iOS') and query_in_workspace.session_id NOT IN"
        " (1,44,87)) as query_in_workspace"
    )
    assert result == correct

    connection.project.set_user({"os_ownership": "Android", "products": "1123, 4434"})
    view = connection.get_view("query_in_workspace")
    result = view.secure_from_statement(query_type="SNOWFLAKE")
    correct = (
        "(select * from analytics.query_in_workspace as query_in_workspace WHERE"
        " query_in_workspace.context_os='Android' and query_in_workspace.customer_id IN ('1123','4434') and"
        " NOT query_in_workspace.context_os IS NULL and query_in_workspace.context_os IN"
        " ('1','Google','os:iOS') and query_in_workspace.session_id NOT IN (1,44,87)) as query_in_workspace"
    )
    assert result == correct

    connection.project.set_user({})
    view = connection.get_view("query_in_workspace")
    result = view.secure_from_statement(query_type="SNOWFLAKE")
    correct = (
        "(select * from analytics.query_in_workspace as query_in_workspace WHERE NOT"
        " query_in_workspace.context_os IS NULL and query_in_workspace.context_os IN ('1','Google','os:iOS')"
        " and query_in_workspace.session_id NOT IN (1,44,87)) as query_in_workspace"
    )
    assert result == correct


@pytest.mark.query
def test_access_filters_on_view_with_always_filter_and_access_filters_error(connection):
    connection.project.set_user({"os_ownership": "iOS"})
    view = connection.get_view("created_workspace")
    with pytest.raises(QueryError) as exc_info:
        view.secure_from_statement(query_type="SNOWFLAKE")
    error_message = (
        "Always filter field customers.is_churned in the view created_workspace is not supported in"
        " exploratory mode because it is in a different view. Please use a derived table to apply always"
        " filter logic across multiple views."
    )
    assert exc_info.value
    assert error_message in str(exc_info.value)


@pytest.mark.query
def test_access_filters_on_view_derived_table(connection):
    connection.project.set_user({"employee_region": "US-West", "owned_region": "Europe"})
    view = connection.get_view("country_detail")
    result = view.secure_from_statement(query_type="SNOWFLAKE")
    correct = (
        "(select * from (SELECT * FROM ANALYTICS.COUNTRY_DETAIL WHERE 'Europe' = COUNTRY_DETAIL.REGION) as"
        " country_detail WHERE country_detail.country='US-West') as country_detail"
    )
    assert result == correct

    connection.project.set_user({"owned_region": "Europe"})
    view = connection.get_view("country_detail")
    result = view.secure_from_statement(query_type="SNOWFLAKE")
    correct = (
        "(SELECT * FROM ANALYTICS.COUNTRY_DETAIL WHERE 'Europe' = COUNTRY_DETAIL.REGION) as country_detail"
    )
    assert result == correct

    connection.project.set_user({})
    view = connection.get_view("country_detail")
    result = view.secure_from_statement(query_type="SNOWFLAKE")
    correct = (
        "(SELECT * FROM ANALYTICS.COUNTRY_DETAIL WHERE '{{ user_attributes['owned_region'] }}' ="
        " COUNTRY_DETAIL.REGION) as country_detail"
    )
    assert result == correct


@pytest.mark.query
def test_access_filters_on_view_no_filters(connection):
    view = connection.get_view("order_lines")
    result = view.secure_from_statement(query_type="SNOWFLAKE")
    correct = "analytics.order_line_items as order_lines"
    assert result == correct


@pytest.mark.query
def test_access_filters_on_view_raises_error(connection):
    connection.project.set_user({"department": "executive", "owned_region": "US-West"})

    view = connection.get_view("orders")
    with pytest.raises(QueryError) as exc_info:
        view.secure_from_statement(query_type="SNOWFLAKE")

    error_message = (
        "Access filter with field customers.region in the view orders is not supported in exploratory mode"
        " because the field is in a different view. Please use a derived table to join the views needed to"
        " apply access filter logic across multiple views"
    )
    assert exc_info.value
    assert error_message in str(exc_info.value)


@pytest.mark.query
def test_access_filters_equal_to(connection):
    connection.project.set_user({"department": "executive"})

    query = connection.get_sql_query(metrics=["total_revenue"], dimensions=["new_vs_repeat"])

    assert "region" not in query

    connection.project.set_user({"department": "executive", "owned_region": "US-West"})

    query = connection.get_sql_query(metrics=["total_revenue"], dimensions=["new_vs_repeat"])

    correct = (
        "SELECT orders.new_vs_repeat as orders_new_vs_repeat,SUM(orders.revenue) as orders_total_revenue "
        "FROM analytics.orders orders LEFT JOIN analytics.customers customers "
        "ON orders.customer_id=customers.customer_id WHERE customers.region='US-West' "
        "GROUP BY orders.new_vs_repeat ORDER BY orders_total_revenue DESC NULLS LAST;"
    )
    connection.project.set_user({})
    assert correct == query


@pytest.mark.query
def test_access_filters_array(connection):
    connection.project.set_user({"department": "executive", "owned_region": "US-West, US-East"})

    query = connection.get_sql_query(metrics=["total_revenue"], dimensions=["new_vs_repeat"])

    correct = (
        "SELECT orders.new_vs_repeat as orders_new_vs_repeat,SUM(orders.revenue) as orders_total_revenue "
        "FROM analytics.orders orders LEFT JOIN analytics.customers customers "
        "ON orders.customer_id=customers.customer_id WHERE customers.region IN ('US-West','US-East') "
        "GROUP BY orders.new_vs_repeat ORDER BY orders_total_revenue DESC NULLS LAST;"
    )
    connection.project.set_user({})
    assert correct == query


@pytest.mark.query
def test_access_filters_underscore(connection):
    connection.project.set_user({"warehouse_location": "New Jersey"})

    query = connection.get_sql_query(metrics=["total_revenue"], dimensions=["new_vs_repeat"])

    correct = (
        "SELECT orders.new_vs_repeat as orders_new_vs_repeat,SUM(orders.revenue) as orders_total_revenue "
        "FROM analytics.orders orders WHERE orders.warehouselocation='New Jersey' "
        "GROUP BY orders.new_vs_repeat ORDER BY orders_total_revenue DESC NULLS LAST;"
    )
    assert correct == query
