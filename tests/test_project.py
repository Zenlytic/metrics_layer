import pytest


@pytest.mark.project
def test_null_values_filters_canon_date(connection):
    revenue_field = connection.project.get_field("total_revenue")

    assert revenue_field.filters is None
    assert revenue_field.canon_date == "orders.order"

    orders_field = connection.project.get_field("number_of_orders")

    assert orders_field.filters is None
    assert orders_field.canon_date == "orders.order"
