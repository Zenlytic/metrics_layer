import pytest


@pytest.mark.mm
def test_dashboard_located(connection):
    dash = connection.get_dashboard("sales_dashboard")

    assert dash is not None
    assert dash.name == "sales_dashboard"
    assert dash.label == "Sales Dashboard (with campaigns)"
    assert dash.layout == "grid"
    assert isinstance(dash.elements(), list)

    first_element = dash.elements()[0]
    assert first_element.title == "First element"
    assert first_element.type == "plot"
    assert first_element.model == "demo"
    assert first_element.explore == "orders"
    assert first_element.metric == "orders.total_revenue"
    assert first_element.slice_by == ["orders.new_vs_repeat", "orders.product"]
