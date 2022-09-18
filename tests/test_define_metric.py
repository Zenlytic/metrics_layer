import pytest


@pytest.mark.project
def test_define_call(connection):
    metric_definition = connection.define(metric="total_item_revenue")
    assert metric_definition == "SUM(order_lines.revenue)"

    metric_definition = connection.define(metric="number_of_email_purchased_items", query_type="SNOWFLAKE")
    correct = "COUNT(case when order_lines.sales_channel='Email' then order_lines.order_id end)"
    assert metric_definition == correct
