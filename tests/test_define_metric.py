from metrics_layer.core import MetricsLayerConnection


def test_define_call(config):
    conn = MetricsLayerConnection(config=config)
    metric_definition = conn.define(metric="total_item_revenue", explore_name="order_lines_all")
    assert metric_definition == "SUM(order_lines.revenue)"

    metric_definition = conn.define(metric="number_of_email_purchased_items", query_type="SNOWFLAKE")
    correct = "COUNT(case when order_lines.sales_channel = 'Email' then order_lines.order_id end)"
    assert metric_definition == correct
