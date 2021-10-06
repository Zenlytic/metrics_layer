from granite.core.query import define


class config_mock:
    pass


def test_define_call(project):
    config_mock.project = project
    metric_definition = define(metric="total_item_revenue", config=config_mock)
    assert metric_definition == "SUM(order_lines.revenue)"

    metric_definition = define(metric="number_of_email_purchased_items", config=config_mock)
    correct = "COUNT(case when order_lines.sales_channel = 'Email' then order_lines.order_id end)"
    assert metric_definition == correct
