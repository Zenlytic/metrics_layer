from granite import GraniteConnection


def test_list_metrics(config):
    conn = GraniteConnection(config=config)
    metrics = conn.list_metrics()
    assert len(metrics) == 15

    metrics = conn.list_metrics(explore_name="order_lines")
    assert len(metrics) == 15

    metrics = conn.list_metrics(view_name="order_lines", names_only=True)
    assert len(metrics) == 4
    assert set(metrics) == {
        "number_of_email_purchased_items",
        "total_item_revenue",
        "total_item_costs",
        "line_item_aov",
    }


def test_list_dimensions(config):
    conn = GraniteConnection(config=config)
    dimensions = conn.list_dimensions()
    assert len(dimensions) == 31

    dimensions = conn.list_dimensions(explore_name="order_lines")
    assert len(dimensions) == 30

    dimensions = conn.list_dimensions(view_name="order_lines", names_only=True)
    dimensions_present = {
        "order_line_id",
        "order_id",
        "customer_id",
        "order",
        "waiting",
        "channel",
        "product_name",
        "is_on_sale_sql",
        "is_on_sale_case",
        "order_tier",
    }
    assert len(dimensions) == 10
    assert set(dimensions) == dimensions_present
