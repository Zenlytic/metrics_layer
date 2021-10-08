from granite.core.query import list_dimensions, list_metrics


class config_mock:
    pass


def test_list_metrics(project):
    config_mock.project = project

    metrics = list_metrics(config=config_mock)
    assert len(metrics) == 14

    metrics = list_metrics(explore_name="order_lines", config=config_mock)
    assert len(metrics) == 14

    metrics = list_metrics(view_name="order_lines", names_only=True, config=config_mock)
    assert len(metrics) == 4
    assert set(metrics) == {
        "number_of_email_purchased_items",
        "total_item_revenue",
        "total_item_costs",
        "line_item_aov",
    }


def test_list_dimensions(project):
    config_mock.project = project
    dimensions = list_dimensions(config=config_mock)
    assert len(dimensions) == 28

    dimensions = list_dimensions(explore_name="order_lines", config=config_mock)
    assert len(dimensions) == 28

    dimensions = list_dimensions(view_name="order_lines", names_only=True, config=config_mock)
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
