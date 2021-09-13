# import pytest

from granite.core.model.project import Project
from granite.core.sql.resolve import SQLResolverByQuery

simple_model = {
    "type": "model",
    "name": "core",
    "connection": "fake",
    "explores": [{"name": "simple_explore", "from": "simple"}],
}

simple_view = {
    "type": "view",
    "name": "simple",
    "sql_table_name": "analytics.orders",
    "fields": [
        {
            "field_type": "measure",
            "type": "number",
            "sql": "CASE WHEN ${average_order_value} = 0 THEN 0 ELSE ${total_revenue} / ${average_order_value} END",  # noqa
            "name": "revenue_per_aov",
        },
        {"field_type": "measure", "type": "sum", "sql": "${TABLE}.revenue", "name": "total_revenue"},
        {
            "field_type": "measure",
            "type": "average",
            "sql": "${TABLE}.revenue",
            "name": "average_order_value",
        },
        {"field_type": "dimension", "type": "string", "sql": "${TABLE}.sales_channel", "name": "channel"},
        {
            "field_type": "dimension",
            "type": "string",
            "sql": "${TABLE}.new_vs_repeat",
            "name": "new_vs_repeat",
        },
        {
            "field_type": "dimension",
            "type": "yesno",
            "sql": "CASE WHEN ${channel} != 'fraud' THEN TRUE ELSE FALSE END",
            "name": "is_valid_order",
        },
    ],
}


def test_simple_query():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(metrics=["total_revenue"], dimensions=["channel"], project=project)
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_two_group_by():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"], dimensions=["channel", "new_vs_repeat"], project=project
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,simple.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel,simple.new_vs_repeat;"
    assert query == correct


def test_simple_query_two_metric():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(
        metrics=["total_revenue", "average_order_value"],
        dimensions=["channel", "new_vs_repeat"],
        project=project,
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,simple.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(simple.revenue) as total_revenue,AVG(simple.revenue) as average_order_value FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel,simple.new_vs_repeat;"
    assert query == correct


def test_simple_query_custom_dimension():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(metrics=["total_revenue"], dimensions=["is_valid_order"], project=project)
    query = resolver.get_query()

    correct = "SELECT CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END as is_valid_order,"
    correct += "SUM(simple.revenue) as total_revenue FROM analytics.orders simple"
    correct += " GROUP BY CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END;"
    assert query == correct


def test_simple_query_custom_metric():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(metrics=["revenue_per_aov"], dimensions=["channel"], project=project)
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,CASE WHEN AVG(simple.revenue) = 0 THEN 0 ELSE SUM(simple.revenue) / AVG(simple.revenue) END as revenue_per_aov FROM "  # noqa
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_where_dict():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field_name": "channel", "expression": "not_equal", "value": "Email"}],
        project=project,
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel != 'Email' GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_where_literal():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"], dimensions=["channel"], where="channel != 'Email'", project=project
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel != 'Email' GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_having_dict():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"],
        dimensions=["channel"],
        having=[{"field_name": "total_revenue", "expression": "greater_than", "value": 12}],
        project=project,
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel HAVING total_revenue > 12;"
    assert query == correct


def test_simple_query_with_having_literal():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"], dimensions=["channel"], having="total_revenue > 12", project=project
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel HAVING total_revenue > 12;"
    assert query == correct


def test_simple_query_with_order_by_dict():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"],
        dimensions=["channel"],
        order_by=[{"field_name": "total_revenue", "sort": "asc"}],
        project=project,
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY total_revenue asc;"
    assert query == correct


def test_simple_query_with_order_by_literal():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"], dimensions=["channel"], order_by="total_revenue asc", project=project
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY total_revenue asc;"
    assert query == correct


def test_simple_query_with_all():
    project = Project(models=[simple_model], views=[simple_view])
    resolver = SQLResolverByQuery(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field_name": "channel", "expression": "not_equal", "value": "Email"}],
        having=[{"field_name": "total_revenue", "expression": "greater_than", "value": 12}],
        order_by=[{"field_name": "total_revenue", "sort": "asc"}],
        project=project,
    )
    query = resolver.get_query()

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel != 'Email' "
    correct += "GROUP BY simple.sales_channel HAVING total_revenue > 12 ORDER BY total_revenue asc;"
    assert query == correct
