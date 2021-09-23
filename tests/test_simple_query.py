import pytest

from granite.core.model.project import Project
from granite.core.query import get_sql_query

simple_model = {
    "type": "model",
    "name": "core",
    "connection": "fake",
    "week_start_day": "sunday",
    "explores": [{"name": "simple_explore", "from": "simple"}],
}

simple_view = {
    "type": "view",
    "name": "simple",
    "sql_table_name": "analytics.orders",
    "fields": [
        {"field_type": "measure", "type": "count", "name": "count"},
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
            "field_type": "dimension_group",
            "type": "time",
            "sql": "${TABLE}.order_date",
            "timeframes": ["raw", "time", "date", "week", "month", "quarter", "year"],
            "name": "order",
        },
        {
            "field_type": "dimension_group",
            "type": "duration",
            "sql_start": "${TABLE}.view_date",
            "sql_end": "${TABLE}.order_date",
            "intervals": ["second", "minute", "hour", "day", "week", "month", "quarter", "year"],
            "name": "waiting",
        },
        {
            "field_type": "dimension",
            "type": "yesno",
            "sql": "CASE WHEN ${channel} != 'fraud' THEN TRUE ELSE FALSE END",
            "name": "is_valid_order",
        },
    ],
}


class config_mock:
    pass


def test_simple_query():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(metrics=["total_revenue"], dimensions=["channel"], config=config_mock)

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_count():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(metrics=["count"], dimensions=["channel"], config=config_mock)

    correct = "SELECT simple.sales_channel as channel,COUNT(*) as count FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct


@pytest.mark.parametrize("group", ["time", "date", "week", "month", "quarter", "year"])
def test_simple_query_dimension_group(group: str):
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(metrics=["total_revenue"], dimensions=[f"order_{group}"], config=config_mock)

    result_lookup = {
        "time": "CAST(simple.order_date as TIMESTAMP)",
        "date": "DATE_TRUNC('DAY', simple.order_date)",
        "week": "DATE_TRUNC('WEEK', simple.order_date + 1) - 1",
        "month": "DATE_TRUNC('MONTH', simple.order_date)",
        "quarter": "DATE_TRUNC('QUARTER', simple.order_date)",
        "year": "DATE_TRUNC('YEAR', simple.order_date)",
    }
    date_result = result_lookup[group]

    correct = f"SELECT {date_result} as order_{group},SUM(simple.revenue) as "
    correct += f"total_revenue FROM analytics.orders simple GROUP BY {date_result};"
    assert query == correct


@pytest.mark.parametrize("interval", ["second", "minute", "hour", "day", "week", "month", "quarter", "year"])
def test_simple_query_dimension_group_interval(interval: str):
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(metrics=["total_revenue"], dimensions=[f"{interval}s_waiting"], config=config_mock)

    result_lookup = {
        "second": "DATEDIFF('SECOND', simple.view_date, simple.order_date)",
        "minute": "DATEDIFF('MINUTE', simple.view_date, simple.order_date)",
        "hour": "DATEDIFF('HOUR', simple.view_date, simple.order_date)",
        "day": "DATEDIFF('DAY', simple.view_date, simple.order_date)",
        "week": "DATEDIFF('WEEK', simple.view_date, simple.order_date)",
        "month": "DATEDIFF('MONTH', simple.view_date, simple.order_date)",
        "quarter": "DATEDIFF('QUARTER', simple.view_date, simple.order_date)",
        "year": "DATEDIFF('YEAR', simple.view_date, simple.order_date)",
    }
    interval_result = result_lookup[interval]

    correct = f"SELECT {interval_result} as {interval}s_waiting,SUM(simple.revenue) as total_revenue FROM "
    correct += f"analytics.orders simple GROUP BY {interval_result};"
    assert query == correct


def test_simple_query_two_group_by():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_revenue"], dimensions=["channel", "new_vs_repeat"], config=config_mock
    )

    correct = "SELECT simple.sales_channel as channel,simple.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel,simple.new_vs_repeat;"
    assert query == correct


def test_simple_query_two_metric():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_revenue", "average_order_value"],
        dimensions=["channel", "new_vs_repeat"],
        config=config_mock,
    )

    correct = "SELECT simple.sales_channel as channel,simple.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(simple.revenue) as total_revenue,AVG(simple.revenue) as average_order_value FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel,simple.new_vs_repeat;"
    assert query == correct


def test_simple_query_custom_dimension():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(metrics=["total_revenue"], dimensions=["is_valid_order"], config=config_mock)

    correct = "SELECT CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END as is_valid_order,"
    correct += "SUM(simple.revenue) as total_revenue FROM analytics.orders simple"
    correct += " GROUP BY CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END;"
    assert query == correct


def test_simple_query_custom_metric():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(metrics=["revenue_per_aov"], dimensions=["channel"], config=config_mock)

    correct = "SELECT simple.sales_channel as channel,CASE WHEN AVG(simple.revenue) = 0 THEN 0 ELSE SUM(simple.revenue) / AVG(simple.revenue) END as revenue_per_aov FROM "  # noqa
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_where_dict():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field": "channel", "expression": "not_equal_to", "value": "Email"}],
        config=config_mock,
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel<>'Email' GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_where_literal():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], where="channel != 'Email'", config=config_mock
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel != 'Email' GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_having_dict():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        having=[{"field": "total_revenue", "expression": "greater_than", "value": 12}],
        config=config_mock,
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel HAVING SUM(simple.revenue)>12;"
    assert query == correct


def test_simple_query_with_having_literal():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], having="total_revenue > 12", config=config_mock
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel HAVING SUM(simple.revenue) > 12;"
    assert query == correct


def test_simple_query_with_order_by_dict():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_revenue", "average_order_value"],
        dimensions=["channel"],
        order_by=[{"field": "total_revenue", "sort": "desc"}, {"field": "average_order_value"}],
        config=config_mock,
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue,AVG(simple.revenue) as average_order_value FROM "  # noqa
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY total_revenue DESC,average_order_value ASC;"  # noqa
    assert query == correct


def test_simple_query_with_order_by_literal():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], order_by="total_revenue asc", config=config_mock
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY total_revenue ASC;"
    assert query == correct


def test_simple_query_with_all():
    project = Project(models=[simple_model], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field": "channel", "expression": "not_equal_to", "value": "Email"}],
        having=[{"field": "total_revenue", "expression": "greater_than", "value": 12}],
        order_by=[{"field": "total_revenue", "sort": "asc"}],
        config=config_mock,
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel<>'Email' "
    correct += "GROUP BY simple.sales_channel HAVING SUM(simple.revenue)>12 ORDER BY total_revenue ASC;"
    assert query == correct


def test_simple_query_sql_always_where():
    modified_explore = {**simple_model["explores"][0], "sql_always_where": "${new_vs_repeat} = 'Repeat'"}

    project = Project(models=[{**simple_model, "explores": [modified_explore]}], views=[simple_view])
    config_mock.project = project
    query = get_sql_query(metrics=["total_revenue"], dimensions=["channel"], config=config_mock)

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.new_vs_repeat = 'Repeat' GROUP BY simple.sales_channel;"
    assert query == correct
