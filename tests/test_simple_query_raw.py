import pytest

from granite import GraniteConnection
from granite.core.model.project import Project
from granite.core.sql.query_errors import ArgumentError

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
        {
            "name": "order_id",
            "field_type": "dimension",
            "type": "string",
            "primary_key": "yes",
            "sql": "${TABLE}.order_id",
        },
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


def test_simple_query(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = GraniteConnection(config=config)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["order_id", "channel"])

    correct = "SELECT simple.order_id as order_id,simple.sales_channel as channel,simple.revenue "
    correct += "as total_revenue FROM analytics.orders simple;"
    assert query == correct


def test_query_complex_metric(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = GraniteConnection(config=config)
    query = conn.get_sql_query(metrics=["revenue_per_aov"], dimensions=["order_id", "channel"])

    correct = "SELECT simple.order_id as order_id,simple.sales_channel as channel,simple.revenue as "
    correct += "average_order_value,simple.revenue as total_revenue FROM analytics.orders simple;"
    assert query == correct


def test_query_complex_metric_having_error(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = GraniteConnection(config=config)
    with pytest.raises(ArgumentError) as exc_info:
        conn.get_sql_query(
            metrics=["revenue_per_aov"],
            dimensions=["order_id", "channel"],
            having=[{"field": "revenue_per_aov", "expression": "greater_than", "value": 13}],
        )

    assert exc_info.value


def test_query_complex_metric_order_by_error(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = GraniteConnection(config=config)
    with pytest.raises(ArgumentError) as exc_info:
        conn.get_sql_query(
            metrics=["revenue_per_aov"],
            dimensions=["order_id", "channel"],
            order_by=[{"field": "revenue_per_aov", "sort": "desc"}],
        )

    assert exc_info.value


def test_query_complex_metric_all(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = GraniteConnection(config=config)
    query = conn.get_sql_query(
        metrics=["revenue_per_aov"],
        dimensions=["order_id", "channel"],
        where=[{"field": "channel", "expression": "not_equal_to", "value": "Email"}],
    )

    correct = "SELECT simple.order_id as order_id,simple.sales_channel as channel,simple.revenue as "
    correct += "average_order_value,simple.revenue as total_revenue FROM analytics.orders simple"
    correct += " WHERE simple.sales_channel<>'Email';"
    assert query == correct
