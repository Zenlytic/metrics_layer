import pytest

from granite.core.model import Definitions, Project
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
            "timeframes": [
                "raw",
                "time",
                "date",
                "week",
                "month",
                "quarter",
                "year",
                "day_of_week",
                "hour_of_day",
            ],
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
    query = get_sql_query(metrics=["total_revenue"], dimensions=["channel"], config=config)

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_single_metric(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(metrics=["total_revenue"], config=config)

    correct = "SELECT SUM(simple.revenue) as total_revenue FROM analytics.orders simple;"
    assert query == correct


def test_simple_query_single_dimension(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(dimensions=["channel"], config=config)

    correct = "SELECT simple.sales_channel as channel FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_count(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(metrics=["count"], dimensions=["channel"], config=config)

    correct = "SELECT simple.sales_channel as channel,COUNT(*) as count FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct


@pytest.mark.parametrize(
    "group,query_type",
    [
        ("time", Definitions.snowflake),
        ("date", Definitions.snowflake),
        ("week", Definitions.snowflake),
        ("month", Definitions.snowflake),
        ("quarter", Definitions.snowflake),
        ("year", Definitions.snowflake),
        ("hour_of_day", Definitions.snowflake),
        ("day_of_week", Definitions.snowflake),
        ("time", Definitions.bigquery),
        ("date", Definitions.bigquery),
        ("week", Definitions.bigquery),
        ("month", Definitions.bigquery),
        ("quarter", Definitions.bigquery),
        ("year", Definitions.bigquery),
        ("hour_of_day", Definitions.bigquery),
        ("day_of_week", Definitions.bigquery),
    ],
)
def test_simple_query_dimension_group(config, group: str, query_type: str):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue"], dimensions=[f"order_{group}"], config=config, query_type=query_type
    )

    if query_type == Definitions.snowflake:
        result_lookup = {
            "time": "CAST(simple.order_date as TIMESTAMP)",
            "date": "DATE_TRUNC('DAY', simple.order_date)",
            "week": "DATE_TRUNC('WEEK', simple.order_date + 1) - 1",
            "month": "DATE_TRUNC('MONTH', simple.order_date)",
            "quarter": "DATE_TRUNC('QUARTER', simple.order_date)",
            "year": "DATE_TRUNC('YEAR', simple.order_date)",
            "hour_of_day": "HOUR(simple.order_date)",
            "day_of_week": "DAYOFWEEK(simple.order_date)",
        }
    else:
        result_lookup = {
            "time": "CAST(simple.order_date as TIMESTAMP)",
            "date": "DATE_TRUNC(simple.order_date, DAY)",
            "week": "DATE_TRUNC(simple.order_date + 1, WEEK) - 1",
            "month": "DATE_TRUNC(simple.order_date, MONTH)",
            "quarter": "DATE_TRUNC(simple.order_date, QUARTER)",
            "year": "DATE_TRUNC(simple.order_date, YEAR)",
            "hour_of_day": f"CAST(simple.order_date AS STRING FORMAT 'HH24')",
            "day_of_week": f"CAST(simple.order_date AS STRING FORMAT 'DAY')",
        }

    date_result = result_lookup[group]

    correct = f"SELECT {date_result} as order_{group},SUM(simple.revenue) as "
    correct += f"total_revenue FROM analytics.orders simple GROUP BY {date_result};"
    assert query == correct


@pytest.mark.parametrize(
    "interval,query_type",
    [
        ("second", Definitions.snowflake),
        ("minute", Definitions.snowflake),
        ("hour", Definitions.snowflake),
        ("day", Definitions.snowflake),
        ("week", Definitions.snowflake),
        ("month", Definitions.snowflake),
        ("quarter", Definitions.snowflake),
        ("year", Definitions.snowflake),
        ("day", Definitions.bigquery),
        ("week", Definitions.bigquery),
        ("month", Definitions.bigquery),
        ("quarter", Definitions.bigquery),
        ("year", Definitions.bigquery),
        ("hour", Definitions.bigquery),  # Should raise error
    ],
)
def test_simple_query_dimension_group_interval(config, interval: str, query_type: str):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project

    raises_error = interval == "hour" and query_type == Definitions.bigquery
    if raises_error:
        with pytest.raises(KeyError) as exc_info:
            get_sql_query(
                metrics=["total_revenue"],
                dimensions=[f"{interval}s_waiting"],
                config=config,
                query_type=query_type,
            )
    else:
        query = get_sql_query(
            metrics=["total_revenue"],
            dimensions=[f"{interval}s_waiting"],
            config=config,
            query_type=query_type,
        )

    if query_type == Definitions.snowflake:
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
    else:
        result_lookup = {
            "day": "DATE_DIFF(simple.order_date, simple.view_date, DAY)",
            "week": "DATE_DIFF(simple.order_date, simple.view_date, ISOWEEK)",
            "month": "DATE_DIFF(simple.order_date, simple.view_date, MONTH)",
            "quarter": "DATE_DIFF(simple.order_date, simple.view_date, QUARTER)",
            "year": "DATE_DIFF(simple.order_date, simple.view_date, ISOYEAR)",
        }

    if raises_error:
        assert exc_info.value
    else:
        interval_result = result_lookup[interval]

        correct = (
            f"SELECT {interval_result} as {interval}s_waiting,SUM(simple.revenue) as total_revenue FROM "
        )
        correct += f"analytics.orders simple GROUP BY {interval_result};"
        assert query == correct


def test_simple_query_two_group_by(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(metrics=["total_revenue"], dimensions=["channel", "new_vs_repeat"], config=config)

    correct = "SELECT simple.sales_channel as channel,simple.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel,simple.new_vs_repeat;"
    assert query == correct


def test_simple_query_two_metric(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue", "average_order_value"],
        dimensions=["channel", "new_vs_repeat"],
        config=config,
    )

    correct = "SELECT simple.sales_channel as channel,simple.new_vs_repeat as new_vs_repeat,"
    correct += "SUM(simple.revenue) as total_revenue,AVG(simple.revenue) as average_order_value FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel,simple.new_vs_repeat;"
    assert query == correct


def test_simple_query_custom_dimension(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(metrics=["total_revenue"], dimensions=["is_valid_order"], config=config)

    correct = "SELECT CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END as is_valid_order,"
    correct += "SUM(simple.revenue) as total_revenue FROM analytics.orders simple"
    correct += " GROUP BY CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END;"
    assert query == correct


def test_simple_query_custom_metric(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(metrics=["revenue_per_aov"], dimensions=["channel"], config=config)

    correct = "SELECT simple.sales_channel as channel,CASE WHEN AVG(simple.revenue) = 0 THEN 0 ELSE SUM(simple.revenue) / AVG(simple.revenue) END as revenue_per_aov FROM "  # noqa
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct


@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.bigquery])
def test_simple_query_with_where_dim_group(config, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field": "order_date", "expression": "greater_than", "value": "2021-08-04"}],
        config=config,
        query_type=query_type,
    )

    if query_type == Definitions.snowflake:
        condition = "DATE_TRUNC('DAY', simple.order_date)>'2021-08-04'"
    else:
        condition = "DATE_TRUNC(simple.order_date, DAY)>'2021-08-04'"

    correct = (
        "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
        f"analytics.orders simple WHERE {condition} GROUP BY simple.sales_channel;"
    )
    assert query == correct


def test_simple_query_with_where_dict(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field": "channel", "expression": "not_equal_to", "value": "Email"}],
        config=config,
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel<>'Email' GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_where_literal(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], where="channel != 'Email'", config=config
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel != 'Email' GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_with_having_dict(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        having=[{"field": "total_revenue", "expression": "greater_than", "value": 12}],
        config=config,
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel HAVING SUM(simple.revenue)>12;"
    assert query == correct


def test_simple_query_with_having_literal(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], having="total_revenue > 12", config=config
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel HAVING SUM(simple.revenue) > 12;"
    assert query == correct


def test_simple_query_with_order_by_dict(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue", "average_order_value"],
        dimensions=["channel"],
        order_by=[{"field": "total_revenue", "sort": "desc"}, {"field": "average_order_value"}],
        config=config,
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue,AVG(simple.revenue) as average_order_value FROM "  # noqa
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY total_revenue DESC,average_order_value ASC;"  # noqa
    assert query == correct


def test_simple_query_with_order_by_literal(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], order_by="total_revenue asc", config=config
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY total_revenue ASC;"
    assert query == correct


def test_simple_query_with_all(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field": "channel", "expression": "not_equal_to", "value": "Email"}],
        having=[{"field": "total_revenue", "expression": "greater_than", "value": 12}],
        order_by=[{"field": "total_revenue", "sort": "asc"}],
        config=config,
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.sales_channel<>'Email' "
    correct += "GROUP BY simple.sales_channel HAVING SUM(simple.revenue)>12 ORDER BY total_revenue ASC;"
    assert query == correct


def test_simple_query_sql_always_where(config):
    modified_explore = {**simple_model["explores"][0], "sql_always_where": "${new_vs_repeat} = 'Repeat'"}

    project = Project(models=[{**simple_model, "explores": [modified_explore]}], views=[simple_view])
    config.project = project
    query = get_sql_query(metrics=["total_revenue"], dimensions=["channel"], config=config)

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple WHERE simple.new_vs_repeat = 'Repeat' GROUP BY simple.sales_channel;"
    assert query == correct


def test_simple_query_invalid_sql_always_where(config):
    # Looker conditional example
    modified_explore = {
        **simple_model["explores"][0],
        "sql_always_where": "{% if order_line.current_date_range._is_filtered %} ${new_vs_repeat} = 'Repeat'",
    }

    project = Project(models=[{**simple_model, "explores": [modified_explore]}], views=[simple_view])
    config.project = project
    query = get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], config=config, suppress_warnings=True
    )

    correct = "SELECT simple.sales_channel as channel,SUM(simple.revenue) as total_revenue FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel;"
    assert query == correct
