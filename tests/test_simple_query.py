from datetime import datetime

import pendulum
import pytest

from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException
from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.model import Definitions, Project

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
        {"field_type": "dimension", "sql": "${TABLE}.group_name", "name": "group"},
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
                "day_of_month",
                "hour_of_day",
            ],
            "label": "Order Created",
            "name": "order",
        },
        {
            "field_type": "dimension_group",
            "type": "time",
            "datatype": "datetime",
            "sql": "${TABLE}.previous_order_date",
            "timeframes": [
                "raw",
                "time",
                "date",
                "week",
                "month",
                "quarter",
                "year",
            ],
            "name": "previous_order",
        },
        {
            "field_type": "dimension_group",
            "type": "time",
            "datatype": "date",
            "sql": "${TABLE}.first_order_date",
            "timeframes": [
                "raw",
                "time",
                "date",
                "week",
                "month",
                "quarter",
                "year",
            ],
            "name": "first_order",
        },
        {
            "field_type": "dimension_group",
            "type": "duration",
            "sql_start": "${TABLE}.view_date",
            "sql_end": "${TABLE}.order_date",
            "intervals": ["second", "minute", "hour", "day", "week", "month", "quarter", "year"],
            "name": "waiting",
            "label": "Between view and order",
        },
        {
            "field_type": "dimension",
            "type": "yesno",
            "sql": "CASE WHEN ${channel} != 'fraud' THEN TRUE ELSE FALSE END",
            "name": "is_valid_order",
        },
    ],
}


@pytest.mark.query
def test_simple_query(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["channel"])

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
    )
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY simple_total_revenue DESC;"
    assert query == correct


@pytest.mark.query
def test_simple_query_single_metric(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(metrics=["total_revenue"])

    correct = (
        "SELECT SUM(simple.revenue) as simple_total_revenue "
        "FROM analytics.orders simple ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_single_dimension(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(dimensions=["channel"])

    correct = (
        "SELECT simple.sales_channel as simple_channel FROM analytics.orders simple "
        "GROUP BY simple.sales_channel ORDER BY simple_channel ASC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_count(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(metrics=["count"], dimensions=["channel"])

    correct = "SELECT simple.sales_channel as simple_channel,COUNT(*) as simple_count FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY simple_count DESC;"
    assert query == correct


@pytest.mark.query
def test_simple_query_alias_keyword(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(metrics=["count"], dimensions=["group"])

    correct = "SELECT simple.group_name as simple_group,COUNT(*) as simple_count FROM "
    correct += "analytics.orders simple GROUP BY simple.group_name ORDER BY simple_count DESC;"
    assert query == correct


@pytest.mark.parametrize(
    "group,query_type",
    [
        ("date", Definitions.snowflake),
        ("week", Definitions.snowflake),
        ("date", Definitions.redshift),
        ("week", Definitions.redshift),
        ("date", Definitions.bigquery),
        ("week", Definitions.bigquery),
    ],
)
@pytest.mark.query
def test_simple_query_dimension_group_timezone(config, group: str, query_type: str):
    project = Project(models=[simple_model], views=[simple_view])
    project.set_timezone("America/New_York")
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(
        metrics=["total_revenue"], dimensions=[f"order_{group}"], query_type=query_type
    )

    if query_type in {Definitions.snowflake, Definitions.redshift}:
        result_lookup = {
            "date": "DATE_TRUNC('DAY', CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS TIMESTAMP_NTZ))",  # noqa
            "week": "DATE_TRUNC('WEEK', CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS TIMESTAMP_NTZ) AS DATE) + 1) - 1",  # noqa
        }
        order_by = " ORDER BY simple_total_revenue DESC"
    else:
        result_lookup = {
            "date": "CAST(DATE_TRUNC(CAST(DATETIME(CAST(simple.order_date AS DATETIME), 'America/New_York') AS DATE), DAY) AS TIMESTAMP)",  # noqa
            "week": "CAST(DATE_TRUNC(CAST(DATETIME(CAST(simple.order_date AS DATETIME), 'America/New_York') AS DATE) + 1, WEEK) - 1 AS TIMESTAMP)",  # noqa
        }
        order_by = ""

    date_result = result_lookup[group]

    correct = (
        f"SELECT {date_result} as simple_order_{group},SUM(simple.revenue) as "
        f"simple_total_revenue FROM analytics.orders simple GROUP BY {date_result}"
        f"{order_by};"
    )
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
        ("day_of_month", Definitions.snowflake),
        ("time", Definitions.redshift),
        ("date", Definitions.redshift),
        ("week", Definitions.redshift),
        ("month", Definitions.redshift),
        ("quarter", Definitions.redshift),
        ("year", Definitions.redshift),
        ("hour_of_day", Definitions.redshift),
        ("day_of_week", Definitions.redshift),
        ("time", Definitions.bigquery),
        ("date", Definitions.bigquery),
        ("week", Definitions.bigquery),
        ("month", Definitions.bigquery),
        ("quarter", Definitions.bigquery),
        ("year", Definitions.bigquery),
        ("hour_of_day", Definitions.bigquery),
        ("day_of_week", Definitions.bigquery),
        ("day_of_month", Definitions.bigquery),
    ],
)
@pytest.mark.query
def test_simple_query_dimension_group(config, group: str, query_type: str):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(
        metrics=["total_revenue"], dimensions=[f"order_{group}"], query_type=query_type
    )
    field = project.get_field(f"order_{group}")

    if query_type in {Definitions.snowflake, Definitions.redshift}:
        result_lookup = {
            "time": "CAST(simple.order_date AS TIMESTAMP)",
            "date": "DATE_TRUNC('DAY', simple.order_date)",
            "week": "DATE_TRUNC('WEEK', CAST(simple.order_date AS DATE) + 1) - 1",
            "month": "DATE_TRUNC('MONTH', simple.order_date)",
            "quarter": "DATE_TRUNC('QUARTER', simple.order_date)",
            "year": "DATE_TRUNC('YEAR', simple.order_date)",
            "hour_of_day": "HOUR(simple.order_date)",
            "day_of_week": "DAYOFWEEK(simple.order_date)",
            "day_of_month": "DAYOFMONTH(simple.order_date)",
        }
        order_by = " ORDER BY simple_total_revenue DESC"
    else:
        result_lookup = {
            "time": "CAST(simple.order_date AS TIMESTAMP)",
            "date": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), DAY) AS TIMESTAMP)",
            "week": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE) + 1, WEEK) - 1 AS TIMESTAMP)",
            "month": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), MONTH) AS TIMESTAMP)",
            "quarter": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), QUARTER) AS TIMESTAMP)",
            "year": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), YEAR) AS TIMESTAMP)",
            "hour_of_day": f"CAST(simple.order_date AS STRING FORMAT 'HH24')",
            "day_of_week": f"CAST(simple.order_date AS STRING FORMAT 'DAY')",
            "day_of_month": "EXTRACT(DAY FROM simple.order_date)",
        }
        order_by = ""

    date_result = result_lookup[group]

    correct = (
        f"SELECT {date_result} as simple_order_{group},SUM(simple.revenue) as "
        f"simple_total_revenue FROM analytics.orders simple GROUP BY {date_result}"
        f"{order_by};"
    )
    assert query == correct

    correct_label = f"Order Created {group.replace('_', ' ').title()}"
    assert field.label == correct_label


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
        ("second", Definitions.redshift),
        ("minute", Definitions.redshift),
        ("hour", Definitions.redshift),
        ("day", Definitions.redshift),
        ("week", Definitions.redshift),
        ("month", Definitions.redshift),
        ("quarter", Definitions.redshift),
        ("year", Definitions.redshift),
        ("day", Definitions.bigquery),
        ("week", Definitions.bigquery),
        ("month", Definitions.bigquery),
        ("quarter", Definitions.bigquery),
        ("year", Definitions.bigquery),
        ("hour", Definitions.bigquery),
        ("minute", Definitions.bigquery),
        ("second", Definitions.bigquery),
        ("millisecond", Definitions.bigquery),  # Should raise error
    ],
)
@pytest.mark.query
def test_simple_query_dimension_group_interval(config, interval: str, query_type: str):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    raises_error = interval == "millisecond" and query_type == Definitions.bigquery
    if raises_error:
        with pytest.raises(AccessDeniedOrDoesNotExistException) as exc_info:
            conn.get_sql_query(
                metrics=["total_revenue"],
                dimensions=[f"{interval}s_waiting"],
                query_type=query_type,
            )
    else:
        query = conn.get_sql_query(
            metrics=["total_revenue"],
            dimensions=[f"{interval}s_waiting"],
            query_type=query_type,
        )
        field = project.get_field(f"{interval}s_waiting")

    if query_type in {Definitions.snowflake, Definitions.redshift}:
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
        order_by = " ORDER BY simple_total_revenue DESC"
    else:
        result_lookup = {
            "second": "TIMESTAMP_DIFF(CAST(simple.order_date as TIMESTAMP), CAST(simple.view_date as TIMESTAMP), SECOND)",  # noqa
            "minute": "TIMESTAMP_DIFF(CAST(simple.order_date as TIMESTAMP), CAST(simple.view_date as TIMESTAMP), MINUTE)",  # noqa
            "hour": "TIMESTAMP_DIFF(CAST(simple.order_date as TIMESTAMP), CAST(simple.view_date as TIMESTAMP), HOUR)",  # noqa
            "day": "DATE_DIFF(CAST(simple.order_date as DATE), CAST(simple.view_date as DATE), DAY)",
            "week": "DATE_DIFF(CAST(simple.order_date as DATE), CAST(simple.view_date as DATE), ISOWEEK)",
            "month": "DATE_DIFF(CAST(simple.order_date as DATE), CAST(simple.view_date as DATE), MONTH)",
            "quarter": "DATE_DIFF(CAST(simple.order_date as DATE), CAST(simple.view_date as DATE), QUARTER)",
            "year": "DATE_DIFF(CAST(simple.order_date as DATE), CAST(simple.view_date as DATE), ISOYEAR)",
        }
        order_by = ""
    if raises_error:
        assert exc_info.value
    else:
        interval_result = result_lookup[interval]

        correct = (
            f"SELECT {interval_result} as simple_{interval}s_waiting,"
            "SUM(simple.revenue) as simple_total_revenue FROM "
            f"analytics.orders simple GROUP BY {interval_result}{order_by};"
        )
        assert query == correct

        correct_label = f"{interval.replace('_', ' ').title()}s Between view and order"
        assert field.label == correct_label


@pytest.mark.query
def test_simple_query_two_group_by(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["channel", "new_vs_repeat"])

    correct = (
        "SELECT simple.sales_channel as simple_channel,simple.new_vs_repeat as simple_new_vs_repeat,"
        "SUM(simple.revenue) as simple_total_revenue FROM analytics.orders simple "
        "GROUP BY simple.sales_channel,simple.new_vs_repeat ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_two_metric(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(
        metrics=["total_revenue", "average_order_value"],
        dimensions=["channel", "new_vs_repeat"],
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,simple.new_vs_repeat as simple_new_vs_repeat,"
        "SUM(simple.revenue) as simple_total_revenue,AVG(simple.revenue) as simple_average_order_value FROM "
        "analytics.orders simple GROUP BY simple.sales_channel,simple.new_vs_repeat "
        "ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_custom_dimension(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["is_valid_order"])

    correct = (
        "SELECT CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END as simple_is_valid_order,"
        "SUM(simple.revenue) as simple_total_revenue FROM analytics.orders simple"
        " GROUP BY CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END "
        "ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_custom_metric(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(metrics=["revenue_per_aov"], dimensions=["channel"])

    correct = (
        "SELECT simple.sales_channel as simple_channel,CASE WHEN (AVG(simple.revenue)) = 0 THEN "
        "0 ELSE (SUM(simple.revenue)) / (AVG(simple.revenue)) END as simple_revenue_per_aov FROM "
        "analytics.orders simple GROUP BY simple.sales_channel ORDER BY simple_revenue_per_aov DESC;"
    )
    assert query == correct


@pytest.mark.parametrize(
    "field,expression,value,query_type",
    [
        ("order_date", "greater_than", "2021-08-04", Definitions.snowflake),
        ("order_date", "greater_than", "2021-08-04", Definitions.redshift),
        ("order_date", "greater_than", "2021-08-04", Definitions.bigquery),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.snowflake),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.redshift),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.bigquery),
        ("previous_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.snowflake),
        ("previous_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.redshift),
        ("previous_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.bigquery),
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.snowflake),
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.redshift),
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.bigquery),
        ("order_date", "matches", "last year", Definitions.snowflake),
        ("order_date", "matches", "last year", Definitions.redshift),
        ("order_date", "matches", "last year", Definitions.bigquery),
    ],
)
@pytest.mark.query
def test_simple_query_with_where_dim_group(config, field, expression, value, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field": field, "expression": expression, "value": value}],
        query_type=query_type,
    )

    if query_type == Definitions.bigquery:
        order_by = ""
    else:
        order_by = " ORDER BY simple_total_revenue DESC"

    sf_or_rs = query_type in {Definitions.snowflake, Definitions.redshift}
    if sf_or_rs and expression == "greater_than" and isinstance(value, str):
        condition = f"DATE_TRUNC('DAY', simple.{field})>'2021-08-04'"
    elif sf_or_rs and isinstance(value, datetime):
        condition = f"DATE_TRUNC('DAY', simple.{field})>'2021-08-04T00:00:00'"
    elif (
        query_type == Definitions.bigquery
        and expression == "greater_than"
        and isinstance(value, str)
        and field == "order_date"
    ):
        condition = "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), DAY) AS TIMESTAMP)>'2021-08-04'"
    elif query_type == Definitions.bigquery and isinstance(value, datetime) and field == "order_date":
        condition = "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), DAY) AS TIMESTAMP)>TIMESTAMP('2021-08-04 00:00:00')"  # noqa
    elif (
        query_type == Definitions.bigquery and isinstance(value, datetime) and field == "previous_order_date"
    ):
        condition = "CAST(DATE_TRUNC(CAST(simple.previous_order_date AS DATE), DAY) AS DATETIME)>DATETIME('2021-08-04 00:00:00')"  # noqa
    elif query_type == Definitions.bigquery and isinstance(value, datetime) and field == "first_order_date":
        condition = "CAST(DATE_TRUNC(CAST(simple.first_order_date AS DATE), DAY) AS DATE)>DATE('2021-08-04 00:00:00')"  # noqa
    elif sf_or_rs and expression == "matches":
        last_year = pendulum.now("UTC").year - 1
        condition = f"DATE_TRUNC('DAY', simple.{field})>='{last_year}-01-01T00:00:00' AND "
        condition += f"DATE_TRUNC('DAY', simple.{field})<='{last_year}-12-31T23:59:59'"
    elif query_type == Definitions.bigquery and expression == "matches":
        last_year = pendulum.now("UTC").year - 1
        condition = f"CAST(DATE_TRUNC(CAST(simple.{field} AS DATE), DAY) AS TIMESTAMP)>=TIMESTAMP('{last_year}-01-01T00:00:00') AND "  # noqa
        condition += f"CAST(DATE_TRUNC(CAST(simple.{field} AS DATE), DAY) AS TIMESTAMP)<=TIMESTAMP('{last_year}-12-31T23:59:59')"  # noqa

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        f"analytics.orders simple WHERE {condition} GROUP BY simple.sales_channel{order_by};"
    )
    assert query == correct


@pytest.mark.parametrize(
    "field_name,filter_type,value",
    [
        ("channel", "equal_to", "Email"),
        ("channel", "not_equal_to", "Email"),
        ("channel", "contains", "Email"),
        ("channel", "does_not_contain", "Email"),
        ("channel", "contains_case_insensitive", "Email"),
        ("channel", "does_not_contain_case_insensitive", "Email"),
        ("channel", "starts_with", "Email"),
        ("channel", "ends_with", "Email"),
        ("channel", "does_not_start_with", "Email"),
        ("channel", "does_not_end_with", "Email"),
        ("channel", "starts_with_case_insensitive", "Email"),
        ("channel", "ends_with_case_insensitive", "Email"),
        ("channel", "does_not_start_with_case_insensitive", "Email"),
        ("channel", "does_not_end_with_case_insensitive", "Email"),
        ("is_valid_order", "is_null", None),
        ("is_valid_order", "is_not_null", None),
        ("is_valid_order", "boolean_true", None),
        ("is_valid_order", "boolean_false", None),
    ],
)
@pytest.mark.query
def test_simple_query_with_where_dict(config, field_name, filter_type, value):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=[f"simple.channel"],
        where=[{"field": field_name, "expression": filter_type, "value": value}],
    )

    result_lookup = {
        "equal_to": f"='{value}'",
        "not_equal_to": f"<>'{value}'",
        "contains": f" LIKE '%{value}%'",
        "does_not_contain": f" NOT LIKE '%{value}%'",
        "contains_case_insensitive": f" ILIKE '%{value}%'",
        "does_not_contain_case_insensitive": f" NOT ILIKE '%{value}%'",
        "starts_with": f" LIKE '{value}%'",
        "ends_with": f" LIKE '%{value}'",
        "does_not_start_with": f" NOT LIKE '{value}%'",
        "does_not_end_with": f" NOT LIKE '%{value}'",
        "starts_with_case_insensitive": f" ILIKE '{value}%'",
        "ends_with_case_insensitive": f" ILIKE '%{value}'",
        "does_not_start_with_case_insensitive": f" NOT ILIKE '{value}%'",
        "does_not_end_with_case_insensitive": f" NOT ILIKE '%{value}'",
        "is_null": " IS NULL",
        "is_not_null": " IS NULL",
        "boolean_true": "",
        "boolean_false": "",
    }
    prefix_filter = {
        "is_not_null": "NOT ",
        "boolean_false": "NOT ",
    }
    dim_lookup = {
        "channel": "simple.sales_channel",
        "is_valid_order": "CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END",
    }
    filter_expr = result_lookup[filter_type]
    prefix_expr = prefix_filter.get(filter_type, "")
    dim_expr = dim_lookup[field_name]
    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        f"analytics.orders simple WHERE {prefix_expr}{dim_expr}{filter_expr} GROUP BY simple.sales_channel "
        "ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_where_literal(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(
        metrics=["total_revenue"], dimensions=["simple.channel"], where="simple.channel != 'Email'"
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        "analytics.orders simple WHERE simple.sales_channel != 'Email' GROUP BY simple.sales_channel "
        "ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.parametrize(
    "filter_type",
    [
        "equal_to",
        "not_equal_to",
        "less_than",
        "less_or_equal_than",
        "greater_or_equal_than",
        "greater_than",
        "is_null",
        "is_not_null",
    ],
)
@pytest.mark.query
def test_simple_query_with_having_dict(config, filter_type):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        having=[{"field": "total_revenue", "expression": filter_type, "value": 12}],
    )

    result_lookup = {
        "equal_to": "=12",
        "not_equal_to": "<>12",
        "less_than": "<12",
        "less_or_equal_than": "<=12",
        "greater_or_equal_than": ">=12",
        "greater_than": ">12",
        "is_null": " IS NULL",
        "is_not_null": " IS NULL",
    }
    filter_expr = result_lookup[filter_type]
    full_expr = f"SUM(simple.revenue){filter_expr}"
    if filter_type == "is_not_null":
        full_expr = "NOT " + full_expr
    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        f"analytics.orders simple GROUP BY simple.sales_channel HAVING {full_expr} "
        "ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_having_literal(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["channel"], having="total_revenue > 12")

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        "analytics.orders simple GROUP BY simple.sales_channel HAVING (SUM(simple.revenue)) > 12 "
        "ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_order_by_dict(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(
        metrics=["total_revenue", "average_order_value"],
        dimensions=["channel"],
        order_by=[{"field": "total_revenue", "sort": "asc"}, {"field": "average_order_value"}],
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue,"
        "AVG(simple.revenue) as simple_average_order_value FROM analytics.orders simple "
        "GROUP BY simple.sales_channel ORDER BY total_revenue ASC,average_order_value ASC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_order_by_literal(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], order_by="total_revenue asc"
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        "analytics.orders simple GROUP BY simple.sales_channel ORDER BY total_revenue ASC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_all(config):
    project = Project(models=[simple_model], views=[simple_view])
    config.project = project
    conn = MetricsLayerConnection(config=config)
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field": "channel", "expression": "not_equal_to", "value": "Email"}],
        having=[{"field": "total_revenue", "expression": "greater_than", "value": 12}],
        order_by=[{"field": "total_revenue", "sort": "asc"}],
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        "analytics.orders simple WHERE simple.sales_channel<>'Email' "
        "GROUP BY simple.sales_channel HAVING SUM(simple.revenue)>12 ORDER BY total_revenue ASC;"
    )
    assert query == correct
