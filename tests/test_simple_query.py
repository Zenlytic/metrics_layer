from copy import deepcopy
from datetime import datetime

import pendulum
import pytest
from metrics_layer.core.parse.connections import BaseConnection

from metrics_layer.core.exceptions import AccessDeniedOrDoesNotExistException
from metrics_layer.core import MetricsLayerConnection
from metrics_layer.core.model import Definitions, Project

simple_model = {
    "type": "model",
    "name": "core",
    "connection": "testing_snowflake",
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
        {"field_type": "measure", "type": "max", "sql": "${TABLE}.revenue", "name": "max_revenue"},
        {"field_type": "measure", "type": "min", "sql": "${TABLE}.revenue", "name": "min_revenue"},
        {"field_type": "measure", "type": "count_distinct", "sql": "${group}", "name": "unique_groups"},
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
                "second",
                "minute",
                "hour",
                "date",
                "week",
                "month",
                "quarter",
                "year",
                "week_index",
                "week_of_year",
                "week_of_month",
                "month_index",
                "month_of_year",
                "month_of_year_index",
                "month_name",
                "quarter_of_year",
                "day_of_week",
                "day_of_month",
                "day_of_year",
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
            "convert_timezone": False,
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
def test_simple_query_dynamic_schema():
    view = deepcopy(simple_view)
    view["sql_table_name"] = "{{ref('orders')}}"

    simple_model["connection"] = "testing_simple_snowflake"
    project = Project(models=[simple_model], views=[view])

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        "{} simple GROUP BY simple.sales_channel ORDER BY simple_total_revenue DESC;"
    )

    class sf_mock(BaseConnection):
        name = "testing_simple_snowflake"
        type = "SNOWFLAKE"
        database = "analytics"
        schema = "testing"

    sf = sf_mock()

    conn = MetricsLayerConnection(project=project, connections=[sf])
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["channel"])

    table_name = "testing.orders"
    assert query == correct.format(table_name)

    sf.schema = "prod"
    conn = MetricsLayerConnection(project=project, connections=[sf])
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["channel"])

    table_name = "prod.orders"
    assert query == correct.format(table_name)

    simple_model["connection"] = "testing_snowflake"


@pytest.mark.query
def test_simple_query(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["channel"])

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
    )
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY simple_total_revenue DESC;"
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize(
    "metric,query_type",
    [
        ("max_revenue", Definitions.snowflake),
        ("min_revenue", Definitions.snowflake),
        ("max_revenue", Definitions.databricks),
        ("min_revenue", Definitions.databricks),
        ("max_revenue", Definitions.druid),
        ("min_revenue", Definitions.druid),
        ("max_revenue", Definitions.sql_server),
        ("min_revenue", Definitions.sql_server),
        ("max_revenue", Definitions.redshift),
        ("min_revenue", Definitions.redshift),
        ("max_revenue", Definitions.postgres),
        ("min_revenue", Definitions.postgres),
        ("max_revenue", Definitions.bigquery),
        ("min_revenue", Definitions.bigquery),
        ("max_revenue", Definitions.duck_db),
        ("min_revenue", Definitions.duck_db),
    ],
)
def test_simple_query_min_max(connections, metric, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=[metric], dimensions=["channel"], query_type=query_type)

    agg = "MIN" if "min" in metric else "MAX"
    group_by = "simple.sales_channel"
    semi = ";"
    if query_type in {Definitions.snowflake, Definitions.redshift, Definitions.duck_db}:
        order_by = f" ORDER BY simple_{agg.lower()}_revenue DESC"
    else:
        order_by = ""

    if query_type == Definitions.druid:
        semi = ""

    if query_type == Definitions.bigquery:
        group_by = "simple_channel"
    correct = (
        f"SELECT simple.sales_channel as simple_channel,{agg}(simple.revenue) as simple_{agg.lower()}_revenue"
        f" FROM analytics.orders simple GROUP BY {group_by}{order_by}{semi}"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize(
    "query_type",
    [
        (Definitions.snowflake),
        (Definitions.databricks),
        (Definitions.druid),
        (Definitions.sql_server),
        (Definitions.redshift),
        (Definitions.postgres),
        (Definitions.bigquery),
        (Definitions.duck_db),
    ],
)
def test_simple_query_count_distinct(connections, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["unique_groups"], dimensions=["channel"], query_type=query_type)

    group_by = "simple.sales_channel"
    semi = ";"
    if query_type in {Definitions.snowflake, Definitions.redshift, Definitions.duck_db}:
        order_by = f" ORDER BY simple_unique_groups DESC"
    else:
        order_by = ""

    if query_type == Definitions.druid:
        semi = ""

    if query_type == Definitions.bigquery:
        group_by = "simple_channel"
    correct = (
        "SELECT simple.sales_channel as simple_channel,COUNT(DISTINCT(simple.group_name)) "
        "as simple_unique_groups FROM analytics.orders simple "
        f"GROUP BY {group_by}{order_by}{semi}"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_single_metric(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["total_revenue"])

    correct = (
        "SELECT SUM(simple.revenue) as simple_total_revenue "
        "FROM analytics.orders simple ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_single_dimension(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(dimensions=["channel"])

    correct = (
        "SELECT simple.sales_channel as simple_channel FROM analytics.orders simple "
        "GROUP BY simple.sales_channel ORDER BY simple_channel ASC;"
    )
    assert query == correct


@pytest.mark.query
@pytest.mark.parametrize("query_type", [Definitions.snowflake, Definitions.sql_server])
def test_simple_query_limit(connections, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(dimensions=["channel"], limit=10, query_type=query_type)

    if Definitions.snowflake == query_type:
        correct = (
            "SELECT simple.sales_channel as simple_channel FROM analytics.orders simple "
            "GROUP BY simple.sales_channel ORDER BY simple_channel ASC LIMIT 10;"
        )
    elif Definitions.sql_server == query_type:
        correct = (
            "SELECT TOP (10) simple.sales_channel as simple_channel FROM analytics.orders simple "
            "GROUP BY simple.sales_channel;"
        )
    else:
        raise NotImplementedError()
    assert query == correct


@pytest.mark.query
def test_simple_query_count(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["count"], dimensions=["channel"])

    correct = "SELECT simple.sales_channel as simple_channel,COUNT(*) as simple_count FROM "
    correct += "analytics.orders simple GROUP BY simple.sales_channel ORDER BY simple_count DESC;"
    assert query == correct


@pytest.mark.query
def test_simple_query_alias_keyword(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["count"], dimensions=["group"])

    correct = "SELECT simple.group_name as simple_group,COUNT(*) as simple_count FROM "
    correct += "analytics.orders simple GROUP BY simple.group_name ORDER BY simple_count DESC;"
    assert query == correct


@pytest.mark.parametrize(
    "field,group,query_type",
    [
        ("order", "date", Definitions.snowflake),
        ("order", "week", Definitions.snowflake),
        ("previous_order", "date", Definitions.snowflake),
        ("order", "date", Definitions.databricks),
        ("order", "week", Definitions.databricks),
        ("previous_order", "date", Definitions.databricks),
        ("order", "date", Definitions.druid),
        ("order", "week", Definitions.druid),
        ("previous_order", "date", Definitions.druid),
        ("order", "date", Definitions.duck_db),
        ("order", "week", Definitions.duck_db),
        ("previous_order", "date", Definitions.duck_db),
        ("order", "date", Definitions.sql_server),
        ("order", "week", Definitions.sql_server),
        ("previous_order", "date", Definitions.sql_server),
        ("order", "date", Definitions.redshift),
        ("order", "week", Definitions.redshift),
        ("order", "date", Definitions.postgres),
        ("order", "week", Definitions.postgres),
        ("order", "date", Definitions.bigquery),
        ("order", "week", Definitions.bigquery),
    ],
)
@pytest.mark.query
def test_simple_query_dimension_group_timezone(connections, field: str, group: str, query_type: str):
    project = Project(models=[simple_model], views=[simple_view])
    project.set_timezone("America/New_York")
    conn = MetricsLayerConnection(project=project, connections=connections)
    if query_type == Definitions.bigquery:
        where_field = "order_raw"
    else:
        where_field = "order_date"
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=[f"{field}_{group}"],
        where=[{"field": where_field, "expression": "matches", "value": "month to date"}],
        query_type=query_type,
    )

    semi = ";"
    date_format = "%Y-%m-%dT%H:%M:%S"
    start = pendulum.now("America/New_York").start_of("month").strftime(date_format)
    if pendulum.now("America/New_York").day == 1:
        end = pendulum.now("America/New_York").end_of("day").strftime(date_format)
    else:
        end = pendulum.now("America/New_York").end_of("day").subtract(days=1).strftime(date_format)

    if query_type in {Definitions.snowflake, Definitions.redshift}:
        ttype = "TIMESTAMP" if query_type == Definitions.redshift else "TIMESTAMP_NTZ"
        if field == "previous_order":
            result_lookup = {"date": "DATE_TRUNC('DAY', simple.previous_order_date)"}
        else:
            result_lookup = {
                "date": f"DATE_TRUNC('DAY', CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS {ttype}) AS TIMESTAMP))",  # noqa
                "week": f"DATE_TRUNC('WEEK', CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS {ttype}) AS TIMESTAMP) AS DATE) + 1) - 1",  # noqa
            }
        where = (
            "WHERE DATE_TRUNC('DAY', CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) "
            f"AS {ttype}) AS TIMESTAMP))>='{start}' AND DATE_TRUNC('DAY', "
            f"CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS {ttype}) AS TIMESTAMP))<='{end}'"  # noqa
        )
        order_by = " ORDER BY simple_total_revenue DESC"
    elif query_type == Definitions.databricks:
        if field == "previous_order":
            result_lookup = {"date": "DATE_TRUNC('DAY', CAST(simple.previous_order_date AS TIMESTAMP))"}
        else:
            result_lookup = {
                "date": f"DATE_TRUNC('DAY', CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS TIMESTAMP_NTZ) AS TIMESTAMP) AS TIMESTAMP))",  # noqa
                "week": f"DATE_TRUNC('WEEK', CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS TIMESTAMP_NTZ) AS TIMESTAMP) AS TIMESTAMP) + INTERVAL '1' DAY) - INTERVAL '1' DAY",  # noqa
            }
        where = (
            "WHERE DATE_TRUNC('DAY', CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) "
            f"AS TIMESTAMP_NTZ) AS TIMESTAMP) AS TIMESTAMP))>='{start}' AND DATE_TRUNC('DAY', "
            f"CAST(CAST(CAST(CONVERT_TIMEZONE('America/New_York', simple.order_date) AS TIMESTAMP_NTZ) AS TIMESTAMP) AS TIMESTAMP))<='{end}'"  # noqa
        )
        order_by = ""
    elif query_type in {Definitions.postgres, Definitions.duck_db}:
        if field == "previous_order":
            if query_type == Definitions.duck_db:
                result_lookup = {"date": "DATE_TRUNC('DAY', CAST(simple.previous_order_date AS TIMESTAMP))"}
            else:
                result_lookup = {"date": "DATE_TRUNC('DAY', simple.previous_order_date)"}

        else:
            result_lookup = {
                "date": "DATE_TRUNC('DAY', CAST(CAST(CAST(simple.order_date AS TIMESTAMP) at time zone 'utc' at time zone 'America/New_York' AS TIMESTAMP) AS TIMESTAMP))",  # noqa
                "week": "DATE_TRUNC('WEEK', CAST(CAST(CAST(simple.order_date AS TIMESTAMP) at time zone 'utc' at time zone 'America/New_York' AS TIMESTAMP) AS TIMESTAMP) + INTERVAL '1' DAY) - INTERVAL '1' DAY",  # noqa
            }
        where = (
            "WHERE DATE_TRUNC('DAY', CAST(CAST(CAST(simple.order_date AS TIMESTAMP) at time zone 'utc' at time zone 'America/New_York' AS TIMESTAMP) "  # noqa
            f"AS TIMESTAMP))>='{start}' AND DATE_TRUNC('DAY', "
            f"CAST(CAST(CAST(simple.order_date AS TIMESTAMP) at time zone 'utc' at time zone 'America/New_York' AS TIMESTAMP) AS TIMESTAMP))<='{end}'"  # noqa
        )
        if query_type == Definitions.duck_db:
            order_by = " ORDER BY simple_total_revenue DESC"
        else:
            order_by = ""
    elif query_type == Definitions.bigquery:
        result_lookup = {
            "date": "CAST(DATE_TRUNC(CAST(CAST(DATETIME(CAST(simple.order_date AS TIMESTAMP), 'America/New_York') AS TIMESTAMP) AS DATE), DAY) AS TIMESTAMP)",  # noqa
            "week": "CAST(DATE_TRUNC(CAST(CAST(DATETIME(CAST(simple.order_date AS TIMESTAMP), 'America/New_York') AS TIMESTAMP) AS DATE) + 1, WEEK) - 1 AS TIMESTAMP)",  # noqa
        }
        where = (
            "WHERE CAST(DATETIME(CAST(simple.order_date AS TIMESTAMP), 'America/New_York')"
            f" AS TIMESTAMP)>=TIMESTAMP('{start}') AND CAST(DATETIME(CAST(simple.order_date "
            f"AS TIMESTAMP), 'America/New_York') AS TIMESTAMP)<=TIMESTAMP('{end}')"
        )
        order_by = ""
    elif query_type == Definitions.druid:
        if field == "previous_order":
            result_lookup = {"date": "DATE_TRUNC('DAY', CAST(simple.previous_order_date AS TIMESTAMP))"}
        else:
            result_lookup = {
                "date": "DATE_TRUNC('DAY', CAST(simple.order_date AS TIMESTAMP))",  # noqa
                "week": "DATE_TRUNC('WEEK', CAST(simple.order_date AS TIMESTAMP) + INTERVAL '1' DAY) - INTERVAL '1' DAY",  # noqa
            }
        where = (
            f"WHERE DATE_TRUNC('DAY', CAST(simple.order_date AS TIMESTAMP))>='{start}' "
            f"AND DATE_TRUNC('DAY', CAST(simple.order_date AS TIMESTAMP))<='{end}'"
        )
        order_by = ""
        semi = ""

    elif query_type == Definitions.sql_server:
        if field == "previous_order":
            result_lookup = {"date": "CAST(CAST(simple.previous_order_date AS DATE) AS DATETIME)"}
        else:
            result_lookup = {
                "date": "CAST(CAST(simple.order_date AS DATE) AS DATETIME)",  # noqa
                "week": "DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, DATEADD(DAY, 1, CAST(simple.order_date AS DATE))), 0))",  # noqa
            }
        where = (
            f"WHERE CAST(CAST(simple.order_date AS DATE) AS DATETIME)>='{start}' "
            f"AND CAST(CAST(simple.order_date AS DATE) AS DATETIME)<='{end}'"
        )
        order_by = ""

    date_result = result_lookup[group]

    correct = (
        f"SELECT {date_result} as simple_{field}_{group},SUM(simple.revenue) as "
        f"simple_total_revenue FROM analytics.orders simple {where} "
        f"GROUP BY {date_result if query_type != Definitions.bigquery else f'simple_{field}_{group}'}"
        f"{order_by}{semi}"
    )
    assert query == correct


@pytest.mark.parametrize(
    "group,query_type",
    [
        ("time", Definitions.snowflake),
        ("second", Definitions.snowflake),
        ("minute", Definitions.snowflake),
        ("hour", Definitions.snowflake),
        ("date", Definitions.snowflake),
        ("week", Definitions.snowflake),
        ("month", Definitions.snowflake),
        ("quarter", Definitions.snowflake),
        ("year", Definitions.snowflake),
        ("week_index", Definitions.snowflake),
        ("week_of_year", Definitions.snowflake),
        ("week_of_month", Definitions.snowflake),
        ("month_of_year_index", Definitions.snowflake),
        ("month_index", Definitions.snowflake),
        ("month_of_year", Definitions.snowflake),
        ("month_name", Definitions.snowflake),
        ("quarter_of_year", Definitions.snowflake),
        ("hour_of_day", Definitions.snowflake),
        ("day_of_week", Definitions.snowflake),
        ("day_of_month", Definitions.snowflake),
        ("day_of_year", Definitions.snowflake),
        ("time", Definitions.databricks),
        ("second", Definitions.databricks),
        ("minute", Definitions.databricks),
        ("hour", Definitions.databricks),
        ("date", Definitions.databricks),
        ("week", Definitions.databricks),
        ("month", Definitions.databricks),
        ("quarter", Definitions.databricks),
        ("year", Definitions.databricks),
        ("week_index", Definitions.databricks),
        ("week_of_month", Definitions.databricks),
        ("month_of_year_index", Definitions.databricks),
        ("month_of_year", Definitions.databricks),
        ("quarter_of_year", Definitions.databricks),
        ("hour_of_day", Definitions.databricks),
        ("day_of_week", Definitions.databricks),
        ("day_of_month", Definitions.databricks),
        ("day_of_year", Definitions.databricks),
        ("time", Definitions.druid),
        ("second", Definitions.druid),
        ("minute", Definitions.druid),
        ("hour", Definitions.druid),
        ("date", Definitions.druid),
        ("week", Definitions.druid),
        ("month", Definitions.druid),
        ("quarter", Definitions.druid),
        ("year", Definitions.druid),
        ("week_index", Definitions.druid),
        ("week_of_month", Definitions.druid),
        ("month_of_year_index", Definitions.druid),
        ("month_of_year", Definitions.druid),
        ("quarter_of_year", Definitions.druid),
        ("hour_of_day", Definitions.druid),
        ("day_of_week", Definitions.druid),
        ("day_of_month", Definitions.druid),
        ("day_of_year", Definitions.druid),
        ("time", Definitions.sql_server),
        ("second", Definitions.sql_server),
        ("minute", Definitions.sql_server),
        ("hour", Definitions.sql_server),
        ("date", Definitions.sql_server),
        ("week", Definitions.sql_server),
        ("month", Definitions.sql_server),
        ("quarter", Definitions.sql_server),
        ("year", Definitions.sql_server),
        ("week_index", Definitions.sql_server),
        ("week_of_month", Definitions.sql_server),
        ("month_of_year_index", Definitions.sql_server),
        ("month_of_year", Definitions.sql_server),
        ("quarter_of_year", Definitions.sql_server),
        ("hour_of_day", Definitions.sql_server),
        ("day_of_week", Definitions.sql_server),
        ("day_of_month", Definitions.sql_server),
        ("day_of_year", Definitions.sql_server),
        ("time", Definitions.redshift),
        ("second", Definitions.redshift),
        ("minute", Definitions.redshift),
        ("hour", Definitions.redshift),
        ("date", Definitions.redshift),
        ("week", Definitions.redshift),
        ("month", Definitions.redshift),
        ("quarter", Definitions.redshift),
        ("year", Definitions.redshift),
        ("week_index", Definitions.redshift),
        ("week_of_month", Definitions.redshift),
        ("month_of_year_index", Definitions.redshift),
        ("month_of_year", Definitions.redshift),
        ("quarter_of_year", Definitions.redshift),
        ("hour_of_day", Definitions.redshift),
        ("day_of_week", Definitions.redshift),
        ("day_of_month", Definitions.redshift),
        ("day_of_year", Definitions.redshift),
        ("time", Definitions.postgres),
        ("second", Definitions.postgres),
        ("minute", Definitions.postgres),
        ("hour", Definitions.postgres),
        ("date", Definitions.postgres),
        ("week", Definitions.postgres),
        ("month", Definitions.postgres),
        ("quarter", Definitions.postgres),
        ("year", Definitions.postgres),
        ("week_index", Definitions.postgres),
        ("week_of_month", Definitions.postgres),
        ("month_of_year_index", Definitions.postgres),
        ("month_of_year", Definitions.postgres),
        ("quarter_of_year", Definitions.postgres),
        ("hour_of_day", Definitions.postgres),
        ("day_of_week", Definitions.postgres),
        ("day_of_month", Definitions.postgres),
        ("day_of_year", Definitions.postgres),
        ("time", Definitions.duck_db),
        ("second", Definitions.duck_db),
        ("minute", Definitions.duck_db),
        ("hour", Definitions.duck_db),
        ("date", Definitions.duck_db),
        ("week", Definitions.duck_db),
        ("month", Definitions.duck_db),
        ("quarter", Definitions.duck_db),
        ("year", Definitions.duck_db),
        ("week_index", Definitions.duck_db),
        ("week_of_month", Definitions.duck_db),
        ("month_of_year_index", Definitions.duck_db),
        ("month_of_year", Definitions.duck_db),
        ("quarter_of_year", Definitions.duck_db),
        ("hour_of_day", Definitions.duck_db),
        ("day_of_week", Definitions.duck_db),
        ("day_of_month", Definitions.duck_db),
        ("day_of_year", Definitions.duck_db),
        ("time", Definitions.bigquery),
        ("second", Definitions.bigquery),
        ("minute", Definitions.bigquery),
        ("hour", Definitions.bigquery),
        ("date", Definitions.bigquery),
        ("week", Definitions.bigquery),
        ("month", Definitions.bigquery),
        ("quarter", Definitions.bigquery),
        ("year", Definitions.bigquery),
        ("week_index", Definitions.bigquery),
        ("week_of_month", Definitions.bigquery),
        ("month_of_year_index", Definitions.bigquery),
        ("month_of_year", Definitions.bigquery),
        ("quarter_of_year", Definitions.bigquery),
        ("hour_of_day", Definitions.bigquery),
        ("day_of_week", Definitions.bigquery),
        ("day_of_month", Definitions.bigquery),
        ("day_of_year", Definitions.bigquery),
    ],
)
@pytest.mark.query
def test_simple_query_dimension_group(connections, group: str, query_type: str):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue"], dimensions=[f"order_{group}"], query_type=query_type
    )
    field = project.get_field(f"order_{group}")

    semi = ";"
    if query_type in {Definitions.snowflake, Definitions.redshift}:
        result_lookup = {
            "time": "CAST(simple.order_date AS TIMESTAMP)",
            "second": "DATE_TRUNC('SECOND', simple.order_date)",
            "minute": "DATE_TRUNC('MINUTE', simple.order_date)",
            "hour": "DATE_TRUNC('HOUR', simple.order_date)",
            "date": "DATE_TRUNC('DAY', simple.order_date)",
            "week": "DATE_TRUNC('WEEK', CAST(simple.order_date AS DATE) + 1) - 1",
            "month": "DATE_TRUNC('MONTH', simple.order_date)",
            "quarter": "DATE_TRUNC('QUARTER', simple.order_date)",
            "year": "DATE_TRUNC('YEAR', simple.order_date)",
            "week_index": f"EXTRACT(WEEK FROM simple.order_date)",
            "week_of_year": f"EXTRACT(WEEK FROM simple.order_date)",
            "week_of_month": f"EXTRACT(WEEK FROM simple.order_date) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', simple.order_date)) + 1",  # noqa
            "month_of_year_index": f"EXTRACT(MONTH FROM simple.order_date)",
            "month_index": f"EXTRACT(MONTH FROM simple.order_date)",
            "month_of_year": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'MON')",
            "month_name": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'MON')",
            "quarter_of_year": "EXTRACT(QUARTER FROM simple.order_date)",
            "hour_of_day": "HOUR(CAST(simple.order_date AS TIMESTAMP))",
            "day_of_week": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'Dy')",
            "day_of_month": "EXTRACT(DAY FROM simple.order_date)",
            "day_of_year": "EXTRACT(DOY FROM simple.order_date)",
        }
        order_by = " ORDER BY simple_total_revenue DESC"

    elif query_type == Definitions.sql_server:
        result_lookup = {
            "time": "CAST(simple.order_date AS DATETIME)",
            "second": "DATEADD(SECOND, DATEDIFF(SECOND, 0, CAST(simple.order_date AS DATETIME)), 0)",
            "minute": "DATEADD(MINUTE, DATEDIFF(MINUTE, 0, CAST(simple.order_date AS DATETIME)), 0)",
            "hour": "DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(simple.order_date AS DATETIME)), 0)",
            "date": "CAST(CAST(simple.order_date AS DATE) AS DATETIME)",
            "week": "DATEADD(DAY, -1, DATEADD(WEEK, DATEDIFF(WEEK, 0, DATEADD(DAY, 1, CAST(simple.order_date AS DATE))), 0))",  # noqa
            "month": "DATEADD(MONTH, DATEDIFF(MONTH, 0, CAST(simple.order_date AS DATE)), 0)",
            "quarter": "DATEADD(QUARTER, DATEDIFF(QUARTER, 0, CAST(simple.order_date AS DATE)), 0)",
            "year": "DATEADD(YEAR, DATEDIFF(YEAR, 0, CAST(simple.order_date AS DATE)), 0)",
            "week_index": f"EXTRACT(WEEK FROM CAST(simple.order_date AS DATE))",
            "week_of_month": f"EXTRACT(WEEK FROM CAST(simple.order_date AS DATE)) - EXTRACT(WEEK FROM DATEADD(MONTH, DATEDIFF(MONTH, 0, CAST(simple.order_date AS DATE)), 0)) + 1",  # noqa
            "month_of_year_index": f"EXTRACT(MONTH FROM CAST(simple.order_date AS DATE))",
            "month_of_year": "LEFT(DATENAME(MONTH, CAST(simple.order_date AS DATE)), 3)",
            "quarter_of_year": "DATEPART(QUARTER, CAST(simple.order_date AS DATE))",
            "hour_of_day": "DATEPART(HOUR, CAST(simple.order_date AS DATETIME))",
            "day_of_week": "LEFT(DATENAME(WEEKDAY, CAST(simple.order_date AS DATE)), 3)",
            "day_of_month": "DATEPART(DAY, CAST(simple.order_date AS DATE))",
            "day_of_year": "DATEPART(DOY, CAST(simple.order_date AS DATE))",
        }
        order_by = ""

    elif query_type in {Definitions.postgres, Definitions.databricks, Definitions.druid, Definitions.duck_db}:
        result_lookup = {
            "time": "CAST(simple.order_date AS TIMESTAMP)",
            "second": "DATE_TRUNC('SECOND', CAST(simple.order_date AS TIMESTAMP))",
            "minute": "DATE_TRUNC('MINUTE', CAST(simple.order_date AS TIMESTAMP))",
            "hour": "DATE_TRUNC('HOUR', CAST(simple.order_date AS TIMESTAMP))",
            "date": "DATE_TRUNC('DAY', CAST(simple.order_date AS TIMESTAMP))",
            "week": "DATE_TRUNC('WEEK', CAST(simple.order_date AS TIMESTAMP) + INTERVAL '1' DAY) - INTERVAL '1' DAY",  # noqa
            "month": "DATE_TRUNC('MONTH', CAST(simple.order_date AS TIMESTAMP))",
            "quarter": "DATE_TRUNC('QUARTER', CAST(simple.order_date AS TIMESTAMP))",
            "year": "DATE_TRUNC('YEAR', CAST(simple.order_date AS TIMESTAMP))",
            "week_index": f"EXTRACT(WEEK FROM CAST(simple.order_date AS TIMESTAMP))",
            "week_of_month": f"EXTRACT(WEEK FROM CAST(simple.order_date AS TIMESTAMP)) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', CAST(simple.order_date AS TIMESTAMP))) + 1",  # noqa
            "month_of_year_index": f"EXTRACT(MONTH FROM CAST(simple.order_date AS TIMESTAMP))",
            "month_of_year": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'MON')",
            "quarter_of_year": "EXTRACT(QUARTER FROM CAST(simple.order_date AS TIMESTAMP))",
            "hour_of_day": "EXTRACT('HOUR' FROM CAST(simple.order_date AS TIMESTAMP))",
            "day_of_week": "TO_CHAR(CAST(simple.order_date AS TIMESTAMP), 'Dy')",
            "day_of_month": "EXTRACT('DAY' FROM CAST(simple.order_date AS TIMESTAMP))",
            "day_of_year": "EXTRACT('DOY' FROM CAST(simple.order_date AS TIMESTAMP))",
        }
        if query_type == Definitions.duck_db:
            order_by = " ORDER BY simple_total_revenue DESC"
        else:
            order_by = ""

        if query_type == Definitions.databricks:
            result_lookup["month_of_year"] = "DATE_FORMAT(CAST(simple.order_date AS TIMESTAMP), 'MMM')"
            result_lookup["hour_of_day"] = "EXTRACT(HOUR FROM CAST(simple.order_date AS TIMESTAMP))"
            result_lookup["day_of_week"] = "DATE_FORMAT(CAST(simple.order_date AS TIMESTAMP), 'E')"
            result_lookup["day_of_month"] = "EXTRACT(DAY FROM CAST(simple.order_date AS TIMESTAMP))"
            result_lookup["day_of_year"] = "EXTRACT(DOY FROM CAST(simple.order_date AS TIMESTAMP))"
        if query_type == Definitions.druid:
            result_lookup[
                "month_of_year"
            ] = "CASE EXTRACT(MONTH FROM CAST(simple.order_date AS TIMESTAMP)) WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar' WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun' WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep' WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec' ELSE 'Invalid Month' END"  # noqa
            result_lookup["hour_of_day"] = "EXTRACT(HOUR FROM CAST(simple.order_date AS TIMESTAMP))"
            result_lookup[
                "day_of_week"
            ] = "CASE EXTRACT(DOW FROM CAST(simple.order_date AS TIMESTAMP)) WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue' WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat' WHEN 7 THEN 'Sun' ELSE 'Invalid Day' END"  # noqa
            result_lookup["day_of_month"] = "EXTRACT(DAY FROM CAST(simple.order_date AS TIMESTAMP))"
            result_lookup["day_of_year"] = "EXTRACT(DOY FROM CAST(simple.order_date AS TIMESTAMP))"
            semi = ""
    elif query_type == Definitions.bigquery:
        result_lookup = {
            "time": "CAST(simple.order_date AS TIMESTAMP)",
            "second": "CAST(DATETIME_TRUNC(CAST(simple.order_date AS DATETIME), SECOND) AS TIMESTAMP)",
            "minute": "CAST(DATETIME_TRUNC(CAST(simple.order_date AS DATETIME), MINUTE) AS TIMESTAMP)",
            "hour": "CAST(DATETIME_TRUNC(CAST(simple.order_date AS DATETIME), HOUR) AS TIMESTAMP)",
            "date": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), DAY) AS TIMESTAMP)",
            "week": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE) + 1, WEEK) - 1 AS TIMESTAMP)",
            "month": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), MONTH) AS TIMESTAMP)",
            "quarter": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), QUARTER) AS TIMESTAMP)",
            "year": "CAST(DATE_TRUNC(CAST(simple.order_date AS DATE), YEAR) AS TIMESTAMP)",
            "week_index": f"EXTRACT(WEEK FROM simple.order_date)",
            "week_of_month": f"EXTRACT(WEEK FROM simple.order_date) - EXTRACT(WEEK FROM DATE_TRUNC(CAST(simple.order_date AS DATE), MONTH)) + 1",  # noqa
            "month_of_year_index": f"EXTRACT(MONTH FROM simple.order_date)",
            "month_of_year": "FORMAT_DATETIME('%B', CAST(simple.order_date as DATETIME))",
            "quarter_of_year": "EXTRACT(QUARTER FROM simple.order_date)",
            "hour_of_day": f"CAST(simple.order_date AS STRING FORMAT 'HH24')",
            "day_of_week": f"CAST(simple.order_date AS STRING FORMAT 'DAY')",
            "day_of_month": "EXTRACT(DAY FROM simple.order_date)",
            "day_of_year": "EXTRACT(DAYOFYEAR FROM simple.order_date)",
        }
        order_by = ""
    else:
        raise ValueError(f"Query type {query_type} not supported")

    date_result = result_lookup[group]

    correct = (
        f"SELECT {date_result} as simple_order_{group},SUM(simple.revenue) as "
        f"simple_total_revenue FROM analytics.orders simple "
        f"GROUP BY {date_result if query_type != Definitions.bigquery else f'simple_order_{group}'}"
        f"{order_by}{semi}"
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
        ("second", Definitions.databricks),
        ("minute", Definitions.databricks),
        ("hour", Definitions.databricks),
        ("day", Definitions.databricks),
        ("week", Definitions.databricks),
        ("month", Definitions.databricks),
        ("quarter", Definitions.databricks),
        ("year", Definitions.databricks),
        ("second", Definitions.druid),
        ("minute", Definitions.druid),
        ("hour", Definitions.druid),
        ("day", Definitions.druid),
        ("week", Definitions.druid),
        ("month", Definitions.druid),
        ("quarter", Definitions.druid),
        ("year", Definitions.druid),
        ("second", Definitions.sql_server),
        ("minute", Definitions.sql_server),
        ("hour", Definitions.sql_server),
        ("day", Definitions.sql_server),
        ("week", Definitions.sql_server),
        ("month", Definitions.sql_server),
        ("quarter", Definitions.sql_server),
        ("year", Definitions.sql_server),
        ("second", Definitions.redshift),
        ("minute", Definitions.redshift),
        ("hour", Definitions.redshift),
        ("day", Definitions.redshift),
        ("week", Definitions.redshift),
        ("month", Definitions.redshift),
        ("quarter", Definitions.redshift),
        ("year", Definitions.redshift),
        ("second", Definitions.postgres),
        ("minute", Definitions.postgres),
        ("hour", Definitions.postgres),
        ("day", Definitions.postgres),
        ("week", Definitions.postgres),
        ("month", Definitions.postgres),
        ("quarter", Definitions.postgres),
        ("year", Definitions.postgres),
        ("second", Definitions.duck_db),
        ("minute", Definitions.duck_db),
        ("hour", Definitions.duck_db),
        ("day", Definitions.duck_db),
        ("week", Definitions.duck_db),
        ("month", Definitions.duck_db),
        ("quarter", Definitions.duck_db),
        ("year", Definitions.duck_db),
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
def test_simple_query_dimension_group_interval(connections, interval: str, query_type: str):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
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

    semi = ";"
    if query_type in {Definitions.snowflake, Definitions.redshift, Definitions.duck_db}:
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
    elif query_type == Definitions.druid:
        result_lookup = {
            "second": "TIMESTAMPDIFF(SECOND, simple.view_date, simple.order_date)",
            "minute": "TIMESTAMPDIFF(MINUTE, simple.view_date, simple.order_date)",
            "hour": "TIMESTAMPDIFF(HOUR, simple.view_date, simple.order_date)",
            "day": "TIMESTAMPDIFF(DAY, simple.view_date, simple.order_date)",
            "week": "TIMESTAMPDIFF(WEEK, simple.view_date, simple.order_date)",
            "month": "TIMESTAMPDIFF(MONTH, simple.view_date, simple.order_date)",
            "quarter": "TIMESTAMPDIFF(QUARTER, simple.view_date, simple.order_date)",
            "year": "TIMESTAMPDIFF(YEAR, simple.view_date, simple.order_date)",
        }
        order_by = ""
        semi = ""
    elif query_type in {Definitions.sql_server, Definitions.databricks}:
        result_lookup = {
            "second": "DATEDIFF(SECOND, simple.view_date, simple.order_date)",
            "minute": "DATEDIFF(MINUTE, simple.view_date, simple.order_date)",
            "hour": "DATEDIFF(HOUR, simple.view_date, simple.order_date)",
            "day": "DATEDIFF(DAY, simple.view_date, simple.order_date)",
            "week": "DATEDIFF(WEEK, simple.view_date, simple.order_date)",
            "month": "DATEDIFF(MONTH, simple.view_date, simple.order_date)",
            "quarter": "DATEDIFF(QUARTER, simple.view_date, simple.order_date)",
            "year": "DATEDIFF(YEAR, simple.view_date, simple.order_date)",
        }
        order_by = ""
    elif query_type == Definitions.postgres:
        result_lookup = {
            "second": "DATE_PART('DAY', AGE(simple.order_date, simple.view_date)) * 24 + DATE_PART('HOUR', AGE(simple.order_date, simple.view_date)) * 60 + DATE_PART('MINUTE', AGE(simple.order_date, simple.view_date)) * 60 + DATE_PART('SECOND', AGE(simple.order_date, simple.view_date))",  # noqa
            "minute": "DATE_PART('DAY', AGE(simple.order_date, simple.view_date)) * 24 + DATE_PART('HOUR', AGE(simple.order_date, simple.view_date)) * 60 + DATE_PART('MINUTE', AGE(simple.order_date, simple.view_date))",  # noqa
            "hour": "DATE_PART('DAY', AGE(simple.order_date, simple.view_date)) * 24 + DATE_PART('HOUR', AGE(simple.order_date, simple.view_date))",  # noqa
            "day": "DATE_PART('DAY', AGE(simple.order_date, simple.view_date))",
            "week": "TRUNC(DATE_PART('DAY', AGE(simple.order_date, simple.view_date))/7)",
            "month": "DATE_PART('YEAR', AGE(simple.order_date, simple.view_date)) * 12 + (DATE_PART('month', AGE(simple.order_date, simple.view_date)))",  # noqa
            "quarter": "DATE_PART('YEAR', AGE(simple.order_date, simple.view_date)) * 4 + TRUNC(DATE_PART('month', AGE(simple.order_date, simple.view_date))/3)",  # noqa
            "year": "DATE_PART('YEAR', AGE(simple.order_date, simple.view_date))",
        }
        order_by = ""
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
            f"analytics.orders simple GROUP BY {interval_result if query_type != Definitions.bigquery else f'simple_{interval}s_waiting'}"  # noqa
            f"{order_by}{semi}"
        )
        assert query == correct

        correct_label = f"{interval.replace('_', ' ').title()}s Between view and order"
        assert field.label == correct_label


@pytest.mark.query
def test_simple_query_two_group_by(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["channel", "new_vs_repeat"])

    correct = (
        "SELECT simple.sales_channel as simple_channel,simple.new_vs_repeat as simple_new_vs_repeat,"
        "SUM(simple.revenue) as simple_total_revenue FROM analytics.orders simple "
        "GROUP BY simple.sales_channel,simple.new_vs_repeat ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_two_metric(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
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
def test_simple_query_custom_dimension(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["is_valid_order"])

    correct = (
        "SELECT CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END as simple_is_valid_order,"
        "SUM(simple.revenue) as simple_total_revenue FROM analytics.orders simple"
        " GROUP BY CASE WHEN simple.sales_channel != 'fraud' THEN TRUE ELSE FALSE END "
        "ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_custom_metric(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
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
        ("order_date", "greater_than", "2021-08-04", Definitions.databricks),
        ("order_date", "greater_than", "2021-08-04", Definitions.druid),
        ("order_date", "greater_than", "2021-08-04", Definitions.sql_server),
        ("order_date", "greater_than", "2021-08-04", Definitions.redshift),
        ("order_date", "greater_than", "2021-08-04", Definitions.bigquery),
        ("order_date", "greater_than", "2021-08-04", Definitions.duck_db),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.snowflake),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.databricks),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.druid),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.sql_server),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.redshift),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.bigquery),
        ("order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.duck_db),
        ("previous_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.snowflake),
        ("previous_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.databricks),
        ("previous_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.druid),
        ("previous_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.sql_server),
        ("previous_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.redshift),
        ("previous_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.bigquery),
        ("previous_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.duck_db),
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.snowflake),
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.databricks),
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.druid),
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.sql_server),
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.redshift),
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.bigquery),
        ("first_order_date", "greater_than", datetime(year=2021, month=8, day=4), Definitions.duck_db),
        ("order_date", "matches", "last week", Definitions.snowflake),
        ("order_date", "matches", "last year", Definitions.snowflake),
        ("order_date", "matches", "last year", Definitions.databricks),
        ("order_date", "matches", "last year", Definitions.druid),
        ("order_date", "matches", "last year", Definitions.sql_server),
        ("order_date", "matches", "last year", Definitions.redshift),
        ("order_date", "matches", "last year", Definitions.bigquery),
        ("order_date", "matches", "last year", Definitions.duck_db),
    ],
)
@pytest.mark.query
def test_simple_query_with_where_dim_group(connections, field, expression, value, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=["channel"],
        where=[{"field": field, "expression": expression, "value": value}],
        query_type=query_type,
    )

    if query_type in {
        Definitions.bigquery,
        Definitions.databricks,
        Definitions.druid,
        Definitions.sql_server,
    }:
        order_by = ""
    else:
        order_by = " ORDER BY simple_total_revenue DESC"

    semi = ";"
    if query_type == Definitions.druid:
        semi = ""
    sf_or_rs = query_type in {
        Definitions.snowflake,
        Definitions.redshift,
        Definitions.druid,
        Definitions.duck_db,
        Definitions.databricks,
    }
    if query_type not in {Definitions.druid, Definitions.duck_db, Definitions.databricks}:
        field_id = f"simple.{field}"
    else:
        field_id = f"CAST(simple.{field} AS TIMESTAMP)"
    if sf_or_rs and expression == "greater_than" and isinstance(value, str):
        condition = f"DATE_TRUNC('DAY', {field_id})>'2021-08-04'"
    elif query_type == Definitions.sql_server and isinstance(value, str) and expression == "greater_than":
        condition = f"CAST(CAST({field_id} AS DATE) AS DATETIME)>'2021-08-04'"
    elif (
        query_type == Definitions.sql_server and isinstance(value, datetime) and expression == "greater_than"
    ):
        condition = f"CAST(CAST({field_id} AS DATE) AS DATETIME)>'2021-08-04T00:00:00'"
    elif sf_or_rs and isinstance(value, datetime):
        condition = f"DATE_TRUNC('DAY', {field_id})>'2021-08-04T00:00:00'"
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
    elif sf_or_rs and expression == "matches" and value == "last year":
        last_year = pendulum.now("UTC").year - 1
        condition = f"DATE_TRUNC('DAY', {field_id})>='{last_year}-01-01T00:00:00' AND "
        condition += f"DATE_TRUNC('DAY', {field_id})<='{last_year}-12-31T23:59:59'"
    elif query_type == Definitions.sql_server and expression == "matches" and value == "last year":
        last_year = pendulum.now("UTC").year - 1
        condition = f"CAST(CAST({field_id} AS DATE) AS DATETIME)>='{last_year}-01-01T00:00:00' AND "
        condition += f"CAST(CAST({field_id} AS DATE) AS DATETIME)<='{last_year}-12-31T23:59:59'"
    elif sf_or_rs and expression == "matches" and value == "last week":
        date_format = "%Y-%m-%dT%H:%M:%S"
        pendulum.week_starts_at(pendulum.SUNDAY)
        pendulum.week_ends_at(pendulum.SATURDAY)
        start_of = pendulum.now("UTC").subtract(days=7).start_of("week").strftime(date_format)
        end_of = pendulum.now("UTC").subtract(days=7).end_of("week").strftime(date_format)
        condition = f"DATE_TRUNC('DAY', {field_id})>='{start_of}' AND "
        condition += f"DATE_TRUNC('DAY', {field_id})<='{end_of}'"
        pendulum.week_starts_at(pendulum.MONDAY)
        pendulum.week_ends_at(pendulum.SUNDAY)
    elif query_type == Definitions.bigquery and expression == "matches":
        last_year = pendulum.now("UTC").year - 1
        condition = f"CAST(DATE_TRUNC(CAST(simple.{field} AS DATE), DAY) AS TIMESTAMP)>=TIMESTAMP('{last_year}-01-01T00:00:00') AND "  # noqa
        condition += f"CAST(DATE_TRUNC(CAST(simple.{field} AS DATE), DAY) AS TIMESTAMP)<=TIMESTAMP('{last_year}-12-31T23:59:59')"  # noqa

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        f"analytics.orders simple WHERE {condition} "
        f"GROUP BY {'simple.sales_channel' if query_type != Definitions.bigquery else 'simple_channel'}"
        f"{order_by}{semi}"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_convert_tz_alias_no(connections):
    fields = [f if f["name"] != "order" else {**f, "convert_tz": False} for f in simple_view["fields"]]
    project = Project(models=[simple_model], views=[{**simple_view, "fields": fields}])
    project.set_timezone("America/New_York")
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(metrics=["total_revenue"], dimensions=["order_date"])

    correct = (
        "SELECT DATE_TRUNC('DAY', simple.order_date) as simple_order_date,"
        "SUM(simple.revenue) as simple_total_revenue FROM analytics.orders simple "
        "GROUP BY DATE_TRUNC('DAY', simple.order_date) ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


# Druid does not support ilike
@pytest.mark.parametrize(
    "field_name,filter_type,value,query_type",
    [
        ("channel", "equal_to", "Email", Definitions.snowflake),
        ("channel", "not_equal_to", "Email", Definitions.snowflake),
        ("channel", "contains", "Email", Definitions.snowflake),
        ("channel", "does_not_contain", "Email", Definitions.snowflake),
        ("channel", "contains_case_insensitive", "Email", Definitions.snowflake),
        ("channel", "contains_case_insensitive", "Email", Definitions.databricks),
        ("channel", "contains_case_insensitive", "Email", Definitions.druid),
        ("channel", "contains_case_insensitive", "Email", Definitions.sql_server),
        ("channel", "does_not_contain_case_insensitive", "Email", Definitions.snowflake),
        ("channel", "does_not_contain_case_insensitive", "Email", Definitions.databricks),
        ("channel", "does_not_contain_case_insensitive", "Email", Definitions.druid),
        ("channel", "does_not_contain_case_insensitive", "Email", Definitions.sql_server),
        ("channel", "starts_with", "Email", Definitions.snowflake),
        ("channel", "ends_with", "Email", Definitions.snowflake),
        ("channel", "does_not_start_with", "Email", Definitions.snowflake),
        ("channel", "does_not_end_with", "Email", Definitions.snowflake),
        ("channel", "starts_with_case_insensitive", "Email", Definitions.snowflake),
        ("channel", "ends_with_case_insensitive", "Email", Definitions.snowflake),
        ("channel", "does_not_start_with_case_insensitive", "Email", Definitions.snowflake),
        ("channel", "does_not_end_with_case_insensitive", "Email", Definitions.snowflake),
        ("is_valid_order", "is_null", None, Definitions.snowflake),
        ("is_valid_order", "is_not_null", None, Definitions.snowflake),
        ("is_valid_order", "is_not_null", None, Definitions.databricks),
        ("is_valid_order", "is_not_null", None, Definitions.druid),
        ("is_valid_order", "is_not_null", None, Definitions.sql_server),
        ("is_valid_order", "boolean_true", None, Definitions.snowflake),
        ("is_valid_order", "boolean_false", None, Definitions.snowflake),
        ("is_valid_order", "boolean_true", None, Definitions.sql_server),
        ("is_valid_order", "boolean_false", None, Definitions.sql_server),
    ],
)
@pytest.mark.query
def test_simple_query_with_where_dict(connections, field_name, filter_type, value, query_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)

    query = conn.get_sql_query(
        metrics=["total_revenue"],
        dimensions=[f"simple.channel"],
        where=[{"field": field_name, "expression": filter_type, "value": value}],
        query_type=query_type,
    )

    if query_type == Definitions.snowflake:
        order_by = " ORDER BY simple_total_revenue DESC"
        semi = ";"
    elif query_type == Definitions.druid:
        order_by = ""
        semi = ""
    else:
        order_by = ""
        semi = ";"
    result_lookup = {
        "equal_to": f"='{value}'",
        "not_equal_to": f"<>'{value}'",
        "contains": f" LIKE '%{value}%'",
        "does_not_contain": f" NOT LIKE '%{value}%'",
        "contains_case_insensitive": f" LIKE LOWER('%{value}%')",
        "does_not_contain_case_insensitive": f" NOT LIKE LOWER('%{value}%')",
        "starts_with": f" LIKE '{value}%'",
        "ends_with": f" LIKE '%{value}'",
        "does_not_start_with": f" NOT LIKE '{value}%'",
        "does_not_end_with": f" NOT LIKE '%{value}'",
        "starts_with_case_insensitive": f" LIKE LOWER('{value}%')",
        "ends_with_case_insensitive": f" LIKE LOWER('%{value}')",
        "does_not_start_with_case_insensitive": f" NOT LIKE LOWER('{value}%')",
        "does_not_end_with_case_insensitive": f" NOT LIKE LOWER('%{value}')",
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
    dim_func = {
        "contains_case_insensitive": lambda d: f"LOWER({d})",
        "does_not_contain_case_insensitive": lambda d: f"LOWER({d})",
        "starts_with_case_insensitive": lambda d: f"LOWER({d})",
        "ends_with_case_insensitive": lambda d: f"LOWER({d})",
        "does_not_start_with_case_insensitive": lambda d: f"LOWER({d})",
        "does_not_end_with_case_insensitive": lambda d: f"LOWER({d})",
    }

    filter_expr = result_lookup[filter_type]
    prefix_expr = prefix_filter.get(filter_type, "")
    dim_expr = dim_lookup[field_name]
    dim_func = dim_func.get(filter_type, lambda d: d)
    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        f"analytics.orders simple WHERE {prefix_expr}{dim_func(dim_expr)}{filter_expr} "
        f"GROUP BY simple.sales_channel{order_by}{semi}"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_where_literal(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue"], dimensions=["simple.channel"], where="${simple.channel} != 'Email'"
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
def test_simple_query_with_having_dict(connections, filter_type):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
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
def test_simple_query_with_having_literal(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], having="${total_revenue} > 12"
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        "analytics.orders simple GROUP BY simple.sales_channel HAVING (SUM(simple.revenue)) > 12 "
        "ORDER BY simple_total_revenue DESC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_order_by_dict(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue", "average_order_value"],
        dimensions=["channel"],
        order_by=[{"field": "total_revenue", "sort": "asc"}, {"field": "average_order_value"}],
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue,"
        "AVG(simple.revenue) as simple_average_order_value FROM analytics.orders simple "
        "GROUP BY simple.sales_channel ORDER BY simple_total_revenue ASC,simple_average_order_value ASC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_order_by_literal(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
    query = conn.get_sql_query(
        metrics=["total_revenue"], dimensions=["channel"], order_by="total_revenue asc"
    )

    correct = (
        "SELECT simple.sales_channel as simple_channel,SUM(simple.revenue) as simple_total_revenue FROM "
        "analytics.orders simple GROUP BY simple.sales_channel ORDER BY simple_total_revenue ASC;"
    )
    assert query == correct


@pytest.mark.query
def test_simple_query_with_all(connections):
    project = Project(models=[simple_model], views=[simple_view])
    conn = MetricsLayerConnection(project=project, connections=connections)
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
        "GROUP BY simple.sales_channel HAVING SUM(simple.revenue)>12 ORDER BY simple_total_revenue ASC;"
    )
    assert query == correct
